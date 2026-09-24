# ruff: noqa: ANN401
"""Canonical bounded redaction for errors, logs, reports, and evidence.

Rendering applies three rule families in a fixed priority. Exact secrets come first: values the
redactor or the request's diagnostic context registered, then secret-bearing URL paths, query values,
bearer tokens, JSON fields, environment assignments and cookies. The structured diagnostic context
comes second: caller field names become request-local ``field#N`` aliases. The conservative
bare-credential heuristic for free text comes last, so recognizing an identifier never overrides a
known secret. Without a context and without registered secrets the output is exactly the historical
free-text redaction; the one declared change is that mapping keys the rules hide stay distinct.
"""

from __future__ import annotations
import json
import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from b24api._diagnostics import DiagnosticContext

REDACTED = "[REDACTED]"
TRUNCATED = "[TRUNCATED]"
_WEBHOOK_RE = re.compile(r"https?://[^\s/]+/rest/(?:api/)?[0-9]+/[A-Za-z0-9_-]{6,}/?", re.IGNORECASE)
_QUERY_SECRET_RE = re.compile(
    r"(?P<prefix>[?&](?:auth|access_token|refresh_token)=)[^\s&#]+",
    re.IGNORECASE,
)
_BEARER_RE = re.compile(r"(?P<prefix>\bBearer\s+)[A-Za-z0-9._~+/=-]+", re.IGNORECASE)
_JSON_SECRET_RE = re.compile(
    r"(?P<prefix>[\"'](?:auth|access_token|refresh_token|client_secret|application_token)[\"']\s*:\s*[\"'])"
    r"[^\"']*(?P<suffix>[\"'])",
    re.IGNORECASE,
)
_ENV_SECRET_RE = re.compile(
    r"(?P<prefix>\b(?:AUTH_ID|APPLICATION_TOKEN|ACCESS_TOKEN|REFRESH_TOKEN|CLIENT_SECRET)=)[^\s,;]+",
    re.IGNORECASE,
)
_COOKIE_RE = re.compile(r"(?P<prefix>\b(?:Cookie|Set-Cookie)\s*:\s*)[^\r\n]+", re.IGNORECASE)
_BARE_CREDENTIAL_RE = re.compile(r"\b[A-Za-z0-9_-]{16,}\b")
_WEBHOOK_CREDENTIAL_RE = re.compile(r"/rest/(?:api/)?[^/\s]+/(?P<token>[^/\s?#]+)/", re.IGNORECASE)
_HIDDEN_KEY = "[REDACTED#{}]"
# Shorter values cannot be told apart from ordinary words, so they are never registered as exact secrets.
MINIMUM_SECRET_LENGTH = 6

DEFAULT_SENSITIVE_KEYS = frozenset(
    {
        "access_token",
        "application_token",
        "auth",
        "auth_id",
        "authorization",
        "client_secret",
        "cookie",
        "password",
        "refresh_token",
        "secret",
        "set-cookie",
        "token",
        "webhook",
        "webhook_url",
    },
)

PathPart = str | int


class SafeText(str):
    """Text already composed from rendered parts; rendering it again returns it unchanged.

    Only the error hierarchy constructs it, around a message whose every caller- or portal-derived
    part went through ``render_text`` or ``render_code``. Any string operation on it yields a plain
    ``str``, so derived text is rendered as free text again.
    """

    __slots__ = ()


def secret_values(values: Collection[str]) -> frozenset[str]:
    """Normalize exact secret registrations, dropping values too short to be told apart from words."""
    if isinstance(values, str):
        raise TypeError("known secrets must be a collection of strings, not one string")
    if not all(isinstance(value, str) for value in values):
        raise TypeError("known secrets must be strings")
    return frozenset(value for value in values if len(value) >= MINIMUM_SECRET_LENGTH)


def webhook_secrets(webhook_url: str) -> frozenset[str]:
    """Return the credential segment of a classic or V3 webhook URL as an exact secret."""
    match = _WEBHOOK_CREDENTIAL_RE.search(webhook_url)
    return secret_values((match.group("token"),)) if match is not None else frozenset()


def is_secret(value: str, secrets: frozenset[str], *, ignore_case: bool = False) -> bool:
    """Report whether a whole value equals an exact secret, optionally up to letter case."""
    if value in secrets:
        return True
    folded = value.casefold()
    return ignore_case and any(secret.casefold() == folded for secret in secrets)


def replace_secrets(text: str, secrets: frozenset[str]) -> str:
    """Replace every occurrence of every exact secret, longest first so an overlap leaves no suffix."""
    for secret in sorted(secrets, key=len, reverse=True):
        if secret in text:
            text = text.replace(secret, REDACTED)
    return text


@dataclass(slots=True)
class _RenderState:
    """State of one rendering call: the recursion guard and this call's hidden-key numbering."""

    context: DiagnosticContext | None
    active: set[int] = field(default_factory=set)
    hidden: dict[str, int] = field(default_factory=dict)
    next_hidden: int = 1

    def hidden_label(self, key: str, taken: set[str]) -> str:
        """Number a hidden key by first appearance, never reusing a label already shown in one mapping."""
        number = self.hidden.get(key)
        if number is None or _HIDDEN_KEY.format(number) in taken:
            number = self.next_hidden
            while _HIDDEN_KEY.format(number) in taken:
                number += 1
            self.next_hidden = number + 1
            self.hidden.setdefault(key, number)
        return _HIDDEN_KEY.format(number)


@dataclass(frozen=True, slots=True)
class Redactor:
    """Recursively scrub secrets and configured PII while bounding output."""

    sensitive_keys: frozenset[str] = DEFAULT_SENSITIVE_KEYS
    secret_paths: tuple[tuple[PathPart, ...], ...] = ()
    pii_fields: frozenset[str] = field(default_factory=frozenset)
    max_depth: int = 12
    max_items: int = 100
    max_string: int = 500
    known_secrets: frozenset[str] = field(default_factory=frozenset, repr=False)

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        if self.max_depth < 1:
            raise ValueError("max_depth must be positive")
        if self.max_items < 1:
            raise ValueError("max_items must be positive")
        if self.max_string < len(TRUNCATED) + 1:
            raise ValueError("max_string is too small")
        object.__setattr__(self, "known_secrets", secret_values(self.known_secrets))

    def is_known_secret(self, value: str, *, ignore_case: bool = False) -> bool:
        """Report whether a whole value equals a registered exact secret, optionally up to letter case."""
        return is_secret(value, self.known_secrets, ignore_case=ignore_case)

    def redact_text(self, value: str) -> str:
        """Scrub credential-bearing textual forms and apply a hard length bound."""
        return self.render_text(value)

    def render_text(self, value: str, *, context: DiagnosticContext | None = None) -> str:
        """Render free text: exact secrets, then request field aliases, then the bare-credential heuristic."""
        if isinstance(value, SafeText):
            return value
        scrubbed = replace_secrets(value, self.known_secrets)
        if context is not None:
            scrubbed = context.replace_secrets(scrubbed)
        scrubbed = _WEBHOOK_RE.sub(REDACTED, scrubbed)
        scrubbed = _QUERY_SECRET_RE.sub(lambda match: match.group("prefix") + REDACTED, scrubbed)
        scrubbed = _BEARER_RE.sub(lambda match: match.group("prefix") + REDACTED, scrubbed)
        scrubbed = _JSON_SECRET_RE.sub(
            lambda match: match.group("prefix") + REDACTED + match.group("suffix"),
            scrubbed,
        )
        scrubbed = _ENV_SECRET_RE.sub(lambda match: match.group("prefix") + REDACTED, scrubbed)
        scrubbed = _COOKIE_RE.sub(lambda match: match.group("prefix") + REDACTED, scrubbed)
        if context is not None:
            scrubbed = context.alias_text(scrubbed)
        scrubbed = _BARE_CREDENTIAL_RE.sub(_redact_bare_credential, scrubbed)
        if len(scrubbed) <= self.max_string:
            return scrubbed
        keep = self.max_string - len(TRUNCATED)
        return scrubbed[:keep] + TRUNCATED

    def redact(self, value: Any, *, context: DiagnosticContext | None = None) -> Any:
        """Return a detached JSON-compatible redacted representation."""
        return self._redact(value, path=(), depth=0, state=_RenderState(context))

    def safe_preview(
        self,
        body: bytes | str | None,
        *,
        max_chars: int = 500,
        context: DiagnosticContext | None = None,
    ) -> str | None:
        """Create a bounded redacted body preview without retaining the raw body."""
        if body is None:
            return None
        text = body.decode("utf-8", errors="replace") if isinstance(body, bytes) else body
        try:
            parsed = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            preview = self.render_text(text, context=context)
        else:
            redacted = self.redact(parsed, context=context)
            preview = json.dumps(redacted, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        if len(preview) <= max_chars:
            return preview
        keep = max(0, max_chars - len(TRUNCATED))
        return preview[:keep] + TRUNCATED

    def _redact(  # noqa: PLR0911
        self,
        value: Any,
        *,
        path: tuple[PathPart, ...],
        depth: int,
        state: _RenderState,
    ) -> Any:
        if self._is_secret_path(path):
            return REDACTED
        if isinstance(value, str):
            return self.render_text(value, context=state.context)
        if value is None or isinstance(value, bool | int | float):
            return value
        if depth >= self.max_depth:
            return TRUNCATED
        if isinstance(value, Mapping):
            return self._redact_mapping(value, path=path, depth=depth, state=state)
        if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray):
            return self._redact_sequence(value, path=path, depth=depth, state=state)
        return self.render_text(str(value), context=state.context)

    def _redact_mapping(
        self,
        value: Mapping[object, object],
        *,
        path: tuple[PathPart, ...],
        depth: int,
        state: _RenderState,
    ) -> dict[str, Any] | str:
        identity = id(value)
        if identity in state.active:
            return TRUNCATED
        state.active.add(identity)
        try:
            entries = [
                (str(raw_key), *self._render_key(str(raw_key), state.context), item)
                for raw_key, item in list(value.items())[: self.max_items]
            ]
            taken = {label for _, label, _, _ in entries if label is not None}
            redacted: dict[str, Any] = {}
            for raw, label, key, item in entries:
                # A hidden key gets a distinct numbered label instead of merging into one "[REDACTED]" key;
                # a shown label that some earlier key already produced is hidden the same way.
                shown = label if label is not None and label not in redacted else state.hidden_label(raw, taken)
                taken.add(shown)
                # The value keeps the historical key for sensitivity and configured paths; only the label changed.
                if self._is_sensitive_key(key):
                    redacted[shown] = REDACTED
                else:
                    redacted[shown] = self._redact(item, path=(*path, key), depth=depth + 1, state=state)
            if len(value) > self.max_items:
                redacted[TRUNCATED] = TRUNCATED
            return redacted
        finally:
            state.active.remove(identity)

    def _render_key(self, raw: str, context: DiagnosticContext | None) -> tuple[str | None, str]:
        """Return the shown label, or None for a key that must be hidden, and the historical key text."""
        historical = self.render_text(raw)
        if context is None:
            return (raw if historical == raw else None), historical
        # A key is shown only when request field aliases are the one change rendering made to it.
        aliased = context.alias_text(raw)
        shown = aliased if self.render_text(raw, context=context) == aliased else None
        return shown, historical

    def _redact_sequence(
        self,
        value: Sequence[object],
        *,
        path: tuple[PathPart, ...],
        depth: int,
        state: _RenderState,
    ) -> list[Any] | str:
        identity = id(value)
        if identity in state.active:
            return TRUNCATED
        state.active.add(identity)
        try:
            redacted = [
                self._redact(item, path=(*path, index), depth=depth + 1, state=state)
                for index, item in enumerate(value[: self.max_items])
            ]
            if len(value) > self.max_items:
                redacted.append(TRUNCATED)
            return redacted
        finally:
            state.active.remove(identity)

    def _is_sensitive_key(self, key: str) -> bool:
        normalized = key.casefold().replace("-", "_")
        sensitive = {item.casefold().replace("-", "_") for item in self.sensitive_keys | self.pii_fields}
        return normalized in sensitive

    def _is_secret_path(self, path: tuple[PathPart, ...]) -> bool:
        normalized_path = tuple(item.casefold() if isinstance(item, str) else item for item in path)
        return any(
            normalized_path == tuple(item.casefold() if isinstance(item, str) else item for item in configured_path)
            for configured_path in self.secret_paths
        )


DEFAULT_REDACTOR = Redactor()


def _redact_bare_credential(match: re.Match[str]) -> str:
    candidate = match.group(0)
    if len(candidate) in {40, 64} and all(character in "0123456789abcdefABCDEF" for character in candidate):
        return candidate
    if any(character.isalpha() for character in candidate) and any(character.isdigit() for character in candidate):
        return REDACTED
    return candidate
