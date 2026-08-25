# ruff: noqa: ANN401
"""Canonical bounded redaction for errors, logs, reports, and evidence."""

from __future__ import annotations
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

REDACTED = "[REDACTED]"
TRUNCATED = "[TRUNCATED]"
_WEBHOOK_RE = re.compile(r"https?://[^\s/]+/rest/[0-9]+/[A-Za-z0-9_-]{6,}/?", re.IGNORECASE)
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


@dataclass(frozen=True, slots=True)
class Redactor:
    """Recursively scrub secrets and configured PII while bounding output."""

    sensitive_keys: frozenset[str] = DEFAULT_SENSITIVE_KEYS
    secret_paths: tuple[tuple[PathPart, ...], ...] = ()
    pii_fields: frozenset[str] = field(default_factory=frozenset)
    max_depth: int = 12
    max_items: int = 100
    max_string: int = 500

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        if self.max_depth < 1:
            raise ValueError("max_depth must be positive")
        if self.max_items < 1:
            raise ValueError("max_items must be positive")
        if self.max_string < len(TRUNCATED) + 1:
            raise ValueError("max_string is too small")

    def redact_text(self, value: str) -> str:
        """Scrub credential-bearing textual forms and apply a hard length bound."""
        scrubbed = _WEBHOOK_RE.sub(REDACTED, value)
        scrubbed = _QUERY_SECRET_RE.sub(lambda match: match.group("prefix") + REDACTED, scrubbed)
        scrubbed = _BEARER_RE.sub(lambda match: match.group("prefix") + REDACTED, scrubbed)
        scrubbed = _JSON_SECRET_RE.sub(
            lambda match: match.group("prefix") + REDACTED + match.group("suffix"),
            scrubbed,
        )
        scrubbed = _ENV_SECRET_RE.sub(lambda match: match.group("prefix") + REDACTED, scrubbed)
        scrubbed = _COOKIE_RE.sub(lambda match: match.group("prefix") + REDACTED, scrubbed)
        scrubbed = _BARE_CREDENTIAL_RE.sub(_redact_bare_credential, scrubbed)
        if len(scrubbed) <= self.max_string:
            return scrubbed
        keep = self.max_string - len(TRUNCATED)
        return scrubbed[:keep] + TRUNCATED

    def redact(self, value: Any) -> Any:
        """Return a detached JSON-compatible redacted representation."""
        return self._redact(value, path=(), depth=0, active=set())

    def safe_preview(self, body: bytes | str | None, *, max_chars: int = 500) -> str | None:
        """Create a bounded redacted body preview without retaining the raw body."""
        if body is None:
            return None
        text = body.decode("utf-8", errors="replace") if isinstance(body, bytes) else body
        try:
            parsed = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            preview = self.redact_text(text)
        else:
            preview = json.dumps(self.redact(parsed), ensure_ascii=False, separators=(",", ":"), sort_keys=True)
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
        active: set[int],
    ) -> Any:
        if self._is_secret_path(path):
            return REDACTED
        if isinstance(value, str):
            return self.redact_text(value)
        if value is None or isinstance(value, bool | int | float):
            return value
        if depth >= self.max_depth:
            return TRUNCATED
        if isinstance(value, Mapping):
            return self._redact_mapping(value, path=path, depth=depth, active=active)
        if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray):
            return self._redact_sequence(value, path=path, depth=depth, active=active)
        return self.redact_text(str(value))

    def _redact_mapping(
        self,
        value: Mapping[object, object],
        *,
        path: tuple[PathPart, ...],
        depth: int,
        active: set[int],
    ) -> dict[str, Any] | str:
        identity = id(value)
        if identity in active:
            return TRUNCATED
        active.add(identity)
        try:
            redacted: dict[str, Any] = {}
            for index, (raw_key, item) in enumerate(value.items()):
                if index >= self.max_items:
                    redacted[TRUNCATED] = TRUNCATED
                    break
                key = self.redact_text(str(raw_key))
                child_path = (*path, key)
                if self._is_sensitive_key(key):
                    redacted[key] = REDACTED
                else:
                    redacted[key] = self._redact(item, path=child_path, depth=depth + 1, active=active)
            return redacted
        finally:
            active.remove(identity)

    def _redact_sequence(
        self,
        value: Sequence[object],
        *,
        path: tuple[PathPart, ...],
        depth: int,
        active: set[int],
    ) -> list[Any] | str:
        identity = id(value)
        if identity in active:
            return TRUNCATED
        active.add(identity)
        try:
            redacted = [
                self._redact(item, path=(*path, index), depth=depth + 1, active=active)
                for index, item in enumerate(value[: self.max_items])
            ]
            if len(value) > self.max_items:
                redacted.append(TRUNCATED)
            return redacted
        finally:
            active.remove(identity)

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
