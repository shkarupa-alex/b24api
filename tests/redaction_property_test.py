"""Redactor properties and the section 3.9 output matrix (A7, B16, C11).

The property half compares the new renderer with a frozen copy of the redactor as it was before A7:
outside the structured diagnostic path every scalar and every mapping value renders byte for byte
as before, the set of visible mapping keys is unchanged, and the only declared difference is that
keys the rules hide become distinct ``[REDACTED#k]`` labels instead of merging into one key.

The matrix half feeds credential shapes (random tokens with ``_``, mixed case and length, tokens
equal to a known V3 code or a ``UF_CRM_*`` name, webhook URLs, ``Authorization``, cookies, auth-flow
values and redirect locations) through every error channel b24api renders: ``str``, ``repr``,
``to_safe_dict``, mapping keys and values of the body preview, the CLI and a report violation. The
HTTPX INFO and hpack DEBUG channels belong to the logging-shield matrix.
"""

from __future__ import annotations
import asyncio
import functools
import io
import json
import re
import string
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from b24api import Bitrix24, ReplaySafety, Request, RouteKind, Settings, WireResponse, cli
from b24api.contracts import Violation, ViolationSeverity
from b24api.contracts.request import diagnostic_context
from b24api.contracts.v3_codes import KNOWN_V3_ERROR_CODES, METHOD_NOT_FOUND_ERROR_CODE, VALIDATION_ERROR_CODE
from b24api.errors import ApiResponseError
from b24api.redaction import REDACTED, Redactor
from b24api.testing import ScriptedExchange, ScriptedTransport
from b24api.transport.protocol import ProtocolCodec

_HIDDEN_LABEL = re.compile(r"\[REDACTED#[1-9][0-9]*\]")
_CORRECTNESS = 4

# ---- frozen pre-A7 redactor: the oracle of the "unchanged outside the structured path" property ----

_LEGACY_WEBHOOK_RE = re.compile(r"https?://[^\s/]+/rest/(?:api/)?[0-9]+/[A-Za-z0-9_-]{6,}/?", re.IGNORECASE)
_LEGACY_QUERY_SECRET_RE = re.compile(r"(?P<prefix>[?&](?:auth|access_token|refresh_token)=)[^\s&#]+", re.IGNORECASE)
_LEGACY_BEARER_RE = re.compile(r"(?P<prefix>\bBearer\s+)[A-Za-z0-9._~+/=-]+", re.IGNORECASE)
_LEGACY_JSON_SECRET_RE = re.compile(
    r"(?P<prefix>[\"'](?:auth|access_token|refresh_token|client_secret|application_token)[\"']\s*:\s*[\"'])"
    r"[^\"']*(?P<suffix>[\"'])",
    re.IGNORECASE,
)
_LEGACY_ENV_SECRET_RE = re.compile(
    r"(?P<prefix>\b(?:AUTH_ID|APPLICATION_TOKEN|ACCESS_TOKEN|REFRESH_TOKEN|CLIENT_SECRET)=)[^\s,;]+",
    re.IGNORECASE,
)
_LEGACY_COOKIE_RE = re.compile(r"(?P<prefix>\b(?:Cookie|Set-Cookie)\s*:\s*)[^\r\n]+", re.IGNORECASE)
_LEGACY_BARE_CREDENTIAL_RE = re.compile(r"\b[A-Za-z0-9_-]{16,}\b")
_LEGACY_SENSITIVE = frozenset(
    [
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
        "set_cookie",
        "token",
        "webhook",
        "webhook_url",
    ],
)


def _legacy_bare(match: re.Match[str]) -> str:
    candidate = match.group(0)
    if len(candidate) in {40, 64} and all(character in "0123456789abcdefABCDEF" for character in candidate):
        return candidate
    if any(character.isalpha() for character in candidate) and any(character.isdigit() for character in candidate):
        return "[REDACTED]"
    return candidate


@dataclass(frozen=True)
class _LegacyRedactor:
    """Verbatim behavior of ``b24api.redaction.Redactor`` at 7db6793 (default configuration)."""

    max_depth: int = 12
    max_items: int = 100
    max_string: int = 500

    def redact_text(self, value: str) -> str:
        scrubbed = _LEGACY_WEBHOOK_RE.sub("[REDACTED]", value)
        scrubbed = _LEGACY_QUERY_SECRET_RE.sub(lambda match: match.group("prefix") + "[REDACTED]", scrubbed)
        scrubbed = _LEGACY_BEARER_RE.sub(lambda match: match.group("prefix") + "[REDACTED]", scrubbed)
        scrubbed = _LEGACY_JSON_SECRET_RE.sub(
            lambda match: match.group("prefix") + "[REDACTED]" + match.group("suffix"),
            scrubbed,
        )
        scrubbed = _LEGACY_ENV_SECRET_RE.sub(lambda match: match.group("prefix") + "[REDACTED]", scrubbed)
        scrubbed = _LEGACY_COOKIE_RE.sub(lambda match: match.group("prefix") + "[REDACTED]", scrubbed)
        scrubbed = _LEGACY_BARE_CREDENTIAL_RE.sub(_legacy_bare, scrubbed)
        if len(scrubbed) <= self.max_string:
            return scrubbed
        return scrubbed[: self.max_string - len("[TRUNCATED]")] + "[TRUNCATED]"

    def redact(self, value: Any, depth: int = 0) -> Any:  # noqa: ANN401 - frozen copy
        if isinstance(value, str):
            return self.redact_text(value)
        if value is None or isinstance(value, bool | int | float):
            return value
        if depth >= self.max_depth:
            return "[TRUNCATED]"
        if isinstance(value, Mapping):
            redacted: dict[str, Any] = {}
            for index, (raw_key, item) in enumerate(value.items()):
                if index >= self.max_items:
                    redacted["[TRUNCATED]"] = "[TRUNCATED]"
                    break
                key = self.redact_text(str(raw_key))
                sensitive = key.casefold().replace("-", "_") in _LEGACY_SENSITIVE
                redacted[key] = "[REDACTED]" if sensitive else self.redact(item, depth + 1)
            return redacted
        if isinstance(value, Sequence):
            items = [self.redact(item, depth + 1) for item in value[: self.max_items]]
            return [*items, "[TRUNCATED]"] if len(value) > self.max_items else items
        return self.redact_text(str(value))


LEGACY = _LegacyRedactor()

# ---- strategies ----

_TOKEN_ALPHABET = string.ascii_letters + string.digits + "_"
tokens = st.text(_TOKEN_ALPHABET, min_size=16, max_size=64).filter(
    lambda token: any(character.isalpha() for character in token) and any(character.isdigit() for character in token),
)
credential_text = st.one_of(
    st.text(max_size=80),
    tokens,
    tokens.map(lambda token: f"https://portal.invalid/rest/1/{token}/crm.deal.list"),
    tokens.map(lambda token: f"Authorization: Bearer {token}"),
    tokens.map(lambda token: f"Cookie: PHPSESSID={token}"),
    tokens.map(lambda token: f'{{"access_token": "{token}"}}'),
    tokens.map(lambda token: f"AUTH_ID={token} and more"),
    st.sampled_from(sorted(KNOWN_V3_ERROR_CODES)),
    st.integers(min_value=0, max_value=10**15).map(lambda number: f"UF_CRM_{number}"),
)
json_scalars = st.one_of(st.none(), st.booleans(), st.integers(), credential_text)
json_values = st.recursive(
    json_scalars,
    lambda children: st.one_of(
        st.lists(children, max_size=4),
        st.dictionaries(credential_text, children, max_size=4),
    ),
    max_leaves=12,
)


def _assert_matches_legacy(new: Any, raw: Any, depth: int = 0) -> None:  # noqa: ANN401
    """Values equal the frozen oracle; visible keys are the oracle's; hidden keys are distinct labels."""
    if not isinstance(raw, Mapping) or depth >= LEGACY.max_depth:
        if isinstance(raw, list) and depth < LEGACY.max_depth:
            assert isinstance(new, list)
            assert len(new) == len(raw)
            for new_item, raw_item in zip(new, raw, strict=True):
                _assert_matches_legacy(new_item, raw_item, depth + 1)
            return
        assert new == LEGACY.redact(raw, depth)
        return
    assert isinstance(new, dict)
    hidden = [raw_key for raw_key in raw if LEGACY.redact_text(raw_key) != raw_key]
    visible = {raw_key for raw_key in raw if LEGACY.redact_text(raw_key) == raw_key}
    assert {key for key in new if not _HIDDEN_LABEL.fullmatch(key)} == visible
    assert len([key for key in new if _HIDDEN_LABEL.fullmatch(key)]) == len(hidden)
    labels = iter(key for key in new if _HIDDEN_LABEL.fullmatch(key))
    for raw_key, raw_item in raw.items():
        shown = raw_key if raw_key in visible else next(labels)
        legacy_key = LEGACY.redact_text(raw_key)
        if legacy_key.casefold().replace("-", "_") in _LEGACY_SENSITIVE:
            assert new[shown] == REDACTED
        else:
            _assert_matches_legacy(new[shown], raw_item, depth + 1)


@given(credential_text)
def test_free_text_renders_exactly_as_the_legacy_redactor(text: str) -> None:
    assert Redactor().redact_text(text) == LEGACY.redact_text(text)
    assert Redactor(max_string=40).redact_text(text) == _LegacyRedactor(max_string=40).redact_text(text)


@given(json_values)
def test_scalars_and_mapping_values_outside_the_structured_path_are_unchanged(value: Any) -> None:  # noqa: ANN401
    _assert_matches_legacy(Redactor().redact(value), value)


@given(tokens, tokens)
def test_two_different_hidden_mapping_keys_never_merge(first: str, second: str) -> None:
    if first == second:
        return
    redacted = Redactor().redact({first: "a", second: "b", "visible": "c"})

    assert redacted == {"[REDACTED#1]": "a", "[REDACTED#2]": "b", "visible": "c"}
    assert set(LEGACY.redact({first: "a", second: "b", "visible": "c"})) - {"[REDACTED]"} == {"visible"}


# ---- matrix ----


def _error_channels(error: ApiResponseError) -> str:
    report = Violation(ViolationSeverity.BLOCKING, "api_failure", "call failed", error=error).to_safe_dict()
    return "\n".join(
        (str(error), repr(error), json.dumps(error.to_safe_dict(), ensure_ascii=False), json.dumps(report)),
    )


def _render_everywhere(text: str, *, codec: ProtocolCodec, request: Request | None = None) -> ApiResponseError:
    body = {
        "error": {
            "code": text,
            "message": f"failed: {text}",
            "validation": [{"field": f"filter.{text}", "message": text}],
        },
        "echo": {text: text, "nested": [text]},
    }
    error = codec.error_from_http(
        status_code=400,
        body=body,
        diagnostics=diagnostic_context(request) if request is not None else None,
    )
    assert isinstance(error, ApiResponseError)
    return error


@functools.cache
def _vocabulary() -> str:
    """Return every fixed word the error channels print around a value, visible or hidden."""
    renders = (_render_everywhere(text, codec=ProtocolCodec()) for text in ("neutral", "n3utr4l_h1dd3n_v4lu3"))
    return "\n".join(_error_channels(error) for error in renders)


# Registered secrets of any shape, including digit-free ones the heuristic keeps; a value that is part of
# the fixed rendering vocabulary (for example ``rest_module``) cannot be told apart from that vocabulary.
any_tokens = st.text(_TOKEN_ALPHABET, min_size=6, max_size=64).filter(lambda token: token not in _vocabulary())


@settings(max_examples=60, suppress_health_check=[HealthCheck.too_slow])
@given(any_tokens)
def test_registered_secret_of_any_shape_is_absent_from_every_channel(token: str) -> None:
    codec = ProtocolCodec(redactor=Redactor(known_secrets=frozenset({token})))
    request = Request("x.list", {"access_token": token, "select": ["ID"]}, route=RouteKind.BARE)

    for error in (
        _render_everywhere(token, codec=codec),
        _render_everywhere(token, codec=ProtocolCodec(), request=request),
    ):
        assert token not in _error_channels(error)


@settings(max_examples=60, suppress_health_check=[HealthCheck.too_slow])
@given(tokens)
def test_unregistered_heuristic_tokens_stay_hidden_as_before(token: str) -> None:
    assert token not in _error_channels(_render_everywhere(token, codec=ProtocolCodec()))


@pytest.mark.parametrize("code", sorted(KNOWN_V3_ERROR_CODES))
def test_token_equal_to_a_known_code_is_redacted_once_registered(code: str) -> None:
    visible = _render_everywhere(code, codec=ProtocolCodec())
    registered = _render_everywhere(code, codec=ProtocolCodec(redactor=Redactor(known_secrets=frozenset({code}))))

    assert visible.wire_code == code
    assert code.casefold() not in _error_channels(registered).casefold()


@pytest.mark.parametrize(
    "template",
    [
        "https://portal.invalid/rest/1/{token}/crm.deal.list",
        "https://portal.invalid/rest/api/1/{token}/tasks.task.get",
        "Authorization: Bearer {token}",
        "Cookie: PHPSESSID={token}",
        "Set-Cookie: BITRIX_SM_UIDH={token}; path=/",
        "https://portal.invalid/rest/crm.deal.list?auth={token}&refresh_token={token}",
        '{{"access_token": "{token}", "refresh_token": "{token}"}}',
        "APPLICATION_TOKEN={token}",
        "Location: https://other.invalid/rest/7/{token}/profile",
    ],
)
@pytest.mark.parametrize("token", ["Ab_cdEfgh_ijKLmn_op", "zz_zzzzzzzzzzzzzzzzzzzzzz_zz", "q1_W2e3R4t5Y6u7I8o9"])
def test_credential_forms_are_absent_from_every_channel(template: str, token: str) -> None:
    text = template.format(token=token)
    request = Request("crm.deal.list", {"filter": {"ID": 1}}, route=RouteKind.BARE)

    assert token not in _error_channels(_render_everywhere(text, codec=ProtocolCodec()))
    assert token not in _error_channels(_render_everywhere(text, codec=ProtocolCodec(), request=request))


@pytest.mark.parametrize("name", ["UF_CRM_1700000000123", "ufCrm5_1700000000123", "zz_zzzzzzzzzzzzzzzz", "TITLE"])
def test_field_name_of_any_shape_renders_as_its_alias_in_every_channel(name: str) -> None:
    request = Request("crm.item.list", {"select": ["ID", name]}, route=RouteKind.BARE)
    error = _render_everywhere(name, codec=ProtocolCodec(), request=request)
    rendered = _error_channels(error)

    assert error.validation[0].field == "filter.field#2"
    assert "field#2" in rendered
    assert re.search(rf"(?<![A-Za-z0-9_-]){re.escape(name)}(?![A-Za-z0-9_-])", rendered) is None


def test_settings_token_equal_to_a_known_code_is_redacted_through_the_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    settings_value = Settings(webhook_url=f"https://fixture.invalid/rest/1/{METHOD_NOT_FOUND_ERROR_CODE}/")
    request = Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.API_V3)
    body = {"error": {"code": METHOD_NOT_FOUND_ERROR_CODE, "message": "missing", "validation": []}}
    response = WireResponse(404, (("content-type", "application/json"),), json.dumps(body).encode())
    transport = ScriptedTransport((ScriptedExchange(request, response),))
    monkeypatch.setattr(cli, "Bitrix24", lambda: Bitrix24(settings_value, transport=transport))
    stderr = io.StringIO()

    code = cli.main(
        ["call", "profile", "--route", "api_v3", "--replay-safety", "safe"],
        stdout=io.StringIO(),
        stderr=stderr,
    )

    assert code == _CORRECTNESS
    assert METHOD_NOT_FOUND_ERROR_CODE.casefold() not in stderr.getvalue().casefold()


def test_known_code_stays_diagnosable_through_client_and_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    settings_value = Settings(webhook_url="https://fixture.invalid/" + "rest/1/" + "s3cr3t_t0k3n_value/")
    request = Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.API_V3)
    body = {"error": {"code": VALIDATION_ERROR_CODE, "message": "bad", "validation": []}}
    response = WireResponse(400, (("content-type", "application/json"),), json.dumps(body).encode())

    async def run() -> ApiResponseError:
        async with Bitrix24(settings_value, transport=ScriptedTransport((ScriptedExchange(request, response),))) as c:
            with pytest.raises(ApiResponseError) as caught:
                await c.call(request)
        return caught.value

    assert VALIDATION_ERROR_CODE in _error_channels(asyncio.run(run()))
    transport = ScriptedTransport((ScriptedExchange(request, response),))
    monkeypatch.setattr(cli, "Bitrix24", lambda: Bitrix24(settings_value, transport=transport))
    stderr = io.StringIO()
    assert cli.main(["call", "profile", "--route", "api_v3", "--replay-safety", "safe"], stderr=stderr) == _CORRECTNESS
    assert VALIDATION_ERROR_CODE in stderr.getvalue()
