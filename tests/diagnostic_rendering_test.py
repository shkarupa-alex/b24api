"""Contextual safe rendering of V3 codes, request field names and mapping keys (A7, C11, D21, D35).

C11 decision: the bare-credential heuristic stays as the free-text safeguard. Removing it requires
every credential source to be registered as an exact secret before any output; the matrix below
shows that one source cannot be, so the counterexample is recorded here and pinned by
``test_c11_foreign_response_token_is_covered_only_by_the_heuristic``.

Coverage matrix (source -> registered as an exact secret before any output? -> other free-text coverage):

- Webhook from ``Settings`` (``Bitrix24(settings)``) -> yes, ``Bitrix24`` builds its codec redactor with
  ``webhook_secrets(settings.webhook_url)`` -> full-URL pattern, heuristic.
- ``Bitrix24.from_webhook`` -> yes, it validates the URL through ``Settings`` and builds the client
  from them, so it inherits the row above -> as above.
- URL of an injected transport -> only as the ``Settings`` webhook it must match by host; the
  ``Transport`` protocol never exposes a token, so a different token inside it is unknown -> full-URL
  pattern, heuristic.
- Auth-flow values (``auth``/``access_token``/... request parameters, sensitive scoped headers) -> yes,
  the request's ``DiagnosticContext`` registers them for that request's errors -> sensitive keys,
  JSON/query/env patterns, heuristic.
- Redirect hop tokens -> no, the portal builds the hop URL and it reaches only HTTPX records ->
  ``logging_shield`` hop pattern, full-URL pattern, heuristic.
- Foreign tokens in response data (another user's webhook, an app token inside an error text) -> no,
  unknowable before the response arrives -> heuristic only.

The redirect and foreign-token rows are the counterexample: their only coverage in free text is the
heuristic, so it is not removed. Registration is still added because the heuristic alone misses a
digit-free token in a scheme-less path (``test_configured_webhook_token_is_an_exact_secret``).
"""

from __future__ import annotations
import asyncio
import io
import json
import pickle

import pytest

from b24api import Bitrix24, ReplaySafety, Request, RouteKind, Settings, cli
from b24api.contracts import Command, CommandFailure, Violation, ViolationSeverity
from b24api.contracts.request import diagnostic_context
from b24api.contracts.v3_codes import (
    KNOWN_V3_ERROR_CODES,
    METHOD_NOT_FOUND_ERROR_CODE,
    VALIDATION_ERROR_CODE,
    render_code,
)
from b24api.errors import ApiResponseError
from b24api.redaction import REDACTED, Redactor
from b24api.testing import ScriptedExchange, ScriptedTransport
from b24api.transport import WireResponse
from b24api.transport.protocol import ProtocolCodec

# Digit-free, so the bare-credential heuristic alone leaves it visible.
UNREGISTERED_CREDENTIAL = "zxcvbnmlkjhgfdsa"
USER_FIELD = "UF_CRM_1700000000123"
OTHER_USER_FIELD = "UF_CRM_1700000000456"
WEBHOOK_CREDENTIAL = "qwertyuiopasdfgh"
AUTH_VALUE = "plmoknijbuhvygct"
SETTINGS = Settings(webhook_url=f"https://fixture.invalid/rest/1/{WEBHOOK_CREDENTIAL}/")
JSON_HEADERS = (("content-type", "application/json"),)
_CORRECTNESS = 4


def _channels(error: ApiResponseError) -> str:
    return "\n".join((str(error), repr(error), json.dumps(error.to_safe_dict(), ensure_ascii=False)))


def _v3_body(code: str, message: str, *fields: str) -> dict[str, object]:
    validation = [{"field": f"filter.{name}", "message": f"{name} is invalid"} for name in fields]
    return {"error": {"code": code, "message": message, "validation": validation}}


def _wire(status: int, payload: object) -> WireResponse:
    return WireResponse(status, JSON_HEADERS, json.dumps(payload).encode())


def test_known_v3_code_is_diagnosable_in_str_repr_and_safe_dict() -> None:
    error = ProtocolCodec().error_from_http(
        status_code=404,
        body=_v3_body(METHOD_NOT_FOUND_ERROR_CODE, "Method not found"),
    )

    assert isinstance(error, ApiResponseError)
    assert str(error) == f"API error [{METHOD_NOT_FOUND_ERROR_CODE}]: Method not found"
    assert METHOD_NOT_FOUND_ERROR_CODE in repr(error)
    safe = error.to_safe_dict()
    assert safe["wire_code"] == safe["original_code"] == safe["normalized_code"] == METHOD_NOT_FOUND_ERROR_CODE
    assert safe["code"] == METHOD_NOT_FOUND_ERROR_CODE.lower()
    assert METHOD_NOT_FOUND_ERROR_CODE in str(safe["message"])
    assert error.retryable is False


def test_every_known_code_renders_verbatim_and_its_neighbours_do_not() -> None:
    for code in KNOWN_V3_ERROR_CODES:
        assert render_code(code) == code
        assert render_code(code.lower()) == code.lower()
    assert VALIDATION_ERROR_CODE in KNOWN_V3_ERROR_CODES


@pytest.mark.parametrize(
    "code",
    [
        "BITRIX_REST_V3_EXCEPTION_SOMENEWEXCEPTION",
        f"{METHOD_NOT_FOUND_ERROR_CODE}X",
        f"X{METHOD_NOT_FOUND_ERROR_CODE}",
        USER_FIELD,
        "ufCrm5_1700000000123",
        "123e4567-e89b-12d3-a456-426614174000",
    ],
)
def test_other_identifier_shapes_keep_free_text_redaction(code: str) -> None:
    error = ProtocolCodec().error_from_http(status_code=400, body=_v3_body(code, "bad"))

    assert isinstance(error, ApiResponseError)
    assert render_code(code) == REDACTED
    assert code not in _channels(error)
    assert code.lower() not in _channels(error)


def test_exact_secret_beats_known_code_recognition() -> None:
    codec = ProtocolCodec(redactor=Redactor(known_secrets=frozenset({METHOD_NOT_FOUND_ERROR_CODE})))
    error = codec.error_from_http(status_code=404, body=_v3_body(METHOD_NOT_FOUND_ERROR_CODE, "gone"))
    request = Request("profile", {"auth": VALIDATION_ERROR_CODE}, route=RouteKind.BARE)
    from_request = ProtocolCodec().error_from_http(
        status_code=400,
        body=_v3_body(VALIDATION_ERROR_CODE, "bad"),
        diagnostics=diagnostic_context(request),
    )

    assert isinstance(error, ApiResponseError)
    assert isinstance(from_request, ApiResponseError)
    assert METHOD_NOT_FOUND_ERROR_CODE not in _channels(error).upper()
    assert VALIDATION_ERROR_CODE not in _channels(from_request).upper()
    assert error.wire_code == from_request.wire_code == REDACTED


def test_hidden_mapping_keys_stay_distinct_and_keep_their_values() -> None:
    value = {USER_FIELD: "a", OTHER_USER_FIELD: "b", "visible": {USER_FIELD: 1}, "token": "x"}

    assert Redactor().redact(value) == {
        "[REDACTED#1]": "a",
        "[REDACTED#2]": "b",
        "visible": {"[REDACTED#1]": 1},
        "token": REDACTED,
    }
    error = ProtocolCodec().error_from_http(
        status_code=400,
        body={"error": "INVALID", "fields": {USER_FIELD: "a", OTHER_USER_FIELD: "b"}},
    )
    assert isinstance(error, ApiResponseError)
    assert error.evidence.body_preview is not None
    assert '"[REDACTED#1]":"a","[REDACTED#2]":"b"' in error.evidence.body_preview


def test_hidden_labels_never_collide_with_a_visible_key() -> None:
    redacted = Redactor().redact({"[REDACTED#1]": "literal", USER_FIELD: "hidden"})

    assert redacted == {"[REDACTED#1]": "literal", "[REDACTED#2]": "hidden"}


def test_request_field_names_render_as_distinct_aliases_in_every_channel(monkeypatch: pytest.MonkeyPatch) -> None:
    request = Request(
        "tasks.task.list",
        {"filter": {f">={UNREGISTERED_CREDENTIAL}": 1, USER_FIELD: 2}, "select": ["*", USER_FIELD]},
        ReplaySafety.SAFE,
        route=RouteKind.API_V3,
    )
    body = _v3_body(
        VALIDATION_ERROR_CODE, f"{UNREGISTERED_CREDENTIAL} conflicts with {USER_FIELD}", *(UNREGISTERED_CREDENTIAL,)
    )
    body["echo"] = {UNREGISTERED_CREDENTIAL: USER_FIELD, USER_FIELD: UNREGISTERED_CREDENTIAL}
    transport = ScriptedTransport((ScriptedExchange(request, _wire(400, body)),))

    async def run() -> ApiResponseError:
        async with Bitrix24(SETTINGS, transport=transport) as client:
            with pytest.raises(ApiResponseError) as caught:
                await client.call(request)
        return caught.value

    error = asyncio.run(run())
    report = Violation(ViolationSeverity.BLOCKING, "api_failure", "call failed", error=error).to_safe_dict()
    rendered = _channels(error) + json.dumps(report)

    assert str(error) == f"API error [{VALIDATION_ERROR_CODE}]: field#1 conflicts with field#2"
    assert error.validation[0].field == "filter.field#1"
    assert error.evidence.body_preview is not None
    assert '"echo":{"field#1":"field#2","field#2":"field#1"}' in error.evidence.body_preview
    assert UNREGISTERED_CREDENTIAL not in rendered
    assert USER_FIELD not in rendered
    assert not any(value is not None and "DiagnosticContext" in type(value).__name__ for value in vars(error).values())

    cli_transport = ScriptedTransport((ScriptedExchange(request, _wire(400, body)),))
    monkeypatch.setattr(cli, "Bitrix24", lambda: Bitrix24(SETTINGS, transport=cli_transport))
    stderr = io.StringIO()
    code = cli.main(
        [
            "call",
            request.method,
            "--route",
            "api_v3",
            "--replay-safety",
            "safe",
            "--params",
            json.dumps(request.copy_parameters()),
        ],
        stdout=io.StringIO(),
        stderr=stderr,
    )
    assert code == _CORRECTNESS
    assert "field#1 conflicts with field#2" in stderr.getvalue()
    assert VALIDATION_ERROR_CODE in stderr.getvalue()
    assert UNREGISTERED_CREDENTIAL not in stderr.getvalue()
    assert USER_FIELD not in stderr.getvalue()


def test_physical_batch_renders_each_command_through_its_own_request() -> None:
    first = Request("crm.item.list", {"filter": {"TITLE": 1, USER_FIELD: 2}}, ReplaySafety.SAFE, route=RouteKind.BARE)
    second = Request("crm.item.list", {"select": [USER_FIELD]}, ReplaySafety.SAFE, route=RouteKind.BARE)
    template = ScriptedExchange.batch((first, second), ({}, {}), total=0)
    keys = ("c000000000000", "c000000000001")
    envelope = {
        "result": {
            "result": {},
            "result_error": {
                key: {"error": "ERROR_FIELD", "error_description": f"{USER_FIELD} denied"} for key in keys
            },
            "result_total": {},
            "result_next": {},
        },
    }
    transport = ScriptedTransport((ScriptedExchange(template.request, _wire(200, envelope)),))

    async def run() -> list[str]:
        async with Bitrix24(SETTINGS, transport=transport) as client:
            stream = client.batch_outcomes([Command(first, 1), Command(second, 2)])
            async with stream:
                outcomes = [item async for item in stream]
        return [str(item.error) for item in outcomes if isinstance(item, CommandFailure)]

    assert asyncio.run(run()) == [
        "API error [ERROR_FIELD] (normalized: error_field): field#2 denied",
        "API error [ERROR_FIELD] (normalized: error_field): field#1 denied",
    ]


def test_configured_webhook_token_is_an_exact_secret() -> None:
    request = Request("profile", route=RouteKind.BARE)
    body = {"error": "NOT_FOUND", "error_description": f"No route /rest/1/{WEBHOOK_CREDENTIAL}/profile.json"}
    transport = ScriptedTransport((ScriptedExchange(request, _wire(404, body)),))

    async def run() -> ApiResponseError:
        async with Bitrix24(SETTINGS, transport=transport) as client:
            with pytest.raises(ApiResponseError) as caught:
                await client.call(request)
        return caught.value

    # The heuristic alone keeps a digit-free token in a scheme-less path; registration closes it.
    assert WEBHOOK_CREDENTIAL in Redactor().redact_text(str(body["error_description"]))
    assert WEBHOOK_CREDENTIAL not in _channels(asyncio.run(run()))


def test_short_configured_webhook_token_is_still_an_exact_secret() -> None:
    short = "abcde"
    request = Request("profile", route=RouteKind.BARE)
    body = {"error": "NOT_FOUND", "error_description": f"token {short} is not valid for abcdef"}
    transport = ScriptedTransport((ScriptedExchange(request, _wire(404, body)),))

    settings = Settings(webhook_url=f"https://fixture.invalid/rest/1/{short}/")

    async def run() -> ApiResponseError:
        async with Bitrix24(settings, transport=transport) as client:
            with pytest.raises(ApiResponseError) as caught:
                await client.call(request)
        return caught.value

    error = asyncio.run(run())
    # A short secret is hidden wherever it stands as a whole token; a longer word that contains it stays readable.
    assert error.description == f"token {REDACTED} is not valid for abcdef"
    assert f" {short} " not in _channels(error)


def test_short_request_auth_value_is_hidden_as_a_whole_token() -> None:
    request = Request("crm.item.list", {"auth": "k9x", "select": ["TITLE"]}, route=RouteKind.BARE)
    error = ProtocolCodec().error_from_http(
        status_code=401,
        body={"error": "expired_token", "error_description": "token k9x expired; k9xy is another value"},
        diagnostics=diagnostic_context(request),
    )

    assert isinstance(error, ApiResponseError)
    assert error.description == f"token {REDACTED} expired; k9xy is another value"


def test_request_auth_values_are_exact_secrets_that_are_never_aliased() -> None:
    request = Request("crm.item.list", {"auth": AUTH_VALUE, "select": [AUTH_VALUE, "TITLE"]}, route=RouteKind.BARE)
    error = ProtocolCodec().error_from_http(
        status_code=401,
        body={"error": "expired_token", "error_description": f"token {AUTH_VALUE} expired for TITLE"},
        diagnostics=diagnostic_context(request),
    )

    assert isinstance(error, ApiResponseError)
    assert error.description == f"token {REDACTED} expired for field#1"
    assert AUTH_VALUE not in _channels(error)


def test_v3_list_filters_and_operator_prefixes_share_one_alias_map() -> None:
    context = diagnostic_context(
        Request(
            "tasks.task.list",
            {"order": {"ID": "desc"}, "filter": [["TITLE", "=", "x"], ["ID", ">", 1]], "select": ["*", "NAME"]},
            route=RouteKind.API_V3,
        ),
    )
    classic = diagnostic_context(Request("crm.item.list", {"filter": {"!=STATUS": 1, ">=ID": 2}}, route=RouteKind.BARE))

    assert context.alias_text("ID TITLE NAME IDX *") == "field#1 field#2 field#3 IDX *"
    assert classic.alias_text("STATUS !=STATUS >=ID") == "field#1 !=field#1 >=field#2"


def test_diagnostic_context_never_serializes_raw_names() -> None:
    context = diagnostic_context(Request("x.list", {"select": [UNREGISTERED_CREDENTIAL]}, route=RouteKind.BARE))

    assert repr(context) == "DiagnosticContext(fields=1)"
    with pytest.raises(TypeError, match="cannot be serialized"):
        pickle.dumps(context)
    assert UNREGISTERED_CREDENTIAL not in repr(Redactor(known_secrets=frozenset({UNREGISTERED_CREDENTIAL})))


def test_c11_foreign_response_token_is_covered_only_by_the_heuristic() -> None:
    foreign = "a1b2c3d4e5f6g7h8"
    request = Request("profile", {"auth": AUTH_VALUE}, route=RouteKind.BARE)
    codec = ProtocolCodec(redactor=Redactor(known_secrets=frozenset({WEBHOOK_CREDENTIAL})))
    error = codec.error_from_http(
        status_code=400,
        body={"error": "INVALID", "error_description": f"Application token {foreign} was revoked"},
        diagnostics=diagnostic_context(request),
    )

    assert isinstance(error, ApiResponseError)
    # No registration can know this token in advance; the retained heuristic is what hides it.
    assert not codec.redactor.is_known_secret(foreign)
    assert not diagnostic_context(request).is_known_secret(foreign)
    assert foreign not in _channels(error)


def test_alias_words_as_field_names_are_aliased_exactly_once() -> None:
    request = Request("x.list", {"select": ["field", "REDACTED"]}, route=RouteKind.BARE)
    error = ProtocolCodec().error_from_http(
        status_code=400,
        body={"error": "BAD", "error_description": "field and REDACTED", "echo": {"field": 1, USER_FIELD: 2}},
        diagnostics=diagnostic_context(request),
    )

    assert isinstance(error, ApiResponseError)
    assert error.description == "field#1 and field#2"
    assert error.evidence.body_preview is not None
    assert '"echo":{"[REDACTED#1]":2,"field#1":1}' in error.evidence.body_preview
