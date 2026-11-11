"""#19 (A19): a REST v3 validation item without ``field`` stays an API refusal with ``field=None``."""

from __future__ import annotations
import json

import pytest

from b24api import ApiResponseError, Bitrix24, ProtocolError, ReplaySafety, Request, RouteKind, Settings
from b24api.errors import ValidationIssue
from b24api.testing.scripted import ScriptedExchange, ScriptedTransport
from b24api.transport.base import WireResponse
from b24api.transport.protocol import ProtocolCodec

V3_VALIDATION = "BITRIX_REST_V3_EXCEPTION_VALIDATION_REQUESTVALIDATIONEXCEPTION"
HTTP_BAD_REQUEST = 400
MAX_VALIDATION_TEXT = 256
MAX_V3_SAFE_BYTES = 8 * 1024


def _decode(issue: object, *, retry_codes: tuple[str, ...] = ()) -> object:
    return ProtocolCodec().error_from_http(
        status_code=HTTP_BAD_REQUEST,
        body={"error": {"code": V3_VALIDATION, "message": "Validation failed", "validation": [issue]}},
        retry_codes=retry_codes,
    )


def test_a19_fieldless_item_via_codec_is_api_error_with_field_none() -> None:
    error = _decode({"message": "The task responsible user does not exist."})

    assert isinstance(error, ApiResponseError)
    assert error.normalized_code == V3_VALIDATION
    assert error.validation == (ValidationIssue(None, "The task responsible user does not exist."),)
    assert error.retryable is False
    assert error.truncated is False


def test_a19_fieldless_item_safe_form_serializes_field_as_null() -> None:
    error = _decode({"message": "redacted"})

    assert isinstance(error, ApiResponseError)
    safe = error.to_safe_dict()
    assert safe["validation"] == [{"field": None, "message": "redacted"}]
    assert '"validation": [{"field": null, "message": "redacted"}]' in json.dumps(safe)


def test_a19_fieldless_and_named_items_keep_their_order_and_fields() -> None:
    error = ProtocolCodec().error_from_http(
        status_code=HTTP_BAD_REQUEST,
        body={
            "error": {
                "code": V3_VALIDATION,
                "message": "Validation failed",
                "validation": [{"field": "fields.title", "message": "Required"}, {"message": "No such user"}],
            },
        },
    )

    assert isinstance(error, ApiResponseError)
    assert [issue.field for issue in error.validation] == ["fields.title", None]


@pytest.mark.parametrize(
    "field",
    [None, 7, 1.5, True, {"name": "title"}, ["title"]],
    ids=["null", "int", "float", "bool", "mapping", "sequence"],
)
def test_a19_present_non_string_field_is_malformed_protocol_error(field: object) -> None:
    error = _decode({"field": field, "message": "bad"})

    assert isinstance(error, ProtocolError)
    assert not isinstance(error, ApiResponseError)
    assert str(error) == "Malformed V3 error object"


@pytest.mark.parametrize("issue", [{}, {"field": "fields.title"}, {"message": None}, {"message": 3}])
def test_a19_message_stays_required_as_a_string(issue: dict[str, object]) -> None:
    assert isinstance(_decode(issue), ProtocolError)


def test_a19_fieldless_message_keeps_redaction_limits() -> None:
    error = _decode({"message": "é" * 1_000})

    assert isinstance(error, ApiResponseError)
    assert error.validation[0].field is None
    assert len(error.validation[0].message) == MAX_VALIDATION_TEXT
    assert error.truncated is True
    assert len(json.dumps(error.to_safe_dict(), ensure_ascii=False).encode("utf-8")) <= MAX_V3_SAFE_BYTES


def test_a19_fieldless_retryability_follows_the_code_only() -> None:
    # The V3 validation code stays non-retryable even when listed; field absence does not change that.
    listed = _decode({"message": "bad"}, retry_codes=(V3_VALIDATION,))
    assert isinstance(listed, ApiResponseError)
    assert listed.retryable is False
    transient = ProtocolCodec().error_from_http(
        status_code=HTTP_BAD_REQUEST,
        body={"error": {"code": "SOME_TRANSIENT", "message": "later", "validation": [{"message": "busy"}]}},
        retry_codes=("SOME_TRANSIENT",),
    )
    assert isinstance(transient, ApiResponseError)
    assert transient.retryable is True


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [200, HTTP_BAD_REQUEST], ids=["http200", "http400"])
async def test_a19_fieldless_item_via_unsafe_direct_call_sends_once(status_code: int) -> None:
    request = Request(
        "tasks.task.add",
        {"fields": {"title": "synthetic validation", "creatorId": 7, "responsibleId": 9}},
        ReplaySafety.UNSAFE,
        route=RouteKind.API_V3,
    )
    payload = {
        "error": {
            "code": V3_VALIDATION,
            "message": "Validation failed.",
            "validation": [{"message": "The task responsible user does not exist."}],
        },
    }
    body = json.dumps(payload).encode("utf-8")
    transport = ScriptedTransport(
        (ScriptedExchange(request, WireResponse(status_code, (("content-type", "application/json"),), body)),),
    )

    async with Bitrix24(Settings(webhook_url="https://fixture.invalid/rest/1/test/"), transport=transport) as api:
        with pytest.raises(ApiResponseError) as raised:
            await api.call(request)

    transport.assert_exhausted()
    assert len(transport.calls) == 1
    assert raised.value.normalized_code == V3_VALIDATION
    assert raised.value.validation == (ValidationIssue(None, "The task responsible user does not exist."),)


@pytest.mark.asyncio
async def test_a19_present_null_field_via_unsafe_direct_call_is_protocol_error() -> None:
    request = Request("tasks.task.add", {"fields": {"title": "x"}}, ReplaySafety.UNSAFE, route=RouteKind.API_V3)
    payload = {
        "error": {
            "code": V3_VALIDATION,
            "message": "Validation failed.",
            "validation": [{"field": None, "message": "x"}],
        }
    }
    transport = ScriptedTransport((ScriptedExchange.json(request, payload),))

    async with Bitrix24(Settings(webhook_url="https://fixture.invalid/rest/1/test/"), transport=transport) as api:
        with pytest.raises(ProtocolError) as raised:
            await api.call(request)

    transport.assert_exhausted()
    assert len(transport.calls) == 1
    assert not isinstance(raised.value, ApiResponseError)
