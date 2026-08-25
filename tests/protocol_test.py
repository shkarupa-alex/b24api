"""Tests for structured error precedence and redacted exception evidence."""

from dataclasses import dataclass

from pytest_mock import MockerFixture

from b24api.contracts.request import summarize_request
from b24api.errors import (
    AmbiguousExecutionError,
    ApiResponseError,
    B24ApiError,
    BatchCommandError,
    BudgetExceededError,
    CapabilityError,
    HTTPGatewayError,
    PaginationError,
    ProtocolError,
    TransportError,
)
from b24api.protocol import ProtocolCodec

EXAMPLE_CREDENTIAL = "n1x2y3z4q5w6e7r8"
WEBHOOK = "https://portal.invalid/" + "rest/1/" + EXAMPLE_CREDENTIAL + "/"
HTTP_SERVICE_UNAVAILABLE = 503
HTTP_TOO_MANY_REQUESTS = 429


@dataclass
class _Request:
    method: str
    parameters: dict[str, object]


def test_structured_body_precedes_http_status_and_preserves_codes() -> None:
    error = ProtocolCodec().error_from_http(
        status_code=HTTP_SERVICE_UNAVAILABLE,
        body={"error": " Access_DENIED ", "error_description": "No access"},
        request_summary=summarize_request("profile"),
    )

    assert isinstance(error, ApiResponseError)
    assert not isinstance(error, HTTPGatewayError)
    assert error.original_code == " Access_DENIED "
    assert error.code == " access_denied "
    assert error.normalized_code == "access_denied"
    assert error.http_status == HTTP_SERVICE_UNAVAILABLE


def test_numeric_code_and_retry_classification() -> None:
    numeric = ProtocolCodec().error_from_http(
        status_code=200,
        body={"error": 0, "error_description": "Unknown"},
    )
    retryable = ProtocolCodec().error_from_http(
        status_code=HTTP_TOO_MANY_REQUESTS,
        body={"error": "OPERATION_TIME_LIMIT", "error_description": "Wait"},
        retry_codes={"operation_time_limit"},
    )

    assert isinstance(numeric, ApiResponseError)
    assert numeric.original_code == 0
    assert numeric.code == "0"
    assert isinstance(retryable, ApiResponseError)
    assert retryable.retryable is True


def test_gateway_and_protocol_evidence_are_bounded_and_redacted() -> None:
    gateway = ProtocolCodec().error_from_http(
        status_code=502,
        body=f"nginx failed for {WEBHOOK}",
        headers={
            "Authorization": f"Bearer {EXAMPLE_CREDENTIAL}",
            "Set-Cookie": f"session={EXAMPLE_CREDENTIAL}",
            "X-Request-ID": "request-1",
            "Content-Type": "text/html",
        },
    )
    malformed = ProtocolCodec().error_from_http(status_code=200, body="{not-json")

    assert isinstance(gateway, HTTPGatewayError)
    assert isinstance(malformed, ProtocolError)
    rendered = repr(gateway.to_safe_dict())
    assert EXAMPLE_CREDENTIAL not in rendered
    assert "/rest/1/" not in rendered
    assert gateway.evidence.request_id == "request-1"
    assert dict(gateway.evidence.headers) == {"content-type": "text/html", "x-request-id": "request-1"}


def test_empty_success_has_no_error() -> None:
    assert ProtocolCodec().error_from_http(status_code=200, body=b"") is None


def test_success_body_does_not_pay_for_recursive_error_preview(mocker: MockerFixture) -> None:
    codec = ProtocolCodec()
    preview = mocker.patch.object(codec, "_body_preview", side_effect=AssertionError("previewed success"))

    assert codec.error_from_http(status_code=200, body=b'{"result":[{"id":1}]}') is None
    preview.assert_not_called()


def test_structured_description_and_request_context_are_safe() -> None:
    request = _Request(method="profile", parameters={"auth": EXAMPLE_CREDENTIAL, "select": ["ID"]})
    error = ApiResponseError(
        code="ACCESS_DENIED",
        description=f"Rejected {WEBHOOK}",
        request=request,
        headers={"Authorization": f"Bearer {EXAMPLE_CREDENTIAL}", "x-request-id": "safe-id"},
    )

    rendered = str(error) + repr(error) + repr(error.to_safe_dict())
    assert error.request is not None
    assert error.request.method == "profile"
    assert error.request.parameter_keys == ("auth", "select")
    assert EXAMPLE_CREDENTIAL not in rendered
    assert "/rest/1/" not in rendered


def test_complete_v2_error_hierarchy() -> None:
    assert issubclass(TransportError, B24ApiError)
    assert issubclass(HTTPGatewayError, B24ApiError)
    assert issubclass(ProtocolError, B24ApiError)
    assert issubclass(ApiResponseError, B24ApiError)
    assert issubclass(BatchCommandError, ApiResponseError)
    assert issubclass(CapabilityError, B24ApiError)
    assert issubclass(PaginationError, B24ApiError)
    assert issubclass(BudgetExceededError, B24ApiError)
    assert issubclass(AmbiguousExecutionError, B24ApiError)
    assert TransportError("transport").origin.value == "transport"
    assert CapabilityError("capability").origin.value == "capability"
    assert PaginationError("pagination").origin.value == "pagination"
    assert BatchCommandError(code="failed", description=None).origin.value == "batch_command"
