"""Redacted Bitrix24 error hierarchy and compatibility aliases."""

from __future__ import annotations
from enum import StrEnum
from typing import TYPE_CHECKING, Any, ClassVar

import httpx

from b24api.models import RequestSummary, ResponseEvidence, summarize_request, summarize_request_like
from b24api.redaction import DEFAULT_REDACTOR, Redactor

if TYPE_CHECKING:
    from collections.abc import Mapping


class ErrorOrigin(StrEnum):
    """Layer that produced or detected an error."""

    REST_MODULE = "rest_module"
    BATCH_COMMAND = "batch_command"
    HTTP_GATEWAY = "http_gateway"
    TRANSPORT = "transport"
    PROTOCOL = "protocol"
    CAPABILITY = "capability"
    PAGINATION = "pagination"
    BUDGET = "budget"
    AMBIGUOUS_EXECUTION = "ambiguous_execution"


class FailurePhase(StrEnum):
    """Last transport lifecycle phase conclusively reached before failure."""

    NOT_DISPATCHED = "not_dispatched"
    CONNECTION_ESTABLISHED = "connection_established"
    DISPATCH_STARTED = "dispatch_started"
    HEADERS_RECEIVED = "headers_received"
    BODY_PARTIALLY_RECEIVED = "body_partially_received"
    RESPONSE_COMPLETE = "response_complete"


class B24ApiError(Exception):
    """Base error whose default text and serialization contain safe evidence only."""

    default_origin: ClassVar[ErrorOrigin | None] = None

    def __init__(  # noqa: PLR0913
        self,
        message: str,
        *,
        origin: ErrorOrigin | None = None,
        description: str | None = None,
        request_summary: RequestSummary | None = None,
        evidence: ResponseEvidence | None = None,
        retryable: bool = False,
        redactor: Redactor = DEFAULT_REDACTOR,
    ) -> None:
        """Initialize instance state."""
        resolved_origin = origin or self.default_origin
        if resolved_origin is None:
            raise TypeError("origin is required for B24ApiError")
        self.origin = resolved_origin
        self.description = redactor.redact_text(description) if description is not None else None
        self.request_summary = request_summary
        self.request = request_summary
        self.evidence = evidence or ResponseEvidence()
        self.retryable = retryable
        safe_message = redactor.redact_text(message)
        super().__init__(safe_message)

    @property
    def http_status(self) -> int | None:
        """Return the http status."""
        return self.evidence.http_status

    def to_safe_dict(self) -> dict[str, object]:
        """Serialize only bounded redacted fields."""
        return {
            "type": type(self).__name__,
            "origin": self.origin.value,
            "message": str(self),
            "description": self.description,
            "request": self.request_summary.to_dict() if self.request_summary else None,
            "evidence": self.evidence.to_dict(),
            "retryable": self.retryable,
        }

    def __repr__(self) -> str:
        """Return a safe representation."""
        return f"{type(self).__name__}({self.to_safe_dict()!r})"


class TransportError(B24ApiError):
    """Transport lifecycle failure."""

    default_origin = ErrorOrigin.TRANSPORT

    def __init__(  # noqa: PLR0913
        self,
        message: str,
        *,
        phase: FailurePhase = FailurePhase.DISPATCH_STARTED,
        request_summary: RequestSummary | None = None,
        evidence: ResponseEvidence | None = None,
        retryable: bool = True,
        redactor: Redactor = DEFAULT_REDACTOR,
    ) -> None:
        """Initialize instance state."""
        if not isinstance(phase, FailurePhase):
            raise TypeError("phase must be a FailurePhase")
        self.phase = phase
        super().__init__(
            message,
            request_summary=request_summary,
            evidence=evidence,
            retryable=retryable,
            redactor=redactor,
        )

    @property
    def possible_acceptance(self) -> bool:
        """Whether any request bytes may have reached the server."""
        return self.phase not in {FailurePhase.NOT_DISPATCHED, FailurePhase.CONNECTION_ESTABLISHED}

    def to_safe_dict(self) -> dict[str, object]:
        """Return the to safe dict representation."""
        safe = super().to_safe_dict()
        safe.update({"phase": self.phase.value, "possible_acceptance": self.possible_acceptance})
        return safe


class HTTPGatewayError(B24ApiError):
    """Non-structured HTTP or gateway failure."""

    default_origin = ErrorOrigin.HTTP_GATEWAY


class ProtocolError(B24ApiError):
    """Malformed or contradictory protocol envelope."""

    default_origin = ErrorOrigin.PROTOCOL


class ApiResponseError(B24ApiError):
    """Structured Bitrix REST error with committed `.code` semantics."""

    def __init__(  # noqa: PLR0913
        self,
        *,
        code: str | int,
        description: str | None,
        request: Any = None,  # noqa: ANN401
        request_summary: RequestSummary | None = None,
        http_status: int | None = None,
        headers: Mapping[str, str] | None = None,
        body_preview: str | None = None,
        origin: ErrorOrigin = ErrorOrigin.REST_MODULE,
        retryable: bool = False,
        redactor: Redactor = DEFAULT_REDACTOR,
    ) -> None:
        """Initialize instance state."""
        self.original_code = code
        self.code = str(code).lower()
        self.normalized_code = str(code).strip().casefold()
        summary = request_summary or summarize_request_like(request, redactor=redactor)
        safe_description = redactor.redact_text(description) if description is not None else None
        if self.code and safe_description:
            message = f"API error [{self.code}]: {safe_description}"
        elif self.code:
            message = f"API error [{self.code}]"
        else:
            message = f"API error: {safe_description}"
        redacted_headers = redactor.redact(dict(headers or {}))
        safe_headers = tuple(sorted((str(key), str(value)) for key, value in redacted_headers.items()))
        evidence = ResponseEvidence(
            http_status=http_status,
            request_id=dict(safe_headers).get("x-request-id"),
            headers=safe_headers,
            body_preview=redactor.redact_text(body_preview) if body_preview is not None else None,
        )
        super().__init__(
            message,
            origin=origin,
            description=safe_description,
            request_summary=summary,
            evidence=evidence,
            retryable=retryable,
            redactor=redactor,
        )

    def to_safe_dict(self) -> dict[str, object]:
        """Return the to safe dict representation."""
        safe = super().to_safe_dict()
        safe.update(
            {
                "original_code": (
                    self.original_code
                    if isinstance(self.original_code, int)
                    else DEFAULT_REDACTOR.redact_text(self.original_code)
                ),
                "code": DEFAULT_REDACTOR.redact_text(self.code),
                "normalized_code": DEFAULT_REDACTOR.redact_text(self.normalized_code),
            },
        )
        return safe


class RetryApiResponseError(ApiResponseError):
    """Structured API error classified as retryable by explicit policy."""

    def __init__(self, **kwargs: Any) -> None:  # noqa: ANN401
        """Initialize instance state."""
        kwargs["retryable"] = True
        super().__init__(**kwargs)


class BatchCommandError(ApiResponseError):
    """Structured error correlated to one batch command."""

    def __init__(self, **kwargs: Any) -> None:  # noqa: ANN401
        """Initialize instance state."""
        kwargs.setdefault("origin", ErrorOrigin.BATCH_COMMAND)
        super().__init__(**kwargs)


class CapabilityError(B24ApiError):
    """Requested plan or feature is not authorized for the query shape."""

    default_origin = ErrorOrigin.CAPABILITY


class PaginationError(B24ApiError):
    """Pagination invariant violation."""

    default_origin = ErrorOrigin.PAGINATION


class BudgetExceededError(B24ApiError):
    """Execution would exceed an explicit operational budget."""

    default_origin = ErrorOrigin.BUDGET


class ResponseTooLargeError(B24ApiError):
    """A decompressed response exceeded the configured byte ceiling."""

    default_origin = ErrorOrigin.PROTOCOL


class AmbiguousExecutionError(B24ApiError):
    """A mutation may have executed but no conclusive response was observed."""

    default_origin = ErrorOrigin.AMBIGUOUS_EXECUTION


class IncompleteTraversalError(B24ApiError):
    """Compatibility traversal ended without a completed terminal report."""

    def __init__(self, *, report: object) -> None:
        """Initialize instance state."""
        self.report = report
        super().__init__(
            "Traversal did not complete",
            origin=ErrorOrigin.PAGINATION,
        )


class RetryHTTPStatusError(HTTPGatewayError):
    """Import-compatible retryable HTTP status error with redacted presentation."""

    def __init__(self, message: str, *, request: httpx.Request, response: httpx.Response) -> None:
        """Initialize instance state."""
        self.httpx_request = request
        self.httpx_response = response
        endpoint = request.url.path.rstrip("/").rsplit("/", maxsplit=1)[-1] or "unknown"
        request_summary = summarize_request(f"{request.method} {endpoint}")
        body_preview: str | None
        try:
            body_preview = DEFAULT_REDACTOR.safe_preview(response.content)
        except httpx.ResponseNotRead:
            body_preview = None
        evidence = ResponseEvidence(http_status=response.status_code, body_preview=body_preview)
        super().__init__(
            f"Retryable HTTP status {response.status_code}",
            origin=ErrorOrigin.HTTP_GATEWAY,
            description=message,
            request_summary=request_summary,
            evidence=evidence,
            retryable=True,
        )


BudgetError = BudgetExceededError
AmbiguousOutcomeError = AmbiguousExecutionError

__all__ = [
    "AmbiguousExecutionError",
    "AmbiguousOutcomeError",
    "ApiResponseError",
    "B24ApiError",
    "BatchCommandError",
    "BudgetError",
    "BudgetExceededError",
    "CapabilityError",
    "ErrorOrigin",
    "FailurePhase",
    "HTTPGatewayError",
    "IncompleteTraversalError",
    "PaginationError",
    "ProtocolError",
    "ResponseTooLargeError",
    "RetryApiResponseError",
    "RetryHTTPStatusError",
    "TransportError",
]
