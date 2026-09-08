"""Redacted Bitrix24 error hierarchy."""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, ClassVar

from b24api._error_types import ErrorOrigin, FailurePhase
from b24api.contracts.policy import AmbiguityReason, IdentityCoercion
from b24api.contracts.response import ResponseEvidence, ResultCollectionShape
from b24api.redaction import DEFAULT_REDACTOR, Redactor

if TYPE_CHECKING:
    from collections.abc import Mapping

    from b24api.contracts.command import CommandOutcome
    from b24api.contracts.reference import ReferenceOutcome
    from b24api.contracts.report import OperationReport
    from b24api.contracts.request import PathPart, RequestSummary, ResultSelector


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


class EnvelopeContractError(HTTPGatewayError):
    """A 2xx response violated the canonical Bitrix envelope contract."""


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
        self.wire_code = redactor.redact_text(str(code))
        summary = request_summary
        safe_description = redactor.redact_text(description) if description is not None else None
        rendered_code = self.wire_code
        normalized_suffix = f" (normalized: {self.normalized_code})" if rendered_code != self.normalized_code else ""
        if rendered_code and safe_description:
            message = f"API error [{rendered_code}]{normalized_suffix}: {safe_description}"
        elif rendered_code:
            message = f"API error [{rendered_code}]{normalized_suffix}"
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
                "wire_code": self.wire_code,
            },
        )
        return safe


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


class IdentityContractError(PaginationError):
    """A row did not satisfy its declared identity contract."""

    def __init__(  # noqa: PLR0913
        self,
        *,
        path: tuple[PathPart, ...],
        coercion: IdentityCoercion,
        observed_type: str,
        row_offset: int,
        request_summary: RequestSummary | None = None,
        component_index: int | None = None,
        component_label: str | None = None,
        redactor: Redactor = DEFAULT_REDACTOR,
    ) -> None:
        """Build value-free identity failure diagnostics."""
        self.path = tuple(path)
        self.coercion = coercion
        self.observed_type = redactor.redact_text(observed_type)
        self.row_offset = row_offset
        self.component_index = component_index
        self.component_label = redactor.redact_text(component_label) if component_label is not None else None
        phrases = {
            IdentityCoercion.EXACT_STRING: "an exact string",
            IdentityCoercion.EXACT_INTEGER: "an exact integer",
            IdentityCoercion.DECIMAL_STRING_INTEGER: "a decimal string integer",
        }
        message = f"identity at {self.path!r} must be {phrases[coercion]}, got {self.observed_type} (row {row_offset})"
        if component_index is not None:
            suffix = f" {self.component_label}" if self.component_label is not None else ""
            message += f" [component {component_index}{suffix}]"
        super().__init__(message, request_summary=request_summary, redactor=redactor)

    def to_safe_dict(self) -> dict[str, object]:
        """Return structured value-free identity evidence."""
        safe = super().to_safe_dict()
        safe.update(
            {
                "path": list(self.path),
                "coercion": self.coercion.value,
                "observed_type": self.observed_type,
                "row_offset": self.row_offset,
                "component_index": self.component_index,
                "component_label": self.component_label,
            },
        )
        return safe


class ResultShapeError(CapabilityError):
    """A resolved result value had the wrong declared collection shape."""

    def __init__(  # noqa: PLR0913
        self,
        *,
        selector: ResultSelector,
        expected_shape: ResultCollectionShape,
        observed_type: str,
        request_summary: RequestSummary | None = None,
        page_offset: int | None = None,
        redactor: Redactor = DEFAULT_REDACTOR,
    ) -> None:
        """Build value-free result-shape diagnostics."""
        self.selector = selector
        self.expected_shape = expected_shape
        self.observed_type = redactor.redact_text(observed_type)
        self.page_offset = page_offset
        phrases = {
            ResultCollectionShape.SEQUENCE: "a sequence",
            ResultCollectionShape.MAPPING_VALUES: "a mapping",
            ResultCollectionShape.MAPPING_VALUES_OR_EMPTY: "a mapping or empty sequence",
        }
        message = f"selected result at {selector.path!r} must be {phrases[expected_shape]}, got {self.observed_type}"
        if page_offset is not None:
            message += f" (offset {page_offset})"
        super().__init__(message, request_summary=request_summary, redactor=redactor)

    def to_safe_dict(self) -> dict[str, object]:
        """Return structured value-free shape evidence."""
        safe = super().to_safe_dict()
        safe.update(
            {
                "selector": list(self.selector.path),
                "expected_shape": self.expected_shape.value,
                "observed_type": self.observed_type,
                "page_offset": self.page_offset,
            },
        )
        return safe


class BudgetExceededError(B24ApiError):
    """Execution would exceed an explicit operational budget."""

    default_origin = ErrorOrigin.BUDGET


class ResponseTooLargeError(B24ApiError):
    """A decompressed response exceeded the configured byte ceiling."""

    default_origin = ErrorOrigin.PROTOCOL


class AmbiguousExecutionError(B24ApiError):
    """A mutation may have executed but no conclusive response was observed."""

    default_origin = ErrorOrigin.AMBIGUOUS_EXECUTION

    def __init__(  # noqa: PLR0913
        self,
        message: str,
        *,
        reason: AmbiguityReason,
        declared_unsafe: bool,
        request_summary: RequestSummary | None = None,
        evidence: ResponseEvidence | None = None,
        redactor: Redactor = DEFAULT_REDACTOR,
    ) -> None:
        """Initialize typed ambiguity evidence."""
        if not isinstance(reason, AmbiguityReason) or not isinstance(declared_unsafe, bool):
            raise TypeError("ambiguity reason and declared_unsafe must use declared types")
        self.reason = reason
        self.declared_unsafe = declared_unsafe
        super().__init__(message, request_summary=request_summary, evidence=evidence, redactor=redactor)

    def to_safe_dict(self) -> dict[str, object]:
        """Return structured ambiguity evidence."""
        safe = super().to_safe_dict()
        safe.update({"reason": self.reason.value, "declared_unsafe": self.declared_unsafe})
        return safe


class IncompleteTraversalError(B24ApiError):
    """Traversal ended without complete terminal evidence."""

    def __init__(self, *, report: object) -> None:
        """Initialize instance state."""
        self.report = report
        message = "Traversal did not complete"
        violations = getattr(report, "violations", ())
        blocking = next(
            (item for item in violations if getattr(getattr(item, "severity", None), "value", None) == "blocking"),
            None,
        )
        if blocking is not None:
            message += f" [{blocking.code}] {blocking.message}"
        super().__init__(
            message,
            origin=ErrorOrigin.PAGINATION,
        )


class InputSourceError(B24ApiError):
    """A caller-owned sync or async input source failed during admission."""

    default_origin = ErrorOrigin.CAPABILITY


class BatchFailed[C](B24ApiError):  # noqa: N818 - normative public name
    """Bounded fail-fast batch window plus its final report."""

    def __init__(self, outcomes: tuple[CommandOutcome[C], ...], *, report: OperationReport) -> None:
        """Retain only the bounded active window."""
        self.outcomes = outcomes
        self.report = report
        super().__init__("Logical batch did not complete", origin=ErrorOrigin.BATCH_COMMAND)


class ReferenceFailed[C](B24ApiError):  # noqa: N818 - normative public name
    """Bounded fail-fast reference window plus its final report."""

    def __init__(self, outcomes: tuple[ReferenceOutcome[C], ...], *, report: OperationReport) -> None:
        """Retain only the bounded active window."""
        self.outcomes = outcomes
        self.report = report
        super().__init__("Reference traversal did not complete", origin=ErrorOrigin.PAGINATION)


_PUBLIC_ERROR_NAMES = (
    "AmbiguousExecutionError ApiResponseError B24ApiError BatchCommandError BatchFailed BudgetExceededError "
    "CapabilityError EnvelopeContractError ErrorOrigin FailurePhase HTTPGatewayError IdentityContractError "
    "IncompleteTraversalError InputSourceError PaginationError ProtocolError ReferenceFailed ResponseTooLargeError "
    "ResultShapeError TransportError"
)
__all__ = tuple(_PUBLIC_ERROR_NAMES.split())
