"""Redacted Bitrix24 error hierarchy."""

from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from b24api._error_types import ErrorOrigin, FailurePhase
from b24api.contracts.error_base import B24ApiError, BudgetExceededError
from b24api.contracts.evidence import ResponseEvidence
from b24api.contracts.keyset_capability import KeysetCapabilityReport, KeysetCapabilityVerdict, KeysetInconclusiveReason
from b24api.contracts.policy import AmbiguityReason, IdentityCoercion, ReplayDisposition
from b24api.contracts.response import ResultCollectionShape
from b24api.contracts.v3_codes import render_code
from b24api.redaction import DEFAULT_REDACTOR, Redactor, SafeText

if TYPE_CHECKING:
    from collections.abc import Mapping

    from b24api._diagnostics import DiagnosticContext
    from b24api.contracts.command import CommandOutcome
    from b24api.contracts.reference import ReferenceOutcome
    from b24api.contracts.report import OperationReport
    from b24api.contracts.request import PathPart, ResultSelector
    from b24api.contracts.request_summary import RequestSummary


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
        return {**super().to_safe_dict(), "phase": self.phase.value, "possible_acceptance": self.possible_acceptance}


class HTTPGatewayError(B24ApiError):
    """Non-structured HTTP or gateway failure."""

    default_origin = ErrorOrigin.HTTP_GATEWAY


class ProtocolError(B24ApiError):
    """Malformed or contradictory protocol envelope."""

    default_origin = ErrorOrigin.PROTOCOL


class EnvelopeContractError(HTTPGatewayError, ProtocolError):
    """A 2xx body is not a Bitrix envelope: a gateway-origin failure that ``except ProtocolError`` catches."""

    default_origin = ErrorOrigin.HTTP_GATEWAY


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    """One redacted V3 validation location and message."""

    field: str
    message: str


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
        validation: tuple[ValidationIssue, ...] = (),
        truncated: bool = False,
        code_is_exact: bool = False,
        origin: ErrorOrigin = ErrorOrigin.REST_MODULE,
        retryable: bool = False,
        redactor: Redactor = DEFAULT_REDACTOR,
        diagnostics: DiagnosticContext | None = None,
    ) -> None:
        """Keep raw codes as attributes; render codes, message and evidence once for every output channel."""
        self.original_code = code
        self.code = str(code).lower()
        self.normalized_code = str(code) if code_is_exact else str(code).strip().casefold()
        self.wire_code = render_code(code, redactor=redactor, context=diagnostics)
        self._safe_codes = {
            "original_code": code if isinstance(code, int) else self.wire_code,
            "code": render_code(self.code, redactor=redactor, context=diagnostics),
            "normalized_code": render_code(self.normalized_code, redactor=redactor, context=diagnostics),
        }
        self.validation = tuple(validation)
        self.truncated = truncated
        safe_description = redactor.render_text(description, context=diagnostics) if description is not None else None
        rendered_code = self.wire_code
        normalized = self._safe_codes["normalized_code"]
        normalized_suffix = f" (normalized: {normalized})" if rendered_code != normalized else ""
        if rendered_code and safe_description:
            message = f"API error [{rendered_code}]{normalized_suffix}: {safe_description}"
        elif rendered_code:
            message = f"API error [{rendered_code}]{normalized_suffix}"
        else:
            message = f"API error: {safe_description}"
        redacted_headers = redactor.redact(dict(headers or {}), context=diagnostics)
        safe_headers = tuple(sorted((str(key), str(value)) for key, value in redacted_headers.items()))
        evidence = ResponseEvidence(
            http_status=http_status,
            request_id=dict(safe_headers).get("x-request-id"),
            headers=safe_headers,
            # The codec already rendered the preview through the request context; aliases must not be re-aliased.
            body_preview=redactor.redact_text(body_preview) if body_preview is not None else None,
        )
        super().__init__(
            SafeText(message),  # every part above is already rendered; a second pass would hide the known code
            origin=origin,
            description=safe_description,
            request_summary=request_summary,
            evidence=evidence,
            retryable=retryable,
            redactor=redactor,
        )

    def to_safe_dict(self) -> dict[str, object]:
        """Return the to safe dict representation."""
        safe = super().to_safe_dict()
        safe.update(
            {
                **self._safe_codes,
                "wire_code": self.wire_code,
                "validation": [{"field": issue.field, "message": issue.message} for issue in self.validation],
                "truncated": self.truncated,
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


class PageAdaptationViolation(StrEnum):
    """Closed reasons a page adapter can violate its public contract."""

    NOT_ADAPTED_PAGE = "not_adapted_page"
    CARDINALITY_CHANGED = "cardinality_changed"
    ORDER_OR_IDENTITY_CHANGED = "order_or_identity_changed"
    NON_JSON_VALUE = "non_json_value"
    ADAPTER_RAISED = "adapter_raised"


class PageAdaptationError(CapabilityError):
    """A page adapter violated the immutable adaptation contract."""

    def __init__(  # noqa: PLR0913
        self,
        *,
        violation: PageAdaptationViolation,
        adapter: str,
        row_offset: int | None = None,
        page_offset: int | None = None,
        request_summary: RequestSummary | None = None,
        redactor: Redactor = DEFAULT_REDACTOR,
    ) -> None:
        """Build value-free adapter failure diagnostics."""
        if not isinstance(violation, PageAdaptationViolation):
            raise TypeError("violation must be a PageAdaptationViolation")
        self.violation = violation
        self.adapter = redactor.redact_text(adapter)
        self.row_offset = row_offset
        self.page_offset = page_offset
        messages = {
            PageAdaptationViolation.NOT_ADAPTED_PAGE: "page adapter did not return AdaptedPage",
            PageAdaptationViolation.CARDINALITY_CHANGED: "page adapter changed row cardinality",
            PageAdaptationViolation.ORDER_OR_IDENTITY_CHANGED: "page adapter changed row order or identity",
            PageAdaptationViolation.NON_JSON_VALUE: "page adapter returned a non-JSON value",
            PageAdaptationViolation.ADAPTER_RAISED: "page adapter raised an exception",
        }
        message = f"{messages[violation]} ({self.adapter})"
        if row_offset is not None:
            message += f" (row {row_offset})"
        if page_offset is not None:
            message += f" (page {page_offset})"
        super().__init__(message, request_summary=request_summary, retryable=False, redactor=redactor)

    def to_safe_dict(self) -> dict[str, object]:
        """Return structured value-free adaptation evidence."""
        safe = super().to_safe_dict()
        safe.update(
            {
                "violation": self.violation.value,
                "adapter": self.adapter,
                "row_offset": self.row_offset,
                "page_offset": self.page_offset,
            },
        )
        return safe


class KeysetCapabilityError(CapabilityError):
    """A keyset capability verdict that is not verified, with its full report."""

    def __init__(self, *, report: KeysetCapabilityReport, redactor: Redactor = DEFAULT_REDACTOR) -> None:
        """Retain the one canonical verifier report."""
        if not isinstance(report, KeysetCapabilityReport):
            raise TypeError("report must be a KeysetCapabilityReport")
        if report.verdict is KeysetCapabilityVerdict.VERIFIED:
            raise ValueError("verified capability reports are returned, not raised")
        self.report = report
        message = (
            "keyset capability is unsupported"
            if report.verdict is KeysetCapabilityVerdict.UNSUPPORTED
            else "keyset capability verdict is inconclusive"
        )
        if report.inconclusive_reason is not None:
            message += f" ({report.inconclusive_reason.value})"
        super().__init__(message, retryable=False, redactor=redactor)

    @property
    def verdict(self) -> KeysetCapabilityVerdict:
        """Return the report verdict."""
        return self.report.verdict

    @property
    def inconclusive_reason(self) -> KeysetInconclusiveReason | None:
        """Return the report's actionable inconclusive reason."""
        return self.report.inconclusive_reason

    def to_safe_dict(self) -> dict[str, object]:
        """Serialize counters and outcomes without identity evidence."""
        safe = super().to_safe_dict()
        safe.update(
            {
                "verdict": self.verdict.value,
                "inconclusive_reason": (
                    self.inconclusive_reason.value if self.inconclusive_reason is not None else None
                ),
                "logical_commands": self.report.logical_commands,
                "batch_waves": self.report.batch_waves,
                "physical_requests": self.report.physical_requests,
                "cross_digit_pair_exercised": self.report.cross_digit_pair_exercised,
                "checks": [{"name": check.name.value, "outcome": check.outcome.value} for check in self.report.checks],
            },
        )
        return safe


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

    def __init__(
        self,
        *,
        report: object,
        error: B24ApiError | None = None,
        replay_disposition: ReplayDisposition = ReplayDisposition.NOT_ELIGIBLE,
    ) -> None:
        """Initialize instance state."""
        if not isinstance(replay_disposition, ReplayDisposition):
            raise TypeError("replay_disposition must be a ReplayDisposition")
        if error is not None and not isinstance(error, B24ApiError):
            raise TypeError("error must be a B24ApiError or None")
        self.report = report
        self.error = error
        self.replay_disposition = replay_disposition
        super().__init__("Traversal did not complete", origin=ErrorOrigin.PAGINATION)

    def __str__(self) -> str:
        """Render the current attached report's first blocking cause."""
        message = "Traversal did not complete"
        violations = getattr(self.report, "violations", ())
        blocking = next(
            (item for item in violations if getattr(getattr(item, "severity", None), "value", None) == "blocking"),
            None,
        )
        if blocking is not None:
            message += f" [{blocking.code}] {blocking.message}"
        return message

    def to_safe_dict(self) -> dict[str, object]:
        """Expose the retained cause and replay decision without diagnostic I/O."""
        safe = super().to_safe_dict()
        safe.update(
            {
                "cause": self.error.to_safe_dict() if self.error is not None else None,
                "replay_disposition": self.replay_disposition.value,
            },
        )
        return safe


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


__all__ = (
    "AmbiguousExecutionError",
    "ApiResponseError",
    "B24ApiError",
    "BatchCommandError",
    "BatchFailed",
    "BudgetExceededError",
    "CapabilityError",
    "EnvelopeContractError",
    "ErrorOrigin",
    "FailurePhase",
    "HTTPGatewayError",
    "IdentityContractError",
    "IncompleteTraversalError",
    "InputSourceError",
    "KeysetCapabilityError",
    "PageAdaptationError",
    "PageAdaptationViolation",
    "PaginationError",
    "ProtocolError",
    "ReferenceFailed",
    "ResponseTooLargeError",
    "ResultShapeError",
    "TransportError",
    "ValidationIssue",
)
