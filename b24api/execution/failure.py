"""Canonical failure classification and report attachment."""

from __future__ import annotations
import asyncio
import contextlib
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, TypeVar, cast

from b24api.contracts.policy import KernelState, ReplayDisposition
from b24api.contracts.report import TerminalState, Violation, ViolationSeverity
from b24api.errors import (
    AmbiguousExecutionError,
    ApiResponseError,
    B24ApiError,
    BatchCommandError,
    BudgetExceededError,
    CapabilityError,
    EnvelopeContractError,
    HTTPGatewayError,
    IdentityContractError,
    IncompleteTraversalError,
    InputSourceError,
    PaginationError,
    ProtocolError,
    ResponseTooLargeError,
    ResultShapeError,
    TransportError,
)

if TYPE_CHECKING:
    from b24api.execution.snapshot import KernelReport

R = TypeVar("R")


@dataclass(frozen=True, slots=True)
class FailureClass:
    """Stable report classification for a terminal exception."""

    code: str
    severity: ViolationSeverity = ViolationSeverity.BLOCKING
    incomplete: bool = False


def classify_failure(error: BaseException) -> FailureClass:  # noqa: C901, PLR0911, PLR0912
    """Classify a failure using the normative first-match order."""
    report_cause = getattr(error, "report_cause", None)
    if isinstance(report_cause, BaseException) and report_cause is not error:
        return classify_failure(report_cause)
    if isinstance(error, IncompleteTraversalError):
        if isinstance(error.__cause__, BaseException):
            cause = classify_failure(error.__cause__)
            return FailureClass(cause.code, cause.severity, incomplete=True)
        return FailureClass("pagination_invariant", incomplete=True)
    if isinstance(error, IdentityContractError):
        return FailureClass("identity_contract", incomplete=True)
    if isinstance(error, ResultShapeError):
        return FailureClass("result_shape")
    if isinstance(error, PaginationError):
        return FailureClass("pagination_invariant", incomplete=True)
    if isinstance(error, CapabilityError):
        return FailureClass("capability_rejected")
    if isinstance(error, AmbiguousExecutionError):
        return FailureClass("ambiguous_execution")
    if isinstance(error, BudgetExceededError):
        return FailureClass("budget_exhausted", incomplete=True)
    if isinstance(error, ResponseTooLargeError):
        return FailureClass("response_too_large")
    if isinstance(error, TransportError):
        return FailureClass("transport_failure")
    if isinstance(error, EnvelopeContractError):
        return FailureClass("envelope_contract")
    if isinstance(error, HTTPGatewayError):
        return FailureClass("http_failure")
    if isinstance(error, ProtocolError):
        return FailureClass("protocol_failure")
    if isinstance(error, BatchCommandError):
        return FailureClass("batch_command_failure")
    if isinstance(error, ApiResponseError):
        return FailureClass("api_error")
    if isinstance(error, InputSourceError):
        return FailureClass("source_failure")
    return FailureClass("internal_failure")


EARLY_CLOSE_REASON = "stream closed before exhaustion"


def report_reason(error: BaseException) -> str:
    """Return the public terminal reason for a failure: its report cause's declared name or type name.

    Private carrier exceptions declare ``report_cause`` (the public failure they carry) and may declare
    ``report_name``, so a report never names a private class.
    """
    cause = getattr(error, "report_cause", error)
    if not isinstance(cause, BaseException):
        cause = error
    return str(getattr(cause, "report_name", type(cause).__name__))


def cleanup_failure_violation(error: BaseException, *, secondary: bool) -> Violation:
    """Record a cleanup failure; ``secondary`` marks one that follows an earlier primary failure."""
    outcome = "also failed" if secondary else "failed"
    return Violation(ViolationSeverity.BLOCKING, "cleanup_failure", f"batch cleanup {outcome} ({type(error).__name__})")


CLEANUP_FAILED_REASON = "stream cleanup failed"


def with_cleanup_failure(report: KernelReport, error: BaseException, *, terminal: bool) -> KernelReport:
    """Return the kernel report with one cleanup-failure violation.

    A terminal cleanup failure (no earlier primary failure) also turns the report FAILED with
    ``CLEANUP_FAILED_REASON``; a secondary one keeps the primary outcome and adds its violation.
    """
    violations = report.violations
    if not (terminal and any(item.code == "cleanup_failure" for item in violations)):
        violations = (*violations, cleanup_failure_violation(error, secondary=not terminal))
    if not terminal:
        return replace(report, violations=violations)
    return replace(report, state=KernelState.FAILED, terminal_reason=CLEANUP_FAILED_REASON, violations=violations)


def attach_report(error: BaseException, report: object) -> None:
    """Attach immutable report evidence without replacing the primary error."""
    with contextlib.suppress(AttributeError, TypeError):
        error.report = report  # type: ignore[attr-defined]


def finalize_failure[R](
    error: BaseException,
    report: R,
    *,
    operation: str,
    terminal_reason: str,
) -> tuple[R, BaseException]:
    """Append one stable blocking violation and attach the frozen report."""
    if isinstance(error, asyncio.CancelledError):
        attach_report(error, report)
        return report, error
    failure = classify_failure(error)
    report_error = getattr(error, "report_cause", error)
    replay_disposition = getattr(
        error,
        "replay_disposition",
        getattr(report_error, "replay_disposition", ReplayDisposition.NOT_ELIGIBLE),
    )
    if isinstance(error, IncompleteTraversalError):
        report_error = error.error or report_error
        replay_disposition = error.replay_disposition
    error_name = report_reason(report_error)
    reason = terminal_reason if terminal_reason and terminal_reason != error_name else f"{operation} failed"
    violations = tuple(getattr(report, "violations", ()))
    if not any(item.code == failure.code for item in violations):
        violations = (
            *violations,
            Violation(
                failure.severity,
                failure.code,
                f"{reason} ({error_name})",
                error=report_error if isinstance(report_error, B24ApiError) else None,
                replay_disposition=replay_disposition,
            ),
        )
    state: object = TerminalState.INCOMPLETE if failure.incomplete else TerminalState.FAILED
    if isinstance(getattr(report, "state", None), KernelState):
        state = KernelState.INCOMPLETE if failure.incomplete else KernelState.FAILED
    changes: dict[str, object] = {
        "state": state,
        "terminal_reason": terminal_reason or operation,
        "violations": violations,
    }
    if hasattr(report, "exhausted"):
        changes["exhausted"] = False
    if getattr(report, "empty_source_witness", None) is not None:
        changes["empty_source_witness"] = None
    try:
        frozen = cast(
            "R",
            replace(cast("Any", report), **changes),
        )
    except Exception as finalization_error:  # noqa: BLE001
        finalization_error.__context__ = error.__context__
        error.__context__ = finalization_error
        attach_report(error, report)
        return report, error
    attach_report(error, frozen)
    return frozen, error


__all__ = [
    "CLEANUP_FAILED_REASON",
    "EARLY_CLOSE_REASON",
    "FailureClass",
    "classify_failure",
    "cleanup_failure_violation",
    "finalize_failure",
    "report_reason",
    "with_cleanup_failure",
]
