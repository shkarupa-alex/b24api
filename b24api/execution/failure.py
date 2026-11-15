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
    CleanupOverrunError,
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
    if isinstance(error, BudgetExceededError) and not isinstance(error, CleanupOverrunError):
        # A cleanup overrun refused no work: it is classified as any other failed close, not as the budget's end.
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
# The terminal reason of every stream its reader's cancellation ended, in every family.
CANCELLED_REASON = "iteration cancelled"


def report_reason(error: BaseException) -> str:
    """Return the public terminal reason for a failure: its report cause's declared name or type name.

    Private carrier exceptions declare ``report_cause`` (the public failure they carry) and may declare
    ``report_name``, so a report never names a private class.
    """
    cause = getattr(error, "report_cause", error)
    if not isinstance(cause, BaseException):
        cause = error
    return str(getattr(cause, "report_name", type(cause).__name__))


def public_cause(error: BaseException) -> BaseException:
    """Return the public failure a private carrier carries (``report_cause``); any other error is its own."""
    cause = getattr(error, "report_cause", None)
    return cause if isinstance(cause, BaseException) else error


CLEANUP_FAILED_REASON = "stream cleanup failed"


def cleanup_failure_violation(error: BaseException, *, secondary: bool) -> Violation:
    """Record a failed close or cleanup, in one shape for every stream.

    ``secondary`` marks one behind an earlier failure ("stream cleanup also failed (X)"); without one the close is
    what failed ("stream cleanup failed (X)", the report's terminal reason ``CLEANUP_FAILED_REASON``).
    """
    outcome = "also failed" if secondary else "failed"
    return Violation(
        ViolationSeverity.BLOCKING, "cleanup_failure", f"stream cleanup {outcome} ({report_reason(error)})"
    )


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


def with_cleanup_failure_behind(report: KernelReport, error: BaseException) -> KernelReport:
    """Return the report with one cleanup-failure violation for ``error``, behind what ended the stream first.

    A source its stream closed early records the failed close as what failed it; when the stream ended by something
    that came first (its reader's cancellation), that close is recorded once, as "stream cleanup also failed (X)".
    """
    violations = tuple(item for item in report.violations if item.code != "cleanup_failure")
    return replace(report, violations=(*violations, cleanup_failure_violation(error, secondary=True)))


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
    """Record one stable blocking violation for the failure and attach the frozen report.

    The failure is primary: its violation precedes the cleanup failures behind it ("stream cleanup also failed").
    """
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
        primary = Violation(
            failure.severity,
            failure.code,
            f"{reason} ({error_name})",
            error=report_error if isinstance(report_error, B24ApiError) else None,
            replay_disposition=replay_disposition,
        )
        violations = (
            *(item for item in violations if item.code != "cleanup_failure"),
            primary,
            *(item for item in violations if item.code == "cleanup_failure"),
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
    "with_cleanup_failure_behind",
]
