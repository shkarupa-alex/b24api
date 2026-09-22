"""Canonical failure classification and report attachment."""

from __future__ import annotations
import asyncio
import contextlib
from dataclasses import dataclass, replace
from typing import Any, TypeVar, cast

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
        report_error,
        "replay_disposition",
        ReplayDisposition.NOT_ELIGIBLE,
    )
    if isinstance(error, IncompleteTraversalError):
        report_error = error.error or report_error
        replay_disposition = error.replay_disposition
    error_name = str(getattr(report_error, "report_name", type(report_error).__name__))
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


__all__ = ["FailureClass", "classify_failure", "finalize_failure"]
