"""Execution facts of a failed command, and the one mapping from them to its outcome and settlement.

The executor and the batch dialects record the facts from a command's sends, the only places that know them:
whether it was handed to the transport, whether a send may have run, and which failure explains that. Every
public outcome variant and every completion settlement is chosen here from those facts, never from the
exception type, which names why a command failed and stays the outcome's cause.
"""

from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from b24api.contracts.completion import CommandSettlement
from b24api.contracts.policy import AmbiguityReason
from b24api.errors import (
    AmbiguousExecutionError,
    ApiResponseError,
    B24ApiError,
    BudgetExceededError,
    HTTPGatewayError,
    ProtocolError,
    ResponseTooLargeError,
    TransportError,
)

if TYPE_CHECKING:
    from b24api.contracts.policy import ExecutionPolicy
    from b24api.contracts.request_summary import RequestSummary

_FACTS = "_b24api_execution_facts"
_MAY_HAVE_RUN = "Command may have executed; automatic replay is forbidden"
_HTTP_SUCCESS_MINIMUM = 200
_HTTP_SUCCESS_MAXIMUM = 299


class Execution(Enum):
    """What is known about whether a failed command ran; it decides the outcome variant and the settlement."""

    #: Never handed to the transport, by its own failure: a failure outcome that settles as not executed.
    NOT_SENT = "not_sent"
    #: Proven not sent because the server halted before it or fail-fast stopped it: a ``HALTED`` outcome.
    WITHHELD = "withheld"
    #: Sent, and every send that may have run was answered conclusively or proven refused: a failure.
    ANSWERED = "answered"
    #: A send may have run and nothing proves otherwise: an unknown outcome, never replayed automatically.
    POSSIBLY_RUN = "possibly_run"

    @property
    def dispatched(self) -> bool:
        """Return whether the command was handed to the transport."""
        return self in {Execution.ANSWERED, Execution.POSSIBLY_RUN}

    @property
    def settlement(self) -> CommandSettlement:
        """Return the completion settlement of a command that failed this way."""
        if self is Execution.POSSIBLY_RUN:
            return CommandSettlement.UNKNOWN
        if self is Execution.ANSWERED:
            return CommandSettlement.FAILURE
        return CommandSettlement.NOT_EXECUTED


@dataclass(frozen=True, slots=True)
class _ExecutionFacts:
    """Recorded on a failure by the code that sent, or withheld, its command.

    ``unproven`` is the failure of the latest send that may have run, when another failure ended the command.
    """

    execution: Execution
    unproven: B24ApiError | None = None


def record_dispatch(error: BaseException, *, possibly_executed: bool, unproven: B24ApiError | None = None) -> None:
    """Record that the command of ``error`` was dispatched, whether a send may have run and which one did."""
    execution = Execution.POSSIBLY_RUN if possibly_executed else Execution.ANSWERED
    error.__dict__[_FACTS] = _ExecutionFacts(execution, None if unproven is error else unproven)


def record_withheld(error: BaseException) -> None:
    """Record that the command of ``error`` was proven not sent because the server or fail-fast stopped it."""
    error.__dict__[_FACTS] = _ExecutionFacts(Execution.WITHHELD)


def execution(error: object) -> Execution:
    """Return what the recorded facts say about whether the command of a failure ran.

    A failure without facts was never sent. An ambiguity is a possible execution by definition, also when it
    was built outside the code that records facts.
    """
    facts = getattr(error, _FACTS, None)
    if isinstance(facts, _ExecutionFacts):
        return facts.execution
    return Execution.POSSIBLY_RUN if isinstance(error, AmbiguousExecutionError) else Execution.NOT_SENT


def ambiguity_of(
    error: B24ApiError, *, request_summary: RequestSummary | None, declared_unsafe: bool
) -> AmbiguousExecutionError | None:
    """Return the ambiguity a failed command's outcome carries when it may have run; ``None`` when it did not.

    This is the one choice between an unknown outcome and any other: every dialect and mapper makes it here.
    """
    if execution(error) is not Execution.POSSIBLY_RUN:
        return None
    return possible_execution(error, request_summary=request_summary, declared_unsafe=declared_unsafe)


def answered_conclusively(error: BaseException, policy: ExecutionPolicy) -> bool:
    """Return whether the server answered a send conclusively, so that its failure is the send's outcome.

    A structured Bitrix error, top-level or found in the result by the request's own contract, answers it, and so
    does any status outside 2xx that the policy does not hold ambiguous, whatever its body: an error body that
    cannot be decoded changes nothing about the status. A 2xx answer that cannot be decoded proves nothing, as a
    lost or late answer and an ambiguous status do not.
    """
    if isinstance(error, ApiResponseError):
        return True
    if not isinstance(error, HTTPGatewayError | ProtocolError):
        return False
    status = error.http_status
    return status is not None and not http_success(status) and not ambiguous_status(status, policy)


def http_success(status: int) -> bool:
    """Return whether an HTTP status is a 2xx success."""
    return _HTTP_SUCCESS_MINIMUM <= status <= _HTTP_SUCCESS_MAXIMUM


def ambiguous_status(status: int | None, policy: ExecutionPolicy) -> bool:
    """Return whether a failure status leaves open whether the request ran, as the policy holds it."""
    return (
        status is not None and not http_success(status) and status in policy.ambiguity.ambiguous_unstructured_statuses
    )


def ambiguity_reason(error: B24ApiError) -> AmbiguityReason:
    """Name why a dispatched command has no conclusive outcome, from the send that may have run."""
    error = _explained_by(error)
    if isinstance(error, AmbiguousExecutionError):
        return error.reason
    if isinstance(error, ResponseTooLargeError):
        return AmbiguityReason.RESPONSE_LIMIT_AFTER_DISPATCH
    if isinstance(error, BudgetExceededError) or isinstance(error.__cause__, TimeoutError):
        return AmbiguityReason.DEADLINE_AFTER_DISPATCH
    if isinstance(error, TransportError):
        return AmbiguityReason.CONNECTION_LOST_AFTER_DISPATCH
    # An answer arrived but proves nothing: an ambiguous status, or a body that cannot be decoded.
    return AmbiguityReason.HTTP_STATUS_AFTER_DISPATCH


def possible_execution(
    error: B24ApiError,
    *,
    request_summary: RequestSummary | None,
    declared_unsafe: bool,
    message: str = _MAY_HAVE_RUN,
) -> AmbiguousExecutionError:
    """Return the ambiguity that reports a possibly executed command; one about this same request is kept.

    The reason and evidence name the latest send that may have run. The cause keeps the original failure,
    unwrapped from an ambiguity about the physical batch, unless a later conclusive answer ended the command;
    then it is the failure of that earlier send.
    """
    if isinstance(error, AmbiguousExecutionError) and error.request_summary == request_summary:
        return error
    unproven = _recorded_unproven(error)
    source = _explained_by(error)
    ambiguous = AmbiguousExecutionError(
        message,
        reason=ambiguity_reason(source),
        declared_unsafe=declared_unsafe,
        request_summary=request_summary,
        evidence=source.evidence,
    )
    original = error.__cause__ if isinstance(error, AmbiguousExecutionError) else None
    terminal = original if isinstance(original, BaseException) else error
    ambiguous.__cause__ = terminal if unproven is None or _caused_by(terminal, unproven) else unproven
    record_dispatch(ambiguous, possibly_executed=True, unproven=source)
    return ambiguous


def _recorded_unproven(error: BaseException) -> B24ApiError | None:
    facts = getattr(error, _FACTS, None)
    return facts.unproven if isinstance(facts, _ExecutionFacts) else None


def _explained_by(error: B24ApiError) -> B24ApiError:
    """Return the failure of the send that may have run: recorded, or behind an ambiguity or a stopped retry."""
    recorded = _recorded_unproven(error)
    if recorded is not None:
        return recorded
    cause = error.__cause__
    if isinstance(error, AmbiguousExecutionError | BudgetExceededError) and isinstance(cause, B24ApiError):
        return _explained_by(cause)
    return error


def _caused_by(error: BaseException, cause: BaseException) -> bool:
    current: BaseException | None = error
    while current is not None:
        if current is cause:
            return True
        current = current.__cause__
    return False


__all__ = [
    "Execution",
    "ambiguity_of",
    "ambiguity_reason",
    "ambiguous_status",
    "answered_conclusively",
    "execution",
    "http_success",
    "possible_execution",
    "record_dispatch",
    "record_withheld",
]
