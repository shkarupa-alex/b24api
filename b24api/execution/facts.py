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
    ResultTooLargeError,
    TransportError,
)

if TYPE_CHECKING:
    from b24api.contracts.evidence import ResponseEvidence
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
    # Answered with a success: the failure is not the command's but its page's, ended before it took the answer.
    succeeded: bool = False


def record_dispatch(error: BaseException, *, possibly_executed: bool, unproven: B24ApiError | None = None) -> None:
    """Record that the command of ``error`` was dispatched, whether a send may have run and which one did."""
    execution = Execution.POSSIBLY_RUN if possibly_executed else Execution.ANSWERED
    error.__dict__[_FACTS] = _ExecutionFacts(execution, None if unproven is error else unproven)


def dropped_result(
    *, rows: int, limit: int, request_summary: RequestSummary | None, evidence: ResponseEvidence | None
) -> ResultTooLargeError:
    """Return the failure of a command that ran and was answered, but whose result the row budget drops.

    It is recorded answered: the command's outcome is a failure settled as failed, never one proven not run, and
    it keeps the answer's ``evidence``.
    """
    error = ResultTooLargeError(rows=rows, limit=limit, request_summary=request_summary, evidence=evidence)
    record_dispatch(error, possibly_executed=False)
    return error


def record_withheld(error: BaseException) -> None:
    """Record that the command of ``error`` was proven not sent because the server or fail-fast stopped it."""
    error.__dict__[_FACTS] = _ExecutionFacts(Execution.WITHHELD)


def record_execution(error: BaseException, known: Execution, *, succeeded: bool = False) -> None:
    """Record on ``error`` what is ``known`` about its command from elsewhere, such as a batch it was a command of.

    ``succeeded`` is set for a command answered with a success (``ANSWERED``), whose page ended before it took the
    answer.
    """
    error.__dict__[_FACTS] = _ExecutionFacts(known, succeeded=succeeded)


def settled_as(known: Execution, *, succeeded: bool) -> CommandSettlement:
    """Return the completion settlement of a command known so: one answered with a success succeeded."""
    return CommandSettlement.SUCCESS if succeeded else known.settlement


def settlement(error: object) -> CommandSettlement:
    """Return the completion settlement of a command whose page failed with ``error`` (``settled_as``)."""
    facts = getattr(error, _FACTS, None)
    return settled_as(execution(error), succeeded=isinstance(facts, _ExecutionFacts) and facts.succeeded)


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


_UNAUTHORIZED = 401


class Verdict(Enum):
    """What the failure that ended one send proves about whether its request ran: one value, by one rule.

    The executor, the classic and the REST v3 batch dialects all read a send's failure through ``send_verdict``,
    and derive from it both whether the request may be sent again and what its outcome is.
    """

    #: The request never reached the server, or the server refused it before running it.
    NOT_RUN = "not_run"
    #: The server answered it: the failure is its outcome.
    ANSWERED = "answered"
    #: It may have run and nothing proves otherwise: its outcome is unknown.
    MAY_HAVE_RUN = "may_have_run"


def send_verdict(error: BaseException, policy: ExecutionPolicy, *, v3_envelope: bool = False) -> Verdict:
    """Return what the failure that ended a send proves about whether its request ran (``Verdict``).

    The failure is read whole, as the decoder left it: a 2xx body whose control members contradict each other or
    repeat, or that is malformed, is a ``ProtocolError`` that proves nothing. A transport failure proves the request
    did not run only when no byte of it may have reached the server. An answer the policy holds ambiguous (408 and
    every 5xx by default) proves nothing, structured or not, unless a listed refusal code proves it refused; a
    listed refusal status (423, 425, 429) whose body is a gateway's page proves it refused: a body without a
    readable top-level ``error`` that the decoder did not refuse for a repeated member.
    Any other structured error, and any other status outside 2xx whatever its body, answers it. An oversized or
    lost answer and any failure that is not an answer prove nothing.

    ``v3_envelope`` reads the top-level answer to a REST 3.0 batch envelope. Such an envelope runs its commands
    in turn and aborts on the first that fails, its load limiter included, so its error, structured, unreadable or
    malformed, proves nothing about the commands before that one, except a 401 (credentials no command could pass)
    and a gateway's page at a listed refusal status. A classic batch envelope is checked and
    limited as one request, so its answer is read as a lone request's.
    """
    if isinstance(error, TransportError):
        return Verdict.MAY_HAVE_RUN if error.possible_acceptance else Verdict.NOT_RUN
    if isinstance(error, ApiResponseError):
        return _structured_verdict(error, policy, v3_envelope=v3_envelope)
    if isinstance(error, HTTPGatewayError | ProtocolError):
        return _status_verdict(error, policy, v3_envelope=v3_envelope)
    # An ambiguity already decided, an oversized answer, and any failure that is not an answer prove nothing.
    return Verdict.MAY_HAVE_RUN


def _structured_verdict(error: ApiResponseError, policy: ExecutionPolicy, *, v3_envelope: bool) -> Verdict:
    """Return what a structured Bitrix error proves (``send_verdict``)."""
    status = error.http_status
    if v3_envelope:
        # A REST 3.0 envelope aborts on its first failed command, after the commands before it ran: only a
        # refusal of the credentials, which no command of it could pass, answers every command alike.
        return Verdict.ANSWERED if status == _UNAUTHORIZED else Verdict.MAY_HAVE_RUN
    if str(error.original_code).strip().casefold() in policy.ambiguity.refusal_api_codes:
        return Verdict.NOT_RUN
    return Verdict.MAY_HAVE_RUN if ambiguous_status(status, policy) else Verdict.ANSWERED


def _status_verdict(error: HTTPGatewayError | ProtocolError, policy: ExecutionPolicy, *, v3_envelope: bool) -> Verdict:
    """Return what an answer without a readable structured error proves, by its status (``send_verdict``)."""
    status = error.http_status
    if status is None or http_success(status) or ambiguous_status(status, policy):
        return Verdict.MAY_HAVE_RUN
    # A gateway's page (no readable top-level `error`, no repeated top-level member) at a listed refusal status is the
    # refusal; a body the server wrote but the client cannot read (a repeat, a contradiction, a malformed error)
    # proves no refusal.
    if isinstance(error, HTTPGatewayError) and status in policy.ambiguity.refusal_http_statuses:
        return Verdict.NOT_RUN
    if v3_envelope:
        # As for a structured error: only a refusal of the credentials answers every command of the envelope.
        return Verdict.ANSWERED if status == _UNAUTHORIZED else Verdict.MAY_HAVE_RUN
    return Verdict.ANSWERED


def carry_facts(source: BaseException, carrier: BaseException) -> None:
    """Record on ``carrier``, a failure that carries ``source`` in its place, what is known of ``source``'s command.

    The one way a failure is replaced by another that stands for it (an outcome's carrier of a foreign exception,
    for example): whatever was recorded about whether the command ran moves with it.
    """
    facts = getattr(source, _FACTS, None)
    if isinstance(facts, _ExecutionFacts):
        carrier.__dict__[_FACTS] = facts


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
    unproven: B24ApiError | None = None,
) -> AmbiguousExecutionError:
    """Return the ambiguity that reports a possibly executed command; one about this same request is kept.

    The reason and evidence name the latest send that may have run. The cause keeps the original failure,
    unwrapped from an ambiguity about the physical batch, unless a later conclusive answer ended the command;
    then it is the failure of that earlier send. ``unproven`` names that send's failure when the failure's own
    facts cannot: it is shared with commands that were not in that send.
    """
    if isinstance(error, AmbiguousExecutionError) and error.request_summary == request_summary:
        return error
    unproven = unproven or _recorded_unproven(error)
    source = unproven or _explained_by(error)
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
    """Return the failure of the send that may have run: recorded, or behind an ambiguity.

    A stop before a send is explained only by what its recorder recorded, never by its type or its cause.
    """
    recorded = _recorded_unproven(error)
    if recorded is not None:
        return recorded
    cause = error.__cause__
    if isinstance(error, AmbiguousExecutionError) and isinstance(cause, B24ApiError):
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
    "Verdict",
    "ambiguity_of",
    "ambiguity_reason",
    "ambiguous_status",
    "carry_facts",
    "dropped_result",
    "execution",
    "http_success",
    "possible_execution",
    "record_dispatch",
    "record_execution",
    "record_withheld",
    "send_verdict",
    "settled_as",
    "settlement",
]
