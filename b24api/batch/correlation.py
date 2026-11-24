"""Correlated per-command outcomes shared by the classic and the native v3 batch dialects."""

from __future__ import annotations
import dataclasses
from dataclasses import dataclass
from typing import TYPE_CHECKING

from b24api.batch.outcome import BatchCommandEvidence, BatchFailure, BatchOutcome, BatchSuccess
from b24api.contracts.policy import ReplayDisposition
from b24api.contracts.request import ReplaySafety, Request
from b24api.errors import B24ApiError, ProtocolError, TransportError
from b24api.execution.facts import ambiguity_of, possible_execution, record_withheld

if TYPE_CHECKING:
    from b24api.contracts.response import Response
    from b24api.execution.executor import _PossiblyRun


class _BatchNotExecutedError(ProtocolError):
    """Internal correlated marker of a command the server halted before or fail-fast stopped.

    Its execution facts, not its type, say whether the command was proven not to run.
    """


@dataclass(frozen=True, slots=True)
class _Command:
    index: int
    stable_key: str
    request: Request
    correlation: object


def _command_failure(
    command: _Command,
    error: B24ApiError,
    *,
    evidence: BatchCommandEvidence,
    refused: bool = False,
) -> BatchFailure:
    safety = command.request.replay_safety or ReplaySafety.UNKNOWN
    # A listed Bitrix refusal of the command, or a transport failure before dispatch, proves it did not run.
    proven_not_run = refused or (isinstance(error, TransportError) and not error.possible_acceptance)
    eligible = error.retryable and (safety is ReplaySafety.SAFE or proven_not_run)
    return BatchFailure(
        command.index,
        command.stable_key,
        command.request,
        error,
        replay_safety=safety,
        replay_disposition=ReplayDisposition.ELIGIBLE if eligible else ReplayDisposition.NOT_ELIGIBLE,
        correlation=command.correlation,
        evidence=evidence,
    )


def _shared_failure(command: _Command, error: B24ApiError) -> BatchFailure:
    return _command_failure(
        command,
        error,
        evidence=BatchCommandEvidence(command.index, command.stable_key),
    )


def _by_facts(outcome: BatchOutcome, earlier: _PossiblyRun | None = None) -> BatchOutcome:
    """Give a failed command the outcome its execution facts call for; the batch dialects report only this.

    Until a command's outcome is final, its failure keeps its own error and replay disposition, which decide
    whether it is sent again; a command that may have run then carries the ambiguity that explains it.
    ``earlier`` is a send of the command before the one its failure records that may have run: a failure a
    classic round shares records only that round's send, which a command carried past a round did not share.
    """
    if isinstance(outcome, BatchSuccess) or not isinstance(outcome.error, B24ApiError):
        return outcome
    summary, unsafe = outcome.request.summary, outcome.replay_safety is ReplaySafety.UNSAFE
    ambiguity = ambiguity_of(outcome.error, request_summary=summary, declared_unsafe=unsafe)
    if ambiguity is None and earlier is not None:
        ambiguity = possible_execution(
            outcome.error, request_summary=summary, declared_unsafe=unsafe, unproven=earlier.failure
        )
    if ambiguity is None:
        return outcome
    return dataclasses.replace(outcome, error=ambiguity, replay_disposition=ReplayDisposition.NOT_ELIGIBLE)


def _withheld(message: str, command: _Command) -> _BatchNotExecutedError:
    """Mark a command fail-fast stopped before its envelope was sent: it was proven not to run."""
    error = _BatchNotExecutedError(message, request_summary=command.request.summary)
    record_withheld(error)
    return error


def _command_success(command: _Command, response: Response, evidence: BatchCommandEvidence) -> BatchSuccess:
    return BatchSuccess._from_response(  # noqa: SLF001 - trusted correlated decoder fast path
        command.index,
        command.stable_key,
        command.request,
        response,
        command.correlation,
        evidence,
    )


__all__: list[str] = []
