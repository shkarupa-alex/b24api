"""Correlated per-command outcomes shared by the classic and the native v3 batch dialects."""

from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING

from b24api.batch.outcome import BatchCommandEvidence, BatchFailure, BatchSuccess
from b24api.contracts.policy import ReplayDisposition
from b24api.contracts.request import ReplaySafety, Request
from b24api.errors import B24ApiError, ProtocolError, TransportError

if TYPE_CHECKING:
    from b24api.contracts.response import Response


class _BatchNotExecutedError(ProtocolError):
    """Internal correlated marker for a fail-fast window stopped before dispatch."""


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
