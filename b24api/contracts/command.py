"""Logical batch commands and closed correlated terminal outcomes."""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

from b24api.models import JsonValue, Request, RequestSummary, Response

if TYPE_CHECKING:
    from collections.abc import Iterable

    from b24api.error import B24ApiError


@dataclass(frozen=True, slots=True)
class Command[C]:
    """One request plus opaque caller-owned off-wire correlation."""

    request: Request
    correlation: C = field(repr=False)

    def __post_init__(self) -> None:
        """Reject non-canonical requests before source admission."""
        if not isinstance(self.request, Request):
            raise TypeError("command request must be a Request")


class NotExecutedReason(StrEnum):
    """Closed reasons proving that an admitted command was not dispatched."""

    HALTED = "halted"
    SOURCE_FAILED = "source_failed"
    LOCAL_VALIDATION_FAILED = "local_validation_failed"
    SCHEDULER_STOPPED = "scheduler_stopped"


@dataclass(frozen=True, slots=True)
class CommandSuccess[C]:
    """A successful response correlated to one logical command."""

    index: int
    correlation: C = field(repr=False)
    request_summary: RequestSummary
    response: Response = field(repr=False)

    @property
    def result(self) -> JsonValue:
        """Return a detached decoded result."""
        return self.response.result


@dataclass(frozen=True, slots=True)
class CommandFailure[C]:
    """A conclusive command failure."""

    index: int
    correlation: C = field(repr=False)
    request_summary: RequestSummary
    error: B24ApiError


@dataclass(frozen=True, slots=True)
class CommandNotExecuted[C]:
    """An admitted command proven not to have executed."""

    index: int
    correlation: C = field(repr=False)
    request_summary: RequestSummary
    reason: NotExecutedReason


@dataclass(frozen=True, slots=True)
class CommandOutcomeUnknown[C]:
    """A command whose server acceptance cannot be excluded."""

    index: int
    correlation: C = field(repr=False)
    request_summary: RequestSummary
    error: B24ApiError


type CommandOutcome[C] = CommandSuccess[C] | CommandFailure[C] | CommandNotExecuted[C] | CommandOutcomeUnknown[C]


@dataclass(frozen=True, slots=True)
class CommandOutcomeBuckets[C]:
    """Finite pure partition of every command outcome variant."""

    successes: tuple[CommandSuccess[C], ...]
    failures: tuple[CommandFailure[C], ...]
    not_executed: tuple[CommandNotExecuted[C], ...]
    unknown: tuple[CommandOutcomeUnknown[C], ...]


def partition_command_outcomes[C](outcomes: Iterable[CommandOutcome[C]]) -> CommandOutcomeBuckets[C]:
    """Partition a finite iterable without dropping negative states."""
    successes: list[CommandSuccess[C]] = []
    failures: list[CommandFailure[C]] = []
    not_executed: list[CommandNotExecuted[C]] = []
    unknown: list[CommandOutcomeUnknown[C]] = []
    for outcome in outcomes:
        if isinstance(outcome, CommandSuccess):
            successes.append(outcome)
        elif isinstance(outcome, CommandFailure):
            failures.append(outcome)
        elif isinstance(outcome, CommandNotExecuted):
            not_executed.append(outcome)
        elif isinstance(outcome, CommandOutcomeUnknown):
            unknown.append(outcome)
        else:
            raise TypeError("outcomes must contain only CommandOutcome values")
    return CommandOutcomeBuckets(tuple(successes), tuple(failures), tuple(not_executed), tuple(unknown))


__all__ = [
    "Command",
    "CommandFailure",
    "CommandNotExecuted",
    "CommandOutcome",
    "CommandOutcomeBuckets",
    "CommandOutcomeUnknown",
    "CommandSuccess",
    "NotExecutedReason",
    "partition_command_outcomes",
]
