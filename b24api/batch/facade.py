"""Public logical-batch composition over the bounded batch kernel."""

from __future__ import annotations
from collections.abc import AsyncIterable, Callable, Iterable
from typing import TYPE_CHECKING, cast

from b24api._stream import MappedOperationStream
from b24api.batch.logical import LogicalBatchKernelStream, _BatchWindowError
from b24api.contracts.command import (
    Command,
    CommandFailure,
    CommandNotExecuted,
    CommandOutcome,
    CommandOutcomeUnknown,
    CommandSuccess,
)
from b24api.errors import BatchFailed, InputSourceError

if TYPE_CHECKING:
    from b24api.contracts.policy import ExecutionPolicy
    from b24api.contracts.report import OperationReport
    from b24api.contracts.stream import OperationStream
    from b24api.execution.executor import Executor

type CommandSource[C] = Iterable[Command[C]] | AsyncIterable[Command[C]]
type Deregister = Callable[[object], None]


def _outcome_variant(outcome: CommandOutcome[object]) -> str:
    if isinstance(outcome, CommandSuccess):
        return "success"
    if isinstance(outcome, CommandFailure):
        return "failure"
    if isinstance(outcome, CommandNotExecuted):
        return "not_executed"
    if isinstance(outcome, CommandOutcomeUnknown):
        return "unknown"
    raise TypeError("batch stream emitted an unknown command outcome")


def _require_success(outcome: CommandOutcome[object]) -> CommandSuccess[object]:
    if not isinstance(outcome, CommandSuccess):
        raise TypeError("fail-fast batch kernel emitted a negative outcome")
    return outcome


def _error_items(error: BaseException) -> tuple[CommandOutcome[object], ...]:
    if isinstance(error, _BatchWindowError):
        return error.outcomes
    return ()


def _fail_fast_error(error: BaseException, report: OperationReport) -> BaseException:
    if isinstance(error, _BatchWindowError):
        return BatchFailed(error.outcomes, report=report)
    if isinstance(error, InputSourceError):
        return BatchFailed((), report=report)
    return error


def resolve_batch_size(batch_size: int | None, policy: ExecutionPolicy) -> int:
    """Resolve a physical chunk size without changing the logical source limit."""
    ceiling = min(50, policy.max_buffered_commands)
    size = ceiling if batch_size is None else batch_size
    if not isinstance(size, int) or isinstance(size, bool) or not 1 <= size <= ceiling:
        raise ValueError("batch_size must be within 1..50 and the command buffer ceiling")
    return size


def batch_stream[C](
    executor: Executor,
    commands: CommandSource[C],
    *,
    batch_size: int | None,
    policy: ExecutionPolicy,
    deregister: Deregister,
) -> OperationStream[CommandSuccess[C]]:
    """Compose the public fail-fast logical batch stream."""
    source = LogicalBatchKernelStream(
        executor,
        commands,
        batch_size=resolve_batch_size(batch_size, policy),
        fail_fast=True,
        policy=policy,
    )
    stream = MappedOperationStream(
        source,
        _require_success,
        operation="batch",
        classify=_outcome_variant,
        error_mapper=_fail_fast_error,
        error_items=_error_items,
        source_admitted=lambda: source.admitted,
        source_buffered_commands=lambda: source.buffered_commands_high_water,
        deregister=deregister,
    )
    return cast("OperationStream[CommandSuccess[C]]", stream)


def batch_outcome_stream[C](
    executor: Executor,
    commands: CommandSource[C],
    *,
    batch_size: int | None,
    policy: ExecutionPolicy,
    deregister: Deregister,
) -> OperationStream[CommandOutcome[C]]:
    """Compose the public total-outcome logical batch stream."""
    source = LogicalBatchKernelStream(
        executor,
        commands,
        batch_size=resolve_batch_size(batch_size, policy),
        fail_fast=False,
        policy=policy,
    )
    stream = MappedOperationStream(
        source,
        lambda outcome: outcome,
        operation="batch_outcomes",
        classify=_outcome_variant,
        error_items=_error_items,
        source_admitted=lambda: source.admitted,
        source_buffered_commands=lambda: source.buffered_commands_high_water,
        deregister=deregister,
    )
    return cast("OperationStream[CommandOutcome[C]]", stream)


__all__ = ["CommandSource", "batch_outcome_stream", "batch_stream", "resolve_batch_size"]
