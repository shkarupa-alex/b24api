"""Bounded independent command fan-out over direct or physical-batch dispatch."""

from __future__ import annotations
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, cast

from b24api._sources import OwnedSource
from b24api.completion.operation_stream import MappedOperationStream
from b24api.contracts.command import (
    Command,
    CommandFailure,
    CommandNotExecuted,
    CommandOutcome,
    CommandOutcomeUnknown,
    CommandSuccess,
)
from b24api.errors import AmbiguousExecutionError, B24ApiError, BatchFailed, CapabilityError, InputSourceError
from b24api.references.dispatch import (
    _KernelFanOutSuccess,
    _ReferenceWindowError,
)
from b24api.references.dispatch_plan import kernel_dispatch
from b24api.references.outcome import KernelReferenceFailure, ReferenceRequest
from b24api.references.stream import (
    iter_references as _iter_references,
)
from b24api.traversal.plans import SingleResponsePlan

if TYPE_CHECKING:
    from b24api.contracts.dispatch import DispatchSpec
    from b24api.contracts.policy import ExecutionPolicy
    from b24api.contracts.report import OperationReport, Violation
    from b24api.contracts.request import Request
    from b24api.contracts.stream import OperationStream
    from b24api.execution.snapshot import KernelReport

type CommandSource[C] = Iterable[Command[C]] | AsyncIterable[Command[C]]
type KernelFanOutEvent = _KernelFanOutSuccess | KernelReferenceFailure
type Deregister = Callable[[object], None]


class FanOutKernelStream(AsyncIterator[KernelFanOutEvent], Protocol):
    """Narrow structural view of the independent scheduler output."""

    report: KernelReport
    active_references_high_water: int

    async def aclose(self) -> None:
        """Close scheduler-owned resources."""
        ...


@dataclass(frozen=True, slots=True)
class _CommandContext:
    index: int
    correlation: object


class _CommandSourceError(Exception):
    """Carry a failed fan-out input source; reports name the public failure it maps to (``report_cause``)."""

    def __init__(self) -> None:
        super().__init__("fan-out input source failed")
        self.report_cause = InputSourceError("Fan-out input source failed")


def _reference(command: object, index: int) -> ReferenceRequest:
    if not isinstance(command, Command):
        raise TypeError("fan-out source must yield Command values")
    return ReferenceRequest(
        command.request,
        f"c{index:012d}",
        _CommandContext(index, command.correlation),
    )


def command_source[C](
    source: CommandSource[C],
    audit: Callable[[Request], Violation | None] | None = None,
) -> OwnedSource[ReferenceRequest]:
    """Own a fan-out command source; ``audit`` observes each admitted request."""
    observe = None if audit is None else (lambda reference: audit(reference.request))
    return OwnedSource.adapt(
        source,
        accept=_reference,
        observe=observe,
        failure=lambda _error: _CommandSourceError(),
    )


class _FanOutMapper:
    def __call__(self, event: KernelFanOutEvent) -> CommandOutcome[object]:
        """Convert one independently correlated terminal event."""
        if isinstance(event, _KernelFanOutSuccess):
            context = cast("_CommandContext", event.reference.correlation)
            return CommandSuccess(
                context.index,
                context.correlation,
                event.reference.request.summary,
                event.response,
            )
        context = cast("_CommandContext", event.correlation)
        error = event.error if isinstance(event.error, B24ApiError) else CapabilityError("fan-out command failed")
        if isinstance(error, AmbiguousExecutionError):
            return CommandOutcomeUnknown(context.index, context.correlation, event.request.summary, error)
        return CommandFailure(context.index, context.correlation, event.request.summary, error)


def _fanout_variant(outcome: CommandOutcome[object]) -> str:
    if isinstance(outcome, CommandSuccess):
        return "success"
    if isinstance(outcome, CommandFailure):
        return "failure"
    if isinstance(outcome, CommandNotExecuted):
        return "not_executed"
    if isinstance(outcome, CommandOutcomeUnknown):
        return "unknown"
    raise TypeError("fan-out emitted an unknown command outcome")


def _require_success(outcome: CommandOutcome[object]) -> CommandSuccess[object]:
    if not isinstance(outcome, CommandSuccess):
        raise TypeError("fail-fast fan-out kernel emitted a negative outcome")
    return outcome


def _fanout_error_items(
    error: BaseException,
    mapper: _FanOutMapper,
) -> tuple[CommandOutcome[object], ...]:
    if isinstance(error, _ReferenceWindowError):
        return (mapper(error.failure),)
    return ()


def _fanout_error(
    error: BaseException,
    report: OperationReport,
    mapper: _FanOutMapper,
    *,
    tolerant: bool,
) -> BaseException:
    if isinstance(error, _ReferenceWindowError):
        return BatchFailed(_fanout_error_items(error, mapper), report=report)
    if isinstance(error, _CommandSourceError):
        return error.report_cause if tolerant else BatchFailed((), report=report)
    return error


def kernel_fanout_stream[C](
    executor: object,
    commands: CommandSource[C] | OwnedSource[ReferenceRequest],
    *,
    dispatch: DispatchSpec,
    policy: ExecutionPolicy,
    tolerant: bool,
) -> FanOutKernelStream:
    """Build the internal fan-out stream without pulling its source."""
    from b24api.execution import Executor  # noqa: PLC0415 - narrow internal composition import

    if not isinstance(executor, Executor):
        raise TypeError("executor must be an Executor")
    dispatch_plan = kernel_dispatch(dispatch, policy)
    stream = _iter_references(
        executor,
        commands if isinstance(commands, OwnedSource) else command_source(commands),
        plan=SingleResponsePlan(
            reject_continuation=False,
            reject_positive_total_over_result=False,
        ),
        dispatch=dispatch_plan,
        output_order=dispatch_plan.output_order,
        tolerant=tolerant,
        policy=policy,
        _whole_result=True,
        _emit_response=True,
        _capture_fail_fast=not tolerant,
    )
    return cast("FanOutKernelStream", stream)


def fanout_stream[C](  # noqa: PLR0913
    executor: object,
    commands: CommandSource[C] | OwnedSource[ReferenceRequest],
    *,
    dispatch: DispatchSpec,
    policy: ExecutionPolicy,
    tolerant: bool,
    deregister: Deregister,
) -> OperationStream[CommandOutcome[C]]:
    """Compose the public independent command fan-out stream."""
    source = kernel_fanout_stream(
        executor,
        commands,
        dispatch=dispatch,
        policy=policy,
        tolerant=tolerant,
    )
    mapper = _FanOutMapper()
    item_mapper = mapper if tolerant else (lambda event: _require_success(mapper(event)))
    stream = MappedOperationStream(
        source,
        item_mapper,
        operation="fan_out_outcomes" if tolerant else "fan_out",
        classify=_fanout_variant,
        error_mapper=lambda error, report: _fanout_error(error, report, mapper, tolerant=tolerant),
        error_items=lambda error: _fanout_error_items(error, mapper),
        source_active_references=lambda: source.active_references_high_water,
        deregister=deregister,
    )
    return cast("OperationStream[CommandOutcome[C]]", stream)


__all__ = [
    "CommandSource",
    "FanOutKernelStream",
    "KernelFanOutEvent",
    "_FanOutMapper",
    "_fanout_error",
    "_fanout_error_items",
    "_fanout_variant",
    "fanout_stream",
    "kernel_fanout_stream",
]
