"""Bounded independent command fan-out over direct or physical-batch dispatch."""

from __future__ import annotations
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, Self, cast, runtime_checkable

from b24api._stream import MappedOperationStream
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
from b24api.references.facade import _kernel_dispatch
from b24api.references.outcome import ReferenceFailure as KernelFailure
from b24api.references.outcome import ReferenceRequest
from b24api.references.stream import (
    iter_references as _iter_references,
)
from b24api.traversal.plans import SingleResponsePlan

if TYPE_CHECKING:
    from b24api.contracts.dispatch import DispatchSpec
    from b24api.contracts.policy import ExecutionPolicy
    from b24api.contracts.stream import OperationStream
    from b24api.execution.snapshot import KernelReport

type CommandSource[C] = Iterable[Command[C]] | AsyncIterable[Command[C]]
type KernelFanOutEvent = _KernelFanOutSuccess | KernelFailure
type Deregister = Callable[[object], None]


class FanOutKernelStream(AsyncIterator[KernelFanOutEvent], Protocol):
    """Narrow structural view of the independent scheduler output."""

    report: KernelReport
    active_references_high_water: int

    async def aclose(self) -> None:
        """Close scheduler-owned resources."""
        ...


@runtime_checkable
class _SyncClosable(Protocol):
    def close(self) -> None:
        """Close a synchronous iterator."""
        ...


@runtime_checkable
class _AsyncClosable(Protocol):
    async def aclose(self) -> None:
        """Close an asynchronous iterator."""
        ...


@dataclass(frozen=True, slots=True)
class _CommandContext:
    index: int
    correlation: object


class _CommandSourceError(Exception):
    pass


def _reference(command: Command[object], index: int) -> ReferenceRequest:
    if not isinstance(command, Command):
        raise TypeError("fan-out source must yield Command values")
    return ReferenceRequest(
        command.request,
        f"c{index:012d}",
        _CommandContext(index, command.correlation),
    )


class _SyncCommandAdapter[C](Iterator[ReferenceRequest]):
    def __init__(self, source: Iterable[Command[C]]) -> None:
        self._iterator = iter(source)
        self._index = 0

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> ReferenceRequest:
        try:
            command = next(self._iterator)
            reference = _reference(cast("Command[object]", command), self._index)
        except StopIteration:
            raise
        except Exception as error:
            raise _CommandSourceError from error
        self._index += 1
        return reference

    def close(self) -> None:
        if isinstance(self._iterator, _SyncClosable):
            self._iterator.close()


class _AsyncCommandAdapter[C](AsyncIterator[ReferenceRequest]):
    def __init__(self, source: AsyncIterable[Command[C]]) -> None:
        self._iterator = aiter(source)
        self._index = 0

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> ReferenceRequest:
        try:
            command = await anext(self._iterator)
            reference = _reference(cast("Command[object]", command), self._index)
        except StopAsyncIteration:
            raise
        except Exception as error:
            raise _CommandSourceError from error
        self._index += 1
        return reference

    async def aclose(self) -> None:
        if isinstance(self._iterator, _AsyncClosable):
            await self._iterator.aclose()


def _command_source[C](source: CommandSource[C]) -> Iterable[ReferenceRequest] | AsyncIterable[ReferenceRequest]:
    if isinstance(source, AsyncIterable):
        return _AsyncCommandAdapter(source)
    return _SyncCommandAdapter(source)


class _FanOutMapper:
    def __call__(self, event: KernelFanOutEvent) -> CommandOutcome[object]:
        """Convert one independently correlated terminal event."""
        if isinstance(event, _KernelFanOutSuccess):
            context = cast("_CommandContext", event.reference.payload)
            return CommandSuccess(
                context.index,
                context.correlation,
                event.reference.request.summary,
                event.response,
            )
        context = cast("_CommandContext", event.payload)
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
    report: object,
    mapper: _FanOutMapper,
    *,
    tolerant: bool,
) -> BaseException:
    if isinstance(error, _ReferenceWindowError):
        return BatchFailed(_fanout_error_items(error, mapper), report=report)
    if isinstance(error, _CommandSourceError):
        source_error = InputSourceError("Fan-out input source failed")
        return source_error if tolerant else BatchFailed((), report=report)
    return error


def kernel_fanout_stream[C](
    executor: object,
    commands: CommandSource[C],
    *,
    dispatch: DispatchSpec,
    policy: ExecutionPolicy,
    tolerant: bool,
) -> FanOutKernelStream:
    """Build the internal fan-out stream without pulling its source."""
    from b24api.execution import Executor  # noqa: PLC0415 - narrow internal composition import

    if not isinstance(executor, Executor):
        raise TypeError("executor must be an Executor")
    kernel_dispatch = _kernel_dispatch(dispatch, policy)
    stream = _iter_references(
        executor,
        _command_source(commands),
        plan=SingleResponsePlan(
            reject_continuation=False,
            reject_positive_total_over_result=False,
        ),
        dispatch=kernel_dispatch,
        output_order=kernel_dispatch.output_order,
        tolerant=tolerant,
        policy=policy,
        _whole_result=True,
        _emit_response=True,
        _capture_fail_fast=not tolerant,
    )
    return cast("FanOutKernelStream", stream)


def fanout_stream[C](  # noqa: PLR0913
    executor: object,
    commands: CommandSource[C],
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
