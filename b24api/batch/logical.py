"""Canonical arbitrary-length logical batch over bounded physical Bitrix batches."""

# ruff: noqa: SLF001, PLR0912, PLR0915, TRY301 - bounded orchestration state machine

from __future__ import annotations
import asyncio
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Iterable, Iterator
from typing import Protocol, Self, cast, runtime_checkable

from b24api.batch.engine import BatchExecutor, BatchInput, BatchSource
from b24api.batch.outcome import BatchFailure as KernelFailure
from b24api.batch.outcome import BatchSuccess as KernelSuccess
from b24api.batch.stream import _iterate_source, _next_chunk
from b24api.contracts.command import (
    Command,
    CommandFailure,
    CommandNotExecuted,
    CommandOutcome,
    CommandOutcomeUnknown,
    CommandSuccess,
    NotExecutedReason,
)
from b24api.contracts.policy import (
    CompletionAssurance,
    ExecutionPolicy,
    KernelState,
    SnapshotRequirement,
    SnapshotState,
)
from b24api.errors import AmbiguousExecutionError, B24ApiError, BatchCommandError, InputSourceError, ProtocolError
from b24api.execution import (
    AsyncIteratorController,
    Executor,
    await_cancellation_resistant,
    await_cleanup_resistant,
    rearm_cancellation,
)
from b24api.execution.snapshot import KernelReport

type CommandSource[C] = Iterable[Command[C]] | AsyncIterable[Command[C]]


@runtime_checkable
class _SyncClosable(Protocol):
    def close(self) -> None:
        """Close a caller-owned synchronous iterator."""
        ...


@runtime_checkable
class _AsyncClosable(Protocol):
    async def aclose(self) -> None:
        """Close a caller-owned asynchronous iterator."""
        ...


class _SyncCommandAdapter[C](Iterator[tuple[object, object]]):
    def __init__(self, source: Iterable[Command[C]]) -> None:
        self._iterator = iter(source)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> tuple[object, object]:
        command = next(self._iterator)
        if not isinstance(command, Command):
            raise TypeError("batch source must yield Command values")
        return command.request, command.correlation

    def close(self) -> None:
        if isinstance(self._iterator, _SyncClosable):
            self._iterator.close()


class _AsyncCommandAdapter[C](AsyncIterator[tuple[object, object]]):
    def __init__(self, source: AsyncIterable[Command[C]]) -> None:
        self._iterator = aiter(source)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> tuple[object, object]:
        command = await anext(self._iterator)
        if not isinstance(command, Command):
            raise TypeError("batch source must yield Command values")
        return command.request, command.correlation

    async def aclose(self) -> None:
        if isinstance(self._iterator, _AsyncClosable):
            await self._iterator.aclose()


def _adapt_source[C](
    source: CommandSource[C],
) -> Iterable[tuple[object, object]] | AsyncIterable[tuple[object, object]]:
    if isinstance(source, AsyncIterable):
        return _AsyncCommandAdapter(source)
    return _SyncCommandAdapter(source)


class _BatchWindowError(Exception):
    def __init__(self, outcomes: tuple[CommandOutcome[object], ...]) -> None:
        self.outcomes = outcomes
        super().__init__("logical batch physical window failed")


def _public_outcomes(
    outcomes: tuple[KernelSuccess | KernelFailure, ...],
    *,
    halt: bool,
) -> tuple[CommandOutcome[object], ...]:
    converted: list[CommandOutcome[object]] = []
    halted = False
    for outcome in outcomes:
        if isinstance(outcome, KernelSuccess):
            response = outcome.response
            if response is None:
                raise ProtocolError("correlated batch success has no response envelope")
            converted.append(
                CommandSuccess(outcome.command_index, outcome.payload, outcome.request.summary, response),
            )
            continue
        error = outcome.error
        if halt and halted and isinstance(error, ProtocolError):
            converted.append(
                CommandNotExecuted(
                    outcome.command_index,
                    outcome.payload,
                    outcome.request.summary,
                    NotExecutedReason.HALTED,
                ),
            )
            continue
        if isinstance(error, BatchCommandError):
            halted = halt
        if not isinstance(error, B24ApiError):
            error = ProtocolError("batch command failed without a typed safe error")
        if isinstance(error, AmbiguousExecutionError):
            converted.append(
                CommandOutcomeUnknown(outcome.command_index, outcome.payload, outcome.request.summary, error),
            )
        else:
            converted.append(CommandFailure(outcome.command_index, outcome.payload, outcome.request.summary, error))
    return tuple(converted)


class LogicalBatchKernelStream[C]:
    """Kernel-report stream used by the public v2 lifecycle adapter."""

    def __init__(
        self,
        executor: Executor,
        source: CommandSource[C],
        *,
        batch_size: int,
        fail_fast: bool,
        policy: ExecutionPolicy,
    ) -> None:
        """Initialize bounded admission without pulling the source."""
        self._batch_executor = BatchExecutor(executor)
        self._source = cast("BatchSource", _adapt_source(source))
        self._batch_size = batch_size
        self._fail_fast = fail_fast
        self._context = executor.context(policy)
        self._runner: AsyncIterator[CommandOutcome[object]] | None = None
        self._controller: AsyncIteratorController[BatchInput] | None = None
        self._closed = False
        self._emitted = 0
        self._batch_requests = 0
        self._batch_commands = 0
        self.buffered_commands_high_water = 0
        self.report = KernelReport()

    def __aiter__(self) -> Self:
        """Return this asynchronous iterator."""
        return self

    async def __anext__(self) -> CommandOutcome[object]:
        """Return one correlated command outcome."""
        if self._closed:
            raise StopAsyncIteration
        if self._runner is None:
            self._runner = self._run()
        return await anext(self._runner)

    async def aclose(self) -> None:
        """Close input and pending work idempotently."""
        if self._closed:
            return
        self._closed = True
        if self._runner is not None and hasattr(self._runner, "aclose"):
            await cast("_AsyncClosable", self._runner).aclose()
        await self._close_controller()
        if self.report.state is KernelState.NOT_STARTED:
            await self._finalize(KernelState.CANCELLED, "stream closed before exhaustion")

    async def _run(self) -> AsyncGenerator[CommandOutcome[object]]:  # noqa: C901
        await self._context.start()
        controller: AsyncIteratorController[BatchInput] = AsyncIteratorController(
            _iterate_source(self._source),
            input_error="batch input exceeded operation time budget",
            cleanup_error="batch source cleanup exceeded operation time budget",
        )
        self._controller = controller
        next_index = 0
        primary_error: BaseException | None = None
        pending_cancellation: asyncio.CancelledError | None = None
        try:
            while True:
                try:
                    chunk = await _next_chunk(
                        controller,
                        min(
                            self._batch_size,
                            self._context.policy.max_buffered_commands,
                            self._context.policy.max_buffered_rows,
                        ),
                        start_index=next_index,
                        context=self._context,
                    )
                except B24ApiError:
                    raise
                except Exception as error:
                    raise InputSourceError("Logical batch input source failed") from error
                if not chunk.commands:
                    break
                next_index += len(chunk.commands)
                self.buffered_commands_high_water = max(
                    self.buffered_commands_high_water,
                    len(chunk.commands),
                )
                if chunk.source_error is not None:
                    reason = (
                        NotExecutedReason.LOCAL_VALIDATION_FAILED
                        if isinstance(chunk.source_error, TypeError | ValueError)
                        else NotExecutedReason.SOURCE_FAILED
                    )
                    pending = tuple(
                        CommandNotExecuted(command.index, command.payload, command.request.summary, reason)
                        for command in chunk.commands
                    )
                    if self._fail_fast:
                        raise _BatchWindowError(cast("tuple[CommandOutcome[object], ...]", pending))
                    for pending_outcome in pending:
                        self._emitted += 1
                        yield pending_outcome
                    raise InputSourceError("Logical batch input source failed") from chunk.source_error
                self._batch_requests += 1
                self._batch_commands += len(chunk.commands)
                kernel = await self._batch_executor._execute_chunk(
                    chunk.commands,
                    context=self._context,
                    halt=self._fail_fast,
                )
                buffered_rows = sum(
                    outcome.decoded_rows if isinstance(outcome, KernelSuccess) else 1 for outcome in kernel
                )
                outcomes = _public_outcomes(kernel, halt=self._fail_fast)
                await self._context.set_buffered_rows(buffered_rows)
                if self._fail_fast and any(not isinstance(outcome, CommandSuccess) for outcome in outcomes):
                    failed_at = next(
                        index for index, outcome in enumerate(outcomes) if not isinstance(outcome, CommandSuccess)
                    )
                    for successful_outcome in outcomes[:failed_at]:
                        self._emitted += 1
                        yield successful_outcome
                    raise _BatchWindowError(outcomes[failed_at:])
                for batch_outcome in outcomes:
                    self._emitted += 1
                    yield batch_outcome
                await self._context.set_buffered_rows(0)
            await self._finalize(KernelState.COMPLETED, "input exhausted")
        except asyncio.CancelledError as error:
            primary_error = error
            repeated = await await_cancellation_resistant(
                self._finalize(KernelState.CANCELLED, "iteration cancelled"),
            )
            if repeated is not None:
                pending_cancellation = repeated
            raise
        except BaseException as error:
            primary_error = error
            pending_cancellation = await await_cancellation_resistant(
                self._finalize(KernelState.FAILED, type(error).__name__),
            )
            raise
        finally:
            await self._context.set_buffered_rows(0)
            cleanup = await await_cleanup_resistant(self._close_controller())
            if cleanup.error is not None:
                if primary_error is None or isinstance(primary_error, asyncio.CancelledError | GeneratorExit):
                    await self._finalize(KernelState.FAILED, "batch source cleanup failed")
                    rearm_cancellation(cleanup.cancellation)
                    raise cleanup.error
                primary_error.add_note(f"batch source cleanup also failed ({type(cleanup.error).__name__})")
            if cleanup.cancellation is not None:
                if primary_error is None or isinstance(primary_error, asyncio.CancelledError | GeneratorExit):
                    raise cleanup.cancellation
                pending_cancellation = cleanup.cancellation
            self._closed = True
            if primary_error is not None and not isinstance(primary_error, asyncio.CancelledError | GeneratorExit):
                rearm_cancellation(pending_cancellation)

    async def _close_controller(self) -> None:
        controller = self._controller
        if controller is None:
            return
        await controller.aclose(remaining=max(0.0, self._context.policy.max_elapsed - self._context.elapsed))
        self._controller = None

    async def _finalize(self, state: KernelState, reason: str) -> None:
        if self.report.state is not KernelState.NOT_STARTED:
            return
        snapshot = await self._context.snapshot()
        snapshot_state = (
            SnapshotState.NOT_REQUESTED
            if self._context.policy.consistency.snapshot_requirement is SnapshotRequirement.TRAVERSAL_ONLY
            else SnapshotState.UNVERIFIED
        )
        if state is KernelState.COMPLETED and snapshot_state is SnapshotState.UNVERIFIED:
            state = KernelState.INCOMPLETE
            reason = "required snapshot was not verified"
        self.report = KernelReport(
            state=state,
            assurance=CompletionAssurance.CALLER_ASSERTED,
            snapshot=snapshot_state,
            plan_id="batch" if self._fail_fast else "batch_outcomes",
            dispatch_id="batch",
            emitted_rows=self._emitted,
            unique_rows=self._emitted,
            physical_requests=snapshot.counters.physical_requests,
            logical_pages=snapshot.counters.logical_pages,
            batch_requests=self._batch_requests,
            batch_commands=self._batch_commands,
            retries=snapshot.retries,
            cooldown_seconds=snapshot.cooldown_seconds,
            buffered_rows_high_water=snapshot.counters.buffered_rows_high_water,
            terminal_reason=reason,
        )


__all__ = ["CommandSource", "LogicalBatchKernelStream", "_BatchWindowError"]
