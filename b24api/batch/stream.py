"""Bounded fail-fast and total-outcome Bitrix batch execution."""

from __future__ import annotations
import asyncio
import contextlib
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Iterator
from typing import Protocol, Self, cast, runtime_checkable

from b24api.batch.engine import (
    _MISSING,
    _SYNC_EXHAUSTED,
    BatchExecutor,
    BatchSource,
    BatchStreamItem,
    _batch_outcome_row_weight,
    _BatchInput,
    _BatchItem,
    _Chunk,
    _Command,
    _raise_source_error,
)
from b24api.batch.outcome import BatchSuccess
from b24api.contracts.policy import (
    CompletionAssurance,
    ExecutionPolicy,
    KernelState,
    SnapshotRequirement,
    SnapshotState,
)
from b24api.contracts.report import Violation, ViolationSeverity
from b24api.contracts.request import Request
from b24api.execution import (
    AsyncIteratorController,
    ExecutionContext,
    await_cancellation_resistant,
    await_cleanup_resistant,
    rearm_cancellation,
)
from b24api.execution.failure import (
    CLEANUP_FAILED_REASON,
    EARLY_CLOSE_REASON,
    report_reason,
    with_cleanup_failure,
)
from b24api.execution.failure import attach_report as _attach_report
from b24api.execution.snapshot import KernelReport


@runtime_checkable
class _AsyncClosable(Protocol):
    async def aclose(self) -> None: ...


@runtime_checkable
class _SyncClosable(Protocol):
    def close(self) -> None: ...


class _BatchOutcomeStream(AsyncIterator[BatchStreamItem]):
    """Internal lazy outcome stream used by exact counted traversal."""

    def __init__(  # noqa: PLR0913
        self,
        batch_executor: BatchExecutor,
        source: BatchSource,
        *,
        batch_size: int,
        policy: ExecutionPolicy,
        context: ExecutionContext | None = None,
        logical_page_per_command: bool = False,
    ) -> None:
        """Initialize instance state."""
        self._executor = batch_executor
        self._source = source
        self._batch_size = batch_size
        if context is not None and context.policy != policy:
            raise ValueError("shared batch context must use the exact stream policy")
        self._context = context or batch_executor.executor.context(policy)
        self._logical_page_per_command = logical_page_per_command
        self._runner: AsyncGenerator[BatchStreamItem] | None = None
        self._source_controller: AsyncIteratorController[_BatchItem] | None = None
        self._prefetched: BatchStreamItem | object = _MISSING
        self._closed = False
        self._batch_requests = 0
        self._batch_commands = 0
        self._emitted = 0
        self.report = KernelReport()

    def __aiter__(self) -> Self:
        """Return this asynchronous iterator."""
        return self

    async def __anext__(self) -> BatchStreamItem:
        """Return the next asynchronous item."""
        if self._closed:
            raise StopAsyncIteration
        if self._prefetched is not _MISSING:
            item = cast("BatchStreamItem", self._prefetched)
            self._prefetched = _MISSING
            self._emitted += 1
            return item
        if self._runner is None:
            self._runner = self._run()
        item = await anext(self._runner)
        self._emitted += 1
        return item

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        if self._closed:
            raise RuntimeError("stream is closed")
        if self._runner is None:
            self._runner = self._run()
            with contextlib.suppress(StopAsyncIteration):
                self._prefetched = await anext(self._runner)
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Exit the asynchronous context."""
        await self.aclose()

    async def aclose(self) -> None:
        """Close owned asynchronous resources."""
        if self._closed:
            await self._observe_source_cleanup()
            return
        self._closed = True
        try:
            if self._runner is not None:
                await self._runner.aclose()
        except BaseException as error:
            if self.report.state is KernelState.NOT_STARTED:
                cancellation = await await_cancellation_resistant(
                    self._finalize(KernelState.CANCELLED, "stream cleanup failed"),
                )
                if cancellation is not None:
                    _attach_report(cancellation, self.report)
                    raise cancellation from error
            _attach_report(error, self.report)
            raise
        finally:
            self._prefetched = _MISSING
        await self._observe_source_cleanup()
        if self.report.state is KernelState.NOT_STARTED and self._runner is not None:
            await self._finalize(KernelState.CANCELLED, EARLY_CLOSE_REASON)

    async def _run(self) -> AsyncGenerator[BatchStreamItem]:  # noqa: C901, PLR0912, PLR0915
        await self._context.start()
        source = AsyncIteratorController(
            _iterate_source(self._source),
            input_error="batch input exceeded operation time budget",
            cleanup_error="batch source cleanup exceeded operation time budget",
        )
        self._source_controller = source
        next_index = 0
        naturally_exhausted = False
        primary_error: BaseException | None = None
        pending_cancellation: asyncio.CancelledError | None = None
        try:
            while True:
                chunk = await _next_chunk(
                    source,
                    min(
                        self._batch_size,
                        self._context.policy.max_buffered_commands,
                        self._context.policy.max_buffered_rows,
                    ),
                    start_index=next_index,
                    context=self._context,
                )
                if not chunk.commands:
                    break
                next_index += len(chunk.commands)
                if chunk.source_error is not None:
                    _raise_source_error(chunk.source_error)
                if self._executor._will_dispatch_commands(chunk.commands, halt=False):  # noqa: SLF001
                    self._batch_requests += 1
                self._batch_commands += len(chunk.commands)
                reservations = []
                try:
                    if self._logical_page_per_command:
                        for _ in chunk.commands:
                            reservation = await self._context.reserve_page()
                            reservations.append(reservation)
                    outcomes = await self._executor._execute_chunk(  # noqa: SLF001
                        chunk.commands,
                        context=self._context,
                        halt=False,
                    )
                except BaseException:
                    for reservation in reservations:
                        self._context.release_page(reservation)
                    raise
                if reservations:
                    for reservation, outcome in zip(reservations, outcomes, strict=True):
                        if isinstance(outcome, BatchSuccess):
                            self._context.commit_page(reservation)
                        else:
                            self._context.release_page(reservation)
                buffered_rows = sum(_batch_outcome_row_weight(outcome) for outcome in outcomes)
                await self._context.set_buffered_rows(buffered_rows)
                for outcome in outcomes:
                    outcome_rows = _batch_outcome_row_weight(outcome)
                    yield outcome
                    buffered_rows -= outcome_rows
                    await self._context.set_buffered_rows(buffered_rows)
            naturally_exhausted = True
            await self._finalize(KernelState.COMPLETED, "input exhausted")
        except asyncio.CancelledError as error:
            primary_error = error
            repeated = await await_cancellation_resistant(
                self._finalize(KernelState.CANCELLED, "iteration cancelled"),
            )
            if repeated is not None:
                primary_error = repeated
                _attach_report(repeated, self.report)
                raise repeated from error
            _attach_report(error, self.report)
            raise
        except GeneratorExit as error:
            primary_error = error
            cancellation = await await_cancellation_resistant(
                self._finalize(KernelState.CANCELLED, EARLY_CLOSE_REASON),
            )
            if cancellation is not None:
                primary_error = cancellation
                _attach_report(cancellation, self.report)
                raise cancellation from error
            _attach_report(error, self.report)
            raise
        except BaseException as error:
            primary_error = error
            cancellation = await await_cancellation_resistant(
                self._finalize(KernelState.FAILED, report_reason(error)),
            )
            if cancellation is not None:
                _attach_report(cancellation, self.report)
                pending_cancellation = cancellation
            _attach_report(error, self.report)
            raise
        finally:
            cleanup = await await_cleanup_resistant(self._cleanup_source(source))
            if cleanup.error is not None:
                cleanup_error = cleanup.error
                if primary_error is None or isinstance(primary_error, asyncio.CancelledError | GeneratorExit):
                    await self._record_terminal_cleanup_failure(cleanup_error)
                    pending = cleanup.cancellation
                    if pending is None and isinstance(primary_error, asyncio.CancelledError):
                        pending = primary_error
                    rearm_cancellation(pending)
                    raise cleanup_error
                self._record_cleanup_failure(cleanup_error, primary_error)
                pending_cancellation = cleanup.cancellation
                if pending_cancellation is None and isinstance(cleanup_error, asyncio.CancelledError):
                    pending_cancellation = cleanup_error
            elif cleanup.cancellation is not None and (
                primary_error is None or isinstance(primary_error, asyncio.CancelledError | GeneratorExit)
            ):
                _attach_report(cleanup.cancellation, self.report)
                raise cleanup.cancellation
            elif cleanup.cancellation is not None and primary_error is not None:
                self._record_cleanup_failure(cleanup.cancellation, primary_error)
                pending_cancellation = cleanup.cancellation
            if not naturally_exhausted and self.report.state is KernelState.NOT_STARTED:
                await self._finalize(KernelState.CANCELLED, "stream abandoned")
            if self.report.state is not KernelState.NOT_STARTED:
                self._closed = True
            if primary_error is not None and not isinstance(primary_error, asyncio.CancelledError | GeneratorExit):
                rearm_cancellation(pending_cancellation)

    async def _cleanup_source(self, source: AsyncIteratorController[_BatchItem]) -> None:
        await self._context.set_buffered_rows(0)
        await source.aclose(
            remaining=max(0.0, self._context.policy.max_elapsed - self._context.elapsed),
        )

    async def _observe_source_cleanup(self) -> None:
        controller = self._source_controller
        if controller is None:
            return
        try:
            await self._cleanup_source(controller)
        except BaseException as error:
            if self.report.state is not KernelState.FAILED:
                await self._record_terminal_cleanup_failure(error)
            else:
                _attach_report(error, self.report)
            raise

    def _record_cleanup_failure(self, error: BaseException, primary_error: BaseException) -> None:
        self.report = with_cleanup_failure(self.report, error, terminal=False)
        _attach_report(primary_error, self.report)

    async def _record_terminal_cleanup_failure(self, error: BaseException) -> None:
        if self.report.state is KernelState.NOT_STARTED:
            await self._finalize(KernelState.FAILED, CLEANUP_FAILED_REASON)
        self.report = with_cleanup_failure(self.report, error, terminal=True)
        _attach_report(error, self.report)

    async def _finalize(self, state: KernelState, reason: str) -> None:
        if self.report.state is not KernelState.NOT_STARTED:
            return
        snapshot = await self._context.snapshot()
        consistency = self._context.policy.consistency
        snapshot_state = (
            SnapshotState.NOT_REQUESTED
            if consistency.snapshot_requirement is SnapshotRequirement.TRAVERSAL_ONLY
            else SnapshotState.UNVERIFIED
        )
        violations: tuple[Violation, ...] = ()
        if state is KernelState.COMPLETED and snapshot_state is SnapshotState.UNVERIFIED:
            state = KernelState.INCOMPLETE
            reason = "required snapshot was not verified"
            violations = (
                Violation(
                    severity=ViolationSeverity.BLOCKING,
                    code="snapshot_unverified",
                    message="the requested stable snapshot was not verified",
                ),
            )
        self.report = KernelReport(
            state=state,
            assurance=CompletionAssurance.CALLER_ASSERTED,
            snapshot=snapshot_state,
            plan_id="batch_kernel",
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
            violations=violations,
            terminal_reason=reason,
        )


def batch_outcome_stream(
    engine: BatchExecutor,
    requests: BatchSource,
    *,
    batch_size: int | None = None,
    policy: ExecutionPolicy | None = None,
) -> _BatchOutcomeStream:
    """Build the internal total-outcome stream that kernel tests drive directly."""
    size = engine.portal_command_cap if batch_size is None else batch_size
    if isinstance(size, bool) or not 1 <= size <= engine.portal_command_cap:
        raise ValueError("batch_size must be within the portal command cap")
    return _BatchOutcomeStream(engine, requests, batch_size=size, policy=policy or ExecutionPolicy())


async def _iterate_source(source: BatchSource) -> AsyncGenerator[_BatchItem]:
    if isinstance(source, AsyncIterable):
        iterator = aiter(source)
        try:
            async for item in iterator:
                yield item
        finally:
            if isinstance(iterator, _AsyncClosable):
                await iterator.aclose()
        return
    if source.__class__ is list or source.__class__ is tuple:
        for item in source:
            yield item
        return
    sync_iterator = iter(source)
    try:
        while True:
            sync_item = await _next_sync_owned(sync_iterator)
            if sync_item is _SYNC_EXHAUSTED:
                return
            yield cast("_BatchItem", sync_item)
    finally:
        if isinstance(sync_iterator, _SyncClosable):
            await _close_sync_owned(sync_iterator)


def _next_sync(iterator: Iterator[_BatchItem]) -> _BatchItem | object:
    try:
        return next(iterator)
    except StopIteration:
        return _SYNC_EXHAUSTED


async def _next_sync_owned(iterator: Iterator[_BatchItem]) -> _BatchItem | object:
    pull = asyncio.create_task(asyncio.to_thread(_next_sync, iterator))
    try:
        return await asyncio.shield(pull)
    except asyncio.CancelledError:
        with contextlib.suppress(BaseException):
            await pull
        raise


async def _close_sync_owned(iterator: _SyncClosable) -> None:
    close = asyncio.create_task(asyncio.to_thread(iterator.close))
    try:
        await asyncio.shield(close)
    except asyncio.CancelledError:
        with contextlib.suppress(BaseException):
            await close
        raise


async def _next_chunk(
    source: AsyncIteratorController[_BatchItem],
    size: int,
    *,
    start_index: int,
    context: ExecutionContext,
) -> _Chunk:
    commands: list[_Command] = []
    for offset in range(size):
        try:
            item = await source.get(context)
        except StopAsyncIteration:
            break
        except Exception as error:
            if not commands:
                raise
            return _Chunk(tuple(commands), error)
        index = start_index + offset
        if isinstance(item, Request):
            request, correlation = item, None
        elif isinstance(item, _BatchInput):
            request, correlation = item.request, item.correlation
        else:
            if not commands:
                raise TypeError("physical batch source must yield Request values")
            return _Chunk(tuple(commands), TypeError("physical batch source must yield Request values"))
        commands.append(_Command(index, f"c{index:012d}", request, correlation))
    return _Chunk(tuple(commands))
