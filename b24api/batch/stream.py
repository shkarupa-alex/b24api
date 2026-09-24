"""Bounded fail-fast and total-outcome Bitrix batch execution."""

from __future__ import annotations
from collections.abc import AsyncGenerator, AsyncIterator
from typing import TYPE_CHECKING, Self, cast

from b24api._sources import OwnedSource
from b24api.batch.engine import (
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
)
from b24api.execution.lifecycle import (
    CleanupAttempt,
    LifecycleHooks,
    OperationRunner,
    TerminalCause,
    failed_kernel_report,
    kernel_terminal,
    with_cleanup_attempt,
)
from b24api.execution.snapshot import KernelReport

if TYPE_CHECKING:
    from types import TracebackType


class _BatchOutcomeStream(AsyncIterator[BatchStreamItem]):
    """Internal lazy outcome stream used by exact counted traversal."""

    def __init__(  # noqa: PLR0913
        self,
        batch_executor: BatchExecutor,
        source: BatchSource | OwnedSource[_BatchItem],
        *,
        batch_size: int,
        policy: ExecutionPolicy,
        context: ExecutionContext | None = None,
        logical_page_per_command: bool = False,
    ) -> None:
        """Initialize instance state."""
        self._executor = batch_executor
        self._source = own_batch_source(source)
        self._batch_size = batch_size
        if context is not None and context.policy != policy:
            raise ValueError("shared batch context must use the exact stream policy")
        self._context = context or batch_executor.executor.context(policy)
        self._logical_page_per_command = logical_page_per_command
        self._source_controller: AsyncIteratorController[_BatchItem] | None = None
        self._source_taken = False
        self._started = False
        self._batch_requests = 0
        self._batch_commands = 0
        self._emitted = 0
        self.report = KernelReport()
        self._runner = OperationRunner(
            self._run(),
            LifecycleHooks(finalize=self._finalize, failure_report=_failure_report, cleanup=self._cleanup),
            isolated_pulls=False,
        )

    def __aiter__(self) -> Self:
        """Return this asynchronous iterator."""
        return self

    async def __anext__(self) -> BatchStreamItem:
        """Return the next asynchronous item."""
        return await anext(self._runner)

    async def __aenter__(self) -> Self:
        """Enter without reading an item."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close on exit without replacing the body's primary exception."""
        await self._runner.__aexit__(exc_type, exc, traceback)

    async def aclose(self) -> None:
        """Close owned asynchronous resources; the report is published after cleanup."""
        await self._runner.aclose()

    async def _run(self) -> AsyncGenerator[BatchStreamItem]:
        self._started = True
        await self._context.start()
        source = AsyncIteratorController(
            self._source,
            input_error="batch input exceeded operation time budget",
            cleanup_error="batch source cleanup exceeded operation time budget",
        )
        self._source_controller = source
        self._source_taken = True
        next_index = 0
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
            outcomes = await self._execute(chunk.commands)
            buffered_rows = sum(_batch_outcome_row_weight(outcome) for outcome in outcomes)
            await self._context.set_buffered_rows(buffered_rows)
            for outcome in outcomes:
                outcome_rows = _batch_outcome_row_weight(outcome)
                self._emitted += 1
                yield outcome
                buffered_rows -= outcome_rows
                await self._context.set_buffered_rows(buffered_rows)

    async def _execute(self, commands: tuple[_Command, ...]) -> tuple[BatchStreamItem, ...]:
        """Execute one chunk; a page per command is reserved first and settled by its outcome."""
        reservations = []
        try:
            if self._logical_page_per_command:
                for _ in commands:
                    reservation = await self._context.reserve_page()
                    reservations.append(reservation)
            outcomes = await self._executor._execute_chunk(commands, context=self._context, halt=False)  # noqa: SLF001
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
        return outcomes

    async def _cleanup(self) -> None:
        source = self._source_controller
        if source is None:
            if not self._source_taken:
                # No controller took the source this stream opened (closed before the first pull, or the
                # context failed to start), so the stream closes it itself. OwnedSource.aclose is idempotent.
                (await self._source.aclose()).raise_failure()
            return
        await self._context.set_buffered_rows(0)
        await source.aclose(remaining=max(0.0, self._context.policy.max_elapsed - self._context.elapsed))

    async def _finalize(self, cause: TerminalCause, failure: str | None, attempt: CleanupAttempt) -> KernelReport:
        if not self._started:
            return self.report
        state, reason = kernel_terminal(cause, failure)
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
        self.report = with_cleanup_attempt(self.report, cause, attempt)
        return self.report


def _failure_report(_cause: TerminalCause, reason: str, _attempt: CleanupAttempt) -> KernelReport:
    return failed_kernel_report(reason)


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


def _accept_physical(item: object, _index: int) -> _BatchItem:
    if not isinstance(item, Request | _BatchInput):
        raise TypeError("physical batch source must yield Request values")
    return item


def own_batch_source(source: BatchSource | OwnedSource[_BatchItem]) -> OwnedSource[_BatchItem]:
    """Adopt a raw physical source; a family that already owns its source passes it through."""
    if isinstance(source, OwnedSource):
        return cast("OwnedSource[_BatchItem]", source)
    return OwnedSource.adapt(source, accept=_accept_physical, inline_sequences=True)


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
        request, correlation = (item, None) if isinstance(item, Request) else (item.request, item.correlation)
        commands.append(_Command(index, f"c{index:012d}", request, correlation))
    return _Chunk(tuple(commands))
