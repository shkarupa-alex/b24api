"""Canonical arbitrary-length logical batch over bounded physical Bitrix batches."""

from __future__ import annotations
from collections.abc import AsyncGenerator, AsyncIterable, Iterable
from typing import TYPE_CHECKING, Self, cast

from b24api._sources import OwnedSource
from b24api.batch.engine import (
    BatchExecutor,
    BatchProgress,
    _batch_outcome_row_weight,
    _BatchInput,
    _BatchItem,
    _Chunk,
)
from b24api.batch.outcome import BatchFailure as KernelFailure
from b24api.batch.outcome import BatchSuccess as KernelSuccess
from b24api.batch.stream import _next_chunk, _within_row_budget
from b24api.completion.reference_recorder import ReferenceCompletionRecorder
from b24api.contracts.command import (
    Command,
    CommandFailure,
    CommandNotExecuted,
    CommandOutcome,
    CommandOutcomeUnknown,
    CommandSuccess,
    NotExecutedReason,
)
from b24api.contracts.completion import BindingClosure, CleanupState, CommandSettlement, StreamClosure
from b24api.contracts.policy import (
    CompletionAssurance,
    ExecutionPolicy,
    KernelState,
    SnapshotRequirement,
    SnapshotState,
)
from b24api.contracts.request import ReplaySafety
from b24api.errors import (
    InputSourceError,
    ProtocolError,
)
from b24api.execution import (
    AsyncIteratorController,
    Executor,
)
from b24api.execution.facts import Execution, ambiguity_of, execution, settlement
from b24api.execution.lifecycle import (
    CleanupAttempt,
    LifecycleHooks,
    OperationRunner,
    TerminalCause,
    cleanup_failed,
    failed_kernel_report,
    kernel_terminal,
    with_cleanup_attempt,
)
from b24api.execution.snapshot import KernelReport

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.contracts.report import Violation
    from b24api.contracts.request import Request
    from b24api.execution.context import ExecutionContext

type CommandSource[C] = Iterable[Command[C]] | AsyncIterable[Command[C]]


def _accept_command(command: object, _index: int) -> _BatchItem:
    if not isinstance(command, Command):
        raise TypeError("batch source must yield Command values")
    return _BatchInput(command.request, command.correlation)


def command_source[C](
    commands: CommandSource[C],
    audit: Callable[[Request], Violation | None] | None = None,
) -> OwnedSource[_BatchItem]:
    """Own a logical command source; ``audit`` observes each admitted request."""
    observe = None if audit is None else (lambda item: audit(cast("_BatchInput", item).request))
    return OwnedSource.adapt(commands, accept=_accept_command, observe=observe, failure=_source_failure)


def _source_failure(_error: Exception) -> InputSourceError:
    """Map anything the caller's source raised, its own ``B24ApiError`` included, to the source's failure."""
    return InputSourceError("Logical batch input source failed")


class _BatchWindowError(Exception):
    """Carry the failed fail-fast window; reports name the public failure it carries (``report_cause``)."""

    report_name = "BatchFailed"

    def __init__(
        self,
        outcomes: tuple[CommandOutcome[object], ...],
        *,
        source_error: Exception | None = None,
    ) -> None:
        self.outcomes = outcomes
        errors = (getattr(outcome, "error", None) for outcome in outcomes)
        carried = source_error or next((error for error in errors if isinstance(error, BaseException)), None)
        self.report_cause: BaseException = carried if carried is not None else self
        super().__init__("logical batch physical window failed")


def _public_outcomes(outcomes: tuple[KernelSuccess | KernelFailure, ...]) -> tuple[CommandOutcome[object], ...]:
    converted: list[CommandOutcome[object]] = []
    for outcome in outcomes:
        if isinstance(outcome, KernelSuccess):
            response = outcome.response
            if response is None:
                raise ProtocolError("correlated batch success has no response envelope")
            converted.append(
                CommandSuccess(outcome.command_index, outcome.correlation, outcome.request.summary, response),
            )
            continue
        error = outcome.error
        # The variant follows the execution facts; only the physical decoder proves a command withheld.
        summary = outcome.request.summary
        ambiguity = ambiguity_of(
            error, request_summary=summary, declared_unsafe=outcome.replay_safety is ReplaySafety.UNSAFE
        )
        if execution(error) is Execution.WITHHELD:
            converted.append(
                CommandNotExecuted(outcome.command_index, outcome.correlation, summary, NotExecutedReason.HALTED)
            )
        elif ambiguity is not None:
            converted.append(
                CommandOutcomeUnknown(
                    outcome.command_index, outcome.correlation, summary, ambiguity, outcome.replay_disposition
                )
            )
        else:
            converted.append(
                CommandFailure(outcome.command_index, outcome.correlation, summary, error, outcome.replay_disposition)
            )
    return tuple(converted)


class LogicalBatchKernelStream[C]:
    """Kernel-report stream used by the public v2 lifecycle adapter."""

    # Commands are pulled and sent inside a read; nothing runs between reads.
    works_between_reads = False

    def __init__(
        self,
        executor: Executor,
        source: CommandSource[C] | OwnedSource[_BatchItem],
        *,
        batch_size: int,
        fail_fast: bool,
        policy: ExecutionPolicy,
    ) -> None:
        """Initialize bounded admission without pulling the source."""
        self._batch_executor = BatchExecutor(executor)
        self._source = source if isinstance(source, OwnedSource) else command_source(source)
        self._batch_size = batch_size
        self._fail_fast = fail_fast
        self._context = executor.context(policy)
        self._completion = ReferenceCompletionRecorder()
        # Owns the source from the start, so even a close before the first pull is bounded by the cleanup window;
        # ``None`` once the one close attempt has been made.
        self._controller: AsyncIteratorController[_BatchItem] | None = AsyncIteratorController(
            self._source,
            input_error="batch input exceeded operation time budget",
            cleanup_error="batch source cleanup exceeded operation time budget",
        )
        self._runner = OperationRunner(
            self._run(),
            LifecycleHooks(finalize=self._finalize, failure_report=self._failure_report, cleanup=self._cleanup),
            isolated_pulls=False,
        )
        self._emitted = 0
        # The chunk being sent or delivered, where each of its commands stands, and the indexes delivered so far:
        # what an end before its last outcome leaves undelivered (``_undelivered``).
        self._chunk: tuple[tuple[int, ...], BatchProgress] | None = None
        self._delivered: set[int] = set()
        self._batch_requests = 0
        self._batch_commands = 0
        self.admitted = 0
        self.buffered_commands_high_water = 0
        self.report = KernelReport()

    def __aiter__(self) -> Self:
        """Return this asynchronous iterator."""
        return self

    @property
    def operation_context(self) -> ExecutionContext:
        """Return the operation context whose elapsed budget bounds the wrapper's cleanup."""
        return self._context

    @property
    def cleanup_failure(self) -> BaseException | None:
        """Return the cleanup failure this stream raised as its end (``OperationRunner.cleanup_failure``)."""
        return self._runner.cleanup_failure

    @property
    def completion_gate(self) -> object:
        """Expose the live logical-command completion gate."""
        return self._completion.gate

    @property
    def source_violations(self) -> tuple[Violation, ...]:
        """Return the bounded violations observed while admitting source commands."""
        return self._source.violations

    def _settle_outcome(self, outcome: CommandOutcome[object]) -> None:
        binding = self._completion.binding(outcome.index)
        if isinstance(outcome, CommandSuccess):
            binding.settled(CommandSettlement.SUCCESS)
            binding.validated(1)
        elif isinstance(outcome, CommandNotExecuted):
            binding.settled(CommandSettlement.NOT_EXECUTED)
        else:
            binding.settled(settlement(outcome.error))

    def _hand_over(self, outcome: CommandOutcome[object]) -> None:
        """Settle an outcome and count it delivered: the consumer holds it once it is yielded, whatever it does next."""
        self._settle_outcome(outcome)
        self._emitted += 1
        self._delivered.add(outcome.index)

    def _finish_outcome(self, outcome: CommandOutcome[object]) -> None:
        binding = self._completion.binding(outcome.index)
        if isinstance(outcome, CommandSuccess):
            binding.deliver_current()
            closure = BindingClosure.SINGLE_RESPONSE
        elif isinstance(outcome, CommandOutcomeUnknown):
            closure = BindingClosure.UNKNOWN
        else:
            closure = BindingClosure.FAILURE
        self._completion.terminal(outcome.index, closure)

    def _undelivered(self) -> tuple[int, int]:
        """Count the commands of the current chunk without a delivered outcome: possibly run, and never sent."""
        if self._chunk is None:
            return 0, 0
        indexes, progress = self._chunk
        executions = [progress.execution(index) for index in indexes if index not in self._delivered]
        possibly_run = sum(each is Execution.POSSIBLY_RUN for each in executions)
        return possibly_run, sum(not each.dispatched for each in executions)

    async def __anext__(self) -> CommandOutcome[object]:
        """Return one correlated command outcome."""
        return await anext(self._runner)

    async def aclose(self) -> None:
        """Close input and pending work idempotently; the report is published after cleanup."""
        await self._runner.aclose()

    def _count_batch_request(self) -> None:
        """Count one physical batch envelope the chunk sends in its first round."""
        self._batch_requests += 1

    async def _run(self) -> AsyncGenerator[CommandOutcome[object]]:  # noqa: C901
        await self._context.start()
        controller = self._controller
        if controller is None:
            raise RuntimeError("logical batch source is already closed")
        next_index = 0
        while True:
            chunk = await self._read_chunk(controller, next_index)
            if not chunk.commands:
                # The source has ended. The operation ends with its budget if the budget refused work, as a reference
                # operation's does; a deadline that passed with nothing left to admit refused none.
                self._context.raise_if_refused()
                break
            next_index += len(chunk.commands)
            self.admitted += len(chunk.commands)
            for command in chunk.commands:
                self._completion.admit(command.index)
            self.buffered_commands_high_water = max(
                self.buffered_commands_high_water,
                len(chunk.commands),
            )
            if (failure := chunk.source_error) is not None:
                reason = (
                    NotExecutedReason.LOCAL_VALIDATION_FAILED
                    if failure.__cause__ is not None and failure.__cause__ is self._source.rejected
                    else NotExecutedReason.SOURCE_FAILED
                )
                pending = tuple(
                    CommandNotExecuted(command.index, command.correlation, command.request.summary, reason)
                    for command in chunk.commands
                )
                if self._fail_fast:
                    raise _BatchWindowError(cast("tuple[CommandOutcome[object], ...]", pending), source_error=failure)
                for pending_outcome in pending:
                    self._emitted += 1
                    yield pending_outcome
                    self._completion.terminal(pending_outcome.index, BindingClosure.FAILURE)
                raise failure
            for command in chunk.commands:
                self._completion.binding(command.index).scheduled()
            self._batch_commands += len(chunk.commands)
            progress = BatchProgress()
            self._chunk, self._delivered = (tuple(command.index for command in chunk.commands), progress), set()
            kernel = await self._batch_executor.execute_chunk(
                chunk.commands,
                context=self._context,
                halt=self._fail_fast,
                on_envelope=self._count_batch_request,
                split_held=True,
                progress=progress,
            )
            kernel = _within_row_budget(kernel, self._context.policy.max_buffered_rows)
            await self._context.set_buffered_rows(sum(_batch_outcome_row_weight(outcome) for outcome in kernel))
            outcomes = _public_outcomes(kernel)
            if self._fail_fast and any(not isinstance(outcome, CommandSuccess) for outcome in outcomes):
                failed_at = next(
                    index for index, outcome in enumerate(outcomes) if not isinstance(outcome, CommandSuccess)
                )
                for successful_outcome in outcomes[:failed_at]:
                    self._hand_over(successful_outcome)
                    yield successful_outcome
                    self._finish_outcome(successful_outcome)
                raise _BatchWindowError(outcomes[failed_at:])
            for batch_outcome in outcomes:
                self._hand_over(batch_outcome)
                yield batch_outcome
                self._finish_outcome(batch_outcome)
            await self._context.set_buffered_rows(0)
            self._chunk = None
            if chunk.refusal is not None:
                raise chunk.refusal

    async def _read_chunk(self, controller: AsyncIteratorController[_BatchItem], start_index: int) -> _Chunk:
        """Read the next chunk; a source that fails before its first command fails the stream (``InputSourceError``)."""
        return await _next_chunk(
            controller,
            min(
                self._batch_size,
                self._context.policy.max_buffered_commands,
                self._context.policy.max_buffered_rows,
            ),
            start_index=start_index,
            context=self._context,
        )

    async def _close_controller(self) -> None:
        # One close attempt: a failed close is recorded once and never retried by a later aclose().
        controller, self._controller = self._controller, None
        if controller is not None:
            await controller.aclose(self._context)

    async def _cleanup(self) -> None:
        await self._context.set_buffered_rows(0)
        await self._close_controller()

    async def _finalize(self, cause: TerminalCause, failure: str | None, attempt: CleanupAttempt) -> KernelReport:
        state, reason = kernel_terminal(cause, failure)
        snapshot = await self._context.snapshot()
        possibly_run, not_sent = self._undelivered()
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
            emitted_rows=self._emitted,
            unique_rows=self._emitted,
            physical_requests=snapshot.counters.physical_requests,
            logical_pages=snapshot.counters.logical_pages,
            batch_requests=self._batch_requests,
            batch_commands=self._batch_commands,
            retries=snapshot.retries,
            cooldown_seconds=snapshot.cooldown_seconds,
            buffered_rows_high_water=snapshot.counters.buffered_rows_high_water,
            ambiguous_physical_attempts=snapshot.ambiguous_physical_attempts,
            batch_isolation_requests=snapshot.batch_isolation_requests,
            terminal_reason=reason,
            undelivered_possibly_run=possibly_run,
            undelivered_not_sent=not_sent,
        )
        self._completion.stream_terminal(
            StreamClosure.NATURAL
            if state is KernelState.COMPLETED
            else StreamClosure.CANCELLED
            if state is KernelState.CANCELLED
            else StreamClosure.EARLY_CLOSE,
        )
        self.report = with_cleanup_attempt(self.report, cause, attempt)
        self._completion.cleanup(CleanupState.FAILURE if cleanup_failed(cause, attempt) else CleanupState.SUCCESS)
        return self.report

    def _failure_report(self, cause: TerminalCause, reason: str, attempt: CleanupAttempt) -> KernelReport:
        # The finalizer raised: publish the fallback as this kernel's report and close its completion gate,
        # so a public adapter over it can still publish its one FAILED report.
        self.report = failed_kernel_report(cause, reason, attempt)
        self._completion.gate.close_after_failure(
            CleanupState.FAILURE if cleanup_failed(cause, attempt) else CleanupState.SUCCESS
        )
        return self.report


__all__ = ["CommandSource", "LogicalBatchKernelStream", "_BatchWindowError"]
