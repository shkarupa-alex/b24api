"""Fast-keyset lifecycle adapter and terminal report construction."""

from __future__ import annotations
from typing import TYPE_CHECKING, Self

from b24api.completion.fast_recorder import FastCompletionRecorder
from b24api.contracts.completion import CleanupState
from b24api.contracts.json import JsonValue, thaw_json
from b24api.contracts.policy import (
    CompletionAssurance,
    KernelState,
    ReplayDisposition,
    SnapshotRequirement,
    SnapshotState,
)
from b24api.errors import B24ApiError, BudgetExceededError, IncompleteTraversalError, PaginationError
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
    from collections.abc import AsyncGenerator

    from b24api.completion.gate import CompletionGate
    from b24api.execution.context import ExecutionContext
    from b24api.traversal.keyset_runtime import KeysetRuntime

# The failures that leave a fast keyset traversal incomplete rather than failed.
_INCOMPLETE = (IncompleteTraversalError, PaginationError, BudgetExceededError)


class KeysetFastStream:
    """Single-use lifecycle shell around the fast keyset runtime; ``OperationRunner`` owns its termination."""

    # Every wave is read inside a read; nothing runs between reads.
    works_between_reads = False

    def __init__(self, runtime: KeysetRuntime) -> None:
        """Initialize without starting planning or I/O."""
        recorder = runtime.completion_recorder
        if not isinstance(recorder, FastCompletionRecorder):
            raise TypeError("a flat fast keyset stream owns its own completion gate")
        self._runtime = runtime
        self._recorder = recorder
        self.report = KernelReport()
        self._runner: OperationRunner[JsonValue, KernelReport] = OperationRunner(
            self._run(),
            LifecycleHooks(
                finalize=self._finalize,
                failure_report=self._failure_report,
                cleanup=runtime.aclose,
                propagate=self._propagate,
            ),
            isolated_pulls=False,
        )

    def __aiter__(self) -> Self:
        """Return this single-use async iterator."""
        return self

    @property
    def operation_context(self) -> ExecutionContext:
        """Return the operation context whose elapsed budget bounds the wrapper's cleanup."""
        return self._runtime.context.execution_context

    @property
    def completion_gate(self) -> CompletionGate:
        """Expose the active fast-keyset completion evidence to the public adapter."""
        return self._recorder.gate

    @property
    def cleanup_failure(self) -> BaseException | None:
        """Return the cleanup failure this stream raised as its end (``OperationRunner.cleanup_failure``)."""
        return self._runner.cleanup_failure

    async def __anext__(self) -> JsonValue:
        """Return the next admitted row; the read that ends the traversal publishes the report."""
        return await anext(self._runner)

    async def aclose(self) -> None:
        """Close early, release runtime resources, and freeze the report."""
        await self._runner.aclose()

    async def _run(self) -> AsyncGenerator[JsonValue]:
        while rows := await self._runtime.next_rows():
            for row in rows:
                self._runtime.mark_emitted(1)
                yield thaw_json(row)

    def _propagate(self, error: BaseException, report: KernelReport) -> BaseException:
        # A failed command's IncompleteTraversalError is raised as is, with the report the runner attaches to it.
        if isinstance(error, IncompleteTraversalError) or not isinstance(error, _INCOMPLETE):
            return error
        return IncompleteTraversalError(
            report=report,
            error=error if isinstance(error, B24ApiError) else None,
            replay_disposition=getattr(error, "replay_disposition", ReplayDisposition.NOT_ELIGIBLE),
        )

    async def _finalize(self, cause: TerminalCause, failure: str | None, attempt: CleanupAttempt) -> KernelReport:
        state, reason = kernel_terminal(cause, failure)
        if cause is TerminalCause.EXHAUSTED:
            reason = "fast keyset traversal completed"
        elif isinstance(primary := self._runner.primary_error, _INCOMPLETE):
            state, reason = KernelState.INCOMPLETE, type(primary).__name__
        snapshot = await self._runtime.context.snapshot()
        snapshot_state = (
            SnapshotState.NOT_REQUESTED
            if self._runtime.context.policy.consistency.snapshot_requirement is SnapshotRequirement.TRAVERSAL_ONLY
            else SnapshotState.UNVERIFIED
        )
        if state is KernelState.COMPLETED and snapshot_state is SnapshotState.UNVERIFIED:
            state = KernelState.INCOMPLETE
            reason = "required snapshot was not verified"
        records, dropped = self._runtime.trace.snapshot()
        self.report = KernelReport(
            state=state,
            assurance=CompletionAssurance.CALLER_ASSERTED,
            snapshot=snapshot_state,
            emitted_rows=self._runtime.counters.emitted_rows,
            unique_rows=self._runtime.counters.unique_rows,
            physical_requests=snapshot.counters.physical_requests,
            logical_pages=snapshot.counters.logical_pages,
            batch_requests=self._runtime.batch_requests,
            batch_commands=self._runtime.batch_commands,
            retries=snapshot.retries,
            cooldown_seconds=snapshot.cooldown_seconds,
            buffered_rows_high_water=snapshot.counters.buffered_rows_high_water,
            violations=tuple(self._runtime.violations),
            terminal_reason=reason,
            page_trace=records,
            page_trace_truncated=any(dropped.values()),
            keyset_execution=self._runtime.report_fragment(),
        )
        self.report = with_cleanup_attempt(self.report, cause, attempt)
        counters = self._runtime.counters
        cleanup = CleanupState.FAILURE if cleanup_failed(cause, attempt) else CleanupState.SUCCESS
        closure, qualified_witnesses = self._runtime.completion_closure()
        # A traversal read to its end whose close failed did not complete; any other end keeps its own closure.
        completed_but_failed = state is KernelState.COMPLETED and cleanup is CleanupState.FAILURE
        self._recorder.terminal(
            KernelState.FAILED if completed_but_failed else state,
            rows_emitted=counters.emitted_rows,
            rows_admitted=counters.admitted_rows,
            cleanup=cleanup,
            closure=closure,
            qualified_witnesses=qualified_witnesses,
        )
        return self.report

    def _failure_report(self, cause: TerminalCause, reason: str, attempt: CleanupAttempt) -> KernelReport:
        # The finalizer raised: publish the fallback as this kernel's report and close its completion gate,
        # so the public adapter over it can still publish its one FAILED report.
        self.report = failed_kernel_report(cause, reason, attempt)
        self._recorder.gate.close_after_failure(
            CleanupState.FAILURE if cleanup_failed(cause, attempt) else CleanupState.SUCCESS
        )
        return self.report


__all__ = ["KeysetFastStream"]
