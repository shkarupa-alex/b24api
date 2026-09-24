"""Fast-keyset lifecycle adapter and terminal report construction."""

# ruff: noqa: TRY301

from __future__ import annotations
import asyncio
from collections import deque
from typing import TYPE_CHECKING, Self

from b24api.contracts.completion import CleanupState
from b24api.contracts.json import FrozenJson, JsonValue, _thaw_json
from b24api.contracts.policy import (
    CompletionAssurance,
    KernelState,
    ReplayDisposition,
    SnapshotRequirement,
    SnapshotState,
)
from b24api.contracts.report import (
    Violation,
    ViolationSeverity,
)
from b24api.errors import B24ApiError, BudgetExceededError, IncompleteTraversalError, PaginationError
from b24api.execution.context import await_cancellation_resistant, await_cleanup_resistant, rearm_cancellation
from b24api.execution.failure import attach_report as _attach_report
from b24api.execution.snapshot import KernelReport

if TYPE_CHECKING:
    from b24api.completion.gate import CompletionGate
    from b24api.traversal.keyset_runtime import KeysetRuntime


class KeysetFastStream:
    """Single-use lifecycle shell around the fast keyset runtime."""

    def __init__(self, runtime: KeysetRuntime) -> None:
        """Initialize without starting planning or I/O."""
        self._runtime = runtime
        self._buffer: deque[FrozenJson] = deque()
        self._closed = False
        self.report = KernelReport()

    def __aiter__(self) -> Self:
        """Return this single-use async iterator."""
        return self

    @property
    def completion_gate(self) -> CompletionGate:
        """Expose the active fast-keyset completion evidence to the public adapter."""
        return self._runtime.completion_recorder.gate

    async def __anext__(self) -> JsonValue:
        """Return the next admitted row or finalize terminal evidence."""
        if self._closed:
            raise StopAsyncIteration
        try:
            while not self._buffer:
                rows = await self._runtime.next_rows()
                if not rows:
                    await self._terminate(KernelState.COMPLETED, "fast keyset traversal completed")
                    self._closed = self._runtime.closed
                    raise StopAsyncIteration
                self._buffer.extend(rows)
            item = self._buffer.popleft()
            self._runtime.mark_emitted(1)
            return _thaw_json(item)
        except StopAsyncIteration:
            raise
        except asyncio.CancelledError as error:
            await self._terminate(KernelState.CANCELLED, "iteration cancelled", primary=error)
            _attach_report(error, self.report)
            self._closed = self._runtime.closed
            raise
        except (IncompleteTraversalError, PaginationError, BudgetExceededError) as error:
            await self._terminate(KernelState.INCOMPLETE, type(error).__name__, primary=error)
            cause = error.error if isinstance(error, IncompleteTraversalError) else error
            incomplete = IncompleteTraversalError(
                report=self.report,
                error=cause if isinstance(cause, B24ApiError) else None,
                replay_disposition=(
                    error.replay_disposition
                    if isinstance(error, IncompleteTraversalError)
                    else getattr(error, "replay_disposition", ReplayDisposition.NOT_ELIGIBLE)
                ),
            )
            self._closed = self._runtime.closed
            raise incomplete from cause
        except BaseException as error:
            await self._terminate(KernelState.FAILED, type(error).__name__, primary=error)
            _attach_report(error, self.report)
            self._closed = self._runtime.closed
            raise

    async def aclose(self) -> None:
        """Close early, release runtime resources, and freeze the report."""
        if self._closed:
            return
        await self._terminate(KernelState.CANCELLED, "stream closed before exhaustion")

    async def _terminate(self, state: KernelState, reason: str, *, primary: BaseException | None = None) -> None:
        self._buffer.clear()
        outcome = await await_cleanup_resistant(self._runtime.aclose())
        cleanup = outcome.error
        if cleanup is not None:
            self._runtime.violations.append(
                Violation(ViolationSeverity.BLOCKING, "cleanup_failure", type(cleanup).__name__),
            )
        if cleanup is not None and state is KernelState.COMPLETED:
            state, reason = KernelState.FAILED, "fast keyset cleanup failed"
        finalize_cancellation = await await_cancellation_resistant(self._finalize(state, reason))
        cancellation = finalize_cancellation or outcome.cancellation
        self._closed = self._runtime.closed
        if cleanup is not None and primary is not None:
            primary.add_note(f"fast keyset cleanup also failed ({type(cleanup).__name__})")
        if cleanup is not None and primary is None:
            _attach_report(cleanup, self.report)
            rearm_cancellation(cancellation)
            raise cleanup
        if cancellation is not None and primary is None:
            _attach_report(cancellation, self.report)
            raise cancellation
        if cancellation is not None and not isinstance(primary, asyncio.CancelledError):
            rearm_cancellation(cancellation)

    async def _finalize(self, state: KernelState, reason: str) -> None:
        if self.report.state is not KernelState.NOT_STARTED:
            return
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
        counters = self._runtime.counters
        cleanup = (
            CleanupState.FAILURE
            if any(violation.code == "cleanup_failure" for violation in self._runtime.violations)
            else CleanupState.SUCCESS
        )
        closure, qualified_witnesses = self._runtime.completion_closure()
        self._runtime.completion_recorder.terminal(
            state,
            rows_emitted=counters.emitted_rows,
            rows_admitted=counters.admitted_rows,
            cleanup=cleanup,
            closure=closure,
            qualified_witnesses=qualified_witnesses,
        )


__all__ = ["KeysetFastStream"]
