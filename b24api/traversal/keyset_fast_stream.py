"""Fast-keyset lifecycle adapter, deterministic trace, and report construction."""

# ruff: noqa: TRY301

from __future__ import annotations
import asyncio
from collections import Counter, deque
from dataclasses import replace
from typing import TYPE_CHECKING, Self

from b24api.contracts.completion import CleanupState
from b24api.contracts.json import FrozenJson, JsonValue, _thaw_json
from b24api.contracts.keyset_execution import KeysetPhase, TraceClass
from b24api.contracts.policy import CompletionAssurance, KernelState, SnapshotRequirement, SnapshotState
from b24api.contracts.report import (
    PageOutcome,
    PageRecord,
    Violation,
    ViolationSeverity,
)
from b24api.errors import BudgetExceededError, IncompleteTraversalError, PaginationError
from b24api.execution.context import await_cancellation_resistant, await_cleanup_resistant, rearm_cancellation
from b24api.execution.failure import attach_report as _attach_report
from b24api.execution.snapshot import KernelReport
from b24api.traversal.keyset_observation import PageObservation

if TYPE_CHECKING:
    from collections.abc import Mapping

    from b24api.completion.gate import CompletionGate
    from b24api.traversal.keyset_scheduler import KeysetFastScheduler


class FastTraceRecorder:
    """Retain first/last records under fixed per-class quotas."""

    def __init__(self, capacity: int) -> None:
        """Derive fixed quotas and initialize bounded buffers."""
        if not isinstance(capacity, int) or isinstance(capacity, bool) or capacity < 0:
            raise ValueError("trace capacity must be a non-negative integer")
        planning = capacity // 8
        terminal = capacity // 8
        body = capacity // 4
        self._quotas = {
            TraceClass.PLANNING: planning,
            TraceClass.TERMINAL: terminal,
            TraceClass.BODY: body,
            TraceClass.ANOMALY: capacity - planning - terminal - body,
        }
        self._heads: dict[TraceClass, list[PageRecord]] = {kind: [] for kind in TraceClass}
        self._tails: dict[TraceClass, deque[PageRecord]] = {
            kind: deque(maxlen=self._quotas[kind] // 2) for kind in TraceClass
        }
        self._dropped: dict[TraceClass, int] = dict.fromkeys(TraceClass, 0)
        self._command_sequences: dict[str, int] = {}
        self._sequence_commands: dict[int, str] = {}
        self._phase_commands: Counter[KeysetPhase] = Counter()
        self._phase_rows: Counter[KeysetPhase] = Counter()
        self._phase_empty: Counter[KeysetPhase] = Counter()

    @staticmethod
    def classify(observation: PageObservation) -> TraceClass:
        """Apply the normative anomaly-first class priority."""
        if (
            observation.violation is not None
            or observation.outcome is not PageOutcome.COMMITTED
            or observation.rejection_code is not None
        ):
            return TraceClass.ANOMALY
        if observation.phase in {KeysetPhase.BOUNDARY, KeysetPhase.CANARY, KeysetPhase.ANCHOR_PROBE}:
            return TraceClass.PLANNING
        if observation.phase is KeysetPhase.FINISH:
            return TraceClass.TERMINAL
        return TraceClass.BODY

    def record(self, observation: PageObservation) -> None:
        """Convert and retain one finalized observation exactly once."""
        trace_class = self.classify(observation)
        observation = replace(observation, trace_class=trace_class)
        self._phase_commands[observation.phase] += 1
        self._phase_rows[observation.phase] += observation.rows_selected
        if observation.rows_selected == 0:
            self._phase_empty[observation.phase] += 1
        record = PageRecord(
            sequence=observation.ordinal,
            offset=None,
            dispatch=observation.dispatch,
            batch_index=observation.batch_index,
            rows_selected=observation.rows_selected,
            rows_admitted=observation.rows_admitted,
            reported_total=observation.reported_total,
            reported_next=observation.reported_next,
            outcome=observation.outcome,
            rejection_code=observation.rejection_code,
            phase=observation.phase,
            lane_ordinal=observation.lane_ordinal,
        )
        quota = self._quotas[trace_class]
        head_cap = (quota + 1) // 2
        head = self._heads[trace_class]
        tail = self._tails[trace_class]
        if len(head) < head_cap:
            head.append(record)
            self._retain_command(observation.command_id, record.sequence)
        elif tail.maxlen:
            if len(tail) == tail.maxlen:
                self._dropped[trace_class] += 1
                self._forget_sequence(tail[0].sequence)
            tail.append(record)
            self._retain_command(observation.command_id, record.sequence)
        else:
            self._dropped[trace_class] += 1

    def _retain_command(self, command_id: str, sequence: int) -> None:
        self._command_sequences[command_id] = sequence
        self._sequence_commands[sequence] = command_id

    def _forget_sequence(self, sequence: int) -> None:
        command_id = self._sequence_commands.pop(sequence, None)
        if command_id is not None:
            self._command_sequences.pop(command_id, None)

    def admit(self, command_id: str, rows: int) -> None:
        """Finalize admission on one retained successful observation."""
        sequence = self._command_sequences.pop(command_id, None)
        if sequence is None:
            return
        self._sequence_commands.pop(sequence, None)
        for records in (*self._heads.values(), *self._tails.values()):
            for index, record in enumerate(records):
                if record.sequence == sequence:
                    records[index] = replace(record, rows_admitted=rows)
                    return

    def phase_commands(self, phase: KeysetPhase) -> int:
        """Return observed command count for one phase."""
        return self._phase_commands[phase]

    def phase_rows(self, phase: KeysetPhase) -> int:
        """Return selected row count for one phase."""
        return self._phase_rows[phase]

    def phase_empty(self, phase: KeysetPhase) -> int:
        """Return empty response count for one phase."""
        return self._phase_empty[phase]

    def snapshot(self) -> tuple[tuple[PageRecord, ...], Mapping[TraceClass, int]]:
        """Return retained records and exact per-class drop counts."""
        records = tuple(
            sorted(
                (record for kind in TraceClass for record in (*self._heads[kind], *self._tails[kind])),
                key=lambda record: record.sequence,
            ),
        )
        return records, dict(self._dropped)

    def class_counts(self) -> tuple[tuple[TraceClass, int], ...]:
        """Return retained counts in enum order."""
        return tuple((kind, len(self._heads[kind]) + len(self._tails[kind])) for kind in TraceClass)


class KeysetFastStream:
    """Single-use lifecycle shell around the fast scheduler."""

    def __init__(self, scheduler: KeysetFastScheduler) -> None:
        """Initialize without starting planning or I/O."""
        self._scheduler = scheduler
        self._buffer: deque[FrozenJson] = deque()
        self._closed = False
        self.report = KernelReport()

    def __aiter__(self) -> Self:
        """Return this single-use async iterator."""
        return self

    @property
    def completion_gate(self) -> CompletionGate:
        """Expose the active fast-keyset completion evidence to the public adapter."""
        return self._scheduler.completion_recorder.gate

    async def __anext__(self) -> JsonValue:
        """Return the next admitted row or finalize terminal evidence."""
        if self._closed:
            raise StopAsyncIteration
        try:
            while not self._buffer:
                rows = await self._scheduler.next_rows()
                if not rows:
                    await self._terminate(KernelState.COMPLETED, "fast keyset traversal completed")
                    self._closed = self._scheduler._closed  # noqa: SLF001 - lifecycle shell owns its scheduler
                    raise StopAsyncIteration
                self._buffer.extend(rows)
            item = self._buffer.popleft()
            self._scheduler.mark_emitted(1)
            return _thaw_json(item)
        except StopAsyncIteration:
            raise
        except asyncio.CancelledError as error:
            await self._terminate(KernelState.CANCELLED, "iteration cancelled", primary=error)
            _attach_report(error, self.report)
            self._closed = self._scheduler._closed  # noqa: SLF001 - lifecycle shell owns its scheduler
            raise
        except (PaginationError, BudgetExceededError) as error:
            await self._terminate(KernelState.INCOMPLETE, type(error).__name__, primary=error)
            incomplete = IncompleteTraversalError(report=self.report)
            self._closed = self._scheduler._closed  # noqa: SLF001 - lifecycle shell owns its scheduler
            raise incomplete from error
        except BaseException as error:
            await self._terminate(KernelState.FAILED, type(error).__name__, primary=error)
            _attach_report(error, self.report)
            self._closed = self._scheduler._closed  # noqa: SLF001 - lifecycle shell owns its scheduler
            raise

    async def aclose(self) -> None:
        """Close early, release scheduler resources, and freeze the report."""
        if self._closed:
            return
        await self._terminate(KernelState.CANCELLED, "stream closed before exhaustion")

    async def _terminate(self, state: KernelState, reason: str, *, primary: BaseException | None = None) -> None:
        self._buffer.clear()
        outcome = await await_cleanup_resistant(self._scheduler.aclose())
        cleanup = outcome.error
        if cleanup is not None:
            self._scheduler.violations.append(
                Violation(ViolationSeverity.BLOCKING, "cleanup_failure", type(cleanup).__name__),
            )
        if cleanup is not None and state is KernelState.COMPLETED:
            state, reason = KernelState.FAILED, "fast keyset cleanup failed"
        finalize_cancellation = await await_cancellation_resistant(self._finalize(state, reason))
        cancellation = finalize_cancellation or outcome.cancellation
        self._closed = self._scheduler._closed  # noqa: SLF001 - lifecycle shell owns its scheduler
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
        snapshot = await self._scheduler.context.snapshot()
        snapshot_state = (
            SnapshotState.NOT_REQUESTED
            if self._scheduler.context.policy.consistency.snapshot_requirement is SnapshotRequirement.TRAVERSAL_ONLY
            else SnapshotState.UNVERIFIED
        )
        if state is KernelState.COMPLETED and snapshot_state is SnapshotState.UNVERIFIED:
            state = KernelState.INCOMPLETE
            reason = "required snapshot was not verified"
        records, dropped = self._scheduler.trace.snapshot()
        self.report = KernelReport(
            state=state,
            assurance=CompletionAssurance.CALLER_ASSERTED,
            snapshot=snapshot_state,
            plan_id="iter_list_keyset_fast",
            dispatch_id="batch",
            emitted_rows=self._scheduler.counters.emitted_rows,
            unique_rows=self._scheduler.counters.unique_rows,
            physical_requests=snapshot.counters.physical_requests,
            logical_pages=snapshot.counters.logical_pages,
            batch_requests=self._scheduler.batch_requests,
            batch_commands=self._scheduler.batch_commands,
            retries=snapshot.retries,
            cooldown_seconds=snapshot.cooldown_seconds,
            buffered_rows_high_water=snapshot.counters.buffered_rows_high_water,
            violations=tuple(self._scheduler.violations),
            terminal_reason=reason,
            page_trace=records,
            page_trace_truncated=any(dropped.values()),
            keyset_execution=self._scheduler.report_fragment(),
        )
        counters = self._scheduler.counters
        cleanup = (
            CleanupState.FAILURE
            if any(violation.code == "cleanup_failure" for violation in self._scheduler.violations)
            else CleanupState.SUCCESS
        )
        self._scheduler.completion_recorder.terminal(
            state,
            rows_emitted=counters.emitted_rows,
            rows_admitted=counters.admitted_rows,
            cleanup=cleanup,
        )


__all__ = ["FastTraceRecorder", "KeysetFastStream", "PageObservation"]
