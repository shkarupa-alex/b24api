"""Fast-keyset lifecycle adapter, deterministic trace, and report construction."""

# ruff: noqa: TRY300, TRY301

from __future__ import annotations
import asyncio
from collections import Counter, deque
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Self

from b24api.contracts.keyset_execution import (
    ClosureWitness,
    KeysetAssuranceSource,
    KeysetExecutionKind,
    KeysetPhase,
    KeysetSelectionReason,
    TraceClass,
)
from b24api.contracts.policy import CompletionAssurance, KernelState, SnapshotRequirement, SnapshotState
from b24api.contracts.report import (
    KeysetExecutionReport,
    PageDispatch,
    PageOutcome,
    PageRecord,
    PageRejectionCode,
    Violation,
)
from b24api.errors import IncompleteTraversalError, PaginationError
from b24api.execution.failure import attach_report as _attach_report
from b24api.execution.snapshot import KernelReport

if TYPE_CHECKING:
    from collections.abc import Mapping

    from b24api.contracts.json import JsonValue
    from b24api.traversal.keyset_auto import FinalSelection, Preselection, TotalHintState
    from b24api.traversal.keyset_scheduler import KeysetFastScheduler
    from b24api.traversal.ordered_admission import FastCounters


@dataclass(frozen=True, slots=True)
class PageObservation:
    """Final value-free staging record for one fast logical page."""

    ordinal: int
    phase: KeysetPhase
    lane_ordinal: int | None
    command_id: str
    dispatch: PageDispatch
    batch_index: int | None
    rows_selected: int
    rows_admitted: int
    reported_total: int | None
    reported_next: int | None
    page_full: bool
    witness: ClosureWitness | None
    outcome: PageOutcome
    rejection_code: PageRejectionCode | None
    violation: Violation | None
    trace_class: TraceClass


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


def build_keyset_execution_report(  # noqa: PLR0913
    *,
    requested: KeysetExecutionKind,
    preselection: Preselection | None,
    final: FinalSelection | None,
    counters: FastCounters,
    trace: FastTraceRecorder,
    total_hint: TotalHintState,
) -> KeysetExecutionReport:
    """Build a redacted immutable aggregate from planner and trace state."""
    phase_requests = {phase: int(trace.phase_commands(phase) > 0) for phase in KeysetPhase}
    selected = (
        final.kind
        if final is not None
        else (
            KeysetExecutionKind.BOUNDARY_ONLY
            if preselection is not None and preselection.plan.value == "boundary_only"
            else KeysetExecutionKind.SEQUENTIAL
        )
    )
    reason = (
        preselection.reason
        if preselection is not None
        else (
            KeysetSelectionReason.EXPLICIT_RANGE
            if requested is KeysetExecutionKind.RANGE
            else KeysetSelectionReason.EXPLICIT_PARTITIONED
        )
    )
    selected_estimate = final.estimate.requests if final is not None else None
    records, dropped = trace.snapshot()
    del records
    return KeysetExecutionReport(
        requested_kind=requested,
        selected_kind=selected,
        preselection_reason=reason,
        final_selection_reason=final.reason if final is not None else None,
        assurance_source=(
            KeysetAssuranceSource.CANARY_VERIFIED_BOUNDS
            if selected in {KeysetExecutionKind.RANGE, KeysetExecutionKind.PARTITIONED}
            else KeysetAssuranceSource.ORDERED_PREFIX_ONLY
        ),
        planning_requests=sum(
            phase_requests[phase] for phase in (KeysetPhase.BOUNDARY, KeysetPhase.CANARY, KeysetPhase.ANCHOR_PROBE)
        ),
        boundary_requests=phase_requests[KeysetPhase.BOUNDARY],
        canary_requests=phase_requests[KeysetPhase.CANARY],
        anchor_probe_requests=phase_requests[KeysetPhase.ANCHOR_PROBE],
        canary_commands=trace.phase_commands(KeysetPhase.CANARY),
        canary_rows=trace.phase_rows(KeysetPhase.CANARY),
        anchor_probe_commands=trace.phase_commands(KeysetPhase.ANCHOR_PROBE),
        anchor_count=0,
        empty_anchor_probes=trace.phase_empty(KeysetPhase.ANCHOR_PROBE),
        probe_rows_discarded=counters.probe_rows_discarded,
        boundary_overlap_rows=counters.boundary_overlap_rows,
        head_page_admitted=counters.admitted_rows > 0,
        sequential_requests_estimate=(preselection.sequential_estimate.requests if preselection is not None else None),
        selected_requests_estimate=selected_estimate,
        head_rows=0,
        tail_rows=0,
        interior_span=preselection.interior_span if preselection is not None else None,
        interior_rows_estimate=preselection.interior_rows_estimate if preselection is not None else None,
        total_rows_estimate=preselection.total_rows_estimate if preselection is not None else None,
        density_numerator=preselection.density_numerator if preselection is not None else None,
        density_denominator=preselection.density_denominator if preselection is not None else None,
        effective_window_width=(final.estimate.window_width if final is not None else None),
        range_window_count=(final.estimate.window_count if final is not None else None),
        target_lanes=None,
        actual_lanes=final.lane_count if final is not None else None,
        continuation_count=trace.phase_commands(KeysetPhase.BODY) + trace.phase_commands(KeysetPhase.FINISH),
        closure_witness_counts=(),
        effective_batch_capacity=0,
        total_hint_requested=total_hint.requested,
        total_hint_observed=total_hint.observed,
        total_hint_plausible=total_hint.plausible,
        total_hint_used=total_hint.used,
        trace_retained_by_class=trace.class_counts(),
        trace_dropped_by_class=tuple((kind, dropped[kind]) for kind in TraceClass),
        raw_rows=counters.raw_rows,
    )


class KeysetFastStream:
    """Single-use lifecycle shell around the fast scheduler."""

    def __init__(self, scheduler: KeysetFastScheduler) -> None:
        """Initialize without starting planning or I/O."""
        self._scheduler = scheduler
        self._buffer: deque[JsonValue] = deque()
        self._closed = False
        self.report = KernelReport()

    def __aiter__(self) -> Self:
        """Return this single-use async iterator."""
        return self

    async def __anext__(self) -> JsonValue:
        """Return the next admitted row or finalize terminal evidence."""
        if self._closed:
            raise StopAsyncIteration
        try:
            while not self._buffer:
                rows = await self._scheduler.next_rows()
                if not rows:
                    await self._scheduler.aclose()
                    await self._finalize(KernelState.COMPLETED, "fast keyset traversal completed")
                    self._closed = True
                    raise StopAsyncIteration
                self._buffer.extend(rows)
            item = self._buffer.popleft()
            self._scheduler.mark_emitted(1)
            return item
        except StopAsyncIteration:
            raise
        except asyncio.CancelledError as error:
            await self._scheduler.aclose()
            await self._finalize(KernelState.CANCELLED, "iteration cancelled")
            _attach_report(error, self.report)
            self._closed = True
            raise
        except PaginationError as error:
            await self._scheduler.aclose()
            await self._finalize(KernelState.INCOMPLETE, type(error).__name__)
            incomplete = IncompleteTraversalError(report=self.report)
            self._closed = True
            raise incomplete from error
        except BaseException as error:
            await self._scheduler.aclose()
            await self._finalize(KernelState.FAILED, type(error).__name__)
            _attach_report(error, self.report)
            self._closed = True
            raise

    async def aclose(self) -> None:
        """Close early, release scheduler resources, and freeze the report."""
        if self._closed:
            return
        self._closed = True
        await self._scheduler.aclose()
        await self._finalize(KernelState.CANCELLED, "stream closed before exhaustion")

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


__all__ = ["FastTraceRecorder", "KeysetFastStream", "PageObservation", "build_keyset_execution_report"]
