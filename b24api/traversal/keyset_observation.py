"""Value-only fast keyset page observations, their staging and the bounded page trace."""

from __future__ import annotations
from collections import deque
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, NoReturn

from b24api.contracts.keyset_execution import ClosureWitness, KeysetPhase, TraceClass
from b24api.contracts.report import (
    PageDispatch,
    PageOutcome,
    PageRecord,
    PageRejectionCode,
    Violation,
    ViolationSeverity,
)
from b24api.errors import PaginationError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from b24api.contracts.response import Response
    from b24api.traversal.keyset_ordered_admission import OrderedAdmissionState
    from b24api.traversal.keyset_page_validation import LaneCommandPlan


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
    witness: ClosureWitness | None
    outcome: PageOutcome
    rejection_code: PageRejectionCode | None
    violation: Violation | None
    trace_class: TraceClass


def page_observation(  # noqa: PLR0913
    ordinal: int,
    plan: LaneCommandPlan,
    *,
    index: int | None,
    selected: int,
    admitted: int,
    outcome: PageOutcome = PageOutcome.COMMITTED,
    rejection: PageRejectionCode | None = None,
    violation: Violation | None = None,
    response: Response | None = None,
    witness: ClosureWitness | None = None,
    dispatch: PageDispatch = PageDispatch.BATCH,
) -> PageObservation:
    """Build value-only trace evidence independently of scheduler sequencing."""
    return PageObservation(
        ordinal,
        plan.phase,
        plan.lane_ordinal,
        plan.command_id,
        dispatch,
        index,
        selected,
        admitted,
        response.total if response is not None and response.total is not None and response.total >= 0 else None,
        response.next if response is not None else None,
        witness,
        outcome,
        rejection,
        violation,
        TraceClass.BODY,
    )


def flush_staged_observations(
    staged: list[tuple[LaneCommandPlan, int, int, Response | None, ClosureWitness | None]],
    record: Callable[..., None],
    *,
    violation: Violation | None = None,
    offending: set[str] | None = None,
) -> None:
    """Finalize staged semantic observations as committed or anomalous records."""
    for plan, index, selected, response, witness in staged:
        if violation is None:
            record(plan, index=index, selected=selected, admitted=0, response=response, witness=witness)
            continue
        is_offending = offending is None or plan.command_id in offending
        record(
            plan,
            index=index,
            selected=selected,
            admitted=0,
            outcome=PageOutcome.REJECTED,
            rejection=(
                PageRejectionCode.RANGE_CONTRADICTION if is_offending else PageRejectionCode.TRANSACTION_ABORTED
            ),
            violation=violation if is_offending else None,
        )
    staged.clear()


def stage_or_record_observation(  # noqa: PLR0913
    staged: list[tuple[LaneCommandPlan, int, int, Response | None, ClosureWitness | None]],
    record: Callable[..., None],
    *,
    stage: bool,
    plan: LaneCommandPlan,
    index: int,
    rows: int,
    response: Response | None,
    witness: ClosureWitness | None,
) -> None:
    """Retain semantic evidence until validation or record it immediately."""
    if stage:
        staged.append((plan, index, rows, response, witness))
    else:
        record(plan, index=index, selected=rows, admitted=0, response=response, witness=witness)


def abort_staged_observations(
    staged: list[tuple[LaneCommandPlan, int, int, Response | None, ClosureWitness | None]],
    record: Callable[..., None],
    admission: OrderedAdmissionState,
    violations: list[Violation],
) -> None:
    """Finalize earlier semantic waves when a later planning wave fails."""
    if not staged:
        return
    boundary = sum(item[2] for item in staged if item[0].phase.value == "boundary")
    admission.record_discarded(boundary)
    violation = (
        violations[-1]
        if violations
        else Violation(ViolationSeverity.BLOCKING, "planning_aborted", "fast keyset semantic planning aborted")
    )
    flush_staged_observations(staged, record, violation=violation, offending=set())


def reject_boundary_observations(  # noqa: PLR0913
    staged: list[tuple[LaneCommandPlan, int, int, Response | None, ClosureWitness | None]],
    record: Callable[..., None],
    admission: OrderedAdmissionState,
    *,
    rows: int,
    violation: Violation,
    raw: bool,
) -> None:
    """Finalize invalid boundary evidence without manufacturing commits."""
    admission.record_raw(rows, discarded=True) if raw else admission.record_discarded(rows)
    flush_staged_observations(staged, record, violation=violation)


def raise_boundary_cap_contradiction(
    staged: list[tuple[LaneCommandPlan, int, int, Response | None, ClosureWitness | None]],
    record: Callable[..., None],
    admission: OrderedAdmissionState,
    violations: list[Violation],
    rows: int,
) -> NoReturn:
    """Reject staged boundaries that fail the declared page-cap contract."""
    message = "boundary pages did not establish page-cap agreement"
    violation = Violation(ViolationSeverity.BLOCKING, "page_cap_contradiction", message)
    violations.append(violation)
    reject_boundary_observations(staged, record, admission, rows=rows, violation=violation, raw=False)
    raise PaginationError(message)


def finalize_boundary_observations(
    staged: list[tuple[LaneCommandPlan, int, int, Response | None, ClosureWitness | None]],
    record: Callable[..., None],
) -> None:
    """Commit validated boundary observations without capability assertions."""
    flush_staged_observations(staged, record)


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

    @staticmethod
    def classify(observation: PageObservation) -> TraceClass:
        """Apply the normative anomaly-first class priority."""
        if (
            observation.violation is not None
            or observation.outcome is not PageOutcome.COMMITTED
            or observation.rejection_code is not None
        ):
            return TraceClass.ANOMALY
        if observation.phase in {KeysetPhase.BOUNDARY, KeysetPhase.ANCHOR_PROBE}:
            return TraceClass.PLANNING
        if observation.phase is KeysetPhase.FINISH:
            return TraceClass.TERMINAL
        return TraceClass.BODY

    def record(self, observation: PageObservation) -> None:
        """Convert and retain one finalized observation exactly once."""
        trace_class = self.classify(observation)
        observation = replace(observation, trace_class=trace_class)
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


__all__ = ["FastTraceRecorder", "PageObservation"]
