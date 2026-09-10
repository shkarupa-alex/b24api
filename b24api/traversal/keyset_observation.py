"""Value-only staging record for fast keyset trace observations."""

# ruff: noqa: SLF001

from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING, NoReturn

from b24api.contracts.keyset_execution import ClosureWitness, TraceClass
from b24api.contracts.report import PageDispatch, PageOutcome, PageRejectionCode, Violation, ViolationSeverity
from b24api.errors import PaginationError

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.contracts.keyset_execution import KeysetPhase
    from b24api.contracts.response import Response
    from b24api.traversal.keyset_scheduler import KeysetFastScheduler
    from b24api.traversal.ordered_admission import OrderedAdmissionState
    from b24api.traversal.page_validation import LaneCommandPlan, LaneReceipt


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


def page_observation(  # noqa: PLR0913
    ordinal: int,
    plan: LaneCommandPlan,
    *,
    index: int | None,
    selected: int,
    admitted: int,
    effective_page_cap: int,
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
        selected == effective_page_cap,
        witness,
        outcome,
        rejection,
        violation,
        TraceClass.BODY,
    )


def record_scheduler_observation(  # noqa: PLR0913
    scheduler: KeysetFastScheduler,
    plan: LaneCommandPlan,
    *,
    index: int | None,
    selected: int,
    admitted: int,
    outcome: PageOutcome,
    rejection: PageRejectionCode | None,
    violation: Violation | None,
    response: Response | None,
    witness: ClosureWitness | None,
    dispatch: PageDispatch,
) -> None:
    """Record one scheduler observation and advance its sole ordinal."""
    scheduler.trace.record(
        page_observation(
            scheduler._observation_ordinal,
            plan,
            index=index,
            selected=selected,
            admitted=admitted,
            effective_page_cap=scheduler.effective_page_cap,
            outcome=outcome,
            rejection=rejection,
            violation=violation,
            response=response,
            witness=witness,
            dispatch=dispatch,
        ),
    )
    scheduler._observation_ordinal += 1


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
    canary = sum(item[2] for item in staged if item[0].phase.value == "canary")
    admission.record_discarded(boundary)
    admission.record_raw(canary, discarded=True)
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


def validate_canary_observations(  # noqa: PLR0913
    receipts: tuple[LaneReceipt, ...],
    expected: dict[str, tuple[int, ...]],
    staged: list[tuple[LaneCommandPlan, int, int, Response | None, ClosureWitness | None]],
    record: Callable[..., None],
    admission: OrderedAdmissionState,
    violations: list[Violation],
) -> None:
    """Finalize all phase-owned observations after semantic canary validation."""
    by_id = {receipt.command_id: receipt for receipt in receipts}
    offending = {command_id for command_id, values in expected.items() if by_id[command_id].identities != values}
    admission.record_raw(sum(len(receipt.rows) for receipt in receipts), discarded=True)
    if offending:
        violation = Violation(
            ViolationSeverity.BLOCKING,
            "canary_contradiction",
            "bounded keyset capability canary failed",
        )
        violations.append(violation)
        boundary_rows = sum(item[2] for item in staged if item[0].phase.value == "boundary")
        anchor_rows = sum(item[2] for item in staged if item[0].phase.value == "anchor_probe")
        admission.record_discarded(boundary_rows)
        admission.record_raw(anchor_rows, discarded=True)
        flush_staged_observations(staged, record, violation=violation, offending=offending)
        raise PaginationError("bounded keyset capability canary failed")
    flush_staged_observations(staged, record)


async def close_scheduler(scheduler: KeysetFastScheduler) -> None:
    """Release all state retained by the scheduler owner exactly once."""
    if scheduler._closed:
        return
    scheduler.admission.discard_unadmitted_raw()
    scheduler._frozen_report = scheduler.report_fragment()
    if scheduler._buffer_balance:
        await scheduler._adjust_buffer(-scheduler._buffer_balance)
    if scheduler._buffer_balance != 0:
        raise RuntimeError("fast scheduler buffer balance survived cleanup")
    for retained in (
        scheduler._pending,
        scheduler._lane_rows,
        scheduler._lane_identities,
        scheduler._lane_commands,
        scheduler._lanes,
    ):
        retained.clear()
    scheduler._tail = None
    scheduler._anchor_rows.clear()
    scheduler._anchor_commands.clear()
    scheduler._planning_bounds.clear()
    scheduler._planning_descending.clear()
    scheduler._boundary_totals.clear()
    scheduler._staged_observations.clear()
    scheduler._plan_outcome = None
    scheduler._range_geometry = None
    scheduler.admission.assert_clean()
    scheduler.admission.close()
    scheduler._closed = True


__all__: list[str] = []
