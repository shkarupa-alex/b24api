"""Value-only staging record for fast keyset trace observations."""

from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING, NoReturn

from b24api.contracts.report import PageOutcome, PageRejectionCode, Violation, ViolationSeverity
from b24api.errors import PaginationError

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.contracts.keyset_execution import ClosureWitness, KeysetPhase, TraceClass
    from b24api.contracts.report import PageDispatch
    from b24api.contracts.response import Response
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
            plan, index=index, selected=selected, admitted=0, outcome=PageOutcome.REJECTED,
            rejection=(PageRejectionCode.RANGE_CONTRADICTION if is_offending
                       else PageRejectionCode.TRANSACTION_ABORTED),
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
    record: Callable[..., None], admission: OrderedAdmissionState, violations: list[Violation],
) -> None:
    """Finalize earlier semantic waves when a later planning wave fails."""
    if not staged:
        return
    boundary = sum(item[2] for item in staged if item[0].phase.value == "boundary")
    canary = sum(item[2] for item in staged if item[0].phase.value == "canary")
    admission.record_discarded(boundary)
    admission.record_raw(canary, discarded=True)
    violation = violations[-1] if violations else Violation(
        ViolationSeverity.BLOCKING, "planning_aborted", "fast keyset semantic planning aborted")
    flush_staged_observations(staged, record, violation=violation, offending=set())


def reject_boundary_observations(  # noqa: PLR0913
    staged: list[tuple[LaneCommandPlan, int, int, Response | None, ClosureWitness | None]],
    record: Callable[..., None], admission: OrderedAdmissionState, *, rows: int, violation: Violation, raw: bool,
) -> None:
    """Finalize invalid boundary evidence without manufacturing commits."""
    admission.record_raw(rows, discarded=True) if raw else admission.record_discarded(rows)
    flush_staged_observations(staged, record, violation=violation)


def raise_boundary_cap_contradiction(
    staged: list[tuple[LaneCommandPlan, int, int, Response | None, ClosureWitness | None]],
    record: Callable[..., None], admission: OrderedAdmissionState, violations: list[Violation], rows: int,
) -> NoReturn:
    """Reject staged boundaries that fail the declared page-cap contract."""
    message = "boundary pages did not establish page-cap agreement"
    violation = Violation(ViolationSeverity.BLOCKING, "page_cap_contradiction", message)
    violations.append(violation)
    reject_boundary_observations(staged, record, admission, rows=rows, violation=violation, raw=False)
    raise PaginationError(message)


def validate_canary_observations(  # noqa: PLR0913
    receipts: tuple[LaneReceipt, ...], expected: dict[str, tuple[int, ...]],
    staged: list[tuple[LaneCommandPlan, int, int, Response | None, ClosureWitness | None]],
    record: Callable[..., None], admission: OrderedAdmissionState, violations: list[Violation],
) -> None:
    """Finalize all phase-owned observations after semantic canary validation."""
    by_id = {receipt.command_id: receipt for receipt in receipts}
    offending = {command_id for command_id, values in expected.items() if by_id[command_id].identities != values}
    admission.record_raw(sum(len(receipt.rows) for receipt in receipts), discarded=True)
    if offending:
        violation = Violation(
            ViolationSeverity.BLOCKING, "canary_contradiction", "bounded keyset capability canary failed")
        violations.append(violation)
        admission.record_discarded(sum(item[2] for item in staged if item[0].phase.value == "boundary"))
        flush_staged_observations(staged, record, violation=violation, offending=offending)
        raise PaginationError("bounded keyset capability canary failed")
    flush_staged_observations(staged, record)


__all__: list[str] = []
