"""Value-only staging record for fast keyset trace observations."""

from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING

from b24api.contracts.report import PageOutcome, PageRejectionCode

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.contracts.keyset_execution import ClosureWitness, KeysetPhase, TraceClass
    from b24api.contracts.report import PageDispatch, Violation
    from b24api.contracts.response import Response
    from b24api.traversal.page_validation import LaneCommandPlan


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


__all__: list[str] = []
