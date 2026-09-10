"""Value-only staging record for fast keyset trace observations."""

from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from b24api.contracts.keyset_execution import ClosureWitness, KeysetPhase, TraceClass
    from b24api.contracts.report import PageDispatch, PageOutcome, PageRejectionCode, Violation


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


__all__: list[str] = []
