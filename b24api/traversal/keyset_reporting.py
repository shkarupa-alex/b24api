"""Immutable aggregate reporting for the fast keyset scheduler."""

# ruff: noqa: SLF001

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.contracts.keyset_execution import (
    AutoKeysetExecution,
    ClosureWitness,
    KeysetAssuranceSource,
    KeysetExecutionKind,
    KeysetPhase,
    KeysetSelectionReason,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    TraceClass,
)
from b24api.contracts.report import KeysetExecutionReport

if TYPE_CHECKING:
    from b24api.traversal.keyset_scheduler import KeysetFastScheduler


def initial_report_selection(
    execution: RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution,
) -> tuple[KeysetExecutionKind, KeysetSelectionReason]:
    """Represent incomplete planning inside the existing closed report enums."""
    reason = (
        KeysetSelectionReason.EXPLICIT_PARTITIONED
        if isinstance(execution, PartitionedKeysetExecution)
        else KeysetSelectionReason.EXPLICIT_RANGE
        if isinstance(execution, RangeKeysetExecution)
        else KeysetSelectionReason.INSUFFICIENT_PREDICTED_GAIN
    )
    return KeysetExecutionKind.AUTO, reason


def build_scheduler_report(scheduler: KeysetFastScheduler) -> KeysetExecutionReport:
    """Freeze redacted planner, provenance, closure, and trace aggregates."""
    counters = scheduler.counters
    _, dropped = scheduler.trace.snapshot()
    return KeysetExecutionReport(
        requested_kind=(
            KeysetExecutionKind.AUTO
            if isinstance(scheduler.execution, AutoKeysetExecution)
            else KeysetExecutionKind.PARTITIONED
            if isinstance(scheduler.execution, PartitionedKeysetExecution)
            else KeysetExecutionKind.RANGE
        ),
        selected_kind=scheduler._selected,
        preselection_reason=scheduler._reason,
        final_selection_reason=scheduler._final.reason if scheduler._final is not None else None,
        assurance_source=(
            KeysetAssuranceSource.CANARY_VERIFIED_BOUNDS
            if scheduler._assured
            else KeysetAssuranceSource.ORDERED_PREFIX_ONLY
        ),
        planning_requests=scheduler._planning_physical_requests,
        boundary_requests=scheduler._planning_requests[KeysetPhase.BOUNDARY],
        canary_requests=scheduler._planning_requests[KeysetPhase.CANARY],
        anchor_probe_requests=scheduler._planning_requests[KeysetPhase.ANCHOR_PROBE],
        canary_commands=scheduler.trace.phase_commands(KeysetPhase.CANARY),
        canary_rows=scheduler.trace.phase_rows(KeysetPhase.CANARY),
        anchor_probe_commands=getattr(scheduler, "_anchor_probe_commands", 0),
        anchor_count=scheduler._anchor_count,
        empty_anchor_probes=getattr(scheduler, "_empty_anchor_probes", 0),
        probe_rows_discarded=counters.probe_rows_discarded,
        boundary_overlap_rows=counters.boundary_overlap_rows,
        head_page_admitted=scheduler._head_admitted,
        sequential_requests_estimate=(
            scheduler._preselection.sequential_estimate.requests if scheduler._preselection else None
        ),
        selected_requests_estimate=scheduler._selected_estimate,
        head_rows=scheduler._head_rows,
        tail_rows=scheduler._tail_rows,
        interior_span=scheduler._interior_span,
        interior_rows_estimate=scheduler._interior_estimate,
        total_rows_estimate=scheduler._total_estimate,
        density_numerator=scheduler._density_num,
        density_denominator=scheduler._density_den,
        effective_window_width=scheduler._window_width,
        range_window_count=scheduler._window_count,
        target_lanes=scheduler._target_lanes,
        actual_lanes=(
            scheduler._window_count
            if scheduler._selected is KeysetExecutionKind.RANGE
            else len(scheduler._lanes)
            if scheduler._selected is KeysetExecutionKind.PARTITIONED
            else None
        ),
        continuation_count=scheduler._continuations,
        closure_witness_counts=tuple((kind, scheduler._closures[kind]) for kind in ClosureWitness),
        effective_batch_capacity=scheduler.batch_capacity,
        total_hint_requested=scheduler._total_hint.requested,
        total_hint_observed=scheduler._total_hint.observed,
        total_hint_plausible=scheduler._total_hint.plausible,
        total_hint_used=scheduler._total_hint.used,
        trace_retained_by_class=scheduler.trace.class_counts(),
        trace_dropped_by_class=tuple((kind, dropped[kind]) for kind in TraceClass),
    )


__all__ = ["build_scheduler_report", "initial_report_selection"]
