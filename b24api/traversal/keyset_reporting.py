"""Immutable aggregate reporting for the fast keyset runtime."""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.contracts.keyset_execution import (
    AutoKeysetExecution,
    ClosureWitness,
    KeysetAssuranceSource,
    KeysetExecutionKind,
    KeysetPhase,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    TraceClass,
)
from b24api.contracts.report import KeysetExecutionReport

if TYPE_CHECKING:
    from b24api.traversal.keyset_observation import FastTraceRecorder
    from b24api.traversal.keyset_ordered_admission import FastCounters
    from b24api.traversal.keyset_plan import PlanningFacts
    from b24api.traversal.keyset_transaction_contract import KeysetTransactionState


def build_keyset_report(  # noqa: PLR0913 - the report joins planner evidence with runtime counters
    *,
    execution: RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution,
    facts: PlanningFacts,
    transactions: KeysetTransactionState,
    counters: FastCounters,
    trace: FastTraceRecorder,
    batch_capacity: int,
    head_admitted: bool,
    window_width: int | None,
    window_count: int | None,
) -> KeysetExecutionReport:
    """Freeze redacted planner, provenance, closure, and trace aggregates."""
    _, dropped = trace.snapshot()
    selected = facts.selected
    return KeysetExecutionReport(
        requested_kind=(
            KeysetExecutionKind.AUTO
            if isinstance(execution, AutoKeysetExecution)
            else KeysetExecutionKind.PARTITIONED
            if isinstance(execution, PartitionedKeysetExecution)
            else KeysetExecutionKind.RANGE
        ),
        selected_kind=selected,
        preselection_reason=facts.reason,
        final_selection_reason=facts.final.reason if facts.final is not None else None,
        assurance_source=(
            KeysetAssuranceSource.CALLER_ASSERTED_BOUNDS
            if selected in {KeysetExecutionKind.RANGE, KeysetExecutionKind.PARTITIONED}
            else KeysetAssuranceSource.ORDERED_PREFIX_ONLY
        ),
        planning_requests=transactions.planning_physical_requests,
        boundary_requests=transactions.planning_requests[KeysetPhase.BOUNDARY],
        anchor_probe_requests=transactions.planning_requests[KeysetPhase.ANCHOR_PROBE],
        anchor_probe_commands=facts.anchor_probe_commands,
        anchor_count=facts.anchor_count,
        empty_anchor_probes=facts.empty_anchor_probes,
        probe_rows_discarded=counters.probe_rows_discarded,
        boundary_overlap_rows=counters.boundary_overlap_rows,
        head_page_admitted=head_admitted,
        sequential_requests_estimate=(facts.preselection.sequential_estimate.requests if facts.preselection else None),
        selected_requests_estimate=facts.selected_estimate,
        head_rows=facts.head_rows,
        tail_rows=facts.tail_rows,
        interior_span=facts.interior_span,
        interior_rows_estimate=facts.interior_estimate,
        total_rows_estimate=facts.total_estimate,
        density_numerator=facts.density_numerator,
        density_denominator=facts.density_denominator,
        effective_window_width=window_width,
        range_window_count=window_count,
        target_lanes=facts.target_lanes,
        actual_lanes=(
            window_count
            if selected is KeysetExecutionKind.RANGE
            else len(transactions.lanes)
            if selected is KeysetExecutionKind.PARTITIONED
            else None
        ),
        continuation_count=transactions.continuations,
        closure_witness_counts=tuple((kind, transactions.closures[kind]) for kind in ClosureWitness),
        effective_batch_capacity=batch_capacity,
        total_hint_requested=facts.total_hint.requested,
        total_hint_observed=facts.total_hint.observed,
        total_hint_plausible=facts.total_hint.plausible,
        total_hint_used=facts.total_hint.used,
        trace_retained_by_class=trace.class_counts(),
        trace_dropped_by_class=tuple((kind, dropped[kind]) for kind in TraceClass),
    )


__all__ = ["build_keyset_report"]
