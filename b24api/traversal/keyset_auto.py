"""Pure deterministic automatic keyset execution selector."""

# ruff: noqa: FBT003

from __future__ import annotations
import itertools
from dataclasses import dataclass, replace
from enum import StrEnum

from b24api.contracts.keyset_execution import (
    KeysetExecutionKind,
    KeysetSelectionReason,
)
from b24api.traversal.keyset_costs import BoundaryFacts, CostEstimate, SelectorInputs, ceil_div, estimates

SEQ_FLOOR_REQUESTS = 3
FAST_GAIN_NUM = 3
FAST_GAIN_DEN = 5
FINISH_REQUESTS = 1
CANARY_COMMANDS = 5
MIN_WINDOW_WIDTH = 2
MIN_CANARY_PREFIX = 2


class Preselected(StrEnum):
    """Boundary-only first-stage result."""

    BOUNDARY_ONLY = "boundary_only"
    SEQUENTIAL = "sequential"
    RANGE = "range"
    PROBE_ANCHORS = "probe_anchors"


@dataclass(frozen=True, slots=True)
class Preselection:
    """First-stage automatic selection and auditable estimates."""

    plan: Preselected
    reason: KeysetSelectionReason
    interior_rows_estimate: int
    total_rows_estimate: int
    sequential_estimate: CostEstimate
    range_estimate: CostEstimate | None
    partition_estimate: CostEstimate | None
    interior_span: int = 0
    density_numerator: int = 0
    density_denominator: int = 1


@dataclass(frozen=True, slots=True)
class AnchorFacts:
    """Normalized occupied-anchor probe observations."""

    anchors: tuple[int, ...]
    probe_commands: int
    empty_probes: int


@dataclass(frozen=True, slots=True)
class TotalHintState:
    """Normalized advisory-total evidence."""

    requested: bool
    observed: int | None
    plausible: bool
    used: bool


@dataclass(frozen=True, slots=True)
class FinalSelection:
    """Post-anchor final automatic selection."""

    kind: KeysetExecutionKind
    reason: KeysetSelectionReason
    lane_count: int
    depth: int
    estimate: CostEstimate


def preselect(inputs: SelectorInputs) -> Preselection:  # noqa: PLR0911
    """Apply the frozen ordered automatic preselection rules."""
    sequential, range_estimate, partition_estimate, geometry = estimates(
        inputs,
        canary_commands=CANARY_COMMANDS,
        finish_requests=FINISH_REQUESTS,
    )
    span, numerator, denominator, interior_rows, all_rows = geometry

    def selected(plan: Preselected, reason: KeysetSelectionReason) -> Preselection:
        return Preselection(
            plan,
            reason,
            interior_rows,
            all_rows,
            sequential,
            range_estimate,
            partition_estimate,
            span,
            numerator,
            denominator,
        )

    boundary = inputs.boundary
    if boundary.head_rows == 0:
        return selected(Preselected.BOUNDARY_ONLY, KeysetSelectionReason.EMPTY_SELECTION)
    if boundary.overlapping:
        return selected(Preselected.BOUNDARY_ONLY, KeysetSelectionReason.BOUNDARY_OVERLAP)
    if boundary.adjacent:
        return selected(Preselected.BOUNDARY_ONLY, KeysetSelectionReason.ADJACENT_BOUNDARIES)
    if (
        inputs.effective_page_cap < MIN_CANARY_PREFIX
        or boundary.head_rows < MIN_CANARY_PREFIX
        or boundary.tail_rows < MIN_CANARY_PREFIX
    ):
        return selected(Preselected.SEQUENTIAL, KeysetSelectionReason.SMALL_SELECTION)
    if inputs.batch_capacity < 1 or sequential.requests <= SEQ_FLOOR_REQUESTS:
        return selected(Preselected.SEQUENTIAL, KeysetSelectionReason.SMALL_SELECTION)
    candidates = [estimate for estimate in (range_estimate, partition_estimate) if estimate.eligible]
    best = min(
        candidates,
        key=lambda estimate: (estimate.requests, estimate.kind is not KeysetExecutionKind.RANGE),
        default=None,
    )
    if best is None or FAST_GAIN_DEN * best.requests > FAST_GAIN_NUM * sequential.requests:
        return selected(Preselected.SEQUENTIAL, KeysetSelectionReason.INSUFFICIENT_PREDICTED_GAIN)
    if best.kind is KeysetExecutionKind.RANGE:
        return selected(Preselected.RANGE, KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET)
    return selected(Preselected.PROBE_ANCHORS, KeysetSelectionReason.WIDE_SPAN_PARTITIONING)


def constrain_anchor_preselection(selection: Preselection, *, anchor_capacity: int) -> Preselection:
    """Keep a feasible range candidate when only anchor retention is unavailable."""
    if selection.plan is not Preselected.PROBE_ANCHORS or anchor_capacity > 0:
        return selection
    estimate = selection.range_estimate
    range_wins = (
        estimate is not None
        and estimate.eligible
        and FAST_GAIN_DEN * estimate.requests <= FAST_GAIN_NUM * selection.sequential_estimate.requests
    )
    return replace(
        selection,
        plan=Preselected.RANGE if range_wins else Preselected.SEQUENTIAL,
        reason=(
            KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET
            if range_wins
            else KeysetSelectionReason.INSUFFICIENT_PREDICTED_GAIN
        ),
    )


def finalize(inputs: SelectorInputs, preselection: Preselection, anchors: AnchorFacts) -> FinalSelection:
    """Select a final plan after occupied-anchor observations."""
    if preselection.plan is not Preselected.PROBE_ANCHORS:
        raise ValueError("finalize requires an anchor-probe preselection")
    boundary = inputs.boundary
    if boundary.head_max is None or boundary.tail_min is None:
        raise ValueError("anchor finalization requires non-empty boundary facts")
    fences = (boundary.head_max, *anchors.anchors, boundary.tail_min)
    spans = tuple(max(0, right - left - 1) for left, right in itertools.pairwise(fences))
    lane_count = max(1, len(anchors.anchors) + 1)
    rows = tuple(ceil_div(span * preselection.density_numerator, preselection.density_denominator) for span in spans)
    depth = max(1, *(ceil_div(value, inputs.effective_page_cap) for value in rows))
    part_remaining = ceil_div(lane_count, inputs.batch_capacity) * depth + FINISH_REQUESTS
    part = CostEstimate(
        KeysetExecutionKind.PARTITIONED,
        part_remaining,
        True,
        None,
        None,
        lane_count,
        depth,
        groups=ceil_div(lane_count, inputs.batch_capacity),
    )
    sequential_requests = preselection.sequential_estimate.requests
    if FAST_GAIN_DEN * part_remaining <= FAST_GAIN_NUM * sequential_requests:
        reason = (
            KeysetSelectionReason.DEGENERATE_SINGLE_LANE
            if not anchors.anchors
            else KeysetSelectionReason.WIDE_SPAN_PARTITIONING
        )
        return FinalSelection(KeysetExecutionKind.PARTITIONED, reason, lane_count, depth, part)
    range_estimate = preselection.range_estimate
    if range_estimate is not None and range_estimate.eligible:
        remaining = (range_estimate.groups or 0) * (range_estimate.depth or 1) + FINISH_REQUESTS
        if FAST_GAIN_DEN * remaining <= FAST_GAIN_NUM * sequential_requests:
            estimate = CostEstimate(
                KeysetExecutionKind.RANGE,
                remaining,
                True,
                range_estimate.window_width,
                range_estimate.window_count,
                None,
                range_estimate.depth,
                range_estimate.rows_per_window,
                range_estimate.groups,
            )
            return FinalSelection(
                KeysetExecutionKind.RANGE,
                KeysetSelectionReason.POST_PROBE_RANGE_PREFERRED,
                0,
                estimate.depth or 1,
                estimate,
            )
    return FinalSelection(
        KeysetExecutionKind.SEQUENTIAL,
        KeysetSelectionReason.POST_PROBE_GAIN_LOST,
        0,
        1,
        preselection.sequential_estimate,
    )


def normalize_total_hint(*, requested: bool, head: object, tail: object, maximum: int) -> TotalHintState:
    """Normalize two advisory totals without assigning correctness meaning."""
    valid_head = isinstance(head, int) and not isinstance(head, bool) and head >= 0
    valid_tail = isinstance(tail, int) and not isinstance(tail, bool) and tail >= 0
    consistent = valid_head and valid_tail and head == tail
    observed = head if consistent and isinstance(head, int) and not isinstance(head, bool) else None
    plausible = observed is not None and observed <= maximum
    return TotalHintState(requested, observed, plausible, False)


__all__ = [
    "CANARY_COMMANDS",
    "FAST_GAIN_DEN",
    "FAST_GAIN_NUM",
    "FINISH_REQUESTS",
    "MIN_WINDOW_WIDTH",
    "SEQ_FLOOR_REQUESTS",
    "AnchorFacts",
    "BoundaryFacts",
    "CostEstimate",
    "FinalSelection",
    "Preselected",
    "Preselection",
    "SelectorInputs",
    "TotalHintState",
    "constrain_anchor_preselection",
    "finalize",
    "normalize_total_hint",
    "preselect",
]
