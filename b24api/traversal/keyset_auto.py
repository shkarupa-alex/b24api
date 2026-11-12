"""Pure deterministic automatic keyset execution selector."""

from __future__ import annotations
import itertools
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import TYPE_CHECKING

from b24api.contracts.keyset_execution import (
    KeysetExecutionKind,
    KeysetSelectionReason,
)
from b24api.traversal.keyset_geometry import (
    BoundaryFacts,
    CostEstimate,
    SelectorInputs,
    ceil_div,
    estimates,
    lane_overhead,
    partition_estimate,
    sequential_estimate,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

SEQ_FLOOR_REQUESTS = 3
FAST_GAIN_NUM = 3
FAST_GAIN_DEN = 5
FINISH_REQUESTS = 1
MIN_WINDOW_WIDTH = 2


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
    anchor_probes: int
    interior_span: int = 0
    density_numerator: int = 0
    density_denominator: int = 1


@dataclass(frozen=True, slots=True)
class AnchorFacts:
    """Normalized occupied-anchor probe observations.

    ``empty_below`` pairs a fence (an anchor, or the tail boundary) with the lowest probe guess that
    reached it: a probe returns the first identity above its guess, so none lies between the two.
    """

    anchors: tuple[int, ...]
    empty_probes: int
    empty_below: tuple[tuple[int, int], ...] = ()


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
            partition_estimate.probes,
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
    """Select a final plan after occupied-anchor observations.

    Probes that found identity ranges empty can make partitioning pay off where the lanes at boundary
    density did not. That switch from sequential traversal is taken only while the pages left cover
    the estimate of sequential traversal plus every lane's end pages and the finish page, and the
    partitioned plan's own estimate. Both extrapolate the boundary density, so a budget sized for
    sequential traversal can still fail the partitioned plan where the interior is denser than the
    boundary pages. Likewise the switch spends fewer requests than sequential traversal only by
    estimate; the requests it really sends follow the rows the lanes hold.
    """
    if preselection.plan is not Preselected.PROBE_ANCHORS:
        raise ValueError("finalize requires an anchor-probe preselection")
    boundary = inputs.boundary
    if boundary.head_max is None or boundary.tail_min is None:
        raise ValueError("anchor finalization requires non-empty boundary facts")
    gaps = tuple(itertools.pairwise((boundary.head_max, *anchors.anchors, boundary.tail_min)))
    empty_below = dict(anchors.empty_below)
    lane_count = len(anchors.anchors) + 1
    sequential = preselection.sequential_estimate

    def rows(spans: Iterable[int]) -> tuple[int, ...]:
        numerator, denominator = preselection.density_numerator, preselection.density_denominator
        return tuple(ceil_div(span * numerator, denominator) for span in spans)

    def lanes_of(lane_rows: tuple[int, ...]) -> CostEstimate:
        return inputs.admit(
            partition_estimate(
                inputs,
                lanes=lane_count,
                lane_rows=max(lane_rows),
                probes=0,
                finish_requests=FINISH_REQUESTS,
            ),
        )

    def gains(estimate: CostEstimate) -> bool:
        return estimate.eligible and FAST_GAIN_DEN * estimate.requests <= FAST_GAIN_NUM * sequential.requests

    probed_rows = rows(max(0, max(left, empty_below.get(right, right - 1)) - left) for left, right in gaps)
    part = lanes_of(probed_rows)
    # Sequential traversal of the boundary rows, the anchors and the rows the probes left.
    boundary_rows = preselection.total_rows_estimate - preselection.interior_rows_estimate
    probed_all_rows = boundary_rows + len(anchors.anchors) + sum(probed_rows)
    probed_sequential = sequential_estimate(inputs, probed_all_rows, finish_requests=FINISH_REQUESTS)
    switch_pages = probed_sequential.pages + lane_overhead(inputs, probes=0, lanes=lane_count)[1] + FINISH_REQUESTS
    at_boundary_density = lanes_of(rows(max(0, right - left - 1) for left, right in gaps))
    if gains(part) and (gains(at_boundary_density) or inputs.fits(pages=max(switch_pages, part.pages))):
        reason = (
            KeysetSelectionReason.DEGENERATE_SINGLE_LANE
            if not anchors.anchors
            else KeysetSelectionReason.WIDE_SPAN_PARTITIONING
        )
        return FinalSelection(KeysetExecutionKind.PARTITIONED, reason, lane_count, part.depth or 1, part)
    range_estimate = preselection.range_estimate
    if range_estimate is not None and range_estimate.eligible:
        estimate = inputs.admit(range_estimate)
        if gains(estimate):
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
        sequential,
    )


def normalize_total_hint(*, requested: bool, head: object, tail: object, maximum: int) -> TotalHintState:
    """Normalize two advisory totals without assigning correctness meaning."""
    valid_head = isinstance(head, int) and not isinstance(head, bool) and head >= 0
    valid_tail = isinstance(tail, int) and not isinstance(tail, bool) and tail >= 0
    consistent = valid_head and valid_tail and head == tail
    observed = head if consistent and isinstance(head, int) and not isinstance(head, bool) else None
    plausible = observed is not None and observed <= maximum
    return TotalHintState(requested, observed, plausible, used=False)


__all__ = [
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
