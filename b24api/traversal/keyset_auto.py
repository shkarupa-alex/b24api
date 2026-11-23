"""Pure deterministic automatic keyset execution selector."""

from __future__ import annotations
import itertools
from dataclasses import dataclass, replace
from enum import StrEnum
from fractions import Fraction
from math import ceil

from b24api.contracts.keyset_execution import (
    KeysetExecutionKind,
    KeysetSelectionReason,
)
from b24api.traversal.keyset_geometry import (
    BoundaryDensity,
    BoundaryFacts,
    CostEstimate,
    SelectorInputs,
    estimates,
    partition_estimate,
    range_estimate,
    sequential_estimate,
)
from b24api.traversal.keyset_waves import IdentityRun, LaneShape

SEQ_FLOOR_REQUESTS = 3
FAST_GAIN_NUM = 3
FAST_GAIN_DEN = 5


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
    # Per anchor, what its probe page showed above it: ``(fence, rows, ids)``.
    above: tuple[tuple[int, int, int], ...] = ()


@dataclass(frozen=True, slots=True)
class TotalHintState:
    """Normalized advisory-total evidence."""

    requested: bool
    observed: int | None
    plausible: bool
    used: bool


@dataclass(frozen=True, slots=True)
class FinalSelection:
    """Post-anchor final automatic selection, and the estimates it was chosen on."""

    kind: KeysetExecutionKind
    reason: KeysetSelectionReason
    estimate: CostEstimate
    # Sequential traversal of the rows the probes measured, every option was weighed against.
    sequential: CostEstimate
    interior_rows: int


def pays_off(inputs: SelectorInputs, fast: CostEstimate, sequential: CostEstimate) -> bool:
    """Return whether a fast plan is worth taking over sequential traversal.

    It must be eligible and spend at most ``FAST_GAIN_NUM / FAST_GAIN_DEN`` of the requests of
    sequential traversal. Under a budget too tight for its estimate it is taken only while the
    estimate of sequential traversal does not fit either: both extrapolate the same density.
    """
    return (
        fast.eligible
        and FAST_GAIN_DEN * fast.requests <= FAST_GAIN_NUM * sequential.requests
        and (
            inputs.fits(requests=fast.requests, pages=fast.pages)
            or not inputs.fits(requests=sequential.requests, pages=sequential.pages)
        )
    )


def preselect(inputs: SelectorInputs) -> Preselection:  # noqa: PLR0911
    """Apply the frozen ordered automatic preselection rules."""
    sequential, range_estimate, partition_estimate, geometry = estimates(inputs)
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
    candidates = [
        estimate for estimate in (range_estimate, partition_estimate) if pays_off(inputs, estimate, sequential)
    ]
    best = min(
        candidates,
        key=lambda estimate: (estimate.requests, estimate.kind is not KeysetExecutionKind.RANGE),
        default=sequential,
    )
    # Every estimate extrapolates the boundary density; the anchor probes measure the interior. A plan
    # estimated above what any probing plan must spend is not taken blind: the probes go first and the
    # plan is chosen on what they saw. Range windows over an interior emptier than the boundary pages
    # would spend their whole window count; sequential traversal would miss the lanes that skip it.
    if best.requests > partition_estimate.min_requests and inputs.fits(
        requests=partition_estimate.min_requests, pages=partition_estimate.min_pages
    ):
        return selected(Preselected.PROBE_ANCHORS, KeysetSelectionReason.WIDE_SPAN_PARTITIONING)
    if best.kind is KeysetExecutionKind.RANGE:
        return selected(Preselected.RANGE, KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET)
    return selected(Preselected.SEQUENTIAL, KeysetSelectionReason.INSUFFICIENT_PREDICTED_GAIN)


def constrain_anchor_preselection(
    inputs: SelectorInputs, selection: Preselection, *, anchor_capacity: int
) -> Preselection:
    """Keep a feasible range candidate when only anchor retention is unavailable."""
    if selection.plan is not Preselected.PROBE_ANCHORS or anchor_capacity > 0:
        return selection
    estimate = selection.range_estimate
    range_wins = estimate is not None and pays_off(inputs, estimate, selection.sequential_estimate)
    return replace(
        selection,
        plan=Preselected.RANGE if range_wins else Preselected.SEQUENTIAL,
        reason=(
            KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET
            if range_wins
            else KeysetSelectionReason.INSUFFICIENT_PREDICTED_GAIN
        ),
    )


def probed_runs(boundary: BoundaryFacts, anchors: AnchorFacts) -> tuple[tuple[LaneShape, ...], tuple[IdentityRun, ...]]:
    """Return the lanes the anchors fence and the identity runs of the interior, both in ascending order.

    A lane opens at its lower fence and holds rows up to the guess of the probe that found its upper
    fence: the values from that guess up are empty. Its rows are priced at the density measured just
    above its lower fence: the head boundary page's for the first lane, the anchor's probe page for
    the others. A page of one row measures no span unless a short page exhausts what it reads, when
    it proves the lane empty up to its fence; a lane whose anchor's page measured no span is priced at
    the density of both boundary pages together. A cluster that no anchor opens and no probe lands
    in goes unseen.
    """
    head_min, head_max, tail_min, tail_max = boundary.head_min, boundary.head_max, boundary.tail_min, boundary.tail_max
    if head_min is None or head_max is None or tail_min is None or tail_max is None:
        raise ValueError("anchor finalization requires non-empty boundary facts")
    empty_below = dict(anchors.empty_below)
    above = {fence: Fraction(rows, ids) for fence, rows, ids in anchors.above if ids}
    spread = Fraction(boundary.head_rows + boundary.tail_rows, (head_max - head_min + 1) + (tail_max - tail_min + 1))
    density = Fraction(boundary.head_rows, head_max - head_min + 1)
    lanes: list[LaneShape] = []
    runs: list[IdentityRun] = []
    for left, right in itertools.pairwise((head_max, *anchors.anchors, tail_min)):
        if left != head_max:
            density = above.get(left, spread)
            runs.append(IdentityRun(1, Fraction(1)))
        ids = max(0, right - left - 1)
        unknown = min(ids, max(0, empty_below.get(right, right - 1) - left))
        lanes.append(LaneShape(min(unknown, ceil(unknown * density)), ids))
        runs.extend((IdentityRun(unknown, density), IdentityRun(ids - unknown, Fraction(0))))
    return tuple(lanes), tuple(run for run in runs if run.ids)


def finalize(inputs: SelectorInputs, preselection: Preselection, anchors: AnchorFacts) -> FinalSelection:
    """Select a final plan after occupied-anchor observations.

    The probes measure the interior (``probed_runs``): the rows of each lane, and of each identity
    run range windows would read. Partitioning, range windows and sequential traversal are each
    priced over those rows, with nothing left to probe; the cheaper of partitioning and range windows
    that pays off against the sequential estimate is taken (partitioning, whose lanes the anchors
    already fence, when they cost as many requests), and otherwise sequential traversal
    carrying that estimate. Range windows keep the width the boundary density gives them, as the
    runtime builds them.
    """
    if preselection.plan is not Preselected.PROBE_ANCHORS:
        raise ValueError("finalize requires an anchor-probe preselection")
    lanes, runs = probed_runs(inputs.boundary, anchors)
    interior_rows = len(anchors.anchors) + sum(lane.rows for lane in lanes)
    # Sequential traversal of the boundary rows, the anchors and the rows the probes left.
    boundary_rows = preselection.total_rows_estimate - preselection.interior_rows_estimate
    sequential = sequential_estimate(inputs, boundary_rows + interior_rows)
    part = partition_estimate(
        inputs,
        lanes[::-1] if inputs.descending else lanes,
        probes=0,
        anchors=len(anchors.anchors),
        request_limit=sequential.requests,
    )
    windows = range_estimate(
        inputs,
        BoundaryDensity(preselection.interior_span, preselection.density_numerator, preselection.density_denominator),
        runs,
        request_limit=sequential.requests,
    )
    options = [estimate for estimate in (part, windows) if pays_off(inputs, estimate, sequential)]
    best = min(options, key=lambda estimate: (estimate.requests, estimate is not part), default=None)
    if best is None:
        kind, reason, chosen = KeysetExecutionKind.SEQUENTIAL, KeysetSelectionReason.POST_PROBE_GAIN_LOST, sequential
    elif best is windows:
        kind, reason, chosen = KeysetExecutionKind.RANGE, KeysetSelectionReason.POST_PROBE_RANGE_PREFERRED, windows
    else:
        kind, chosen = KeysetExecutionKind.PARTITIONED, part
        reason = (
            KeysetSelectionReason.DEGENERATE_SINGLE_LANE
            if not anchors.anchors
            else KeysetSelectionReason.WIDE_SPAN_PARTITIONING
        )
    return FinalSelection(kind, reason, chosen, sequential, interior_rows)


def normalize_total_hint(*, requested: bool, head: object, tail: object, maximum: int) -> TotalHintState:
    """Normalize two advisory totals without assigning correctness meaning.

    A total the server sends unasked is reported as observed but never plausible, so it steers no plan.
    """
    valid_head = isinstance(head, int) and not isinstance(head, bool) and head >= 0
    valid_tail = isinstance(tail, int) and not isinstance(tail, bool) and tail >= 0
    consistent = valid_head and valid_tail and head == tail
    observed = head if consistent and isinstance(head, int) and not isinstance(head, bool) else None
    plausible = requested and observed is not None and observed <= maximum
    return TotalHintState(requested, observed, plausible, used=False)


__all__ = [
    "FAST_GAIN_DEN",
    "FAST_GAIN_NUM",
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
    "pays_off",
    "preselect",
    "probed_runs",
]
