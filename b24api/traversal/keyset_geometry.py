"""Pure integer geometry for fast keysets: boundary density, costs, range windows and partitions."""

from __future__ import annotations
from dataclasses import dataclass, replace
from fractions import Fraction
from typing import TYPE_CHECKING

from b24api.contracts.keyset_execution import (
    AutoKeysetExecution,
    ClosureWitness,
    KeysetExecutionKind,
    KeysetPageCompletion,
    RangeKeysetExecution,
)
from b24api.traversal.keyset_fast_plan import LaneBounds, LaneSpec, LaneState, plan_lanes_from_anchors
from b24api.traversal.keyset_waves import IdentityRun, LaneShape, WaveRules, probe_waves, simulate_lanes, window_shapes

if TYPE_CHECKING:
    from collections.abc import Mapping

_MINIMUM_LANES = 2


@dataclass(frozen=True, slots=True)
class BoundaryFacts:
    """Value-free boundary observations consumed by the selector."""

    head_rows: int
    tail_rows: int
    head_min: int | None
    head_max: int | None
    tail_min: int | None
    tail_max: int | None
    overlapping: bool
    adjacent: bool


@dataclass(frozen=True, slots=True)
class BoundaryDensity:
    """Interior span and observed boundary density, computed once from boundary facts."""

    span: int
    numerator: int
    denominator: int


def boundary_density(facts: BoundaryFacts) -> BoundaryDensity | None:
    """Return the interior span and boundary density, or None while either boundary is empty."""
    if facts.head_min is None or facts.head_max is None or facts.tail_min is None or facts.tail_max is None:
        return None
    return BoundaryDensity(
        max(0, facts.tail_min - facts.head_max - 1),
        facts.head_rows + facts.tail_rows,
        max(1, (facts.head_max - facts.head_min + 1) + (facts.tail_max - facts.tail_min + 1)),
    )


@dataclass(frozen=True, slots=True)
class SelectorInputs:
    """Integer-only selector inputs."""

    boundary: BoundaryFacts
    effective_page_cap: int
    batch_capacity: int
    available_rows: int
    target_lanes: int
    range_window_width: int | None
    completion: KeysetPageCompletion
    advisory_total: int | None
    descending: bool = False
    remaining_requests: int | None = None
    remaining_pages: int | None = None

    @property
    def head_rows(self) -> int:
        """Return the rows of the boundary page output starts with, admitted before any body wave."""
        return self.boundary.tail_rows if self.descending else self.boundary.head_rows

    def body_rows(self, *, anchors: int) -> int:
        """Return the rows the buffer leaves a plan's body waves beside the tail page and ``anchors`` anchors.

        ``available_rows`` is what the buffer left beside both boundary pages; the head page drains
        before the first body wave, the tail page and every retained anchor stay to the end.
        """
        return self.available_rows + self.head_rows - anchors

    def probe_waves(self, probes: int) -> int | None:
        """Return the waves ``probes`` anchor probes take beside the boundary pages, or ``None`` with no room."""
        return probe_waves(
            probes, capacity=self.batch_capacity, free_rows=self.available_rows, reserve=self.effective_page_cap
        )

    def fits(self, *, requests: int = 0, pages: int = 0) -> bool:
        """Return whether ``requests`` and ``pages`` fit what is left of the operation budget."""
        return (self.remaining_requests is None or requests <= self.remaining_requests) and (
            self.remaining_pages is None or pages <= self.remaining_pages
        )

    def admit(self, estimate: CostEstimate) -> CostEstimate:
        """Keep a fast plan eligible only if its minimum cost fits the operation budget left.

        The minimum cost (``min_requests``, ``min_pages``) never extrapolates the interior density, so
        an over-counted interior does not rule a plan out; a plan is refused only when it must fail.
        """
        fits = self.fits(requests=estimate.min_requests, pages=estimate.min_pages)
        return replace(estimate, eligible=estimate.eligible and fits)


@dataclass(frozen=True, slots=True)
class CostEstimate:
    """Deterministic physical-request and logical-page cost of one plan.

    ``requests`` and ``pages`` come from running the body-wave rule over the plan's lanes at the
    estimated rows (``keyset_waves.simulate_lanes``); an ineligible plan carries its minimum cost.
    """

    kind: KeysetExecutionKind
    requests: int
    eligible: bool
    pages: int = 0
    # What the plan must spend whatever the interior holds; the budget check uses only these.
    min_requests: int = 0
    min_pages: int = 0
    # Anchor probes still to send, counted in the estimate.
    probes: int = 0
    window_width: int | None = None
    window_count: int | None = None
    lane_count: int | None = None


ANCHOR_PROBE_WAVES = 2
# Every fast plan ends with one finish request, which reads the tail boundary's remainder.
FINISH_REQUESTS = 1


def ceil_div(numerator: int, denominator: int) -> int:
    """Return integer ceiling division."""
    return (numerator + denominator - 1) // denominator


def _priced(
    inputs: SelectorInputs,
    estimate: CostEstimate,
    shapes: tuple[LaneShape, ...],
    *,
    anchors: int,
    request_limit: int,
) -> CostEstimate:
    """Price ``estimate`` by running the body-wave rule over ``shapes``, after its probes and before its finish.

    The estimate's anchor probes (``probes``) go first, in their waves; ``anchors`` anchor
    rows stay in the buffer beside the lanes. Range windows slide: no more of them are open than a
    wave carries. A plan whose minimum cost does not fit the budget left, or whose body the buffer
    cannot hold or that spends more requests than ``request_limit``, stays ineligible at its minimum cost.
    """
    admitted = inputs.admit(estimate)
    if not admitted.eligible:
        return admitted
    # An eligible plan's probes fit the buffer (``partition_estimate``).
    planning_waves = inputs.probe_waves(estimate.probes) or 0
    rules = WaveRules(
        inputs.effective_page_cap,
        inputs.completion,
        inputs.batch_capacity,
        inputs.body_rows(anchors=anchors),
    )
    body = simulate_lanes(
        shapes,
        rules,
        sliding=estimate.kind is KeysetExecutionKind.RANGE,
        request_limit=max(0, request_limit - planning_waves - FINISH_REQUESTS),
    )
    if body is None:
        return replace(admitted, eligible=False)
    return replace(
        admitted,
        requests=planning_waves + body.requests + FINISH_REQUESTS,
        pages=estimate.probes + body.pages + FINISH_REQUESTS,
    )


def range_estimate(
    inputs: SelectorInputs,
    density: BoundaryDensity,
    runs: tuple[IdentityRun, ...],
    *,
    request_limit: int,
) -> CostEstimate:
    """Estimate the range windows the runtime builds at the boundary ``density``, as it reads them.

    The windows are known before any of them is read: their count and width are exact. ``runs``
    spread the interior's estimated rows over its identity values, in ascending order; each window
    holds the rows of the values it spans. Every window is read at least once, at most
    ``batch_capacity`` a wave.
    """
    span, numerator, denominator = density.span, density.numerator, density.denominator
    width = range_window_width(
        page_cap=inputs.effective_page_cap,
        span=span,
        density_numerator=numerator,
        density_denominator=denominator,
        explicit=inputs.range_window_width,
    )
    windows = ceil_div(span, width - 1) if span else 0
    minimum = ceil_div(windows, max(1, inputs.batch_capacity)) + FINISH_REQUESTS
    estimate = CostEstimate(
        KeysetExecutionKind.RANGE,
        minimum,
        eligible=inputs.batch_capacity > 0,
        pages=windows + FINISH_REQUESTS,
        min_requests=minimum,
        min_pages=windows + FINISH_REQUESTS,
        window_width=width,
        window_count=windows,
    )
    shapes = window_shapes(runs, step=width - 1)
    return _priced(
        inputs,
        estimate,
        shapes[::-1] if inputs.descending else shapes,
        anchors=0,
        request_limit=request_limit,
    )


def partition_estimate(
    inputs: SelectorInputs,
    shapes: tuple[LaneShape, ...],
    *,
    probes: int,
    anchors: int,
    request_limit: int,
) -> CostEstimate:
    """Estimate ``probes`` anchor probes still to send, then the lanes of ``shapes``, as the runtime reads them.

    Every probe is a page, in the waves ``SelectorInputs.probe_waves`` gives; ``anchors`` anchor rows
    stay in the buffer beside the lanes. The minimum cost is every probe, and one page for each lane:
    all of them once the anchors are known (no probe left), only one while they come from probes still
    to send.
    """
    capacity = max(1, inputs.batch_capacity)
    lanes = sum(shape.count for shape in shapes)
    planning_waves = inputs.probe_waves(probes)
    known_lanes = 1 if probes else lanes
    # Probes the buffer cannot carry leave the plan ineligible; its minimum then counts a wave a probe.
    minimum = (probes if planning_waves is None else planning_waves) + ceil_div(known_lanes, capacity) + FINISH_REQUESTS
    estimate = CostEstimate(
        KeysetExecutionKind.PARTITIONED,
        minimum,
        eligible=inputs.batch_capacity > 0 and planning_waves is not None,
        pages=probes + known_lanes + FINISH_REQUESTS,
        min_requests=minimum,
        min_pages=probes + known_lanes + FINISH_REQUESTS,
        probes=probes,
        lane_count=lanes,
    )
    return _priced(
        inputs,
        estimate,
        shapes,
        anchors=anchors,
        request_limit=request_limit,
    )


def _uniform_partition(
    inputs: SelectorInputs,
    span: int,
    interior_rows: int,
    *,
    probes: int,
    request_limit: int,
) -> CostEstimate:
    """Estimate ``probes`` evenly spaced probes and the ``probes + 1`` lanes they leave, all alike."""
    lanes = probes + 1
    shape = LaneShape(ceil_div(interior_rows, lanes), ceil_div(span, lanes), lanes)
    return partition_estimate(
        inputs,
        (shape,),
        probes=probes,
        anchors=probes,
        request_limit=request_limit,
    )


def anchor_probe_count(
    inputs: SelectorInputs,
    span: int,
    interior_rows: int,
    *,
    request_limit: int,
) -> CostEstimate:
    """Return the partition estimate of the probes automatic partitioning sends.

    ``target_lanes`` probes, or as many as ``ANCHOR_PROBE_WAVES`` waves carry (no more than the
    buffer retains beside one page) when the cost model prices those finer lanes cheaper and their
    estimate fits what is left of the operation budget. Finer probes also measure the interior
    finer: the plan chosen after them may well be another.
    """
    target = inputs.target_lanes
    coarse = _uniform_partition(inputs, span, interior_rows, probes=target, request_limit=request_limit)
    ceiling = min(inputs.batch_capacity * ANCHOR_PROBE_WAVES, inputs.available_rows - inputs.effective_page_cap)
    if ceiling <= target:
        return coarse
    finer = _uniform_partition(inputs, span, interior_rows, probes=ceiling, request_limit=request_limit)
    cheaper = finer.eligible and (not coarse.eligible or finer.requests < coarse.requests)
    return finer if cheaper and inputs.fits(requests=finer.requests, pages=finer.pages) else coarse


def selected_range_geometry(
    *,
    execution: RangeKeysetExecution | AutoKeysetExecution,
    page_cap: int,
    density: BoundaryDensity,
) -> tuple[int, int]:
    """Return selected range width and count without materializing windows."""
    explicit = execution.window_width if isinstance(execution, RangeKeysetExecution) else execution.range_window_width
    width = range_window_width(
        page_cap=page_cap,
        span=density.span,
        density_numerator=density.numerator,
        density_denominator=density.denominator,
        explicit=explicit,
    )
    return width, ceil_div(density.span, width - 1) if density.span else 0


def _geometry(inputs: SelectorInputs) -> tuple[int, int, int, int, int]:
    boundary = inputs.boundary
    if boundary.head_rows == 0:
        return 0, 0, 1, 0, 0
    if boundary.overlapping:
        rows = max(boundary.head_rows, boundary.tail_rows)
        return 0, rows, max(1, rows), 0, rows
    density = boundary_density(boundary)
    if density is None:
        rows = boundary.head_rows + boundary.tail_rows
        return 0, 0, 1, rows, rows
    span, numerator, denominator = density.span, density.numerator, density.denominator
    density_rows = ceil_div(span * numerator, denominator)
    advisory_rows = 0
    advisory = inputs.advisory_total
    if isinstance(advisory, int) and not isinstance(advisory, bool) and 0 <= advisory <= numerator + span:
        advisory_rows = max(0, advisory - numerator)
    interior_rows = max(density_rows, advisory_rows)
    return span, numerator, denominator, interior_rows, numerator + interior_rows


def estimates(
    inputs: SelectorInputs,
) -> tuple[CostEstimate, CostEstimate, CostEstimate, tuple[int, int, int, int, int]]:
    """Price sequential traversal, range windows and automatic partitioning from integer observations.

    A fast plan that would spend more requests than sequential traversal is not priced further: it
    stays ineligible.
    """
    span, numerator, denominator, interior_rows, all_rows = _geometry(inputs)
    sequential = sequential_estimate(inputs, all_rows)
    geometry = (span, numerator, denominator, interior_rows, all_rows)
    if inputs.batch_capacity < 1:
        unavailable = CostEstimate(KeysetExecutionKind.RANGE, 0, eligible=False)
        partition = CostEstimate(KeysetExecutionKind.PARTITIONED, 0, eligible=False)
        return sequential, unavailable, partition, geometry
    windows = range_estimate(
        inputs,
        BoundaryDensity(span, numerator, denominator),
        (IdentityRun(span, Fraction(interior_rows, span)),) if span else (),
        request_limit=sequential.requests,
    )
    partition = anchor_probe_count(
        inputs,
        span,
        interior_rows,
        request_limit=sequential.requests,
    )
    return sequential, windows, partition, geometry


def sequential_estimate(inputs: SelectorInputs, all_rows: int) -> CostEstimate:
    """Estimate sequential traversal of ``all_rows`` rows past the boundary page already admitted."""
    requests = max(1, ceil_div(max(0, all_rows - inputs.head_rows), inputs.effective_page_cap)) + FINISH_REQUESTS
    return CostEstimate(KeysetExecutionKind.SEQUENTIAL, requests, eligible=True, pages=requests)


def range_window_width(
    *,
    page_cap: int,
    span: int,
    density_numerator: int,
    density_denominator: int,
    explicit: int | None,
) -> int:
    """Return the integer-only window width: a page of rows at the measured density, unless explicit.

    A window spans at least a page of identity values, so a dense window closes on its one page.
    """
    if explicit is not None:
        return explicit
    density_width = page_cap * density_denominator // max(1, density_numerator)
    return max(2, min(max(density_width, page_cap), span + 1))


def closure_witness(
    *,
    bounds: LaneBounds,
    cursor: int,
    identities: tuple[int, ...],
    page_cap: int,
    completion: KeysetPageCompletion,
) -> ClosureWitness | None:
    """Classify independently sufficient lane closure evidence."""
    if not identities:
        return ClosureWitness.EMPTY
    upper = bounds.upper_exclusive
    if (
        upper is not None
        and len(identities) == upper - cursor - 1
        and identities[0] == cursor + 1
        and identities[-1] == upper - 1
    ):
        return ClosureWitness.LATTICE_FULL
    if upper is not None and identities[-1] == upper - 1:
        return ClosureWitness.TOP
    if completion is KeysetPageCompletion.SHORT_PAGE_EXHAUSTS and len(identities) < page_cap:
        return ClosureWitness.SHORT_PAGE
    return None


def descending_closure_witness(
    lane: LaneState,
    identities: tuple[int, ...],
    *,
    completion: KeysetPageCompletion,
    page_cap: int,
) -> ClosureWitness | None:
    """Classify descending closure with exact lattice proof taking precedence."""
    if not identities:
        return ClosureWitness.EMPTY
    lower = lane.spec.bounds.lower_exclusive
    cursor = lane.cursor
    if (
        lower is not None
        and cursor is not None
        and len(identities) == cursor - lower - 1
        and identities[0] == cursor - 1
        and identities[-1] == lower + 1
    ):
        return ClosureWitness.LATTICE_FULL
    if lower is not None and identities[-1] == lower + 1:
        return ClosureWitness.TOP
    if completion is KeysetPageCompletion.SHORT_PAGE_EXHAUSTS and len(identities) < page_cap:
        return ClosureWitness.SHORT_PAGE
    return None


def anchor_guesses(*, lo: int, upper_exclusive: int, target_lanes: int) -> tuple[int, ...]:
    """Return the exact deduplicated integer-floor anchor guesses.

    ``target_lanes`` guesses split the interior into ``target_lanes`` equal identity ranges, the first
    at ``lo``: its probe finds the first interior identity and samples the rows above it, and the
    lane below that identity is known empty, so it costs no request.
    """
    if any(not isinstance(value, int) or isinstance(value, bool) for value in (lo, upper_exclusive, target_lanes)):
        raise TypeError("anchor guess operands must be exact integers")
    if target_lanes < _MINIMUM_LANES:
        raise ValueError("target_lanes must be at least 2")
    span = max(0, upper_exclusive - lo - 1)
    return tuple(sorted({lo + (index * span) // target_lanes for index in range(target_lanes)}))


def normalize_anchors(*, lo: int, upper_exclusive: int, anchors: tuple[int, ...]) -> tuple[int, ...]:
    """Validate and normalize occupied anchors inside the captured fence."""
    if any(not isinstance(value, int) or isinstance(value, bool) for value in anchors):
        raise TypeError("anchors must be exact integers")
    return tuple(sorted({value for value in anchors if lo < value < upper_exclusive}))


def partition_lane_specs(
    *,
    lo: int,
    upper_exclusive: int,
    anchors: tuple[int, ...],
    descending: bool,
    empty_below: Mapping[int, int],
) -> tuple[LaneSpec, ...]:
    """Plan partition lanes between the boundary fences in output order.

    ``empty_below`` maps a fence to the lowest guess of a probe that found it: a lane whose lower
    fence is at or above that guess holds no identity and is ``known_empty``.
    """
    specs = tuple(
        replace(spec, known_empty=empty_below.get(upper, upper) <= lower)
        if (lower := spec.bounds.lower_exclusive) is not None and (upper := spec.bounds.upper_exclusive) is not None
        else spec
        for spec in plan_lanes_from_anchors(lo=lo, upper_exclusive=upper_exclusive, anchors=anchors)
    )
    if not descending:
        return specs
    return tuple(
        replace(
            spec,
            ordinal=ordinal,
            descending=True,
            retained_upper_anchor=spec.bounds.lower_exclusive if spec.bounds.lower_exclusive != lo else None,
        )
        for ordinal, spec in enumerate(reversed(specs))
    )


__all__ = [
    "ANCHOR_PROBE_WAVES",
    "BoundaryDensity",
    "BoundaryFacts",
    "CostEstimate",
    "SelectorInputs",
    "anchor_guesses",
    "anchor_probe_count",
    "boundary_density",
    "ceil_div",
    "closure_witness",
    "descending_closure_witness",
    "estimates",
    "normalize_anchors",
    "partition_estimate",
    "partition_lane_specs",
    "range_estimate",
    "range_window_width",
    "selected_range_geometry",
    "sequential_estimate",
]
