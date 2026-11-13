"""Pure integer geometry for fast keysets: boundary density, costs, range windows and partitions."""

from __future__ import annotations
from dataclasses import dataclass, replace

from b24api.contracts.keyset_execution import (
    AutoKeysetExecution,
    ClosureWitness,
    KeysetExecutionKind,
    KeysetPageCompletion,
    RangeKeysetExecution,
)
from b24api.traversal.keyset_fast_plan import LaneBounds, LaneSpec, LaneState, plan_lanes_from_anchors

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
    max_range_waves: int
    range_window_width: int | None
    completion: KeysetPageCompletion
    advisory_total: int | None
    descending: bool = False
    remaining_requests: int | None = None
    remaining_pages: int | None = None

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
    """Deterministic physical-request cost estimate."""

    kind: KeysetExecutionKind
    requests: int
    eligible: bool
    window_width: int | None = None
    window_count: int | None = None
    lane_count: int | None = None
    depth: int | None = None
    rows_per_window: int | None = None
    groups: int | None = None
    planning_waves: int | None = None
    pages: int = 0
    # What the plan must spend whatever the interior holds; the budget check uses only these.
    min_requests: int = 0
    min_pages: int = 0
    # Anchor probes still to send, counted in the estimate.
    probes: int = 0


ANCHOR_PROBE_WAVES = 2


def ceil_div(numerator: int, denominator: int) -> int:
    """Return integer ceiling division."""
    return (numerator + denominator - 1) // denominator


def lane_end_pages(completion: KeysetPageCompletion) -> int:
    """Return the pages the estimates assume a lane spends past its full pages.

    The page the lane ends on, assumed not full, and an empty confirmation page where the completion
    may need one to close a lane. A lane whose last page reaches its upper fence closes without the
    confirmation, so the assumption usually over-counts dense lanes.
    """
    return 1 + int(completion is KeysetPageCompletion.EMPTY_CONFIRMATION)


def lane_overhead(inputs: SelectorInputs, *, probes: int, lanes: int) -> tuple[int, int]:
    """Return the requests and logical pages the estimates add to the full pages of the rows.

    Every probe is a page, ``batch_capacity`` probes a wave; every lane is assumed to spend its end
    pages, every group of ``batch_capacity`` lanes as many waves.
    """
    end, capacity = lane_end_pages(inputs.completion), inputs.batch_capacity
    return (
        ceil_div(probes, capacity) + ceil_div(lanes, capacity) * end,
        probes + lanes * end,
    )


def anchor_probe_count(inputs: SelectorInputs, interior_rows: int, *, finish_requests: int) -> int:
    """Return how many anchors automatic partitioning probes.

    ``target_lanes``, or more when lanes that many would hold more rows than a full wave of lanes can
    keep in the row buffer beside the retained anchors: up to ``ANCHOR_PROBE_WAVES`` waves of probes
    and as many anchors as the buffer retains beside one page. Every extra probe and every extra lane
    spends pages the ``target_lanes`` plan does not, so the finer lanes are kept only while what is
    left of the operation budget covers the ``target_lanes`` plan's estimate plus that difference, and
    the finer plan's own estimate, which rounds its lanes' rows to pages on its own.
    """
    cap, capacity, target = inputs.effective_page_cap, inputs.batch_capacity, inputs.target_lanes
    if capacity < 1:
        return target  # no wave fits, so no plan partitions
    ceiling = max(target, min(capacity * ANCHOR_PROBE_WAVES, inputs.available_rows - cap))
    lane_room = (inputs.available_rows - ceiling - cap) // max(1, capacity - 1)
    probes = min(ceiling, max(target, ceil_div(interior_rows, max(cap, lane_room))))
    if probes == target:
        return target
    coarse = _partition_plan(inputs, interior_rows, probes=target, finish_requests=finish_requests)
    finer = _partition_plan(inputs, interior_rows, probes=probes, finish_requests=finish_requests)
    finer_requests, finer_pages = lane_overhead(inputs, probes=probes, lanes=probes + 1)
    coarse_requests, coarse_pages = lane_overhead(inputs, probes=target, lanes=target + 1)
    fits = inputs.fits(
        requests=max(finer.requests, coarse.requests + finer_requests - coarse_requests),
        pages=max(finer.pages, coarse.pages + finer_pages - coarse_pages),
    )
    return probes if fits else target


def _partition_plan(inputs: SelectorInputs, interior_rows: int, *, probes: int, finish_requests: int) -> CostEstimate:
    lanes = probes + 1
    return partition_estimate(
        inputs,
        lanes=lanes,
        lane_rows=ceil_div(interior_rows, lanes),
        probes=probes,
        finish_requests=finish_requests,
    )


def selected_range_geometry(
    *,
    execution: RangeKeysetExecution | AutoKeysetExecution,
    completion: KeysetPageCompletion,
    page_cap: int,
    density: BoundaryDensity,
) -> tuple[int, int]:
    """Return selected range width and count without materializing windows."""
    explicit = execution.window_width if isinstance(execution, RangeKeysetExecution) else execution.range_window_width
    width = range_window_width(
        completion=completion,
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
    *,
    finish_requests: int,
) -> tuple[CostEstimate, CostEstimate, CostEstimate, tuple[int, int, int, int, int]]:
    """Compute every frozen candidate formula from integer observations."""
    span, numerator, denominator, interior_rows, all_rows = _geometry(inputs)
    cap, capacity = inputs.effective_page_cap, inputs.batch_capacity
    sequential = sequential_estimate(inputs, all_rows, finish_requests=finish_requests)
    if capacity < 1:
        unavailable = CostEstimate(KeysetExecutionKind.RANGE, 0, eligible=False)
        partition = CostEstimate(KeysetExecutionKind.PARTITIONED, 0, eligible=False)
        return sequential, unavailable, partition, (span, numerator, denominator, interior_rows, all_rows)
    width = range_window_width(
        completion=inputs.completion,
        page_cap=cap,
        span=span,
        density_numerator=numerator,
        density_denominator=denominator,
        explicit=inputs.range_window_width,
    )
    windows = (span + width - 2) // (width - 1) if span else 0
    rows_per_window = ceil_div((width - 1) * numerator, denominator)
    # A window fills the pages before the one holding its last row, then spends a lane's end pages.
    range_depth = (max(1, rows_per_window) - 1) // cap + lane_end_pages(inputs.completion)
    groups = ceil_div(windows, capacity) if windows else 0
    range_estimate = inputs.admit(
        CostEstimate(
            KeysetExecutionKind.RANGE,
            groups * range_depth + finish_requests,
            groups * range_depth <= inputs.max_range_waves,
            width,
            windows,
            None,
            range_depth,
            rows_per_window,
            groups,
            0,
            pages=windows * range_depth + finish_requests,
            # Every window is read at least once, at most ``batch_capacity`` windows a wave.
            min_requests=groups + finish_requests,
            min_pages=windows + finish_requests,
        ),
    )
    probes = anchor_probe_count(inputs, interior_rows, finish_requests=finish_requests)
    partition = inputs.admit(_partition_plan(inputs, interior_rows, probes=probes, finish_requests=finish_requests))
    return sequential, range_estimate, partition, (span, numerator, denominator, interior_rows, all_rows)


def sequential_estimate(inputs: SelectorInputs, all_rows: int, *, finish_requests: int) -> CostEstimate:
    """Estimate sequential traversal of ``all_rows`` rows past the boundary page already admitted."""
    admitted_head_rows = inputs.boundary.tail_rows if inputs.descending else inputs.boundary.head_rows
    requests = max(1, ceil_div(max(0, all_rows - admitted_head_rows), inputs.effective_page_cap)) + finish_requests
    return CostEstimate(KeysetExecutionKind.SEQUENTIAL, requests, eligible=True, pages=requests)


def partition_estimate(
    inputs: SelectorInputs,
    *,
    lanes: int,
    lane_rows: int,
    probes: int,
    finish_requests: int,
) -> CostEstimate:
    """Estimate ``probes`` anchor probes still to send, then ``lanes`` lanes of up to ``lane_rows`` rows.

    Every wave advances up to ``batch_capacity`` lanes by one page, so each group of that many lanes
    takes as many waves as its deepest lane has pages: its full pages and its end pages. That holds
    while a group of lanes keeps its rows in the row buffer; behind a smaller buffer, later lanes wait
    for the earlier ones to close and the selection takes more waves than estimated.

    The minimum cost is every probe, and one page for each lane: all ``lanes`` once the anchors are
    known (no probe left), only one while they come from probes still to send.
    """
    cap, capacity = inputs.effective_page_cap, inputs.batch_capacity
    overhead_requests, overhead_pages = lane_overhead(inputs, probes=probes, lanes=lanes)
    full_pages = lane_rows // cap
    groups = ceil_div(lanes, capacity)
    known_lanes = 1 if probes else lanes
    planning_waves = ceil_div(probes, capacity)
    return CostEstimate(
        KeysetExecutionKind.PARTITIONED,
        overhead_requests + groups * full_pages + finish_requests,
        eligible=True,
        lane_count=lanes,
        depth=full_pages + lane_end_pages(inputs.completion),
        groups=groups,
        planning_waves=planning_waves,
        pages=overhead_pages + lanes * full_pages + finish_requests,
        min_requests=planning_waves + ceil_div(known_lanes, capacity) + finish_requests,
        min_pages=probes + known_lanes + finish_requests,
        probes=probes,
    )


def range_window_width(  # noqa: PLR0913
    *,
    completion: KeysetPageCompletion,
    page_cap: int,
    span: int,
    density_numerator: int,
    density_denominator: int,
    explicit: int | None,
) -> int:
    """Return the frozen integer-only width formula used by range planning."""
    if explicit is not None:
        return explicit
    if completion is KeysetPageCompletion.EMPTY_CONFIRMATION:
        return max(2, min(page_cap, span + 1))
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
    """Return the exact deduplicated integer-floor anchor guesses."""
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
) -> tuple[LaneSpec, ...]:
    """Plan partition lanes between the boundary fences in output order."""
    specs = plan_lanes_from_anchors(lo=lo, upper_exclusive=upper_exclusive, anchors=anchors)
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
    "lane_end_pages",
    "lane_overhead",
    "normalize_anchors",
    "partition_estimate",
    "partition_lane_specs",
    "range_window_width",
    "selected_range_geometry",
    "sequential_estimate",
]
