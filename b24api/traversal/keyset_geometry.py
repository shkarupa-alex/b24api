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
    target_lanes: int
    max_range_waves: int
    range_window_width: int | None
    completion: KeysetPageCompletion
    advisory_total: int | None
    descending: bool = False


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


def ceil_div(numerator: int, denominator: int) -> int:
    """Return integer ceiling division."""
    return (numerator + denominator - 1) // denominator


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
    admitted_head_rows = inputs.boundary.tail_rows if inputs.descending else inputs.boundary.head_rows
    sequential = CostEstimate(
        KeysetExecutionKind.SEQUENTIAL,
        max(1, ceil_div(max(0, all_rows - admitted_head_rows), cap)) + finish_requests,
        eligible=True,
    )
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
    range_depth = max(1, ceil_div(rows_per_window, cap)) + int(
        inputs.completion is KeysetPageCompletion.EMPTY_CONFIRMATION,
    )
    groups = ceil_div(windows, capacity) if windows else 0
    range_estimate = CostEstimate(
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
    )
    lanes = min(inputs.target_lanes, max(1, ceil_div(interior_rows, cap)))
    partition_depth = max(1, ceil_div(interior_rows, lanes * cap))
    planning_waves = max(1, ceil_div(inputs.target_lanes, capacity))
    partition = CostEstimate(
        KeysetExecutionKind.PARTITIONED,
        planning_waves + ceil_div(lanes, capacity) * partition_depth + finish_requests,
        eligible=True,
        lane_count=lanes,
        depth=partition_depth,
        groups=ceil_div(lanes, capacity),
        planning_waves=planning_waves,
    )
    return sequential, range_estimate, partition, (span, numerator, denominator, interior_rows, all_rows)


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
    "BoundaryDensity",
    "BoundaryFacts",
    "CostEstimate",
    "SelectorInputs",
    "anchor_guesses",
    "boundary_density",
    "ceil_div",
    "closure_witness",
    "descending_closure_witness",
    "estimates",
    "normalize_anchors",
    "partition_lane_specs",
    "range_window_width",
    "selected_range_geometry",
]
