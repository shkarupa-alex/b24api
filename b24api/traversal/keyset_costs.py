"""Pure integer geometry and cost arithmetic for automatic keyset selection."""

# ruff: noqa: FBT003

from __future__ import annotations
from dataclasses import dataclass

from b24api.contracts.keyset_execution import (
    AutoKeysetExecution,
    KeysetExecutionKind,
    KeysetPageCompletion,
    RangeKeysetExecution,
)
from b24api.traversal.keyset_range import range_window_width


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
    window_width: int | None
    window_count: int | None
    lane_count: int | None
    depth: int | None
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
    ascending: tuple[int, ...],
    descending: tuple[int, ...],
) -> tuple[int, int]:
    """Return selected range width and count without materializing windows."""
    lo, hi = max(ascending), min(descending)
    explicit = execution.window_width if isinstance(execution, RangeKeysetExecution) else execution.range_window_width
    width = range_window_width(
        completion=completion,
        page_cap=page_cap,
        span=max(0, hi - lo - 1),
        density_numerator=len(ascending) + len(descending),
        density_denominator=max(1, max(ascending) - min(ascending) + 1 + max(descending) - min(descending) + 1),
        explicit=explicit,
    )
    span = max(0, hi - lo - 1)
    return width, ceil_div(span, width - 1) if span else 0


def _geometry(inputs: SelectorInputs) -> tuple[int, int, int, int, int]:
    boundary = inputs.boundary
    if boundary.head_rows == 0:
        return 0, 0, 1, 0, 0
    if boundary.overlapping:
        rows = max(boundary.head_rows, boundary.tail_rows)
        return 0, rows, max(1, rows), 0, rows
    endpoints = (boundary.head_min, boundary.head_max, boundary.tail_min, boundary.tail_max)
    if any(value is None for value in endpoints):
        rows = boundary.head_rows + boundary.tail_rows
        return 0, 0, 1, rows, rows
    head_min, head_max, tail_min, tail_max = (value for value in endpoints if value is not None)
    span = max(0, tail_min - head_max - 1)
    numerator = boundary.head_rows + boundary.tail_rows
    denominator = max(1, (head_max - head_min + 1) + (tail_max - tail_min + 1))
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
    canary_commands: int,
    finish_requests: int,
) -> tuple[CostEstimate, CostEstimate, CostEstimate, tuple[int, int, int, int, int]]:
    """Compute every frozen candidate formula from integer observations."""
    span, numerator, denominator, interior_rows, all_rows = _geometry(inputs)
    cap, capacity = inputs.effective_page_cap, inputs.batch_capacity
    admitted_head_rows = inputs.boundary.tail_rows if inputs.descending else inputs.boundary.head_rows
    sequential = CostEstimate(
        KeysetExecutionKind.SEQUENTIAL,
        max(1, ceil_div(max(0, all_rows - admitted_head_rows), cap)) + finish_requests,
        True,
        None,
        None,
        None,
        None,
    )
    if capacity < 1:
        unavailable = CostEstimate(KeysetExecutionKind.RANGE, 0, False, None, None, None, None)
        partition = CostEstimate(KeysetExecutionKind.PARTITIONED, 0, False, None, None, None, None)
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
        ceil_div(canary_commands, capacity) + groups * range_depth + finish_requests,
        groups * range_depth <= inputs.max_range_waves,
        width,
        windows,
        None,
        range_depth,
        rows_per_window,
        groups,
        ceil_div(canary_commands, capacity),
    )
    lanes = min(inputs.target_lanes, max(1, ceil_div(interior_rows, cap)))
    partition_depth = max(1, ceil_div(interior_rows, lanes * cap))
    planning_waves = (
        1
        if canary_commands + inputs.target_lanes <= capacity
        else ceil_div(canary_commands, capacity) + ceil_div(inputs.target_lanes, capacity)
    )
    partition = CostEstimate(
        KeysetExecutionKind.PARTITIONED,
        planning_waves + ceil_div(lanes, capacity) * partition_depth + finish_requests,
        True,
        None,
        None,
        lanes,
        partition_depth,
        None,
        ceil_div(lanes, capacity),
        planning_waves,
    )
    return sequential, range_estimate, partition, (span, numerator, denominator, interior_rows, all_rows)


__all__ = ["BoundaryFacts", "CostEstimate", "SelectorInputs", "ceil_div", "estimates", "selected_range_geometry"]
