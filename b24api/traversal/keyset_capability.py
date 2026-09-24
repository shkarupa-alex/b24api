"""Pure capability-planning values for fast integer keysets."""

# ruff: noqa: PLR2004

from __future__ import annotations
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from b24api.contracts.keyset_execution import (
    AutoKeysetExecution,
    KeysetExecutionKind,
    KeysetPageCompletion,
    KeysetPhase,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
)
from b24api.errors import PaginationError
from b24api.traversal.keyset_auto import BoundaryFacts, TotalHintState, normalize_total_hint
from b24api.traversal.keyset_fast_plan import (
    LaneBounds,
    LaneKind,
    LaneSpec,
    LaneState,
    LaneStatus,
    plan_lanes_from_anchors,
    plan_windows,
    window_count,
)
from b24api.traversal.keyset_partition import anchor_guesses, normalize_anchors
from b24api.traversal.keyset_range import range_window_width

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.contracts.json import FrozenJson
    from b24api.contracts.request import Request
    from b24api.contracts.traversal import KeysetSpec
    from b24api.traversal.keyset_page_validation import LaneCommandPlan, LaneReceipt


@dataclass(frozen=True, slots=True)
class CapabilityCommand:
    """Describe one capability command before scheduler correlation is assigned."""

    ordinal: int
    bounds: LaneBounds
    descending: bool
    reserve: int
    expects_single_row: bool = False
    expected: tuple[int, ...] | None = None


def build_capability_plans(  # noqa: PLR0913
    commands: tuple[CapabilityCommand, ...],
    phase: KeysetPhase,
    controls: Callable[..., Request],
    lane_plan: Callable[..., LaneCommandPlan],
    page_cap: int,
    planning_bounds: dict[str, LaneBounds],
    planning_descending: dict[str, bool],
) -> tuple[LaneCommandPlan, ...]:
    """Create capability commands and their validation lookup state."""
    plans = []
    for command in commands:
        bounds = command.bounds
        request = controls(
            direction="DESC" if command.descending else "ASC",
            lower=bounds.lower_exclusive,
            upper=bounds.upper_exclusive,
            limit=1 if command.expects_single_row else page_cap,
        )
        lane = LaneState(
            LaneSpec(
                ordinal=command.ordinal,
                kind=LaneKind.LANE,
                bounds=bounds,
                descending=command.descending,
                retained_upper_anchor=None,
            ),
            bounds.upper_exclusive if command.descending else bounds.lower_exclusive,
            LaneStatus.OPEN,
            None,
            0,
            command.reserve,
        )
        plan = lane_plan(
            lane,
            phase=phase,
            request=request,
            reserve=command.reserve,
            single=command.expects_single_row,
        )
        planning_bounds[plan.command_id] = bounds
        planning_descending[plan.command_id] = command.descending
        plans.append(plan)
    return tuple(plans)


@dataclass(frozen=True, slots=True)
class AnchorResult:
    """Return normalized anchor evidence without mutating scheduler state."""

    anchors: tuple[int, ...]
    rows: dict[int, FrozenJson]
    commands: dict[int, str]
    empty_probes: int
    raw_rows: int
    discarded_rows: int


@dataclass(frozen=True, slots=True)
class LaneGeometry:
    """Return selected body lanes with range-only report geometry."""

    specs: tuple[LaneSpec, ...]
    window_width: int | None
    window_count: int | None


@dataclass(frozen=True, slots=True)
class BoundaryAnalysis:
    """Collect pure boundary facts, density evidence, and advisory-total state."""

    facts: BoundaryFacts
    total_hint: TotalHintState
    interior_span: int | None
    density_numerator: int | None
    density_denominator: int | None


def analyze_boundary(
    ascending: LaneReceipt,
    descending: LaneReceipt,
    *,
    ascending_total: int | None,
    descending_total: int | None,
    advisory: bool,
) -> BoundaryAnalysis:
    """Normalize boundary observations without mutating selection state."""
    maximum_hint = len(ascending.rows) + len(descending.rows)
    if ascending.identities and descending.identities:
        maximum_hint += max(0, min(descending.identities) - max(ascending.identities) - 1)
    total_hint = normalize_total_hint(
        requested=advisory,
        head=ascending_total,
        tail=descending_total,
        maximum=maximum_hint,
    )
    overlap = bool(
        ascending.identities and descending.identities and max(ascending.identities) >= min(descending.identities),
    )
    adjacent = bool(
        ascending.identities and descending.identities and min(descending.identities) - max(ascending.identities) == 1,
    )
    facts = BoundaryFacts(
        len(ascending.rows),
        len(descending.rows),
        min(ascending.identities) if ascending.identities else None,
        max(ascending.identities) if ascending.identities else None,
        min(descending.identities) if descending.identities else None,
        max(descending.identities) if descending.identities else None,
        overlap,
        adjacent,
    )
    if not ascending.identities or not descending.identities:
        return BoundaryAnalysis(facts, total_hint, None, None, None)
    interior_span = max(0, min(descending.identities) - max(ascending.identities) - 1)
    density_denominator = max(
        1,
        max(ascending.identities)
        - min(ascending.identities)
        + 1
        + max(descending.identities)
        - min(descending.identities)
        + 1,
    )
    return BoundaryAnalysis(
        facts,
        total_hint,
        interior_span,
        len(ascending.rows) + len(descending.rows),
        density_denominator,
    )


def canary_commands(
    ascending: tuple[int, ...],
    descending: tuple[int, ...],
    page_cap: int,
) -> tuple[CapabilityCommand, ...]:
    """Derive the five fixed bounded-capability checks from boundary prefixes."""
    prefixes = (ascending, tuple(reversed(descending)))
    pairs = tuple((values[index], values[index + 1]) for values in prefixes for index in range(len(values) - 1))
    pair = next(
        ((left, right) for left, right in pairs if len(str(abs(left))) != len(str(abs(right)))),
        pairs[0] if pairs else None,
    )
    if pair is None or page_cap < 2:
        raise PaginationError("boundary facts cannot construct five capability canaries")
    p, q = pair
    definitions = (
        (p, p + 1, ()),
        (p - 1, p, ()),
        (p - 1, p + 1, (p,)),
        (p - 1, q + 1, (p, q)),
        (p - 1, q + 1, (q, p)),
    )
    return tuple(
        CapabilityCommand(
            ordinal,
            LaneBounds(definitions[ordinal][0], definitions[ordinal][1]),
            ordinal == 4,
            page_cap,
            expected=definitions[ordinal][2],
        )
        for ordinal in (2, 0, 4, 1, 3)
    )


def anchor_commands(
    *,
    lo: int,
    upper_exclusive: int,
    target_lanes: int,
    page_cap: int,
    writable_limit: bool,
) -> tuple[CapabilityCommand, ...]:
    """Describe occupied-anchor probes independently of scheduler state."""
    reserve = 1 if writable_limit else page_cap
    return tuple(
        CapabilityCommand(
            ordinal,
            LaneBounds(guess, upper_exclusive),
            descending=False,
            reserve=reserve,
            expects_single_row=writable_limit,
        )
        for ordinal, guess in enumerate(
            anchor_guesses(lo=lo, upper_exclusive=upper_exclusive, target_lanes=target_lanes),
        )
    )


def normalize_anchor_receipts(
    receipts: tuple[LaneReceipt, ...],
    *,
    lo: int,
    upper_exclusive: int,
) -> AnchorResult:
    """Normalize probe receipts into retained anchor values and provenance."""
    anchors: list[int] = []
    rows: dict[int, FrozenJson] = {}
    commands: dict[int, str] = {}
    empty = discarded = 0
    for receipt in receipts:
        if not receipt.rows:
            empty += 1
            continue
        anchor = receipt.identities[0]
        anchors.append(anchor)
        rows.setdefault(anchor, receipt.rows[0])
        commands.setdefault(anchor, receipt.command_id)
        discarded += max(0, len(receipt.rows) - 1)
    normalized = normalize_anchors(lo=lo, upper_exclusive=upper_exclusive, anchors=tuple(anchors))
    return AnchorResult(
        normalized,
        rows,
        commands,
        empty,
        sum(len(receipt.rows) for receipt in receipts),
        discarded + len(anchors) - len(normalized),
    )


def compact_anchor_receipts(
    receipts: tuple[LaneReceipt, ...],
) -> tuple[tuple[LaneReceipt, ...], int, int]:
    """Keep only each probe's usable first row and report discarded raw rows."""
    compact = tuple(
        replace(
            receipt,
            rows=receipt.rows[:1],
            identities=receipt.identities[:1],
        )
        for receipt in receipts
    )
    retained = sum(bool(receipt.rows) for receipt in receipts)
    return compact, sum(len(receipt.rows) for receipt in receipts) - retained, retained


def anchor_capable_batch_capacity(*, current: int, available_rows: int, page_cap: int, target_lanes: int) -> int:
    """Disable auto anchor probing unless one body page and all possible anchors fit."""
    return current if available_rows >= page_cap + target_lanes else 0


def selected_lane_geometry(  # noqa: PLR0913
    *,
    selected: KeysetExecutionKind,
    execution: RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution,
    keyset: KeysetSpec,
    completion: KeysetPageCompletion,
    page_cap: int,
    ascending: tuple[int, ...],
    descending: tuple[int, ...],
    anchors: tuple[int, ...],
) -> LaneGeometry:
    """Plan body lane values without touching scheduler lifecycle state."""
    lo, hi = max(ascending), min(descending)
    width = count = None
    if selected is KeysetExecutionKind.RANGE:
        explicit = (
            execution.window_width
            if isinstance(execution, RangeKeysetExecution)
            else execution.range_window_width
            if isinstance(execution, AutoKeysetExecution)
            else None
        )
        width = range_window_width(
            completion=completion,
            page_cap=page_cap,
            span=max(0, hi - lo - 1),
            density_numerator=len(ascending) + len(descending),
            density_denominator=max(
                1,
                max(ascending) - min(ascending) + 1 + max(descending) - min(descending) + 1,
            ),
            explicit=explicit,
        )
        count = window_count(lo=lo, upper_exclusive=hi, width=width)
        specs = plan_windows(lo=lo, upper_exclusive=hi, width=width)
    else:
        specs = plan_lanes_from_anchors(lo=lo, upper_exclusive=hi, anchors=anchors)
    if keyset.direction == "descending":
        specs = tuple(
            replace(
                spec,
                ordinal=ordinal,
                descending=True,
                retained_upper_anchor=(
                    spec.bounds.lower_exclusive
                    if selected is KeysetExecutionKind.PARTITIONED and spec.bounds.lower_exclusive != lo
                    else None
                ),
            )
            for ordinal, spec in enumerate(reversed(specs))
        )
    return LaneGeometry(specs, width, count)


def lane_for_command(
    plan: LaneCommandPlan,
    *,
    planning_bounds: dict[str, LaneBounds],
    planning_descending: dict[str, bool],
    finish_lane: LaneState | None,
    lanes: list[LaneState],
) -> LaneState:
    """Resolve the immutable validation view for one correlated command."""
    if plan.phase is KeysetPhase.BOUNDARY:
        return LaneState(
            LaneSpec(plan.lane_ordinal, LaneKind.HEAD, LaneBounds(None, None), plan.lane_ordinal == 1, None),
            None,
            LaneStatus.OPEN,
            None,
            0,
            plan.reserved_rows,
        )
    if plan.phase is KeysetPhase.ANCHOR_PROBE:
        bounds = planning_bounds[plan.command_id]
        descending = planning_descending.get(plan.command_id, False)
        return LaneState(
            LaneSpec(plan.lane_ordinal, LaneKind.LANE, bounds, descending, None),
            bounds.upper_exclusive if descending else bounds.lower_exclusive,
            LaneStatus.OPEN,
            None,
            0,
            plan.reserved_rows,
        )
    if plan.phase is KeysetPhase.FINISH:
        if finish_lane is None:
            raise RuntimeError("finish command lacks its scheduler-owned lane")
        return finish_lane
    return next(lane for lane in lanes if lane.spec.ordinal == plan.lane_ordinal)


__all__: list[str] = []
