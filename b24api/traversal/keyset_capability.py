"""Pure capability-planning values for fast integer keysets."""

# ruff: noqa: FBT003, PLR2004

from __future__ import annotations
from collections import deque
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from b24api.contracts.keyset_execution import (
    AutoKeysetExecution,
    ClosureWitness,
    KeysetExecutionKind,
    KeysetPageCompletion,
    KeysetPhase,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    TraceClass,
)
from b24api.contracts.report import PageDispatch, PageOutcome, PageRejectionCode, Violation
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
)
from b24api.traversal.keyset_observation import PageObservation
from b24api.traversal.keyset_partition import anchor_guesses, normalize_anchors
from b24api.traversal.keyset_range import range_window_width

if TYPE_CHECKING:
    from b24api.contracts.json import JsonValue
    from b24api.contracts.response import Response
    from b24api.contracts.traversal import KeysetSpec
    from b24api.traversal.page_validation import LaneCommandPlan, LaneReceipt


@dataclass(frozen=True, slots=True)
class CapabilityCommand:
    """Describe one capability command before scheduler correlation is assigned."""

    ordinal: int
    bounds: LaneBounds
    descending: bool
    reserve: int
    expects_single_row: bool = False
    expected: tuple[int, ...] | None = None


@dataclass(frozen=True, slots=True)
class AnchorResult:
    """Return normalized anchor evidence without mutating scheduler state."""

    anchors: tuple[int, ...]
    rows: dict[int, JsonValue]
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
        requested=advisory, head=ascending_total, tail=descending_total, maximum=maximum_hint,
    )
    overlap = bool(
        ascending.identities
        and descending.identities
        and max(ascending.identities) >= min(descending.identities),
    )
    adjacent = bool(
        ascending.identities
        and descending.identities
        and min(descending.identities) - max(ascending.identities) == 1,
    )
    facts = BoundaryFacts(
        len(ascending.rows), len(descending.rows),
        min(ascending.identities) if ascending.identities else None,
        max(ascending.identities) if ascending.identities else None,
        min(descending.identities) if descending.identities else None,
        max(descending.identities) if descending.identities else None,
        overlap, adjacent,
    )
    if not ascending.identities or not descending.identities:
        return BoundaryAnalysis(facts, total_hint, None, None, None)
    interior_span = max(0, min(descending.identities) - max(ascending.identities) - 1)
    density_denominator = max(
        1,
        max(ascending.identities) - min(ascending.identities)
        + 1
        + max(descending.identities)
        - min(descending.identities)
        + 1,
    )
    return BoundaryAnalysis(
        facts, total_hint, interior_span, len(ascending.rows) + len(descending.rows), density_denominator,
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
    rows: dict[int, JsonValue] = {}
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
        specs = plan_windows(lo=lo, upper_exclusive=hi, width=width)
        count = len(specs)
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
    if lower is not None and lane.cursor is not None and identities == tuple(range(lane.cursor - 1, lower, -1)):
        return ClosureWitness.LATTICE_FULL
    if lower is not None and identities[-1] == lower + 1:
        return ClosureWitness.TOP
    if completion is KeysetPageCompletion.SHORT_PAGE_EXHAUSTS and len(identities) < page_cap:
        return ClosureWitness.SHORT_PAGE
    return None


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
            LaneSpec(plan.lane_ordinal, LaneKind.HEAD, LaneBounds(None, None), plan.lane_ordinal == 1, True, None),
            None, LaneStatus.OPEN, None, 0, plan.reserved_rows, deque(),
        )
    if plan.phase in {KeysetPhase.CANARY, KeysetPhase.ANCHOR_PROBE}:
        bounds = planning_bounds[plan.command_id]
        descending = planning_descending.get(plan.command_id, False)
        return LaneState(
            LaneSpec(plan.lane_ordinal, LaneKind.LANE, bounds, descending, False, None),
            bounds.upper_exclusive if descending else bounds.lower_exclusive,
            LaneStatus.OPEN, None, 0, plan.reserved_rows, deque(),
        )
    if plan.phase is KeysetPhase.FINISH:
        if finish_lane is None:
            raise RuntimeError("finish command lacks its scheduler-owned lane")
        return finish_lane
    return next(lane for lane in lanes if lane.spec.ordinal == plan.lane_ordinal)


def page_observation(  # noqa: PLR0913
    ordinal: int,
    plan: LaneCommandPlan,
    *,
    index: int | None,
    selected: int,
    admitted: int,
    effective_page_cap: int,
    outcome: PageOutcome = PageOutcome.COMMITTED,
    rejection: PageRejectionCode | None = None,
    violation: Violation | None = None,
    response: Response | None = None,
    witness: ClosureWitness | None = None,
    dispatch: PageDispatch = PageDispatch.BATCH,
) -> PageObservation:
    """Build value-only trace evidence independently of scheduler sequencing."""
    return PageObservation(
        ordinal, plan.phase, plan.lane_ordinal, plan.command_id, dispatch, index, selected, admitted,
        response.total if response is not None and response.total is not None and response.total >= 0 else None,
        response.next if response is not None else None,
        selected == effective_page_cap, witness, outcome, rejection, violation, TraceClass.BODY,
    )


__all__: list[str] = []
