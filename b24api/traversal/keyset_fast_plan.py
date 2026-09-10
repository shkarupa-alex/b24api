"""Pure common plan values and exclusive keyset interval algebra."""

from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections import deque

    from b24api.contracts.json import JsonValue
    from b24api.contracts.keyset_execution import ClosureWitness, KeysetExecutionKind, KeysetPageCompletion
    from b24api.contracts.request import IdentitySpec
    from b24api.contracts.response import ResultCollectionShape
    from b24api.contracts.traversal import KeysetSpec

type Identity = int
MIN_WINDOW_WIDTH = 2


class LaneKind(StrEnum):
    """Output component owned by one scheduler lane."""

    HEAD = "head"
    WINDOW = "window"
    LANE = "lane"
    TAIL = "tail"
    FINISH = "finish"


class LaneStatus(StrEnum):
    """Mutable scheduler status of a lane."""

    OPEN = "open"
    CLOSED = "closed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class LaneBounds:
    """Canonical exclusive numeric bounds."""

    lower_exclusive: Identity | None
    upper_exclusive: Identity | None

    def __post_init__(self) -> None:
        """Validate exact ordered bounds."""
        for value in (self.lower_exclusive, self.upper_exclusive):
            if value is not None and (not isinstance(value, int) or isinstance(value, bool)):
                raise TypeError("lane bounds must be exact integers or None")
        if (
            self.lower_exclusive is not None
            and self.upper_exclusive is not None
            and self.lower_exclusive >= self.upper_exclusive
        ):
            raise ValueError("lane lower bound must be below its upper bound")


@dataclass(frozen=True, slots=True)
class LaneSpec:
    """Immutable ownership and wire-bound description of one lane."""

    ordinal: int
    kind: LaneKind
    bounds: LaneBounds
    descending: bool
    owns_output: bool
    retained_upper_anchor: Identity | None


@dataclass(slots=True)
class LaneState:
    """Operation-local mutable lane ledger."""

    spec: LaneSpec
    cursor: Identity | None
    status: LaneStatus
    witness: ClosureWitness | None
    rounds: int
    reserved_rows: int
    retained: deque[JsonValue]


@dataclass(frozen=True, slots=True)
class FastKeysetPlan:
    """Selected scheduler plan."""

    kind: KeysetExecutionKind
    lanes: tuple[LaneSpec, ...]
    page_size: int
    effective_page_cap: int
    batch_capacity: int
    completion: KeysetPageCompletion
    identity: IdentitySpec
    keyset: KeysetSpec
    collection_shape: ResultCollectionShape
    finish_cursor: Identity | None


def plan_windows(*, lo: Identity, upper_exclusive: Identity, width: int) -> tuple[LaneSpec, ...]:
    """Partition ``(lo, upper_exclusive)`` into exact disjoint windows."""
    if any(not isinstance(value, int) or isinstance(value, bool) for value in (lo, upper_exclusive, width)):
        raise TypeError("window operands must be exact integers")
    if width < MIN_WINDOW_WIDTH:
        raise ValueError("width must be at least 2")
    if upper_exclusive <= lo + 1:
        return ()
    lanes: list[LaneSpec] = []
    lower = lo
    step = width - 1
    while lower + 1 < upper_exclusive:
        upper = min(upper_exclusive, lower + width)
        lanes.append(
            LaneSpec(
                ordinal=len(lanes),
                kind=LaneKind.WINDOW,
                bounds=LaneBounds(lower, upper),
                descending=False,
                owns_output=True,
                retained_upper_anchor=None,
            ),
        )
        lower += step
    return tuple(lanes)


def plan_lanes_from_anchors(
    *,
    lo: Identity,
    upper_exclusive: Identity,
    anchors: tuple[Identity, ...] | list[Identity],
) -> tuple[LaneSpec, ...]:
    """Build contiguous lanes whose retained upper anchors have one owner."""
    if any(not isinstance(value, int) or isinstance(value, bool) for value in (lo, upper_exclusive, *anchors)):
        raise TypeError("lane operands must be exact integers")
    if upper_exclusive <= lo:
        return ()
    normalized = tuple(sorted({value for value in anchors if lo < value < upper_exclusive}))
    lanes: list[LaneSpec] = []
    lower = lo
    for anchor in normalized:
        lanes.append(
            LaneSpec(
                ordinal=len(lanes),
                kind=LaneKind.LANE,
                bounds=LaneBounds(lower, anchor),
                descending=False,
                owns_output=True,
                retained_upper_anchor=anchor,
            ),
        )
        lower = anchor
    lanes.append(
        LaneSpec(
            ordinal=len(lanes),
            kind=LaneKind.LANE,
            bounds=LaneBounds(lower, upper_exclusive),
            descending=False,
            owns_output=True,
            retained_upper_anchor=None,
        ),
    )
    return tuple(lanes)


def fit_wave[T](plans: tuple[T, ...], *, reserves: tuple[int, ...], commands: int, rows: int) -> tuple[T, ...]:
    """Return the largest deterministic prefix fitting both capacities."""
    selected = 0
    reserved = 0
    for reserve in reserves[:commands]:
        if reserved + reserve > rows:
            break
        selected += 1
        reserved += reserve
    return plans[:selected]


__all__ = [
    "FastKeysetPlan",
    "Identity",
    "LaneBounds",
    "LaneKind",
    "LaneSpec",
    "LaneState",
    "LaneStatus",
    "fit_wave",
    "plan_lanes_from_anchors",
    "plan_windows",
]
