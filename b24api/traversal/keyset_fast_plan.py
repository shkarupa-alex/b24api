"""Pure common plan values and exclusive keyset interval algebra."""

from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from b24api.contracts.json import FrozenJson
    from b24api.contracts.keyset_execution import ClosureWitness

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


@dataclass(slots=True)
class LazyRangePlan:
    """Create and release one numeric-window lane at a time."""

    lo: Identity
    upper_exclusive: Identity
    width: int
    count: int
    descending: bool
    next_ordinal: int = 0

    def append_next(
        self,
        lanes: list[LaneState],
        rows: dict[int, list[FrozenJson]],
        identities: dict[int, list[int]],
        commands: dict[int, list[tuple[str, int]]],
    ) -> None:
        """Append the next output-ordered lane to scheduler-owned ledgers."""
        if self.next_ordinal >= self.count:
            return
        spec = window_spec(
            lo=self.lo,
            upper_exclusive=self.upper_exclusive,
            width=self.width,
            ordinal=self.next_ordinal,
            count=self.count,
            descending=self.descending,
        )
        self.next_ordinal += 1
        lanes.append(
            LaneState(
                spec,
                spec.bounds.upper_exclusive if spec.descending else spec.bounds.lower_exclusive,
                LaneStatus.OPEN,
                None,
                0,
                0,
            ),
        )
        rows[spec.ordinal], identities[spec.ordinal], commands[spec.ordinal] = [], [], []

    def fill(
        self,
        capacity: int,
        lanes: list[LaneState],
        rows: dict[int, list[FrozenJson]],
        identities: dict[int, list[int]],
        commands: dict[int, list[tuple[str, int]]],
    ) -> None:
        """Fill one bounded sliding group without materializing the full range."""
        while len(lanes) < capacity and self.next_ordinal < self.count:
            self.append_next(lanes, rows, identities, commands)


def plan_windows(*, lo: Identity, upper_exclusive: Identity, width: int) -> tuple[LaneSpec, ...]:
    """Partition ``(lo, upper_exclusive)`` into exact disjoint windows."""
    if any(not isinstance(value, int) or isinstance(value, bool) for value in (lo, upper_exclusive, width)):
        raise TypeError("window operands must be exact integers")
    if width < MIN_WINDOW_WIDTH:
        raise ValueError("width must be at least 2")
    if upper_exclusive <= lo + 1:
        return ()
    count = window_count(lo=lo, upper_exclusive=upper_exclusive, width=width)
    return tuple(
        window_spec(
            lo=lo,
            upper_exclusive=upper_exclusive,
            width=width,
            ordinal=ordinal,
            count=count,
            descending=False,
        )
        for ordinal in range(count)
    )


def window_count(*, lo: Identity, upper_exclusive: Identity, width: int) -> int:
    """Count exact disjoint windows without materializing their lane states."""
    if any(not isinstance(value, int) or isinstance(value, bool) for value in (lo, upper_exclusive, width)):
        raise TypeError("window operands must be exact integers")
    if width < MIN_WINDOW_WIDTH:
        raise ValueError("width must be at least 2")
    span = max(0, upper_exclusive - lo - 1)
    return (span + width - 2) // (width - 1) if span else 0


def window_spec(  # noqa: PLR0913
    *,
    lo: Identity,
    upper_exclusive: Identity,
    width: int,
    ordinal: int,
    count: int,
    descending: bool,
) -> LaneSpec:
    """Create one output-ordered range lane from constant-size geometry state."""
    if not 0 <= ordinal < count:
        raise IndexError("window ordinal is outside the planned geometry")
    geometric = count - ordinal - 1 if descending else ordinal
    lower = lo + geometric * (width - 1)
    upper = min(upper_exclusive, lower + width)
    return LaneSpec(
        ordinal=ordinal,
        kind=LaneKind.WINDOW,
        bounds=LaneBounds(lower, upper),
        descending=descending,
        retained_upper_anchor=None,
    )


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
    "Identity",
    "LaneBounds",
    "LaneKind",
    "LaneSpec",
    "LaneState",
    "LaneStatus",
    "LazyRangePlan",
    "fit_wave",
    "plan_lanes_from_anchors",
    "plan_windows",
    "window_count",
    "window_spec",
]
