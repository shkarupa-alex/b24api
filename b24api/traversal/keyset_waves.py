"""The body-wave rule of a fast keyset plan, and the cost model that runs that same rule on estimated lanes.

Output is strictly ordered, so a lane's rows wait in the row buffer until every lane before it has
closed; the frontier lane (the first open one) drains as it reads. A wave therefore advances lanes
only while their pages fit what the buffer leaves free: the row budget, not the command cap, bounds
how many pages a wave carries once the lanes are deeper than one page. The planner prices a plan by
running ``wave_members`` over lanes of estimated rows (``simulate_lanes``), so the estimate follows
whatever the runtime will do with the same lanes, wave by wave.
"""

from __future__ import annotations
from collections import deque
from dataclasses import dataclass
from fractions import Fraction
from math import ceil, floor
from typing import TYPE_CHECKING

from b24api.contracts.keyset_execution import KeysetPageCompletion

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence


def wave_room(*, capacity: int, free_rows: int, reserve: int) -> int:
    """Return how many pages the next body wave carries: at most ``capacity``, each reserving ``reserve`` free rows."""
    return min(capacity, max(0, free_rows) // max(1, reserve))


def probe_waves(probes: int, *, capacity: int, free_rows: int, reserve: int) -> int | None:
    """Return the waves ``probes`` anchor probes take, or ``None`` when the buffer has no room for one.

    Each wave carries ``wave_room`` probes; every probe keeps one row, its anchor, in the buffer.
    """
    waves = 0
    while probes > 0:
        room = wave_room(capacity=capacity, free_rows=free_rows, reserve=reserve)
        if room < 1:
            return None
        sent = min(room, probes)
        probes -= sent
        free_rows -= sent
        waves += 1
    return waves


def wave_members[L](open_lanes: Sequence[L], *, room: int) -> tuple[L, ...]:
    """Return the open lanes the next body wave advances by one page each: the first ``room``, in output order.

    The frontier (the first open lane) always leads. Every wave is such a prefix, so a lane has never
    read more pages than an open lane before it: no lane outruns the lanes that drain before it.
    ``room`` is ``wave_room``.
    """
    return tuple(open_lanes[: max(0, room)])


@dataclass(frozen=True, slots=True)
class LanePages:
    """The pages a lane spends before it closes, as the share of lanes like it that need one more page.

    ``pages`` closes the lane when its last row sits on the value right below its upper fence (the
    runtime's ``ClosureWitness.TOP``); otherwise it needs one more page, an empty one, to close.
    ``confirmation`` is the chance of that: the share of its values that hold no row.
    """

    pages: int
    confirmation: Fraction


def lane_pages(*, rows: int, ids: int, page_cap: int, completion: KeysetPageCompletion) -> LanePages:
    """Return the pages a lane of ``rows`` rows over ``ids`` identity values spends before it closes.

    A lane with no identity value between its fences sends no page. A lane holding every value it
    spans closes on the page that reaches its upper fence. Otherwise it reads its pages, the last one
    full or short: it closes on that page when its last row sits right below the upper fence, or a
    short one under short-page completion. Else an empty page confirms the end. Rows spread over the
    lane's values put a row on its top value at its density, so that page is needed at the chance the
    top value holds none.
    """
    if ids <= 0:
        return LanePages(0, Fraction(0))
    if rows >= ids:
        return LanePages(max(1, -(-rows // page_cap)), Fraction(0))
    full, rest = divmod(rows, page_cap)
    confirms = rest == 0 or completion is KeysetPageCompletion.EMPTY_CONFIRMATION
    return LanePages(full + int(rest > 0), Fraction(ids - rows, ids) if confirms else Fraction(0))


@dataclass(frozen=True, slots=True)
class LaneShape:
    """``count`` consecutive lanes of the same estimated rows over the same identity span."""

    rows: int
    ids: int
    count: int = 1


@dataclass(frozen=True, slots=True)
class IdentityRun:
    """``ids`` consecutive identity values holding rows at an estimated ``density`` (rows per value)."""

    ids: int
    density: Fraction


def window_shapes(runs: Sequence[IdentityRun], *, step: int) -> tuple[LaneShape, ...]:
    """Cut ``runs`` into the range windows of ``step`` identity values each, as the runtime cuts the interior.

    A window's rows are the rows its values hold at their runs' densities, rounded up; the last
    window holds what is left. Equal consecutive windows share one shape.
    """
    shapes: list[LaneShape] = []

    def emit(rows: Fraction, ids: int, count: int = 1) -> None:
        shape = LaneShape(min(ids, ceil(rows)), ids, count)
        last = shapes[-1] if shapes else None
        if last is not None and (last.rows, last.ids) == (shape.rows, shape.ids):
            shapes[-1] = LaneShape(last.rows, last.ids, last.count + count)
        else:
            shapes.append(shape)

    filled, rows = 0, Fraction(0)
    for run in runs:
        left = run.ids
        while left:
            if filled == 0 and left >= step:
                whole = left // step
                emit(step * run.density, step, whole)
                left -= whole * step
                continue
            take = min(left, step - filled)
            filled, rows, left = filled + take, rows + take * run.density, left - take
            if filled == step:
                emit(rows, step)
                filled, rows = 0, Fraction(0)
    if filled:
        emit(rows, filled)
    return tuple(shapes)


@dataclass(frozen=True, slots=True)
class WaveCost:
    """Physical requests (waves) and logical pages (commands) one simulated lane plan spends."""

    requests: int
    pages: int


@dataclass(frozen=True, slots=True)
class WaveRules:
    """What bounds a plan's body waves: the page cap, how a lane completes, the command cap, the row budget.

    ``row_budget`` is what the buffer leaves the body: the policy's row budget less the rows the plan
    keeps aside (the tail boundary page, retained anchors).
    """

    page_cap: int
    completion: KeysetPageCompletion
    capacity: int
    row_budget: int


@dataclass(slots=True)
class _Lane:
    rows_left: int
    pages_left: int
    held: int = 0


@dataclass(slots=True)
class _Run:
    """``count`` more consecutive lanes of ``rows`` rows each.

    Each lane spends ``pages`` pages and the confirmation pages its share gives it: every lane takes the pages that
    bring the count given out to the sum of the shares so far, rounded to the nearest whole page. A lane's share is
    ``share / scale`` pages; ``given`` is what of that sum, plus a half, is left past the whole pages, in
    ``scale``-ths, so a lane's count takes integers only.
    """

    rows: int
    pages: int
    count: int
    share: int
    scale: int
    given: int

    def next_pages(self) -> int:
        """Return the pages the run's next lane spends."""
        whole, self.given = divmod(self.given + self.share, self.scale)
        return self.pages + whole


class _Lanes:
    """The lanes a plan opens, in output order."""

    def __init__(self, runs: Iterator[_Run]) -> None:
        self._runs = runs
        self._head = next(runs, None)

    def take(self) -> _Lane | None:
        head = self._head
        if head is None:
            return None
        lane = _Lane(head.rows, head.next_pages())
        head.count -= 1
        if head.count == 0:
            self._head = next(self._runs, None)
        return lane


def simulate_lanes(
    shapes: Sequence[LaneShape],
    rules: WaveRules,
    *,
    sliding: bool,
    request_limit: int,
) -> WaveCost | None:
    """Run ``wave_members`` over lanes of estimated rows; ``None`` past ``request_limit`` or with no room.

    ``sliding`` keeps at most ``capacity`` lanes open at once and opens the next one as the frontier
    closes, as range windows are opened. Each wave the frontier drains what it read; a closed
    frontier lets the next lane drain. The confirmation pages are spread evenly over the lanes at
    their chance of being needed (``LanePages``), in output order.

    The estimate is exact: every wave is stepped, but a run of waves only the frontier reads is taken
    at once. A plan that opens every lane at once steps a wave only while more lanes than the frontier
    read, which the row budget bounds. The simulation stops as soon as it passes ``request_limit``, so
    its work follows the requests of the cheaper of the plan and that limit; the planner runs it off
    the event loop (``keyset_plan``).
    """
    page_cap, capacity, row_budget = rules.page_cap, rules.capacity, rules.row_budget
    if capacity < 1:
        return None
    lanes = _Lanes(_expand(shapes, page_cap=page_cap, completion=rules.completion))
    active: deque[_Lane] = deque()
    group = capacity if sliding else None
    _fill(active, lanes, group)
    held = requests = pages = 0
    while active:
        members = wave_members(
            [lane for lane in active if lane.pages_left],
            room=wave_room(capacity=capacity, free_rows=row_budget - held, reserve=page_cap),
        )
        if not members:
            return None
        frontier = members[0]
        # A wave of the frontier alone repeats until it closes: the frontier drains what it reads, so the
        # room stays as it is, and no lane opens before the frontier closes. Take those waves at once.
        waves = frontier.pages_left if len(members) == 1 else 1
        requests += waves
        if requests > request_limit:
            return None
        pages += len(members) * waves
        for lane in members:
            read = min(page_cap * waves, lane.rows_left)
            lane.rows_left -= read
            lane.held += read
            held += read
            lane.pages_left -= waves
        while active:
            head = active[0]
            held -= head.held
            head.held = 0
            if head.pages_left:
                break
            active.popleft()
            _fill(active, lanes, group)
    return WaveCost(requests, pages)


def _expand(shapes: Sequence[LaneShape], *, page_cap: int, completion: KeysetPageCompletion) -> Iterator[_Run]:
    # Every shape is one run; a run whose lanes spend no page at all opens no lane. The lanes before a run need
    # ``chance`` confirmation pages in all: rounded to the nearest, a half more, its whole pages are given out.
    chance = Fraction(1, 2)
    for shape in shapes:
        cost = lane_pages(rows=shape.rows, ids=shape.ids, page_cap=page_cap, completion=completion)
        share, scale = cost.confirmation.numerator, cost.confirmation.denominator
        run = _Run(shape.rows, cost.pages, shape.count, share, scale, floor((chance - floor(chance)) * scale))
        chance += cost.confirmation * shape.count
        if shape.count and cost.pages + cost.confirmation:
            yield run


def _fill(active: deque[_Lane], lanes: _Lanes, group: int | None) -> None:
    while group is None or len(active) < group:
        lane = lanes.take()
        if lane is None:
            return
        active.append(lane)


__all__ = [
    "IdentityRun",
    "LanePages",
    "LaneShape",
    "WaveCost",
    "WaveRules",
    "lane_pages",
    "probe_waves",
    "simulate_lanes",
    "wave_members",
    "wave_room",
    "window_shapes",
]
