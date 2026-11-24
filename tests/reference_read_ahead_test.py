"""INPUT delivery reads reference pages ahead (D19): batches stay full, the row pool still bounds the rows.

Under INPUT delivery the consumer drains bindings in input order. A binding used to wait for the consumer to
leave each page before it fetched the next, so every binding behind the frontier stood still and the
frontier's continuation pages left almost one per physical batch. Now a binding reads its pages ahead into
its own queue while the row pool has room for them; the pool keeps room for the frontier and the bindings
nearest it, and the dispatcher counts the rows the consumer frees on its own as coalescing potential.

The transport answers after a fixed number of event-loop turns and a VirtualClock replaces the coalescing
timer, so every wave follows from the order of events alone, never from wall time or thread scheduling.
"""

from __future__ import annotations
import asyncio
from collections import Counter
from typing import TYPE_CHECKING

import pytest

from b24api import (
    BatchDispatch,
    Binding,
    CountedTraversal,
    CursorSpec,
    CursorTraversal,
    DirectDispatch,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    ResultSelector,
    SequentialTraversal,
    TerminalState,
)
from b24api.contracts import (
    Command,
    CommandSuccess,
    ContinuePage,
    DeliveryOrder,
    PageBoundary,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ReferenceSource,
)
from b24api.errors import BudgetExceededError
from tests.scripting import HANG_BOUND, BatchPortal, VirtualClock, client_for, on_loop

if TYPE_CHECKING:
    from b24api.contracts import OperationReport, PageStopPolicy, ReferenceOutcome, TraversalSpec
    from tests.scripting import Envelope, Query

PAGE = 5
# The default pool holds fifty pages, as 2 500 rows hold fifty pages of 50.
POOL = 50 * PAGE
REVIEW_PARENTS = 300
# Every tenth parent has eight pages, the rest one.
SKEWED_PAGES, SKEW = 8, 10
# Event-loop turns one physical request takes: the consumer and the producers run meanwhile. The
# consumer drains the pool faster than the wire fills it; a fast wire outpaces the consumer.
WIRE_TURNS, FAST_WIRE_TURNS = 200, 10
BATCH = 50
TRAVERSALS: dict[str, TraversalSpec] = {
    "sequential": SequentialTraversal(page_size=PAGE),
    "counted": CountedTraversal(page_size=PAGE),
}


def _pages(parent: int, pages: int | None) -> int:
    """Return a parent's pages: ``pages`` each, or skewed when ``None``."""
    if pages is not None:
        return pages
    return SKEWED_PAGES if parent % SKEW == 0 else 1


def _total(parent: int, pages: int | None) -> int:
    """Return a parent's rows: full pages, then a short last one."""
    return (_pages(parent, pages) - 1) * PAGE + PAGE // 2 + 1


def _portal(pages: int | None, turns: int) -> BatchPortal:
    def answer(_method: str, query: Query) -> Envelope:
        parent, offset = int(query["PARENT"]), int(query.get("start", "0"))
        total = _total(parent, pages)
        envelope: Envelope = {
            "result": [{"ID": parent * 100_000 + row} for row in range(offset, min(offset + PAGE, total))],
            "total": total,
        }
        if offset + PAGE < total:
            envelope["next"] = offset + PAGE
        return envelope

    return BatchPortal(answer, wire_turns=turns)


async def _traverse(  # noqa: PLR0913 - one scenario's knobs
    order: DeliveryOrder,
    traversal: TraversalSpec,
    *,
    parents: int = REVIEW_PARENTS,
    pages: int | None = 2,
    policy: ExecutionPolicy | None = None,
    turns: int = WIRE_TURNS,
    page_stop: PageStopPolicy | None = None,
    consumed: Counter[int] | None = None,
    budget_end: bool = False,
    coalesce_wait: float = BatchDispatch().coalesce_wait,
) -> tuple[list[ReferenceOutcome[int]], OperationReport, BatchPortal]:
    """Drain one reference traversal over ``parents`` bindings; fail rather than hang past ``HANG_BOUND``.

    ``consumed`` counts each binding's rows as the consumer receives them. With ``budget_end`` the stream must end
    with the operation's budget (``BudgetExceededError``), and what it delivered before is returned.
    """
    portal = _portal(pages, turns)
    bindings = on_loop(
        Binding("parent", (ParameterUpdate(ParameterPath(("PARENT",)), parent),), parent) for parent in range(parents)
    )
    async with client_for(portal, policy=policy or ExecutionPolicy(max_buffered_rows=POOL)) as client:
        stream = client.iter_reference_outcomes(
            Request.bare("gamma.list", {}, ReplaySafety.SAFE),
            bindings,
            traversal=traversal,
            dispatch=BatchDispatch(batch_size=BATCH, output_order=order, coalesce_wait=coalesce_wait),
            page_stop=page_stop,
        )
        events: list[ReferenceOutcome[int]] = []
        try:
            async with asyncio.timeout(HANG_BOUND), stream:
                async for event in stream:
                    events.append(event)
                    if consumed is not None and isinstance(event, ReferenceItem):
                        consumed[event.binding_index] += 1
        except BudgetExceededError:
            if not budget_end:
                raise
        else:
            assert not budget_end, "the stream did not end with the operation's budget"
    report = stream.report
    assert report is not None
    return events, report, portal


def _rows(events: list[ReferenceOutcome[int]]) -> list[tuple[int, object]]:
    return [
        (event.correlation, event.item["ID"] if isinstance(event.item, dict) else None)
        for event in events
        if isinstance(event, ReferenceItem)
    ]


def _expected(parents: int, pages: int | None) -> list[tuple[int, object]]:
    return [(parent, parent * 100_000 + row) for parent in range(parents) for row in range(_total(parent, pages))]


# The most physical requests READY and INPUT send. Before read-ahead INPUT sent 25 to 47 times as many as READY. A
# READY consumer takes every page waiting for it, so the pool projects its rows as coming back: counting them held
# would send READY 28 to 35 requests here. Each binding ahead of the INPUT frontier keeps the part of its room its pages
# in flight do not hold: keeping its whole room as well would cost the review's 5,000 bindings of one and two pages 201
# requests instead of 125, for 24 instead of 35 on the counted traversal of two pages here. A sequential binding ends
# at its page without ``next``: on the skewed sources that leaves READY 14 requests where it sent 22 with an empty
# confirmation per binding.
_REQUESTS = {
    (2, "sequential"): (12, 28),
    (2, "counted"): (13, 35),
    (None, "sequential"): (14, 59),
    (None, "counted"): (11, 15),
}


@pytest.mark.parametrize(("pages", "name"), list(_REQUESTS), ids=lambda value: str(value or "skewed"))
@pytest.mark.asyncio
async def test_input_batches_stay_near_ready_in_order_and_within_the_pool(
    monkeypatch: pytest.MonkeyPatch, pages: int | None, name: str
) -> None:
    """The review's 300 parents of two pages, and a skewed variant: INPUT fills its batches as READY does.

    The rows still arrive in binding order, every binding completes in turn, the row pool is never
    exceeded and no wave waits for the coalescing timer.
    """
    clock = VirtualClock().install(monkeypatch)
    traversal = TRAVERSALS[name]
    _ready_events, _ready_report, ready = await _traverse(DeliveryOrder.READY, traversal, pages=pages)
    events, report, portal = await _traverse(DeliveryOrder.INPUT, traversal, pages=pages)

    ready_requests, input_requests = _REQUESTS[pages, name]
    assert len(ready.waves) <= ready_requests
    assert len(portal.waves) <= input_requests
    assert sum(portal.waves) == sum(ready.waves)
    assert _rows(events) == _expected(REVIEW_PARENTS, pages)
    completes = [event.correlation for event in events if isinstance(event, ReferenceComplete)]
    assert completes == list(range(REVIEW_PARENTS))
    assert report.state is TerminalState.COMPLETED
    assert not report.violations
    assert report.buffered_rows_high_water <= POOL
    assert clock.timer_firings == 0


# Physical requests over the fewest that carry the commands; before read-ahead INPUT sent 47 and 24
# times the fewest, and as many as 26 and 24 times when only the frontier's rows counted as freed.
_FAST_WIRE_RATIO = {(2, "sequential"): 4.0, (None, "counted"): 4.0}


@pytest.mark.parametrize(("pages", "name"), list(_FAST_WIRE_RATIO), ids=lambda value: str(value or "skewed"))
@pytest.mark.asyncio
async def test_input_batches_wait_for_the_rows_the_consumer_frees_on_its_own(
    monkeypatch: pytest.MonkeyPatch, pages: int | None, name: str
) -> None:
    """When the wire outpaces the consumer, the pool fills with rows waiting for it, and waves still fill.

    The consumer frees the rows of every binding it passes with no page sent: those that add no page any
    more, and then the first one that still does. A wave waits for the pages they make room for.
    """
    clock = VirtualClock().install(monkeypatch)
    events, report, portal = await _traverse(DeliveryOrder.INPUT, TRAVERSALS[name], pages=pages, turns=FAST_WIRE_TURNS)

    fewest = -(-sum(portal.waves) // BATCH)
    assert len(portal.waves) <= _FAST_WIRE_RATIO[pages, name] * fewest
    assert _rows(events) == _expected(REVIEW_PARENTS, pages)
    assert report.buffered_rows_high_water <= POOL
    assert clock.timer_firings == 0


@pytest.mark.asyncio
async def test_input_completes_within_a_request_budget_ready_needs_little_more_than(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sixty requests carry the review's traversal under INPUT; before read-ahead it needed 852."""
    VirtualClock().install(monkeypatch)
    policy = ExecutionPolicy(max_buffered_rows=POOL, max_requests=60)
    events, report, portal = await _traverse(DeliveryOrder.INPUT, TRAVERSALS["sequential"], policy=policy)

    assert report.state is TerminalState.COMPLETED
    assert report.physical_requests == len(portal.waves) <= policy.max_requests
    assert _rows(events) == _expected(REVIEW_PARENTS, 2)


@pytest.mark.parametrize("name", list(TRAVERSALS))
@pytest.mark.asyncio
async def test_pages_read_ahead_never_outrun_the_page_budget(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    """Reading ahead spends logical pages before the consumer reaches them, never past ``max_pages``.

    The page the budget refuses ends the operation, as a refused request does: the rows delivered before it are
    each binding's own, still in binding order.
    """
    VirtualClock().install(monkeypatch)
    policy = ExecutionPolicy(max_buffered_rows=POOL, max_pages=100)
    events, report, portal = await _traverse(DeliveryOrder.INPUT, TRAVERSALS[name], policy=policy, budget_end=True)

    assert sum(portal.waves) == report.logical_pages <= policy.max_pages
    assert report.state is TerminalState.INCOMPLETE
    order = [event.correlation for event in events]
    assert order == sorted(order)
    rows = _rows(events)
    for parent in range(REVIEW_PARENTS):
        delivered = [row for correlation, row in rows if correlation == parent]
        assert delivered == [parent * 100_000 + row for row in range(len(delivered))]


@pytest.mark.parametrize("pool", [PAGE, 2 * PAGE], ids=["one-page", "two-pages"])
@pytest.mark.parametrize("name", list(TRAVERSALS))
@pytest.mark.asyncio
async def test_a_tiny_pool_still_moves_the_frontier(monkeypatch: pytest.MonkeyPatch, pool: int, name: str) -> None:
    """The frontier always has room: bindings reading ahead never take the rows it needs to progress.

    Every binding behind the frontier leaves free the frontier's next page, less the rows the frontier
    holds, which come back to it; so the traversal completes in order within the hang bound.
    """
    VirtualClock().install(monkeypatch)
    parents = 30
    events, report, _portal = await _traverse(
        DeliveryOrder.INPUT,
        TRAVERSALS[name],
        parents=parents,
        pages=None,
        policy=ExecutionPolicy(max_buffered_rows=pool),
    )

    assert not [event for event in events if isinstance(event, ReferenceFailure)]
    assert _rows(events) == _expected(parents, None)
    assert report.buffered_rows_high_water <= pool


class _OfferedAfterConsumption:
    """A page stop counting the pages offered before the consumer received all their rows."""

    def __init__(self, consumed: Counter[int]) -> None:
        """Watch the consumer's per-binding row counts."""
        self.consumed = consumed
        self.offered: Counter[int] = Counter()
        self.early = 0

    async def on_page(self, boundary: PageBoundary) -> ContinuePage:
        """Record whether the consumer has received every row offered so far."""
        self.offered[boundary.binding_id] += len(boundary.rows)
        self.early += self.consumed[boundary.binding_id] < self.offered[boundary.binding_id]
        return ContinuePage.CONTINUE


@pytest.mark.asyncio
async def test_a_page_stop_binding_still_waits_for_its_consumer(monkeypatch: pytest.MonkeyPatch) -> None:
    """A page stop sees a page only after the consumer received all its rows, so its binding never reads ahead."""
    VirtualClock().install(monkeypatch)
    parents, consumed = 30, Counter[int]()
    stop = _OfferedAfterConsumption(consumed)
    events, report, _portal = await _traverse(
        DeliveryOrder.INPUT,
        TRAVERSALS["sequential"],
        parents=parents,
        page_stop=stop,
        consumed=consumed,
    )

    assert report.state is TerminalState.COMPLETED
    assert _rows(events) == _expected(parents, 2)
    assert stop.offered.total() == len(events) - parents
    assert stop.early == 0


@pytest.mark.asyncio
async def test_input_fan_out_reads_whole_results_ahead(monkeypatch: pytest.MonkeyPatch) -> None:
    """Whole results read ahead too: commands leave in full batches while the consumer passes the results.

    Each result the consumer passes frees an admission slot, and the next command from the source fills
    the wave without any batch sent first. Before read-ahead every command left in a batch of its own.
    """
    clock = VirtualClock().install(monkeypatch)
    portal = BatchPortal(lambda _method, query: {"result": {"ID": int(query["id"])}}, wire_turns=WIRE_TURNS)
    commands = [
        Command(Request.bare("item.get", {"id": identity}, ReplaySafety.SAFE), identity) for identity in range(11)
    ]
    async with client_for(portal) as client:
        stream = client.fan_out_outcomes(
            on_loop(commands),
            dispatch=BatchDispatch(batch_size=3, concurrency=1, output_order=DeliveryOrder.INPUT),
        )
        async with asyncio.timeout(HANG_BOUND), stream:
            outcomes = [outcome async for outcome in stream]

    assert portal.waves == [3, 3, 3, 2]
    assert [outcome.correlation for outcome in outcomes if isinstance(outcome, CommandSuccess)] == list(range(11))
    assert clock.timer_firings == 0


# Two bindings of 58 pages; a wave of 50 takes most of a window, so a tail sends its pages in four waves.
WINDOW_PARENTS, WINDOW_PAGES, WINDOW_WAVES = 2, 58, 8


@pytest.mark.parametrize("active", [1, 4])
@pytest.mark.asyncio
async def test_a_counted_window_fills_its_waves_whatever_the_active_bindings(
    monkeypatch: pytest.MonkeyPatch, active: int
) -> None:
    """The pages waiting for a wave are bounded by their rows, not by the active bindings.

    One or four bindings active at once and no coalescing: each counted tail puts its window of
    pages in the queue at once, and a wave carries them all. A queue of one page per active binding sent the 116
    pages in 116 requests with one active binding, and four pages a request with four.
    """
    VirtualClock().install(monkeypatch)
    policy = ExecutionPolicy(max_buffered_rows=POOL, max_active_references=active)

    events, report, portal = await _traverse(
        DeliveryOrder.INPUT,
        TRAVERSALS["counted"],
        parents=WINDOW_PARENTS,
        pages=WINDOW_PAGES,
        policy=policy,
        coalesce_wait=0.0,
    )

    assert _rows(events) == _expected(WINDOW_PARENTS, WINDOW_PAGES)
    assert report.state is TerminalState.COMPLETED
    assert sum(portal.waves) == WINDOW_PARENTS * WINDOW_PAGES
    assert len(portal.waves) <= WINDOW_WAVES


# Sources whose page caps differ: cursor pages of 2 rows and sequential pages of 3. A binding's (method, rows, lag in
# event-loop turns): cursor rows count down from its rows, sequential rows count up to them.
_CURSOR = CursorSpec(ParameterPath(("BEFORE",)), ("ID",), IdentityCoercion.EXACT_INTEGER, "descending", "last")
_CURSOR_PAGES = CursorTraversal(selector=ResultSelector(), cursor=_CURSOR, page_size=2)
_SEQUENTIAL_PAGES = SequentialTraversal(
    identity=IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_INTEGER), page_size=3
)
type _MixedBindings = tuple[tuple[str, int, int], ...]
_MIXED_CAPS: dict[str, tuple[int, DirectDispatch | BatchDispatch, _MixedBindings]] = {
    "batch": (
        6,
        BatchDispatch(batch_size=2, concurrency=3, output_order=DeliveryOrder.INPUT, coalesce_wait=0.0),
        (("cursor", 4, 8), ("sequential", 12, 8), ("cursor", 12, 0), ("cursor", 2, 1), ("cursor", 2, 0)),
    ),
    "direct": (
        12,
        DirectDispatch(concurrency=5, output_order=DeliveryOrder.INPUT),
        (
            ("cursor", 6, 1),
            ("sequential", 18, 8),
            ("cursor", 6, 0),
            ("cursor", 12, 1),
            ("cursor", 12, 0),
            ("cursor", 12, 1),
            ("sequential", 15, 8),
        ),
    ),
    # Sources of one cap: the reserve is the cap the first admitted binding raised it to, before any frontier moved.
    "admitted": (
        3,
        BatchDispatch(batch_size=3, concurrency=1, output_order=DeliveryOrder.INPUT, coalesce_wait=0.0),
        (("cursor", 3, 4), ("cursor", 8, 0)),
    ),
}


def _mixed_rows(method: str, rows: int, query: Query) -> Envelope:
    if method == "cursor.list":
        before = query.get("BEFORE")
        top = rows if before is None else int(str(before)) - 1
        return {"result": [{"ID": value} for value in range(top, max(0, top - 2), -1)]}
    start = int(str(query.get("start", 0)))
    body: Envelope = {"result": [{"ID": value} for value in range(start + 1, min(rows, start + 3) + 1)]}
    if start + 3 < rows:
        body["next"] = start + 3
    return body


async def _read_mixed(
    bindings: _MixedBindings, budget: int, dispatch: DirectDispatch | BatchDispatch
) -> tuple[BatchPortal, list[object]]:
    async def answer(method: str, query: Query) -> Envelope:
        _kind, rows, lag = bindings[int(str(query["PARENT"]))]
        for _turn in range(lag):
            await asyncio.sleep(0)
        return _mixed_rows(method, rows, query)

    sources = [
        ReferenceSource(
            Request.bare(f"{kind}.list", {}, ReplaySafety.SAFE),
            _CURSOR_PAGES if kind == "cursor" else _SEQUENTIAL_PAGES,
            Binding("parent", (ParameterUpdate(ParameterPath(("PARENT",)), index),), index),
        )
        for index, (kind, _rows, _lag) in enumerate(bindings)
    ]
    portal = BatchPortal(answer)
    policy = ExecutionPolicy(max_elapsed=_MIXED_ELAPSED, max_buffered_rows=budget)
    async with client_for(portal, policy=policy) as client:
        stream = client.iter_reference_source_outcomes(on_loop(sources), dispatch=dispatch)
        async with stream:
            events: list[object] = [event async for event in stream]
    assert not [event for event in events if isinstance(event, ReferenceFailure)]
    delivered = [event.binding_index for event in events if isinstance(event, ReferenceItem)]
    assert delivered == [index for index, (_kind, rows, _lag) in enumerate(bindings) for _row in range(rows)]
    return portal, events


@pytest.mark.parametrize("name", list(_MIXED_CAPS))
@pytest.mark.asyncio
async def test_the_input_frontier_finds_its_next_page_whatever_cap_the_binding_that_becomes_it_has(name: str) -> None:
    """The frontier reserve covers the largest cap of the producers still adding pages, raised as each is admitted.

    Bindings behind the frontier read ahead into the pool; with a reserve smaller than the frontier's page, the pages
    read ahead behind it could hold the rows its next page needs, and the read would wait until ``max_elapsed``.
    """
    budget, dispatch, bindings = _MIXED_CAPS[name]

    await _read_mixed(bindings, budget, dispatch)


@pytest.mark.asyncio
async def test_the_input_frontier_reserve_shrinks_to_the_cap_of_the_producers_left(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once the binding with the largest page has ended, the frontier keeps free only the page of those left.

    A sequential binding (3-row pages) and two cursor bindings (2-row pages) share 4 rows. Once the sequential one ends,
    the frontier keeps 2 rows for its next page, so the next binding's page joins the frontier's wave; a reserve still
    sized for 3 would send it alone.
    """
    VirtualClock().install(monkeypatch)
    dispatch = BatchDispatch(batch_size=2, concurrency=1, output_order=DeliveryOrder.INPUT, coalesce_wait=0.0)

    portal, _events = await _read_mixed((("sequential", 3, 0), ("cursor", 2, 0), ("cursor", 2, 0)), 4, dispatch)

    assert portal.waves == [1, 2, 1, 1]


# A hang ends the read with a refusal within this budget, so the test fails rather than waits.
_MIXED_ELAPSED = 5.0
