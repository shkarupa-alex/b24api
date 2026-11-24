"""White-box reference scheduling: the batch assembler's coalescing read and potential, fast lease sizing.

S3 shares one batch queue and one row pool among every reference binding. A page the assembler's
coalescing read took joins the round-robin backlog before the next chunk forms, a page on its way to
the queue counts as coalescing potential until it arrives, and a fast keyset lease is
``clamp(free - R_keep, P, G_cap)``.
"""

from __future__ import annotations
import asyncio
import contextlib
import functools
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from b24api import (
    AutoKeysetExecution,
    BatchDispatch,
    BatchFailed,
    CountedTraversal,
    DirectDispatch,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    KeysetTraversal,
    ParameterPath,
    ReferenceFailed,
    ReplaySafety,
    Request,
    ResultSelector,
    SequentialTraversal,
    StableIntegerKeysetContract,
    TerminalState,
)
from b24api.batch.engine import BatchExecutor
from b24api.contracts import Binding, DeliveryOrder, ParameterUpdate, ReferenceItem, ReferenceSource
from b24api.contracts.command import Command
from b24api.contracts.completion import CommandSettlement, PageAcknowledged, PageDelivered
from b24api.contracts.policy import AmbiguityReason
from b24api.contracts.request import RouteKind
from b24api.errors import AmbiguousExecutionError, BudgetExceededError
from b24api.execution import Executor, RateCoordinator
from b24api.execution.context import CleanupResult, ExecutionContext, spawn_settled
from b24api.execution.facts import Execution, execution, settlement
from b24api.references.counted_io import ReferenceTailStream, _abandoned_settlement, _TailSlot
from b24api.references.dispatch import (
    _BatchPageDispatcher,
    _ChunkPageError,
    _DoneEvent,
    _failed_page,
    _FailureEvent,
    _PendingBatch,
    _ProducerState,
    _Producing,
    _RowBuffer,
)
from b24api.references.scheduler import ReferenceScheduler
from b24api.transport import TransportCapabilities
from b24api.traversal.counted_batch import _settle_tail_cleanup
from b24api.traversal.plans import KernelBatchDispatch, ReferenceOutputOrder
from tests.scripting import (
    DEADLINE_BUDGET,
    HANG_BOUND,
    UNREACHABLE_COALESCE_WAIT,
    BatchPortal,
    Blocker,
    ResponderTransport,
    VirtualClock,
    client_for,
    on_loop,
    reached,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from b24api.contracts import TraversalSpec
    from b24api.references.dispatch import _AdmissionState, _DispatchedPage, _Event, _Transit, _Work
    from b24api.references.program import FastKeysetProgram
    from b24api.references.run import _ReferenceRun
    from b24api.transport import WireRequest, WireResponse
    from b24api.traversal.identity import _Page
    from tests.scripting import Answer, Envelope, Query


_COALESCING_TURNS = 1000


def _counted_answer(_method: str, query: Query) -> Envelope:
    """Serve the 1001-row counted fixture in pages of 50."""
    offset = int(query.get("start", "0"))
    envelope: Envelope = {"result": [{"ID": row} for row in range(offset, min(offset + 50, 1001))], "total": 1001}
    if offset + 50 < 1001:  # noqa: PLR2004 - the counted fixture's size
        envelope["next"] = offset + 50
    return envelope


def _dispatcher_page(key: str) -> _PendingBatch:
    loop = asyncio.get_running_loop()
    request = Request("item.list", {"PARENT": key}, route=RouteKind.BARE)
    futures = (loop.create_future(), loop.create_future(), loop.create_future())
    return _PendingBatch(request, key, *futures)


def _settle(page: _PendingBatch) -> None:
    page.admitted.set_result(None)
    page.settled.set_result(None)


async def _parked_page_dispatcher() -> tuple[_BatchPageDispatcher, BatchPortal, list[_PendingBatch], _PendingBatch]:
    """Leave a page in the assembler's completed coalescing read while four pages fill the backlog.

    The producer state changes before the coalescing read runs: five pages of producer ``x`` are
    queued, the assembler drains them and sends ``a`` with the first one, and the read stays
    outstanding. While the assembler waits for a send slot, producer ``c``'s page arrives and the
    read takes it.
    """
    portal = BatchPortal(lambda _method, _query: {"result": []})
    executor = Executor(portal)
    context = executor.context(ExecutionPolicy(max_pages=100, max_pages_per_reference=100))
    await context.start()
    state = _ProducerState({"a": 0, "x": 1, "c": 2}, dict.fromkeys(("a", "x", "c"), _Producing.RUNNABLE))
    buffer = _RowBuffer(100, context, producer_state=state)
    first, leftovers, late = _dispatcher_page("a"), [_dispatcher_page("x") for _ in range(5)], _dispatcher_page("c")
    injected = False

    async def state_changes_first(futures: tuple[asyncio.Future[object], ...], seconds: float) -> None:
        nonlocal injected
        if not injected:
            injected = True
            for page in leftovers:
                dispatcher._queue.put_nowait(page)  # noqa: SLF001
            return
        await asyncio.wait(futures, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)

    dispatcher = _BatchPageDispatcher(
        executor,
        context,
        KernelBatchDispatch(batch_size=2, concurrency=1, coalesce_wait=UNREACHABLE_COALESCE_WAIT),
        producer_state=state,
        buffer=buffer,
        wait=state_changes_first,
    )
    dispatcher._queue.put_nowait(first)  # noqa: SLF001
    dispatcher._ensure_workers()  # noqa: SLF001
    await asyncio.wait_for(asyncio.gather(first.future, leftovers[0].future), timeout=HANG_BOUND)
    dispatcher._queue.put_nowait(late)  # noqa: SLF001
    for _ in range(5):
        await asyncio.sleep(0)
    return dispatcher, portal, [first, *leftovers], late


def _now(dispatcher: _BatchPageDispatcher, keys: set[str]) -> bool:
    """Return whether the assembler's coalescing read projects a page joining a chunk of ``keys``, no wave landing."""
    return dispatcher._potential(keys)[0]  # noqa: SLF001 - the assembler's coalescing read


@pytest.mark.asyncio
async def test_a_page_the_coalescing_read_took_joins_the_next_round_robin_wave() -> None:
    """A full backlog at every wave start used to keep the read's page out of every wave."""
    dispatcher, portal, pages, late = await _parked_page_dispatcher()
    _settle(pages[0])
    _settle(pages[1])
    await asyncio.wait_for(late.future, timeout=HANG_BOUND)
    for page in (*pages[2:], late):
        if page.future.done():
            _settle(page)
    await dispatcher.aclose()
    parents = [query["PARENT"] for query in portal.queries]
    assert parents[:4] == ["a", "x", "x", "c"]
    assert portal.waves[:2] == [2, 2]


@pytest.mark.asyncio
async def test_closing_the_dispatcher_cancels_a_page_the_coalescing_read_took() -> None:
    dispatcher, _portal, pages, late = await _parked_page_dispatcher()
    await dispatcher.aclose()
    assert late.future.cancelled()
    assert all(page.future.done() for page in pages)


@pytest.mark.asyncio
async def test_a_page_on_its_way_to_the_queue_is_potential_until_it_arrives() -> None:
    """A started command whose producer left the runnable set counts once, whichever way it ends.

    One that must still reserve its logical page counts only while the page budget admits it; one
    holding its reservation counts regardless. Neither depends on the chunk being formed, and a
    retired producer's commands never count.
    """
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy(max_pages=1, max_pages_per_reference=1))
    await context.start()
    state = _ProducerState({"r0": 0})
    buffer = _RowBuffer(10, context, producer_state=state)
    dispatcher = _BatchPageDispatcher(
        executor,
        context,
        KernelBatchDispatch(batch_size=2, coalesce_wait=UNREACHABLE_COALESCE_WAIT),
        producer_state=state,
        buffer=buffer,
    )
    potential = functools.partial(_now, dispatcher)
    assert potential(set()) == 0

    reserved = state.depart("r0", page_reserved=True)
    assert potential({"r0"})
    reserved.arrive()
    reserved.arrive()
    assert potential(set()) == 0
    assert not state.in_transit

    reserving = state.depart("r0", page_reserved=False)
    assert potential(set()) == 1
    page = await context.reserve_page(reference="r0")
    assert potential(set()) == 0
    reserving.reserved()
    assert potential(set()) == 1
    reserving.arrive()
    context.release_page(page)
    assert not state.in_transit
    assert not state.reserving

    retired = state.depart("r0", page_reserved=True)
    del state.indexes["r0"]
    assert potential(set()) == 0
    retired.arrive()

    async def never_started() -> _DispatchedPage:
        raise AssertionError("cancelled before its first step")

    cancelled = state.depart("r1", page_reserved=False)
    task = spawn_settled(never_started())
    cancelled.carried_by(task)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert not state.reserving
    await buffer.close()


@pytest.mark.parametrize("ready", [True, False])
@pytest.mark.asyncio
async def test_a_pending_refill_is_potential_whatever_the_chunk_carries(*, ready: bool) -> None:
    """A windowed producer's refill counts while its own pages wait in the chunk, under either order.

    Under INPUT delivery a binding behind the frontier reads ahead as well: its refill waits on its own
    tail, not on the frontier's consumer.
    """
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy(max_pages=100, max_pages_per_reference=100))
    await context.start()
    state = _ProducerState({"r0": 0, "r1": 1})
    buffer = _RowBuffer(10, context, producer_state=state, ordered=not ready)
    dispatcher = _BatchPageDispatcher(
        executor,
        context,
        KernelBatchDispatch(batch_size=2, coalesce_wait=UNREACHABLE_COALESCE_WAIT),
        producer_state=state,
        buffer=buffer,
        pending_continuations_can_progress=ready,
    )
    potential = functools.partial(_now, dispatcher)
    state.move("r1", _Producing.REFILLING)
    assert potential({"r1"})
    state.move("r0", _Producing.REFILLING)
    assert potential({"r0", "r1"})
    state.move("r0", _Producing.AWAITING_CONSUMER)
    state.move("r1", _Producing.WAITING)
    assert not potential({"r0"})
    await buffer.close()


@pytest.mark.asyncio
async def test_an_input_page_behind_the_frontier_leaves_room_for_the_bindings_nearer_it() -> None:
    """Bindings read ahead under INPUT: a page keeps free the next page of each producer ahead of it.

    The frontier's room, less the rows it holds, and the room of every producer between it and the page
    stay free; a producer that retires keeps none. The frontier itself is never held back.
    """
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy())
    await context.start()
    state = _ProducerState({"r0": 0, "r1": 1, "r3": 3}, caps={"r0": 2, "r1": 2, "r3": 2})
    buffer = _RowBuffer(10, context, head_reserve=2, producer_state=state, ordered=True)
    held = await buffer.reserve(2, 5)
    # Five rows free: the frontier's page and r1's page stay free, so r3's page does not fit.
    assert not buffer.can_reserve(3, 2)
    assert buffer.can_reserve(1, 2)
    assert buffer.can_reserve(0, 5)
    state.retire("r1")
    assert buffer.can_reserve(3, 2)
    await buffer.abort(held)
    await buffer.close()


@pytest.mark.asyncio
async def test_the_input_drain_passes_the_bindings_that_add_no_page() -> None:
    """The INPUT consumer frees, with no page sent, the queued rows up to the first binding still producing."""
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy())
    await context.start()
    state = _ProducerState({"r2": 2}, caps={"r2": 2}, admitted=3)
    buffer = _RowBuffer(10, context, head_reserve=2, producer_state=state, ordered=True)
    reservations = [await buffer.reserve(index, amount) for index, amount in ((0, 3), (1, 2), (2, 2))]
    for reservation, rows in zip(reservations, (3, 2, 1), strict=True):
        await buffer.accept(reservation, rows)
    drain = buffer.drain()
    assert drain is not None
    assert (drain.head, drain.rows, drain.head_rows) == (2, 6, 1)
    # Four rows are free now; ten once the consumer has drained the queued pages.
    assert not buffer.can_reserve(3, 8)
    assert buffer.can_reserve(3, 8, drain=drain)
    # A binding still producing stops the drain: the consumer waits for its next page.
    state.register("r1", 1, 2)
    drain = buffer.drain()
    assert drain is not None
    assert (drain.head, drain.rows) == (1, 5)
    assert _RowBuffer(10, context, producer_state=state).drain() is None
    for reservation in reservations:
        await buffer.abort(reservation)
    await buffer.close()


def _producers(*indexes: int, cap: int = 2) -> _ProducerState:
    state = _ProducerState({})
    for index in indexes:
        state.register(f"r{index}", index, cap)
    return state


@pytest.mark.asyncio
async def test_an_ordered_pool_wakes_the_waiting_pages_nearest_the_frontier_first() -> None:
    """Rows coming back go to the waiting page nearest the frontier that fits.

    A binding ahead that holds its page already, here one woken for its rows, keeps no further room: its rows
    come back to it before its next page.
    """
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy())
    await context.start()
    buffer = _RowBuffer(10, context, head_reserve=2, producer_state=_producers(0, 1, 2, 3), ordered=True)
    frontier = await buffer.reserve(0, 10)
    await buffer.accept(frontier, 10)
    page = 2
    waiting = {index: asyncio.create_task(buffer.reserve(index, page)) for index in (3, 1, 2)}
    await asyncio.sleep(0)
    assert not any(task.done() for task in waiting.values())

    await buffer.release(frontier, 2)
    granted = await waiting[1]
    await asyncio.sleep(0)
    assert not waiting[2].done()
    await buffer.release(frontier, 2)
    assert (await waiting[2]).amount == page
    await asyncio.sleep(0)
    assert not waiting[3].done()
    await buffer.release(frontier, 2)
    assert (await waiting[3]).amount == page

    await buffer.close()
    assert granted.amount == 0


@pytest.mark.asyncio
async def test_an_ordered_pool_keeps_the_room_of_a_binding_ahead_that_holds_no_page() -> None:
    """A producer between the frontier and a page that holds no rows keeps its next page's room free."""
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy())
    await context.start()
    buffer = _RowBuffer(10, context, head_reserve=2, producer_state=_producers(0, 1, 2), ordered=True)
    frontier = await buffer.reserve(0, 10)
    await buffer.accept(frontier, 10)
    page = 2
    waiting = asyncio.create_task(buffer.reserve(2, page))
    await buffer.release(frontier, page)
    await asyncio.sleep(0)
    # Two rows are free, but r1, nearer the frontier, holds nothing: they stay its room.
    assert not waiting.done()
    await buffer.release(frontier, page)
    assert (await waiting).amount == page
    await buffer.close()


@pytest.mark.asyncio
async def test_an_ordered_pool_page_that_waits_out_the_elapsed_budget_leaves_the_queue_and_holds_nothing() -> None:
    """A page behind the frontier that waits for rows past the operation's elapsed budget ends unsent.

    It leaves the queue and holds no row, so the rows released afterwards go to the page that still waits.
    """
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy(max_elapsed=DEADLINE_BUDGET))
    await context.start()
    buffer = _RowBuffer(10, context, head_reserve=2, producer_state=_producers(0, 1, 2), ordered=True)
    frontier = await buffer.reserve(0, 10)
    await buffer.accept(frontier, 10)

    with pytest.raises(BudgetExceededError, match="row buffer wait exceeded operation time budget"):
        await buffer.reserve(2, 2)

    assert not buffer._waiters  # noqa: SLF001
    assert (buffer._available, buffer._reservations) == (0, [frontier])  # noqa: SLF001
    assert dict(buffer._held) == {0: 10}  # noqa: SLF001
    await buffer.close()


@pytest.mark.asyncio
async def test_an_ordered_pool_page_that_stops_waiting_once_woken_hands_its_rows_to_the_next() -> None:
    """A woken page whose wait ends before its task takes the rows gives them back to the pages still waiting.

    The elapsed budget ends a wait by cancelling it, as here: the rows promised to the woken page would
    otherwise be withheld from every later page.
    """
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy())
    await context.start()
    buffer = _RowBuffer(10, context, head_reserve=2, producer_state=_producers(0, 1), ordered=True)
    frontier = await buffer.reserve(0, 10)
    await buffer.accept(frontier, 10)
    page = 2
    first = asyncio.create_task(buffer.reserve(1, page))
    second = asyncio.create_task(buffer.reserve(1, page))
    await asyncio.sleep(0)

    # The rows are promised to the first page, which stops waiting before its task resumes.
    await buffer.release(frontier, page)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    async with asyncio.timeout(HANG_BOUND):
        assert (await second).amount == page
    assert not buffer._waiters  # noqa: SLF001
    assert buffer._promised == 0  # noqa: SLF001
    await buffer.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("ordered", [False, True], ids=["ready", "input"])
async def test_a_release_wakes_only_the_waiting_pages_its_rows_serve(ordered: bool) -> None:  # noqa: FBT001
    """Rows coming back wake the waiting pages they serve and no others, so the wake-ups stay linear.

    Every page woken takes its rows once its task resumes; until then they are promised to it, and a page
    woken for the same rows would only queue again.
    """
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy())
    await context.start()
    pages, page = 5, 2
    buffer = _RowBuffer(pages * page, context, producer_state=_producers(0, 1) if ordered else None, ordered=ordered)
    held = await buffer.reserve(0, pages * page)
    await buffer.accept(held, pages * page)
    tasks = [asyncio.create_task(buffer.reserve(1, page)) for _page in range(pages)]
    await asyncio.sleep(0)
    waiters = list(buffer._waiters)  # noqa: SLF001
    assert len(waiters) == pages

    for served in range(1, pages + 1):
        await buffer.release(held, page)
        assert sum(waiter.woken.done() for waiter in waiters) == served
        async with asyncio.timeout(HANG_BOUND):
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        tasks = [task for task in tasks if not task.done()]
    await buffer.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("taker", ["page", "lease"])
async def test_rows_promised_to_a_woken_page_are_its_own_until_it_resumes(taker: str) -> None:
    """A page or a lease asking for rows after a release does not take those a woken page was promised.

    It took them, so the woken page found its rows gone and queued again, and the page that fitted in what was
    left stayed parked until the next change of the pool.
    """
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy())
    await context.start()
    buffer = _RowBuffer(5, context)
    held = await buffer.reserve(0, 5)
    await buffer.accept(held, 5)
    woken = asyncio.create_task(buffer.reserve(1, 5))
    await asyncio.sleep(0)

    await buffer.release(held, 5)
    # Eager, so the later request asks before the woken page resumes, as one already running would.
    later = asyncio.eager_task_factory(
        asyncio.get_running_loop(),
        buffer.reserve(2, 1) if taker == "page" else buffer.lease(2, lambda free: free or None),
    )
    assert not later.done()
    async with asyncio.timeout(HANG_BOUND):
        reservation = await woken
    assert (reservation.amount, later.done()) == (5, False)

    await buffer.abort(reservation)
    async with asyncio.timeout(HANG_BOUND):
        assert (await later).amount == (1 if taker == "page" else 5)
    await buffer.close()


@pytest.mark.asyncio
async def test_a_woken_page_that_no_longer_fits_gives_its_promise_to_the_pages_it_serves() -> None:
    """A woken page that no longer fits when it resumes waits again and wakes the waiting pages that fit.

    Here the frontier's reserve grows between the wake and the resume; the page's promise went back to the pool
    with no page woken for it, so the smaller page that fitted stayed parked.
    """
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy())
    await context.start()
    buffer = _RowBuffer(10, context, ordered=True)
    held = await buffer.reserve(0, 10)
    await buffer.accept(held, 10)
    small_page = 2
    large = asyncio.create_task(buffer.reserve(1, 9))
    small = asyncio.create_task(buffer.reserve(2, small_page))
    await asyncio.sleep(0)

    await buffer.release(held, 10)
    buffer.raise_head_reserve(5)
    async with asyncio.timeout(HANG_BOUND):
        assert (await small).amount == small_page
    assert not large.done()
    large.cancel()
    await buffer.close()


@pytest.mark.asyncio
async def test_a_page_given_back_leaves_the_pool_and_no_other() -> None:
    """The pool lists each reservation as its own: giving one back removes that one, not an equal one.

    A page charged to a lease holds no pool rows; once an aborted page's rows were zeroed, the pool removed
    that lease page in its place and kept the aborted page as one that may still return rows.
    """
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy())
    await context.start()
    buffer = _RowBuffer(10, context)
    lease_page = await buffer.reserve(0, 0)
    page = await buffer.reserve(0, 7)

    await buffer.abort(page)

    assert not buffer.releasable_elsewhere(1)
    await buffer.accept(lease_page, 0)
    assert buffer._reservations == []  # noqa: SLF001
    await buffer.close()


@pytest.mark.asyncio
async def test_an_input_tail_keeps_room_for_the_pages_it_puts_in_flight_at_once() -> None:
    """A counted tail sends up to its window of pages at once: the pages behind it leave room for all of them."""
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy())
    await context.start()
    state = _producers(0, 1, 2)
    buffer = _RowBuffer(10, context, head_reserve=2, producer_state=state, ordered=True)
    held = await buffer.reserve(0, 2)
    # Eight rows free: the frontier holds its reserve, r1 keeps room for one page.
    assert buffer.can_reserve(2, 6)
    state.set_window("r1", 3)
    assert not buffer.can_reserve(2, 4)
    assert buffer.can_reserve(2, 2)
    # Its last page sent, the tail keeps no room.
    state.set_window("r1", 0)
    assert buffer.can_reserve(2, 8)
    await buffer.abort(held)
    await buffer.close()


@pytest.mark.asyncio
async def test_a_binding_charged_to_its_lease_keeps_no_room_behind_it() -> None:
    """Once a fast keyset binding charges its pages to its lease, the pages behind it keep no room for them."""
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy())
    await context.start()
    state = _producers(0, 1, 2)
    buffer = _RowBuffer(10, context, head_reserve=2, producer_state=state, ordered=True)
    held = await buffer.reserve(0, 2)
    # Eight rows free: r1 keeps room for one page of two rows.
    assert not buffer.can_reserve(2, 8)
    state.charge_to_lease("r1")
    assert buffer.can_reserve(2, 8)
    await buffer.abort(held)
    await buffer.close()


@pytest.mark.asyncio
async def test_an_input_consumer_draining_a_queued_page_wakes_the_dispatcher_with_no_fact_changed() -> None:
    """The drain projection counts a queued page's rows already: the consumer freeing them changes no fact it reads."""
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy())
    await context.start()
    for ordered in (True, False):
        state = _producers(0, 1)
        buffer = _RowBuffer(10, context, head_reserve=2, producer_state=state, ordered=ordered)
        page = await buffer.reserve(0, 4)
        await buffer.accept(page, 4)
        facts, revision = state.facts, state.revision
        projected = [buffer.can_reserve(1, rows, drain=buffer.drain()) for rows in (8, 9)]
        await buffer.release(page, 1)
        assert state.revision == revision + 1
        assert state.facts == facts + (not ordered)
        if ordered:
            assert projected == [buffer.can_reserve(1, rows, drain=buffer.drain()) for rows in (8, 9)] == [True, False]
        await buffer.abort(page)
        await buffer.close()


@pytest.mark.asyncio
async def test_a_refill_counts_the_rows_its_consumer_returns_first() -> None:
    """A refill waiting on a held receipt reserves from the rows that receipt returns, not only free ones."""
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy(max_pages=100, max_pages_per_reference=100))
    await context.start()
    state = _ProducerState({"r0": 0}, default_cap=2)
    buffer = _RowBuffer(2, context, producer_state=state)
    dispatcher = _BatchPageDispatcher(
        executor,
        context,
        KernelBatchDispatch(batch_size=2, coalesce_wait=UNREACHABLE_COALESCE_WAIT),
        producer_state=state,
        buffer=buffer,
    )
    potential = functools.partial(_now, dispatcher)
    held = await buffer.reserve(0, 2)
    state.move("r0", _Producing.REFILLING)
    assert not potential({"r0"})
    state.move("r0", _Producing.REFILLING, returns=held)
    assert potential({"r0"})
    page = await context.reserve_page(reference="r0")
    assert potential({"r0"})
    pages = [await context.reserve_page(reference="r0") for _ in range(99)]
    # The page budget is spent: the refill could not reserve its logical page.
    assert not potential({"r0"})
    for reservation in (page, *pages):
        context.release_page(reservation)
    await buffer.abort(held)
    await buffer.close()


@pytest.mark.asyncio
async def test_every_fast_keyset_wave_command_is_counted_in_transit_until_it_is_queued(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The wave engine leaves the runnable set as it starts a wave: each command counts until queued."""
    fetch = _BatchPageDispatcher.fetch
    commands: list[bool] = []

    async def tracked(
        self: _BatchPageDispatcher,
        request: Request,
        reference_id: str,
        *,
        transit: _Transit | None = None,
        **options: object,
    ) -> _DispatchedPage:
        if options.get("page_reserved"):
            commands.append(transit is not None and not transit.arrived and transit.page_reserved)
        return await fetch(self, request, reference_id, transit=transit, **options)  # type: ignore[arg-type]

    monkeypatch.setattr(_BatchPageDispatcher, "fetch", tracked)

    def answer(_method: str, query: Query) -> Envelope:
        rows = [
            row
            for row in range(1, 26)
            if row > int(query.get("filter[>ID]", "0")) and row < int(query.get("filter[<ID]", "26"))
        ]
        rows.sort(reverse=query.get("order[ID]") == "DESC")
        return {"result": [{"ID": row} for row in rows[: int(query.get("limit", "10"))]]}

    traversal = KeysetTraversal(
        ResultSelector(),
        IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_INTEGER),
        page_size=10,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=AutoKeysetExecution(StableIntegerKeysetContract()),
    )
    sources = [
        ReferenceSource(
            Request.bare("kappa.list", {}),
            traversal,
            Binding("parent", (ParameterUpdate(ParameterPath(("PARENT",)), parent),), parent),
        )
        for parent in (1, 2)
    ]
    async with client_for(BatchPortal(answer)) as api:
        stream = api.iter_reference_source_outcomes(sources, dispatch=BatchDispatch())
        async with stream:
            _events = [event async for event in stream]
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert commands
    assert all(commands)


@pytest.mark.asyncio
async def test_every_counted_tail_page_reaches_fetch_in_transit_and_leaves_it_reserved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tail page counts in transit from its start, gated by the page budget until fetch reserves its page."""
    fetch = _BatchPageDispatcher.fetch
    pages: list[tuple[bool, bool]] = []

    async def tracked(
        self: _BatchPageDispatcher,
        request: Request,
        reference_id: str,
        *,
        transit: _Transit | None = None,
        **options: object,
    ) -> _DispatchedPage:
        if transit is None:
            return await fetch(self, request, reference_id, **options)  # type: ignore[arg-type]
        entered = not transit.arrived and not transit.page_reserved
        dispatched = await fetch(self, request, reference_id, transit=transit, **options)  # type: ignore[arg-type]
        pages.append((entered, transit.arrived and transit.page_reserved))
        return dispatched

    monkeypatch.setattr(_BatchPageDispatcher, "fetch", tracked)

    def answer(_method: str, query: Query) -> Envelope:
        offset = int(query.get("start", "0"))
        envelope: Envelope = {"result": [{"ID": row} for row in range(offset, min(offset + 50, 151))], "total": 151}
        if offset + 50 < 151:  # noqa: PLR2004 - the counted fixture's size
            envelope["next"] = offset + 50
        return envelope

    binding = Binding("parent", (ParameterUpdate(ParameterPath(("PARENT",)), 1),), 1)
    source = ReferenceSource(Request.bare("gamma.list", {}), CountedTraversal(), binding)
    async with client_for(BatchPortal(answer)) as api:
        stream = api.iter_reference_source_outcomes([source], dispatch=BatchDispatch())
        async with stream:
            _events = [event async for event in stream]
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert pages == [(True, True)] * 3  # offsets 50, 100 and 150


@pytest.mark.parametrize("ready", [True, False])
@pytest.mark.asyncio
async def test_a_failed_counted_binding_closes_its_tail_before_its_terminal_event(
    monkeypatch: pytest.MonkeyPatch, *, ready: bool
) -> None:
    """The tail is settled and stops advertising a refill before the binding's failure is emitted."""
    deliver = ReferenceScheduler._deliver_page  # noqa: SLF001 - the delivery seam the failure enters
    delivered = 0
    observed: list[tuple[bool, set[int]]] = []

    async def failing(self: ReferenceScheduler, run: _ReferenceRun, page: _Page) -> bool:
        nonlocal delivered
        delivered += 1
        if delivered == 2:  # noqa: PLR2004 - the first tail page, after the head
            raise RuntimeError("delivery failed")
        return await deliver(self, run, page)

    failure_event = ReferenceScheduler._failure_event  # noqa: SLF001 - observed when it is built

    def observing(self: ReferenceScheduler, run: _ReferenceRun, error: Exception) -> object:
        observed.append(
            (
                self.producer_state.status.get(run.producer_key) is _Producing.REFILLING,
                set(run.completion._reserved),  # noqa: SLF001 - pages the binding has not settled
            )
        )
        return failure_event(self, run, error)

    monkeypatch.setattr(ReferenceScheduler, "_deliver_page", failing)
    monkeypatch.setattr(ReferenceScheduler, "_failure_event", observing)

    def answer(_method: str, query: Query) -> Envelope:
        offset = int(query.get("start", "0"))
        envelope: Envelope = {"result": [{"ID": row} for row in range(offset, min(offset + 50, 1001))], "total": 1001}
        if offset + 50 < 1001:  # noqa: PLR2004 - the counted fixture's size
            envelope["next"] = offset + 50
        return envelope

    binding = Binding("parent", (ParameterUpdate(ParameterPath(("PARENT",)), 1),), 1)
    source = ReferenceSource(Request.bare("gamma.list", {}), CountedTraversal(), binding)
    order = DeliveryOrder.READY if ready else DeliveryOrder.INPUT
    async with client_for(BatchPortal(answer), policy=ExecutionPolicy(max_buffered_rows=100)) as api:
        stream = api.iter_reference_source_outcomes([source], dispatch=BatchDispatch(batch_size=10, output_order=order))
        async with stream:
            _events = [event async for event in stream]
    assert stream.report is not None
    assert observed == [(False, set())]


# A terminal event whose publication waits, as it would for room in its queue, longer than a wave waits for an event.
_STALLED_PUBLISH_TURNS = 200
_FAILING_PARENT = 1


def _rows_answer(*, counted: bool, fail: bool) -> Answer:
    """Serve 20 rows per parent in pages of the request's size; the failing parent is refused its second page."""

    def answer(_method: str, query: Query) -> Envelope:
        offset, size = int(query.get("start", "0")), int(query.get("limit", "2"))
        if fail and int(query["PARENT"]) == _FAILING_PARENT and offset == size:
            return {"error": "ACCESS_DENIED", "error_description": "denied"}
        envelope: Envelope = {"result": [{"ID": row} for row in range(offset, min(offset + size, 20))]}
        if counted:
            envelope["total"] = 20
        if offset + size < 20:  # noqa: PLR2004 - the fixture's size
            envelope["next"] = offset + size
        return envelope

    return answer


def _keyset_answer(*, fail: bool) -> Answer:
    """Serve identities 1..60 by keyset; the failing parent is refused its first command."""

    def answer(_method: str, query: Query) -> Envelope:
        if fail and int(query["PARENT"]) == _FAILING_PARENT:
            return {"error": "ACCESS_DENIED", "error_description": "denied"}
        rows = [
            row
            for row in range(1, 61)
            if int(query.get("filter[>ID]", "0")) < row < int(query.get("filter[<ID]", "61"))
        ]
        rows.sort(reverse=query.get("order[ID]") == "DESC")
        return {"result": [{"ID": row} for row in rows[: int(query.get("limit", "10"))]]}

    return answer


_INTEGER_ID = IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_INTEGER)
_PUBLISHING = {
    "sequential": (SequentialTraversal(identity=_INTEGER_ID, page_size=2), False),
    "counted": (CountedTraversal(page_size=2), True),
    "fast-keyset": (
        KeysetTraversal(
            ResultSelector(),
            _INTEGER_ID,
            page_size=10,
            keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
            execution=AutoKeysetExecution(StableIntegerKeysetContract()),
        ),
        None,
    ),
}


@pytest.mark.parametrize("fail", [True, False], ids=["failure", "success"])
@pytest.mark.parametrize("order", [DeliveryOrder.READY, DeliveryOrder.INPUT])
@pytest.mark.parametrize("kind", list(_PUBLISHING))
@pytest.mark.asyncio
async def test_a_binding_stops_counting_as_a_producer_before_it_publishes_its_terminal_event(
    monkeypatch: pytest.MonkeyPatch, kind: str, order: DeliveryOrder, *, fail: bool
) -> None:
    """A binding at its terminal adds no command: no wave waits for it while its terminal event is published.

    Each terminal publication is held for longer than a wave waits for an event, as a publication
    waiting for room in its queue would be: a wave that still counted the publisher would fire its timer.
    """
    clock = VirtualClock().install(monkeypatch)
    states: list[_ProducerState] = []
    counted_at_publish: list[bool] = []
    start, put = ReferenceScheduler._start_binding, asyncio.Queue.put  # noqa: SLF001 - the seams observed

    def capturing(self: ReferenceScheduler, work: _Work, admission: _AdmissionState) -> None:
        states.append(self.producer_state)
        start(self, work, admission)

    async def stalled(queue: asyncio.Queue[object], item: object) -> None:
        if isinstance(item, _DoneEvent | _FailureEvent):
            counted_at_publish.append(f"r{item.work.index}" in states[-1].indexes)
            for _turn in range(_STALLED_PUBLISH_TURNS):
                await asyncio.sleep(0)
        await put(queue, item)

    monkeypatch.setattr(ReferenceScheduler, "_start_binding", capturing)
    monkeypatch.setattr(asyncio.Queue, "put", stalled)
    traversal, counted = _PUBLISHING[kind]
    answer = _keyset_answer(fail=fail) if counted is None else _rows_answer(counted=counted, fail=fail)
    method = "kappa.list" if counted is None else "gamma.list"
    sources = [
        ReferenceSource(
            Request.bare(method, {}),
            cast("TraversalSpec", traversal),
            Binding("parent", (ParameterUpdate(ParameterPath(("PARENT",)), parent),), parent),
        )
        for parent in (0, 1, 2)
    ]
    async with client_for(BatchPortal(answer)) as api:
        # Fed on the event loop: a pull in a worker thread could outlast a virtual wait and fire its timer.
        stream = api.iter_reference_source_outcomes(
            on_loop(sources), dispatch=BatchDispatch(batch_size=10, output_order=order)
        )
        async with stream:
            _events = [event async for event in stream]

    assert stream.report is not None
    assert stream.report.state is (TerminalState.COMPLETED_WITH_FAILURES if fail else TerminalState.COMPLETED)
    assert counted_at_publish == [False] * 3
    assert clock.timer_firings == 0


@pytest.mark.asyncio
async def test_a_tail_cleanup_failure_never_replaces_the_failure_that_ended_its_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The binding fails with its delivery error; the tail's cleanup failure is raised only after it."""
    deliver = ReferenceScheduler._deliver_page  # noqa: SLF001 - the delivery seam the failure enters
    delivered = 0
    errors: list[BaseException] = []

    async def failing(self: ReferenceScheduler, run: _ReferenceRun, page: _Page) -> bool:
        nonlocal delivered
        delivered += 1
        if delivered == 2:  # noqa: PLR2004 - the first tail page, after the head
            raise RuntimeError("delivery failed")
        return await deliver(self, run, page)

    async def failing_cleanup(_tail: ReferenceTailStream) -> None:
        raise ValueError("tail cleanup failed")

    failure_event = ReferenceScheduler._failure_event  # noqa: SLF001 - observed when it is built

    def observing(self: ReferenceScheduler, run: _ReferenceRun, error: Exception) -> _FailureEvent:
        errors.append(error)
        return failure_event(self, run, error)

    monkeypatch.setattr(ReferenceScheduler, "_deliver_page", failing)
    monkeypatch.setattr(ReferenceTailStream, "aclose", failing_cleanup)
    monkeypatch.setattr(ReferenceScheduler, "_failure_event", observing)
    binding = Binding("parent", (ParameterUpdate(ParameterPath(("PARENT",)), 1),), 1)
    source = ReferenceSource(Request.bare("gamma.list", {}), CountedTraversal(), binding)
    async with client_for(BatchPortal(_counted_answer)) as api:
        stream = api.iter_reference_source_outcomes([source], dispatch=BatchDispatch(batch_size=10))
        with pytest.raises(ValueError, match="tail cleanup failed"):
            async with stream:
                _events = [event async for event in stream]
    # The binding failed with its own error; the cleanup failure still ends the operation afterwards.
    assert [(type(error), str(error)) for error in errors] == [(RuntimeError, "delivery failed")]


@pytest.mark.asyncio
async def test_closing_a_tail_inside_a_cancelled_task_does_not_arm_another_cancellation() -> None:
    """A stream closed while its task is cancelled already carries that cancellation."""
    task = asyncio.current_task()
    assert task is not None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.sleep(0)
    assert task.cancelling() == 1
    _settle_tail_cleanup(CleanupResult(None, None), GeneratorExit())
    await asyncio.sleep(0)  # no cancellation was re-armed
    with pytest.raises(ValueError, match="cleanup"):
        _settle_tail_cleanup(CleanupResult(None, ValueError("cleanup")), GeneratorExit())
    await asyncio.sleep(0)  # nor when the cleanup itself failed
    task.uncancel()


def _grant(pool: int, free: int) -> int | None:
    """Ask a READY scheduler for a lease of the second of two partitioned bindings (C=10, A=4).

    An ordinary binding is admitted beside them and the source has ended, so
    R_keep = R = 0 under READY, F = 2, P = 34, Q = 44 and G_cap = max(P, pool // 2).
    """
    fast = SimpleNamespace(automatic=False, floor=34, quota=44, effective_page_cap=10)
    buffer = SimpleNamespace(maximum=pool, head_index=0, head_reserve=0, releasable_elsewhere=lambda _index: True)
    scheduler = SimpleNamespace(
        buffer=buffer,
        output_order=ReferenceOutputOrder.READY,
        _admission=SimpleNamespace(tasks={0: None, 1: None, 2: None}),
        _source_ended=True,
        _fast_active={0, 1},
    )
    scheduler._share = functools.partial(ReferenceScheduler._share, cast("ReferenceScheduler", scheduler))  # noqa: SLF001
    grant = ReferenceScheduler._lease_size  # noqa: SLF001
    return grant(cast("ReferenceScheduler", scheduler), cast("FastKeysetProgram", fast), 1, free)


@pytest.mark.parametrize(
    ("pool", "free", "granted"),
    [
        pytest.param(80, 34, 34, id="share-below-quota-starts-from-the-floor"),
        pytest.param(80, 33, None, id="below-the-floor-waits"),
        pytest.param(80, 60, 40, id="share-below-quota-is-capped"),
        pytest.param(100, 40, None, id="share-holding-the-quota-waits-for-it"),
        pytest.param(100, 44, 44, id="share-holding-the-quota-takes-it"),
        pytest.param(100, 70, 50, id="share-holding-the-quota-is-capped"),
    ],
)
def test_a_fast_lease_is_the_free_rows_clamped_between_the_floor_and_the_share(
    pool: int,
    free: int,
    granted: int | None,
) -> None:
    """S3 allocation: G = clamp(free - R_keep, P, G_cap); G < Q only while G_cap < Q."""
    assert _grant(pool, free) == granted


# Leases of the fast bindings, the one that plans, the bindings active, the delivery order, and its lease once grown.
# A pool of 100 keeps 10 for the INPUT frontier; every source has been read.
_LEASE_GROWTH = {
    # The second binding's share is (100 - 10) // 2 = 45: it may take 15 more, but only 10 are free beside the reserve.
    "behind_the_frontier": ((50, 30), 1, 2, ReferenceOutputOrder.INPUT, 30 + (100 - 50 - 30 - 10)),
    # The frontier keeps no reserve for itself: its share is 100 // 2.
    "the_frontier": ((30, 20), 0, 2, ReferenceOutputOrder.INPUT, 50),
    # A lone binding of a source read out keeps nothing free: the whole pool, in either order.
    "lone_input": ((30,), 0, 1, ReferenceOutputOrder.INPUT, 100),
    "lone_ready": ((30,), 0, 1, ReferenceOutputOrder.READY, 100),
}


@pytest.mark.parametrize("case", list(_LEASE_GROWTH))
@pytest.mark.asyncio
async def test_a_lease_grows_as_its_binding_plans_to_the_share_the_facts_then_known_give(case: str) -> None:
    """A lease grows to its share of the free rows, never into the reserve of a frontier it is not."""
    leases, index, active, order, expected = _LEASE_GROWTH[case]
    buffer = _RowBuffer(
        100, cast("ExecutionContext", None), head_reserve=10, ordered=order is ReferenceOutputOrder.INPUT
    )
    held = [
        await buffer.lease(binding, functools.partial(lambda amount, _free: amount, amount))
        for binding, amount in enumerate(leases)
    ]
    scheduler = SimpleNamespace(
        buffer=buffer,
        output_order=order,
        _admission=SimpleNamespace(tasks=dict.fromkeys(range(active))),
        _source_ended=True,
        _fast_active=set(range(len(leases))),
    )
    scheduler._share = functools.partial(ReferenceScheduler._share, cast("ReferenceScheduler", scheduler))  # noqa: SLF001
    fast = SimpleNamespace(automatic=False, floor=20, quota=30, effective_page_cap=10)
    run = SimpleNamespace(work=SimpleNamespace(index=index))

    grown = ReferenceScheduler.grow_lease(
        cast("ReferenceScheduler", scheduler), cast("_ReferenceRun", run), cast("FastKeysetProgram", fast), held[index]
    )

    assert grown == held[index].amount == expected


@pytest.mark.parametrize("ordered", [True, False], ids=["ordered", "unordered"])
@pytest.mark.asyncio
async def test_an_ordered_pool_grows_no_lease_while_an_earlier_binding_waits_for_one(*, ordered: bool) -> None:
    """Rows an ordered pool holds back for an earlier lease waiter are not a later lease's to grow into."""
    buffer = _RowBuffer(100, cast("ExecutionContext", None), ordered=ordered)
    lease = await buffer.lease(1, lambda _free: 30)
    waiting = asyncio.create_task(buffer.lease(0, lambda _free: None))
    for _turn in range(3):
        await asyncio.sleep(0)

    buffer.grow_lease(lease, lambda free: free)

    assert lease.amount == (30 if ordered else 100)
    waiting.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await waiting


async def _pool_context() -> ExecutionContext:
    context = Executor(BatchPortal(lambda _method, _query: {"result": []})).context(ExecutionPolicy())
    await context.start()
    return context


async def _turns(count: int = 5) -> None:
    for _turn in range(count):
        await asyncio.sleep(0)


# A pool of 100 rows; a page reserves its cap and is answered with more.
POOL, CAP, ANSWER, PAGE = 100, 40, 60, 10


@pytest.mark.asyncio
async def test_a_page_waiting_to_grow_takes_the_rows_that_come_back_before_any_page_reserves() -> None:
    """While a page waits to grow, no page reserves rows: the rows another page's landing returns are its first.

    Two pages hold 80 rows; the page answered with 30 more waits, and a page that would fit the 20 free waits too.
    """
    buffer = _RowBuffer(POOL, await _pool_context())
    longer = await buffer.reserve(0, CAP)
    landing = await buffer.reserve(1, CAP)
    growing = asyncio.create_task(buffer.grow(longer, CAP + 30))
    await _turns()
    reserving = asyncio.create_task(buffer.reserve(2, PAGE))
    await _turns()

    assert not growing.done()
    assert not reserving.done()
    await buffer.accept(landing, 0)
    await _turns()

    assert growing.result() is None
    assert longer.amount == CAP + 30
    assert reserving.result().amount == PAGE


@pytest.mark.asyncio
async def test_an_ordered_pool_grows_only_the_frontier_and_serves_it_while_a_page_behind_waits() -> None:
    """A page behind the INPUT frontier waits for the frontier to reach it: the frontier still reserves, no one else.

    Its rows reach the consumer only after the frontier's, so it takes none the frontier could need meanwhile, however
    many are free.
    """
    buffer = _RowBuffer(POOL, await _pool_context(), ordered=True)
    behind = await buffer.reserve(1, CAP)
    growing = asyncio.create_task(buffer.grow(behind, ANSWER))
    await _turns()
    frontier = asyncio.create_task(buffer.reserve(0, PAGE))
    reserving = asyncio.create_task(buffer.reserve(2, PAGE))
    await _turns()

    assert frontier.result().amount == PAGE
    assert not growing.done()
    assert not reserving.done()
    await buffer.advance_head(1, 0)
    await _turns()

    assert growing.result() is None
    assert behind.amount == ANSWER
    assert reserving.result().amount == PAGE


@pytest.mark.asyncio
async def test_a_frontier_page_waiting_to_grow_holds_back_the_frontier_s_next_page_too() -> None:
    """The frontier's own next page takes no row the frontier's page waiting to grow needs."""
    buffer = _RowBuffer(POOL, await _pool_context(), ordered=True)
    longer = await buffer.reserve(0, CAP)
    landing = await buffer.reserve(1, CAP)
    growing = asyncio.create_task(buffer.grow(longer, ANSWER + PAGE))
    await _turns()
    reserving = asyncio.create_task(buffer.reserve(0, PAGE))
    await _turns()

    assert not reserving.done()  # 20 rows are free, but the frontier's page lacks 30
    await buffer.accept(landing, 0)
    await _turns()

    assert growing.result() is None
    assert reserving.result().amount == PAGE


@pytest.mark.parametrize("ordered", [True, False], ids=["input", "ready"])
@pytest.mark.asyncio
async def test_a_page_waiting_to_grow_waits_for_its_own_binding_s_earlier_rows(*, ordered: bool) -> None:
    """The consumer takes a binding's earlier pages before its later one, whatever the order of delivery."""
    buffer = _RowBuffer(POOL, await _pool_context(), ordered=ordered)
    earlier = await buffer.reserve(0, POOL - CAP - PAGE)
    await buffer.accept(earlier, earlier.amount)
    longer = await buffer.reserve(0, CAP)
    growing = asyncio.create_task(buffer.grow(longer, ANSWER))
    await _turns()

    assert not growing.done()
    await buffer.release(earlier, earlier.amount)
    await _turns()
    assert growing.result() is None


@pytest.mark.parametrize("ordered", [True, False], ids=["input", "ready"])
@pytest.mark.asyncio
async def test_a_page_waits_to_grow_only_for_rows_the_consumer_takes_without_it(*, ordered: bool) -> None:
    """Rows read ahead behind the INPUT frontier come back only after it; under READY the consumer takes them anyway.

    The frontier's page lacks 20 rows and only 10 are free. Under INPUT the rest is held by a page of the binding
    behind it, waiting for the consumer, so the frontier's result never fits and is dropped at once with the most it
    could hold. Under READY the consumer takes those rows whatever the frontier does, so it waits for them.
    """
    buffer = _RowBuffer(POOL, await _pool_context(), ordered=ordered)
    frontier = await buffer.reserve(0, CAP)
    read_ahead = await buffer.reserve(1, POOL - CAP - PAGE)
    await buffer.accept(read_ahead, read_ahead.amount)
    growing = asyncio.create_task(buffer.grow(frontier, ANSWER))
    await _turns()

    if ordered:
        assert growing.result() == CAP + PAGE
        assert frontier.amount == CAP
        return
    assert not growing.done()
    await buffer.release(read_ahead, read_ahead.amount)
    await _turns()
    assert growing.result() is None
    assert frontier.amount == ANSWER


@pytest.mark.parametrize("ordered", [True, False], ids=["input", "ready"])
@pytest.mark.asyncio
async def test_a_planned_lease_returns_rows_to_a_page_waiting_to_grow_only_under_ready(*, ordered: bool) -> None:
    """A planned lease returns its rows when its binding ends, which under INPUT comes only after the frontier."""
    buffer = _RowBuffer(POOL, await _pool_context(), ordered=ordered)
    frontier = await buffer.reserve(0, CAP)
    lease = await buffer.lease(1, lambda _free: POOL - CAP - PAGE)
    await buffer.settle_lease(lease, lease.amount)
    growing = asyncio.create_task(buffer.grow(frontier, ANSWER))
    await _turns()

    assert growing.done() is ordered
    if ordered:
        assert growing.result() == CAP + PAGE
        return
    await buffer.abort(lease)
    await _turns()
    assert growing.result() is None


@pytest.mark.asyncio
async def test_a_page_waiting_to_grow_counts_no_row_of_its_own_binding_in_flight() -> None:
    """The binding's other pages in flight land after this page is admitted: they return nothing it can wait for."""
    buffer = _RowBuffer(POOL, await _pool_context())
    longer = await buffer.reserve(0, CAP)
    await buffer.reserve(0, POOL - CAP - PAGE)
    growing = asyncio.create_task(buffer.grow(longer, ANSWER))
    await _turns()

    assert growing.result() == CAP + PAGE


@pytest.mark.parametrize("ordered", [True, False], ids=["input", "ready"])
@pytest.mark.asyncio
async def test_a_page_starting_to_grow_takes_the_rows_just_promised_to_a_waiting_page(*, ordered: bool) -> None:
    """Rows the pool has promised to a woken page are free for a page that starts to grow before it resumes.

    The woken page is held back as soon as the growth starts: resuming, it finds it does not fit and waits again.
    """
    half = POOL // 2
    buffer = _RowBuffer(POOL, await _pool_context(), ordered=ordered)
    longer = await buffer.reserve(0, half)
    landing = await buffer.reserve(1, half)
    waiting = asyncio.create_task(buffer.reserve(2, half))
    await _turns()
    await buffer.accept(landing, 0)  # its 50 rows are promised to binding 2's page, whose task has not resumed

    # The growth starts before the woken page's task resumes, as when another binding's task lands in between.
    grown = asyncio.Task(buffer.grow(longer, POOL - PAGE), loop=asyncio.get_running_loop(), eager_start=True)
    await _turns()
    assert grown.result() is None
    assert longer.amount == POOL - PAGE
    assert not waiting.done()
    await buffer.abort(longer)
    await _turns()
    assert waiting.result().amount == half


@pytest.mark.asyncio
async def test_a_page_waits_to_grow_only_while_every_row_it_lacks_may_come_back() -> None:
    """A page whose missing rows are held by a page waiting to grow is dropped at once; the other keeps waiting.

    Two results of 80 rows each reserved 40; 10 rows are free and a third page holds 10 in flight. The first waits for
    the second's landing; once the second starts to grow, it is dropped at once with the 50 rows it could hold, and the
    first then waits for its reservation, given back as it ends.
    """
    buffer = _RowBuffer(POOL, await _pool_context())
    first = await buffer.reserve(0, CAP)
    second = await buffer.reserve(1, CAP)
    slow = await buffer.reserve(2, PAGE)
    growing = asyncio.create_task(buffer.grow(first, 2 * CAP))
    await _turns()

    dropped = asyncio.create_task(buffer.grow(second, 2 * CAP))
    await _turns()
    assert dropped.result() == CAP + PAGE
    assert not growing.done()
    await buffer.abort(second)
    await _turns()
    assert growing.result() is None
    assert first.amount == 2 * CAP
    await buffer.abort(slow)


@pytest.mark.parametrize("ordered", [True, False], ids=["input", "ready"])
@pytest.mark.asyncio
async def test_a_result_as_long_as_the_pool_waits_for_exactly_the_rows_it_lacks(*, ordered: bool) -> None:
    """Only a result longer than the pool never fits; one that needs every row that may come back waits for them.

    Two pages hold the whole pool; the first is answered with the pool's 100 rows and lacks exactly the 50 the second
    returns as it lands empty.
    """
    half = POOL // 2
    buffer = _RowBuffer(POOL, await _pool_context(), ordered=ordered)
    whole = await buffer.reserve(0, half)
    landing = await buffer.reserve(1, half)
    growing = asyncio.create_task(buffer.grow(whole, POOL))
    await _turns()

    assert not growing.done()
    await buffer.accept(landing, 0)
    await _turns()
    assert growing.result() is None
    assert whole.amount == POOL


@pytest.mark.asyncio
async def test_a_page_behind_the_input_frontier_waits_to_grow_only_while_every_row_it_lacks_may_come_back() -> None:
    """Behind the frontier, the rows the bindings before a page hold come back to it, those behind it do not.

    The frontier holds 10 rows in flight; two results of 80 rows behind it reserved 40 each, and 10 rows are free. The
    first waits while the second is in flight; once the second waits to grow as well, its rows come back only after
    the first, which is dropped at once. The second counts the first's rows, which come back before it, and grows once
    the frontier reaches it.
    """
    buffer = _RowBuffer(POOL, await _pool_context(), ordered=True)
    frontier = await buffer.reserve(0, PAGE)
    first = await buffer.reserve(1, CAP)
    second = await buffer.reserve(2, CAP)
    first_growing = asyncio.create_task(buffer.grow(first, 2 * CAP))
    await _turns()
    assert not first_growing.done()

    second_growing = asyncio.create_task(buffer.grow(second, 2 * CAP))
    await _turns()

    assert first_growing.result() == CAP + PAGE
    assert not second_growing.done()
    await buffer.abort(first)
    await buffer.abort(frontier)
    await buffer.advance_head(2, 0)
    await _turns()
    assert second_growing.result() is None
    assert second.amount == 2 * CAP


@pytest.mark.asyncio
async def test_the_input_frontier_waiting_to_grow_is_dropped_once_the_page_it_waits_for_starts_to_grow() -> None:
    """The frontier waits for a page behind it to land; once that page starts to grow, its rows never come back.

    The page behind waits for the frontier to move on, so the frontier must decide again at once, not wait for it.
    """
    buffer = _RowBuffer(POOL, await _pool_context(), ordered=True)
    frontier = await buffer.reserve(0, CAP)
    behind = await buffer.reserve(1, CAP)
    growing = asyncio.create_task(buffer.grow(frontier, POOL - PAGE))
    await _turns()
    assert not growing.done()

    behind_growing = asyncio.create_task(buffer.grow(behind, ANSWER))
    await _turns()

    assert growing.result() == POOL - CAP
    await buffer.abort(frontier)
    await buffer.advance_head(1, 0)
    await _turns()
    assert behind_growing.result() is None


@pytest.mark.asyncio
async def test_a_page_waiting_to_grow_waits_for_an_unplanned_lease_and_no_lease_is_granted_meanwhile() -> None:
    """An unplanned lease returns rows as its binding plans; meanwhile no lease takes the rows that come back."""
    half = POOL // 2
    buffer = _RowBuffer(POOL, await _pool_context())
    lease = await buffer.lease(1, lambda _free: half)
    page = await buffer.reserve(0, half)
    growing = asyncio.create_task(buffer.grow(page, POOL - PAGE))
    await _turns()
    leasing = asyncio.create_task(buffer.lease(2, lambda free: free or None))
    await _turns()

    assert not growing.done()
    assert not leasing.done()
    await buffer.settle_lease(lease, PAGE)
    await _turns()
    assert growing.result() is None
    assert page.amount == POOL - PAGE
    leasing.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await leasing


@pytest.mark.asyncio
@pytest.mark.parametrize("ordered", [False, True])
async def test_the_pool_projects_no_page_fitting_while_a_page_waits_to_grow(*, ordered: bool) -> None:
    """The dispatcher's projection follows the pool: while a page waits to grow, no other page fits.

    A page that needs no rows of the pool, one charged to a lease or holding its rows already, still comes.
    """
    buffer = _RowBuffer(POOL, await _pool_context(), ordered=ordered)
    longer = await buffer.reserve(0, CAP)
    await buffer.reserve(1, CAP)
    assert buffer.fits_any([(2, PAGE)])
    growing = asyncio.create_task(buffer.grow(longer, ANSWER + PAGE))
    await _turns()

    assert not growing.done()
    assert not buffer.can_reserve(2, PAGE)
    assert not buffer.fits_any([(2, PAGE)])
    assert buffer.can_reserve(2, 0)
    assert buffer.fits_any([(2, 0)])
    growing.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await growing


@pytest.mark.asyncio
async def test_a_change_of_the_rooms_wakes_a_page_on_the_next_release_however_few_rows_it_frees() -> None:
    """A pass records the free rows the next one needs, at a version of the rooms: once they change, any release serves.

    A page of binding 3 needs its 10 rows, the frontier's 10 and the rooms of producers 1 and 2: 40, with 36 free. Once
    producer 2 retires it needs 30, and the next release, of a single row, wakes it.
    """
    state = _ProducerState({})
    buffer = _RowBuffer(POOL, await _pool_context(), head_reserve=PAGE, producer_state=state, ordered=True)
    held = await buffer.reserve(5, POOL - 35)
    await buffer.accept(held, held.amount)
    for key, index in (("r1", 1), ("r2", 2), ("r3", 3)):
        state.register(key, index, PAGE)
    page = asyncio.create_task(buffer.reserve(3, PAGE))
    await _turns()
    await buffer.release(held, 1)
    await _turns()
    assert not page.done()

    state.retire("r2")
    await _turns()
    await buffer.release(held, 1)
    await _turns()

    assert page.result().amount == PAGE


@pytest.mark.asyncio
async def test_rows_a_producer_takes_without_a_pass_lower_what_a_waiting_page_needs_on_the_next_release() -> None:
    """A page reserved at once, with no pass, resets the free rows the next pass needs.

    A page of binding 3 needs its 10 rows, the frontier's 10 and the rooms of producers 1 and 2: 40, with 36 free. Once
    producer 1 reserves its page at once, it needs 30 with 26 free, and the release of 4 rows wakes it.
    """
    state = _ProducerState({})
    buffer = _RowBuffer(POOL, await _pool_context(), head_reserve=PAGE, producer_state=state, ordered=True)
    held = await buffer.reserve(5, POOL - 35)
    await buffer.accept(held, held.amount)
    for key, index in (("r1", 1), ("r2", 2), ("r3", 3)):
        state.register(key, index, PAGE)
    page = asyncio.create_task(buffer.reserve(3, PAGE))
    await _turns()
    await buffer.release(held, 1)
    await _turns()
    assert not page.done()

    nearer = asyncio.create_task(buffer.reserve(1, PAGE))
    await _turns()
    assert nearer.result().amount == PAGE
    await buffer.release(held, 4)
    await _turns()

    assert page.result().amount == PAGE


@pytest.mark.asyncio
async def test_rows_promised_to_the_frontier_count_as_its_own_for_the_pages_behind_it_in_the_same_pass() -> None:
    """A pass that wakes the frontier's page lets a page behind it leave free only the reserve the frontier lacks.

    Binding 3 holds 90 rows in flight; the frontier's reserve then grows to 20, and the frontier and binding 2 each
    wait, for 20 and 10 rows. Binding 3's page lands 30 rows short: the frontier is promised 20 of the 40 free, which
    cover its whole reserve, so binding 2 is woken for the other 20 in the same pass.
    """
    state = _ProducerState({})
    buffer = _RowBuffer(POOL, await _pool_context(), head_reserve=PAGE, producer_state=state, ordered=True)
    behind = await buffer.reserve(3, 60)
    await buffer.reserve(3, 30)
    buffer.raise_head_reserve(2 * PAGE)
    state.register("r2", 2, PAGE)
    frontier = asyncio.create_task(buffer.reserve(0, 2 * PAGE))
    second = asyncio.create_task(buffer.reserve(2, PAGE))
    await _turns()
    assert not frontier.done()
    assert not second.done()

    await buffer.accept(behind, 30)
    await _turns()

    assert frontier.result().amount == 2 * PAGE
    assert second.result().amount == PAGE


@pytest.mark.asyncio
async def test_a_lease_growing_as_its_binding_plans_leaves_the_rows_promised_to_a_woken_page() -> None:
    """Rows promised to a woken page are its own until it resumes: a lease growing meanwhile is offered none of them."""
    leased, page = 2, 3
    buffer = _RowBuffer(10, await _pool_context())
    held = await buffer.reserve(0, 8)
    await buffer.accept(held, 8)
    lease = await buffer.lease(2, lambda _free: leased)
    woken = asyncio.create_task(buffer.reserve(1, page))
    await _turns()
    await buffer.release(held, page)
    offered: list[int] = []

    def take_every_free_row(free: int) -> int:
        offered.append(free)
        return free

    buffer.grow_lease(lease, take_every_free_row)
    await _turns()

    assert offered == [0]
    assert lease.amount == leased
    assert woken.result().amount == page


@pytest.mark.asyncio
async def test_a_wave_leaves_without_a_page_whose_fetch_was_cancelled_while_it_coalesced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A page abandoned in a coalescing chunk is never sent: its wave carries only the live page and frees its slot."""
    VirtualClock(turns=_COALESCING_TURNS).install(monkeypatch)
    portal = BatchPortal(lambda _method, _query: {"result": []})
    executor = Executor(portal)
    context = executor.context(ExecutionPolicy(max_pages=10, max_pages_per_reference=10))
    await context.start()
    state = _ProducerState({"a": 0, "b": 1, "c": 2}, dict.fromkeys(("a", "b", "c"), _Producing.RUNNABLE))
    buffer = _RowBuffer(10, context, producer_state=state)
    dispatcher = _BatchPageDispatcher(
        executor,
        context,
        KernelBatchDispatch(batch_size=3, concurrency=1, coalesce_wait=UNREACHABLE_COALESCE_WAIT),
        producer_state=state,
        buffer=buffer,
    )
    kept = asyncio.create_task(dispatcher.fetch(Request("item.list", {"PARENT": "a"}, route=RouteKind.BARE), "a"))
    abandoned = asyncio.create_task(dispatcher.fetch(Request("item.list", {"PARENT": "b"}, route=RouteKind.BARE), "b"))
    for _turn in range(_COALESCING_TURNS // 2):  # both pages join the chunk, which waits for the third producer
        await asyncio.sleep(0)
    abandoned.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await abandoned
    # A stranded wave would hang the kept page; the bound only turns that into a failure.
    page = await asyncio.wait_for(kept, timeout=HANG_BOUND)
    assert page.admission is not None
    assert page.settlement is not None
    page.admission.set_result(None)
    page.settlement.set_result(None)
    await dispatcher.aclose()
    await buffer.close()

    # Only the kept page left: the abandoned one was never sent.
    assert portal.waves == [1]


@pytest.mark.asyncio
async def test_a_page_whose_wave_was_sent_when_the_deadline_passed_settles_unknown() -> None:
    """The deadline bounds admission: the executor ends a sent wave, and a still-queued page was never sent."""
    blocker = Blocker()
    executor = Executor(ResponderTransport(blocker))
    context = executor.context(ExecutionPolicy(max_pages=10, max_pages_per_reference=10, max_elapsed=DEADLINE_BUDGET))
    await context.start()
    state = _ProducerState({"a": 0, "b": 1}, dict.fromkeys(("a", "b"), _Producing.RUNNABLE))
    buffer = _RowBuffer(10, context, producer_state=state)
    dispatcher = _BatchPageDispatcher(
        executor,
        context,
        KernelBatchDispatch(batch_size=1, concurrency=1, coalesce_wait=0.0),
        producer_state=state,
        buffer=buffer,
    )
    sent = asyncio.create_task(dispatcher.fetch(Request("item.list", {"PARENT": "a"}, route=RouteKind.BARE), "a"))
    await reached(blocker.started)
    queued = asyncio.create_task(dispatcher.fetch(Request("item.list", {"PARENT": "b"}, route=RouteKind.BARE), "b"))
    results = await asyncio.gather(sent, queued, return_exceptions=True)
    await dispatcher.aclose()
    await buffer.close()

    # The sent page fails with the executor's own evidence; only the queued one is stopped at admission.
    assert not isinstance(results[0], BudgetExceededError)
    assert isinstance(results[1], BudgetExceededError)
    assert "admission" in str(results[1])
    assert [_failed_page(cast("BaseException", result)).settlement for result in results] == [
        CommandSettlement.UNKNOWN,
        CommandSettlement.NOT_EXECUTED,
    ]


def _three_producer_dispatcher(
    executor: Executor, context: ExecutionContext, *, batch_size: int = 1, coalesce_wait: float = 0.0
) -> tuple[_BatchPageDispatcher, _RowBuffer]:
    """A dispatcher of producers ``a``, ``b`` and ``c`` with one send slot."""
    state = _ProducerState({"a": 0, "b": 1, "c": 2}, dict.fromkeys(("a", "b", "c"), _Producing.RUNNABLE))
    buffer = _RowBuffer(10, context, producer_state=state)
    shape = KernelBatchDispatch(batch_size=batch_size, concurrency=1, coalesce_wait=coalesce_wait)
    return _BatchPageDispatcher(executor, context, shape, producer_state=state, buffer=buffer), buffer


def _page(parent: str) -> Request:
    return Request("item.list", {"PARENT": parent}, route=RouteKind.BARE)


@pytest.mark.asyncio
async def test_a_page_cancelled_after_its_wave_was_sent_settles_unknown() -> None:
    blocker = Blocker()
    executor = Executor(ResponderTransport(blocker))
    context = executor.context(ExecutionPolicy(max_pages=10, max_pages_per_reference=10))
    await context.start()
    dispatcher, buffer = _three_producer_dispatcher(executor, context)
    cancelled: list[asyncio.CancelledError] = []

    async def fetch() -> None:
        try:
            await dispatcher.fetch(_page("a"), "a")
        except asyncio.CancelledError as error:
            cancelled.append(error)
            raise

    task = asyncio.create_task(fetch())
    await asyncio.wait_for(blocker.started.wait(), timeout=HANG_BOUND)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    await dispatcher.aclose()
    await buffer.close()

    # The wave left with the page, so the page may have run.
    (error,) = cancelled
    assert _failed_page(error).settlement is CommandSettlement.UNKNOWN


@pytest.mark.asyncio
async def test_a_wave_left_with_no_page_is_not_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    VirtualClock(turns=_COALESCING_TURNS).install(monkeypatch)
    portal = BatchPortal(lambda _method, _query: {"result": []})
    executor = Executor(portal)
    context = executor.context(ExecutionPolicy(max_pages=10, max_pages_per_reference=10))
    await context.start()
    dispatcher, buffer = _three_producer_dispatcher(
        executor, context, batch_size=3, coalesce_wait=UNREACHABLE_COALESCE_WAIT
    )
    abandoned = asyncio.create_task(dispatcher.fetch(_page("b"), "b"))
    for _turn in range(_COALESCING_TURNS // 2):  # the page joins a chunk, which waits for the other producers
        await asyncio.sleep(0)
    abandoned.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await abandoned
    for _turn in range(_COALESCING_TURNS * 3):  # the chunk is handed to a send worker, with no live page left
        await asyncio.sleep(0)
    page = await asyncio.wait_for(dispatcher.fetch(_page("a"), "a"), timeout=HANG_BOUND)
    assert page.admission is not None
    assert page.settlement is not None
    page.admission.set_result(None)
    page.settlement.set_result(None)
    await dispatcher.aclose()
    await buffer.close()

    assert portal.waves == [1]


@pytest.mark.asyncio
async def test_a_send_that_breaks_an_invariant_fails_its_pages_as_possibly_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """A sent page no longer has an admission deadline: a failure of the send path must still end its wait."""

    async def no_outcomes(_engine: BatchExecutor, _requests: tuple[Request, ...], **_options: object) -> tuple[()]:
        return ()

    monkeypatch.setattr(BatchExecutor, "execute_requests", no_outcomes)
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy(max_pages=10, max_pages_per_reference=10))
    await context.start()
    dispatcher, buffer = _three_producer_dispatcher(executor, context)
    with pytest.raises(_ChunkPageError) as caught:
        await asyncio.wait_for(dispatcher.fetch(_page("a"), "a"), timeout=HANG_BOUND)
    await dispatcher.aclose()
    await buffer.close()

    # The page reports the send path's own error, and it may have run: its chunk never ended.
    failed = _failed_page(caught.value)
    assert isinstance(failed.error, ValueError)
    assert "zip" in str(failed.error)
    assert failed.settlement is CommandSettlement.UNKNOWN


class _EndlessReferences:
    """Commands of one SAFE method without end; the third is read only once ``refused`` is set.

    It records every pull and whether the source was closed.
    """

    def __init__(self, refused: asyncio.Event) -> None:
        self.refused = refused
        self.pulled = 0
        self.closed = False

    async def commands(self) -> AsyncGenerator[Command[int]]:
        try:
            while True:
                if self.pulled == 2:  # noqa: PLR2004 - the third pull waits for the budget's refusal
                    await reached(self.refused)
                self.pulled += 1
                yield Command(Request("test.get", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE), self.pulled)
        finally:
            self.closed = True

    async def bindings(self) -> AsyncGenerator[Binding[int]]:
        async for command in self.commands():
            yield Binding(str(command.correlation), (), command.correlation)


def _one_row_each(request: Request) -> object:
    """Answer a direct page, or each command of a classic batch, with one row: every binding yields an item."""
    commands = request.copy_parameters().get("cmd")
    if isinstance(commands, dict):
        return {"result": {"result": {key: [{"ID": 1}] for key in commands}, "result_error": []}}
    return {"result": [{"ID": 1}]}


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["fan_out", "iter_references"])
@pytest.mark.parametrize(
    "dispatch",
    [DirectDispatch(concurrency=2), BatchDispatch(batch_size=1, concurrency=2)],
    ids=["direct", "batch"],
)
async def test_a_fail_fast_operation_its_request_budget_closes_records_only_the_spent_budget(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    dispatch: DirectDispatch | BatchDispatch,
) -> None:
    """A source closed by the request budget ends as input does, not as a failure its cleanup records.

    The order is scripted: the budget allows one request and refuses the second binding's send; the source reads a
    third binding only after that refusal, so the producer then meets the spent budget; and the stream takes up the
    refused binding's failure only once the producer has ended. The window's fail-fast failure is therefore raised
    after the producer ended at the budget, never before: the order in which a producer that failed at the budget
    left its error to the cleanup, recorded as a blocking ``cleanup_failure``.
    """
    admit = ExecutionContext.admit_request
    produce = ReferenceScheduler._produce  # noqa: SLF001
    consume = ReferenceScheduler._consume_event  # noqa: SLF001
    wake = ReferenceScheduler._wake_lease_waiters  # noqa: SLF001
    refused = asyncio.Event()
    producer_ended = asyncio.Event()
    woken_ended: list[bool] = []

    def observed_admit(context: ExecutionContext) -> None:
        try:
            admit(context)
        except BudgetExceededError:
            refused.set()
            raise

    async def observed_produce(scheduler: ReferenceScheduler, *args: object) -> None:
        try:
            await produce(scheduler, *args)  # type: ignore[arg-type]
        finally:
            producer_ended.set()

    async def consume_after_the_producer(scheduler: ReferenceScheduler, event: _Event) -> AsyncGenerator[object]:
        if isinstance(event, _FailureEvent):
            await reached(producer_ended)
        async for item in consume(scheduler, event):
            yield item

    async def observed_wake(scheduler: ReferenceScheduler) -> None:
        woken_ended.append(scheduler._source_ended)  # noqa: SLF001
        await wake(scheduler)

    monkeypatch.setattr(ExecutionContext, "admit_request", observed_admit)
    monkeypatch.setattr(ReferenceScheduler, "_produce", observed_produce)
    monkeypatch.setattr(ReferenceScheduler, "_consume_event", consume_after_the_producer)
    monkeypatch.setattr(ReferenceScheduler, "_wake_lease_waiters", observed_wake)
    source = _EndlessReferences(refused)
    transport = ResponderTransport(_one_row_each)
    client = client_for(transport)
    policy = ExecutionPolicy(max_requests=1)
    stream = (
        client.fan_out(source.commands(), dispatch=dispatch, policy=policy)
        if operation == "fan_out"
        else client.iter_references(
            Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
            source.bindings(),
            traversal=SequentialTraversal(),
            dispatch=dispatch,
            policy=policy,
        )
    )

    with pytest.raises((BatchFailed, ReferenceFailed)):
        async for _item in stream:
            pass

    report = stream.report
    assert report is not None
    assert report.state is TerminalState.INCOMPLETE
    assert [violation.code for violation in report.violations] == ["budget_exhausted"]
    assert len(transport.requests) == 1
    assert source.pulled == 3  # noqa: PLR2004 - the read binding the budget refused, and the one read while it did
    assert source.closed
    # The budget's end of the source wakes lease waiters as a read-out source does.
    assert True in woken_ended


class _Commands:
    """Commands of one SAFE method without end, recording every pull."""

    def __init__(self) -> None:
        self.pulled = 0

    async def commands(self) -> AsyncGenerator[Command[int]]:
        while True:
            self.pulled += 1
            yield Command(Request("test.get", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE), self.pulled)


@pytest.mark.asyncio
async def test_a_producer_waiting_for_a_slot_meets_the_spent_budget_once_it_has_the_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The producer checks the request budget after it acquires a slot, not before.

    Two slots and a budget of one request: the producer reads two commands and waits for a slot before either
    is sent; the first is sent and the second's send is then refused. Under INPUT delivery the first command's
    success frees a slot before the stream takes up the refused one, which it does only once the producer has
    ended. Had the producer checked the budget before its wait, it would go on to read a binding with the spent
    budget, and fail on it: its error would be left to the cleanup, recorded as a blocking ``cleanup_failure``.
    """
    produce = ReferenceScheduler._produce  # noqa: SLF001
    consume = ReferenceScheduler._consume_event  # noqa: SLF001
    producer_ended = asyncio.Event()

    async def observed_produce(scheduler: ReferenceScheduler, *args: object) -> None:
        try:
            await produce(scheduler, *args)  # type: ignore[arg-type]
        finally:
            producer_ended.set()

    async def consume_after_the_producer(scheduler: ReferenceScheduler, event: _Event) -> AsyncGenerator[object]:
        if isinstance(event, _FailureEvent):
            await reached(producer_ended)
        async for item in consume(scheduler, event):
            yield item

    monkeypatch.setattr(ReferenceScheduler, "_produce", observed_produce)
    monkeypatch.setattr(ReferenceScheduler, "_consume_event", consume_after_the_producer)
    source = _Commands()
    transport = ResponderTransport(_one_row_each)
    stream = client_for(transport).fan_out(
        source.commands(),
        dispatch=DirectDispatch(concurrency=2, output_order=DeliveryOrder.INPUT),
        policy=ExecutionPolicy(max_requests=1, max_active_references=2),
    )

    with pytest.raises(BatchFailed):
        async for _item in stream:
            pass

    report = stream.report
    assert report is not None
    assert [violation.code for violation in report.violations] == ["budget_exhausted"]
    assert len(transport.requests) == 1
    assert source.pulled == 2  # noqa: PLR2004 - the sent command and the refused one


@pytest.mark.asyncio
async def test_an_input_page_read_ahead_is_acknowledged_only_once_its_consumer_has_received_it() -> None:
    """Reading ahead hands pages over long before the consumer reaches them; only a received page is acknowledged.

    A page is acknowledged after its last row reaches the consumer, never when its binding queues it, so an
    early close leaves every page read ahead but not received unacknowledged.
    """
    page, pages = 5, 3

    def answer(_method: str, query: Query) -> Envelope:
        parent, offset = int(query["PARENT"]), int(query.get("start", "0"))
        total = pages * page
        envelope: Envelope = {"result": [{"ID": parent * 100 + row} for row in range(offset, offset + page)]}
        if offset + page < total:
            envelope["next"] = offset + page
        return envelope

    bindings = on_loop(
        Binding("parent", (ParameterUpdate(ParameterPath(("PARENT",)), parent),), parent) for parent in range(3)
    )
    async with client_for(BatchPortal(answer, wire_turns=20), policy=ExecutionPolicy(max_buffered_rows=100)) as client:
        stream = client.iter_reference_outcomes(
            Request.bare("gamma.list", {}),
            bindings,
            traversal=SequentialTraversal(page_size=page),
            dispatch=BatchDispatch(output_order=DeliveryOrder.INPUT, coalesce_wait=UNREACHABLE_COALESCE_WAIT),
        )
        # The stream's completion gate is no public surface: observe the events it records.
        gate = cast("Any", stream)._source.completion_gate  # noqa: SLF001
        events: list[object] = []
        emit = gate.emit

        def observe(event: object) -> None:
            events.append(event)
            emit(event)

        gate.emit = observe
        async with stream:
            received = 0
            async for outcome in stream:
                received += isinstance(outcome, ReferenceItem)
                if received == page + 1:
                    break
        decision = gate.decision()

    delivered = {event.page_id for event in events if isinstance(event, PageDelivered)}
    acknowledged = [event.page_id for event in events if isinstance(event, PageAcknowledged)]
    assert decision.pages_scheduled > pages
    assert len(acknowledged) == decision.pages_acknowledged == 1
    assert set(acknowledged) <= delivered


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fetch", "settlement"),
    [
        ("never_spawned", CommandSettlement.NOT_EXECUTED),
        ("cancelled_before_it_ran", CommandSettlement.NOT_EXECUTED),
        ("answered", CommandSettlement.SUCCESS),
        ("failed_possibly_run", CommandSettlement.UNKNOWN),
    ],
)
async def test_an_abandoned_tail_page_settles_from_how_its_fetch_ended(
    fetch: str, settlement: CommandSettlement
) -> None:
    # The fetch task returns how it ended (``Settled``), so it never ends in its failure: an abandoned page is
    # settled from the failure its fetch recorded, a page the send may have run as unknown, never as a success.
    slot = _TailSlot(ordinal=0, page_id=1, sequence=0)

    async def dispatch() -> _DispatchedPage:
        if fetch == "failed_possibly_run":
            raise AmbiguousExecutionError(
                "lost", reason=AmbiguityReason.CONNECTION_LOST_AFTER_DISPATCH, declared_unsafe=False
            )
        return cast("_DispatchedPage", None)

    async def page() -> _DispatchedPage:
        # As ``ReferenceTailStream._fetch`` does: the slot records whatever the dispatch raised.
        try:
            return await dispatch()
        except BaseException as error:
            slot.error = error
            raise

    if fetch != "never_spawned":
        slot.task = spawn_settled(page())
        if fetch == "cancelled_before_it_ran":
            slot.task.cancel()
        await asyncio.gather(slot.task, return_exceptions=True)
    assert _abandoned_settlement(slot) is settlement


class _BlockedWire(ResponderTransport):
    """Hold every send on the wire until cancelled, a REST v3 batch envelope's included."""

    capabilities = TransportCapabilities(routes=frozenset({RouteKind.BARE, RouteKind.API_V3}), json_array_body=True)

    def __init__(self, blocker: Blocker) -> None:
        """Block classic sends with ``blocker`` and set its ``started`` for a v3 envelope too."""
        super().__init__(blocker)
        self.blocker = blocker

    async def send_wire(self, request: WireRequest, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Hold the envelope on the wire."""
        del request, attempt_timeout, max_response_bytes
        self.blocker.started.set()
        await asyncio.Future[None]()
        raise AssertionError("unreachable")


def _staged_wave(monkeypatch: pytest.MonkeyPatch, stage: str) -> tuple[BatchPortal | _BlockedWire, asyncio.Event]:
    """Hold a two-page wave at ``stage``; the event is set once it is held there."""
    if stage in {"on_wire", "v3_on_wire"}:
        blocker = Blocker()
        return _BlockedWire(blocker), blocker.started
    waiting = asyncio.Event()

    async def held(*_args: object, **_kwargs: object) -> None:
        waiting.set()
        await asyncio.Future[None]()

    if stage == "before_send":
        monkeypatch.setattr(RateCoordinator, "acquire", held)
    else:
        monkeypatch.setattr(Executor, "replay_after", held)
    refused: list[str] = []

    def answer(_method: str, query: Query) -> Envelope:
        if query["id"] == "2" and not refused:
            refused.append(query["id"])
            return {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "slow"}
        return {"result": [{"ID": int(query["id"])}]}

    return BatchPortal(answer), waiting


_POSSIBLY_RUN = (Execution.POSSIBLY_RUN, CommandSettlement.UNKNOWN)
_CANCELLED_PAGES = {
    # (stage, how the pages end): what each of the two pages' failure records, and how its page settles
    ("before_send", "pages"): [_POSSIBLY_RUN, _POSSIBLY_RUN],
    ("before_send", "close"): [(Execution.NOT_SENT, CommandSettlement.NOT_EXECUTED)] * 2,
    ("on_wire", "pages"): [_POSSIBLY_RUN, _POSSIBLY_RUN],
    ("on_wire", "close"): [_POSSIBLY_RUN, _POSSIBLY_RUN],
    ("replay_wait", "pages"): [(Execution.ANSWERED, CommandSettlement.SUCCESS), _POSSIBLY_RUN],
    ("replay_wait", "close"): [
        (Execution.ANSWERED, CommandSettlement.SUCCESS),
        (Execution.ANSWERED, CommandSettlement.FAILURE),
    ],
    ("v3_on_wire", "pages"): [_POSSIBLY_RUN, _POSSIBLY_RUN],
    ("v3_on_wire", "close"): [_POSSIBLY_RUN, _POSSIBLY_RUN],
}


@pytest.mark.asyncio
@pytest.mark.parametrize(("stage", "how"), list(_CANCELLED_PAGES))
async def test_a_page_cancelled_at_any_stage_of_its_wave_keeps_what_its_command_did(
    monkeypatch: pytest.MonkeyPatch, stage: str, how: str
) -> None:
    """Each cancelled page records what its command did, and settles from it as the flat path does.

    Two pages share one wave. Either the pages are cancelled, and the wave runs on for its other commands, so a
    command not answered yet may still run; or the dispatcher closes, so the wave sends nothing more. A wave held
    at its permit sent nothing; one on the wire, a REST v3 envelope's included, may have run every command. The
    wave answers one page and refuses the other (QUERY_LIMIT_EXCEEDED), then waits to send the refused one
    again: the answered page succeeded, under one encoding with the flat path, and the refused one may run while
    the wave runs on, or is an answered failure once it sends nothing more.
    """
    transport, waiting = _staged_wave(monkeypatch, stage)
    executor = Executor(transport)
    context = executor.context(ExecutionPolicy(max_pages=100, max_pages_per_reference=100))
    await context.start()
    dispatcher = _BatchPageDispatcher(
        executor, context, KernelBatchDispatch(batch_size=2, concurrency=1, coalesce_wait=UNREACHABLE_COALESCE_WAIT)
    )
    route = RouteKind.API_V3 if stage == "v3_on_wire" else RouteKind.BARE
    failures: dict[int, BaseException] = {}

    async def fetch(item: int) -> None:
        request = Request("crm.item.get", parameters={"id": item}, replay_safety=ReplaySafety.SAFE, route=route)
        try:
            await dispatcher.fetch(request, f"r{item}")
        except BaseException as error:
            failures[item] = error
            raise

    pages = [asyncio.create_task(fetch(item)) for item in (1, 2)]
    await asyncio.wait_for(waiting.wait(), timeout=HANG_BOUND)
    if how == "pages":
        for page in pages:
            page.cancel()
        await asyncio.gather(*pages, return_exceptions=True)
    await dispatcher.aclose()
    await asyncio.gather(*pages, return_exceptions=True)

    assert all(isinstance(failures[item], asyncio.CancelledError) for item in (1, 2))
    assert [(execution(failures[item]), settlement(failures[item])) for item in (1, 2)] == _CANCELLED_PAGES[stage, how]
