"""White-box reference scheduling: the batch assembler's coalescing read and potential, fast lease sizing.

S3 shares one batch queue and one row pool among every reference binding. A page the assembler's
coalescing read took joins the round-robin backlog before the next chunk forms, a page on its way to
the queue counts as coalescing potential until it arrives, and a fast keyset lease is
``clamp(free - R_keep, P, G_cap)``.
"""

from __future__ import annotations
import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from b24api import (
    AutoKeysetExecution,
    BatchDispatch,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    KeysetTraversal,
    ParameterPath,
    Request,
    ResultSelector,
    StableIntegerKeysetContract,
    TerminalState,
)
from b24api.contracts import Binding, ParameterUpdate, ReferenceSource
from b24api.contracts.completion import CommandSettlement
from b24api.contracts.request import RouteKind
from b24api.errors import BudgetExceededError
from b24api.execution import Executor
from b24api.references.dispatch import (
    _BatchPageDispatcher,
    _failed_settlement,
    _PendingBatch,
    _ProducerState,
    _RowBuffer,
)
from b24api.references.scheduler import ReferenceScheduler
from b24api.traversal.plans import KernelBatchDispatch, ReferenceOutputOrder
from tests.scripting import BatchPortal, client_for

if TYPE_CHECKING:
    from b24api.references.dispatch import _DispatchedPage, _Transit
    from b24api.references.program import FastKeysetProgram
    from tests.scripting import Envelope, Query


def _dispatcher_page(key: str) -> _PendingBatch:
    loop = asyncio.get_running_loop()
    request = Request("item.list", {"PARENT": key}, route=RouteKind.BARE)
    return _PendingBatch(request, key, loop.create_future(), loop.create_future(), loop.create_future())


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
    state = _ProducerState({"a", "x", "c"}, {"a": 0, "x": 1, "c": 2})
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
        KernelBatchDispatch(batch_size=2, concurrency=1, coalesce_wait=1.0),
        producer_state=state,
        buffer=buffer,
        page_cap=1,
        wait=state_changes_first,
    )
    dispatcher._queue.put_nowait(first)  # noqa: SLF001
    dispatcher._ensure_workers()  # noqa: SLF001
    await asyncio.wait_for(asyncio.gather(first.future, leftovers[0].future), timeout=5)
    dispatcher._queue.put_nowait(late)  # noqa: SLF001
    for _ in range(5):
        await asyncio.sleep(0)
    return dispatcher, portal, [first, *leftovers], late


@pytest.mark.asyncio
async def test_a_page_the_coalescing_read_took_joins_the_next_round_robin_wave() -> None:
    """A full backlog at every wave start used to keep the read's page out of every wave."""
    dispatcher, portal, pages, late = await _parked_page_dispatcher()
    _settle(pages[0])
    _settle(pages[1])
    await asyncio.wait_for(late.future, timeout=5)
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
    state = _ProducerState(set(), {"r0": 0})
    buffer = _RowBuffer(10, context, producer_state=state)
    dispatcher = _BatchPageDispatcher(
        executor,
        context,
        KernelBatchDispatch(batch_size=2, coalesce_wait=1),
        producer_state=state,
        buffer=buffer,
        page_cap=1,
    )
    potential = dispatcher._potential  # noqa: SLF001 - the assembler's coalescing read
    assert potential(set()) == 0

    reserved = state.depart("r0", page_reserved=True)
    assert potential({"r0"}) == 1
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
    task = asyncio.create_task(never_started())
    cancelled.carried_by(task)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert not state.reserving
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


def _grant(pool: int, free: int) -> int | None:
    """Ask a READY scheduler for a lease of the second of two partitioned bindings (C=10, A=4).

    An ordinary binding is admitted beside them and the source iterator is exhausted, so
    R_keep = R = 0 under READY, F = 2, P = 34, Q = 44 and G_cap = max(P, pool // 2).
    """
    fast = SimpleNamespace(automatic=False, floor=34, quota=44, effective_page_cap=10)
    buffer = SimpleNamespace(maximum=pool, head_index=0, head_reserve=0, releasable_elsewhere=lambda _index: True)
    scheduler = SimpleNamespace(
        buffer=buffer,
        output_order=ReferenceOutputOrder.READY,
        _admission=SimpleNamespace(tasks={0: None, 1: None, 2: None}),
        _source_exhausted=True,
        _fast_active={0, 1},
    )
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


@pytest.mark.asyncio
async def test_a_page_whose_wave_was_sent_when_the_deadline_passed_settles_unknown() -> None:
    """A deadline after the send began proves nothing about execution; a still-queued page was never sent."""
    portal = BatchPortal(lambda _method, _query: {"result": []})
    executor = Executor(portal)
    context = executor.context(ExecutionPolicy(max_pages=10, max_pages_per_reference=10, max_elapsed=0.2))
    await context.start()
    state = _ProducerState({"a", "b"}, {"a": 0, "b": 1})
    buffer = _RowBuffer(10, context, producer_state=state)
    dispatcher = _BatchPageDispatcher(
        executor,
        context,
        KernelBatchDispatch(batch_size=1, concurrency=1, coalesce_wait=0.0),
        producer_state=state,
        buffer=buffer,
        page_cap=1,
    )
    sending = asyncio.Event()

    async def in_flight(*_args: object, **_kwargs: object) -> tuple[object, ...]:
        sending.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    dispatcher._executor.execute_requests = in_flight  # type: ignore[method-assign, assignment]  # noqa: SLF001
    sent = asyncio.create_task(dispatcher.fetch(Request("item.list", {"PARENT": "a"}, route=RouteKind.BARE), "a"))
    await asyncio.wait_for(sending.wait(), timeout=5)
    queued = asyncio.create_task(dispatcher.fetch(Request("item.list", {"PARENT": "b"}, route=RouteKind.BARE), "b"))
    results = await asyncio.gather(sent, queued, return_exceptions=True)
    await dispatcher.aclose()
    await buffer.close()

    assert all(isinstance(result, BudgetExceededError) for result in results)
    assert [_failed_settlement(cast("BaseException", result)) for result in results] == [
        CommandSettlement.UNKNOWN,
        CommandSettlement.NOT_EXECUTED,
    ]
