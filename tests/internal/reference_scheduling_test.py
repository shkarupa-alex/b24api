"""White-box reference scheduling: the batch assembler's coalescing read and potential, fast lease sizing.

S3 shares one batch queue and one row pool among every reference binding. A page the assembler's
coalescing read took joins the round-robin backlog before the next chunk forms, a page on its way to
the queue counts as coalescing potential until it arrives, and a fast keyset lease is
``clamp(free - R_keep, P, G_cap)``.
"""

from __future__ import annotations
import asyncio
import contextlib
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

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
from b24api.contracts import Binding, DeliveryOrder, ParameterUpdate, ReferenceSource
from b24api.contracts.command import Command
from b24api.contracts.completion import CommandSettlement
from b24api.contracts.request import RouteKind
from b24api.errors import BudgetExceededError
from b24api.execution import Executor
from b24api.execution.context import CleanupResult, ExecutionContext
from b24api.references.counted_io import ReferenceTailStream
from b24api.references.dispatch import (
    _BatchPageDispatcher,
    _DoneEvent,
    _failed_settlement,
    _FailureEvent,
    _PendingBatch,
    _ProducerState,
    _RowBuffer,
)
from b24api.references.scheduler import ReferenceScheduler
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
        KernelBatchDispatch(batch_size=2, concurrency=1, coalesce_wait=UNREACHABLE_COALESCE_WAIT),
        producer_state=state,
        buffer=buffer,
        page_cap=1,
        wait=state_changes_first,
    )
    dispatcher._queue.put_nowait(first)  # noqa: SLF001
    dispatcher._ensure_workers()  # noqa: SLF001
    await asyncio.wait_for(asyncio.gather(first.future, leftovers[0].future), timeout=HANG_BOUND)
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
    state = _ProducerState(set(), {"r0": 0})
    buffer = _RowBuffer(10, context, producer_state=state)
    dispatcher = _BatchPageDispatcher(
        executor,
        context,
        KernelBatchDispatch(batch_size=2, coalesce_wait=UNREACHABLE_COALESCE_WAIT),
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


@pytest.mark.parametrize("ready", [True, False])
@pytest.mark.asyncio
async def test_a_pending_refill_is_potential_whatever_the_chunk_carries(*, ready: bool) -> None:
    """A windowed producer's refill counts while its own pages wait in the chunk.

    Under INPUT delivery only the frontier's consumer progresses on its own, so only its refill counts.
    """
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy(max_pages=100, max_pages_per_reference=100))
    await context.start()
    state = _ProducerState(set(), {"r0": 0, "r1": 1})
    buffer = _RowBuffer(10, context, producer_state=state, ordered=not ready)
    dispatcher = _BatchPageDispatcher(
        executor,
        context,
        KernelBatchDispatch(batch_size=2, coalesce_wait=UNREACHABLE_COALESCE_WAIT),
        producer_state=state,
        buffer=buffer,
        page_cap=1,
        pending_continuations_can_progress=ready,
    )
    potential = dispatcher._potential  # noqa: SLF001 - the assembler's coalescing read
    state.pending_refills["r1"] = None
    assert potential({"r1"}) == int(ready)
    state.pending_refills["r0"] = None
    assert potential({"r0", "r1"}) == 1 + int(ready)
    state.pending_continuations.add("r0")
    state.pending_refills.clear()
    assert potential({"r0"}) == 0
    await buffer.close()


@pytest.mark.asyncio
async def test_a_refill_counts_the_rows_its_consumer_returns_first() -> None:
    """A refill waiting on a held receipt reserves from the rows that receipt returns, not only free ones."""
    executor = Executor(BatchPortal(lambda _method, _query: {"result": []}))
    context = executor.context(ExecutionPolicy(max_pages=100, max_pages_per_reference=100))
    await context.start()
    state = _ProducerState(set(), {"r0": 0})
    buffer = _RowBuffer(2, context, producer_state=state)
    dispatcher = _BatchPageDispatcher(
        executor,
        context,
        KernelBatchDispatch(batch_size=2, coalesce_wait=UNREACHABLE_COALESCE_WAIT),
        producer_state=state,
        buffer=buffer,
        page_cap=2,
    )
    potential = dispatcher._potential  # noqa: SLF001 - the assembler's coalescing read
    held = await buffer.reserve(0, 2)
    state.pending_refills["r0"] = None
    assert potential({"r0"}) == 0
    state.pending_refills["r0"] = held
    assert potential({"r0"}) == 1
    page = await context.reserve_page(reference="r0")
    assert potential({"r0"}) == 1
    pages = [await context.reserve_page(reference="r0") for _ in range(99)]
    # The page budget is spent: the refill could not reserve its logical page.
    assert potential({"r0"}) == 0
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

    def observing(self: ReferenceScheduler, run: _ReferenceRun, error: BaseException) -> object:
        observed.append(
            (
                run.producer_key in self.producer_state.pending_refills,
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

    def observing(self: ReferenceScheduler, run: _ReferenceRun, error: BaseException) -> _FailureEvent:
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
async def test_a_wave_leaves_without_a_page_whose_fetch_was_cancelled_while_it_coalesced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A page abandoned in a coalescing chunk is never sent: its wave carries only the live page and frees its slot."""
    VirtualClock(turns=_COALESCING_TURNS).install(monkeypatch)
    portal = BatchPortal(lambda _method, _query: {"result": []})
    executor = Executor(portal)
    context = executor.context(ExecutionPolicy(max_pages=10, max_pages_per_reference=10))
    await context.start()
    state = _ProducerState({"a", "b", "c"}, {"a": 0, "b": 1, "c": 2})
    buffer = _RowBuffer(10, context, producer_state=state)
    dispatcher = _BatchPageDispatcher(
        executor,
        context,
        KernelBatchDispatch(batch_size=3, concurrency=1, coalesce_wait=UNREACHABLE_COALESCE_WAIT),
        producer_state=state,
        buffer=buffer,
        page_cap=1,
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
    assert [_failed_settlement(cast("BaseException", result)) for result in results] == [
        CommandSettlement.UNKNOWN,
        CommandSettlement.NOT_EXECUTED,
    ]


def _three_producer_dispatcher(
    executor: Executor, context: ExecutionContext, *, batch_size: int = 1, coalesce_wait: float = 0.0
) -> tuple[_BatchPageDispatcher, _RowBuffer]:
    """A dispatcher of producers ``a``, ``b`` and ``c`` with one send slot."""
    state = _ProducerState({"a", "b", "c"}, {"a": 0, "b": 1, "c": 2})
    buffer = _RowBuffer(10, context, producer_state=state)
    shape = KernelBatchDispatch(batch_size=batch_size, concurrency=1, coalesce_wait=coalesce_wait)
    return _BatchPageDispatcher(executor, context, shape, producer_state=state, buffer=buffer, page_cap=1), buffer


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
    assert _failed_settlement(error) is CommandSettlement.UNKNOWN


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
    with pytest.raises(ValueError, match="zip") as caught:
        await asyncio.wait_for(dispatcher.fetch(_page("a"), "a"), timeout=HANG_BOUND)
    await dispatcher.aclose()
    await buffer.close()

    # The page fails with the send path's own error, and it may have run.
    assert _failed_settlement(caught.value) is CommandSettlement.UNKNOWN


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
