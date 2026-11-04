"""White-box reference scheduling: the batch assembler's coalescing read and fast lease sizing.

S3 shares one batch queue and one row pool among every reference binding. A page the assembler's
coalescing read took joins the round-robin backlog before the next chunk forms, and a fast keyset
lease is ``clamp(free - R_keep, P, G_cap)``.
"""

from __future__ import annotations
import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from b24api import ExecutionPolicy, Request
from b24api.contracts.request import RouteKind
from b24api.execution import Executor
from b24api.references.dispatch import _BatchPageDispatcher, _PendingBatch, _ProducerState, _RowBuffer
from b24api.references.scheduler import ReferenceScheduler
from b24api.traversal.plans import KernelBatchDispatch, ReferenceOutputOrder
from tests.scripting import BatchPortal

if TYPE_CHECKING:
    from b24api.references.program import FastKeysetProgram


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
    grant = ReferenceScheduler._lease_grant  # noqa: SLF001
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
