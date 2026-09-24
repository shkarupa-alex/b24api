"""C8: the rate coordinator and the execution ledger mutate state without locks.

Both run on one event loop and every mutation is synchronous code between awaits, so no other task can
observe a half-applied update. The locks they used to take never guarded an ``await``, were therefore
never contended, and neither excluded anything nor added a cancellation point. These tests pin the
property the removal relies on: each mutator finishes in one step, and concurrent use keeps capacity.
"""

from __future__ import annotations
import asyncio
import random
import time
from typing import TYPE_CHECKING, Any

import pytest

from b24api.contracts.policy import ExecutionPolicy
from b24api.errors import BudgetExceededError
from b24api.execution import RateCoordinator, WorkClass
from b24api.execution.context import ExecutionContext

if TYPE_CHECKING:
    from collections.abc import Coroutine

_COOLDOWN = 1.5
_BUFFERED = 3


def _one_step[T](coroutine: Coroutine[Any, Any, T]) -> T:
    """Run a coroutine to completion without letting it yield to the event loop."""
    try:
        coroutine.send(None)
    except StopIteration as done:
        return done.value  # type: ignore[no-any-return]
    coroutine.close()
    pytest.fail("mutator suspended, so another task could interleave with its update")


@pytest.mark.asyncio
async def test_every_ledger_mutator_completes_without_suspending() -> None:
    context = ExecutionContext(ExecutionPolicy(max_pages=4), RateCoordinator())

    _one_step(context.start())
    _one_step(context.reserve_attempt(attempts_for_request=1, retry_started=time.monotonic()))
    _one_step(context.record_retry())
    _one_step(context.record_cooldown(_COOLDOWN))
    single = _one_step(context.reserve_page(reference="a"))
    wave = _one_step(context.reserve_pages(2))
    _one_step(context.set_buffered_rows(_BUFFERED))
    _one_step(context.adjust_buffered_rows(-1))
    context.commit_page(single)
    for reservation in wave:
        context.release_page(reservation)
    snapshot = _one_step(context.snapshot())

    assert snapshot.retries == 1
    assert snapshot.cooldown_seconds == _COOLDOWN
    assert snapshot.counters.logical_pages == 1
    assert snapshot.counters.buffered_rows == _BUFFERED - 1


@pytest.mark.asyncio
async def test_every_coordinator_mutator_completes_without_suspending() -> None:
    coordinator = RateCoordinator(max_concurrency=1)

    permit = _one_step(coordinator.acquire(WorkClass.BATCH, methods=frozenset({"batch"})))
    _one_step(coordinator.observe_operation_time_limit("crm.item.list", delay=60.0))
    _one_step(coordinator.observe_throttle(30.0, reason="retry-after"))
    busy = _one_step(coordinator.snapshot())
    permit.release_now()

    assert busy.active_permits == 1
    assert busy.method_cooldowns == 1
    await coordinator.close()
    assert _one_step(coordinator.snapshot()).active_permits == 0


@pytest.mark.asyncio
async def test_concurrent_admission_with_cancellations_never_exceeds_or_leaks_capacity() -> None:
    capacity = 3
    coordinator = RateCoordinator(max_concurrency=capacity)
    rng = random.Random(8)
    active = high_water = 0

    async def worker(index: int) -> None:
        nonlocal active, high_water
        permit = await coordinator.acquire(list(WorkClass)[index % len(WorkClass)], methods=frozenset({"m"}))
        active += 1
        high_water = max(high_water, active)
        try:
            for _ in range(rng.randrange(1, 4)):
                await asyncio.sleep(0)
        finally:
            active -= 1
            permit.release_now()

    tasks = [asyncio.create_task(worker(index)) for index in range(60)]
    for _ in range(5):
        await asyncio.sleep(0)
    for task in rng.sample(tasks, 20):
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    snapshot = await coordinator.snapshot()
    assert high_water == capacity
    assert snapshot.active_permits == 0
    assert all(count == 0 for _work_class, count in snapshot.queued)
    await coordinator.close()


@pytest.mark.asyncio
async def test_concurrent_page_reservations_respect_the_budget_and_wake_waiters() -> None:
    budget, callers = 5, 40
    context = ExecutionContext(ExecutionPolicy(max_pages=budget, max_elapsed=5.0), RateCoordinator())
    rng = random.Random(30)
    committed = rejected = 0

    async def page() -> None:
        nonlocal committed, rejected
        try:
            reservation = await context.reserve_page()
        except BudgetExceededError:
            rejected += 1
            return
        await asyncio.sleep(0)
        if rng.random() < 1 / 2:
            context.release_page(reservation)
        else:
            context.commit_page(reservation)
            committed += 1

    await asyncio.gather(*(page() for _ in range(callers)))

    snapshot = await context.snapshot()
    assert committed == snapshot.counters.logical_pages == budget
    assert committed + rejected <= callers
    assert context.can_reserve_page() is False
