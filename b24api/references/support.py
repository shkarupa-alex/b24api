"""Bounded fair scheduling for independent and paginated references."""

from __future__ import annotations
import asyncio
import contextlib
from collections.abc import AsyncGenerator, AsyncIterable, Iterator
from typing import TYPE_CHECKING, Protocol, cast, runtime_checkable

from b24api.plans import (
    CountedOffsetPlan,
    DirectDispatch,
    DispatchPlan,
    ItemCursorPlan,
    KeysetPlan,
    ListPlan,
    OffsetSequentialPlan,
    ReferenceOutputOrder,
)
from b24api.references.dispatch import (
    _SYNC_EXHAUSTED,
    ReferenceSource,
    _Event,
)

if TYPE_CHECKING:
    from b24api.contracts.policy import ExecutionPolicy
    from b24api.execution.snapshot import KernelReport
    from b24api.references.outcome import ReferenceRequest


@runtime_checkable
class _AsyncClosable(Protocol):
    async def aclose(self) -> None: ...


@runtime_checkable
class _SyncClosable(Protocol):
    def close(self) -> None: ...


async def _iterate_references(source: ReferenceSource) -> AsyncGenerator[ReferenceRequest]:
    if isinstance(source, AsyncIterable):
        async_iterator = aiter(source)
        try:
            async for item in async_iterator:
                yield item
        finally:
            if isinstance(async_iterator, _AsyncClosable):
                await async_iterator.aclose()
        return
    if source.__class__ is list or source.__class__ is tuple:
        for item in source:
            yield item
        return
    sync_iterator = iter(source)
    try:
        while True:
            sync_item = await _next_sync_owned(sync_iterator)
            if sync_item is _SYNC_EXHAUSTED:
                return
            yield cast("ReferenceRequest", sync_item)
    finally:
        if isinstance(sync_iterator, _SyncClosable):
            await _close_sync_owned(sync_iterator)


async def _wait_for_admission(producer: asyncio.Task[None], changed: asyncio.Event) -> None:
    if producer.done():
        await producer
        return
    waiter = asyncio.create_task(changed.wait())
    try:
        done, _ = await asyncio.wait((producer, waiter), return_when=asyncio.FIRST_COMPLETED)
        if waiter in done:
            changed.clear()
            return
        if producer in done:
            await producer
    finally:
        if not waiter.done():
            waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


def _next_sync(iterator: Iterator[ReferenceRequest]) -> ReferenceRequest | object:
    try:
        return next(iterator)
    except StopIteration:
        return _SYNC_EXHAUSTED


async def _next_sync_owned(iterator: Iterator[ReferenceRequest]) -> ReferenceRequest | object:
    pull = asyncio.create_task(asyncio.to_thread(_next_sync, iterator))
    try:
        return await asyncio.shield(pull)
    except asyncio.CancelledError:
        with contextlib.suppress(BaseException):
            await pull
        raise


async def _close_sync_owned(iterator: _SyncClosable) -> None:
    close = asyncio.create_task(asyncio.to_thread(iterator.close))
    try:
        await asyncio.shield(close)
    except asyncio.CancelledError:
        with contextlib.suppress(BaseException):
            await close
        raise


async def _wait_for_event(queue: asyncio.Queue[_Event], producer: asyncio.Task[None]) -> _Event:
    if producer.done():
        return await queue.get()
    getter = asyncio.create_task(queue.get())
    try:
        await asyncio.wait((producer, getter), return_when=asyncio.FIRST_COMPLETED)
        return await getter
    finally:
        if not getter.done():
            getter.cancel()
        await asyncio.gather(getter, return_exceptions=True)


async def _wait_for_cleanup_tasks(
    tasks: tuple[asyncio.Task[None], ...],
    *,
    remaining: float,
) -> tuple[set[asyncio.Task[None]], list[BaseException]]:
    active = set(tasks)
    if not active:
        return set(), []
    await asyncio.sleep(0)
    done, pending = await asyncio.wait(active, timeout=max(0.0, remaining))
    errors = [error for task in done if not task.cancelled() and (error := task.exception()) is not None]
    for task in pending:
        task.cancel()
        task.add_done_callback(_consume_task_result)
    return pending, errors


def _consume_task_result(task: asyncio.Task[object]) -> None:
    with contextlib.suppress(asyncio.CancelledError, Exception):
        task.result()


async def _finish_task(tasks: dict[int, asyncio.Task[None]], index: int) -> None:
    task = tasks.pop(index)
    await task


def _page_cap(
    plan: ListPlan,
    dispatch: DispatchPlan,
    policy: ExecutionPolicy,
    *,
    whole_result: bool,
    page_cap_hint: int | None,
) -> int:
    if whole_result:
        if isinstance(dispatch, DirectDispatch):
            concurrent_results = min(
                dispatch.concurrency,
                policy.max_direct_concurrency,
                policy.max_active_references,
            )
        else:
            concurrent_results = min(dispatch.batch_size, policy.max_active_references)
        return max(1, policy.max_buffered_rows // concurrent_results)
    if page_cap_hint is not None:
        return min(page_cap_hint, policy.max_buffered_rows)
    requested = (
        plan.requested_page_size
        if isinstance(plan, OffsetSequentialPlan | CountedOffsetPlan | KeysetPlan | ItemCursorPlan)
        else None
    )
    if requested is None:
        return policy.max_buffered_rows
    return min(requested, policy.max_buffered_rows)


def _active_limit(order: ReferenceOutputOrder, policy: ExecutionPolicy, page_cap: int) -> int:
    if order is ReferenceOutputOrder.READY:
        return policy.max_active_references
    buffer_bound = max(1, policy.max_buffered_rows // page_cap + 1)
    return min(policy.max_active_references, buffer_bound)


def _attach_report(error: BaseException, report: KernelReport) -> None:
    with contextlib.suppress(AttributeError, TypeError):
        error.report = report  # type: ignore[attr-defined]
