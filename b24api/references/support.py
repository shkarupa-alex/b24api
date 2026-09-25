"""Bounded fair scheduling for independent and paginated references."""

from __future__ import annotations
import asyncio
import contextlib
from typing import TYPE_CHECKING

from b24api.completion.closure import qualified_closure
from b24api.contracts.completion import BindingClosure
from b24api.contracts.report import PageRecord, Violation, ViolationSeverity
from b24api.traversal.plans import (
    CountedOffsetPlan,
    DispatchPlan,
    ItemCursorPlan,
    KernelDirectDispatch,
    KeysetPlan,
    ListPlan,
    OffsetSequentialPlan,
    ReferenceOutputOrder,
)

if TYPE_CHECKING:
    from b24api.completion.reference_recorder import ReferenceCompletionRecorder
    from b24api.contracts.policy import ExecutionPolicy
    from b24api.references.dispatch import _DoneEvent, _Event
    from b24api.traversal.driver import PaginationDriver


def _done_closure(event: _DoneEvent) -> BindingClosure:
    """Translate the validated source termination into its gate witness class."""
    if event.stopped_reason:
        return BindingClosure.CALLER_STOP
    return qualified_closure(event.terminal_reason) or BindingClosure.SOURCE_EMPTY


def _finish_done_completion(completion: ReferenceCompletionRecorder, event: _DoneEvent) -> None:
    """Retire an acknowledged reference with its qualified closure evidence."""
    completion.binding(event.work.index).complete_omitted_empty()
    closure = _done_closure(event)
    completion.terminal(
        event.work.index,
        closure,
        qualified_total=event.qualified_total if closure is BindingClosure.QUALIFIED_TOTAL else None,
    )


def _new_page_records(driver: PaginationDriver, previous_count: int) -> tuple[PageRecord, ...]:
    """Return at most one fresh record for a logical fetch."""
    if driver.page_trace_count == previous_count:
        return ()
    if driver.page_trace_count != previous_count + 1 or driver.last_page_record is None:
        raise RuntimeError("one logical fetch must produce at most one page record")
    return (driver.last_page_record,)


def _record_cleanup_failure(violations: list[Violation], error: BaseException) -> None:
    """Retain a bounded safe indication of reference cleanup failure."""
    violations.append(
        Violation(
            severity=ViolationSeverity.BLOCKING,
            code="cleanup_failure",
            message=f"reference cleanup also failed ({type(error).__name__})",
        )
    )


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
        if isinstance(dispatch, KernelDirectDispatch):
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
