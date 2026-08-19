"""Bounded fair scheduling for independent and paginated references."""

from __future__ import annotations
import asyncio
import contextlib
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Protocol, Self, cast, runtime_checkable

from b24api.batch import BatchExecutor
from b24api.error import BudgetExceededError, CapabilityError
from b24api.execution import ExecutionContext, Executor, WorkClass
from b24api.models import (
    BatchFailure,
    CompletionAssurance,
    ExecutionPolicy,
    IdentitySpec,
    JsonValue,
    OperationReport,
    ReferenceFailure,
    ReferenceItem,
    ReferenceRequest,
    ReplayDisposition,
    ReplaySafety,
    Request,
    Response,
    ResultSelector,
    SnapshotRequirement,
    SnapshotState,
    TerminalState,
    Violation,
    ViolationSeverity,
)
from b24api.pagination import PaginationDriver
from b24api.plans import (
    BatchDispatch,
    CountedOffsetPlan,
    DirectDispatch,
    DispatchPlan,
    ItemCursorPlan,
    KeysetPlan,
    ListPlan,
    OffsetSequentialPlan,
    ReferenceOutputOrder,
    SingleResponsePlan,
)

type ReferenceSource = Iterable[ReferenceRequest] | AsyncIterable[ReferenceRequest]
type ReferenceStreamItem = ReferenceItem | ReferenceFailure
_MISSING = object()


@runtime_checkable
class _AsyncClosable(Protocol):
    async def aclose(self) -> None: ...


@runtime_checkable
class _SyncClosable(Protocol):
    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class _Work:
    index: int
    reference: ReferenceRequest


@dataclass(slots=True)
class _Reservation:
    index: int
    amount: int
    accepted: bool = False


@dataclass(slots=True)
class _PageEvent:
    work: _Work
    items: tuple[JsonValue, ...]
    reservation: _Reservation
    acknowledged: asyncio.Future[None]


@dataclass(frozen=True, slots=True)
class _DoneEvent:
    work: _Work


@dataclass(frozen=True, slots=True)
class _FailureEvent:
    work: _Work
    error: BaseException
    cursor: JsonValue
    page_state: int
    partial_rows: int
    replay_disposition: ReplayDisposition = ReplayDisposition.NOT_ELIGIBLE


type _Event = _PageEvent | _DoneEvent | _FailureEvent


@dataclass(slots=True)
class _PendingBatch:
    request: Request
    reference_id: str
    future: asyncio.Future[Response]


class _BatchPageError(Exception):
    def __init__(self, failure: BatchFailure) -> None:
        self.failure = failure
        super().__init__("batch page command failed")


class _RowBuffer:
    """Reserve page capacity before I/O and account decoded retained rows exactly."""

    def __init__(self, maximum: int, context: ExecutionContext, *, head_reserve: int = 0) -> None:
        self.maximum = maximum
        self.context = context
        self._available = maximum
        self._accounted = 0
        self._head_index = 0
        self._head_reserve = head_reserve
        self._closed = False
        self._condition = asyncio.Condition()
        self._reservations: list[_Reservation] = []

    async def reserve(self, index: int, amount: int) -> _Reservation:
        if amount > self.maximum:
            raise BudgetExceededError("one decoded page exceeds the global row buffer")
        async with self._condition:
            await self._condition.wait_for(lambda: self._closed or self._can_reserve(index, amount))
            if self._closed:
                raise asyncio.CancelledError
            self._available -= amount
            reservation = _Reservation(index, amount)
            self._reservations.append(reservation)
            return reservation

    def _can_reserve(self, index: int, amount: int) -> bool:
        if self._available < amount:
            return False
        if self._head_reserve == 0 or index == self._head_index:
            return True
        head_has_capacity = any(
            reservation.index == self._head_index and reservation.amount > 0 for reservation in self._reservations
        )
        required_head_reserve = 0 if head_has_capacity else self._head_reserve
        return self._available - amount >= required_head_reserve

    async def accept(self, reservation: _Reservation, actual: int) -> None:
        if actual > reservation.amount:
            await self.abort(reservation)
            raise BudgetExceededError("decoded page exceeded its reserved row capacity")
        async with self._condition:
            if self._closed:
                raise asyncio.CancelledError
            unused = reservation.amount - actual
            reservation.amount = actual
            reservation.accepted = True
            self._available += unused
            self._accounted += actual
            if actual == 0:
                self._reservations.remove(reservation)
            self._condition.notify_all()
        if actual:
            await self.context.adjust_buffered_rows(actual)

    async def release(self, reservation: _Reservation, count: int) -> None:
        if count < 0 or count > reservation.amount:
            raise ValueError("invalid row-buffer release")
        if not count:
            return
        async with self._condition:
            if reservation.amount == 0:
                return
            reservation.amount -= count
            self._available += count
            self._accounted -= count
            if reservation.amount == 0:
                self._reservations.remove(reservation)
            self._condition.notify_all()
        await self.context.adjust_buffered_rows(-count)

    async def abort(self, reservation: _Reservation) -> None:
        amount = reservation.amount
        if not amount:
            return
        async with self._condition:
            amount = reservation.amount
            if not amount:
                return
            reservation.amount = 0
            self._available += amount
            if reservation.accepted:
                self._accounted -= amount
            self._reservations.remove(reservation)
            self._condition.notify_all()
        if reservation.accepted:
            await self.context.adjust_buffered_rows(-amount)

    async def advance_head(self, index: int, reserve: int) -> None:
        async with self._condition:
            self._head_index = index
            self._head_reserve = reserve
            self._condition.notify_all()

    async def close(self) -> None:
        async with self._condition:
            if self._closed:
                return
            self._closed = True
            self._accounted = 0
            self._available = self.maximum
            for reservation in self._reservations:
                reservation.amount = 0
            self._reservations.clear()
            self._condition.notify_all()
        await self.context.set_buffered_rows(0)


class _DirectPageDispatcher:
    def __init__(
        self,
        executor: Executor,
        context: ExecutionContext,
        plan: DirectDispatch,
    ) -> None:
        self.executor = executor
        self.context = context
        concurrency = min(plan.concurrency, context.policy.max_direct_concurrency)
        self._semaphore = asyncio.Semaphore(concurrency)
        self.batch_requests = 0
        self.batch_commands = 0

    async def fetch(self, request: Request, reference_id: str) -> Response:
        remaining = self.context.policy.max_elapsed - self.context.elapsed
        if remaining <= 0:
            raise BudgetExceededError("operation elapsed budget exhausted before direct admission")
        try:
            async with asyncio.timeout(remaining):
                async with self._semaphore:
                    response = await self.executor.execute(
                        request,
                        context=self.context,
                        work_class=WorkClass.TRAVERSAL_DIRECT,
                    )
        except TimeoutError as error:
            raise BudgetExceededError("direct scheduler admission exceeded operation time budget") from error
        await self.context.reserve_page(reference=reference_id)
        return response

    async def aclose(self) -> None:
        return


class _BatchPageDispatcher:
    def __init__(
        self,
        executor: Executor,
        context: ExecutionContext,
        plan: BatchDispatch,
    ) -> None:
        self.context = context
        self.plan = plan
        self._executor = BatchExecutor(executor)
        self._queue: asyncio.Queue[_PendingBatch | None] = asyncio.Queue(
            maxsize=context.policy.max_active_references,
        )
        self._worker: asyncio.Task[None] | None = None
        self._closed = False
        self.batch_requests = 0
        self.batch_commands = 0

    async def fetch(self, request: Request, reference_id: str) -> Response:
        if self._closed:
            raise RuntimeError("batch page dispatcher is closed")
        if self._worker is None:
            self._worker = asyncio.create_task(self._run())
        future: asyncio.Future[Response] = asyncio.get_running_loop().create_future()
        pending = _PendingBatch(request, reference_id, future)
        remaining = self.context.policy.max_elapsed - self.context.elapsed
        if remaining <= 0:
            raise BudgetExceededError("operation elapsed budget exhausted before batch admission")
        try:
            async with asyncio.timeout(remaining):
                await self._queue.put(pending)
                return await future
        except TimeoutError as error:
            future.cancel()
            raise BudgetExceededError("batch scheduler admission exceeded operation time budget") from error

    async def _run(self) -> None:  # noqa: C901, PLR0912
        while True:
            first = await self._queue.get()
            if first is None:
                return
            chunk = [first]
            await asyncio.sleep(0)
            while len(chunk) < self.plan.batch_size:
                try:
                    pending = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if pending is None:
                    await self._queue.put(None)
                    break
                chunk.append(pending)
            self.batch_requests += 1
            self.batch_commands += len(chunk)
            try:
                outcomes = await self._executor.execute_requests(
                    tuple(item.request for item in chunk),
                    context=self.context,
                    fallback_failed=self.plan.fallback_failed,
                )
            except asyncio.CancelledError:
                for item in chunk:
                    item.future.cancel()
                raise
            except Exception as error:  # noqa: BLE001 - total chunk correlation boundary
                for item in chunk:
                    if not item.future.done():
                        item.future.set_exception(error)
                continue
            for item, outcome in zip(chunk, outcomes, strict=True):
                if item.future.done():
                    continue
                if isinstance(outcome, BatchFailure):
                    item.future.set_exception(_BatchPageError(outcome))
                    continue
                success = outcome
                if success.response is None:
                    item.future.set_exception(CapabilityError("batch page response metadata is unavailable"))
                    continue
                try:
                    await self.context.reserve_page(reference=item.reference_id)
                except Exception as error:  # noqa: BLE001 - propagate any budget/driver failure
                    item.future.set_exception(error)
                else:
                    item.future.set_result(success.response)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._worker is None:
            return
        if self._worker.done():
            await self._worker
            return
        await self._queue.put(None)
        await self._worker


type _PageDispatcher = _DirectPageDispatcher | _BatchPageDispatcher


class ReferenceScheduler:
    """Admit bounded reference state and schedule one sequential page per reference."""

    def __init__(  # noqa: PLR0913
        self,
        executor: Executor,
        *,
        plan: ListPlan,
        dispatch: DispatchPlan,
        selector: ResultSelector | None,
        identity: IdentitySpec | None,
        output_order: ReferenceOutputOrder,
        tolerant: bool,
        policy: ExecutionPolicy,
        whole_result: bool = False,
    ) -> None:
        self.executor = executor
        self.plan = plan
        self.dispatch = dispatch
        self.selector = selector
        self.identity = identity
        self.output_order = output_order
        self.tolerant = tolerant
        self.whole_result = whole_result
        self.context = executor.context(policy)
        self.page_cap = _page_cap(plan, policy, whole_result=whole_result)
        self.active_limit = _active_limit(output_order, policy, self.page_cap)
        self.dispatcher: _PageDispatcher
        if isinstance(dispatch, DirectDispatch):
            self.dispatcher = _DirectPageDispatcher(executor, self.context, dispatch)
        else:
            self.dispatcher = _BatchPageDispatcher(executor, self.context, dispatch)
        head_reserve = self.page_cap if output_order is ReferenceOutputOrder.INPUT else 0
        self.buffer = _RowBuffer(policy.max_buffered_rows, self.context, head_reserve=head_reserve)
        self.violations: list[Violation] = []

    async def outcomes(self, source: ReferenceSource) -> AsyncGenerator[ReferenceStreamItem]:
        iterator = _iterate_references(source)
        tasks: dict[int, asyncio.Task[None]] = {}
        queues: dict[int, asyncio.Queue[_Event]] = {}
        ready: asyncio.Queue[_Event] = asyncio.Queue(maxsize=self.active_limit)
        next_index = 0
        input_exhausted = False

        async def admit() -> None:
            nonlocal next_index, input_exhausted
            while not input_exhausted and len(tasks) < self.active_limit:
                try:
                    reference = await anext(iterator)
                except StopAsyncIteration:
                    input_exhausted = True
                    return
                if not isinstance(reference, ReferenceRequest):
                    raise TypeError("reference source must yield ReferenceRequest values")
                work = _Work(next_index, reference)
                next_index += 1
                queue = ready if self.output_order is ReferenceOutputOrder.READY else asyncio.Queue(maxsize=1)
                queues[work.index] = queue
                tasks[work.index] = asyncio.create_task(self._run_reference(work, queue))

        try:
            await admit()
            if self.output_order is ReferenceOutputOrder.READY:
                async for outcome in self._ready(tasks, queues, ready, admit):
                    yield outcome
            else:
                async for outcome in self._input(tasks, queues, admit):
                    yield outcome
        finally:
            for task in tasks.values():
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks.values(), return_exceptions=True)
            await iterator.aclose()
            await self.buffer.close()
            await self.dispatcher.aclose()

    async def _run_reference(self, work: _Work, output: asyncio.Queue[_Event]) -> None:
        reservation: _Reservation | None = None
        partial_rows = 0
        page_state = 0

        async def fetch(request: Request) -> Response:
            nonlocal page_state, reservation
            reservation = await self.buffer.reserve(work.index, self.page_cap)
            try:
                response = await self.dispatcher.fetch(request, f"r{work.index}")
            except BaseException:
                await self.buffer.abort(reservation)
                reservation = None
                raise
            page_state += 1
            return response

        driver = PaginationDriver(
            self.executor,
            work.reference.request,
            self.plan,
            selector=self.selector,
            identity=self.identity,
            context=self.context,
            fetch=fetch,
            single_result_as_item=self.whole_result,
        )
        try:
            async for page in driver.pages():
                if reservation is None:
                    raise RuntimeError("page completed without a buffer reservation")  # noqa: TRY301
                await self.buffer.accept(reservation, len(page.items))
                acknowledged = asyncio.get_running_loop().create_future()
                await output.put(_PageEvent(work, page.items, reservation, acknowledged))
                await acknowledged
                partial_rows += len(page.items)
                reservation = None
            if reservation is not None:
                await self.buffer.abort(reservation)
                reservation = None
            await output.put(_DoneEvent(work))
        except asyncio.CancelledError:
            raise
        except _BatchPageError as error:
            await output.put(
                _FailureEvent(
                    work,
                    cast("BaseException", error.failure.error),
                    driver.cursor_state,
                    page_state,
                    partial_rows,
                    error.failure.replay_disposition,
                ),
            )
        except Exception as error:  # noqa: BLE001 - per-reference tolerant outcome boundary
            await output.put(
                _FailureEvent(
                    work,
                    error,
                    driver.cursor_state,
                    page_state,
                    partial_rows,
                ),
            )
        finally:
            if reservation is not None:
                await self.buffer.abort(reservation)

    async def _ready(
        self,
        tasks: dict[int, asyncio.Task[None]],
        queues: dict[int, asyncio.Queue[_Event]],
        ready: asyncio.Queue[_Event],
        admit: _Admit,
    ) -> AsyncGenerator[ReferenceStreamItem]:
        while tasks:
            event = await ready.get()
            async for outcome in self._consume_event(event):
                yield outcome
            if isinstance(event, _PageEvent):
                continue
            await _finish_task(tasks, event.work.index)
            queues.pop(event.work.index, None)
            await admit()

    async def _input(
        self,
        tasks: dict[int, asyncio.Task[None]],
        queues: dict[int, asyncio.Queue[_Event]],
        admit: _Admit,
    ) -> AsyncGenerator[ReferenceStreamItem]:
        while tasks:
            head = min(tasks)
            event = await queues[head].get()
            async for outcome in self._consume_event(event):
                yield outcome
            if isinstance(event, _PageEvent):
                continue
            await _finish_task(tasks, head)
            queues.pop(head, None)
            await admit()
            next_head = min(tasks) if tasks else head + 1
            await self.buffer.advance_head(next_head, self.page_cap)

    async def _consume_event(self, event: _Event) -> AsyncGenerator[ReferenceStreamItem]:
        if isinstance(event, _PageEvent):
            try:
                for item in event.items:
                    yield ReferenceItem(event.work.reference.reference_key, item)
                    await self.buffer.release(event.reservation, 1)
            finally:
                if not event.acknowledged.done():
                    event.acknowledged.set_result(None)
            return
        if isinstance(event, _DoneEvent):
            return
        if not self.tolerant:
            raise event.error
        self.violations.append(
            Violation(
                severity=ViolationSeverity.WARNING,
                code="reference_failure",
                message="one reference produced a typed failure outcome",
            ),
        )
        request = event.work.reference.request
        safety = request.replay_safety or ReplaySafety.UNKNOWN
        yield ReferenceFailure(
            event.work.reference.reference_key,
            request,
            event.error,
            cursor=event.cursor,
            page_state=event.page_state,
            partial_rows=event.partial_rows,
            replay_safety=safety,
            replay_disposition=event.replay_disposition,
        )


type _Admit = Callable[[], Awaitable[None]]


class ReferenceStream(AsyncIterator[ReferenceStreamItem]):
    """Lazy reference stream with one frozen report and deterministic cleanup."""

    def __init__(self, scheduler: ReferenceScheduler, source: ReferenceSource) -> None:
        self._scheduler = scheduler
        self._source = source
        self._runner: AsyncGenerator[ReferenceStreamItem] | None = None
        self._prefetched: ReferenceStreamItem | object = _MISSING
        self._closed = False
        self._emitted = 0
        self.report = OperationReport()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> ReferenceStreamItem:
        if self._closed:
            raise StopAsyncIteration
        if self._prefetched is not _MISSING:
            item = cast("ReferenceStreamItem", self._prefetched)
            self._prefetched = _MISSING
            self._record_delivery(item)
            return item
        if self._runner is None:
            self._runner = self._run()
        item = await anext(self._runner)
        self._record_delivery(item)
        return item

    def _record_delivery(self, item: ReferenceStreamItem) -> None:
        if isinstance(item, ReferenceItem):
            self._emitted += 1

    async def __aenter__(self) -> Self:
        if self._closed:
            raise RuntimeError("stream is closed")
        if self._runner is None:
            self._runner = self._run()
            with contextlib.suppress(StopAsyncIteration):
                self._prefetched = await anext(self._runner)
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._runner is not None:
            await self._runner.aclose()
        self._prefetched = _MISSING
        if self.report.state is TerminalState.NOT_STARTED and self._runner is not None:
            await self._finalize(TerminalState.CANCELLED, "stream closed before exhaustion")

    async def _run(self) -> AsyncGenerator[ReferenceStreamItem]:
        outcomes = self._scheduler.outcomes(self._source)
        naturally_exhausted = False
        try:
            async for outcome in outcomes:
                yield outcome
            naturally_exhausted = True
            await self._finalize(TerminalState.COMPLETED, "reference input exhausted")
        except asyncio.CancelledError as error:
            await self._finalize(TerminalState.CANCELLED, "iteration cancelled")
            _attach_report(error, self.report)
            raise
        except GeneratorExit as error:
            await self._finalize(TerminalState.CANCELLED, "stream closed before exhaustion")
            _attach_report(error, self.report)
            raise
        except BaseException as error:
            await self._finalize(TerminalState.FAILED, type(error).__name__)
            _attach_report(error, self.report)
            raise
        finally:
            await outcomes.aclose()
            if not naturally_exhausted and self.report.state is TerminalState.NOT_STARTED:
                await self._finalize(TerminalState.CANCELLED, "stream abandoned")
            if self.report.state is not TerminalState.NOT_STARTED:
                self._closed = True

    async def _finalize(self, state: TerminalState, reason: str) -> None:
        if self.report.state is not TerminalState.NOT_STARTED:
            return
        snapshot = await self._scheduler.context.snapshot()
        consistency = self._scheduler.context.policy.consistency
        snapshot_state = (
            SnapshotState.NOT_REQUESTED
            if consistency.snapshot_requirement is SnapshotRequirement.TRAVERSAL_ONLY
            else SnapshotState.UNVERIFIED
        )
        self.report = OperationReport(
            state=state,
            assurance=CompletionAssurance.CALLER_ASSERTED,
            snapshot=snapshot_state,
            plan_id=type(self._scheduler.plan).__name__,
            dispatch_id=type(self._scheduler.dispatch).__name__,
            emitted_rows=self._emitted,
            unique_rows=self._emitted,
            physical_requests=snapshot.counters.physical_requests,
            logical_pages=snapshot.counters.logical_pages,
            batch_requests=self._scheduler.dispatcher.batch_requests,
            batch_commands=self._scheduler.dispatcher.batch_commands,
            retries=snapshot.retries,
            cooldown_seconds=snapshot.cooldown_seconds,
            buffered_rows_high_water=snapshot.counters.buffered_rows_high_water,
            violations=tuple(self._scheduler.violations),
            terminal_reason=reason,
        )


def fan_out(  # noqa: PLR0913
    executor: Executor,
    requests: ReferenceSource,
    *,
    dispatch: DispatchPlan,
    output_order: ReferenceOutputOrder = ReferenceOutputOrder.READY,
    tolerant: bool = False,
    policy: ExecutionPolicy | None = None,
) -> ReferenceStream:
    """Schedule independent requests as single-response reference traversals."""
    return iter_references(
        executor,
        requests,
        plan=SingleResponsePlan(),
        dispatch=dispatch,
        output_order=output_order,
        tolerant=tolerant,
        policy=policy,
        _whole_result=True,
    )


def iter_references(  # noqa: PLR0913
    executor: Executor,
    requests: ReferenceSource,
    *,
    plan: ListPlan,
    dispatch: DispatchPlan,
    selector: ResultSelector | None = None,
    identity: IdentitySpec | None = None,
    output_order: ReferenceOutputOrder = ReferenceOutputOrder.READY,
    tolerant: bool = False,
    policy: ExecutionPolicy | None = None,
    _whole_result: bool = False,
) -> ReferenceStream:
    """Construct a lazy bounded reference traversal stream without I/O."""
    if not isinstance(dispatch, BatchDispatch | DirectDispatch):
        raise TypeError("dispatch must be a canonical DispatchPlan")
    if not isinstance(output_order, ReferenceOutputOrder):
        raise TypeError("output_order must be ReferenceOutputOrder")
    if dispatch.output_order is not output_order:
        raise ValueError("dispatch and stream output order must agree")
    scheduler = ReferenceScheduler(
        executor,
        plan=plan,
        dispatch=dispatch,
        selector=selector,
        identity=identity,
        output_order=output_order,
        tolerant=tolerant,
        policy=policy or ExecutionPolicy(),
        whole_result=_whole_result,
    )
    return ReferenceStream(scheduler, requests)


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
    sync_iterator = iter(source)
    try:
        for item in sync_iterator:
            yield item
    finally:
        if isinstance(sync_iterator, _SyncClosable):
            sync_iterator.close()


async def _finish_task(tasks: dict[int, asyncio.Task[None]], index: int) -> None:
    task = tasks.pop(index)
    await task


def _page_cap(plan: ListPlan, policy: ExecutionPolicy, *, whole_result: bool) -> int:
    if whole_result:
        return 1
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


def _attach_report(error: BaseException, report: OperationReport) -> None:
    with contextlib.suppress(AttributeError, TypeError):
        error.report = report  # type: ignore[attr-defined]


__all__ = [
    "ReferenceScheduler",
    "ReferenceSource",
    "ReferenceStream",
    "ReferenceStreamItem",
    "fan_out",
    "iter_references",
]
