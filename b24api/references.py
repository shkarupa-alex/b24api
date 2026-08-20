"""Bounded fair scheduling for independent and paginated references."""

from __future__ import annotations
import asyncio
import contextlib
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Iterable, Iterator
from dataclasses import dataclass
from typing import Protocol, Self, cast, runtime_checkable

from b24api.batch import BatchExecutor
from b24api.error import BudgetExceededError, CapabilityError
from b24api.execution import (
    AsyncIteratorController,
    ExecutionContext,
    Executor,
    WorkClass,
    await_cancellation_resistant,
)
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
_SYNC_EXHAUSTED = object()
_BATCH_COALESCE_IDLE_TURNS = 4


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
    item_weights: tuple[int, ...]
    unique_mask: tuple[bool, ...]
    violations: tuple[Violation, ...]
    reservation: _Reservation
    acknowledged: asyncio.Future[None]


@dataclass(frozen=True, slots=True)
class _DoneEvent:
    work: _Work
    violations: tuple[Violation, ...]


@dataclass(frozen=True, slots=True)
class _FailureEvent:
    work: _Work
    error: BaseException
    cursor: JsonValue
    page_state: int
    partial_rows: int
    violations: tuple[Violation, ...]
    replay_disposition: ReplayDisposition = ReplayDisposition.NOT_ELIGIBLE


type _Event = _PageEvent | _DoneEvent | _FailureEvent


@dataclass(slots=True)
class _PendingBatch:
    request: Request
    reference_id: str
    future: asyncio.Future[Response]


@dataclass(slots=True)
class _AdmissionState:
    tasks: dict[int, asyncio.Task[None]]
    queues: dict[int, asyncio.Queue[_Event]]
    ready: asyncio.Queue[_Event]
    slots: asyncio.Semaphore
    changed: asyncio.Event


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
        reservation = await self.context.reserve_page(reference=reference_id)
        remaining = self.context.policy.max_elapsed - self.context.elapsed
        if remaining <= 0:
            self.context.release_page(reservation)
            raise BudgetExceededError("operation elapsed budget exhausted before direct admission")
        try:
            async with asyncio.timeout(remaining):
                async with self._semaphore:
                    response = await self.executor.execute(
                        request,
                        context=self.context,
                        work_class=WorkClass.TRAVERSAL_DIRECT,
                    )
                    self.context.commit_page(reservation)
        except TimeoutError as error:
            self.context.release_page(reservation)
            raise BudgetExceededError("direct scheduler admission exceeded operation time budget") from error
        except BaseException:
            self.context.release_page(reservation)
            raise
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
        reservation = await self.context.reserve_page(reference=reference_id)
        future: asyncio.Future[Response] = asyncio.get_running_loop().create_future()
        pending = _PendingBatch(request, reference_id, future)
        remaining = self.context.policy.max_elapsed - self.context.elapsed
        if remaining <= 0:
            self.context.release_page(reservation)
            raise BudgetExceededError("operation elapsed budget exhausted before batch admission")
        try:
            async with asyncio.timeout(remaining):
                await self._queue.put(pending)
                response = await future
                self.context.commit_page(reservation)
                return response
        except asyncio.CancelledError:
            future.cancel()
            self.context.release_page(reservation)
            raise
        except TimeoutError as error:
            future.cancel()
            self.context.release_page(reservation)
            raise BudgetExceededError("batch scheduler admission exceeded operation time budget") from error
        except BaseException:
            self.context.release_page(reservation)
            raise

    async def _run(self) -> None:  # noqa: C901, PLR0912
        while not self._closed:
            first = await self._queue.get()
            if first is None:
                return
            if first.future.done():
                continue
            chunk = [first]
            idle_turns = 0
            stop_requested = False
            while len(chunk) < self.plan.batch_size and idle_turns < _BATCH_COALESCE_IDLE_TURNS:
                await asyncio.sleep(0)
                added = False
                while len(chunk) < self.plan.batch_size:
                    try:
                        pending = self._queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if pending is None:
                        stop_requested = True
                        break
                    if pending.future.done():
                        continue
                    chunk.append(pending)
                    added = True
                if stop_requested:
                    break
                idle_turns = 0 if added else idle_turns + 1
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
                item.future.set_result(success.response)
            if stop_requested:
                return

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._worker is None:
            return
        if not self._worker.done():
            self._worker.cancel()
        try:
            await asyncio.sleep(0)
            if not self._worker.done():
                remaining = max(0.0, self.context.policy.max_elapsed - self.context.elapsed)
                done, _ = await asyncio.wait((self._worker,), timeout=remaining)
                if not done:
                    raise BudgetExceededError("batch dispatcher cleanup exceeded operation time budget")
            with contextlib.suppress(asyncio.CancelledError):
                await self._worker
        finally:
            while True:
                try:
                    pending = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if pending is not None and not pending.future.done():
                    pending.future.cancel()


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
        page_cap_hint: int | None = None,
    ) -> None:
        """Initialize instance state."""
        self.executor = executor
        self.plan = plan
        self.dispatch = dispatch
        self.selector = selector
        self.identity = identity
        self.output_order = output_order
        self.tolerant = tolerant
        self.whole_result = whole_result
        self.context = executor.context(policy)
        self.page_cap = _page_cap(
            plan,
            dispatch,
            policy,
            whole_result=whole_result,
            page_cap_hint=page_cap_hint,
        )
        self.active_limit = _active_limit(output_order, policy, self.page_cap)
        self.dispatcher: _PageDispatcher
        if isinstance(dispatch, DirectDispatch):
            self.dispatcher = _DirectPageDispatcher(executor, self.context, dispatch)
        else:
            self.dispatcher = _BatchPageDispatcher(executor, self.context, dispatch)
        head_reserve = self.page_cap if output_order is ReferenceOutputOrder.INPUT else 0
        self.buffer = _RowBuffer(policy.max_buffered_rows, self.context, head_reserve=head_reserve)
        self.violations: list[Violation] = []
        self._delivery_uniqueness: dict[int, tuple[ReferenceItem, bool]] = {}
        self._source_controller: AsyncIteratorController[ReferenceRequest] | None = None

    async def outcomes(self, source: ReferenceSource) -> AsyncGenerator[ReferenceStreamItem]:
        """Yield correlated operation outcomes."""
        PaginationDriver.validate_contract(
            self.plan,
            self.identity,
            self.context.policy,
        )
        await self.context.start()
        iterator = AsyncIteratorController(
            _iterate_references(source),
            input_error="reference input exceeded operation time budget",
            cleanup_error="reference source cleanup exceeded operation time budget",
        )
        self._source_controller = iterator
        admission = _AdmissionState(
            tasks={},
            queues={},
            ready=asyncio.Queue(maxsize=self.active_limit),
            slots=asyncio.Semaphore(self.active_limit),
            changed=asyncio.Event(),
        )
        producer = asyncio.create_task(self._produce(iterator, admission))
        primary_error: BaseException | None = None

        try:
            if self.output_order is ReferenceOutputOrder.READY:
                async for outcome in self._ready(admission, producer):
                    yield outcome
            else:
                async for outcome in self._input(admission, producer):
                    yield outcome
        except BaseException as error:
            primary_error = error
            raise
        finally:
            try:
                cleanup_cancellation = await await_cancellation_resistant(
                    self._cleanup(iterator, admission, producer),
                )
            except BaseException as cleanup_error:
                if primary_error is None or isinstance(primary_error, asyncio.CancelledError | GeneratorExit):
                    raise
                self._record_cleanup_failure(cleanup_error)
            else:
                if cleanup_cancellation is not None and primary_error is None:
                    raise cleanup_cancellation
                if cleanup_cancellation is not None and not isinstance(
                    primary_error,
                    asyncio.CancelledError | GeneratorExit,
                ):
                    self._record_cleanup_failure(cleanup_cancellation)

    async def _cleanup(
        self,
        iterator: AsyncIteratorController[ReferenceRequest],
        admission: _AdmissionState,
        producer: asyncio.Task[None],
    ) -> None:
        producer.cancel()
        for task in admission.tasks.values():
            task.cancel()
        cleanup_errors: list[BaseException] = []
        try:
            await self.buffer.close()
        except BaseException as error:  # noqa: BLE001 - cleanup continues across owned resources
            cleanup_errors.append(error)
        try:
            await self.dispatcher.aclose()
        except BaseException as error:  # noqa: BLE001 - cleanup continues across owned resources
            cleanup_errors.append(error)
        pending, task_errors = await _wait_for_cleanup_tasks(
            (producer, *admission.tasks.values()),
            remaining=max(0.0, self.context.policy.max_elapsed - self.context.elapsed),
        )
        cleanup_errors.extend(task_errors)
        if pending:
            cleanup_errors.append(BudgetExceededError("reference task cleanup exceeded operation time budget"))
        else:
            try:
                await iterator.aclose(
                    remaining=max(0.0, self.context.policy.max_elapsed - self.context.elapsed),
                )
            except BaseException as error:  # noqa: BLE001 - attach at the stream boundary
                cleanup_errors.append(error)
        self._delivery_uniqueness.clear()
        if cleanup_errors:
            raise cleanup_errors[0]

    async def _produce(
        self,
        iterator: AsyncIteratorController[ReferenceRequest],
        admission: _AdmissionState,
    ) -> None:
        next_index = 0
        try:
            while True:
                await admission.slots.acquire()
                try:
                    reference = await iterator.get(self.context)
                except StopAsyncIteration:
                    admission.slots.release()
                    return
                except BaseException:
                    admission.slots.release()
                    raise
                if not isinstance(reference, ReferenceRequest):
                    admission.slots.release()
                    raise TypeError("reference source must yield ReferenceRequest values")
                work = _Work(next_index, reference)
                next_index += 1
                queue = admission.ready if self.output_order is ReferenceOutputOrder.READY else asyncio.Queue(maxsize=1)
                admission.queues[work.index] = queue
                admission.tasks[work.index] = asyncio.create_task(self._run_reference(work, queue))
                admission.changed.set()
        finally:
            admission.changed.set()

    async def _run_reference(self, work: _Work, output: asyncio.Queue[_Event]) -> None:
        reservation: _Reservation | None = None
        partial_rows = 0
        page_state = 0
        violation_offset = 0

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
            page_cap_hint=self.page_cap,
        )
        try:
            async for page in driver.pages():
                if reservation is None:
                    raise RuntimeError("page completed without a buffer reservation")  # noqa: TRY301
                await self.buffer.accept(reservation, page.retained_rows)
                acknowledged = asyncio.get_running_loop().create_future()
                page_violations = tuple(driver.violations[violation_offset:])
                violation_offset = len(driver.violations)
                await output.put(
                    _PageEvent(
                        work,
                        page.items,
                        page.item_weights,
                        driver.last_page_unique_mask,
                        page_violations,
                        reservation,
                        acknowledged,
                    ),
                )
                await acknowledged
                partial_rows += len(page.items)
                reservation = None
            if reservation is not None:
                await self.buffer.abort(reservation)
                reservation = None
            await output.put(_DoneEvent(work, tuple(driver.violations[violation_offset:])))
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
                    tuple(driver.violations[violation_offset:]),
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
                    tuple(driver.violations[violation_offset:]),
                ),
            )
        finally:
            if reservation is not None:
                await self.buffer.abort(reservation)

    async def _ready(
        self,
        admission: _AdmissionState,
        producer: asyncio.Task[None],
    ) -> AsyncGenerator[ReferenceStreamItem]:
        while admission.tasks or not producer.done():
            if not admission.tasks:
                await _wait_for_admission(producer, admission.changed)
                continue
            event = await _wait_for_event(admission.ready, producer)
            self._record_event_violations(event)
            async for outcome in self._consume_event(event):
                yield outcome
            if isinstance(event, _PageEvent):
                continue
            await _finish_task(admission.tasks, event.work.index)
            admission.queues.pop(event.work.index, None)
            admission.slots.release()
        await producer

    async def _input(
        self,
        admission: _AdmissionState,
        producer: asyncio.Task[None],
    ) -> AsyncGenerator[ReferenceStreamItem]:
        while admission.tasks or not producer.done():
            if not admission.tasks:
                await _wait_for_admission(producer, admission.changed)
                continue
            head = min(admission.tasks)
            event = await _wait_for_event(admission.queues[head], producer)
            self._record_event_violations(event)
            async for outcome in self._consume_event(event):
                yield outcome
            if isinstance(event, _PageEvent):
                continue
            await _finish_task(admission.tasks, head)
            admission.queues.pop(head, None)
            admission.slots.release()
            next_head = min(admission.tasks) if admission.tasks else head + 1
            await self.buffer.advance_head(next_head, self.page_cap)
        await producer

    def _record_event_violations(self, event: _Event) -> None:
        self.violations.extend(event.violations)

    def _record_cleanup_failure(self, error: BaseException) -> None:
        self.violations.append(
            Violation(
                severity=ViolationSeverity.BLOCKING,
                code="cleanup_failure",
                message=f"reference cleanup also failed ({type(error).__name__})",
            ),
        )

    async def _consume_event(self, event: _Event) -> AsyncGenerator[ReferenceStreamItem]:
        if isinstance(event, _PageEvent):
            try:
                for item, weight, is_unique in zip(
                    event.items,
                    event.item_weights,
                    event.unique_mask,
                    strict=True,
                ):
                    outcome = ReferenceItem(
                        event.work.reference.reference_key,
                        item,
                        event.work.reference.payload,
                    )
                    self._delivery_uniqueness[id(outcome)] = (outcome, is_unique)
                    yield outcome
                    await self.buffer.release(event.reservation, weight)
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
            payload=event.work.reference.payload,
        )

    def record_delivery(self, item: ReferenceItem) -> bool:
        """Record one delivered reference item."""
        stored = self._delivery_uniqueness.pop(id(item), None)
        return stored is not None and stored[0] is item and stored[1]

    async def observe_source_cleanup(self) -> None:
        """Observe completion of source cleanup."""
        controller = self._source_controller
        if controller is None:
            return
        await controller.aclose(
            remaining=max(0.0, self.context.policy.max_elapsed - self.context.elapsed),
        )


class ReferenceStream(AsyncIterator[ReferenceStreamItem]):
    """Lazy reference stream with one frozen report and deterministic cleanup."""

    def __init__(  # noqa: PLR0913
        self,
        scheduler: ReferenceScheduler,
        source: ReferenceSource,
        *,
        assurance: CompletionAssurance = CompletionAssurance.CALLER_ASSERTED,
        profile_id: str | None = None,
        profile_version: int | None = None,
        profile_source_sha256: str | None = None,
        profile_evidence_sha256: tuple[str, ...] = (),
        profile_evidence_candidate_sha: str | None = None,
    ) -> None:
        """Initialize instance state."""
        self._scheduler = scheduler
        self._source = source
        self._runner: AsyncGenerator[ReferenceStreamItem] | None = None
        self._prefetched: ReferenceStreamItem | object = _MISSING
        self._closed = False
        self._emitted = 0
        self._unique_emitted = 0
        self._assurance = assurance
        self._profile_id = profile_id
        self._profile_version = profile_version
        self._profile_source_sha256 = profile_source_sha256
        self._profile_evidence_sha256 = profile_evidence_sha256
        self._profile_evidence_candidate_sha = profile_evidence_candidate_sha
        self.report = OperationReport(
            assurance=assurance,
            profile_id=profile_id,
            profile_version=profile_version,
            profile_applicable=True if profile_id is not None else None,
            profile_source_sha256=profile_source_sha256,
            profile_evidence_sha256=profile_evidence_sha256,
            profile_evidence_candidate_sha=profile_evidence_candidate_sha,
        )

    def __aiter__(self) -> Self:
        """Return this asynchronous iterator."""
        return self

    async def __anext__(self) -> ReferenceStreamItem:
        """Return the next asynchronous item."""
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
            if self._scheduler.record_delivery(item):
                self._unique_emitted += 1

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        if self._closed:
            raise RuntimeError("stream is closed")
        if self._runner is None:
            self._runner = self._run()
            with contextlib.suppress(StopAsyncIteration):
                self._prefetched = await anext(self._runner)
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Exit the asynchronous context."""
        await self.aclose()

    async def aclose(self) -> None:
        """Close owned asynchronous resources."""
        if self._closed:
            await self._observe_source_cleanup()
            return
        self._closed = True
        try:
            if self._runner is not None:
                await self._runner.aclose()
        except BaseException as error:
            if self.report.state is TerminalState.NOT_STARTED:
                cancellation = await await_cancellation_resistant(
                    self._finalize(TerminalState.CANCELLED, "stream cleanup failed"),
                )
                if cancellation is not None:
                    _attach_report(cancellation, self.report)
                    raise cancellation from error
            _attach_report(error, self.report)
            raise
        finally:
            self._prefetched = _MISSING
        await self._observe_source_cleanup()
        if self.report.state is TerminalState.NOT_STARTED and self._runner is not None:
            await self._finalize(TerminalState.CANCELLED, "stream closed before exhaustion")

    async def _observe_source_cleanup(self) -> None:
        try:
            await self._scheduler.observe_source_cleanup()
        except BaseException as error:
            if self.report.state is TerminalState.NOT_STARTED:
                cancellation = await await_cancellation_resistant(
                    self._finalize(TerminalState.CANCELLED, "stream cleanup failed"),
                )
                if cancellation is not None:
                    _attach_report(cancellation, self.report)
                    raise cancellation from error
            _attach_report(error, self.report)
            raise

    async def _run(self) -> AsyncGenerator[ReferenceStreamItem]:  # noqa: C901, PLR0912
        outcomes = self._scheduler.outcomes(self._source)
        naturally_exhausted = False
        try:
            async for outcome in outcomes:
                yield outcome
            naturally_exhausted = True
            await self._finalize(TerminalState.COMPLETED, "reference input exhausted")
        except asyncio.CancelledError as error:
            repeated = await await_cancellation_resistant(
                self._finalize(TerminalState.CANCELLED, "iteration cancelled"),
            )
            if repeated is not None:
                _attach_report(repeated, self.report)
                raise repeated from error
            _attach_report(error, self.report)
            raise
        except GeneratorExit as error:
            cancellation = await await_cancellation_resistant(
                self._finalize(TerminalState.CANCELLED, "stream closed before exhaustion"),
            )
            if cancellation is not None:
                _attach_report(cancellation, self.report)
                raise cancellation from error
            _attach_report(error, self.report)
            raise
        except BaseException as error:
            cancellation = await await_cancellation_resistant(
                self._finalize(TerminalState.FAILED, type(error).__name__),
            )
            if cancellation is not None:
                _attach_report(cancellation, self.report)
                raise cancellation from error
            _attach_report(error, self.report)
            raise
        finally:
            try:
                cleanup_cancellation = await await_cancellation_resistant(outcomes.aclose())
            except BaseException as cleanup_error:
                if self.report.state is TerminalState.NOT_STARTED:
                    cancellation = await await_cancellation_resistant(
                        self._finalize(TerminalState.CANCELLED, "stream cleanup failed"),
                    )
                    if cancellation is not None:
                        _attach_report(cancellation, self.report)
                        raise cancellation from cleanup_error
                _attach_report(cleanup_error, self.report)
                raise
            if cleanup_cancellation is not None:
                _attach_report(cleanup_cancellation, self.report)
                raise cleanup_cancellation
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
        violations = tuple(self._scheduler.violations)
        if state is TerminalState.COMPLETED and snapshot_state is SnapshotState.UNVERIFIED:
            state = TerminalState.INCOMPLETE
            reason = "required snapshot was not verified"
            violations = (
                *violations,
                Violation(
                    severity=ViolationSeverity.BLOCKING,
                    code="snapshot_unverified",
                    message="the requested stable snapshot was not verified",
                ),
            )
        self.report = OperationReport(
            state=state,
            assurance=self._assurance,
            snapshot=snapshot_state,
            plan_id=type(self._scheduler.plan).__name__,
            dispatch_id=type(self._scheduler.dispatch).__name__,
            profile_id=self._profile_id,
            profile_version=self._profile_version,
            profile_applicable=True if self._profile_id is not None else None,
            profile_source_sha256=self._profile_source_sha256,
            profile_evidence_sha256=self._profile_evidence_sha256,
            profile_evidence_candidate_sha=self._profile_evidence_candidate_sha,
            emitted_rows=self._emitted,
            unique_rows=self._unique_emitted,
            physical_requests=snapshot.counters.physical_requests,
            logical_pages=snapshot.counters.logical_pages,
            batch_requests=self._scheduler.dispatcher.batch_requests,
            batch_commands=self._scheduler.dispatcher.batch_commands,
            retries=snapshot.retries,
            cooldown_seconds=snapshot.cooldown_seconds,
            buffered_rows_high_water=snapshot.counters.buffered_rows_high_water,
            violations=violations,
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
    _page_cap_hint: int | None = None,
    _assurance: CompletionAssurance = CompletionAssurance.CALLER_ASSERTED,
    _profile_id: str | None = None,
    _profile_version: int | None = None,
    _profile_source_sha256: str | None = None,
    _profile_evidence_sha256: tuple[str, ...] = (),
    _profile_evidence_candidate_sha: str | None = None,
) -> ReferenceStream:
    """Construct a lazy bounded reference traversal stream without I/O."""
    PaginationDriver.validate_plan(plan)
    if not isinstance(dispatch, BatchDispatch | DirectDispatch):
        raise TypeError("dispatch must be a canonical DispatchPlan")
    if not isinstance(output_order, ReferenceOutputOrder):
        raise TypeError("output_order must be ReferenceOutputOrder")
    if dispatch.output_order is not output_order:
        raise ValueError("dispatch and stream output order must agree")
    if _page_cap_hint is not None and (
        not isinstance(_page_cap_hint, int) or isinstance(_page_cap_hint, bool) or _page_cap_hint < 1
    ):
        raise ValueError("page cap hint must be a positive integer")
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
        page_cap_hint=_page_cap_hint,
    )
    return ReferenceStream(
        scheduler,
        requests,
        assurance=_assurance,
        profile_id=_profile_id,
        profile_version=_profile_version,
        profile_source_sha256=_profile_source_sha256,
        profile_evidence_sha256=_profile_evidence_sha256,
        profile_evidence_candidate_sha=_profile_evidence_candidate_sha,
    )


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
