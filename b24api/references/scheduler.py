"""Bounded fair scheduling for independent and paginated references."""

from __future__ import annotations
import asyncio
from dataclasses import replace
from typing import TYPE_CHECKING, cast

from b24api.contracts.page import IdentityPageAdapter, PageAdapter
from b24api.contracts.report import PageDispatch, PageRecord, Violation, ViolationSeverity, retain_page_trace
from b24api.contracts.request import ReplaySafety, Request, ResultSelector, TraversalIdentity
from b24api.errors import BudgetExceededError, CapabilityError
from b24api.execution import (
    AsyncIteratorController,
    Executor,
    await_cleanup_resistant,
    rearm_cancellation,
)
from b24api.references.dispatch import (
    ReferenceSource,
    ReferenceStreamItem,
    _AdmissionState,
    _BatchPageDispatcher,
    _BatchPageError,
    _DirectPageDispatcher,
    _DispatchedPage,
    _DoneEvent,
    _Event,
    _FailureEvent,
    _KernelFanOutSuccess,
    _KernelReferenceComplete,
    _PageEvent,
    _ProducerState,
    _ReferenceWindowError,
    _Reservation,
    _RowBuffer,
    _Work,
)
from b24api.references.outcome import (
    ReferenceFailure,
    ReferenceItem,
    ReferenceRequest,
)
from b24api.references.support import (
    _active_limit,
    _finish_task,
    _iterate_references,
    _page_cap,
    _wait_for_admission,
    _wait_for_cleanup_tasks,
    _wait_for_event,
)
from b24api.traversal import PaginationDriver
from b24api.traversal.plans import (
    BatchDispatch,
    DirectDispatch,
    DispatchPlan,
    ListPlan,
    ReferenceOutputOrder,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from b24api.contracts.policy import ExecutionPolicy
    from b24api.contracts.response import Response

type _PageDispatcher = _DirectPageDispatcher | _BatchPageDispatcher
_IDENTITY_PAGE_ADAPTER = IdentityPageAdapter()


class ReferenceScheduler:
    """Admit bounded reference state and schedule one sequential page per reference."""

    def __init__(  # noqa: PLR0913
        self,
        executor: Executor,
        *,
        plan: ListPlan,
        dispatch: DispatchPlan,
        selector: ResultSelector | None,
        identity: TraversalIdentity | None,
        output_order: ReferenceOutputOrder,
        tolerant: bool,
        policy: ExecutionPolicy,
        whole_result: bool = False,
        emit_complete: bool = False,
        emit_response: bool = False,
        capture_fail_fast: bool = False,
        page_cap_hint: int | None = None,
        page_adapter: PageAdapter = _IDENTITY_PAGE_ADAPTER,
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
        self.emit_complete = emit_complete
        self.emit_response = emit_response
        self.capture_fail_fast = capture_fail_fast
        self.page_adapter = page_adapter
        self.context = executor.context(policy)
        self.page_cap = _page_cap(
            plan,
            dispatch,
            policy,
            whole_result=whole_result,
            page_cap_hint=page_cap_hint,
        )
        self.active_limit = _active_limit(output_order, policy, self.page_cap)
        self.producer_state = _ProducerState(set(), {})
        head_reserve = self.page_cap if output_order is ReferenceOutputOrder.INPUT else 0
        self.buffer = _RowBuffer(
            policy.max_buffered_rows,
            self.context,
            head_reserve=head_reserve,
            producer_state=self.producer_state,
        )
        self.dispatcher: _PageDispatcher
        if isinstance(dispatch, DirectDispatch):
            self.dispatcher = _DirectPageDispatcher(executor, self.context, dispatch)
        else:
            self.dispatcher = _BatchPageDispatcher(
                executor,
                self.context,
                dispatch,
                producer_state=self.producer_state,
                buffer=self.buffer,
                page_cap=self.page_cap,
                pending_continuations_can_progress=output_order is ReferenceOutputOrder.READY,
            )
        self.violations: list[Violation] = []
        self.page_trace: list[PageRecord] = []
        self.page_trace_truncated = False
        self._delivery_uniqueness: dict[int, tuple[ReferenceItem, bool]] = {}
        self._source_controller: AsyncIteratorController[ReferenceRequest] | None = None
        self.active_references_high_water = 0
        self._next_page_sequence = 0

    async def outcomes(self, source: ReferenceSource) -> AsyncGenerator[ReferenceStreamItem]:  # noqa: C901, PLR0912
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
        pending_cancellation: asyncio.CancelledError | None = None

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
            cleanup = await await_cleanup_resistant(self._cleanup(iterator, admission, producer, primary_error))
            if cleanup.error is not None:
                cleanup_error = cleanup.error
                if primary_error is None or isinstance(primary_error, asyncio.CancelledError | GeneratorExit):
                    self._record_cleanup_failure(cleanup_error)
                    pending = cleanup.cancellation
                    if pending is None and isinstance(primary_error, asyncio.CancelledError):
                        pending = primary_error
                    rearm_cancellation(pending)
                    raise cleanup_error
                self._record_cleanup_failure(cleanup_error)
                if isinstance(cleanup_error, asyncio.CancelledError):
                    pending_cancellation = cleanup_error
                if cleanup.cancellation is not None:
                    pending_cancellation = cleanup.cancellation
            elif cleanup.cancellation is not None and primary_error is None:
                raise cleanup.cancellation
            elif cleanup.cancellation is not None and not isinstance(
                primary_error,
                asyncio.CancelledError | GeneratorExit,
            ):
                self._record_cleanup_failure(cleanup.cancellation)
                pending_cancellation = cleanup.cancellation
            if primary_error is not None and not isinstance(primary_error, asyncio.CancelledError | GeneratorExit):
                rearm_cancellation(pending_cancellation)

    async def _cleanup(
        self,
        iterator: AsyncIteratorController[ReferenceRequest],
        admission: _AdmissionState,
        producer: asyncio.Task[None],
        primary_error: BaseException | None,
    ) -> None:
        self.producer_state.closing = True
        self.producer_state.touch()
        producer.cancel()
        for task in admission.tasks.values():
            task.cancel()
        cleanup_errors: list[BaseException] = []
        try:
            await self.buffer.close()
        except BaseException as error:  # noqa: BLE001 - cleanup continues across owned resources
            if error is not primary_error:
                cleanup_errors.append(error)
        try:
            await self.dispatcher.aclose()
        except BaseException as error:  # noqa: BLE001 - cleanup continues across owned resources
            if error is not primary_error:
                cleanup_errors.append(error)
        pending, task_errors = await _wait_for_cleanup_tasks(
            (producer, *admission.tasks.values()),
            remaining=max(0.0, self.context.policy.max_elapsed - self.context.elapsed),
        )
        cleanup_errors.extend(error for error in task_errors if error is not primary_error)
        if pending:
            cleanup_errors.append(BudgetExceededError("reference task cleanup exceeded operation time budget"))
        else:
            try:
                await iterator.aclose(
                    remaining=max(0.0, self.context.policy.max_elapsed - self.context.elapsed),
                )
            except BaseException as error:  # noqa: BLE001 - attach at the stream boundary
                if error is not primary_error:
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
                self.producer_state.next_key = f"r{next_index}"
                self.producer_state.next_index = next_index
                self.producer_state.touch()
                await admission.slots.acquire()
                self.producer_state.source_pull_in_flight = True
                self.producer_state.touch()
                try:
                    reference = await iterator.get(self.context)
                except StopAsyncIteration:
                    self.producer_state.source_pull_in_flight = False
                    self.producer_state.source_terminal = True
                    self.producer_state.next_key = None
                    self.producer_state.next_index = None
                    self.producer_state.touch()
                    admission.slots.release()
                    return
                except BaseException:
                    self.producer_state.source_pull_in_flight = False
                    self.producer_state.next_key = None
                    self.producer_state.next_index = None
                    self.producer_state.touch()
                    admission.slots.release()
                    raise
                self.producer_state.source_pull_in_flight = False
                self.producer_state.next_key = None
                self.producer_state.next_index = None
                self.producer_state.touch()
                if not isinstance(reference, ReferenceRequest):
                    admission.slots.release()
                    raise TypeError("reference source must yield ReferenceRequest values")
                work = _Work(next_index, reference)
                next_index += 1
                queue = admission.ready if self.output_order is ReferenceOutputOrder.READY else asyncio.Queue(maxsize=1)
                admission.queues[work.index] = queue
                run = (
                    self._emit_local_non_execution(work, queue)
                    if reference.not_executed_reason is not None
                    else self._run_reference(work, queue)
                )
                admission.tasks[work.index] = asyncio.create_task(run)
                self.active_references_high_water = max(self.active_references_high_water, len(admission.tasks))
                admission.changed.set()
        finally:
            self.producer_state.source_pull_in_flight = False
            self.producer_state.source_terminal = True
            self.producer_state.next_key = None
            self.producer_state.next_index = None
            self.producer_state.touch()
            admission.changed.set()

    async def _run_reference(  # noqa: C901, PLR0915 - owns the per-reference transaction boundary
        self,
        work: _Work,
        output: asyncio.Queue[_Event],
    ) -> None:
        producer_key = f"r{work.index}"
        self.producer_state.runnable.add(producer_key)
        self.producer_state.indexes[producer_key] = work.index
        self.producer_state.touch()
        await asyncio.sleep(0)
        reservation: _Reservation | None = None
        partial_rows = 0
        page_state = 0
        violation_offset = 0
        trace_offset = 0
        scheduled_sequences: list[int] = []
        settlement: asyncio.Future[None] | None = None

        def settle_page() -> None:
            nonlocal settlement
            if settlement is not None and not settlement.done():
                settlement.set_result(None)
            settlement = None

        async def fetch(request: Request) -> Response:
            nonlocal page_state, reservation, settlement
            self.producer_state.runnable.discard(producer_key)
            self.producer_state.admitting.add(producer_key)
            self.producer_state.touch()
            sequence = self._next_page_sequence
            self._next_page_sequence += 1
            scheduled_sequences.append(sequence)
            try:
                reservation = await self.buffer.reserve(work.index, self.page_cap)
                dispatched: _DispatchedPage = await self.dispatcher.fetch(request, f"r{work.index}")
                settlement = dispatched.settlement
            except BaseException as error:
                self.producer_state.admitting.discard(producer_key)
                self.producer_state.touch()
                if reservation is not None:
                    await self.buffer.abort(reservation)
                reservation = None
                dispatch = PageDispatch.BATCH if isinstance(self.dispatch, BatchDispatch) else PageDispatch.DIRECT
                batch_index = error.failure.command_index if isinstance(error, _BatchPageError) else None
                report_error = (
                    error.failure.error
                    if isinstance(error, _BatchPageError) and isinstance(error.failure.error, BaseException)
                    else error
                )
                if isinstance(error, _BatchPageError) or bool(
                    getattr(error, "_b24api_dispatch_started", False),
                ):
                    driver.set_page_dispatch(dispatch=dispatch, batch_index=batch_index)
                    driver.record_unknown_page(
                        dispatch=dispatch,
                        batch_index=batch_index,
                        error=report_error,
                    )
                raise
            driver.set_page_dispatch(
                dispatch=dispatched.dispatch,
                batch_index=dispatched.batch_index,
            )
            page_state += 1
            return dispatched.response

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
            page_adapter=self.page_adapter,
            initial_cursor=work.reference.initial_cursor,
        )
        try:
            async for page in driver.pages():
                if reservation is None:
                    raise RuntimeError("page completed without a buffer reservation")  # noqa: TRY301
                await self.buffer.accept(reservation, page.retained_rows)
                if self.output_order is ReferenceOutputOrder.INPUT:
                    settle_page()
                acknowledged = asyncio.get_running_loop().create_future()
                if page.continuing:
                    self.producer_state.pending_continuations.add(producer_key)
                    self.producer_state.touch()
                page_violations = tuple(driver.violations[violation_offset:])
                violation_offset = len(driver.violations)
                await output.put(
                    _PageEvent(
                        work,
                        page.items,
                        page.response,
                        page.item_weights,
                        driver.last_page_unique_mask,
                        page_violations,
                        self._annotate_page_records(
                            work,
                            self._new_page_records(driver, trace_offset),
                            scheduled_sequences,
                        ),
                        reservation,
                        acknowledged,
                    ),
                )
                trace_offset = driver.page_trace_count
                await acknowledged
                if self.output_order is ReferenceOutputOrder.READY:
                    settle_page()
                self.producer_state.pending_continuations.discard(producer_key)
                if page.continuing:
                    self.producer_state.runnable.add(producer_key)
                    self.producer_state.touch()
                partial_rows += len(page.items)
                reservation = None
            if reservation is not None:
                await self.buffer.abort(reservation)
                reservation = None
            await output.put(
                _DoneEvent(
                    work,
                    partial_rows,
                    tuple(driver.violations[violation_offset:]),
                    self._annotate_page_records(
                        work,
                        self._new_page_records(driver, trace_offset),
                        scheduled_sequences,
                    ),
                ),
            )
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
                    self._annotate_page_records(
                        work,
                        self._new_page_records(driver, trace_offset),
                        scheduled_sequences,
                    ),
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
                    self._annotate_page_records(
                        work,
                        self._new_page_records(driver, trace_offset),
                        scheduled_sequences,
                    ),
                ),
            )
        finally:
            settle_page()
            self.producer_state.runnable.discard(producer_key)
            self.producer_state.admitting.discard(producer_key)
            self.producer_state.pending_continuations.discard(producer_key)
            self.producer_state.indexes.pop(producer_key, None)
            self.producer_state.touch()
            if reservation is not None:
                await self.buffer.abort(reservation)

    async def _emit_local_non_execution(self, work: _Work, output: asyncio.Queue[_Event]) -> None:
        """Emit one proved local terminal state without touching the dispatcher."""
        reason = work.reference.not_executed_reason
        if reason is None:
            raise RuntimeError("local non-execution requires a reason")
        await output.put(
            _FailureEvent(
                work,
                CapabilityError("reference binding failed local validation"),
                None,
                0,
                0,
                (),
                not_executed_reason=reason,
            ),
        )

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
        combined = tuple(sorted((*self.page_trace, *event.page_records), key=lambda record: record.sequence))
        retained, truncated = retain_page_trace(combined, self.context.policy.page_trace_limit)
        self.page_trace[:] = retained
        self.page_trace_truncated = self.page_trace_truncated or truncated

    @staticmethod
    def _new_page_records(driver: PaginationDriver, previous_count: int) -> tuple[PageRecord, ...]:
        if driver.page_trace_count == previous_count:
            return ()
        if driver.page_trace_count != previous_count + 1 or driver.last_page_record is None:
            raise RuntimeError("one logical fetch must produce at most one page record")
        return (driver.last_page_record,)

    @staticmethod
    def _annotate_page_records(
        work: _Work,
        records: tuple[PageRecord, ...],
        scheduled_sequences: list[int],
    ) -> tuple[PageRecord, ...]:
        return tuple(
            replace(
                record,
                sequence=scheduled_sequences[record.sequence],
                reference_index=work.index,
            )
            for record in records
        )

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
                if self.emit_response:
                    yield _KernelFanOutSuccess(event.work.index, event.work.reference, event.response)
                    for weight in event.item_weights:
                        await self.buffer.release(event.reservation, weight)
                    return
                for item, weight, is_unique in zip(
                    event.items,
                    event.item_weights,
                    event.unique_mask,
                    strict=True,
                ):
                    outcome = ReferenceItem._from_frozen(  # noqa: SLF001 - trusted frozen traversal row
                        event.work.reference.reference_key,
                        item,
                        event.work.reference.correlation,
                    )
                    self._delivery_uniqueness[id(outcome)] = (outcome, is_unique)
                    yield outcome
                    await self.buffer.release(event.reservation, weight)
            finally:
                if not event.acknowledged.done():
                    event.acknowledged.set_result(None)
            return
        if isinstance(event, _DoneEvent):
            if self.emit_complete:
                yield _KernelReferenceComplete(event.work.index, event.work.reference, event.row_count)
            return
        request = event.work.reference.request
        failure = ReferenceFailure(
            event.work.reference.reference_key,
            request,
            event.error,
            cursor=event.cursor,
            page_state=event.page_state,
            partial_rows=event.partial_rows,
            replay_safety=request.replay_safety or ReplaySafety.UNKNOWN,
            replay_disposition=event.replay_disposition,
            correlation=event.work.reference.correlation,
            not_executed_reason=event.not_executed_reason,
        )
        if not self.tolerant:
            if self.capture_fail_fast:
                raise _ReferenceWindowError(failure)
            raise event.error
        self.violations.append(
            Violation(
                severity=ViolationSeverity.WARNING,
                code="reference_failure",
                message="one reference produced a typed failure outcome",
            ),
        )
        yield failure

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
