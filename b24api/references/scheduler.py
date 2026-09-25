"""Bounded fair scheduling for independent and paginated references."""

from __future__ import annotations
import asyncio
import functools
from dataclasses import dataclass, field, replace
from inspect import isawaitable
from typing import TYPE_CHECKING, cast

from b24api.completion.reference_recorder import ReferenceCompletionRecorder
from b24api.contracts.completion import BindingClosure, CommandSettlement
from b24api.contracts.page import IdentityPageAdapter, PageAdapter
from b24api.contracts.page_stop import CallerStop, ContinuePage, PageBoundary, PageStopPolicy
from b24api.contracts.report import PageDispatch, PageRecord, Violation, ViolationSeverity, retain_page_trace
from b24api.contracts.request import ReplaySafety, Request, ResultSelector, TraversalIdentity
from b24api.contracts.violation import retain_violations
from b24api.errors import AmbiguousExecutionError, ApiResponseError, BudgetExceededError, CapabilityError
from b24api.execution import (
    AsyncIteratorController,
    Executor,
    await_cleanup_resistant,
    rearm_cancellation,
)
from b24api.references.dispatch import (
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
    KernelReferenceFailure,
    KernelReferenceItem,
    ReferenceRequest,
)
from b24api.references.support import (
    _active_limit,
    _finish_done_completion,
    _finish_task,
    _new_page_records,
    _page_cap,
    _record_cleanup_failure,
    _wait_for_admission,
    _wait_for_cleanup_tasks,
    _wait_for_event,
)
from b24api.traversal import PaginationDriver
from b24api.traversal.plans import (
    DispatchPlan,
    KernelBatchDispatch,
    KernelDirectDispatch,
    ListPlan,
    ReferenceOutputOrder,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from b24api._sources import OwnedSource
    from b24api.completion.reference_recorder import ReferenceBindingRecorder
    from b24api.contracts.policy import ExecutionPolicy
    from b24api.contracts.response import Response
    from b24api.traversal.identity import _Page

type _PageDispatcher = _DirectPageDispatcher | _BatchPageDispatcher
_IDENTITY_PAGE_ADAPTER = IdentityPageAdapter()


def _failed_settlement(error: BaseException) -> CommandSettlement:
    """Classify a page whose dispatch raised: ambiguous, failed, possibly sent, or never sent."""
    if isinstance(error, _BatchPageError) and isinstance(error.failure.error, AmbiguousExecutionError):
        return CommandSettlement.UNKNOWN
    if isinstance(error, _BatchPageError | ApiResponseError):
        return CommandSettlement.FAILURE
    if bool(getattr(error, "_b24api_dispatch_started", False)):
        return CommandSettlement.UNKNOWN
    return CommandSettlement.NOT_EXECUTED


@dataclass(slots=True)
class _ReferenceRun:
    """Mutable state of one reference transaction, shared by the driver's fetch hook and the page loop."""

    work: _Work
    output: asyncio.Queue[_Event]
    completion: ReferenceBindingRecorder
    producer_key: str
    driver: PaginationDriver = field(init=False)
    reservation: _Reservation | None = None
    partial_rows: int = 0
    page_state: int = 0
    violation_offset: int = 0
    trace_offset: int = 0
    scheduled_sequences: list[int] = field(default_factory=list)
    stopped_reason: str | None = None
    page_admission: asyncio.Future[None] | None = None
    settlement: asyncio.Future[None] | None = None

    def admit_page(self) -> None:
        """Release the dispatcher's admission slot once the page is in the row buffer."""
        if self.page_admission is not None and not self.page_admission.done():
            self.page_admission.set_result(None)
        self.page_admission = None

    def settle_page(self) -> None:
        """Release the dispatcher's settlement wait for the current page."""
        if self.settlement is not None and not self.settlement.done():
            self.settlement.set_result(None)
        self.settlement = None


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
        page_stop: PageStopPolicy | None = None,
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
        self.page_stop = page_stop
        self.stopped_bindings = 0
        self.completion = ReferenceCompletionRecorder()
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
        if isinstance(dispatch, KernelDirectDispatch):
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
        self._delivery_uniqueness: dict[int, tuple[KernelReferenceItem, bool]] = {}
        self._source_controller: AsyncIteratorController[ReferenceRequest] | None = None
        self.active_references_high_water = 0
        self._next_page_sequence = 0

    async def outcomes(self, source: OwnedSource[ReferenceRequest]) -> AsyncGenerator[ReferenceStreamItem]:  # noqa: C901, PLR0912
        """Yield correlated operation outcomes."""
        PaginationDriver.validate_contract(
            self.plan,
            self.identity,
            self.context.policy,
        )
        await self.context.start()
        iterator = AsyncIteratorController(
            source,
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
                    _record_cleanup_failure(self.violations, cleanup_error)
                    pending = cleanup.cancellation
                    if pending is None and isinstance(primary_error, asyncio.CancelledError):
                        pending = primary_error
                    rearm_cancellation(pending)
                    raise cleanup_error
                _record_cleanup_failure(self.violations, cleanup_error)
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
                _record_cleanup_failure(self.violations, cleanup.cancellation)
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
                await admission.slots.acquire()
                self.producer_state.next_key = f"r{next_index}"
                self.producer_state.next_index = next_index
                self.producer_state.source_pull_in_flight = True
                self.producer_state.touch()
                try:
                    reference = await iterator.get(self.context)
                except StopAsyncIteration:
                    self.producer_state.source_pull_in_flight = False
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
                work = _Work(next_index, reference)
                self.completion.admit(next_index)
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
            self.producer_state.next_key = None
            self.producer_state.next_index = None
            self.producer_state.touch()
            admission.changed.set()

    async def _run_reference(self, work: _Work, output: asyncio.Queue[_Event]) -> None:
        """Own one reference transaction from its first page to its terminal event."""
        run = _ReferenceRun(work, output, self.completion.binding(work.index), f"r{work.index}")
        self.producer_state.runnable.add(run.producer_key)
        self.producer_state.indexes[run.producer_key] = work.index
        self.producer_state.touch()
        await asyncio.sleep(0)
        driver = run.driver = PaginationDriver(
            self.executor,
            work.reference.request,
            self.plan,
            selector=self.selector,
            identity=self.identity,
            context=self.context,
            fetch=functools.partial(self._fetch_page, run),
            single_result_as_item=self.whole_result,
            page_cap_hint=self.page_cap,
            page_adapter=self.page_adapter,
            initial_cursor=work.reference.initial_cursor,
            completion_recorder=run.completion,
        )
        pages = driver.pages()
        try:
            async for page in pages:
                if not await self._deliver_page(run, page):
                    break
            if run.reservation is not None:
                await self.buffer.abort(run.reservation)
                run.reservation = None
            await output.put(
                _DoneEvent(
                    work,
                    run.partial_rows,
                    tuple(driver.violations[run.violation_offset :]),
                    self._pending_page_records(run),
                    run.stopped_reason,
                    driver.terminal_reason,
                    driver.expected_total,
                ),
            )
        except asyncio.CancelledError:
            raise
        except _BatchPageError as error:
            failure = self._failure_event(run, cast("BaseException", error.failure.error))
            await output.put(replace(failure, replay_disposition=error.failure.replay_disposition))
        except Exception as error:  # noqa: BLE001 - per-reference tolerant outcome boundary
            await output.put(self._failure_event(run, error))
        finally:
            await pages.aclose()
            run.admit_page()
            run.settle_page()
            self.producer_state.runnable.discard(run.producer_key)
            self.producer_state.admitting.discard(run.producer_key)
            self.producer_state.pending_continuations.discard(run.producer_key)
            self.producer_state.indexes.pop(run.producer_key, None)
            self.producer_state.touch()
            if run.reservation is not None:
                await self.buffer.abort(run.reservation)

    async def _fetch_page(self, run: _ReferenceRun, request: Request) -> Response:
        """Reserve buffer capacity and dispatch one page; the driver calls this as its fetch hook."""
        self.producer_state.runnable.discard(run.producer_key)
        self.producer_state.admitting.add(run.producer_key)
        self.producer_state.touch()
        sequence = self._next_page_sequence
        self._next_page_sequence += 1
        run.scheduled_sequences.append(sequence)
        run.completion.scheduled()
        try:
            run.reservation = await self.buffer.reserve(run.work.index, self.page_cap)
            dispatched: _DispatchedPage = await self.dispatcher.fetch(request, run.producer_key)
            run.page_admission = dispatched.admission
            run.settlement = dispatched.settlement
        except BaseException as error:
            run.completion.settled(_failed_settlement(error))
            self.producer_state.admitting.discard(run.producer_key)
            self.producer_state.touch()
            if run.reservation is not None:
                await self.buffer.abort(run.reservation)
            run.reservation = None
            if isinstance(error, _BatchPageError) or bool(getattr(error, "_b24api_dispatch_started", False)):
                dispatch = PageDispatch.BATCH if isinstance(self.dispatch, KernelBatchDispatch) else PageDispatch.DIRECT
                batch_index = error.failure.command_index if isinstance(error, _BatchPageError) else None
                report_error = (
                    error.failure.error
                    if isinstance(error, _BatchPageError) and isinstance(error.failure.error, BaseException)
                    else error
                )
                run.driver.set_page_dispatch(dispatch=dispatch, batch_index=batch_index)
                run.driver.record_unknown_page(dispatch=dispatch, batch_index=batch_index, error=report_error)
            raise
        run.driver.set_page_dispatch(dispatch=dispatched.dispatch, batch_index=dispatched.batch_index)
        run.completion.settled(CommandSettlement.SUCCESS)
        run.page_state += 1
        return dispatched.response

    async def _deliver_page(self, run: _ReferenceRun, page: _Page) -> bool:
        """Hand one validated page to the consumer; return False when the caller stops the reference."""
        if run.reservation is None:
            raise RuntimeError("page completed without a buffer reservation")
        await self.buffer.accept(run.reservation, page.retained_rows)
        # The physical sender owns capacity only until the decoded page is
        # admitted to the bounded row buffer.  Holding that slot until a
        # READY consumer acknowledges every row would serialize consumer
        # work with the next network request.
        run.admit_page()
        if self.output_order is ReferenceOutputOrder.INPUT:
            run.settle_page()
        acknowledged = asyncio.get_running_loop().create_future()
        if page.continuing:
            self.producer_state.pending_continuations.add(run.producer_key)
            self.producer_state.touch()
        page_violations = tuple(run.driver.violations[run.violation_offset :])
        run.violation_offset = len(run.driver.violations)
        page_records = self._pending_page_records(run)
        await run.output.put(
            _PageEvent(
                run.work,
                page.items,
                page.response,
                page.item_weights,
                run.driver.last_page_unique_mask,
                page_violations,
                page_records,
                run.reservation,
                acknowledged,
            ),
        )
        run.trace_offset = run.driver.page_trace_count
        await acknowledged
        if self.output_order is ReferenceOutputOrder.READY:
            run.settle_page()
        self.producer_state.pending_continuations.discard(run.producer_key)
        run.partial_rows += len(page.items)
        run.reservation = None
        if await self._caller_stopped(run, page, page_records):
            run.completion.acknowledged()
            return False
        run.completion.acknowledged()
        if page.continuing:
            self.producer_state.runnable.add(run.producer_key)
            self.producer_state.touch()
        return True

    async def _caller_stopped(self, run: _ReferenceRun, page: _Page, page_records: tuple[PageRecord, ...]) -> bool:
        """Ask the page stop policy about a delivered page; True stops a reference that would continue."""
        if self.page_stop is None:
            return False
        if len(page_records) != 1:
            raise RuntimeError("validated reference page lacks completion provenance")
        decision = self.page_stop.on_page(PageBoundary(run.work.index, page_records[0], tuple(page.items)))
        if isawaitable(decision):
            decision = await decision
        if not isinstance(decision, ContinuePage | CallerStop):
            raise TypeError("reference page stop policy returned an invalid decision")
        if not (isinstance(decision, CallerStop) and page.continuing):
            return False
        run.stopped_reason = decision.reason
        self.stopped_bindings += 1
        return True

    def _pending_page_records(self, run: _ReferenceRun) -> tuple[PageRecord, ...]:
        """Return the driver's page records since the last event, tagged with this reference."""
        return self._annotate_page_records(
            run.work,
            _new_page_records(run.driver, run.trace_offset),
            run.scheduled_sequences,
        )

    def _failure_event(self, run: _ReferenceRun, error: BaseException) -> _FailureEvent:
        """Build the terminal failure event with the partial progress the reference reached."""
        return _FailureEvent(
            run.work,
            error,
            run.driver.cursor_state,
            run.page_state,
            run.partial_rows,
            tuple(run.driver.violations[run.violation_offset :]),
            self._pending_page_records(run),
        )

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
        self.violations = list(retain_violations((*self.violations, *event.violations)))
        combined = tuple(sorted((*self.page_trace, *event.page_records), key=lambda record: record.sequence))
        retained, truncated = retain_page_trace(combined, self.context.policy.page_trace_limit)
        self.page_trace[:] = retained
        self.page_trace_truncated = self.page_trace_truncated or truncated

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

    async def _consume_event(self, event: _Event) -> AsyncGenerator[ReferenceStreamItem]:
        if isinstance(event, _PageEvent):
            try:
                if self.emit_response:
                    yield _KernelFanOutSuccess(event.work.reference, event.response)
                    for weight in event.item_weights:
                        await self.buffer.release(event.reservation, weight)
                    self.completion.binding(event.work.index).delivered()
                    return
                for item, weight, is_unique in zip(
                    event.items,
                    event.item_weights,
                    event.unique_mask,
                    strict=True,
                ):
                    outcome = KernelReferenceItem._from_frozen(  # noqa: SLF001 - trusted frozen traversal row
                        event.work.reference.reference_key,
                        item,
                        event.work.reference.correlation,
                    )
                    self._delivery_uniqueness[id(outcome)] = (outcome, is_unique)
                    yield outcome
                    await self.buffer.release(event.reservation, weight)
                self.completion.binding(event.work.index).delivered()
            finally:
                if not event.acknowledged.done():
                    event.acknowledged.set_result(None)
            return
        if isinstance(event, _DoneEvent):
            _finish_done_completion(self.completion, event)
            if self.emit_complete:
                yield _KernelReferenceComplete(
                    event.work.reference,
                    event.row_count,
                    event.stopped_reason,
                )
            return
        request = event.work.reference.request
        self.completion.terminal(
            event.work.index,
            BindingClosure.UNKNOWN if self.completion.binding(event.work.index).unknown else BindingClosure.FAILURE,
        )
        failure = KernelReferenceFailure(
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
        violation = Violation(
            ViolationSeverity.WARNING,
            "reference_failure",
            "one reference produced a typed failure outcome",
            replay_disposition=event.replay_disposition,
        )
        self.violations[:] = retain_violations((*self.violations, violation))
        yield failure

    def record_delivery(self, item: KernelReferenceItem) -> bool:
        """Record one delivered reference item."""
        stored = self._delivery_uniqueness.pop(id(item), None)
        return stored is not None and stored[0] is item and stored[1]
