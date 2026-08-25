"""Bounded fair scheduling for independent and paginated references."""

from __future__ import annotations
import asyncio
from typing import TYPE_CHECKING, cast

from b24api.contracts.report import Violation, ViolationSeverity
from b24api.contracts.request import IdentitySpec, ReplaySafety, Request, ResultSelector
from b24api.error import BudgetExceededError
from b24api.execution import (
    AsyncIteratorController,
    Executor,
    await_cleanup_resistant,
    rearm_cancellation,
)
from b24api.plans import (
    DirectDispatch,
    DispatchPlan,
    ListPlan,
    ReferenceOutputOrder,
)
from b24api.references.dispatch import (
    ReferenceSource,
    ReferenceStreamItem,
    _AdmissionState,
    _BatchPageDispatcher,
    _BatchPageError,
    _DirectPageDispatcher,
    _DoneEvent,
    _Event,
    _FailureEvent,
    _KernelFanOutSuccess,
    _KernelReferenceComplete,
    _PageEvent,
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

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from b24api.contracts.policy import ExecutionPolicy
    from b24api.contracts.response import Response

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
        emit_complete: bool = False,
        emit_response: bool = False,
        capture_fail_fast: bool = False,
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
        self.emit_complete = emit_complete
        self.emit_response = emit_response
        self.capture_fail_fast = capture_fail_fast
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
        self.active_references_high_water = 0

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
            cleanup = await await_cleanup_resistant(self._cleanup(iterator, admission, producer))
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
                self.active_references_high_water = max(self.active_references_high_water, len(admission.tasks))
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
                        page.response,
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
            await output.put(_DoneEvent(work, partial_rows, tuple(driver.violations[violation_offset:])))
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
            payload=event.work.reference.payload,
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
