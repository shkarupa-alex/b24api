"""Bounded fair scheduling for independent and paginated references."""

from __future__ import annotations
import asyncio
import functools
from dataclasses import replace
from inspect import isawaitable
from typing import TYPE_CHECKING, cast

from b24api.completion.reference_recorder import ReferenceCompletionRecorder
from b24api.contracts.completion import BindingClosure, CommandSettlement
from b24api.contracts.page import IdentityPageAdapter, PageAdapter
from b24api.contracts.page_stop import CallerStop, ContinuePage, PageBoundary, PageStopPolicy
from b24api.contracts.report import PageDispatch, PageRecord, Violation, ViolationSeverity, retain_page_trace
from b24api.contracts.request import ReplaySafety, Request, ResultSelector, TraversalIdentity
from b24api.contracts.response import result_snapshot
from b24api.contracts.violation import retain_violations
from b24api.errors import BudgetExceededError, CapabilityError
from b24api.execution import (
    AsyncIteratorController,
    Executor,
    await_cleanup_resistant,
    rearm_cancellation,
)
from b24api.references.counted_io import SharedCountedWaveIO
from b24api.references.dispatch import (
    ReferenceStreamItem,
    _AdmissionState,
    _BatchPageDispatcher,
    _BatchPageError,
    _DirectPageDispatcher,
    _DispatchedPage,
    _DoneEvent,
    _Event,
    _failed_dispatch,
    _failed_settlement,
    _FailureEvent,
    _KernelFanOutSuccess,
    _KernelReferenceComplete,
    _PageEvent,
    _ProducerState,
    _ReferenceWindowError,
    _RowBuffer,
    _Work,
)
from b24api.references.keyset_run import run_fast_keyset
from b24api.references.outcome import (
    KernelReferenceFailure,
    KernelReferenceItem,
    ReferenceRequest,
)
from b24api.references.program import FastKeysetProgram, SourceProgram
from b24api.references.run import _ReferenceRun
from b24api.references.support import (
    _active_limit,
    _declared_short_page_width,
    _done_closure,
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
    CountedOffsetPlan,
    DispatchPlan,
    KernelBatchDispatch,
    KernelDirectDispatch,
    ListPlan,
    ReferenceOutputOrder,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from b24api._sources import OwnedSource
    from b24api.contracts.policy import ExecutionPolicy
    from b24api.contracts.report import KeysetSelectionSummary
    from b24api.contracts.response import Response
    from b24api.references.dispatch import _Reservation
    from b24api.traversal.identity import _Page

type _PageDispatcher = _DirectPageDispatcher | _BatchPageDispatcher
_IDENTITY_PAGE_ADAPTER = IdentityPageAdapter()


class ReferenceScheduler:
    """Admit bounded reference state and schedule one sequential page per reference."""

    def __init__(  # noqa: PLR0913
        self,
        executor: Executor,
        *,
        plan: ListPlan | None,
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
        fast: FastKeysetProgram | None = None,
        keyset_selection: KeysetSelectionSummary | None = None,
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
        # Without a default plan every admitted reference carries its own lowered program (heterogeneous).
        self.default_program: SourceProgram | None = None
        if plan is not None:
            counted = isinstance(plan, CountedOffsetPlan) and isinstance(dispatch, KernelBatchDispatch)
            self.default_program = SourceProgram(
                plan,
                selector,
                identity,
                page_adapter,
                _page_cap(plan, dispatch, policy, whole_result=whole_result, page_cap_hint=page_cap_hint),
                page_cap_hint=page_cap_hint,
                page_stop=page_stop,
                whole_result=whole_result,
                counted_page_size=page_cap_hint if counted and not whole_result else None,
                fast=fast,
                keyset_selection=keyset_selection,
            )
        self.page_cap = 1 if self.default_program is None else self.default_program.page_cap
        self.active_limit = (
            policy.max_active_references
            if self.default_program is None
            else _active_limit(output_order, policy, self.page_cap)
        )
        self.producer_state = _ProducerState(set(), {})
        self._caps: dict[int, int] = {}
        head_reserve = (
            self.page_cap if output_order is ReferenceOutputOrder.INPUT and self.default_program is not None else 0
        )
        self.buffer = _RowBuffer(
            policy.max_buffered_rows,
            self.context,
            head_reserve=head_reserve,
            producer_state=self.producer_state,
            ordered=output_order is ReferenceOutputOrder.INPUT,
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
        # Lease decisions read the admitted set, source exhaustion and the fast bindings holding or
        # awaiting a lease; a change of either wakes lease waiters only while one is waiting.
        self._admission: _AdmissionState | None = None
        self._source_exhausted = False
        self._fast_active: set[int] = set()
        self._fast_waiting = 0

    async def outcomes(self, source: OwnedSource[ReferenceRequest]) -> AsyncGenerator[ReferenceStreamItem]:  # noqa: C901, PLR0912
        """Yield correlated operation outcomes."""
        if self.default_program is not None:
            PaginationDriver.validate_contract(
                self.default_program.plan,
                self.default_program.identity,
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
        self._admission = admission
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
                    self._source_exhausted = True
                    await self._wake_lease_waiters()
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
                if reference.not_executed_reason is None:
                    self._admit_program(work)
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

    def _program(self, work: _Work) -> SourceProgram:
        program = work.reference.program or self.default_program
        if program is None:
            raise RuntimeError("heterogeneous reference lacks its lowered program")
        return program

    def _admit_program(self, work: _Work) -> None:
        """Record an admitted reference's row cap; under INPUT the frontier reserve covers every admitted cap.

        Any admitted binding may become the frontier while rows behind it stay held, so the reserve
        is the largest admitted cap, not only the current head's.
        """
        cap = self._program(work).page_cap
        self._caps[work.index] = cap
        if self.output_order is ReferenceOutputOrder.INPUT:
            self.buffer.raise_head_reserve(cap)

    def _head_cap(self, index: int) -> int:
        return self._caps.get(index, self.page_cap if self.default_program is not None else 0)

    async def _run_reference(self, work: _Work, output: asyncio.Queue[_Event]) -> None:
        """Own one reference transaction from its first page to its terminal event."""
        program = self._program(work)
        run = _ReferenceRun(work, output, self.completion.binding(work.index), f"r{work.index}", program)
        self.producer_state.runnable.add(run.producer_key)
        self.producer_state.indexes[run.producer_key] = work.index
        self.producer_state.caps[run.producer_key] = program.page_cap
        self.producer_state.touch()
        await asyncio.sleep(0)
        pages = None if program.fast is not None else self._driven_pages(run)
        try:
            if program.fast is not None:
                await run_fast_keyset(self, run, program.fast)
            elif pages is not None:
                await self._drive(run, pages)
        except asyncio.CancelledError:
            raise
        except _BatchPageError as error:
            failure = self._failure_event(run, cast("BaseException", error.failure.error))
            await output.put(replace(failure, replay_disposition=error.failure.replay_disposition))
        except Exception as error:  # noqa: BLE001 - per-reference tolerant outcome boundary
            await output.put(self._failure_event(run, error))
        finally:
            if pages is not None:
                await pages.aclose()
            self._retire_producer(run)
            self._fast_active.discard(work.index)
            if run.reservation is not None:
                await self.buffer.abort(run.reservation)

    def _driven_pages(self, run: _ReferenceRun) -> AsyncGenerator[_Page]:
        """Build the binding's pagination driver and return its page stream."""
        program, work = run.program, run.work
        run.driver = PaginationDriver(
            self.executor,
            work.reference.request,
            program.plan,
            selector=program.selector,
            identity=program.identity,
            context=self.context,
            fetch=functools.partial(self._fetch_page, run),
            single_result_as_item=program.whole_result,
            page_cap_hint=program.page_cap,
            page_adapter=program.page_adapter,
            initial_cursor=work.reference.initial_cursor,
            completion_recorder=run.completion,
        )
        return self._pages(run)

    async def _drive(self, run: _ReferenceRun, pages: AsyncGenerator[_Page]) -> None:
        """Deliver the driver's pages, then the binding's terminal event."""
        driver, program = run.driver, run.program
        async for page in pages:
            if not await self._deliver_page(run, page):
                break
        if run.reservation is not None:
            await self.buffer.abort(run.reservation)
            run.reservation = None
        await run.output.put(
            _DoneEvent(
                run.work,
                run.partial_rows,
                tuple(driver.violations[run.violation_offset :]),
                self._pending_page_records(run),
                run.stopped_reason,
                driver.terminal_reason,
                driver.expected_total,
                _declared_short_page_width(program.plan, driver.terminal_reason),
                keyset_selection=program.keyset_selection,
            ),
        )

    def _retire_producer(self, run: _ReferenceRun) -> None:
        """Release the run's dispatcher waits and remove it from the advisory producer state."""
        run.admit_page()
        run.settle_page()
        self.producer_state.runnable.discard(run.producer_key)
        self.producer_state.admitting.discard(run.producer_key)
        self.producer_state.pending_continuations.discard(run.producer_key)
        self.producer_state.pending_refills.pop(run.producer_key, None)
        self.producer_state.indexes.pop(run.producer_key, None)
        self.producer_state.caps.pop(run.producer_key, None)
        self.producer_state.touch()

    @property
    def batch_dispatcher(self) -> _BatchPageDispatcher:
        """Return the shared batch dispatcher fast keyset waves travel through."""
        if not isinstance(self.dispatcher, _BatchPageDispatcher):
            raise CapabilityError("fast keyset reference traversal requires BatchDispatch")
        return self.dispatcher

    @property
    def wave_command_cap(self) -> int:
        """Return the shared physical batch size: one fast wave fits one shared batch."""
        if not isinstance(self.dispatch, KernelBatchDispatch):
            raise CapabilityError("fast keyset reference traversal requires BatchDispatch")
        return self.dispatch.batch_size

    async def lease_rows(self, run: _ReferenceRun, fast: FastKeysetProgram) -> _Reservation:
        """Wait for the binding's row lease ``G``; it is held, never grown, until the binding's terminal."""
        index = run.work.index
        self._fast_active.add(index)
        self._fast_waiting += 1
        # A binding waiting for its lease cannot add a command, so no wave may wait to coalesce with it.
        self.producer_state.runnable.discard(run.producer_key)
        self.producer_state.touch()
        try:
            # The wait is bounded by the operation's elapsed budget, as every dispatch wait is.
            remaining = self.context.policy.max_elapsed - self.context.elapsed
            if remaining <= 0:
                raise BudgetExceededError("operation elapsed budget exhausted before the fast keyset lease")
            try:
                async with asyncio.timeout(remaining):
                    lease = await self.buffer.lease(index, functools.partial(self._lease_size, fast, index))
            except TimeoutError as error:
                raise BudgetExceededError("fast keyset lease wait exceeded the operation time budget") from error
        finally:
            self._fast_waiting -= 1
        # Every page of a leased binding is charged to its lease, none to the free pool rows.
        self.producer_state.caps[run.producer_key] = 0
        self.producer_state.runnable.add(run.producer_key)
        self.producer_state.touch()
        return lease

    async def settle_lease(self, lease: _Reservation, rows: int) -> None:
        """Fix a planned binding's lease at ``rows``, giving the rest back to the bindings awaiting one."""
        await self.buffer.settle_lease(lease, rows)

    def _lease_size(self, fast: FastKeysetProgram, index: int, free: int) -> int | None:
        """Size one fast lease from the free rows, or return None to wait.

        P = 3C+A is the floor a bounded plan needs and Q = 4C+A the comfortable quota. A binding
        keeps the existing frontier reserve R free while any other source is active or unread, and
        takes at most an even share of the pool among the fast sources that are active or may still
        arrive. AUTO below the floor runs exclusively on the whole pool, like a flat traversal, and
        under INPUT only once it is the frontier. The
        INPUT frontier never waits on rows that only its own progress could release: it takes what
        is free, or fails finitely; a lease whose binding has not planned yet may still be trimmed, so
        the frontier waits for it to settle.
        """
        pool = self.buffer.maximum
        frontier = self.output_order is ReferenceOutputOrder.INPUT and index == self.buffer.head_index
        stranded = frontier and not self.buffer.releasable_elsewhere(index)
        if fast.automatic and pool < fast.floor:
            # Under INPUT only the frontier may take the whole pool: a binding behind it would hold
            # every row while the frontier, which its own rows wait for, could reserve nothing.
            if free == pool and (frontier or self.output_order is not ReferenceOutputOrder.INPUT):
                return pool
            if stranded:
                if free >= 2 * fast.effective_page_cap:
                    return free
                raise BudgetExceededError("fast keyset lease cannot fit the boundary wave at the INPUT frontier")
            return None
        admission = self._admission
        others = admission is not None and len(admission.tasks) > 1
        keep = 0 if frontier or not (others or not self._source_exhausted) else self.buffer.head_reserve
        sources = len(self._fast_active) + (0 if self._source_exhausted else 1)
        cap = max(fast.floor, (pool - keep) // sources)
        available = free - keep
        # G = clamp(free - R_keep, P, G_cap): a share below the quota Q starts from the floor P, while a
        # share that can hold Q waits for it rather than plan below the comfortable quota.
        if available >= (fast.floor if cap < fast.quota else fast.quota):
            return min(available, cap)
        if stranded:
            if free >= fast.floor:
                return min(free, cap)
            raise BudgetExceededError("fast keyset lease cannot fit its plan at the INPUT frontier")
        return None

    async def _wake_lease_waiters(self) -> None:
        if self._fast_waiting:
            await self.buffer.poke()

    def _pages(self, run: _ReferenceRun) -> AsyncGenerator[_Page]:
        """Run a counted program's head and dense tail on the shared queue, any other on its driver."""
        page_size = run.program.counted_page_size
        if page_size is None or not isinstance(self.dispatch, KernelBatchDispatch):
            return run.driver.pages()
        return run.driver.counted_batch_pages(
            batch_size=self.dispatch.batch_size,
            page_size=page_size,
            io=SharedCountedWaveIO(self, run),
        )

    def take_page_sequence(self) -> int:
        """Return the next operation-wide page sequence, in scheduling order."""
        sequence = self._next_page_sequence
        self._next_page_sequence += 1
        return sequence

    async def dispatch_page(self, run: _ReferenceRun, request: Request) -> _DispatchedPage:
        """Schedule one page, reserve its row capacity and dispatch it; the caller settles it."""
        self.producer_state.runnable.discard(run.producer_key)
        self.producer_state.admitting.add(run.producer_key)
        self.producer_state.touch()
        run.scheduled_sequences.append(self.take_page_sequence())
        run.completion.scheduled()
        run.reservation = await self.buffer.reserve(run.work.index, run.program.page_cap)
        dispatched = await self.dispatcher.fetch(request, run.producer_key)
        run.page_admission = dispatched.admission
        run.settlement = dispatched.settlement
        run.page_state += 1
        return dispatched

    async def abandon_page(self, run: _ReferenceRun) -> None:
        """Return the row capacity of a page whose dispatch raised."""
        self.producer_state.admitting.discard(run.producer_key)
        self.producer_state.touch()
        if run.reservation is not None:
            await self.buffer.abort(run.reservation)
        run.reservation = None

    @property
    def page_dispatch(self) -> PageDispatch:
        """Return the physical dispatch every page of this operation travels by."""
        return PageDispatch.BATCH if isinstance(self.dispatch, KernelBatchDispatch) else PageDispatch.DIRECT

    async def _fetch_page(self, run: _ReferenceRun, request: Request) -> Response:
        """Reserve buffer capacity and dispatch one page; the driver calls this as its fetch hook."""
        try:
            dispatched = await self.dispatch_page(run, request)
        except BaseException as error:
            run.completion.settled(_failed_settlement(error))
            await self.abandon_page(run)
            if isinstance(error, _BatchPageError) or bool(getattr(error, "_b24api_dispatch_started", False)):
                failed = _failed_dispatch(self.page_dispatch, error)
                run.driver.set_page_dispatch(dispatch=failed.dispatch, batch_index=failed.batch_index)
                run.driver.record_unknown_page(
                    dispatch=failed.dispatch,
                    batch_index=failed.batch_index,
                    error=failed.error or error,
                )
            raise
        run.driver.set_page_dispatch(dispatch=dispatched.dispatch, batch_index=dispatched.batch_index)
        run.completion.settled(CommandSettlement.SUCCESS)
        return dispatched.response

    async def _deliver_page(self, run: _ReferenceRun, page: _Page) -> bool:
        """Hand one validated page to the consumer; return False when the caller stops the reference."""
        if run.reservation is None:
            raise RuntimeError("page completed without a buffer reservation")
        if not run.reservation.accepted:
            # A counted head's rows were already charged when it was selected (LeaseCountedRowAccount).
            await self.buffer.accept(run.reservation, page.retained_rows)
        # The physical sender owns capacity only until the decoded page is
        # admitted to the bounded row buffer.  Holding that slot until a
        # READY consumer acknowledges every row would serialize consumer
        # work with the next network request.
        run.admit_page()
        if self.output_order is ReferenceOutputOrder.INPUT:
            run.settle_page()
        acknowledged = asyncio.get_running_loop().create_future()
        # A dense counted tail reports its own pending pages to the dispatcher (ReferenceTailStream).
        if page.continuing and not run.tail_active:
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
        if page.continuing and not run.tail_active:
            self.producer_state.runnable.add(run.producer_key)
            self.producer_state.touch()
        return True

    async def _caller_stopped(self, run: _ReferenceRun, page: _Page, page_records: tuple[PageRecord, ...]) -> bool:
        """Ask the page stop policy about a delivered page; True stops a reference that would continue."""
        page_stop = run.program.page_stop
        if page_stop is None:
            return False
        if len(page_records) != 1:
            raise RuntimeError("validated reference page lacks completion provenance")
        boundary = PageBoundary(
            run.work.index,
            page_records[0],
            tuple(page.items),
            result=result_snapshot(page.response),
        )
        decision = page_stop.on_page(boundary)
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
        if run.failure is not None:
            return run.failure  # a fast keyset binding prepared its own evidence before re-raising
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
            self._caps.pop(event.work.index, None)
            admission.slots.release()
            await self._wake_lease_waiters()
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
            self._caps.pop(head, None)
            admission.slots.release()
            await self._wake_lease_waiters()
            next_head = min(admission.tasks) if admission.tasks else head + 1
            await self.buffer.advance_head(next_head, max(self._caps.values(), default=self._head_cap(next_head)))
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

    async def _consume_page(self, event: _PageEvent) -> AsyncGenerator[ReferenceStreamItem]:
        """Yield one page's rows; acknowledge the page however the consumer leaves it."""
        try:
            reservation = event.reservation
            if event.emitted is None and reservation is None:
                raise RuntimeError("reference page lacks its row reservation")
            if self.emit_response and reservation is not None:
                if event.response is None:
                    raise RuntimeError("fan-out page lacks its response")
                yield _KernelFanOutSuccess(event.work.reference, event.response)
                for weight in event.item_weights:
                    await self.buffer.release(reservation, weight)
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
                if event.emitted is not None:
                    # A fast keyset row stays charged to its binding's lease; its runtime acknowledges pages.
                    event.emitted(weight)
                elif reservation is not None:
                    await self.buffer.release(reservation, weight)
            if event.emitted is None:
                self.completion.binding(event.work.index).delivered()
        finally:
            if not event.acknowledged.done():
                event.acknowledged.set_result(None)

    async def _consume_event(self, event: _Event) -> AsyncGenerator[ReferenceStreamItem]:
        if isinstance(event, _PageEvent):
            async for outcome in self._consume_page(event):
                yield outcome
            return
        if isinstance(event, _DoneEvent):
            _finish_done_completion(self.completion, event)
            if self.emit_complete:
                yield _KernelReferenceComplete(
                    event.work.reference,
                    event.row_count,
                    event.stopped_reason,
                    _done_closure(event),
                    event.keyset_selection,
                    event.keyset_execution,
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
