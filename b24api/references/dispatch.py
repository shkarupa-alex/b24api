"""Bounded fair scheduling for independent and paginated references."""

from __future__ import annotations
import asyncio
import contextlib
from collections.abc import AsyncIterable, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from b24api.batch.engine import BatchExecutor
from b24api.batch.outcome import BatchFailure
from b24api.contracts.policy import ReplayDisposition
from b24api.contracts.report import PageDispatch
from b24api.errors import BudgetExceededError, CapabilityError
from b24api.execution import (
    ExecutionContext,
    Executor,
    WorkClass,
)
from b24api.references.outcome import (
    ReferenceFailure,
    ReferenceItem,
    ReferenceRequest,
)

if TYPE_CHECKING:
    from b24api.contracts.command import NotExecutedReason
    from b24api.contracts.json import FrozenJson, JsonValue
    from b24api.contracts.report import PageRecord, Violation
    from b24api.contracts.request import Request
    from b24api.contracts.response import Response
    from b24api.traversal.plans import (
        BatchDispatch,
        DirectDispatch,
    )

type ReferenceSource = Iterable[ReferenceRequest] | AsyncIterable[ReferenceRequest]
_MISSING = object()
_SYNC_EXHAUSTED = object()


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
    items: tuple[FrozenJson, ...]
    response: Response
    item_weights: tuple[int, ...]
    unique_mask: tuple[bool, ...]
    violations: tuple[Violation, ...]
    page_records: tuple[PageRecord, ...]
    reservation: _Reservation
    acknowledged: asyncio.Future[None]


@dataclass(frozen=True, slots=True)
class _DoneEvent:
    work: _Work
    row_count: int
    violations: tuple[Violation, ...]
    page_records: tuple[PageRecord, ...]
    stopped_reason: str | None = None


@dataclass(frozen=True, slots=True)
class _FailureEvent:
    work: _Work
    error: BaseException
    cursor: JsonValue
    page_state: int
    partial_rows: int
    violations: tuple[Violation, ...]
    page_records: tuple[PageRecord, ...] = ()
    replay_disposition: ReplayDisposition = ReplayDisposition.NOT_ELIGIBLE
    not_executed_reason: NotExecutedReason | None = None


type _Event = _PageEvent | _DoneEvent | _FailureEvent


@dataclass(frozen=True, slots=True)
class _KernelReferenceComplete:
    work_index: int
    reference: ReferenceRequest
    row_count: int
    stopped_reason: str | None = None


@dataclass(frozen=True, slots=True)
class _KernelFanOutSuccess:
    work_index: int
    reference: ReferenceRequest
    response: Response


type ReferenceStreamItem = ReferenceItem | ReferenceFailure | _KernelReferenceComplete | _KernelFanOutSuccess


class _ReferenceWindowError(Exception):
    def __init__(self, failure: ReferenceFailure) -> None:
        self.failure = failure
        super().__init__("reference traversal window failed")
        self.report_cause = failure.error if isinstance(failure.error, BaseException) else self
        if self.report_cause is not self:
            self.__cause__ = self.report_cause


@dataclass(slots=True)
class _PendingBatch:
    request: Request
    reference_id: str
    future: asyncio.Future[_DispatchedPage]
    admitted: asyncio.Future[None]
    settled: asyncio.Future[None]


@dataclass(slots=True)
class _ProducerState:
    """Advisory single-event-loop producer availability for batch coalescing."""

    runnable: set[str]
    indexes: dict[str, int]
    admitting: set[str] = field(default_factory=set)
    pending_continuations: set[str] = field(default_factory=set)
    next_key: str | None = None
    next_index: int | None = None
    source_pull_in_flight: bool = False
    source_terminal: bool = False
    closing: bool = False
    revision: int = 0
    _waiters: list[asyncio.Future[None]] = field(default_factory=list)

    def touch(self) -> None:
        """Synchronously publish a state change to every waiting worker."""
        self.revision += 1
        waiters, self._waiters = self._waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    def changed(self, seen: int) -> asyncio.Future[None]:
        """Return a future resolved after the observed revision changes."""
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        if self.revision != seen or self.closing:
            future.set_result(None)
        else:
            self._waiters.append(future)
        return future


@dataclass(frozen=True, slots=True)
class _DispatchedPage:
    response: Response
    dispatch: PageDispatch
    batch_index: int | None = None
    admission: asyncio.Future[None] | None = None
    settlement: asyncio.Future[None] | None = None


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

    def __init__(
        self,
        maximum: int,
        context: ExecutionContext,
        *,
        head_reserve: int = 0,
        producer_state: _ProducerState | None = None,
    ) -> None:
        self.maximum = maximum
        self.context = context
        self._available = maximum
        self._accounted = 0
        self._head_index = 0
        self._head_reserve = head_reserve
        self._closed = False
        self._condition = asyncio.Condition()
        self._reservations: list[_Reservation] = []
        self._producer_state = producer_state

    async def reserve(self, index: int, amount: int) -> _Reservation:
        if amount > self.maximum:
            raise BudgetExceededError("one decoded page exceeds the global row buffer")
        async with self._condition:
            await self._condition.wait_for(lambda: self._closed or self.can_reserve(index, amount))
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

    def can_reserve(self, index: int, amount: int) -> bool:
        """Return whether a reservation can be admitted immediately."""
        return not self._closed and self._can_reserve(index, amount)

    def _touch(self) -> None:
        if self._producer_state is not None:
            self._producer_state.touch()

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
            self._touch()
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
            self._touch()
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
            self._touch()
        if reservation.accepted:
            await self.context.adjust_buffered_rows(-amount)

    async def advance_head(self, index: int, reserve: int) -> None:
        async with self._condition:
            self._head_index = index
            self._head_reserve = reserve
            self._condition.notify_all()
            self._touch()

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
            self._touch()
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

    async def fetch(self, request: Request, reference_id: str) -> _DispatchedPage:
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
        return _DispatchedPage(response, PageDispatch.DIRECT)

    async def aclose(self) -> None:
        return


class _BatchPageDispatcher:
    def __init__(  # noqa: PLR0913 - dispatcher owns all capacity views used by its advisory predicate
        self,
        executor: Executor,
        context: ExecutionContext,
        plan: BatchDispatch,
        *,
        producer_state: _ProducerState | None = None,
        buffer: _RowBuffer | None = None,
        page_cap: int = 1,
        pending_continuations_can_progress: bool = True,
    ) -> None:
        self.context = context
        self.plan = plan
        self._executor = BatchExecutor(executor)
        self._queue: asyncio.Queue[_PendingBatch] = asyncio.Queue(
            maxsize=context.policy.max_active_references,
        )
        concurrency = min(self.plan.concurrency, self.context.policy.max_active_references)
        self._send_queue: asyncio.Queue[list[_PendingBatch]] = asyncio.Queue(maxsize=concurrency)
        self._send_slots = asyncio.Semaphore(concurrency)
        self._producer_state = producer_state
        self._buffer = buffer
        self._page_cap = page_cap
        self._pending_continuations_can_progress = pending_continuations_can_progress
        self._assembler: asyncio.Task[None] | None = None
        self._worker: asyncio.Task[None] | None = None
        self._workers: tuple[asyncio.Task[None], ...] = ()
        self._settlement_workers: set[asyncio.Task[None]] = set()
        self._closed = False
        self._active_sends = 0
        self._settling_waves = 0
        self.batch_requests = 0
        self.batch_commands = 0

    async def fetch(self, request: Request, reference_id: str) -> _DispatchedPage:  # noqa: C901, PLR0912, PLR0915
        if self._closed:
            raise RuntimeError("batch page dispatcher is closed")
        reservation = await self.context.reserve_page(reference=reference_id)
        future: asyncio.Future[_DispatchedPage] = asyncio.get_running_loop().create_future()
        admitted: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        settled: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        pending = _PendingBatch(request, reference_id, future, admitted, settled)
        remaining = self.context.policy.max_elapsed - self.context.elapsed
        if remaining <= 0:
            self.context.release_page(reservation)
            raise BudgetExceededError("operation elapsed budget exhausted before batch admission")
        try:
            async with asyncio.timeout(remaining):
                await self._queue.put(pending)
                if self._producer_state is not None:
                    self._producer_state.admitting.discard(reference_id)
                self._ensure_workers()
                if self._producer_state is not None:
                    self._producer_state.touch()
                response = await future
                self.context.commit_page(reservation)
                if self._producer_state is not None:
                    self._producer_state.touch()
                return response
        except asyncio.CancelledError:
            future.cancel()
            if not admitted.done():
                admitted.set_result(None)
            if not settled.done():
                settled.set_result(None)
            self.context.release_page(reservation)
            if self._producer_state is not None:
                self._producer_state.touch()
            raise
        except TimeoutError as error:
            future.cancel()
            if not admitted.done():
                admitted.set_result(None)
            if not settled.done():
                settled.set_result(None)
            self.context.release_page(reservation)
            if self._producer_state is not None:
                self._producer_state.touch()
            raise BudgetExceededError("batch scheduler admission exceeded operation time budget") from error
        except BaseException:
            if not admitted.done():
                admitted.set_result(None)
            if not settled.done():
                settled.set_result(None)
            self.context.release_page(reservation)
            if self._producer_state is not None:
                self._producer_state.touch()
            raise

    def _ensure_workers(self) -> None:
        if self._assembler is not None:
            return
        concurrency = min(self.plan.concurrency, self.context.policy.max_active_references)
        self._assembler = asyncio.create_task(self._run())
        senders = tuple(asyncio.create_task(self._send_run()) for _index in range(concurrency))
        self._worker = senders[0]
        self._workers = (self._assembler, *senders)

    async def _run(self) -> None:  # noqa: C901, PLR0912, PLR0915
        get_task: asyncio.Task[_PendingBatch] | None = None
        slot_acquired = False
        try:
            while not self._closed:
                await self._send_slots.acquire()
                slot_acquired = True
                if get_task is None:
                    first = await self._queue.get()
                else:
                    first = await get_task
                    get_task = None
                if first.future.done():
                    self._send_slots.release()
                    slot_acquired = False
                    continue
                chunk = [first]
                self._drain_nowait(chunk)
                loop = asyncio.get_running_loop()
                deadline = loop.time() + self.plan.coalesce_wait
                coalescing_delay = 0.0
                while len(chunk) < self.plan.batch_size:
                    self._drain_nowait(chunk)
                    if len(chunk) >= self.plan.batch_size:
                        break
                    if self.plan.coalesce_wait <= 0:
                        break
                    if self._potential({item.reference_id for item in chunk}) == 0:
                        break
                    remaining = min(
                        deadline - loop.time(),
                        self.context.policy.max_elapsed - self.context.elapsed,
                    )
                    if remaining <= 0:
                        break
                    state = self._producer_state
                    if state is None:
                        break
                    seen = state.revision
                    if get_task is None:
                        get_task = asyncio.create_task(self._queue.get())
                    wake = state.changed(seen)
                    wait_started = loop.time()
                    await asyncio.wait(
                        [cast("asyncio.Future[object]", get_task), cast("asyncio.Future[object]", wake)],
                        timeout=remaining,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    coalescing_delay += loop.time() - wait_started
                    if get_task.done():
                        pending = get_task.result()
                        get_task = None
                        if not pending.future.done():
                            chunk.append(pending)
                        self._drain_nowait(chunk)
                self._observe_wave(len(chunk), coalescing_delay)
                await self._send_queue.put(chunk)
                slot_acquired = False
        finally:
            if slot_acquired:
                self._send_slots.release()
            if get_task is not None and not get_task.done():
                get_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await get_task

    def _drain_nowait(self, chunk: list[_PendingBatch]) -> None:
        while len(chunk) < self.plan.batch_size:
            try:
                pending = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            if not pending.future.done():
                chunk.append(pending)

    def _observe_wave(self, command_count: int, coalescing_delay: float) -> None:
        """Test/benchmark seam for per-wave coalescing-cost accounting."""
        del command_count, coalescing_delay

    def _potential(self, chunk_keys: set[str]) -> int:
        state, buffer = self._producer_state, self._buffer
        if state is None or buffer is None or state.closing or self._queue.qsize() >= self._queue.maxsize:
            return 0
        admitted = sum(
            key not in chunk_keys
            and self.context.can_reserve_page(reference=key)
            and buffer.can_reserve(state.indexes[key], self._page_cap)
            for key in state.runnable
            if key in state.indexes
        )
        admitting = sum(
            key not in chunk_keys
            and self.context.can_reserve_page(reference=key)
            and buffer.can_reserve(state.indexes[key], self._page_cap)
            for key in state.admitting
            if self._pending_continuations_can_progress and key in state.indexes
        )
        continuations = sum(
            key not in chunk_keys
            and self.context.can_reserve_page(reference=key)
            and buffer.can_reserve(state.indexes[key], self._page_cap)
            for key in state.pending_continuations
            if self._pending_continuations_can_progress and key in state.indexes
        )
        pending_pull = int(
            self._pending_continuations_can_progress
            and state.next_key is not None
            and state.next_index is not None
            and state.next_key not in chunk_keys
            and state.source_pull_in_flight
            and self.context.can_reserve_page(reference=state.next_key)
            and buffer.can_reserve(state.next_index, self._page_cap),
        )
        settling_admission = sum(
            key not in chunk_keys
            and key in state.indexes
            and not (
                self.context.can_reserve_page(reference=key) and buffer.can_reserve(state.indexes[key], self._page_cap)
            )
            for key in state.admitting
            if self._pending_continuations_can_progress and (self._active_sends > 0 or self._settling_waves > 0)
        )
        return admitted + admitting + continuations + pending_pull + settling_admission

    async def _send(self, chunk: list[_PendingBatch]) -> None:
        self.batch_requests += 1
        self.batch_commands += len(chunk)
        try:
            outcomes = await self._executor.execute_requests(
                tuple(item.request for item in chunk),
                context=self.context,
            )
        except asyncio.CancelledError:
            for item in chunk:
                item.future.cancel()
            raise
        except Exception as error:  # noqa: BLE001 - total chunk correlation boundary
            for item in chunk:
                if not item.future.done():
                    item.future.set_exception(error)
            return
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
            item.future.set_result(
                _DispatchedPage(
                    success.response,
                    PageDispatch.BATCH,
                    success.command_index,
                    item.admitted,
                    item.settled,
                ),
            )

    async def _wait_for_settlement(self, chunk: list[_PendingBatch]) -> None:
        try:
            await asyncio.gather(*(item.settled for item in chunk))
        finally:
            self._settling_waves -= 1
            if self._producer_state is not None:
                self._producer_state.touch()

    async def _send_run(self) -> None:
        while not self._closed:
            chunk = await self._send_queue.get()
            self._active_sends += 1
            if self._producer_state is not None:
                self._producer_state.touch()
            try:
                await self._send(chunk)
                self._settling_waves += 1
                settlement_worker = asyncio.create_task(self._wait_for_settlement(chunk))
                self._settlement_workers.add(settlement_worker)
                settlement_worker.add_done_callback(self._settlement_workers.discard)
                await asyncio.gather(*(item.admitted for item in chunk))
            finally:
                self._active_sends -= 1
                if self._producer_state is not None:
                    self._producer_state.touch()
                self._send_slots.release()

    async def aclose(self) -> None:  # noqa: C901, PLR0912 - closes every independently owned batch worker
        if self._closed:
            return
        self._closed = True
        if self._producer_state is not None:
            self._producer_state.closing = True
            self._producer_state.touch()
        if self._assembler is None:
            return
        owned_workers = (*self._workers, *self._settlement_workers)
        for worker in owned_workers:
            if not worker.done():
                worker.cancel()
        try:
            await asyncio.sleep(0)
            active = tuple(worker for worker in owned_workers if not worker.done())
            if active:
                remaining = max(0.0, self.context.policy.max_elapsed - self.context.elapsed)
                _, pending_workers = await asyncio.wait(active, timeout=remaining)
                if pending_workers:
                    raise BudgetExceededError("batch dispatcher cleanup exceeded operation time budget")
            for worker in owned_workers:
                with contextlib.suppress(asyncio.CancelledError):
                    await worker
        finally:
            while True:
                try:
                    chunk = self._send_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                for item in chunk:
                    if not item.future.done():
                        item.future.cancel()
                    if not item.admitted.done():
                        item.admitted.set_result(None)
                    if not item.settled.done():
                        item.settled.set_result(None)
            while True:
                try:
                    pending = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if not pending.future.done():
                    pending.future.cancel()
                if not pending.admitted.done():
                    pending.admitted.set_result(None)
                if not pending.settled.done():
                    pending.settled.set_result(None)
