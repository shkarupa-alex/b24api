"""Bounded fair scheduling for independent and paginated references."""

from __future__ import annotations
import asyncio
import contextlib
from collections import Counter, deque
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from b24api.batch.engine import BatchExecutor
from b24api.batch.outcome import BatchFailure, BatchSuccess
from b24api.contracts.policy import ReplayDisposition
from b24api.contracts.report import PageDispatch
from b24api.errors import B24ApiError, BudgetExceededError, CapabilityError
from b24api.execution import (
    ExecutionContext,
    Executor,
    WorkClass,
)
from b24api.execution.facts import Execution, execution, record_dispatch
from b24api.references.outcome import (
    KernelReferenceFailure,
    KernelReferenceItem,
    ReferenceRequest,
)

if TYPE_CHECKING:
    from b24api.batch.outcome import BatchOutcome
    from b24api.contracts.command import NotExecutedReason
    from b24api.contracts.completion import BindingClosure, CommandSettlement
    from b24api.contracts.json import FrozenJson, JsonValue
    from b24api.contracts.report import KeysetExecutionReport, KeysetSelectionSummary, PageRecord, Violation
    from b24api.contracts.request import Request
    from b24api.contracts.response import Response
    from b24api.execution.context import PageReservation
    from b24api.traversal.plans import (
        KernelBatchDispatch,
        KernelDirectDispatch,
    )
    from b24api.traversal.values import IdentityValue

type ReferenceRequestSource = Iterable[ReferenceRequest] | AsyncIterable[ReferenceRequest]
type _WaitFirst = Callable[[tuple[asyncio.Future[object], ...], float], Awaitable[None]]
_MISSING = object()


def _loop_time() -> float:
    return asyncio.get_running_loop().time()


async def _wait_first(futures: tuple[asyncio.Future[object], ...], seconds: float) -> None:
    await asyncio.wait(futures, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)


@dataclass(frozen=True, slots=True)
class _Work:
    index: int
    reference: ReferenceRequest


@dataclass(slots=True)
class _Reservation:
    index: int
    amount: int
    accepted: bool = False
    # A fast keyset lease: held from activation to terminal, only ever trimmed, charged by its own budget view.
    lease: bool = False
    # A lease whose binding has chosen its plan: it keeps its size, so it returns rows only at terminal.
    settled: bool = False


@dataclass(slots=True)
class _PageEvent:
    work: _Work
    items: tuple[FrozenJson, ...]
    response: Response | None
    item_weights: tuple[int, ...]
    unique_mask: tuple[bool, ...]
    violations: tuple[Violation, ...]
    page_records: tuple[PageRecord, ...]
    reservation: _Reservation | None
    acknowledged: asyncio.Future[None]
    # A fast keyset row group is held by its binding's lease, not a page reservation: each delivered
    # row is reported to the binding's runtime instead of being released to the shared pool.
    emitted: Callable[[int], None] | None = None


@dataclass(frozen=True, slots=True)
class _DoneEvent:
    work: _Work
    row_count: int
    violations: tuple[Violation, ...]
    page_records: tuple[PageRecord, ...]
    stopped_reason: str | None = None
    terminal_reason: str | None = None
    qualified_total: int | None = None
    declared_short_page_width: int | None = None
    closure: BindingClosure | None = None
    qualified_witnesses: int | None = None
    keyset_selection: KeysetSelectionSummary | None = None
    keyset_execution: KeysetExecutionReport | None = None


@dataclass(frozen=True, slots=True)
class _FailureEvent:
    work: _Work
    error: BaseException
    cursor: JsonValue | IdentityValue
    page_state: int
    partial_rows: int
    violations: tuple[Violation, ...]
    page_records: tuple[PageRecord, ...] = ()
    replay_disposition: ReplayDisposition = ReplayDisposition.NOT_ELIGIBLE
    not_executed_reason: NotExecutedReason | None = None


type _Event = _PageEvent | _DoneEvent | _FailureEvent


@dataclass(frozen=True, slots=True)
class _KernelReferenceComplete:
    reference: ReferenceRequest
    row_count: int
    stopped_reason: str | None = None
    closure: BindingClosure | None = None
    keyset_selection: KeysetSelectionSummary | None = None
    keyset_execution: KeysetExecutionReport | None = None


@dataclass(frozen=True, slots=True)
class _KernelFanOutSuccess:
    reference: ReferenceRequest
    response: Response


type ReferenceStreamItem = (
    KernelReferenceItem | KernelReferenceFailure | _KernelReferenceComplete | _KernelFanOutSuccess
)


class _ReferenceWindowError(Exception):
    def __init__(self, failure: KernelReferenceFailure) -> None:
        self.failure = failure
        self.replay_disposition = failure.replay_disposition
        super().__init__("reference traversal window failed")
        self.report_cause = failure.error if isinstance(failure.error, BaseException) else self
        if self.report_cause is not self:
            self.__cause__ = self.report_cause


@dataclass(frozen=True, slots=True)
class _DecodeOptions:
    """Envelope decoding options; one physical batch carries pages with equal options only."""

    advisory_totals: bool = False
    strict_envelope: bool = False
    strict_json_members: bool = False


_PLAIN_DECODING = _DecodeOptions()


@dataclass(slots=True)
class _PendingBatch:
    request: Request
    reference_id: str
    future: asyncio.Future[_DispatchedPage]
    admitted: asyncio.Future[None]
    settled: asyncio.Future[None]
    dispatch_started: bool = False
    options: _DecodeOptions = _PLAIN_DECODING
    # The operation deadline of the page's admission; the send of its wave lifts it.
    admission: asyncio.Timeout | None = None

    def mark_sent(self) -> bool:
        """Hand the page to its wave's send, lifting its admission deadline; ``False`` when it was abandoned.

        A page whose fetch has ended (every way out of the fetch completes or cancels its future) or whose deadline
        has expired is abandoned: no one waits for it, so it is not sent and stays proven not executed. The check
        and the hand-over happen in one step of the event loop, so the fetch cannot end in between.
        """
        admission = self.admission
        if self.future.done() or (admission is not None and admission.expired()):
            return False
        if admission is not None:
            admission.reschedule(None)
        self.dispatch_started = True
        return True


@dataclass(slots=True)
class _ProducerState:
    """Advisory single-event-loop producer availability for batch coalescing."""

    runnable: set[str]
    indexes: dict[str, int]
    caps: dict[str, int] = field(default_factory=dict)
    admitting: set[str] = field(default_factory=set)
    pending_continuations: set[str] = field(default_factory=set)
    # Windowed producers (a counted tail) holding their next page while it waits for nothing the
    # dispatcher must send first: its consumer freeing a window slot, or its own row reservation.
    # Unlike a pending continuation, their pages already in a chunk do not exclude them. Each maps
    # to the reservation its consumer returns to the pool before that page reserves its own, if any.
    pending_refills: dict[str, _Reservation | None] = field(default_factory=dict)
    # Pages whose rows and dispatch are committed but which have not reached the queue yet: a
    # producer that leaves ``runnable`` right after starting their tasks still has them on the way.
    # ``reserving`` holds those that must still reserve their logical page, ``in_transit`` the rest.
    reserving: Counter[str] = field(default_factory=Counter)
    in_transit: Counter[str] = field(default_factory=Counter)
    next_key: str | None = None
    next_index: int | None = None
    source_pull_in_flight: bool = False
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

    def depart(self, key: str, *, page_reserved: bool) -> _Transit:
        """Count one page on its way to the queue until ``fetch`` queues it or its task ends."""
        (self.in_transit if page_reserved else self.reserving)[key] += 1
        self.touch()
        return _Transit(self, key, page_reserved=page_reserved)

    def changed(self, seen: int) -> asyncio.Future[None]:
        """Return a future resolved after the observed revision changes."""
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        if self.revision != seen or self.closing:
            future.set_result(None)
        else:
            self._waiters.append(future)
        return future


@dataclass(slots=True, eq=False)
class _Transit:
    """One in-transit page's arrival: counted down exactly once, when queued or when its task ends."""

    state: _ProducerState
    key: str
    page_reserved: bool
    arrived: bool = False

    def carried_by(self, task: asyncio.Task[_DispatchedPage]) -> None:
        """End the transit when ``task`` ends, also if it is cancelled before it first runs."""
        task.add_done_callback(self.arrive)

    def reserved(self) -> None:
        """Record that the page holds its logical page reservation; it no longer depends on the budget."""
        if self.arrived or self.page_reserved:
            return
        self.page_reserved = True
        _count_down(self.state.reserving, self.key)
        self.state.in_transit[self.key] += 1
        self.state.touch()

    def arrive(self, _task: object = None) -> None:
        """Count the page down once: it reached the queue, or its task ended without queuing it."""
        if self.arrived:
            return
        self.arrived = True
        _count_down(self.state.in_transit if self.page_reserved else self.state.reserving, self.key)
        self.state.touch()


def _count_down(counter: Counter[str], key: str) -> None:
    remaining = counter[key] - 1
    if remaining > 0:
        counter[key] = remaining
    else:
        del counter[key]


@dataclass(frozen=True, slots=True)
class _DispatchedPage:
    response: Response
    dispatch: PageDispatch
    batch_index: int | None = None
    admission: asyncio.Future[None] | None = None
    settlement: asyncio.Future[None] | None = None
    outcome: BatchSuccess | None = field(default=None, repr=False)


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


def _failed_execution(error: BaseException) -> Execution:
    """Return what the facts say about a page whose dispatch raised, a batch page's from its command outcome."""
    return execution(error.failure.error if isinstance(error, _BatchPageError) else error)


def _failed_settlement(error: BaseException) -> CommandSettlement:
    """Settle a page whose dispatch raised from its execution facts: possibly run, failed, or never sent."""
    return _failed_execution(error).settlement


@dataclass(frozen=True, slots=True)
class _FailedDispatch:
    dispatch: PageDispatch
    batch_index: int | None
    error: BaseException | None


def _failed_dispatch(dispatch: PageDispatch, error: BaseException) -> _FailedDispatch:
    """Return the dispatch provenance and the reportable error of a page whose dispatch raised."""
    if isinstance(error, _BatchPageError):
        cause = error.failure.error
        return _FailedDispatch(
            dispatch, error.failure.command_index, cause if isinstance(cause, BaseException) else None
        )
    return _FailedDispatch(dispatch, None, None)


class _RowBuffer:
    """Reserve page capacity before I/O and account decoded retained rows exactly."""

    def __init__(
        self,
        maximum: int,
        context: ExecutionContext,
        *,
        head_reserve: int = 0,
        producer_state: _ProducerState | None = None,
        ordered: bool = False,
    ) -> None:
        self.maximum = maximum
        self.context = context
        self._available = maximum
        self._head_index = 0
        self._head_reserve = head_reserve
        self._closed = False
        self._condition = asyncio.Condition()
        self._reservations: list[_Reservation] = []
        self._producer_state = producer_state
        # INPUT delivery: rows go to bindings in binding order, so no binding takes rows while an
        # earlier one, which becomes the frontier first, waits for its lease.
        self._ordered = ordered
        self._lease_waiting: set[int] = set()

    @property
    def head_index(self) -> int:
        """Return the INPUT frontier whose page capacity is held in reserve."""
        return self._head_index

    @property
    def head_reserve(self) -> int:
        """Return the rows kept free for the next page of whichever admitted binding becomes the INPUT frontier."""
        return self._head_reserve

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

    def _can_reserve(self, index: int, amount: int, returning: int = 0) -> bool:
        available = self._available + returning
        if available < amount:
            return False
        # A page charged to a fast keyset lease reserves no pool rows: it cannot take the frontier's reserve.
        if amount == 0:
            return True
        if self._behind_lease_waiter(index):
            return False
        if self._head_reserve == 0 or index == self._head_index:
            return True
        # Rows the frontier holds come back to it; only the rest of its reserve must stay free. A
        # frontier page accepted short returns its unused rows, and those stay the frontier's: if
        # another binding took them, rows held behind the frontier could never be released.
        head_held = sum(
            reservation.amount for reservation in self._reservations if reservation.index == self._head_index
        )
        return available - amount >= self._head_reserve - head_held

    def can_reserve(self, index: int, amount: int, *, returning: int = 0) -> bool:
        """Return whether a reservation can be admitted immediately, or once ``returning`` rows come back."""
        return not self._closed and self._can_reserve(index, amount, returning)

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
            self._reservations.remove(reservation)
            self._condition.notify_all()
            self._touch()
        if reservation.accepted:
            await self.context.adjust_buffered_rows(-amount)

    async def lease(self, index: int, grant: Callable[[int], int | None]) -> _Reservation:
        """Hold one capacity until terminal, never grown; ``grant`` sizes it from the free rows or asks to wait.

        ``grant`` runs under the pool condition, so the decision and the reservation are atomic. It
        may raise to refuse a lease no releaser can ever satisfy. An ordered pool asks it only once no
        earlier binding waits for a lease. A binding leaving the wait, with a lease or without one,
        wakes the others: whether it still waits decides whether a later binding may take rows.
        """
        async with self._condition:
            self._lease_waiting.add(index)
            try:
                while True:
                    if self._closed:
                        raise asyncio.CancelledError
                    amount = None if self._behind_lease_waiter(index) else grant(self._available)
                    if amount is not None:
                        break
                    await self._condition.wait()
                if isinstance(amount, bool) or not 0 < amount <= self._available:
                    raise RuntimeError("fast keyset lease exceeds the free row pool")
                self._available -= amount
                reservation = _Reservation(index, amount, lease=True)
                self._reservations.append(reservation)
            finally:
                self._lease_waiting.discard(index)
                self._condition.notify_all()
                self._touch()
            return reservation

    def _behind_lease_waiter(self, index: int) -> bool:
        """Return whether an ordered pool holds rows back from ``index`` for an earlier lease waiter."""
        return self._ordered and any(waiting < index for waiting in self._lease_waiting)

    async def settle_lease(self, reservation: _Reservation, amount: int) -> None:
        """Fix a planned binding's lease at ``amount`` rows, give the rest back and wake the capacity waiters.

        Until its binding has planned, a lease may still shrink, so a waiting INPUT frontier counts it
        as releasable; settling it, trimmed or not, is the fact that frontier waits for. A lease is never
        accepted, so the shared row counter holds only what its binding charged; trimming moves free
        pool rows only. A pool closed meanwhile has already reclaimed the lease.
        """
        if not reservation.lease or amount <= 0:
            raise RuntimeError("only a lease settles, and never to nothing")
        async with self._condition:
            if self._closed or reservation.amount == 0:
                return
            if amount > reservation.amount:
                raise RuntimeError("a lease cannot grow")
            self._available += reservation.amount - amount
            reservation.amount = amount
            reservation.settled = True
            self._condition.notify_all()
            self._touch()

    def releasable_elsewhere(self, index: int) -> bool:
        """Return whether another binding may still return capacity without this one's progress.

        An unaccepted page returns its unused rows when it settles, and an unsettled lease may be trimmed
        once its binding has planned, which needs no delivery.
        """
        return any(
            reservation.index != index
            and not reservation.accepted
            and (not reservation.lease or not reservation.settled)
            for reservation in self._reservations
        )

    async def poke(self) -> None:
        """Wake capacity waiters after a scheduling fact a lease decision depends on has changed."""
        async with self._condition:
            self._condition.notify_all()

    def raise_head_reserve(self, reserve: int) -> None:
        """Reserve more rows for the INPUT frontier, synchronously: a stricter reserve wakes no waiter."""
        self._head_reserve = max(self._head_reserve, min(reserve, self.maximum))

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
        plan: KernelDirectDispatch,
    ) -> None:
        self.executor = executor
        self.context = context
        concurrency = min(plan.concurrency, context.policy.max_direct_concurrency)
        self._semaphore = asyncio.Semaphore(concurrency)
        self.batch_requests = 0
        self.batch_commands = 0

    async def fetch(self, request: Request, reference_id: str, *, transit: _Transit | None = None) -> _DispatchedPage:
        # A direct page has no queue to coalesce in: nothing waits for it to arrive.
        if transit is not None:
            transit.arrive()
        reservation = await self.context.reserve_page(reference=reference_id)
        remaining = self.context.policy.max_elapsed - self.context.elapsed
        if remaining <= 0:
            self.context.release_page(reservation)
            raise BudgetExceededError("operation elapsed budget exhausted before direct admission")
        try:
            # The deadline bounds admission only: once sent, the executor's own budgets end the request and
            # record whether it may have run, which a deadline raised around the send would discard.
            async with asyncio.timeout(remaining):
                await self._semaphore.acquire()
        except TimeoutError as error:
            self.context.release_page(reservation)
            raise BudgetExceededError("direct scheduler admission exceeded operation time budget") from error
        except BaseException:
            self.context.release_page(reservation)
            raise
        try:
            response = await self.executor.execute(request, context=self.context, work_class=WorkClass.TRAVERSAL_DIRECT)
            self.context.commit_page(reservation)
        except BaseException:
            self.context.release_page(reservation)
            raise
        finally:
            self._semaphore.release()
        return _DispatchedPage(response, PageDispatch.DIRECT)

    async def aclose(self) -> None:
        return


class _BatchPageDispatcher:
    def __init__(  # noqa: PLR0913 - dispatcher owns all capacity views used by its advisory predicate
        self,
        executor: Executor,
        context: ExecutionContext,
        plan: KernelBatchDispatch,
        *,
        producer_state: _ProducerState | None = None,
        buffer: _RowBuffer | None = None,
        page_cap: int = 1,
        pending_continuations_can_progress: bool = True,
        time: Callable[[], float] | None = None,
        wait: _WaitFirst | None = None,
    ) -> None:
        self.context = context
        self.plan = plan
        # Internal clock and wait seams (not public API): tests drive coalescing on a virtual clock.
        self._time = time or _loop_time
        self._wait = wait or _wait_first
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
        # Pages drained from the queue wait here per producer, in first-arrival order, so one producer's
        # queued tail cannot keep a later producer's page out of the next wave (round-robin fairness).
        self._backlog: dict[str, deque[_PendingBatch]] = {}
        # The assembler's outstanding coalescing read of the queue. A page it has taken belongs in the
        # backlog before any chunk forms, or a backlog that never empties would keep it out of every wave.
        self._getter: asyncio.Task[_PendingBatch] | None = None
        self._pending_continuations_can_progress = pending_continuations_can_progress
        self._assembler: asyncio.Task[None] | None = None
        self._workers: tuple[asyncio.Task[None], ...] = ()
        self._settlement_workers: set[asyncio.Task[None]] = set()
        self._closed = False
        self._active_sends = 0
        self._settling_waves = 0
        self.batch_requests = 0
        self.batch_commands = 0

    async def fetch(  # noqa: C901
        self,
        request: Request,
        reference_id: str,
        *,
        options: _DecodeOptions = _PLAIN_DECODING,
        page_reserved: bool = False,
        transit: _Transit | None = None,
    ) -> _DispatchedPage:
        """Queue one page; a fast keyset wave has already reserved its logical pages (``page_reserved``).

        ``transit`` is the page's in-transit count, taken when its task started; queuing the page ends it.
        """
        if self._closed:
            raise RuntimeError("batch page dispatcher is closed")
        reservation = None if page_reserved else await self.context.reserve_page(reference=reference_id)
        if transit is not None:
            transit.reserved()
        future: asyncio.Future[_DispatchedPage] = asyncio.get_running_loop().create_future()
        admitted: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        settled: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        pending = _PendingBatch(request, reference_id, future, admitted, settled, options=options)
        remaining = self.context.policy.max_elapsed - self.context.elapsed
        if remaining <= 0:
            if reservation is not None:
                self.context.release_page(reservation)
            raise BudgetExceededError("operation elapsed budget exhausted before batch admission")
        try:
            # The deadline bounds admission only: once the wave is sent, the executor's own budgets end it and
            # record whether it may have run, as they do for every other send.
            async with asyncio.timeout(remaining) as admission:
                pending.admission = admission
                await self._queue_page(pending, transit)
                response = await future
        except asyncio.CancelledError as error:
            future.cancel()
            self._abandon(pending, reservation)
            if pending.dispatch_started:
                # A page cancelled after its wave was sent may have executed; settlement must say so.
                record_dispatch(error, possibly_executed=True)
            raise
        except TimeoutError as error:
            # A page whose wave was sent no longer has a deadline: this one was never sent.
            future.cancel()
            self._abandon(pending, reservation)
            raise BudgetExceededError("batch scheduler admission exceeded operation time budget") from error
        except BaseException:
            future.cancel()
            self._abandon(pending, reservation)
            raise
        if reservation is not None:
            self.context.commit_page(reservation)
        if self._producer_state is not None:
            self._producer_state.touch()
        return response

    async def _queue_page(self, pending: _PendingBatch, transit: _Transit | None) -> None:
        """Queue a page for the next wave; it has arrived, and its producer is no longer admitting it."""
        await self._queue.put(pending)
        if transit is not None:
            transit.arrive()
        if self._producer_state is not None:
            self._producer_state.admitting.discard(pending.reference_id)
        self._ensure_workers()
        if self._producer_state is not None:
            self._producer_state.touch()

    def _abandon(self, pending: _PendingBatch, reservation: PageReservation | None) -> None:
        """Release what a page that will not be returned holds: its wave barriers and its page reservation."""
        if not pending.admitted.done():
            pending.admitted.set_result(None)
        if not pending.settled.done():
            pending.settled.set_result(None)
        if reservation is not None:
            self.context.release_page(reservation)
        if self._producer_state is not None:
            self._producer_state.touch()

    def _ensure_workers(self) -> None:
        if self._assembler is not None:
            return
        concurrency = min(self.plan.concurrency, self.context.policy.max_active_references)
        self._assembler = asyncio.create_task(self._run())
        senders = tuple(asyncio.create_task(self._send_run()) for _index in range(concurrency))
        self._workers = (self._assembler, *senders)

    async def _run(self) -> None:  # noqa: C901, PLR0912, PLR0915
        slot_acquired = False
        try:
            while not self._closed:
                await self._send_slots.acquire()
                slot_acquired = True
                self._collect_getter()
                # Pages left in the backlog by a full chunk form the next one without a new arrival:
                # their producers may all be waiting on these very pages.
                if not self._backlog_pending():
                    if self._getter is None:
                        first = await self._queue.get()
                    else:
                        first = await self._getter
                        self._getter = None
                    self._enqueue(first)
                chunk: list[_PendingBatch] = []
                self._drain_nowait(chunk)
                if not chunk:
                    self._send_slots.release()
                    slot_acquired = False
                    continue
                deadline = self._time() + self.plan.coalesce_wait
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
                        deadline - self._time(),
                        self.context.policy.max_elapsed - self.context.elapsed,
                    )
                    if remaining <= 0:
                        break
                    state = self._producer_state
                    if state is None:
                        break
                    seen = state.revision
                    if self._getter is None:
                        self._getter = asyncio.create_task(self._queue.get())
                    wake = state.changed(seen)
                    wait_started = self._time()
                    await self._wait(
                        (cast("asyncio.Future[object]", self._getter), cast("asyncio.Future[object]", wake)),
                        remaining,
                    )
                    coalescing_delay += self._time() - wait_started
                self._observe_wave(len(chunk), coalescing_delay)
                await self._send_queue.put(chunk)
                slot_acquired = False
        finally:
            if slot_acquired:
                self._send_slots.release()
            getter = self._getter
            if getter is not None and not getter.done():
                getter.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await getter
            # A page the read took before it was cancelled stays reachable for cleanup to cancel.
            self._collect_getter()

    def _collect_getter(self) -> None:
        """Move a page the coalescing read has already taken from the queue into the backlog."""
        getter = self._getter
        if getter is None or not getter.done():
            return
        self._getter = None
        if not getter.cancelled():
            self._enqueue(getter.result())

    def _enqueue(self, pending: _PendingBatch) -> None:
        if not pending.future.done():
            self._backlog.setdefault(pending.reference_id, deque()).append(pending)

    def _drain_nowait(self, chunk: list[_PendingBatch]) -> None:
        """Move every queued page to the backlog, then fill the chunk round-robin across producers."""
        # The coalescing read took its page before anything still queued arrived: arrival order holds.
        self._collect_getter()
        while True:
            try:
                self._enqueue(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        while len(chunk) < self.plan.batch_size and self._backlog:
            progressed = False
            for key in tuple(self._backlog):
                waiting = self._backlog[key]
                while waiting and waiting[0].future.done():
                    waiting.popleft()
                # One envelope is decoded with one set of options; a page with other options waits.
                if (
                    waiting
                    and len(chunk) < self.plan.batch_size
                    and (not chunk or waiting[0].options == chunk[0].options)
                ):
                    chunk.append(waiting.popleft())
                    progressed = True
                if not waiting:
                    del self._backlog[key]
            if not progressed:
                return

    def _backlog_pending(self) -> bool:
        return any(not pending.future.done() for waiting in self._backlog.values() for pending in waiting)

    def _pending_count(self) -> int:
        return self._queue.qsize() + sum(len(waiting) for waiting in self._backlog.values())

    def _observe_wave(self, command_count: int, coalescing_delay: float) -> None:
        """Test/benchmark seam for per-wave coalescing-cost accounting."""
        del command_count, coalescing_delay

    def _potential(self, chunk_keys: set[str]) -> int:
        state, buffer = self._producer_state, self._buffer
        if state is None or buffer is None or state.closing or self._pending_count() >= self._queue.maxsize:
            return 0
        admitted = sum(
            key not in chunk_keys
            and self.context.can_reserve_page(reference=key)
            and buffer.can_reserve(state.indexes[key], state.caps.get(key, self._page_cap))
            for key in state.runnable
            if key in state.indexes
        )
        admitting = sum(
            key not in chunk_keys
            and self.context.can_reserve_page(reference=key)
            and buffer.can_reserve(state.indexes[key], state.caps.get(key, self._page_cap))
            for key in state.admitting
            if self._pending_continuations_can_progress and key in state.indexes
        )
        continuations = sum(
            key not in chunk_keys
            and self.context.can_reserve_page(reference=key)
            and buffer.can_reserve(state.indexes[key], state.caps.get(key, self._page_cap))
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
                self.context.can_reserve_page(reference=key)
                and buffer.can_reserve(state.indexes[key], state.caps.get(key, self._page_cap))
            )
            for key in state.admitting
            if self._pending_continuations_can_progress and (self._active_sends > 0 or self._settling_waves > 0)
        )
        # Under INPUT delivery only the frontier's consumer progresses whatever the chunk carries.
        refills = sum(
            (self._pending_continuations_can_progress or state.indexes[key] == buffer.head_index)
            and self.context.can_reserve_page(reference=key)
            and buffer.can_reserve(
                state.indexes[key],
                state.caps.get(key, self._page_cap),
                returning=0 if returned is None else returned.amount,
            )
            for key, returned in state.pending_refills.items()
            if key in state.indexes
        )
        # A page on its way to the queue arrives without any consumer progress, under either order;
        # one that must still reserve its logical page arrives only if the page budget admits it.
        in_transit = sum(count for key, count in state.in_transit.items() if key in state.indexes) + sum(
            count
            for key, count in state.reserving.items()
            if key in state.indexes and self.context.can_reserve_page(reference=key)
        )
        return admitted + admitting + continuations + refills + pending_pull + settling_admission + in_transit

    def _count_batch_request(self) -> None:
        """Count one physical batch envelope the chunk sends in its first round."""
        self.batch_requests += 1

    async def _send(self, chunk: list[_PendingBatch]) -> None:
        requests = tuple(item.request for item in chunk)
        self.batch_commands += len(chunk)
        options = chunk[0].options
        try:
            outcomes = await self._executor.execute_requests(
                requests,
                context=self.context,
                advisory_totals=options.advisory_totals,
                strict_envelope=options.strict_envelope,
                strict_json_members=options.strict_json_members,
                on_envelope=self._count_batch_request,
            )
            self._correlate(chunk, outcomes)
        except asyncio.CancelledError:
            for item in chunk:
                item.future.cancel()
            raise
        except Exception as error:  # noqa: BLE001 - total chunk correlation boundary
            if not isinstance(error, B24ApiError):
                # A failure the engine did not classify may come after the wave left: a sent page no longer has a
                # deadline to end its wait, so it fails with this error as possibly run.
                record_dispatch(error, possibly_executed=True)
            for item in chunk:
                if not item.future.done():
                    item.future.set_exception(error)

    @staticmethod
    def _correlate(chunk: list[_PendingBatch], outcomes: tuple[BatchOutcome, ...]) -> None:
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
                    success,
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
                # Only pages whose fetch still waits are sent; an abandoned one was never sent and settles so.
                live = [item for item in chunk if item.mark_sent()]
                if live:
                    await self._send(live)
                    self._settling_waves += 1
                    settlement_worker = asyncio.create_task(self._wait_for_settlement(live))
                    self._settlement_workers.add(settlement_worker)
                    settlement_worker.add_done_callback(self._settlement_workers.discard)
                    await asyncio.gather(*(item.admitted for item in live))
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
                    self._enqueue(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            for waiting in self._backlog.values():
                for pending in waiting:
                    if not pending.future.done():
                        pending.future.cancel()
                    if not pending.admitted.done():
                        pending.admitted.set_result(None)
                    if not pending.settled.done():
                        pending.settled.set_result(None)
            self._backlog.clear()
