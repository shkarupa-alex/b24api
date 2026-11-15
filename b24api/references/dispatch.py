"""Bounded fair scheduling for independent and paginated references."""

from __future__ import annotations
import asyncio
import contextlib
from bisect import bisect_left, insort
from collections import Counter, deque
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from enum import Enum
from itertools import accumulate
from typing import TYPE_CHECKING, cast

from b24api.batch.engine import BatchExecutor
from b24api.batch.outcome import BatchFailure, BatchSuccess
from b24api.contracts.policy import ReplayDisposition
from b24api.contracts.report import PageDispatch
from b24api.errors import B24ApiError, BudgetExceededError, CapabilityError, CleanupOverrunError
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


class _Hold(Enum):
    """What rows a reservation holds in the pool."""

    # A page's capacity, reserved before its I/O: its unused rows come back when it is accepted.
    IN_FLIGHT = "in_flight"
    # An accepted page's rows, waiting for the consumer, who releases them one by one.
    QUEUED = "queued"
    # A fast keyset lease, held from activation to terminal, only ever trimmed and charged by its own
    # budget view; its binding has not chosen its plan yet, so it may still shrink.
    LEASE = "lease"
    # A lease whose binding has chosen its plan: it keeps its size and returns rows only at terminal.
    SETTLED_LEASE = "settled_lease"


@dataclass(slots=True)
class _Reservation:
    index: int
    amount: int
    hold: _Hold = _Hold.IN_FLIGHT


@dataclass(slots=True, eq=False)
class _Waiter:
    """A page waiting for its rows; the pool wakes it by resolving ``woken`` once its rows are free."""

    index: int
    amount: int
    woken: asyncio.Future[None]


def _waiter_index(waiter: _Waiter) -> int:
    return waiter.index


def _reduce(counter: Counter[int], key: int, amount: int) -> None:
    """Take ``amount`` off ``counter[key]``, dropping the key at zero."""
    remaining = counter[key] - amount
    if remaining > 0:
        counter[key] = remaining
    else:
        del counter[key]


@dataclass(frozen=True, slots=True)
class _Drain:
    """Where an INPUT consumer gets with no page sent: the frontier it then waits on, and the rows it frees.

    ``head_rows`` are the freed rows the new frontier itself held; ``passed`` counts the bindings the
    consumer leaves behind, each of which frees an admission slot for the next binding from the source.
    """

    head: int
    rows: int = 0
    head_rows: int = 0
    passed: int = 0


@dataclass(frozen=True, slots=True)
class _Rooms:
    """The producing bindings in binding order and the room each keeps, at one version of the producers.

    A producer's room is the rows of the pages it may still put in flight at once, its next page or a
    counted tail's window: the rows an ordered pool keeps free for it while it is nearer the frontier
    than the page reserving.
    """

    version: int
    indexes: tuple[int, ...]
    # The rooms of the producers summed in binding order.
    cumulative: tuple[int, ...]

    def _before(self, index: int) -> int:
        position = bisect_left(self.indexes, index)
        return self.cumulative[position - 1] if position else 0

    def producing(self, index: int) -> bool:
        """Return whether the binding at ``index`` is a producer."""
        position = bisect_left(self.indexes, index)
        return position < len(self.indexes) and self.indexes[position] == index

    def room(self, index: int) -> int:
        """Return the room of the producer at ``index``, nothing for a binding that adds no page."""
        return self._before(index + 1) - self._before(index)

    def between(self, head: int, index: int) -> int:
        """Return the room of the producers after ``head`` and before ``index``."""
        return max(0, self._before(index) - self._before(head + 1))


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
    # Resolved once the consumer leaves the page, for a binding that waits for that before its next
    # page; ``None`` when the binding read ahead, and the consumer acknowledges the page itself.
    acknowledged: asyncio.Future[None] | None
    # A fast keyset row group is held by its binding's lease, not a page reservation: each delivered
    # row is reported to the binding's runtime instead of being released to the shared pool.
    emitted: Callable[[int], None] | None = None
    # The binding's completion page id of an ordinary or counted page.
    page_id: int | None = None


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
    # Producers whose last page has reserved its rows (a whole result): they add no page, and an
    # ordered pool keeps no room for them.
    reserved_last: set[str] = field(default_factory=set)
    # Changes whenever a producer's room may change: one is registered, retires, has its pages charged
    # to a lease, reserves its last page or changes its window. ``indexes``, ``caps`` and ``windows``
    # change only through those methods.
    rooms_version: int = 0
    # Producers that may put more than one page in flight at once (a counted tail), and how many.
    windows: dict[str, int] = field(default_factory=dict)
    # Pages whose rows and dispatch are committed but which have not reached the queue yet: a
    # producer that leaves ``runnable`` right after starting their tasks still has them on the way.
    # ``reserving`` holds those that must still reserve their logical page, ``in_transit`` the rest.
    reserving: Counter[str] = field(default_factory=Counter)
    in_transit: Counter[str] = field(default_factory=Counter)
    next_key: str | None = None
    next_index: int | None = None
    # The bindings pulled from the source so far: every index below it is admitted.
    admitted: int = 0
    source_exhausted: bool = False
    source_pull_in_flight: bool = False
    closing: bool = False
    revision: int = 0
    # Changes with every fact the dispatcher's projection reads; ``revision`` also with the consumer's
    # progress, which leaves that projection as it was.
    facts: int = 0
    _waiters: list[asyncio.Future[None]] = field(default_factory=list)

    def touch(self) -> None:
        """Synchronously publish a state change to every waiting worker."""
        self.facts += 1
        self.progress()

    def progress(self) -> None:
        """Wake every waiting worker after the consumer's progress, which changes no fact the projection reads.

        An INPUT consumer draining a queued page frees rows the drain projection already counts. A wave
        still waits for the pages that progress brings; it need not project them again.
        """
        self.revision += 1
        waiters, self._waiters = self._waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    def retire(self, key: str) -> None:
        """Stop counting a producer that adds no further command: it publishes its terminal, or its task ended.

        Only a producer with an index counts, so a mark left behind by a retired producer never counts.
        """
        registered = self.indexes.pop(key, None) is not None
        self.runnable.discard(key)
        self.admitting.discard(key)
        self.pending_continuations.discard(key)
        self.pending_refills.pop(key, None)
        self.reserved_last.discard(key)
        self.windows.pop(key, None)
        self.caps.pop(key, None)
        self.rooms_version += 1
        if registered:
            self.touch()

    def register(self, key: str, index: int, cap: int) -> None:
        """Count an admitted binding as a runnable producer whose pages reserve ``cap`` rows each."""
        self.runnable.add(key)
        self.indexes[key] = index
        self.caps[key] = cap
        self.rooms_version += 1

    def charge_to_lease(self, key: str) -> None:
        """Charge every further page of producer ``key`` to its lease: none reserves pool rows."""
        self.caps[key] = 0
        self.rooms_version += 1
        self.touch()

    def reserve_last(self, key: str) -> None:
        """Record that producer ``key`` has reserved the rows of its last page."""
        self.reserved_last.add(key)
        self.rooms_version += 1
        self.touch()

    def set_window(self, key: str, pages: int) -> None:
        """Record that producer ``key`` may put ``pages`` pages in flight at once."""
        if self.windows.get(key) != pages:
            self.windows[key] = pages
            self.rooms_version += 1

    def room(self, key: str) -> int:
        """Return the rows of the pages producer ``key`` may still put in flight at once."""
        if key in self.reserved_last:
            return 0
        return self.caps.get(key, 0) * self.windows.get(key, 1)

    def rooms(self) -> _Rooms:
        """Return the producers in binding order with the room each keeps."""
        ordered = sorted((index, self.room(key)) for key, index in self.indexes.items())
        return _Rooms(
            self.rooms_version,
            tuple(index for index, _room in ordered),
            tuple(accumulate(room for _index, room in ordered)),
        )

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
    """Reserve page capacity before I/O and account decoded retained rows exactly.

    The rows each binding holds are counted per binding, and so are the rows of its accepted pages that
    wait for the consumer, so no check scans the reservations. A page that cannot reserve waits until a
    change of the pool wakes it; an ordered pool wakes only the waiting pages that fit, nearest the
    frontier first (``_serve``).
    """

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
        self._reservations: list[_Reservation] = []
        # The rows each binding holds, and those of its accepted pages that wait for the consumer.
        self._held: Counter[int] = Counter()
        self._queued: Counter[int] = Counter()
        # Pages waiting for rows, in binding order in an ordered pool, and fast keyset bindings waiting
        # for a lease.
        self._waiters: list[_Waiter] = []
        # The rows the woken pages will take once their tasks resume: no other waiter is woken for them.
        self._promised = 0
        self._lease_waiters: list[asyncio.Future[None]] = []
        self._producer_state = producer_state
        # INPUT delivery: rows go to bindings in binding order, so no binding takes rows while an
        # earlier one, which becomes the frontier first, waits for its lease.
        self._ordered = ordered
        self._lease_waiting: set[int] = set()
        self._rooms: _Rooms | None = None

    @property
    def head_index(self) -> int:
        """Return the INPUT frontier whose page capacity is held in reserve."""
        return self._head_index

    @property
    def head_reserve(self) -> int:
        """Return the rows kept free for the next page of whichever admitted binding becomes the INPUT frontier."""
        return self._head_reserve

    async def reserve(self, index: int, amount: int) -> _Reservation:
        """Reserve a page's rows, waiting for the consumer to release them within the operation's elapsed budget."""
        if amount > self.maximum:
            raise BudgetExceededError("one decoded page exceeds the global row buffer")
        if self._closed:
            raise asyncio.CancelledError
        if not self._can_reserve(index, amount):
            async with self.context.deadline("row buffer wait exceeded operation time budget"):
                await self._wait_for_rows(index, amount)
        return self._take(index, amount, _Hold.IN_FLIGHT)

    async def _wait_for_rows(self, index: int, amount: int) -> None:
        """Wait until ``amount`` rows fit for binding ``index``; the caller takes them in the same step.

        A page that stops waiting, by cancellation or by the elapsed budget, leaves the queue. If it was
        woken already, the rows promised to it go back to the others and the next waiting pages that fit
        are woken; a page never woken blocked no other, as each one's turn depends on the rows alone.
        """
        while not self._can_reserve(index, amount):
            waiter = _Waiter(index, amount, asyncio.get_running_loop().create_future())
            if self._ordered:
                insort(self._waiters, waiter, key=_waiter_index)
            else:
                self._waiters.append(waiter)
            try:
                await waiter.woken
            except BaseException:
                if waiter in self._waiters:
                    self._waiters.remove(waiter)
                elif waiter.woken.done() and not waiter.woken.cancelled():
                    # Woken while its task was being cancelled: another page may take its rows.
                    self._promised -= amount
                    self._changed()
                raise
            self._promised -= amount
            if self._closed:
                raise asyncio.CancelledError

    def _take(self, index: int, amount: int, hold: _Hold) -> _Reservation:
        self._available -= amount
        self._held[index] += amount
        reservation = _Reservation(index, amount, hold)
        self._reservations.append(reservation)
        return reservation

    def _give_back(self, reservation: _Reservation) -> int:
        """Return every row ``reservation`` holds to the pool, and the count."""
        amount = reservation.amount
        reservation.amount = 0
        self._available += amount
        _reduce(self._held, reservation.index, amount)
        if reservation.hold is _Hold.QUEUED:
            _reduce(self._queued, reservation.index, amount)
        self._reservations.remove(reservation)
        return amount

    def _serve(self) -> None:
        """Wake the waiting pages that may take their rows now; each checks again once its task resumes.

        An unordered pool wakes every page, in the order they came: each check is a count, and the pages
        a READY consumer receives depend on which of them takes the rows first. An ordered pool keeps
        free the room of the producers nearer the frontier, which costs more to check, so it wakes only
        the pages whose rows are free beyond those the pages already woken will take, in binding order. A
        page behind the frontier that does not fit ends the pass: a later page keeps free the room of
        every producer between the frontier and it, that page's room included, which covers that page's
        rows, so it does not fit either.
        """
        position, stop_after = 0, None
        while position < len(self._waiters):
            waiter = self._waiters[position]
            if stop_after is not None and waiter.index > stop_after:
                return
            if waiter.woken.done():
                del self._waiters[position]
            elif not self._ordered or self._can_reserve(waiter.index, waiter.amount, -self._promised):
                del self._waiters[position]
                self._promised += waiter.amount
                waiter.woken.set_result(None)
            else:
                if self._ends_pass(waiter):
                    stop_after = waiter.index
                position += 1

    def _ends_pass(self, waiter: _Waiter) -> bool:
        """Return whether no later page fits while ``waiter``, behind the frontier, does not."""
        state = self._producer_state
        if not self._ordered or state is None or self._head_reserve == 0 or waiter.index <= self._head_index:
            return False
        return self._rooms_now(state).room(waiter.index) >= waiter.amount

    def _changed(self) -> None:
        """Grant the waiting pages that now fit and wake the bindings waiting for a lease."""
        self._serve()
        waiters, self._lease_waiters = self._lease_waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    def _can_reserve(self, index: int, amount: int, returning: int = 0, drain: _Drain | None = None) -> bool:
        head, drained, head_drained = (
            (self._head_index, 0, 0) if drain is None else (drain.head, drain.rows, drain.head_rows)
        )
        available = self._available + returning + drained
        if available < amount:
            return False
        # A page charged to a fast keyset lease reserves no pool rows: it cannot take the frontier's reserve.
        if amount == 0:
            return True
        if self._behind_lease_waiter(index):
            return False
        if self._head_reserve == 0 or index == head:
            return True
        return available - amount >= self._kept_free(index, head, head_drained)

    def _kept_free(self, index: int, head: int, head_drained: int) -> int:
        """Return the rows a page at ``index``, behind frontier ``head``, leaves free for the bindings ahead.

        The frontier keeps room for its next page, whichever admitted binding it is. Rows it holds come
        back to it; only the rest of its reserve must stay free. A frontier page accepted short returns
        its unused rows, and those stay the frontier's: if another binding took them, rows held behind the
        frontier could never be released. ``head_drained`` of its rows are counted as already back.

        Under INPUT delivery every producer between the frontier and ``index`` keeps its room as well, the
        pages it may put in flight at once: bindings read ahead, and the nearer the frontier, the sooner
        the consumer drains their rows. Without it, the bindings behind would fill the pool with their
        pages, and each page a binding nearer the frontier then needs would travel alone, once the
        consumer has drained the frontier.
        """
        between = 0
        if self._ordered and self._producer_state is not None:
            between = self._rooms_now(self._producer_state).between(head, index)
        return max(0, self._head_reserve - self._held[head] + head_drained) + between

    def _rooms_now(self, state: _ProducerState) -> _Rooms:
        """Return the producers' rooms, read again only once a producer's room may have changed."""
        rooms = self._rooms
        if rooms is None or rooms.version != state.rooms_version:
            rooms = self._rooms = state.rooms()
        return rooms

    def drain(self) -> _Drain | None:
        """Return where an INPUT consumer gets with no page sent, or ``None`` without ordered delivery.

        Pages read ahead wait in their bindings' queues, not in a wave. The consumer drains the frontier's,
        passes every admitted binding that adds no page any more, and drains the first one that still adds
        one, or waits for the source; the rows those pages hold come back to the pool on the way.
        """
        state = self._producer_state
        if not self._ordered or state is None:
            return None
        rooms = self._rooms_now(state)
        head, rows = self._head_index, 0
        while True:
            rows += self._queued[head]
            if rooms.producing(head) or head + 1 >= state.admitted:
                return _Drain(head, rows, self._queued[head], head - self._head_index)
            head += 1

    def can_reserve(self, index: int, amount: int, *, returning: int = 0, drain: _Drain | None = None) -> bool:
        """Return whether a reservation can be admitted immediately, or once rows come back.

        ``returning`` rows come back to the pool; a ``drain`` projects the pool to where the INPUT
        consumer gets on its own.
        """
        return not self._closed and self._can_reserve(index, amount, returning, drain)

    def _touch(self) -> None:
        if self._producer_state is not None:
            self._producer_state.touch()

    async def accept(self, reservation: _Reservation, actual: int) -> None:
        if actual > reservation.amount:
            await self.abort(reservation)
            raise BudgetExceededError("decoded page exceeded its reserved row capacity")
        if self._closed:
            raise asyncio.CancelledError
        unused = reservation.amount - actual
        reservation.amount = actual
        reservation.hold = _Hold.QUEUED
        self._available += unused
        _reduce(self._held, reservation.index, unused)
        if actual:
            self._queued[reservation.index] += actual
        else:
            self._reservations.remove(reservation)
        self._changed()
        self._touch()
        if actual:
            await self.context.adjust_buffered_rows(actual)

    async def release(self, reservation: _Reservation, count: int) -> None:
        if count < 0 or count > reservation.amount:
            raise ValueError("invalid row-buffer release")
        if not count or reservation.amount == 0:
            return
        reservation.amount -= count
        self._available += count
        _reduce(self._held, reservation.index, count)
        if reservation.hold is _Hold.QUEUED:
            _reduce(self._queued, reservation.index, count)
        if reservation.amount == 0:
            self._reservations.remove(reservation)
        self._changed()
        if self._ordered and reservation.hold is _Hold.QUEUED and self._producer_state is not None:
            # The drain projection counts these rows already, whether queued or free.
            self._producer_state.progress()
        else:
            self._touch()
        await self.context.adjust_buffered_rows(-count)

    async def abort(self, reservation: _Reservation) -> None:
        if not reservation.amount:
            return
        queued = reservation.hold is _Hold.QUEUED
        amount = self._give_back(reservation)
        self._changed()
        self._touch()
        if queued:
            await self.context.adjust_buffered_rows(-amount)

    async def lease(self, index: int, grant: Callable[[int], int | None]) -> _Reservation:
        """Hold one capacity until terminal, never grown; ``grant`` sizes it from the free rows or asks to wait.

        ``grant`` runs with no await between its decision and the reservation, so the two are atomic. It
        may raise to refuse a lease no releaser can ever satisfy. An ordered pool asks it only once no
        earlier binding waits for a lease. A binding leaving the wait, with a lease or without one,
        wakes the others: whether it still waits decides whether a later binding may take rows.
        """
        self._lease_waiting.add(index)
        try:
            while True:
                if self._closed:
                    raise asyncio.CancelledError
                amount = None if self._behind_lease_waiter(index) else grant(self._available)
                if amount is not None:
                    break
                wake: asyncio.Future[None] = asyncio.get_running_loop().create_future()
                self._lease_waiters.append(wake)
                await wake
            if isinstance(amount, bool) or not 0 < amount <= self._available:
                raise RuntimeError("fast keyset lease exceeds the free row pool")
            return self._take(index, amount, _Hold.LEASE)
        finally:
            self._lease_waiting.discard(index)
            self._changed()
            self._touch()

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
        if reservation.hold not in {_Hold.LEASE, _Hold.SETTLED_LEASE} or amount <= 0:
            raise RuntimeError("only a lease settles, and never to nothing")
        if self._closed or reservation.amount == 0:
            return
        if amount > reservation.amount:
            raise RuntimeError("a lease cannot grow")
        trimmed = reservation.amount - amount
        self._available += trimmed
        _reduce(self._held, reservation.index, trimmed)
        reservation.amount = amount
        reservation.hold = _Hold.SETTLED_LEASE
        self._changed()
        self._touch()

    def releasable_elsewhere(self, index: int) -> bool:
        """Return whether another binding may still return capacity without this one's progress.

        A page in flight returns its unused rows when it settles, and an unsettled lease may be trimmed
        once its binding has planned, which needs no delivery.
        """
        return any(
            reservation.index != index and reservation.hold in {_Hold.IN_FLIGHT, _Hold.LEASE}
            for reservation in self._reservations
        )

    async def poke(self) -> None:
        """Wake capacity waiters after a scheduling fact a lease decision depends on has changed."""
        self._changed()

    def raise_head_reserve(self, reserve: int) -> None:
        """Reserve more rows for the INPUT frontier, synchronously: a stricter reserve wakes no waiter."""
        self._head_reserve = max(self._head_reserve, min(reserve, self.maximum))

    async def advance_head(self, index: int, reserve: int) -> None:
        self._head_index = index
        self._head_reserve = reserve
        self._changed()
        self._touch()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._available = self.maximum
        for reservation in self._reservations:
            reservation.amount = 0
        self._reservations.clear()
        self._held.clear()
        self._queued.clear()
        waiters, self._waiters = self._waiters, []
        for waiter in waiters:
            if not waiter.woken.done():
                self._promised += waiter.amount
                waiter.woken.set_result(None)
        self._changed()
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
        try:
            # The deadline bounds admission only: once sent, the executor's own budgets end the request and
            # record whether it may have run, which a deadline raised around the send would discard.
            async with self.context.deadline("direct scheduler admission exceeded operation time budget"):
                await self._semaphore.acquire()
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

    async def fetch(
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
        try:
            # The deadline bounds admission only: once the wave is sent, the executor's own budgets end it and
            # record whether it may have run, as they do for every other send.
            async with self.context.deadline("batch scheduler admission exceeded operation time budget") as admission:
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
        except BaseException:
            # A refusal by the deadline included: a page whose wave was sent no longer has one, so it was never sent.
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
                projected: tuple[int, int, int] | None = None
                while len(chunk) < self.plan.batch_size:
                    self._drain_nowait(chunk)
                    if len(chunk) >= self.plan.batch_size:
                        break
                    if self.plan.coalesce_wait <= 0:
                        break
                    # The pages still to come change only with a fact, the chunk or the queue.
                    facts = (
                        self._producer_state.facts if self._producer_state else 0,
                        len(chunk),
                        self._pending_count(),
                    )
                    if facts != projected:
                        projected = facts
                        if self._potential({item.reference_id for item in chunk}) == 0:
                            break
                    remaining = min(deadline - self._time(), self.context.coalescing_time_left())
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

        # The rows the consumer gives back on its own. A READY consumer acknowledges the receipt a
        # refill waits on, whichever binding it is; an INPUT consumer drains the queues ahead of it.
        ready = self._pending_continuations_can_progress
        drain = buffer.drain()

        def fits(key: str, held: _Reservation | None = None) -> bool:
            """Return whether the producer's next page reserves its page and rows once the consumer gives rows back."""
            return self.context.can_reserve_page(reference=key) and buffer.can_reserve(
                state.indexes[key],
                state.caps.get(key, self._page_cap),
                returning=held.amount if ready and held is not None else 0,
                drain=drain,
            )

        admitted = sum(key not in chunk_keys and fits(key) for key in state.runnable if key in state.indexes)
        admitting = sum(key not in chunk_keys and fits(key) for key in state.admitting if key in state.indexes)
        continuations = sum(
            key not in chunk_keys and fits(key) for key in state.pending_continuations if ready and key in state.indexes
        )
        pending_pull = int(
            state.next_key is not None
            and state.next_index is not None
            and state.next_key not in chunk_keys
            and state.source_pull_in_flight
            and self.context.can_reserve_page(reference=state.next_key)
            and buffer.can_reserve(state.next_index, self._page_cap, drain=drain),
        )
        settling_admission = sum(
            key not in chunk_keys and key in state.indexes and not fits(key)
            for key in state.admitting
            if ready and (self._active_sends > 0 or self._settling_waves > 0)
        )
        refills = sum(fits(key, returned) for key, returned in state.pending_refills.items() if key in state.indexes)
        # A page on its way to the queue arrives without any consumer progress, under either order;
        # one that must still reserve its logical page arrives only if the page budget admits it.
        in_transit = sum(count for key, count in state.in_transit.items() if key in state.indexes) + sum(
            count
            for key, count in state.reserving.items()
            if key in state.indexes and self.context.can_reserve_page(reference=key)
        )
        # Under INPUT each binding the consumer passes frees an admission slot: the next binding from the
        # source arrives without any page sent.
        admissions = 0
        if drain is not None and not state.source_exhausted:
            first = state.admitted + pending_pull
            admissions = sum(
                self.context.can_reserve_page() and buffer.can_reserve(first + offset, self._page_cap, drain=drain)
                for offset in range(drain.passed)
            )
        waiting = admitted + admitting + continuations + refills + pending_pull + settling_admission
        return waiting + in_transit + admissions

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
                _, pending_workers = await asyncio.wait(active, timeout=self.context.cleanup_window())
                if pending_workers:
                    raise CleanupOverrunError("batch dispatcher cleanup exceeded operation time budget")
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
