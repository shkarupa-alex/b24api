"""Bounded fair scheduling for independent and paginated references."""

from __future__ import annotations
import asyncio
import contextlib
from bisect import bisect_left, insort
from collections import Counter, deque
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, cast

from b24api.batch.engine import BatchExecutor, BatchProgress
from b24api.batch.outcome import BatchFailure, BatchSuccess, settlement_of
from b24api.contracts.policy import ReplayDisposition
from b24api.contracts.report import PageDispatch
from b24api.errors import BudgetExceededError, CapabilityError, CleanupOverrunError
from b24api.execution import (
    ExecutionContext,
    Executor,
    WorkClass,
)
from b24api.execution.context import cancelled_here, latch_escape
from b24api.execution.facts import settlement
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
    from b24api.execution.context import PageReservation, Settled
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
    # A fast keyset lease, held from activation to terminal and charged by its own budget view; its binding has
    # not chosen its plan yet, so it may still grow once, as it plans, or shrink to the rows its plan can hold.
    LEASE = "lease"
    # A lease whose binding has chosen its plan: it keeps its size and returns rows only at terminal.
    SETTLED_LEASE = "settled_lease"


@dataclass(slots=True, eq=False)
class _Reservation:
    """Rows one binding holds in the pool; each is its own: the pool lists and removes it by identity."""

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
    """The producing bindings in binding order and the room each has, at one version of the producers.

    A producer's room is the rows of the pages it may still put in flight at once, its next page or a
    counted tail's window. An ordered pool keeps free the part of it that the producer does not hold
    already (``_RowBuffer._kept``) while it is nearer the frontier than the page reserving.
    """

    version: int
    indexes: tuple[int, ...]
    rooms: tuple[int, ...]

    def producing(self, index: int) -> bool:
        """Return whether the binding at ``index`` is a producer."""
        position = bisect_left(self.indexes, index)
        return position < len(self.indexes) and self.indexes[position] == index

    def span(self, head: int, index: int) -> range:
        """Return the positions of the producers after ``head`` and before ``index``."""
        return range(bisect_left(self.indexes, head + 1), bisect_left(self.indexes, index))


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
    error: Exception
    cursor: JsonValue | IdentityValue
    page_state: int
    partial_rows: int
    violations: tuple[Violation, ...]
    page_records: tuple[PageRecord, ...] = ()
    replay_disposition: ReplayDisposition = ReplayDisposition.NOT_ELIGIBLE
    not_executed_reason: NotExecutedReason | None = None


@dataclass(frozen=True, slots=True)
class _EndedEvent:
    """A binding ended in no outcome: its task ended in a ``BaseException`` that is neither a failure nor an interrupt.

    It takes the binding's place in the order its outcome would have, so the reader gets there instead of waiting
    for an end that never comes, and raises what the task ended in as it is (``_finish_task``). It carries no
    evidence.
    """

    work: _Work


type _Event = _PageEvent | _DoneEvent | _FailureEvent | _EndedEvent


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


_PLAIN_DECODING = _DecodeOptions()


@dataclass(slots=True)
class _PendingBatch:
    request: Request
    reference_id: str
    future: asyncio.Future[_DispatchedPage]
    admitted: asyncio.Future[None]
    settled: asyncio.Future[None]
    options: _DecodeOptions = _PLAIN_DECODING
    # The operation deadline of the page's admission; the send of its wave lifts it.
    admission: asyncio.Timeout | None = None
    # Once its wave was handed to its send: where the page's command stands in that chunk, and its index there.
    progress: BatchProgress | None = None
    position: int = 0

    def record(self, error: BaseException) -> None:
        """Record on the page's ``error`` what its command is known to have done, if its wave was handed to its send.

        Its outcome in the chunk says, once final or once the chunk has ended; until then the chunk, which runs on
        for the other pages, may still send it, so it may run (``BatchProgress``). A page never handed to a send
        records nothing: it stays proven not executed.
        """
        if self.progress is not None:
            self.progress.record(error, self.position)

    def mark_sent(self, progress: BatchProgress, position: int) -> bool:
        """Hand the page to its wave's send as command ``position`` of its chunk; ``False`` when it was abandoned.

        The hand-over lifts its admission deadline. A page whose fetch has ended (every way out of the fetch
        completes or cancels its future) or whose deadline has expired is abandoned: no one waits for it, so it is
        not sent and stays proven not executed. The check and the hand-over happen in one step of the event loop,
        so the fetch cannot end in between.
        """
        admission = self.admission
        if self.future.done() or (admission is not None and admission.expired()):
            return False
        if admission is not None:
            admission.reschedule(None)
        self.progress, self.position = progress, position
        return True


class _Producing(Enum):
    """What a producer's next page waits for; the dispatcher's projection counts a producer by this alone."""

    # Nothing the dispatcher must send first: the page comes as soon as its rows are free.
    RUNNABLE = "runnable"
    # The page is reserving its rows on its way to the queue.
    ADMITTING = "admitting"
    # The consumer taking the producer's last page: a pending continuation, which follows once acknowledged.
    AWAITING_CONSUMER = "awaiting_consumer"
    # A counted tail's page held for its window: the consumer frees a slot, and with ``refill_returns`` gives back
    # the rows of a page it holds first. Unlike a pending continuation, its pages already in a chunk do not exclude
    # it.
    REFILLING = "refilling"
    # A page the producer sent, its lease or its window: it adds no page on its own.
    WAITING = "waiting"


@dataclass(slots=True)
class _ProducerState:
    """Advisory single-event-loop producer availability for batch coalescing.

    Each registered producer has one status (``_Producing``), changed only through ``move`` and
    ``leave``, and its page cap in ``caps``; ``retire`` forgets both.
    """

    indexes: dict[str, int]
    status: dict[str, _Producing] = field(default_factory=dict)
    # The rows each registered producer's pages reserve in the pool (none once charged to a lease), and those of
    # a binding the source has not yielded yet: the one copy of the page cap the pool and the dispatcher read.
    caps: dict[str, int] = field(default_factory=dict)
    default_cap: int = 1
    # A refilling tail's page whose consumer returns these rows to the pool before the page reserves its own.
    refill_returns: dict[str, _Reservation] = field(default_factory=dict)
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
        self.status.pop(key, None)
        self.refill_returns.pop(key, None)
        self.reserved_last.discard(key)
        self.windows.pop(key, None)
        self.caps.pop(key, None)
        self.rooms_version += 1
        if registered:
            self.touch()

    def register(self, key: str, index: int, cap: int) -> None:
        """Count an admitted binding as a runnable producer whose pages reserve ``cap`` rows each."""
        self.indexes[key] = index
        self.status[key] = _Producing.RUNNABLE
        self.caps[key] = cap
        self.rooms_version += 1

    def move(self, key: str, status: _Producing, *, returns: _Reservation | None = None) -> None:
        """Record what producer ``key``'s next page waits for now, and publish it; a retired producer stays gone.

        ``returns`` is the reservation a refilling tail's consumer gives back before its page reserves.
        """
        if key not in self.indexes:
            return
        self.status[key] = status
        if returns is None:
            self.refill_returns.pop(key, None)
        else:
            self.refill_returns[key] = returns
        self.touch()

    def leave(self, key: str, status: _Producing) -> None:
        """Move producer ``key`` from ``status`` to ``WAITING`` if it is still there: another step may have moved it."""
        if self.status.get(key) is status:
            self.move(key, _Producing.WAITING)

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
            tuple(room for _index, room in ordered),
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

    def carried_by(self, task: asyncio.Future[Settled[_DispatchedPage]]) -> None:
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
    tasks: dict[int, asyncio.Task[Settled[None]]]
    queues: dict[int, asyncio.Queue[_Event]]
    ready: asyncio.Queue[_Event]
    slots: asyncio.Semaphore
    changed: asyncio.Event


@dataclass(frozen=True, slots=True)
class _FailedPage:
    """What a page whose dispatch raised reports, and how it settles, from what its command did.

    Every reader of a failed page reads it through ``_failed_page``: the binding's failure and its replay
    disposition, the page's settlement and trace, and a command failure that a wave or a tail takes as an outcome.
    The settlement is the one fact of what the command did, and the trace's one input (``failed_page``): a page
    that never executed, never sent, is not traced, and the settlement decides the code of every other page.
    """

    error: BaseException
    settlement: CommandSettlement
    # The command's own final outcome, when the batch answered or ended it: a failure of that command alone.
    failure: BatchFailure | None = None
    # The command's index in the physical batch it was handed to: its own outcome's, or its position in a chunk
    # that failed as a whole; none for a page the dispatcher never handed to a batch.
    batch_index: int | None = None


class _SentPageError(Exception):
    """The failure the batch dispatcher gives a page it handed to a batch: it carries the page's own facts."""

    def __init__(self, message: str, page: _FailedPage) -> None:
        self.page = page
        super().__init__(message)


class _BatchPageError(_SentPageError):
    """A page whose command the batch answered, or ended, with its own failure (``failure``)."""

    def __init__(self, failure: BatchFailure) -> None:
        cause = failure.error
        super().__init__(
            "batch page command failed",
            _FailedPage(
                cause if isinstance(cause, BaseException) else self,
                settlement_of(failure),
                failure,
                failure.command_index,
            ),
        )


class _ChunkPageError(_SentPageError):
    """A page of a chunk that failed as a whole: it reports the chunk's ``error``, and keeps what its own command did.

    The error is the chunk's, the same for every page of it; what each command did is its own, read where the chunk
    stands when it fails (``BatchProgress``), as a page cancelled meanwhile records it.
    """

    def __init__(self, error: Exception, progress: BatchProgress, position: int) -> None:
        super().__init__(
            "batch page failed with its chunk",
            _FailedPage(error, progress.settlement(position), batch_index=position),
        )


def _failed_page(error: BaseException) -> _FailedPage:
    """Return a failed page's facts: those the dispatcher gave it, or those recorded on its failure.

    A page the dispatcher handed to a batch carries its own; any other failure (a page cancelled after its wave
    left, an admission refusal) has them recorded on it.
    """
    if isinstance(error, _SentPageError):
        return error.page
    return _FailedPage(error, settlement(error))


class _RowBuffer:
    """Reserve page capacity before I/O and account decoded retained rows exactly.

    The rows each binding holds are counted per binding, and so are the rows of its accepted pages that
    wait for the consumer, so no check scans the reservations. A page that cannot reserve waits until a
    change of the pool wakes it; the pool wakes only the waiting pages that fit, an ordered pool nearest
    the frontier first (``_serve``).
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
        # The rows each binding's pages reserved before their I/O hold until they are accepted.
        self._in_flight: Counter[int] = Counter()
        # Pages waiting for rows, in binding order in an ordered pool, and fast keyset bindings waiting
        # for a lease.
        self._waiters: list[_Waiter] = []
        # The rows the woken pages will take once their tasks resume, in all and per binding: no other waiter
        # is woken for them, and an ordered pool counts them as held by their bindings already.
        self._promised = 0
        self._promised_at: Counter[int] = Counter()
        # The free rows the next pass of ``_serve`` needs to wake a page, at a version of the producers' rooms: a
        # release that frees fewer rows, the rooms unchanged, wakes none. Anything that lowers what a waiting page
        # needs resets it: a new waiter, rows taken (``_hold``), a promise given back.
        self._wake_at = 0
        self._wake_version = -1
        # Bindings waiting for a lease, and pages waiting to grow (``grow``): each decides again on every change.
        self._rechecks: list[asyncio.Future[None]] = []
        # Pages answered with more rows than they reserved, waiting for the rest: their rows come back only once
        # they have grown, so no one waits for them.
        self._growing: set[_Reservation] = set()
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
        if not self._can_reserve(index, amount, -self._promised):
            async with self.context.deadline("row buffer wait exceeded operation time budget"):
                await self._wait_for_rows(index, amount)
        return self._take(index, amount, _Hold.IN_FLIGHT)

    async def _wait_for_rows(self, index: int, amount: int) -> None:
        """Wait until ``amount`` rows fit for binding ``index``; the caller takes them in the same step.

        Rows promised to a woken page are its own until it resumes: no other page and no lease takes them. A
        woken page that no longer fits when it resumes, as an ordered pool's rooms changed meanwhile, waits
        again and gives its promise to the waiting pages it serves. A page that stops waiting, by
        cancellation or by the elapsed budget, leaves the queue, and if it was woken already gives its
        promise back the same way; a page never woken blocked no other, as each one's turn depends on the
        rows alone.
        """
        while not self._can_reserve(index, amount, -self._promised):
            waiter = _Waiter(index, amount, asyncio.get_running_loop().create_future())
            if self._ordered:
                insort(self._waiters, waiter, key=_waiter_index)
            else:
                self._waiters.append(waiter)
            # A new waiter may need fewer free rows than those already waiting.
            self._wake_at = 0
            try:
                await waiter.woken
            except BaseException:
                if waiter in self._waiters:
                    self._waiters.remove(waiter)
                elif waiter.woken.done() and not waiter.woken.cancelled():
                    # Woken while its task was being cancelled: another page may take its rows.
                    self._unpromise(index, amount)
                    self._changed()
                raise
            self._unpromise(index, amount)
            if self._closed:
                raise asyncio.CancelledError
            if not self._can_reserve(index, amount, -self._promised):
                self._changed()

    def _promise(self, waiter: _Waiter) -> None:
        self._promised += waiter.amount
        self._promised_at[waiter.index] += waiter.amount
        waiter.woken.set_result(None)

    def _unpromise(self, index: int, amount: int) -> None:
        self._promised -= amount
        _reduce(self._promised_at, index, amount)
        # Rows given back or taken change what the pages behind this binding must leave free.
        self._wake_at = 0

    def _take(self, index: int, amount: int, hold: _Hold) -> _Reservation:
        self._hold(index, amount, in_flight=hold is _Hold.IN_FLIGHT)
        reservation = _Reservation(index, amount, hold)
        if amount:
            # Only a reservation holding rows is listed: a page charged to a lease holds none to give back.
            self._reservations.append(reservation)
        return reservation

    def _hold(self, index: int, amount: int, *, in_flight: bool) -> None:
        """Move ``amount`` free rows to binding ``index``, as rows its page holds in flight or as its lease."""
        self._available -= amount
        self._held[index] += amount
        if in_flight:
            self._in_flight[index] += amount
        # Rows a producer between the frontier and a waiting page takes drop out of what that page leaves free.
        self._wake_at = 0

    def _give_back(self, reservation: _Reservation) -> int:
        """Return every row ``reservation`` holds to the pool, and the count."""
        amount = reservation.amount
        reservation.amount = 0
        self._available += amount
        _reduce(self._held, reservation.index, amount)
        if reservation.hold is _Hold.QUEUED:
            _reduce(self._queued, reservation.index, amount)
        elif reservation.hold is _Hold.IN_FLIGHT:
            _reduce(self._in_flight, reservation.index, amount)
        self._reservations.remove(reservation)
        return amount

    def _serve(self) -> None:
        """Wake the waiting pages that may take their rows now; each checks again once its task resumes.

        A page is woken only for rows free beyond those the pages already woken will take, so a release
        wakes no page that would find its rows gone. An unordered pool wakes the pages that fit in the
        order they came; an ordered pool in binding order, nearest the frontier first, each counting the
        rows the pages woken before it take. One pass reads every waiting page once (``_kept_ahead``), and
        records the free rows the next pass needs, so a release that frees fewer runs none until rows are taken
        or a waiter changes, which may lower that need.
        """
        version = -1 if self._producer_state is None else self._producer_state.rooms_version
        if not self._waiters or (self._available - self._promised < self._wake_at and version == self._wake_version):
            return
        wake_at: int | None = None
        position = 0
        ahead = self._kept_ahead()
        while position < len(self._waiters):
            waiter = self._waiters[position]
            if waiter.woken.done():
                del self._waiters[position]
                continue
            need = ahead(waiter.index, waiter.amount)
            if need is not None and self._available - self._promised >= need:
                del self._waiters[position]
                self._promise(waiter)
                continue
            if need is not None:
                wake_at = need if wake_at is None else min(wake_at, need)
            position += 1
        # A page held back for a lease waiter or a page waiting to grow needs that one to move, which serves the pool
        # again.
        self._wake_at = self.maximum + 1 if wake_at is None else wake_at
        self._wake_version = version

    def _kept_ahead(self, drain: _Drain | None = None) -> Callable[[int, int], int | None]:
        """Return the free rows a page needs, for pages asked in binding order within one pass.

        The rows a page needs are its own and those it leaves free for the bindings ahead (``_kept``). An
        ordered pool's waiting pages are asked in binding order, so the producers between the frontier and each
        page are summed once for the whole pass, each as it stands when the pass reaches it: after the pages
        woken before it, its own included, have taken their promise. A page the pool holds back (``_held_back``)
        needs ``None``. A ``drain`` asks it where the INPUT consumer gets on its own.
        """
        state = self._producer_state
        rooms = self._rooms_now(state) if self._ordered and state is not None else None
        head, head_drained = (self._head_index, 0) if drain is None else (drain.head, drain.head_rows)
        position = 0 if rooms is None else bisect_left(rooms.indexes, head + 1)
        between = 0

        def need(index: int, amount: int) -> int | None:
            nonlocal position, between
            if amount == 0:
                return 0
            if self._held_back(index):
                return None
            if self._head_reserve == 0 or index == head:
                return amount
            if rooms is not None:
                while position < len(rooms.indexes) and rooms.indexes[position] < index:
                    between += self._unheld(rooms, position)
                    position += 1
            return amount + self._frontier_kept(head, head_drained) + between

        return need

    def _changed(self) -> None:
        """Grant the waiting pages that now fit and wake the bindings waiting for a lease or to grow a page."""
        self._wake_at = 0
        self._serve()
        self._wake_rechecks()

    def _wake_rechecks(self) -> None:
        waiters, self._rechecks = self._rechecks, []
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
        if self._held_back(index):
            return False
        if self._head_reserve == 0 or index == head:
            return True
        return available - amount >= self._kept(index, head, head_drained)

    def _frontier_kept(self, head: int, head_drained: int) -> int:
        """Return the part of the frontier's reserve it does not hold: rows it holds come back to it."""
        holding = self._held[head] + self._promised_at[head]
        return max(0, self._head_reserve - holding + head_drained)

    def _kept(self, index: int, head: int, head_drained: int) -> int:
        """Return the rows a page at ``index``, behind frontier ``head``, leaves free for the bindings ahead.

        The frontier keeps room for its next page, whichever admitted binding it is. Rows it holds come
        back to it; only the rest of its reserve must stay free. A frontier page accepted short returns
        its unused rows, and those stay the frontier's: if another binding took them, rows held behind the
        frontier could never be released. ``head_drained`` of its rows are counted as already back.

        Under INPUT delivery every producer between the frontier and ``index`` keeps its room as well, the
        pages it may put in flight at once: bindings read ahead, and the nearer the frontier, the sooner
        the consumer drains their rows. Without it, the bindings behind would fill the pool with their
        pages, and each page a binding nearer the frontier then needs would travel alone, once the
        consumer has drained the frontier. A producer's page in flight gives its unused rows back as it lands,
        and the pool serves the producer nearest the frontier first, so only the part of its room that its pages
        in flight do not hold is kept.
        """
        between = 0
        if self._ordered and self._producer_state is not None:
            between = self._between(self._rooms_now(self._producer_state), head, index)
        return self._frontier_kept(head, head_drained) + between

    def _between(self, rooms: _Rooms, head: int, index: int) -> int:
        """Return the rooms the producers after ``head`` and before ``index`` do not hold."""
        return sum(self._unheld(rooms, position) for position in rooms.span(head, index))

    def _unheld(self, rooms: _Rooms, position: int) -> int:
        """Return the part of a producer's room its pages in flight or woken for their rows do not hold.

        Those come back to it as they land; the rows of its pages waiting for the consumer do not, as it reads
        ahead of the consumer.
        """
        index = rooms.indexes[position]
        holding = self._in_flight[index] + self._promised_at[index]
        return max(0, rooms.rooms[position] - holding)

    def in_flight(self, index: int) -> int:
        """Return the rows binding ``index``'s pages reserved before their I/O and hold until accepted."""
        return self._in_flight[index]

    def reserving(self, index: int) -> int:
        """Return the rows binding ``index`` holds before its pages' I/O, those reserved and those promised to it."""
        return self._in_flight[index] + self._promised_at[index]

    def fits_any(self, pages: list[tuple[int, int]], *, drain: _Drain | None = None, returning: int = 0) -> bool:
        """Return whether any of ``pages``, each a binding index and its rows, could reserve its rows.

        This is the pool's own rule (``_serve``) after ``returning`` rows come back (pages the consumer returns, or
        waves that land). The consumer's own progress comes back as well: a ``drain`` projects an ordered pool to
        where the INPUT consumer gets on its own, and a READY consumer takes every page waiting for it, in any order.
        """
        if self._closed:
            return False
        drained = (0 if drain is None else drain.rows) if self._ordered else self._queued.total()
        free = self._available - self._promised + drained + returning
        need = self._kept_ahead(drain)
        return any((rows := need(index, amount)) is not None and free >= rows for index, amount in sorted(pages))

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
        _reduce(self._in_flight, reservation.index, reservation.amount)
        reservation.amount = actual
        reservation.hold = _Hold.QUEUED
        self._available += unused
        _reduce(self._held, reservation.index, unused)
        if actual:
            self._queued[reservation.index] += actual
        elif unused:
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
        elif reservation.hold is _Hold.IN_FLIGHT:
            _reduce(self._in_flight, reservation.index, count)
        if reservation.amount == 0:
            self._reservations.remove(reservation)
        # The consumer releases rows one by one: a release serves only once the rows the next page needs are free.
        self._serve()
        self._wake_rechecks()
        if self._ordered and reservation.hold is _Hold.QUEUED and self._producer_state is not None:
            # Freed rows wake the producers and waves waiting for room; the drain projection counts a queued page's
            # rows already, so this wakes them without a fact change that would make a coalescing wave project again.
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
        """Hold one capacity until terminal; ``grant`` sizes it from the free rows or asks to wait.

        ``grant`` sees the free rows not promised to a woken page, and runs with no await between its decision
        and the reservation, so the two are atomic. It
        may raise to refuse a lease no releaser can ever satisfy. An ordered pool asks it only once no
        earlier binding waits for a lease. A binding leaving the wait, with a lease or without one,
        wakes the others: whether it still waits decides whether a later binding may take rows.
        """
        self._lease_waiting.add(index)
        try:
            while True:
                if self._closed:
                    raise asyncio.CancelledError
                free = self._available - self._promised
                amount = None if self._held_back(index) else grant(free)
                if amount is not None:
                    break
                wake: asyncio.Future[None] = asyncio.get_running_loop().create_future()
                self._rechecks.append(wake)
                await wake
            if isinstance(amount, bool) or not 0 < amount <= free:
                raise RuntimeError("fast keyset lease exceeds the free row pool")
            return self._take(index, amount, _Hold.LEASE)
        finally:
            self._lease_waiting.discard(index)
            self._changed()
            self._touch()

    def _held_back(self, index: int) -> bool:
        """Return whether the pool holds its rows back from binding ``index``'s pages and lease.

        An ordered pool holds them back for an earlier binding waiting for a lease. Every pool holds them back
        while a page waits to grow (``grow``): its rows are decoded already, so the rows that come back are its
        first, and no page reserves rows that could keep it waiting. An ordered pool still serves the frontier,
        unless the page waiting to grow is the frontier's: a page behind the frontier waits for it to move on.
        """
        if self._ordered and any(waiting < index for waiting in self._lease_waiting):
            return True
        if not self._growing:
            return False
        if not self._ordered or index != self._head_index:
            return True
        return any(reservation.index == index for reservation in self._growing)

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

    def grow_lease(self, reservation: _Reservation, grant: Callable[[int], int]) -> None:
        """Grow an unsettled lease by the rows ``grant`` takes of the free ones.

        A lease is sized when its binding is admitted, before the facts its share depends on are known; its
        binding grows it once, as it plans. ``grant`` sees the free rows not promised to a woken page and runs
        with no await between its decision and the reservation. An ordered pool grows no lease while an
        earlier binding waits for one, and a pool closed meanwhile has already reclaimed it.
        """
        if reservation.hold is not _Hold.LEASE:
            raise RuntimeError("only a lease whose binding has not planned grows")
        if self._closed or reservation.amount == 0 or self._held_back(reservation.index):
            return
        free = self._available - self._promised
        more = grant(free)
        if isinstance(more, bool) or more > free:
            raise RuntimeError("fast keyset lease grows beyond the free row pool")
        if more <= 0:
            return
        self._hold(reservation.index, more, in_flight=False)
        reservation.amount += more
        self._touch()

    def releasable_elsewhere(self, index: int) -> bool:
        """Return whether another binding may still return capacity without this one's progress.

        A page in flight returns its unused rows when it settles, unless it waits to grow, and an unsettled lease
        may be trimmed once its binding has planned, which needs no delivery.
        """
        return any(
            reservation.index != index
            and reservation.hold in {_Hold.IN_FLIGHT, _Hold.LEASE}
            and reservation not in self._growing
            for reservation in self._reservations
        )

    async def grow(self, reservation: _Reservation, total: int) -> int | None:
        """Grow a page's reservation to the ``total`` rows its answer holds; ``None`` once it holds them.

        A page reserves its page cap before its I/O, and an answer can hold more rows than that: a whole result
        of a call that does not page. Those rows are decoded already, so while the page waits for the rest the
        pool holds its rows back from every other page and lease (``_held_back``): the rows that come back are
        this page's first, and no page reserves rows that could keep it waiting. It waits while the rows it lacks
        may all still come back without a request to be sent or progress of its own (``_returnable``). In an
        ordered pool only the frontier grows: a page behind it waits for the frontier to reach it, as its rows
        would anyway, and takes no row the frontier could need; it too waits only while the rows it lacks may all
        come back by then. When the rows that may come back are too few, ``total`` exceeds the pool, or the
        operation's elapsed budget runs out while the page waits, the rows never fit: returns the most the page could
        hold, and keeps its reservation as it was. The page was answered, so the budget's refusal, recorded in the
        context, ends the operation but not as this page's own failure: its command ran.

        While it waits, its decoded rows are held outside the pool: one answer per page in flight at most, each
        no longer than the pool, as no page reserves rows meanwhile but the frontier of an ordered pool.
        """
        if total > self.maximum:
            return self.maximum
        self._growing.add(reservation)
        self._wake_rechecks()
        self._touch()
        try:
            async with self.context.deadline("row buffer wait exceeded operation time budget"):
                return await self._grown(reservation, total)
        except BudgetExceededError:
            return reservation.amount + max(0, self._available)
        finally:
            self._growing.discard(reservation)
            self._changed()
            self._touch()

    async def _grown(self, reservation: _Reservation, total: int) -> int | None:
        """Wait until the page grows to ``total`` rows (``None``) or they never fit (the most it could hold)."""
        index = reservation.index
        while True:
            if self._closed:
                raise asyncio.CancelledError
            # Where this page may grow, every other page is held back now, a woken one too: the rows promised to it
            # are this page's, and the woken page, resuming, finds it does not fit and waits again.
            more, free = total - reservation.amount, self._available
            if (not self._ordered or index == self._head_index) and free >= more:
                self._hold(index, more, in_flight=True)
                reservation.amount = total
                return None
            if free + self._returnable(index) < more:
                return reservation.amount + max(0, free)
            wake: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._rechecks.append(wake)
            await wake

    def _returnable(self, index: int) -> int:
        """Return the most rows that may come back to a page of binding ``index`` waiting to grow, without its progress.

        A page of another binding returns its unused rows when it lands: no page reserves rows while one waits to
        grow, so the pages reserved before are sent and land. A lease returns rows when its binding plans, and a
        planned one when its binding ends. Rows waiting for the consumer come back as the consumer takes them: the
        binding's own earlier pages, and under READY delivery every page. Under INPUT delivery every row the
        bindings before it hold comes back before it is the frontier, theirs waiting to grow too, while the rows
        read ahead for the bindings behind it, and a planned lease of one of them, come back only after it. A page
        waiting to grow returns none to a page before it, nor to any page in an unordered pool, and the binding's own
        other pages in flight land only after this one is admitted. Each counts every row it holds, the most it may
        return: a page may land empty, and a binding's lease comes back whole when it ends.
        """
        returnable = 0
        for other in self._reservations:
            if self._ordered and other.index < index:
                returns = True
            elif other in self._growing:
                returns = False
            elif other.hold is _Hold.QUEUED:
                returns = other.index == index or not self._ordered
            elif other.hold is _Hold.SETTLED_LEASE:
                returns = other.index != index and not self._ordered
            else:
                returns = other.index != index
            if returns:
                returnable += other.amount
        return returnable

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
        self._in_flight.clear()
        waiters, self._waiters = self._waiters, []
        for waiter in waiters:
            if not waiter.woken.done():
                self._promise(waiter)
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
        self._semaphore = asyncio.Semaphore(plan.concurrency)
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
        # Pages waiting for a wave. Not bounded by a count of its own: a page reaches it only with its rows
        # reserved (or charged to its binding's lease) and within its producer's window, so the row pool and the
        # active bindings' windows bound it. A count of active bindings would hold a binding's window to one page
        # a wave.
        self._queue: asyncio.Queue[_PendingBatch] = asyncio.Queue()
        self._send_queue: asyncio.Queue[list[_PendingBatch]] = asyncio.Queue(maxsize=plan.concurrency)
        self._send_slots = asyncio.Semaphore(plan.concurrency)
        self._producer_state = producer_state
        self._buffer = buffer
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
        # The producers with pages in waves sent and not yet in the row pool: their rows come back as they land.
        self._flying: Counter[str] = Counter()
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
            # A page cancelled after its wave was sent may have executed: what its command did says how.
            pending.record(error)
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
        self._queue.put_nowait(pending)
        if transit is not None:
            transit.arrive()
        if self._producer_state is not None:
            self._producer_state.leave(pending.reference_id, _Producing.ADMITTING)
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
        self._assembler = asyncio.create_task(self._run())
        senders = tuple(asyncio.create_task(self._send_run()) for _index in range(self.plan.concurrency))
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
                # The coalescing deadline runs while pages may come now; pages that only a wave in flight frees the
                # rows of wait for it to land instead (``_landing``), and the deadline starts once they may come.
                deadline: float | None = None
                landing = False
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
                        now, after_landing = self._potential({item.reference_id for item in chunk})
                        if not after_landing:
                            break
                        landing = not now
                        if landing:
                            deadline = None
                    if not landing and deadline is None:
                        deadline = self._time() + self.plan.coalesce_wait
                    budget_left = self.context.coalescing_time_left()
                    remaining = budget_left if deadline is None else min(deadline - self._time(), budget_left)
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

    def _potential(self, chunk_keys: set[str]) -> tuple[bool, bool]:
        """Return whether a page may still join the chunk: now, and once the waves in flight have landed.

        The dispatcher lists the producers that may add a page without one being sent (``_candidates``); the row
        pool says whether one of their pages fits (``_RowBuffer.fits_any``), by the rule it grants rows with. A
        page already on its way has its rows. The second answer also gives back the rows the pages of the waves in
        flight hold until they land.
        """
        state, buffer = self._producer_state, self._buffer
        if state is None or buffer is None or state.closing:
            return False, False
        drain = buffer.drain()
        pages, returning = self._candidates(state, buffer, chunk_keys, drain)
        # A page on its way to the queue arrives without any consumer progress, under either order;
        # one that must still reserve its logical page arrives only if the page budget admits it.
        in_transit = any(count and key in state.indexes for key, count in state.in_transit.items()) or any(
            count and key in state.indexes and self.context.can_reserve_page(reference=key)
            for key, count in state.reserving.items()
        )
        if in_transit or buffer.fits_any(pages, drain=drain, returning=returning):
            return True, True
        landing = sum(buffer.in_flight(state.indexes[key]) for key in self._flying if key in state.indexes)
        return False, bool(landing) and buffer.fits_any(pages, drain=drain, returning=returning + landing)

    def _candidates(
        self, state: _ProducerState, buffer: _RowBuffer, chunk_keys: set[str], drain: _Drain | None
    ) -> tuple[list[tuple[int, int]], int]:
        """Return the pages producers may add without a page sent, and the rows the consumer returns before them.

        Each page is a binding index and its rows. A runnable or admitting producer adds its next page, an
        admitting one needing only the rows it does not hold or have promised yet; under READY so does a
        producer whose next page waits for the consumer to take its last; a counted tail refills its window once
        the consumer returns its slot's rows; the source pull in flight adds its binding's first page. Under
        INPUT each binding the consumer passes frees an admission slot: the next binding from the source arrives
        without any page sent. A producer whose page the operation's page budget refuses adds none.
        """
        budgeted = self.context.can_reserve_page

        def page(key: str) -> tuple[int, int]:
            index = state.indexes[key]
            return index, state.caps.get(key, state.default_cap)

        pages: list[tuple[int, int]] = []
        returning = 0
        for key, status in state.status.items():
            if status is _Producing.WAITING or not budgeted(reference=key):
                continue
            if status is _Producing.REFILLING:
                returned = state.refill_returns.get(key)
                returning += 0 if returned is None else returned.amount
            elif (key in chunk_keys and key not in state.windows) or (
                status is _Producing.AWAITING_CONSUMER and not self._pending_continuations_can_progress
            ):
                # A producer's page in the chunk is its last until the chunk lands, unless it has a window.
                continue
            index, cap = page(key)
            # An admitting page needs only the rows it does not hold or have promised yet.
            pages.append((index, max(0, cap - buffer.reserving(index)) if status is _Producing.ADMITTING else cap))
        if (
            state.next_key is not None
            and state.next_index is not None
            and state.next_key not in chunk_keys
            and state.source_pull_in_flight
            and budgeted(reference=state.next_key)
        ):
            pages.append((state.next_index, state.default_cap))
        if drain is not None and not state.source_exhausted and budgeted():
            first = state.admitted + int(state.source_pull_in_flight and state.next_index is not None)
            pages.extend((first + offset, state.default_cap) for offset in range(drain.passed))
        return pages, returning

    def _fly(self, item: _PendingBatch) -> None:
        """Count the page's producer among those with pages in flight until the page is in the row pool."""
        key = item.reference_id
        self._flying[key] += 1
        item.admitted.add_done_callback(lambda _future: _count_down(self._flying, key))

    def _count_batch_request(self) -> None:
        """Count one physical batch envelope the chunk sends in its first round."""
        self.batch_requests += 1

    async def _send(self, chunk: list[_PendingBatch], progress: BatchProgress) -> None:
        requests = tuple(item.request for item in chunk)
        self.batch_commands += len(chunk)
        options = chunk[0].options
        try:
            outcomes = await self._executor.execute_requests(
                requests,
                context=self.context,
                advisory_totals=options.advisory_totals,
                strict_envelope=options.strict_envelope,
                on_envelope=self._count_batch_request,
                progress=progress,
            )
            self._correlate(chunk, outcomes)
        except asyncio.CancelledError as cancellation:
            if cancelled_here():
                for item in chunk:
                    item.future.cancel()
                raise
            # Nobody cancelled this worker: the wave failed with a cancellation of something it awaited. Its pages
            # fail with it, by what the wave's progress says each command did, and the worker sends the next wave.
            error = CapabilityError("batch wave was cancelled while its operation was running")
            error.__cause__ = cancellation
            for position, item in enumerate(chunk):
                if not item.future.done():
                    item.future.set_exception(_ChunkPageError(error, progress, position))
        except BaseException as error:  # noqa: BLE001 - total chunk correlation boundary, an escape's too
            # Every page fails with the chunk's error and keeps what its own command did, which the chunk's
            # progress says at the page's position, whatever failed: a sent page no longer has a deadline to end
            # its wait. An escape (``is_escape``) is no page's outcome: it is the operation's latch's, and reaches
            # every page's fetch as itself, though the pages a close cancelled no longer deliver it.
            latch_escape(error)
            for position, item in enumerate(chunk):
                if not item.future.done():
                    item.future.set_exception(
                        _ChunkPageError(error, progress, position) if isinstance(error, Exception) else error
                    )

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
                progress, live = BatchProgress(), list[_PendingBatch]()
                for item in chunk:
                    if item.mark_sent(progress, len(live)):
                        live.append(item)
                if live:
                    for item in live:
                        self._fly(item)
                    await self._send(live, progress)
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
