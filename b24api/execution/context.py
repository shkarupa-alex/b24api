"""Transport lifecycle, replay-aware retries, and shared rate coordination."""

from __future__ import annotations
import asyncio
import contextlib
import contextvars
import inspect
import sys
import time
import weakref
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Protocol, TypeGuard, cast
from uuid import uuid4

from b24api.contracts.policy import BudgetCounters, ExecutionPolicy
from b24api.errors import BudgetExceededError, CleanupOverrunError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from b24api.execution.rate import RateCoordinator

    type Clock = Callable[[], float]


# The work tokens of the stream runners whose isolated pull or cleanup owns the current task. Each runs in its
# own task, and every task started while it runs copies its context, so the marker reaches all work done
# on its behalf: a family's workers and the caller code they run (a page-stop callback, a transport, and a
# source, whose own context the source owner gives the marker of each step it runs). A token, not the runner,
# so a task that outlives the stream never keeps it alive.
WORK_OWNERS: ContextVar[tuple[object, ...]] = ContextVar("b24api_work_owners", default=())

REQUEST_BUDGET_EXHAUSTED = "physical request budget exhausted"
LOGICAL_PAGE_BUDGET_EXHAUSTED = "logical page budget exhausted"
# A request's own retry window closed before the operation's deadline: that request alone is refused, whatever the
# work it was refused (``ExecutionContext.time_left``), and the operation goes on.
RETRY_WINDOW_EXHAUSTED = "per-request retry time budget exhausted"
# Where an error carries the mark of the context whose refusal it is (``ExecutionContext.ended_by``).
_ENDS_OPERATION = "_b24api_ends_operation"

# The least time a cleanup step may wait for what it closes: a caller source's close, or the workers and tasks the
# operation cancelled. Cleanup runs when the operation ends, which is often because ``max_elapsed`` ran out, so the
# time left is then zero; a close that ends at once still needs a few loop turns, and a synchronous source's close a
# worker-thread hop. The grace lets such a close finish, so the budget's end is never reported as a cleanup failure,
# while a close that really blocks overruns the budget by at most this much per step.
CLEANUP_GRACE_SECONDS = 0.1

# How long a close, or the end of a stream a read runs, defers a cancellation of its caller: a short wait of its own,
# not the operation's time left (which may be the whole ``max_elapsed``), so a cancelled close returns promptly. Cleanup
# still running then goes on in the background, reported as a cleanup failure (``CleanupOverrunError``) behind the
# cancellation, which is what the close raises: the cancellation caused the overrun.
CANCEL_GRACE_SECONDS = 1.0


@dataclass(frozen=True, slots=True)
class ExecutionSnapshot:
    """Operation-local attempt, retry, and cooldown counters."""

    counters: BudgetCounters
    retries: int
    cooldown_seconds: float
    elapsed: float
    ambiguous_physical_attempts: int = field(default=0, kw_only=True)
    batch_isolation_requests: int = field(default=0, kw_only=True)


@dataclass(frozen=True, slots=True)
class PageReservation:
    """One reserved logical page, charged by ``commit_page`` or returned by ``release_page``."""

    sequence: int
    reference: str | None


@dataclass(slots=True)
class _ReferencePages:
    """The page ledger of one reference binding: committed pages, reservations in flight, and closure.

    An entry exists only while it carries state: it is created by the binding's first reservation and
    dropped once nothing is in flight and either nothing was committed or the binding has closed. A
    closed binding reserves no further pages, so its committed count is no longer needed.
    """

    committed: int = 0
    pending: int = 0
    closed: bool = False

    @property
    def settled(self) -> bool:
        """Return whether the entry can be dropped without losing a count any ceiling still needs."""
        return self.pending == 0 and (self.closed or self.committed == 0)


class ExecutionContext:
    """Mutable operation-scoped budget ledger with immutable snapshots.

    The ledger is plain counters and one entry per open reference binding, so every reservation,
    settlement and admission check is O(1); the validated ``BudgetCounters`` value is built only by
    ``snapshot()``. Every state change is synchronous code between awaits, so the event loop
    serializes it and no lock is needed; a mutator must never await in the middle of an update.
    """

    def __init__(
        self,
        policy: ExecutionPolicy,
        coordinator: RateCoordinator,
        *,
        clock: Clock = time.monotonic,
    ) -> None:
        """Initialize instance state."""
        self.policy = policy
        self.coordinator = coordinator
        self._clock = clock
        self._start: float | None = None
        self._physical_requests = 0
        # The first refusal of work by the request budget or the operation's deadline (a send, a page, an admission
        # or a pull), which ends the operation: neither budget comes back, so no later work can be admitted either.
        self._refusal: str | None = None
        # Marks the errors this context raises as that refusal, so a refusal is told by its mark, not by its text. The
        # mark is a value unique to this context, so it survives a copy or a pickle round trip of the error with it.
        self._refusal_mark = f"operation-refusal-{uuid4().hex}"
        self._logical_pages = 0
        self._buffered_rows = 0
        self._buffered_rows_high_water = 0
        self._reference_pages: dict[str, _ReferencePages] = {}
        self._retries = 0
        self._cooldown_seconds = 0.0
        self._ambiguous_physical_attempts = 0
        self._batch_isolation_requests = 0
        self._page_sequence = 0
        self._page_reservations: dict[PageReservation, None] = {}
        self._retained_identity_keys = 0
        self._page_changed = asyncio.Event()

    @property
    def execution_context(self) -> ExecutionContext:
        """Return this context: a flat traversal plans and executes on the operation context itself."""
        return self

    @property
    def elapsed(self) -> float:
        """Return the elapsed."""
        if self._start is None:
            return 0.0
        return max(0.0, self._clock() - self._start)

    @property
    def refusal(self) -> str | None:
        """Return the first refusal of work by the operation's budget, which ends the operation, or ``None``.

        It is the refusal's message: the request budget's (``REQUEST_BUDGET_EXHAUSTED``), or the deadline's, which
        names what the deadline refused, or the page budget's (``LOGICAL_PAGE_BUDGET_EXHAUSTED``). Only a refusal sets
        it: a budget spent exactly by the last send the
        operation needed, or a deadline that passes while the application holds control once nothing is left to
        admit, ends nothing. The reference scheduler reads its source ahead of the sends, so it may have read the
        source out before the refusal: once it is set, every later pull is refused, which ends its source, and once
        the bindings read are settled, the scheduler ends the operation with it.
        """
        return self._refusal

    def time_left(self, refusal: str, *, retry_started: float | None = None) -> float:
        """Return the time left: the operation's, or a request's retry window from ``retry_started`` if shorter.

        With none left, raise the refusal. When the operation's deadline closed it, the refusal names the work
        ``refusal`` names, and is recorded (``refusal``) and ends the operation; when the request's own retry window
        closed first, it is ``RETRY_WINDOW_EXHAUSTED``, refuses that request alone and records nothing. With
        ``deadline``, the one way to ask for the time left, so time refuses work only here; ``coalescing_time_left``
        alone reads it without refusing, for a wait that ends in a send, never in a refusal.
        """
        left = self._time_left(retry_started)
        if left > 0:
            return left
        raise self._refuse(refusal, retry_started)

    def coalescing_time_left(self) -> float:
        """Return the operation's time left, which may be none, without refusing anything.

        The one reader of the time left that does not refuse, for the batch dispatcher's coalescing wait: when the
        time runs out it stops waiting for more commands and sends the chunk it holds, which the send's own permit
        then admits or refuses (``time_left``).
        """
        return self._time_left(None)

    @contextlib.asynccontextmanager
    async def deadline(self, refusal: str, *, retry_started: float | None = None) -> AsyncIterator[asyncio.Timeout]:
        """Run the block within the time left (``time_left``), refusing as ``time_left`` refuses.

        The refusal is raised before the block when no time is left, and in place of the ``TimeoutError`` that
        ends the block when the time runs out while it waits: its own, or that of a wait the block handed the same
        deadline to (a rate coordinator's admission budget). The block is given its timeout, which a block whose
        wait ends in work the deadline must not cut (a sent batch wave) lifts with ``reschedule(None)``.
        """
        left = self.time_left(refusal, retry_started=retry_started)
        try:
            async with asyncio.timeout(left) as timeout:
                yield timeout
        except TimeoutError as error:
            raise self._refuse(refusal, retry_started) from error

    def raise_if_refused(self) -> None:
        """Raise the operation's recorded refusal (``refusal``) again, if there is one."""
        if self._refusal is not None:
            raise self.operation_refusal(self._refusal)

    def operation_refusal(self, message: str) -> BudgetExceededError:
        """Return an error that ends the operation with its refusal (``ended_by``); only a refusal site builds one.

        Every work the budget refuses once it has refused work is refused as the operation's end, whatever its
        message names (the permit, a page's rows, an admission, the source pull): the message names the work, the
        mark says whose end it is.
        """
        error = BudgetExceededError(message)
        error.__dict__[_ENDS_OPERATION] = self._refusal_mark
        return error

    def ended_by(self, error: object) -> TypeGuard[BudgetExceededError]:
        """Return whether ``error`` is this operation's refusal, which ends it: one this context made as such.

        Any other budget error, such as a request's own retry window closing or a result the row budget dropped,
        fails its work alone, also once the operation has recorded a refusal.
        """
        return isinstance(error, BudgetExceededError) and getattr(error, _ENDS_OPERATION, None) == self._refusal_mark

    def _refuse(self, refusal: str, retry_started: float | None) -> BudgetExceededError:
        start = self._start
        if (
            retry_started is not None
            and start is not None
            and retry_started + self.policy.max_retry_elapsed_per_request < start + self.policy.max_elapsed
        ):
            return BudgetExceededError(RETRY_WINDOW_EXHAUSTED)
        self._record_refusal(refusal)
        return self.operation_refusal(refusal)

    def _record_refusal(self, message: str) -> None:
        if self._refusal is None:
            self._refusal = message

    def admit_request(self) -> None:
        """Refuse a send once the request budget is spent, recording the refusal that ends the operation.

        The executor asks before a send queues for its permit, so a spent budget never waits for a cooldown just to
        refuse, and again when it reserves the attempt.
        """
        if self._physical_requests >= self.policy.max_requests:
            self._record_refusal(REQUEST_BUDGET_EXHAUSTED)
            raise self.operation_refusal(REQUEST_BUDGET_EXHAUSTED)

    def planning_rows(self) -> int:
        """Return the buffered rows a keyset plan may use: the operation's whole row budget."""
        return self.policy.max_buffered_rows

    async def start(self) -> None:
        """Start the operation clock exactly once when execution first begins, on the loop its client serves.

        An operation started from another event loop is refused before it does any work: ``RuntimeError``
        (``RateCoordinator.bind_loop``), which the operation raises as it is, never as a command's or a binding's
        outcome.
        """
        self.coordinator.bind_loop()
        if self._start is None:
            self._start = self._clock()

    async def reserve_attempt(self, *, attempts_for_request: int) -> None:
        """Reserve the attempt budget; the send's dispatch check just before it (``time_left``) is its time check."""
        self.admit_request()
        if attempts_for_request >= self.policy.max_attempts_per_request:
            raise BudgetExceededError("per-request attempt budget exhausted")
        self._physical_requests += 1

    def release_attempt(self) -> None:
        """Release a reserved attempt whose transport refused it before it left the process: no physical request.

        The send held its reservation until its transport returned, so a concurrent send the spent request budget
        refused meanwhile stays refused. With no attempt reserved there is nothing to release: that is a misuse.
        """
        if self._physical_requests == 0:
            raise RuntimeError("no reserved attempt to release")
        self._physical_requests -= 1

    async def record_retry(self) -> None:
        """Record the retry."""
        self._retries += 1

    async def record_cooldown(self, seconds: float) -> None:
        """Record the cooldown."""
        self._cooldown_seconds += max(0.0, seconds)

    def record_ambiguous_physical_attempt(self) -> None:
        """Count one dispatched batch envelope that left its commands without attributable outcomes."""
        self._ambiguous_physical_attempts += 1

    def record_batch_isolation_requests(self, count: int) -> None:
        """Count individual requests sent to attribute commands an ambiguous batch envelope left unresolved."""
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise ValueError("isolation request count must be a non-negative integer")
        self._batch_isolation_requests += count

    async def reserve_page(self, *, reference: str | None = None) -> PageReservation:
        """Reserve page capacity before I/O without charging the response counter."""
        while True:
            self._check_page_budget(reference)
            if self.can_reserve_page(reference=reference):
                reservation = PageReservation(self._page_sequence, reference)
                self._page_sequence += 1
                self._page_reservations[reservation] = None
                if reference is not None:
                    self._reference_pages.setdefault(reference, _ReferencePages()).pending += 1
                return reservation
            self._page_changed.clear()
            async with self.deadline("page reservation exceeded operation time budget"):
                await self._page_changed.wait()

    def can_reserve_page(self, *, reference: str | None = None) -> bool:
        """Return whether one page reservation can be admitted immediately."""
        if self._logical_pages + len(self._page_reservations) >= self.policy.max_pages:
            return False
        if reference is None:
            return True
        entry = self._reference_pages.get(reference)
        return entry is None or entry.committed + entry.pending < self.policy.max_pages_per_reference

    async def reserve_pages(self, count: int) -> tuple[PageReservation, ...]:
        """Atomically reserve a fixed operation-local page wave, waiting while pages in flight may still come back.

        A wave the committed pages leave no room for never fits, as committed pages never come back: the operation's
        page budget refuses it, which ends the operation as a spent budget does (``_check_page_budget``).
        """
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise ValueError("page reservation count must be a positive integer")
        self.time_left("page reservation exceeded operation time budget")
        while True:
            if self._logical_pages + count > self.policy.max_pages:
                raise self._page_budget_refusal()
            if self._logical_pages + len(self._page_reservations) + count <= self.policy.max_pages:
                reservations = tuple(PageReservation(self._page_sequence + offset, None) for offset in range(count))
                self._page_sequence += count
                self._page_reservations.update(dict.fromkeys(reservations))
                return reservations
            self._page_changed.clear()
            async with self.deadline("page reservation exceeded operation time budget"):
                await self._page_changed.wait()

    def commit_page(self, reservation: PageReservation) -> None:
        """Atomically charge one decoded response with no cancellation point."""
        if reservation not in self._page_reservations:
            raise RuntimeError("page reservation is not active")
        self._check_page_budget(reservation.reference)
        self._page_reservations.pop(reservation)
        self._logical_pages += 1
        if reservation.reference is not None:
            entry = self._reference_pages[reservation.reference]
            entry.pending -= 1
            entry.committed += 1
            self._drop_settled(reservation.reference, entry)
        self._page_changed.set()

    def release_page(self, reservation: PageReservation) -> None:
        """Atomically release capacity when no decoded response was returned."""
        if reservation not in self._page_reservations:
            return
        self._page_reservations.pop(reservation)
        if reservation.reference is not None:
            entry = self._reference_pages[reservation.reference]
            entry.pending -= 1
            self._drop_settled(reservation.reference, entry)
        self._page_changed.set()

    def close_reference(self, reference: str) -> None:
        """Forget a closed binding's committed pages; it must reserve no further page under ``reference``.

        A reservation still in flight keeps the entry until it is committed or released, so both
        ceilings stay exact while the ledger holds state only for open bindings.
        """
        entry = self._reference_pages.get(reference)
        if entry is not None:
            entry.closed = True
            self._drop_settled(reference, entry)

    def _check_page_budget(self, reference: str | None) -> None:
        """Refuse a page once the operation or the reference has committed its whole page budget.

        The operation's page budget, once spent, never comes back, so its refusal ends the operation as the request
        budget's does (``refusal``); a reference's own page budget refuses that reference alone.
        """
        if self._logical_pages >= self.policy.max_pages:
            raise self._page_budget_refusal()
        entry = None if reference is None else self._reference_pages.get(reference)
        if entry is not None and entry.committed >= self.policy.max_pages_per_reference:
            raise BudgetExceededError("per-reference page budget exhausted")

    def _page_budget_refusal(self) -> BudgetExceededError:
        """Record and return the operation's page budget refusal, which ends the operation."""
        self._record_refusal(LOGICAL_PAGE_BUDGET_EXHAUSTED)
        return self.operation_refusal(LOGICAL_PAGE_BUDGET_EXHAUSTED)

    def _drop_settled(self, reference: str, entry: _ReferencePages) -> None:
        if entry.settled:
            del self._reference_pages[reference]

    async def set_buffered_rows(self, rows: int) -> None:
        """Record retained decoded rows and enforce the global buffer ceiling."""
        self._hold_buffered_rows(rows)

    async def adjust_buffered_rows(self, delta: int) -> None:
        """Atomically account concurrent scheduler buffers by a signed delta."""
        if not isinstance(delta, int) or isinstance(delta, bool):
            raise TypeError("buffer delta must be an integer")
        self._hold_buffered_rows(self._buffered_rows + delta)

    def _hold_buffered_rows(self, rows: int) -> None:
        if rows < 0:
            raise ValueError("buffered row count cannot be negative")
        if rows > self.policy.max_buffered_rows:
            raise BudgetExceededError("buffered row budget exhausted")
        self._buffered_rows = rows
        self._buffered_rows_high_water = max(rows, self._buffered_rows_high_water)

    def ensure_identity_capacity(self, additional: int) -> None:
        """Reject a page that would exceed the operation-wide identity budget."""
        if not isinstance(additional, int) or isinstance(additional, bool) or additional < 0:
            raise ValueError("additional identity keys must be a non-negative integer")
        if self._retained_identity_keys + additional > self.policy.max_identity_keys:
            raise BudgetExceededError("identity evidence budget exhausted")

    def retain_identity_key(self) -> None:
        """Charge one identity after its page-wide capacity check succeeds."""
        self.ensure_identity_capacity(1)
        self._retained_identity_keys += 1

    def release_identity_keys(self, count: int) -> None:
        """Release identities owned by one closing traversal binding."""
        if not isinstance(count, int) or isinstance(count, bool) or not 0 <= count <= self._retained_identity_keys:
            raise RuntimeError("identity evidence accounting is inconsistent")
        self._retained_identity_keys -= count

    def cleanup_window(self) -> float:
        """Return how long a cleanup step starting now may wait: the time left, but at least the cleanup grace."""
        return max(self.policy.max_elapsed - self.elapsed, CLEANUP_GRACE_SECONDS)

    def _time_left(self, retry_started: float | None) -> float:
        """Return the tighter operation or retry time remaining; before its window opens, only the operation's."""
        operation = self.policy.max_elapsed - self.elapsed
        if retry_started is None:
            return operation
        return min(operation, self.policy.max_retry_elapsed_per_request - max(0.0, self._clock() - retry_started))

    async def snapshot(self) -> ExecutionSnapshot:
        """Return the current immutable snapshot."""
        return ExecutionSnapshot(
            counters=BudgetCounters(
                physical_requests=self._physical_requests,
                logical_pages=self._logical_pages,
                pages_per_reference=tuple(
                    (reference, entry.committed)
                    for reference, entry in self._reference_pages.items()
                    if entry.committed
                ),
                buffered_rows=self._buffered_rows,
                buffered_rows_high_water=self._buffered_rows_high_water,
            ),
            retries=self._retries,
            cooldown_seconds=self._cooldown_seconds,
            elapsed=self.elapsed,
            ambiguous_physical_attempts=self._ambiguous_physical_attempts,
            batch_isolation_requests=self._batch_isolation_requests,
        )


class OwnedPull[T](Protocol):
    """An owned source: pulls raise ``StopAsyncIteration`` at the end and close never raises."""

    async def __anext__(self) -> T:
        """Pull one item."""
        ...

    async def aclose(self) -> CleanupResult:
        """Close once and return the outcome."""
        ...


def is_interrupt(error: BaseException | None) -> bool:
    """Whether ``error`` interrupts the program rather than fails work: a ``KeyboardInterrupt`` or ``SystemExit``.

    These are what the event loop raises out of ``asyncio.run`` from a callback; it logs and drops any other
    exception there.
    """
    return isinstance(error, KeyboardInterrupt | SystemExit)


def is_escape(error: BaseException | None) -> bool:
    """Whether ``error`` is no failure and no cancellation: the operation's latch's (``OperationLatch``).

    Only an ``Exception`` is a failure. An interrupt (``is_interrupt``) is an escape, and so is any other
    ``BaseException`` but a cancellation and a ``GeneratorExit`` (a coroutine being closed): it propagates as it is,
    never caught as a failure, wrapped, ranked or made an outcome.
    """
    return error is not None and not isinstance(error, Exception | asyncio.CancelledError | GeneratorExit)


class LatchState(Enum):
    """Where an operation's ``OperationLatch`` stands: running, ending, or ended."""

    # The operation runs: the first interrupt latched wakes its owner, which ends the operation at once.
    ARMED = "armed"
    # The operation is cleaning up: an interrupt latched now is raised at its end, waking nothing (its cleanup runs
    # then, an inline kernel's in the very read a wake would cancel).
    ENDING = "ending"
    # The end is out: what it raised is raised, and anything latched from now on goes to the event loop.
    CLOSED = "closed"


class OperationLatch:
    """Every escape (``is_escape``) an operation's own code caught, wherever it caught it, until its end raises it.

    Library code that catches an escape in an operation's work either raises it at once or hands it to the latch
    (``latch_escape``); there is no third way. Having handed it on, it goes on as with any end: turns it into a
    binding's end, a page's failure or a task's ``Settled`` result, or leaves it on a future a close cancelled.
    ``settle`` hands on whatever a library task ends in, so every task the library starts around caller code is
    such a site.

    The first interrupt ends the operation: latched while it runs, it wakes its owner. Any other escape, a second
    interrupt included, stays undelivered until the operation's end. That end (``close``) raises the interrupt, ahead
    of anything else, or else the first undelivered escape, once, as from a ``finally``. The escapes it does not
    raise, and any caught once it is out, go to the event loop (``hand_on``), once each. The operation raises one
    interrupt at most: one caught after an end that raised none is raised out of ``asyncio.run`` as any task's is,
    and every other escape, a second interrupt included, goes to the loop's exception handler, with the operation's
    report. An operation nothing ends (a stream left unclosed when ``asyncio.run`` shuts down, or dropped) reports
    every escape still undelivered to that handler as the latch is collected, as asyncio reports a task's exception
    nobody retrieved. So no route an escape takes (an event, a future, a task's result, a cancelled pull) loses it
    silently.
    """

    __slots__ = ("_loop", "_out", "_undelivered", "_wake", "first", "report", "state")

    def __init__(self, wake: Callable[[], None] | None = None) -> None:
        """Hold no escape yet; ``wake`` ends the operation at once when an interrupt is latched while it runs."""
        self.first: BaseException | None = None
        self.state = LatchState.ARMED
        # The operation's published report, for an escape handed to the event loop after it.
        self.report: object | None = None
        self._wake = wake
        # The escapes caught and not raised yet, in the order they were caught: one each catcher records.
        self._undelivered: list[BaseException] = []
        # The loop the first undelivered escape was caught on, whose exception handler the latch's collection uses.
        self._loop: asyncio.AbstractEventLoop | None = None
        # The escapes that are out, each once however many catchers record it: raised by the operation's end, or
        # handed to the event loop.
        self._out: list[BaseException] = []

    def end(self) -> None:
        """The operation is ending: an interrupt latched from now on is raised at its end, waking nothing."""
        if self.state is LatchState.ARMED:
            self.state = LatchState.ENDING

    def close(self) -> BaseException | None:
        """The operation's end: return what it raises, and hand every other undelivered escape to the event loop.

        That is the interrupt, which every end raises (a read a concurrent close ended raises it too), or else the
        first undelivered escape, which only the first end raises. Nothing is latched from now on: the latch
        outlives the operation in the context of any task its work started, so it lets go of its owner.
        """
        self.state = LatchState.CLOSED
        self._wake = None
        undelivered, self._undelivered = self._undelivered, []
        raised = self.first
        if raised is None and undelivered:
            raised = undelivered.pop(0)
        if raised is not None and not self._is_out(raised):
            self._out.append(raised)
        for error in undelivered:
            self.hand_on(error)
        return raised

    def close_to_loop(self, *, raised_by_read: bool = False) -> None:
        """The operation's end with no read or close to raise to: what it raises (``close``) leaves the event loop.

        The wake calls it once ``record`` latched the interrupt (``first``), which ``close`` returns; it is raised out
        of ``asyncio.run`` from a callback. So is the end of an operation whose close can raise nothing (its coroutine
        closed), unless that interrupt is out already, or a read the close ended raises it (``raised_by_read``). Any
        other escape goes to the loop's exception handler, as ``hand_on`` hands it.
        """
        interrupt_out = self.first is not None and self._is_out(self.first)
        escape = self.close()
        if escape is None or (is_interrupt(escape) and (interrupt_out or raised_by_read)):
            return
        self._deliver(escape, raise_it=is_interrupt(escape))

    def record(self, error: BaseException | None) -> None:
        """Latch ``error`` when it is an escape; anything else is not this latch's.

        Once the operation's end is out, an escape goes to the event loop instead, unless the end raised it.
        """
        if error is None or not is_escape(error) or error is self.first:
            return
        if self.state is LatchState.CLOSED:
            self.hand_on(error)
        elif is_interrupt(error) and self.first is None:
            self.first = error
            if self.state is LatchState.ARMED and self._wake is not None:
                self._wake()
        else:
            # Any other escape, a second interrupt included, waits for the end, which hands on what it does not raise.
            self._loop = self._loop or asyncio.get_running_loop()
            self._undelivered.append(error)

    def hand_on(self, error: BaseException) -> None:
        """Hand ``error`` to the event loop, once: no read or close of the operation will raise it.

        The operation raises one interrupt at most: the loop raises an interrupt out of ``asyncio.run`` from a callback
        only while none of the operation's is out yet (its end raised none). Raising another would stop the loop again
        while the caller unwinds from the first (its client's close, say), so every other escape, a second interrupt
        included, is reported to the loop's exception handler.
        """
        if self._is_out(error):
            return
        raise_it = is_interrupt(error) and not any(is_interrupt(out) for out in self._out)
        self._out.append(error)
        self._deliver(error, raise_it=raise_it)

    def __del__(self) -> None:
        """Report every escape no end raised to its loop's exception handler: nothing ended the operation.

        As asyncio reports a task's exception nobody retrieved, it reports and never raises, an interrupt included,
        on a closed loop too. The interrupt that ends the operation (``first``) is not among them: its wake delivers
        it at once (a read in flight raises it, or the event loop, ``close_to_loop``), one latched while the operation
        ends is raised by that end (or by the event loop, when the end's claimant can raise nothing), and it also
        leaves on its own through an inline kernel's read that ends in it, so reporting it here could deliver it twice.
        """
        if self._loop is not None:
            for error in self._undelivered:
                if not self._is_out(error):
                    self._out.append(error)
                    self._report(error, self._loop)

    def _is_out(self, error: BaseException) -> bool:
        """Return whether ``error`` is out already: raised by an end, or handed to the event loop."""
        return any(out is error for out in self._out)

    def _deliver(self, error: BaseException, *, raise_it: bool) -> None:
        """Raise ``error`` out of the running loop from a callback, or report it to the loop's exception handler."""
        loop = asyncio.get_running_loop()
        if raise_it:
            loop.call_soon(_raise, error)
        else:
            self._report(error, loop)

    def _report(self, error: BaseException, loop: asyncio.AbstractEventLoop) -> None:
        """Report ``error`` to ``loop``'s exception handler, with the operation's report."""
        context: dict[str, object] = {
            "message": "b24api: an operation could no longer deliver this exception to a read or a close",
            "exception": error,
        }
        if self.report is not None:
            context["report"] = self.report
        loop.call_exception_handler(context)


def _raise(error: BaseException) -> None:
    """Raise ``error`` from an event loop callback: an interrupt leaves the loop from there, out of ``asyncio.run``."""
    raise error


# The latch of the operation whose work runs the current code: its runner sets it in its read and cleanup tasks, and
# every task they start inherits it.
OPERATION_LATCH: ContextVar[OperationLatch | None] = ContextVar("b24api_operation_latch", default=None)


def outside_operation_work() -> contextvars.Context:
    """Return a copy of the current context that belongs to no stream's work: no work marker, no operation latch.

    For work the library starts on behalf of no stream, such as the client's close, which decided ownership already.
    """
    context = contextvars.copy_context()
    context.run(WORK_OWNERS.set, ())
    context.run(OPERATION_LATCH.set, None)
    return context


def latch_escape(error: BaseException) -> None:
    """Hand ``error`` to the current operation's ``OperationLatch`` when it is an escape (``is_escape``)."""
    if (latch := OPERATION_LATCH.get()) is not None:
        latch.record(error)


class Settled[T]:
    """How a library task's work ended, returned as the task's result: a value, or the failure its owner raises.

    A task the library starts around caller code (a source's pull or close, a stream's read or cleanup) returns this
    instead of ending in the failure. ``asyncio.run`` reports every task that its shutdown cancelled and that then
    ended in an exception, whether or not its owner retrieves it later, so only a task that never ends in one keeps
    the failure its owner's alone. That includes ``KeyboardInterrupt`` and ``SystemExit``: a task ending in one
    stops the event loop from inside that task, leaving the owner waiting. An escape (``is_escape``) is latched on the
    operation besides (``OperationLatch``), which raises it as its end whatever the owner does with the result, or
    hands it to the event loop once the operation has ended.
    """

    __slots__ = ("error", "value")

    def __init__(self, value: T | None = None, *, error: BaseException | None = None) -> None:
        """Hold the work's value, or the failure it ended in."""
        self.value = value
        self.error = error

    def unwrap(self) -> T:
        """Return the value, or raise the failure."""
        if self.error is not None:
            raise self.error
        return cast("T", self.value)


async def settle[T](work: Awaitable[T]) -> Settled[T]:
    """Await ``work`` and return how it ended (``Settled``); a cancellation still ends the task cancelled.

    A ``GeneratorExit`` is no way the work ended: the task's coroutine is being closed (a loop that stopped leaves it
    to the garbage collector), so it passes through.
    """
    try:
        return Settled(await work)
    except (asyncio.CancelledError, GeneratorExit):
        raise
    except BaseException as error:  # noqa: BLE001 - returned for the task's owner to raise: the task ends without it
        latch_escape(error)
        settled: Settled[T] = Settled(error=error)
    # The work's cleanup may have re-armed a cancellation it absorbed on this task, which would end it cancelled and
    # lose the failure. Whoever cancelled it learns how it ended from its owner, so the task takes the cancellation.
    if (task := asyncio.current_task()) is not None and task.cancelling():
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(0)
    return settled


def spawn_settled[T](work: Awaitable[T], *, context: contextvars.Context | None = None) -> asyncio.Task[Settled[T]]:
    """Start ``work`` as a task that returns how it ended (``Settled``) and never ends in a failure.

    ``context`` runs it in that context instead of a copy of the current one.
    """
    task = asyncio.get_running_loop().create_task(settle(work), context=context)
    if inspect.iscoroutine(work):
        # A task cancelled before its first step never starts ``settle``, which would leave ``work`` never awaited;
        # once the task is done, ``work`` has finished or never started, so closing it is safe.
        task.add_done_callback(lambda _task: work.close())
    return task


async def finish_in_thread[R](call: Callable[[], R]) -> R:
    """Run ``call`` in a worker thread that a cancellation waits for instead of abandoning.

    The cancelled call's failure is dropped. An escape (``is_escape``) it ends in is not: its task returns it
    (``settle``) in the operation's context, so the operation's latch has it (``OperationLatch``).
    """
    work = spawn_settled(asyncio.to_thread(call))
    try:
        return (await asyncio.shield(work)).unwrap()
    except asyncio.CancelledError:
        with contextlib.suppress(asyncio.CancelledError):
            await work
        raise


def own_failure[T](task: asyncio.Task[Settled[T]]) -> Exception | None:
    """Return the failure a settled task has already ended in on its own: not cancelled, not a source's end.

    A wait for such a task that is then stopped (a close cancels it) ends in that failure, not in the stop: the
    failure came first, so it is the waiter's, and no cleanup failure of the stop.
    """
    if not task.done() or task.cancelled():
        return None
    error = task.result().error
    return error if isinstance(error, Exception) and not isinstance(error, StopAsyncIteration) else None


class AsyncIteratorController[T]:
    """Own one async source pull and retain cleanup past a public deadline."""

    def __init__(
        self,
        source: OwnedPull[T],
        *,
        input_error: str,
        cleanup_error: str,
    ) -> None:
        """Own one source iterator and bound its pulls and cleanup."""
        self._source = source
        self._input_error = input_error
        self._cleanup_error = cleanup_error
        # Both run caller code; neither ends in its failure (``spawn_settled``).
        self._active_pull: asyncio.Task[Settled[T]] | None = None
        self._cleanup_task: asyncio.Task[Settled[None]] | None = None
        # Set once a pull ends the source: its end is read once, and no budget can end what has nothing left.
        self._exhausted = False

    async def get(self, context: ExecutionContext) -> T:
        """Pull one item within the operation's budget: its time left, and a budget that refused nothing yet.

        Once a pull has ended the source, every later one raises ``StopAsyncIteration`` again without reading the
        source or the budget.
        """
        if self._active_pull is not None:
            raise RuntimeError("source pull is already active")
        if self._cleanup_task is not None:
            raise RuntimeError("source cleanup has already started")
        if self._exhausted:
            raise StopAsyncIteration
        # No item pulled once the budget refused work could be sent: the source ends with the operation's refusal.
        context.raise_if_refused()
        pull: asyncio.Task[Settled[T]] | None = None
        try:
            async with context.deadline(self._input_error):
                pull = spawn_settled(anext(self._source))
                self._active_pull = pull
                await asyncio.wait((pull,))
        except BaseException as error:
            # A source that failed before the wait was stopped failed on its own (``own_failure``): the pull ends in
            # that failure, below.
            if is_escape(error) or pull is None or own_failure(pull) is None:
                if pull is not None:
                    if not pull.done():
                        pull.cancel()
                    self._retain_close_after(pull)
                raise
        try:
            settled = await pull
        finally:
            self._active_pull = None
        if isinstance(settled.error, StopAsyncIteration):
            self._exhausted = True
        return settled.unwrap()

    async def aclose(self, context: ExecutionContext) -> None:
        """Close the source, waiting for it within the operation's cleanup window.

        A close still running when the window ends goes on in the background, retained here, and the
        wait raises the cleanup error.
        """
        if self._cleanup_task is None:
            active = self._active_pull
            if active is None:
                self._cleanup_task = spawn_settled(self._close_source())
            else:
                active.cancel()
                self._retain_close_after(active)
        cleanup = self._cleanup_task
        if cleanup is None:
            return
        done, _ = await asyncio.wait((cleanup,), timeout=context.cleanup_window())
        if done:
            (await cleanup).unwrap()
            return
        raise CleanupOverrunError(self._cleanup_error)

    def _retain_close_after(self, pull: asyncio.Task[Settled[T]]) -> None:
        if self._cleanup_task is None:
            self._cleanup_task = spawn_settled(self._close_after(pull))

    async def _close_after(self, pull: asyncio.Task[Settled[T]]) -> None:
        pull_error: BaseException | None = None
        with contextlib.suppress(asyncio.CancelledError):
            pull_error = (await pull).error
        if isinstance(pull_error, StopAsyncIteration):
            pull_error = None
        if self._active_pull is pull:
            self._active_pull = None
        try:
            await self._close_source()
        except BaseException as close_error:
            # A close failure that already chains to its own cause (the source's exception) keeps it.
            if pull_error is not None and close_error.__cause__ is None:
                raise close_error from pull_error
            raise
        if pull_error is not None:
            raise pull_error

    async def _close_source(self) -> None:
        (await self._source.aclose()).raise_failure()


def _observe_background_cleanup(task: asyncio.Future[object]) -> None:
    """Retrieve late exceptions while preserving them for any later await."""
    if not task.cancelled():
        task.exception()


async def await_cancellation_resistant(awaitable: Awaitable[object]) -> asyncio.CancelledError | None:
    """Finish an owned state transition and return the last concurrent cancellation."""
    task = _owned_task(awaitable)
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.wait((task,), return_when=asyncio.ALL_COMPLETED)
        except asyncio.CancelledError as error:
            cancellation = error
    try:
        _ended(await task)
    except asyncio.CancelledError:
        if cancellation is not None:
            return cancellation
        raise
    return cancellation


def _owned_task(work: Awaitable[object]) -> asyncio.Future[object]:
    """Run ``work`` as a task of its own that never ends in its failure, unless it already is a task."""
    if isinstance(work, asyncio.Future):
        return work
    return cast("asyncio.Future[object]", spawn_settled(work))


def _ended(result: object) -> None:
    """Raise the failure an owned task returned (``Settled``) instead of ending in it."""
    if isinstance(result, Settled):
        result.unwrap()


@dataclass(frozen=True, slots=True)
class CleanupResult:
    """What owned cleanup reported: a concurrent caller cancellation and its own failure."""

    cancellation: asyncio.CancelledError | None
    error: BaseException | None
    # Cleanup a deadline stopped waiting for, still running; its owner must keep a reference to it.
    unfinished: asyncio.Future[object] | None = field(default=None, kw_only=True)

    def raise_failure(self) -> None:
        """Raise the deferred cancellation first, then the cleanup failure."""
        if self.cancellation is not None:
            raise self.cancellation
        if self.error is not None:
            raise self.error


# When the grace of each cancellation a cleanup deferred ends (``_grace_end``).
_GRACE_ENDS: weakref.WeakKeyDictionary[asyncio.CancelledError, float] = weakref.WeakKeyDictionary()


def _grace_end(cancellation: asyncio.CancelledError, grace: float) -> float:
    """Return when ``cancellation``'s grace ends: from the first cleanup that defers it, however many defer it after.

    One cancellation passes through every close it unwinds (a stream's ``async with``, then the client's): together
    they defer it for one grace, not one each. A cancellation a cleanup replayed (``rearm_cancellation``) arrives as a
    new ``CancelledError``, a new delivery with a grace of its own.
    """
    if (end := _GRACE_ENDS.get(cancellation)) is not None:
        return end
    return _share_grace(cancellation, asyncio.get_running_loop().time() + grace)


def _share_grace(cancellation: asyncio.CancelledError, end: float) -> float:
    """Give ``cancellation`` the grace ending at ``end``, unless a cleanup deferred it already; return its end."""
    return _GRACE_ENDS.setdefault(cancellation, end)


def grace_left(cancellation: asyncio.CancelledError, grace: float) -> float:
    """Return what is left of ``cancellation``'s grace (``_grace_end``), starting it if no wait deferred it yet."""
    return max(0.0, _grace_end(cancellation, grace) - asyncio.get_running_loop().time())


def share_grace(source: asyncio.CancelledError, target: asyncio.CancelledError) -> None:
    """Give ``target``, raised for ``source`` (by work ``source`` cancelled), what is left of ``source``'s grace."""
    if (end := _GRACE_ENDS.get(source)) is not None:
        _share_grace(target, end)


async def await_cleanup_resistant(
    awaitable: Awaitable[object],
    *,
    grace: float | None = None,
    cancelled: asyncio.CancelledError | None = None,
) -> CleanupResult:
    """Finish owned cleanup without losing a concurrent caller cancellation.

    ``grace`` (seconds) bounds how long a cancellation is deferred, counted from when a cleanup first defers it: one
    that arrives here, or one the caller already holds (``cancelled``), whose grace may have begun at an earlier close
    (``_grace_end``). Cleanup still running when the grace ends is left to finish in the background, returned as
    ``unfinished`` with a ``CleanupOverrunError``. Without a cancellation, or without a grace, cleanup is awaited to
    its end.
    """
    task = _owned_task(awaitable)
    cancellation: asyncio.CancelledError | None = None
    loop = asyncio.get_running_loop()
    deadline = _grace_end(cancelled, grace) if grace is not None and cancelled is not None else None
    while not task.done():
        timeout = None if deadline is None else max(0.0, deadline - loop.time())
        try:
            await asyncio.wait((task,), timeout=timeout)
        except asyncio.CancelledError as error:
            cancellation = error
            if grace is not None:
                # A cancellation that arrives while another's grace runs ends with it: the cleanup waits no longer.
                deadline = _grace_end(error, grace) if deadline is None else _share_grace(error, deadline)
            continue
        if not task.done():
            task.add_done_callback(_observe_background_cleanup)
            overrun = CleanupOverrunError("cleanup outlasted the grace a cancellation is deferred for")
            return CleanupResult(cancellation, overrun, unfinished=task)
    try:
        _ended(await task)
    except asyncio.CancelledError as error:
        if cancellation is not None:
            return CleanupResult(cancellation, None)
        return CleanupResult(None, error)
    except BaseException as error:  # noqa: BLE001 - cleanup failure is returned with cancellation
        return CleanupResult(cancellation, error)
    return CleanupResult(cancellation, None)


def rearm_cancellation(cancellation: asyncio.CancelledError | None) -> None:
    """Replay a cancellation that atomic cleanup absorbed, after a failure that is not one replaced it.

    The replay is a new delivery: each exit it reaches defers it for a grace of its own (``_grace_end``).

    ``None`` means cleanup absorbed no cancellation, so nothing is replayed: a cancellation that is
    itself leaving as the exception, or one the caller already handled, is never delivered twice.

    A structured owner of the cancellation (``asyncio.timeout``, ``TaskGroup``) that takes it back (``Task.uncancel``)
    as the failure passes through it withdraws the replay too. Python 3.13 disarms a cancellation that ``uncancel``
    takes back, so the replay is armed now: it lands where the task next waits, or ends the task cancelled if it
    returns first. Python 3.12 does not disarm it, so an armed replay would cancel a later wait that nobody cancelled;
    there the replay is decided where the task next waits, and a task that returns without waiting again ends with
    what it returned (``_REPLAY_ARMED``).
    """
    task = asyncio.current_task()
    if cancellation is None or task is None or not task.cancelling():
        return
    message = cancellation.args[0] if cancellation.args else None
    if _REPLAY_ARMED:
        _replay_cancellation(task, message)
    else:
        asyncio.get_running_loop().call_soon(_replay_cancellation, task, message)


# Whether ``Task.uncancel`` disarms a cancellation it takes back (Python 3.13 and later), so a replay can be armed at
# once instead of decided at the task's next wait.
_REPLAY_ARMED = sys.version_info >= (3, 13)


def _replay_cancellation(task: asyncio.Task[object], message: object) -> None:
    """Deliver a replayed cancellation to ``task``, unless it ended or nobody requests the cancellation any more."""
    if task.done() or not task.cancelling():
        return
    # The request is counted already: replaying it must not count it twice.
    task.uncancel()
    task.cancel(message)


@dataclass(frozen=True, slots=True)
class CancellationMark:
    """The current task's cancellation count when an operation began.

    A cancellation requested after the mark is pending even when a foreign awaitable suppressed it;
    one the caller had already received before the operation began, as in its own cleanup, is not.
    """

    count: int

    @classmethod
    def now(cls) -> CancellationMark:
        """Mark the current task's count as it stands, without a checkpoint.

        For a reader whose next await is itself the checkpoint, such as a pull; an operation that sends
        requests marks with ``mark_cancellation()``, which delivers a replayed cancellation first.
        """
        task = asyncio.current_task()
        return cls(task.cancelling() if task is not None else 0)

    def requested(self) -> bool:
        """Return whether the task was cancelled after the mark."""
        task = asyncio.current_task()
        return task is not None and task.cancelling() > self.count

    def raise_if_requested(self) -> None:
        """Raise a cancellation requested after the mark that no await delivered."""
        if self.requested():
            raise asyncio.CancelledError


def cancelled_here() -> bool:
    """Return whether the current task itself is being cancelled, not only something it awaited."""
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


def own_cancellation(error: BaseException | None) -> asyncio.CancelledError | None:
    """Return ``error`` if it is the current task's own cancellation, ``None`` for anything else.

    A ``CancelledError`` the task was never asked for is not its own: one a future another task cancelled ended in,
    or the one an awaited task that was cancelled raised. The task's close then runs as nobody's cancellation.
    """
    return error if isinstance(error, asyncio.CancelledError) and cancelled_here() else None


async def mark_cancellation() -> CancellationMark:
    """Deliver a replayed cancellation at this checkpoint, then mark the count the operation starts from."""
    task = asyncio.current_task()
    if task is not None and task.cancelling():
        # A replayed cancellation is armed, not yet delivered; a handled one only leaves the count raised.
        await asyncio.sleep(0)
    return CancellationMark.now()
