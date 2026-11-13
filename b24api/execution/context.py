"""Transport lifecycle, replay-aware retries, and shared rate coordination."""

from __future__ import annotations
import asyncio
import contextlib
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from b24api.contracts.policy import BudgetCounters, ExecutionPolicy
from b24api.errors import BudgetExceededError, CleanupOverrunError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from b24api.execution.rate import RateCoordinator

    type Clock = Callable[[], float]


REQUEST_BUDGET_EXHAUSTED = "physical request budget exhausted"
# A request's own retry window closed before the operation's deadline: that request alone is refused, whatever the
# work it was refused (``ExecutionContext.time_left``), and the operation goes on.
RETRY_WINDOW_EXHAUSTED = "per-request retry time budget exhausted"

# The least time a cleanup step may wait for what it closes: a caller source's close, or the workers and tasks the
# operation cancelled. Cleanup runs when the operation ends, which is often because ``max_elapsed`` ran out, so the
# time left is then zero; a close that ends at once still needs a few loop turns, and a synchronous source's close a
# worker-thread hop. The grace lets such a close finish, so the budget's end is never reported as a cleanup failure,
# while a close that really blocks overruns the budget by at most this much per step.
CLEANUP_GRACE_SECONDS = 0.1


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
        # Set once the request budget refuses a send: the count never falls, so no later send can be admitted.
        self._requests_refused = False
        # The first refusal of work by the request budget or the operation's deadline (a send, a page, an admission
        # or a pull), which ends the operation: neither budget comes back, so no later work can be admitted either.
        self._refusal: str | None = None
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
    def requests_refused(self) -> bool:
        """Return whether the request budget has refused a send: no send can follow.

        Once it is set, the batch streams' source pull (``AsyncIteratorController.get``) refuses further input.
        """
        return self._requests_refused

    @property
    def refusal(self) -> str | None:
        """Return the first refusal of work by the operation's budget, which ends the operation, or ``None``.

        It is the refusal's message: the request budget's (``REQUEST_BUDGET_EXHAUSTED``), or the deadline's, which
        names what the deadline refused. Only a refusal sets it: a budget spent exactly by the last send the
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
            raise BudgetExceededError(self._refusal)

    def _refuse(self, refusal: str, retry_started: float | None) -> BudgetExceededError:
        start = self._start
        if (
            retry_started is not None
            and start is not None
            and retry_started + self.policy.max_retry_elapsed_per_request < start + self.policy.max_elapsed
        ):
            return BudgetExceededError(RETRY_WINDOW_EXHAUSTED)
        self._record_refusal(refusal)
        return BudgetExceededError(refusal)

    def _record_refusal(self, message: str) -> None:
        if self._refusal is None:
            self._refusal = message

    def admit_request(self) -> None:
        """Refuse a send once the request budget is spent, recording the refusal that ends the operation.

        The executor asks before a send queues for its permit, so a spent budget never waits for a cooldown just to
        refuse, and again when it reserves the attempt.
        """
        if self._physical_requests >= self.policy.max_requests:
            self._requests_refused = True
            self._record_refusal(REQUEST_BUDGET_EXHAUSTED)
            raise BudgetExceededError(REQUEST_BUDGET_EXHAUSTED)

    async def start(self) -> None:
        """Start the operation clock exactly once when execution first begins."""
        if self._start is None:
            self._start = self._clock()

    async def reserve_attempt(self, *, attempts_for_request: int) -> None:
        """Reserve the attempt budget; the send's dispatch check just before it (``time_left``) is its time check."""
        self.admit_request()
        if attempts_for_request >= self.policy.max_attempts_per_request:
            raise BudgetExceededError("per-request attempt budget exhausted")
        self._physical_requests += 1

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
        """Atomically reserve a fixed operation-local page wave or fail immediately."""
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise ValueError("page reservation count must be a positive integer")
        self.time_left("page reservation exceeded operation time budget")
        used = self._logical_pages + len(self._page_reservations)
        if used + count > self.policy.max_pages:
            raise BudgetExceededError("logical page budget cannot fit the requested wave")
        reservations = tuple(PageReservation(self._page_sequence + offset, None) for offset in range(count))
        self._page_sequence += count
        self._page_reservations.update(dict.fromkeys(reservations))
        return reservations

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
        """Refuse a page once the operation or the reference has committed its whole page budget."""
        if self._logical_pages >= self.policy.max_pages:
            raise BudgetExceededError("logical page budget exhausted")
        entry = None if reference is None else self._reference_pages.get(reference)
        if entry is not None and entry.committed >= self.policy.max_pages_per_reference:
            raise BudgetExceededError("per-reference page budget exhausted")

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
        self._active_pull: asyncio.Task[T] | None = None
        self._cleanup_task: asyncio.Task[None] | None = None

    async def get(self, context: ExecutionContext) -> T:
        """Pull one item within the operation's budget: its time left, and a request budget that refused nothing."""
        if self._active_pull is not None:
            raise RuntimeError("source pull is already active")
        if self._cleanup_task is not None:
            raise RuntimeError("source cleanup has already started")
        if context.requests_refused:
            # No item pulled now could be sent: the operation ends with its request budget, as with its time budget.
            raise BudgetExceededError(REQUEST_BUDGET_EXHAUSTED)
        pull: asyncio.Task[T] | None = None
        try:
            async with context.deadline(self._input_error):
                pull = asyncio.create_task(anext(self._source))
                self._active_pull = pull
                await asyncio.wait((pull,))
        except BaseException:
            if pull is not None:
                if not pull.done():
                    pull.cancel()
                self._retain_close_after(pull)
            raise
        try:
            return await pull
        finally:
            self._active_pull = None

    async def aclose(self, context: ExecutionContext) -> None:
        """Close the source, waiting for it within the operation's cleanup window.

        A close still running when the window ends goes on in the background, retained here, and the
        wait raises the cleanup error.
        """
        if self._cleanup_task is None:
            active = self._active_pull
            if active is None:
                self._retain_cleanup(asyncio.create_task(self._close_source()))
            else:
                active.cancel()
                self._retain_close_after(active)
        cleanup = self._cleanup_task
        if cleanup is None:
            return
        done, _ = await asyncio.wait((cleanup,), timeout=context.cleanup_window())
        if done:
            await cleanup
            return
        raise CleanupOverrunError(self._cleanup_error)

    def _retain_close_after(self, pull: asyncio.Task[T]) -> None:
        if self._cleanup_task is None:
            self._retain_cleanup(asyncio.create_task(self._close_after(pull)))

    def _retain_cleanup(self, cleanup: asyncio.Task[None]) -> None:
        self._cleanup_task = cleanup
        cleanup.add_done_callback(_observe_background_cleanup)

    async def _close_after(self, pull: asyncio.Task[T]) -> None:
        pull_error: BaseException | None = None
        try:
            await pull
        except (asyncio.CancelledError, StopAsyncIteration):
            pass
        except Exception as error:  # noqa: BLE001 - preserve the source error through owned cleanup
            pull_error = error
        if self._active_pull is pull:
            self._active_pull = None
        try:
            await self._close_source()
        except BaseException as close_error:
            if pull_error is not None:
                raise close_error from pull_error
            raise
        if pull_error is not None:
            raise pull_error

    async def _close_source(self) -> None:
        (await self._source.aclose()).raise_failure()


def _observe_background_cleanup(task: asyncio.Future[None]) -> None:
    """Retrieve late exceptions while preserving them for any later await."""
    if not task.cancelled():
        task.exception()


async def await_cancellation_resistant(awaitable: Awaitable[None]) -> asyncio.CancelledError | None:
    """Finish an owned state transition and return the last concurrent cancellation."""
    task: asyncio.Future[None] = asyncio.ensure_future(awaitable)
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.wait((task,), return_when=asyncio.ALL_COMPLETED)
        except asyncio.CancelledError as error:
            cancellation = error
    try:
        await task
    except asyncio.CancelledError:
        if cancellation is not None:
            return cancellation
        raise
    return cancellation


@dataclass(frozen=True, slots=True)
class CleanupResult:
    """What owned cleanup reported: a concurrent caller cancellation and its own failure."""

    cancellation: asyncio.CancelledError | None
    error: BaseException | None
    # Cleanup a deadline stopped waiting for, still running; its owner must keep a reference to it.
    unfinished: asyncio.Future[None] | None = field(default=None, kw_only=True)

    def raise_failure(self) -> None:
        """Raise the deferred cancellation first, then the cleanup failure."""
        if self.cancellation is not None:
            raise self.cancellation
        if self.error is not None:
            raise self.error


async def await_cleanup_resistant(
    awaitable: Awaitable[None],
    *,
    deadline: float | None = None,
    cancelled: bool = False,
) -> CleanupResult:
    """Finish owned cleanup without losing a concurrent caller cancellation.

    ``deadline`` (event-loop time) bounds how long a cancellation is deferred, one that arrives here
    or one the caller already holds (``cancelled``): cleanup still running at the deadline is left
    to finish in the background, returned as ``unfinished`` with a ``BudgetExceededError``. Without a
    cancellation, or without a deadline, cleanup is awaited to its end.
    """
    task: asyncio.Future[None] = asyncio.ensure_future(awaitable)
    cancellation: asyncio.CancelledError | None = None
    loop = asyncio.get_running_loop()
    while not task.done():
        timeout = None
        if deadline is not None and (cancelled or cancellation is not None):
            timeout = max(0.0, deadline - loop.time())
        try:
            await asyncio.wait((task,), timeout=timeout)
        except asyncio.CancelledError as error:
            cancellation = error
            continue
        if not task.done():
            task.add_done_callback(_observe_background_cleanup)
            overrun = CleanupOverrunError("cleanup exceeded the operation time budget")
            return CleanupResult(cancellation, overrun, unfinished=task)
    try:
        await task
    except asyncio.CancelledError as error:
        if cancellation is not None:
            return CleanupResult(cancellation, None)
        return CleanupResult(None, error)
    except BaseException as error:  # noqa: BLE001 - cleanup failure is returned with cancellation
        return CleanupResult(cancellation, error)
    return CleanupResult(cancellation, None)


def rearm_cancellation(cancellation: asyncio.CancelledError | None) -> None:
    """Replay a cancellation that atomic cleanup absorbed, after a failure that is not one replaced it.

    ``None`` means cleanup absorbed no cancellation, so nothing is replayed: a cancellation that is
    itself leaving as the exception, or one the caller already handled, is never delivered twice.
    """
    task = asyncio.current_task()
    if cancellation is None or task is None or not task.cancelling():
        return
    task.uncancel()
    task.cancel(cancellation.args[0] if cancellation.args else None)


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


async def mark_cancellation() -> CancellationMark:
    """Deliver a replayed cancellation at this checkpoint, then mark the count the operation starts from."""
    task = asyncio.current_task()
    if task is not None and task.cancelling():
        # A replayed cancellation is armed, not yet delivered; a handled one only leaves the count raised.
        await asyncio.sleep(0)
    return CancellationMark.now()
