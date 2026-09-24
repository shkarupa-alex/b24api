"""Transport lifecycle, replay-aware retries, and shared rate coordination."""

from __future__ import annotations
import asyncio
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from b24api.contracts.policy import BudgetCounters, ExecutionPolicy
from b24api.errors import (
    BudgetExceededError,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from b24api.execution.rate import RateCoordinator

    type Clock = Callable[[], float]


@dataclass(frozen=True, slots=True)
class ExecutionSnapshot:
    """Operation-local attempt, retry, and cooldown counters."""

    counters: BudgetCounters
    retries: int
    cooldown_seconds: float
    elapsed: float


@dataclass(frozen=True, slots=True)
class _PageReservation:
    sequence: int
    reference: str | None


class ExecutionContext:
    """Mutable operation-scoped budget ledger with immutable snapshots."""

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
        self._counters = BudgetCounters()
        self._retries = 0
        self._cooldown_seconds = 0.0
        self._page_sequence = 0
        self._page_reservations: dict[_PageReservation, None] = {}
        self._retained_identity_keys = 0
        self._lock = asyncio.Lock()
        self._page_changed = asyncio.Event()

    @property
    def elapsed(self) -> float:
        """Return the elapsed."""
        if self._start is None:
            return 0.0
        return max(0.0, self._clock() - self._start)

    async def start(self) -> None:
        """Start the operation clock exactly once when execution first begins."""
        async with self._lock:
            if self._start is None:
                self._start = self._clock()

    async def reserve_attempt(self, *, attempts_for_request: int, retry_started: float) -> None:
        """Reserve the attempt budget."""
        async with self._lock:
            self._counters = self._counters.reserve_attempt(
                self.policy,
                attempts_for_request=attempts_for_request,
                retry_elapsed=max(0.0, self._clock() - retry_started),
                total_elapsed=self.elapsed,
            )

    async def record_retry(self) -> None:
        """Record the retry."""
        async with self._lock:
            self._retries += 1

    async def record_cooldown(self, seconds: float) -> None:
        """Record the cooldown."""
        async with self._lock:
            self._cooldown_seconds += max(0.0, seconds)

    async def reserve_page(self, *, reference: str | None = None) -> _PageReservation:
        """Reserve page capacity before I/O without charging the response counter."""
        try:
            async with asyncio.timeout(self.policy.max_elapsed - self.elapsed):
                while True:
                    async with self._lock:
                        if self._counters.logical_pages >= self.policy.max_pages:
                            raise BudgetExceededError("logical page budget exhausted")
                        committed = (
                            dict(self._counters.pages_per_reference).get(reference, 0) if reference is not None else 0
                        )
                        if reference is not None and committed >= self.policy.max_pages_per_reference:
                            raise BudgetExceededError("per-reference page budget exhausted")
                        if self.can_reserve_page(reference=reference):
                            reservation = _PageReservation(self._page_sequence, reference)
                            self._page_sequence += 1
                            self._page_reservations[reservation] = None
                            return reservation
                        self._page_changed.clear()
                    await self._page_changed.wait()
        except TimeoutError as error:
            raise BudgetExceededError("page reservation exceeded operation time budget") from error

    def can_reserve_page(self, *, reference: str | None = None) -> bool:
        """Return whether one page reservation can be admitted immediately."""
        if self._counters.logical_pages >= self.policy.max_pages:
            return False
        committed = dict(self._counters.pages_per_reference).get(reference, 0) if reference is not None else 0
        pending_for_reference = sum(item.reference == reference for item in self._page_reservations)
        global_available = self._counters.logical_pages + len(self._page_reservations) < self.policy.max_pages
        reference_available = (
            reference is None or committed + pending_for_reference < self.policy.max_pages_per_reference
        )
        return global_available and reference_available

    async def reserve_pages(self, count: int) -> tuple[_PageReservation, ...]:
        """Atomically reserve a fixed operation-local page wave or fail immediately."""
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise ValueError("page reservation count must be a positive integer")
        async with self._lock:
            if self.policy.max_elapsed - self.elapsed <= 0:
                raise BudgetExceededError("page reservation exceeded operation time budget")
            used = self._counters.logical_pages + len(self._page_reservations)
            if used + count > self.policy.max_pages:
                raise BudgetExceededError("logical page budget cannot fit the requested wave")
            reservations = tuple(_PageReservation(self._page_sequence + offset, None) for offset in range(count))
            self._page_sequence += count
            self._page_reservations.update(dict.fromkeys(reservations))
            return reservations

    def commit_page(self, reservation: _PageReservation) -> None:
        """Atomically charge one decoded response with no cancellation point."""
        if reservation not in self._page_reservations:
            raise RuntimeError("page reservation is not active")
        self._counters = self._counters.reserve_page(self.policy, reference=reservation.reference)
        self._page_reservations.pop(reservation)
        self._page_changed.set()

    def release_page(self, reservation: _PageReservation) -> None:
        """Atomically release capacity when no decoded response was returned."""
        if reservation not in self._page_reservations:
            return
        self._page_reservations.pop(reservation)
        self._page_changed.set()

    async def set_buffered_rows(self, rows: int) -> None:
        """Record retained decoded rows and enforce the global buffer ceiling."""
        async with self._lock:
            self._counters = self._counters.with_buffered_rows(self.policy, rows)

    async def adjust_buffered_rows(self, delta: int) -> None:
        """Atomically account concurrent scheduler buffers by a signed delta."""
        if not isinstance(delta, int) or isinstance(delta, bool):
            raise TypeError("buffer delta must be an integer")
        async with self._lock:
            target = self._counters.buffered_rows + delta
            self._counters = self._counters.with_buffered_rows(self.policy, target)

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

    def remaining_time(self, *, retry_started: float) -> float:
        """Return the tighter operation or retry time remaining."""
        return min(
            self.policy.max_elapsed - self.elapsed,
            self.policy.max_retry_elapsed_per_request - max(0.0, self._clock() - retry_started),
        )

    async def snapshot(self) -> ExecutionSnapshot:
        """Return the current immutable snapshot."""
        async with self._lock:
            return ExecutionSnapshot(
                counters=self._counters,
                retries=self._retries,
                cooldown_seconds=self._cooldown_seconds,
                elapsed=self.elapsed,
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
        """Pull one item within the operation's remaining budget."""
        if self._active_pull is not None:
            raise RuntimeError("source pull is already active")
        if self._cleanup_task is not None:
            raise RuntimeError("source cleanup has already started")
        remaining = context.policy.max_elapsed - context.elapsed
        if remaining <= 0:
            raise BudgetExceededError(self._input_error)
        pull = asyncio.create_task(anext(self._source))
        self._active_pull = pull
        try:
            done, _ = await asyncio.wait((pull,), timeout=remaining)
        except BaseException:
            if not pull.done():
                pull.cancel()
            self._retain_close_after(pull)
            raise
        if done:
            try:
                return await pull
            finally:
                self._active_pull = None
        pull.cancel()
        self._retain_close_after(pull)
        raise BudgetExceededError(self._input_error)

    async def aclose(self, *, remaining: float) -> None:
        """Close the source within the supplied remaining duration."""
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
        await asyncio.sleep(0)
        if cleanup.done():
            await cleanup
            return
        done, _ = await asyncio.wait((cleanup,), timeout=max(0.0, remaining))
        if done:
            await cleanup
            return
        raise BudgetExceededError(self._cleanup_error)

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


def _observe_background_cleanup(task: asyncio.Task[None]) -> None:
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

    def raise_failure(self) -> None:
        """Raise the deferred cancellation first, then the cleanup failure."""
        if self.cancellation is not None:
            raise self.cancellation
        if self.error is not None:
            raise self.error


async def await_cleanup_resistant(awaitable: Awaitable[None]) -> CleanupResult:
    """Finish owned cleanup without losing a concurrent caller cancellation."""
    task: asyncio.Future[None] = asyncio.ensure_future(awaitable)
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.wait((task,), return_when=asyncio.ALL_COMPLETED)
        except asyncio.CancelledError as error:
            cancellation = error
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
    """Replay a cancellation after a primary failure has crossed its atomic cleanup."""
    task = asyncio.current_task()
    if task is None or not task.cancelling():
        return
    task.uncancel()
    message = cancellation.args[0] if cancellation is not None and cancellation.args else None
    task.cancel(message)


def _raise_for_pending_cancellation() -> None:
    task = asyncio.current_task()
    if task is not None and task.cancelling():
        raise asyncio.CancelledError


async def _checkpoint_pending_cancellation() -> None:
    task = asyncio.current_task()
    if task is None or not task.cancelling():
        return
    await asyncio.sleep(0)
    _raise_for_pending_cancellation()
