"""Transport lifecycle, replay-aware retries, and shared rate coordination."""

from __future__ import annotations
import asyncio
import contextlib
import itertools
import math
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Self

from b24api.errors import ClientClosedError

type Clock = Callable[[], float]
type Sleeper = Callable[[float], Awaitable[None]]

_HTTP_STATUS_MINIMUM = 100
_HTTP_STATUS_MAXIMUM = 599
_RETRY_AFTER_CAP_SECONDS = 3_600.0
_METHOD_LIMIT_CAP = 1_024
_WAKE_TASK_NAME = "b24api-rate-wake"
# An answer raises the rate the host's send gap allows by this fraction of the initial gap's rate: the pace finds the
# portal's rate again in a few dozen answers after a refusal. With the default half-second initial gap, answers at the
# pace unpace the host in about a minute from a one-second gap and about two from a thirty-second one; a step relative
# to the gap would take a quarter of an hour.
# Past ``_PACE_UNPACED_RATE`` times the initial gap's rate the host is no longer paced.
_PACE_STEP = 1 / 32
_PACE_UNPACED_RATE = 16
FOREIGN_LOOP = "a Bitrix24 client serves the one event loop it was first used in; create a client per event loop"


@dataclass(frozen=True, slots=True)
class DeadlineBudget:
    """Absolute monotonic deadline for admission to a physical request."""

    deadline: float

    def __post_init__(self) -> None:
        """Reject deadlines that cannot be compared to monotonic time."""
        if not math.isfinite(self.deadline):
            raise ValueError("deadline must be finite")


class RatePolicyCapacityError(RuntimeError):
    """The bounded method cooldown table cannot admit a new key."""


class CoordinatorBudgetError(TimeoutError):
    """Admission could not finish before its monotonic deadline."""


@dataclass(frozen=True, slots=True)
class _Slot:
    """The pace slot a permit was granted in: when the grant set the next permit's earliest moment."""

    next_at: float


@dataclass(slots=True)
class _Waiter:
    future: asyncio.Future[None]
    methods: frozenset[str]
    arrival: int
    # The pace slot of the permit granted to it, if the host was paced.
    slot: _Slot | None = None


def _requeue(queue: deque[_Waiter], waiter: _Waiter) -> None:
    """Queue a waiter that gave its permit back where its arrival puts it: behind earlier arrivals, ahead of later."""
    place = next((index for index, each in enumerate(queue) if each.arrival > waiter.arrival), len(queue))
    queue.insert(place, waiter)


class WorkClass(StrEnum):
    """Fair scheduling classes shared by direct, batch, and retry work."""

    INTERACTIVE_DIRECT = "interactive_direct"
    TRAVERSAL_DIRECT = "traversal_direct"
    BATCH = "batch"
    RETRY = "retry"


class CoordinatorState(StrEnum):
    """Externally observable coordinator lifecycle."""

    OPEN = "open"
    COOLDOWN = "cooldown"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class CoordinatorSnapshot:
    """Immutable safe coordinator observation."""

    state: CoordinatorState
    cooldown_until: float | None
    cooldown_reason: str | None
    active_permits: int
    queued: tuple[tuple[WorkClass, int], ...]
    method_cooldowns: int = 0
    # The seconds between the host's sends while a request-rate refusal paces it; 0 when it is not paced.
    pace_gap: float = 0.0


class _Permit:
    def __init__(self, coordinator: RateCoordinator, slot: _Slot | None) -> None:
        self._coordinator = coordinator
        self._slot = slot
        self._unsent = False
        self._released = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.release()

    async def release(self) -> None:
        self.release_now()

    def unsent(self) -> None:
        """Mark the permit's send as never left: its release gives its pace slot back.

        The pace spaces the sends, not the permits: a send refused after its permit (by the deadline, the retry window
        or the attempt budget) takes no slot from the one behind it.
        """
        self._unsent = True

    def release_now(self) -> None:
        """Return capacity synchronously and only then mark the permit released."""
        if self._released:
            return
        self._coordinator._release_now(self._slot if self._unsent else None)  # noqa: SLF001
        self._released = True


@dataclass(slots=True)
class _Streak:
    """Consecutive operating-time refusals of one method, and when the latest pause for them began."""

    count: int
    since: float


@dataclass(slots=True)
class _Pace:
    """The host's send pace under its request-rate limit: at most one permit granted per ``gap`` seconds.

    The portal limits the requests of a webhook by rate (a leaky bucket), so pausing alone sends the waiting requests
    in a burst that is refused again. A refusal widens the gap; the answers narrow it again, a little each, until
    the host is no longer paced (``gap == 0``).
    """

    gap: float = 0.0
    # The gap a refusal of an unpaced host sets: the answers quicken the pace in steps of its rate.
    initial: float = 0.0
    # The earliest moment the next permit may be granted.
    next_at: float = -math.inf
    # When the gap last widened: a refusal of a send that left before then belongs to the burst it answers.
    since: float = -math.inf


def _check_admission(work_class: WorkClass, methods: frozenset[str], budget: DeadlineBudget | None) -> None:
    """Refuse an admission request whose arguments are not what ``RateCoordinator.acquire`` takes."""
    if not isinstance(work_class, WorkClass):
        raise TypeError("work_class must be a WorkClass")
    if not isinstance(methods, frozenset) or not methods or any(not isinstance(m, str) or not m for m in methods):
        raise ValueError("methods must be a non-empty frozenset of method names")
    if budget is not None and not isinstance(budget, DeadlineBudget):
        raise TypeError("budget must be a DeadlineBudget")


class RateCoordinator:
    """Cooldown-aware fair permit scheduler shared by every execution class.

    Retries and first sends take turns: a retry waits for every other permit at most, since its retry window
    runs while it queues, and first sends share the other turns, interactive and traversal work twice as
    often as batches. Every state change is synchronous code between awaits, so the event loop serializes it
    and no lock is needed; a mutator must never await in the middle of an update.
    """

    _cycle = (
        WorkClass.INTERACTIVE_DIRECT,
        WorkClass.TRAVERSAL_DIRECT,
        WorkClass.BATCH,
        WorkClass.INTERACTIVE_DIRECT,
        WorkClass.TRAVERSAL_DIRECT,
    )

    def __init__(
        self, *, max_concurrency: int = 10, clock: Clock = time.monotonic, sleep: Sleeper = asyncio.sleep
    ) -> None:
        """Initialize instance state; ``sleep`` waits out a hold on ``clock``'s time, as the executor's does."""
        if isinstance(max_concurrency, bool) or max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        self._max_concurrency = max_concurrency
        self._clock = clock
        self._sleep = sleep
        self._queues: dict[WorkClass, deque[_Waiter]] = {work_class: deque() for work_class in WorkClass}
        self._arrivals = itertools.count()
        self._method_until: dict[str, float] = {}
        # The monotonic moment each method's operating time resets, as its latest answer gave it.
        self._method_reset: dict[str, float] = {}
        # Operating-time refusals in a row, by method.
        self._streaks: dict[str, _Streak] = {}
        self._pace = _Pace()
        # When the wake task wakes next, as it last read its deadlines; ``None`` once it has none left.
        self._wake_at: float | None = None
        self._host: str | None = None
        self._retry_turn = False
        self._cycle_index = 0
        self._active = 0
        self._state = CoordinatorState.OPEN
        self._cooldown_until: float | None = None
        self._cooldown_reason: str | None = None
        self._wake_task: asyncio.Task[None] | None = None
        # The loop the coordinator first admitted work on: its futures and wake task belong to it, so it serves no
        # other (a wake task left on a closed loop would leave every later waiter queued forever).
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def max_concurrency(self) -> int:
        """Return how many requests the coordinator keeps in flight at once, across all the client's work."""
        return self._max_concurrency

    async def acquire(
        self,
        work_class: WorkClass,
        *,
        methods: frozenset[str],
        budget: DeadlineBudget | None = None,
    ) -> _Permit:
        """Acquire one coordinator permit for the requested work class.

        A permit granted as another is released is checked again when its waiter wakes (``_still_admits``).
        """
        _check_admission(work_class, methods, budget)
        loop = asyncio.get_running_loop()
        if self._state is CoordinatorState.CLOSED:
            raise ClientClosedError("rate coordinator is closed")
        self.bind_loop()
        self._refresh_cooldown()
        if budget is not None and self._clock() >= budget.deadline:
            raise CoordinatorBudgetError("coordinator admission budget exhausted")
        queue = self._queues[work_class]
        waiter = _Waiter(loop.create_future(), methods, next(self._arrivals))
        queue.append(waiter)
        try:
            async with (
                contextlib.nullcontext()
                if budget is None
                else asyncio.timeout_at(loop.time() + max(0.0, budget.deadline - self._clock()))
            ):
                while True:
                    self._grant()
                    await waiter.future
                    if self._closed():
                        # Closed between the grant and this wake-up: the send is refused as a queued one is.
                        self._return_granted(waiter)
                        raise ClientClosedError("rate coordinator is closed")
                    if self._still_admits(methods):
                        return _Permit(self, waiter.slot)
                    # The answer that freed this permit held the host or a method of ``methods`` before this waiter
                    # woke: the permit goes back, and the waiter keeps the place its arrival gave it, ahead of
                    # every waiter that queued after it.
                    self._return_granted(waiter)
                    waiter = _Waiter(loop.create_future(), methods, waiter.arrival)
                    _requeue(queue, waiter)
        except (asyncio.CancelledError, TimeoutError) as error:
            future = waiter.future
            was_granted = future.done() and not future.cancelled() and future.exception() is None
            if was_granted:
                self._return_granted(waiter)
            else:
                with contextlib.suppress(ValueError):
                    queue.remove(waiter)
            self._grant()
            if isinstance(error, TimeoutError):
                raise CoordinatorBudgetError("coordinator admission budget exhausted") from error
            raise

    def bind_loop(self) -> None:
        """Serve the running event loop if it is the first one work started on; refuse any other (``check_loop``).

        Every operation binds it as it starts (``ExecutionContext.start``), before it does any work, and every
        admission does again.
        """
        self.check_loop()
        if self._loop is None:
            self._loop = asyncio.get_running_loop()

    def check_loop(self) -> None:
        """Refuse the running event loop with ``RuntimeError`` (``FOREIGN_LOOP``) if work started on another one.

        A client's close checks it without binding the loop: a client no work used closes in any loop.
        """
        if self._loop is not None and self._loop is not asyncio.get_running_loop():
            raise RuntimeError(FOREIGN_LOOP)

    def hold_method(self, method: str, delay: float) -> None:
        """Hold every permit for ``method`` for ``delay`` seconds (capped at an hour), or longer if already held."""
        if not isinstance(method, str) or not method:
            raise ValueError("method must be a non-empty string")
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("method cooldown must be finite and non-negative")
        if self._state is CoordinatorState.CLOSED:
            return
        self._prune_methods()
        if method not in self._method_until and len(self._method_until) >= _METHOD_LIMIT_CAP:
            raise RatePolicyCapacityError("method cooldown table is full")
        deadline = self._clock() + min(delay, _RETRY_AFTER_CAP_SECONDS)
        self._method_until[method] = max(deadline, self._method_until.get(method, deadline))
        self._schedule_wake()
        self._grant()

    def method_reset(self, method: str) -> float | None:
        """Return how long until ``method``'s operating time resets, as its latest answer said; ``None``: unknown."""
        until = self._method_reset.get(method)
        if until is None:
            return None
        left = until - self._clock()
        return left if left > 0 else None

    def observe_reset(self, method: str, delay: float) -> None:
        """Record that ``method``'s operating time resets in ``delay`` seconds (capped at an hour)."""
        if not math.isfinite(delay) or delay < 0 or self._state is CoordinatorState.CLOSED:
            return
        now = self._clock()
        self._method_reset = {name: until for name, until in self._method_reset.items() if until > now}
        if method in self._method_reset or len(self._method_reset) < _METHOD_LIMIT_CAP:
            self._method_reset[method] = now + min(delay, _RETRY_AFTER_CAP_SECONDS)

    def limit_streak(self, method: str, *, sent_at: float) -> int:
        """Count an operating-time refusal of ``method`` for a send that left at ``sent_at``.

        A send that left after the latest pause of the method began was refused again after that pause: the
        streak grows. One that left before it, or as it began, belongs to the burst that pause answers: the
        streak stays.
        """
        streak = self._streaks.get(method)
        if streak is not None and sent_at <= streak.since:
            return streak.count
        if streak is None and len(self._streaks) >= _METHOD_LIMIT_CAP:
            del self._streaks[next(iter(self._streaks))]
        count = 1 if streak is None else streak.count + 1
        self._streaks[method] = _Streak(count, self._clock())
        return count

    def slow_host(self, *, sent_at: float, initial: float, factor: float, maximum: float) -> float:
        """Widen the host's send gap for a request-rate refusal of a send that left at ``sent_at``; return the gap.

        A send that left after the gap last widened was refused at the pace it set: the gap grows by ``factor``,
        from ``initial`` up to ``maximum``. One that left before it belongs to the burst that widening answers: the
        gap stays. The caller holds the host for the gap, so the refused burst waits as the pace would have.
        """
        pace = self._pace
        if sent_at > pace.since:
            # A policy with no initial delay retries at once: it paces nothing.
            pace.gap = min(maximum, max(initial, pace.gap * factor)) if initial > 0 else 0.0
            pace.initial = initial
            pace.since = self._clock()
        return pace.gap

    def observe_answer(self, methods: frozenset[str], *, sent_at: float) -> None:
        """End the streaks of ``methods`` that an answer about anything but the load, sent at ``sent_at``, ends.

        A send that left after the latest pause of a method began was answered after it: the method's streak ends.
        """
        for method in methods:
            streak = self._streaks.get(method)
            if streak is not None and sent_at > streak.since:
                del self._streaks[method]

    def speed_host(self, *, sent_at: float) -> None:
        """Narrow the host's send gap for its answer, about anything but the load, to a send that left at ``sent_at``.

        Only a send that left after the gap last widened proves the portal takes that pace; each such answer
        raises the rate the gap allows by a step (``_PACE_STEP`` of the initial gap's rate), and past
        ``_PACE_UNPACED_RATE`` times that rate the host is no longer paced. A batch is one send: its commands'
        entries narrow nothing.
        """
        pace = self._pace
        if pace.gap and sent_at > pace.since:
            rate = 1 / pace.gap + _PACE_STEP / pace.initial
            pace.gap = 0.0 if rate > _PACE_UNPACED_RATE / pace.initial else 1 / rate

    def admission_delay(self, methods: frozenset[str]) -> float:
        """Return how long the host cooldown, its pace and the cooldowns of ``methods`` still hold a permit for them.

        The pace holds the next permit until a gap after the latest one granted (``slow_host``).
        """
        return self.admission_delays((methods,))[0]

    def admission_delays(self, scopes: tuple[frozenset[str], ...]) -> tuple[float, ...]:
        """Return ``admission_delay`` of each of ``scopes``, all measured at one reading of the clock.

        Sends held until the same instant are held equally long: a plan that compares their holds compares the
        deadlines they wait for, not readings of the clock taken one after another.
        """
        if self._state is CoordinatorState.CLOSED:
            return tuple(0.0 for _ in scopes)
        now = self._clock()
        host = (
            self._cooldown_until
            if self._state is CoordinatorState.COOLDOWN and self._cooldown_until is not None
            else now
        )
        if self._pace.gap:
            host = max(host, self._pace.next_at)
        return tuple(
            max(0.0, max((self._method_until.get(method, now) for method in methods), default=now) - now, host - now)
            for methods in scopes
        )

    def bind_host(self, host: str) -> None:
        """Refuse to share a coordinator between distinct portal hosts."""
        if not isinstance(host, str) or not host:
            raise ValueError("host must be a non-empty normalized host")
        if self._host is not None and self._host != host:
            raise ValueError("rate coordinator is already bound to another host")
        self._host = host

    async def observe_throttle(self, delay: float, *, reason: str) -> float:
        """Hold the host for ``delay`` seconds (capped at an hour), merged by the latest deadline.

        Returns how far it extended the hold: the seconds the host is held that it was not held before, so the
        holds a burst of refusals asks for at once add up to the time the host was held, never more.
        """
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("cooldown delay must be a finite non-negative number")
        delay = min(delay, _RETRY_AFTER_CAP_SECONDS)
        if self._state is CoordinatorState.CLOSED:
            return 0.0
        now = self._clock()
        held = now if self._cooldown_until is None else max(now, self._cooldown_until)
        candidate = now + delay
        if self._cooldown_until is None or candidate > self._cooldown_until:
            self._cooldown_until = candidate
            self._cooldown_reason = reason
        self._state = CoordinatorState.COOLDOWN
        self._schedule_wake()
        return max(0.0, candidate - held)

    async def close(self) -> None:
        """Reject queued and future admission, then cancel and await the cooldown wake task."""
        wake_task, self._wake_task = self._wake_task, None
        if self._state is not CoordinatorState.CLOSED:
            self._state = CoordinatorState.CLOSED
            for queue in self._queues.values():
                while queue:
                    future = queue.popleft().future
                    if not future.done():
                        future.set_exception(ClientClosedError("rate coordinator is closed"))
        if wake_task is not None:
            wake_task.cancel()
            await asyncio.gather(wake_task, return_exceptions=True)

    async def aclose(self) -> None:
        """Close the coordinator; alias of :meth:`close` for owned-resource cleanup."""
        await self.close()

    async def snapshot(self) -> CoordinatorSnapshot:
        """Return the current immutable snapshot."""
        self._refresh_cooldown()
        return CoordinatorSnapshot(
            state=self._state,
            cooldown_until=self._cooldown_until,
            cooldown_reason=self._cooldown_reason,
            active_permits=self._active,
            queued=tuple((work_class, len(self._queues[work_class])) for work_class in WorkClass),
            method_cooldowns=len(self._method_until),
            pace_gap=self._pace.gap,
        )

    def _release_now(self, unsent: _Slot | None) -> None:
        """Return one permit without an await, so cancellation cannot interrupt the accounting.

        ``unsent`` is the pace slot of a permit whose send never left, given back with it.
        """
        if self._active < 1:
            raise RuntimeError("permit accounting underflow")
        self._active -= 1
        self._give_back(unsent)
        self._refresh_cooldown()
        self._grant()

    def _closed(self) -> bool:
        """Return whether the coordinator is closed: read again after an await, where ``close`` may have run."""
        return self._state is CoordinatorState.CLOSED

    def _still_admits(self, methods: frozenset[str]) -> bool:
        """Return whether a permit granted for ``methods`` may still be used: nothing held them since the grant.

        A permit is granted as another is released, before the released request has read its answer; a refusal there
        holds the host or a method at once, and the granted send must wait for that hold as a queued one does.
        """
        self._refresh_cooldown()
        return self._state is not CoordinatorState.COOLDOWN and self._methods_ready(methods)

    def _return_granted(self, waiter: _Waiter) -> None:
        """Take back a permit granted to ``waiter`` that it will not use, and the pace slot it was granted in."""
        if self._active < 1:
            raise RuntimeError("granted permit accounting underflow")
        self._active -= 1
        self._give_back(waiter.slot)

    def _give_back(self, slot: _Slot | None) -> None:
        """Give back a pace slot no send took, if no later grant has taken the next one: the next permit may go now.

        A slot is taken only once the one before it has passed, so now is never earlier than the pace allowed.
        """
        if slot is not None and self._pace.next_at == slot.next_at:
            self._pace.next_at = self._clock()

    def _grant(self) -> None:
        if self._state is not CoordinatorState.OPEN:
            return
        while self._active < self._max_concurrency:
            # The pace holds every waiter alike, so it is read before one is chosen: a held grant takes no turn.
            if self._pace_holds():
                return
            retry = self._eligible(WorkClass.RETRY)
            first = None if retry is not None and self._retry_turn else self._next_first_send()
            chosen = first or retry
            if chosen is None:
                return
            self._retry_turn = chosen is first
            work_class, waiter = chosen
            waiter.slot = self._take_pace_slot()
            self._queues[work_class].remove(waiter)
            self._active += 1
            waiter.future.set_result(None)

    def _pace_holds(self) -> bool:
        """Return whether the host's pace holds every permit back now.

        A permit it holds back is granted by the wake task once the gap has passed: the wake task is woken now
        when it would sleep past that.
        """
        pace = self._pace
        if not pace.gap or self._clock() >= pace.next_at:
            return False
        if any(self._queues.values()) and (self._wake_at is None or pace.next_at < self._wake_at):
            self._schedule_wake()
        return True

    def _take_pace_slot(self) -> _Slot | None:
        """Space the next permit a gap from now; return the slot this one took, given back if it sends nothing."""
        pace = self._pace
        if not pace.gap:
            return None
        slot = _Slot(self._clock() + pace.gap)
        pace.next_at = slot.next_at
        return slot

    def _next_first_send(self) -> tuple[WorkClass, _Waiter] | None:
        """Return the next eligible first send in the cycle's order, moving the cycle past it."""
        for offset in range(len(self._cycle)):
            position = (self._cycle_index + offset) % len(self._cycle)
            eligible = self._eligible(self._cycle[position])
            if eligible is not None:
                self._cycle_index = (position + 1) % len(self._cycle)
                return eligible
        return None

    def _eligible(self, work_class: WorkClass) -> tuple[WorkClass, _Waiter] | None:
        waiter = next(
            (each for each in self._queues[work_class] if not each.future.done() and self._methods_ready(each.methods)),
            None,
        )
        return None if waiter is None else (work_class, waiter)

    def _methods_ready(self, methods: frozenset[str]) -> bool:
        now = self._clock()
        return all(self._method_until.get(method, 0.0) <= now for method in methods)

    def _prune_methods(self) -> None:
        """Drop the method holds that ended; a waiter one held may go now, so the free permits are granted at once.

        Whoever reads the holds prunes them (a re-check on wake, ``snapshot``, a new hold): the wake task, sleeping
        for a pruned deadline, then finds none left and grants nothing.
        """
        now = self._clock()
        held = len(self._method_until)
        self._method_until = {method: until for method, until in self._method_until.items() if until > now}
        if len(self._method_until) < held:
            self._grant()

    def _refresh_cooldown(self) -> None:
        self._prune_methods()
        if (
            self._state is CoordinatorState.COOLDOWN
            and self._cooldown_until is not None
            and self._clock() >= self._cooldown_until
        ):
            self._state = CoordinatorState.OPEN
            self._cooldown_until = None
            self._cooldown_reason = None
            self._grant()

    def _schedule_wake(self) -> None:
        if self._wake_task is not None:
            self._wake_task.cancel()
        self._wake_task = asyncio.create_task(self._wake_after_cooldown(), name=_WAKE_TASK_NAME)
        # The new task reads every deadline when it first runs, now: no deadline needs another until then.
        self._wake_at = self._clock()

    async def _wake_after_cooldown(self) -> None:
        try:
            while True:
                if self._state is CoordinatorState.CLOSED:
                    return
                # What ended by now is lifted and its permits granted first: every deadline left is in the future.
                self._refresh_cooldown()
                self._grant()
                deadlines = list(self._method_until.values())
                if self._cooldown_until is not None:
                    deadlines.append(self._cooldown_until)
                now = self._clock()
                pace = self._pace
                if pace.gap and pace.next_at > now and any(self._queues.values()):
                    deadlines.append(pace.next_at)
                if not deadlines:
                    return
                self._wake_at = min(deadlines)
                await self._sleep(self._wake_at - now)
        finally:
            if self._wake_task is asyncio.current_task():
                self._wake_at = None
