"""Transport lifecycle, replay-aware retries, and shared rate coordination."""

from __future__ import annotations
import asyncio
import contextlib
import math
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Self

type Clock = Callable[[], float]
type Sleeper = Callable[[float], Awaitable[None]]

_HTTP_STATUS_MINIMUM = 100
_HTTP_STATUS_MAXIMUM = 599
_RETRY_AFTER_CAP_SECONDS = 3_600.0
_METHOD_LIMIT_CAP = 1_024


@dataclass(frozen=True, slots=True)
class DeadlineBudget:
    """Absolute monotonic deadline for admission to a physical request."""

    deadline: float

    def __post_init__(self) -> None:
        """Reject deadlines that cannot be compared to monotonic time."""
        if not math.isfinite(self.deadline):
            raise ValueError("deadline must be finite")


class CoordinatorClosedError(RuntimeError):
    """Admission was rejected because the coordinator was closed."""


class RatePolicyCapacityError(RuntimeError):
    """The bounded method cooldown table cannot admit a new key."""


class CoordinatorBudgetError(TimeoutError):
    """Admission could not finish before its monotonic deadline."""


@dataclass(slots=True)
class _Waiter:
    future: asyncio.Future[None]
    methods: frozenset[str]


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


class _Permit:
    def __init__(self, coordinator: RateCoordinator) -> None:
        self._coordinator = coordinator
        self._released = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.release()

    async def release(self) -> None:
        if self._released:
            return
        self._released = True
        await self._coordinator._release()  # noqa: SLF001


class RateCoordinator:
    """Cooldown-aware fair permit scheduler shared by every execution class."""

    _cycle = (
        WorkClass.INTERACTIVE_DIRECT,
        WorkClass.TRAVERSAL_DIRECT,
        WorkClass.BATCH,
        WorkClass.INTERACTIVE_DIRECT,
        WorkClass.TRAVERSAL_DIRECT,
        WorkClass.RETRY,
    )

    def __init__(
        self,
        *,
        max_concurrency: int = 10,
        clock: Clock = time.monotonic,
        operation_time_limit_delay: float = 120.0,
    ) -> None:
        """Initialize instance state."""
        if isinstance(max_concurrency, bool) or max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        if not math.isfinite(operation_time_limit_delay) or operation_time_limit_delay < 0:
            raise ValueError("operation_time_limit_delay must be finite and non-negative")
        self._max_concurrency = max_concurrency
        self._clock = clock
        self._condition = asyncio.Condition()
        self._queues: dict[WorkClass, deque[_Waiter]] = {work_class: deque() for work_class in WorkClass}
        self._method_until: dict[str, float] = {}
        self._host: str | None = None
        self._operation_time_limit_delay = operation_time_limit_delay
        self._cycle_index = 0
        self._active = 0
        self._state = CoordinatorState.OPEN
        self._cooldown_until: float | None = None
        self._cooldown_reason: str | None = None
        self._wake_task: asyncio.Task[None] | None = None

    async def acquire(
        self,
        work_class: WorkClass,
        *,
        methods: frozenset[str],
        budget: DeadlineBudget | None = None,
    ) -> _Permit:
        """Acquire one coordinator permit for the requested work class."""
        if not isinstance(work_class, WorkClass):
            raise TypeError("work_class must be a WorkClass")
        if not isinstance(methods, frozenset) or not methods or any(not isinstance(m, str) or not m for m in methods):
            raise ValueError("methods must be a non-empty frozenset of method names")
        if budget is not None and not isinstance(budget, DeadlineBudget):
            raise TypeError("budget must be a DeadlineBudget")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        waiter = _Waiter(future, methods)
        async with self._condition:
            if self._state is CoordinatorState.CLOSED:
                raise CoordinatorClosedError("rate coordinator is closed")
            self._refresh_cooldown_locked()
            if budget is not None and self._clock() >= budget.deadline:
                raise CoordinatorBudgetError("coordinator admission budget exhausted")
            self._queues[work_class].append(waiter)
            self._grant_locked()
        try:
            if budget is None:
                await future
            else:
                async with asyncio.timeout_at(loop.time() + max(0.0, budget.deadline - self._clock())):
                    await future
        except (asyncio.CancelledError, TimeoutError) as error:
            async with self._condition:
                was_granted = future.done() and not future.cancelled() and future.exception() is None
                if was_granted:
                    self._return_granted_locked()
                else:
                    with contextlib.suppress(ValueError):
                        self._queues[work_class].remove(waiter)
                self._grant_locked()
            if isinstance(error, TimeoutError):
                raise CoordinatorBudgetError("coordinator admission budget exhausted") from error
            raise
        return _Permit(self)

    async def observe_operation_time_limit(self, method: str, *, delay: float | None = None) -> float:
        """Pause only the method that reported OPERATION_TIME_LIMIT."""
        if not isinstance(method, str) or not method:
            raise ValueError("method must be a non-empty string")
        seconds = self._operation_time_limit_delay if delay is None else delay
        if not math.isfinite(seconds) or seconds < 0:
            raise ValueError("method cooldown must be finite and non-negative")
        seconds = min(seconds, _RETRY_AFTER_CAP_SECONDS)
        async with self._condition:
            if self._state is CoordinatorState.CLOSED:
                return 0.0
            self._prune_methods_locked()
            if method not in self._method_until and len(self._method_until) >= _METHOD_LIMIT_CAP:
                raise RatePolicyCapacityError("method cooldown table is full")
            deadline = self._clock() + seconds
            self._method_until[method] = max(deadline, self._method_until.get(method, deadline))
            self._schedule_wake_locked()
            self._grant_locked()
            return max(0.0, self._method_until[method] - self._clock())

    async def observe_api_throttle(self, method: str, code: str, *, delay: float | None = None) -> float:
        """Record a method-scoped throttle signal without dispatching a retry."""
        if not isinstance(code, str) or not code:
            raise ValueError("code must be a non-empty string")
        if code.casefold() != "operation_time_limit":
            return 0.0
        return await self.observe_operation_time_limit(method, delay=delay)

    def bind_host(self, host: str) -> None:
        """Refuse to share a coordinator between distinct portal hosts."""
        if not isinstance(host, str) or not host:
            raise ValueError("host must be a non-empty normalized host")
        if self._host is not None and self._host != host:
            raise ValueError("rate coordinator is already bound to another host")
        self._host = host

    async def observe_throttle(self, delay: float, *, reason: str) -> float:
        """Merge a throttle hint by the latest bounded monotonic deadline."""
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("cooldown delay must be a finite non-negative number")
        delay = min(delay, _RETRY_AFTER_CAP_SECONDS)
        async with self._condition:
            if self._state is CoordinatorState.CLOSED:
                return 0.0
            candidate = self._clock() + delay
            if self._cooldown_until is None or candidate > self._cooldown_until:
                self._cooldown_until = candidate
                self._cooldown_reason = reason
            self._state = CoordinatorState.COOLDOWN
            self._schedule_wake_locked()
            return max(0.0, self._cooldown_until - self._clock())

    async def close(self) -> None:
        """Close owned resources."""
        async with self._condition:
            if self._state is CoordinatorState.CLOSED:
                return
            self._state = CoordinatorState.CLOSED
            if self._wake_task is not None:
                self._wake_task.cancel()
                self._wake_task = None
            for queue in self._queues.values():
                while queue:
                    future = queue.popleft().future
                    if not future.done():
                        future.set_exception(CoordinatorClosedError("rate coordinator is closed"))

    async def snapshot(self) -> CoordinatorSnapshot:
        """Return the current immutable snapshot."""
        async with self._condition:
            self._refresh_cooldown_locked()
            return CoordinatorSnapshot(
                state=self._state,
                cooldown_until=self._cooldown_until,
                cooldown_reason=self._cooldown_reason,
                active_permits=self._active,
                queued=tuple((work_class, len(self._queues[work_class])) for work_class in WorkClass),
                method_cooldowns=len(self._method_until),
            )

    async def _release(self) -> None:
        async with self._condition:
            if self._active < 1:
                raise RuntimeError("permit accounting underflow")
            self._active -= 1
            self._refresh_cooldown_locked()
            self._grant_locked()

    def _return_granted_locked(self) -> None:
        if self._active < 1:
            raise RuntimeError("granted permit accounting underflow")
        self._active -= 1

    def _grant_locked(self) -> None:
        if self._state is not CoordinatorState.OPEN:
            return
        empty_visits = 0
        while self._active < self._max_concurrency and empty_visits < len(self._cycle):
            work_class = self._cycle[self._cycle_index]
            self._cycle_index = (self._cycle_index + 1) % len(self._cycle)
            queue = self._queues[work_class]
            eligible = next(
                (waiter for waiter in queue if not waiter.future.done() and self._methods_ready_locked(waiter.methods)),
                None,
            )
            if eligible is None:
                empty_visits += 1
                continue
            empty_visits = 0
            queue.remove(eligible)
            self._active += 1
            eligible.future.set_result(None)

    def _methods_ready_locked(self, methods: frozenset[str]) -> bool:
        now = self._clock()
        return all(self._method_until.get(method, 0.0) <= now for method in methods)

    def _prune_methods_locked(self) -> None:
        now = self._clock()
        self._method_until = {method: until for method, until in self._method_until.items() if until > now}

    def _refresh_cooldown_locked(self) -> None:
        self._prune_methods_locked()
        if (
            self._state is CoordinatorState.COOLDOWN
            and self._cooldown_until is not None
            and self._clock() >= self._cooldown_until
        ):
            self._state = CoordinatorState.OPEN
            self._cooldown_until = None
            self._cooldown_reason = None
            self._grant_locked()

    def _schedule_wake_locked(self) -> None:
        if self._wake_task is not None:
            self._wake_task.cancel()
        self._wake_task = asyncio.create_task(self._wake_after_cooldown())

    async def _wake_after_cooldown(self) -> None:
        while True:
            async with self._condition:
                if self._state is CoordinatorState.CLOSED:
                    return
                deadlines = list(self._method_until.values())
                if self._cooldown_until is not None:
                    deadlines.append(self._cooldown_until)
                if not deadlines:
                    return
                remaining = min(deadlines) - self._clock()
                if remaining <= 0:
                    self._refresh_cooldown_locked()
                    self._grant_locked()
                    continue
            await asyncio.sleep(remaining)
