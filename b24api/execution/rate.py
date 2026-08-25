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

    def __init__(self, *, max_concurrency: int = 10, clock: Clock = time.monotonic) -> None:
        """Initialize instance state."""
        if isinstance(max_concurrency, bool) or max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        self._max_concurrency = max_concurrency
        self._clock = clock
        self._condition = asyncio.Condition()
        self._queues: dict[WorkClass, deque[asyncio.Future[None]]] = {work_class: deque() for work_class in WorkClass}
        self._cycle_index = 0
        self._active = 0
        self._state = CoordinatorState.OPEN
        self._cooldown_until: float | None = None
        self._cooldown_reason: str | None = None
        self._wake_task: asyncio.Task[None] | None = None

    async def acquire(self, work_class: WorkClass) -> _Permit:
        """Acquire one coordinator permit for the requested work class."""
        if not isinstance(work_class, WorkClass):
            raise TypeError("work_class must be a WorkClass")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        async with self._condition:
            if self._state is CoordinatorState.CLOSED:
                raise RuntimeError("rate coordinator is closed")
            self._refresh_cooldown_locked()
            self._queues[work_class].append(future)
            self._grant_locked()
        try:
            await future
        except asyncio.CancelledError:
            async with self._condition:
                was_granted = future.done() and not future.cancelled() and future.exception() is None
                if was_granted:
                    self._return_granted_locked()
                else:
                    with contextlib.suppress(ValueError):
                        self._queues[work_class].remove(future)
                self._grant_locked()
            raise
        return _Permit(self)

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
                    future = queue.popleft()
                    if not future.done():
                        future.set_exception(RuntimeError("rate coordinator is closed"))

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
            while queue and queue[0].cancelled():
                queue.popleft()
            if not queue:
                empty_visits += 1
                continue
            empty_visits = 0
            future = queue.popleft()
            self._active += 1
            future.set_result(None)

    def _refresh_cooldown_locked(self) -> None:
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
                if self._state is not CoordinatorState.COOLDOWN or self._cooldown_until is None:
                    return
                remaining = self._cooldown_until - self._clock()
                if remaining <= 0:
                    self._refresh_cooldown_locked()
                    return
            await asyncio.sleep(remaining)
