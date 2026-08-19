"""Transport lifecycle, replay-aware retries, and shared rate coordination."""

from __future__ import annotations
import asyncio
import contextlib
import email.utils
import json
import math
import random
import time
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, Self

import httpx

from b24api.error import (
    AmbiguousExecutionError,
    ApiResponseError,
    B24ApiError,
    BudgetExceededError,
    FailurePhase,
    HTTPGatewayError,
    ProtocolError,
    RetryApiResponseError,
    TransportError,
)
from b24api.models import (
    BudgetCounters,
    ExecutionPolicy,
    ReplaySafety,
    Request,
    RequestSummary,
    Response,
    ResponseEvidence,
    ResponseTime,
)
from b24api.protocol import ProtocolCodec

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
class WireResponse:
    """Complete bounded transport response consumed by the protocol layer."""

    status_code: int
    headers: tuple[tuple[str, str], ...]
    body: bytes

    def __post_init__(self) -> None:
        if not _HTTP_STATUS_MINIMUM <= self.status_code <= _HTTP_STATUS_MAXIMUM:
            raise ValueError("HTTP status must be between 100 and 599")
        object.__setattr__(self, "headers", tuple(self.headers))
        object.__setattr__(self, "body", bytes(self.body))

    @property
    def header_map(self) -> dict[str, str]:
        return {name.casefold(): value for name, value in self.headers}


class Transport(Protocol):
    """One complete request attempt with classified transport failures."""

    async def send(self, request: Request, *, attempt_timeout: float) -> WireResponse: ...


class _PhaseTracker:
    """Translate httpcore trace events into the last conclusive lifecycle phase."""

    def __init__(self) -> None:
        self.phase = FailurePhase.NOT_DISPATCHED

    async def __call__(self, event_name: str, _info: Mapping[str, object]) -> None:
        if event_name.endswith(("connect_tcp.complete", "start_tls.complete")):
            self.phase = FailurePhase.CONNECTION_ESTABLISHED
        elif ".send_request_" in event_name and event_name.endswith(".started"):
            self.phase = FailurePhase.DISPATCH_STARTED
        elif event_name.endswith("receive_response_headers.complete"):
            self.phase = FailurePhase.HEADERS_RECEIVED
        elif event_name.endswith("receive_response_body.started"):
            self.phase = FailurePhase.BODY_PARTIALLY_RECEIVED


class HttpxTransport:
    """HTTPX transport with conservative failure-phase classification."""

    def __init__(self, webhook_url: str, *, client: httpx.AsyncClient | None = None) -> None:
        if not webhook_url.endswith("/"):
            webhook_url += "/"
        self._webhook_url = webhook_url
        self._client = client or httpx.AsyncClient()
        self._owns_client = client is None
        self._closed = False

    async def send(self, request: Request, *, attempt_timeout: float) -> WireResponse:
        if self._closed:
            raise RuntimeError("transport is closed")
        tracker = _PhaseTracker()
        try:
            http_request = self._client.build_request(
                "POST",
                f"{self._webhook_url}{request.method}",
                headers={"Content-Type": "application/json"},
                json=request.to_wire_parameters(),
            )
            http_request.extensions["trace"] = tracker
            http_request.extensions["timeout"] = {
                "connect": attempt_timeout,
                "read": attempt_timeout,
                "write": attempt_timeout,
                "pool": attempt_timeout,
            }
            response = await self._client.send(http_request)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as error:
            raise TransportError(
                "Transport failed before dispatch",
                phase=tracker.phase,
                request_summary=request.summary,
            ) from error
        except (httpx.WriteError, httpx.WriteTimeout) as error:
            raise TransportError(
                "Transport failed during or after possible request dispatch",
                phase=_at_least_dispatch_started(tracker.phase),
                request_summary=request.summary,
            ) from error
        except (httpx.ReadError, httpx.ReadTimeout, httpx.RemoteProtocolError) as error:
            raise TransportError(
                "Transport failed after possible request dispatch",
                phase=_at_least_dispatch_started(tracker.phase),
                request_summary=request.summary,
            ) from error
        except httpx.TransportError as error:
            raise TransportError(
                "Unclassified transport failure after possible dispatch",
                phase=_at_least_dispatch_started(tracker.phase),
                request_summary=request.summary,
            ) from error
        return WireResponse(
            status_code=response.status_code,
            headers=tuple(response.headers.multi_items()),
            body=response.content,
        )

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_client:
            await self._client.aclose()


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


@dataclass(frozen=True, slots=True)
class ExecutionSnapshot:
    """Operation-local attempt, retry, and cooldown counters."""

    counters: BudgetCounters
    retries: int
    cooldown_seconds: float
    elapsed: float


class ExecutionContext:
    """Mutable operation-scoped budget ledger with immutable snapshots."""

    def __init__(
        self,
        policy: ExecutionPolicy,
        coordinator: RateCoordinator,
        *,
        clock: Clock = time.monotonic,
    ) -> None:
        self.policy = policy
        self.coordinator = coordinator
        self._clock = clock
        self._start = clock()
        self._counters = BudgetCounters()
        self._retries = 0
        self._cooldown_seconds = 0.0
        self._lock = asyncio.Lock()

    @property
    def elapsed(self) -> float:
        return max(0.0, self._clock() - self._start)

    async def reserve_attempt(self, *, attempts_for_request: int, retry_started: float) -> None:
        async with self._lock:
            self._counters = self._counters.reserve_attempt(
                self.policy,
                attempts_for_request=attempts_for_request,
                retry_elapsed=max(0.0, self._clock() - retry_started),
                total_elapsed=self.elapsed,
            )

    async def record_retry(self) -> None:
        async with self._lock:
            self._retries += 1

    async def record_cooldown(self, seconds: float) -> None:
        async with self._lock:
            self._cooldown_seconds += max(0.0, seconds)

    async def reserve_page(self, *, reference: str | None = None) -> None:
        """Charge one decoded logical page before it is accepted by a driver."""
        async with self._lock:
            self._counters = self._counters.reserve_page(self.policy, reference=reference)

    async def set_buffered_rows(self, rows: int) -> None:
        """Record retained decoded rows and enforce the global buffer ceiling."""
        async with self._lock:
            self._counters = self._counters.with_buffered_rows(self.policy, rows)

    def remaining_time(self, *, retry_started: float) -> float:
        return min(
            self.policy.max_elapsed - self.elapsed,
            self.policy.max_retry_elapsed_per_request - max(0.0, self._clock() - retry_started),
        )

    async def snapshot(self) -> ExecutionSnapshot:
        async with self._lock:
            return ExecutionSnapshot(
                counters=self._counters,
                retries=self._retries,
                cooldown_seconds=self._cooldown_seconds,
                elapsed=self.elapsed,
            )


class Executor:
    """Execute canonical requests with conservative replay and budget semantics."""

    def __init__(  # noqa: PLR0913
        self,
        transport: Transport,
        *,
        coordinator: RateCoordinator | None = None,
        codec: ProtocolCodec | None = None,
        clock: Clock = time.monotonic,
        sleep: Sleeper = asyncio.sleep,
        random_source: Callable[[], float] = random.random,
    ) -> None:
        self.transport = transport
        self.coordinator = coordinator or RateCoordinator(clock=clock)
        self.codec = codec or ProtocolCodec()
        self._clock = clock
        self._sleep = sleep
        self._random = random_source

    def context(self, policy: ExecutionPolicy | None = None) -> ExecutionContext:
        return ExecutionContext(policy or ExecutionPolicy(), self.coordinator, clock=self._clock)

    async def execute(  # noqa: C901
        self,
        request: Request,
        *,
        context: ExecutionContext | None = None,
        policy: ExecutionPolicy | None = None,
        work_class: WorkClass = WorkClass.INTERACTIVE_DIRECT,
    ) -> Response:
        if context is not None and policy is not None:
            raise ValueError("pass context or policy, not both")
        if not isinstance(request, Request):
            raise TypeError("request must be canonical Request")
        context = context or self.context(policy)
        retry_started = self._clock()
        attempts = 0
        while True:
            remaining = context.remaining_time(retry_started=retry_started)
            if remaining <= 0:
                raise BudgetExceededError("execution time budget exhausted")
            scheduled_class = work_class if attempts == 0 else WorkClass.RETRY
            try:
                async with asyncio.timeout(remaining):
                    permit = await context.coordinator.acquire(scheduled_class)
            except TimeoutError as error:
                raise BudgetExceededError("permit wait exhausted execution time budget") from error
            try:
                async with permit:
                    remaining = context.remaining_time(retry_started=retry_started)
                    if remaining <= 0:
                        raise BudgetExceededError("execution time budget exhausted before dispatch")
                    await context.reserve_attempt(attempts_for_request=attempts, retry_started=retry_started)
                    try:
                        async with asyncio.timeout(remaining):
                            wire = await self.transport.send(request, attempt_timeout=remaining)
                    except TimeoutError as error:
                        raise TransportError(
                            "Transport attempt exceeded its elapsed budget",
                            phase=FailurePhase.DISPATCH_STARTED,
                            request_summary=request.summary,
                        ) from error
            except asyncio.CancelledError:
                raise
            except B24ApiError as error:
                await self._prepare_retry(
                    request,
                    error,
                    context=context,
                    retry_started=retry_started,
                    attempts=attempts,
                )
                attempts += 1
                continue

            response_error = self.codec.error_from_http(
                status_code=wire.status_code,
                body=wire.body,
                request_summary=request.summary,
                headers=wire.header_map,
                retry_codes=context.policy.retry.transient_api_codes,
            )
            if response_error is None:
                return _decode_success(wire, request_summary=request.summary)
            await self._prepare_retry(
                request,
                response_error,
                context=context,
                retry_started=retry_started,
                attempts=attempts,
                wire=wire,
            )
            attempts += 1

    async def _prepare_retry(  # noqa: PLR0913
        self,
        request: Request,
        error: B24ApiError,
        *,
        context: ExecutionContext,
        retry_started: float,
        attempts: int,
        wire: WireResponse | None = None,
    ) -> None:
        safety = request.replay_safety or ReplaySafety.UNKNOWN
        if isinstance(error, TransportError) and error.possible_acceptance and safety is not ReplaySafety.SAFE:
            raise AmbiguousExecutionError(
                "Request may have executed; automatic replay is forbidden",
                request_summary=request.summary,
                evidence=error.evidence,
            ) from error

        retryable = _is_retryable(error, safety=safety, policy=context.policy)
        if not retryable:
            raise error

        next_attempt = attempts + 1
        if next_attempt >= context.policy.max_attempts_per_request:
            raise BudgetExceededError("per-request attempt budget exhausted") from error
        delay = _retry_delay(context.policy, retry_number=next_attempt, random_source=self._random)
        throttle_delay = _retry_after_seconds(wire) if wire is not None else None
        if throttle_delay is not None:
            delay = max(delay, throttle_delay)
            merged = await context.coordinator.observe_throttle(delay, reason=_throttle_reason(error))
            await context.record_cooldown(merged)
        remaining = context.remaining_time(retry_started=retry_started)
        if delay >= remaining:
            raise BudgetExceededError("retry delay would exceed elapsed budget") from error
        await context.record_retry()
        if throttle_delay is None and delay > 0:
            await self._sleep(delay)


def _is_retryable(error: B24ApiError, *, safety: ReplaySafety, policy: ExecutionPolicy) -> bool:
    if isinstance(error, TransportError):
        return not error.possible_acceptance or safety is ReplaySafety.SAFE
    if safety is not ReplaySafety.SAFE:
        return False
    if isinstance(error, RetryApiResponseError):
        return True
    return isinstance(error, HTTPGatewayError) and error.http_status in policy.retry.transient_http_statuses


def _at_least_dispatch_started(phase: FailurePhase) -> FailurePhase:
    if phase in {FailurePhase.NOT_DISPATCHED, FailurePhase.CONNECTION_ESTABLISHED}:
        return FailurePhase.DISPATCH_STARTED
    return phase


def _retry_delay(
    policy: ExecutionPolicy,
    *,
    retry_number: int,
    random_source: Callable[[], float],
) -> float:
    retry = policy.retry
    base = min(retry.maximum_delay, retry.initial_delay * retry.backoff ** max(0, retry_number - 1))
    if retry.jitter == 0 or base == 0:
        return base
    factor = 1 - retry.jitter + (2 * retry.jitter * random_source())
    return max(0.0, base * factor)


def _retry_after_seconds(wire: WireResponse) -> float | None:
    headers = wire.header_map
    raw = headers.get("retry-after") or headers.get("x-bitrix-ratelimit-reset")
    if raw is None:
        return None
    with contextlib.suppress(ValueError):
        value = float(raw)
        if math.isfinite(value) and value >= 0:
            return min(value, _RETRY_AFTER_CAP_SECONDS)
    with contextlib.suppress(TypeError, ValueError, OverflowError):
        parsed = email.utils.parsedate_to_datetime(raw)
        return min(max(0.0, parsed.timestamp() - time.time()), _RETRY_AFTER_CAP_SECONDS)
    return None


def _throttle_reason(error: B24ApiError) -> str:
    if isinstance(error, ApiResponseError):
        return error.normalized_code
    return f"http_{error.http_status}"


def _decode_success(wire: WireResponse, *, request_summary: RequestSummary) -> Response:
    evidence = _wire_evidence(wire)
    try:
        payload = json.loads(wire.body, parse_constant=_reject_json_constant)
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as error:
        raise HTTPGatewayError(
            "Malformed successful HTTP response",
            request_summary=request_summary,
            evidence=evidence,
        ) from error
    if not isinstance(payload, Mapping) or "result" not in payload:
        raise HTTPGatewayError(
            "Successful response is missing the result envelope",
            request_summary=request_summary,
            evidence=evidence,
        )
    total = payload.get("total")
    next_value = payload.get("next")
    if total is not None and (not isinstance(total, int) or isinstance(total, bool)):
        raise HTTPGatewayError("Response total must be an integer", request_summary=request_summary, evidence=evidence)
    if isinstance(total, int) and total < -1:
        raise HTTPGatewayError(
            "Response total must be -1 or non-negative",
            request_summary=request_summary,
            evidence=evidence,
        )
    if next_value is not None and (not isinstance(next_value, int) or isinstance(next_value, bool)):
        raise HTTPGatewayError("Response next must be an integer", request_summary=request_summary, evidence=evidence)
    try:
        return Response(
            payload["result"],
            time=_decode_response_time(payload.get("time")),
            total=total,
            next=next_value,
            evidence=evidence,
        )
    except (TypeError, ValueError, OverflowError) as error:
        raise ProtocolError(
            "Successful response violates the canonical response contract",
            request_summary=request_summary,
            evidence=evidence,
        ) from error


def _decode_response_time(raw: object) -> ResponseTime | None:
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise TypeError("response time must be an object")

    def number(name: str, *, default: float = 0.0) -> float:
        value = raw.get(name, default)
        if not isinstance(value, int | float) or isinstance(value, bool) or not math.isfinite(float(value)):
            raise ValueError("response time contains an invalid number")
        return float(value)

    operating = raw.get("operating")
    operating_reset_at = raw.get("operating_reset_at")
    return ResponseTime(
        start=number("start"),
        finish=number("finish"),
        duration=number("duration"),
        processing=number("processing"),
        date_start=str(raw.get("date_start", "")),
        date_finish=str(raw.get("date_finish", "")),
        operating_reset_at=None if operating_reset_at is None else number("operating_reset_at"),
        operating=None if operating is None else number("operating"),
    )


def _wire_evidence(wire: WireResponse) -> ResponseEvidence:
    safe_headers = {
        name: value
        for name, value in wire.header_map.items()
        if name in {"content-type", "retry-after", "x-request-id", "x-bitrix-ratelimit-reset"}
    }
    return ResponseEvidence(
        http_status=wire.status_code,
        request_id=safe_headers.get("x-request-id"),
        headers=tuple(sorted(safe_headers.items())),
    )


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant is forbidden: {value}")


__all__ = [
    "CoordinatorSnapshot",
    "CoordinatorState",
    "ExecutionContext",
    "ExecutionSnapshot",
    "Executor",
    "HttpxTransport",
    "RateCoordinator",
    "Transport",
    "WireResponse",
    "WorkClass",
]
