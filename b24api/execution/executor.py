"""Transport lifecycle, replay-aware retries, and shared rate coordination."""

from __future__ import annotations
import asyncio
import contextlib
import email.utils
import json
import math
import random
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from b24api.contracts.policy import ExecutionPolicy
from b24api.contracts.request import ReplaySafety, Request, RequestSummary
from b24api.contracts.response import (
    Response,
    ResponseEvidence,
    ResponseTime,
)
from b24api.error import (
    AmbiguousExecutionError,
    ApiResponseError,
    B24ApiError,
    BudgetExceededError,
    FailurePhase,
    HTTPGatewayError,
    ProtocolError,
    ResponseTooLargeError,
    TransportError,
)
from b24api.execution.context import (
    ExecutionContext,
    _checkpoint_pending_cancellation,
    _raise_for_pending_cancellation,
)
from b24api.execution.rate import RateCoordinator, WorkClass
from b24api.protocol import ProtocolCodec

if TYPE_CHECKING:
    from b24api.transport.base import Transport, WireResponse

type Clock = Callable[[], float]
type Sleeper = Callable[[float], Awaitable[None]]

_HTTP_STATUS_MINIMUM = 100
_HTTP_STATUS_MAXIMUM = 599
_RETRY_AFTER_CAP_SECONDS = 3_600.0


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
        """Initialize instance state."""
        self.transport = transport
        self.coordinator = coordinator or RateCoordinator(clock=clock)
        self.codec = codec or ProtocolCodec()
        self._clock = clock
        self._sleep = sleep
        self._random = random_source

    def context(self, policy: ExecutionPolicy | None = None) -> ExecutionContext:
        """Create an operation execution context."""
        return ExecutionContext(policy or ExecutionPolicy(), self.coordinator, clock=self._clock)

    async def execute(  # noqa: C901
        self,
        request: Request,
        *,
        context: ExecutionContext | None = None,
        policy: ExecutionPolicy | None = None,
        work_class: WorkClass = WorkClass.INTERACTIVE_DIRECT,
    ) -> Response:
        """Execute one canonical request."""
        if context is not None and policy is not None:
            raise ValueError("pass context or policy, not both")
        if not isinstance(request, Request):
            raise TypeError("request must be canonical Request")
        await _checkpoint_pending_cancellation()
        context = context or self.context(policy)
        await context.start()
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
                            wire = await self.transport.send(
                                request,
                                attempt_timeout=remaining,
                                max_response_bytes=context.policy.max_response_bytes,
                            )
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
                response = _decode_success(wire, request_summary=request.summary)
                _raise_for_pending_cancellation()
                if context.remaining_time(retry_started=retry_started) <= 0:
                    raise BudgetExceededError("transport completed after execution time budget")
                return response
            _raise_for_pending_cancellation()
            if context.remaining_time(retry_started=retry_started) <= 0:
                raise BudgetExceededError("transport completed after execution time budget")
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
        if isinstance(error, ResponseTooLargeError) and safety is not ReplaySafety.SAFE:
            raise AmbiguousExecutionError(
                "Request may have executed before its oversized response was rejected",
                request_summary=request.summary,
                evidence=error.evidence,
            ) from error
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
    if isinstance(error, ApiResponseError) and error.retryable:
        return True
    return isinstance(error, HTTPGatewayError) and error.http_status in policy.retry.transient_http_statuses


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


__all__ = ["Executor"]
