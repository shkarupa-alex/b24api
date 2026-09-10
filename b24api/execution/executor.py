"""Transport lifecycle, replay-aware retries, and shared rate coordination."""

from __future__ import annotations
import asyncio
import contextlib
import email.utils
import hashlib
import json
import math
import random
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from b24api._error_types import FailurePhase
from b24api.contracts.json import _json_type_name
from b24api.contracts.policy import AmbiguityReason, ExecutionPolicy
from b24api.contracts.request import ReplaySafety, Request, RequestSummary, ResultErrorShape
from b24api.contracts.response import (
    BinaryEvidence,
    BinaryResponse,
    Response,
    ResponseEvidence,
    ResponseTime,
)
from b24api.errors import (
    AmbiguousExecutionError,
    ApiResponseError,
    B24ApiError,
    BatchCommandError,
    BudgetExceededError,
    CapabilityError,
    EnvelopeContractError,
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
from b24api.transport.base import TransportCapabilities, WireRequest, WireTransport
from b24api.transport.protocol import ProtocolCodec

if TYPE_CHECKING:
    from b24api.transport.base import Transport, WireResponse

type Clock = Callable[[], float]
type Sleeper = Callable[[float], Awaitable[None]]

_HTTP_STATUS_MINIMUM = 100
_HTTP_STATUS_MAXIMUM = 599
_HTTP_SUCCESS_MINIMUM = 200
_HTTP_SUCCESS_MAXIMUM = 299
_HTTP_REDIRECTION_MINIMUM = 300
_HTTP_CLIENT_ERROR_MINIMUM = 400
_RETRY_AFTER_CAP_SECONDS = 3_600.0


def _mark_dispatch_started(error: BaseException) -> None:
    """Attach private request-local proof that an attempt crossed dispatch admission."""
    error.__dict__["_b24api_dispatch_started"] = True


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
        self._wire_transport = transport if isinstance(transport, WireTransport) else None
        self.coordinator = coordinator or RateCoordinator(clock=clock)
        self.codec = codec or ProtocolCodec()
        self._clock = clock
        self._sleep = sleep
        self._random = random_source

    def _preflight_request(self, request: Request) -> None:
        """Validate transport representation without reserving budget or dispatching."""
        _preflight_transport(self._wire_transport, request)

    def context(self, policy: ExecutionPolicy | None = None) -> ExecutionContext:
        """Create an operation execution context."""
        return ExecutionContext(policy or ExecutionPolicy(), self.coordinator, clock=self._clock)

    async def execute(
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
        context = context or self.context(policy)
        wire = await self._execute_wire(request, context=context, work_class=work_class, binary=False)
        try:
            response = _decode_success(wire, request_summary=request.summary)
            _raise_embedded_result_error(
                request,
                response.result,
                http_status=wire.status_code,
                retry_codes=context.policy.retry.transient_api_codes,
            )
        except BaseException as error:
            _mark_dispatch_started(error)
            raise
        return response

    async def execute_bytes(
        self,
        request: Request,
        *,
        context: ExecutionContext | None = None,
        policy: ExecutionPolicy | None = None,
        work_class: WorkClass = WorkClass.INTERACTIVE_DIRECT,
    ) -> BinaryResponse:
        """Execute one request with explicitly selected binary success semantics."""
        if context is not None and policy is not None:
            raise ValueError("pass context or policy, not both")
        if not isinstance(request, Request):
            raise TypeError("request must be canonical Request")
        context = context or self.context(policy)
        wire = await self._execute_wire(request, context=context, work_class=work_class, binary=True)
        content_type = wire.content_type
        digest = hashlib.sha256(wire.body).hexdigest() if context.policy.binary_digest else None
        evidence = BinaryEvidence(wire.status_code, content_type, wire.byte_length, digest)
        return BinaryResponse(wire.body, content_type=content_type, evidence=evidence)

    async def _execute_wire(
        self,
        request: Request,
        *,
        context: ExecutionContext,
        work_class: WorkClass,
        binary: bool,
    ) -> WireResponse:
        """Attach request-local dispatch evidence to every escaping failure."""
        dispatch_started = False

        def mark_dispatch_started() -> None:
            nonlocal dispatch_started
            dispatch_started = True

        try:
            return await self._execute_wire_attempts(
                request,
                context=context,
                work_class=work_class,
                binary=binary,
                on_dispatch=mark_dispatch_started,
            )
        except BaseException as error:
            if dispatch_started:
                _mark_dispatch_started(error)
            raise

    async def _execute_wire_attempts(  # noqa: C901, PLR0912, PLR0915
        self,
        request: Request,
        *,
        context: ExecutionContext,
        work_class: WorkClass,
        binary: bool,
        on_dispatch: Callable[[], None],
    ) -> WireResponse:
        """Run the shared attempt loop and return one conclusive raw response."""
        self._preflight_request(request)
        wire_request = WireRequest(request) if self._wire_transport is not None else None
        await _checkpoint_pending_cancellation()
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
                    on_dispatch()
                    try:
                        async with asyncio.timeout(remaining):
                            wire = await _send_transport(
                                self.transport,
                                self._wire_transport,
                                request,
                                wire_request=wire_request,
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

            response_error: B24ApiError | None = None
            if not binary or not _HTTP_SUCCESS_MINIMUM <= wire.status_code <= _HTTP_SUCCESS_MAXIMUM:
                content_type = (wire.content_type or "").split(";", 1)[0].strip().casefold()
                body: bytes | None = wire.body
                if binary and content_type != "application/json" and not content_type.endswith("+json"):
                    body = None
                if wire.status_code < _HTTP_SUCCESS_MINIMUM or (
                    _HTTP_REDIRECTION_MINIMUM <= wire.status_code < _HTTP_CLIENT_ERROR_MINIMUM
                ):
                    response_error = HTTPGatewayError(
                        f"HTTP gateway error {wire.status_code}",
                        request_summary=request.summary,
                        evidence=_wire_evidence(wire),
                    )
                else:
                    response_error = self.codec.error_from_http(
                        status_code=wire.status_code,
                        body=body,
                        request_summary=request.summary,
                        headers=wire.header_map,
                        retry_codes=context.policy.retry.transient_api_codes,
                    )
                if response_error is None and not _HTTP_SUCCESS_MINIMUM <= wire.status_code <= _HTTP_SUCCESS_MAXIMUM:
                    response_error = HTTPGatewayError(
                        f"HTTP gateway error {wire.status_code}",
                        request_summary=request.summary,
                        evidence=_wire_evidence(wire),
                    )
            if response_error is None:
                _raise_for_pending_cancellation()
                if context.remaining_time(retry_started=retry_started) <= 0:
                    raise BudgetExceededError("transport completed after execution time budget")
                return wire
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
                reason=AmbiguityReason.RESPONSE_LIMIT_AFTER_DISPATCH,
                declared_unsafe=safety is ReplaySafety.UNSAFE,
                request_summary=request.summary,
                evidence=error.evidence,
            ) from error
        if isinstance(error, TransportError) and error.possible_acceptance and safety is not ReplaySafety.SAFE:
            reason = (
                AmbiguityReason.DEADLINE_AFTER_DISPATCH
                if isinstance(error.__cause__, TimeoutError)
                else AmbiguityReason.CONNECTION_LOST_AFTER_DISPATCH
            )
            raise AmbiguousExecutionError(
                "Request may have executed; automatic replay is forbidden",
                reason=reason,
                declared_unsafe=safety is ReplaySafety.UNSAFE,
                request_summary=request.summary,
                evidence=error.evidence,
            ) from error
        if (
            isinstance(error, HTTPGatewayError)
            and not isinstance(error, EnvelopeContractError)
            and error.http_status is not None
            and not _HTTP_SUCCESS_MINIMUM <= error.http_status <= _HTTP_SUCCESS_MAXIMUM
            and error.http_status in context.policy.ambiguity.ambiguous_unstructured_statuses
            and safety is not ReplaySafety.SAFE
        ):
            raise AmbiguousExecutionError(
                "Request may have executed before the unstructured HTTP failure",
                reason=AmbiguityReason.HTTP_STATUS_AFTER_DISPATCH,
                declared_unsafe=safety is ReplaySafety.UNSAFE,
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
    return (
        isinstance(error, HTTPGatewayError)
        and not isinstance(error, EnvelopeContractError)
        and error.http_status is not None
        and not _HTTP_SUCCESS_MINIMUM <= error.http_status <= _HTTP_SUCCESS_MAXIMUM
        and error.http_status in policy.retry.transient_http_statuses
    )


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


def _preflight_transport(transport: WireTransport | None, request: Request) -> None:
    """Reject unsupported request representation before budget reservation or I/O."""
    advanced = request.encoding.value != "json" or bool(request.headers.items)
    if transport is None:
        if not advanced:
            return
        raise CapabilityError("transport does not support advanced request delivery")
    capabilities = transport.capabilities
    if not isinstance(capabilities, TransportCapabilities):
        raise CapabilityError("transport exposes malformed capabilities")
    if request.encoding not in capabilities.encodings:
        raise CapabilityError(f"transport does not support {request.encoding.value} request bodies")
    if request.headers.items and not capabilities.scoped_headers:
        raise CapabilityError("transport does not support scoped request headers")


async def _send_transport(  # noqa: PLR0913 - keeps legacy and wire boundaries explicit
    transport: Transport,
    wire_transport: WireTransport | None,
    request: Request,
    *,
    wire_request: WireRequest | None,
    attempt_timeout: float,
    max_response_bytes: int,
) -> WireResponse:
    if wire_transport is not None:
        capabilities = wire_transport.capabilities
        if not isinstance(capabilities, TransportCapabilities):
            raise CapabilityError("transport exposes malformed capabilities")
        if wire_request is None:
            raise RuntimeError("wire transport request was not prepared")
        return await wire_transport.send_wire(
            wire_request,
            attempt_timeout=attempt_timeout,
            max_response_bytes=max_response_bytes,
        )
    return await transport.send(
        request,
        attempt_timeout=attempt_timeout,
        max_response_bytes=max_response_bytes,
    )


def _resolve_optional_path(value: object, path: tuple[str | int, ...]) -> tuple[bool, object]:
    current = value
    for part in path:
        if isinstance(part, str):
            if not isinstance(current, Mapping) or part not in current:
                return False, None
            current = current[part]
        else:
            if not isinstance(current, list | tuple) or part >= len(current):
                return False, None
            current = current[part]
    return True, current


def _result_protocol_error(
    message: str,
    request: Request,
    *,
    observed: object,
    path: tuple[str | int, ...],
) -> ProtocolError:
    """Create value-free embedded-result shape evidence."""
    error = ProtocolError(message, request_summary=request.summary)
    error.observed_type = _json_type_name(observed)  # type: ignore[attr-defined]
    error.path = path  # type: ignore[attr-defined]
    return error


def _raise_embedded_result_error(  # noqa: C901, PLR0912
    request: Request,
    result: object,
    *,
    http_status: int,
    retry_codes: frozenset[str],
    batch: bool = False,
) -> None:
    """Evaluate the request-local embedded-error contract."""
    spec = request.result_error
    if spec is None:
        return
    found, selected = _resolve_optional_path(result, spec.selector.path)
    if not found or selected is None:
        return
    candidates: tuple[tuple[int | None, Mapping[object, object]], ...]
    if spec.shape is ResultErrorShape.MAPPING:
        if not isinstance(selected, Mapping):
            raise _result_protocol_error(
                "Embedded result error selector must resolve to a mapping",
                request,
                observed=selected,
                path=spec.selector.path,
            )
        candidates = ((None, selected),)
    else:
        if not isinstance(selected, list | tuple):
            raise _result_protocol_error(
                "Embedded result error selector must resolve to a sequence",
                request,
                observed=selected,
                path=spec.selector.path,
            )
        candidates = tuple(enumerate(selected))
    for index, candidate in candidates:
        candidate_path = spec.selector.path if index is None else (*spec.selector.path, index)
        if not isinstance(candidate, Mapping):
            raise _result_protocol_error(
                "Embedded result error item must be a mapping",
                request,
                observed=candidate,
                path=candidate_path,
            )
        found, code = _resolve_optional_path(candidate, spec.code_path)
        if not found or code is None or code == "" or (type(code) is int and code == 0):
            continue
        if isinstance(code, bool) or not isinstance(code, str | int):
            raise _result_protocol_error(
                "Embedded result error code must be a string or integer",
                request,
                observed=code,
                path=(*candidate_path, *spec.code_path),
            )
        description: str | None = None
        if spec.description_path is not None:
            description_found, raw_description = _resolve_optional_path(candidate, spec.description_path)
            if description_found and raw_description is not None:
                if isinstance(raw_description, bool) or not isinstance(raw_description, str | int):
                    raise _result_protocol_error(
                        "Embedded result error description must be a string or integer",
                        request,
                        observed=raw_description,
                        path=(*candidate_path, *spec.description_path),
                    )
                description = str(raw_description)
        error_type = BatchCommandError if batch else ApiResponseError
        raise error_type(
            code=code,
            description=description,
            request_summary=request.summary,
            http_status=http_status,
            retryable=str(code).strip().casefold() in retry_codes,
        )


def _decode_success(wire: WireResponse, *, request_summary: RequestSummary) -> Response:
    evidence = _wire_evidence(wire)
    try:
        payload = json.loads(
            wire.body,
            parse_constant=_reject_json_constant,
            object_pairs_hook=_reject_duplicate_json_keys,
        )
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as error:
        raise EnvelopeContractError(
            "Malformed successful HTTP response",
            request_summary=request_summary,
            evidence=evidence,
        ) from error
    if not isinstance(payload, Mapping) or "result" not in payload:
        raise EnvelopeContractError(
            "Successful response is missing the result envelope",
            request_summary=request_summary,
            evidence=evidence,
        )
    total = payload.get("total")
    next_value = payload.get("next")
    if total is not None and (not isinstance(total, int) or isinstance(total, bool)):
        raise EnvelopeContractError(
            "Response total must be an integer",
            request_summary=request_summary,
            evidence=evidence,
        )
    if isinstance(total, int) and total < -1:
        raise EnvelopeContractError(
            "Response total must be -1 or non-negative",
            request_summary=request_summary,
            evidence=evidence,
        )
    if next_value is not None and (not isinstance(next_value, int) or isinstance(next_value, bool)):
        raise EnvelopeContractError(
            "Response next must be an integer",
            request_summary=request_summary,
            evidence=evidence,
        )
    # Some Bitrix24 endpoints use -1 as the terminal continuation sentinel.
    # Canonical consumers use None for the same meaning.
    if next_value == -1:
        next_value = None
    elif isinstance(next_value, int) and next_value < -1:
        raise EnvelopeContractError(
            "Response next must be -1 or non-negative",
            request_summary=request_summary,
            evidence=evidence,
        )
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


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Decode an object while enforcing unique JSON member names."""
    decoded: dict[str, object] = {}
    for key, value in pairs:
        if key in decoded:
            raise ValueError(f"duplicate JSON object name: {key}")
        decoded[key] = value
    return decoded


__all__ = ["Executor"]
