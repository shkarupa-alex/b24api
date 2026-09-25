"""Transport lifecycle, replay-aware retries, and shared rate coordination."""

from __future__ import annotations
import asyncio
import hashlib
import json
import math
import random
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from b24api._error_types import FailurePhase
from b24api.contracts.evidence import ResponseEvidence
from b24api.contracts.json import _json_type_name
from b24api.contracts.policy import AmbiguityReason, ExecutionPolicy
from b24api.contracts.request import ReplaySafety, Request, ResultErrorShape, diagnostic_context
from b24api.contracts.response import (
    BinaryEvidence,
    BinaryResponse,
    Response,
    ResponseTime,
)
from b24api.errors import (
    AmbiguousExecutionError,
    ApiResponseError,
    B24ApiError,
    BatchCommandError,
    BudgetExceededError,
    EnvelopeContractError,
    HTTPGatewayError,
    ProtocolError,
    ResponseTooLargeError,
    TransportError,
)
from b24api.execution.boundary import send_transport
from b24api.execution.context import (
    ExecutionContext,
    _checkpoint_pending_cancellation,
    _raise_for_pending_cancellation,
)
from b24api.execution.rate import DeadlineBudget, RateCoordinator, WorkClass
from b24api.execution.throttle import _retry_after_seconds, _retry_delay, _throttle_reason
from b24api.redaction import DEFAULT_REDACTOR, Redactor
from b24api.transport.base import WireRequest, WireTransport, preflight_transport
from b24api.transport.protocol import ProtocolCodec

if TYPE_CHECKING:
    from b24api.contracts.request_summary import RequestSummary
    from b24api.transport.base import Transport, WireResponse

type Clock = Callable[[], float]
type Sleeper = Callable[[float], Awaitable[None]]

_HTTP_STATUS_MINIMUM = 100
_HTTP_STATUS_MAXIMUM = 599


class _BodyMode(Enum):
    """Select how a conclusive success body is interpreted."""

    BINARY = "binary"
    JSON = "json"
    JSON_STRICT_MEMBERS = "json_strict_members"


@dataclass(frozen=True, slots=True)
class _ParsedBody:
    """Carry the result of the one strict parse of a JSON success body."""

    payload: object
    failure: ValueError | None


@dataclass(slots=True)
class _RequestAttempts:
    """Attempts and retry clock one request shares across its sends, including a batch's replay rounds."""

    started: float | None = None
    used: int = 0

    def begin(self, clock: Clock) -> tuple[float, int]:
        """Start the retry clock on the first send and return it with the attempts already spent."""
        if self.started is None:
            self.started = clock()
        return self.started, self.used


class _DecodedJsonObject(dict[str, object]):
    """Last-member-wins JSON object retaining duplicate-name evidence."""

    __slots__ = ("duplicate_names",)

    def __init__(self, pairs: list[tuple[str, object]]) -> None:
        duplicates: list[str] = []
        values: dict[str, object] = {}
        for key, value in pairs:
            if key in values:
                duplicates.append(key)
            values[key] = value
        super().__init__(values)
        self.duplicate_names = tuple(duplicates)


_HTTP_SUCCESS_MINIMUM = 200
_HTTP_SUCCESS_MAXIMUM = 299
_HTTP_REDIRECTION_MINIMUM = 300
_HTTP_CLIENT_ERROR_MINIMUM = 400


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
        try:
            host = transport.host
        except AttributeError:
            raise TypeError("transport must expose the normalized portal host its coordinator binds") from None
        self.coordinator.bind_host(host)
        self.codec = codec or ProtocolCodec()
        self._clock = clock
        self._sleep = sleep
        self._random = random_source

    def preflight_request(self, request: Request) -> None:
        """Validate transport representation without reserving budget or dispatching."""
        preflight_transport(self._wire_transport, request)

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
        strict_json_members: bool = False,
        _admission_methods: frozenset[str] | None = None,
        _attempts: _RequestAttempts | None = None,
    ) -> Response:
        """Execute one canonical request."""
        if context is not None and policy is not None:
            raise ValueError("pass context or policy, not both")
        if not isinstance(request, Request):
            raise TypeError("request must be canonical Request")
        if not isinstance(strict_json_members, bool):
            raise TypeError("strict_json_members must be a boolean")
        context = context or self.context(policy)
        attempts = _attempts or _RequestAttempts()
        spent = attempts.used
        try:
            wire, parsed = await self._execute_wire_attempts(
                request,
                context=context,
                work_class=work_class,
                mode=_BodyMode.JSON_STRICT_MEMBERS if strict_json_members else _BodyMode.JSON,
                methods=_admission_methods or frozenset({request.method}),
                shared=attempts,
            )
        except BaseException as error:
            if attempts.used > spent:
                _mark_dispatch_started(error)
            raise
        try:
            response = _decode_success(wire, parsed, request_summary=request.summary)
            _raise_embedded_result_error(
                request,
                response.result,
                http_status=wire.status_code,
                retry_codes=context.policy.retry.transient_api_codes,
                redactor=self.codec.redactor,
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
        attempts = _RequestAttempts()
        try:
            wire, _parsed = await self._execute_wire_attempts(
                request,
                context=context,
                work_class=work_class,
                mode=_BodyMode.BINARY,
                methods=frozenset({request.method}),
                shared=attempts,
            )
        except BaseException as error:
            if attempts.used:
                _mark_dispatch_started(error)
            raise
        content_type = wire.content_type
        digest = hashlib.sha256(wire.body).hexdigest() if context.policy.binary_digest else None
        evidence = BinaryEvidence(wire.status_code, content_type, wire.byte_length, digest)
        return BinaryResponse(wire.body, content_type=content_type, evidence=evidence)

    async def _execute_wire_attempts(  # noqa: C901, PLR0913, PLR0915
        self,
        request: Request,
        *,
        context: ExecutionContext,
        work_class: WorkClass,
        mode: _BodyMode,
        methods: frozenset[str],
        shared: _RequestAttempts,
    ) -> tuple[WireResponse, _ParsedBody | None]:
        """Run the attempt loop and return one conclusive raw response; ``shared`` counts every dispatch."""
        self.preflight_request(request)
        wire_request = WireRequest(request) if self._wire_transport is not None else None
        await _checkpoint_pending_cancellation()
        await context.start()
        retry_started, attempts = shared.begin(self._clock)
        last_error: B24ApiError | None = None
        while True:
            if (remaining := context.remaining_time(retry_started=retry_started)) <= 0:
                raise BudgetExceededError("execution time budget exhausted")
            scheduled_class = work_class if attempts == 0 else WorkClass.RETRY
            try:
                async with asyncio.timeout(remaining):
                    permit = await context.coordinator.acquire(
                        scheduled_class, methods=methods, budget=DeadlineBudget(self._clock() + remaining)
                    )
            except TimeoutError as error:
                raise BudgetExceededError("permit wait exhausted execution time budget") from (last_error or error)
            try:
                async with permit:
                    remaining = context.remaining_time(retry_started=retry_started)
                    if remaining <= 0:
                        raise BudgetExceededError("execution time budget exhausted before dispatch")
                    await context.reserve_attempt(attempts_for_request=attempts, retry_started=retry_started)
                    shared.used = attempts + 1
                    try:
                        async with asyncio.timeout(remaining):
                            wire = await send_transport(
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
                last_error = error
                attempts += 1
                continue

            response_error, parsed = self._classify_response(request, wire, context=context, mode=mode)
            if response_error is None:
                _raise_for_pending_cancellation()
                if context.remaining_time(retry_started=retry_started) <= 0:
                    raise BudgetExceededError("transport completed after execution time budget")
                return wire, parsed
            code = response_error.normalized_code if isinstance(response_error, ApiResponseError) else None
            method_cooldown = (
                await context.coordinator.observe_api_throttle(request.method, code)
                if code == "operation_time_limit"
                else 0.0
            )
            throttle_delay = _retry_after_seconds(wire)
            if throttle_delay is not None:
                merged = await context.coordinator.observe_throttle(
                    throttle_delay, reason=_throttle_reason(response_error)
                )
                await context.record_cooldown(merged)
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
                method_cooldown=method_cooldown,
            )
            last_error = response_error
            attempts += 1

    def _classify_response(
        self,
        request: Request,
        wire: WireResponse,
        *,
        context: ExecutionContext,
        mode: _BodyMode,
    ) -> tuple[B24ApiError | None, _ParsedBody | None]:
        """Classify one conclusive response, parsing a JSON success body exactly once."""
        success = _HTTP_SUCCESS_MINIMUM <= wire.status_code <= _HTTP_SUCCESS_MAXIMUM
        if success and mode is _BodyMode.BINARY:
            return None, None
        parsed = None
        body: bytes | Mapping[str, object] | None = wire.body
        if success:
            parsed = _parse_success_body(wire.body, strict_members=mode is _BodyMode.JSON_STRICT_MEMBERS)
            if parsed.failure is not None and not isinstance(parsed.failure, json.JSONDecodeError):
                # Invalid UTF-8, a non-finite number or a duplicate correlation key is a defect only the
                # strict parse sees; a lenient second parse must not turn it into a structured API error.
                return None, parsed
            if parsed.failure is None:
                payload = parsed.payload
                if not (isinstance(payload, Mapping) and "error" in payload):
                    return None, parsed
                # The codec classifies the structured error from the one strict parse, not a second one.
                body = payload
        content_type = (wire.content_type or "").split(";", 1)[0].strip().casefold()
        if mode is _BodyMode.BINARY and content_type != "application/json" and not content_type.endswith("+json"):
            body = None
        if wire.status_code < _HTTP_SUCCESS_MINIMUM or (
            _HTTP_REDIRECTION_MINIMUM <= wire.status_code < _HTTP_CLIENT_ERROR_MINIMUM
        ):
            return HTTPGatewayError(
                f"HTTP gateway error {wire.status_code}",
                request_summary=request.summary,
                evidence=_wire_evidence(wire),
            ), parsed
        response_error = self.codec.error_from_http(
            status_code=wire.status_code,
            body=body,
            request_summary=request.summary,
            headers=wire.header_map,
            retry_codes=context.policy.retry.transient_api_codes,
            diagnostics=diagnostic_context(request),
        )
        if response_error is None and not success:
            response_error = HTTPGatewayError(
                f"HTTP gateway error {wire.status_code}",
                request_summary=request.summary,
                evidence=_wire_evidence(wire),
            )
        return response_error, parsed

    async def pause_before_replay(
        self, context: ExecutionContext, *, attempts: _RequestAttempts, method_cooldown: float = 0.0
    ) -> bool:
        """Wait before sending part of a physical batch again; ``False`` when the budget forbids another round.

        The round spends the same attempt and retry-time budget as the sends before it.
        """
        if attempts.started is None or attempts.used >= context.policy.max_attempts_per_request:
            return False
        delay = max(
            _retry_delay(context.policy, retry_number=attempts.used, random_source=self._random), method_cooldown
        )
        if delay >= context.remaining_time(retry_started=attempts.started):
            return False
        await context.record_retry()
        # The coordinator holds the next permit until a method cooldown ends, as for a direct retry.
        if not method_cooldown and delay > 0:
            await self._sleep(delay)
        return True

    async def _prepare_retry(  # noqa: PLR0913
        self,
        request: Request,
        error: B24ApiError,
        *,
        context: ExecutionContext,
        retry_started: float,
        attempts: int,
        wire: WireResponse | None = None,
        method_cooldown: float = 0.0,
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
        # A physical batch follows the same rule with its combined safety (owner decision, 2026-09-25): SAFE is
        # replayed within the budget; UNSAFE/UNKNOWN only when the failure proves nothing ran.
        retryable = _is_retryable(error, safety=safety, policy=context.policy)
        if not retryable:
            raise error

        next_attempt = attempts + 1
        if next_attempt >= context.policy.max_attempts_per_request:
            raise BudgetExceededError("per-request attempt budget exhausted") from error
        delay = _retry_delay(context.policy, retry_number=next_attempt, random_source=self._random)
        throttle_delay = _retry_after_seconds(wire) if wire is not None else None
        # The coordinator holds the next permit until a method cooldown ends; past the budget it cannot succeed.
        delay = max(delay, throttle_delay or 0.0, method_cooldown)
        remaining = context.remaining_time(retry_started=retry_started)
        if delay >= remaining:
            raise BudgetExceededError("retry delay would exceed elapsed budget") from error
        await context.record_retry()
        if throttle_delay is None and not method_cooldown and delay > 0:
            await self._sleep(delay)


def _is_retryable(error: B24ApiError, *, safety: ReplaySafety, policy: ExecutionPolicy) -> bool:
    """SAFE work is retried after any transient failure; other work only when the failure proves nothing ran."""
    if isinstance(error, TransportError):
        return error.retryable and (not error.possible_acceptance or safety is ReplaySafety.SAFE)
    if isinstance(error, ApiResponseError):
        return error.retryable and (safety is ReplaySafety.SAFE or _refused_before_running(error, policy))
    transient = (
        isinstance(error, HTTPGatewayError)
        and not isinstance(error, EnvelopeContractError)
        and error.http_status is not None
        and not _HTTP_SUCCESS_MINIMUM <= error.http_status <= _HTTP_SUCCESS_MAXIMUM
        and error.http_status in policy.retry.transient_http_statuses
    )
    # Only a listed refusal status (423, 425, 429 by default) proves the request was not accepted.
    return transient and (
        safety is ReplaySafety.SAFE
        or (
            error.http_status in policy.ambiguity.refusal_http_statuses
            and error.http_status not in policy.ambiguity.ambiguous_unstructured_statuses
        )
    )


def _refused_before_running(error: ApiResponseError, policy: ExecutionPolicy) -> bool:
    """Return whether Bitrix answered with a listed refusal (a request or time quota by default) before running."""
    return str(error.original_code).strip().casefold() in policy.ambiguity.refusal_api_codes


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


def _raise_embedded_result_error(  # noqa: C901, PLR0912, PLR0913
    request: Request,
    result: object,
    *,
    http_status: int,
    retry_codes: frozenset[str],
    batch: bool = False,
    redactor: Redactor = DEFAULT_REDACTOR,
) -> None:
    """Evaluate the request-local embedded-error contract, rendering through that request's context."""
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
            redactor=redactor,
            diagnostics=diagnostic_context(request),
        )


def _parse_success_body(body: bytes, *, strict_members: bool) -> _ParsedBody:
    """Run the one strict JSON parse of a success body, keeping a failure for the envelope decoder."""
    try:
        payload = json.loads(
            body.decode("utf-8"),
            parse_constant=_reject_json_constant,
            object_pairs_hook=_DecodedJsonObject if strict_members else None,
        )
        if strict_members:
            _reject_duplicate_batch_correlation_keys(payload)
    except ValueError as error:  # JSONDecodeError and UnicodeDecodeError are ValueErrors
        return _ParsedBody(None, error)
    return _ParsedBody(payload, None)


def _decode_success(wire: WireResponse, parsed: _ParsedBody | None, *, request_summary: RequestSummary) -> Response:
    if parsed is None:
        raise RuntimeError("JSON success response lacks its parsed body")
    evidence = _wire_evidence(wire)
    if parsed.failure is not None:
        raise EnvelopeContractError(
            "Malformed successful HTTP response",
            request_summary=request_summary,
            evidence=evidence,
        ) from parsed.failure
    payload = parsed.payload
    if not isinstance(payload, Mapping) or "result" not in payload:
        raise EnvelopeContractError(
            "Successful response is missing the result envelope",
            request_summary=request_summary,
            evidence=evidence,
        )
    total, next_value = payload.get("total"), payload.get("next")
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

    operating, operating_reset_at = raw.get("operating"), raw.get("operating_reset_at")
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


def _reject_duplicate_batch_correlation_keys(payload: object) -> None:
    """Enforce unique command keys without changing ordinary row JSON semantics."""
    if not isinstance(payload, Mapping):
        return
    batch_envelope = payload.get("result")
    if not isinstance(batch_envelope, Mapping):
        return
    for field in ("result", "result_error", "result_total", "result_next"):
        correlation = batch_envelope.get(field)
        if isinstance(correlation, _DecodedJsonObject) and correlation.duplicate_names:
            raise ValueError(f"duplicate batch correlation key in {field}")


__all__ = ["Executor"]
