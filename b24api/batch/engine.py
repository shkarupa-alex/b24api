"""Bounded fail-fast and total-outcome Bitrix batch execution."""

from __future__ import annotations
import asyncio
import contextlib
from collections.abc import AsyncIterable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from b24api.batch.outcome import (
    BatchCommandEvidence,
    BatchFailure,
    BatchOutcome,
    BatchSuccess,
)
from b24api.contracts.policy import (
    ExecutionPolicy,
    ReplayDisposition,
)
from b24api.contracts.request import ReplaySafety, Request
from b24api.contracts.response import Response
from b24api.errors import B24ApiError, BatchCommandError, ErrorOrigin, ProtocolError
from b24api.execution import (
    ExecutionContext,
    Executor,
    WorkClass,
)
from b24api.plans import PORTAL_BATCH_CAP
from b24api.query import build_query

if TYPE_CHECKING:
    from b24api.batch.stream import _BatchOutcomeStream
    from b24api.contracts.json import JsonValue

type RequestMapping = Mapping[str, object]
type RequestWithPayload = tuple[Request | RequestMapping, object]
type BatchInput = Request | RequestMapping | RequestWithPayload
type BatchSource = Iterable[BatchInput] | AsyncIterable[BatchInput]
type BatchStreamItem = BatchOutcome

_REQUEST_PAYLOAD_TUPLE_LENGTH = 2
_MISSING = object()
_SYNC_EXHAUSTED = object()


@dataclass(frozen=True, slots=True)
class _Command:
    index: int
    stable_key: str
    request: Request
    payload: object
    has_payload: bool


@dataclass(frozen=True, slots=True)
class _Chunk:
    commands: tuple[_Command, ...]
    source_error: Exception | None = None


@dataclass(frozen=True, slots=True)
class _BatchEnvelope:
    results: Mapping[str, object]
    errors: Mapping[str, object]
    totals: Mapping[str, object]
    continuations: Mapping[str, object]


class BatchExecutor:
    """Execute one correlated physical Bitrix batch chunk."""

    def __init__(
        self,
        executor: Executor,
        *,
        portal_command_cap: int = PORTAL_BATCH_CAP,
    ) -> None:
        """Initialize instance state."""
        if isinstance(portal_command_cap, bool) or not 1 <= portal_command_cap <= PORTAL_BATCH_CAP:
            raise ValueError("portal command cap must be between 1 and 50")
        self.executor = executor
        self.portal_command_cap = portal_command_cap

    def _outcomes(
        self,
        requests: BatchSource,
        *,
        batch_size: int | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> _BatchOutcomeStream:
        """Build the internal total-outcome stream used by kernel tests and traversal."""
        from b24api.batch.stream import _BatchOutcomeStream  # noqa: PLC0415

        size = self.portal_command_cap if batch_size is None else batch_size
        if isinstance(size, bool) or not 1 <= size <= self.portal_command_cap:
            raise ValueError("batch_size must be within the portal command cap")
        return _BatchOutcomeStream(
            self,
            requests,
            batch_size=size,
            policy=policy or ExecutionPolicy(),
        )

    async def _execute_chunk(
        self,
        commands: tuple[_Command, ...],
        *,
        context: ExecutionContext,
        halt: bool,
    ) -> tuple[BatchOutcome, ...]:
        request = _batch_request(commands, halt=halt)
        try:
            response = await self.executor.execute(request, context=context, work_class=WorkClass.BATCH)
            envelope = _decode_batch_envelope(response.result)
        except asyncio.CancelledError:
            raise
        except B24ApiError as error:
            return tuple(_shared_failure(command, error) for command in commands)

        outcomes: list[BatchOutcome] = []
        for command in commands:
            evidence = BatchCommandEvidence(command.index, command.stable_key)
            if command.stable_key in envelope.errors:
                command_error = self._command_error(
                    envelope.errors[command.stable_key],
                    command,
                    retry_codes=context.policy.retry.transient_api_codes,
                )
                evidence = BatchCommandEvidence(
                    command.index,
                    command.stable_key,
                    original_code=command_error.original_code,
                    normalized_code=command_error.normalized_code,
                )
                outcomes.append(_command_failure(command, command_error, evidence=evidence))
                continue
            if command.stable_key not in envelope.results:
                missing_error = ProtocolError(
                    "Batch result map is missing a submitted command",
                    origin=ErrorOrigin.PROTOCOL,
                    request_summary=command.request.summary,
                )
                outcomes.append(_command_failure(command, missing_error, evidence=evidence))
                continue
            try:
                command_response = Response(
                    envelope.results[command.stable_key],
                    total=_optional_batch_integer(envelope.totals, command.stable_key, field="total"),
                    next=_optional_batch_integer(envelope.continuations, command.stable_key, field="next"),
                    evidence=response.evidence,
                )
            except (TypeError, ValueError) as error:
                protocol_error = ProtocolError(
                    "Batch command metadata is malformed",
                    origin=ErrorOrigin.PROTOCOL,
                    request_summary=command.request.summary,
                    evidence=response.evidence,
                )
                protocol_error.__cause__ = error
                outcomes.append(_command_failure(command, protocol_error, evidence=evidence))
                continue
            outcomes.append(
                BatchSuccess._from_response(  # noqa: SLF001 - trusted correlated decoder fast path
                    command.index,
                    command.stable_key,
                    command.request,
                    command_response,
                    command.payload,
                    evidence,
                ),
            )
        return tuple(outcomes)

    async def execute_requests(
        self,
        requests: tuple[Request, ...],
        *,
        context: ExecutionContext,
    ) -> tuple[BatchOutcome, ...]:
        """Execute one scheduler-owned chunk with total per-command correlation."""
        if not requests or len(requests) > self.portal_command_cap:
            raise ValueError("scheduler batch chunk must contain 1..portal_command_cap requests")
        commands = tuple(
            _Command(
                index=index,
                stable_key=f"c{index:012d}",
                request=request,
                payload=None,
                has_payload=False,
            )
            for index, request in enumerate(requests)
        )
        return await self._execute_chunk(
            commands,
            context=context,
            halt=False,
        )

    def _command_error(
        self,
        raw: object,
        command: _Command,
        *,
        retry_codes: frozenset[str],
    ) -> BatchCommandError:
        if not isinstance(raw, Mapping):
            return BatchCommandError(
                code="malformed_batch_error",
                description="Batch error entry must be an object",
                request_summary=command.request.summary,
            )
        code = raw.get("error")
        if not isinstance(code, str | int) or isinstance(code, bool):
            return BatchCommandError(
                code="malformed_batch_error",
                description="Batch error entry has no scalar error code",
                request_summary=command.request.summary,
            )
        normalized = str(code).strip().casefold()
        description = raw.get("error_description")
        return BatchCommandError(
            code=code,
            description=None if description is None else str(description),
            request_summary=command.request.summary,
            retryable=normalized in retry_codes,
        )


def _coerce_input(raw: BatchInput) -> tuple[Request, object, bool]:
    if isinstance(raw, tuple):
        if len(raw) != _REQUEST_PAYLOAD_TUPLE_LENGTH:
            raise ValueError("request/payload tuple must contain exactly two values")
        request, payload = raw
        return _coerce_request(request), payload, True
    return _coerce_request(raw), None, False


def _coerce_request(raw: Request | RequestMapping) -> Request:
    if isinstance(raw, Request):
        return raw
    if not isinstance(raw, Mapping):
        raise TypeError("batch input must be a Request, mapping, or request/payload tuple")
    unknown = set(raw) - {"method", "parameters", "replay_safety"}
    if unknown:
        raise ValueError(f"unknown request fields: {sorted(unknown)}")
    method = raw.get("method")
    parameters = raw.get("parameters")
    safety = raw.get("replay_safety", ReplaySafety.UNKNOWN)
    if not isinstance(method, str):
        raise TypeError("mapping request requires a string method")
    if parameters is not None and not isinstance(parameters, Mapping):
        raise TypeError("mapping request parameters must be a mapping")
    if isinstance(safety, str):
        with contextlib.suppress(ValueError):
            safety = ReplaySafety(safety)
    if not isinstance(safety, ReplaySafety):
        raise TypeError("mapping replay_safety must be a ReplaySafety or enum value")
    return Request(method, parameters, replay_safety=safety)


def _batch_request(commands: tuple[_Command, ...], *, halt: bool) -> Request:
    safety_values = {command.request.replay_safety or ReplaySafety.UNKNOWN for command in commands}
    if safety_values == {ReplaySafety.SAFE}:
        safety = ReplaySafety.SAFE
    elif ReplaySafety.UNSAFE in safety_values:
        safety = ReplaySafety.UNSAFE
    else:
        safety = ReplaySafety.UNKNOWN
    encoded = {command.stable_key: _command_query(command.request) for command in commands}
    return Request("batch", {"halt": int(halt), "cmd": encoded}, replay_safety=safety)


def _command_query(request: Request) -> str:
    query = build_query(cast("dict[Any, Any]", request.to_wire_parameters()))
    return request.method if not query else f"{request.method}?{query}"


def _decode_batch_envelope(raw: JsonValue) -> _BatchEnvelope:
    if not isinstance(raw, dict):
        raise ProtocolError("Batch result envelope must be an object", origin=ErrorOrigin.PROTOCOL)
    if "result_error" not in raw:
        raise ProtocolError("Batch result envelope is missing result_error", origin=ErrorOrigin.PROTOCOL)
    results = _decode_php_map(raw.get("result"), field="result")
    errors = _decode_php_map(raw["result_error"], field="result_error")
    totals = _decode_optional_php_map(raw, field="result_total")
    continuations = _decode_optional_php_map(raw, field="result_next")
    return _BatchEnvelope(
        results=results,
        errors=errors,
        totals=totals,
        continuations=continuations,
    )


def _decode_optional_php_map(raw: dict[str, JsonValue], *, field: str) -> dict[str, JsonValue]:
    if field not in raw:
        return {}
    return _decode_php_map(raw[field], field=field)


def _optional_batch_integer(values: Mapping[str, object], key: str, *, field: str) -> int | None:
    if key not in values:
        return None
    value = values[key]
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"batch {field} must be an integer")
    return value


def _decode_php_map(raw: JsonValue, *, field: str) -> dict[str, JsonValue]:
    if isinstance(raw, list):
        if raw:
            raise ProtocolError(
                f"Non-empty PHP batch {field} array is malformed",
                origin=ErrorOrigin.PROTOCOL,
            )
        return {}
    if not isinstance(raw, dict):
        raise ProtocolError(
            f"Batch {field} must be an object or empty array",
            origin=ErrorOrigin.PROTOCOL,
        )
    return raw


def _command_failure(
    command: _Command,
    error: B24ApiError,
    *,
    evidence: BatchCommandEvidence,
) -> BatchFailure:
    safety = command.request.replay_safety or ReplaySafety.UNKNOWN
    eligible = safety is ReplaySafety.SAFE and error.retryable
    return BatchFailure(
        command.index,
        command.stable_key,
        command.request,
        error,
        replay_safety=safety,
        replay_disposition=ReplayDisposition.ELIGIBLE if eligible else ReplayDisposition.NOT_ELIGIBLE,
        payload=command.payload,
        evidence=evidence,
    )


def _shared_failure(command: _Command, error: B24ApiError) -> BatchFailure:
    return _command_failure(
        command,
        error,
        evidence=BatchCommandEvidence(command.index, command.stable_key),
    )


def _raise_source_error(error: Exception) -> None:
    raise error


def _batch_outcome_row_weight(outcome: BatchOutcome) -> int:
    return outcome.decoded_rows if isinstance(outcome, BatchSuccess) else 1


__all__: list[str] = []
