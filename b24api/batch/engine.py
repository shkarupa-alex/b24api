"""Bounded fail-fast and total-outcome Bitrix batch execution."""

from __future__ import annotations
import asyncio
from collections.abc import AsyncIterable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from b24api._error_types import ErrorOrigin
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
from b24api.contracts.request import ReplaySafety, Request, RouteKind
from b24api.contracts.response import Response
from b24api.encoding import encode_php_query
from b24api.errors import B24ApiError, BatchCommandError, CapabilityError, ProtocolError
from b24api.execution import (
    ExecutionContext,
    Executor,
    WorkClass,
)
from b24api.execution.executor import _raise_embedded_result_error
from b24api.traversal.plans import PORTAL_BATCH_CAP

if TYPE_CHECKING:
    from b24api.batch.stream import _BatchOutcomeStream
    from b24api.contracts.json import JsonValue

_MISSING = object()
_SYNC_EXHAUSTED = object()


@dataclass(frozen=True, slots=True)
class _BatchInput:
    request: Request
    correlation: object = None


type _BatchItem = Request | _BatchInput
type BatchSource = Iterable[_BatchItem] | AsyncIterable[_BatchItem]
type BatchStreamItem = BatchOutcome


@dataclass(frozen=True, slots=True)
class _Command:
    index: int
    stable_key: str
    request: Request
    correlation: object


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

    async def _execute_chunk(  # noqa: PLR0913
        self,
        commands: tuple[_Command, ...],
        *,
        context: ExecutionContext,
        halt: bool,
        advisory_totals: bool = False,
        strict_envelope: bool = False,
        strict_json_members: bool = False,
    ) -> tuple[BatchOutcome, ...]:
        rejected: dict[int, BatchOutcome] = {}
        eligible = commands
        if not halt:
            eligible, rejected = _partition_capabilities(commands)
            if not eligible:
                return tuple(rejected[command.index] for command in commands)
        request = _batch_request(eligible, halt=halt)
        try:
            response = await self.executor.execute(
                request,
                context=context,
                work_class=WorkClass.BATCH,
                strict_json_members=strict_json_members,
                _admission_methods=frozenset(command.request.method for command in eligible),
            )
            envelope = _decode_batch_envelope(
                response.result,
                expected_keys=frozenset(command.stable_key for command in eligible),
                strict=strict_envelope,
            )
        except asyncio.CancelledError:
            raise
        except B24ApiError as error:
            if strict_envelope and isinstance(error, ProtocolError) and error.request_summary is None:
                scoped_error = ProtocolError(
                    str(error),
                    origin=error.origin,
                    description=error.description,
                    request_summary=request.summary,
                    evidence=error.evidence,
                    retryable=error.retryable,
                )
                scoped_error.__cause__ = error
                error = scoped_error
            failures = tuple(_shared_failure(command, error) for command in eligible)
            return _merge_outcomes(commands, failures, rejected)

        outcomes = tuple(
            self._decode_command(
                command,
                envelope,
                response,
                context=context,
                advisory_totals=advisory_totals,
            )
            for command in eligible
        )
        for command, outcome in zip(eligible, outcomes, strict=True):
            if (
                isinstance(outcome, BatchFailure)
                and isinstance(outcome.error, BatchCommandError)
                and outcome.error.normalized_code == "operation_time_limit"
            ):
                await context.coordinator.observe_api_throttle(command.request.method, outcome.error.normalized_code)
        return _merge_outcomes(commands, outcomes, rejected)

    def _decode_command(
        self,
        command: _Command,
        envelope: _BatchEnvelope,
        response: Response,
        *,
        context: ExecutionContext,
        advisory_totals: bool = False,
    ) -> BatchOutcome:
        evidence = BatchCommandEvidence(command.index, command.stable_key)
        if command.stable_key in envelope.errors:
            command_error = self._command_error(
                envelope.errors[command.stable_key],
                command,
                retry_codes=context.policy.retry.transient_api_codes,
            )
            command_evidence = BatchCommandEvidence(
                command.index,
                command.stable_key,
                original_code=command_error.original_code,
                normalized_code=command_error.normalized_code,
            )
            return _command_failure(command, command_error, evidence=command_evidence)
        if command.stable_key not in envelope.results:
            missing_error = ProtocolError(
                "Batch result map is missing a submitted command",
                origin=ErrorOrigin.PROTOCOL,
                request_summary=command.request.summary,
            )
            return _command_failure(command, missing_error, evidence=evidence)
        try:
            command_response = Response(
                envelope.results[command.stable_key],
                total=_optional_batch_integer(
                    envelope.totals,
                    command.stable_key,
                    field="total",
                    malformed_as_none=advisory_totals,
                ),
                next=_optional_batch_integer(envelope.continuations, command.stable_key, field="next"),
                evidence=response.evidence,
            )
            _raise_embedded_result_error(
                command.request,
                command_response.result,
                http_status=response.evidence.http_status or 200,
                retry_codes=context.policy.retry.transient_api_codes,
                batch=True,
            )
        except BatchCommandError as error:
            command_evidence = BatchCommandEvidence(
                command.index,
                command.stable_key,
                original_code=error.original_code,
                normalized_code=error.normalized_code,
            )
            return _command_failure(command, error, evidence=command_evidence)
        except ProtocolError as error:
            return _command_failure(command, error, evidence=evidence)
        except (TypeError, ValueError) as error:
            protocol_error = ProtocolError(
                "Batch command metadata is malformed",
                origin=ErrorOrigin.PROTOCOL,
                request_summary=command.request.summary,
                evidence=response.evidence,
            )
            protocol_error.__cause__ = error
            return _command_failure(command, protocol_error, evidence=evidence)
        return BatchSuccess._from_response(  # noqa: SLF001 - trusted correlated decoder fast path
            command.index,
            command.stable_key,
            command.request,
            command_response,
            command.correlation,
            evidence,
        )

    async def execute_requests(
        self,
        requests: tuple[Request, ...],
        *,
        context: ExecutionContext,
        advisory_totals: bool = False,
        strict_envelope: bool = False,
        strict_json_members: bool = False,
    ) -> tuple[BatchOutcome, ...]:
        """Execute one scheduler-owned chunk with total per-command correlation."""
        if not requests or len(requests) > self.portal_command_cap:
            raise ValueError("scheduler batch chunk must contain 1..portal_command_cap requests")
        commands = tuple(
            _Command(
                index=index,
                stable_key=f"c{index:012d}",
                request=request,
                correlation=None,
            )
            for index, request in enumerate(requests)
        )
        return await self._execute_chunk(
            commands,
            context=context,
            halt=False,
            advisory_totals=advisory_totals,
            strict_envelope=strict_envelope,
            strict_json_members=strict_json_members,
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


def _batch_request(commands: tuple[_Command, ...], *, halt: bool) -> Request:
    if any(
        command.request.route is not RouteKind.BARE
        or command.request.encoding.value != "json"
        or command.request.headers.items
        for command in commands
    ):
        raise CapabilityError("physical batch supports BARE JSON requests without scoped headers; use direct dispatch")
    safety_values = {command.request.replay_safety or ReplaySafety.UNKNOWN for command in commands}
    if safety_values == {ReplaySafety.SAFE}:
        safety = ReplaySafety.SAFE
    elif ReplaySafety.UNSAFE in safety_values:
        safety = ReplaySafety.UNSAFE
    else:
        safety = ReplaySafety.UNKNOWN
    encoded = {command.stable_key: _command_query(command.request) for command in commands}
    return Request("batch", parameters={"halt": int(halt), "cmd": encoded}, replay_safety=safety, route=RouteKind.BARE)


def _command_query(request: Request) -> str:
    query = _build_query(cast("Mapping[str | int, object]", request.to_wire_parameters()))
    return request.method if not query else f"{request.method}?{query}"


def _build_query(parameters: Mapping[str | int, object], path: str = "%s") -> str:
    """Encode nested JSON values with Bitrix/PHP bracket semantics."""
    return encode_php_query(parameters, path)


def _decode_batch_envelope(
    raw: JsonValue,
    *,
    expected_keys: frozenset[str],
    strict: bool = False,
) -> _BatchEnvelope:
    if not isinstance(raw, dict):
        raise ProtocolError("Batch result envelope must be an object", origin=ErrorOrigin.PROTOCOL)
    if "result_error" not in raw:
        raise ProtocolError("Batch result envelope is missing result_error", origin=ErrorOrigin.PROTOCOL)
    results = _decode_php_map(raw.get("result"), field="result")
    errors = _decode_php_map(raw["result_error"], field="result_error")
    totals = _decode_optional_php_map(raw, field="result_total")
    continuations = _decode_optional_php_map(raw, field="result_next")
    result_keys = frozenset(results)
    error_keys = frozenset(errors)
    if strict and (result_keys & error_keys or not (result_keys | error_keys).issubset(expected_keys)):
        raise ProtocolError(
            "Batch result correlation keys are duplicated or unknown",
            origin=ErrorOrigin.PROTOCOL,
        )
    if strict and (
        not frozenset(totals).issubset(expected_keys) or not frozenset(continuations).issubset(expected_keys)
    ):
        raise ProtocolError("Batch metadata contains an unknown correlation key", origin=ErrorOrigin.PROTOCOL)
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


def _optional_batch_integer(
    values: Mapping[str, object],
    key: str,
    *,
    field: str,
    malformed_as_none: bool = False,
) -> int | None:
    if key not in values:
        return None
    value = values[key]
    if not isinstance(value, int) or isinstance(value, bool):
        if malformed_as_none:
            return None
        raise TypeError(f"batch {field} must be an integer")
    if field == "total" and value < 0:
        if malformed_as_none:
            return None
        if value < -1:
            raise ValueError("batch total must be -1 or non-negative")
    if field == "next" and value == -1:
        return None
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


def _merge_outcomes(
    commands: tuple[_Command, ...],
    executed: tuple[BatchOutcome, ...],
    rejected: Mapping[int, BatchOutcome],
) -> tuple[BatchOutcome, ...]:
    """Restore caller order after tolerant local capability rejection."""
    by_index = {outcome.command_index: outcome for outcome in executed}
    by_index.update(rejected)
    return tuple(by_index[command.index] for command in commands)


def _partition_capabilities(
    commands: tuple[_Command, ...],
) -> tuple[tuple[_Command, ...], dict[int, BatchOutcome]]:
    """Separate tolerant commands that physical batch cannot represent."""
    eligible: list[_Command] = []
    rejected: dict[int, BatchOutcome] = {}
    for command in commands:
        if (
            command.request.route is RouteKind.BARE
            and command.request.encoding.value == "json"
            and not command.request.headers.items
        ):
            eligible.append(command)
            continue
        error = CapabilityError(
            "physical batch supports BARE JSON requests without scoped headers; use direct dispatch",
            request_summary=command.request.summary,
        )
        rejected[command.index] = _command_failure(
            command,
            error,
            evidence=BatchCommandEvidence(command.index, command.stable_key),
        )
    return tuple(eligible), rejected


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
        correlation=command.correlation,
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
