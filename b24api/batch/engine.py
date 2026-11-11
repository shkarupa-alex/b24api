"""Bounded fail-fast and total-outcome Bitrix batch execution."""

from __future__ import annotations
import asyncio
from collections.abc import AsyncIterable, Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from b24api._error_types import ErrorOrigin
from b24api.batch.correlation import (
    _BatchNotExecutedError,
    _Command,
    _command_failure,
    _command_success,
    _shared_failure,
)
from b24api.batch.outcome import (
    BatchCommandEvidence,
    BatchFailure,
    BatchOutcome,
    BatchSuccess,
)
from b24api.batch.v3 import _V3Resolution
from b24api.contracts.dispatch import PORTAL_BATCH_CAP
from b24api.contracts.policy import (
    AmbiguityReason,
    ReplayDisposition,
)
from b24api.contracts.request import ReplaySafety, Request, diagnostic_context
from b24api.contracts.request_summary import RouteKind
from b24api.contracts.response import Response
from b24api.encoding import encode_php_query
from b24api.errors import (
    AmbiguousExecutionError,
    ApiResponseError,
    B24ApiError,
    BatchCommandError,
    BudgetExceededError,
    CapabilityError,
    HTTPGatewayError,
    ProtocolError,
    ResponseTooLargeError,
    TransportError,
)
from b24api.execution import (
    ExecutionContext,
    Executor,
    WorkClass,
)
from b24api.execution.executor import (
    _is_retryable,
    _raise_embedded_result_error,
    _refused_before_running,
    _RequestAttempts,
)

if TYPE_CHECKING:
    from b24api.contracts.json import JsonValue

_MISSING = object()
_UNSUPPORTED_COMMAND = "physical batch supports BARE JSON requests without scoped headers; use direct dispatch"
_UNSUPPORTED_EMPTY_CONTAINER = (
    "classic batch cannot carry an empty array or object, which PHP bracket encoding drops; use direct dispatch"
)
_UNSUPPORTED_V3_TRANSPORT = "transport does not support native REST v3 batch envelopes; use direct dispatch"


@dataclass(frozen=True, slots=True)
class _BatchInput:
    request: Request
    correlation: object = None


type _BatchItem = Request | _BatchInput
type BatchSource = Iterable[_BatchItem] | AsyncIterable[_BatchItem]
type BatchStreamItem = BatchOutcome


@dataclass(frozen=True, slots=True)
class _Chunk:
    commands: tuple[_Command, ...]
    source_error: Exception | None = None


@dataclass(frozen=True, slots=True)
class _RoundOptions:
    halt: bool
    advisory_totals: bool
    strict_envelope: bool
    strict_json_members: bool


@dataclass(frozen=True, slots=True)
class _Round:
    outcomes: tuple[BatchOutcome, ...]
    replay: tuple[_Command, ...] = ()
    method_cooldown: float = 0.0


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
            raise ValueError(f"portal command cap must be between 1 and {PORTAL_BATCH_CAP}")
        self.executor = executor
        self.portal_command_cap = portal_command_cap

    async def execute_chunk(  # noqa: PLR0913
        self,
        commands: tuple[_Command, ...],
        *,
        context: ExecutionContext,
        halt: bool,
        advisory_totals: bool = False,
        strict_envelope: bool = False,
        strict_json_members: bool = False,
        on_envelope: Callable[[], None] | None = None,
    ) -> tuple[BatchOutcome, ...]:
        """Execute one admitted chunk: classic BARE and native v3 commands never share an envelope.

        ``on_envelope`` is called once as each admitted envelope group starts its first round, the 2.5.2 rule for
        an admitted chunk: the group counts even when the budget then stops its first send. Halted groups, replay
        rounds, envelope retries and isolation requests are not counted.
        """
        eligible, rejected = self._partition_capabilities(commands)
        if halt and rejected:
            first_rejected = next(command.index for command in commands if command.index in rejected)
            return tuple(
                rejected[command.index]
                if command.index == first_rejected
                else _command_failure(
                    command,
                    _BatchNotExecutedError(
                        "physical batch command was not executed after fail-fast capability rejection",
                        request_summary=command.request.summary,
                    ),
                    evidence=BatchCommandEvidence(command.index, command.stable_key),
                )
                for command in commands
            )
        if not eligible:
            return tuple(rejected[command.index] for command in commands)
        options = _RoundOptions(halt, advisory_totals, strict_envelope, strict_json_members)
        settled: dict[int, BatchOutcome] = {}
        for group in _envelope_groups(eligible):
            if halt and any(not isinstance(outcome, BatchSuccess) for outcome in settled.values()):
                # Fail-fast: a later envelope of the accepted chunk is not sent after a failed one.
                settled.update((command.index, _halted_failure(command)) for command in group)
                continue
            if on_envelope is not None:
                on_envelope()
            if group[0].request.route is RouteKind.API_V3:
                outcomes = await _V3Resolution(self.executor, context).run(group)
            else:
                outcomes = await self._execute_bare(group, context=context, options=options)
            settled.update((outcome.command_index, outcome) for outcome in outcomes)
        return _merge_outcomes(commands, tuple(settled.values()), rejected)

    async def _execute_bare(
        self,
        eligible: tuple[_Command, ...],
        *,
        context: ExecutionContext,
        options: _RoundOptions,
    ) -> tuple[BatchOutcome, ...]:
        """Run one classic batch envelope and its replay rounds."""
        settled: dict[int, BatchOutcome] = {}
        # Every command of a replay round was sent in each round before it, so one count bounds them all.
        pending, rounds, attempts = eligible, 0, _RequestAttempts()
        while pending:
            sent = await self._dispatch_round(pending, context=context, options=options, attempts=attempts)
            # A replay the budget stops before it runs leaves each command the outcome it last received.
            settled.update(
                (outcome.command_index, outcome)
                for outcome in sent.outcomes
                if not (rounds and isinstance(outcome, BatchFailure) and isinstance(outcome.error, BudgetExceededError))
            )
            rounds += 1
            # Only the commands that may run again are sent again, in a smaller physical batch; the rest keep
            # the outcome this round gave them, which also stands for a replay the budget does not allow.
            if not sent.replay or not await self.executor.pause_before_replay(
                context, attempts=attempts, method_cooldown=sent.method_cooldown
            ):
                break
            pending = sent.replay
        return tuple(settled.values())

    async def _dispatch_round(
        self,
        commands: tuple[_Command, ...],
        *,
        context: ExecutionContext,
        options: _RoundOptions,
        attempts: _RequestAttempts,
    ) -> _Round:
        """Send one physical batch; return each command's outcome and the commands that may be sent again.

        SAFE commands are sent again after any transient failure; the others only when their failure proves
        they did not run (owner decision, 2026-09-25). A fail-fast batch is never split.
        """
        request = _batch_request(commands, halt=options.halt)
        attempts.new_round()
        try:
            response = await self.executor.execute(
                request,
                context=context,
                work_class=WorkClass.BATCH,
                strict_json_members=options.strict_json_members,
                _admission_methods=frozenset(command.request.method for command in commands),
                _attempts=attempts,
            )
            envelope = _decode_batch_envelope(
                response.result,
                expected_keys=frozenset(command.stable_key for command in commands),
                strict=options.strict_envelope,
            )
        except asyncio.CancelledError:
            raise
        except B24ApiError as error:
            if options.strict_envelope and isinstance(error, ProtocolError) and error.request_summary is None:
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
            # The SAFE commands are sent again after any failure a SAFE request would retry, whether or not the
            # batch may have run and whatever its other commands are.
            replay = () if options.halt or not _safe_replay_allowed(error, context) else commands
            safe = tuple(command for command in replay if command.request.replay_safety is ReplaySafety.SAFE)
            if not _possibly_executed(error, attempts):
                return _Round(tuple(_shared_failure(command, error) for command in commands), safe)
            return _Round(
                tuple(_unknown_failure(command, error, attempts.unproven_failure) for command in commands), safe
            )

        outcomes = tuple(
            self._decode_command(
                command,
                envelope,
                response,
                context=context,
                advisory_totals=options.advisory_totals,
            )
            for command in commands
        )
        method_cooldown = 0.0
        for command, outcome in zip(commands, outcomes, strict=True):
            if (
                isinstance(outcome, BatchFailure)
                and isinstance(outcome.error, BatchCommandError)
                and outcome.error.normalized_code == "operation_time_limit"
            ):
                cooldown = await context.coordinator.observe_api_throttle(
                    command.request.method, outcome.error.normalized_code
                )
                method_cooldown = max(method_cooldown, cooldown)
        # A SAFE command is sent again after its own transient error; any other only after a listed refusal.
        replay = (
            ()
            if options.halt
            else tuple(
                command
                for command, outcome in zip(commands, outcomes, strict=True)
                if isinstance(outcome, BatchFailure)
                and isinstance(outcome.error, BatchCommandError)
                and outcome.replay_disposition is ReplayDisposition.ELIGIBLE
            )
        )
        return _Round(outcomes, replay, method_cooldown)

    def _batch_route(self, request: Request) -> RouteKind | None:
        """Return the envelope dialect a request can travel in, or ``None`` when no envelope represents it."""
        if _classic_representable(request) and not request.has_empty_container:
            return RouteKind.BARE
        if _v3_representable(request) and self.executor.supports_v3_batch:
            return RouteKind.API_V3
        return None

    def _partition_capabilities(
        self,
        commands: tuple[_Command, ...],
    ) -> tuple[tuple[_Command, ...], dict[int, BatchOutcome]]:
        """Separate tolerant commands that physical batch cannot represent."""
        eligible: list[_Command] = []
        rejected: dict[int, BatchOutcome] = {}
        for command in commands:
            if self._batch_route(command.request) is not None:
                eligible.append(command)
                continue
            # A command is refused before any reservation when no envelope the transport sends represents it.
            message = (
                _UNSUPPORTED_V3_TRANSPORT
                if _v3_representable(command.request)
                else _UNSUPPORTED_EMPTY_CONTAINER
                if _classic_representable(command.request)
                else _UNSUPPORTED_COMMAND
            )
            error = CapabilityError(message, request_summary=command.request.summary)
            rejected[command.index] = _command_failure(
                command,
                error,
                evidence=BatchCommandEvidence(command.index, command.stable_key),
            )
        return tuple(eligible), rejected

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
            return _command_failure(
                command,
                command_error,
                evidence=command_evidence,
                refused=_refused_before_running(command_error, context.policy),
            )
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
                redactor=self.executor.codec.redactor,
            )
        except BatchCommandError as error:
            command_evidence = BatchCommandEvidence(
                command.index,
                command.stable_key,
                original_code=error.original_code,
                normalized_code=error.normalized_code,
            )
            return _command_failure(
                command, error, evidence=command_evidence, refused=_refused_before_running(error, context.policy)
            )
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
        return _command_success(command, command_response, evidence)

    async def execute_requests(  # noqa: PLR0913 - mirrors execute_chunk's round options
        self,
        requests: tuple[Request, ...],
        *,
        context: ExecutionContext,
        advisory_totals: bool = False,
        strict_envelope: bool = False,
        strict_json_members: bool = False,
        on_envelope: Callable[[], None] | None = None,
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
        return await self.execute_chunk(
            commands,
            context=context,
            halt=False,
            advisory_totals=advisory_totals,
            strict_envelope=strict_envelope,
            strict_json_members=strict_json_members,
            on_envelope=on_envelope,
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
        # Each command renders through its own request's context: the physical batch keeps one map per key.
        return BatchCommandError(
            code=code,
            description=None if description is None else str(description),
            request_summary=command.request.summary,
            retryable=normalized in retry_codes,
            redactor=self.executor.codec.redactor,
            diagnostics=diagnostic_context(command.request),
        )


def _batch_request(commands: tuple[_Command, ...], *, halt: bool) -> Request:
    if any(
        command.request.route is not RouteKind.BARE
        or command.request.encoding.value != "json"
        or command.request.positional is not None
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


def _classic_representable(request: Request) -> bool:
    """Return whether a request has a classic batch command form, whatever values its parameters hold."""
    return (
        request.route is RouteKind.BARE
        and not request.method.endswith(".json")
        and request.encoding.value == "json"
        and request.positional is None
        and not request.headers.items
    )


def _v3_representable(request: Request) -> bool:
    """Return whether a request has a native v3 batch command form, whatever the transport supports."""
    return (
        request.route is RouteKind.API_V3
        and request.encoding.value == "json"
        and request.positional is None
        and not request.headers.items
    )


def _envelope_groups(eligible: tuple[_Command, ...]) -> tuple[tuple[_Command, ...], ...]:
    """Split admitted commands into contiguous envelopes that keep their input order.

    A run of classic commands shares one envelope as before; a native v3 run carries one safety class:
    a SAFE run never carries an UNSAFE or UNKNOWN command, and UNSAFE and UNKNOWN travel together. A
    change of dialect or v3 safety class closes the run, so no command is sent before an earlier one.
    """
    groups: list[tuple[_Command, ...]] = []
    run: list[_Command] = []
    for command in eligible:
        if run and _envelope_kind(run[-1]) != _envelope_kind(command):
            groups.append(tuple(run))
            run = []
        run.append(command)
    if run:
        groups.append(tuple(run))
    return tuple(groups)


def _envelope_kind(command: _Command) -> tuple[RouteKind, bool]:
    """Return what an envelope must share: the dialect and, for native v3, the safety class."""
    route = command.request.route
    return route, route is RouteKind.API_V3 and _is_safe(command)


def _is_safe(command: _Command) -> bool:
    return command.request.replay_safety is ReplaySafety.SAFE


def _halted_failure(command: _Command) -> BatchFailure:
    return _shared_failure(
        command,
        _BatchNotExecutedError(
            "physical batch command was not executed after a fail-fast envelope failure",
            request_summary=command.request.summary,
        ),
    )


def _possibly_executed(error: B24ApiError, attempts: _RequestAttempts) -> bool:
    """Return whether a failed physical batch may have executed its admitted commands."""
    # A send of this round that no failure proved refused may have run, whatever ended the round.
    if attempts.earlier_unproven:
        return True
    if isinstance(error, BudgetExceededError):
        return attempts.last_unproven
    if isinstance(error, TransportError):
        return error.possible_acceptance
    return isinstance(error, AmbiguousExecutionError | ResponseTooLargeError)


def _safe_replay_allowed(error: B24ApiError, context: ExecutionContext) -> bool:
    """Return whether the failure that made a batch's outcome unknown would let a SAFE request retry."""
    cause = error.__cause__ if isinstance(error, AmbiguousExecutionError) else error
    return isinstance(cause, B24ApiError) and _is_retryable(cause, safety=ReplaySafety.SAFE, policy=context.policy)


def _unknown_failure(command: _Command, error: B24ApiError, unproven: B24ApiError | None = None) -> BatchFailure:
    """Give one admitted command its own possible-execution outcome, kept unless a SAFE replay replaces it.

    The reason and cause name the latest failure of a send that may have run; without one (an answer that came
    after the budget, for example), the failure that ended the round.
    """
    source = error
    if isinstance(error, BudgetExceededError) and isinstance(error.__cause__, B24ApiError):
        source = error.__cause__
    source = unproven or source
    if isinstance(source, AmbiguousExecutionError):
        reason = source.reason
    elif isinstance(source, ResponseTooLargeError):
        reason = AmbiguityReason.RESPONSE_LIMIT_AFTER_DISPATCH
    elif isinstance(source, BudgetExceededError) or isinstance(source.__cause__, TimeoutError):
        reason = AmbiguityReason.DEADLINE_AFTER_DISPATCH
    elif isinstance(source, HTTPGatewayError | ApiResponseError):
        reason = AmbiguityReason.HTTP_STATUS_AFTER_DISPATCH
    else:
        reason = AmbiguityReason.CONNECTION_LOST_AFTER_DISPATCH
    ambiguous = AmbiguousExecutionError(
        "Physical batch may have executed this command; automatic replay is forbidden",
        reason=reason,
        declared_unsafe=command.request.replay_safety is ReplaySafety.UNSAFE,
        request_summary=command.request.summary,
        evidence=source.evidence,
    )
    # Keep the original failure as the cause instead of the executor's batch-scoped ambiguity wrapper.
    original = error.__cause__ if isinstance(error, AmbiguousExecutionError) else None
    terminal = original if isinstance(original, BaseException) else error
    # The chain must reach the send that may have run, even when a later failure ended the round.
    ambiguous.__cause__ = terminal if unproven is None or _caused_by(terminal, unproven) else unproven
    return _shared_failure(command, ambiguous)


def _caused_by(error: BaseException, cause: BaseException) -> bool:
    current: BaseException | None = error
    while current is not None:
        if current is cause:
            return True
        current = current.__cause__
    return False


def _raise_source_error(error: Exception) -> None:
    raise error


def _batch_outcome_row_weight(outcome: BatchOutcome) -> int:
    return outcome.decoded_rows if isinstance(outcome, BatchSuccess) else 1


__all__: list[str] = []
