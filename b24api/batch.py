"""Bounded fail-fast and total-outcome Bitrix batch execution."""

from __future__ import annotations
import asyncio
import contextlib
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol, Self, cast, runtime_checkable

from b24api.error import B24ApiError, BatchCommandError, ErrorOrigin, ProtocolError
from b24api.execution import (
    AsyncIteratorController,
    ExecutionContext,
    Executor,
    WorkClass,
    await_cancellation_resistant,
)
from b24api.models import (
    BatchCommandEvidence,
    BatchFailure,
    BatchOutcome,
    BatchSuccess,
    CompletionAssurance,
    ExecutionPolicy,
    JsonValue,
    OperationReport,
    ReplayDisposition,
    ReplaySafety,
    Request,
    Response,
    SnapshotRequirement,
    SnapshotState,
    TerminalState,
    Violation,
    ViolationSeverity,
)
from b24api.plans import PORTAL_BATCH_CAP
from b24api.query import build_query

type RequestMapping = Mapping[str, object]
type RequestWithPayload = tuple[Request | RequestMapping, object]
type BatchInput = Request | RequestMapping | RequestWithPayload
type BatchSource = Iterable[BatchInput] | AsyncIterable[BatchInput]
type FailFastItem = JsonValue | tuple[JsonValue, object]
type BatchStreamItem = FailFastItem | BatchOutcome

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


@runtime_checkable
class _AsyncClosable(Protocol):
    async def aclose(self) -> None: ...


@runtime_checkable
class _SyncClosable(Protocol):
    def close(self) -> None: ...


class BatchExecutor:
    """Create lazy bounded streams over one shared replay-aware executor."""

    def __init__(
        self,
        executor: Executor,
        *,
        portal_command_cap: int = PORTAL_BATCH_CAP,
        fallback_eligible_codes: frozenset[str] = frozenset(),
    ) -> None:
        if isinstance(portal_command_cap, bool) or not 1 <= portal_command_cap <= PORTAL_BATCH_CAP:
            raise ValueError("portal command cap must be between 1 and 50")
        self.executor = executor
        self.portal_command_cap = portal_command_cap
        self.fallback_eligible_codes = frozenset(code.strip().casefold() for code in fallback_eligible_codes)

    def batch(
        self,
        requests: BatchSource,
        *,
        batch_size: int | None = None,
        with_payload: bool = False,
        policy: ExecutionPolicy | None = None,
    ) -> BatchStream:
        """Return a lazy fail-fast stream using Bitrix `halt=true`."""
        return BatchStream(
            self,
            requests,
            batch_size=self._batch_size(batch_size),
            tolerant=False,
            with_payload=with_payload,
            fallback_failed="none",
            policy=policy or ExecutionPolicy(),
        )

    def batch_outcomes(
        self,
        requests: BatchSource,
        *,
        batch_size: int | None = None,
        policy: ExecutionPolicy | None = None,
        fallback_failed: Literal["none", "direct"] = "none",
    ) -> BatchStream:
        """Return a lazy stream with exactly one typed outcome per command."""
        if fallback_failed not in {"none", "direct"}:
            raise ValueError("fallback_failed must be none or direct")
        return BatchStream(
            self,
            requests,
            batch_size=self._batch_size(batch_size),
            tolerant=True,
            with_payload=False,
            fallback_failed=fallback_failed,
            policy=policy or ExecutionPolicy(),
        )

    def _batch_size(self, requested: int | None) -> int:
        size = self.portal_command_cap if requested is None else requested
        if isinstance(size, bool) or not 1 <= size <= self.portal_command_cap:
            raise ValueError("batch_size must be within the portal command cap")
        return size

    async def _execute_chunk(  # noqa: C901
        self,
        commands: tuple[_Command, ...],
        *,
        tolerant: bool,
        fallback_failed: Literal["none", "direct"],
        context: ExecutionContext,
    ) -> tuple[BatchOutcome, ...]:
        request = _batch_request(commands, halt=not tolerant)
        try:
            response = await self.executor.execute(request, context=context, work_class=WorkClass.BATCH)
            envelope = _decode_batch_envelope(response.result)
        except asyncio.CancelledError:
            raise
        except B24ApiError as error:
            if not tolerant:
                raise
            failures = tuple(_shared_failure(command, error) for command in commands)
            return await self._fallback(failures, fallback_failed=fallback_failed, context=context)

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
                if not tolerant:
                    raise command_error
                outcomes.append(_command_failure(command, command_error, evidence=evidence))
                continue
            if command.stable_key not in envelope.results:
                missing_error = ProtocolError(
                    "Batch result map is missing a submitted command",
                    origin=ErrorOrigin.PROTOCOL,
                    request_summary=command.request.summary,
                )
                if not tolerant:
                    raise missing_error
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
                if not tolerant:
                    raise protocol_error from error
                outcomes.append(_command_failure(command, protocol_error, evidence=evidence))
                continue
            outcomes.append(
                BatchSuccess(
                    command.index,
                    command.stable_key,
                    command.request,
                    envelope.results[command.stable_key],
                    command.payload,
                    evidence,
                    response=command_response,
                ),
            )
        return await self._fallback(tuple(outcomes), fallback_failed=fallback_failed, context=context)

    async def execute_requests(
        self,
        requests: tuple[Request, ...],
        *,
        context: ExecutionContext,
        fallback_failed: Literal["none", "direct"] = "none",
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
            tolerant=True,
            fallback_failed=fallback_failed,
            context=context,
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
            retryable=normalized in self.fallback_eligible_codes or normalized in retry_codes,
        )

    async def _fallback(
        self,
        outcomes: tuple[BatchOutcome, ...],
        *,
        fallback_failed: Literal["none", "direct"],
        context: ExecutionContext,
    ) -> tuple[BatchOutcome, ...]:
        if fallback_failed == "none":
            return outcomes
        resolved: list[BatchOutcome] = []
        for outcome in outcomes:
            if not isinstance(outcome, BatchFailure) or not _fallback_eligible(outcome):
                resolved.append(outcome)
                continue
            try:
                response = await self.executor.execute(
                    outcome.request,
                    context=context,
                    work_class=WorkClass.TRAVERSAL_DIRECT,
                )
            except asyncio.CancelledError:
                raise
            except B24ApiError as error:
                resolved.append(
                    BatchFailure(
                        outcome.command_index,
                        outcome.stable_key,
                        outcome.request,
                        error,
                        replay_safety=outcome.replay_safety,
                        replay_disposition=ReplayDisposition.DIRECT_REPLAY_FAILED,
                        payload=outcome.payload,
                        evidence=outcome.evidence,
                    ),
                )
            else:
                resolved.append(
                    BatchSuccess(
                        outcome.command_index,
                        outcome.stable_key,
                        outcome.request,
                        response.result,
                        outcome.payload,
                        outcome.evidence,
                        replay_disposition=ReplayDisposition.REPLAYED_DIRECT,
                        response=response,
                    ),
                )
        return tuple(resolved)


class BatchStream(AsyncIterator[BatchStreamItem]):
    """Lazy async/context-managed batch stream with a frozen terminal report."""

    def __init__(  # noqa: PLR0913
        self,
        batch_executor: BatchExecutor,
        source: BatchSource,
        *,
        batch_size: int,
        tolerant: bool,
        with_payload: bool,
        fallback_failed: Literal["none", "direct"],
        policy: ExecutionPolicy,
    ) -> None:
        self._executor = batch_executor
        self._source = source
        self._batch_size = batch_size
        self._tolerant = tolerant
        self._with_payload = with_payload
        self._fallback_failed = fallback_failed
        self._context = batch_executor.executor.context(policy)
        self._runner: AsyncGenerator[BatchStreamItem] | None = None
        self._source_controller: AsyncIteratorController[BatchInput] | None = None
        self._prefetched: BatchStreamItem | object = _MISSING
        self._closed = False
        self._batch_requests = 0
        self._batch_commands = 0
        self._emitted = 0
        self.report = OperationReport()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> BatchStreamItem:
        if self._closed:
            raise StopAsyncIteration
        if self._prefetched is not _MISSING:
            item = cast("BatchStreamItem", self._prefetched)
            self._prefetched = _MISSING
            self._emitted += 1
            return item
        if self._runner is None:
            self._runner = self._run()
        item = await anext(self._runner)
        self._emitted += 1
        return item

    async def __aenter__(self) -> Self:
        if self._closed:
            raise RuntimeError("stream is closed")
        if self._runner is None:
            self._runner = self._run()
            with contextlib.suppress(StopAsyncIteration):
                self._prefetched = await anext(self._runner)
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._closed:
            await self._observe_source_cleanup()
            return
        self._closed = True
        try:
            if self._runner is not None:
                await self._runner.aclose()
        except BaseException as error:
            if self.report.state is TerminalState.NOT_STARTED:
                cancellation = await await_cancellation_resistant(
                    self._finalize(TerminalState.CANCELLED, "stream cleanup failed"),
                )
                if cancellation is not None:
                    _attach_report(cancellation, self.report)
                    raise cancellation from error
            _attach_report(error, self.report)
            raise
        finally:
            self._prefetched = _MISSING
        await self._observe_source_cleanup()
        if self.report.state is TerminalState.NOT_STARTED and self._runner is not None:
            await self._finalize(TerminalState.CANCELLED, "stream closed before exhaustion")

    async def _run(self) -> AsyncGenerator[BatchStreamItem]:  # noqa: C901, PLR0912, PLR0915
        await self._context.start()
        source = AsyncIteratorController(
            _iterate_source(self._source),
            input_error="batch input exceeded operation time budget",
            cleanup_error="batch source cleanup exceeded operation time budget",
        )
        self._source_controller = source
        next_index = 0
        naturally_exhausted = False
        try:
            while True:
                chunk = await _next_chunk(
                    source,
                    self._batch_size,
                    start_index=next_index,
                    context=self._context,
                )
                if not chunk.commands:
                    break
                next_index += len(chunk.commands)
                if chunk.source_error is not None:
                    if self._tolerant:
                        for command in chunk.commands:
                            yield _source_failure(command, chunk.source_error)
                    _raise_source_error(chunk.source_error)
                self._batch_requests += 1
                self._batch_commands += len(chunk.commands)
                outcomes = await self._executor._execute_chunk(  # noqa: SLF001
                    chunk.commands,
                    tolerant=self._tolerant,
                    fallback_failed=self._fallback_failed,
                    context=self._context,
                )
                for outcome in outcomes:
                    if self._tolerant:
                        yield outcome
                    else:
                        success = cast("BatchSuccess", outcome)
                        if self._with_payload:
                            yield success.result, success.payload
                        else:
                            yield success.result
            naturally_exhausted = True
            await self._finalize(TerminalState.COMPLETED, "input exhausted")
        except asyncio.CancelledError as error:
            repeated = await await_cancellation_resistant(
                self._finalize(TerminalState.CANCELLED, "iteration cancelled"),
            )
            if repeated is not None:
                _attach_report(repeated, self.report)
                raise repeated from error
            _attach_report(error, self.report)
            raise
        except GeneratorExit as error:
            cancellation = await await_cancellation_resistant(
                self._finalize(TerminalState.CANCELLED, "stream closed before exhaustion"),
            )
            if cancellation is not None:
                _attach_report(cancellation, self.report)
                raise cancellation from error
            _attach_report(error, self.report)
            raise
        except BaseException as error:
            cancellation = await await_cancellation_resistant(
                self._finalize(TerminalState.FAILED, type(error).__name__),
            )
            if cancellation is not None:
                _attach_report(cancellation, self.report)
                raise cancellation from error
            _attach_report(error, self.report)
            raise
        finally:
            cleanup_cancellation = await await_cancellation_resistant(self._cleanup_source(source))
            if cleanup_cancellation is not None:
                _attach_report(cleanup_cancellation, self.report)
                raise cleanup_cancellation
            if not naturally_exhausted and self.report.state is TerminalState.NOT_STARTED:
                await self._finalize(TerminalState.CANCELLED, "stream abandoned")
            if self.report.state is not TerminalState.NOT_STARTED:
                self._closed = True

    async def _cleanup_source(self, source: AsyncIteratorController[BatchInput]) -> None:
        try:
            await source.aclose(
                remaining=max(0.0, self._context.policy.max_elapsed - self._context.elapsed),
            )
        except BaseException as cleanup_error:
            if self.report.state is TerminalState.NOT_STARTED:
                cancellation = await await_cancellation_resistant(
                    self._finalize(TerminalState.CANCELLED, "stream cleanup failed"),
                )
                if cancellation is not None:
                    _attach_report(cancellation, self.report)
                    raise cancellation from cleanup_error
            _attach_report(cleanup_error, self.report)
            raise

    async def _observe_source_cleanup(self) -> None:
        controller = self._source_controller
        if controller is None:
            return
        await self._cleanup_source(controller)

    async def _finalize(self, state: TerminalState, reason: str) -> None:
        if self.report.state is not TerminalState.NOT_STARTED:
            return
        snapshot = await self._context.snapshot()
        consistency = self._context.policy.consistency
        snapshot_state = (
            SnapshotState.NOT_REQUESTED
            if consistency.snapshot_requirement is SnapshotRequirement.TRAVERSAL_ONLY
            else SnapshotState.UNVERIFIED
        )
        violations: tuple[Violation, ...] = ()
        if state is TerminalState.COMPLETED and snapshot_state is SnapshotState.UNVERIFIED:
            state = TerminalState.INCOMPLETE
            reason = "required snapshot was not verified"
            violations = (
                Violation(
                    severity=ViolationSeverity.BLOCKING,
                    code="snapshot_unverified",
                    message="the requested stable snapshot was not verified",
                ),
            )
        self.report = OperationReport(
            state=state,
            assurance=CompletionAssurance.CALLER_ASSERTED,
            snapshot=snapshot_state,
            plan_id="batch_outcomes" if self._tolerant else "batch",
            dispatch_id="batch",
            emitted_rows=self._emitted,
            unique_rows=self._emitted,
            physical_requests=snapshot.counters.physical_requests,
            batch_requests=self._batch_requests,
            batch_commands=self._batch_commands,
            retries=snapshot.retries,
            cooldown_seconds=snapshot.cooldown_seconds,
            violations=violations,
            terminal_reason=reason,
        )


async def _iterate_source(source: BatchSource) -> AsyncGenerator[BatchInput]:
    if isinstance(source, AsyncIterable):
        iterator = aiter(source)
        try:
            async for item in iterator:
                yield item
        finally:
            if isinstance(iterator, _AsyncClosable):
                await iterator.aclose()
        return
    if source.__class__ is list or source.__class__ is tuple:
        for item in source:
            yield item
        return
    sync_iterator = iter(source)
    try:
        while True:
            sync_item = await _next_sync_owned(sync_iterator)
            if sync_item is _SYNC_EXHAUSTED:
                return
            yield cast("BatchInput", sync_item)
    finally:
        if isinstance(sync_iterator, _SyncClosable):
            await _close_sync_owned(sync_iterator)


def _next_sync(iterator: Iterator[BatchInput]) -> BatchInput | object:
    try:
        return next(iterator)
    except StopIteration:
        return _SYNC_EXHAUSTED


async def _next_sync_owned(iterator: Iterator[BatchInput]) -> BatchInput | object:
    pull = asyncio.create_task(asyncio.to_thread(_next_sync, iterator))
    try:
        return await asyncio.shield(pull)
    except asyncio.CancelledError:
        with contextlib.suppress(BaseException):
            await pull
        raise


async def _close_sync_owned(iterator: _SyncClosable) -> None:
    close = asyncio.create_task(asyncio.to_thread(iterator.close))
    try:
        await asyncio.shield(close)
    except asyncio.CancelledError:
        with contextlib.suppress(BaseException):
            await close
        raise


def _attach_report(error: BaseException, report: OperationReport) -> None:
    with contextlib.suppress(AttributeError, TypeError):
        error.report = report  # type: ignore[attr-defined]


async def _next_chunk(
    source: AsyncIteratorController[BatchInput],
    size: int,
    *,
    start_index: int,
    context: ExecutionContext,
) -> _Chunk:
    commands: list[_Command] = []
    for offset in range(size):
        try:
            raw = await source.get(context)
            request, payload, has_payload = _coerce_input(raw)
        except StopAsyncIteration:
            break
        except Exception as error:
            if not commands:
                raise
            return _Chunk(tuple(commands), error)
        index = start_index + offset
        commands.append(_Command(index, f"c{index:012d}", request, payload, has_payload))
    return _Chunk(tuple(commands))


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
    safety = raw.get("replay_safety")
    if not isinstance(method, str):
        raise TypeError("mapping request requires a string method")
    if parameters is not None and not isinstance(parameters, Mapping):
        raise TypeError("mapping request parameters must be a mapping")
    if isinstance(safety, str):
        with contextlib.suppress(ValueError):
            safety = ReplaySafety(safety)
    if safety is not None and not isinstance(safety, ReplaySafety):
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


def _source_failure(command: _Command, error: Exception) -> BatchFailure:
    return BatchFailure(
        command.index,
        command.stable_key,
        command.request,
        error,
        replay_safety=command.request.replay_safety or ReplaySafety.UNKNOWN,
        replay_disposition=ReplayDisposition.NOT_ELIGIBLE,
        payload=command.payload,
        evidence=BatchCommandEvidence(command.index, command.stable_key),
    )


def _raise_source_error(error: Exception) -> None:
    raise error


def _fallback_eligible(outcome: BatchFailure) -> bool:
    return (
        outcome.replay_safety is ReplaySafety.SAFE
        and outcome.replay_disposition is ReplayDisposition.ELIGIBLE
        and isinstance(outcome.error, B24ApiError)
        and outcome.error.retryable
    )


__all__ = [
    "BatchExecutor",
    "BatchInput",
    "BatchSource",
    "BatchStream",
    "BatchStreamItem",
    "FailFastItem",
]
