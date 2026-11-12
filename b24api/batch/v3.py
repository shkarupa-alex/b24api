"""Native REST v3 batch: envelope encoding, attribution, bounded recovery and SAFE isolation.

One run resolves one contiguous safety group of a chunk. Its commands move ``pending -> dispatched ->
unresolved -> resolving -> settled | settled_unknown``; each is admitted and settled exactly once, and
recovery never creates a logical page. The group's ``_RequestAttempts`` is the single owner of the attempt
lineage: every envelope send counts against every command it carries, and embedded retries and isolated
requests inherit that lineage instead of receiving a fresh budget.
"""

from __future__ import annotations
import asyncio
import dataclasses
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from b24api.batch.correlation import (
    _Command,
    _command_failure,
    _command_success,
    _shared_failure,
)
from b24api.batch.outcome import BatchCommandEvidence, BatchFailure
from b24api.contracts.json import FrozenMapping, _freeze_json
from b24api.contracts.policy import ReplayDisposition
from b24api.contracts.request import ReplaySafety
from b24api.contracts.response import Response
from b24api.contracts.wire_body import V3BatchBody, V3BatchCommand, envelope_safety
from b24api.errors import (
    ApiResponseError,
    B24ApiError,
    BatchCommandError,
    HTTPGatewayError,
    ProtocolError,
)
from b24api.execution import WorkClass
from b24api.execution.executor import (
    _is_retryable,
    _proves_not_run,
    _raise_embedded_result_error,
    _refused_before_running,
    _RequestAttempts,
)
from b24api.transport.base import WireRequest

if TYPE_CHECKING:
    from collections.abc import Iterable

    from b24api.batch.outcome import BatchOutcome
    from b24api.contracts.json import JsonValue
    from b24api.contracts.request import Request
    from b24api.execution import ExecutionContext, Executor
    from b24api.execution.executor import PhysicalAttemptOutcome

_OPERATION_TIME_LIMIT = "operation_time_limit"


@dataclass(slots=True)
class _V3Resolution:
    """Resolve one native v3 safety group to exactly one outcome per command."""

    executor: Executor
    context: ExecutionContext
    # The group's attempt lineage, which also records whether an envelope that carried the still pending
    # commands may have run them.
    attempts: _RequestAttempts = field(default_factory=_RequestAttempts)
    envelope_retry_used: bool = False
    settled: dict[int, BatchOutcome] = field(default_factory=dict)
    # The outcome each command last received, kept when a later round is stopped before it is sent.
    last: dict[int, BatchOutcome] = field(default_factory=dict)

    async def run(self, commands: tuple[_Command, ...]) -> tuple[BatchOutcome, ...]:
        """Send the group's envelopes until every command has its one settled outcome."""
        safe = all(command.request.replay_safety is ReplaySafety.SAFE for command in commands)
        pending = commands
        while pending:
            attempt = await self.executor._execute_physical_batch_attempt(  # noqa: SLF001 - batch-owned physical port
                _envelope(pending),
                context=self.context,
                attempts=self.attempts,
                methods=frozenset(command.request.method for command in pending),
            )
            pending = await self._classify(pending, attempt, safe=safe)
        return tuple(self.settled[command.index] for command in commands)

    async def _classify(
        self, pending: tuple[_Command, ...], attempt: PhysicalAttemptOutcome, *, safe: bool
    ) -> tuple[_Command, ...]:
        """Classify one physical attempt and return the commands that are sent again."""
        if attempt.kind == "cancelled":
            # Cancellation after dispatch: nothing is replayed and nothing is guessed; it propagates.
            raise attempt.error if isinstance(attempt.error, BaseException) else asyncio.CancelledError
        if attempt.kind == "response" and attempt.response is not None:
            elements = _complete_array(attempt.response, len(pending))
            if elements is not None:
                return await self._attributed(pending, elements, attempt.response)
            malformed = ProtocolError(
                "REST v3 batch result is not a complete array of command responses",
                evidence=attempt.response.evidence,
            )
            return await self._unresolved(pending, malformed, safe=safe)
        error = attempt.error
        if not isinstance(error, B24ApiError):
            raise TypeError("physical batch attempt failed without a typed error")
        if not attempt.dispatch_started:
            return self._stop(pending, error)
        if attempt.transport_proves_not_accepted or _unstructured_refusal(error, self.context):
            return await self._not_run(pending, error, safe=safe)
        return await self._unresolved(pending, error, safe=safe)

    async def _attributed(
        self, pending: tuple[_Command, ...], elements: list[JsonValue], response: Response
    ) -> tuple[_Command, ...]:
        """Apply each command's own embedded-error contract to its element of a complete array."""
        outcomes = tuple(
            self._decode(command, element, response) for command, element in zip(pending, elements, strict=True)
        )
        # Each element answered this send; a command sent again carries only what earlier sends left open.
        self.attempts.classified(None, may_have_run=False)
        cooldown = 0.0
        for command, outcome in zip(pending, outcomes, strict=True):
            self.last[command.index] = outcome
            error = outcome.error if isinstance(outcome, BatchFailure) else None
            if isinstance(error, BatchCommandError) and error.normalized_code == _OPERATION_TIME_LIMIT:
                observed = await self.context.coordinator.observe_api_throttle(
                    command.request.method, error.normalized_code
                )
                cooldown = max(cooldown, observed)
        # A SAFE command is sent again after its own transient error; any other only after a listed refusal.
        replay = tuple(
            command
            for command, outcome in zip(pending, outcomes, strict=True)
            if isinstance(outcome, BatchFailure)
            and isinstance(outcome.error, BatchCommandError)
            and outcome.replay_disposition is ReplayDisposition.ELIGIBLE
        )
        replayed = {command.index for command in replay}
        self._settle(command for command in pending if command.index not in replayed)
        if replay and await self.executor.pause_before_replay(
            self.context, attempts=self.attempts, method_cooldown=cooldown
        ):
            return replay
        self._settle(replay)
        return ()

    def _decode(self, command: _Command, element: JsonValue, response: Response) -> BatchOutcome:
        """Decode one element; its failure records what this send and the earlier ones left open about it."""
        evidence = BatchCommandEvidence(command.index, command.stable_key)
        policy = self.context.policy
        command_response = Response(element, evidence=response.evidence)
        try:
            _raise_embedded_result_error(
                command.request,
                element,
                http_status=response.evidence.http_status or 200,
                retry_codes=policy.retry.transient_api_codes,
                batch=True,
                redactor=self.executor.codec.redactor,
            )
        except BatchCommandError as error:
            coded = BatchCommandEvidence(
                command.index,
                command.stable_key,
                original_code=error.original_code,
                normalized_code=error.normalized_code,
            )
            self.attempts.record(error, policy)
            return _command_failure(command, error, evidence=coded, refused=_refused_before_running(error, policy))
        except ProtocolError as error:
            self.attempts.record(error, policy)
            return _shared_failure(command, error)
        return _command_success(command, command_response, evidence)

    async def _not_run(self, pending: tuple[_Command, ...], error: B24ApiError, *, safe: bool) -> tuple[_Command, ...]:
        """Proven nonexecution of the envelope: the ordinary bounded retry, whatever the safety class."""
        self.attempts.classified(error, may_have_run=False)
        self.attempts.record(error, self.context.policy)
        for command in pending:
            self.last[command.index] = _shared_failure(command, error)
        safety = ReplaySafety.SAFE if safe else ReplaySafety.UNKNOWN
        if _is_retryable(error, safety=safety, policy=self.context.policy) and await self.executor.pause_before_replay(
            self.context, attempts=self.attempts
        ):
            return pending
        self._settle(pending)
        return ()

    async def _unresolved(
        self, pending: tuple[_Command, ...], error: B24ApiError, *, safe: bool
    ) -> tuple[_Command, ...]:
        """Possible execution without attributable outcomes: an abort, a malformed result or a lost answer."""
        self.context.record_ambiguous_physical_attempt()
        self.attempts.classified(error, may_have_run=True)
        self.attempts.record(error, self.context.policy)
        cooldown = await self._observe_abort_throttle(pending, error)
        for command in pending:
            self.last[command.index] = _shared_failure(command, error)
        if (
            safe
            and not self.envelope_retry_used
            and _is_retryable(error, safety=ReplaySafety.SAFE, policy=self.context.policy)
        ):
            self.envelope_retry_used = True
            if await self.executor.pause_before_replay(self.context, attempts=self.attempts, method_cooldown=cooldown):
                return pending
        if safe and self._isolation_allowed():
            for command, outcome in zip(pending, await self._isolate(pending), strict=True):
                self.last[command.index] = outcome
        self._settle(pending)
        return ()

    def _stop(self, pending: tuple[_Command, ...], error: B24ApiError) -> tuple[_Command, ...]:
        """A round stopped before dispatch leaves each command the outcome it last received."""
        for command in pending:
            self.last.setdefault(command.index, _shared_failure(command, error))
        self._settle(pending)
        return ()

    def _settle(self, commands: Iterable[_Command]) -> None:
        for command in commands:
            self.settled[command.index] = self.last[command.index]

    async def _observe_abort_throttle(self, pending: tuple[_Command, ...], error: B24ApiError) -> float:
        """A top-level throttle code cannot be attributed, so every distinct inner method observes it."""
        if not isinstance(error, ApiResponseError) or error.normalized_code != _OPERATION_TIME_LIMIT:
            return 0.0
        cooldown = 0.0
        for method in sorted({command.request.method for command in pending}):
            observed = await self.context.coordinator.observe_api_throttle(method, error.normalized_code)
            cooldown = max(cooldown, observed)
        return cooldown

    def _isolation_allowed(self) -> bool:
        """One bounded attribution phase, only within the inherited attempt and retry-time budgets."""
        policy = self.context.policy
        started = self.attempts.started
        return (
            policy.retry.isolate_safe_batch_abort
            and started is not None
            and self.attempts.used < policy.max_attempts_per_request
            and self.context.remaining_time(retry_started=started) > 0
        )

    async def _isolate(self, pending: tuple[_Command, ...]) -> tuple[BatchOutcome, ...]:
        """Send each unresolved SAFE command individually; they are never packed into a batch again."""
        tasks = [asyncio.ensure_future(self._isolate_one(command)) for command in pending]
        try:
            return tuple(await asyncio.gather(*tasks))
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def _isolate_one(self, command: _Command) -> BatchOutcome:
        # A per-command snapshot of the group lineage: started, used and possible-execution flags carry over.
        lineage = dataclasses.replace(self.attempts)
        spent = lineage.used
        try:
            response = await self.executor.execute(
                command.request,
                context=self.context,
                work_class=WorkClass.RETRY,
                _attempts=lineage,
            )
        except B24ApiError as error:
            if lineage.used == spent:
                # The executor records facts for its own sends; the envelope already sent this command.
                lineage.record_unsent(error)
            return _isolated_failure(command, error)
        finally:
            self.context.record_batch_isolation_requests(lineage.used - spent)
        return _command_success(command, response, BatchCommandEvidence(command.index, command.stable_key))


def _isolated_failure(command: _Command, error: B24ApiError) -> BatchOutcome:
    """An individual answer names its code; whether the command may have run is in the execution facts."""
    if isinstance(error, ApiResponseError):
        evidence = BatchCommandEvidence(
            command.index,
            command.stable_key,
            original_code=error.original_code,
            normalized_code=error.normalized_code,
        )
        return _command_failure(command, error, evidence=evidence)
    return _shared_failure(command, error)


def _envelope(commands: tuple[_Command, ...]) -> WireRequest:
    body = V3BatchBody(tuple(V3BatchCommand(command.request.method, _query(command.request)) for command in commands))
    return WireRequest._v3_batch(  # noqa: SLF001 - the batch package owns the v3 envelope constructor
        body, replay_safety=envelope_safety(command.request.replay_safety for command in commands)
    )


def _query(request: Request) -> FrozenMapping:
    query = _freeze_json(request.copy_parameters())
    if not isinstance(query, FrozenMapping):  # pragma: no cover - a canonical request has named parameters
        raise TypeError("v3 batch command requires named parameters")
    return query


def _complete_array(response: Response, expected: int) -> list[JsonValue] | None:
    """Return the input-ordered elements only when the array has exactly one JSON object per command."""
    result = response.result
    if not isinstance(result, list) or len(result) != expected or any(not isinstance(item, dict) for item in result):
        return None
    return result


def _unstructured_refusal(error: B24ApiError, context: ExecutionContext) -> bool:
    """A listed unstructured refusal status proves the envelope was not accepted; a structured error never does."""
    return isinstance(error, HTTPGatewayError) and _proves_not_run(error, context.policy)


__all__: list[str] = []
