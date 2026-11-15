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
    _stopped_after,
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
    BudgetExceededError,
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
    _retry_refusal,
    _SendPlan,
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
        # The failure of the whole envelope that the pending commands are sent again after; ``None`` after their own
        # elements answered them.
        followed: B24ApiError | None = None
        while pending:
            attempt = await self.executor._execute_physical_batch_attempt(  # noqa: SLF001 - batch-owned physical port
                _envelope(pending),
                context=self.context,
                attempts=self.attempts,
                methods=frozenset(command.request.method for command in pending),
            )
            pending, followed = await self._classify(pending, attempt, safe=safe, followed=followed)
        return tuple(self.settled[command.index] for command in commands)

    async def _classify(
        self,
        pending: tuple[_Command, ...],
        attempt: PhysicalAttemptOutcome,
        *,
        safe: bool,
        followed: B24ApiError | None,
    ) -> tuple[tuple[_Command, ...], B24ApiError | None]:
        """Classify one physical attempt; return the commands sent again, and the envelope failure they follow."""
        if attempt.kind == "cancelled":
            # Cancellation after dispatch: nothing is replayed and nothing is guessed; it propagates.
            raise attempt.error if isinstance(attempt.error, BaseException) else asyncio.CancelledError
        if attempt.kind == "response" and attempt.response is not None:
            elements = _complete_array(attempt.response, len(pending))
            if elements is not None:
                return await self._attributed(pending, elements, attempt.response), None
            malformed = ProtocolError(
                "REST v3 batch result is not a complete array of command responses",
                evidence=attempt.response.evidence,
            )
            replay = await self._unresolved(pending, malformed, safe=safe)
            return replay, malformed if replay else None
        error = attempt.error
        if not isinstance(error, B24ApiError):
            raise TypeError("physical batch attempt failed without a typed error")
        if not attempt.dispatch_started:
            return self._stop(pending, error, followed=followed), None
        if attempt.transport_proves_not_accepted or _unstructured_refusal(error, self.context):
            replay = await self._not_run(pending, error, safe=safe)
        else:
            replay = await self._unresolved(pending, error, safe=safe)
        return replay, error if replay else None

    async def _attributed(
        self, pending: tuple[_Command, ...], elements: list[JsonValue], response: Response
    ) -> tuple[_Command, ...]:
        """Apply each command's own embedded-error contract to its element of a complete array."""
        outcomes = tuple(
            self._decode(command, element, response) for command, element in zip(pending, elements, strict=True)
        )
        # Each element answered this send; a command sent again carries only what earlier sends left open.
        self.attempts.classified(None, may_have_run=False)
        for command, outcome in zip(pending, outcomes, strict=True):
            self.last[command.index] = outcome
            error = outcome.error if isinstance(outcome, BatchFailure) else None
            if isinstance(error, BatchCommandError) and error.normalized_code == _OPERATION_TIME_LIMIT:
                await self.context.coordinator.observe_api_throttle(command.request.method, error.normalized_code)
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
        if replay and await self._pause(replay) is None:
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
        if _is_retryable(error, safety=safety, policy=self.context.policy):
            stop = await self._pause(pending)
            if stop is None:
                return pending
            # As a lone request and a classic batch end: with the budget that stopped the retry, caused by the refusal.
            stopped = _stopped_after(BudgetExceededError(stop), error, self.attempts.lineage)
            for command in pending:
                self.last[command.index] = _shared_failure(command, stopped)
        self._settle(pending)
        return ()

    async def _unresolved(
        self, pending: tuple[_Command, ...], error: B24ApiError, *, safe: bool
    ) -> tuple[_Command, ...]:
        """Possible execution without attributable outcomes: an abort, a malformed result or a lost answer."""
        self.context.record_ambiguous_physical_attempt()
        self.attempts.classified(error, may_have_run=True)
        self.attempts.record(error, self.context.policy)
        await self._observe_abort_throttle(pending, error)
        for command in pending:
            self.last[command.index] = _shared_failure(command, error)
        if safe:
            # The one envelope retry of a transient failure, else the isolation of each command: either is a replay
            # of the group, planned and paced as one. A replay the budget refuses leaves each command the
            # envelope's failure.
            retry = not self.envelope_retry_used and _is_retryable(
                error, safety=ReplaySafety.SAFE, policy=self.context.policy
            )
            if (retry or self.context.policy.retry.isolate_safe_batch_abort) and await self._pause(pending) is None:
                if retry:
                    self.envelope_retry_used = True
                    return pending
                await self._isolate(pending)
        self._settle(pending)
        return ()

    def _stop(
        self, pending: tuple[_Command, ...], error: B24ApiError, *, followed: B24ApiError | None
    ) -> tuple[_Command, ...]:
        """End the commands of a round that ``error`` stopped before its send, as a classic batch ends its own.

        Sent again after a failure of the whole envelope and stopped before the send, by the budget (at the permit,
        or at the send for too little window after the wait) or by any other failure (a closed client), each ends
        with that stop, caused by the failure it followed (``_stopped_after``). Commands their own elements
        answered keep that answer; those of a first send end with the stop.
        """
        if followed is not None:
            ended = _stopped_after(error, followed, self.attempts.lineage)
            for command in pending:
                self.last[command.index] = _shared_failure(command, ended)
        for command in pending:
            self.last.setdefault(command.index, _shared_failure(command, error))
        self._settle(pending)
        return ()

    def _settle(self, commands: Iterable[_Command]) -> None:
        for command in commands:
            self.settled[command.index] = self.last[command.index]

    async def _observe_abort_throttle(self, pending: tuple[_Command, ...], error: B24ApiError) -> None:
        """A top-level throttle code cannot be attributed, so every distinct inner method observes it."""
        if not isinstance(error, ApiResponseError) or error.normalized_code != _OPERATION_TIME_LIMIT:
            return
        for method in sorted({command.request.method for command in pending}):
            await self.context.coordinator.observe_api_throttle(method, error.normalized_code)

    async def _pause(self, commands: tuple[_Command, ...]) -> str | None:
        """Wait before sending ``commands`` again; return why the group's budget forbids it, if it does.

        A group carries one safety class and travels in one envelope, so its replay, another envelope or the isolation
        of its commands, is admitted or refused whole: by the attempt budget, the retry window, the operation's
        deadline (recorded) or a wait, the longer of the backoff and the coordinator's hold, that the window cannot
        carry.
        """
        safety = envelope_safety(command.request.replay_safety for command in commands)
        methods = frozenset(command.request.method for command in commands)
        try:
            plan = self.executor.plan_replay(self.context, attempts=self.attempts, sends=((safety, methods),))
        except BudgetExceededError as refusal:
            return str(refusal)
        if plan is None or plan.plans != (_SendPlan.SEND,):
            return _retry_refusal(plan)
        await self.executor.replay_after(self.context, attempts=self.attempts, pause=plan.pause)
        return None

    async def _isolate(self, pending: tuple[_Command, ...]) -> None:
        """Send each unresolved SAFE command alone, never packed into a batch again, after the group's replay wait.

        The commands go one at a time: the isolation stays within the dispatch slot its envelope held, one request
        in flight. Each send is an ordinary request on the command's copy of the group lineage, which the executor
        admits at its permit and paces through its own retries.
        """
        for command in pending:
            self.last[command.index] = await self._isolate_one(command)

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
