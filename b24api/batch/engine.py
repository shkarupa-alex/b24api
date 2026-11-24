"""Bounded fail-fast and total-outcome Bitrix batch execution."""

from __future__ import annotations
import asyncio
import copy
from collections import deque
from collections.abc import AsyncIterable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from b24api._error_types import ErrorOrigin
from b24api.batch.correlation import (
    _BatchNotExecutedError,
    _by_facts,
    _Command,
    _command_failure,
    _command_success,
    _shared_failure,
    _withheld,
)
from b24api.batch.outcome import (
    BatchCommandEvidence,
    BatchFailure,
    BatchOutcome,
    BatchSuccess,
)
from b24api.batch.v3 import _V3Resolution, observe_entries
from b24api.contracts.dispatch import PORTAL_BATCH_CAP
from b24api.contracts.json import repeated_names
from b24api.contracts.policy import (
    ReplayDisposition,
)
from b24api.contracts.request import ReplaySafety, Request, diagnostic_context
from b24api.contracts.request_summary import RouteKind
from b24api.contracts.response import Response, ResponseTime
from b24api.encoding import encode_php_query
from b24api.errors import (
    AmbiguousExecutionError,
    B24ApiError,
    BatchCommandError,
    BudgetExceededError,
    CapabilityError,
    ProtocolError,
)
from b24api.execution import (
    ExecutionContext,
    Executor,
    WorkClass,
)
from b24api.execution.executor import (
    _HOLD_REFUSAL,
    _decode_response_time,
    _is_retryable,
    _Lineage,
    _raise_embedded_result_error,
    _RequestAttempts,
    _retry_refusal,
    _SendPlan,
    _Settled,
    ends_held_out,
)
from b24api.execution.facts import Execution, Verdict, execution, record_execution, send_verdict, settled_as

if TYPE_CHECKING:
    from b24api.contracts.completion import CommandSettlement
    from b24api.contracts.evidence import ResponseEvidence
    from b24api.contracts.json import JsonValue
    from b24api.contracts.policy import ExecutionPolicy

_MISSING = object()
_UNSUPPORTED_COMMAND = "physical batch supports BARE JSON requests without scoped headers; use direct dispatch"
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
    # The operation's budget refused the pull after these commands: they are sent (the budget refuses each send
    # in turn), then the stream ends with the refusal.
    refusal: BudgetExceededError | None = None


@dataclass(frozen=True, slots=True)
class _RoundOptions:
    halt: bool
    advisory_totals: bool
    strict_envelope: bool
    # Whether a command held longer than the others of its envelope goes in a later one (``_split_held``).
    split_held: bool = False


@dataclass(frozen=True, slots=True)
class _Round:
    """What one classic round gave its commands, which of them may run again, and the envelope's own failure.

    ``failure`` is set when the envelope failed as a whole (no entry answered a command); ``None`` when each
    command's entry answered it. ``send`` is what the round's send alone left open, the same for each command it
    carried; nothing sent when the round was stopped before its send.
    """

    outcomes: tuple[BatchOutcome, ...]
    send: _Lineage
    replay: tuple[_Command, ...] = ()
    failure: B24ApiError | None = None

    @property
    def dispatched(self) -> bool:
        """Return whether the round's envelope was handed to the transport."""
        return self.send.latest is not _Settled.NOTHING_SENT

    @property
    def answered_whole(self) -> bool:
        """Return whether the envelope failed as a whole with an answer that leaves nothing open: a refusal."""
        return self.failure is not None and self.send.latest is _Settled.ANSWERED


@dataclass(frozen=True, slots=True)
class _Answered:
    """Its own entry answered the command, with a failure it may be sent again after."""

    outcome: BatchOutcome


@dataclass(frozen=True, slots=True)
class _Follows:
    """The command's envelope failed as a whole, with a failure it may be sent again after."""

    failure: B24ApiError


@dataclass(frozen=True, slots=True)
class _Waiting:
    """A command that may run again: what its latest send gave it, and what the sends it was in left open."""

    state: _Answered | _Follows
    lineage: _Lineage


def _state(outcome: BatchOutcome, sent: _Round) -> _Answered | _Follows:
    """Return what a sent round left a command it may send again: its entry's answer, or its envelope's failure."""
    return _Answered(outcome) if sent.failure is None else _Follows(sent.failure)


@dataclass(slots=True)
class _Lineup:
    """Where each command of one classic envelope stands between its rounds.

    ``settled`` holds each command's final outcome; ``waiting`` each command that may run again, in one state:
    answered by its own entry, an answer a stop leaves it; or following the failure of its envelope as a whole,
    which the budget that stops it reports as its cause, as a lone request's would be, or which ends it when it
    is held out (``stop``). Each waiting command
    keeps the lineage of the sends it was in: a SAFE command the last plan left for a later round is
    ``carried``, misses that round, and is planned again with the next round's commands, so what a round's
    send left open is added only to the commands that were in it.
    """

    settled: dict[int, BatchOutcome] = field(default_factory=dict)
    waiting: dict[int, _Waiting] = field(default_factory=dict)
    carried: tuple[_Command, ...] = ()

    def after_round(self, sent: _Round, commands: tuple[_Command, ...]) -> tuple[_Command, ...]:
        """Record what a round gave its commands; return those that may run again, carried ones included.

        A round proves nothing new about a command when it was stopped before its send, or when its envelope was
        refused as a whole after the command's own entry answered it: the command stays as it was. A failure that
        stops a round before its send, the budget's or any other (a closed client), stops each command it holds
        as ``_stop`` does, whichever command's failure asked for that round.
        """
        again = {command.index for command in sent.replay}
        for command, outcome in zip(commands, sent.outcomes, strict=True):
            before = self.waiting.pop(command.index, None)
            if before is None:
                self._after_first(command, outcome, sent, again=command.index in again)
            elif command.index in again and (
                not sent.dispatched or (isinstance(before.state, _Answered) and sent.answered_whole)
            ):
                self.waiting[command.index] = _Waiting(before.state, before.lineage.after(sent.send))
            elif command.index in again:
                self.waiting[command.index] = _Waiting(_state(outcome, sent), before.lineage.after(sent.send))
            elif not sent.dispatched and sent.failure is not None:
                self._stop(command, before, sent.failure)
            else:
                self._settle(outcome, before.lineage)
        candidates, self.carried = (*sent.replay, *self.carried), ()
        return candidates

    def _after_first(self, command: _Command, outcome: BatchOutcome, sent: _Round, *, again: bool) -> None:
        """Record what the first round gave a command, which nothing was sent for before."""
        if again:
            self.waiting[command.index] = _Waiting(_state(outcome, sent), _Lineage().after(sent.send))
        else:
            self._settle(outcome, _Lineage())

    def stop(
        self, commands: tuple[_Command, ...], refusal: BudgetExceededError, *, plan: _SendPlan = _SendPlan.REFUSE
    ) -> None:
        """End the commands the budget keeps from being sent again, for ``refusal``.

        A command held out (``ends_held_out``) after its envelope's answer about the server's load ends with that
        answer instead, as a lone request does.
        """
        for command in commands:
            waiting = self.waiting.pop(command.index)
            if isinstance(waiting.state, _Follows) and ends_held_out(plan, waiting.state.failure):
                self._settle(_shared_failure(command, waiting.state.failure), waiting.lineage)
            else:
                self._stop(command, waiting, refusal)

    def _stop(self, command: _Command, waiting: _Waiting, stop: B24ApiError) -> None:
        """End a command ``stop`` keeps from being sent again: with its entry's answer, or with the stop.

        The stop is what ended a command that followed its round's failure, so it is the outcome, caused by the
        failure that command followed, possibly run only when a send it was in may have run. The executor chains a
        stop to the batch's latest failure, but a command carried past a round followed an earlier one, so each
        command gets its own copy of the stop, with the stop's traceback and its own cause.
        """
        if isinstance(waiting.state, _Answered):
            self._settle(waiting.state.outcome, waiting.lineage)
            return
        error = copy.copy(stop).with_traceback(stop.__traceback__)
        error.__cause__ = waiting.state.failure
        waiting.lineage.record_open(error)
        self._settle(_shared_failure(command, error), waiting.lineage)

    def _settle(self, outcome: BatchOutcome, lineage: _Lineage) -> None:
        """Make an outcome final, adding what the command's own sends before it left open (``lineage``)."""
        self.settled[outcome.command_index] = _by_facts(outcome, lineage.left_open())


class BatchProgress:
    """Where each command of one running chunk stands, by command index, for a caller whose wait for it ends early.

    The engine keeps it as the chunk runs: the final outcomes of its envelope groups, a classic envelope's lineup
    (each waiting command's latest answer and the lineage of its sends) and the round being sent. ``execution``
    reads it by the rule a command's own failure follows: a command whose latest send may have run without an
    answer, or one before it, may have run; one answered, by its entry or with its envelope, was answered
    (it ran, or was refused, as its answer says); one never sent was not sent. Until the chunk has ended, a
    command without a final outcome may still be sent, so it may run. A REST v3 group sends each command it still
    resolves in every envelope, so the group's lineage is each such command's own, as a classic round's is.
    """

    def __init__(self) -> None:
        """Start with nothing sent."""
        self._final: Mapping[int, BatchOutcome] = {}
        # The final outcomes of the group in progress, which it settles before the chunk's map has them.
        self._group: Mapping[int, BatchOutcome] = {}
        self._lineup: _Lineup | None = None
        self._attempts: _RequestAttempts | None = None
        self._round: frozenset[int] = frozenset()
        self._opaque: frozenset[int] = frozenset()
        self._ended = False

    def end(self) -> None:
        """Record that the chunk has ended: nothing more of it is sent."""
        self._ended = True

    def _outcome(self, index: int) -> BatchOutcome | None:
        """Return the final outcome of command ``index``, or ``None`` while it has none."""
        outcome = self._final.get(index)
        return self._group.get(index) if outcome is None else outcome

    def track_groups(self, settled: Mapping[int, BatchOutcome]) -> None:
        """Read the chunk's final outcomes from ``settled``, which the engine fills as each group ends."""
        self._final = settled

    def track_bare(self, lineup: _Lineup, attempts: _RequestAttempts) -> None:
        """Read a classic envelope's commands from its lineup and the attempts its rounds share."""
        self._lineup, self._attempts, self._opaque, self._group = lineup, attempts, frozenset(), lineup.settled

    def track_round(self, commands: tuple[_Command, ...]) -> None:
        """Mark the commands of the latest classic round, whose sends ``execution`` adds to each one's lineage.

        Once the round's outcomes are in the lineup, a waiting command's lineage holds its sends and the attempts
        start the next round (``_RequestAttempts.start_round``), which adds nothing, so the mark stays until then.
        """
        self._round = frozenset(command.index for command in commands)

    def track_group(
        self, commands: tuple[_Command, ...], attempts: _RequestAttempts, settled: Mapping[int, BatchOutcome]
    ) -> None:
        """Read a REST v3 group's commands from the attempts its envelopes share and the outcomes it settled."""
        self._lineup, self._attempts, self._round, self._group = None, attempts, frozenset(), settled
        self._opaque = frozenset(command.index for command in commands)

    def settlement(self, index: int) -> CommandSettlement:
        """Return how command ``index`` settles from what it did so far, by the one rule (``settled_as``)."""
        return settled_as(self.execution(index), succeeded=self._succeeded(index))

    def record(self, error: BaseException, index: int) -> None:
        """Record on ``error``, its page's failure, what command ``index`` did so far, a success included."""
        record_execution(error, self.execution(index), succeeded=self._succeeded(index))

    def _succeeded(self, index: int) -> bool:
        """Return whether command ``index`` was answered with a success."""
        return isinstance(self._outcome(index), BatchSuccess)

    def execution(self, index: int) -> Execution:
        """Return what command ``index`` is known to have done so far."""
        outcome = self._outcome(index)
        if outcome is not None:
            return Execution.ANSWERED if isinstance(outcome, BatchSuccess) else execution(outcome.error)
        if not self._ended:
            return Execution.POSSIBLY_RUN
        if index in self._opaque and self._attempts is not None:
            return self._attempts.lineage.unsent()
        waiting = None if self._lineup is None else self._lineup.waiting.get(index)
        lineage = _Lineage() if waiting is None else waiting.lineage
        if index in self._round and self._attempts is not None:
            lineage = lineage.after(self._attempts.lineage)
        return lineage.unsent()


@dataclass(frozen=True, slots=True)
class _BatchEnvelope:
    results: Mapping[str, object]
    errors: Mapping[str, object]
    totals: Mapping[str, object]
    continuations: Mapping[str, object]
    evidence: ResponseEvidence
    # Keys that are both a result and an error: the answer contradicts itself and attributes nothing.
    contradictory: frozenset[str] = frozenset()
    # Each command's ``result_time``: its timing, diagnostic only.
    times: Mapping[str, object] = field(default_factory=dict)


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
        on_envelope: Callable[[], None] | None = None,
        split_held: bool = False,
        progress: BatchProgress | None = None,
    ) -> tuple[BatchOutcome, ...]:
        """Execute one admitted chunk: classic BARE and native v3 commands never share an envelope.

        ``on_envelope`` is called once as each admitted envelope group starts its first round, the 2.5.2 rule for
        an admitted chunk: the group counts even when its permit then refuses its first send. Commands the first
        send's plan refuses at once (``_split_held``) form no group and are not counted. Halted groups, replay
        rounds, envelope retries and isolation requests are not counted.

        ``split_held`` is set for a user's logical batch, whose commands are independent outcomes: a command whose
        own method is held longer than the others goes in a later envelope, so that the others go now, and one held
        beyond what the operation has left is refused (``_split_held``). A
        fail-fast (``halt``) chunk is never split: leaving a command out lets no other command go when its failure
        fails them all, so the envelope goes whole to its permit, which waits within the operation's budget or
        refuses at once a hold that outlasts it. Pages of a traversal, and the commands a dispatcher batches, are not
        split either: they wait at the permit, as they would sent one by one.

        ``progress``, when given, is kept on where each command stands while the chunk runs (``BatchProgress``), for
        a caller whose wait for the chunk a cancellation or another failure ends.
        """
        eligible, rejected = self._partition_capabilities(commands)
        settled: Mapping[int, BatchOutcome] = {}
        progress = BatchProgress() if progress is None else progress
        try:
            if halt and rejected:
                first_rejected = next(command.index for command in commands if command.index in rejected)
                message = "physical batch command was not executed after fail-fast capability rejection"
                rejected = {
                    command.index: rejected[command.index]
                    if command.index == first_rejected
                    else _shared_failure(command, _withheld(message, command))
                    for command in commands
                }
            elif eligible:
                options = _RoundOptions(halt, advisory_totals, strict_envelope, split_held and not halt)
                settled = await self._execute_groups(
                    eligible, context=context, options=options, on_envelope=on_envelope, progress=progress
                )
        finally:
            # Whichever way the chunk ends, a chunk refused before any send included, nothing more of it is sent.
            progress.end()
        # Every outcome of the chunk leaves the engine through the one mapping from execution facts.
        return tuple(_by_facts(outcome) for outcome in _merge_outcomes(commands, tuple(settled.values()), rejected))

    async def _execute_groups(
        self,
        eligible: tuple[_Command, ...],
        *,
        context: ExecutionContext,
        options: _RoundOptions,
        on_envelope: Callable[[], None] | None,
        progress: BatchProgress,
    ) -> dict[int, BatchOutcome]:
        settled: dict[int, BatchOutcome] = {}
        progress.track_groups(settled)
        groups = deque(_envelope_groups(eligible))
        while groups:
            group = groups.popleft()
            if options.halt and any(not isinstance(outcome, BatchSuccess) for outcome in settled.values()):
                # Fail-fast: a later envelope of the accepted chunk is not sent after a failed one.
                message = "physical batch command was not executed after a fail-fast envelope failure"
                settled.update(
                    (command.index, _shared_failure(command, _withheld(message, command))) for command in group
                )
                continue
            sending, later, refused = (
                self._split_held(group, context=context) if options.split_held else (group, (), {})
            )
            settled.update(refused)
            if later:
                # Held longer than the others: a later envelope, planned again once this one is sent.
                groups.append(later)
            if not sending:
                continue
            if on_envelope is not None:
                on_envelope()
            if sending[0].request.route is RouteKind.API_V3:
                resolution = _V3Resolution(self.executor, context)
                progress.track_group(sending, resolution.attempts, resolution.settled)
                outcomes = await resolution.run(sending)
            else:
                outcomes = await self._execute_bare(sending, context=context, options=options, progress=progress)
            settled.update((outcome.command_index, outcome) for outcome in outcomes)
        return settled

    def _split_held(
        self, group: tuple[_Command, ...], *, context: ExecutionContext
    ) -> tuple[tuple[_Command, ...], tuple[_Command, ...], dict[int, BatchOutcome]]:
        """Split a group into what its first send carries now, what a later envelope carries, and what is refused.

        ``Executor.plan_first_send`` decides, so one method's hold never holds back or fails the other commands of
        its envelope: a command held longer goes in a later envelope, and only one held beyond what the operation
        has left is refused, never sent: it ends with ``BudgetExceededError``, proven not run.
        """
        try:
            plan = self.executor.plan_first_send(
                context, methods=tuple(frozenset({command.request.method}) for command in group)
            )
        except BudgetExceededError as refusal:
            # No time is left: the deadline refused the envelope's permit, so none of its commands is sent.
            return (), (), {command.index: _shared_failure(command, refusal) for command in group}
        if plan is None:
            return group, (), {}
        sending, later = (
            tuple(group[position] for position in plan.sends(each)) for each in (_SendPlan.SEND, _SendPlan.CARRY)
        )
        refused = tuple(command for command, each in zip(group, plan.plans, strict=True) if each.refused)
        return (
            sending,
            later,
            {command.index: _shared_failure(command, BudgetExceededError(_HOLD_REFUSAL)) for command in refused},
        )

    async def _execute_bare(
        self,
        eligible: tuple[_Command, ...],
        *,
        context: ExecutionContext,
        options: _RoundOptions,
        progress: BatchProgress,
    ) -> tuple[BatchOutcome, ...]:
        """Run one classic batch envelope and its replay rounds, keeping ``progress`` on where each command stands.

        What ends the envelope instead of an outcome (a cancellation, in flight or in a wait) records nothing of its
        own: each command settles from ``progress``, the one record of what it did.
        """
        # The rounds share one attempt count, one retry window and one wait; each command keeps the lineage of the
        # rounds it was in (``_Lineup``). The engine alone decides every round after the first, whether the
        # envelope failed as a whole or its entries answered: the executor sends each round once.
        attempts = _RequestAttempts(caller_retries=True)
        lineup = _Lineup()
        pending = eligible
        progress.track_bare(lineup, attempts)
        while pending:
            progress.track_round(pending)
            sent = await self._dispatch_round(pending, context=context, options=options, attempts=attempts)
            # Only the commands that may run again are sent again, in a smaller physical batch; the rest keep the
            # outcome this round gave them. ``attempts`` covers the next round from here on.
            candidates = lineup.after_round(sent, pending)
            attempts.start_round()
            pending = await self._next_round(candidates, lineup, context=context, attempts=attempts, options=options)
        return tuple(lineup.settled.values())

    async def _next_round(
        self,
        candidates: tuple[_Command, ...],
        lineup: _Lineup,
        *,
        context: ExecutionContext,
        attempts: _RequestAttempts,
        options: _RoundOptions,
    ) -> tuple[_Command, ...]:
        """Plan the next round from the commands that may run again, wait for it, and return what it sends.

        Each command is planned on its own safety and its own method's hold within the round's one plan (see
        ``_RoundPlan``): the round sends the commands the window admits, leaves a SAFE command whose own wait is
        longer for a later round, and stops the others, unless it is fail-fast, which is never split.
        """
        if not candidates:
            return ()
        try:
            plan = self.executor.plan_replay(
                context,
                attempts=attempts,
                sends=tuple(
                    (command.request.replay_safety, frozenset({command.request.method})) for command in candidates
                ),
                whole=options.halt,
            )
        except BudgetExceededError as refusal:
            lineup.stop(candidates, refusal)
            return ()
        if plan is None:
            lineup.stop(candidates, BudgetExceededError(_retry_refusal(plan)))
            return ()
        sending, carried, refused, held_out = (
            tuple(candidates[index] for index in plan.sends(each))
            for each in (_SendPlan.SEND, _SendPlan.CARRY, _SendPlan.REFUSE, _SendPlan.HELD_OUT)
        )
        lineup.carried = carried
        stop = BudgetExceededError(_retry_refusal(plan))
        lineup.stop(refused, stop)
        lineup.stop(held_out, stop, plan=_SendPlan.HELD_OUT)
        if sending:
            await self.executor.replay_after(context, attempts=attempts, pause=plan.pause)
        return sending

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
        spent = attempts.used
        # A failure of the executor carries the facts it recorded about this round's send; one of the engine's own
        # is recorded here. What each command's earlier sends left open is its own (``_Lineup``).
        failure: B24ApiError | None = None
        owned = False
        try:
            response = await self.executor.execute(
                request,
                context=context,
                work_class=WorkClass.BATCH,
                _admission_methods=frozenset(command.request.method for command in commands),
                _attempts=attempts,
            )
        except asyncio.CancelledError:
            raise
        except B24ApiError as error:
            failure = error
        else:
            try:
                envelope = _decode_batch_envelope(
                    response.result,
                    expected_keys=frozenset(command.stable_key for command in commands),
                    strict=options.strict_envelope,
                    evidence=response.evidence,
                )
            except B24ApiError as error:
                # An answer that attributes nothing is still the envelope's answer (``observe_envelope_answer``).
                self.executor.observe_envelope_answer(context, (), sent_at=attempts.sent_at)
                failure, owned = error, True
        if failure is not None:
            scoped = _scoped(failure, request, strict=options.strict_envelope)
            if (owned or scoped is not failure) and attempts.used > spent:
                attempts.record(scoped, context.policy)
            replay = _envelope_replay(
                commands, scoped, context, refused_at_send=failure is attempts.refusal, halt=options.halt
            )
            shared = tuple(_shared_failure(command, scoped) for command in commands)
            return _Round(shared, attempts.lineage, replay, scoped)

        outcomes = self._decode_commands(commands, envelope, context=context, options=options, attempts=attempts)
        # Each command's entry answered this send; a command sent again carries only what earlier sends left open.
        attempts.classified(None, may_have_run=False)
        await observe_entries(self.executor, context, commands, outcomes, sent_at=attempts.sent_at)
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
        return _Round(outcomes, attempts.lineage, replay)

    def _batch_route(self, request: Request) -> RouteKind | None:
        """Return the envelope dialect a request can travel in, or ``None`` when no envelope represents it."""
        if _classic_representable(request) and request.classic_batch_obstacle is None:
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
            obstacle = command.request.classic_batch_obstacle
            message = (
                _UNSUPPORTED_V3_TRANSPORT
                if _v3_representable(command.request)
                else f"classic batch cannot carry {obstacle}; use direct dispatch"
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

    def _decode_commands(
        self,
        commands: tuple[_Command, ...],
        envelope: _BatchEnvelope,
        *,
        context: ExecutionContext,
        options: _RoundOptions,
        attempts: _RequestAttempts,
    ) -> tuple[BatchOutcome, ...]:
        """Decode every command's entry in submission order, tracking the server's own halt.

        Under ``halt`` Bitrix stops the envelope at the first command it answers with an error, so only a
        command after such an entry and without one of its own is proven not executed by this send. Each
        command's failure records what this send and the earlier ones left open about it.
        """
        outcomes: list[BatchOutcome] = []
        halted = False
        for command in commands:
            key = command.stable_key
            outcome = _unattributed(command, envelope, halted=halted, attempts=attempts, policy=context.policy)
            if outcome is None:
                outcome = self._decode_command(
                    command, envelope, context=context, advisory_totals=options.advisory_totals
                )
                if isinstance(outcome, BatchFailure):
                    attempts.record(outcome.error, context.policy)
            outcomes.append(outcome)
            # The server halts after an error it answered a command with; an entry that names no error proves none.
            answered_error = isinstance(outcome, BatchFailure) and isinstance(outcome.error, BatchCommandError)
            halted = halted or (
                options.halt and key in envelope.errors and key not in envelope.contradictory and answered_error
            )
        return tuple(outcomes)

    def _decode_command(
        self,
        command: _Command,
        envelope: _BatchEnvelope,
        *,
        context: ExecutionContext,
        advisory_totals: bool = False,
    ) -> BatchOutcome:
        """Decode the one entry, an error or a result, that answers a command."""
        evidence = BatchCommandEvidence(command.index, command.stable_key)
        if command.stable_key in envelope.errors:
            command_error = self._command_error(
                envelope.errors[command.stable_key],
                command,
                retry_codes=context.policy.retry.transient_api_codes,
                evidence=envelope.evidence,
            )
            if isinstance(command_error, ProtocolError):
                return _shared_failure(command, command_error)
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
                refused=send_verdict(command_error, context.policy) is Verdict.NOT_RUN,
            )
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
                time=_command_time(envelope.times.get(command.stable_key)),
                evidence=envelope.evidence,
            )
            _raise_embedded_result_error(
                command.request,
                command_response.result,
                http_status=envelope.evidence.http_status or 200,
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
                command,
                error,
                evidence=command_evidence,
                refused=send_verdict(error, context.policy) is Verdict.NOT_RUN,
            )
        except ProtocolError as error:
            return _shared_failure(command, error)
        except (TypeError, ValueError) as error:
            protocol_error = ProtocolError(
                "Batch command metadata is malformed",
                origin=ErrorOrigin.PROTOCOL,
                request_summary=command.request.summary,
                evidence=envelope.evidence,
            )
            protocol_error.__cause__ = error
            return _shared_failure(command, protocol_error)
        return _command_success(command, command_response, evidence)

    async def execute_requests(  # noqa: PLR0913 - mirrors execute_chunk's round options
        self,
        requests: tuple[Request, ...],
        *,
        context: ExecutionContext,
        advisory_totals: bool = False,
        strict_envelope: bool = False,
        on_envelope: Callable[[], None] | None = None,
        progress: BatchProgress | None = None,
    ) -> tuple[BatchOutcome, ...]:
        """Execute one scheduler-owned chunk with total per-command correlation; ``progress`` as ``execute_chunk``."""
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
            on_envelope=on_envelope,
            progress=progress,
        )

    def _command_error(
        self,
        raw: object,
        command: _Command,
        *,
        retry_codes: frozenset[str],
        evidence: ResponseEvidence,
    ) -> BatchCommandError | ProtocolError:
        """Read a command's error entry: the server's error, or a ``ProtocolError`` for an entry that names none.

        An entry that is not an object with a scalar ``error`` code, or that repeats a member name, answers nothing:
        the command may have run.
        """
        code, description = (
            (raw.get("error"), raw.get("error_description")) if isinstance(raw, Mapping) else (None, None)
        )
        if not isinstance(code, str | int) or isinstance(code, bool) or repeated_names(raw):
            return ProtocolError(
                "Batch error entry is not an object with a scalar error code",
                origin=ErrorOrigin.PROTOCOL,
                request_summary=command.request.summary,
                evidence=evidence,
            )
        normalized = str(code).strip().casefold()
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


# The members of a classic batch envelope that map command keys to each command's answer.
_COMMAND_MAPS = ("result", "result_error", "result_total", "result_next", "result_time")


def _decode_batch_envelope(
    raw: JsonValue,
    *,
    expected_keys: frozenset[str],
    strict: bool = False,
    evidence: ResponseEvidence,
) -> _BatchEnvelope:
    if not isinstance(raw, dict):
        raise ProtocolError("Batch result envelope must be an object", origin=ErrorOrigin.PROTOCOL)
    if "result_error" not in raw:
        raise ProtocolError("Batch result envelope is missing result_error", origin=ErrorOrigin.PROTOCOL)
    # A repeated member or command key holds two answers where the decoder kept one: none of them can be trusted.
    if repeated_names(raw) or any(repeated_names(raw.get(field)) for field in _COMMAND_MAPS):
        raise ProtocolError(
            "Batch result envelope repeats a member name", origin=ErrorOrigin.PROTOCOL, evidence=evidence
        )
    results = _decode_php_map(raw.get("result"), field="result")
    errors = _decode_php_map(raw["result_error"], field="result_error")
    totals = _decode_optional_php_map(raw, field="result_total")
    continuations = _decode_optional_php_map(raw, field="result_next")
    # The commands' timing is diagnostic, as each command's is (``_command_time``): a member that is not a map of
    # them is left out, and the answers are kept.
    timing = raw.get("result_time")
    times = timing if isinstance(timing, dict) else {}
    result_keys = frozenset(results)
    error_keys = frozenset(errors)
    contradictory = result_keys & error_keys
    if strict and (contradictory or not (result_keys | error_keys).issubset(expected_keys)):
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
        evidence=evidence,
        contradictory=contradictory,
        times=times,
    )


def _command_time(raw: object) -> ResponseTime | None:
    """Decode a command's ``result_time`` as a direct answer's ``time`` is: one that cannot be read is left out."""
    try:
        return _decode_response_time(raw)
    except (TypeError, ValueError, OverflowError):
        return None


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
    change of dialect or v3 safety class closes the run, so the envelopes keep the commands' input order (the first
    send may still move a command held longer than the rest of its envelope to a later one, after the others).
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


def _safe_only(commands: tuple[_Command, ...]) -> tuple[_Command, ...]:
    """Return the SAFE commands, which may be sent again whatever the failure or window left proves."""
    return tuple(command for command in commands if _is_safe(command))


def _unattributed(
    command: _Command,
    envelope: _BatchEnvelope,
    *,
    halted: bool,
    attempts: _RequestAttempts,
    policy: ExecutionPolicy,
) -> BatchOutcome | None:
    """Return the outcome of a command no single entry answers, or ``None`` when one does.

    The server's own halt proves that this send did not run a command it left without an entry. Any other
    command without one entry, missing or both a result and an error, was dispatched and may have run.
    """
    key = command.stable_key
    if key in envelope.contradictory:
        message = "Batch command is both a result and an error"
    elif key in envelope.results or key in envelope.errors:
        return None
    elif halted:
        halt = _BatchNotExecutedError(
            "physical batch command was not executed after the server halted the batch",
            request_summary=command.request.summary,
        )
        attempts.withhold(halt)
        return _shared_failure(command, halt)
    else:
        message = "Batch result map is missing a submitted command"
    error = ProtocolError(
        message, origin=ErrorOrigin.PROTOCOL, request_summary=command.request.summary, evidence=envelope.evidence
    )
    attempts.record(error, policy)
    return _shared_failure(command, error)


def _scoped(error: B24ApiError, request: Request, *, strict: bool) -> B24ApiError:
    """Return a strict envelope's protocol failure scoped to the physical batch request it answered."""
    if not (strict and isinstance(error, ProtocolError) and error.request_summary is None):
        return error
    scoped = ProtocolError(
        str(error),
        origin=error.origin,
        description=error.description,
        request_summary=request.summary,
        evidence=error.evidence,
        retryable=error.retryable,
    )
    scoped.__cause__ = error
    return scoped


def _envelope_replay(
    commands: tuple[_Command, ...], error: B24ApiError, context: ExecutionContext, *, refused_at_send: bool, halt: bool
) -> tuple[_Command, ...]:
    """Return the commands of a failed envelope that may be sent again.

    Each may be when a lone request of its safety would be retried after the failure: a SAFE one after any
    transient failure, the others only when it proves the envelope did not run. A round refused at its send, for
    too little window after its wait, carries its SAFE commands on. A fail-fast batch goes again whole or not at all.
    """
    replay = (
        _safe_only(commands)
        if refused_at_send
        else tuple(command for command in commands if _replayable(error, command, context))
    )
    return () if halt and len(replay) < len(commands) else replay


def _replayable(error: B24ApiError, command: _Command, context: ExecutionContext) -> bool:
    """Return whether a lone request of the command's safety would be retried after its envelope's failure.

    A failure that made the envelope's outcome unknown is judged by the failure that caused it.
    """
    cause = error.__cause__ if isinstance(error, AmbiguousExecutionError) else error
    return isinstance(cause, B24ApiError) and _is_retryable(
        cause, safety=command.request.replay_safety, policy=context.policy
    )


def _raise_source_error(error: Exception) -> None:
    raise error


def _batch_outcome_row_weight(outcome: BatchOutcome) -> int:
    """Return the rows an outcome holds in the row budget: its result's rows, and at least its own one."""
    return max(outcome.decoded_rows, 1) if isinstance(outcome, BatchSuccess) else 1


__all__: list[str] = []
