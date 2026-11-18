"""Transport lifecycle, replay-aware retries, and shared rate coordination."""

from __future__ import annotations
import asyncio
import dataclasses
import hashlib
import json
import math
import random
import time
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Literal, NamedTuple

from b24api._error_types import FailurePhase
from b24api.contracts.evidence import ResponseEvidence
from b24api.contracts.json import _json_type_name, repeated_names
from b24api.contracts.policy import ExecutionPolicy
from b24api.contracts.request import ReplaySafety, Request, ResultErrorShape, diagnostic_context
from b24api.contracts.response import (
    BinaryEvidence,
    BinaryResponse,
    Response,
    ResponseTime,
)
from b24api.errors import (
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
from b24api.execution.boundary import send_transport, send_wire_envelope
from b24api.execution.context import ExecutionContext, mark_cancellation
from b24api.execution.facts import (
    Execution,
    Verdict,
    http_success,
    possible_execution,
    record_dispatch,
    record_withheld,
    send_verdict,
)
from b24api.execution.rate import DeadlineBudget, RateCoordinator, WorkClass
from b24api.execution.throttle import (
    Limit,
    _retry_after_seconds,
    _retry_delay,
    _throttle_reason,
    backoff_delay,
    hints_load,
    limit_of,
    operating_reset_delay,
)
from b24api.redaction import DEFAULT_REDACTOR, Redactor
from b24api.transport.base import (
    TransportCapabilities,
    WireRequest,
    WireTransport,
    preflight_transport,
    preflight_wire,
    transport_supports_json_array_body,
)
from b24api.transport.disposition import disposition_filename
from b24api.transport.protocol import ProtocolCodec, decode_json

if TYPE_CHECKING:
    from b24api.contracts.request_summary import RequestSummary
    from b24api.execution.rate import _Permit
    from b24api.transport.base import Transport, WireResponse

type Clock = Callable[[], float]
type Sleeper = Callable[[float], Awaitable[None]]

_HTTP_STATUS_MINIMUM = 100
_HTTP_STATUS_MAXIMUM = 599
# Why a send is refused before it queues: a hold the coordinator already knows of outlasts what it may wait. The
# send is refused while time is left, so this refuses that request alone, as a retry delay too long for the window.
_HOLD_REFUSAL = "coordinator hold exceeds execution time budget"
# The work the deadline refuses (``ExecutionContext.time_left``): a send's permit, the wait for it, or its dispatch.
_PERMIT_REFUSAL = "execution time budget exhausted"
_PERMIT_WAIT_REFUSAL = "permit wait exhausted execution time budget"
_DISPATCH_REFUSAL = "execution time budget exhausted before dispatch"
_RETRY_REFUSAL = "execution time budget exhausted before the retry"


def _permit_window(context: ExecutionContext, attempts: _RequestAttempts, methods: frozenset[str]) -> float:
    """Return the time a send may wait for its permit, refusing it when the budget cannot carry the wait."""
    context.admit_request()
    remaining = context.time_left(_PERMIT_REFUSAL, retry_started=attempts.started)
    if context.coordinator.admission_delay(methods) >= remaining:
        raise BudgetExceededError(_HOLD_REFUSAL)
    return remaining


class _BodyMode(Enum):
    """Select how a conclusive success body is interpreted."""

    BINARY = "binary"
    JSON = "json"


@dataclass(frozen=True, slots=True)
class _ParsedBody:
    """Carry the result of the one strict parse of a JSON success body."""

    payload: object
    failure: ValueError | None


class _Settled(Enum):
    """A latest send that leaves nothing open about whether the command ran."""

    #: Nothing was sent yet.
    NOTHING_SENT = "nothing_sent"
    #: Answered conclusively, or proven refused.
    ANSWERED = "answered"


@dataclass(frozen=True, slots=True)
class _PossiblyRun:
    """A send that may have run; ``failure`` explains it once its answer was classified as proving nothing."""

    failure: B24ApiError | None = None


@dataclass(frozen=True, slots=True)
class _Lineage:
    """What a command's sends leave open about whether it ran: the latest send, and whether one before it may have.

    A send may have run from its dispatch until its answer is classified; a send before the latest that may have
    run stays so, because no later answer proves anything about it.
    """

    latest: _Settled | _PossiblyRun = _Settled.NOTHING_SENT
    earlier: _PossiblyRun | None = None

    def __post_init__(self) -> None:
        """Refuse an earlier send before any send."""
        if self.latest is _Settled.NOTHING_SENT and self.earlier is not None:
            raise ValueError("a lineage with nothing sent has no earlier send")

    @property
    def unproven(self) -> B24ApiError | None:
        """Return the failure of the latest send that may have run and whose failure is known."""
        if isinstance(self.latest, _PossiblyRun) and self.latest.failure is not None:
            return self.latest.failure
        return None if self.earlier is None else self.earlier.failure

    def dispatched(self) -> _Lineage:
        """Return the lineage after one more send, which may have run until its answer is classified."""
        earlier = _PossiblyRun(self.unproven) if isinstance(self.latest, _PossiblyRun) else self.earlier
        return _Lineage(_PossiblyRun(), earlier)

    def classified(self, failure: B24ApiError | None, *, may_have_run: bool) -> _Lineage:
        """Return the lineage once the latest send's answer proved nothing (``failure`` explains it) or settled it."""
        if self.latest is _Settled.NOTHING_SENT:
            raise RuntimeError("a lineage with nothing sent has no answer to classify")
        return _Lineage(_PossiblyRun(failure) if may_have_run else _Settled.ANSWERED, self.earlier)

    def after(self, send: _Lineage) -> _Lineage:
        """Return this lineage after one more send that ended as the lineage of that send alone (``send``) says.

        A send that never left (``send`` has nothing sent) changes nothing.
        """
        if send.latest is _Settled.NOTHING_SENT:
            return self
        return _Lineage(send.latest, self.left_open())

    def left_open(self) -> _PossiblyRun | None:
        """Return the latest of these sends that may have run, as a send after them sees it; ``None``: none may have."""
        return self.dispatched().earlier

    def explains(self, error: BaseException, policy: ExecutionPolicy) -> bool:
        """Return whether ``error`` leaves the latest send possibly run: it is what left it so, or settles nothing."""
        latest = self.latest
        return isinstance(latest, _PossiblyRun) and (
            error is latest.failure or send_verdict(error, policy) is Verdict.MAY_HAVE_RUN
        )

    def record(self, error: BaseException, policy: ExecutionPolicy) -> None:
        """Record on the failure that ended the command, or answered it, what these sends left open.

        The failure explains itself when it leaves the latest send possibly run; otherwise the latest earlier send
        that may have run explains the outcome.
        """
        explains_itself = self.explains(error, policy)
        record_dispatch(
            error,
            possibly_executed=self.earlier is not None or explains_itself,
            unproven=None if explains_itself else self.unproven,
        )

    def unsent(self) -> Execution:
        """Return what these sends leave known of a command ended without a further send: the one left-open rule.

        Before any send it was not sent; otherwise it may have run when one of them may have, else it was answered.
        """
        if self.latest is _Settled.NOTHING_SENT:
            return Execution.NOT_SENT
        return Execution.ANSWERED if self.left_open() is None else Execution.POSSIBLY_RUN

    def record_open(self, error: BaseException) -> None:
        """Record on a failure what these sends left open, as their answers were classified (``unsent``).

        The failure adds nothing to them: it stopped a further send before it left, cut the latest one before its
        answer (a cancellation), or is the failure the latest send's classification already holds. A failure
        before any send keeps no facts: it was never sent, and no recorder can say otherwise.
        """
        known = self.unsent()
        if known is not Execution.NOT_SENT:
            record_dispatch(error, possibly_executed=known is Execution.POSSIBLY_RUN, unproven=self.unproven)

    def withhold(self, error: BaseException) -> None:
        """Record a command the server halted before: proven not run by the latest send, but not by earlier ones."""
        if self.earlier is not None:
            record_dispatch(error, possibly_executed=True, unproven=self.unproven)
        else:
            record_withheld(error)


@dataclass(frozen=True, slots=True)
class _Pause:
    """The planned wait before a retry, against the retry window left when it was planned.

    The wait is the longer of the request's own backoff and how long the coordinator still holds a permit for its
    methods (a host throttle hint or a method cooldown). The coordinator enforces its hold at admission, so the
    request sleeps only when its own backoff is longer, and the hold ends within that sleep. ``planned_at`` is
    when the wait began, so the send after it can measure the wait it actually got (see ``_RequestAttempts.admit``).
    """

    backoff: float
    held: float
    window: float
    planned_at: float

    @property
    def delay(self) -> float:
        """Return how long the retry waits before its send."""
        return max(self.backoff, self.held)

    def admits(self, safety: ReplaySafety) -> bool:
        """Return whether a send of ``safety`` may follow the wait.

        The wait must fit the window. A send that may not be repeated must also keep at least as much window after
        the wait as the wait itself (see ``_RequestAttempts.admit``), strictly: time passes between the plan and
        the send, so a plan that leaves exactly the wait would be refused at the send after the whole wait. A SAFE
        send goes with any window left.
        """
        if self.delay >= self.window:
            return False
        return safety is ReplaySafety.SAFE or 2 * self.delay < self.window


class _SendPlan(Enum):
    """What the next round does with one of its sends."""

    #: Sent after the round's wait.
    SEND = "send"
    #: A SAFE send whose own wait outlasts the round's: left out of this round and planned again in the next.
    CARRY = "carry"
    #: Not sent again: the window cannot carry its own wait for its safety, the backoff with its hold.
    REFUSE = "refuse"
    #: Not sent again: its own hold alone outlasts the window for its safety, whatever the backoff. When the failure
    #: it followed is the server's answer about its load (``hints_load``), that answer says why, so the send ends
    #: with it, its code and its evidence, instead of the budget's refusal (``ends_held_out``).
    HELD_OUT = "held_out"

    @property
    def refused(self) -> bool:
        """Return whether the send is not sent again."""
        return self in {_SendPlan.REFUSE, _SendPlan.HELD_OUT}


@dataclass(frozen=True, slots=True)
class _RoundPlan:
    """A round's wait, and what it does with each of its sends: a retry or replay round, or a batch's first send.

    Each send is weighed against the round's one backoff and its own methods' hold, and admitted when that wait
    fits the window for its safety; one that does not fit is refused, held out when its hold alone does not fit
    (``_SendPlan.HELD_OUT``). A whole round is refused whole when one of its sends does not fit. The round waits as
    little as it can: as long as its admitted sends that may not be repeated need, so each of them is sent, and at
    least until one admitted send can go. Every admitted send whose own wait is no longer is sent; a longer one,
    which may always be repeated, is carried to a later round (or a first send to a later envelope) instead of
    holding the others back. A round that carries a send always sends one.
    """

    pause: _Pause
    plans: tuple[_SendPlan, ...]

    def __post_init__(self) -> None:
        """Refuse a round that carries a send but sends none: nothing would spend an attempt on its way."""
        if _SendPlan.CARRY in self.plans and _SendPlan.SEND not in self.plans:
            raise ValueError("a round that carries a send must send one")

    def sends(self, plan: _SendPlan) -> tuple[int, ...]:
        """Return the positions of the sends with ``plan``."""
        return tuple(index for index, own in enumerate(self.plans) if own is plan)


def ends_held_out(plan: _SendPlan, followed: B24ApiError) -> bool:
    """Return whether a send not sent again ends with the failure it ``followed`` rather than the budget's refusal.

    It does when its own hold alone outlasts its window (``_SendPlan.HELD_OUT``) and that failure is the server's
    answer about its load: a lone request, a classic batch command and a REST v3 group end alike.
    """
    return plan is _SendPlan.HELD_OUT and hints_load(followed)


def _retry_refusal(replay: _RoundPlan | None) -> str:
    """Return why the budget refused a retry: no attempt was left (no plan), or the window does not carry it."""
    return "per-request attempt budget exhausted" if replay is None else "retry delay would exceed elapsed budget"


def _plan_round(own: tuple[_Pause, ...], safeties: tuple[ReplaySafety, ...], *, whole: bool) -> _RoundPlan:
    """Plan a round from each send's own pause (see ``_RoundPlan``)."""
    first = own[0]
    refusals = tuple(
        _SendPlan.HELD_OUT if not dataclasses.replace(each, backoff=0.0).admits(safety) else _SendPlan.REFUSE
        for each, safety in zip(own, safeties, strict=True)
    )
    if whole:
        pause = dataclasses.replace(first, held=max(each.held for each in own))
        if all(pause.admits(safety) for safety in safeties):
            return _RoundPlan(pause, tuple(_SendPlan.SEND for _ in own))
        return _RoundPlan(pause, refusals)
    fits = tuple(each.admits(safety) for each, safety in zip(own, safeties, strict=True))
    admitted = tuple(each.held for each, fit in zip(own, fits, strict=True) if fit)
    # The longest hold of an admitted send that may not be repeated: it is sent in this round, never carried.
    needed = max(
        (
            each.held
            for each, fit, safety in zip(own, fits, safeties, strict=True)
            if fit and safety is not ReplaySafety.SAFE
        ),
        default=0.0,
    )
    held = max(needed, min(admitted, default=0.0))
    pause = dataclasses.replace(first, held=held)
    return _RoundPlan(
        pause,
        tuple(
            refusal if not fit else _SendPlan.SEND if each.delay <= pause.delay else _SendPlan.CARRY
            for each, fit, refusal in zip(own, fits, refusals, strict=True)
        ),
    )


@dataclass(slots=True)
class _RequestAttempts:
    """Attempts, retry window, wait and execution lineage one request shares across its sends.

    The retry window opens when the request's first send is granted its coordinator permit: queueing for that
    permit is bounded by the operation's elapsed budget alone, and every send, wait and permit after it spends
    the window. A batch's replay rounds and isolated sends share the attempts, the window and the wait.

    What the sends leave open about whether a command ran is that command's own: it follows only the sends the
    command was in. A REST v3 group sends every command it still resolves in each envelope, and an isolated send
    takes a copy, so ``lineage`` is each of those commands' own. A classic batch round may carry a SAFE command
    past a round, so its engine keeps each command's lineage itself and starts ``lineage`` afresh for each round
    (``start_round``): there it says what that round's send alone left open.
    """

    started: float | None = None
    used: int = 0
    lineage: _Lineage = _Lineage()
    # The latest failure a send ended with: the cause of whatever stops the next send before it leaves (``chain_stop``).
    retry_cause: B24ApiError | None = None
    # Whoever decides each send after a failure: the executor retries within one ``execute`` call, unless the
    # classic batch engine that shares these attempts decides every replay round itself and sends once per call.
    caller_retries: bool = False
    # The wait waited before the next send, which that send consumes; the first send, and a send after a failure
    # sent at once, has none.
    pause: _Pause | None = None
    # The budget that refused this classic batch round's send at its admission, for too little window after its
    # wait: the engine, which sees only the failure of its round, tells this refusal from any other stop before a
    # send by it, and plans that round's SAFE commands again. ``start_round`` clears it.
    refusal: BudgetExceededError | None = None
    # When the latest send left: a limit refusal of a send that left after the latest pause for that limit began
    # lengthens the next pause (``RateCoordinator.limit_streak``).
    sent_at: float = 0.0

    def open_window(self, clock: Clock) -> float:
        """Open the retry window when the first send is granted its permit, and return when it opened."""
        if self.started is None:
            self.started = clock()
        return self.started

    def admit(self, remaining: float, safety: ReplaySafety, *, now: float) -> None:
        """Refuse a send that may not be repeated when the window left (``remaining``, above 0) is less than its wait.

        A send that may not be repeated and is cut by the window turns a refusal already proven into an unknown
        outcome, so it is started only with at least as much window left as it waited since its wait was planned.
        That is never less than the planned wait once the window left is short: the plan left more than twice
        the wait, so a window left shorter than the planned wait means a wait longer than half of it, a late wake
        or a long permit queue. A SAFE send is started with any window left, since it may still succeed and a cut
        one is replayed harmlessly. The refusal is kept as ``refusal``.
        """
        pause = self.pause
        if safety is not ReplaySafety.SAFE and pause is not None and remaining < now - pause.planned_at:
            self.refusal = BudgetExceededError("retry window left after the wait is shorter than the wait")
            raise self.refusal

    def start_round(self) -> None:
        """Start a classic batch round, whose commands keep their own lineages: ``lineage`` and ``refusal`` cover it."""
        self.lineage = _Lineage()
        self.refusal = None

    def dispatched(self) -> None:
        """Record a send, which may have run until its failure proves otherwise; it consumes the wait before it."""
        self.lineage = self.lineage.dispatched()
        self.used += 1
        self.pause = None

    def chain_stop(self, error: B24ApiError) -> None:
        """Chain a failure that stopped a send before it left to the failure of the latest send, if one failed.

        The one rule for every stop before a send, by the budget or otherwise (a closed client), whichever single
        request, batch round or isolated send it stops: what ended the send is the outcome, and what it followed is
        its cause. The stop ran nothing, so whoever records how the request ended records what the sends before it
        left open (``record_open``), never the stop as a send that may have run. A stop raised where the request is
        not known (the attempt budget, for example) names the request of the failure it followed.
        """
        cause = self.retry_cause
        if cause is not None:
            error.__cause__ = cause
            if error.request_summary is None:
                error.request_summary = error.request = cause.request_summary

    def settle(self, error: B24ApiError, policy: ExecutionPolicy) -> Verdict:
        """Record how the last send failed, by what its failure proves (``send_verdict``), and return that."""
        verdict = send_verdict(error, policy)
        self.classified(error, may_have_run=verdict is Verdict.MAY_HAVE_RUN)
        return verdict

    def classified(self, failure: B24ApiError | None, *, may_have_run: bool) -> None:
        """Record how the last send ended: possibly run (``failure`` explains it), or answered or refused.

        A batch dialect reads its own envelopes: an abort, a malformed or a lost answer leaves the envelope
        possibly run, and an answer attributed to each command, or a proven refusal, settles it.
        """
        if failure is not None:
            self.retry_cause = failure
        self.lineage = self.lineage.classified(failure, may_have_run=may_have_run)

    def record(self, error: BaseException, policy: ExecutionPolicy) -> None:
        """Record on the failure that ended the request, or answered one of its commands, what its sends left open."""
        self.lineage.record(error, policy)

    def record_open(self, error: BaseException) -> None:
        """Record on a failure what the sends so far left open, as their answers were classified; with none, nothing."""
        self.lineage.record_open(error)

    def withhold(self, error: BaseException) -> None:
        """Record a command the server halted before: proven not run by the last send, but not by earlier ones."""
        self.lineage.withhold(error)


@dataclass(frozen=True, slots=True)
class PhysicalAttemptOutcome:
    """What one physical send of a batch envelope produced, before any dialect classification or retry.

    ``dispatch_started`` comes from dispatch facts (an attempt was reserved and handed to a transport that did
    not refuse it before it left), never from the absence of a response. What a dispatched attempt's failure
    proves is ``send_verdict``'s, read by the dialect.
    """

    kind: Literal["response", "error", "cancelled"]
    wire: WireResponse | None = dataclasses.field(repr=False)
    response: Response | None = dataclasses.field(repr=False)
    error: BaseException | None = dataclasses.field(repr=False)
    dispatch_started: bool


_HTTP_CLIENT_ERROR_MINIMUM = 400


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

    def preflight_wire(self, wire: WireRequest) -> None:
        """Validate a transport-facing representation, such as a v3 batch envelope, before reservation."""
        preflight_wire(self._wire_transport, wire)

    @property
    def supports_v3_batch(self) -> bool:
        """Return whether the transport declares native REST v3 batch envelopes."""
        return transport_supports_json_array_body(self._wire_transport)

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
        _admission_methods: frozenset[str] | None = None,
        _attempts: _RequestAttempts | None = None,
    ) -> Response:
        """Execute one canonical request."""
        if context is not None and policy is not None:
            raise ValueError("pass context or policy, not both")
        if not isinstance(request, Request):
            raise TypeError("request must be canonical Request")
        context = context or self.context(policy)
        attempts = _attempts or _RequestAttempts()
        try:
            _wire, response = await self._execute_wire_attempts(
                request,
                context=context,
                work_class=work_class,
                mode=_BodyMode.JSON,
                methods=_admission_methods or frozenset({request.method}),
                shared=attempts,
            )
            response = _decoded(response)
        except BaseException as error:
            # Each send's failure was classified where it ended; a failure that ends the request after them adds
            # nothing (a stop before a further send, or a cancellation): what they left open is the lineage's, the
            # sends of an envelope an isolated command was in included. Before any send it keeps no facts.
            attempts.record_open(error)
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
            wire, _response = await self._execute_wire_attempts(
                request,
                context=context,
                work_class=work_class,
                mode=_BodyMode.BINARY,
                methods=frozenset({request.method}),
                shared=attempts,
            )
        except BaseException as error:
            attempts.record_open(error)
            raise
        content_type = wire.content_type
        digest = hashlib.sha256(wire.body).hexdigest() if context.policy.binary_digest else None
        filename, filename_status = disposition_filename(wire.headers)
        evidence = BinaryEvidence(
            wire.status_code, content_type, wire.byte_length, digest, filename_status=filename_status
        )
        return BinaryResponse(wire.body, content_type=content_type, evidence=evidence, filename=filename)

    async def _execute_wire_attempts(  # noqa: PLR0913
        self,
        request: Request,
        *,
        context: ExecutionContext,
        work_class: WorkClass,
        mode: _BodyMode,
        methods: frozenset[str],
        shared: _RequestAttempts,
    ) -> tuple[WireResponse, Response | None]:
        """Run the attempt loop and return one conclusive response, decoded unless binary; ``shared`` counts sends."""
        self.preflight_request(request)
        wire_request = WireRequest(request) if self._wire_transport is not None else None
        cancellation = await mark_cancellation()
        await context.start()
        attempts = shared.used
        while True:
            permit = await self._permit(
                context, shared, work_class=work_class if attempts == 0 else WorkClass.RETRY, methods=methods
            )
            try:
                async with permit:
                    remaining = await self._start_send(context, shared, request.replay_safety, spent=attempts)
                    wire = await self._dispatch(
                        context,
                        shared,
                        self._send_attempt(request, wire_request, context=context, remaining=remaining),
                    )
            except asyncio.CancelledError:
                raise
            except B24ApiError as error:
                # A transport that turned a cancellation into a failure is neither retried nor reported by it.
                if cancellation.requested():
                    raise asyncio.CancelledError from error
                if shared.used == attempts:
                    # Stopped before it left, and chained where it stopped: nothing was sent to retry.
                    raise
                await self._prepare_retry(request, error, context=context, shared=shared, methods=methods)
                attempts += 1
                continue

            response_error, response = self._classify_response(request, wire, context=context, mode=mode)
            if response_error is None:
                # A classic batch envelope's own name is none of the methods it is admitted under, so its answer
                # is no answer of theirs: their commands' entries are (``observe_command``).
                self.observe_answer(context, wire, response, methods=methods & {request.method}, sent_at=shared.sent_at)
                # An answer that arrives after the budget is still the answer: it is returned, never discarded.
                cancellation.raise_if_requested()
                return wire, response
            # The methods the coordinator admits: a classic batch envelope's own name is not one of them.
            observe = self.observe_failure if request.method in methods else self.observe_envelope_failure
            await observe(context, response_error, methods=methods, sent_at=shared.sent_at, wire=wire)
            cancellation.raise_if_requested()
            # A late failure meets the ambiguity rules first; the budget stops only a retry it would ask for.
            await self._prepare_retry(request, response_error, context=context, shared=shared, methods=methods)
            attempts += 1

    async def _permit(
        self,
        context: ExecutionContext,
        attempts: _RequestAttempts,
        *,
        work_class: WorkClass,
        methods: frozenset[str],
    ) -> _Permit:
        """Queue for a coordinator permit within the budget left: the operation's until the retry window opens.

        A spent request budget, and a hold the coordinator already has on ``methods`` (a host throttle hint or a
        method cooldown) that outlasts the budget, fail at once instead of queueing for nothing or waiting the budget
        out. Every failure here stops a send before it leaves, by the budget or otherwise (a closed client): after
        a failed send, it is caused by the failure of the request's latest send, whichever call or replay round
        made it (``_RequestAttempts.chain_stop``, the one rule for single requests, batch rounds and isolated
        sends, applied here, at the admission and at the transport).
        """
        try:
            remaining = _permit_window(context, attempts, methods)
            async with context.deadline(_PERMIT_WAIT_REFUSAL, retry_started=attempts.started):
                return await context.coordinator.acquire(
                    work_class, methods=methods, budget=DeadlineBudget(self._clock() + remaining)
                )
        except B24ApiError as error:
            attempts.chain_stop(error)
            raise

    async def _start_send(
        self, context: ExecutionContext, attempts: _RequestAttempts, safety: ReplaySafety, *, spent: int
    ) -> float:
        """Admit and count the send a permit was granted for, and return the window left for it.

        Refused here, by the deadline, the retry window or the attempt budget, the send never leaves: the refusal
        is chained to the failure it followed, as at the permit.
        """
        try:
            retry_started = attempts.open_window(self._clock)
            remaining = context.time_left(_DISPATCH_REFUSAL, retry_started=retry_started)
            attempts.admit(remaining, safety, now=self._clock())
            await context.reserve_attempt(attempts_for_request=spent)
        except B24ApiError as error:
            attempts.chain_stop(error)
            raise
        attempts.sent_at = self._clock()
        return remaining

    @staticmethod
    async def _dispatch(
        context: ExecutionContext, attempts: _RequestAttempts, send: Coroutine[object, object, WireResponse]
    ) -> WireResponse:
        """Hand an admitted, counted send to its transport, and record it sent unless the transport kept it.

        A transport that refuses the send before it leaves the process, for good, raises a ``TransportError`` of
        phase ``not_dispatched`` that is not retryable, whichever transport it is: ``ClientClosedError`` from a
        closed one, a request HTTPX could not build. That is a stop before the send, as at the permit. The
        reservation is released, so it is no physical request, nothing is recorded sent, so no batch round or
        isolated send counts it, and it is chained to the failure it followed. Any other outcome, a cancellation
        and a retryable failure before dispatch (a refused connection) included, is a send.
        """
        try:
            wire = await send
        except TransportError as error:
            if error.phase is not FailurePhase.NOT_DISPATCHED or error.retryable:
                attempts.dispatched()
                raise
            context.release_attempt()
            attempts.chain_stop(error)
            raise
        except BaseException:
            attempts.dispatched()
            raise
        attempts.dispatched()
        return wire

    async def _send_attempt(
        self,
        request: Request,
        wire_request: WireRequest | None,
        *,
        context: ExecutionContext,
        remaining: float,
    ) -> WireResponse:
        """Send one admitted, counted attempt within what is left of the retry window.

        A transport that bounds its attempts itself (``TransportCapabilities.bounds_attempts``) ends this one by
        ``remaining`` and says how far it got. Any other is cut here when the window closes, which proves nothing
        about how far it got.
        """
        try:
            async with asyncio.timeout(_executor_deadline(self._wire_transport, remaining)):
                return await send_transport(
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

    async def _execute_physical_batch_attempt(
        self,
        envelope: WireRequest,
        *,
        context: ExecutionContext,
        attempts: _RequestAttempts,
        methods: frozenset[str],
    ) -> PhysicalAttemptOutcome:
        """Send one batch envelope once and report what happened, without generic retry or refusal handling.

        The caller classifies the outcome by its dialect before any retry: a structured error that would
        prove a single request refused does not prove that an envelope's commands did not run. Dispatch is
        proven by the send recorded (``_dispatch``), never by the absence of a response.
        """
        try:
            self.preflight_wire(envelope)
        except CapabilityError as error:
            return _local_attempt_failure(error)
        cancellation = await mark_cancellation()
        await context.start()
        spent = attempts.used
        try:
            wire = await self._send_envelope(envelope, context=context, attempts=attempts, methods=methods)
        except asyncio.CancelledError as error:
            if attempts.used == spent:
                raise
            return _dispatched_attempt("cancelled", error=error)
        except B24ApiError as error:
            if attempts.used == spent:
                cancellation.raise_if_requested()
                return _local_attempt_failure(error)
            if cancellation.requested():
                # The transport turned a cancellation into this failure: the envelope ends cancelled.
                return _dispatched_attempt("cancelled", error=asyncio.CancelledError())
            return _dispatched_attempt("error", error=error)
        # A refusal of the envelope as a whole cannot be attributed to one command: it holds every method the
        # envelope is admitted under.
        response_error, response = self._classify_response(
            None, wire, context=context, mode=_BodyMode.JSON, summary=envelope.summary
        )
        if response_error is not None:
            await self.observe_envelope_failure(
                context, response_error, methods=methods, sent_at=attempts.sent_at, wire=wire
            )
        return _envelope_answered(wire, response_error, response, cancelled=cancellation.requested())

    async def _send_envelope(
        self,
        envelope: WireRequest,
        *,
        context: ExecutionContext,
        attempts: _RequestAttempts,
        methods: frozenset[str],
    ) -> WireResponse:
        """Acquire a permit, reserve one attempt and send the envelope; budget failures precede dispatch."""
        wire_transport = self._wire_transport
        if wire_transport is None:  # pragma: no cover - preflight_wire refuses a send-only transport
            raise CapabilityError("send-only transport does not support wire envelopes")
        spent = attempts.used
        permit = await self._permit(
            context, attempts, work_class=WorkClass.BATCH if spent == 0 else WorkClass.RETRY, methods=methods
        )
        async with permit:
            remaining = await self._start_send(context, attempts, envelope.replay_safety, spent=spent)
            send = _send_envelope_attempt(wire_transport, envelope, context=context, remaining=remaining)
            return await self._dispatch(context, attempts, send)

    def _read_answer(
        self, request: Request | None, wire: WireResponse, *, context: ExecutionContext, summary: RequestSummary | None
    ) -> B24ApiError | Response | Mapping[str, object] | None:
        """Read a 2xx JSON answer once (``_read_success``), then the request's own embedded-error contract on it.

        The contract may find an error in the answer: the send fails as any failed send does, before its retry and
        the load limit it names are decided.
        """
        read = _read_success(wire, summary=summary)
        if request is None or not isinstance(read, Response):
            return read
        try:
            _raise_embedded_result_error(
                request,
                read.result,
                http_status=wire.status_code,
                retry_codes=context.policy.retry.transient_api_codes,
                redactor=self.codec.redactor,
            )
        except B24ApiError as error:
            return error
        return read

    def _classify_response(
        self,
        request: Request | None,
        wire: WireResponse,
        *,
        context: ExecutionContext,
        mode: _BodyMode,
        summary: RequestSummary | None = None,
    ) -> tuple[B24ApiError | None, Response | None]:
        """Classify one response whole: its failure, or the success it decodes to (``None`` for a binary one).

        A JSON success body is parsed exactly once, and its envelope decoded, before anything in it is read as an
        answer: a body that cannot be decoded is the send's failure, as an error status is. An envelope with no
        canonical ``Request`` form passes ``request=None`` and its own ``summary``.
        """
        request_summary = request.summary if request is not None else summary
        success = http_success(wire.status_code)
        if success and mode is _BodyMode.BINARY:
            return None, None
        body: bytes | Mapping[str, object] | None = wire.body
        if success:
            read = self._read_answer(request, wire, context=context, summary=request_summary)
            if isinstance(read, B24ApiError):
                return read, None
            if isinstance(read, Response):
                return None, read
            if read is not None:
                # The codec classifies the structured error from the one strict parse, not a second one.
                body = read
        content_type = (wire.content_type or "").split(";", 1)[0].strip().casefold()
        if mode is _BodyMode.BINARY and content_type != "application/json" and not content_type.endswith("+json"):
            body = None
        if not success and wire.status_code < _HTTP_CLIENT_ERROR_MINIMUM:  # an informational or redirect status
            return HTTPGatewayError(
                f"HTTP gateway error {wire.status_code}",
                request_summary=request_summary,
                evidence=_wire_evidence(wire),
            ), None
        response_error = self.codec.error_from_http(
            status_code=wire.status_code,
            body=body,
            request_summary=request_summary,
            headers=wire.header_map,
            retry_codes=context.policy.retry.transient_api_codes,
            diagnostics=diagnostic_context(request) if request is not None else None,
        )
        if response_error is None and success:  # a 2xx body the parse refused (an empty one) stays malformed
            response_error = _malformed_success(wire, None, request_summary=request_summary)
        elif response_error is None:
            response_error = HTTPGatewayError(
                f"HTTP gateway error {wire.status_code}",
                request_summary=request_summary,
                evidence=_wire_evidence(wire),
            )
        return response_error, None

    def observe_answer(
        self,
        context: ExecutionContext,
        wire: WireResponse | None,
        response: Response | None,
        *,
        methods: frozenset[str],
        sent_at: float,
    ) -> None:
        """Record what an answer that is no limit refusal says about the load: of ``methods``, its own methods.

        It ends the host's limit streak and theirs, and records when a method's operating time resets, from the
        ``time`` block of its answer.
        """
        coordinator = context.coordinator
        coordinator.observe_answer(methods, sent_at=sent_at)
        timing = None if response is None else response.time
        if timing is None or timing.operating_reset_at is None:
            return
        date = None if wire is None else wire.header_map.get("date")
        for method in methods:
            coordinator.observe_reset(method, operating_reset_delay(timing.operating_reset_at, date))

    async def observe_failure(
        self,
        context: ExecutionContext,
        error: B24ApiError,
        *,
        methods: frozenset[str],
        sent_at: float,
        wire: WireResponse | None = None,
    ) -> None:
        """Record what a failed answer of ``methods``' own request says about the load.

        A failure about the load holds what its limit holds (``_hold``). Any other failure answers the request as
        a success does: it ends the limit streaks of the host and of ``methods`` (``observe_answer``).
        """
        if hints_load(error):
            await self._hold(context, error, methods=methods, sent_at=sent_at, wire=wire)
        else:
            context.coordinator.observe_answer(methods, sent_at=sent_at)

    async def observe_envelope_failure(
        self,
        context: ExecutionContext,
        error: B24ApiError,
        *,
        methods: frozenset[str],
        sent_at: float,
        wire: WireResponse,
    ) -> None:
        """Record what a failed answer of a whole batch envelope, admitted under ``methods``, says about the load.

        A failure about the load cannot be attributed to one command: it holds every method its limit holds
        (``_hold``). Any other failure is the host's answer, not its commands': it ends the host's limit streak.
        """
        if hints_load(error):
            await self._hold(context, error, methods=methods, sent_at=sent_at, wire=wire)
        else:
            context.coordinator.observe_answer(frozenset(), sent_at=sent_at)

    async def _hold(
        self,
        context: ExecutionContext,
        error: B24ApiError,
        *,
        methods: frozenset[str],
        sent_at: float,
        wire: WireResponse | None,
    ) -> None:
        """Pause what a failure about the load holds: the host, or each of ``methods``.

        A method's operating-time refusal holds each of ``methods`` alone: for the server's hint, else until its
        operating time resets, as its latest answer said, else for the retry backoff of its limit streak. A
        request-rate refusal holds the host for the server's hint, else for the backoff of the host's limit streak,
        so the sends that share the host slow down together. Any other failure about the load (429 or 503,
        ``hints_load``) holds the host for its hint, if it has one. The host's hold is added to the operation's
        cooldown.
        """
        coordinator = context.coordinator
        limit = limit_of(error)
        hint = _retry_after_seconds(wire) if wire is not None else None
        if limit is Limit.METHOD:
            for method in sorted(methods):
                delay = hint if hint is not None else coordinator.method_reset(method)
                if delay is None:
                    delay = backoff_delay(context.policy, coordinator.limit_streak(method, sent_at=sent_at))
                coordinator.hold_method(method, delay)
            return
        if limit is Limit.HOST and hint is None:
            hint = backoff_delay(context.policy, coordinator.limit_streak(None, sent_at=sent_at))
        if hint is not None:
            cooldown = await coordinator.observe_throttle(hint, reason=_throttle_reason(error))
            await context.record_cooldown(cooldown)

    async def observe_command(
        self,
        context: ExecutionContext,
        method: str,
        answer: Response | B24ApiError | None,
        *,
        sent_at: float,
    ) -> None:
        """Record what one command's own entry says about the load: its failure, or its success (with its timing).

        An entry carries no hint of its own.
        """
        if isinstance(answer, B24ApiError):
            await self.observe_failure(context, answer, methods=frozenset({method}), sent_at=sent_at)
        else:
            self.observe_answer(context, None, answer, methods=frozenset({method}), sent_at=sent_at)

    def plan_replay(
        self,
        context: ExecutionContext,
        *,
        attempts: _RequestAttempts,
        sends: tuple[tuple[ReplaySafety, frozenset[str]], ...],
        whole: bool = False,
    ) -> _RoundPlan | None:
        """Plan the wait before the request's next round and what it does with each send; ``None``: no attempt left.

        This plans the executor's own retries and the replay rounds of a physical batch. Each send is a safety and
        the methods it is admitted under, weighed against the round's one backoff or its own methods' hold when
        longer (see ``_RoundPlan`` and ``_Pause.admits``). The holds are those the coordinator knows when planning:
        one on a method of one command does not stop another's replay, but a hold that appears during the wait
        is met at the permit. A ``whole`` round (a fail-fast batch) waits the longest wait of all its sends and is
        sent whole or not at all. A backoff already waited for a send that was then refused at its admission is
        not waited again. A carried send costs no attempt of its own: every round that carries one sends another,
        which spends the round's attempt, so the attempt budget bounds how often a send is carried; one carried
        past the round that spends the last attempt meets no plan (``None``) and is stopped by that budget. With no
        window left, the round is refused by time as any work is (``ExecutionContext.time_left``): the operation's
        deadline records its refusal and ends the operation, a retry window closed first refuses this request alone.
        A plan that refuses a send refuses it while time is left, which ends no operation.
        """
        if attempts.started is None or attempts.used >= context.policy.max_attempts_per_request:
            return None
        window = context.time_left(_RETRY_REFUSAL, retry_started=attempts.started)
        backoff = (
            0.0
            if attempts.pause is not None
            else _retry_delay(context.policy, retry_number=attempts.used, random_source=self._random)
        )
        now = self._clock()
        holds = context.coordinator.admission_delays(tuple(methods for _, methods in sends))
        own = tuple(_Pause(backoff, held, window, now) for held in holds)
        return _plan_round(own, tuple(safety for safety, _ in sends), whole=whole)

    def plan_first_send(self, context: ExecutionContext, *, methods: tuple[frozenset[str], ...]) -> _RoundPlan | None:
        """Plan which commands an envelope's first send leaves for a later envelope; ``None``: it goes whole.

        An envelope is admitted under every method it carries, so one command's method hold would hold back all the
        others. Each command is planned as a replay round plans its send (``_plan_round``), with no backoff: by how
        much longer its own methods are held than the host, which every command waits for alike. A first send
        repeats nothing, so each is weighed as a send that may be repeated, whatever its safety, and against what
        the operation has left after the host's hold: its retry window opens at its permit, after the hold. So the
        least held commands go now, a command held longer goes in a later envelope once its hold ends, and only one
        whose hold outlasts what the operation has left is refused. When every command goes now, or none can, the
        envelope goes whole to its permit, which waits within the operation's budget or refuses at once a hold that
        outlasts it, as for a lone request. A fail-fast envelope is never planned here: it goes whole to its permit.
        With no time left, it raises the deadline's refusal of the permit (``ExecutionContext.time_left``), recorded.
        """
        now = self._clock()
        host, *holds = context.coordinator.admission_delays((frozenset(), *methods))
        window = context.time_left(_PERMIT_REFUSAL) - host
        pauses = tuple(_Pause(0.0, held - host, window, now) for held in holds)
        plan = _plan_round(pauses, tuple(ReplaySafety.SAFE for _ in pauses), whole=False)
        if all(each is _SendPlan.SEND for each in plan.plans) or all(each.refused for each in plan.plans):
            return None
        return plan

    async def replay_after(self, context: ExecutionContext, *, attempts: _RequestAttempts, pause: _Pause) -> None:
        """Wait out a pause ``plan_replay`` planned before the replay round it admits."""
        await self._wait(context, attempts, pause)

    async def _prepare_retry(
        self,
        request: Request,
        error: B24ApiError,
        *,
        context: ExecutionContext,
        shared: _RequestAttempts,
        methods: frozenset[str],
    ) -> None:
        safety = self._conclude(request, error, policy=context.policy, shared=shared)
        # SAFE work is retried within the budget; UNSAFE/UNKNOWN only when the failure proves nothing ran (owner
        # decision, 2026-09-25). A classic batch envelope's caller decides its replay, per command.
        retryable = _is_retryable(error, safety=safety, policy=context.policy)
        if not retryable or shared.caller_retries:
            raise error
        try:
            replay = self.plan_replay(context, attempts=shared, sends=((safety, methods),))
        except BudgetExceededError as refusal:
            shared.chain_stop(refusal)
            raise
        if replay is not None and ends_held_out(replay.plans[0], error):
            # The server holds the request's methods or its host longer than its window can wait, whatever its own
            # backoff: the request ends with the refusal that says so, its code and its evidence.
            raise error
        if replay is None or replay.plans != (_SendPlan.SEND,):
            stop = BudgetExceededError(_retry_refusal(replay))
            shared.chain_stop(stop)
            raise stop
        await self._wait(context, shared, replay.pause)

    @staticmethod
    def _conclude(
        request: Request, error: B24ApiError, *, policy: ExecutionPolicy, shared: _RequestAttempts
    ) -> ReplaySafety:
        """Record how a send ended by what its failure proves (``send_verdict``); return the request's safety.

        The one ending of every failed send, whatever found the failure (the transport, the status, the decoder or
        the request's own embedded-error contract): a send that may have run ends a request that is not ``SAFE``
        with an ``AmbiguousExecutionError``, its failure the cause.
        """
        verdict = shared.settle(error, policy)
        safety = request.replay_safety or ReplaySafety.UNKNOWN
        if safety is not ReplaySafety.SAFE and verdict is Verdict.MAY_HAVE_RUN:
            raise possible_execution(
                error,
                request_summary=request.summary,
                declared_unsafe=safety is ReplaySafety.UNSAFE,
                message=_ambiguity_message(error),
            ) from error
        return safety

    async def _wait(self, context: ExecutionContext, attempts: _RequestAttempts, pause: _Pause) -> None:
        """Record and wait out a planned pause; the coordinator's hold ends within the backoff when that is longer.

        A wait for a send already waited for, which its admission then refused, is the same retry: counted once.
        """
        if attempts.pause is None:
            await context.record_retry()
        attempts.pause = pause
        if pause.backoff > pause.held:
            await self._sleep(pause.backoff)


def _ambiguity_message(error: B24ApiError) -> str:
    """Say how a failed send that may have run may have been accepted: by what ended it."""
    if isinstance(error, ResponseTooLargeError):
        return "Request may have executed before its oversized response was rejected"
    if isinstance(error, TransportError):
        return "Request may have executed; automatic replay is forbidden"
    if isinstance(error, HTTPGatewayError) and not isinstance(error, EnvelopeContractError):
        return "Request may have executed before the unstructured HTTP failure"
    return "Request may have executed; its answer does not prove whether it ran"


def _executor_deadline(wire_transport: WireTransport | None, remaining: float) -> float | None:
    """Return the deadline the executor sets around an attempt: none when its transport bounds its attempts itself.

    Such a transport (``TransportCapabilities.bounds_attempts``) ends the attempt by ``remaining`` and says how far
    it got. A deadline set here would fire first and prove nothing about how far it got.
    """
    capabilities = None if wire_transport is None else wire_transport.capabilities
    bounded = isinstance(capabilities, TransportCapabilities) and capabilities.bounds_attempts
    return None if bounded else remaining


async def _send_envelope_attempt(
    wire_transport: WireTransport, envelope: WireRequest, *, context: ExecutionContext, remaining: float
) -> WireResponse:
    """Send one admitted, counted envelope within what is left of the retry window, bounded as a lone attempt is."""
    try:
        async with asyncio.timeout(_executor_deadline(wire_transport, remaining)):
            return await send_wire_envelope(
                wire_transport,
                envelope,
                attempt_timeout=remaining,
                max_response_bytes=context.policy.max_response_bytes,
            )
    except TimeoutError as error:
        raise TransportError(
            "Transport attempt exceeded its elapsed budget",
            phase=FailurePhase.DISPATCH_STARTED,
            request_summary=envelope.summary,
        ) from error


def _decoded(response: Response | None) -> Response:
    """Return the decoded answer a JSON send returns."""
    if response is None:  # pragma: no cover - a JSON mode always decodes its answer
        raise RuntimeError("JSON success response lacks its decoded envelope")
    return response


def _local_attempt_failure(error: B24ApiError) -> PhysicalAttemptOutcome:
    """An attempt that failed before dispatch: nothing reached the transport."""
    return PhysicalAttemptOutcome("error", None, None, error, dispatch_started=False)


def _dispatched_attempt(
    kind: Literal["response", "error", "cancelled"],
    *,
    wire: WireResponse | None = None,
    response: Response | None = None,
    error: BaseException | None = None,
) -> PhysicalAttemptOutcome:
    return PhysicalAttemptOutcome(kind, wire, response, error, dispatch_started=True)


def _envelope_answered(
    wire: WireResponse, error: B24ApiError | None, response: Response | None, *, cancelled: bool
) -> PhysicalAttemptOutcome:
    """An envelope's answer: cancelled while it was read, its failure, or its decoded success."""
    if cancelled:
        return _dispatched_attempt("cancelled", wire=wire, error=asyncio.CancelledError())
    if error is not None:
        return _dispatched_attempt("error", wire=wire, error=error)
    return _dispatched_attempt("response", wire=wire, response=response)


def _is_retryable(
    error: B24ApiError, *, safety: ReplaySafety, policy: ExecutionPolicy, v3_envelope: bool = False
) -> bool:
    """SAFE work is retried after any transient failure; other work only when the failure proves nothing ran.

    Whether it proves that is ``send_verdict``'s, the one rule; ``v3_envelope`` reads a REST 3.0 envelope's answer.
    """
    if isinstance(error, TransportError | ApiResponseError):
        transient = error.retryable
    else:
        transient = (
            isinstance(error, HTTPGatewayError)
            and not isinstance(error, EnvelopeContractError)
            and error.http_status is not None
            and not http_success(error.http_status)
            and error.http_status in policy.retry.transient_http_statuses
        )
    return transient and (
        safety is ReplaySafety.SAFE or send_verdict(error, policy, v3_envelope=v3_envelope) is Verdict.NOT_RUN
    )


class _PathRead(NamedTuple):
    """What a path through a decoded result reaches: ``found`` and its ``value``, unless an object on it repeats."""

    found: bool
    value: object
    repeated: bool = False


def _read_path(value: object, path: tuple[str | int, ...]) -> _PathRead:
    """Follow ``path``; a repeated name the path goes through stops it, since the value it would reach is unknown."""
    current = value
    for part in path:
        if part in repeated_names(current):
            return _PathRead(found=False, value=None, repeated=True)
        if isinstance(part, str):
            if not isinstance(current, Mapping) or part not in current:
                return _PathRead(found=False, value=None)
            current = current[part]
        else:
            if not isinstance(current, list | tuple) or part >= len(current):
                return _PathRead(found=False, value=None)
            current = current[part]
    return _PathRead(found=True, value=current)


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
    # A name repeated on the way to the error or its code holds two answers where the decoder kept the last: the one
    # read, an error or none, proves nothing.
    found, selected, repeated = _read_path(result, spec.selector.path)
    if repeated:
        raise _result_protocol_error(
            "Embedded result error repeats a member name", request, observed=None, path=spec.selector.path
        )
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
        found, code, repeated = _read_path(candidate, spec.code_path)
        if not repeated and (not found or code is None or code == "" or (type(code) is int and code == 0)):
            continue
        if repeated or repeated_names(candidate):
            # Any member of an error repeated (its code, its message) leaves the error read here unreadable.
            raise _result_protocol_error(
                "Embedded result error repeats a member name", request, observed=candidate, path=candidate_path
            )
        if isinstance(code, bool) or not isinstance(code, str | int):
            raise _result_protocol_error(
                "Embedded result error code must be a string or integer",
                request,
                observed=code,
                path=(*candidate_path, *spec.code_path),
            )
        description: str | None = None
        if spec.description_path is not None:
            description_found, raw_description, description_repeated = _read_path(candidate, spec.description_path)
            if description_repeated:
                raise _result_protocol_error(
                    "Embedded result error repeats a member name",
                    request,
                    observed=candidate,
                    path=(*candidate_path, *spec.description_path),
                )
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


def _read_success(
    wire: WireResponse, *, summary: RequestSummary | None
) -> B24ApiError | Response | Mapping[str, object] | None:
    """Read a 2xx JSON body once: its failure, its decoded success, or its top-level error for the codec.

    ``None`` is a body that is not JSON at all, which the codec reads as its malformed answer.
    """
    parsed = _parse_success_body(wire.body)
    if isinstance(parsed.failure, json.JSONDecodeError):
        return None
    if parsed.failure is not None:
        # Invalid UTF-8, a non-finite number or a repeated control member is a defect only the strict parse sees; a
        # lenient second parse must not turn it into a structured API error.
        return _malformed_success(wire, parsed.failure, request_summary=summary)
    payload = parsed.payload
    if isinstance(payload, Mapping) and "error" in payload:
        return payload
    try:
        return _decode_success(wire, payload, request_summary=summary)
    except ProtocolError as error:
        return error


def _parse_success_body(body: bytes) -> _ParsedBody:
    """Run the one strict JSON parse of a success body (``decode_json``), keeping a failure for the classifier."""
    try:
        payload = decode_json(body.decode("utf-8"), allow_constants=False)
    except ValueError as error:  # JSONDecodeError and UnicodeDecodeError are ValueErrors
        return _ParsedBody(None, error)
    return _ParsedBody(payload, None)


def _malformed_success(
    wire: WireResponse, failure: ValueError | None, *, request_summary: RequestSummary | None
) -> EnvelopeContractError:
    """Return the failure of a 2xx answer whose body cannot be decoded: it proves nothing about the send."""
    error = EnvelopeContractError(
        "Malformed successful HTTP response", request_summary=request_summary, evidence=_wire_evidence(wire)
    )
    error.__cause__ = failure
    return error


def _decode_success(wire: WireResponse, payload: object, *, request_summary: RequestSummary | None) -> Response:
    """Decode the envelope of a 2xx answer without a top-level error; raise ``ProtocolError`` when it is not one.

    The server's timing (``time``) is diagnostic: one that cannot be read, a negative duration after a server clock
    step for example, is left out (``Response.time`` is ``None``) and the answer is kept. ``total`` and ``next``
    drive pagination, so a malformed one makes the answer unreadable.
    """
    evidence = _wire_evidence(wire)
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
        time: ResponseTime | None = _decode_response_time(payload.get("time"))
    except (TypeError, ValueError, OverflowError):
        time = None
    try:
        return Response(payload["result"], time=time, total=total, next=next_value, evidence=evidence)
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


__all__ = ["Executor"]
