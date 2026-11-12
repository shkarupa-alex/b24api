"""Transport lifecycle, replay-aware retries, and shared rate coordination."""

from __future__ import annotations
import asyncio
import dataclasses
import hashlib
import json
import math
import random
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Literal

from b24api._error_types import FailurePhase
from b24api.contracts.evidence import ResponseEvidence
from b24api.contracts.json import _json_type_name
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
from b24api.execution.context import CancellationMark, ExecutionContext, mark_cancellation
from b24api.execution.facts import (
    ambiguous_status,
    answered_conclusively,
    http_success,
    possible_execution,
    record_dispatch,
    record_withheld,
)
from b24api.execution.rate import DeadlineBudget, RateCoordinator, WorkClass
from b24api.execution.throttle import _retry_after_seconds, _retry_delay, _throttle_reason
from b24api.redaction import DEFAULT_REDACTOR, Redactor
from b24api.transport.base import (
    WireRequest,
    WireTransport,
    preflight_transport,
    preflight_wire,
    transport_supports_json_array_body,
)
from b24api.transport.disposition import disposition_filename
from b24api.transport.protocol import ProtocolCodec

if TYPE_CHECKING:
    from b24api.contracts.request_summary import RequestSummary
    from b24api.execution.rate import _Permit
    from b24api.transport.base import Transport, WireResponse

type Clock = Callable[[], float]
type Sleeper = Callable[[float], Awaitable[None]]

_HTTP_STATUS_MINIMUM = 100
_HTTP_STATUS_MAXIMUM = 599
# Why a send is refused before it queues: a hold the coordinator already knows of outlasts what it may wait.
_HOLD_REFUSAL = "coordinator hold exceeds execution time budget"


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
            error is latest.failure or not answered_conclusively(error, policy)
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

    def record_unsent(self, error: BaseException) -> None:
        """Record a failure before a further send left: what these sends left open stays open."""
        record_dispatch(error, possibly_executed=self.left_open() is not None, unproven=self.unproven)

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
    #: Not sent again: the window cannot carry its own wait for its safety.
    REFUSE = "refuse"


@dataclass(frozen=True, slots=True)
class _RoundPlan:
    """A round's wait, and what it does with each of its sends: a retry or replay round, or a batch's first send.

    Each send is weighed against the round's one backoff and its own methods' hold, and admitted when that wait
    fits the window for its safety. With an admitted send that may not be repeated, the round waits the longest
    admitted wait that such a send can still afford (at most half the window), so each of them is sent; a SAFE
    send whose own wait is longer is carried to a later round instead of taking their replay away. With no such
    send, the round waits the longest wait among its SAFE sends. A round that carries a send always sends one.
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


def _retry_refusal(replay: _RoundPlan | None) -> str:
    """Return why the budget refused a retry: no attempt was left (no plan), or the window does not carry it."""
    return "per-request attempt budget exhausted" if replay is None else "retry delay would exceed elapsed budget"


def _plan_round(own: tuple[_Pause, ...], safeties: tuple[ReplaySafety, ...], *, whole: bool) -> _RoundPlan:
    """Plan a round from each send's own pause (see ``_RoundPlan``)."""
    first = own[0]
    if whole:
        pause = dataclasses.replace(first, held=max(each.held for each in own))
        plan = _SendPlan.SEND if all(pause.admits(safety) for safety in safeties) else _SendPlan.REFUSE
        return _RoundPlan(pause, tuple(plan for _ in own))
    fits = tuple(each.admits(safety) for each, safety in zip(own, safeties, strict=True))
    unrepeatable = any(fit and safety is not ReplaySafety.SAFE for fit, safety in zip(fits, safeties, strict=True))
    # With a send that may not be repeated, the round waits only as long as such a send can afford.
    held = max(
        (
            each.held
            for each, fit in zip(own, fits, strict=True)
            if fit and (not unrepeatable or each.admits(ReplaySafety.UNSAFE))
        ),
        default=0.0,
    )
    pause = dataclasses.replace(first, held=held)
    return _RoundPlan(
        pause,
        tuple(
            _SendPlan.REFUSE if not fit else _SendPlan.SEND if each.delay <= pause.delay else _SendPlan.CARRY
            for each, fit in zip(own, fits, strict=True)
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
    # The latest failure a send ended with: the one a budget that stops the retry it asked for reports.
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

    def open_window(self, clock: Clock) -> float:
        """Open the retry window when the first send is granted its permit, and return when it opened."""
        if self.started is None:
            self.started = clock()
        return self.started

    def admit(self, remaining: float, safety: ReplaySafety, *, now: float) -> None:
        """Refuse a send the window cannot carry: none is left, or one that may not be repeated gets less than its wait.

        A send that may not be repeated and is cut by the window turns a refusal already proven into an unknown
        outcome, so it is started only with at least as much window left as it waited since its wait was planned.
        That is never less than the planned wait once the window left is short: the plan left more than twice
        the wait, so a window left shorter than the planned wait means a wait longer than half of it, a late wake
        or a long permit queue. A SAFE send is started with any window left, since it may still succeed and a cut
        one is replayed harmlessly. The refusal is kept as ``refusal``.
        """
        if remaining <= 0:
            raise BudgetExceededError("execution time budget exhausted before dispatch")
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

    def settle(self, error: B24ApiError, policy: ExecutionPolicy) -> None:
        """Record how the last send failed; a budget that stops a retry reports the failure that asked for it."""
        if isinstance(error, BudgetExceededError):
            if error.__cause__ is None:
                error.__cause__ = self.retry_cause
            return
        settled = _proves_not_run(error, policy) or answered_conclusively(error, policy)
        self.classified(error, may_have_run=not settled)

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

    def record_unsent(self, error: BaseException) -> None:
        """Record a failure before a further send left: what the earlier sends left open stays open."""
        self.lineage.record_unsent(error)

    def withhold(self, error: BaseException) -> None:
        """Record a command the server halted before: proven not run by the last send, but not by earlier ones."""
        self.lineage.withhold(error)


@dataclass(frozen=True, slots=True)
class PhysicalAttemptOutcome:
    """What one physical send of a batch envelope produced, before any dialect classification or retry.

    ``dispatch_started`` comes from dispatch facts (an attempt was reserved and handed to the transport),
    never from the absence of a response. ``transport_proves_not_accepted`` is set only when nothing
    reached the server: the attempt failed before dispatch, or the transport proved it was not accepted.
    """

    kind: Literal["response", "error", "cancelled"]
    wire: WireResponse | None = dataclasses.field(repr=False)
    response: Response | None = dataclasses.field(repr=False)
    error: BaseException | None = dataclasses.field(repr=False)
    dispatch_started: bool
    transport_proves_not_accepted: bool


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
                attempts.record(error, context.policy)
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
            attempts.record(error, context.policy)
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
                attempts.record(error, context.policy)
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
    ) -> tuple[WireResponse, _ParsedBody | None]:
        """Run the attempt loop and return one conclusive raw response; ``shared`` counts every dispatch."""
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
                    retry_started = shared.open_window(self._clock)
                    remaining = context.remaining_time(retry_started=retry_started)
                    shared.admit(remaining, request.replay_safety, now=self._clock())
                    await context.reserve_attempt(attempts_for_request=attempts, retry_started=retry_started)
                    shared.dispatched()
                    wire = await self._send_attempt(request, wire_request, context=context, remaining=remaining)
            except asyncio.CancelledError:
                raise
            except B24ApiError as error:
                # A transport that turned a cancellation into a failure is neither retried nor reported by it.
                if cancellation.requested():
                    raise asyncio.CancelledError from error
                await self._prepare_retry(request, error, context=context, shared=shared, methods=methods)
                attempts += 1
                continue

            response_error, parsed = self._classify_response(request, wire, context=context, mode=mode)
            if response_error is None:
                # An answer that arrives after the budget is still the answer: it is returned, never discarded.
                cancellation.raise_if_requested()
                return wire, parsed
            code = response_error.normalized_code if isinstance(response_error, ApiResponseError) else None
            if code == "operation_time_limit":
                # The methods the coordinator admits: a classic batch envelope's own name is not one of them.
                for method in sorted(methods):
                    await context.coordinator.observe_api_throttle(method, code)
            await self._observe_throttle_hint(wire, response_error, context=context)
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
        out. Each failure reports the failure of the request's latest send, whichever call or replay round made it.
        """
        cause = attempts.retry_cause
        try:
            context.admit_request()
        except BudgetExceededError as error:
            raise error from cause
        if (remaining := context.remaining_time(retry_started=attempts.started)) <= 0:
            raise BudgetExceededError("execution time budget exhausted") from cause
        if context.coordinator.admission_delay(methods) >= remaining:
            raise BudgetExceededError(_HOLD_REFUSAL) from cause
        try:
            async with asyncio.timeout(remaining):
                return await context.coordinator.acquire(
                    work_class, methods=methods, budget=DeadlineBudget(self._clock() + remaining)
                )
        except TimeoutError as error:
            raise BudgetExceededError("permit wait exhausted execution time budget") from (cause or error)

    async def _send_attempt(
        self,
        request: Request,
        wire_request: WireRequest | None,
        *,
        context: ExecutionContext,
        remaining: float,
    ) -> WireResponse:
        """Send one admitted, counted attempt within what is left of the retry window."""
        try:
            async with asyncio.timeout(remaining):
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
        proven by the attempt reservation, never by the absence of a response.
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
            proven = isinstance(error, TransportError) and not error.possible_acceptance
            return _dispatched_attempt("error", error=error, proven_not_accepted=proven)
        return await self._classify_envelope(envelope, wire, context=context, cancellation=cancellation)

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
            retry_started = attempts.open_window(self._clock)
            remaining = context.remaining_time(retry_started=retry_started)
            attempts.admit(remaining, envelope.replay_safety, now=self._clock())
            await context.reserve_attempt(attempts_for_request=spent, retry_started=retry_started)
            attempts.dispatched()
            try:
                async with asyncio.timeout(remaining):
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

    async def _classify_envelope(
        self,
        envelope: WireRequest,
        wire: WireResponse,
        *,
        context: ExecutionContext,
        cancellation: CancellationMark,
    ) -> PhysicalAttemptOutcome:
        """Classify one envelope response with the shared primitives, observing any host throttle hint."""
        summary = envelope.summary
        response_error, parsed = self._classify_response(
            None, wire, context=context, mode=_BodyMode.JSON, summary=summary
        )
        if response_error is not None:
            await self._observe_throttle_hint(wire, response_error, context=context)
        if cancellation.requested():
            return _dispatched_attempt("cancelled", wire=wire, error=asyncio.CancelledError())
        if response_error is not None:
            return _dispatched_attempt("error", wire=wire, error=response_error)
        try:
            response = _decode_success(wire, parsed, request_summary=summary)
        except ProtocolError as error:
            return _dispatched_attempt("error", wire=wire, error=error)
        return _dispatched_attempt("response", wire=wire, response=response)

    def _classify_response(
        self,
        request: Request | None,
        wire: WireResponse,
        *,
        context: ExecutionContext,
        mode: _BodyMode,
        summary: RequestSummary | None = None,
    ) -> tuple[B24ApiError | None, _ParsedBody | None]:
        """Classify one conclusive response, parsing a JSON success body exactly once.

        An envelope with no canonical ``Request`` form passes ``request=None`` and its own ``summary``.
        """
        request_summary = request.summary if request is not None else summary
        success = http_success(wire.status_code)
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
        if not success and wire.status_code < _HTTP_CLIENT_ERROR_MINIMUM:  # an informational or redirect status
            return HTTPGatewayError(
                f"HTTP gateway error {wire.status_code}",
                request_summary=request_summary,
                evidence=_wire_evidence(wire),
            ), parsed
        response_error = self.codec.error_from_http(
            status_code=wire.status_code,
            body=body,
            request_summary=request_summary,
            headers=wire.header_map,
            retry_codes=context.policy.retry.transient_api_codes,
            diagnostics=diagnostic_context(request) if request is not None else None,
        )
        if response_error is None and not success:
            response_error = HTTPGatewayError(
                f"HTTP gateway error {wire.status_code}",
                request_summary=request_summary,
                evidence=_wire_evidence(wire),
            )
        return response_error, parsed

    async def _observe_throttle_hint(
        self, wire: WireResponse, error: B24ApiError, *, context: ExecutionContext
    ) -> None:
        """Pause the portal host for the cooldown a failed answer hints, if it hints one."""
        hint = _retry_after_seconds(wire)
        if hint is not None:
            cooldown = await context.coordinator.observe_throttle(hint, reason=_throttle_reason(error))
            await context.record_cooldown(cooldown)

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
        past the round that spends the last attempt meets no plan (``None``) and is stopped by that budget.
        """
        if attempts.started is None or attempts.used >= context.policy.max_attempts_per_request:
            return None
        backoff = (
            0.0
            if attempts.pause is not None
            else _retry_delay(context.policy, retry_number=attempts.used, random_source=self._random)
        )
        window = context.remaining_time(retry_started=attempts.started)
        now = self._clock()
        own = tuple(_Pause(backoff, context.coordinator.admission_delay(methods), window, now) for _, methods in sends)
        return _plan_round(own, tuple(safety for safety, _ in sends), whole=whole)

    def plan_first_send(self, context: ExecutionContext, *, methods: tuple[frozenset[str], ...]) -> _RoundPlan | None:
        """Plan which commands an envelope's first send leaves out for their own methods' hold; ``None``: none.

        An envelope is admitted under every method it carries, so one command's method hold holds back all the others.
        Each command is weighed as a replay round weighs its send (``_Pause.admits``), with no backoff: by how much
        longer its own methods are held than the host, which every command waits for alike, against what its send may
        wait for, the retry window it opens and what the operation has left after the host hold, whichever is shorter.
        A first send repeats nothing and its window opens at its permit, after the hold, so each is weighed as a send
        that may be repeated, whatever its safety. Leaving a command out is worth it only to let the others go: when
        every command can wait, or none can, the envelope goes whole to its permit, which waits within the operation's
        budget or refuses at once a hold that outlasts it, as for a lone request. Otherwise ``_plan_round`` refuses
        each command that cannot wait, the others being sent once the longest hold among them ends (the coordinator
        enforces it at the permit). A fail-fast envelope is never planned here: it goes whole to its permit.
        """
        now = self._clock()
        host = context.coordinator.admission_delay(frozenset())
        window = min(context.remaining_time(retry_started=now), context.remaining_time(retry_started=None) - host)
        pauses = tuple(_Pause(0.0, context.coordinator.admission_delay(each) - host, window, now) for each in methods)
        fits = tuple(pause.admits(ReplaySafety.SAFE) for pause in pauses)
        if all(fits) or not any(fits):
            return None
        return _plan_round(pauses, tuple(ReplaySafety.SAFE for _ in pauses), whole=False)

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
        shared.settle(error, context.policy)
        safety = request.replay_safety or ReplaySafety.UNKNOWN
        # A send that may have run is never repeated for work that is not SAFE: the request ends unknown.
        message = _possible_acceptance(error, context.policy)
        if safety is not ReplaySafety.SAFE and message is not None:
            raise possible_execution(
                error, request_summary=request.summary, declared_unsafe=safety is ReplaySafety.UNSAFE, message=message
            ) from error
        # SAFE work is retried within the budget; UNSAFE/UNKNOWN only when the failure proves nothing ran (owner
        # decision, 2026-09-25). A classic batch envelope's caller decides its replay, per command.
        retryable = _is_retryable(error, safety=safety, policy=context.policy)
        if not retryable or shared.caller_retries:
            raise error
        replay = self.plan_replay(context, attempts=shared, sends=((safety, methods),))
        if replay is None or replay.plans != (_SendPlan.SEND,):
            raise BudgetExceededError(_retry_refusal(replay)) from error
        await self._wait(context, shared, replay.pause)

    async def _wait(self, context: ExecutionContext, attempts: _RequestAttempts, pause: _Pause) -> None:
        """Record and wait out a planned pause; the coordinator's hold ends within the backoff when that is longer.

        A wait for a send already waited for, which its admission then refused, is the same retry: counted once.
        """
        if attempts.pause is None:
            await context.record_retry()
        attempts.pause = pause
        if pause.backoff > pause.held:
            await self._sleep(pause.backoff)


def _possible_acceptance(error: B24ApiError, policy: ExecutionPolicy) -> str | None:
    """Say how a failed send may have been accepted: an oversized or lost answer, or an ambiguous status."""
    if isinstance(error, ResponseTooLargeError):
        return "Request may have executed before its oversized response was rejected"
    if isinstance(error, TransportError):
        return "Request may have executed; automatic replay is forbidden" if error.possible_acceptance else None
    if (
        isinstance(error, HTTPGatewayError)
        and not isinstance(error, EnvelopeContractError)
        and ambiguous_status(error.http_status, policy)
    ):
        return "Request may have executed before the unstructured HTTP failure"
    return None


def _local_attempt_failure(error: B24ApiError) -> PhysicalAttemptOutcome:
    """An attempt that failed before dispatch: nothing reached the transport."""
    return PhysicalAttemptOutcome(
        "error", None, None, error, dispatch_started=False, transport_proves_not_accepted=True
    )


def _dispatched_attempt(
    kind: Literal["response", "error", "cancelled"],
    *,
    wire: WireResponse | None = None,
    response: Response | None = None,
    error: BaseException | None = None,
    proven_not_accepted: bool = False,
) -> PhysicalAttemptOutcome:
    return PhysicalAttemptOutcome(
        kind, wire, response, error, dispatch_started=True, transport_proves_not_accepted=proven_not_accepted
    )


def _is_retryable(error: B24ApiError, *, safety: ReplaySafety, policy: ExecutionPolicy) -> bool:
    """SAFE work is retried after any transient failure; other work only when the failure proves nothing ran."""
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
    return transient and (safety is ReplaySafety.SAFE or _proves_not_run(error, policy))


def _proves_not_run(error: B24ApiError, policy: ExecutionPolicy) -> bool:
    """Return whether a failure proves the request did not run: it was never dispatched, or Bitrix refused it."""
    if isinstance(error, TransportError):
        return not error.possible_acceptance
    if isinstance(error, ApiResponseError):
        return _refused_before_running(error, policy)
    # Only a listed refusal status (423, 425, 429 by default) proves the request was not accepted.
    return (
        isinstance(error, HTTPGatewayError)
        and not isinstance(error, EnvelopeContractError)
        and error.http_status in policy.ambiguity.refusal_http_statuses
        and error.http_status not in policy.ambiguity.ambiguous_unstructured_statuses
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
