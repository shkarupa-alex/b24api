"""Shared offline transports and the public client factory behind the conftest fixtures (B15).

A test either scripts exact exchanges with the public ``ScriptedTransport`` or answers each request
from a function with :class:`ResponderTransport` (the ``scripted_transport`` fixture). Both reach
the client through the public constructor, ``Bitrix24(Settings(...), transport=...)``, so the tests
exercise the same wiring as an application: a transport stays with its caller, and the client owns
its rate coordinator. Transports written for a single unusual protocol scenario (blocking,
cancellation, malformed wire) stay local to their tests.
"""

from __future__ import annotations
import asyncio
import inspect
import json
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast
from urllib.parse import parse_qsl, urlsplit

from b24api import Bitrix24, ExecutionPolicy, Settings
from b24api.encoding import encode_php_query
from b24api.references import dispatch as dispatch_module
from b24api.transport import WireResponse

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping

    import pytest

    from b24api import Request, Transport
    from b24api.contracts import UnknownRequestAudit

HOST = "fixture.invalid"
_JSON = (("content-type", "application/json"),)

type Reply = object
type Responder = Callable[[Request], Reply | Awaitable[Reply]]


def json_response(payload: object, *, status: int = 200) -> WireResponse:
    """Encode ``payload`` as a compact JSON response."""
    return WireResponse(status, _JSON, json.dumps(payload, separators=(",", ":")).encode())


class ResponderTransport:
    """Answer every request by calling a responder.

    The responder sees requests in send order and may be async. A ``WireResponse`` reply is returned
    as is, an exception instance is raised (a transport failure), and anything else is sent as a 200
    JSON body. ``requests`` and ``attempt_timeouts`` record every send; ``closed`` records ``aclose``,
    which a client never calls on a transport it was given.
    """

    def __init__(self, respond: Responder, *, host: str = HOST) -> None:
        """Keep the responder; no request has been seen yet."""
        self._respond = respond
        self.host = host
        self.requests: list[Request] = []
        self.attempt_timeouts: list[float] = []
        self.closed = False

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Record the request and return, or raise, the responder's reply."""
        if attempt_timeout <= 0 or max_response_bytes < 1:
            raise ValueError("transport budgets must be positive")
        self.requests.append(request)
        self.attempt_timeouts.append(attempt_timeout)
        reply = self._respond(request)
        if inspect.isawaitable(reply):
            reply = await reply
        if isinstance(reply, BaseException):
            raise reply
        return reply if isinstance(reply, WireResponse) else json_response(reply)

    async def aclose(self) -> None:
        """Record that the owner closed the transport."""
        self.closed = True

    @property
    def parameters(self) -> list[Mapping[str, object]]:
        """Return each recorded request's parameters, in send order."""
        return [request.copy_parameters() for request in self.requests]


def replies(*items: object) -> Responder:
    """Answer the n-th request with the n-th item; a callable item is called with the request."""
    remaining = list(items)

    def respond(request: Request) -> object:
        if not remaining:
            raise AssertionError(f"unexpected request: {request.method}")
        item = remaining.pop(0)
        return item(request) if callable(item) else item

    return respond


def client_for(
    transport: Transport,
    *,
    policy: ExecutionPolicy | None = None,
    unknown_request_audit: UnknownRequestAudit | None = None,
) -> Bitrix24:
    """Build a client over ``transport`` through the public constructor."""
    settings = Settings(webhook_url=f"https://{transport.host}/rest/1/token/")
    return Bitrix24(settings, transport=transport, policy=policy, unknown_request_audit=unknown_request_audit)


type TransportFactory = Callable[[Responder], ResponderTransport]
type ClientFactory = Callable[..., Bitrix24]


def always(reply: object) -> Responder:
    """Answer every request with the same reply."""
    return lambda _request: reply


class Blocker:
    """A responder that never answers: it sets ``started``, waits to be cancelled, then sets ``cancelled``."""

    def __init__(self) -> None:
        """Neither event is set yet."""
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def __call__(self, _request: Request) -> object:
        """Block until cancelled."""
        self.started.set()
        try:
            return await asyncio.Future[object]()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class VirtualClock:
    """Coalescing clock in loop turns: a wait ends by an event within ``turns`` turns, else its timer fires.

    Installed over the batch dispatcher's clock and timed wait, it makes coalescing structural: no
    wall time is read, and ``timer_firings`` counts every wait that ended by its deadline.
    """

    def __init__(self, turns: int = 64) -> None:
        """Start at time zero with no timer fired."""
        self.now = 0.0
        self.turns = turns
        self.timer_firings = 0

    def install(self, monkeypatch: pytest.MonkeyPatch) -> VirtualClock:
        """Replace the dispatcher's clock and timed wait for the rest of the test."""
        monkeypatch.setattr(dispatch_module, "_loop_time", self.time)
        monkeypatch.setattr(dispatch_module, "_wait_first", self.wait)
        return self

    def time(self) -> float:
        """Return the virtual time."""
        return self.now

    async def wait(self, futures: tuple[asyncio.Future[object], ...], seconds: float) -> None:
        """End when a future is done within the turn bound, else advance the clock past the deadline."""
        for _turn in range(self.turns):
            if any(future.done() for future in futures):
                return
            await asyncio.sleep(0)
        self.now += seconds
        self.timer_firings += 1


# Timing: a test's verdict must follow from the order of events it scripts, never from how fast a busy
# host runs its threads, its event loop or the dispatcher's coalescing timer.

# ``BatchDispatch`` caps ``coalesce_wait`` at one second. A test asserting exact waves that is not about
# the timer sets this, so a wave closes by its size or because no more input can join it, never by a
# 20 ms timer that one descheduling of the loop outruns. A test about the timer drives a VirtualClock.
UNREACHABLE_COALESCE_WAIT = 1.0

# The operation budget of a test whose subject is that deadline firing while a source pull, a source
# close or a send stays blocked until the test releases it. Wide enough that a worker-thread hop or a
# stalled loop cannot spend it before the test reaches the blocked step; since that step never ends on
# its own, a short budget proves nothing this one does not.
DEADLINE_BUDGET = 0.2

# How late past ``DEADLINE_BUDGET`` the deadline may end the blocked step: room for a loaded host to run
# the timer, yet well below any fixed cleanup timeout a regression might use in place of the budget.
DEADLINE_SLACK = 0.3

# How long past the deadline cleanup may wait for what still blocks: the documented grace of 0.1 s, pinned
# here rather than read from ``CLEANUP_GRACE_SECONDS``, so a longer grace fails the tests that bound it.
CLEANUP_GRACE = 0.1

# Only turns a hang into a failure: how long a test waits for something it caused, an event its fake sets,
# a deadline it set, or a short stream it drains. Generous, since it costs time only when a test already
# fails: a stalled or loaded host never reaches it, and it never races a budget. Whether a deadline fired
# near its budget is checked against ``DEADLINE_SLACK`` instead.
HANG_BOUND = 30.0


def deadline_started() -> float:
    """Return the loop time a test takes just before it starts an operation on ``DEADLINE_BUDGET``."""
    return asyncio.get_running_loop().time()


def assert_fired_near_deadline(started: float) -> None:
    """Assert the operation deadline, not some fixed timeout, ended the blocked step begun at ``started``."""
    elapsed = asyncio.get_running_loop().time() - started
    if elapsed >= DEADLINE_BUDGET + CLEANUP_GRACE + DEADLINE_SLACK:
        raise AssertionError(f"the deadline fired {elapsed:.3f} s after the start, not near {DEADLINE_BUDGET} s")


async def reached(event: asyncio.Event, bound: float = HANG_BOUND) -> None:
    """Wait for a test-owned ``event``; fail the test, rather than hang it, if it is not set in ``bound``."""
    try:
        async with asyncio.timeout(bound):
            await event.wait()
    except TimeoutError:
        raise AssertionError(f"the test's event was not set within {bound} s") from None


class WorkerStarts(ThreadPoolExecutor):
    """Worker threads for a loop's default executor that set an event as each submitted call starts.

    A synchronous source is pulled and closed in worker threads; installed with
    ``loop.set_default_executor``, this lets a test wait until a given call is running instead of
    guessing how long a thread takes to start.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        """Start with no call submitted."""
        super().__init__(max_workers=4)
        self._loop = loop
        self._submitted = 0
        self._started: dict[int, asyncio.Event] = {}

    def started(self, call: int) -> asyncio.Event:
        """Return the event set once the ``call``-th submitted call (from zero) starts running."""
        return self._started.setdefault(call, asyncio.Event())

    def submit[R](self, fn: Callable[..., R], /, *args: object, **kwargs: object) -> Future[R]:
        """Submit ``fn`` and set its event once its worker thread starts running it."""
        started = self.started(self._submitted)
        self._submitted += 1

        def run() -> R:
            self._loop.call_soon_threadsafe(started.set)
            return fn(*args, **kwargs)

        return super().submit(run)


async def on_loop[T](items: Iterable[T]) -> AsyncIterator[T]:
    """Yield ``items`` on the event loop.

    A public family pulls a synchronous source in a worker thread, so when each item reaches the
    scheduler depends on OS thread scheduling, which no virtual clock sets. A test that asserts exact
    waves, wave timings or an event order shared by two runs feeds its input through this instead.
    """
    for item in items:
        yield item


async def drain_to_failure[T, E: BaseException](stream: AsyncIterator[T], failure: type[E]) -> tuple[list[T], E]:
    """Drain ``stream`` until it raises ``failure``; return the items it delivered before, and that failure."""
    items: list[T] = []
    try:
        async for item in stream:
            items.append(item)  # noqa: PERF401 - keep the items delivered before the failure
    except failure as error:
        return items, error
    raise AssertionError(f"the stream ended without {failure.__name__}")


def attached_report(error: BaseException) -> object:
    """Return the report an operation attached to its exception, or ``None``."""
    return getattr(error, "report", None)


# Offline portal answering direct and classic batch requests command by command: tests describe the
# server as one function from ``(method, flat query)`` to a REST envelope, and the portal records every
# physical request with its command count so a test can assert waves without scripting each request.

type Query = dict[str, str]
type Envelope = dict[str, object]
type Answer = Callable[[str, Query], Envelope | Awaitable[Envelope]]


def flat_query(parameters: Mapping[str, object]) -> Query:
    """Flatten parameters with the PHP bracket encoding batch commands use on the wire."""
    return dict(parse_qsl(encode_php_query(cast("Mapping[str | int, object]", parameters)), keep_blank_values=True))


@dataclass
class BatchPortal:
    """Serve envelopes from ``answer`` and record each physical request's command count.

    ``wire_turns`` is a deterministic wire latency: each physical request yields that many event-loop
    turns before it is answered, so the consumer and the producers run while it is on the wire.
    """

    answer: Answer
    host: str = HOST
    waves: list[int] = field(default_factory=list)
    methods: list[tuple[str, ...]] = field(default_factory=list)
    queries: list[Query] = field(default_factory=list)
    wire_turns: int = 0

    async def _envelope(self, method: str, query: Query) -> Envelope:
        self.queries.append(query)
        envelope = self.answer(method, query)
        if inspect.isawaitable(envelope):
            envelope = await envelope
        return envelope

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Answer one direct request or one classic physical batch."""
        del attempt_timeout, max_response_bytes
        for _turn in range(self.wire_turns):
            await asyncio.sleep(0)
        parameters = request.copy_parameters()
        if request.method != "batch":
            self.waves.append(1)
            self.methods.append((request.method,))
            body = await self._envelope(request.method, flat_query(parameters))
            return WireResponse(200, _JSON, json.dumps(body, separators=(",", ":")).encode())
        commands = parameters["cmd"]
        if not isinstance(commands, dict):
            raise TypeError("batch commands must be a mapping")
        self.waves.append(len(commands))
        result: dict[str, object] = {}
        totals: dict[str, object] = {}
        nexts: dict[str, object] = {}
        errors: dict[str, object] = {}
        methods: list[str] = []
        for key, command in commands.items():
            if not isinstance(command, str):
                raise TypeError("batch command must be a string")
            split = urlsplit(command)
            methods.append(split.path)
            envelope = await self._envelope(split.path, dict(parse_qsl(split.query, keep_blank_values=True)))
            if "error" in envelope:
                errors[key] = {"error": envelope["error"], "error_description": envelope.get("error_description")}
                continue
            result[key] = envelope["result"]
            # A classic batch carries a total or a next only as an integer entry; null means absent.
            if envelope.get("total") is not None:
                totals[key] = envelope["total"]
            if envelope.get("next") is not None:
                nexts[key] = envelope["next"]
        self.methods.append(tuple(methods))
        body = {
            "result": {
                "result": result,
                "result_error": errors,
                "result_total": totals,
                "result_next": nexts,
            }
        }
        return WireResponse(200, _JSON, json.dumps(body, separators=(",", ":")).encode())

    async def aclose(self) -> None:
        """Nothing to release."""
