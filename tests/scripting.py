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
from typing import TYPE_CHECKING

from b24api import Bitrix24, ExecutionPolicy, Settings
from b24api.transport import WireResponse

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

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
