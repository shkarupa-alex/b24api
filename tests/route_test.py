"""Exact route construction and preservation across public request values."""

import httpx
import pytest

from b24api import BodyEncoding, Request, RouteKind
from b24api.errors import CapabilityError
from b24api.execution import Executor, WireResponse
from b24api.transport import HttpxTransport, TransportCapabilities, WireRequest


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("route", "expected"),
    [
        (RouteKind.BARE, "/rest/1/token/im.v2.Chat.Message.CommentInfo.list"),
        (RouteKind.JSON, "/rest/1/token/im.v2.Chat.Message.CommentInfo.list.json"),
        (RouteKind.API_V3, "/rest/api/1/token/im.v2.Chat.Message.CommentInfo.list"),
    ],
)
async def test_route_resolves_at_transport_and_survives_request_copy(route: RouteKind, expected: str) -> None:
    paths: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(200, json={"result": True})

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    transport = HttpxTransport("https://portal.invalid/rest/1/token/", client=client)
    try:
        original = Request("im.v2.Chat.Message.CommentInfo.list", route=route)
        copied = original.with_parameters({"ID": 1})
        wire = WireRequest(copied)
        assert copied.route is route
        assert wire.route is route
        assert wire.summary.route is route
        await transport.send_wire(wire, attempt_timeout=1, max_response_bytes=1024)
        assert paths == [expected]
    finally:
        await transport.aclose()
        await client.aclose()


def test_json_route_rejects_double_suffix_and_v3_form() -> None:
    with pytest.raises(TypeError, match="route"):
        Request("profile")  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="suffix"):
        Request("crm.item.list.json", route=RouteKind.JSON)
    with pytest.raises(ValueError, match="JSON"):
        Request("tasks.task.list", route=RouteKind.API_V3, encoding=BodyEncoding.FORM_URLENCODED)


def test_transport_rejects_nonclassic_webhook_base() -> None:
    with pytest.raises(ValueError, match="classic"):
        HttpxTransport("https://portal.invalid/rest/api/1/token/")


class _SendOnlyTransport:
    """A 2.2.0-style transport that builds the classic URL whatever route it is given."""

    host = "fixture.invalid"

    def __init__(self) -> None:
        self.calls = 0

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del request, attempt_timeout, max_response_bytes
        self.calls += 1
        return WireResponse(200, (), b'{"result":{"handler":"classic"}}')


class _BareWireTransport(_SendOnlyTransport):
    """A wire transport that declares only the default bare route."""

    capabilities = TransportCapabilities()

    async def send_wire(self, request: WireRequest, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del request, attempt_timeout, max_response_bytes
        self.calls += 1
        return WireResponse(200, (), b'{"result":{"handler":"classic"}}')


@pytest.mark.asyncio
@pytest.mark.parametrize("transport_type", [_SendOnlyTransport, _BareWireTransport])
@pytest.mark.parametrize("route", [RouteKind.JSON, RouteKind.API_V3])
async def test_undeclared_route_is_refused_before_the_transport_is_called(
    transport_type: type[_SendOnlyTransport],
    route: RouteKind,
) -> None:
    transport = transport_type()
    executor = Executor(transport)
    with pytest.raises(CapabilityError, match=f"does not build the {route.value} route"):
        await executor.execute(Request("tasks.task.list", route=route))
    assert transport.calls == 0
    response = await executor.execute(Request("tasks.task.list", route=RouteKind.BARE))
    assert response.result == {"handler": "classic"}
    assert transport.calls == 1


def test_transport_capabilities_default_to_the_bare_route_and_validate_routes() -> None:
    assert TransportCapabilities().routes == frozenset({RouteKind.BARE})
    assert HttpxTransport.capabilities.routes == frozenset(RouteKind)
    with pytest.raises(TypeError, match="routes"):
        TransportCapabilities(routes=frozenset())
    with pytest.raises(TypeError, match="routes"):
        TransportCapabilities(routes=frozenset({"api_v3"}))  # type: ignore[arg-type]
