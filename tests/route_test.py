"""Exact route construction and preservation across public request values."""

import httpx
import pytest

from b24api import BodyEncoding, Request, RouteKind
from b24api.transport import HttpxTransport, WireRequest


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
