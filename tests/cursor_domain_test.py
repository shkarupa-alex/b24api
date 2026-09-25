"""Exclusive positive cursor controls must not silently restart at the head."""

from __future__ import annotations
from typing import TYPE_CHECKING

import pytest

from b24api import (
    CapabilityError,
    CursorSpec,
    IdentityCoercion,
    IncompleteTraversalError,
    ParameterPath,
    Request,
    ResultSelector,
    RouteKind,
)
from b24api.contracts import CursorDomain
from tests.scripting import ResponderTransport

if TYPE_CHECKING:
    from tests.scripting import ClientFactory


def _messages(ids: tuple[int, ...] = (3, 2, 1)) -> ResponderTransport:
    """LAST_ID strictly excludes IDs at or above a positive control; pages hold two rows."""

    def respond(request: Request) -> object:
        control = request.copy_parameters().get("LAST_ID", 10)
        assert isinstance(control, int)
        return {"result": [{"id": value} for value in ids if value < control][:2]}

    return ResponderTransport(respond)


def _controls(transport: ResponderTransport) -> list[object]:
    return [parameters.get("LAST_ID", 10) for parameters in transport.parameters]


def _cursor() -> CursorSpec:
    return CursorSpec(
        ParameterPath(("LAST_ID",)),
        ("id",),
        IdentityCoercion.EXACT_INTEGER,
        "descending",
        "last",
        domain=CursorDomain.EXCLUSIVE_POSITIVE_INTEGER,
    )


@pytest.mark.asyncio
async def test_exclusive_cursor_allows_nonexistent_positive_boundary_and_one_empty(
    scripted_client: ClientFactory,
) -> None:
    transport = _messages()
    client = scripted_client(transport)
    stream = client.iter_list_cursor(
        Request("messages.get", {"LAST_ID": 10}, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=2,
    )
    assert [row["id"] async for row in stream] == [3, 2, 1]
    assert _controls(transport) == [10, 2, 1]
    assert stream.report is not None
    assert stream.report.exhausted


@pytest.mark.asyncio
async def test_exclusive_boundary_one_exhausts_the_range_below_one(scripted_client: ClientFactory) -> None:
    transport = _messages()
    client = scripted_client(transport)
    stream = client.iter_list_cursor(
        Request("messages.get", {"LAST_ID": 1}, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=2,
    )
    assert [row async for row in stream] == []
    assert _controls(transport) == [1]
    assert stream.report is not None
    assert stream.report.exhausted


@pytest.mark.asyncio
async def test_server_emitted_zero_cursor_is_incomplete(scripted_client: ClientFactory) -> None:
    transport = _messages((0,))
    client = scripted_client(transport)
    stream = client.iter_list_cursor(
        Request("messages.get", {"LAST_ID": 1}, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=2,
    )
    with pytest.raises(IncompleteTraversalError):
        _ = [row async for row in stream]
    assert _controls(transport) == [1]


@pytest.mark.asyncio
@pytest.mark.parametrize("ignored", [0, None, -1, "bad"])
async def test_ignored_or_invalid_initial_cursor_rejects_before_io(
    ignored: object, scripted_client: ClientFactory
) -> None:
    transport = _messages()
    client = scripted_client(transport)
    stream = client.iter_list_cursor(
        Request("messages.get", {"LAST_ID": ignored}, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=2,
    )
    with pytest.raises(CapabilityError, match="exclusive range cursor"):
        _ = [row async for row in stream]
    assert _controls(transport) == []
