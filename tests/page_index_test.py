"""One-based wire page indexes advance independently of selected row count."""

from __future__ import annotations
from typing import TYPE_CHECKING

import pytest

from b24api import OffsetSpec, ParameterPath, Request, RouteKind
from b24api.contracts import PageIndex
from b24api.errors import CapabilityError

if TYPE_CHECKING:
    from tests.scripting import ClientFactory, ResponderTransport, TransportFactory


def _page(request: Request) -> object:
    """Independent oracle with 10, 3, and 0 decoded rows."""
    page = request.copy_parameters()["page"]
    assert isinstance(page, int)
    return {"result": [{"id": index} for index in range((page - 1) * 10, (page - 1) * 10 + {1: 10, 2: 3}.get(page, 0))]}


def _pages(transport: ResponderTransport) -> list[object]:
    return [parameters["page"] for parameters in transport.parameters]


@pytest.mark.asyncio
async def test_one_based_page_index_uses_wire_increment_not_row_count(
    scripted_transport: TransportFactory, scripted_client: ClientFactory
) -> None:
    transport = scripted_transport(_page)
    client = scripted_client(transport)
    path = ParameterPath(("page",))
    index = PageIndex(path, initial=1, increment=1, max_rows=10)
    stream = client.iter_list(
        Request("example.list", route=RouteKind.BARE),
        page_size=10,
        offset=OffsetSpec(parameter_path=path, page_index=index),
    )
    rows = [row async for row in stream]
    assert _pages(transport) == [1, 2, 3]
    assert [row["id"] for row in rows] == list(range(13))
    assert stream.report is not None
    assert stream.report.exhausted


def test_page_index_rejects_a_zero_based_offset_spelling() -> None:
    path = ParameterPath(("start",))
    with pytest.raises(ValueError, match="initial"):
        PageIndex(path, initial=0, increment=50, max_rows=50)


@pytest.mark.asyncio
async def test_page_index_rejects_incompatible_limit_and_counted_tail(
    scripted_transport: TransportFactory, scripted_client: ClientFactory
) -> None:
    path = ParameterPath(("page",))
    index = PageIndex(path, max_rows=10)
    transport = scripted_transport(_page)
    client = scripted_client(transport)
    request = Request("example.list", route=RouteKind.BARE)
    with pytest.raises(ValueError, match="max_rows"):
        client.iter_list(request, page_size=50, offset=OffsetSpec(parameter_path=path, page_index=index))
    with pytest.raises(ValueError, match="page_index"):
        client.iter_list_counted(request, page_size=10, offset=OffsetSpec(parameter_path=path, page_index=index))
    assert _pages(transport) == []


@pytest.mark.asyncio
async def test_page_index_zero_is_rejected_before_dispatch(
    scripted_transport: TransportFactory, scripted_client: ClientFactory
) -> None:
    path = ParameterPath(("page",))
    transport = scripted_transport(_page)
    client = scripted_client(transport)
    stream = client.iter_list(
        Request("example.list", {"page": 0}, route=RouteKind.BARE),
        page_size=10,
        offset=OffsetSpec(parameter_path=path, page_index=PageIndex(path, max_rows=10)),
    )
    with pytest.raises(CapabilityError, match="admitted range"):
        _ = [row async for row in stream]
    assert _pages(transport) == []
