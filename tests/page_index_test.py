"""One-based wire page indexes advance independently of selected row count."""

from __future__ import annotations
import json

import pytest

from b24api import Bitrix24, OffsetSpec, PageIndex, ParameterPath, Request, RouteKind
from b24api.errors import CapabilityError
from b24api.execution import Executor, WireResponse


class PageTransport:
    """Independent oracle with 10, 3, and 0 decoded rows."""

    def __init__(self) -> None:
        """Track exact wire page controls."""
        self.pages: list[int] = []

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Return the independently defined page for this index."""
        assert attempt_timeout > 0
        assert max_response_bytes > 0
        page = request.copy_parameters()["page"]
        assert isinstance(page, int)
        self.pages.append(page)
        rows = [{"id": index} for index in range((page - 1) * 10, (page - 1) * 10 + {1: 10, 2: 3}.get(page, 0))]
        return WireResponse(200, (), json.dumps({"result": rows}).encode())


@pytest.mark.asyncio
async def test_one_based_page_index_uses_wire_increment_not_row_count() -> None:
    transport = PageTransport()
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    path = ParameterPath(("page",))
    index = PageIndex(path, initial=1, increment=1, max_rows=10)
    stream = client.iter_list(
        Request("example.list", route=RouteKind.BARE),
        page_size=10,
        offset=OffsetSpec(parameter_path=path, page_index=index),
    )
    rows = [row async for row in stream]
    assert transport.pages == [1, 2, 3]
    assert [row["id"] for row in rows] == list(range(13))
    assert stream.report is not None
    assert stream.report.exhausted


def test_page_index_rejects_incompatible_limit_and_counted_tail() -> None:
    path = ParameterPath(("page",))
    index = PageIndex(path, max_rows=10)
    transport = PageTransport()
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    request = Request("example.list", route=RouteKind.BARE)
    with pytest.raises(ValueError, match="max_rows"):
        client.iter_list(request, page_size=50, offset=OffsetSpec(parameter_path=path, page_index=index))
    with pytest.raises(ValueError, match="page_index"):
        client.iter_list_counted(request, page_size=10, offset=OffsetSpec(parameter_path=path, page_index=index))
    assert transport.pages == []


@pytest.mark.asyncio
async def test_page_index_zero_is_rejected_before_dispatch() -> None:
    path = ParameterPath(("page",))
    transport = PageTransport()
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    stream = client.iter_list(
        Request("example.list", {"page": 0}, route=RouteKind.BARE),
        page_size=10,
        offset=OffsetSpec(parameter_path=path, page_index=PageIndex(path, max_rows=10)),
    )
    with pytest.raises(CapabilityError, match="admitted range"):
        _ = [row async for row in stream]
    assert transport.pages == []
