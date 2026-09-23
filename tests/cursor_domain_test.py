"""Exclusive positive cursor controls must not silently restart at the head."""

from __future__ import annotations
import json

import pytest

from b24api import (
    Bitrix24,
    CapabilityError,
    CursorDomain,
    CursorSpec,
    IdentityCoercion,
    IncompleteTraversalError,
    ParameterPath,
    Request,
    ResultSelector,
    RouteKind,
)
from b24api.execution import Executor, WireResponse


class MessageTransport:
    """LAST_ID strictly excludes IDs at or above a positive control."""

    host = "fixture.invalid"

    def __init__(self, ids: tuple[int, ...] = (3, 2, 1)) -> None:
        """Retain all physical cursor controls."""
        self.controls: list[int] = []
        self.ids = ids

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Return descending rows below the supplied exclusive boundary."""
        assert attempt_timeout > 0
        assert max_response_bytes > 0
        parameters = request.copy_parameters()
        control = parameters.get("LAST_ID", 10)
        assert isinstance(control, int)
        self.controls.append(control)
        rows = [{"id": value} for value in self.ids if value < control]
        return WireResponse(200, (), json.dumps({"result": rows[:2]}).encode())


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
async def test_exclusive_cursor_allows_nonexistent_positive_boundary_and_one_empty() -> None:
    transport = MessageTransport()
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    stream = client.iter_list_cursor(
        Request("messages.get", {"LAST_ID": 10}, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=2,
    )
    assert [row["id"] async for row in stream] == [3, 2, 1]
    assert transport.controls == [10, 2, 1]
    assert stream.report is not None
    assert stream.report.exhausted


@pytest.mark.asyncio
async def test_exclusive_boundary_one_exhausts_the_range_below_one() -> None:
    transport = MessageTransport()
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    stream = client.iter_list_cursor(
        Request("messages.get", {"LAST_ID": 1}, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=2,
    )
    assert [row async for row in stream] == []
    assert transport.controls == [1]
    assert stream.report is not None
    assert stream.report.exhausted


@pytest.mark.asyncio
async def test_server_emitted_zero_cursor_is_incomplete() -> None:
    transport = MessageTransport((0,))
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    stream = client.iter_list_cursor(
        Request("messages.get", {"LAST_ID": 1}, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=2,
    )
    with pytest.raises(IncompleteTraversalError):
        _ = [row async for row in stream]
    assert transport.controls == [1]


@pytest.mark.asyncio
@pytest.mark.parametrize("ignored", [0, None, -1, "bad"])
async def test_ignored_or_invalid_initial_cursor_rejects_before_io(ignored: object) -> None:
    transport = MessageTransport()
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    stream = client.iter_list_cursor(
        Request("messages.get", {"LAST_ID": ignored}, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=2,
    )
    with pytest.raises(CapabilityError, match="exclusive range cursor"):
        _ = [row async for row in stream]
    assert transport.controls == []
