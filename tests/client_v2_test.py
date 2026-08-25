"""Characterization of the thin v2 facade and public stream lifecycle."""

from __future__ import annotations
import json
from typing import TYPE_CHECKING

import pytest

from b24api.client import Bitrix24
from b24api.contracts import (
    CursorSpec,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    ParameterPath,
    ReplaySafety,
    Request,
    ResultCollectionShape,
    ResultSelector,
    TerminalState,
    TraversalAssurance,
)
from b24api.execution import Executor, WireResponse
from b24api.settings import Settings

if TYPE_CHECKING:
    from collections.abc import Callable

HTTP_OK = 200


class FunctionTransport:
    """Return deterministic Bitrix-shaped envelopes."""

    host = "test.invalid"

    def __init__(self, handler: Callable[[Request], object]) -> None:
        """Store the response callback and observations."""
        self.handler = handler
        self.requests: list[Request] = []
        self.closed = False

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Return one encoded response within the supplied ceilings."""
        assert attempt_timeout > 0
        self.requests.append(request)
        body = json.dumps(self.handler(request), separators=(",", ":")).encode()
        assert len(body) <= max_response_bytes
        return WireResponse(HTTP_OK, (("content-type", "application/json"),), body)

    async def aclose(self) -> None:
        """Record caller/owner lifecycle tests."""
        self.closed = True


def _client(transport: FunctionTransport) -> Bitrix24:
    return Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam


def _identity() -> IdentitySpec:
    return IdentitySpec(
        item_path=("ID",),
        filter_key="ID",
        order_key="ID",
        coercion=IdentityCoercion.EXACT_INTEGER,
    )


@pytest.mark.asyncio
async def test_call_and_call_response_have_stable_detached_types() -> None:
    transport = FunctionTransport(lambda _request: {"result": {"items": [1, 2]}})
    client = _client(transport)

    decoded = await client.call(Request("test.get", replay_safety=ReplaySafety.SAFE))
    response = await client.call_response(Request("test.get", replay_safety=ReplaySafety.SAFE))

    assert decoded == {"items": [1, 2]}
    assert response.result == decoded
    assert response.result is not decoded


def test_injected_transport_host_must_match_settings_before_io() -> None:
    transport = FunctionTransport(lambda _request: {"result": None})
    settings = Settings(webhook_url="https://other.invalid/rest/1/token/")

    with pytest.raises(ValueError, match="host does not match"):
        Bitrix24(settings, transport=transport)

    assert transport.requests == []


@pytest.mark.asyncio
async def test_iter_list_is_sequential_mechanics_only_and_report_is_post_cleanup() -> None:
    pages = {0: [{"ID": 1}, {"ID": 2}], 2: [{"ID": 3}], 3: []}

    def handler(request: Request) -> object:
        start = request.copy_parameters().get("start", 0)
        assert isinstance(start, int)
        assert not isinstance(start, bool)
        return {"result": pages[start]}

    transport = FunctionTransport(handler)
    stream = _client(transport).iter_list(Request("test.list", replay_safety=ReplaySafety.SAFE))

    assert stream.report is None
    rows = [item async for item in stream]

    assert rows == [{"ID": 1}, {"ID": 2}, {"ID": 3}]
    assert [request.copy_parameters()["start"] for request in transport.requests] == [0, 2, 3]
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.assurance is TraversalAssurance.MECHANICS_ONLY
    assert stream.report is stream.report


@pytest.mark.asyncio
async def test_mapping_values_shape_preserves_mapping_insertion_order() -> None:
    responses = [
        {"result": {"items": {"b": {"ID": 2}, "a": {"ID": 1}}}},
        {"result": {"items": {}}},
    ]
    stream = _client(FunctionTransport(lambda _request: responses.pop(0))).iter_list(
        Request("test.list", replay_safety=ReplaySafety.SAFE),
        selector=ResultSelector(("items",)),
        collection_shape=ResultCollectionShape.MAPPING_VALUES,
    )

    assert [item async for item in stream] == [{"ID": 2}, {"ID": 1}]


@pytest.mark.asyncio
async def test_keyset_and_cursor_are_explicit_strict_alternatives() -> None:
    def keyset_handler(request: Request) -> object:
        parameters = request.copy_parameters()
        filters = parameters.get("filter", {})
        assert isinstance(filters, dict)
        boundary = filters.get(">ID", 0)
        assert isinstance(boundary, int)
        assert not isinstance(boundary, bool)
        rows = [item for item in ({"ID": 1}, {"ID": 2}, {"ID": 3}) if item["ID"] > boundary][:2]
        return {"result": rows}

    keyset_transport = FunctionTransport(keyset_handler)
    keyset = _client(keyset_transport).iter_list_keyset(
        Request("test.list", replay_safety=ReplaySafety.SAFE),
        selector=ResultSelector.root(),
        identity=_identity(),
        keyset=KeysetSpec(),
    )
    assert [item async for item in keyset] == [{"ID": 1}, {"ID": 2}, {"ID": 3}]

    def cursor_handler(request: Request) -> object:
        boundary = request.copy_parameters().get("LAST_ID", 0)
        assert isinstance(boundary, int)
        assert not isinstance(boundary, bool)
        rows = [item for item in ({"ID": 1}, {"ID": 2}) if item["ID"] > boundary][:1]
        return {"result": rows}

    cursor = _client(FunctionTransport(cursor_handler)).iter_list_cursor(
        Request("im.list", replay_safety=ReplaySafety.SAFE),
        selector=ResultSelector.root(),
        cursor=CursorSpec(
            ParameterPath(("LAST_ID",)),
            ("ID",),
            IdentityCoercion.EXACT_INTEGER,
            "ascending",
            "last",
        ),
    )
    assert [item async for item in cursor] == [{"ID": 1}, {"ID": 2}]


@pytest.mark.asyncio
async def test_partial_helper_closes_without_claiming_completion() -> None:
    transport = FunctionTransport(lambda _request: {"result": [{"ID": 1}, {"ID": 2}]})
    stream = _client(transport).iter_list(Request("test.list", replay_safety=ReplaySafety.SAFE))

    partial = await stream.first()

    assert partial.value == ({"ID": 1},)
    assert partial.report.state is TerminalState.EARLY_CLOSED
    assert stream.report is partial.report
    with pytest.raises(RuntimeError, match="closed before exhaustion"):
        await anext(stream)


@pytest.mark.asyncio
async def test_client_closes_active_stream_before_owned_transport_but_not_injected_transport() -> None:
    transport = FunctionTransport(lambda _request: {"result": [{"ID": 1}]})
    settings = Settings(webhook_url="https://test.invalid/rest/1/token/")
    client = Bitrix24(settings, transport=transport)
    stream = client.iter_list(Request("test.list", replay_safety=ReplaySafety.SAFE))

    await client.aclose()

    assert stream.report is not None
    assert stream.report.state is TerminalState.EARLY_CLOSED
    assert not transport.closed
    with pytest.raises(RuntimeError, match="client is closed"):
        await client.call(Request("test.get"))
