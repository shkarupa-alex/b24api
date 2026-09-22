"""Exact PHP positional JSON slots, preflight, and transactional controls."""

from __future__ import annotations
import json

import httpx
import pytest

from b24api import (
    BodyEncoding,
    EmptyArray,
    EmptyObject,
    Null,
    Omitted,
    PositionalArguments,
    PositionalLayout,
    Present,
    Request,
    RouteKind,
    SlotContract,
    SlotShape,
)
from b24api.batch.engine import BatchExecutor
from b24api.batch.outcome import BatchFailure
from b24api.errors import CapabilityError
from b24api.execution import Executor, HttpxTransport
from b24api.transport import WireResponse


def _elapsed_layout() -> PositionalLayout:
    return PositionalLayout(
        "task.elapseditem.getlist.five.v1",
        (
            SlotContract("taskId", SlotShape.SCALAR, fixed=True),
            SlotContract("order", SlotShape.OBJECT),
            SlotContract("filter", SlotShape.OBJECT),
            SlotContract("select", SlotShape.ARRAY),
            SlotContract("params", SlotShape.OBJECT),
        ),
        control_paths=frozenset({(4, "NAV_PARAMS", "iNumPage")}),
    )


def _arguments() -> PositionalArguments:
    layout = _elapsed_layout()
    return PositionalArguments(
        (Present(42), EmptyObject(), EmptyObject(), EmptyArray(), Present({"NAV_PARAMS": {"iNumPage": 1}})),
        layout.layout_id,
        layout=layout,
    )


def test_positional_control_writer_is_atomic_and_preserves_exact_slots() -> None:
    original = _arguments()
    updated = original.write_control((4, "NAV_PARAMS", "iNumPage"), 2)
    assert original.to_wire_slots() == [42, {}, {}, [], {"NAV_PARAMS": {"iNumPage": 1}}]
    assert updated.to_wire_slots() == [42, {}, {}, [], {"NAV_PARAMS": {"iNumPage": 2}}]
    with pytest.raises(ValueError, match="not declared"):
        original.write_control((4, "nav_params", "iNumPage"), 3)
    with pytest.raises(ValueError, match="not declared"):
        original.write_control((0,), 43)
    assert original.to_wire_slots()[-1] == {"NAV_PARAMS": {"iNumPage": 1}}
    assert "42" not in repr(original)


def test_positional_rejects_wrong_arity_shape_and_internal_omission() -> None:
    layout = _elapsed_layout()
    with pytest.raises(ValueError, match="arity"):
        PositionalArguments((*_arguments().slots, Null()), layout.layout_id, layout=layout)
    with pytest.raises(ValueError, match="trailing"):
        PositionalArguments(
            (Present(42), Omitted(), EmptyObject(), EmptyArray(), Present({"NAV_PARAMS": {"iNumPage": 1}})),
            layout.layout_id,
            layout=layout,
        )
    with pytest.raises(ValueError, match="object"):
        PositionalArguments(
            (Present(42), EmptyArray(), EmptyObject(), EmptyArray(), Present({"NAV_PARAMS": {"iNumPage": 1}})),
            layout.layout_id,
            layout=layout,
        )
    with pytest.raises(ValueError, match="JSON"):
        Request(
            "task.elapseditem.getlist", _arguments(), route=RouteKind.BARE,
            encoding=BodyEncoding.FORM_URLENCODED,
        )
    with pytest.raises(ValueError, match="API_V3"):
        Request("task.elapseditem.getlist", _arguments(), route=RouteKind.API_V3)


def test_positional_null_and_trailing_omission_are_distinct() -> None:
    layout = PositionalLayout(
        "sample.v1",
        (SlotContract("one", SlotShape.ANY), SlotContract("two", SlotShape.ANY), SlotContract("three", SlotShape.ANY)),
    )
    arguments = PositionalArguments((Present(1), Null(), Omitted()), layout.layout_id, layout=layout)
    assert arguments.to_wire_slots() == [1, None]


@pytest.mark.asyncio
async def test_positional_requires_advertised_wire_capability_before_io() -> None:
    class LegacyTransport:
        calls = 0

        async def send(
            self, request: Request, *, attempt_timeout: float, max_response_bytes: int,
        ) -> WireResponse:
            assert request.method
            assert attempt_timeout > 0
            assert max_response_bytes > 0
            self.calls += 1
            return WireResponse(200, (), b'{"result":[]}')

    transport = LegacyTransport()
    with pytest.raises(CapabilityError, match="positional"):
        await Executor(transport).execute(Request("task.elapseditem.getlist", _arguments(), route=RouteKind.BARE))
    assert transport.calls == 0


@pytest.mark.asyncio
async def test_positional_direct_request_sends_exact_json_array_and_batch_rejects() -> None:
    bodies: list[bytes] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content)
        return httpx.Response(200, content=b'{"result":[]}', request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = HttpxTransport("https://example.invalid/rest/1/token/", client=client)
    executor = Executor(transport)
    request = Request("task.elapseditem.getlist", _arguments(), route=RouteKind.BARE)
    try:
        await executor.execute(request)
        assert bodies == [b'[42,{},{},[],{"NAV_PARAMS":{"iNumPage":1}}]']
        assert request.positional is not None
        assert json.loads(bodies[0]) == request.positional.to_wire_slots()
        outcomes = [outcome async for outcome in BatchExecutor(executor)._outcomes([request])]  # noqa: SLF001
        assert len(outcomes) == 1
        assert isinstance(outcomes[0], BatchFailure)
        assert isinstance(outcomes[0].error, CapabilityError)
        assert len(bodies) == 1
    finally:
        await transport.aclose()
        await client.aclose()
