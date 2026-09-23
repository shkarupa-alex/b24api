"""Exact PHP positional JSON slots, preflight, and transactional controls."""

from __future__ import annotations
import json

import httpx
import pytest

from b24api import (
    Bitrix24,
    BodyEncoding,
    EmptyArray,
    EmptyObject,
    Null,
    OffsetSpec,
    Omitted,
    PageIndex,
    ParameterPath,
    PositionalArguments,
    PositionalLayout,
    Present,
    Request,
    RouteKind,
    Settings,
    SlotContract,
    SlotShape,
)
from b24api.batch.engine import BatchExecutor
from b24api.batch.outcome import BatchFailure
from b24api.errors import CapabilityError
from b24api.execution import Executor, HttpxTransport
from b24api.testing import ScriptedTransport
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


def test_positional_control_writer_can_create_only_the_declared_final_mapping_leaf() -> None:
    layout = _elapsed_layout()
    original = PositionalArguments(
        (Present(42), EmptyObject(), EmptyObject(), EmptyArray(), Present({"NAV_PARAMS": {}})),
        layout.layout_id,
        layout=layout,
    )
    updated = original.write_control((4, "NAV_PARAMS", "iNumPage"), 2)
    assert original.to_wire_slots()[-1] == {"NAV_PARAMS": {}}
    assert updated.to_wire_slots()[-1] == {"NAV_PARAMS": {"iNumPage": 2}}

    missing_parent = PositionalArguments(
        (Present(42), EmptyObject(), EmptyObject(), EmptyArray(), Present({})),
        layout.layout_id,
        layout=layout,
    )
    with pytest.raises(ValueError, match="does not exist"):
        missing_parent.write_control((4, "NAV_PARAMS", "iNumPage"), 2)

    for near_match in ("inumpage", "INUMPAGE"):
        ambiguous = PositionalArguments(
            (Present(42), EmptyObject(), EmptyObject(), EmptyArray(), Present({"NAV_PARAMS": {near_match: 7}})),
            layout.layout_id,
            layout=layout,
        )
        with pytest.raises(ValueError, match="near-match casing"):
            ambiguous.write_control((4, "NAV_PARAMS", "iNumPage"), 2)
        assert ambiguous.to_wire_slots()[-1] == {"NAV_PARAMS": {near_match: 7}}

    intermediate_ambiguous = PositionalArguments(
        (
            Present(42),
            EmptyObject(),
            EmptyObject(),
            EmptyArray(),
            Present({"NAV_PARAMS": {"iNumPage": 1}, "nav_params": {"iNumPage": 7}}),
        ),
        layout.layout_id,
        layout=layout,
    )
    with pytest.raises(ValueError, match="near-match casing"):
        intermediate_ambiguous.write_control((4, "NAV_PARAMS", "iNumPage"), 2)
    assert intermediate_ambiguous.to_wire_slots()[-1] == {
        "NAV_PARAMS": {"iNumPage": 1},
        "nav_params": {"iNumPage": 7},
    }


def test_positional_layout_rejects_overlapping_control_paths() -> None:
    with pytest.raises(ValueError, match="non-overlapping"):
        PositionalLayout(
            "overlap.v1",
            (SlotContract("params", SlotShape.OBJECT),),
            control_paths=frozenset({(0, "NAV_PARAMS"), (0, "NAV_PARAMS", "iNumPage")}),
        )


@pytest.mark.asyncio
async def test_positional_near_match_fails_public_traversal_preflight_before_io() -> None:
    layout = _elapsed_layout()
    arguments = PositionalArguments(
        (Present(42), EmptyObject(), EmptyObject(), EmptyArray(), Present({"NAV_PARAMS": {"inumpage": 7}})),
        layout.layout_id,
        layout=layout,
    )
    transport = ScriptedTransport(())
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    path = ParameterPath((4, "NAV_PARAMS", "iNumPage"))

    async with Bitrix24(settings, transport=transport) as client:
        stream = client.iter_list(
            Request("task.elapseditem.getlist", arguments, route=RouteKind.BARE),
            page_size=2,
            offset=OffsetSpec(parameter_path=path, page_index=PageIndex(path, max_rows=2)),
        )
        with pytest.raises(CapabilityError, match="positional request conflicts"):
            _ = [row async for row in stream]

    assert transport.calls == ()


@pytest.mark.asyncio
async def test_positional_intermediate_near_match_fails_public_traversal_before_io() -> None:
    layout = _elapsed_layout()
    arguments = PositionalArguments(
        (
            Present(42),
            EmptyObject(),
            EmptyObject(),
            EmptyArray(),
            Present({"NAV_PARAMS": {"iNumPage": 1}, "nav_params": {"iNumPage": 7}}),
        ),
        layout.layout_id,
        layout=layout,
    )
    transport = ScriptedTransport(())
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    path = ParameterPath((4, "NAV_PARAMS", "iNumPage"))

    async with Bitrix24(settings, transport=transport) as client:
        stream = client.iter_list(
            Request("task.elapseditem.getlist", arguments, route=RouteKind.BARE),
            page_size=2,
            offset=OffsetSpec(parameter_path=path, page_index=PageIndex(path, max_rows=2)),
        )
        with pytest.raises(CapabilityError, match="positional request conflicts"):
            _ = [row async for row in stream]

    assert transport.calls == ()


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
            "task.elapseditem.getlist",
            _arguments(),
            route=RouteKind.BARE,
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
        host = "fixture.invalid"
        calls = 0

        async def send(
            self,
            request: Request,
            *,
            attempt_timeout: float,
            max_response_bytes: int,
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


@pytest.mark.asyncio
async def test_public_positional_traversal_sends_five_exact_task_scoped_json_slots() -> None:
    bodies: list[bytes] = []
    task_id = 42
    slot_count = len(_elapsed_layout().slots)

    async def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content)
        slots = json.loads(request.content)
        if len(slots) != slot_count or slots[0] != task_id:
            raise AssertionError("positional traversal lost the fixed task scope")
        page = slots[4]["NAV_PARAMS"]["iNumPage"]
        ids = {1: (1, 2), 2: (3,), 3: ()}[page]
        return httpx.Response(
            200,
            json={"result": [{"id": identity, "taskId": 42} for identity in ids]},
            request=request,
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = HttpxTransport("https://example.invalid/rest/1/token/", client=http_client)
    settings = Settings(webhook_url="https://example.invalid/rest/1/token/")
    path = ParameterPath((4, "NAV_PARAMS", "iNumPage"))
    try:
        async with Bitrix24(settings, transport=transport) as client:
            stream = client.iter_list(
                Request("task.elapseditem.getlist", _arguments(), route=RouteKind.BARE),
                page_size=2,
                offset=OffsetSpec(parameter_path=path, page_index=PageIndex(path, max_rows=2)),
            )
            rows = [row async for row in stream]
            assert [(row["id"], row["taskId"]) for row in rows] == [(1, 42), (2, 42), (3, 42)]
            assert stream.report is not None
            assert stream.report.exhausted
        assert bodies == [
            b'[42,{},{},[],{"NAV_PARAMS":{"iNumPage":1}}]',
            b'[42,{},{},[],{"NAV_PARAMS":{"iNumPage":2}}]',
            b'[42,{},{},[],{"NAV_PARAMS":{"iNumPage":3}}]',
        ]
    finally:
        await transport.aclose()
        await http_client.aclose()
