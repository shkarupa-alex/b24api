"""Offline qualification of the optional sequential keyset profile for five-slot positional requests.

This proves the client mechanics only: exact wire slots, strict identity progression, and refusal
before I/O. It does not qualify any portal's task scope or ID filter semantics.
"""

from __future__ import annotations
import json
from typing import TYPE_CHECKING, cast

import httpx
import pytest

from b24api import (
    Bitrix24,
    EmptyArray,
    EmptyObject,
    IdentityCoercion,
    IdentitySpec,
    KeysetCapabilityVerdict,
    KeysetSpec,
    OffsetSpec,
    PageIndex,
    ParameterPath,
    PartitionedKeysetExecution,
    PositionalArguments,
    PositionalLayout,
    Present,
    RangeKeysetExecution,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    SequentialKeysetExecution,
    Settings,
    SlotContract,
    SlotShape,
    StableIntegerKeysetContract,
    TerminalState,
    TotalTermination,
    TraversalAssurance,
)
from b24api.contracts.positional import PositionalControlError, PositionalControlFault
from b24api.errors import CapabilityError, IncompleteTraversalError, PaginationError
from b24api.execution import HttpxTransport
from b24api.testing import ScriptedTransport
from b24api.traversal.identity import _request_with_controls

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.contracts.positional import Slot
    from b24api.contracts.report import OperationReport

TASK_ID = 42
PAGE_SIZE = 2
SOURCE_IDS = (3, 5, 8, 13, 21)
REPEATED_PAGE_REQUESTS = 2
ORDER_CONTROL = (1, "ID")
FILTER_CONTROL = (2, ">ID")
GUARD_CONTROLS = frozenset({ORDER_CONTROL, FILTER_CONTROL, (2, "<ID"), (2, "ID")})
HOST = "example.invalid"
SETTINGS = Settings(webhook_url=f"https://{HOST}/rest/1/token/")
IDENTITY = IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.DECIMAL_STRING_INTEGER)
KEYSET = KeysetSpec(
    order_path=ParameterPath((1,)),
    filter_path=ParameterPath((2,)),
    start_suppression_path=None,
)
CALLER_VALUE = "caller-owned-filter-value"
_ROOT = ResultSelector.root()


def _layout(
    control_paths: frozenset[tuple[str | int, ...]] = frozenset({ORDER_CONTROL, FILTER_CONTROL}),
) -> PositionalLayout:
    return PositionalLayout(
        "task.elapseditem.getlist.five.keyset.v1",
        (
            SlotContract("taskId", SlotShape.SCALAR, fixed=True),
            SlotContract("order", SlotShape.OBJECT),
            SlotContract("filter", SlotShape.OBJECT),
            SlotContract("select", SlotShape.ARRAY),
            SlotContract("params", SlotShape.OBJECT),
        ),
        control_paths=control_paths,
    )


def _request(
    *,
    order: Slot | None = None,
    filter_slot: Slot | None = None,
    layout: PositionalLayout | None = None,
) -> Request:
    layout = layout or _layout()
    arguments = PositionalArguments(
        (
            Present(TASK_ID),
            Present({}) if order is None else order,
            Present({}) if filter_slot is None else filter_slot,
            EmptyArray(),
            Present({"NAV_PARAMS": {"nPageSize": PAGE_SIZE}}),
        ),
        layout.layout_id,
        layout=layout,
    )
    return Request("task.elapseditem.getlist", arguments, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)


def _server(*, honor_filter: bool = True) -> tuple[list[bytes], Callable[[httpx.Request], httpx.Response]]:
    bodies: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content)
        slots = json.loads(request.content)
        if len(slots) != len(_layout().slots) or slots[0] != TASK_ID:
            raise AssertionError("positional keyset lost the fixed five-slot task scope")
        if slots[1] != {"ID": "ASC"}:
            raise AssertionError("positional keyset did not write the declared order leaf")
        lower = slots[2].get(">ID", 0) if honor_filter else 0
        ids = [identity for identity in SOURCE_IDS if identity > lower][:PAGE_SIZE]
        return httpx.Response(
            200,
            json={"result": [{"ID": str(identity), "TASK_ID": str(TASK_ID)} for identity in ids]},
            request=request,
        )

    return bodies, handler


async def _collect(
    handler: Callable[[httpx.Request], httpx.Response],
    request: Request,
) -> tuple[list[dict[str, str]], OperationReport | None]:
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = HttpxTransport(str(SETTINGS.webhook_url), client=http_client)
    try:
        async with Bitrix24(SETTINGS, transport=transport) as client:
            stream = client.iter_list_keyset(
                request,
                selector=_ROOT,
                identity=IDENTITY,
                page_size=PAGE_SIZE,
                keyset=KEYSET,
                execution=SequentialKeysetExecution(),
            )
            try:
                rows = [cast("dict[str, str]", row) async for row in stream]
            finally:
                report = stream.report
            return rows, report
    finally:
        await transport.aclose()
        await http_client.aclose()


def _keyset_stream(client: Bitrix24, request: Request, **kwargs: object) -> object:
    return client.iter_list_keyset(
        request,
        selector=_ROOT,
        identity=IDENTITY,
        page_size=PAGE_SIZE,
        keyset=KEYSET,
        **kwargs,  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_sequential_positional_keyset_writes_exact_slots_and_advances_strictly() -> None:
    bodies, handler = _server()

    rows, report = await _collect(handler, _request())

    assert [int(row["ID"]) for row in rows] == list(SOURCE_IDS)
    assert {row["TASK_ID"] for row in rows} == {str(TASK_ID)}
    assert bodies == [
        b'[42,{"ID":"ASC"},{},[],{"NAV_PARAMS":{"nPageSize":2}}]',
        b'[42,{"ID":"ASC"},{">ID":5},[],{"NAV_PARAMS":{"nPageSize":2}}]',
        b'[42,{"ID":"ASC"},{">ID":13},[],{"NAV_PARAMS":{"nPageSize":2}}]',
        b'[42,{"ID":"ASC"},{">ID":21},[],{"NAV_PARAMS":{"nPageSize":2}}]',
    ]
    assert report is not None
    assert report.state is TerminalState.COMPLETED
    assert report.exhausted
    assert report.assurance is TraversalAssurance.IDENTITY_EXACT


@pytest.mark.asyncio
async def test_sequential_positional_keyset_rejects_an_ignored_identity_filter() -> None:
    bodies, handler = _server(honor_filter=False)

    with pytest.raises(IncompleteTraversalError) as caught:
        await _collect(handler, _request())

    assert isinstance(caught.value.__cause__, PaginationError)
    assert len(bodies) == REPEATED_PAGE_REQUESTS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("request_factory", "fault"),
    [
        pytest.param(lambda: _request(order=EmptyObject()), PositionalControlFault.ABSENT_SLOT, id="order-slot-absent"),
        pytest.param(
            lambda: _request(filter_slot=EmptyObject()),
            PositionalControlFault.ABSENT_SLOT,
            id="filter-slot-absent",
        ),
        pytest.param(
            lambda: _request(layout=_layout(frozenset({ORDER_CONTROL}))),
            PositionalControlFault.UNDECLARED_PATH,
            id="filter-leaf-undeclared",
        ),
        pytest.param(
            lambda: _request(layout=_layout(frozenset({FILTER_CONTROL}))),
            PositionalControlFault.UNDECLARED_PATH,
            id="order-leaf-undeclared",
        ),
        pytest.param(
            lambda: _request(filter_slot=Present({">id": CALLER_VALUE})),
            PositionalControlFault.NEAR_MATCH_CASING,
            id="filter-near-match",
        ),
    ],
)
async def test_positional_keyset_control_faults_fail_before_io_with_a_value_free_reason(
    request_factory: Callable[[], Request],
    fault: PositionalControlFault,
) -> None:
    transport = ScriptedTransport((), host=HOST)

    async with Bitrix24(SETTINGS, transport=transport) as client:
        stream = _keyset_stream(client, request_factory(), execution=SequentialKeysetExecution())
        with pytest.raises(CapabilityError) as caught:
            _ = [row async for row in stream]  # type: ignore[attr-defined]

    assert transport.calls == ()
    error = caught.value
    assert type(error) is CapabilityError
    assert isinstance(error.__cause__, PositionalControlError)
    assert error.__cause__.fault is fault
    assert str(error).startswith("positional request conflicts with declared traversal controls")
    assert fault.value in str(error)
    assert CALLER_VALUE not in str(error)
    assert str(TASK_ID) not in str(error)


@pytest.mark.asyncio
async def test_positional_page_index_near_match_keeps_its_class_and_names_the_reason() -> None:
    layout = PositionalLayout(
        "task.elapseditem.getlist.five.v1",
        _layout().slots,
        control_paths=frozenset({(4, "NAV_PARAMS", "iNumPage")}),
    )
    arguments = PositionalArguments(
        (Present(TASK_ID), EmptyObject(), EmptyObject(), EmptyArray(), Present({"NAV_PARAMS": {"inumpage": 7}})),
        layout.layout_id,
        layout=layout,
    )
    transport = ScriptedTransport((), host=HOST)
    path = ParameterPath((4, "NAV_PARAMS", "iNumPage"))

    async with Bitrix24(SETTINGS, transport=transport) as client:
        stream = client.iter_list(
            Request("task.elapseditem.getlist", arguments, route=RouteKind.BARE),
            page_size=PAGE_SIZE,
            offset=OffsetSpec(parameter_path=path, page_index=PageIndex(path, max_rows=PAGE_SIZE)),
        )
        with pytest.raises(CapabilityError, match="near-match casing") as caught:
            _ = [row async for row in stream]

    assert transport.calls == ()
    assert isinstance(caught.value.__cause__, PositionalControlError)
    assert caught.value.__cause__.fault is PositionalControlFault.NEAR_MATCH_CASING


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "execution",
    [
        pytest.param({}, id="default-auto"),
        pytest.param({"execution": RangeKeysetExecution(contract=StableIntegerKeysetContract())}, id="range-batch"),
        pytest.param(
            {"execution": PartitionedKeysetExecution(contract=StableIntegerKeysetContract())},
            id="partitioned-batch",
        ),
    ],
)
async def test_positional_keyset_refuses_auto_and_batched_execution_before_io(execution: dict[str, object]) -> None:
    transport = ScriptedTransport((), host=HOST)

    async with Bitrix24(SETTINGS, transport=transport) as client:
        with pytest.raises(CapabilityError, match="use SequentialKeysetExecution for positional requests"):
            _keyset_stream(client, _request(), **execution)

    assert transport.calls == ()


@pytest.mark.asyncio
async def test_positional_request_cannot_use_counted_physical_batching() -> None:
    transport = ScriptedTransport((), host=HOST)

    async with Bitrix24(SETTINGS, transport=transport) as client:
        with pytest.raises(CapabilityError, match="JSON requests without scoped headers"):
            client.iter_list_counted(
                _request(),
                offset=OffsetSpec(total_termination=TotalTermination.EXACT_QUALIFIED),
            )

    assert transport.calls == ()


def _oracle(requests: list[list[object]]) -> Callable[[httpx.Request], httpx.Response]:
    """Serve a strict ordered ID source that honors every declared bound, as a qualified portal must."""
    source = range(1, 121)

    def admitted(identity: int, bounds: dict[str, int]) -> bool:
        checks = {
            ">ID": identity > bounds.get(">ID", identity - 1),
            "<ID": identity < bounds.get("<ID", identity + 1),
            "ID": identity == bounds.get("ID", identity),
        }
        return set(bounds) <= set(checks) and all(checks.values())

    def handler(request: httpx.Request) -> httpx.Response:
        slots = json.loads(request.content)
        requests.append(slots)
        if len(slots) != len(_layout().slots) or slots[0] != TASK_ID:
            raise AssertionError("positional guard lost the fixed five-slot task scope")
        ids = [identity for identity in source if admitted(identity, slots[2])]
        if slots[1].get("ID") == "DESC":
            ids.reverse()
        page = ids[: slots[4]["NAV_PARAMS"]["nPageSize"]]
        return httpx.Response(200, json={"result": [{"ID": str(identity)} for identity in page]}, request=request)

    return handler


@pytest.mark.asyncio
async def test_documented_positional_keyset_guard_verifies_offline() -> None:
    requests: list[list[object]] = []
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(_oracle(requests)))
    transport = HttpxTransport(str(SETTINGS.webhook_url), client=http_client)
    try:
        async with Bitrix24(SETTINGS, transport=transport) as client:
            report = await client.verify_keyset_capability(
                _request(layout=_layout(GUARD_CONTROLS)),
                selector=_ROOT,
                identity=IDENTITY,
                page_size=PAGE_SIZE,
                keyset=KEYSET,
            )
    finally:
        await transport.aclose()
        await http_client.aclose()

    assert report.verdict is KeysetCapabilityVerdict.VERIFIED
    assert requests
    assert {len(slots) for slots in requests} == {len(_layout().slots)}


@pytest.mark.asyncio
@pytest.mark.parametrize("params", [{}, {"NAV_PARAMS": []}], ids=["absent", "wrong-shape"])
async def test_positional_page_control_without_its_parent_fails_before_io(params: dict[str, object]) -> None:
    path = ParameterPath((4, "NAV_PARAMS", "iNumPage"))
    layout = PositionalLayout("page.v1", _layout().slots, control_paths=frozenset({path.path}))
    arguments = PositionalArguments(
        (Present(TASK_ID), EmptyObject(), EmptyObject(), EmptyArray(), Present(params)),
        layout.layout_id,
        layout=layout,
    )
    transport = ScriptedTransport((), host=HOST)

    async with Bitrix24(SETTINGS, transport=transport) as client:
        stream = client.iter_list(
            Request("task.elapseditem.getlist", arguments, route=RouteKind.BARE),
            page_size=PAGE_SIZE,
            offset=OffsetSpec(parameter_path=path, page_index=PageIndex(path, max_rows=PAGE_SIZE)),
        )
        with pytest.raises(CapabilityError) as caught:
            _ = [row async for row in stream]

    assert transport.calls == ()
    assert str(caught.value).endswith(PositionalControlFault.MISSING_PARENT.value)
    assert isinstance(caught.value.__cause__, PositionalControlError)
    assert caught.value.__cause__.fault is PositionalControlFault.MISSING_PARENT


def test_positional_control_value_outside_its_slot_contract_uses_the_generic_reason() -> None:
    layout = PositionalLayout("array.v1", (SlotContract("page", SlotShape.ARRAY),), control_paths=frozenset({(0,)}))
    arguments = PositionalArguments((Present([]),), layout.layout_id, layout=layout)
    request = Request("example.list", arguments, route=RouteKind.BARE)

    with pytest.raises(CapabilityError) as caught:
        _request_with_controls(request, {ParameterPath((0,)): 5}, allow_create=True)

    assert type(caught.value) is CapabilityError
    assert isinstance(caught.value.__cause__, ValueError)
    assert not isinstance(caught.value.__cause__, PositionalControlError)
    assert str(caught.value).endswith("control value does not satisfy its slot contract")
