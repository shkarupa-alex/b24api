"""W7 facade mapping, compatibility, and signature characterization."""

from __future__ import annotations
import inspect
import json
from typing import TYPE_CHECKING, cast
from urllib.parse import parse_qs, urlsplit

import pytest

from b24api.error import IncompleteTraversalError
from b24api.execution import Executor, WireResponse
from b24api.facade import Bitrix24
from b24api.models import (
    ConsistencyPolicy,
    ExecutionPolicy,
    OperationReport,
    ReferenceBinding,
    ReferenceItem,
    ReplaySafety,
    Request,
    SnapshotRequirement,
    TerminalState,
)
from b24api.plans import DirectDispatch, SingleResponsePlan

if TYPE_CHECKING:
    from collections.abc import Callable

MINIMUM_BATCH_CALLS = 2


class FunctionTransport:
    """Record canonical requests and return JSON envelopes from a handler."""

    def __init__(self, handler: Callable[[Request], object]) -> None:
        self.handler = handler
        self.requests: list[Request] = []

    async def send(self, request: Request, *, attempt_timeout: float) -> WireResponse:
        del attempt_timeout
        self.requests.append(request)
        body = json.dumps(self.handler(request), separators=(",", ":")).encode()
        return WireResponse(200, (("content-type", "application/json"),), body)


def _client(handler: Callable[[Request], object]) -> tuple[Bitrix24, FunctionTransport]:
    transport = FunctionTransport(handler)
    return Bitrix24._from_executor(Executor(transport)), transport  # noqa: SLF001


@pytest.mark.asyncio
async def test_call_uses_canonical_executor_and_preserves_raw_contract() -> None:
    client, transport = _client(lambda request: {"result": {"method": request.method}})

    assert await client.call({"method": "profile"}) == {"method": "profile"}
    raw = await client.call({"method": "profile"}, raw=True)

    assert raw.result == {"method": "profile"}
    assert [request.method for request in transport.requests] == ["profile", "profile"]


@pytest.mark.asyncio
async def test_batch_preserves_payload_and_legacy_list_selection() -> None:
    def handler(request: Request) -> object:
        assert request.method == "batch"
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": {key: {"items": [{"ID": index + 1}]} for index, key in enumerate(commands)},
                "result_error": [],
            },
        }

    client, _transport = _client(handler)
    outcomes = [
        item
        async for item in client.batch(
            [({"method": "one"}, "p1"), ({"method": "two"}, "p2")],
            with_payload=True,
            list_method=True,
        )
    ]

    assert outcomes == [([{"ID": 1}], "p1"), ([{"ID": 2}], "p2")]


@pytest.mark.asyncio
async def test_sequential_and_counted_wrappers_delegate_to_shared_driver() -> None:
    def handler(request: Request) -> object:
        start = request.copy_parameters().get("start", 0)
        assert isinstance(start, int)
        rows = [{"ID": start + 1}]
        return {"result": rows, "total": 2} | ({"next": 1} if start == 0 else {})

    sequential, sequential_transport = _client(handler)
    counted, counted_transport = _client(handler)

    assert [item async for item in sequential.list_sequential({"method": "crm.item.list"})] == [
        {"ID": 1},
        {"ID": 2},
    ]
    assert [item async for item in counted.list_batched({"method": "crm.item.list"})] == [
        {"ID": 1},
        {"ID": 2},
    ]
    assert [request.copy_parameters()["start"] for request in sequential_transport.requests] == [0, 1]
    assert [request.copy_parameters()["start"] for request in counted_transport.requests] == [0, 1]
    assert all(request.replay_safety is ReplaySafety.SAFE for request in sequential_transport.requests)


@pytest.mark.asyncio
async def test_no_count_wrapper_maps_legacy_id_key_to_exact_keyset() -> None:
    def handler(request: Request) -> object:
        parameters = request.copy_parameters()
        filter_value = parameters.get("filter", {})
        assert isinstance(filter_value, dict)
        if filter_value.get(">ID") == 1:
            return {"result": []}
        return {"result": [{"ID": "1"}]}

    client, transport = _client(handler)
    assert [item async for item in client.list_batched_no_count({"method": "crm.item.list"})] == [{"ID": "1"}]

    assert transport.requests[0].copy_parameters() == {"order": {"ID": "ASC"}, "start": -1}
    assert transport.requests[1].copy_parameters()["filter"] == {">ID": 1}


@pytest.mark.asyncio
async def test_reference_keyset_wrapper_preserves_input_order_and_payload() -> None:
    batch_calls = 0

    def handler(request: Request) -> object:
        nonlocal batch_calls
        assert request.method == "batch"
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        batch_calls += 1
        results: dict[str, object] = {}
        for key, command in commands.items():
            assert isinstance(command, str)
            query = parse_qs(urlsplit(command).query)
            results[key] = [] if any(">ID" in name for name in query) else [{"ID": 1}]
        return {
            "result": {
                "result": results,
                "result_error": [],
            },
        }

    client, _transport = _client(handler)
    items = [
        item
        async for item in client.reference_batched_no_count(
            {"method": "crm.timeline.list"},
            [({"OWNER_ID": 1}, "a"), ({"OWNER_ID": 2}, "b")],
            with_payload=True,
            batch_size=2,
        )
    ]

    assert items == [({"ID": 1}, "a"), ({"ID": 1}, "b")]
    assert batch_calls >= MINIMUM_BATCH_CALLS


@pytest.mark.asyncio
async def test_reference_cursor_wrapper_selects_result_and_short_page_terminal() -> None:
    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": {key: {"items": [{"id": 10}]} for key in commands},
                "result_error": [],
            },
        }

    client, transport = _client(handler)
    items = [
        item
        async for item in client.reference_cursor_no_count(
            {"method": "im.dialog.messages.get"},
            [({"DIALOG_ID": "chat1"}, "chat")],
            list_size=2,
            batch_size=1,
            result_key="items",
            with_payload=True,
        )
    ]

    assert items == [({"id": 10}, "chat")]
    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_iter_reference_binds_updates_and_payload_without_a_second_scheduler() -> None:
    client, transport = _client(lambda _request: {"result": [{"ID": 1}]})
    stream = client.iter_reference(
        {"method": "crm.item.list"},
        [ReferenceBinding("account one", "account-1", {"owner": 7}, payload={"local": 1})],
        plan=SingleResponsePlan(),
        dispatch=DirectDispatch(),
    )

    outcomes = [outcome async for outcome in stream]
    assert len(outcomes) == 1
    assert isinstance(outcomes[0], ReferenceItem)
    assert outcomes[0].item == {"ID": 1}
    assert outcomes[0].payload == {"local": 1}
    assert transport.requests[0].copy_parameters() == {"owner": 7}


@pytest.mark.asyncio
async def test_compatibility_wrapper_raises_when_report_is_not_completed() -> None:
    client, _transport = _client(lambda _request: {"result": []})
    policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(snapshot_requirement=SnapshotRequirement.FROZEN_MANIFEST),
    )

    with pytest.raises(IncompleteTraversalError) as captured:
        _ = [
            item
            async for item in client.list_sequential(
                {"method": "crm.item.list"},
                plan=SingleResponsePlan(),
                policy=policy,
            )
        ]

    report = cast("OperationReport", captured.value.report)
    assert report.state is TerminalState.INCOMPLETE


def test_facade_signature_snapshot_preserves_committed_and_keyword_bridges() -> None:
    expected = {
        "call": ("self", "request", "raw", "policy", "retry"),
        "batch": ("self", "requests", "batch_size", "list_method", "with_payload", "policy"),
        "list_sequential": ("self", "request", "list_size", "plan", "profile", "identity", "policy"),
        "list_batched": ("self", "request", "list_size", "batch_size", "plan", "profile", "identity", "policy"),
        "list_batched_no_count": (
            "self",
            "request",
            "id_key",
            "list_size",
            "batch_size",
            "plan",
            "profile",
            "identity",
            "policy",
        ),
    }

    assert {
        name: tuple(inspect.signature(getattr(Bitrix24, name)).parameters)
        for name in expected
    } == expected
