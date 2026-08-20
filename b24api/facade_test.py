"""W7 facade mapping, compatibility, and signature characterization."""

from __future__ import annotations
import ast
import asyncio
import inspect
import json
import textwrap
from typing import TYPE_CHECKING, cast
from urllib.parse import parse_qs, urlsplit

import pytest

import b24api
import b24api.facade as facade_module
from b24api.entity import (
    BatchResult,
    ErrorResponse,
    ListRequest,
    ListRequestParameters,
)
from b24api.entity import (
    Request as EntityRequest,
)
from b24api.entity import (
    Response as EntityResponse,
)
from b24api.entity import (
    ResponseTime as EntityResponseTime,
)
from b24api.error import (
    ApiResponseError,
    CapabilityError,
    IncompleteTraversalError,
    RetryApiResponseError,
    RetryHTTPStatusError,
)
from b24api.execution import Executor, WireResponse
from b24api.facade import Bitrix24
from b24api.models import (
    ConsistencyPolicy,
    ExecutionPolicy,
    IdentityCoercion,
    IdentityRequirement,
    IdentitySpec,
    JsonValue,
    OperationReport,
    ReferenceBinding,
    ReferenceItem,
    ReplaySafety,
    Request,
    Response,
    ResponseTime,
    ResultSelector,
    SnapshotRequirement,
    TerminalState,
)
from b24api.plans import (
    CursorTerminalRule,
    DirectDispatch,
    ItemCursorPlan,
    OffsetSequentialPlan,
    SingleResponsePlan,
)
from b24api.query import build_query
from b24api.type import ApiTypes

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterator

    from b24api.profiles import EndpointProfile

MINIMUM_BATCH_CALLS = 2
EXPECTED_PAGE_ROWS = 2


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
async def test_batch_list_method_preserves_empty_mapping_as_empty_list() -> None:
    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {"result": {"result": {key: {} for key in commands}, "result_error": []}}

    client, _transport = _client(handler)

    assert [item async for item in client.batch([{"method": "empty"}], list_method=True)] == [[]]


@pytest.mark.asyncio
async def test_public_batch_early_close_closes_owned_sync_source() -> None:
    closed = False

    def source() -> Iterator[dict[str, object]]:
        nonlocal closed
        try:
            yield {"method": "one"}
            yield {"method": "two"}
        finally:
            closed = True

    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {"result": {"result": {key: {"ok": True} for key in commands}, "result_error": []}}

    client, _transport = _client(handler)
    stream = cast("AsyncGenerator[JsonValue, None]", client.batch(source(), batch_size=1))
    assert await anext(stream) == {"ok": True}

    await stream.aclose()

    assert closed


@pytest.mark.asyncio
async def test_public_batch_early_close_closes_owned_async_source() -> None:
    closed = False
    blocker = asyncio.Event()

    async def source() -> AsyncGenerator[dict[str, object]]:
        nonlocal closed
        try:
            yield {"method": "one"}
            await blocker.wait()
            yield {"method": "two"}
        finally:
            closed = True

    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {"result": {"result": {key: {"ok": True} for key in commands}, "result_error": []}}

    client, _transport = _client(handler)
    stream = cast("AsyncGenerator[JsonValue, None]", client.batch(source(), batch_size=1))
    assert await anext(stream) == {"ok": True}

    await stream.aclose()

    assert closed


@pytest.mark.asyncio
async def test_sequential_and_counted_wrappers_delegate_to_shared_driver() -> None:
    def handler(request: Request) -> object:
        start = request.copy_parameters().get("start", 0)
        assert isinstance(start, int)
        rows = [{"ID": start + 1}] if start < EXPECTED_PAGE_ROWS else []
        return {"result": rows, "total": EXPECTED_PAGE_ROWS} | ({"next": 1} if start == 0 else {})

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
async def test_sequential_wrapper_accepts_server_next_without_total() -> None:
    def handler(request: Request) -> object:
        start = request.copy_parameters().get("start", 0)
        if start == 0:
            return {"result": [{"ID": 1}], "next": 1}
        return {"result": []}

    client, transport = _client(handler)
    assert [item async for item in client.list_sequential({"method": "department.get"})] == [{"ID": 1}]
    assert [request.copy_parameters()["start"] for request in transport.requests] == [0, 1]


@pytest.mark.asyncio
async def test_list_size_override_is_enforced_as_compatibility_page_cap() -> None:
    client, _transport = _client(lambda _request: {"result": [{"ID": 1}, {"ID": 2}]})

    with pytest.raises(IncompleteTraversalError) as caught:
        _ = [
            item
            async for item in client.list_sequential(
                {"method": "department.get"},
                list_size=1,
            )
        ]

    assert "declared page cap" in str(caught.value.__cause__)


@pytest.mark.asyncio
async def test_public_list_early_close_propagates_final_incomplete_report() -> None:
    def handler(request: Request) -> object:
        start = request.copy_parameters().get("start", 0)
        assert isinstance(start, int)
        return {"result": [{"ID": start + 1}], "next": start + 1}

    client, _transport = _client(handler)
    stream = client.list_sequential({"method": "department.get"})
    assert await anext(stream) == {"ID": 1}

    with pytest.raises(IncompleteTraversalError) as caught:
        await stream.aclose()

    report = cast("OperationReport", caught.value.report)
    assert report.state is TerminalState.CANCELLED


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

    assert transport.requests[0].copy_parameters() == {
        "order": {"ID": "ASC"},
        "select": ["ID"],
        "start": -1,
    }
    assert transport.requests[1].copy_parameters()["filter"] == {">ID": 1}


@pytest.mark.asyncio
async def test_compatibility_wrapper_unwraps_one_key_result_but_canonical_selector_is_exact() -> None:
    client, _transport = _client(lambda _request: {"result": {"tasks": [{"ID": "1"}]}, "total": 1})

    assert [item async for item in client.list_sequential({"method": "tasks.task.list"})] == [{"ID": "1"}]

    canonical = client.iter_list(
        {"method": "tasks.task.list"},
        plan=OffsetSequentialPlan(),
        selector=ResultSelector.root(),
    )
    with pytest.raises(CapabilityError, match="declared selector"):
        _ = [item async for item in canonical]


@pytest.mark.asyncio
async def test_legacy_list_request_translates_without_injecting_default_containers() -> None:
    client, transport = _client(lambda _request: {"result": []})
    request = ListRequest(
        method="crm.item.list",
        parameters=ListRequestParameters.model_validate({"entityTypeId": 2}),
    )

    assert [item async for item in client.list_sequential(request, plan=SingleResponsePlan())] == []
    assert transport.requests[0].copy_parameters() == {"entityTypeId": 2}


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
async def test_reference_keyset_wrapper_preserves_committed_page_round_order() -> None:
    command_groups: list[list[tuple[str, int]]] = []

    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        group: list[tuple[str, int]] = []
        results: dict[str, object] = {}
        for key, command in commands.items():
            assert isinstance(command, str)
            query = parse_qs(urlsplit(command).query)
            owner = query["filter[OWNER_ID]"][0]
            cursor = int(query.get("filter[>ID]", ["0"])[0])
            group.append((owner, cursor))
            results[key] = (
                [{"ID": cursor + 1, "OWNER_ID": owner}] if cursor < EXPECTED_PAGE_ROWS else []
            )
        command_groups.append(group)
        return {"result": {"result": results, "result_error": []}}

    client, _transport = _client(handler)
    items = [
        item
        async for item in client.reference_batched_no_count(
            {"method": "crm.timeline.list"},
            [({"OWNER_ID": "A"}, "a"), ({"OWNER_ID": "B"}, "b")],
            with_payload=True,
            batch_size=2,
        )
    ]

    assert items == [
        ({"ID": 1, "OWNER_ID": "A"}, "a"),
        ({"ID": 1, "OWNER_ID": "B"}, "b"),
        ({"ID": 2, "OWNER_ID": "A"}, "a"),
        ({"ID": 2, "OWNER_ID": "B"}, "b"),
    ]
    assert command_groups == [
        [("A", 0), ("B", 0)],
        [("A", 1), ("B", 1)],
        [("A", 2), ("B", 2)],
    ]


@pytest.mark.asyncio
async def test_reference_keyset_wrapper_appends_identity_to_legacy_select() -> None:
    seen_select: list[list[str]] = []

    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        results: dict[str, object] = {}
        for key, command in commands.items():
            assert isinstance(command, str)
            query = parse_qs(urlsplit(command).query)
            selected = [values[0] for name, values in query.items() if name.startswith("select[")]
            seen_select.append(selected)
            results[key] = [] if "filter[>ID]" in query else [{"ID": 1, "STATUS_ID": "NEW"}]
        return {"result": {"result": results, "result_error": []}}

    client, _transport = _client(handler)
    items = [
        item
        async for item in client.reference_batched_no_count(
            {
                "method": "crm.timeline.list",
                "parameters": {"select": ["STATUS_ID"]},
            },
            [{"OWNER_ID": "A"}],
            batch_size=1,
        )
    ]

    assert items == [{"ID": 1, "STATUS_ID": "NEW"}]
    assert seen_select
    assert all(selected == ["STATUS_ID", "ID"] for selected in seen_select)


@pytest.mark.asyncio
async def test_public_reference_early_close_closes_owned_async_source() -> None:
    closed = False
    blocker = asyncio.Event()

    async def updates() -> AsyncGenerator[dict[str, object]]:
        nonlocal closed
        try:
            yield {"OWNER_ID": 1}
            await blocker.wait()
            yield {"OWNER_ID": 2}
        finally:
            closed = True

    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        results: dict[str, object] = {}
        for key, command in commands.items():
            assert isinstance(command, str)
            results[key] = [] if "%3EID" in command or ">ID" in command else [{"ID": 1}]
        return {"result": {"result": results, "result_error": []}}

    client, _transport = _client(handler)
    stream = client.reference_batched_no_count(
        {"method": "crm.timeline.list"},
        updates(),
        batch_size=1,
    )
    assert await anext(stream) == {"ID": 1}

    with pytest.raises(IncompleteTraversalError):
        await stream.aclose()

    assert closed


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
async def test_reference_cursor_min_preserves_committed_descending_direction() -> None:
    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": {key: [{"id": 3}, {"id": 2}] for key in commands},
                "result_error": [],
            },
        }

    client, _transport = _client(handler)
    result = [
        item
        async for item in client.reference_cursor_no_count(
            {"method": "im.dialog.messages.get"},
            [{"DIALOG_ID": "chat1"}],
            cursor_take="min",
            list_size=3,
            batch_size=1,
        )
    ]

    assert result == [{"id": 3}, {"id": 2}]


@pytest.mark.asyncio
@pytest.mark.parametrize("row", [{"ID": 1}, {"ID": 1, "cursor": None}])
async def test_reference_cursor_profile_exhaustion_delivers_missing_or_null_terminal_row(
    row: dict[str, JsonValue],
) -> None:
    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {"result": {"result": {key: [row] for key in commands}, "result_error": []}}

    client, _transport = _client(handler)
    plan = ItemCursorPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        cursor_item_path=("cursor",),
        cursor_coercion=IdentityCoercion.EXACT_INTEGER,
        terminal=CursorTerminalRule.PROFILE_CURSOR_EXHAUSTED,
    )
    identity = IdentitySpec(
        item_path=("ID",),
        filter_key="ID",
        order_key="ID",
        coercion=IdentityCoercion.EXACT_INTEGER,
    )

    result = [
        item
        async for item in client.reference_cursor_no_count(
            {"method": "im.dialog.messages.get"},
            [{"DIALOG_ID": "chat1"}],
            plan=plan,
            identity=identity,
            batch_size=1,
        )
    ]

    assert result == [row]


@pytest.mark.asyncio
async def test_reference_cursor_profile_exhaustion_rejects_mixed_cursor_presence() -> None:
    rows = [{"ID": 1, "cursor": 1}, {"ID": 2}]

    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {"result": {"result": dict.fromkeys(commands, rows), "result_error": []}}

    client, _transport = _client(handler)
    plan = ItemCursorPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        cursor_item_path=("cursor",),
        cursor_coercion=IdentityCoercion.EXACT_INTEGER,
        terminal=CursorTerminalRule.PROFILE_CURSOR_EXHAUSTED,
    )
    identity = IdentitySpec(
        item_path=("ID",),
        filter_key="ID",
        order_key="ID",
        coercion=IdentityCoercion.EXACT_INTEGER,
    )

    with pytest.raises(IncompleteTraversalError) as caught:
        _ = [
            item
            async for item in client.reference_cursor_no_count(
                {"method": "im.dialog.messages.get"},
                [{"DIALOG_ID": "chat1"}],
                plan=plan,
                identity=identity,
                batch_size=1,
            )
        ]

    report = cast("OperationReport", caught.value.report)
    assert report.emitted_rows == 0


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


@pytest.mark.asyncio
async def test_compatibility_wrapper_normalizes_detected_pagination_failure() -> None:
    client, _transport = _client(lambda _request: {"result": [{"ID": 1}]})

    with pytest.raises(IncompleteTraversalError) as captured:
        _ = [item async for item in client.list_batched_no_count({"method": "crm.item.list"})]

    report = cast("OperationReport", captured.value.report)
    assert report.state is TerminalState.FAILED
    assert captured.value.__cause__ is not None
    assert "fingerprint" in str(captured.value.__cause__)


@pytest.mark.asyncio
async def test_plan_profile_conflict_refuses_before_transport() -> None:
    client, transport = _client(lambda _request: {"result": []})

    with pytest.raises(CapabilityError, match="cannot both"):
        _ = [
            item
            async for item in client.list_sequential(
                {"method": "crm.item.list"},
                plan=SingleResponsePlan(),
                profile=cast("EndpointProfile", object()),
            )
        ]

    assert transport.requests == []


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
        "reference_batched_no_count": (
            "self",
            "request",
            "updates",
            "id_key",
            "list_size",
            "batch_size",
            "with_payload",
            "plan",
            "profile",
            "identity",
            "policy",
        ),
        "reference_cursor_no_count": (
            "self",
            "request",
            "updates",
            "cursor_param",
            "cursor_field",
            "cursor_take",
            "list_size",
            "list_size_param",
            "batch_size",
            "result_key",
            "with_payload",
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

    constructor = inspect.signature(Bitrix24)
    settings = constructor.parameters["settings"]
    assert settings.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert settings.default is None
    assert settings.annotation == "Settings | None"

    outcomes = inspect.signature(Bitrix24.batch_outcomes)
    assert outcomes.return_annotation == "BatchOutcomeStream"
    assert outcomes.parameters["batch_size"].kind is inspect.Parameter.KEYWORD_ONLY
    assert outcomes.parameters["fallback_failed"].default == "none"

    for name in (
        "list_sequential",
        "list_batched",
        "list_batched_no_count",
        "reference_batched_no_count",
        "reference_cursor_no_count",
    ):
        signature = inspect.signature(getattr(Bitrix24, name))
        assert signature.parameters["profile"].annotation == "EndpointProfile | None"
        for parameter in tuple(signature.parameters.values())[2:]:
            if name.startswith("reference_") and parameter.name == "updates":
                continue
            assert parameter.kind is inspect.Parameter.KEYWORD_ONLY


def test_root_export_snapshot_and_legacy_import_paths() -> None:
    assert b24api.__all__ == [
        "ApiResponseError",
        "BatchFailure",
        "BatchSuccess",
        "Bitrix24",
        "ExecutionPolicy",
        "IdentitySpec",
        "ReferenceFailure",
        "ReferenceItem",
        "Request",
        "Response",
        "ResultSelector",
    ]
    assert b24api.Bitrix24 is Bitrix24
    assert b24api.Request is Request
    assert EntityRequest is Request
    assert EntityResponse is Response
    assert EntityResponseTime is ResponseTime
    assert Request("crm.item.get", {"ID": [1, 2]}).query == "crm.item.get?ID%5B0%5D=1&ID%5B1%5D=2"
    assert build_query({"ID": 1}) == "ID=1"
    assert ApiTypes is not None
    assert all(
        value is not None
        for value in (
            ListRequest,
            ListRequestParameters,
            ErrorResponse,
            BatchResult,
            ApiResponseError,
            RetryApiResponseError,
            RetryHTTPStatusError,
        )
    )


def test_compatibility_wrappers_contain_no_pagination_engine() -> None:
    wrapper_names = (
        "list_sequential",
        "list_batched",
        "list_batched_no_count",
        "reference_batched_no_count",
        "reference_cursor_no_count",
    )
    for name in wrapper_names:
        tree = ast.parse(textwrap.dedent(inspect.getsource(getattr(facade_module.Bitrix24, name))))
        assert not any(isinstance(node, ast.While) for node in ast.walk(tree))
        calls = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        assert "execute" not in calls
