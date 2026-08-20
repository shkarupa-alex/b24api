"""W7 facade mapping, compatibility, and signature characterization."""

from __future__ import annotations
import ast
import asyncio
import inspect
import json
import textwrap
from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs, urlsplit

import pytest

import b24api
import b24api.facade as facade_module
from b24api.batch import BatchStream
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
    CompletionAssurance,
    ConsistencyPolicy,
    ExecutionPolicy,
    IdentityCoercion,
    IdentityRequirement,
    IdentitySpec,
    JsonValue,
    OperationReport,
    OrderSemantics,
    ParameterPath,
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
    KeysetPlan,
    OffsetSequentialPlan,
    SingleResponsePlan,
)
from b24api.profiles import CapabilitySet, load_profile_document
from b24api.query import build_query
from b24api.settings import Settings
from b24api.type import ApiTypes

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterator

    from b24api.profiles import EndpointProfile

MINIMUM_BATCH_CALLS = 2
EXPECTED_PAGE_ROWS = 2


class FunctionTransport:
    """Record canonical requests and return JSON envelopes from a handler."""

    def __init__(self, handler: Callable[[Request], object]) -> None:
        """Initialize instance state."""
        self.handler = handler
        self.requests: list[Request] = []

    async def send(self, request: Request, *, attempt_timeout: float) -> WireResponse:
        """Send one transport request attempt."""
        del attempt_timeout
        self.requests.append(request)
        body = json.dumps(self.handler(request), separators=(",", ":")).encode()
        return WireResponse(200, (("content-type", "application/json"),), body)


def _client(handler: Callable[[Request], object]) -> tuple[Bitrix24, FunctionTransport]:
    transport = FunctionTransport(handler)
    return Bitrix24._from_executor(Executor(transport)), transport  # noqa: SLF001


def _single_profile(*, required_probes: list[dict[str, object]] | None = None) -> EndpointProfile:
    return load_profile_document(
        {
            "schema_version": "1.0",
            "profile_id": "tasks-single-v1",
            "version": 1,
            "endpoint": "tasks-list",
            "method": "tasks.task.list",
            "verified_at": "2026-01-01T00:00:00+00:00",
            "expires_at": "2027-01-01T00:00:00+00:00",
            "applicable_builds": ["build-1"],
            "required_scopes": ["task"],
            "query": {
                "parameter_paths": [],
                "filter_keys": [],
                "filter_operators": [],
                "order": [],
                "selector": [],
            },
            "plan": {
                "kind": "single_response",
                "identity_requirement": "optional",
                "order_semantics": "unordered",
                "duplicate_policy": "report",
                "total_semantics": "advisory",
                "reject_continuation": True,
                "reject_positive_total_over_result": True,
            },
            "identity": None,
            "page_cap": 50,
            "replay_safety": "safe",
            "capabilities": {
                "offset_honored": False,
                "stable_order": False,
                "filter_honored": False,
                "cursor_honored": False,
                "batch_supported": False,
                "fixed_page_cap": True,
            },
            "evidence": [
                {
                    "artifact_sha256": "b" * 64,
                    "candidate_sha": "a" * 40,
                    "reviewed_at": "2025-12-20T00:00:00+00:00",
                    "expires_at": "2027-06-01T00:00:00+00:00",
                    "review_status": "accepted",
                },
            ],
            "required_probes": required_probes or [],
        },
    )


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
        if request.method == "batch":
            commands = request.copy_parameters()["cmd"]
            assert isinstance(commands, dict)
            results: dict[str, object] = {}
            totals: dict[str, int] = {}
            for key, command in commands.items():
                assert isinstance(command, str)
                start = int(parse_qs(urlsplit(command).query)["start"][0])
                results[key] = [{"ID": start + 1}] if start < EXPECTED_PAGE_ROWS else []
                totals[key] = EXPECTED_PAGE_ROWS
            return {
                "result": {
                    "result": results,
                    "result_error": [],
                    "result_total": totals,
                },
            }
        direct_start = request.copy_parameters().get("start", 0)
        assert isinstance(direct_start, int)
        rows = [{"ID": direct_start + 1}] if direct_start < EXPECTED_PAGE_ROWS else []
        return {"result": rows, "total": EXPECTED_PAGE_ROWS} | ({"next": 1} if direct_start == 0 else {})

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
    assert [request.method for request in counted_transport.requests] == ["crm.item.list", "batch"]
    assert all(request.replay_safety is ReplaySafety.SAFE for request in sequential_transport.requests)


@pytest.mark.asyncio
async def test_counted_wrapper_refuses_operation_budget_before_tail_admission() -> None:
    client, transport = _client(lambda _request: {"result": [{"ID": 1}], "total": 1_000_000, "next": 1})
    policy = ExecutionPolicy(max_requests=1, max_pages=1, max_buffered_rows=1)

    with pytest.raises(IncompleteTraversalError) as caught:
        _ = [item async for item in client.list_batched({"method": "crm.item.list"}, policy=policy)]

    report = cast("OperationReport", caught.value.report)
    assert report.state is TerminalState.FAILED
    assert report.physical_requests == 1
    assert report.logical_pages == 1
    assert report.emitted_rows == 0
    assert [request.method for request in transport.requests] == ["crm.item.list"]


@pytest.mark.asyncio
async def test_counted_wrapper_accounts_head_retries_before_emission() -> None:
    class RetryHeadTransport:
        def __init__(self) -> None:
            self.requests: list[Request] = []

        async def send(self, request: Request, *, attempt_timeout: float) -> WireResponse:
            assert attempt_timeout > 0
            self.requests.append(request)
            if len(self.requests) == 1:
                return WireResponse(
                    503,
                    (("content-type", "text/plain"),),
                    b"temporary upstream failure",
                )
            return WireResponse(
                200,
                (("content-type", "application/json"),),
                b'{"result":[{"ID":1}],"total":2,"next":1}',
            )

    async def no_sleep(_seconds: float) -> None:
        return None

    transport = RetryHeadTransport()
    client = Bitrix24._from_executor(Executor(transport, sleep=no_sleep))  # noqa: SLF001
    delivered: list[JsonValue] = []

    async def collect() -> None:
        async for item in client.list_batched(
            {"method": "crm.item.list"},
            policy=ExecutionPolicy(max_requests=2),
        ):
            delivered.append(item)  # noqa: PERF401 - prove refusal precedes first emission

    with pytest.raises(IncompleteTraversalError) as caught:
        await collect()

    report = cast("OperationReport", caught.value.report)
    assert delivered == []
    assert report.physical_requests == EXPECTED_PAGE_ROWS
    assert report.logical_pages == 1
    assert report.emitted_rows == 0
    assert len(transport.requests) == EXPECTED_PAGE_ROWS


@pytest.mark.asyncio
async def test_counted_wrapper_accounts_head_buffer_before_emission() -> None:
    client, transport = _client(lambda _request: {"result": [{"ID": 1}, {"ID": 2}], "total": 2})

    with pytest.raises(IncompleteTraversalError) as caught:
        _ = [
            item
            async for item in client.list_batched(
                {"method": "crm.item.list"},
                policy=ExecutionPolicy(max_buffered_rows=1),
            )
        ]

    report = cast("OperationReport", caught.value.report)
    assert report.emitted_rows == 0
    assert report.physical_requests == 1
    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_counted_wrapper_validates_head_before_first_emission() -> None:
    client, transport = _client(lambda _request: {"result": [{"ID": 1}], "total": 1, "next": 1})
    stream = client.list_batched({"method": "crm.item.list"})

    with pytest.raises(IncompleteTraversalError) as caught:
        await anext(stream)

    report = cast("OperationReport", caught.value.report)
    assert report.emitted_rows == 0
    assert report.logical_pages == 1
    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_counted_wrapper_validates_whole_tail_page_before_emission() -> None:
    def handler(request: Request) -> object:
        if request.method != "batch":
            return {"result": [{"ID": 1}, {"ID": 2}], "total": 4, "next": 2}
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": {key: [{"ID": 3}, {"ID": 1}] for key in commands},
                "result_error": [],
                "result_total": dict.fromkeys(commands, 4),
            },
        }

    client, _transport = _client(handler)
    delivered: list[JsonValue] = []
    identity = IdentitySpec(
        item_path=("ID",),
        filter_key="ID",
        order_key="ID",
        coercion=IdentityCoercion.EXACT_INTEGER,
    )

    async def collect() -> None:
        async for item in client.list_batched(
            {"method": "crm.item.list"},
            batch_size=1,
            identity=identity,
        ):
            delivered.append(item)  # noqa: PERF401 - retain the delivered prefix on failure

    with pytest.raises(IncompleteTraversalError) as caught:
        await collect()

    report = cast("OperationReport", caught.value.report)
    assert delivered == [{"ID": 1}, {"ID": 2}]
    assert report.state is TerminalState.FAILED
    assert report.emitted_rows == EXPECTED_PAGE_ROWS
    assert report.physical_requests == EXPECTED_PAGE_ROWS
    assert report.logical_pages == EXPECTED_PAGE_ROWS


@pytest.mark.asyncio
async def test_counted_wrapper_requires_intermediate_tail_continuation() -> None:
    def handler(request: Request) -> object:
        if request.method != "batch":
            return {"result": [{"ID": 1}], "total": 3, "next": 1}
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        results: dict[str, object] = {}
        for key, command in commands.items():
            assert isinstance(command, str)
            start = int(parse_qs(urlsplit(command).query)["start"][0])
            results[key] = [{"ID": start + 1}]
        return {
            "result": {
                "result": results,
                "result_error": [],
                "result_total": dict.fromkeys(commands, 3),
            },
        }

    client, _transport = _client(handler)
    delivered: list[JsonValue] = []

    async def collect() -> None:
        async for item in client.list_batched({"method": "crm.item.list"}, batch_size=2):
            delivered.append(item)  # noqa: PERF401 - retain the delivered prefix on failure

    with pytest.raises(IncompleteTraversalError) as caught:
        await collect()

    assert delivered == [{"ID": 1}]
    assert "continuation contradicts" in str(caught.value.__cause__)


@pytest.mark.asyncio
async def test_counted_cleanup_cancellation_preserves_detected_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    original_aclose = BatchStream.aclose

    async def controlled_aclose(stream: BatchStream) -> None:
        cleanup_started.set()
        await release_cleanup.wait()
        await original_aclose(stream)

    monkeypatch.setattr(BatchStream, "aclose", controlled_aclose)

    def handler(request: Request) -> object:
        if request.method != "batch":
            return {"result": [{"ID": 1}], "total": 2, "next": 1}
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": {key: [{"ID": 2}] for key in commands},
                "result_error": [],
                "result_total": dict.fromkeys(commands, 3),
            },
        }

    client, _transport = _client(handler)

    async def collect() -> list[JsonValue]:
        return [item async for item in client.list_batched({"method": "crm.item.list"}, batch_size=1)]

    task = asyncio.create_task(collect())
    await cleanup_started.wait()
    task.cancel()
    release_cleanup.set()

    with pytest.raises(IncompleteTraversalError) as caught:
        await task

    report = cast("OperationReport", caught.value.report)
    assert report.state is TerminalState.FAILED
    assert "total contradicts" in str(caught.value.__cause__)


@pytest.mark.asyncio
async def test_counted_wrapper_early_close_has_shared_cancelled_report() -> None:
    client, _transport = _client(lambda _request: {"result": [{"ID": 1}, {"ID": 2}], "total": 2})
    stream = client.list_batched({"method": "crm.item.list"}, batch_size=1)
    assert await anext(stream) == {"ID": 1}

    with pytest.raises(IncompleteTraversalError) as caught:
        await stream.aclose()

    report = cast("OperationReport", caught.value.report)
    assert report.state is TerminalState.CANCELLED
    assert report.physical_requests == 1
    assert report.logical_pages == 1
    assert report.emitted_rows == 1
    assert report.unique_rows == 1


@pytest.mark.asyncio
async def test_counted_wrapper_tail_cleanup_resists_concurrent_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    original_aclose = BatchStream.aclose

    async def gated_aclose(stream: BatchStream) -> None:
        entered.set()
        await release.wait()
        await original_aclose(stream)

    monkeypatch.setattr(BatchStream, "aclose", gated_aclose)

    def handler(request: Request) -> object:
        if request.method != "batch":
            return {"result": [{"ID": 1}], "total": 2, "next": 1}
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": {key: [{"ID": 2}] for key in commands},
                "result_error": [],
                "result_total": dict.fromkeys(commands, 2),
            },
        }

    client, _transport = _client(handler)
    identity = IdentitySpec(
        item_path=("ID",),
        filter_key="ID",
        order_key="ID",
        coercion=IdentityCoercion.EXACT_INTEGER,
    )

    async def consume() -> list[JsonValue]:
        return [
            item
            async for item in client.list_batched(
                {"method": "crm.item.list"},
                batch_size=1,
                identity=identity,
            )
        ]

    task = asyncio.create_task(consume())
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()

    with pytest.raises(asyncio.CancelledError) as caught:
        await task

    report = cast("OperationReport", cast("IncompleteTraversalError", caught.value).report)
    assert report.state is TerminalState.CANCELLED
    assert report.physical_requests == EXPECTED_PAGE_ROWS
    assert report.logical_pages == EXPECTED_PAGE_ROWS


@pytest.mark.asyncio
@pytest.mark.parametrize("parameters", [{"start": 99}, {"START": 99}])
async def test_counted_wrapper_refuses_caller_offset_controls_before_io(
    parameters: dict[str, int],
) -> None:
    client, transport = _client(lambda _request: {"result": []})

    with pytest.raises(CapabilityError, match="conflict"):
        _ = [
            item
            async for item in client.list_batched(
                {"method": "crm.item.list", "parameters": parameters},
            )
        ]

    assert transport.requests == []


@pytest.mark.asyncio
async def test_counted_wrapper_refuses_required_identity_before_io() -> None:
    client, transport = _client(lambda _request: {"result": []})
    policy = ExecutionPolicy(consistency=ConsistencyPolicy(identity_requirement=IdentityRequirement.REQUIRED))

    with pytest.raises(CapabilityError, match="requires IdentitySpec"):
        _ = [item async for item in client.list_batched({"method": "crm.item.list"}, policy=policy)]

    assert transport.requests == []


@pytest.mark.asyncio
async def test_counted_wrapper_required_snapshot_is_incomplete() -> None:
    client, transport = _client(lambda _request: {"result": [], "total": 0})
    policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(snapshot_requirement=SnapshotRequirement.FROZEN_MANIFEST),
    )

    with pytest.raises(IncompleteTraversalError) as caught:
        _ = [item async for item in client.list_batched({"method": "crm.item.list"}, policy=policy)]

    report = cast("OperationReport", caught.value.report)
    assert report.state is TerminalState.INCOMPLETE
    assert report.logical_pages == 1
    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_counted_wrapper_required_snapshot_report_counts_head_and_tail_rows() -> None:
    def handler(request: Request) -> object:
        if request.method != "batch":
            return {"result": [{"ID": 1}], "total": 2, "next": 1}
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": {key: [{"ID": 2}] for key in commands},
                "result_error": [],
                "result_total": dict.fromkeys(commands, 2),
            },
        }

    client, transport = _client(handler)
    policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(snapshot_requirement=SnapshotRequirement.FROZEN_MANIFEST),
    )
    delivered: list[JsonValue] = []

    async def collect() -> None:
        async for item in client.list_batched({"method": "crm.item.list"}, batch_size=1, policy=policy):
            delivered.append(item)  # noqa: PERF401 - retain rows delivered before terminal assurance

    with pytest.raises(IncompleteTraversalError) as caught:
        await collect()

    report = cast("OperationReport", caught.value.report)
    assert delivered == [{"ID": 1}, {"ID": 2}]
    assert report.state is TerminalState.INCOMPLETE
    assert report.emitted_rows == EXPECTED_PAGE_ROWS
    assert report.unique_rows == EXPECTED_PAGE_ROWS
    assert report.physical_requests == EXPECTED_PAGE_ROWS
    assert report.logical_pages == EXPECTED_PAGE_ROWS
    assert report.batch_requests == 1
    assert len(transport.requests) == EXPECTED_PAGE_ROWS


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
async def test_sequential_wrapper_does_not_trust_total_while_next_remains() -> None:
    def handler(request: Request) -> object:
        start = request.copy_parameters().get("start", 0)
        if start == 0:
            return {"result": [{"ID": 1}], "total": 1, "next": 1}
        if start == 1:
            return {"result": [{"ID": 2}], "total": 2}
        raise AssertionError("unexpected offset")

    client, transport = _client(handler)

    assert [item async for item in client.list_sequential({"method": "crm.item.list"})] == [
        {"ID": 1},
        {"ID": 2},
    ]
    assert [request.copy_parameters()["start"] for request in transport.requests] == [0, 1]


@pytest.mark.asyncio
async def test_facade_applies_exact_profile_and_preserves_report_provenance() -> None:
    transport = FunctionTransport(lambda _request: {"result": [{"ID": 1}]})
    client = Bitrix24._from_executor(  # noqa: SLF001
        Executor(transport),
        portal_build="build-1",
        scopes={"task"},
    )
    policy = ExecutionPolicy(consistency=ConsistencyPolicy(snapshot_requirement=SnapshotRequirement.FROZEN_MANIFEST))

    with pytest.raises(IncompleteTraversalError) as caught:
        _ = [
            item
            async for item in client.list_sequential(
                {"method": "tasks.task.list"},
                profile=_single_profile(),
                policy=policy,
            )
        ]

    report = cast("OperationReport", caught.value.report)
    assert report.assurance is CompletionAssurance.PROFILE_VERIFIED
    assert report.profile_id == "tasks-single-v1"
    assert report.profile_version == 1
    assert report.profile_applicable is True
    assert report.profile_source_sha256 is not None
    assert report.profile_evidence_sha256 == ("b" * 64,)
    assert report.profile_evidence_candidate_sha == "a" * 40
    assert len(transport.requests) == 1
    assert transport.requests[0].replay_safety is ReplaySafety.SAFE


@pytest.mark.asyncio
async def test_profile_never_relaxes_explicit_compatibility_page_cap() -> None:
    transport = FunctionTransport(lambda _request: {"result": [{"ID": 1}, {"ID": 2}]})
    client = Bitrix24._from_executor(  # noqa: SLF001
        Executor(transport),
        portal_build="build-1",
        scopes={"task"},
    )

    with pytest.raises(IncompleteTraversalError) as caught:
        _ = [
            item
            async for item in client.list_sequential(
                {"method": "tasks.task.list"},
                list_size=1,
                profile=_single_profile(),
            )
        ]

    report = cast("OperationReport", caught.value.report)
    assert report.emitted_rows == 0
    assert "declared page cap" in str(caught.value.__cause__)
    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_explicit_list_size_narrows_the_profile_plan_request_before_io() -> None:
    expected_rows = 20
    base_profile = _single_profile()
    document = cast(
        "dict[str, Any]",
        json.loads(cast("str", base_profile._source_document)),  # noqa: SLF001 - immutable profile fixture
    )
    document["plan"] = {
        "kind": "offset_sequential",
        "identity_requirement": "optional",
        "order_semantics": "unordered",
        "duplicate_policy": "report",
        "total_semantics": "advisory",
        "offset_path": ["start"],
        "limit_path": ["LIMIT"],
        "requested_page_size": 50,
        "continuation": "server_next_or_observed_count",
        "terminal": ["empty_page"],
        "allow_create_controls": True,
    }
    cast("dict[str, object]", document["capabilities"])["offset_honored"] = True
    profile = load_profile_document(document)

    def handler(request: Request) -> object:
        parameters = request.copy_parameters()
        start = cast("int", parameters.get("start", 0))
        limit = cast("int", parameters["LIMIT"])
        rows = [{"ID": value} for value in range(start + 1, min(start + limit, expected_rows) + 1)]
        return {"result": rows}

    transport = FunctionTransport(handler)
    client = Bitrix24._from_executor(  # noqa: SLF001
        Executor(transport),
        portal_build="build-1",
        scopes={"task"},
    )

    rows = [
        item
        async for item in client.list_sequential(
            {"method": "tasks.task.list"},
            list_size=10,
            profile=profile,
        )
    ]

    assert len(rows) == expected_rows
    assert [request.copy_parameters()["LIMIT"] for request in transport.requests] == [10, 10, 10]


@pytest.mark.asyncio
async def test_facade_refuses_profile_replay_safety_conflict_before_io() -> None:
    client, transport = _client(lambda _request: {"result": []})

    with pytest.raises(CapabilityError, match="replay safety contradicts"):
        _ = [
            item
            async for item in client.list_sequential(
                {"method": "tasks.task.list", "replay_safety": "unsafe"},
                profile=_single_profile(),
            )
        ]

    assert transport.requests == []


@pytest.mark.asyncio
async def test_facade_refuses_profile_with_unobserved_required_probe_before_io() -> None:
    transport = FunctionTransport(lambda _request: {"result": []})
    client = Bitrix24._from_executor(  # noqa: SLF001
        Executor(transport),
        portal_build="build-1",
        scopes={"task"},
    )
    profile = _single_profile(
        required_probes=[
            {
                "probe_id": "must-run",
                "method": "tasks.task.list",
                "max_rows": 1,
                "selector": [],
                "minimal_select": ["ID"],
                "within_caller_filter": True,
            },
        ],
    )

    with pytest.raises(CapabilityError, match="probe_missing"):
        _ = [
            item
            async for item in client.list_sequential(
                {"method": "tasks.task.list"},
                profile=profile,
            )
        ]

    assert transport.requests == []


@pytest.mark.asyncio
async def test_facade_refuses_profile_whose_semantics_no_longer_match_source() -> None:
    client, transport = _client(lambda _request: {"result": [{"ID": 1}], "next": 1})
    original = _single_profile()
    assert isinstance(original.plan, SingleResponsePlan)
    forged = replace(original, plan=replace(original.plan, reject_continuation=False))

    with pytest.raises(CapabilityError, match="provenance_mismatch"):
        _ = [
            item
            async for item in client.list_sequential(
                {"method": "tasks.task.list"},
                profile=forged,
            )
        ]

    assert forged.source_sha256 == original.source_sha256
    assert transport.requests == []


@pytest.mark.asyncio
async def test_reference_wrapper_refuses_profile_without_batch_capability_before_source_pull() -> None:
    transport = FunctionTransport(lambda _request: {"result": []})
    client = Bitrix24._from_executor(  # noqa: SLF001
        Executor(transport),
        portal_build="build-1",
        scopes={"task"},
    )
    pulled = False

    async def updates() -> AsyncGenerator[dict[str, object]]:
        nonlocal pulled
        pulled = True
        yield {"OWNER_ID": 1}

    with pytest.raises(CapabilityError, match="does not authorize batch"):
        _ = [
            item
            async for item in client.reference_batched_no_count(
                {"method": "tasks.task.list"},
                updates(),
                profile=_single_profile(),
            )
        ]

    assert not pulled
    assert transport.requests == []


@pytest.mark.asyncio
async def test_reference_wrapper_refuses_explicit_selector_outside_profile_before_io() -> None:
    transport = FunctionTransport(lambda _request: {"result": {"items": []}})
    client = Bitrix24._from_executor(  # noqa: SLF001
        Executor(transport),
        portal_build="build-1",
        scopes={"task"},
    )

    with pytest.raises(CapabilityError, match="query_shape_mismatch"):
        _ = [
            item
            async for item in client.reference_cursor_no_count(
                {"method": "tasks.task.list"},
                [{"OWNER_ID": 1}],
                result_key="items",
                profile=_single_profile(),
            )
        ]

    assert transport.requests == []


@pytest.mark.asyncio
async def test_profile_refuses_ignored_nondefault_legacy_cursor_controls_before_io() -> None:
    transport = FunctionTransport(lambda _request: {"result": []})
    client = Bitrix24._from_executor(  # noqa: SLF001
        Executor(transport),
        portal_build="build-1",
        scopes={"task"},
    )

    with pytest.raises(CapabilityError, match="cursor controls contradict"):
        _ = [
            item
            async for item in client.reference_cursor_no_count(
                {"method": "tasks.task.list"},
                [{"OWNER_ID": 1}],
                cursor_take="min",
                profile=_single_profile(),
            )
        ]

    assert transport.requests == []


@pytest.mark.asyncio
async def test_profile_refuses_ignored_nondefault_legacy_identity_key_before_io() -> None:
    transport = FunctionTransport(lambda _request: {"result": []})
    client = Bitrix24._from_executor(  # noqa: SLF001
        Executor(transport),
        portal_build="build-1",
        scopes={"task"},
    )

    with pytest.raises(CapabilityError, match="id_key contradicts"):
        _ = [
            item
            async for item in client.list_batched_no_count(
                {"method": "tasks.task.list"},
                id_key="custom_id",
                profile=_single_profile(),
            )
        ]

    assert transport.requests == []


@pytest.mark.asyncio
async def test_profile_refuses_explicit_default_identity_key_before_io() -> None:
    transport = FunctionTransport(lambda _request: {"result": []})
    client = Bitrix24._from_executor(  # noqa: SLF001
        Executor(transport),
        portal_build="build-1",
        scopes={"task"},
    )

    with pytest.raises(CapabilityError, match="id_key contradicts"):
        _ = [
            item
            async for item in client.list_batched_no_count(
                {"method": "tasks.task.list"},
                id_key="ID",
                profile=_single_profile(),
            )
        ]

    assert transport.requests == []


@pytest.mark.asyncio
async def test_explicit_identity_key_must_match_explicit_plan_identity_before_io() -> None:
    client, transport = _client(lambda _request: {"result": []})
    identity = IdentitySpec(
        item_path=("id",),
        filter_key="ID",
        order_key="id",
        coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
    )

    with pytest.raises(CapabilityError, match="id_key contradicts"):
        _ = [
            item
            async for item in client.list_batched_no_count(
                {"method": "tasks.task.list"},
                id_key="CUSTOM",
                plan=KeysetPlan(
                    identity_requirement=IdentityRequirement.REQUIRED,
                    order_semantics=OrderSemantics.ASCENDING,
                ),
                identity=identity,
            )
        ]

    assert transport.requests == []


@pytest.mark.asyncio
async def test_profile_refuses_explicit_default_cursor_controls_before_io() -> None:
    transport = FunctionTransport(lambda _request: {"result": []})
    client = Bitrix24._from_executor(  # noqa: SLF001
        Executor(transport),
        portal_build="build-1",
        scopes={"task"},
    )

    with pytest.raises(CapabilityError, match="cursor controls contradict"):
        _ = [
            item
            async for item in client.reference_cursor_no_count(
                {"method": "tasks.task.list"},
                [{"OWNER_ID": 1}],
                cursor_param="LAST_ID",
                profile=_single_profile(),
            )
        ]

    assert transport.requests == []


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
            results[key] = [{"ID": cursor + 1, "OWNER_ID": owner}] if cursor < EXPECTED_PAGE_ROWS else []
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
async def test_reference_cursor_wrapper_uses_empty_confirmation_not_short_page() -> None:
    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        results: dict[str, object] = {}
        for key, command in commands.items():
            assert isinstance(command, str)
            query = parse_qs(urlsplit(command).query)
            results[key] = {"items": [] if "LAST_ID" in query else [{"id": 10}]}
        return {
            "result": {
                "result": results,
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
    assert len(transport.requests) == EXPECTED_PAGE_ROWS


@pytest.mark.asyncio
async def test_reference_cursor_min_preserves_committed_descending_direction() -> None:
    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        results: dict[str, object] = {}
        for key, command in commands.items():
            assert isinstance(command, str)
            query = parse_qs(urlsplit(command).query)
            results[key] = [] if "LAST_ID" in query else [{"id": 3}, {"id": 2}]
        return {
            "result": {
                "result": results,
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

    assert {name: tuple(inspect.signature(getattr(Bitrix24, name)).parameters) for name in expected} == expected
    assert inspect.signature(Bitrix24.list_batched_no_count).parameters["id_key"].default == "ID"
    cursor_signature = inspect.signature(Bitrix24.reference_cursor_no_count)
    assert cursor_signature.parameters["cursor_param"].default == "LAST_ID"
    assert cursor_signature.parameters["cursor_field"].default == "id"
    assert cursor_signature.parameters["cursor_take"].default == "max"
    assert cursor_signature.parameters["list_size_param"].default == "LIMIT"


def test_facade_derives_profile_query_paths_from_nested_keyset_plan() -> None:
    profile = replace(
        _single_profile(),
        plan=KeysetPlan(
            selector=ResultSelector.root(),
            filter_path=ParameterPath(("params", "filter")),
            order_path=ParameterPath(("params", "order")),
            identity_requirement=IdentityRequirement.REQUIRED,
            order_semantics=OrderSemantics.ASCENDING,
        ),
        identity=IdentitySpec(
            item_path=("ID",),
            filter_key="ID",
            order_key="ID",
            coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
        ),
        capabilities=CapabilitySet(filter_honored=True, stable_order=True, fixed_page_cap=True),
    )

    assert facade_module._profile_query_paths(profile) == (  # noqa: SLF001
        ParameterPath(("params", "filter")),
        ParameterPath(("params", "order")),
    )


def test_settings_reject_batch_size_above_portal_cap() -> None:
    with pytest.raises(ValueError, match="less than or equal to 50"):
        Settings.model_validate(
            {"webhook_url": "https://bitrix24.com/rest/0/test/", "batch_size": 51},
        )

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
    counted_adapter = ast.parse(
        textwrap.dedent(inspect.getsource(facade_module.Bitrix24._list_batched_default)),  # noqa: SLF001
    )
    counted_calls = {
        node.func.attr
        for node in ast.walk(counted_adapter)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert "execute" not in counted_calls
    assert "counted_batch_pages" in counted_calls
