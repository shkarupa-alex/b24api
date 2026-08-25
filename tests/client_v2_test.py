"""Characterization of the thin v2 facade and public stream lifecycle."""

from __future__ import annotations
import json
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlsplit

import pytest

from b24api.client import Bitrix24
from b24api.contracts import (
    BatchDispatch,
    Binding,
    Command,
    CommandFailure,
    CommandNotExecuted,
    CommandSuccess,
    CursorSpec,
    DeliveryOrder,
    DirectDispatch,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    ParameterPath,
    ParameterUpdate,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ReplaySafety,
    Request,
    ResultCollectionShape,
    ResultSelector,
    SequentialTraversal,
    TerminalState,
    TraversalAssurance,
)
from b24api.error import BatchFailed, IncompleteTraversalError, InputSourceError, ReferenceFailed
from b24api.execution import Executor, Transport, WireResponse
from b24api.settings import Settings

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

HTTP_OK = 200
COUNTED_ROWS = 500
EXPECTED_COUNTED_REQUESTS = 2
PAGE_SIZE = 50
LOGICAL_BATCH_COMMANDS = 63
LOGICAL_BATCH_SIZE = 7
LOGICAL_BATCH_REQUESTS = 9
SMALL_BATCH_COMMANDS = 5
FAIL_FAST_WINDOW = 4
FAIL_FAST_HALTED = 3
PARTIAL_WINDOW = 2
REFERENCE_BINDINGS = 2
REFERENCE_EVENTS = 4
FANOUT_COMMANDS = 60
FANOUT_BATCH_SIZE = 10
FANOUT_BATCH_REQUESTS = 6


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


def _client(transport: Transport) -> Bitrix24:
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


@pytest.mark.asyncio
async def test_logical_batch_is_unbounded_ordered_and_correlation_is_strictly_off_wire() -> None:
    correlations = [["private", index] for index in range(LOGICAL_BATCH_COMMANDS)]

    def handler(request: Request) -> object:
        parameters = request.copy_parameters()
        assert parameters["halt"] == 1
        commands = parameters["cmd"]
        assert isinstance(commands, dict)
        serialized = json.dumps(parameters)
        assert "private" not in serialized
        return {
            "result": {
                "result": {key: {"index": int(key[1:])} for key in commands},
                "result_error": {},
            },
        }

    transport = FunctionTransport(handler)
    client = _client(transport)
    stream = client.batch(
        (
            Command(Request("test.get", {"index": index}, ReplaySafety.SAFE), correlations[index])
            for index in range(LOGICAL_BATCH_COMMANDS)
        ),
        batch_size=LOGICAL_BATCH_SIZE,
    )

    outcomes = [outcome async for outcome in stream]

    assert [outcome.index for outcome in outcomes] == list(range(LOGICAL_BATCH_COMMANDS))
    assert all(isinstance(outcome, CommandSuccess) for outcome in outcomes)
    assert all(outcome.correlation is correlations[outcome.index] for outcome in outcomes)
    assert "private" not in repr(outcomes[0])
    assert len(transport.requests) == LOGICAL_BATCH_REQUESTS
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.batch_commands == LOGICAL_BATCH_COMMANDS
    assert stream.report.buffered_commands_high_water == LOGICAL_BATCH_SIZE


@pytest.mark.asyncio
async def test_batch_outcomes_retains_typed_failure_without_halting_later_commands() -> None:
    failed_index = 2

    def handler(request: Request) -> object:
        parameters = request.copy_parameters()
        assert parameters["halt"] == 0
        commands = parameters["cmd"]
        assert isinstance(commands, dict)
        errors = {
            key: {"error": "denied", "error_description": "no access"}
            for key in commands
            if int(key[1:]) == failed_index
        }
        results = {key: int(key[1:]) for key in commands if key not in errors}
        return {"result": {"result": results, "result_error": errors}}

    stream = _client(FunctionTransport(handler)).batch_outcomes(
        [Command(Request("test.get", replay_safety=ReplaySafety.SAFE), index) for index in range(SMALL_BATCH_COMMANDS)],
        batch_size=SMALL_BATCH_COMMANDS,
    )
    outcomes = [outcome async for outcome in stream]

    assert isinstance(outcomes[failed_index], CommandFailure)
    assert all(isinstance(outcome, CommandSuccess) for index, outcome in enumerate(outcomes) if index != failed_index)
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED_WITH_FAILURES
    assert stream.report.successes == SMALL_BATCH_COMMANDS - 1
    assert stream.report.failures == 1


@pytest.mark.asyncio
async def test_fail_fast_batch_raises_bounded_window_after_preceding_successes() -> None:
    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        keys = list(commands)
        return {
            "result": {
                "result": {keys[0]: 0},
                "result_error": {keys[1]: {"error": "denied", "error_description": "stop"}},
            },
        }

    stream = _client(FunctionTransport(handler)).batch(
        [Command(Request("test.get", replay_safety=ReplaySafety.SAFE), index) for index in range(SMALL_BATCH_COMMANDS)],
        batch_size=SMALL_BATCH_COMMANDS,
    )

    first = await anext(stream)
    with pytest.raises(BatchFailed) as captured:
        await anext(stream)

    assert isinstance(first, CommandSuccess)
    assert isinstance(captured.value.outcomes[0], CommandFailure)
    assert all(isinstance(outcome, CommandNotExecuted) for outcome in captured.value.outcomes[1:])
    assert len(captured.value.outcomes) == FAIL_FAST_WINDOW
    assert stream.report is captured.value.report
    assert stream.report is not None
    assert stream.report.state is TerminalState.FAILED
    assert stream.report.successes == 1
    assert stream.report.failures == 1
    assert stream.report.not_executed == FAIL_FAST_HALTED


@pytest.mark.asyncio
async def test_batch_source_failure_does_not_dispatch_partial_window_and_closes_exact_source() -> None:
    closed = False

    async def commands() -> AsyncGenerator[Command[str]]:
        nonlocal closed
        try:
            yield Command(Request("test.get", replay_safety=ReplaySafety.SAFE), "a")
            yield Command(Request("test.get", replay_safety=ReplaySafety.SAFE), "b")
            raise RuntimeError("source broke")
        finally:
            closed = True

    transport = FunctionTransport(lambda _request: pytest.fail("partial window must not be dispatched"))
    stream = _client(transport).batch_outcomes(commands(), batch_size=7)

    first = await anext(stream)
    second = await anext(stream)
    with pytest.raises(InputSourceError):
        await anext(stream)

    assert isinstance(first, CommandNotExecuted)
    assert isinstance(second, CommandNotExecuted)
    assert closed
    assert transport.requests == []
    assert stream.report is not None
    assert stream.report.not_executed == PARTIAL_WINDOW


@pytest.mark.asyncio
async def test_bound_references_apply_nested_updates_off_wire_and_emit_exact_completion() -> None:
    correlations = [["owner-private", 1], ["owner-private", 2]]

    def handler(request: Request) -> object:
        parameters = request.copy_parameters()
        assert "owner-private" not in json.dumps(parameters)
        filters = parameters["filter"]
        assert isinstance(filters, dict)
        owner = filters["OWNER"]
        assert isinstance(owner, int)
        return {"result": [] if parameters["start"] else [{"ID": owner * 10}]}

    transport = FunctionTransport(handler)
    stream = _client(transport).iter_references(
        Request("test.list", replay_safety=ReplaySafety.SAFE),
        [
            Binding(
                f"owner-{owner}",
                (ParameterUpdate(ParameterPath(("filter", "OWNER")), owner),),
                correlations[owner - 1],
            )
            for owner in (1, 2)
        ],
        traversal=SequentialTraversal(identity=_identity()),
        dispatch=DirectDispatch(concurrency=2, output_order=DeliveryOrder.INPUT),
    )

    events = [event async for event in stream]

    assert [type(event) for event in events] == [ReferenceItem, ReferenceComplete] * 2
    items = [event for event in events if isinstance(event, ReferenceItem)]
    completions = [event for event in events if isinstance(event, ReferenceComplete)]
    assert [item.item for item in items] == [{"ID": 10}, {"ID": 20}]
    assert all(item.correlation is correlations[item.binding_index] for item in items)
    assert all(complete.row_count == 1 for complete in completions)
    assert "owner-private" not in repr(items[0])
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.admitted == REFERENCE_BINDINGS
    assert stream.report.emitted == REFERENCE_EVENTS
    assert stream.report.successes == REFERENCE_BINDINGS
    assert stream.report.active_references_high_water == REFERENCE_BINDINGS


@pytest.mark.asyncio
async def test_binding_collision_rejects_before_reference_io() -> None:
    transport = FunctionTransport(lambda _request: pytest.fail("binding collision must reject before I/O"))
    stream = _client(transport).iter_references(
        Request("test.list", replay_safety=ReplaySafety.SAFE),
        [Binding("bad", (ParameterUpdate(ParameterPath(("start",)), 100),), object())],
        traversal=SequentialTraversal(),
    )

    with pytest.raises(InputSourceError):
        await anext(stream)

    assert transport.requests == []
    assert stream.report is not None
    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_tolerant_reference_failure_has_no_false_completion_and_later_binding_runs() -> None:
    def handler(request: Request) -> object:
        parameters = request.copy_parameters()
        filters = parameters["filter"]
        assert isinstance(filters, dict)
        owner = filters["OWNER"]
        if owner == 1:
            return {"error": "denied", "error_description": "no access"}
        return {"result": []}

    stream = _client(FunctionTransport(handler)).iter_reference_outcomes(
        Request("test.list", replay_safety=ReplaySafety.SAFE),
        [
            Binding(
                f"owner-{owner}",
                (ParameterUpdate(ParameterPath(("filter", "OWNER")), owner),),
                owner,
            )
            for owner in (1, 2)
        ],
        traversal=SequentialTraversal(),
        dispatch=DirectDispatch(concurrency=1, output_order=DeliveryOrder.INPUT),
    )

    events = [event async for event in stream]

    assert isinstance(events[0], ReferenceFailure)
    assert events[0].binding_index == 0
    assert isinstance(events[1], ReferenceComplete)
    assert events[1].binding_index == 1
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED_WITH_FAILURES
    assert stream.report.failures == 1
    assert stream.report.successes == 1


@pytest.mark.asyncio
async def test_fail_fast_reference_raises_bounded_reference_failed() -> None:
    transport = FunctionTransport(lambda _request: {"error": "denied", "error_description": "no access"})
    stream = _client(transport).iter_references(
        Request("test.list", replay_safety=ReplaySafety.SAFE),
        [Binding("one", (), {"private": True})],
        traversal=SequentialTraversal(),
        dispatch=DirectDispatch(concurrency=1),
    )

    with pytest.raises(ReferenceFailed) as captured:
        await anext(stream)

    assert len(captured.value.outcomes) == 1
    assert isinstance(captured.value.outcomes[0], ReferenceFailure)
    assert stream.report is captured.value.report


@pytest.mark.asyncio
async def test_direct_fanout_preserves_full_response_without_treating_it_as_traversal() -> None:
    transport = FunctionTransport(
        lambda request: {
            "result": [request.copy_parameters()["value"]],
            "total": 100,
            "next": 50,
        },
    )
    correlations = [{"private": index} for index in range(3)]
    stream = _client(transport).fan_out(
        [
            Command(
                Request("test.get", {"value": index}, ReplaySafety.SAFE),
                correlations[index],
            )
            for index in range(3)
        ],
        dispatch=DirectDispatch(concurrency=2, output_order=DeliveryOrder.INPUT),
    )

    outcomes = [outcome async for outcome in stream]

    assert [outcome.result for outcome in outcomes] == [[0], [1], [2]]
    assert [outcome.response.total for outcome in outcomes] == [100, 100, 100]
    assert [outcome.response.next for outcome in outcomes] == [50, 50, 50]
    assert all(outcome.correlation is correlations[outcome.index] for outcome in outcomes)
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.active_references_high_water == REFERENCE_BINDINGS


@pytest.mark.asyncio
async def test_batch_fanout_spans_physical_windows_and_preserves_global_correlation() -> None:
    def handler(request: Request) -> object:
        assert request.method == "batch"
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": {key: {"key": key} for key in commands},
                "result_error": {},
            },
        }

    transport = FunctionTransport(handler)
    stream = _client(transport).fan_out(
        [Command(Request("test.get", {"value": index}, ReplaySafety.SAFE), index) for index in range(FANOUT_COMMANDS)],
        dispatch=BatchDispatch(
            batch_size=FANOUT_BATCH_SIZE,
            concurrency=2,
            output_order=DeliveryOrder.READY,
        ),
    )

    outcomes = [outcome async for outcome in stream]

    assert sorted(outcome.index for outcome in outcomes) == list(range(FANOUT_COMMANDS))
    assert sorted(outcome.correlation for outcome in outcomes) == list(range(FANOUT_COMMANDS))
    assert len(transport.requests) == FANOUT_BATCH_REQUESTS
    assert stream.report is not None
    assert stream.report.batch_requests == FANOUT_BATCH_REQUESTS
    assert stream.report.batch_commands == FANOUT_COMMANDS


@pytest.mark.asyncio
async def test_tolerant_fanout_continues_after_one_direct_failure() -> None:
    def handler(request: Request) -> object:
        value = request.copy_parameters()["value"]
        if value == 1:
            return {"error": "denied", "error_description": "no access"}
        return {"result": value}

    stream = _client(FunctionTransport(handler)).fan_out_outcomes(
        [Command(Request("test.get", {"value": index}, ReplaySafety.SAFE), index) for index in range(3)],
        dispatch=DirectDispatch(concurrency=2, output_order=DeliveryOrder.INPUT),
    )

    outcomes = [outcome async for outcome in stream]

    assert isinstance(outcomes[0], CommandSuccess)
    assert isinstance(outcomes[1], CommandFailure)
    assert isinstance(outcomes[2], CommandSuccess)
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED_WITH_FAILURES


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
async def test_counted_traversal_preserves_frozen_request_shape_and_exact_identity() -> None:
    identities = tuple(range(1, COUNTED_ROWS + 1))

    def page(start: int) -> tuple[list[dict[str, int]], int | None]:
        rows = [{"ID": identity} for identity in identities[start : start + PAGE_SIZE]]
        next_offset = start + PAGE_SIZE if start + PAGE_SIZE < len(identities) else None
        return rows, next_offset

    def handler(request: Request) -> object:
        if request.method == "test.list":
            raw_start = request.copy_parameters().get("start", 0)
            assert isinstance(raw_start, int)
            rows, next_offset = page(raw_start)
            return {"result": rows, "total": len(identities), "next": next_offset}
        parameters = request.copy_parameters()
        commands = parameters["cmd"]
        assert isinstance(commands, dict)
        results: dict[str, object] = {}
        totals: dict[str, int] = {}
        continuations: dict[str, int] = {}
        for key, raw_query in commands.items():
            assert isinstance(raw_query, str)
            query = parse_qs(urlsplit(raw_query).query)
            start = int(query["start"][0])
            rows, next_offset = page(start)
            results[key] = rows
            totals[key] = len(identities)
            if next_offset is not None:
                continuations[key] = next_offset
        return {
            "result": {
                "result": results,
                "result_error": {},
                "result_total": totals,
                "result_next": continuations,
            },
        }

    portal = FunctionTransport(handler)
    stream = _client(portal).iter_list_counted(
        Request("test.list", replay_safety=ReplaySafety.SAFE),
        identity=_identity(),
    )

    rows = [item async for item in stream]

    assert tuple(row["ID"] for row in rows if isinstance(row, dict)) == identities
    assert len(portal.requests) == EXPECTED_COUNTED_REQUESTS
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.physical_requests == EXPECTED_COUNTED_REQUESTS
    assert stream.report.batch_requests == 1


@pytest.mark.asyncio
async def test_counted_missing_in_band_stride_fails_incomplete() -> None:
    transport = FunctionTransport(lambda _request: {"result": [{"ID": 1}], "total": 2})
    stream = _client(transport).iter_list_counted(
        Request("test.list", replay_safety=ReplaySafety.SAFE),
        identity=_identity(),
    )

    with pytest.raises(IncompleteTraversalError, match="did not complete") as captured:
        await anext(stream)

    assert type(captured.value).__name__ == "IncompleteTraversalError"
    assert stream.report is not None
    assert stream.report.state is TerminalState.INCOMPLETE


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
