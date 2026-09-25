"""Characterization of the thin v2 facade and public stream lifecycle."""

from __future__ import annotations
import asyncio
import json
import warnings
from typing import TYPE_CHECKING, cast
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
    CountedTraversal,
    CursorSpec,
    DeliveryOrder,
    DirectDispatch,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    NotExecutedReason,
    OffsetSpec,
    ParameterPath,
    ParameterUpdate,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ReferenceNotExecuted,
    ReferenceOutcomeUnknown,
    ReplayDisposition,
    ReplaySafety,
    Request,
    ResultCollectionShape,
    ResultSelector,
    RetryPolicy,
    SequentialKeysetExecution,
    SequentialTraversal,
    TerminalState,
    TraversalAssurance,
)
from b24api.contracts.completion import PageAcknowledged, PageScheduled
from b24api.contracts.request import RouteKind
from b24api.errors import (
    AmbiguousExecutionError,
    BatchCommandError,
    BatchFailed,
    BudgetExceededError,
    CapabilityError,
    FailurePhase,
    IncompleteTraversalError,
    InputSourceError,
    ReferenceFailed,
    TransportError,
)
from b24api.settings import Settings
from tests.scripting import ResponderTransport, attached_report, client_for

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator

    from b24api.execution import Transport, WireResponse
    from tests.scripting import ClientFactory

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
LARGE_COUNTED_ROWS = 100_001
LARGE_LOGICAL_BATCH_COMMANDS = 100_000
LARGE_LOGICAL_BATCH_REQUESTS = 2_000
BOUNDED_LOOKAHEAD_COMMANDS = 2_000
CUSTOM_INITIAL_OFFSET = 10
CUSTOM_NEXT_OFFSET = 12


class BlockingCloseTransport(ResponderTransport):
    """Transport whose owned close can be cancelled while it is in flight."""

    def __init__(self) -> None:
        """Create deterministic close synchronization points."""
        super().__init__(lambda _request: {"result": None})
        self.close_started = asyncio.Event()
        self.release_close = asyncio.Event()

    async def aclose(self) -> None:
        """Wait until the test permits the owned close to finish."""
        self.close_started.set()
        await self.release_close.wait()
        self.closed = True


class FailingCloseTransport(ResponderTransport):
    """Transport that records close and then reports an owned cleanup failure."""

    async def aclose(self) -> None:
        """Fail after proving that close was attempted."""
        self.closed = True
        raise RuntimeError("owned transport cleanup failed")


def _ambiguous() -> ResponderTransport:
    """Fail after possible dispatch without exposing caller-owned values."""
    return ResponderTransport(
        lambda _request: TransportError("ambiguous synthetic dispatch", phase=FailurePhase.DISPATCH_STARTED)
    )


class BlockingRequestTransport(ResponderTransport):
    """Expose cancellation of one in-flight public stream pull."""

    def __init__(self) -> None:
        """Create deterministic request synchronization points."""
        super().__init__(lambda _request: None)
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Block until the owner closes or cancels the active operation."""
        del attempt_timeout, max_response_bytes
        self.requests.append(request)
        self.started.set()
        try:
            await asyncio.Future[None]()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


def _client(transport: Transport) -> Bitrix24:
    return client_for(transport)


def _identity() -> IdentitySpec:
    return IdentitySpec(
        item_path=("ID",),
        filter_key="ID",
        order_key="ID",
        coercion=IdentityCoercion.EXACT_INTEGER,
    )


@pytest.mark.asyncio
async def test_call_and_call_response_have_stable_detached_types() -> None:
    transport = ResponderTransport(lambda _request: {"result": {"items": [1, 2]}})
    client = _client(transport)

    decoded = await client.call(Request("test.get", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE))
    response = await client.call_response(Request("test.get", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE))

    assert decoded == {"items": [1, 2]}
    assert response.result == decoded
    assert response.result is not decoded


@pytest.mark.asyncio
async def test_closed_request_mapping_canonicalizes_immediately_and_rejects_unknown_fields() -> None:
    transport = ResponderTransport(lambda request: {"result": request.copy_parameters()})
    client = _client(transport)

    result = await client.call(
        {"method": "test.get", "route": RouteKind.BARE, "parameters": {"ID": 7}, "replay_safety": ReplaySafety.SAFE},
    )

    assert result == {"ID": 7}
    with pytest.raises(ValueError, match="unknown request fields"):
        await client.call({"method": "test.get", "route": RouteKind.BARE, "unexpected": True})  # type: ignore[typeddict-unknown-key]
    with pytest.raises(TypeError, match="must be a ReplaySafety"):
        await client.call({"method": "test.get", "route": RouteKind.BARE, "replay_safety": "safe"})  # type: ignore[typeddict-item]
    assert len(transport.requests) == 1


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

    transport = ResponderTransport(handler)
    client = _client(transport)
    stream = client.batch(
        (
            Command(Request("test.get", {"index": index}, ReplaySafety.SAFE, route=RouteKind.BARE), correlations[index])
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
async def test_public_counted_traversal_above_100k_uses_declared_identity_budget() -> None:
    def page(start: int) -> list[dict[str, int]]:
        return [{"ID": value} for value in range(start, min(start + PAGE_SIZE, LARGE_COUNTED_ROWS))]

    def handler(request: Request) -> object:
        if request.method == "test.list":
            start = int(request.copy_parameters().get("start", 0))
            return {"result": page(start), "total": LARGE_COUNTED_ROWS, "next": start + PAGE_SIZE}
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        results: dict[str, object] = {}
        totals: dict[str, int] = {}
        continuations: dict[str, int] = {}
        for key, encoded in commands.items():
            assert isinstance(encoded, str)
            parameters = parse_qs(urlsplit(encoded).query)
            start = int(parameters.get("start", ["0"])[0])
            results[key] = page(start)
            totals[key] = LARGE_COUNTED_ROWS
            if start + PAGE_SIZE < LARGE_COUNTED_ROWS:
                continuations[key] = start + PAGE_SIZE
        return {
            "result": {
                "result": results,
                "result_error": {},
                "result_total": totals,
                "result_next": continuations,
            },
        }

    transport = ResponderTransport(handler)
    client = _client(transport)
    stream = client.iter_list_counted(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        identity=_identity(),
        policy=ExecutionPolicy(max_identity_keys=LARGE_COUNTED_ROWS),
    )

    async def consume() -> int:
        count = 0
        async for row in stream:
            assert row == {"ID": count}
            count += 1
        return count

    count = await consume()
    assert count == LARGE_COUNTED_ROWS
    assert stream.report is not None
    assert stream.report.unique_rows == LARGE_COUNTED_ROWS
    assert stream.report.assurance is TraversalAssurance.IDENTITY_AND_COUNT_MATCHED


@pytest.mark.asyncio
async def test_counted_identity_budget_stops_before_admitting_an_overflow_page() -> None:
    def handler(request: Request) -> object:
        if request.method != "batch":
            return {
                "result": [{"ID": index} for index in range(PAGE_SIZE)],
                "total": PAGE_SIZE * 2,
                "next": PAGE_SIZE,
            }
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        key = next(iter(commands))
        return {
            "result": {
                "result": {key: [{"ID": index} for index in range(PAGE_SIZE, PAGE_SIZE * 2)]},
                "result_error": {},
                "result_total": {key: PAGE_SIZE * 2},
            },
        }

    stream = _client(ResponderTransport(handler)).iter_list_counted(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        identity=_identity(),
        policy=ExecutionPolicy(max_identity_keys=PAGE_SIZE + 1),
    )

    assert len([await anext(stream) for _index in range(PAGE_SIZE)]) == PAGE_SIZE
    with pytest.raises(IncompleteTraversalError) as captured:
        await anext(stream)

    assert isinstance(captured.value.error, BudgetExceededError)
    assert stream.report is not None
    assert stream.report.state is TerminalState.INCOMPLETE
    assert stream.report.emitted == PAGE_SIZE


@pytest.mark.asyncio
async def test_public_keyset_above_100k_uses_monotonic_progression_without_identity_set() -> None:
    page_size = 2_500

    def handler(request: Request) -> object:
        parameters = request.copy_parameters()
        assert parameters["start"] == -1
        assert parameters["limit"] == page_size
        assert parameters["order"] == {"ID": "ASC"}
        raw_filter = parameters.get("filter", {})
        assert isinstance(raw_filter, dict)
        boundary = int(raw_filter.get(">ID", -1))
        start = boundary + 1
        return {
            "result": [{"ID": value} for value in range(start, min(start + page_size, LARGE_COUNTED_ROWS))],
        }

    transport = ResponderTransport(handler)
    client = _client(transport)
    stream = client.iter_list_keyset(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=page_size,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=SequentialKeysetExecution(),
        policy=ExecutionPolicy(
            max_requests=100,
            max_pages=100,
            max_buffered_rows=page_size,
        ),
    )

    count = 0
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        async for row in stream:
            assert row == {"ID": count}
            count += 1

    assert count == LARGE_COUNTED_ROWS
    assert not [warning for warning in captured if issubclass(warning.category, RuntimeWarning)]
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.unique_rows == LARGE_COUNTED_ROWS
    assert stream.report.assurance is TraversalAssurance.IDENTITY_EXACT


@pytest.mark.asyncio
async def test_logical_batch_pulls_its_generator_with_bounded_lookahead() -> None:
    """The input is never materialized: at every outcome the source is ahead by at most one window."""
    pulled = 0

    def commands() -> Iterator[Command[int]]:
        nonlocal pulled
        for index in range(BOUNDED_LOOKAHEAD_COMMANDS):
            pulled += 1
            yield Command(Request("test.get", {"index": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)

    def handler(request: Request) -> object:
        batch = request.copy_parameters()["cmd"]
        assert isinstance(batch, dict)
        return {"result": {"result": {key: int(key[1:]) for key in batch}, "result_error": {}}}

    stream = _client(ResponderTransport(handler)).batch(commands(), batch_size=PAGE_SIZE)
    count = 0
    lookahead = 0
    async for outcome in stream:
        assert outcome.correlation == count
        count += 1
        lookahead = max(lookahead, pulled - count)

    assert count == BOUNDED_LOOKAHEAD_COMMANDS
    # One physical window of batch_size commands is pulled before its outcomes are yielded.
    assert lookahead <= PAGE_SIZE
    assert stream.report is not None
    assert stream.report.buffered_commands_high_water == PAGE_SIZE


@pytest.mark.slow
@pytest.mark.asyncio
async def test_public_logical_batch_accepts_100k_generator_without_input_materialization() -> None:
    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": {key: int(key[1:]) for key in commands},
                "result_error": {},
            },
        }

    transport = ResponderTransport(handler)
    client = _client(transport)
    stream = client.batch(
        (
            Command(Request("test.get", {"index": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)
            for index in range(LARGE_LOGICAL_BATCH_COMMANDS)
        ),
        batch_size=PAGE_SIZE,
        policy=ExecutionPolicy(max_requests=LARGE_LOGICAL_BATCH_REQUESTS + 1),
    )
    count = 0
    async for outcome in stream:
        assert outcome.correlation == count
        count += 1

    assert count == LARGE_LOGICAL_BATCH_COMMANDS
    assert len(transport.requests) == LARGE_LOGICAL_BATCH_REQUESTS
    assert stream.report is not None
    assert stream.report.buffered_commands_high_water == PAGE_SIZE


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

    stream = _client(ResponderTransport(handler)).batch_outcomes(
        [
            Command(Request("test.get", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE), index)
            for index in range(SMALL_BATCH_COMMANDS)
        ],
        batch_size=SMALL_BATCH_COMMANDS,
    )
    outcomes = [outcome async for outcome in stream]

    assert isinstance(outcomes[failed_index], CommandFailure)
    assert outcomes[failed_index].replay_disposition is ReplayDisposition.NOT_ELIGIBLE
    assert all(isinstance(outcome, CommandSuccess) for index, outcome in enumerate(outcomes) if index != failed_index)
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED_WITH_FAILURES
    assert stream.report.successes == SMALL_BATCH_COMMANDS - 1
    assert stream.report.failures == 1
    gate = stream._source.completion_gate  # noqa: SLF001 - public batch gate positive control
    decision = gate.decision()
    assert decision.state is TerminalState.COMPLETED_WITH_FAILURES
    assert not decision.exhausted
    assert decision.bindings_admitted == decision.bindings_terminal == SMALL_BATCH_COMMANDS
    assert decision.pages_scheduled == SMALL_BATCH_COMMANDS
    assert decision.pages_acknowledged == SMALL_BATCH_COMMANDS - 1
    assert gate.finish() == stream.report


@pytest.mark.asyncio
async def test_logical_batch_preserves_kernel_replay_disposition() -> None:
    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": {},
                "result_error": {
                    key: {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "retry later"} for key in commands
                },
            },
        }

    outcomes = [
        outcome
        async for outcome in _client(ResponderTransport(handler)).batch_outcomes(
            (
                Command(Request("safe.get", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE), "safe"),
                Command(Request("unsafe.add", replay_safety=ReplaySafety.UNSAFE, route=RouteKind.BARE), "unsafe"),
                Command(Request("unknown.get", route=RouteKind.BARE), "unknown"),
            ),
            policy=ExecutionPolicy(retry=RetryPolicy(initial_delay=0, maximum_delay=0, jitter=0)),
        )
    ]
    assert all(isinstance(outcome, CommandFailure) for outcome in outcomes)
    failures = cast("list[CommandFailure[str]]", outcomes)
    # A quota refusal proves no command ran, so each may be replayed whatever its safety (owner decision).
    assert all(outcome.replay_disposition is ReplayDisposition.ELIGIBLE for outcome in failures)


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

    stream = _client(ResponderTransport(handler)).batch(
        [
            Command(Request("test.get", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE), index)
            for index in range(SMALL_BATCH_COMMANDS)
        ],
        batch_size=SMALL_BATCH_COMMANDS,
    )

    first = await anext(stream)
    with pytest.raises(BatchFailed) as captured:
        await anext(stream)

    assert isinstance(first, CommandSuccess)
    assert isinstance(captured.value.outcomes[0], CommandFailure)
    assert captured.value.outcomes[0].replay_disposition is ReplayDisposition.NOT_ELIGIBLE
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
            yield Command(Request("test.get", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE), "a")
            yield Command(Request("test.get", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE), "b")
            raise RuntimeError("source broke")
        finally:
            closed = True

    transport = ResponderTransport(lambda _request: pytest.fail("partial window must not be dispatched"))
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

    transport = ResponderTransport(handler)
    stream = _client(transport).iter_references(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
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
    correlation = object()
    transport = ResponderTransport(lambda _request: pytest.fail("binding collision must reject before I/O"))
    stream = _client(transport).iter_references(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        [Binding("bad", (ParameterUpdate(ParameterPath(("start",)), 100),), correlation)],
        traversal=SequentialTraversal(),
    )

    with pytest.raises(ReferenceFailed) as captured:
        await anext(stream)

    assert len(captured.value.outcomes) == 1
    outcome = captured.value.outcomes[0]
    assert isinstance(outcome, ReferenceNotExecuted)
    assert outcome.correlation is correlation
    assert outcome.reason is NotExecutedReason.LOCAL_VALIDATION_FAILED
    assert transport.requests == []
    assert stream.report is captured.value.report
    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_tolerant_batch_distinguishes_user_source_failure_from_local_item_validation() -> None:
    correlation = object()

    def failed_source() -> Iterator[Command[object]]:
        yield Command(Request("test.get", route=RouteKind.BARE), correlation)
        raise ValueError("caller source failed")

    transport = ResponderTransport(lambda _request: pytest.fail("partial source window must not dispatch"))
    stream = _client(transport).batch_outcomes(failed_source(), batch_size=2)

    outcome = await anext(stream)
    assert isinstance(outcome, CommandNotExecuted)
    assert outcome.correlation is correlation
    assert outcome.reason is NotExecutedReason.SOURCE_FAILED
    with pytest.raises(InputSourceError) as captured:
        await anext(stream)
    # The failure is raised once; a later read ends the iteration (§3.1).
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
    assert isinstance(captured.value.__cause__, ValueError)
    assert transport.requests == []

    def invalid_item_source() -> Iterator[object]:
        yield Command(Request("test.get", route=RouteKind.BARE), correlation)
        yield object()

    local = _client(transport).batch_outcomes(invalid_item_source(), batch_size=2)  # type: ignore[arg-type]
    local_outcome = await anext(local)
    assert isinstance(local_outcome, CommandNotExecuted)
    assert local_outcome.correlation is correlation
    assert local_outcome.reason is NotExecutedReason.LOCAL_VALIDATION_FAILED
    with pytest.raises(InputSourceError):
        await anext(local)


@pytest.mark.asyncio
async def test_early_closed_batch_reports_every_command_admitted_into_the_physical_window() -> None:
    def handler(request: Request) -> object:
        command_map = request.copy_parameters()["cmd"]
        assert isinstance(command_map, dict)
        return {
            "result": {
                "result": {key: {"ok": True} for key in command_map},
                "result_error": {},
            },
        }

    commands = tuple(
        Command(Request("test.get", {"index": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)
        for index in range(SMALL_BATCH_COMMANDS)
    )
    stream = _client(ResponderTransport(handler)).batch_outcomes(
        commands,
        batch_size=SMALL_BATCH_COMMANDS,
    )

    await anext(stream)
    await stream.aclose()

    assert stream.report is not None
    assert stream.report.state is TerminalState.EARLY_CLOSED
    assert stream.report.admitted == SMALL_BATCH_COMMANDS
    assert stream.report.emitted == 1


@pytest.mark.asyncio
async def test_tolerant_local_binding_failure_emits_not_executed_and_continues() -> None:
    correlations = (object(), object())
    transport = ResponderTransport(lambda _request: {"result": []})
    stream = _client(transport).iter_reference_outcomes(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        [
            Binding("invalid", (ParameterUpdate(ParameterPath(("start",)), 100),), correlations[0]),
            Binding("valid", (), correlations[1]),
        ],
        traversal=SequentialTraversal(),
        dispatch=DirectDispatch(concurrency=1, output_order=DeliveryOrder.INPUT),
    )

    outcomes = [outcome async for outcome in stream]

    assert len(outcomes) == REFERENCE_BINDINGS
    assert isinstance(outcomes[0], ReferenceNotExecuted)
    assert outcomes[0].correlation is correlations[0]
    assert outcomes[0].reason is NotExecutedReason.LOCAL_VALIDATION_FAILED
    assert isinstance(outcomes[1], ReferenceComplete)
    assert outcomes[1].correlation is correlations[1]
    assert len(transport.requests) == 1
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED_WITH_FAILURES
    assert stream.report.not_executed == 1
    assert stream.report.successes == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("route", [RouteKind.JSON, RouteKind.API_V3])
async def test_fail_fast_batch_retains_correlations_for_unsupported_route_without_io(route: RouteKind) -> None:
    transport = ResponderTransport(lambda _request: pytest.fail("unsupported physical batch must not be sent"))
    correlations = (object(), object())
    stream = _client(transport).batch(
        (
            Command(Request("test.get", route=route), correlations[0]),
            Command(Request("test.get", route=RouteKind.BARE), correlations[1]),
        ),
        batch_size=2,
    )

    with pytest.raises(BatchFailed) as captured:
        _ = [outcome async for outcome in stream]

    outcomes = captured.value.outcomes
    assert len(outcomes) == len(correlations)
    assert isinstance(outcomes[0], CommandFailure)
    assert outcomes[0].correlation is correlations[0]
    assert isinstance(outcomes[0].error, CapabilityError)
    assert isinstance(outcomes[1], CommandNotExecuted)
    assert outcomes[1].correlation is correlations[1]
    assert transport.requests == []
    assert stream.report.physical_requests == 0
    assert stream.report.batch_requests == 0
    assert stream.report.not_executed == 1
    assert stream.report.successes == 0
    assert stream.report.failures == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dispatch",
    [
        DirectDispatch(concurrency=1, output_order=DeliveryOrder.INPUT),
        BatchDispatch(batch_size=2, concurrency=1, output_order=DeliveryOrder.INPUT),
    ],
)
async def test_reference_source_rejects_uncorrelated_malformed_value_without_fabricating_outcome(
    dispatch: DirectDispatch | BatchDispatch,
) -> None:
    correlation = object()

    def bindings() -> Iterator[Binding[object]]:
        yield Binding("valid", (), correlation)
        yield cast("Binding[object]", object())

    def handler(request: Request) -> object:
        if request.method != "batch":
            return {"result": []}
        command_map = request.copy_parameters()["cmd"]
        assert isinstance(command_map, dict)
        return {
            "result": {
                "result": {key: [] for key in command_map},
                "result_error": {},
            },
        }

    stream = _client(ResponderTransport(handler)).iter_reference_outcomes(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        bindings(),
        traversal=SequentialTraversal(),
        dispatch=dispatch,
    )

    outcome = await anext(stream)
    with pytest.raises(InputSourceError):
        await anext(stream)

    assert isinstance(outcome, ReferenceComplete)
    assert outcome.correlation is correlation
    assert stream.report is not None
    assert stream.report.admitted == 1
    assert stream.report.emitted == 1
    assert stream.report.not_executed == 0
    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_tolerant_reference_preserves_ambiguous_dispatch_as_unknown() -> None:
    correlation = object()
    transport = _ambiguous()
    stream = _client(transport).iter_reference_outcomes(
        Request("test.list", replay_safety=ReplaySafety.UNKNOWN, route=RouteKind.BARE),
        [Binding("ambiguous", (), correlation)],
        traversal=SequentialTraversal(),
        dispatch=DirectDispatch(concurrency=1),
    )

    outcomes = [outcome async for outcome in stream]

    assert len(outcomes) == 1
    outcome = outcomes[0]
    assert isinstance(outcome, ReferenceOutcomeUnknown)
    assert outcome.correlation is correlation
    assert isinstance(outcome.error, AmbiguousExecutionError)
    assert outcome.partial_rows == 0
    assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE
    assert len(transport.requests) == 1
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED_WITH_FAILURES
    assert stream.report.unknown == 1


@pytest.mark.asyncio
async def test_tolerant_reference_preserves_kernel_replay_disposition() -> None:
    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        key = next(iter(commands))
        return {
            "result": {
                "result": {},
                "result_error": {key: {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "retry later"}},
            },
        }

    stream = _client(ResponderTransport(handler)).iter_reference_outcomes(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        [Binding("safe", (), "safe")],
        traversal=SequentialTraversal(),
        dispatch=BatchDispatch(batch_size=1),
        policy=ExecutionPolicy(retry=RetryPolicy(initial_delay=0, maximum_delay=0, jitter=0)),
    )

    outcomes = [outcome async for outcome in stream]

    assert len(outcomes) == 1
    outcome = outcomes[0]
    assert isinstance(outcome, ReferenceFailure)
    assert outcome.replay_disposition is ReplayDisposition.ELIGIBLE
    assert stream.report is not None
    violation = next(item for item in stream.report.violations if item.code == "reference_failure")
    assert violation.replay_disposition is ReplayDisposition.ELIGIBLE


def test_counted_reference_rejects_direct_dispatch_before_binding_pull_or_io() -> None:
    pulled = False

    async def bindings() -> AsyncGenerator[Binding[object]]:
        nonlocal pulled
        pulled = True
        yield Binding("unreachable", (), object())

    transport = ResponderTransport(lambda _request: {"result": []})
    client = _client(transport)

    with pytest.raises(CapabilityError, match="requires BatchDispatch"):
        client.iter_references(
            Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
            bindings(),
            traversal=CountedTraversal(identity=_identity()),
            dispatch=DirectDispatch(),
        )

    assert not pulled
    assert transport.requests == []


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

    stream = _client(ResponderTransport(handler)).iter_reference_outcomes(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
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


@pytest.mark.asyncio
async def test_reference_traversal_enforces_its_local_page_cap_without_a_wire_limit() -> None:
    rows = [{"ID": index} for index in range(PAGE_SIZE + 1)]
    transport = ResponderTransport(lambda _request: {"result": rows})
    correlation = object()
    stream = _client(transport).iter_reference_outcomes(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        [Binding("oversized", (), correlation)],
        traversal=SequentialTraversal(page_size=PAGE_SIZE, identity=_identity()),
        dispatch=DirectDispatch(),
    )

    outcomes = [outcome async for outcome in stream]

    assert len(outcomes) == 1
    assert isinstance(outcomes[0], ReferenceFailure)
    assert outcomes[0].correlation is correlation
    assert outcomes[0].partial_rows == 0
    assert isinstance(outcomes[0].error, IncompleteTraversalError)
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED_WITH_FAILURES
    assert stream.report.buffered_rows_high_water == 0


@pytest.mark.asyncio
async def test_reference_incomplete_maps_to_typed_failure_not_unknown() -> None:
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 1}]})
    correlation = object()
    stream = _client(transport).iter_reference_outcomes(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        [Binding("one", (), correlation)],
        traversal=SequentialTraversal(identity=_identity()),
        dispatch=DirectDispatch(),
    )

    outcomes = [outcome async for outcome in stream]

    assert len(outcomes) == REFERENCE_BINDINGS
    assert isinstance(outcomes[0], ReferenceItem)
    assert isinstance(outcomes[1], ReferenceFailure)
    failure = outcomes[1]
    assert failure.correlation is correlation
    assert failure.partial_rows == 1
    assert isinstance(failure.error, IncompleteTraversalError)
    assert failure.error.report is None
    assert failure.error.error is not None
    assert failure.error.error is failure.error.__cause__
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED_WITH_FAILURES
    assert stream.report.failures == 1
    assert stream.report.successes == 0


@pytest.mark.asyncio
async def test_counted_reference_post_io_capability_failure_is_typed_incomplete() -> None:
    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": {key: [{"ID": 1}] for key in commands},
                "result_error": {},
            },
        }

    correlation = object()
    stream = _client(ResponderTransport(handler)).iter_reference_outcomes(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        [Binding("missing exact total", (), correlation)],
        traversal=CountedTraversal(identity=_identity()),
        dispatch=BatchDispatch(batch_size=1),
    )

    outcomes = [outcome async for outcome in stream]

    assert len(outcomes) == 1
    failure = outcomes[0]
    assert isinstance(failure, ReferenceFailure)
    assert failure.correlation is correlation
    assert failure.partial_rows == 0
    assert isinstance(failure.error, IncompleteTraversalError)
    assert isinstance(failure.error.__cause__, CapabilityError)
    assert failure.error.report is None
    assert failure.error.error is failure.error.__cause__
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED_WITH_FAILURES


@pytest.mark.asyncio
async def test_fail_fast_reference_raises_bounded_reference_failed() -> None:
    transport = ResponderTransport(lambda _request: {"error": "denied", "error_description": "no access"})
    stream = _client(transport).iter_references(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
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
    transport = ResponderTransport(
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
                Request("test.get", {"value": index}, ReplaySafety.SAFE, route=RouteKind.BARE),
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
@pytest.mark.parametrize("concurrency", [1, 2, 3])
async def test_batch_fanout_spans_physical_windows_and_preserves_global_correlation(
    concurrency: int,
) -> None:
    async def handler(request: Request) -> object:
        assert request.method == "batch"
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        await asyncio.sleep(0.005)
        return {
            "result": {
                "result": {key: {"key": key} for key in commands},
                "result_error": {},
            },
        }

    transport = ResponderTransport(handler)
    stream = _client(transport).fan_out(
        [
            Command(Request("test.get", {"value": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)
            for index in range(FANOUT_COMMANDS)
        ],
        # Full windows must not depend on host speed: the widest coalescing wait never splits a window.
        dispatch=BatchDispatch(
            batch_size=FANOUT_BATCH_SIZE,
            concurrency=concurrency,
            output_order=DeliveryOrder.READY,
            coalesce_wait=1,
        ),
    )

    outcomes = [outcome async for outcome in stream]

    assert sorted(outcome.index for outcome in outcomes) == list(range(FANOUT_COMMANDS))
    assert sorted(outcome.correlation for outcome in outcomes) == list(range(FANOUT_COMMANDS))
    assert [len(request.copy_parameters()["cmd"]) for request in transport.requests] == [FANOUT_BATCH_SIZE] * (
        FANOUT_BATCH_REQUESTS
    )
    assert stream.report is not None
    assert stream.report.batch_requests == len(transport.requests)
    assert stream.report.batch_commands == FANOUT_COMMANDS


@pytest.mark.asyncio
async def test_tolerant_fanout_continues_after_one_direct_failure() -> None:
    def handler(request: Request) -> object:
        value = request.copy_parameters()["value"]
        if value == 1:
            return {"error": "denied", "error_description": "no access"}
        return {"result": value}

    stream = _client(ResponderTransport(handler)).fan_out_outcomes(
        [
            Command(Request("test.get", {"value": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)
            for index in range(3)
        ],
        dispatch=DirectDispatch(concurrency=2, output_order=DeliveryOrder.INPUT),
    )

    outcomes = [outcome async for outcome in stream]

    assert isinstance(outcomes[0], CommandSuccess)
    assert isinstance(outcomes[1], CommandFailure)
    assert isinstance(outcomes[2], CommandSuccess)
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED_WITH_FAILURES


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dispatch",
    [
        DirectDispatch(concurrency=1, output_order=DeliveryOrder.INPUT),
        BatchDispatch(batch_size=2, concurrency=1, output_order=DeliveryOrder.INPUT),
    ],
)
@pytest.mark.parametrize("malformed_item", [False, True])
async def test_fanout_source_failure_accounts_known_commands_without_fabricating_correlation(
    dispatch: DirectDispatch | BatchDispatch,
    *,
    malformed_item: bool,
) -> None:
    correlation = object()

    def commands() -> Iterator[Command[object]]:
        yield Command(Request("test.get", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE), correlation)
        if malformed_item:
            yield cast("Command[object]", object())
        raise ValueError("caller source failed")

    def handler(request: Request) -> object:
        if request.method != "batch":
            return {"result": {"ok": True}}
        command_map = request.copy_parameters()["cmd"]
        assert isinstance(command_map, dict)
        return {
            "result": {
                "result": {key: {"ok": True} for key in command_map},
                "result_error": {},
            },
        }

    stream = _client(ResponderTransport(handler)).fan_out_outcomes(commands(), dispatch=dispatch)

    outcome = await anext(stream)
    with pytest.raises(InputSourceError):
        await anext(stream)

    assert isinstance(outcome, CommandSuccess)
    assert outcome.correlation is correlation
    assert stream.report is not None
    assert stream.report.admitted == 1
    assert stream.report.emitted == 1
    assert stream.report.not_executed == 0
    assert stream.report.state is TerminalState.FAILED


def test_injected_transport_host_must_match_settings_before_io() -> None:
    transport = ResponderTransport(lambda _request: {"result": None})
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

    transport = ResponderTransport(handler)
    stream = _client(transport).iter_list(Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE))

    assert stream.report is None
    rows = [item async for item in stream]

    assert rows == [{"ID": 1}, {"ID": 2}, {"ID": 3}]
    assert [request.copy_parameters()["start"] for request in transport.requests] == [0, 2, 3]
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.assurance is TraversalAssurance.MECHANICS_ONLY


@pytest.mark.asyncio
async def test_iter_list_starts_from_the_callers_existing_offset() -> None:
    def handler(request: Request) -> object:
        start = request.copy_parameters()["start"]
        if start == CUSTOM_INITIAL_OFFSET:
            return {
                "result": [{"ID": CUSTOM_INITIAL_OFFSET}, {"ID": CUSTOM_INITIAL_OFFSET + 1}],
                "next": CUSTOM_NEXT_OFFSET,
            }
        assert start == CUSTOM_NEXT_OFFSET
        return {"result": []}

    transport = ResponderTransport(handler)
    client = _client(transport)
    stream = client.iter_list(
        Request("test.list", {"start": CUSTOM_INITIAL_OFFSET}, ReplaySafety.SAFE, route=RouteKind.BARE),
        identity=_identity(),
    )

    assert [row async for row in stream] == [
        {"ID": CUSTOM_INITIAL_OFFSET},
        {"ID": CUSTOM_INITIAL_OFFSET + 1},
    ]
    assert [request.copy_parameters()["start"] for request in transport.requests] == [
        CUSTOM_INITIAL_OFFSET,
        CUSTOM_NEXT_OFFSET,
    ]
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.assurance is TraversalAssurance.IDENTITY_EXACT
    assert stream.report is stream.report


@pytest.mark.asyncio
async def test_sequential_missing_offset_refuses_before_io_when_control_creation_is_disabled() -> None:
    transport = ResponderTransport(lambda _request: pytest.fail("missing traversal control must reject before I/O"))
    stream = _client(transport).iter_list(
        Request("test.list", route=RouteKind.BARE),
        offset=OffsetSpec(allow_create_controls=False),
    )

    with pytest.raises(CapabilityError, match="conflict"):
        await anext(stream)

    assert transport.requests == []
    assert stream.report is not None
    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_mapping_values_shape_preserves_mapping_insertion_order() -> None:
    responses = [
        {"result": {"items": {"b": {"ID": 2}, "a": {"ID": 1}}}},
        {"result": {"items": {}}},
    ]
    stream = _client(ResponderTransport(lambda _request: responses.pop(0))).iter_list(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
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

    keyset_transport = ResponderTransport(keyset_handler)
    keyset = _client(keyset_transport).iter_list_keyset(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        keyset=KeysetSpec(),
        execution=SequentialKeysetExecution(),
    )
    assert [item async for item in keyset] == [{"ID": 1}, {"ID": 2}, {"ID": 3}]

    def cursor_handler(request: Request) -> object:
        boundary = request.copy_parameters().get("LAST_ID", 0)
        assert isinstance(boundary, int)
        assert not isinstance(boundary, bool)
        rows = [item for item in ({"ID": 1}, {"ID": 2}) if item["ID"] > boundary][:1]
        return {"result": rows}

    cursor = _client(ResponderTransport(cursor_handler)).iter_list_cursor(
        Request("im.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
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
        assert events
        assert events[-1] is PageScheduled
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

    portal = ResponderTransport(handler)
    stream = _client(portal).iter_list_counted(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        identity=_identity(),
    )
    gate = stream._source.completion_gate  # noqa: SLF001 - inspect counted lifecycle seam
    events: list[type[object]] = []
    original_emit = gate.emit

    def observe(event: object) -> None:
        events.append(type(event))
        original_emit(event)

    gate.emit = observe

    rows = [item async for item in stream]

    assert tuple(row["ID"] for row in rows if isinstance(row, dict)) == identities
    assert len(portal.requests) == EXPECTED_COUNTED_REQUESTS
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.physical_requests == EXPECTED_COUNTED_REQUESTS
    assert stream.report.batch_requests == 1
    assert gate.decision().pages_scheduled == gate.decision().pages_acknowledged
    assert gate.decision().exhausted
    assert gate.finish() == stream.report
    assert events.count(PageScheduled) == events.count(PageAcknowledged) == COUNTED_ROWS // PAGE_SIZE


@pytest.mark.asyncio
async def test_counted_missing_in_band_stride_fails_incomplete() -> None:
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 1}], "total": 2})
    stream = _client(transport).iter_list_counted(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        identity=_identity(),
    )

    assert await anext(stream) == {"ID": 1}
    with pytest.raises(IncompleteTraversalError, match="did not complete") as captured:
        await anext(stream)

    assert type(captured.value).__name__ == "IncompleteTraversalError"
    assert stream.report is not None
    assert stream.report.state is TerminalState.INCOMPLETE


@pytest.mark.asyncio
async def test_counted_batch_failure_retains_cause_and_kernel_replay_decision() -> None:
    def handler(request: Request) -> object:
        if request.method != "batch":
            return {
                "result": [{"ID": index} for index in range(PAGE_SIZE)],
                "total": PAGE_SIZE * 2,
                "next": PAGE_SIZE,
            }
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        key = next(iter(commands))
        return {
            "result": {
                "result": {},
                "result_error": {key: {"error": "OPERATION_TIME_LIMIT", "error_description": "wait"}},
            },
        }

    stream = _client(ResponderTransport(handler)).iter_list_counted(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        identity=_identity(),
    )

    for _index in range(PAGE_SIZE):
        await anext(stream)
    with pytest.raises(IncompleteTraversalError) as captured:
        await anext(stream)

    error = captured.value
    assert isinstance(error.error, BatchCommandError)
    assert error.error.normalized_code == "operation_time_limit"
    assert error.error.retryable
    assert error.replay_disposition is ReplayDisposition.ELIGIBLE
    assert error.to_safe_dict()["cause"]["normalized_code"] == "operation_time_limit"
    assert stream.report is not None
    violation = next(item for item in stream.report.violations if item.severity.value == "blocking")
    assert violation.error is error.error
    assert violation.replay_disposition is ReplayDisposition.ELIGIBLE


@pytest.mark.asyncio
async def test_counted_page_cap_rejects_a_wider_observed_head_before_tail_or_rows() -> None:
    transport = ResponderTransport(
        lambda _request: {
            "result": [{"ID": index} for index in range(PAGE_SIZE)],
            "total": PAGE_SIZE * 2,
            "next": PAGE_SIZE,
        },
    )
    stream = _client(transport).iter_list_counted(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        identity=_identity(),
        page_size=2,
    )

    with pytest.raises(IncompleteTraversalError):
        await anext(stream)

    assert len(transport.requests) == 1
    assert stream.report is not None
    assert stream.report.emitted == 0
    assert stream.report.state is TerminalState.INCOMPLETE


@pytest.mark.asyncio
async def test_empty_page_with_continuation_never_completes_and_a_later_read_ends_the_iteration() -> None:
    def handler(request: Request) -> object:
        start = int(request.copy_parameters().get("start", 0))
        if start == 0:
            return {"result": [{"ID": 1}], "next": 1}
        return {"result": [], "next": 2}

    transport = ResponderTransport(handler)
    stream = _client(transport).iter_list(
        Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        identity=_identity(),
    )

    assert await anext(stream) == {"ID": 1}
    with pytest.raises(IncompleteTraversalError) as first:
        await anext(stream)
    sent = len(transport.requests)
    # The failure is raised once; a later read starts no work and ends the iteration (§3.1).
    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    assert len(transport.requests) == sent
    assert first.value.report is stream.report
    assert stream.report is not None
    assert stream.report.state is TerminalState.INCOMPLETE
    assert stream.report.terminal_reason == "PaginationError"


@pytest.mark.asyncio
async def test_partial_helper_closes_without_claiming_completion() -> None:
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 1}, {"ID": 2}]})
    stream = _client(transport).iter_list(Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE))

    partial = await stream.first()

    assert partial.value == ({"ID": 1},)
    assert partial.report.state is TerminalState.EARLY_CLOSED
    assert stream.report is partial.report
    sent = len(transport.requests)
    # A read after close starts no work and ends the iteration (§3.1).
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
    assert len(transport.requests) == sent
    assert stream.report is partial.report


@pytest.mark.asyncio
async def test_client_closes_active_stream_before_owned_transport_but_not_injected_transport() -> None:
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 1}]})
    settings = Settings(webhook_url=f"https://{transport.host}/rest/1/token/")
    client = Bitrix24(settings, transport=transport)
    stream = client.iter_list(Request("test.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE))

    await client.aclose()

    assert stream.report is not None
    assert stream.report.state is TerminalState.EARLY_CLOSED
    assert stream._source.completion_gate.finish() == stream.report  # noqa: SLF001 - gate ownership assertion
    assert not transport.closed
    with pytest.raises(RuntimeError, match="client is closed"):
        await client.call(Request("test.get", route=RouteKind.BARE))


@pytest.mark.asyncio
async def test_client_close_finishes_owned_cleanup_before_replaying_cancellation(
    scripted_client: ClientFactory,
) -> None:
    transport = BlockingCloseTransport()
    client = scripted_client(transport)
    client._owned_transport = transport  # type: ignore[assignment]  # noqa: SLF001
    close = asyncio.create_task(client.aclose())
    await transport.close_started.wait()

    close.cancel("external-caller")
    transport.release_close.set()

    with pytest.raises(asyncio.CancelledError) as captured:
        await close
    assert captured.value.args == ("external-caller",)
    assert transport.closed


@pytest.mark.asyncio
async def test_concurrent_client_closes_await_one_owned_cleanup(scripted_client: ClientFactory) -> None:
    transport = BlockingCloseTransport()
    client = scripted_client(transport)
    client._owned_transport = transport  # type: ignore[assignment]  # noqa: SLF001
    first = asyncio.create_task(client.aclose())
    await transport.close_started.wait()
    second = asyncio.create_task(client.aclose())
    await asyncio.sleep(0)

    assert not first.done()
    assert not second.done()
    transport.release_close.set()
    await asyncio.gather(first, second)

    assert transport.closed
    assert client._close_task is not None  # noqa: SLF001 - shared close ownership regression
    assert client._close_task.done()  # noqa: SLF001


@pytest.mark.asyncio
async def test_client_context_preserves_body_error_when_owned_cleanup_fails(scripted_client: ClientFactory) -> None:
    transport = FailingCloseTransport(lambda _request: {"result": None})
    client = scripted_client(transport)
    client._owned_transport = transport  # type: ignore[assignment]  # noqa: SLF001

    with pytest.raises(ValueError, match="primary body failure") as captured:
        async with client:
            raise ValueError("primary body failure")

    assert isinstance(captured.value.__cause__, RuntimeError)
    assert "owned transport cleanup failed" in str(captured.value.__cause__)
    assert transport.closed


@pytest.mark.asyncio
async def test_public_aclose_is_permitted_during_an_inflight_pull_and_cancels_owned_work() -> None:
    transport = BlockingRequestTransport()
    stream = _client(transport).iter_list(Request("test.list", route=RouteKind.BARE))
    pull = asyncio.create_task(anext(stream))
    await transport.started.wait()

    await stream.aclose()

    with pytest.raises(asyncio.CancelledError) as captured:
        await pull
    assert attached_report(captured.value) is stream.report
    assert transport.cancelled.is_set()
    assert stream.report is not None
    # aclose() owns termination, so the cause is an early close even though the read was cancelled.
    assert stream.report.state is TerminalState.EARLY_CLOSED


@pytest.mark.asyncio
async def test_concurrent_public_pulls_reject_without_stealing_the_owned_pull() -> None:
    transport = BlockingRequestTransport()
    stream = _client(transport).iter_list(Request("test.list", route=RouteKind.BARE))
    owned_pull = asyncio.create_task(anext(stream))
    await transport.started.wait()

    with pytest.raises(RuntimeError, match="concurrent stream pull"):
        await anext(stream)

    await stream.aclose()
    with pytest.raises(asyncio.CancelledError):
        await owned_pull


@pytest.mark.asyncio
async def test_long_lived_client_registry_does_not_retain_terminated_streams() -> None:
    client = _client(ResponderTransport(lambda _request: {"result": []}))

    for _ in range(100):
        stream = client.iter_list(Request("test.list", route=RouteKind.BARE))
        assert [row async for row in stream] == []
        assert stream.report is not None
        assert stream.report.state is TerminalState.COMPLETED
        assert len(client._streams) == 0  # noqa: SLF001 - exact weak-registry lifecycle requirement
