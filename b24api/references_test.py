"""W6 tests for bounded reference admission, ordering, batching, and cleanup."""

from __future__ import annotations
import asyncio
import contextlib
import json
import threading
from typing import TYPE_CHECKING, cast
from urllib.parse import parse_qs, urlsplit

import pytest

from b24api.error import (
    ApiResponseError,
    BudgetExceededError,
    CapabilityError,
    FailurePhase,
    HTTPGatewayError,
    ProtocolError,
    TransportError,
)
from b24api.execution import ExecutionContext, Executor, WireResponse
from b24api.models import (
    ConsistencyPolicy,
    DuplicatePolicy,
    ExecutionPolicy,
    IdentityCoercion,
    IdentityRequirement,
    IdentitySpec,
    ParameterPath,
    ReferenceFailure,
    ReferenceItem,
    ReferenceRequest,
    Request,
    RetryPolicy,
    SnapshotRequirement,
    SnapshotState,
    TerminalState,
    TotalSemantics,
    ViolationSeverity,
)
from b24api.plans import (
    BatchDispatch,
    CountedOffsetMode,
    CountedOffsetPlan,
    DirectDispatch,
    DispatchPlan,
    ListPlan,
    OffsetContinuation,
    OffsetSequentialPlan,
    OffsetTerminalRule,
    ReferenceOutputOrder,
)
from b24api.references import fan_out, iter_references

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterator

PAGE_SIZE = 1
TWO_REFERENCES = 2
EXPECTED_LOGICAL_PAGES = 4
CLEANUP_TEST_TIMEOUT = 0.2
PULL_TEST_TIMEOUT = 0.15


class AsyncFunctionTransport:
    def __init__(self, handler: Callable[[Request], object]) -> None:
        self.handler = handler
        self.requests: list[Request] = []

    async def send(self, request: Request, *, attempt_timeout: float) -> WireResponse:
        del attempt_timeout
        self.requests.append(request)
        outcome = self.handler(request)
        if asyncio.iscoroutine(outcome):
            outcome = await outcome
        body = json.dumps(outcome, separators=(",", ":")).encode()
        return WireResponse(200, (("content-type", "application/json"),), body)


class BlockingTransport:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def send(self, request: Request, *, attempt_timeout: float) -> WireResponse:
        del request, attempt_timeout
        self.started.set()
        try:
            return await asyncio.Future[WireResponse]()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


def _reference(key: str) -> ReferenceRequest:
    return ReferenceRequest(Request("crm.item.list", {"ref": key}), key)


def _one_page_plan() -> OffsetSequentialPlan:
    return OffsetSequentialPlan(
        limit_path=ParameterPath(("limit",)),
        requested_page_size=PAGE_SIZE,
        continuation=OffsetContinuation.SERVER_NEXT,
        terminal=frozenset({OffsetTerminalRule.PROFILE_ABSENT_NEXT}),
    )


def _empty_confirmation_plan() -> OffsetSequentialPlan:
    return OffsetSequentialPlan(
        limit_path=ParameterPath(("limit",)),
        requested_page_size=PAGE_SIZE,
        continuation=OffsetContinuation.OBSERVED_COUNT,
        terminal=frozenset({OffsetTerminalRule.EMPTY_PAGE}),
    )


def _identity() -> IdentitySpec:
    return IdentitySpec(
        item_path=("ID",),
        filter_key="ID",
        order_key="ID",
        coercion=IdentityCoercion.EXACT_INTEGER,
    )


def _integer_parameter(request: Request, name: str) -> int:
    value = request.copy_parameters()[name]
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    return value


def _string_parameter(request: Request, name: str) -> str:
    value = request.copy_parameters()[name]
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


@pytest.mark.asyncio
async def test_ready_order_interleaves_references_by_actual_completion() -> None:
    release_first = asyncio.Event()
    second_started = asyncio.Event()

    async def handler(request: Request) -> object:
        reference = request.copy_parameters()["ref"]
        if reference == "first":
            await release_first.wait()
        else:
            second_started.set()
        return {"result": [{"ID": 1, "ref": reference}]}

    transport = AsyncFunctionTransport(handler)
    stream = iter_references(
        Executor(transport),
        [_reference("first"), _reference("second")],
        plan=_one_page_plan(),
        dispatch=DirectDispatch(concurrency=TWO_REFERENCES),
        identity=_identity(),
        policy=ExecutionPolicy(max_active_references=TWO_REFERENCES, max_buffered_rows=TWO_REFERENCES),
    )

    second = asyncio.create_task(anext(stream))
    await second_started.wait()
    assert (await second).reference_key == "second"
    release_first.set()
    assert (await anext(stream)).reference_key == "first"
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
    assert stream.report.state is TerminalState.COMPLETED


@pytest.mark.asyncio
async def test_input_order_allows_later_progress_without_cross_reference_reordering() -> None:
    release_first = asyncio.Event()
    second_started = asyncio.Event()

    async def handler(request: Request) -> object:
        reference = request.copy_parameters()["ref"]
        if reference == "first":
            await release_first.wait()
        else:
            second_started.set()
        return {"result": [{"ID": 1, "ref": reference}]}

    transport = AsyncFunctionTransport(handler)
    stream = iter_references(
        Executor(transport),
        [_reference("first"), _reference("second")],
        plan=_one_page_plan(),
        dispatch=DirectDispatch(concurrency=TWO_REFERENCES, output_order=ReferenceOutputOrder.INPUT),
        identity=_identity(),
        output_order=ReferenceOutputOrder.INPUT,
        policy=ExecutionPolicy(max_active_references=TWO_REFERENCES, max_buffered_rows=TWO_REFERENCES),
    )

    first = asyncio.create_task(anext(stream))
    await asyncio.wait_for(second_started.wait(), timeout=1)
    assert not first.done()
    release_first.set()
    assert (await first).reference_key == "first"
    assert (await anext(stream)).reference_key == "second"
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
    assert stream.report.buffered_rows_high_water == TWO_REFERENCES


@pytest.mark.asyncio
async def test_per_reference_pagination_is_sequential_while_references_are_concurrent() -> None:
    def handler(request: Request) -> object:
        parameters = request.copy_parameters()
        reference = parameters["ref"]
        start = parameters["start"]
        if start == 0:
            return {"result": [{"ID": 1, "ref": reference}]}
        return {"result": []}

    transport = AsyncFunctionTransport(handler)
    plan = OffsetSequentialPlan(
        limit_path=ParameterPath(("limit",)),
        requested_page_size=PAGE_SIZE,
        continuation=OffsetContinuation.OBSERVED_COUNT,
        terminal=frozenset({OffsetTerminalRule.EMPTY_PAGE}),
    )
    stream = iter_references(
        Executor(transport),
        [_reference("a"), _reference("b")],
        plan=plan,
        dispatch=DirectDispatch(concurrency=TWO_REFERENCES),
        identity=_identity(),
        policy=ExecutionPolicy(max_active_references=TWO_REFERENCES, max_buffered_rows=TWO_REFERENCES),
    )

    outcomes = [outcome async for outcome in stream]
    assert [outcome.reference_key for outcome in outcomes] == ["a", "b"]
    by_reference: dict[str, list[int]] = {"a": [], "b": []}
    for request in transport.requests:
        by_reference[_string_parameter(request, "ref")].append(_integer_parameter(request, "start"))
    assert by_reference == {"a": [0, 1], "b": [0, 1]}
    assert stream.report.logical_pages == EXPECTED_LOGICAL_PAGES


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("plan", "policy"),
    [
        (
            OffsetSequentialPlan(identity_requirement=IdentityRequirement.REQUIRED),
            ExecutionPolicy(),
        ),
        (
            OffsetSequentialPlan(),
            ExecutionPolicy(
                consistency=ConsistencyPolicy(identity_requirement=IdentityRequirement.REQUIRED),
            ),
        ),
        (
            CountedOffsetPlan(),
            ExecutionPolicy(
                consistency=ConsistencyPolicy(total_semantics=TotalSemantics.GLOBAL),
            ),
        ),
        (
            CountedOffsetPlan(
                mode=CountedOffsetMode.PARALLEL_FIXED_STRIDE,
                fixed_stride=1,
            ),
            ExecutionPolicy(),
        ),
    ],
)
async def test_invalid_reference_contract_refuses_even_empty_input(
    plan: ListPlan,
    policy: ExecutionPolicy,
) -> None:
    transport = AsyncFunctionTransport(lambda _request: {"result": []})
    stream = iter_references(
        Executor(transport),
        [],
        plan=plan,
        dispatch=DirectDispatch(),
        policy=policy,
    )

    with pytest.raises(CapabilityError) as captured:
        await anext(stream)

    assert captured.value.__dict__["report"] is stream.report
    assert stream.report.state is TerminalState.FAILED
    assert transport.requests == []


@pytest.mark.asyncio
async def test_invalid_reference_contract_refuses_before_blocking_source_pull() -> None:
    pulled = asyncio.Event()

    async def source() -> AsyncGenerator[ReferenceRequest]:
        pulled.set()
        await asyncio.Future[None]()
        yield _reference("unreachable")

    transport = AsyncFunctionTransport(lambda _request: {"result": []})
    stream = iter_references(
        Executor(transport),
        source(),
        plan=OffsetSequentialPlan(identity_requirement=IdentityRequirement.REQUIRED),
        dispatch=DirectDispatch(),
        policy=ExecutionPolicy(max_elapsed=0.03),
    )

    with pytest.raises(CapabilityError, match="IdentitySpec"):
        await anext(stream)

    assert not pulled.is_set()
    assert transport.requests == []


def test_invalid_reference_plan_type_refuses_at_construction() -> None:
    source_touched = False

    def source() -> Iterator[ReferenceRequest]:
        nonlocal source_touched
        source_touched = True
        yield _reference("unreachable")

    with pytest.raises(TypeError, match="canonical ListPlan"):
        iter_references(
            Executor(AsyncFunctionTransport(lambda _request: {"result": []})),
            source(),
            plan=cast("ListPlan", object()),
            dispatch=DirectDispatch(),
        )

    assert not source_touched


@pytest.mark.asyncio
async def test_batch_dispatch_coalesces_pages_and_preserves_total_metadata() -> None:
    def handler(request: Request) -> object:
        assert request.method == "batch"
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        results: dict[str, object] = {}
        totals: dict[str, int] = {}
        for key, command in commands.items():
            assert isinstance(command, str)
            query = parse_qs(urlsplit(command).query)
            reference = query["ref"][0]
            results[key] = [{"ID": 1, "ref": reference}]
            totals[key] = 1
        return {
            "result": {
                "result": results,
                "result_error": [],
                "result_total": totals,
                "result_next": [],
            },
        }

    transport = AsyncFunctionTransport(handler)
    stream = iter_references(
        Executor(transport),
        [_reference("a"), _reference("b")],
        plan=CountedOffsetPlan(limit_path=ParameterPath(("limit",)), requested_page_size=PAGE_SIZE),
        dispatch=BatchDispatch(batch_size=TWO_REFERENCES),
        identity=_identity(),
        policy=ExecutionPolicy(max_active_references=TWO_REFERENCES, max_buffered_rows=TWO_REFERENCES),
    )

    outcomes = [outcome async for outcome in stream]
    assert sorted(outcome.reference_key for outcome in outcomes) == ["a", "b"]
    assert len(transport.requests) == 1
    assert stream.report.batch_requests == 1
    assert stream.report.batch_commands == TWO_REFERENCES
    assert stream.report.logical_pages == TWO_REFERENCES


@pytest.mark.asyncio
async def test_batch_fan_out_coalesces_whole_results_as_one_item_per_reference() -> None:
    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": {key: {"command": key} for key in commands},
                "result_error": [],
            },
        }

    transport = AsyncFunctionTransport(handler)
    stream = fan_out(
        Executor(transport),
        [_reference("a"), _reference("b")],
        dispatch=BatchDispatch(batch_size=TWO_REFERENCES),
        policy=ExecutionPolicy(max_active_references=TWO_REFERENCES, max_buffered_rows=TWO_REFERENCES),
    )

    outcomes = [outcome async for outcome in stream]
    assert len(outcomes) == TWO_REFERENCES
    items = [outcome for outcome in outcomes if isinstance(outcome, ReferenceItem)]
    assert len(items) == TWO_REFERENCES
    assert all(isinstance(outcome.item, dict) for outcome in items)
    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_fan_out_accepts_list_result_whose_total_matches_list_length() -> None:
    transport = AsyncFunctionTransport(
        lambda _request: {"result": [{"ID": 1}, {"ID": 2}], "total": TWO_REFERENCES},
    )
    stream = fan_out(
        Executor(transport),
        [_reference("a")],
        dispatch=DirectDispatch(concurrency=1),
    )

    outcomes = [outcome async for outcome in stream]

    assert len(outcomes) == 1
    assert isinstance(outcomes[0], ReferenceItem)
    assert outcomes[0].item == [{"ID": 1}, {"ID": 2}]
    assert stream.report.state is TerminalState.COMPLETED


@pytest.mark.asyncio
async def test_fan_out_does_not_infer_safe_replay_for_unset_requests() -> None:
    class TransientThenSuccessTransport:
        def __init__(self) -> None:
            self.requests: list[Request] = []

        async def send(self, request: Request, *, attempt_timeout: float) -> WireResponse:
            del attempt_timeout
            self.requests.append(request)
            if len(self.requests) == 1:
                return WireResponse(503, (), b"gateway")
            return WireResponse(200, (), b'{"result":{"ID":1}}')

    transport = TransientThenSuccessTransport()
    request = Request("tasks.task.add")
    stream = fan_out(
        Executor(transport),
        [ReferenceRequest(request, "write")],
        dispatch=DirectDispatch(concurrency=1),
        policy=ExecutionPolicy(
            max_attempts_per_request=TWO_REFERENCES,
            retry=RetryPolicy(initial_delay=0, maximum_delay=0, jitter=0),
        ),
    )

    with pytest.raises(HTTPGatewayError) as captured:
        await anext(stream)

    assert captured.value.__dict__["report"] is stream.report
    assert request.replay_safety is None
    assert len(transport.requests) == 1
    assert transport.requests[0].replay_safety is None
    assert stream.report.retries == 0
    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_batch_fan_out_accepts_list_result_whose_total_matches_list_length() -> None:
    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": {key: [{"ID": 1}, {"ID": 2}] for key in commands},
                "result_error": [],
                "result_total": dict.fromkeys(commands, TWO_REFERENCES),
            },
        }

    stream = fan_out(
        Executor(AsyncFunctionTransport(handler)),
        [_reference("a")],
        dispatch=BatchDispatch(batch_size=1),
    )

    outcomes = [outcome async for outcome in stream]

    assert len(outcomes) == 1
    assert isinstance(outcomes[0], ReferenceItem)
    assert outcomes[0].item == [{"ID": 1}, {"ID": 2}]
    assert stream.report.state is TerminalState.COMPLETED


@pytest.mark.asyncio
@pytest.mark.parametrize("dispatch", [DirectDispatch(concurrency=1), BatchDispatch(batch_size=1)])
async def test_fan_out_list_result_obeys_decoded_row_buffer(dispatch: DispatchPlan) -> None:
    def handler(request: Request) -> object:
        rows = [{"ID": 1}, {"ID": 2}, {"ID": 3}]
        if request.method != "batch":
            return {"result": rows, "total": len(rows)}
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": dict.fromkeys(commands, rows),
                "result_error": [],
                "result_total": dict.fromkeys(commands, len(rows)),
            },
        }

    transport = AsyncFunctionTransport(handler)
    stream = fan_out(
        Executor(transport),
        [_reference("a")],
        dispatch=dispatch,
        policy=ExecutionPolicy(max_buffered_rows=TWO_REFERENCES),
    )

    with pytest.raises(BudgetExceededError, match="decoded page") as captured:
        await anext(stream)

    assert captured.value.__dict__["report"] is stream.report
    assert stream.report.state is TerminalState.FAILED
    assert stream.report.emitted_rows == 0
    assert stream.report.buffered_rows_high_water == 0


@pytest.mark.asyncio
async def test_input_order_rejects_oversized_whole_result_behind_blocked_head() -> None:
    head_started = asyncio.Event()
    release_head = asyncio.Event()
    later_started = asyncio.Event()

    async def handler(request: Request) -> object:
        if request.copy_parameters()["ref"] == "head":
            head_started.set()
            await release_head.wait()
            return {"result": {"head": True}}
        later_started.set()
        rows = [{"ID": index} for index in range(10)]
        return {"result": rows, "total": len(rows)}

    order = ReferenceOutputOrder.INPUT
    stream = fan_out(
        Executor(AsyncFunctionTransport(handler)),
        [_reference("head"), _reference("later")],
        dispatch=DirectDispatch(concurrency=TWO_REFERENCES, output_order=order),
        output_order=order,
        policy=ExecutionPolicy(
            max_active_references=TWO_REFERENCES,
            max_buffered_rows=TWO_REFERENCES,
        ),
    )
    first = asyncio.create_task(anext(stream))
    await head_started.wait()
    await later_started.wait()
    for _ in range(10):
        await asyncio.sleep(0)
    snapshot = await stream._scheduler.context.snapshot()  # noqa: SLF001 - bounded retention regression
    assert snapshot.counters.buffered_rows == 0
    assert snapshot.counters.buffered_rows_high_water == 0

    release_head.set()
    head = await first
    assert isinstance(head, ReferenceItem)
    assert head.reference_key == "head"

    with pytest.raises(BudgetExceededError, match="decoded page"):
        await anext(stream)
    assert stream.report.buffered_rows_high_water == 1


@pytest.mark.asyncio
async def test_tolerant_reference_failure_preserves_total_correlation() -> None:
    def handler(request: Request) -> object:
        if request.copy_parameters()["ref"] == "bad":
            return {"error": "ACCESS_DENIED", "error_description": "denied"}
        return {"result": [{"ID": 1}]}

    transport = AsyncFunctionTransport(handler)
    stream = iter_references(
        Executor(transport),
        [_reference("bad"), _reference("good")],
        plan=_one_page_plan(),
        dispatch=DirectDispatch(concurrency=TWO_REFERENCES),
        identity=_identity(),
        tolerant=True,
        policy=ExecutionPolicy(max_active_references=TWO_REFERENCES, max_buffered_rows=TWO_REFERENCES),
    )

    outcomes = [outcome async for outcome in stream]
    assert {outcome.reference_key for outcome in outcomes} == {"bad", "good"}
    failure = next(outcome for outcome in outcomes if isinstance(outcome, ReferenceFailure))
    assert isinstance(failure.error, ApiResponseError)
    assert stream.report.state is TerminalState.COMPLETED
    assert [violation.code for violation in stream.report.violations] == ["reference_failure"]


@pytest.mark.asyncio
async def test_tolerant_batch_chunk_protocol_failure_yields_every_reference() -> None:
    transport = AsyncFunctionTransport(lambda _request: {"result": []})
    stream = iter_references(
        Executor(transport),
        [_reference("a"), _reference("b")],
        plan=_one_page_plan(),
        dispatch=BatchDispatch(batch_size=TWO_REFERENCES),
        identity=_identity(),
        tolerant=True,
        policy=ExecutionPolicy(max_active_references=TWO_REFERENCES, max_buffered_rows=TWO_REFERENCES),
    )

    outcomes = [outcome async for outcome in stream]
    assert len(outcomes) == TWO_REFERENCES
    assert all(isinstance(outcome, ReferenceFailure) for outcome in outcomes)
    assert {outcome.reference_key for outcome in outcomes} == {"a", "b"}
    assert stream.report.emitted_rows == 0


@pytest.mark.asyncio
async def test_failed_input_head_cannot_deadlock_a_full_later_page() -> None:
    def handler(request: Request) -> object:
        if request.copy_parameters()["ref"] == "first":
            return {"error": "ACCESS_DENIED", "error_description": "denied"}
        return {"result": [{"ID": 1}]}

    stream = iter_references(
        Executor(AsyncFunctionTransport(handler)),
        [_reference("first"), _reference("second")],
        plan=_one_page_plan(),
        dispatch=DirectDispatch(concurrency=TWO_REFERENCES, output_order=ReferenceOutputOrder.INPUT),
        identity=_identity(),
        output_order=ReferenceOutputOrder.INPUT,
        tolerant=True,
        policy=ExecutionPolicy(max_active_references=TWO_REFERENCES, max_buffered_rows=PAGE_SIZE),
    )

    outcomes = await asyncio.wait_for(_collect(stream), timeout=1)
    assert [outcome.reference_key for outcome in outcomes] == ["first", "second"]
    assert isinstance(outcomes[0], ReferenceFailure)
    assert isinstance(outcomes[1], ReferenceItem)


@pytest.mark.asyncio
async def test_fail_fast_reference_error_carries_same_terminal_report() -> None:
    transport = AsyncFunctionTransport(
        lambda _request: {"error": "ACCESS_DENIED", "error_description": "denied"},
    )
    stream = fan_out(
        Executor(transport),
        [_reference("bad")],
        dispatch=DirectDispatch(),
    )

    with pytest.raises(ApiResponseError) as captured:
        await anext(stream)

    assert stream.report.state is TerminalState.FAILED
    assert captured.value.__dict__["report"] is stream.report


@pytest.mark.asyncio
async def test_async_input_admission_is_bounded_and_closed_on_early_exit() -> None:
    produced = 0
    source_closed = asyncio.Event()

    async def source() -> AsyncGenerator[ReferenceRequest]:
        nonlocal produced
        try:
            for index in range(10):
                produced += 1
                yield _reference(str(index))
        finally:
            source_closed.set()

    transport = AsyncFunctionTransport(
        lambda request: {"result": [{"ID": int(_string_parameter(request, "ref"))}]},
    )
    stream = iter_references(
        Executor(transport),
        source(),
        plan=_one_page_plan(),
        dispatch=DirectDispatch(concurrency=TWO_REFERENCES),
        identity=_identity(),
        policy=ExecutionPolicy(max_active_references=TWO_REFERENCES, max_buffered_rows=TWO_REFERENCES),
    )

    assert isinstance(await anext(stream), ReferenceItem)
    assert produced <= TWO_REFERENCES
    await stream.aclose()
    assert source_closed.is_set()
    assert stream.report.state is TerminalState.CANCELLED


@pytest.mark.asyncio
async def test_blocked_second_async_input_does_not_block_ready_first_output() -> None:
    source_blocked = asyncio.Event()

    async def source() -> AsyncGenerator[ReferenceRequest]:
        yield _reference("first")
        source_blocked.set()
        await asyncio.Future[None]()

    stream = fan_out(
        Executor(AsyncFunctionTransport(lambda _request: {"result": {"ok": True}})),
        source(),
        dispatch=DirectDispatch(concurrency=TWO_REFERENCES),
        policy=ExecutionPolicy(max_active_references=TWO_REFERENCES),
    )

    first = await asyncio.wait_for(anext(stream), timeout=1)
    assert first.reference_key == "first"
    assert source_blocked.is_set()
    await stream.aclose()


@pytest.mark.asyncio
async def test_async_reference_input_pull_obeys_operation_elapsed_budget() -> None:
    async def source() -> AsyncGenerator[ReferenceRequest]:
        yield _reference("first")
        await asyncio.Future[None]()

    stream = fan_out(
        Executor(AsyncFunctionTransport(lambda _request: {"result": {"ok": True}})),
        source(),
        dispatch=DirectDispatch(),
        policy=ExecutionPolicy(max_active_references=1, max_elapsed=0.03),
    )

    assert isinstance(await anext(stream), ReferenceItem)
    with pytest.raises(BudgetExceededError, match="reference input") as captured:
        await asyncio.wait_for(anext(stream), timeout=PULL_TEST_TIMEOUT)

    assert captured.value.__dict__["report"] is stream.report
    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_cancellation_resistant_reference_pull_is_closed_after_late_completion() -> None:
    release = asyncio.Event()
    closed = asyncio.Event()

    async def source() -> AsyncGenerator[ReferenceRequest]:
        try:
            yield _reference("first")
            try:
                await asyncio.Future[None]()
            except asyncio.CancelledError:
                while not release.is_set():
                    try:
                        await release.wait()
                    except asyncio.CancelledError:
                        continue
            yield _reference("late")
        finally:
            closed.set()

    stream = fan_out(
        Executor(AsyncFunctionTransport(lambda _request: {"result": {"ok": True}})),
        source(),
        dispatch=DirectDispatch(),
        policy=ExecutionPolicy(max_active_references=1, max_elapsed=0.03),
    )

    assert isinstance(await anext(stream), ReferenceItem)
    with pytest.raises(BudgetExceededError) as captured:
        await asyncio.wait_for(anext(stream), timeout=0.15)
    assert captured.value.__dict__["report"] is stream.report
    assert not closed.is_set()

    release.set()
    await asyncio.wait_for(closed.wait(), timeout=0.2)


@pytest.mark.asyncio
async def test_late_reference_source_cleanup_error_is_observed_by_subsequent_close() -> None:
    release = asyncio.Event()
    closed = asyncio.Event()

    async def source() -> AsyncGenerator[ReferenceRequest]:
        try:
            yield _reference("first")
            try:
                await asyncio.Future[None]()
            except asyncio.CancelledError:
                await release.wait()
            yield _reference("late")
        finally:
            closed.set()
            raise RuntimeError("late reference close boom")

    stream = fan_out(
        Executor(AsyncFunctionTransport(lambda _request: {"result": {"ok": True}})),
        source(),
        dispatch=DirectDispatch(),
        policy=ExecutionPolicy(max_active_references=1, max_elapsed=0.03),
    )
    assert isinstance(await anext(stream), ReferenceItem)
    with pytest.raises(BudgetExceededError):
        await anext(stream)

    release.set()
    await asyncio.wait_for(closed.wait(), timeout=0.2)
    with pytest.raises(RuntimeError, match="late reference close boom") as captured:
        await stream.aclose()
    assert captured.value.__dict__["report"] is stream.report


@pytest.mark.asyncio
async def test_blocking_sync_reference_pull_does_not_block_event_loop_or_deadline() -> None:
    release = threading.Event()
    closed = threading.Event()

    class BlockingPull:
        def __init__(self) -> None:
            self._count = 0

        def __iter__(self) -> BlockingPull:
            return self

        def __next__(self) -> ReferenceRequest:
            self._count += 1
            if self._count == 1:
                return _reference("first")
            release.wait()
            return _reference("late")

        def close(self) -> None:
            closed.set()

    stream = fan_out(
        Executor(AsyncFunctionTransport(lambda _request: {"result": {"ok": True}})),
        BlockingPull(),
        dispatch=DirectDispatch(),
        policy=ExecutionPolicy(max_active_references=1, max_elapsed=0.03),
    )

    assert isinstance(await anext(stream), ReferenceItem)
    started = asyncio.get_running_loop().time()
    with pytest.raises(BudgetExceededError):
        await asyncio.wait_for(anext(stream), timeout=PULL_TEST_TIMEOUT)
    assert asyncio.get_running_loop().time() - started < PULL_TEST_TIMEOUT

    release.set()
    assert await asyncio.to_thread(closed.wait, 0.2)


@pytest.mark.asyncio
@pytest.mark.parametrize("output_order", [ReferenceOutputOrder.READY, ReferenceOutputOrder.INPUT])
async def test_reference_source_failure_drains_all_admitted_outcomes(
    output_order: ReferenceOutputOrder,
) -> None:
    async def source() -> AsyncGenerator[ReferenceRequest]:
        yield _reference("a")
        yield _reference("b")
        raise RuntimeError("reference source boom")

    transport = AsyncFunctionTransport(lambda _request: {"result": {"ok": True}})
    stream = fan_out(
        Executor(transport),
        source(),
        dispatch=DirectDispatch(concurrency=TWO_REFERENCES, output_order=output_order),
        output_order=output_order,
        tolerant=True,
        policy=ExecutionPolicy(max_active_references=TWO_REFERENCES),
    )
    outcomes: list[ReferenceItem | ReferenceFailure] = []

    async def consume() -> None:
        while True:
            outcomes.append(await anext(stream))

    with pytest.raises(RuntimeError, match="reference source boom") as captured:
        await consume()

    assert captured.value.__dict__["report"] is stream.report
    assert sorted(outcome.reference_key for outcome in outcomes) == ["a", "b"]
    assert len(transport.requests) == TWO_REFERENCES
    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_reference_page_budget_blocks_second_direct_request_before_io() -> None:
    transport = AsyncFunctionTransport(lambda _request: {"result": [{"ID": 1}]})
    stream = iter_references(
        Executor(transport),
        [_reference("a")],
        plan=_empty_confirmation_plan(),
        dispatch=DirectDispatch(),
        identity=_identity(),
        policy=ExecutionPolicy(max_pages_per_reference=1),
    )

    with pytest.raises(BudgetExceededError, match="per-reference page budget"):
        await _collect(stream)
    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_reference_page_budget_blocks_second_batch_request_before_io() -> None:
    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": {key: [{"ID": 1}] for key in commands},
                "result_error": [],
            },
        }

    transport = AsyncFunctionTransport(handler)
    stream = iter_references(
        Executor(transport),
        [_reference("a")],
        plan=_empty_confirmation_plan(),
        dispatch=BatchDispatch(batch_size=1),
        identity=_identity(),
        policy=ExecutionPolicy(max_pages_per_reference=1),
    )

    with pytest.raises(BudgetExceededError, match="per-reference page budget"):
        await _collect(stream)
    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_failed_reference_releases_page_slot_for_independent_work() -> None:
    calls: list[str] = []
    bad_started = asyncio.Event()
    release_bad = asyncio.Event()

    async def handler(request: Request) -> object:
        reference = _string_parameter(request, "ref")
        calls.append(reference)
        if reference == "bad":
            bad_started.set()
            await release_bad.wait()
            raise TransportError(
                "connect failed",
                phase=FailurePhase.NOT_DISPATCHED,
                retryable=False,
            )
        return {"result": {"ok": True}}

    stream = fan_out(
        Executor(AsyncFunctionTransport(handler)),
        [_reference("bad"), _reference("good")],
        dispatch=DirectDispatch(concurrency=TWO_REFERENCES),
        tolerant=True,
        policy=ExecutionPolicy(
            max_pages=1,
            max_active_references=TWO_REFERENCES,
            max_attempts_per_request=1,
        ),
    )

    collecting = asyncio.create_task(_collect(stream))
    await bad_started.wait()
    await asyncio.sleep(0)
    assert calls == ["bad"]
    release_bad.set()
    outcomes = await collecting
    assert calls == ["bad", "good"]
    assert isinstance(outcomes[0], ReferenceFailure)
    assert isinstance(outcomes[1], ReferenceItem)
    assert stream.report.logical_pages == 1


@pytest.mark.asyncio
async def test_required_reference_snapshot_finishes_incomplete() -> None:
    policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(snapshot_requirement=SnapshotRequirement.FROZEN_MANIFEST),
    )
    stream = fan_out(
        Executor(AsyncFunctionTransport(lambda _request: {"result": {"ok": True}})),
        [_reference("a")],
        dispatch=DirectDispatch(),
        policy=policy,
    )

    assert len(await _collect(stream)) == 1
    assert stream.report.state is TerminalState.INCOMPLETE
    assert stream.report.snapshot is SnapshotState.UNVERIFIED
    assert [violation.code for violation in stream.report.violations] == ["snapshot_unverified"]


@pytest.mark.asyncio
async def test_duplicate_public_reference_keys_keep_independent_page_budgets() -> None:
    transport = AsyncFunctionTransport(lambda _request: {"result": [{"ID": 1}]})
    stream = iter_references(
        Executor(transport),
        [_reference("same"), _reference("same")],
        plan=_one_page_plan(),
        dispatch=DirectDispatch(concurrency=TWO_REFERENCES),
        identity=_identity(),
        policy=ExecutionPolicy(
            max_active_references=TWO_REFERENCES,
            max_buffered_rows=TWO_REFERENCES,
            max_pages_per_reference=1,
        ),
    )

    outcomes = [outcome async for outcome in stream]
    assert len(outcomes) == TWO_REFERENCES
    assert stream.report.state is TerminalState.COMPLETED
    assert stream._scheduler.buffer._reservations == []  # noqa: SLF001


@pytest.mark.asyncio
async def test_reference_report_sums_per_reference_unique_counts_and_violations() -> None:
    def handler(request: Request) -> object:
        start = _integer_parameter(request, "start")
        pages = {
            0: [{"ID": 1, "revision": "a"}],
            1: [{"ID": 1, "revision": "b"}],
            2: [],
        }
        return {"result": pages[start]}

    plan = OffsetSequentialPlan(
        limit_path=ParameterPath(("limit",)),
        requested_page_size=PAGE_SIZE,
        continuation=OffsetContinuation.OBSERVED_COUNT,
        terminal=frozenset({OffsetTerminalRule.EMPTY_PAGE}),
        duplicate_policy=DuplicatePolicy.REPORT,
    )
    stream = iter_references(
        Executor(AsyncFunctionTransport(handler)),
        [_reference("a")],
        plan=plan,
        dispatch=DirectDispatch(),
        identity=_identity(),
        policy=ExecutionPolicy(
            consistency=ConsistencyPolicy(duplicate_policy=DuplicatePolicy.REPORT),
        ),
    )

    outcomes = [outcome async for outcome in stream]
    assert len(outcomes) == TWO_REFERENCES
    assert stream.report.emitted_rows == TWO_REFERENCES
    assert stream.report.unique_rows == PAGE_SIZE
    assert [violation.code for violation in stream.report.violations] == ["duplicate_identity"]


@pytest.mark.asyncio
async def test_early_close_keeps_delivered_unique_count_and_detected_page_warning() -> None:
    transport = AsyncFunctionTransport(
        lambda _request: {
            "result": [
                {"ID": 1, "revision": "a"},
                {"ID": 1, "revision": "b"},
            ],
        },
    )
    plan = OffsetSequentialPlan(
        limit_path=ParameterPath(("limit",)),
        requested_page_size=TWO_REFERENCES,
        continuation=OffsetContinuation.SERVER_NEXT,
        terminal=frozenset({OffsetTerminalRule.PROFILE_ABSENT_NEXT}),
        duplicate_policy=DuplicatePolicy.REPORT,
    )
    stream = iter_references(
        Executor(transport),
        [_reference("a")],
        plan=plan,
        dispatch=DirectDispatch(),
        identity=_identity(),
        policy=ExecutionPolicy(
            max_buffered_rows=TWO_REFERENCES,
            consistency=ConsistencyPolicy(duplicate_policy=DuplicatePolicy.REPORT),
        ),
    )

    assert isinstance(await anext(stream), ReferenceItem)
    await stream.aclose()

    assert stream.report.emitted_rows == 1
    assert stream.report.unique_rows == 1
    assert [violation.code for violation in stream.report.violations] == ["duplicate_identity"]


@pytest.mark.asyncio
async def test_reference_task_cancellation_closes_transport_and_buffer_state() -> None:
    transport = BlockingTransport()
    stream = fan_out(
        Executor(transport),
        [_reference("a")],
        dispatch=DirectDispatch(),
    )
    task = asyncio.create_task(anext(stream))
    await transport.started.wait()

    await stream._scheduler.context._lock.acquire()  # noqa: SLF001 - repeated-cancel regression
    task.cancel()
    for _ in range(5):
        await asyncio.sleep(0)
    task.cancel()
    stream._scheduler.context._lock.release()  # noqa: SLF001 - repeated-cancel regression
    with pytest.raises(asyncio.CancelledError) as captured:
        await task

    assert captured.value.__dict__["report"] is stream.report
    assert transport.cancelled.is_set()
    assert stream.report.state is TerminalState.CANCELLED
    assert (await stream._scheduler.context.snapshot()).counters.buffered_rows == 0  # noqa: SLF001


@pytest.mark.asyncio
async def test_reference_cancellation_during_failed_finalization_preserves_failure_report() -> None:
    locked = asyncio.Event()

    class FailingSource:
        def __init__(self) -> None:
            self.context: ExecutionContext | None = None

        def __aiter__(self) -> FailingSource:
            return self

        async def __anext__(self) -> ReferenceRequest:
            assert self.context is not None
            await self.context._lock.acquire()  # noqa: SLF001 - deterministic finalize-race regression
            locked.set()
            raise RuntimeError("reference source failed")

    source = FailingSource()
    stream = fan_out(
        Executor(AsyncFunctionTransport(lambda _request: {"result": {"ok": True}})),
        source,
        dispatch=DirectDispatch(),
    )
    source.context = stream._scheduler.context  # noqa: SLF001 - deterministic finalize-race regression
    task = asyncio.create_task(anext(stream))
    await locked.wait()
    for _ in range(50):
        await asyncio.sleep(0)
    task.cancel()
    assert source.context is not None
    source.context._lock.release()  # noqa: SLF001 - deterministic finalize-race regression

    with pytest.raises(RuntimeError, match="reference source failed") as captured:
        await task

    assert captured.value.__dict__["report"] is stream.report
    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_batch_reference_cancellation_interrupts_inflight_batch_request() -> None:
    transport = BlockingTransport()
    stream = fan_out(
        Executor(transport),
        [_reference("a")],
        dispatch=BatchDispatch(),
    )
    task = asyncio.create_task(anext(stream))
    await transport.started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)

    assert transport.cancelled.is_set()
    assert stream.report.state is TerminalState.CANCELLED


@pytest.mark.asyncio
async def test_late_direct_response_after_suppressed_cancellation_cannot_commit_page() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    returned = asyncio.Event()

    class CancellationResistantTransport:
        async def send(self, request: Request, *, attempt_timeout: float) -> WireResponse:
            del request, attempt_timeout
            started.set()
            try:
                await asyncio.Future[None]()
            except asyncio.CancelledError:
                while not release.is_set():
                    try:
                        await release.wait()
                    except asyncio.CancelledError:
                        continue
            returned.set()
            body = json.dumps({"result": {"ok": True}}).encode()
            return WireResponse(200, (("content-type", "application/json"),), body)

    stream = fan_out(
        Executor(CancellationResistantTransport()),
        [_reference("a")],
        dispatch=DirectDispatch(),
        policy=ExecutionPolicy(max_elapsed=0.03),
    )
    task = asyncio.create_task(anext(stream))
    await started.wait()
    task.cancel()

    async def safety_release() -> None:
        await asyncio.sleep(0.2)
        release.set()

    safety = asyncio.create_task(safety_release())
    started_at = asyncio.get_running_loop().time()
    try:
        with pytest.raises(BudgetExceededError, match="reference task cleanup") as captured:
            await task
    finally:
        release.set()
        safety.cancel()
        await asyncio.gather(safety, return_exceptions=True)
    assert asyncio.get_running_loop().time() - started_at < CLEANUP_TEST_TIMEOUT
    frozen_report = stream.report
    assert captured.value.__dict__["report"] is frozen_report
    assert frozen_report.logical_pages == 0

    await asyncio.wait_for(returned.wait(), timeout=0.2)
    for _ in range(10):
        await asyncio.sleep(0)
    snapshot = await stream._scheduler.context.snapshot()  # noqa: SLF001
    assert snapshot.counters.logical_pages == 0
    assert stream.report is frozen_report
    assert stream.report.logical_pages == 0


@pytest.mark.asyncio
async def test_primary_reference_failure_survives_secondary_cleanup_budget_failure() -> None:
    slow_started = asyncio.Event()
    slow_returned = asyncio.Event()
    release = asyncio.Event()

    class CancellationResistantTransport:
        async def send(self, request: Request, *, attempt_timeout: float) -> WireResponse:
            del attempt_timeout
            if request.method == "bad":
                await slow_started.wait()
                return WireResponse(200, (), b"{")
            slow_started.set()
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    continue
            slow_returned.set()
            body = json.dumps({"result": {"ok": True}}).encode()
            return WireResponse(200, (("content-type", "application/json"),), body)

    stream = fan_out(
        Executor(CancellationResistantTransport()),
        [ReferenceRequest(Request("bad"), "bad"), ReferenceRequest(Request("slow"), "slow")],
        dispatch=DirectDispatch(concurrency=TWO_REFERENCES),
        policy=ExecutionPolicy(max_active_references=TWO_REFERENCES, max_elapsed=0.04),
    )
    try:
        with pytest.raises(ProtocolError, match="Malformed JSON response") as captured:
            await anext(stream)
    finally:
        release.set()

    assert captured.value.__dict__["report"] is stream.report
    assert stream.report.state is TerminalState.FAILED
    assert stream.report.terminal_reason == "ProtocolError"
    cleanup = [violation for violation in stream.report.violations if violation.code == "cleanup_failure"]
    assert len(cleanup) == 1
    assert cleanup[0].severity is ViolationSeverity.BLOCKING
    await asyncio.wait_for(slow_returned.wait(), timeout=CLEANUP_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_closed_batch_worker_exits_after_transport_temporarily_resists_cancellation() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class CancellationResistantTransport:
        async def send(self, request: Request, *, attempt_timeout: float) -> WireResponse:
            del attempt_timeout
            started.set()
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    continue
            commands = request.copy_parameters()["cmd"]
            assert isinstance(commands, dict)
            body = json.dumps(
                {
                    "result": {
                        "result": {key: {"ok": True} for key in commands},
                        "result_error": [],
                    },
                },
            ).encode()
            return WireResponse(200, (("content-type", "application/json"),), body)

    stream = fan_out(
        Executor(CancellationResistantTransport()),
        [_reference("a")],
        dispatch=BatchDispatch(batch_size=1),
        policy=ExecutionPolicy(max_elapsed=0.05),
    )
    task = asyncio.create_task(anext(stream))
    await started.wait()
    task.cancel()

    with pytest.raises(BudgetExceededError) as captured:
        await asyncio.wait_for(task, timeout=0.2)
    assert captured.value.__dict__["report"] is stream.report

    dispatcher = stream._scheduler.dispatcher  # noqa: SLF001
    worker = dispatcher._worker  # type: ignore[union-attr]  # noqa: SLF001
    assert worker is not None
    assert not worker.done()
    release.set()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(worker), timeout=0.2)
    assert worker.done()


@pytest.mark.asyncio
async def test_source_close_failure_does_not_skip_owned_resource_cleanup() -> None:
    class RaisingCloseSource:
        def __init__(self) -> None:
            self._yielded = False

        def __aiter__(self) -> RaisingCloseSource:
            return self

        async def __anext__(self) -> ReferenceRequest:
            if self._yielded:
                await asyncio.Future[None]()
            self._yielded = True
            return _reference("a")

        async def aclose(self) -> None:
            raise RuntimeError("source cleanup failed")

    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": {key: {"ok": True} for key in commands},
                "result_error": [],
            },
        }

    transport = AsyncFunctionTransport(handler)
    stream = fan_out(
        Executor(transport),
        RaisingCloseSource(),
        dispatch=BatchDispatch(batch_size=1),
        policy=ExecutionPolicy(max_active_references=1),
    )

    assert isinstance(await anext(stream), ReferenceItem)
    with pytest.raises(RuntimeError, match="source cleanup failed") as captured:
        await stream.aclose()

    assert captured.value.__dict__["report"] is stream.report
    dispatcher = stream._scheduler.dispatcher  # noqa: SLF001
    worker = dispatcher._worker  # type: ignore[union-attr]  # noqa: SLF001
    assert worker is not None
    assert worker.done()
    assert stream._scheduler.buffer._closed  # noqa: SLF001


@pytest.mark.asyncio
async def test_reference_iteration_cancellation_propagates_source_cleanup_error() -> None:
    pulling = asyncio.Event()

    class RaisingCloseSource:
        def __init__(self) -> None:
            self._yielded = False

        def __aiter__(self) -> RaisingCloseSource:
            return self

        async def __anext__(self) -> ReferenceRequest:
            if not self._yielded:
                self._yielded = True
                return _reference("a")
            pulling.set()
            await asyncio.Future[None]()
            raise AssertionError("unreachable")

        async def aclose(self) -> None:
            raise RuntimeError("reference close boom")

    stream = fan_out(
        Executor(AsyncFunctionTransport(lambda _request: {"result": {"ok": True}})),
        RaisingCloseSource(),
        dispatch=DirectDispatch(),
        policy=ExecutionPolicy(max_active_references=1),
    )
    assert isinstance(await anext(stream), ReferenceItem)
    task = asyncio.create_task(anext(stream))
    await pulling.wait()
    task.cancel()

    with pytest.raises(RuntimeError, match="reference close boom") as captured:
        await task
    assert captured.value.__dict__["report"] is stream.report


@pytest.mark.asyncio
async def test_stalled_source_cleanup_is_bounded_after_owned_resources_close() -> None:
    class StalledCloseSource:
        def __init__(self) -> None:
            self._yielded = False

        def __aiter__(self) -> StalledCloseSource:
            return self

        async def __anext__(self) -> ReferenceRequest:
            if self._yielded:
                await asyncio.Future[None]()
            self._yielded = True
            return _reference("a")

        async def aclose(self) -> None:
            await asyncio.Future[None]()

    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {
            "result": {
                "result": {key: {"ok": True} for key in commands},
                "result_error": [],
            },
        }

    stream = fan_out(
        Executor(AsyncFunctionTransport(handler)),
        StalledCloseSource(),
        dispatch=BatchDispatch(batch_size=1),
        policy=ExecutionPolicy(max_active_references=1, max_elapsed=0.05),
    )

    assert isinstance(await anext(stream), ReferenceItem)
    with pytest.raises(BudgetExceededError, match="source cleanup") as captured:
        await asyncio.wait_for(stream.aclose(), timeout=0.2)

    assert captured.value.__dict__["report"] is stream.report
    dispatcher = stream._scheduler.dispatcher  # noqa: SLF001
    worker = dispatcher._worker  # type: ignore[union-attr]  # noqa: SLF001
    assert worker is not None
    assert worker.done()
    assert stream._scheduler.buffer._closed  # noqa: SLF001


@pytest.mark.asyncio
async def test_blocking_sync_source_close_does_not_block_event_loop_or_cleanup_deadline() -> None:
    release_close = threading.Event()
    close_finished = threading.Event()

    class BlockingCloseIterator:
        def __init__(self) -> None:
            self._yielded = False

        def __iter__(self) -> BlockingCloseIterator:
            return self

        def __next__(self) -> ReferenceRequest:
            if self._yielded:
                return _reference("never-admitted")
            self._yielded = True
            return _reference("a")

        def close(self) -> None:
            release_close.wait()
            close_finished.set()

    stream = fan_out(
        Executor(AsyncFunctionTransport(lambda _request: {"result": {"ok": True}})),
        BlockingCloseIterator(),
        dispatch=DirectDispatch(),
        policy=ExecutionPolicy(max_active_references=1, max_elapsed=0.05),
    )

    assert isinstance(await anext(stream), ReferenceItem)
    started = asyncio.get_running_loop().time()
    with pytest.raises(BudgetExceededError, match="source cleanup") as captured:
        await asyncio.wait_for(stream.aclose(), timeout=0.2)
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < CLEANUP_TEST_TIMEOUT
    assert captured.value.__dict__["report"] is stream.report
    assert stream._scheduler.buffer._closed  # noqa: SLF001
    release_close.set()
    assert await asyncio.to_thread(close_finished.wait, 0.2)


def test_batch_reference_stream_construction_is_lazy_outside_an_event_loop() -> None:
    transport = AsyncFunctionTransport(lambda _request: {"result": {"result": [], "result_error": []}})
    stream = fan_out(
        Executor(transport),
        [_reference("a")],
        dispatch=BatchDispatch(),
    )

    assert transport.requests == []
    assert stream.report.state is TerminalState.NOT_STARTED


async def _collect(stream: object) -> list[ReferenceItem | ReferenceFailure]:
    return [outcome async for outcome in stream]  # type: ignore[attr-defined]
