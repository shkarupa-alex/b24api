"""W6 tests for bounded reference admission, ordering, batching, and cleanup."""

from __future__ import annotations
import asyncio
import json
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlsplit

import pytest

from b24api.error import ApiResponseError
from b24api.execution import Executor, WireResponse
from b24api.models import (
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    ParameterPath,
    ReferenceFailure,
    ReferenceItem,
    ReferenceRequest,
    Request,
    TerminalState,
)
from b24api.plans import (
    BatchDispatch,
    CountedOffsetPlan,
    DirectDispatch,
    OffsetContinuation,
    OffsetSequentialPlan,
    OffsetTerminalRule,
    ReferenceOutputOrder,
)
from b24api.references import fan_out, iter_references

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

PAGE_SIZE = 1
TWO_REFERENCES = 2
EXPECTED_LOGICAL_PAGES = 4


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
async def test_reference_task_cancellation_closes_transport_and_buffer_state() -> None:
    transport = BlockingTransport()
    stream = fan_out(
        Executor(transport),
        [_reference("a")],
        dispatch=DirectDispatch(),
    )
    task = asyncio.create_task(anext(stream))
    await transport.started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert transport.cancelled.is_set()
    assert stream.report.state is TerminalState.CANCELLED
    assert (await stream._scheduler.context.snapshot()).counters.buffered_rows == 0  # noqa: SLF001


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
