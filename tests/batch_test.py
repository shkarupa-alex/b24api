# ruff: noqa: SLF001 -- white-box tests for the private physical batch kernel
"""W4 tests for bounded batch streams and total command correlation."""

from __future__ import annotations
import asyncio
import json
import threading
from typing import TYPE_CHECKING, cast

import pytest

from b24api.batch.engine import BatchExecutor
from b24api.batch.outcome import (
    BatchFailure,
    BatchOutcome,
    BatchSuccess,
)
from b24api.batch.stream import _BatchOutcomeStream, batch_outcome_stream
from b24api.contracts.policy import (
    ConsistencyPolicy,
    ExecutionPolicy,
    KernelState,
    ReplayDisposition,
    SnapshotRequirement,
    SnapshotState,
)
from b24api.contracts.request import ReplaySafety, Request, RouteKind
from b24api.errors import (
    BatchCommandError,
    BudgetExceededError,
    CapabilityError,
    FailurePhase,
    ProtocolError,
    TransportError,
)
from b24api.execution import ExecutionContext, Executor, RateCoordinator, WireResponse
from tests.ledger_hold import LedgerHold

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterator

TEST_BATCH_SIZE = 7
TEST_COMMAND_COUNT = 23
EXPECTED_CHUNKS = 4
MIXED_COMMAND_COUNT = 3
HTTP_OK = 200
EXPECTED_TOTAL = 3
PULL_TEST_TIMEOUT = 0.15
PARTIAL_COMMAND_COUNT = 2
NESTED_ROW_COUNT = 2


class CallbackTransport:
    """Provide a deterministic test helper."""

    host = "fixture.invalid"

    def __init__(self, callback: Callable[[Request], WireResponse | Exception]) -> None:
        """Initialize instance state."""
        self.callback = callback
        self.requests: list[Request] = []

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Send one transport request attempt."""
        assert attempt_timeout > 0
        assert max_response_bytes > 0
        self.requests.append(request)
        outcome = self.callback(request)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _batch_keys(request: Request) -> list[str]:
    parameters = request.copy_parameters()
    commands = parameters["cmd"]
    assert isinstance(commands, dict)
    return list(commands)


def _wire_batch(
    keys: list[str],
    *,
    errors: object = None,
    omit_error_key: bool = False,
    omit_result_keys: frozenset[str] = frozenset(),
    empty_results: bool = False,
) -> WireResponse:
    batch_result: dict[str, object] = {
        "result": [] if empty_results else {key: {"key": key} for key in keys if key not in omit_result_keys},
    }
    if not omit_error_key:
        batch_result["result_error"] = [] if errors is None else errors
    body = json.dumps({"result": batch_result}).encode()
    return WireResponse(status_code=200, headers=(("content-type", "application/json"),), body=body)


def _echo_batch(request: Request) -> WireResponse:
    assert request.method == "batch"
    halt = request.copy_parameters()["halt"]
    assert type(halt) is int
    assert halt in {0, 1}
    return _wire_batch(_batch_keys(request))


def _one_attempt_policy() -> ExecutionPolicy:
    return ExecutionPolicy(max_requests=100, max_attempts_per_request=1)


@pytest.mark.asyncio
async def test_async_unlimited_input_pulls_only_one_bounded_chunk_before_first_yield() -> None:
    pulled = 0

    async def requests() -> AsyncGenerator[Request]:
        nonlocal pulled
        for _index in range(TEST_COMMAND_COUNT):
            pulled += 1
            yield Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)

    transport = CallbackTransport(_echo_batch)
    stream = batch_outcome_stream(
        BatchExecutor(Executor(transport)),
        requests(),
        batch_size=TEST_BATCH_SIZE,
    )
    first = await anext(stream)

    assert isinstance(first, BatchSuccess)
    assert pulled == TEST_BATCH_SIZE
    assert transport.requests[0].copy_parameters()["halt"] == 0
    await stream.aclose()
    assert stream.report.state is KernelState.CANCELLED


@pytest.mark.asyncio
async def test_non_bare_inner_command_is_correlated_rejection_before_batch_dispatch() -> None:
    transport = CallbackTransport(_echo_batch)
    stream = batch_outcome_stream(
        BatchExecutor(Executor(transport)),
        [
            Request("im.v2.Chat.Message.CommentInfo.list", route=RouteKind.JSON),
            Request("profile", route=RouteKind.BARE),
        ],
    )
    outcomes = [item async for item in stream]

    assert isinstance(outcomes[0], BatchFailure)
    assert outcomes[0].error.request_summary is not None
    assert outcomes[0].error.request_summary.route is RouteKind.JSON
    assert isinstance(outcomes[1], BatchSuccess)
    assert len(transport.requests) == 1
    assert transport.requests[0].route is RouteKind.BARE
    assert stream.report.batch_requests == 1


@pytest.mark.asyncio
async def test_fully_rejected_window_does_not_count_a_physical_batch_request() -> None:
    transport = CallbackTransport(_echo_batch)
    stream = batch_outcome_stream(
        BatchExecutor(Executor(transport)),
        [Request("im.v2.Chat.Message.CommentInfo.list", route=RouteKind.JSON)],
    )

    outcomes = [item async for item in stream]

    assert len(outcomes) == 1
    assert isinstance(outcomes[0], BatchFailure)
    assert transport.requests == []
    assert stream.report.physical_requests == 0
    assert stream.report.batch_requests == 0


@pytest.mark.asyncio
async def test_batch_method_limit_observes_only_failing_command_without_replay() -> None:
    def callback(request: Request) -> WireResponse:
        keys = _batch_keys(request)
        return _wire_batch(keys, errors={keys[0]: {"error": "OPERATION_TIME_LIMIT", "error_description": "wait"}})

    transport = CallbackTransport(callback)
    coordinator = RateCoordinator(operation_time_limit_delay=0.1)
    executor = BatchExecutor(Executor(transport, coordinator=coordinator))
    context = executor.executor.context(_one_attempt_policy())
    outcomes = await executor.execute_requests(
        (
            Request("crm.item.add", route=RouteKind.BARE, replay_safety=ReplaySafety.UNSAFE),
            Request("profile", route=RouteKind.BARE),
        ),
        context=context,
    )
    assert isinstance(outcomes[0], BatchFailure)
    assert isinstance(outcomes[0].error, BatchCommandError)
    assert outcomes[0].error.normalized_code == "operation_time_limit"
    assert len(transport.requests) == 1
    assert (await coordinator.snapshot()).method_cooldowns == 1
    await coordinator.close()


@pytest.mark.asyncio
async def test_batch_outcomes_obey_decoded_row_buffer_ceiling() -> None:
    transport = CallbackTransport(_echo_batch)
    stream = batch_outcome_stream(
        BatchExecutor(Executor(transport)),
        [Request("profile", route=RouteKind.BARE) for _index in range(MIXED_COMMAND_COUNT)],
        batch_size=MIXED_COMMAND_COUNT,
        policy=ExecutionPolicy(max_buffered_rows=1),
    )

    first = await anext(stream)
    snapshot = await stream._context.snapshot()

    assert isinstance(first, BatchSuccess)
    assert snapshot.counters.buffered_rows == 1
    assert snapshot.counters.buffered_rows_high_water == 1
    assert len(transport.requests) == 1

    remaining = [item async for item in stream]
    assert len(remaining) == MIXED_COMMAND_COUNT - 1
    assert len(transport.requests) == MIXED_COMMAND_COUNT
    assert stream.report.state is KernelState.COMPLETED
    assert stream.report.buffered_rows_high_water == 1


@pytest.mark.asyncio
async def test_batch_list_result_uses_nested_decoded_row_weight() -> None:
    def list_result(request: Request) -> WireResponse:
        keys = _batch_keys(request)
        rows = [{"ID": index} for index in range(NESTED_ROW_COUNT)]
        body = json.dumps(
            {
                "result": {
                    "result": dict.fromkeys(keys, rows),
                    "result_error": [],
                },
            },
        ).encode()
        return WireResponse(200, (("content-type", "application/json"),), body)

    rejected = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(list_result))),
        [Request("profile", route=RouteKind.BARE)],
        batch_size=1,
        policy=ExecutionPolicy(max_buffered_rows=1),
    )
    with pytest.raises(BudgetExceededError, match="buffer") as captured:
        await anext(rejected)
    assert captured.value.__dict__["report"] is rejected.report
    assert rejected.report.state is KernelState.FAILED
    assert rejected.report.emitted_rows == 0
    assert rejected.report.buffered_rows_high_water == 0

    admitted = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(list_result))),
        [Request("profile", route=RouteKind.BARE)],
        batch_size=1,
        policy=ExecutionPolicy(max_buffered_rows=NESTED_ROW_COUNT),
    )
    outcome = await anext(admitted)
    snapshot = await admitted._context.snapshot()
    assert isinstance(outcome, BatchSuccess)
    assert outcome.decoded_rows == NESTED_ROW_COUNT
    assert snapshot.counters.buffered_rows == NESTED_ROW_COUNT
    assert [item async for item in admitted] == []
    assert admitted.report.buffered_rows_high_water == NESTED_ROW_COUNT


@pytest.mark.asyncio
async def test_context_entry_reads_nothing_and_an_unstarted_close_reports_nothing() -> None:
    transport = CallbackTransport(_echo_batch)
    stream = batch_outcome_stream(
        BatchExecutor(Executor(transport)), [Request("profile", route=RouteKind.BARE)], batch_size=1
    )

    async with stream:
        assert transport.requests == []

    assert stream.report.state is KernelState.NOT_STARTED
    assert stream.report.emitted_rows == 0


@pytest.mark.asyncio
async def test_batch_source_cleanup_error_carries_same_report() -> None:
    async def source() -> AsyncGenerator[Request]:
        try:
            while True:
                yield Request("profile", route=RouteKind.BARE)
        finally:
            raise RuntimeError("batch source close boom")

    stream = batch_outcome_stream(BatchExecutor(Executor(CallbackTransport(_echo_batch))), source(), batch_size=1)
    assert isinstance(await anext(stream), BatchSuccess)

    with pytest.raises(RuntimeError, match="batch source close boom") as captured:
        await stream.aclose()

    assert captured.value.__dict__["report"] is stream.report
    assert stream.report.state is KernelState.FAILED
    assert [violation.code for violation in stream.report.violations] == ["cleanup_failure"]


@pytest.mark.asyncio
async def test_primary_batch_failure_survives_secondary_source_cleanup_failure() -> None:
    async def source() -> AsyncGenerator[Request]:
        try:
            yield Request("bad", route=RouteKind.BARE)
            await asyncio.Future[None]()
        finally:
            raise RuntimeError("batch source close boom")

    def malformed(_request: Request) -> WireResponse:
        body = json.dumps({"result": {"result": {}}}).encode()
        return WireResponse(200, (("content-type", "application/json"),), body)

    stream = batch_outcome_stream(BatchExecutor(Executor(CallbackTransport(malformed))), source(), batch_size=1)
    outcome = await anext(stream)
    assert isinstance(outcome, BatchFailure)
    assert isinstance(outcome.error, ProtocolError)

    with pytest.raises(RuntimeError, match="batch source close boom"):
        await stream.aclose()


@pytest.mark.asyncio
async def test_blocking_sync_batch_source_close_obeys_cleanup_deadline() -> None:
    release_close = threading.Event()
    close_finished = threading.Event()

    class BlockingCloseIterator:
        def __iter__(self) -> BlockingCloseIterator:
            return self

        def __next__(self) -> Request:
            return Request("profile", route=RouteKind.BARE)

        def close(self) -> None:
            release_close.wait()
            close_finished.set()

    stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(_echo_batch))),
        BlockingCloseIterator(),
        batch_size=1,
        policy=ExecutionPolicy(max_elapsed=0.05),
    )
    assert isinstance(await anext(stream), BatchSuccess)

    with pytest.raises(BudgetExceededError, match="batch source cleanup") as captured:
        await asyncio.wait_for(stream.aclose(), timeout=0.2)

    assert captured.value.__dict__["report"] is stream.report
    release_close.set()
    assert await asyncio.to_thread(close_finished.wait, 0.2)


@pytest.mark.asyncio
async def test_async_batch_input_pull_obeys_operation_elapsed_budget() -> None:
    async def source() -> AsyncGenerator[Request]:
        yield Request("profile", route=RouteKind.BARE)
        await asyncio.Future[None]()

    stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(_echo_batch))),
        source(),
        batch_size=1,
        policy=ExecutionPolicy(max_elapsed=0.03),
    )

    assert isinstance(await anext(stream), BatchSuccess)
    with pytest.raises(BudgetExceededError, match="batch input") as captured:
        await asyncio.wait_for(anext(stream), timeout=PULL_TEST_TIMEOUT)

    assert captured.value.__dict__["report"] is stream.report
    assert stream.report.state is KernelState.FAILED


@pytest.mark.asyncio
async def test_cancellation_resistant_batch_pull_is_closed_after_late_completion() -> None:
    release = asyncio.Event()
    closed = asyncio.Event()

    async def source() -> AsyncGenerator[Request]:
        try:
            yield Request("profile", route=RouteKind.BARE)
            try:
                await asyncio.Future[None]()
            except asyncio.CancelledError:
                await release.wait()
            yield Request("late", route=RouteKind.BARE)
        finally:
            closed.set()

    stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(_echo_batch))),
        source(),
        batch_size=1,
        policy=ExecutionPolicy(max_elapsed=0.03),
    )

    assert isinstance(await anext(stream), BatchSuccess)
    with pytest.raises(BudgetExceededError) as captured:
        await asyncio.wait_for(anext(stream), timeout=0.15)
    assert captured.value.__dict__["report"] is stream.report
    assert not closed.is_set()

    release.set()
    await asyncio.wait_for(closed.wait(), timeout=0.2)


@pytest.mark.asyncio
async def test_late_batch_source_cleanup_error_does_not_reopen_the_published_report() -> None:
    release = asyncio.Event()
    closed = asyncio.Event()

    async def source() -> AsyncGenerator[Request]:
        try:
            yield Request("profile", route=RouteKind.BARE)
            try:
                await asyncio.Future[None]()
            except asyncio.CancelledError:
                await release.wait()
            yield Request("late", route=RouteKind.BARE)
        finally:
            closed.set()
            raise RuntimeError("late batch close boom")

    stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(_echo_batch))),
        source(),
        batch_size=1,
        policy=ExecutionPolicy(max_elapsed=0.03),
    )
    assert isinstance(await anext(stream), BatchSuccess)
    with pytest.raises(BudgetExceededError) as failed:
        await anext(stream)
    report = stream.report
    assert failed.value.__dict__["report"] is report

    release.set()
    await asyncio.wait_for(closed.wait(), timeout=0.2)
    # The report was published once, after the bounded cleanup; a repeated close changes nothing (§3.1).
    await stream.aclose()
    assert stream.report is report
    assert report.state is KernelState.FAILED


@pytest.mark.asyncio
async def test_blocking_sync_batch_pull_does_not_block_event_loop_or_deadline() -> None:
    release = threading.Event()
    closed = threading.Event()

    class BlockingPull:
        def __init__(self) -> None:
            self._count = 0

        def __iter__(self) -> BlockingPull:
            return self

        def __next__(self) -> Request:
            self._count += 1
            if self._count == 1:
                return Request("profile", route=RouteKind.BARE)
            release.wait()
            return Request("late", route=RouteKind.BARE)

        def close(self) -> None:
            closed.set()

    stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(_echo_batch))),
        BlockingPull(),
        batch_size=1,
        policy=ExecutionPolicy(max_elapsed=0.03),
    )

    assert isinstance(await anext(stream), BatchSuccess)
    started = asyncio.get_running_loop().time()
    with pytest.raises(BudgetExceededError):
        await asyncio.wait_for(anext(stream), timeout=PULL_TEST_TIMEOUT)
    assert asyncio.get_running_loop().time() - started < PULL_TEST_TIMEOUT

    release.set()
    assert await asyncio.to_thread(closed.wait, 0.2)


@pytest.mark.asyncio
async def test_partial_kernel_chunk_is_not_dispatched_before_source_error() -> None:
    async def source() -> AsyncGenerator[Request]:
        yield Request("a", route=RouteKind.BARE)
        yield Request("b", route=RouteKind.BARE)
        raise RuntimeError("batch source boom")

    transport = CallbackTransport(_echo_batch)
    stream = batch_outcome_stream(BatchExecutor(Executor(transport)), source(), batch_size=3)
    outcomes: list[BatchOutcome] = []

    async def consume() -> None:
        while True:
            outcome = await anext(stream)
            assert isinstance(outcome, BatchSuccess | BatchFailure)
            outcomes.append(outcome)

    with pytest.raises(RuntimeError, match="batch source boom") as captured:
        await consume()

    assert captured.value.__dict__["report"] is stream.report
    assert outcomes == []
    assert transport.requests == []
    assert stream.report.emitted_rows == 0
    assert stream.report.state is KernelState.FAILED


@pytest.mark.asyncio
async def test_batch_iteration_cancellation_propagates_source_cleanup_error() -> None:
    pulling = asyncio.Event()

    class RaisingCloseSource:
        def __init__(self) -> None:
            self._yielded = False

        def __aiter__(self) -> RaisingCloseSource:
            return self

        async def __anext__(self) -> Request:
            if not self._yielded:
                self._yielded = True
                return Request("profile", route=RouteKind.BARE)
            pulling.set()
            await asyncio.Future[None]()
            raise AssertionError("unreachable")

        async def aclose(self) -> None:
            raise RuntimeError("batch close boom")

    stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(_echo_batch))),
        RaisingCloseSource(),
        batch_size=1,
    )
    assert isinstance(await anext(stream), BatchSuccess)
    observed: list[tuple[str, object]] = []

    async def consume() -> None:
        current = asyncio.current_task()
        assert current is not None
        try:
            await anext(stream)
        except RuntimeError as error:
            observed.append((str(error), current.cancelling()))
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError as cancellation:
            observed.append((str(cancellation), current.cancelling()))
        while current.cancelling():
            current.uncancel()

    task = asyncio.create_task(consume())
    await pulling.wait()
    task.cancel("external-caller")

    await task
    assert observed == [("batch close boom", 1), ("external-caller", 1)]
    assert stream.report.state is KernelState.FAILED
    assert [violation.code for violation in stream.report.violations] == ["cleanup_failure"]


@pytest.mark.asyncio
async def test_batch_snapshot_policy_controls_terminal_state() -> None:
    default_stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(_echo_batch))),
        [Request("profile", route=RouteKind.BARE)],
    )
    assert len([item async for item in default_stream]) == 1
    assert default_stream.report.state is KernelState.COMPLETED
    assert default_stream.report.snapshot is SnapshotState.NOT_REQUESTED

    stable_policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(snapshot_requirement=SnapshotRequirement.FROZEN_MANIFEST),
    )
    stable_stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(_echo_batch))),
        [Request("profile", route=RouteKind.BARE)],
        policy=stable_policy,
    )
    assert len([item async for item in stable_stream]) == 1
    assert stable_stream.report.state is KernelState.INCOMPLETE
    assert stable_stream.report.snapshot is SnapshotState.UNVERIFIED
    assert [violation.code for violation in stable_stream.report.violations] == ["snapshot_unverified"]


@pytest.mark.asyncio
async def test_batch_cancellation_carries_same_terminal_report() -> None:
    started = asyncio.Event()

    class BlockingTransport:
        host = "fixture.invalid"

        async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
            del request, attempt_timeout, max_response_bytes
            started.set()
            return await asyncio.Future[WireResponse]()

    stream = batch_outcome_stream(
        BatchExecutor(Executor(BlockingTransport())), [Request("profile", route=RouteKind.BARE)]
    )
    task = asyncio.create_task(anext(stream))
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError) as captured:
        await task

    assert captured.value.__dict__["report"] is stream.report
    assert stream.report.state is KernelState.CANCELLED


@pytest.mark.asyncio
async def test_shared_batch_page_reservations_roll_back_when_admission_is_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    second_admission = asyncio.Event()
    blocker = asyncio.Event()
    calls = 0
    original_reserve = ExecutionContext.reserve_page

    async def gated_reserve(context: ExecutionContext, *, reference: str | None = None) -> object:
        nonlocal calls
        calls += 1
        if calls == 2:  # noqa: PLR2004 - exact second-reservation cancellation window
            second_admission.set()
            await blocker.wait()
        return await original_reserve(context, reference=reference)

    monkeypatch.setattr(ExecutionContext, "reserve_page", gated_reserve)
    executor = Executor(CallbackTransport(_echo_batch))
    policy = ExecutionPolicy()
    context = executor.context(policy)
    stream = _BatchOutcomeStream(
        BatchExecutor(executor),
        [Request("one", route=RouteKind.BARE), Request("two", route=RouteKind.BARE)],
        batch_size=2,
        policy=policy,
        context=context,
        logical_page_per_command=True,
    )
    task = asyncio.create_task(anext(stream))
    await second_admission.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError) as captured:
        await task

    assert captured.value.__dict__["report"] is stream.report
    assert stream.report.state is KernelState.CANCELLED
    assert context._page_reservations == {}


@pytest.mark.asyncio
async def test_repeated_batch_cancellation_still_carries_final_report() -> None:
    started = asyncio.Event()

    class BlockingTransport:
        host = "fixture.invalid"

        async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
            del request, attempt_timeout, max_response_bytes
            started.set()
            return await asyncio.Future[WireResponse]()

    stream = batch_outcome_stream(
        BatchExecutor(Executor(BlockingTransport())), [Request("profile", route=RouteKind.BARE)]
    )
    hold = LedgerHold(stream._context)
    task = asyncio.create_task(anext(stream))
    await started.wait()
    hold.acquire()
    task.cancel()
    await hold.blocked.wait()
    task.cancel()
    hold.release()

    with pytest.raises(asyncio.CancelledError) as captured:
        await task

    assert captured.value.__dict__["report"] is stream.report
    assert stream.report.state is KernelState.CANCELLED


@pytest.mark.asyncio
async def test_batch_cancellation_during_failed_finalization_preserves_failure_report() -> None:
    class FailingSource:
        def __init__(self) -> None:
            self.hold: LedgerHold | None = None

        def __aiter__(self) -> FailingSource:
            return self

        async def __anext__(self) -> Request:
            assert self.hold is not None
            self.hold.acquire()
            raise RuntimeError("batch source failed")

    source = FailingSource()
    stream = batch_outcome_stream(BatchExecutor(Executor(CallbackTransport(_echo_batch))), source)
    hold = source.hold = LedgerHold(stream._context)
    primary: list[RuntimeError] = []
    post_failure_executed = False

    async def observe_replayed_cancellation() -> None:
        nonlocal post_failure_executed
        try:
            await anext(stream)
        except RuntimeError as error:
            primary.append(error)
        await asyncio.sleep(0)
        post_failure_executed = True

    task = asyncio.create_task(observe_replayed_cancellation())
    await hold.blocked.wait()
    task.cancel()
    hold.release()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert "batch source failed" in str(primary[0])
    assert primary[0].__dict__["report"] is stream.report
    assert post_failure_executed is False
    assert stream.report.state is KernelState.FAILED


@pytest.mark.asyncio
async def test_php_empty_error_array_is_valid_but_missing_or_nonempty_array_is_malformed() -> None:
    valid = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(_echo_batch))),
        [Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)],
    )
    assert isinstance(await anext(valid), BatchSuccess)

    def missing(request: Request) -> WireResponse:
        return _wire_batch(_batch_keys(request), omit_error_key=True)

    missing_stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(missing))),
        [Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)],
    )
    missing_outcome = await anext(missing_stream)
    assert isinstance(missing_outcome, BatchFailure)
    assert isinstance(missing_outcome.error, ProtocolError)

    def malformed(request: Request) -> WireResponse:
        return _wire_batch(_batch_keys(request), errors=[{"error": "bad"}])

    malformed_stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(malformed))),
        [Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)],
    )
    malformed_outcome = await anext(malformed_stream)
    assert isinstance(malformed_outcome, BatchFailure)
    assert isinstance(malformed_outcome.error, ProtocolError)


@pytest.mark.asyncio
async def test_php_empty_result_array_preserves_all_command_errors() -> None:
    def all_failed(request: Request) -> WireResponse:
        keys = _batch_keys(request)
        return _wire_batch(
            keys,
            errors={key: {"error": "error_not_found", "error_description": "gone"} for key in keys},
            empty_results=True,
        )

    requests = [Request("entity.get", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE) for index in range(3)]
    tolerant = batch_outcome_stream(BatchExecutor(Executor(CallbackTransport(all_failed))), requests)
    outcomes = cast("list[BatchOutcome]", [outcome async for outcome in tolerant])

    assert all(isinstance(outcome, BatchFailure) for outcome in outcomes)
    failures = [outcome for outcome in outcomes if isinstance(outcome, BatchFailure)]
    assert [outcome.error.normalized_code for outcome in failures if isinstance(outcome.error, BatchCommandError)] == [
        "error_not_found",
    ] * 3


@pytest.mark.asyncio
async def test_default_batch_preserves_unknown_total_sentinel() -> None:
    def unknown_total(request: Request) -> WireResponse:
        keys = _batch_keys(request)
        body = json.dumps(
            {
                "result": {
                    "result": {key: [] for key in keys},
                    "result_error": [],
                    "result_total": dict.fromkeys(keys, -1),
                },
            },
        ).encode()
        return WireResponse(200, (("content-type", "application/json"),), body)

    stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(unknown_total))),
        [Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)],
    )
    outcome = await anext(stream)

    assert isinstance(outcome, BatchSuccess)
    assert outcome.response is not None
    assert outcome.response.total == -1


@pytest.mark.asyncio
async def test_tolerant_per_command_error_and_missing_result_each_get_one_outcome() -> None:
    def partial(request: Request) -> WireResponse:
        keys = _batch_keys(request)
        return _wire_batch(
            keys,
            errors={keys[1]: {"error": "denied", "error_description": "no"}},
            omit_result_keys=frozenset({keys[1], keys[2]}),
        )

    stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(partial))),
        [Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE) for _index in range(3)],
    )
    outcomes = cast("list[BatchOutcome]", [outcome async for outcome in stream])

    assert [type(outcome) for outcome in outcomes] == [BatchSuccess, BatchFailure, BatchFailure]
    command_error = outcomes[1]
    missing_error = outcomes[2]
    assert isinstance(command_error, BatchFailure)
    assert isinstance(command_error.error, BatchCommandError)
    assert command_error.evidence is not None
    assert command_error.evidence.original_code == "denied"
    assert command_error.evidence.normalized_code == "denied"
    assert isinstance(missing_error, BatchFailure)
    assert isinstance(missing_error.error, ProtocolError)
    assert [outcome.command_index for outcome in outcomes] == [0, 1, 2]


@pytest.mark.asyncio
async def test_chunk_transport_failure_synthesizes_every_unresolved_outcome() -> None:
    def fail(_request: Request) -> Exception:
        return TransportError("connect", phase=FailurePhase.NOT_DISPATCHED)

    commands = [
        Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE) for _index in range(TEST_BATCH_SIZE)
    ]
    stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(fail))),
        commands,
        policy=_one_attempt_policy(),
    )
    outcomes = cast("list[BatchOutcome]", [outcome async for outcome in stream])

    assert len(outcomes) == TEST_BATCH_SIZE
    assert all(isinstance(outcome, BatchFailure) for outcome in outcomes)
    assert [outcome.command_index for outcome in outcomes] == list(range(TEST_BATCH_SIZE))
    assert stream.report.state is KernelState.COMPLETED


@pytest.mark.asyncio
async def test_overflowed_batch_result_becomes_totally_correlated_protocol_failure() -> None:
    def overflowed(request: Request) -> WireResponse:
        key = _batch_keys(request)[0]
        body = ('{"result":{"result":{"' + key + '":1e400},"result_error":[]}}').encode()
        return WireResponse(status_code=HTTP_OK, headers=(("content-type", "application/json"),), body=body)

    stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(overflowed))),
        [Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)],
    )
    outcomes = cast("list[BatchOutcome]", [outcome async for outcome in stream])

    assert len(outcomes) == 1
    assert isinstance(outcomes[0], BatchFailure)
    assert isinstance(outcomes[0].error, ProtocolError)
    assert outcomes[0].error.http_status == HTTP_OK
    assert outcomes[0].command_index == 0
    assert stream.report.state is KernelState.COMPLETED


@pytest.mark.asyncio
async def test_mixed_chunk_ambiguous_dispatch_is_not_replayed_and_keeps_total_correlation() -> None:
    transport = CallbackTransport(
        lambda _request: TransportError("reset", phase=FailurePhase.DISPATCH_STARTED),
    )
    stream = batch_outcome_stream(
        BatchExecutor(Executor(transport)),
        [
            Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
            Request("crm.deal.add", replay_safety=ReplaySafety.UNSAFE, route=RouteKind.BARE),
            Request("unknown", replay_safety=ReplaySafety.UNKNOWN, route=RouteKind.BARE),
        ],
        policy=_one_attempt_policy(),
    )
    outcomes = cast("list[BatchOutcome]", [outcome async for outcome in stream])

    assert len(transport.requests) == 1
    assert len(outcomes) == MIXED_COMMAND_COUNT
    assert all(isinstance(outcome, BatchFailure) for outcome in outcomes)
    failures = [outcome for outcome in outcomes if isinstance(outcome, BatchFailure)]
    assert all(outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE for outcome in failures)


@pytest.mark.asyncio
async def test_batch_defensively_rejects_a_route_suffixed_inner_method_without_io() -> None:
    request = Request("crm.item.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)
    object.__setattr__(request, "method", "crm.item.list.json")

    def unexpected(_request: Request) -> WireResponse:
        raise AssertionError("route-suffixed inner command reached transport")

    transport = CallbackTransport(unexpected)
    stream = batch_outcome_stream(BatchExecutor(Executor(transport)), [request])
    outcomes = cast("list[BatchOutcome]", [outcome async for outcome in stream])

    assert len(transport.requests) == 0
    assert len(outcomes) == 1
    assert isinstance(outcomes[0], BatchFailure)
    assert isinstance(outcomes[0].error, CapabilityError)


@pytest.mark.asyncio
async def test_batch_success_result_is_detached_from_caller_mutation() -> None:
    original = {"nested": [1]}
    success = BatchSuccess(0, "c0", Request("profile", route=RouteKind.BARE), original)
    original["nested"].append(2)
    first = success.result
    assert isinstance(first, dict)
    nested = first["nested"]
    assert isinstance(nested, list)
    nested.append(3)
    assert success.result == {"nested": [1]}


@pytest.mark.asyncio
async def test_batch_success_preserves_validated_per_command_pagination_metadata() -> None:
    def with_metadata(request: Request) -> WireResponse:
        key = _batch_keys(request)[0]
        body = json.dumps(
            {
                "result": {
                    "result": {key: [{"ID": 1}]},
                    "result_error": [],
                    "result_total": {key: EXPECTED_TOTAL},
                    "result_next": {key: 1},
                },
            },
        ).encode()
        return WireResponse(status_code=HTTP_OK, headers=(), body=body)

    stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(with_metadata))),
        [Request("profile", route=RouteKind.BARE)],
    )
    outcome = await anext(stream)
    assert isinstance(outcome, BatchSuccess)
    assert outcome.response is not None
    assert outcome.response.total == EXPECTED_TOTAL
    assert outcome.response.next == 1


@pytest.mark.asyncio
async def test_malformed_batch_pagination_metadata_is_correlated_failure() -> None:
    def malformed_metadata(request: Request) -> WireResponse:
        key = _batch_keys(request)[0]
        body = json.dumps(
            {
                "result": {
                    "result": {key: [{"ID": 1}]},
                    "result_error": [],
                    "result_total": {key: "three"},
                },
            },
        ).encode()
        return WireResponse(status_code=HTTP_OK, headers=(), body=body)

    stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(malformed_metadata))),
        [Request("profile", route=RouteKind.BARE)],
    )
    outcome = await anext(stream)
    assert isinstance(outcome, BatchFailure)
    assert isinstance(outcome.error, ProtocolError)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["extra", "duplicate"])
async def test_public_batch_preserves_tolerant_unknown_or_duplicate_correlation_keys(fault: str) -> None:
    def malformed_correlation(request: Request) -> WireResponse:
        key = _batch_keys(request)[0]
        if fault == "extra":
            body = json.dumps(
                {"result": {"result": {key: {}, "unexpected": {}}, "result_error": []}},
            ).encode()
        else:
            body = ('{"result":{"result":{"' + key + '":{},"' + key + '":{}},"result_error":[]}}').encode()
        return WireResponse(status_code=HTTP_OK, headers=(), body=body)

    stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(malformed_correlation))),
        [Request("profile", route=RouteKind.BARE)],
    )
    outcome = await anext(stream)

    assert isinstance(outcome, BatchSuccess)


@pytest.mark.asyncio
async def test_early_close_closes_original_sync_and_async_sources() -> None:
    sync_closed = False
    async_closed = False

    def sync_source() -> Iterator[Request]:
        nonlocal sync_closed
        try:
            while True:
                yield Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)
        finally:
            sync_closed = True

    async def async_source() -> AsyncGenerator[Request]:
        nonlocal async_closed
        try:
            while True:
                yield Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)
        finally:
            async_closed = True

    sync_stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(_echo_batch))), sync_source(), batch_size=1
    )
    await anext(sync_stream)
    await sync_stream.aclose()
    assert sync_closed is True

    async_stream = batch_outcome_stream(
        BatchExecutor(Executor(CallbackTransport(_echo_batch))), async_source(), batch_size=1
    )
    await anext(async_stream)
    await async_stream.aclose()
    assert async_closed is True


def test_physical_batch_size_and_non_request_input_fail_before_io() -> None:
    transport = CallbackTransport(_echo_batch)
    batch = BatchExecutor(Executor(transport))
    with pytest.raises(ValueError, match="batch_size"):
        batch_outcome_stream(batch, [], batch_size=0)
    stream = batch_outcome_stream(batch, [{"method": "profile", "extra": True}])

    async def consume() -> None:
        await anext(stream)

    with pytest.raises(TypeError, match="must yield Request"):
        asyncio.run(consume())
    assert not transport.requests
