"""W4 tests for bounded batch streams and total command correlation."""

from __future__ import annotations
import asyncio
import json
import threading
from typing import TYPE_CHECKING, cast

import pytest

from b24api.batch import BatchExecutor
from b24api.error import BatchCommandError, BudgetExceededError, FailurePhase, ProtocolError, TransportError
from b24api.execution import Executor, WireResponse
from b24api.models import (
    BatchFailure,
    BatchOutcome,
    BatchSuccess,
    ConsistencyPolicy,
    ExecutionPolicy,
    ReplayDisposition,
    ReplaySafety,
    Request,
    SnapshotRequirement,
    SnapshotState,
    TerminalState,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterator

TEST_BATCH_SIZE = 7
TEST_COMMAND_COUNT = 23
EXPECTED_CHUNKS = 4
MIXED_COMMAND_COUNT = 3
FALLBACK_HTTP_CALLS = 2
HTTP_OK = 200
EXPECTED_TOTAL = 3
PULL_TEST_TIMEOUT = 0.15
PARTIAL_COMMAND_COUNT = 2


class CallbackTransport:
    def __init__(self, callback: Callable[[Request], WireResponse | Exception]) -> None:
        self.callback = callback
        self.requests: list[Request] = []

    async def send(self, request: Request, *, attempt_timeout: float) -> WireResponse:
        assert attempt_timeout > 0
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
async def test_fail_fast_stream_is_lazy_bounded_ordered_and_payload_correlated() -> None:
    pulled = 0

    def requests() -> Iterator[tuple[Request, object]]:
        nonlocal pulled
        for index in range(TEST_COMMAND_COUNT):
            pulled += 1
            yield Request("profile", {"index": index}, ReplaySafety.SAFE), f"payload-{index}"

    source = requests()
    transport = CallbackTransport(_echo_batch)
    stream = BatchExecutor(Executor(transport)).batch(
        source,
        batch_size=TEST_BATCH_SIZE,
        with_payload=True,
    )

    assert not transport.requests
    first = await anext(stream)
    assert first == ({"key": "c000000000000"}, "payload-0")
    assert pulled == TEST_BATCH_SIZE
    assert len(transport.requests) == 1
    results = [first, *[item async for item in stream]]

    assert len(results) == TEST_COMMAND_COUNT
    assert len(transport.requests) == EXPECTED_CHUNKS
    assert all(request.copy_parameters()["halt"] == 1 for request in transport.requests)
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.batch_commands == TEST_COMMAND_COUNT
    assert stream.report.batch_requests == EXPECTED_CHUNKS
    assert pulled == TEST_COMMAND_COUNT


@pytest.mark.asyncio
async def test_async_unlimited_input_pulls_only_one_bounded_chunk_before_first_yield() -> None:
    pulled = 0

    async def requests() -> AsyncGenerator[Request]:
        nonlocal pulled
        for _index in range(TEST_COMMAND_COUNT):
            pulled += 1
            yield Request("profile", replay_safety=ReplaySafety.SAFE)

    transport = CallbackTransport(_echo_batch)
    stream = BatchExecutor(Executor(transport)).batch_outcomes(
        requests(),
        batch_size=TEST_BATCH_SIZE,
    )
    first = await anext(stream)

    assert isinstance(first, BatchSuccess)
    assert pulled == TEST_BATCH_SIZE
    assert transport.requests[0].copy_parameters()["halt"] == 0
    await stream.aclose()
    assert stream.report.state is TerminalState.CANCELLED


@pytest.mark.asyncio
async def test_context_entry_starts_batch_execution_without_counting_prefetch_as_emitted() -> None:
    transport = CallbackTransport(_echo_batch)
    stream = BatchExecutor(Executor(transport)).batch_outcomes([Request("profile")], batch_size=1)

    async with stream:
        assert len(transport.requests) == 1

    assert stream.report.state is TerminalState.CANCELLED
    assert stream.report.emitted_rows == 0


@pytest.mark.asyncio
async def test_batch_source_cleanup_error_carries_same_report() -> None:
    async def source() -> AsyncGenerator[Request]:
        try:
            while True:
                yield Request("profile")
        finally:
            raise RuntimeError("batch source close boom")

    stream = BatchExecutor(Executor(CallbackTransport(_echo_batch))).batch_outcomes(source(), batch_size=1)
    assert isinstance(await anext(stream), BatchSuccess)

    with pytest.raises(RuntimeError, match="batch source close boom") as captured:
        await stream.aclose()

    assert captured.value.__dict__["report"] is stream.report
    assert stream.report.state is TerminalState.CANCELLED


@pytest.mark.asyncio
async def test_blocking_sync_batch_source_close_obeys_cleanup_deadline() -> None:
    release_close = threading.Event()
    close_finished = threading.Event()

    class BlockingCloseIterator:
        def __iter__(self) -> BlockingCloseIterator:
            return self

        def __next__(self) -> Request:
            return Request("profile")

        def close(self) -> None:
            release_close.wait()
            close_finished.set()

    stream = BatchExecutor(Executor(CallbackTransport(_echo_batch))).batch_outcomes(
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
        yield Request("profile")
        await asyncio.Future[None]()

    stream = BatchExecutor(Executor(CallbackTransport(_echo_batch))).batch_outcomes(
        source(),
        batch_size=1,
        policy=ExecutionPolicy(max_elapsed=0.03),
    )

    assert isinstance(await anext(stream), BatchSuccess)
    with pytest.raises(BudgetExceededError, match="batch input") as captured:
        await asyncio.wait_for(anext(stream), timeout=PULL_TEST_TIMEOUT)

    assert captured.value.__dict__["report"] is stream.report
    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_cancellation_resistant_batch_pull_is_closed_after_late_completion() -> None:
    release = asyncio.Event()
    closed = asyncio.Event()

    async def source() -> AsyncGenerator[Request]:
        try:
            yield Request("profile")
            try:
                await asyncio.Future[None]()
            except asyncio.CancelledError:
                await release.wait()
            yield Request("late")
        finally:
            closed.set()

    stream = BatchExecutor(Executor(CallbackTransport(_echo_batch))).batch_outcomes(
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
                return Request("profile")
            release.wait()
            return Request("late")

        def close(self) -> None:
            closed.set()

    stream = BatchExecutor(Executor(CallbackTransport(_echo_batch))).batch_outcomes(
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
async def test_partial_tolerant_chunk_gets_correlated_failures_before_source_error() -> None:
    async def source() -> AsyncGenerator[Request]:
        yield Request("a")
        yield Request("b")
        raise RuntimeError("batch source boom")

    transport = CallbackTransport(_echo_batch)
    stream = BatchExecutor(Executor(transport)).batch_outcomes(source(), batch_size=3)
    outcomes: list[BatchOutcome] = []

    async def consume() -> None:
        while True:
            outcome = await anext(stream)
            assert isinstance(outcome, BatchSuccess | BatchFailure)
            outcomes.append(outcome)

    with pytest.raises(RuntimeError, match="batch source boom") as captured:
        await consume()

    assert captured.value.__dict__["report"] is stream.report
    assert [outcome.command_index for outcome in outcomes] == [0, 1]
    assert all(isinstance(outcome, BatchFailure) for outcome in outcomes)
    assert transport.requests == []
    assert stream.report.emitted_rows == PARTIAL_COMMAND_COUNT
    assert stream.report.state is TerminalState.FAILED


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
                return Request("profile")
            pulling.set()
            await asyncio.Future[None]()
            raise AssertionError("unreachable")

        async def aclose(self) -> None:
            raise RuntimeError("batch close boom")

    stream = BatchExecutor(Executor(CallbackTransport(_echo_batch))).batch_outcomes(
        RaisingCloseSource(),
        batch_size=1,
    )
    assert isinstance(await anext(stream), BatchSuccess)
    task = asyncio.create_task(anext(stream))
    await pulling.wait()
    task.cancel()

    with pytest.raises(RuntimeError, match="batch close boom") as captured:
        await task
    assert captured.value.__dict__["report"] is stream.report


@pytest.mark.asyncio
async def test_batch_snapshot_policy_controls_terminal_state() -> None:
    default_stream = BatchExecutor(Executor(CallbackTransport(_echo_batch))).batch_outcomes([Request("profile")])
    assert len([item async for item in default_stream]) == 1
    assert default_stream.report.state is TerminalState.COMPLETED
    assert default_stream.report.snapshot is SnapshotState.NOT_REQUESTED

    stable_policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(snapshot_requirement=SnapshotRequirement.FROZEN_MANIFEST),
    )
    stable_stream = BatchExecutor(Executor(CallbackTransport(_echo_batch))).batch_outcomes(
        [Request("profile")],
        policy=stable_policy,
    )
    assert len([item async for item in stable_stream]) == 1
    assert stable_stream.report.state is TerminalState.INCOMPLETE
    assert stable_stream.report.snapshot is SnapshotState.UNVERIFIED
    assert [violation.code for violation in stable_stream.report.violations] == ["snapshot_unverified"]


@pytest.mark.asyncio
async def test_batch_cancellation_carries_same_terminal_report() -> None:
    started = asyncio.Event()

    class BlockingTransport:
        async def send(self, request: Request, *, attempt_timeout: float) -> WireResponse:
            del request, attempt_timeout
            started.set()
            return await asyncio.Future[WireResponse]()

    stream = BatchExecutor(Executor(BlockingTransport())).batch_outcomes([Request("profile")])
    task = asyncio.create_task(anext(stream))
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError) as captured:
        await task

    assert captured.value.__dict__["report"] is stream.report
    assert stream.report.state is TerminalState.CANCELLED


@pytest.mark.asyncio
async def test_repeated_batch_cancellation_still_carries_final_report() -> None:
    started = asyncio.Event()

    class BlockingTransport:
        async def send(self, request: Request, *, attempt_timeout: float) -> WireResponse:
            del request, attempt_timeout
            started.set()
            return await asyncio.Future[WireResponse]()

    stream = BatchExecutor(Executor(BlockingTransport())).batch_outcomes([Request("profile")])
    task = asyncio.create_task(anext(stream))
    await started.wait()
    await stream._context._lock.acquire()  # noqa: SLF001 - deterministic repeated-cancel regression
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    stream._context._lock.release()  # noqa: SLF001 - deterministic repeated-cancel regression

    with pytest.raises(asyncio.CancelledError) as captured:
        await task

    assert captured.value.__dict__["report"] is stream.report
    assert stream.report.state is TerminalState.CANCELLED


@pytest.mark.asyncio
async def test_php_empty_error_array_is_valid_but_missing_or_nonempty_array_is_malformed() -> None:
    valid = BatchExecutor(Executor(CallbackTransport(_echo_batch))).batch_outcomes(
        [Request("profile", replay_safety=ReplaySafety.SAFE)],
    )
    assert isinstance(await anext(valid), BatchSuccess)

    def missing(request: Request) -> WireResponse:
        return _wire_batch(_batch_keys(request), omit_error_key=True)

    missing_stream = BatchExecutor(Executor(CallbackTransport(missing))).batch_outcomes(
        [Request("profile", replay_safety=ReplaySafety.SAFE)],
    )
    missing_outcome = await anext(missing_stream)
    assert isinstance(missing_outcome, BatchFailure)
    assert isinstance(missing_outcome.error, ProtocolError)

    def malformed(request: Request) -> WireResponse:
        return _wire_batch(_batch_keys(request), errors=[{"error": "bad"}])

    malformed_stream = BatchExecutor(Executor(CallbackTransport(malformed))).batch(
        [Request("profile", replay_safety=ReplaySafety.SAFE)],
    )
    with pytest.raises(ProtocolError):
        await anext(malformed_stream)
    assert malformed_stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_php_empty_result_array_preserves_all_command_errors() -> None:
    def all_failed(request: Request) -> WireResponse:
        keys = _batch_keys(request)
        return _wire_batch(
            keys,
            errors={key: {"error": "error_not_found", "error_description": "gone"} for key in keys},
            empty_results=True,
        )

    requests = [Request("entity.get", {"id": index}, ReplaySafety.SAFE) for index in range(3)]
    tolerant = BatchExecutor(Executor(CallbackTransport(all_failed))).batch_outcomes(requests)
    outcomes = cast("list[BatchOutcome]", [outcome async for outcome in tolerant])

    assert all(isinstance(outcome, BatchFailure) for outcome in outcomes)
    failures = [outcome for outcome in outcomes if isinstance(outcome, BatchFailure)]
    assert [outcome.error.normalized_code for outcome in failures if isinstance(outcome.error, BatchCommandError)] == [
        "error_not_found",
    ] * 3

    fail_fast = BatchExecutor(Executor(CallbackTransport(all_failed))).batch(requests)
    with pytest.raises(BatchCommandError) as captured:
        await anext(fail_fast)
    assert captured.value.normalized_code == "error_not_found"


@pytest.mark.asyncio
async def test_tolerant_per_command_error_and_missing_result_each_get_one_outcome() -> None:
    def partial(request: Request) -> WireResponse:
        keys = _batch_keys(request)
        return _wire_batch(
            keys,
            errors={keys[1]: {"error": "denied", "error_description": "no"}},
            omit_result_keys=frozenset({keys[1], keys[2]}),
        )

    stream = BatchExecutor(Executor(CallbackTransport(partial))).batch_outcomes(
        [Request("profile", replay_safety=ReplaySafety.SAFE) for _index in range(3)],
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

    commands = [Request("profile", replay_safety=ReplaySafety.SAFE) for _index in range(TEST_BATCH_SIZE)]
    stream = BatchExecutor(Executor(CallbackTransport(fail))).batch_outcomes(
        commands,
        policy=_one_attempt_policy(),
    )
    outcomes = cast("list[BatchOutcome]", [outcome async for outcome in stream])

    assert len(outcomes) == TEST_BATCH_SIZE
    assert all(isinstance(outcome, BatchFailure) for outcome in outcomes)
    assert [outcome.command_index for outcome in outcomes] == list(range(TEST_BATCH_SIZE))
    assert stream.report.state is TerminalState.COMPLETED


@pytest.mark.asyncio
async def test_overflowed_batch_result_becomes_totally_correlated_protocol_failure() -> None:
    def overflowed(request: Request) -> WireResponse:
        key = _batch_keys(request)[0]
        body = ('{"result":{"result":{"' + key + '":1e400},"result_error":[]}}').encode()
        return WireResponse(status_code=HTTP_OK, headers=(("content-type", "application/json"),), body=body)

    stream = BatchExecutor(Executor(CallbackTransport(overflowed))).batch_outcomes(
        [Request("profile", replay_safety=ReplaySafety.SAFE)],
    )
    outcomes = cast("list[BatchOutcome]", [outcome async for outcome in stream])

    assert len(outcomes) == 1
    assert isinstance(outcomes[0], BatchFailure)
    assert isinstance(outcomes[0].error, ProtocolError)
    assert outcomes[0].error.http_status == HTTP_OK
    assert outcomes[0].command_index == 0
    assert stream.report.state is TerminalState.COMPLETED


@pytest.mark.asyncio
async def test_mixed_chunk_ambiguous_dispatch_is_not_replayed_and_keeps_total_correlation() -> None:
    transport = CallbackTransport(
        lambda _request: TransportError("reset", phase=FailurePhase.DISPATCH_STARTED),
    )
    stream = BatchExecutor(Executor(transport)).batch_outcomes(
        [
            Request("profile", replay_safety=ReplaySafety.SAFE),
            Request("crm.deal.add", replay_safety=ReplaySafety.UNSAFE),
            Request("unknown", replay_safety=ReplaySafety.UNKNOWN),
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
async def test_direct_fallback_reruns_only_safe_eligible_failure() -> None:
    def batch_then_direct(request: Request) -> WireResponse:
        if request.method == "batch":
            keys = _batch_keys(request)
            return _wire_batch(
                keys,
                errors={key: {"error": "batch_hostile", "error_description": "retry direct"} for key in keys},
                empty_results=True,
            )
        return WireResponse(status_code=200, headers=(), body=b'{"result":{"direct":true}}')

    transport = CallbackTransport(batch_then_direct)
    stream = BatchExecutor(
        Executor(transport),
        fallback_eligible_codes=frozenset({"batch_hostile"}),
    ).batch_outcomes(
        [
            Request("profile", replay_safety=ReplaySafety.SAFE),
            Request("crm.deal.add", replay_safety=ReplaySafety.UNSAFE),
        ],
        fallback_failed="direct",
    )
    outcomes = cast("list[BatchOutcome]", [outcome async for outcome in stream])

    assert len(transport.requests) == FALLBACK_HTTP_CALLS
    assert isinstance(outcomes[0], BatchSuccess)
    assert outcomes[0].result == {"direct": True}
    assert outcomes[0].replay_disposition is ReplayDisposition.REPLAYED_DIRECT
    assert isinstance(outcomes[1], BatchFailure)
    assert outcomes[1].replay_disposition is ReplayDisposition.NOT_ELIGIBLE


@pytest.mark.asyncio
async def test_batch_success_result_is_detached_from_caller_mutation() -> None:
    original = {"nested": [1]}
    success = BatchSuccess(0, "c0", Request("profile"), original)
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

    stream = BatchExecutor(Executor(CallbackTransport(with_metadata))).batch_outcomes([Request("profile")])
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

    stream = BatchExecutor(Executor(CallbackTransport(malformed_metadata))).batch_outcomes([Request("profile")])
    outcome = await anext(stream)
    assert isinstance(outcome, BatchFailure)
    assert isinstance(outcome.error, ProtocolError)


@pytest.mark.asyncio
async def test_early_close_closes_original_sync_and_async_sources() -> None:
    sync_closed = False
    async_closed = False

    def sync_source() -> Iterator[Request]:
        nonlocal sync_closed
        try:
            while True:
                yield Request("profile", replay_safety=ReplaySafety.SAFE)
        finally:
            sync_closed = True

    async def async_source() -> AsyncGenerator[Request]:
        nonlocal async_closed
        try:
            while True:
                yield Request("profile", replay_safety=ReplaySafety.SAFE)
        finally:
            async_closed = True

    sync_stream = BatchExecutor(Executor(CallbackTransport(_echo_batch))).batch(sync_source(), batch_size=1)
    await anext(sync_stream)
    await sync_stream.aclose()
    assert sync_closed is True

    async_stream = BatchExecutor(Executor(CallbackTransport(_echo_batch))).batch(async_source(), batch_size=1)
    await anext(async_stream)
    await async_stream.aclose()
    assert async_closed is True


def test_batch_size_and_mapping_shape_fail_before_io() -> None:
    transport = CallbackTransport(_echo_batch)
    batch = BatchExecutor(Executor(transport))
    with pytest.raises(ValueError, match="batch_size"):
        batch.batch([], batch_size=0)
    stream = batch.batch([{"method": "profile", "extra": True}])

    async def consume() -> None:
        await anext(stream)

    with pytest.raises(ValueError, match="unknown request fields"):
        asyncio.run(consume())
    assert not transport.requests
