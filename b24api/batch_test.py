"""W4 tests for bounded batch streams and total command correlation."""

from __future__ import annotations
import asyncio
import json
from typing import TYPE_CHECKING, cast

import pytest

from b24api.batch import BatchExecutor
from b24api.error import BatchCommandError, FailurePhase, ProtocolError, TransportError
from b24api.execution import Executor, WireResponse
from b24api.models import (
    BatchFailure,
    BatchOutcome,
    BatchSuccess,
    ExecutionPolicy,
    ReplayDisposition,
    ReplaySafety,
    Request,
    TerminalState,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterator

TEST_BATCH_SIZE = 7
TEST_COMMAND_COUNT = 23
EXPECTED_CHUNKS = 4
MIXED_COMMAND_COUNT = 3
FALLBACK_HTTP_CALLS = 2


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
