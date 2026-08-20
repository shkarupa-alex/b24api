"""W3 tests for transport phases, replay, budgets, cooldown, and fairness."""

from __future__ import annotations
import asyncio

import httpx
import pytest

from b24api.error import (
    AmbiguousExecutionError,
    BudgetExceededError,
    FailurePhase,
    HTTPGatewayError,
    ProtocolError,
    TransportError,
)
from b24api.execution import (
    CoordinatorState,
    Executor,
    HttpxTransport,
    RateCoordinator,
    WireResponse,
    WorkClass,
)
from b24api.models import ExecutionPolicy, ReplaySafety, Request, RetryPolicy

EXPECTED_RETRIED_CALLS = 2
HTTP_OK = 200


class SequenceTransport:
    """Provide a deterministic test helper."""

    def __init__(self, outcomes: tuple[WireResponse | Exception, ...] | list[WireResponse | Exception]) -> None:
        """Initialize instance state."""
        self.outcomes = list(outcomes)
        self.calls = 0
        self.timeouts: list[float] = []

    async def send(self, request: Request, *, attempt_timeout: float) -> WireResponse:
        """Send one transport request attempt."""
        del request
        self.timeouts.append(attempt_timeout)
        outcome = self.outcomes[self.calls]
        self.calls += 1
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class CancellationTransport(httpx.AsyncBaseTransport):
    """Block after receiving a request so cancellation surfaces can be inspected."""

    def __init__(self) -> None:
        """Initialize synchronization state."""
        self.entered = asyncio.Event()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Wait indefinitely after HTTPX has materialized the credentialed request."""
        del request
        self.entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


def test_transport_constructor_drops_webhook_when_httpx_client_initialization_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sensitive_fragment = "synthetic-constructor-private-fragment"
    monkeypatch.setenv("HTTPS_PROXY", "http://example.invalid:bad")
    monkeypatch.delenv("NO_PROXY", raising=False)

    with pytest.raises(RuntimeError, match="initialization failed") as captured:
        HttpxTransport(f"https://example.invalid/rest/1/{sensitive_fragment}/")

    error = captured.value
    assert error.__cause__ is None
    assert error.__context__ is None
    traceback = error.__traceback__
    while traceback is not None:
        if traceback.tb_frame.f_code.co_filename.endswith("b24api/execution.py"):
            for value in traceback.tb_frame.f_locals.values():
                assert sensitive_fragment not in repr(value)
        traceback = traceback.tb_next


def _success(body: bytes = b'{"result":{"ok":true},"total":1,"next":1}') -> WireResponse:
    return WireResponse(status_code=200, headers=(("content-type", "application/json"),), body=body)


def _policy(*, attempts: int = 3, delay: float = 0.0, elapsed: float = 10.0) -> ExecutionPolicy:
    return ExecutionPolicy(
        max_requests=attempts,
        max_attempts_per_request=attempts,
        max_elapsed=elapsed,
        max_retry_elapsed_per_request=elapsed,
        retry=RetryPolicy(initial_delay=delay, maximum_delay=delay, jitter=0),
    )


@pytest.mark.asyncio
async def test_safe_and_unknown_retry_only_when_replay_is_proven() -> None:
    pre_dispatch = TransportError("connect", phase=FailurePhase.NOT_DISPATCHED)
    safe_transport = SequenceTransport([pre_dispatch, _success()])
    safe = await Executor(safe_transport).execute(
        Request("profile", replay_safety=ReplaySafety.SAFE),
        policy=_policy(),
    )
    assert safe.result == {"ok": True}
    assert safe_transport.calls == EXPECTED_RETRIED_CALLS

    unknown_transport = SequenceTransport(
        [TransportError("connect", phase=FailurePhase.CONNECTION_ESTABLISHED), _success()],
    )
    unknown = await Executor(unknown_transport).execute(Request("profile"), policy=_policy())
    assert unknown.result == {"ok": True}
    assert unknown_transport.calls == EXPECTED_RETRIED_CALLS


@pytest.mark.asyncio
@pytest.mark.parametrize("safety", [None, ReplaySafety.UNKNOWN, ReplaySafety.UNSAFE])
async def test_ambiguous_dispatch_never_retries_unproven_request(safety: ReplaySafety | None) -> None:
    transport = SequenceTransport(
        [TransportError("read failed", phase=FailurePhase.DISPATCH_STARTED), _success()],
    )

    with pytest.raises(AmbiguousExecutionError) as captured:
        await Executor(transport).execute(Request("crm.deal.add", replay_safety=safety), policy=_policy())

    assert transport.calls == 1
    assert isinstance(captured.value.__cause__, TransportError)
    assert captured.value.__cause__.phase is FailurePhase.DISPATCH_STARTED


@pytest.mark.asyncio
async def test_safe_ambiguous_transport_retries_with_counted_attempts() -> None:
    transport = SequenceTransport(
        [TransportError("partial", phase=FailurePhase.BODY_PARTIALLY_RECEIVED), _success()],
    )
    executor = Executor(transport)
    context = executor.context(_policy())

    response = await executor.execute(
        Request("profile", replay_safety=ReplaySafety.SAFE),
        context=context,
    )
    snapshot = await context.snapshot()

    assert response.result == {"ok": True}
    assert snapshot.counters.physical_requests == EXPECTED_RETRIED_CALLS
    assert snapshot.retries == 1


@pytest.mark.asyncio
async def test_operation_elapsed_clock_starts_at_first_execution_not_context_construction() -> None:
    now = [0.0]
    transport = SequenceTransport([_success()])
    executor = Executor(transport, clock=lambda: now[0])
    context = executor.context(_policy(elapsed=1))
    now[0] = 100.0

    response = await executor.execute(Request("profile"), context=context)

    assert response.result == {"ok": True}
    assert (await context.snapshot()).elapsed == 0


@pytest.mark.asyncio
async def test_retry_attempt_and_delay_budgets_terminate_before_extra_io() -> None:
    always_connect: tuple[WireResponse | Exception, ...] = (
        TransportError("connect", phase=FailurePhase.NOT_DISPATCHED),
        TransportError("connect", phase=FailurePhase.NOT_DISPATCHED),
    )
    transport = SequenceTransport(always_connect)
    with pytest.raises(BudgetExceededError, match="attempt"):
        await Executor(transport).execute(Request("profile"), policy=_policy(attempts=2))
    assert transport.calls == EXPECTED_RETRIED_CALLS

    delayed = SequenceTransport([WireResponse(status_code=503, headers=(), body=b"gateway")])
    with pytest.raises(BudgetExceededError, match="delay"):
        await Executor(delayed).execute(
            Request("profile", replay_safety=ReplaySafety.SAFE),
            policy=_policy(delay=2, elapsed=1),
        )
    assert delayed.calls == 1


@pytest.mark.asyncio
async def test_structured_throttle_uses_shared_cooldown_and_safe_replay() -> None:
    transport = SequenceTransport(
        [
            WireResponse(
                status_code=200,
                headers=(("retry-after", "0"),),
                body=b'{"error":"QUERY_LIMIT_EXCEEDED","error_description":"slow"}',
            ),
            _success(),
        ],
    )
    coordinator = RateCoordinator(max_concurrency=1)
    executor = Executor(transport, coordinator=coordinator)
    context = executor.context(_policy())

    await executor.execute(Request("profile", replay_safety=ReplaySafety.SAFE), context=context)
    snapshot = await context.snapshot()

    assert snapshot.retries == 1
    assert transport.calls == EXPECTED_RETRIED_CALLS
    await coordinator.close()


@pytest.mark.asyncio
async def test_coordinator_merges_cooldowns_and_does_not_let_retries_starve_interactive() -> None:
    coordinator = RateCoordinator(max_concurrency=1)
    first = await coordinator.acquire(WorkClass.INTERACTIVE_DIRECT)

    retry_one = asyncio.create_task(coordinator.acquire(WorkClass.RETRY))
    retry_two = asyncio.create_task(coordinator.acquire(WorkClass.RETRY))
    interactive = asyncio.create_task(coordinator.acquire(WorkClass.INTERACTIVE_DIRECT))
    await asyncio.sleep(0)
    await first.release()

    interactive_permit = await asyncio.wait_for(interactive, timeout=1)
    assert not retry_one.done()
    assert not retry_two.done()
    await interactive_permit.release()
    retry_permit = await asyncio.wait_for(retry_one, timeout=1)
    await retry_permit.release()
    second_retry_permit = await asyncio.wait_for(retry_two, timeout=1)
    await second_retry_permit.release()

    await coordinator.observe_throttle(0.01, reason="first")
    await coordinator.observe_throttle(0.03, reason="latest")
    snapshot = await coordinator.snapshot()
    assert snapshot.state is CoordinatorState.COOLDOWN
    assert snapshot.cooldown_reason == "latest"
    permit = await asyncio.wait_for(coordinator.acquire(WorkClass.BATCH), timeout=1)
    await permit.release()
    assert (await coordinator.snapshot()).state is CoordinatorState.OPEN
    await coordinator.close()


@pytest.mark.asyncio
async def test_cancelled_waiter_is_removed_without_leaking_permit() -> None:
    coordinator = RateCoordinator(max_concurrency=1)
    first = await coordinator.acquire(WorkClass.BATCH)
    waiter = asyncio.create_task(coordinator.acquire(WorkClass.TRAVERSAL_DIRECT))
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    await first.release()
    snapshot = await coordinator.snapshot()
    assert snapshot.active_permits == 0
    assert dict(snapshot.queued)[WorkClass.TRAVERSAL_DIRECT] == 0
    await coordinator.close()


@pytest.mark.asyncio
async def test_cancellation_after_grant_returns_capacity() -> None:
    coordinator = RateCoordinator(max_concurrency=1)
    held = await coordinator.acquire(WorkClass.INTERACTIVE_DIRECT)
    waiter = asyncio.create_task(coordinator.acquire(WorkClass.TRAVERSAL_DIRECT))
    await asyncio.sleep(0)

    await held.release()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    assert (await coordinator.snapshot()).active_permits == 0
    replacement = await asyncio.wait_for(coordinator.acquire(WorkClass.BATCH), timeout=1)
    await replacement.release()
    await coordinator.close()


@pytest.mark.asyncio
async def test_permit_wait_is_bounded_without_counting_or_dispatching_an_attempt() -> None:
    coordinator = RateCoordinator(max_concurrency=1)
    held = await coordinator.acquire(WorkClass.BATCH)
    transport = SequenceTransport([_success()])
    executor = Executor(transport, coordinator=coordinator)
    context = executor.context(
        ExecutionPolicy(max_elapsed=0.02, max_retry_elapsed_per_request=0.02),
    )

    with pytest.raises(BudgetExceededError, match="permit wait"):
        await executor.execute(
            Request("profile", replay_safety=ReplaySafety.SAFE),
            context=context,
        )

    assert transport.calls == 0
    assert (await context.snapshot()).counters.physical_requests == 0
    await held.release()
    await coordinator.close()


@pytest.mark.asyncio
async def test_socket_connect_and_post_dispatch_failures_have_distinct_phases() -> None:
    server = await asyncio.start_server(lambda _reader, writer: writer.close(), "127.0.0.1", 0)
    socket = server.sockets[0]
    assert socket is not None
    host, port = socket.getsockname()[:2]
    transport = HttpxTransport(f"http://{host}:{port}/test-endpoint/")
    try:
        with pytest.raises(TransportError) as post_dispatch:
            await transport.send(Request("profile"), attempt_timeout=1)
        assert post_dispatch.value.phase is FailurePhase.DISPATCH_STARTED
    finally:
        await transport.aclose()
        server.close()
        await server.wait_closed()

    dead_transport = HttpxTransport(f"http://{host}:{port}/test-endpoint/")
    try:
        with pytest.raises(TransportError) as not_dispatched:
            await dead_transport.send(Request("profile"), attempt_timeout=0.2)
        assert not_dispatched.value.phase is FailurePhase.NOT_DISPATCHED
    finally:
        await dead_transport.aclose()


@pytest.mark.asyncio
async def test_no_trace_read_failure_is_conservatively_post_dispatch_and_never_replayed() -> None:
    class NoTraceTransport(httpx.AsyncBaseTransport):
        def __init__(self) -> None:
            self.calls = 0

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            self.calls += 1
            if self.calls == 1:
                raise httpx.ReadError("delivered then reset", request=request)
            return httpx.Response(200, request=request, json={"result": {"unexpected_replay": True}})

    raw_transport = NoTraceTransport()
    client = httpx.AsyncClient(transport=raw_transport)
    transport = HttpxTransport("https://example.invalid/test-endpoint/", client=client)
    try:
        with pytest.raises(AmbiguousExecutionError) as captured:
            await Executor(transport).execute(
                Request("crm.deal.add", replay_safety=ReplaySafety.UNSAFE),
                policy=_policy(),
            )
        assert raw_transport.calls == 1
        assert isinstance(captured.value.__cause__, TransportError)
        assert captured.value.__cause__.phase is FailurePhase.DISPATCH_STARTED
    finally:
        await transport.aclose()
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_type",
    [httpx.ReadError, httpx.WriteError, httpx.RemoteProtocolError, httpx.TransportError],
)
async def test_no_trace_post_dispatch_error_classes_have_conservative_minimum_phase(
    error_type: type[httpx.TransportError],
) -> None:
    class RaisingTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise error_type("no trace evidence", request=request)

    client = httpx.AsyncClient(transport=RaisingTransport())
    transport = HttpxTransport("https://example.invalid/test-endpoint/", client=client)
    try:
        with pytest.raises(TransportError) as captured:
            await transport.send(Request("profile"), attempt_timeout=1)
        assert captured.value.phase is FailurePhase.DISPATCH_STARTED
        assert captured.value.possible_acceptance is True
    finally:
        await transport.aclose()
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_type",
    [httpx.ConnectError, httpx.DecodingError, httpx.TooManyRedirects],
)
async def test_transport_error_drops_credentialed_httpx_exception_and_request_locals(
    error_type: type[httpx.RequestError],
) -> None:
    sensitive_fragment = "synthetic-private-fragment-9f4a"

    class RefusingTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise error_type("hostile request failure", request=request)

    client = httpx.AsyncClient(transport=RefusingTransport())
    transport = HttpxTransport(f"https://example.invalid/rest/1/{sensitive_fragment}/", client=client)
    try:
        with pytest.raises(TransportError) as captured:
            await transport.send(Request("profile"), attempt_timeout=1)

        error = captured.value
        assert error.__cause__ is None
        assert error.__context__ is None
        traceback = error.__traceback__
        while traceback is not None:
            if traceback.tb_frame.f_code.co_name == "send":
                for value in traceback.tb_frame.f_locals.values():
                    assert sensitive_fragment not in repr(value)
            traceback = traceback.tb_next
    finally:
        await transport.aclose()
        await client.aclose()


@pytest.mark.asyncio
async def test_transport_cancellation_drops_httpx_traceback_and_request_locals() -> None:
    sensitive_fragment = "synthetic-private-fragment-cancel"
    blocking = CancellationTransport()
    client = httpx.AsyncClient(transport=blocking)
    transport = HttpxTransport(f"https://example.invalid/rest/1/{sensitive_fragment}/", client=client)
    task = asyncio.create_task(transport.send(Request("profile"), attempt_timeout=1))
    try:
        await blocking.entered.wait()
        task.cancel("caller cancelled")

        with pytest.raises(asyncio.CancelledError) as captured:
            await task

        error = captured.value
        assert error.args == ("caller cancelled",)
        assert error.__cause__ is None
        assert error.__context__ is None
        traceback = error.__traceback__
        while traceback is not None:
            if traceback.tb_frame.f_code.co_name == "send":
                assert traceback.tb_frame.f_code.co_filename.endswith("b24api/execution.py")
                for value in traceback.tb_frame.f_locals.values():
                    assert sensitive_fragment not in repr(value)
            traceback = traceback.tb_next
    finally:
        if not task.done():
            task.cancel()
        await transport.aclose()
        await client.aclose()


@pytest.mark.asyncio
async def test_socket_partial_body_failure_is_classified_after_headers() -> None:
    async def partial_body(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")
        writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 20\r\nContent-Type: application/json\r\n\r\n{"res')
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(partial_body, "127.0.0.1", 0)
    socket = server.sockets[0]
    assert socket is not None
    host, port = socket.getsockname()[:2]
    transport = HttpxTransport(f"http://{host}:{port}/test-endpoint/")
    try:
        with pytest.raises(TransportError) as captured:
            await transport.send(Request("profile"), attempt_timeout=1)
        assert captured.value.phase is FailurePhase.BODY_PARTIALLY_RECEIVED
        assert captured.value.possible_acceptance is True
    finally:
        await transport.aclose()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_out_of_range_socket_status_is_typed_and_drops_webhook_locals() -> None:
    sensitive_fragment = "synthetic-private-fragment-status-999"

    async def hostile_status(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")
        writer.write(b"HTTP/1.1 999 Hostile\r\nContent-Length: 2\r\n\r\n{}")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(hostile_status, "127.0.0.1", 0)
    socket = server.sockets[0]
    assert socket is not None
    host, port = socket.getsockname()[:2]
    transport = HttpxTransport(f"http://{host}:{port}/rest/1/{sensitive_fragment}/")
    try:
        with pytest.raises(ProtocolError, match="outside the valid range") as captured:
            await transport.send(Request("profile"), attempt_timeout=1)

        error = captured.value
        assert error.__cause__ is None
        assert error.__context__ is None
        traceback = error.__traceback__
        while traceback is not None:
            if traceback.tb_frame.f_code.co_filename.endswith("b24api/execution.py"):
                for value in traceback.tb_frame.f_locals.values():
                    assert sensitive_fragment not in repr(value)
            traceback = traceback.tb_next
    finally:
        await transport.aclose()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_socket_cancellation_propagates_and_counts_dispatched_attempt() -> None:
    received = asyncio.Event()
    release_server = asyncio.Event()

    async def hanging_response(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")
        received.set()
        await release_server.wait()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(hanging_response, "127.0.0.1", 0)
    socket = server.sockets[0]
    assert socket is not None
    host, port = socket.getsockname()[:2]
    transport = HttpxTransport(f"http://{host}:{port}/test-endpoint/")
    executor = Executor(transport)
    context = executor.context(_policy())
    task = asyncio.create_task(
        executor.execute(
            Request("profile", replay_safety=ReplaySafety.SAFE),
            context=context,
        ),
    )
    try:
        await asyncio.wait_for(received.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert (await context.snapshot()).counters.physical_requests == 1
    finally:
        release_server.set()
        await transport.aclose()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_negative_one_total_sentinel_is_preserved_but_lower_values_are_typed_errors() -> None:
    sentinel = SequenceTransport([_success(b'{"result":[],"total":-1}')])
    response = await Executor(sentinel).execute(Request("im.recent.list"), policy=_policy())
    assert response.total == -1

    invalid = SequenceTransport([_success(b'{"result":[],"total":-2}')])
    with pytest.raises(HTTPGatewayError) as captured:
        await Executor(invalid).execute(Request("profile"), policy=_policy())
    assert captured.value.http_status == HTTP_OK


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b'{"result":[],"next":-1}', id="negative-next"),
        pytest.param(b'{"result":1e400}', id="overflowed-result-number"),
        pytest.param(b'{"result":[],"time":{"duration":-1}}', id="negative-server-duration"),
    ],
)
async def test_success_model_contract_failures_are_typed_and_keep_http_evidence(body: bytes) -> None:
    transport = SequenceTransport([_success(body)])

    with pytest.raises(ProtocolError) as captured:
        await Executor(transport).execute(Request("profile"), policy=_policy())

    assert captured.value.http_status == HTTP_OK
    assert captured.value.request_summary is not None
    assert captured.value.request_summary.method == "profile"
    assert isinstance(captured.value.__cause__, ValueError)
