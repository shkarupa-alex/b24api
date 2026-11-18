"""W3 tests for transport phases, replay, budgets, cooldown, and fairness."""

from __future__ import annotations
import asyncio
import contextlib
import dataclasses
import gc
import weakref
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import pytest

import b24api
from b24api import execution as execution_module
from b24api.contracts import Command, CommandFailure
from b24api.contracts.policy import ExecutionPolicy, RetryPolicy
from b24api.contracts.request import ReplaySafety, Request, RouteKind
from b24api.errors import (
    AmbiguousExecutionError,
    ApiResponseError,
    BudgetExceededError,
    ClientClosedError,
    FailurePhase,
    HTTPGatewayError,
    ProtocolError,
    ResponseTooLargeError,
    TransportError,
)
from b24api.execution import (
    CoordinatorState,
    Executor,
    HttpxTransport,
    RateCoordinator,
    WireResponse,
    WorkClass,
    rearm_cancellation,
)
from b24api.execution.context import LatchState, OperationLatch, is_interrupt, settle
from b24api.execution.facts import Execution, execution
from b24api.execution.rate import (
    CoordinatorBudgetError,
    DeadlineBudget,
    RatePolicyCapacityError,
)
from b24api.testing import ScriptedTransport
from b24api.transport import TransportCapabilities
from b24api.transport import httpx as httpx_transport_module
from tests.scripting import Blocker, ResponderTransport, client_for, replies

if TYPE_CHECKING:
    from collections.abc import Awaitable, Generator, Iterable

    from b24api.transport import WireRequest
    from tests.scripting import ClientFactory, Responder

EXPECTED_RETRIED_CALLS = 2
HTTP_OK = 200
SMALL_RESPONSE_CEILING = 8


def _sequence(outcomes: Iterable[WireResponse | Exception]) -> ResponderTransport:
    """Answer the n-th send with the n-th outcome; an exception outcome is raised."""
    return ResponderTransport(replies(*outcomes))


@pytest.mark.asyncio
async def test_internal_cancelled_error_never_cancels_the_caller() -> None:
    assert asyncio.current_task() is not None
    assert asyncio.current_task().cancelling() == 0

    rearm_cancellation(asyncio.CancelledError("internal cleanup signal"))
    await asyncio.sleep(0)

    assert asyncio.current_task().cancelling() == 0


@pytest.mark.asyncio
async def test_rearmed_cancellation_preserves_nested_cancellation_count() -> None:
    observations: list[int] = []

    async def worker() -> None:
        current = asyncio.current_task()
        assert current is not None
        current.cancel("outer")
        current.cancel("inner")
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError as cancellation:
            observations.append(current.cancelling())
            rearm_cancellation(cancellation)
            observations.append(current.cancelling())
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            observations.append(current.cancelling())
        while current.cancelling():
            current.uncancel()

    task = asyncio.create_task(worker())
    await task

    assert observations == [2, 2, 2]


@pytest.mark.asyncio
async def test_external_cancellation_wins_over_owned_task_cancellation() -> None:
    observed: list[tuple[object, ...]] = []

    async def owned() -> None:
        await asyncio.Event().wait()

    async def worker() -> None:
        current = asyncio.current_task()
        assert current is not None
        child = asyncio.create_task(owned())
        loop = asyncio.get_running_loop()
        loop.call_soon(current.cancel, "external-caller")
        loop.call_soon(child.cancel, "internal-owned")

        cancellation = await execution_module.await_cancellation_resistant(child)
        assert cancellation is not None
        observed.append(cancellation.args)
        rearm_cancellation(cancellation)
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError as replayed:
            observed.append(replayed.args)
        while current.cancelling():
            current.uncancel()

    await asyncio.create_task(worker())

    assert observed == [("external-caller",), ("external-caller",)]


@pytest.mark.asyncio
async def test_failing_owned_cleanup_returns_the_concurrent_external_cancellation() -> None:
    observed: list[tuple[object, ...]] = []

    async def owned() -> None:
        await asyncio.sleep(0)
        raise RuntimeError("owned cleanup failed")

    async def worker() -> None:
        current = asyncio.current_task()
        assert current is not None
        asyncio.get_running_loop().call_soon(current.cancel, "external-caller")
        outcome = await execution_module.await_cleanup_resistant(owned())
        assert isinstance(outcome.error, RuntimeError)
        assert outcome.cancellation is not None
        observed.append(outcome.cancellation.args)
        rearm_cancellation(outcome.cancellation)
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError as replayed:
            observed.append(replayed.args)
        while current.cancelling():
            current.uncancel()

    await asyncio.create_task(worker())

    assert observed == [("external-caller",), ("external-caller",)]


_LIST = Request.bare("entity.list", replay_safety=ReplaySafety.SAFE)
_UNLOCK = Request.bare("entity.unlock")


@pytest.mark.asyncio
async def test_rearm_without_an_absorbed_cancellation_replays_nothing() -> None:
    """A cancellation the task already received is not delivered again when cleanup absorbed none."""

    async def worker() -> None:
        current = asyncio.current_task()
        assert current is not None
        current.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.sleep(0)
        rearm_cancellation(None)
        await asyncio.sleep(0)  # a replay would be delivered here
        current.uncancel()

    await asyncio.create_task(worker())


@pytest.mark.asyncio
async def test_one_cancellation_of_a_reading_task_lets_its_finally_await() -> None:
    """The stream raises the cancellation once; the reader's own cleanup awaits undisturbed."""
    blocker = Blocker()
    steps: list[str] = []
    async with client_for(ResponderTransport(blocker)) as api:

        async def reader() -> None:
            try:
                async for _row in api.iter_list(_LIST):
                    pass
            finally:
                steps.append("finally")
                await asyncio.sleep(0)
                steps.append("finally awaited")

        task = asyncio.create_task(reader())
        await blocker.started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert steps == ["finally", "finally awaited"]


@pytest.mark.asyncio
async def test_a_handled_timeout_around_a_stream_leaves_no_cancellation_behind() -> None:
    """The timeout's cancellation ends the read; once the caller handles the timeout nothing is pending."""
    blocker = Blocker()
    async with client_for(ResponderTransport(blocker)) as api:

        async def reader() -> str:
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(0.01):
                    async for _row in api.iter_list(_LIST):
                        pass
            await asyncio.sleep(0)
            return "resumed"

        assert await asyncio.create_task(reader()) == "resumed"


@pytest.mark.asyncio
async def test_a_call_in_the_cleanup_of_a_cancelled_task_is_sent() -> None:
    """A compensating call after the task handled its cancellation runs; the cancellation is not raised again."""
    transport = ResponderTransport(replies({"result": True}))
    async with client_for(transport) as api:

        async def worker() -> object:
            current = asyncio.current_task()
            assert current is not None
            current.cancel()
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                return await api.call(_UNLOCK)
            return None

        assert await asyncio.create_task(worker()) is True

    assert [request.method for request in transport.requests] == ["entity.unlock"]


@pytest.mark.asyncio
async def test_a_cancellation_requested_during_a_cleanup_call_still_stops_it() -> None:
    """A later cancellation still voids the cleanup call's response, even when the transport suppressed it."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def suppressing(_request: Request) -> object:
        started.set()
        while not release.is_set():
            with contextlib.suppress(asyncio.CancelledError):
                await release.wait()
        return {"result": True}

    transport = ResponderTransport(suppressing)
    async with client_for(transport) as api:

        async def worker() -> object:
            current = asyncio.current_task()
            assert current is not None
            current.cancel()
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                return await api.call(_LIST)
            return None

        task = asyncio.create_task(worker())
        async with asyncio.timeout(1):  # the cleanup call reaches the transport
            await started.wait()
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert len(transport.requests) == 1


def _swallowing_first_send(failure: TransportError, started: asyncio.Event) -> Responder:
    """Answer the first send by swallowing the caller's cancellation and reporting ``failure`` instead."""

    async def respond(request: Request) -> object:
        if not started.is_set():
            started.set()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.Future[None]()
            return failure
        commands = request.copy_parameters().get("cmd")
        if isinstance(commands, dict):
            return {"result": {"result": dict.fromkeys(commands, 1), "result_error": []}}
        return {"result": 1}

    return respond


async def _cancel_once_started(started: asyncio.Event, work: Awaitable[object]) -> None:
    task = asyncio.ensure_future(work)
    async with asyncio.timeout(1):
        await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_a_cancellation_a_transport_turns_into_a_failure_is_not_retried() -> None:
    """The failure proves nothing ran and would be retried, but the request was cancelled meanwhile."""
    started = asyncio.Event()
    failure = TransportError("connect failed", phase=FailurePhase.CONNECTION_ESTABLISHED)
    transport = ResponderTransport(_swallowing_first_send(failure, started))
    async with client_for(transport) as api:
        await _cancel_once_started(started, api.call(_LIST))

    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_a_cancellation_a_transport_turns_into_a_failure_ends_the_batch() -> None:
    """The next chunk is never sent once the first chunk's transport swallowed the cancellation."""
    started = asyncio.Event()
    transport = ResponderTransport(
        _swallowing_first_send(TransportError("lost", phase=FailurePhase.DISPATCH_STARTED), started)
    )
    commands = [Command(Request.bare("entity.add", {"id": index}), index) for index in range(4)]
    async with client_for(transport) as api:

        async def drain() -> None:
            async for _outcome in api.batch_outcomes(commands, batch_size=2):
                pass

        await _cancel_once_started(started, drain())

    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_a_stream_cleanup_failure_behind_the_reader_cancellation_raises_the_cancellation() -> None:
    """The cancellation came first: the cancelled read raises it, and the cleanup failure is in the report."""

    class CloseFailedError(Exception):
        pass

    class Source:
        def __aiter__(self) -> Source:
            return self

        async def __anext__(self) -> Command[int]:
            return Command(_LIST, 1)

        async def aclose(self) -> None:
            raise CloseFailedError

    blocker = Blocker()
    steps: list[str] = []
    async with client_for(ResponderTransport(blocker)) as api:
        stream = api.batch_outcomes(Source(), batch_size=2)

        async def reader() -> None:
            try:
                async for _outcome in stream:
                    pass
            except asyncio.CancelledError:
                steps.append("cancellation raised")
                raise

        task = asyncio.create_task(reader())
        await blocker.started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert steps == ["cancellation raised"]
    report = stream.report
    assert report is not None
    assert [violation.message for violation in report.violations] == ["stream cleanup also failed (InputSourceError)"]


@pytest.mark.asyncio
async def test_transport_webhook_vault_is_opaque_and_gc_bounded() -> None:
    sensitive_fragment = "synthetic-vault-private-fragment"
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: httpx.Response(200)))
    transport = HttpxTransport(
        f"https://example.invalid/rest/1/{sensitive_fragment}/",
        client=client,
    )
    handle = transport._webhook_handle  # noqa: SLF001 - lifecycle regression
    reference = weakref.ref(transport)

    assert sensitive_fragment not in repr(httpx_transport_module.__dict__)
    del transport
    gc.collect()

    assert reference() is None
    with pytest.raises(TransportError, match="credential is unavailable") as refused:
        httpx_transport_module._token_for(handle)  # noqa: SLF001 - lifecycle regression
    assert refused.value.phase is FailurePhase.NOT_DISPATCHED
    assert refused.value.retryable is False
    await client.aclose()


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


_LIBRARY_ROOT = Path(b24api.__file__).resolve().parent


def _assert_library_frames_omit(error: BaseException, fragment: str) -> None:
    """Assert no b24api traceback frame retains the fragment, and that such frames were inspected.

    A suffix filter like ``/b24api/`` would also match this repository's parent directory, and the
    former ``b24api/execution.py`` filter matched no file at all, so the loop inspected nothing.
    """
    checked_frames = 0
    traceback = error.__traceback__
    while traceback is not None:
        if Path(traceback.tb_frame.f_code.co_filename).resolve().is_relative_to(_LIBRARY_ROOT):
            checked_frames += 1
            for value in traceback.tb_frame.f_locals.values():
                assert fragment not in repr(value)
        traceback = traceback.tb_next
    assert checked_frames > 0


def test_transport_constructor_drops_webhook_when_httpx_client_initialization_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sensitive_fragment = "synthetic-constructor-private-fragment"

    def fail_client(**_kwargs: object) -> httpx.AsyncClient:
        raise httpx.InvalidURL("synthetic client initialization failure")

    monkeypatch.setattr(httpx, "AsyncClient", fail_client)

    with pytest.raises(RuntimeError, match="initialization failed") as captured:
        HttpxTransport(f"https://example.invalid/rest/1/{sensitive_fragment}/")

    error = captured.value
    assert error.__cause__ is None
    assert error.__context__ is None
    _assert_library_frames_omit(error, sensitive_fragment)


@pytest.mark.asyncio
async def test_transport_preserves_the_committed_http2_fast_path(monkeypatch: pytest.MonkeyPatch) -> None:
    original_client = httpx.AsyncClient
    captured: dict[str, object] = {}

    def client(**kwargs: object) -> httpx.AsyncClient:
        captured.update(kwargs)
        return original_client(transport=httpx.MockTransport(lambda _request: httpx.Response(200)), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client)
    transport = HttpxTransport("https://example.invalid/rest/1/token/")
    try:
        assert captured["http2"] is True
    finally:
        await transport.aclose()


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
async def test_terminal_negative_one_next_is_normalized() -> None:
    transport = _sequence([_success(b'{"result":[],"next":-1}')])

    response = await Executor(transport).execute(Request("mobile.disk.folder.getchildren", route=RouteKind.BARE))

    assert response.result == []
    assert response.next is None


@pytest.mark.asyncio
async def test_safe_and_unknown_retry_only_when_replay_is_proven() -> None:
    pre_dispatch = TransportError("connect", phase=FailurePhase.NOT_DISPATCHED)
    safe_transport = _sequence([pre_dispatch, _success()])
    safe = await Executor(safe_transport).execute(
        Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        policy=_policy(),
    )
    assert safe.result == {"ok": True}
    assert len(safe_transport.requests) == EXPECTED_RETRIED_CALLS

    unknown_transport = _sequence(
        [TransportError("connect", phase=FailurePhase.CONNECTION_ESTABLISHED), _success()],
    )
    unknown = await Executor(unknown_transport).execute(Request("profile", route=RouteKind.BARE), policy=_policy())
    assert unknown.result == {"ok": True}
    assert len(unknown_transport.requests) == EXPECTED_RETRIED_CALLS


@pytest.mark.asyncio
@pytest.mark.parametrize("safety", [ReplaySafety.UNKNOWN, ReplaySafety.UNSAFE])
async def test_ambiguous_dispatch_never_retries_unproven_request(safety: ReplaySafety) -> None:
    transport = _sequence(
        [TransportError("read failed", phase=FailurePhase.DISPATCH_STARTED), _success()],
    )

    with pytest.raises(AmbiguousExecutionError) as captured:
        await Executor(transport).execute(
            Request("crm.deal.add", replay_safety=safety, route=RouteKind.BARE),
            policy=_policy(),
        )

    assert len(transport.requests) == 1
    assert isinstance(captured.value.__cause__, TransportError)
    assert captured.value.__cause__.phase is FailurePhase.DISPATCH_STARTED


@pytest.mark.asyncio
async def test_safe_ambiguous_transport_retries_with_counted_attempts() -> None:
    transport = _sequence(
        [TransportError("partial", phase=FailurePhase.BODY_PARTIALLY_RECEIVED), _success()],
    )
    executor = Executor(transport)
    context = executor.context(_policy())

    response = await executor.execute(
        Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        context=context,
    )
    snapshot = await context.snapshot()

    assert response.result == {"ok": True}
    assert snapshot.counters.physical_requests == EXPECTED_RETRIED_CALLS
    assert snapshot.retries == 1


@pytest.mark.asyncio
async def test_operation_elapsed_clock_starts_at_first_execution_not_context_construction() -> None:
    now = [0.0]
    transport = _sequence([_success()])
    executor = Executor(transport, clock=lambda: now[0])
    context = executor.context(_policy(elapsed=1))
    now[0] = 100.0

    response = await executor.execute(Request("profile", route=RouteKind.BARE), context=context)

    assert response.result == {"ok": True}
    assert (await context.snapshot()).elapsed == 0


@pytest.mark.asyncio
async def test_retry_attempt_and_delay_budgets_terminate_before_extra_io() -> None:
    always_connect: tuple[WireResponse | Exception, ...] = (
        TransportError("connect", phase=FailurePhase.NOT_DISPATCHED),
        TransportError("connect", phase=FailurePhase.NOT_DISPATCHED),
    )
    transport = _sequence(always_connect)
    with pytest.raises(BudgetExceededError, match="attempt"):
        await Executor(transport).execute(Request("profile", route=RouteKind.BARE), policy=_policy(attempts=2))
    assert len(transport.requests) == EXPECTED_RETRIED_CALLS

    delayed = _sequence([WireResponse(status_code=503, headers=(), body=b"gateway")])
    with pytest.raises(BudgetExceededError, match="delay"):
        await Executor(delayed).execute(
            Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
            policy=_policy(delay=2, elapsed=1),
        )
    assert len(delayed.requests) == 1


@pytest.mark.asyncio
async def test_structured_throttle_uses_shared_cooldown_and_safe_replay() -> None:
    transport = _sequence(
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

    await executor.execute(Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE), context=context)
    snapshot = await context.snapshot()

    assert snapshot.retries == 1
    assert len(transport.requests) == EXPECTED_RETRIED_CALLS
    await coordinator.close()


@pytest.mark.asyncio
async def test_coordinator_merges_cooldowns_and_alternates_retries_with_first_sends() -> None:
    # A retry's window runs while it queues, so it waits for one other permit at most; first sends still get every
    # other turn, so retries never starve them.
    coordinator = RateCoordinator(max_concurrency=1)
    first = await coordinator.acquire(WorkClass.INTERACTIVE_DIRECT, methods=frozenset({"profile"}))

    retry_one = asyncio.create_task(coordinator.acquire(WorkClass.RETRY, methods=frozenset({"profile"})))
    retry_two = asyncio.create_task(coordinator.acquire(WorkClass.RETRY, methods=frozenset({"profile"})))
    interactive = asyncio.create_task(coordinator.acquire(WorkClass.INTERACTIVE_DIRECT, methods=frozenset({"profile"})))
    await asyncio.sleep(0)
    await first.release()

    retry_permit = await asyncio.wait_for(retry_one, timeout=1)
    assert not interactive.done()
    assert not retry_two.done()
    await retry_permit.release()
    interactive_permit = await asyncio.wait_for(interactive, timeout=1)
    assert not retry_two.done()
    await interactive_permit.release()
    second_retry_permit = await asyncio.wait_for(retry_two, timeout=1)
    await second_retry_permit.release()

    await coordinator.observe_throttle(0.01, reason="first")
    await coordinator.observe_throttle(0.03, reason="latest")
    snapshot = await coordinator.snapshot()
    assert snapshot.state is CoordinatorState.COOLDOWN
    assert snapshot.cooldown_reason == "latest"
    permit = await asyncio.wait_for(coordinator.acquire(WorkClass.BATCH, methods=frozenset({"profile"})), timeout=1)
    await permit.release()
    assert (await coordinator.snapshot()).state is CoordinatorState.OPEN
    await coordinator.close()


@pytest.mark.asyncio
async def test_cancelled_waiter_is_removed_without_leaking_permit() -> None:
    coordinator = RateCoordinator(max_concurrency=1)
    first = await coordinator.acquire(WorkClass.BATCH, methods=frozenset({"profile"}))
    waiter = asyncio.create_task(coordinator.acquire(WorkClass.TRAVERSAL_DIRECT, methods=frozenset({"profile"})))
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
    held = await coordinator.acquire(WorkClass.INTERACTIVE_DIRECT, methods=frozenset({"profile"}))
    waiter = asyncio.create_task(coordinator.acquire(WorkClass.TRAVERSAL_DIRECT, methods=frozenset({"profile"})))
    await asyncio.sleep(0)

    await held.release()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    assert (await coordinator.snapshot()).active_permits == 0
    replacement = await asyncio.wait_for(
        coordinator.acquire(WorkClass.BATCH, methods=frozenset({"profile"})),
        timeout=1,
    )
    await replacement.release()
    await coordinator.close()


@pytest.mark.asyncio
async def test_permit_wait_is_bounded_without_counting_or_dispatching_an_attempt() -> None:
    coordinator = RateCoordinator(max_concurrency=1)
    held = await coordinator.acquire(WorkClass.BATCH, methods=frozenset({"profile"}))
    transport = _sequence([_success()])
    executor = Executor(transport, coordinator=coordinator)
    context = executor.context(
        ExecutionPolicy(max_elapsed=0.02, max_retry_elapsed_per_request=0.02),
    )

    with pytest.raises(BudgetExceededError, match="permit wait"):
        await executor.execute(
            Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
            context=context,
        )

    assert len(transport.requests) == 0
    assert (await context.snapshot()).counters.physical_requests == 0
    await held.release()
    await coordinator.close()


@pytest.mark.asyncio
async def test_method_limit_blocks_only_affected_method_and_whole_batch() -> None:
    coordinator = RateCoordinator(max_concurrency=1)
    coordinator.hold_method("crm.item.list", 0.03)
    blocked = asyncio.create_task(
        coordinator.acquire(WorkClass.INTERACTIVE_DIRECT, methods=frozenset({"crm.item.list"})),
    )
    batch = asyncio.create_task(
        coordinator.acquire(WorkClass.BATCH, methods=frozenset({"crm.item.list", "profile"})),
    )
    await asyncio.sleep(0)
    unrelated = await asyncio.wait_for(
        coordinator.acquire(WorkClass.INTERACTIVE_DIRECT, methods=frozenset({"profile"})),
        timeout=1,
    )
    assert not blocked.done()
    assert not batch.done()
    await unrelated.release()
    pending = {blocked, batch}
    while pending:
        done, pending = await asyncio.wait(pending, timeout=1, return_when=asyncio.FIRST_COMPLETED)
        assert done
        for task in done:
            await task.result().release()
    assert (await coordinator.snapshot()).method_cooldowns == 0
    await coordinator.close()


@pytest.mark.asyncio
async def test_method_limit_admission_budget_and_closed_error_are_typed() -> None:
    coordinator = RateCoordinator()
    coordinator.hold_method("profile", 1.0)
    with pytest.raises(CoordinatorBudgetError):
        await coordinator.acquire(
            WorkClass.INTERACTIVE_DIRECT,
            methods=frozenset({"profile"}),
            budget=DeadlineBudget(asyncio.get_running_loop().time() + 0.01),
        )
    assert dict((await coordinator.snapshot()).queued)[WorkClass.INTERACTIVE_DIRECT] == 0
    await coordinator.close()
    with pytest.raises(ClientClosedError, match="rate coordinator is closed"):
        await coordinator.acquire(WorkClass.INTERACTIVE_DIRECT, methods=frozenset({"profile"}))


@pytest.mark.asyncio
async def test_unsafe_operation_time_limit_waits_for_the_method_and_replays() -> None:
    # OPERATION_TIME_LIMIT blocks the method before it runs, so even UNSAFE work is replayed after the cooldown
    # (owner decision, 2026-09-25).
    transport = _sequence(
        [
            WireResponse(
                status_code=200,
                headers=(),
                body=b'{"error":"OPERATION_TIME_LIMIT","error_description":"wait"}',
            ),
            WireResponse(200, (), b'{"result":true}'),
        ],
    )
    coordinator = RateCoordinator()
    # The method's latest answer said its operating time resets in 0.1 seconds: the time limit holds it that long.
    coordinator.observe_reset("crm.item.add", 0.1)
    loop = asyncio.get_running_loop()
    started = loop.time()
    response = await Executor(transport, coordinator=coordinator).execute(
        Request("crm.item.add", route=RouteKind.BARE, replay_safety=ReplaySafety.UNSAFE),
    )
    assert response.result is True
    assert len(transport.requests) == 2  # noqa: PLR2004 - one replay after the method cooldown
    assert loop.time() - started >= 0.1  # noqa: PLR2004 - the replay waited for the method cooldown
    await coordinator.close()


_OPERATION_TIME_LIMIT = WireResponse(
    status_code=200,
    headers=(),
    body=b'{"error":"OPERATION_TIME_LIMIT","error_description":"wait"}',
)
_SAFE_PROFILE = Request("profile", route=RouteKind.BARE, replay_safety=ReplaySafety.SAFE)


@pytest.mark.asyncio
async def test_safe_operation_time_limit_past_the_budget_fails_at_once_with_its_own_code() -> None:
    transport = _sequence([_OPERATION_TIME_LIMIT, WireResponse(200, (), b'{"result":true}')])
    coordinator = RateCoordinator()
    coordinator.observe_reset("profile", 120.0)
    loop = asyncio.get_running_loop()
    started = loop.time()
    with pytest.raises(ApiResponseError) as captured:
        await Executor(transport, coordinator=coordinator).execute(
            _SAFE_PROFILE, policy=ExecutionPolicy(max_retry_elapsed_per_request=2.0)
        )
    # The method's operating time resets in 120 seconds, which a 2-second budget cannot wait: nothing waits, and
    # the request ends with the server's refusal.
    assert loop.time() - started < 1.0
    assert len(transport.requests) == 1
    assert captured.value.normalized_code == "operation_time_limit"
    await coordinator.close()


@pytest.mark.asyncio
async def test_safe_operation_time_limit_within_the_budget_retries_after_the_cooldown() -> None:
    cooldown = 0.2
    transport = _sequence([_OPERATION_TIME_LIMIT, WireResponse(200, (), b'{"result":true}')])
    coordinator = RateCoordinator()
    coordinator.observe_reset("profile", cooldown)
    loop = asyncio.get_running_loop()
    started = loop.time()
    response = await Executor(transport, coordinator=coordinator).execute(
        _SAFE_PROFILE, policy=ExecutionPolicy(max_retry_elapsed_per_request=5.0)
    )
    assert response.result is True
    assert len(transport.requests) == 2  # noqa: PLR2004 - the time limit, then the retried success
    assert loop.time() - started >= cooldown * 0.9
    await coordinator.close()


@pytest.mark.asyncio
async def test_safe_traversal_page_operation_time_limit_keeps_its_typed_cause(scripted_client: ClientFactory) -> None:

    # The server asks for two minutes, which a 2-second window cannot wait: the page ends with its refusal.
    refusal = dataclasses.replace(_OPERATION_TIME_LIMIT, headers=(("retry-after", "120"),))
    transport = _sequence([refusal])
    stream = scripted_client(transport).iter_list(
        Request("crm.item.list", route=RouteKind.BARE, replay_safety=ReplaySafety.SAFE),
        page_size=50,
        policy=ExecutionPolicy(max_retry_elapsed_per_request=2.0),
    )
    with pytest.raises(ApiResponseError) as captured:
        _ = [row async for row in stream]
    assert len(transport.requests) == 1
    assert captured.value.normalized_code == "operation_time_limit"
    assert stream.report is not None
    assert not stream.report.exhausted


@pytest.mark.asyncio
async def test_method_hold_repeated_extends_its_deadline_and_capacity_is_explicit() -> None:
    now, hold = 100.0, 120.0

    def clock() -> float:
        return now

    coordinator = RateCoordinator(clock=clock)
    coordinator.hold_method("profile", hold)
    now += 30.0
    coordinator.hold_method("profile", hold)
    assert coordinator.admission_delay(frozenset({"profile"})) == hold
    # A shorter hold never shortens a longer one.
    coordinator.hold_method("profile", 1.0)
    assert coordinator.admission_delay(frozenset({"profile"})) == hold
    for index in range(1_023):
        coordinator.hold_method(f"method.{index}", hold)
    with pytest.raises(RatePolicyCapacityError):
        coordinator.hold_method("overflow", hold)
    now += 121.0
    assert (await coordinator.snapshot()).method_cooldowns == 0
    coordinator.hold_method("overflow", hold)
    await coordinator.close()


@pytest.mark.asyncio
async def test_socket_connect_and_post_dispatch_failures_have_distinct_phases() -> None:
    server = await asyncio.start_server(lambda _reader, writer: writer.close(), "127.0.0.1", 0)
    socket = server.sockets[0]
    assert socket is not None
    host, port = socket.getsockname()[:2]
    transport = HttpxTransport(f"http://{host}:{port}/rest/1/token/")
    try:
        with pytest.raises(TransportError) as post_dispatch:
            await transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024)
        assert post_dispatch.value.phase is FailurePhase.DISPATCH_STARTED
    finally:
        await transport.aclose()
        server.close()
        await server.wait_closed()

    dead_transport = HttpxTransport(f"http://{host}:{port}/rest/1/token/")
    try:
        with pytest.raises(TransportError) as not_dispatched:
            await dead_transport.send(
                Request("profile", route=RouteKind.BARE),
                attempt_timeout=0.2,
                max_response_bytes=1024,
            )
        assert not_dispatched.value.phase is FailurePhase.NOT_DISPATCHED
    finally:
        await dead_transport.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("safety", [ReplaySafety.UNSAFE, ReplaySafety.SAFE])
async def test_an_attempt_timed_out_in_the_tls_handshake_is_not_sent_and_is_retried_whatever_its_safety(
    safety: ReplaySafety,
) -> None:
    # The server accepts TCP and never answers the TLS handshake. Each attempt times out after the client's attempt
    # timeout (Settings.http_timeout), however long the window of the policy passed beside it: no byte of the request
    # left, so a write is retried as a read is, until its attempts run out, and nothing is unknown.
    connections: list[asyncio.StreamWriter] = []

    async def silent(_reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connections.append(writer)

    server = await asyncio.start_server(silent, "127.0.0.1", 0)
    socket = server.sockets[0]
    assert socket is not None
    host, port = socket.getsockname()[:2]
    settings = b24api.Settings(webhook_url=f"https://{host}:{port}/rest/1/token/", http_timeout=0.2)
    policy = ExecutionPolicy(max_attempts_per_request=3, retry=RetryPolicy(initial_delay=0, jitter=0))
    started = asyncio.get_running_loop().time()
    try:
        async with b24api.Bitrix24(settings, policy=policy) as client:
            with pytest.raises(BudgetExceededError, match="attempt budget exhausted") as raised:
                await client.call(Request("crm.deal.add", {"fields": {"TITLE": "x"}}, safety, route=RouteKind.BARE))
    finally:
        for writer in connections:
            writer.close()
        server.close()
        await server.wait_closed()

    assert len(connections) == policy.max_attempts_per_request
    assert asyncio.get_running_loop().time() - started < policy.max_retry_elapsed_per_request / 10
    cause = raised.value.__cause__
    assert isinstance(cause, TransportError)
    # TCP connected, the TLS handshake never completed: nothing of the request left, so it is no unknown outcome.
    assert cause.phase is FailurePhase.CONNECTION_ESTABLISHED
    assert cause.possible_acceptance is False
    assert execution(raised.value) is Execution.ANSWERED


@pytest.mark.asyncio
@pytest.mark.parametrize("safety", [ReplaySafety.UNSAFE, ReplaySafety.SAFE])
@pytest.mark.parametrize("sent_as", ["call", "batch", "rest-v3-batch"])
async def test_an_attempt_the_retry_window_cuts_in_the_tls_handshake_is_still_proven_not_sent(
    safety: ReplaySafety, sent_as: str
) -> None:
    # As above, but the retry window closes long before the attempt timeout: the window's end cuts the attempt in
    # the TLS handshake, where the transport knows that nothing of the request left. It is no unknown outcome, for a
    # lone call as for a batch envelope's command.
    connections: list[asyncio.StreamWriter] = []

    async def silent(_reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connections.append(writer)

    server = await asyncio.start_server(silent, "127.0.0.1", 0)
    socket = server.sockets[0]
    assert socket is not None
    host, port = socket.getsockname()[:2]
    settings = b24api.Settings(webhook_url=f"https://{host}:{port}/rest/1/token/", http_timeout=5.0)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=0.3, retry=RetryPolicy(initial_delay=0, jitter=0))
    route = RouteKind.API_V3 if sent_as == "rest-v3-batch" else RouteKind.BARE
    request = Request("crm.deal.add", {"fields": {"TITLE": "x"}}, safety, route=route)
    try:
        async with b24api.Bitrix24(settings, policy=policy) as client:
            if sent_as != "call":
                # Two commands, so they go in one envelope rather than as a lone call.
                outcomes = [each async for each in client.batch_outcomes([Command(request, 0), Command(request, 1)])]
                assert all(isinstance(outcome, CommandFailure) for outcome in outcomes)
                first = outcomes[0]
                assert isinstance(first, CommandFailure)
                refusal: BaseException = first.error
            else:
                with pytest.raises(BudgetExceededError) as raised:
                    await client.call(request)
                refusal = raised.value
    finally:
        for writer in connections:
            writer.close()
        server.close()
        await server.wait_closed()

    assert len(connections) == 1
    assert isinstance(refusal, BudgetExceededError)
    assert "retry time budget exhausted" in str(refusal)
    cause = refusal.__cause__
    assert isinstance(cause, TransportError)
    assert cause.phase is FailurePhase.CONNECTION_ESTABLISHED
    assert cause.possible_acceptance is False
    assert execution(refusal) is Execution.ANSWERED


class _SelfBoundedTransport(ScriptedTransport):
    """Bounds its attempts itself, a little after their timeout, and proves that nothing of the request left."""

    capabilities = dataclasses.replace(ScriptedTransport.capabilities, bounds_attempts=True)

    def __init__(self) -> None:
        super().__init__(())

    async def send_wire(self, wire: WireRequest, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del max_response_bytes
        await asyncio.sleep(attempt_timeout + 0.05)
        raise TransportError(
            "Transport attempt timed out before dispatch",
            phase=FailurePhase.CONNECTION_ESTABLISHED,
            request_summary=wire.summary,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("route", [RouteKind.BARE, RouteKind.API_V3])
@pytest.mark.parametrize("commands", [1, 2], ids=["call", "envelope"])
async def test_the_executor_leaves_the_attempt_bound_to_a_transport_that_declares_it(
    route: RouteKind, commands: int
) -> None:
    # The transport ends its attempt itself and says how far it got: the executor sets no deadline of its own that
    # would cut it first and leave the request possibly run, for a lone call as for an envelope.
    request = Request("crm.deal.add", {"fields": {"TITLE": "x"}}, ReplaySafety.UNSAFE, route=route)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=0.1, retry=RetryPolicy(initial_delay=0, jitter=0))
    async with client_for(_SelfBoundedTransport(), policy=policy) as client:
        outcomes = [each async for each in client.batch_outcomes([Command(request, i) for i in range(commands)])]

    for outcome in outcomes:
        assert isinstance(outcome, CommandFailure)
        cause = outcome.error.__cause__
        assert isinstance(cause, TransportError)
        assert cause.phase is FailurePhase.CONNECTION_ESTABLISHED
        assert execution(outcome.error) is Execution.ANSWERED


def test_only_a_transport_that_declares_it_bounds_its_own_attempts() -> None:
    assert TransportCapabilities().bounds_attempts is False
    assert HttpxTransport.capabilities.bounds_attempts is True
    with pytest.raises(TypeError, match="bounds_attempts"):
        TransportCapabilities(bounds_attempts=1)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_transport_enforces_decompressed_response_byte_ceiling_and_closes_response() -> None:
    observed: list[httpx.Response] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        response = httpx.Response(HTTP_OK, request=request, content=b'{"result":"too large"}')
        observed.append(response)
        return response

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = HttpxTransport("https://example.invalid/rest/1/token/", client=client)
    try:
        with pytest.raises(ResponseTooLargeError, match="byte ceiling"):
            await transport.send(
                Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
                attempt_timeout=1,
                max_response_bytes=SMALL_RESPONSE_CEILING,
            )
        assert transport.host == "example.invalid"
        assert observed[0].is_closed
    finally:
        await transport.aclose()
        await client.aclose()


@pytest.mark.asyncio
async def test_oversized_response_after_unknown_dispatch_is_ambiguous() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(HTTP_OK, request=request, content=b'{"result":"too large"}')

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = HttpxTransport("https://example.invalid/rest/1/token/", client=client)
    try:
        with pytest.raises(AmbiguousExecutionError, match="oversized response"):
            await Executor(transport).execute(
                Request("crm.deal.add", replay_safety=ReplaySafety.UNKNOWN, route=RouteKind.BARE),
                policy=ExecutionPolicy(max_response_bytes=SMALL_RESPONSE_CEILING),
            )
    finally:
        await transport.aclose()
        await client.aclose()


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
    transport = HttpxTransport("https://example.invalid/rest/1/token/", client=client)
    try:
        with pytest.raises(AmbiguousExecutionError) as captured:
            await Executor(transport).execute(
                Request("crm.deal.add", replay_safety=ReplaySafety.UNSAFE, route=RouteKind.BARE),
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
    transport = HttpxTransport("https://example.invalid/rest/1/token/", client=client)
    try:
        with pytest.raises(TransportError) as captured:
            await transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024)
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
            await transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024)

        error = captured.value
        assert error.__cause__ is None
        assert error.__context__ is None
        traceback = error.__traceback__
        while traceback is not None:
            if traceback.tb_frame.f_code.co_name == "send":
                for value in traceback.tb_frame.f_locals.values():
                    assert sensitive_fragment not in repr(value)
                    assert sensitive_fragment not in getattr(value, "_webhook_url", "")
                    if isinstance(value, tuple):
                        for item in value:
                            assert sensitive_fragment not in getattr(item, "_webhook_url", "")
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
    task = asyncio.create_task(
        transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024),
    )
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
                assert traceback.tb_frame.f_code.co_filename.endswith("b24api/transport/httpx.py")
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
    transport = HttpxTransport(f"http://{host}:{port}/rest/1/token/")
    try:
        with pytest.raises(TransportError) as captured:
            await transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024)
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
            await transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024)

        error = captured.value
        assert error.__cause__ is None
        assert error.__context__ is None
        _assert_library_frames_omit(error, sensitive_fragment)
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
    transport = HttpxTransport(f"http://{host}:{port}/rest/1/token/")
    executor = Executor(transport)
    context = executor.context(_policy())
    task = asyncio.create_task(
        executor.execute(
            Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
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
    sentinel = _sequence([_success(b'{"result":[],"total":-1}')])
    response = await Executor(sentinel).execute(Request("im.recent.list", route=RouteKind.BARE), policy=_policy())
    assert response.total == -1

    invalid = _sequence([_success(b'{"result":[],"total":-2}')])
    with pytest.raises(HTTPGatewayError) as captured:
        await Executor(invalid).execute(
            Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE), policy=_policy()
        )
    assert captured.value.http_status == HTTP_OK


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b'{"result":1e400}', id="overflowed-result-number"),
    ],
)
async def test_success_model_contract_failures_are_typed_and_keep_http_evidence(body: bytes) -> None:
    transport = _sequence([_success(body)])

    with pytest.raises(ProtocolError) as captured:
        await Executor(transport).execute(
            Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE), policy=_policy()
        )

    assert captured.value.http_status == HTTP_OK
    assert captured.value.request_summary is not None
    assert captured.value.request_summary.method == "profile"
    assert isinstance(captured.value.__cause__, ValueError)


@pytest.mark.parametrize("end", ["raised_none", "raised_its_interrupt"])
def test_a_closed_latch_raises_one_interrupt_and_reports_every_other_late_one(end: str) -> None:
    # Once its operation has ended, the latch keeps no escape. An operation raises one interrupt at most: the first
    # caught after an end that raised none leaves the event loop from a callback, once however many catchers record
    # it; every other late interrupt goes to the exception handler, once, and the one the end raised never again.
    handed: list[tuple[object, ...]] = []
    reported: list[object] = []
    own, late, later = KeyboardInterrupt("own"), SystemExit("late"), KeyboardInterrupt("later")

    async def main() -> None:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _loop, context: reported.append(context["exception"]))
        call_soon = loop.call_soon
        loop.call_soon = lambda _callback, *args, **_kwargs: handed.append(args)  # type: ignore[assignment,method-assign]
        try:
            latch = OperationLatch()
            if end == "raised_its_interrupt":
                latch.record(own)
            assert latch.close() is (own if end == "raised_its_interrupt" else None)
            for error in (late, late, own, RuntimeError("no interrupt"), None, later, later):
                latch.record(error)
            # Another end of the operation (a read a concurrent close ended) raises what the first did, no late one.
            assert latch.close() is (own if end == "raised_its_interrupt" else None)
        finally:
            loop.call_soon = call_soon  # type: ignore[method-assign]

    asyncio.run(main())
    if end == "raised_its_interrupt":
        assert (handed, reported) == ([], [late, later])
    else:
        assert (handed, reported) == ([(late,)], [own, later])


@pytest.mark.parametrize("state", ["running", "ending"])
def test_a_second_interrupt_the_running_operation_latches_is_reported_at_its_end(state: str) -> None:
    # Two interrupts in one operation, running or ending: its end raises the first and reports the second to the
    # loop's exception handler, once however many catchers record it; nothing more leaves the loop.
    handed: list[tuple[object, ...]] = []
    reported: list[object] = []
    first, second = KeyboardInterrupt("first"), SystemExit("second")

    async def main() -> None:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _loop, context: reported.append(context["exception"]))
        call_soon = loop.call_soon
        loop.call_soon = lambda _callback, *args, **_kwargs: handed.append(args)  # type: ignore[assignment,method-assign]
        try:
            latch = OperationLatch()
            latch.record(first)
            if state == "ending":
                latch.end()
            for error in (second, second, first):
                latch.record(error)
            assert latch.close() is first
        finally:
            loop.call_soon = call_soon  # type: ignore[method-assign]

    asyncio.run(main())
    assert (handed, reported) == ([], [second])


@pytest.mark.parametrize("loop_state", ["running", "closed"])
def test_an_abandoned_latch_reports_every_undelivered_escape_and_raises_none(loop_state: str) -> None:
    # An operation nothing ended: as its latch is collected, every escape still undelivered, a second interrupt
    # included, goes to its loop's exception handler, once each, in order; nothing is raised, on a closed loop too.
    reported: list[object] = []
    first, second, other = KeyboardInterrupt("first"), SystemExit("second"), _Escape("other")
    loop = asyncio.new_event_loop()
    loop.set_exception_handler(lambda _loop, context: reported.append(context["exception"]))

    async def abandon() -> None:
        latch = OperationLatch()
        for error in (first, second, other, other):
            latch.record(error)

    try:
        loop.run_until_complete(abandon())
        if loop_state == "closed":
            loop.close()
        gc.collect()
        assert reported == [second, other]
    finally:
        loop.close()


def test_a_latch_wakes_its_owner_only_while_the_operation_runs() -> None:
    # Running, the first interrupt wakes the owner, once (a second waits for the end, which reports it); ending, an
    # interrupt is latched for the end, waking nothing; an interrupt the end handed to the loop (no reader raised it)
    # is not handed again when a catcher records or hands it on later.
    woken: list[bool] = []
    handed: list[tuple[object, ...]] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        call_soon = loop.call_soon
        loop.call_soon = lambda _callback, *args, **_kwargs: handed.append(args)  # type: ignore[assignment,method-assign]
        try:
            running = OperationLatch(lambda: woken.append(True))
            first, second = KeyboardInterrupt("first"), SystemExit("second")
            running.record(first)
            running.record(second)
            assert (running.first, woken) == (first, [True])
            assert running.close() is first
            ending = OperationLatch(lambda: woken.append(False))
            ending.end()
            ending.record(first)
            assert (ending.first, ending.state, woken) == (first, LatchState.ENDING, [True])
            ending.close_to_loop()
            ending.hand_on(first)
            ending.record(first)
        finally:
            loop.call_soon = call_soon  # type: ignore[method-assign]
        assert handed == [(first,)]

    asyncio.run(main())


class _Escape(BaseException):
    """A ``BaseException`` that is neither an interrupt nor a cancellation."""


def test_a_latch_raises_the_first_undelivered_escape_once_and_hands_the_rest_to_the_event_loop() -> None:
    # An escape that is no interrupt stays undelivered until the operation's end, whoever records it how often. The
    # end takes the first, once; it hands every other to the event loop's exception handler, as it does one caught
    # after the end, once each, with the report, and never the one it raised. Behind an interrupt, which the end
    # raises, every one is handed on. A failure, a cancellation and a coroutine's close are not the latch's.
    handled: list[dict[str, object]] = []

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: handled.append(context))
        latch = OperationLatch()
        first, second, late = _Escape("first"), _Escape("second"), _Escape("late")
        for error in (first, first, second, asyncio.CancelledError(), GeneratorExit(), RuntimeError("failure"), None):
            latch.record(error)
        latch.report = report = object()
        assert latch.close() is first
        assert latch.close() is None
        for error in (late, late, first, second):
            latch.record(error)
        assert [context["exception"] for context in handled] == [second, late]
        assert all(context["report"] is report for context in handled)
        handled.clear()
        behind = OperationLatch()
        interrupt, raw = KeyboardInterrupt("first"), _Escape("behind")
        behind.record(raw)
        behind.record(interrupt)
        assert behind.close() is interrupt
        assert behind.close() is interrupt
        assert [context["exception"] for context in handled] == [raw]

    asyncio.run(main())


def test_a_settled_task_lets_its_coroutine_be_closed() -> None:
    # A coroutine closed while it waits (the garbage collector closes those a stopped loop left) gets GeneratorExit:
    # that is no way its work ended, so settle lets it through rather than return it as a failure.
    class _Suspends:
        def __await__(self) -> Generator[None]:
            yield

    coroutine = settle(_Suspends())
    coroutine.send(None)
    coroutine.close()
    assert not is_interrupt(GeneratorExit())
    assert is_interrupt(KeyboardInterrupt())
    assert is_interrupt(SystemExit())
