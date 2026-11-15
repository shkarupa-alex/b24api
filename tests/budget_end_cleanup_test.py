"""The end of ``max_elapsed`` ends the operation; the cleanup that follows it is not a failure for it.

Cleanup always runs once the operation ends, which is often because its time ran out, so it gets the
time left but at least ``CLEANUP_GRACE_SECONDS``: a source whose close ends at once (a list, a plain
generator, an async generator) closes within it, whatever stream reads it, and the report carries the
budget's end alone. A close that really blocks is still bounded, by the grace, and its failure stays
secondary to the budget's. A synchronous source closed while an abandoned pull still runs in its
worker thread waits for that pull, then closes in the same ``contextvars.Context``.

The operation clock is the executor's clock with a skew: the first request spends the whole budget
by moving it forward, so where the budget ends follows from the script, not from how fast the host runs.
"""

from __future__ import annotations
import asyncio
import contextvars
import threading
import time
from typing import TYPE_CHECKING, Any

import pytest

from b24api import (
    AutoKeysetExecution,
    BatchDispatch,
    Binding,
    CursorSpec,
    DirectDispatch,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    KeysetTraversal,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    ResultSelector,
    RetryPolicy,
    RouteKind,
    SequentialTraversal,
    StableIntegerKeysetContract,
    TerminalState,
)
from b24api.batch.engine import BatchExecutor
from b24api.batch.stream import batch_outcome_stream
from b24api.contracts import Command, ReferenceSource
from b24api.contracts.policy import KernelState
from b24api.contracts.report import OperationReport, ViolationSeverity
from b24api.errors import (
    BatchFailed,
    BudgetExceededError,
    CleanupOverrunError,
    IncompleteTraversalError,
    InputSourceError,
    ReferenceFailed,
)
from b24api.execution import Executor, RateCoordinator
from b24api.execution.context import AsyncIteratorController, ExecutionContext
from b24api.execution.failure import cleanup_failure_violation
from b24api.references.binding import _BindingSourceError
from b24api.testing import ScriptedTransport
from b24api.transport import WireResponse
from tests.scripting import (
    CLEANUP_GRACE,
    DEADLINE_BUDGET,
    DEADLINE_SLACK,
    HANG_BOUND,
    BatchPortal,
    Blocker,
    ResponderTransport,
    WorkerStarts,
    assert_fired_near_deadline,
    attached_report,
    client_for,
    deadline_started,
    on_loop,
    reached,
    replies,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterable, Iterator

    from b24api import Bitrix24
    from b24api.contracts.stream import OperationStream
    from b24api.transport import Transport

ITEMS = 3
POLICY = ExecutionPolicy(max_elapsed=60.0)
# Fail-fast families may end on the binding or command the budget refused, under their wrapper.
FAMILIES: dict[str, type[Exception] | None] = {
    "batch": BatchFailed,
    "batch_outcomes": None,
    "fan_out": BatchFailed,
    "fan_out_outcomes": None,
    "iter_references": ReferenceFailed,
    "iter_reference_outcomes": None,
    "iter_reference_sources": ReferenceFailed,
    "iter_reference_source_outcomes": None,
    "iter_cursors": ReferenceFailed,
}
SHAPES = ("list", "generator", "async_generator")
LIST_REQUEST = Request("x.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)
TRAVERSAL = SequentialTraversal(selector=ResultSelector(()), page_size=50)
CURSOR = CursorSpec(
    parameter_path=ParameterPath(("LAST_ID",)),
    item_path=("ID",),
    coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
    direction="ascending",
    take="last",
)


class _OperationClock:
    """The monotonic clock every operation context reads, plus a skew a test adds to spend the budget."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.skew = 0.0

        def context(executor: Executor, policy: ExecutionPolicy | None = None) -> ExecutionContext:
            return ExecutionContext(policy or ExecutionPolicy(), executor.coordinator, clock=self)

        monkeypatch.setattr(Executor, "context", context)

    def __call__(self) -> float:
        return time.monotonic() + self.skew

    def spend(self) -> None:
        self.skew += POLICY.max_elapsed


def _answer(clock: _OperationClock) -> Callable[[Request], object]:
    """Answer every request with one row; the first request spends the operation's whole budget."""

    def respond(request: Request) -> object:
        if clock.skew == 0:
            clock.spend()
        return _one_row(request)

    return respond


def _one_row(request: Request) -> object:
    if request.method == "batch":
        keys = request.copy_parameters()["cmd"]
        assert isinstance(keys, dict)
        return {"result": {"result": {key: [{"ID": 1}] for key in keys}, "result_error": {}}}
    return {"result": [{"ID": 1}]}


class _Closed:
    """Whether the caller's source was closed, by its ``finally``."""

    def __init__(self) -> None:
        self.event = threading.Event()


def _shaped[T](shape: str, items: list[T], closed: _Closed) -> Iterable[T] | AsyncIterator[T]:
    if shape == "list":
        closed.event.set()
        return items

    def generator() -> Iterator[T]:
        try:
            yield from items
        finally:
            closed.event.set()

    async def async_generator() -> AsyncIterator[T]:
        try:
            for item in items:
                yield item
        finally:
            closed.event.set()

    return generator() if shape == "generator" else async_generator()


def _binding(index: int) -> Binding[int]:
    return Binding(
        summary=f"owner {index}",
        updates=(ParameterUpdate(ParameterPath(("filter", "OWNER")), index),),
        correlation=index,
    )


def _command(index: int) -> Command[int]:
    return Command(Request("x.get", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)


def _stream(client: Bitrix24, family: str, shape: str, closed: _Closed) -> OperationStream[Any]:  # noqa: PLR0911
    # One command or binding at a time, so the first send spends the budget before any other is admitted.
    direct = DirectDispatch(concurrency=1)
    commands: Any = _shaped(shape, [_command(index) for index in range(ITEMS)], closed)
    bindings: Any = _shaped(shape, [_binding(index) for index in range(ITEMS)], closed)
    sources: Any = _shaped(
        shape, [ReferenceSource(LIST_REQUEST, TRAVERSAL, _binding(index)) for index in range(ITEMS)], closed
    )
    if family == "batch":
        return client.batch(commands, batch_size=1)
    if family == "batch_outcomes":
        return client.batch_outcomes(commands, batch_size=1)
    if family == "fan_out":
        return client.fan_out(commands, dispatch=direct)
    if family == "fan_out_outcomes":
        return client.fan_out_outcomes(commands, dispatch=direct)
    if family == "iter_references":
        return client.iter_references(LIST_REQUEST, bindings, traversal=TRAVERSAL, dispatch=direct)
    if family == "iter_reference_outcomes":
        return client.iter_reference_outcomes(LIST_REQUEST, bindings, traversal=TRAVERSAL, dispatch=direct)
    if family == "iter_reference_sources":
        return client.iter_reference_sources(sources, dispatch=direct)
    if family == "iter_reference_source_outcomes":
        return client.iter_reference_source_outcomes(sources, dispatch=direct)
    return client.iter_cursors(LIST_REQUEST, bindings, selector=ResultSelector(()), cursor=CURSOR, dispatch=direct)


async def _drain(stream: OperationStream[Any]) -> BaseException:
    try:
        async with stream:
            async for _item in stream:
                pass
    except Exception as error:  # noqa: BLE001 - the test inspects whichever error ended the stream
        return error
    raise AssertionError("the stream completed although its budget refused work")


def _blocking(report: OperationReport) -> list[str]:
    return [violation.code for violation in report.violations if violation.severity is ViolationSeverity.BLOCKING]


def _assert_ended_by_the_budget(error: BaseException, wrapper: type[Exception] | None) -> OperationReport:
    report = attached_report(error)
    assert isinstance(report, OperationReport)
    if not isinstance(error, BudgetExceededError):
        assert wrapper is not None, f"a tolerant stream raised {type(error).__name__}, not the budget's error"
        assert isinstance(error, wrapper)
    assert report.state is TerminalState.INCOMPLETE
    assert report.terminal_reason == BudgetExceededError.__name__
    return report


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", SHAPES)
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_spent_budget_ends_every_stream_with_the_budget_alone(
    monkeypatch: pytest.MonkeyPatch, family: str, shape: str
) -> None:
    clock = _OperationClock(monkeypatch)
    closed = _Closed()
    async with client_for(ResponderTransport(_answer(clock)), policy=POLICY) as client:
        error = await _drain(_stream(client, family, shape, closed))

    report = _assert_ended_by_the_budget(error, FAMILIES[family])
    # The source closed at once, after the budget ran out: its close is no cleanup failure.
    assert _blocking(report) == ["budget_exhausted"]
    assert closed.event.is_set()


class _BlockingClose:
    """Commands from a generator, synchronous or asynchronous, whose close blocks until the test releases it."""

    def __init__(self, items: list[Command[int]]) -> None:
        self.items = iter(items)
        self.release = threading.Event()
        self.closed = threading.Event()
        self.released = asyncio.Event()

    def sync(self) -> Iterator[Command[int]]:
        try:
            yield from self.items
        finally:
            self.release.wait(HANG_BOUND)
            self.closed.set()

    async def asynchronous(self) -> AsyncIterator[Command[int]]:
        try:
            for item in self.items:
                yield item
        finally:
            await reached(self.released)
            self.closed.set()

    def set_free(self) -> None:
        self.release.set()
        self.released.set()


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync_close", "async_close"])
@pytest.mark.parametrize("family", ["batch_outcomes", "fan_out_outcomes"])
async def test_a_blocking_close_after_the_budget_is_bounded_and_never_replaces_the_budget_error(
    monkeypatch: pytest.MonkeyPatch,
    family: str,
    asynchronous: bool,  # noqa: FBT001
) -> None:
    clock = _OperationClock(monkeypatch)
    source = _BlockingClose([_command(index) for index in range(ITEMS)])
    commands = source.asynchronous() if asynchronous else source.sync()
    loop = asyncio.get_running_loop()
    async with client_for(ResponderTransport(_answer(clock)), policy=POLICY) as client:
        if family == "batch_outcomes":
            stream = client.batch_outcomes(commands, batch_size=1)
        else:
            stream = client.fan_out_outcomes(commands, dispatch=DirectDispatch(concurrency=1))
        try:
            started = loop.time()
            error = await asyncio.wait_for(_drain(stream), timeout=HANG_BOUND)
            # Only the grace ended the wait for the close: it blocks until released.
            assert loop.time() - started < CLEANUP_GRACE + DEADLINE_SLACK
            assert not source.closed.is_set()
        finally:
            source.set_free()
        assert await asyncio.to_thread(source.closed.wait, HANG_BOUND)

    report = _assert_ended_by_the_budget(error, None)
    assert isinstance(error, BudgetExceededError)
    assert "cleanup" not in str(error)
    assert sorted(_blocking(report)) == ["budget_exhausted", "cleanup_failure"]


_PULLED = contextvars.ContextVar[str]("pulled", default="caller")


class _BlockingPull:
    """A synchronous source whose second pull blocks until released; its close records what it saw."""

    def __init__(self) -> None:
        self.pulls = 0
        self.release = threading.Event()
        self.pulling = False
        self.closed_during_pull: bool | None = None
        self.closed_in_context: str | None = None

    def __iter__(self) -> _BlockingPull:
        return self

    def __next__(self) -> Command[int]:
        self.pulls += 1
        _PULLED.set(f"pull {self.pulls}")
        if self.pulls > 1:
            self.pulling = True
            self.release.wait(HANG_BOUND)
            self.pulling = False
        return _command(self.pulls)

    def close(self) -> None:
        self.closed_during_pull = self.pulling
        self.closed_in_context = _PULLED.get()


@pytest.mark.asyncio
async def test_a_stream_ended_during_a_sync_pull_closes_its_source_after_the_pull_in_its_context() -> None:
    loop = asyncio.get_running_loop()
    workers = WorkerStarts(loop)
    loop.set_default_executor(workers)
    iterator = _BlockingPull()
    before = asyncio.all_tasks()
    async with client_for(
        ResponderTransport(lambda _request: {"result": [{"ID": 1}]}),
        policy=ExecutionPolicy(max_elapsed=DEADLINE_BUDGET),
    ) as client:
        stream = client.fan_out(iterator, dispatch=DirectDispatch(concurrency=1))
        try:
            # The second pull blocks until released, so only the deadline ends the operation.
            error = await asyncio.wait_for(_drain(stream), timeout=HANG_BOUND)
            report = _assert_ended_by_the_budget(error, BatchFailed)
            # The close could not run within the grace while the pull still ran: a cleanup failure, never the
            # operation's error.
            assert "cleanup_failure" in _blocking(report)
            # A loop shutting down cancels every task left: the pull's, its worker's and the close waiting for it.
            leftovers = asyncio.all_tasks() - before
            for task in leftovers:
                task.cancel()
            # The close's worker call starts while the pull still runs; it waits for the pull.
            await reached(workers.started(2))
        finally:
            iterator.release.set()
        results = await asyncio.wait_for(asyncio.gather(*leftovers, return_exceptions=True), timeout=HANG_BOUND)
    workers.shutdown(wait=True)

    assert [result for result in results if isinstance(result, Exception)] == []
    assert iterator.closed_during_pull is False
    assert iterator.closed_in_context == "pull 2"


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["fan_out_outcomes", "iter_reference_outcomes"])
async def test_a_cancellation_absorbed_by_cleanup_after_the_budget_is_recorded_once(
    monkeypatch: pytest.MonkeyPatch, family: str
) -> None:
    closing = asyncio.Event()
    aclose = AsyncIteratorController.aclose

    async def signalled_close(controller: AsyncIteratorController[Any], context: ExecutionContext) -> None:
        closing.set()
        await aclose(controller, context)

    monkeypatch.setattr(AsyncIteratorController, "aclose", signalled_close)
    iterator = _BlockingPull()
    async with client_for(
        ResponderTransport(lambda _request: {"result": [{"ID": 1}]}),
        policy=ExecutionPolicy(max_elapsed=DEADLINE_BUDGET),
    ) as client:
        dispatch = DirectDispatch(concurrency=1)
        if family == "fan_out_outcomes":
            stream: OperationStream[Any] = client.fan_out_outcomes(iterator, dispatch=dispatch)
        else:
            # One pull of the source per binding.
            bindings = (_binding(command.correlation) for command in iterator)
            stream = client.iter_reference_outcomes(LIST_REQUEST, bindings, traversal=TRAVERSAL, dispatch=dispatch)
        ended: list[BaseException] = []

        async def consume() -> None:
            ended.append(await _drain(stream))
            await asyncio.sleep(0)  # where a re-armed cancellation lands

        consumer = asyncio.create_task(consume())
        try:
            # The deadline cuts the second pull; the source's close then waits for it, and the consumer is
            # cancelled during that wait, which the pull's release ends well within the grace.
            await reached(closing)
            consumer.cancel()
            await asyncio.sleep(0)
        finally:
            iterator.release.set()
        # The budget ended the stream first: the consumer gets its error, the cancellation is re-armed.
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(consumer, timeout=HANG_BOUND)
        assert [type(error) for error in ended] == [BudgetExceededError]

    report = stream.report
    assert report is not None
    assert report.state is TerminalState.INCOMPLETE
    assert [violation.message for violation in report.violations if violation.code == "cleanup_failure"] == [
        "reference cleanup also failed (CancelledError)"
    ]
    assert "budget_exhausted" in _blocking(report)


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["fan_out_outcomes", "fan_out"])
async def test_a_send_permitted_after_the_deadline_ends_the_operation_with_it(
    monkeypatch: pytest.MonkeyPatch, family: str
) -> None:
    clock = _OperationClock(monkeypatch)
    acquire = RateCoordinator.acquire

    async def late_grant(coordinator: RateCoordinator, *args: object, **kwargs: object) -> object:
        permit = await acquire(coordinator, *args, **kwargs)  # type: ignore[arg-type]
        # The permit is granted as the deadline passes, after the source was read out: the send is refused.
        clock.spend()
        return permit

    monkeypatch.setattr(RateCoordinator, "acquire", late_grant)
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 1}]})
    async with client_for(transport, policy=POLICY) as client:
        stream = getattr(client, family)([_command(0)], dispatch=DirectDispatch(concurrency=1))
        error = await _drain(stream)

    report = _assert_ended_by_the_budget(error, FAMILIES[family])
    assert _blocking(report) == ["budget_exhausted"]
    assert transport.requests == []


def _counted_rows(_method: str, query: dict[str, str]) -> dict[str, object]:
    """Serve a counted list of six rows, two a page."""
    start = int(query.get("start", 0))
    return {"result": [{"ID": index} for index in range(start, min(start + 2, 6))], "total": 6, "next": start + 2}


@pytest.mark.asyncio
async def test_a_counted_list_whose_tail_the_deadline_refuses_ends_with_the_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The head is answered in time; the tail's batch is granted its permit as the deadline passes and is refused
    # before dispatch. The refused tail pages are the budget's end, not failed pages of the traversal.
    clock = _OperationClock(monkeypatch)
    acquire = RateCoordinator.acquire
    grants = 0

    async def late_tail_grant(coordinator: RateCoordinator, *args: object, **kwargs: object) -> object:
        nonlocal grants
        permit = await acquire(coordinator, *args, **kwargs)  # type: ignore[arg-type]
        grants += 1
        if grants == 2:  # noqa: PLR2004 - the head's permit, then the tail's
            clock.spend()
        return permit

    monkeypatch.setattr(RateCoordinator, "acquire", late_tail_grant)
    portal = BatchPortal(_counted_rows)
    async with client_for(portal, policy=POLICY) as client:
        error = await asyncio.wait_for(_drain(client.iter_list_counted(LIST_REQUEST, page_size=2)), timeout=HANG_BOUND)

    report = _assert_ended_by_the_budget(error, IncompleteTraversalError)
    assert isinstance(error.__cause__, BudgetExceededError)
    assert _blocking(report) == ["budget_exhausted"]
    assert portal.waves == [1]


_QUOTA_REFUSAL = WireResponse(
    503, (("content-type", "application/json"),), b'{"error":"QUERY_LIMIT_EXCEEDED","error_description":"x"}'
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("policy", "stop"),
    [
        (ExecutionPolicy(max_attempts_per_request=1), "per-request attempt budget exhausted"),
        (
            ExecutionPolicy(max_elapsed=10.0, retry=RetryPolicy(initial_delay=20.0, maximum_delay=20.0, jitter=0)),
            "retry delay would exceed elapsed budget",
        ),
    ],
    ids=["no_attempt_left", "retry_delay"],
)
async def test_a_counted_tail_page_stopped_for_its_request_alone_is_an_incomplete_traversal(
    policy: ExecutionPolicy, stop: str
) -> None:
    # The tail's batch is refused for quota; its replay is stopped for that request alone while the operation has
    # time: a failed page of the traversal, not the operation's budget.
    def respond(request: Request) -> object:
        if request.method == "batch":
            return _QUOTA_REFUSAL
        return _counted_rows(request.method, {"start": str(request.copy_parameters().get("start", 0))})

    async with client_for(ResponderTransport(respond), policy=policy) as client:
        error = await asyncio.wait_for(_drain(client.iter_list_counted(LIST_REQUEST, page_size=2)), timeout=HANG_BOUND)

    assert isinstance(error, IncompleteTraversalError)
    assert isinstance(error.__cause__, BudgetExceededError)
    assert str(error.__cause__) == stop
    report = attached_report(error)
    assert isinstance(report, OperationReport)
    assert report.terminal_reason == IncompleteTraversalError.__name__


def _v3_command(index: int) -> Command[int]:
    return Command(Request("x.item.get", {"id": index}, ReplaySafety.SAFE, route=RouteKind.API_V3), index)


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["fan_out_outcomes", "batch_outcomes"])
async def test_a_v3_envelope_permitted_after_the_deadline_ends_the_operation_with_it(
    monkeypatch: pytest.MonkeyPatch, family: str
) -> None:
    # A v3 batch is sent as one envelope (``Executor._send_envelope``): granted its permit as the deadline passes,
    # it is refused before dispatch, recorded, and the stream ends with the budget.
    clock = _OperationClock(monkeypatch)
    acquire = RateCoordinator.acquire

    async def late_grant(coordinator: RateCoordinator, *args: object, **kwargs: object) -> object:
        permit = await acquire(coordinator, *args, **kwargs)  # type: ignore[arg-type]
        clock.spend()
        return permit

    monkeypatch.setattr(RateCoordinator, "acquire", late_grant)
    transport = ScriptedTransport(())
    commands = [_v3_command(index) for index in range(2)]
    outcomes: list[object] = []
    async with client_for(transport, policy=POLICY) as client:
        if family == "fan_out_outcomes":
            stream: OperationStream[Any] = client.fan_out_outcomes(commands, dispatch=BatchDispatch(batch_size=50))
        else:
            stream = client.batch_outcomes(commands, batch_size=50)
        async with stream:
            error = await asyncio.wait_for(_collect(stream, outcomes), timeout=HANG_BOUND)

    report = _assert_ended_by_the_budget(error, None)
    assert _blocking(report) == ["budget_exhausted"]
    assert transport.wire_calls == ()
    assert [str(getattr(outcome, "error", None)) for outcome in outcomes] == [
        "execution time budget exhausted before dispatch"
    ] * len(commands)


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["fan_out_outcomes", "iter_reference_outcomes"])
async def test_a_wait_for_page_capacity_the_deadline_ends_ends_the_stream_with_it(family: str) -> None:
    # One page in all: the first send holds it past the deadline (UNSAFE, so it is not retried, and slow to unwind,
    # so the page stays held), and the second binding waits for page capacity until the deadline ends the wait.
    async def holds_the_page(_request: Request) -> object:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.shield(asyncio.sleep(CLEANUP_GRACE / 2))
            raise
        raise AssertionError("unreachable")

    policy = ExecutionPolicy(max_elapsed=DEADLINE_BUDGET, max_pages=1)
    async with client_for(ResponderTransport(holds_the_page), policy=policy) as client:
        dispatch = DirectDispatch(concurrency=2)
        if family == "fan_out_outcomes":
            commands = [
                Command(Request("x.add", {"id": i}, ReplaySafety.UNSAFE, route=RouteKind.BARE), i) for i in (0, 1)
            ]
            stream: OperationStream[Any] = client.fan_out_outcomes(commands, dispatch=dispatch)
        else:
            unsafe = Request("x.list", replay_safety=ReplaySafety.UNSAFE, route=RouteKind.BARE)
            bindings = [_binding(index) for index in range(2)]
            stream = client.iter_reference_outcomes(unsafe, bindings, traversal=TRAVERSAL, dispatch=dispatch)
        budget_started = deadline_started()
        error = await asyncio.wait_for(_drain(stream), timeout=HANG_BOUND)
        assert_fired_near_deadline(budget_started)

    report = _assert_ended_by_the_budget(error, None)
    assert str(error) == "page reservation exceeded operation time budget"
    assert report.state is TerminalState.INCOMPLETE


# Loop turns a cancelled send takes to unwind: a few, far fewer than the cleanup grace allows.
UNWIND_TURNS = 10


class _BlocksAfter:
    """Answer the first ``answered`` requests; block every later one until cancelled.

    A blocked send unwinds slowly once cancelled, over ``UNWIND_TURNS`` loop turns.
    """

    def __init__(self, answered: int) -> None:
        self.answered = answered
        self.blocked = asyncio.Event()
        self.unwound = asyncio.Event()

    async def __call__(self, request: Request) -> object:
        if self.answered > 0:
            self.answered -= 1
            return _one_row(request)
        self.blocked.set()
        try:
            return await asyncio.Future[object]()
        except asyncio.CancelledError:
            for _turn in range(UNWIND_TURNS):
                await asyncio.sleep(0)
            self.unwound.set()
            raise


def _one_per_send(dispatch: str) -> DirectDispatch | BatchDispatch:
    # One command per send: the reference task makes it (direct) or the dispatcher's worker does (batch).
    return DirectDispatch(concurrency=2) if dispatch == "direct" else BatchDispatch(batch_size=1, concurrency=2)


@pytest.mark.asyncio
async def test_an_early_close_after_the_budget_waits_the_grace_for_the_work_it_cancels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Direct dispatch reads ahead, so the second send is in flight once the first row is read; the close
    # cancels it and waits for its reference task the grace, although no time is left. (A batch fan-out
    # sends nothing between reads; the cancelled-read test below covers the dispatcher's worker.)
    clock = _OperationClock(monkeypatch)
    responder = _BlocksAfter(answered=1)
    async with client_for(ResponderTransport(responder), policy=POLICY) as client:
        stream = client.fan_out_outcomes([_command(0), _command(1)], dispatch=_one_per_send("direct"))
        await anext(stream)
        await reached(responder.blocked)
        clock.spend()
        await asyncio.wait_for(stream.aclose(), timeout=HANG_BOUND)

    assert responder.unwound.is_set()
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.EARLY_CLOSED
    assert _blocking(report) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("dispatch", ["direct", "batch"])
async def test_a_read_cancelled_after_the_budget_defers_the_cancellation_for_the_grace(
    monkeypatch: pytest.MonkeyPatch, dispatch: str
) -> None:
    # The read's send is in flight in the reference task (direct) or the dispatcher's worker (batch) when
    # the budget runs out; the cancellation waits for it to unwind the grace, not nothing.
    clock = _OperationClock(monkeypatch)
    responder = _BlocksAfter(answered=0)
    async with client_for(ResponderTransport(responder), policy=POLICY) as client:
        stream = client.fan_out_outcomes([_command(0)], dispatch=_one_per_send(dispatch))
        read = asyncio.create_task(anext(stream))
        await reached(responder.blocked)
        clock.spend()
        read.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(read, timeout=HANG_BOUND)

    assert responder.unwound.is_set()
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.CANCELLED
    assert _blocking(report) == []


@pytest.mark.asyncio
async def test_a_read_started_with_no_time_left_defers_its_cancellation_for_the_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The second send is in flight from the read ahead; the next read starts after the budget, and its
    # cancellation still waits for cleanup the grace, not the nothing left.
    clock = _OperationClock(monkeypatch)
    responder = _BlocksAfter(answered=1)
    async with client_for(ResponderTransport(responder), policy=POLICY) as client:
        stream = client.fan_out_outcomes([_command(0), _command(1)], dispatch=_one_per_send("direct"))
        await anext(stream)
        await reached(responder.blocked)
        clock.spend()
        read = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        read.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(read, timeout=HANG_BOUND)

    assert responder.unwound.is_set()
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.CANCELLED
    assert _blocking(report) == []


UNREAD_AHEAD = 100


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync_close", "async_close"])
@pytest.mark.parametrize("family", ["batch_outcomes", "fan_out_outcomes"])
async def test_a_cancellation_outranks_a_close_that_overruns_its_window(
    family: str,
    asynchronous: bool,  # noqa: FBT001
) -> None:
    # More commands than any stream reads ahead, so the source is closed, never read out.
    source = _BlockingClose([_command(index) for index in range(UNREAD_AHEAD)])
    commands = source.asynchronous() if asynchronous else source.sync()
    first = asyncio.Event()
    blocker = Blocker()
    responses = replies({"result": [{"ID": 1}]}, blocker, blocker)
    async with client_for(ResponderTransport(responses), policy=ExecutionPolicy(max_elapsed=DEADLINE_BUDGET)) as client:
        if family == "batch_outcomes":
            stream = client.batch_outcomes(commands, batch_size=1)
        else:
            stream = client.fan_out_outcomes(commands, dispatch=DirectDispatch(concurrency=1))

        async def consume() -> None:
            async with stream:
                async for _item in stream:
                    first.set()

        budget_started = deadline_started()
        consumer = asyncio.create_task(consume())
        try:
            # The consumer read the first outcome and waits in its next read, for a send that never answers.
            await reached(first)
            await reached(blocker.started)
            consumer.cancel()
            # The close blocks until released: its window, the time left, ends the wait for it.
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(consumer, timeout=HANG_BOUND)
            assert_fired_near_deadline(budget_started)
            assert not source.closed.is_set()
        finally:
            source.set_free()
        assert await asyncio.to_thread(source.closed.wait, HANG_BOUND)

    report = stream.report
    assert report is not None
    assert report.state is TerminalState.CANCELLED
    # The overrun is recorded behind the cancellation, as a secondary failure; the caller's cancel is no refusal by
    # the budget.
    assert _blocking(report) == ["cleanup_failure"]
    assert report.terminal_reason == "iteration cancelled"
    subject = "batch" if family == "batch_outcomes" else "reference"
    assert [violation.message for violation in report.violations] == [
        f"{subject} cleanup also failed (CleanupOverrunError)"
    ]


class _ReadThenBlock:
    """One item, then a pull that blocks until the test releases it; records that it was closed."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.closed = asyncio.Event()

    async def items[T](self, first: T) -> AsyncIterator[T]:
        try:
            yield first
            await reached(self.release)
        finally:
            self.closed.set()


async def _collect(stream: AsyncIterator[Any], outcomes: list[object]) -> BaseException:
    try:
        async for outcome in stream:
            outcomes.append(outcome)  # noqa: PERF401 - each outcome is kept as it arrives, before the failure
    except Exception as error:  # noqa: BLE001 - the test inspects whichever error ended the stream
        return error
    raise AssertionError("the stream completed although its budget refused work")


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch_outcomes", "batch"])
async def test_a_deadline_while_a_chunk_fills_ends_the_batch_with_the_budget_not_the_source(family: str) -> None:
    source = _ReadThenBlock()
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 1}]})
    outcomes: list[object] = []
    async with client_for(transport, policy=ExecutionPolicy(max_elapsed=DEADLINE_BUDGET)) as client:
        stream = getattr(client, family)(source.items(_command(0)), batch_size=50)
        budget_started = deadline_started()
        try:
            async with stream:
                error = await _collect(stream, outcomes)
            assert_fired_near_deadline(budget_started)
        finally:
            source.release.set()

    report = _assert_ended_by_the_budget(error, FAMILIES[family])
    assert _blocking(report) == ["budget_exhausted"]
    assert source.closed.is_set()
    # The command read before the deadline is never sent: the budget refuses it, the source failed nothing.
    assert transport.requests == []
    refused = outcomes or list(getattr(error, "outcomes", ()))
    assert [type(outcome).__name__ for outcome in refused] == ["CommandFailure"]
    assert all(isinstance(getattr(outcome, "error", None), BudgetExceededError) for outcome in refused)


@pytest.mark.asyncio
async def test_a_deadline_while_a_kernel_chunk_fills_ends_it_with_the_budget() -> None:
    source = _ReadThenBlock()
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 1}]})
    stream = batch_outcome_stream(
        BatchExecutor(Executor(transport)),
        source.items(Request("profile", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)),
        batch_size=50,
        policy=ExecutionPolicy(max_elapsed=DEADLINE_BUDGET),
    )
    outcomes: list[object] = []
    try:
        error = await _collect(stream, outcomes)
    finally:
        source.release.set()

    assert isinstance(error, BudgetExceededError)
    assert "batch input exceeded" in str(error)
    assert transport.requests == []
    assert [type(outcome).__name__ for outcome in outcomes] == ["BatchFailure"]
    assert stream.report.state is KernelState.FAILED
    assert stream.report.terminal_reason == BudgetExceededError.__name__


class _ClosesSlowly:
    """A caller's iterator that holds a resource from the start: its close blocks until the test releases it."""

    def __init__(self, items: list[object]) -> None:
        self.items = iter(items)
        self.release = asyncio.Event()
        self.closed = asyncio.Event()

    def __aiter__(self) -> _ClosesSlowly:
        return self

    async def __anext__(self) -> object:
        try:
            return next(self.items)
        except StopIteration:
            raise StopAsyncIteration from None

    async def aclose(self) -> None:
        await reached(self.release)
        self.closed.set()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch_outcomes", "fan_out_outcomes", "iter_reference_outcomes"])
async def test_a_close_before_the_first_pull_is_bounded_by_the_cleanup_window(family: str) -> None:
    items: list[object] = [_binding(0)] if family == "iter_reference_outcomes" else [_command(0)]
    source = _ClosesSlowly(items)
    caller: Any = source
    async with client_for(
        ResponderTransport(lambda _request: {"result": [{"ID": 1}]}),
        policy=ExecutionPolicy(max_elapsed=DEADLINE_BUDGET),
    ) as client:
        if family == "batch_outcomes":
            stream: OperationStream[Any] = client.batch_outcomes(caller, batch_size=1)
        elif family == "fan_out_outcomes":
            stream = client.fan_out_outcomes(caller, dispatch=DirectDispatch(concurrency=1))
        else:
            stream = client.iter_reference_outcomes(
                LIST_REQUEST, caller, traversal=TRAVERSAL, dispatch=DirectDispatch(concurrency=1)
            )
        budget_started = deadline_started()
        try:
            with pytest.raises(CleanupOverrunError, match="cleanup exceeded"):
                await asyncio.wait_for(stream.aclose(), timeout=HANG_BOUND)
            assert_fired_near_deadline(budget_started)
            assert not source.closed.is_set()
        finally:
            source.release.set()
        await reached(source.closed)


class _FailingClose:
    """A caller's iterator of commands whose close fails: it overruns its window (blocks until released) or raises."""

    def __init__(self, failure: str) -> None:
        self.items = iter([_command(index) for index in range(ITEMS)])
        self.failure = failure
        self.release = asyncio.Event()

    def __aiter__(self) -> _FailingClose:
        return self

    async def __anext__(self) -> Command[int]:
        try:
            return next(self.items)
        except StopIteration:
            raise StopAsyncIteration from None

    async def aclose(self) -> None:
        if self.failure == "runtime":
            raise RuntimeError("the caller's close failed")
        await reached(self.release)


def _report_shape(report: OperationReport, failure: str) -> tuple[object, ...]:
    """The report with the close's error name taken out, to compare two closes that failed differently."""
    return (
        report.state,
        report.terminal_reason.replace(failure, "<close error>"),
        [(violation.code, violation.message.replace(failure, "<close error>")) for violation in report.violations],
    )


async def _close_fails(family: str, failure: str, *, read_to_end: bool) -> tuple[BaseException, OperationReport]:
    # A short real budget: the stream reads its source well within it, then the close blocks past it.
    source = _FailingClose(failure)
    caller: Any = source
    async with client_for(ResponderTransport(_one_row), policy=ExecutionPolicy(max_elapsed=DEADLINE_BUDGET)) as client:
        if family in {"batch", "batch_outcomes"}:
            stream: OperationStream[Any] = getattr(client, family)(caller, batch_size=1)
        else:
            stream = getattr(client, family)(caller, dispatch=DirectDispatch(concurrency=1))
        try:
            with pytest.raises(Exception) as caught:  # noqa: PT011 - the test compares whichever error the close raised
                await asyncio.wait_for(_read_and_close(stream, read_to_end=read_to_end), timeout=HANG_BOUND)
        finally:
            source.release.set()
    report = stream.report
    assert isinstance(report, OperationReport)
    return caught.value, report


async def _read_and_close(stream: OperationStream[Any], *, read_to_end: bool) -> None:
    async with stream:
        async for _item in stream:
            if not read_to_end:
                break


@pytest.mark.asyncio
@pytest.mark.parametrize("read_to_end", [True, False], ids=["read_to_end", "break"])
@pytest.mark.parametrize("family", ["batch", "batch_outcomes", "fan_out", "fan_out_outcomes"])
async def test_a_close_that_overruns_its_window_is_reported_as_any_failed_close(family: str, read_to_end: bool) -> None:  # noqa: FBT001
    # The overrun refused no work, and all the work was admitted: it is a cleanup failure, reported as a close that
    # raised, not the budget's end, though no time is left when the stream ends.
    overrun, report = await _close_fails(family, "overrun", read_to_end=read_to_end)
    failed, failed_report = await _close_fails(family, "runtime", read_to_end=read_to_end)

    assert type(overrun) is CleanupOverrunError
    assert type(failed) is RuntimeError
    assert _report_shape(report, "CleanupOverrunError") == _report_shape(failed_report, "RuntimeError")
    assert "budget_exhausted" not in _blocking(report)
    assert report.state is (TerminalState.FAILED if read_to_end else TerminalState.EARLY_CLOSED)
    cleanup = [violation.message for violation in report.violations if violation.code == "cleanup_failure"]
    assert len(cleanup) == 1
    assert cleanup[0].endswith("(CleanupOverrunError)")
    if not read_to_end:
        assert report.terminal_reason == "stream cleanup failed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("family", "public"),
    [("iter_reference_outcomes", InputSourceError), ("fan_out_outcomes", InputSourceError), ("fan_out", BatchFailed)],
)
async def test_a_close_after_the_source_failed_in_the_read_ahead_raises_a_public_error(
    family: str, public: type[Exception]
) -> None:
    failed = asyncio.Event()

    async def items() -> AsyncIterator[Any]:
        yield _binding(0) if family == "iter_reference_outcomes" else _command(0)
        failed.set()
        raise RuntimeError("the caller's source failed")

    async with client_for(ResponderTransport(lambda _request: {"result": [{"ID": 1}]}), policy=POLICY) as client:
        dispatch = DirectDispatch(concurrency=2)
        if family == "iter_reference_outcomes":
            stream: OperationStream[Any] = client.iter_reference_outcomes(
                LIST_REQUEST, items(), traversal=TRAVERSAL, dispatch=dispatch
            )
        else:
            stream = getattr(client, family)(items(), dispatch=dispatch)
        with pytest.raises(public) as caught:
            await _break_once(stream, failed)

    assert not type(caught.value).__name__.startswith("_")
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.EARLY_CLOSED
    # The report names the public failure too, never the private one that carried it.
    assert [violation.message for violation in report.violations] == [
        "reference cleanup also failed (InputSourceError)"
    ]


@pytest.mark.parametrize("secondary", [False, True])
def test_a_cleanup_failure_names_the_public_failure_a_private_carrier_maps_to(secondary: bool) -> None:  # noqa: FBT001
    violation = cleanup_failure_violation(
        _BindingSourceError(ValueError("source")), secondary=secondary, subject="reference"
    )
    assert violation.message.endswith("(InputSourceError)")


async def _break_once(stream: OperationStream[Any], failed: asyncio.Event) -> None:
    # Leave the stream after its first item, once its source has failed in the read-ahead.
    async with stream:
        async for _item in stream:
            await reached(failed)
            break


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["iter_reference_outcomes", "iter_reference_source_outcomes"])
async def test_a_read_the_client_closes_raises_its_source_failure_as_a_public_error(family: str) -> None:
    failed = asyncio.Event()

    async def items() -> AsyncIterator[Any]:
        binding = _binding(0)
        yield binding if family == "iter_reference_outcomes" else ReferenceSource(LIST_REQUEST, TRAVERSAL, binding)
        failed.set()
        raise RuntimeError("the caller's source failed")

    blocker = Blocker()
    async with client_for(ResponderTransport(blocker), policy=POLICY) as client:
        if family == "iter_reference_outcomes":
            stream: OperationStream[Any] = client.iter_reference_outcomes(
                LIST_REQUEST, items(), traversal=TRAVERSAL, dispatch=DirectDispatch(concurrency=1)
            )
        else:
            stream = client.iter_reference_source_outcomes(items(), dispatch=DirectDispatch(concurrency=1))
        read = asyncio.create_task(anext(stream))
        await reached(failed)
        await reached(blocker.started)
    # Closing the client closed the stream under the read, which ended with the source's failure.
    with pytest.raises(InputSourceError) as caught:
        await asyncio.wait_for(read, timeout=HANG_BOUND)
    assert not type(caught.value).__name__.startswith("_")
    # It chains straight from the source's own exception, with no private carrier between them.
    assert type(caught.value.__cause__) is RuntimeError
    assert str(caught.value.__cause__) == "the caller's source failed"


# Each wait the deadline bounds refuses through ``ExecutionContext.deadline``: the refusal is recorded and ends a
# tolerant stream with ``BudgetExceededError``. A send in flight past the deadline holds the only slot, the only
# rows or the only lease, so the next binding or command waits there until the deadline ends the wait.

_KEYSET_IDENTITY = IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_INTEGER)
_FAST_KEYSET = KeysetTraversal(
    ResultSelector(),
    _KEYSET_IDENTITY,
    page_size=10,
    keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
    execution=AutoKeysetExecution(StableIntegerKeysetContract()),
)


async def _never_answers(_method: str, _query: object) -> dict[str, object]:
    await asyncio.Event().wait()
    raise AssertionError("unreachable")


def _waiting_stream(client: Bitrix24, wait: str) -> OperationStream[Any]:
    bindings = [_binding(index) for index in range(3)]
    if wait == "direct_admission":
        return client.iter_reference_outcomes(
            LIST_REQUEST, bindings, traversal=TRAVERSAL, dispatch=DirectDispatch(concurrency=1)
        )
    if wait == "batch_admission":
        return client.iter_reference_outcomes(
            LIST_REQUEST, bindings, traversal=TRAVERSAL, dispatch=BatchDispatch(batch_size=1, concurrency=1)
        )
    if wait == "row_buffer":
        return client.fan_out_outcomes([_command(index) for index in range(3)], dispatch=DirectDispatch(concurrency=1))
    sources = [ReferenceSource(Request.bare("kappa.list", {}), _FAST_KEYSET, binding) for binding in bindings]
    return client.iter_reference_source_outcomes(on_loop(sources), dispatch=BatchDispatch(batch_size=10))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("wait", "refusal"),
    [
        ("direct_admission", "direct scheduler admission exceeded operation time budget"),
        ("batch_admission", "batch scheduler admission exceeded operation time budget"),
        ("row_buffer", "row buffer wait exceeded operation time budget"),
        ("fast_keyset_lease", "fast keyset lease wait exceeded the operation time budget"),
    ],
)
async def test_a_wait_the_deadline_ends_records_its_refusal_and_ends_the_stream(wait: str, refusal: str) -> None:
    transport: Transport = BatchPortal(_never_answers) if wait == "fast_keyset_lease" else ResponderTransport(Blocker())
    # Twenty rows hold one fast keyset lease, not two: the second binding waits for the first's.
    rows = 20 if wait == "fast_keyset_lease" else ExecutionPolicy().max_buffered_rows
    policy = ExecutionPolicy(max_elapsed=DEADLINE_BUDGET, max_buffered_rows=rows)
    async with client_for(transport, policy=policy) as client:
        stream = _waiting_stream(client, wait)
        budget_started = deadline_started()
        error = await asyncio.wait_for(_drain(stream), timeout=HANG_BOUND)
        assert_fired_near_deadline(budget_started)

    report = _assert_ended_by_the_budget(error, None)
    assert isinstance(error, BudgetExceededError)
    assert str(error) == refusal
    assert report.state is TerminalState.INCOMPLETE


def _keyset_rows(clock: _OperationClock) -> Callable[[str, dict[str, str]], dict[str, object]]:
    """Serve identities 1..200 by the keyset's bound, order and limit; the first answer spends the whole budget."""

    def answer(_method: str, query: dict[str, str]) -> dict[str, object]:
        if clock.skew == 0:
            clock.spend()
        selected = range(1, 201)
        if ">ID" in (lower := {key[7:-1]: value for key, value in query.items() if key.startswith("filter[")}):
            selected = range(int(lower[">ID"]) + 1, 201)
        upper = int(lower.get("<ID", 201))
        identities = sorted((value for value in selected if value < upper), reverse=query.get("order[ID]") == "DESC")
        return {"result": [{"ID": value} for value in identities[: int(query.get("limit", 10))]]}

    return answer


@pytest.mark.asyncio
async def test_a_fast_keyset_wave_planned_with_no_time_left_ends_the_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    # The first send spends the budget; the next fast keyset wave reserves its pages with no time left.
    clock = _OperationClock(monkeypatch)
    transport = BatchPortal(_keyset_rows(clock))
    async with client_for(transport, policy=POLICY) as client:
        source = ReferenceSource(Request.bare("kappa.list", {}), _FAST_KEYSET, _binding(0))
        stream = client.iter_reference_source_outcomes([source], dispatch=BatchDispatch(batch_size=10))
        error = await asyncio.wait_for(_drain(stream), timeout=HANG_BOUND)

    report = _assert_ended_by_the_budget(error, None)
    assert str(error) == "page reservation exceeded operation time budget"
    assert report.state is TerminalState.INCOMPLETE
    assert len(transport.waves) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["fan_out_outcomes", "iter_reference_outcomes"])
async def test_a_producer_that_waited_for_a_slot_past_the_deadline_ends_the_stream_with_it(family: str) -> None:
    # Two slots, both held by sends answered only after the deadline (UNSAFE, so nothing is retried): the producer
    # waits for a slot, gets one once the deadline has passed, and its next pull is refused.
    pulled = 0

    def bindings() -> Iterator[Any]:
        nonlocal pulled
        for index in range(UNREAD_AHEAD):
            pulled += 1
            if family == "fan_out_outcomes":
                yield Command(Request("x.add", {"id": index}, ReplaySafety.UNSAFE, route=RouteKind.BARE), index)
            else:
                yield _binding(index)

    async def late(_request: Request) -> object:
        await asyncio.sleep(2 * DEADLINE_BUDGET)
        return {"result": [{"ID": 1}]}

    policy = ExecutionPolicy(max_elapsed=DEADLINE_BUDGET, max_active_references=2)
    async with client_for(ResponderTransport(late), policy=policy) as client:
        dispatch = DirectDispatch(concurrency=2)
        if family == "fan_out_outcomes":
            stream: OperationStream[Any] = client.fan_out_outcomes(bindings(), dispatch=dispatch)
        else:
            unsafe = Request("x.list", replay_safety=ReplaySafety.UNSAFE, route=RouteKind.BARE)
            stream = client.iter_reference_outcomes(unsafe, bindings(), traversal=TRAVERSAL, dispatch=dispatch)
        error = await asyncio.wait_for(_drain(stream), timeout=HANG_BOUND)

    report = _assert_ended_by_the_budget(error, None)
    assert str(error) == "reference input exceeded operation time budget"
    assert report.state is TerminalState.INCOMPLETE
    assert pulled == 2  # noqa: PLR2004 - the two in flight; the pull after them is refused, not read


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["fan_out_outcomes", "batch_outcomes"])
async def test_a_source_raising_its_own_budget_error_is_a_source_failure(family: str) -> None:
    # The error is the caller's (a nested operation's budget, say); this operation's budget refused nothing.
    def commands() -> Iterator[Command[int]]:
        yield _command(0)
        raise BudgetExceededError("nested operation budget")

    async with client_for(ResponderTransport(lambda _request: {"result": [{"ID": 1}]}), policy=POLICY) as client:
        if family == "fan_out_outcomes":
            stream: OperationStream[Any] = client.fan_out_outcomes(commands(), dispatch=DirectDispatch())
        else:
            stream = client.batch_outcomes(commands(), batch_size=5)
        error = await asyncio.wait_for(_drain(stream), timeout=HANG_BOUND)

    assert isinstance(error, InputSourceError)
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.FAILED
    assert "budget_exhausted" not in _blocking(report)
