"""How a stream ends when it is closed, cancelled or abandoned while its work runs (review 2026-11-17, D1).

Every scenario is ordered by events the test scripts (a transport or a source that waits for the test), never by
wall time; the waits the library itself bounds (the cancellation grace) are the only time a test spends.
"""

from __future__ import annotations
import asyncio
import contextlib
import contextvars
import gc
import itertools
import logging
import signal
import sys
import threading
import time
import weakref
from typing import TYPE_CHECKING, Any

import pytest

from b24api import (
    BatchDispatch,
    Binding,
    Bitrix24,
    DirectDispatch,
    ExecutionPolicy,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    RouteKind,
    SequentialTraversal,
    TerminalState,
)
from b24api.contracts import (
    Command,
    ContinuePage,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    KeysetTraversal,
    ReferenceComplete,
    ResultSelector,
)
from b24api.contracts.keyset_execution import (
    AutoKeysetExecution,
    KeysetExecution,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    StableIntegerKeysetContract,
)
from b24api.contracts.policy import KernelState
from b24api.errors import (
    BudgetExceededError,
    ClientClosedError,
    InputSourceError,
    StreamClosedError,
    TransportError,
)
from b24api.execution import boundary
from b24api.execution.context import CANCEL_GRACE_SECONDS
from b24api.execution.lifecycle import (
    CleanupAttempt,
    LifecycleHooks,
    OperationRunner,
    TerminalCause,
    failed_kernel_report,
    kernel_terminal,
    with_cleanup_attempt,
)
from b24api.execution.rate import FOREIGN_LOOP
from b24api.execution.snapshot import KernelReport
from b24api.references import scheduler
from tests.scripting import HANG_BOUND, BatchPortal, Blocker, ResponderTransport, client_for, json_response, reached

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Iterator

    from b24api.contracts import PageBoundary
    from b24api.contracts.stream import OperationStream
    from tests.scripting import Envelope, Query

ROWS = 6
PAGE = 2
# Far fewer bindings than an endless source drained until the reader gave up would have yielded.
UNDRAINED = 50
_DENIED = json_response({"error": "ACCESS_DENIED", "error_description": "denied"}, status=403)


def _listing() -> Request:
    return Request("offset.list", {"PARENT": 0}, ReplaySafety.SAFE, route=RouteKind.BARE)


def _command(index: int) -> Command[int]:
    return Command(Request("x.get", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)


def _binding(index: int) -> Binding[int]:
    return Binding(f"parent {index}", (ParameterUpdate(ParameterPath(("PARENT",)), index),), index)


def _page(request: Request) -> object:
    """Answer an offset page of two rows, a single command, or a batch of single commands."""
    parameters = request.copy_parameters()
    if request.method == "batch":
        commands = parameters["cmd"]
        assert isinstance(commands, dict)
        return {"result": {"result": dict.fromkeys(commands, 1), "result_error": {}}}
    if request.method == "x.get":
        return {"result": 1}
    start = parameters.get("start", 0)
    assert isinstance(start, int)
    rows = [{"id": value} for value in range(start + 1, min(start + PAGE, ROWS) + 1)]
    return {"result": rows, "next": start + PAGE if start + PAGE < ROWS else None}


class _Gate:
    """A transport whose ``hold_at``-th send and every later one wait until the test opens the gate."""

    def __init__(self, hold_at: int) -> None:
        self.hold_at = hold_at
        self.held = asyncio.Event()
        self.open = asyncio.Event()
        self.transport = ResponderTransport(self.respond)

    async def respond(self, request: Request) -> object:
        if len(self.transport.requests) >= self.hold_at:
            self.held.set()
            await self.open.wait()
        return _page(request)


async def _drain(stream: OperationStream[Any]) -> list[Any]:
    return [item async for item in stream]


# a read the stream's close ends in another task raises StreamClosedError, never a cancellation.


@pytest.mark.asyncio
async def test_a_read_in_flight_when_another_task_closes_the_stream_raises_stream_closed() -> None:
    gate = _Gate(hold_at=2)
    async with client_for(gate.transport) as client:
        stream = client.iter_list(_listing(), page_size=PAGE)
        reader = asyncio.create_task(_drain(stream))
        await reached(gate.held)
        await stream.aclose()
        raised: BaseException | list[Any] = (await asyncio.gather(reader, return_exceptions=True))[0]
        assert isinstance(raised, StreamClosedError), raised
        assert not reader.cancelled()
        report = stream.report
        assert report is not None
        assert report.state is TerminalState.EARLY_CLOSED
        assert getattr(raised, "report", None) is report
        # A read that starts after the end finds the stream ended.
        with pytest.raises(StopAsyncIteration):
            await anext(stream)


@pytest.mark.asyncio
async def test_a_read_whose_own_task_is_cancelled_still_raises_its_cancellation() -> None:
    gate = _Gate(hold_at=2)
    async with client_for(gate.transport) as client:
        stream = client.iter_list(_listing(), page_size=PAGE)
        reader = asyncio.create_task(_drain(stream))
        await reached(gate.held)
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)
        assert reader.cancelled()
        report = stream.report
        assert report is not None
        assert report.state is TerminalState.CANCELLED


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["cancel", "close"])
async def test_a_read_cancelled_while_another_task_closes_the_stream_raises_its_cancellation(first: str) -> None:
    # The close ends the read too, but the reader's own cancellation is what its task receives, whichever came first.
    gate = _Gate(hold_at=2)
    async with client_for(gate.transport) as client:
        stream = client.iter_list(_listing(), page_size=PAGE)
        reader = asyncio.create_task(_drain(stream))
        await reached(gate.held)
        if first == "cancel":
            reader.cancel()
            closer = asyncio.create_task(stream.aclose())
        else:
            closer = asyncio.create_task(stream.aclose())
            await asyncio.sleep(0)
            reader.cancel()
        async with asyncio.timeout(HANG_BOUND):
            await asyncio.wait((reader, closer))
        assert reader.cancelled()
        assert closer.result() is None
    gate.open.set()


# a close from another task while the failed read cleans up is quiet; the read raises the failure.


def _failing_at(send: int) -> ResponderTransport:
    async def respond(request: Request) -> object:
        await asyncio.sleep(0)
        return _DENIED if len(transport.requests) == send else _page(request)

    transport = ResponderTransport(respond)
    return transport


def _open_failing(family: str, transport: ResponderTransport) -> tuple[Bitrix24, OperationStream[Any]]:
    client = client_for(transport, policy=ExecutionPolicy(max_elapsed=60.0))
    commands = (_command(index) for index in range(6))
    streams: dict[str, Callable[[], OperationStream[Any]]] = {
        "iter_list": lambda: client.iter_list(_listing(), page_size=PAGE),
        "batch": lambda: client.batch(commands, batch_size=1),
        "fan_out": lambda: client.fan_out(commands, dispatch=DirectDispatch(concurrency=1)),
        "references": lambda: client.iter_references(
            _listing(),
            [_binding(index) for index in range(3)],
            traversal=SequentialTraversal(page_size=PAGE),
            dispatch=DirectDispatch(concurrency=1),
        ),
    }
    return client, streams[family]()


async def _close_at_every_turn(opened: Callable[[], tuple[Bitrix24, OperationStream[Any]]]) -> None:
    """Close a stream from another task at every loop turn around its failure; the close is quiet every time.

    Whichever ends the stream first, the closer raises nothing and the stream's report is the one its read raised:
    a failure the read raised is the report's, never an early close, and never a cleanup failure.
    """
    for turns in range(40):
        client, stream = opened()
        reader = asyncio.create_task(_drain(stream))
        for _turn in range(turns):
            await asyncio.sleep(0)
        await stream.aclose()
        raised: BaseException | list[Any] = (await asyncio.gather(reader, return_exceptions=True))[0]
        report = stream.report
        assert report is not None
        codes = [violation.code for violation in report.violations]
        if isinstance(raised, StreamClosedError) or not isinstance(raised, BaseException):
            assert report.state is TerminalState.EARLY_CLOSED, (turns, report)
        else:
            assert isinstance(raised, Exception), (turns, raised)
            assert report.state is TerminalState.FAILED, (turns, report)
            assert getattr(raised, "report", None) is report, turns
        assert "cleanup_failure" not in codes, (turns, report)
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["iter_list", "batch", "fan_out", "references"])
async def test_a_close_from_another_task_never_raises_the_failure_of_the_read_it_ends(family: str) -> None:
    # The close comes before the failing send, while the failed read's cleanup runs, and after.
    await _close_at_every_turn(lambda: _open_failing(family, _failing_at(2)))


@pytest.mark.asyncio
async def test_a_close_from_another_task_cancelled_while_a_failed_read_cleans_up_raises_its_cancellation() -> None:
    # The close returns quietly for the failed read, but its own task's cancellation, arriving while the source's
    # close runs, is the closer's: at every turn the close comes in, a closer cancelled then ends cancelled.
    for turns in range(40):
        source = _SlowClose()
        client, stream = _failing_fan_out(source)
        reader = asyncio.create_task(_drain(stream))
        for _turn in range(turns):
            await asyncio.sleep(0)
        closer = asyncio.create_task(stream.aclose())

        async def cancel_the_closer_as_the_source_closes(
            closer: asyncio.Task[None] = closer, source: _SlowClose = source
        ) -> None:
            await source.closing.wait()
            closer.cancel()
            source.release.set()

        canceller = asyncio.create_task(cancel_the_closer_as_the_source_closes())
        await asyncio.wait((closer,))
        cancelled_during_the_close = source.closing.is_set() and canceller.done()
        canceller.cancel()
        source.release.set()
        await asyncio.gather(reader, canceller, return_exceptions=True)
        if cancelled_during_the_close:
            assert closer.cancelled(), turns
        await client.aclose()


def _failing_fan_out(source: _SlowClose) -> tuple[Bitrix24, OperationStream[Any]]:
    client = client_for(_failing_at(2), policy=ExecutionPolicy(max_elapsed=60.0))
    return client, client.fan_out(source.commands(), dispatch=DirectDispatch(concurrency=1))


class _SourceFailedError(ValueError):
    """The caller's source failed on its own."""


async def _failing_source() -> AsyncIterator[Command[int]]:
    yield _command(0)
    await asyncio.sleep(0)
    raise _SourceFailedError


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch", "fan_out"])
async def test_a_close_from_another_task_never_raises_the_failure_of_the_source(family: str) -> None:
    # The close comes before the source fails, while a read waits for the failed pull, and after: the source's own
    # failure is the read's, or the close ends the stream before the reader reached it, and never the close's.
    def opened() -> tuple[Bitrix24, OperationStream[Any]]:
        client = client_for(ResponderTransport(_page))
        if family == "batch":
            return client, client.batch_outcomes(_failing_source(), batch_size=1)
        return client, client.fan_out_outcomes(_failing_source(), dispatch=DirectDispatch(concurrency=1))

    await _close_at_every_turn(opened)


# collect() and first() keep what they read before the stream failed.


@pytest.mark.asyncio
async def test_collect_attaches_the_items_it_read_to_the_failure_that_ended_it() -> None:
    async with client_for(_failing_at(2)) as client:
        stream = client.iter_list(_listing(), page_size=PAGE)
        with pytest.raises(Exception) as raised:  # noqa: PT011 - the failure is the stream's own
            await stream.collect(limit=10)
        assert raised.value.partial_values == ({"id": 1}, {"id": 2})  # type: ignore[attr-defined]
        report = stream.report
        assert report is not None
        assert report.emitted == len(raised.value.partial_values)  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_first_attaches_the_item_it_read_to_the_close_that_failed_after_it() -> None:
    def commands() -> Iterator[Command[int]]:
        try:
            for index in itertools.count():
                yield _command(index)
        except GeneratorExit:
            raise RuntimeError("the source's close failed") from None

    async with client_for(ResponderTransport(_page)) as client:
        stream = client.fan_out_outcomes(commands(), dispatch=DirectDispatch())
        with pytest.raises(InputSourceError) as raised:
            await stream.first()
        (outcome,) = raised.value.partial_values  # type: ignore[attr-defined]
        assert outcome.index == 0


# a read coroutine closed while it waits (dropped, or driven by hand) leaves no stream stuck in cleanup.


@pytest.mark.asyncio
async def test_a_read_closed_while_it_waits_ends_the_stream_and_leaves_it_closable() -> None:
    gate = _Gate(hold_at=2)
    async with client_for(gate.transport) as client:
        stream = client.iter_list(_listing(), page_size=PAGE)
        await anext(stream)
        await anext(stream)
        read = stream.__anext__()
        read.send(None)
        await reached(gate.held)
        read.close()
        async with asyncio.timeout(HANG_BOUND):
            with pytest.raises(StopAsyncIteration):
                await anext(stream)
            await stream.aclose()
        report = stream.report
        assert report is not None
        assert report.state is TerminalState.EARLY_CLOSED
        assert report.terminal_reason == "stream closed before exhaustion"
        assert report.emitted == PAGE
        assert gate.transport.requests[-1].method == "offset.list"
    gate.open.set()


# a started stream the application drops stays the client's until the client closes it.


@pytest.mark.asyncio
async def test_a_started_stream_dropped_unclosed_is_kept_and_closed_by_its_client(
    caplog: pytest.LogCaptureFixture,
) -> None:
    gate = _Gate(hold_at=3)
    source_closed = asyncio.Event()

    async def bindings() -> AsyncIterator[Binding[int]]:
        try:
            for index in range(3):
                yield _binding(index)
            await gate.open.wait()  # more bindings may come: the source is open until the stream closes it
        finally:
            source_closed.set()

    client = client_for(gate.transport)
    stream = client.iter_reference_outcomes(
        _listing(), bindings(), traversal=SequentialTraversal(page_size=PAGE), dispatch=DirectDispatch(concurrency=2)
    )
    await anext(stream)
    await reached(gate.held)
    alive = weakref.ref(stream)
    del stream
    with caplog.at_level(logging.ERROR, logger="asyncio"):
        gc.collect()
        await asyncio.sleep(0)
        assert alive() is not None
        assert not source_closed.is_set()
        await client.aclose()
        assert source_closed.is_set()
        gc.collect()
    assert alive() is None
    assert not [record for record in caplog.records if "destroyed but it is pending" in record.getMessage()]
    gate.open.set()


# a cancellation the close defers behind the body's exception reaches whoever owns it, once.


class _SlowClose:
    """A command source whose close waits for the test, so a cancellation can arrive while it runs."""

    def __init__(self) -> None:
        self.closing = asyncio.Event()
        self.release = asyncio.Event()

    async def commands(self) -> AsyncIterator[Command[int]]:
        try:
            for index in range(6):
                yield _command(index)
        finally:
            self.closing.set()
            await self.release.wait()


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", ["stream", "client"])
async def test_a_cancellation_during_the_close_behind_a_failed_body_lands_at_the_next_await(owner: str) -> None:
    source = _SlowClose()
    client = client_for(ResponderTransport(_page))
    stream = client.batch_outcomes(source.commands(), batch_size=1)
    caught: list[BaseException] = []
    went_on = asyncio.Event()

    async def body() -> None:
        async with stream if owner == "stream" else client:
            await anext(stream)
            raise ValueError("body failed")

    async def work() -> None:
        try:
            await body()
        except ValueError as error:
            caught.append(error)
        await asyncio.sleep(0)
        went_on.set()

    task = asyncio.create_task(work())
    await reached(source.closing)
    task.cancel()
    source.release.set()
    await asyncio.wait((task,))
    assert task.cancelled()
    assert not went_on.is_set()
    assert [type(error) for error in caught] == [ValueError]
    await client.aclose()


@pytest.mark.asyncio
async def test_a_timeout_firing_during_the_close_behind_a_failed_body_does_not_outlive_its_scope() -> None:
    # The close defers the timeout's cancellation and re-arms it behind the body's exception; the timeout scope
    # then lets the body's exception out and takes its cancellation back. Nothing is left to cancel a later await,
    # on any supported Python.
    loop = asyncio.get_running_loop()
    source = _SlowClose()
    task = asyncio.current_task()
    assert task is not None
    async with client_for(ResponderTransport(_page)) as client:
        stream = client.batch_outcomes(source.commands(), batch_size=1)

        async def expire(scope: asyncio.Timeout) -> None:
            await source.closing.wait()
            scope.reschedule(loop.time())
            # The timeout's cancellation reaches the close before its source lets go.
            loop.call_later(0, source.release.set)

        async def body() -> None:
            async with asyncio.timeout(None) as scope:
                expiry.append(asyncio.create_task(expire(scope)))
                async with stream:
                    await anext(stream)
                    raise ValueError("body failed")

        expiry: list[asyncio.Task[None]] = []
        with pytest.raises(ValueError, match="body failed"):
            await body()
        await expiry[0]
        assert task.cancelling() == 0
        for _turn in range(3):
            await asyncio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", ["stream", "client"])
async def test_a_task_returning_right_after_the_failed_body_keeps_its_cancellation_where_python_can_arm_it(
    owner: str,
) -> None:
    # Python 3.13 disarms a cancellation that a structured owner takes back, so the close arms the one it absorbed at
    # once and a task that returns without waiting again ends cancelled. Python 3.12 cannot disarm it, so there the
    # replay is decided at the task's next wait, and a task that returns first ends with what it returned.
    source = _SlowClose()
    client = client_for(ResponderTransport(_page))
    stream = client.batch_outcomes(source.commands(), batch_size=1)

    async def body() -> None:
        async with stream if owner == "stream" else client:
            await anext(stream)
            raise ValueError("body failed")

    async def work() -> str:
        try:
            await body()
        except ValueError:
            return "returned"
        return "the body did not fail"

    task = asyncio.create_task(work())
    await reached(source.closing)
    task.cancel()
    source.release.set()
    await asyncio.wait((task,))
    if sys.version_info >= (3, 13):
        assert task.cancelled()
    else:
        assert task.result() == "returned"
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", ["stream", "client"])
async def test_a_close_cancelled_past_the_grace_raises_the_cancellation_not_the_overrun(owner: str) -> None:
    # The close overran the one-second grace because the cancellation stopped the wait for it, so the cancellation
    # leaves (here as the timeout's TimeoutError), and the close goes on in the background.
    loop = asyncio.get_running_loop()
    source = _SlowClose()
    client = client_for(ResponderTransport(_page), policy=ExecutionPolicy(max_elapsed=3600.0))
    stream = client.batch_outcomes(source.commands(), batch_size=1)
    await anext(stream)
    # Bounds a close that waited for the cleanup instead of the grace.
    loop.call_later(3.0, source.release.set)
    started = loop.time()
    try:
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.05):
                await (stream.aclose() if owner == "stream" else client.aclose())
        waited = loop.time() - started
    finally:
        source.release.set()
    assert CANCEL_GRACE_SECONDS <= waited < CANCEL_GRACE_SECONDS + 1.0, waited
    await client.aclose()
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.EARLY_CLOSED


@pytest.mark.asyncio
async def test_a_client_close_waits_for_the_cleanup_a_cancelled_stream_close_left_running() -> None:
    # The stream's cancelled close gave up waiting at the grace; its cleanup still runs. The client keeps the stream
    # until that cleanup ends, so its close waits for the source's close before it closes what it owns.
    source = _SlowClose()
    client = client_for(ResponderTransport(_page), policy=ExecutionPolicy(max_elapsed=3600.0))
    stream = client.batch_outcomes(source.commands(), batch_size=1)
    await anext(stream)
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.05):
            await stream.aclose()
    assert stream.report is not None
    closing = asyncio.create_task(client.aclose())
    for _turn in range(20):
        await asyncio.sleep(0)
    assert not closing.done()
    source.release.set()
    await asyncio.wait_for(closing, timeout=HANG_BOUND)


class _ClosesFromItsCleanup:
    """A command source whose close, once the test releases it, closes its own stream, then its own client."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.stream: OperationStream[Any] | None = None
        self.client: Bitrix24 | None = None
        self.closes: dict[str, str] = {}

    async def commands(self) -> AsyncIterator[Command[int]]:
        try:
            for index in range(6):
                yield _command(index)
        finally:
            await self.release.wait()
            assert self.stream is not None
            assert self.client is not None
            for owner, close in (("stream", self.stream.aclose), ("client", self.client.aclose)):
                try:
                    await asyncio.wait_for(close(), timeout=HANG_BOUND)
                except RuntimeError as error:
                    self.closes[owner] = f"refused: {error}"
                except TimeoutError:
                    self.closes[owner] = "waited for itself"
                else:
                    self.closes[owner] = "returned"


@pytest.mark.asyncio
async def test_the_cleanup_a_cancelled_close_left_running_is_still_the_streams_own_work() -> None:
    # The stream's cancelled close gave up waiting at the grace; its source's close goes on in the background and
    # then closes its stream and its client. Each would wait for that very cleanup: closing the stream waits for
    # nothing, closing the client is refused, and the client can then be closed.
    source = _ClosesFromItsCleanup()
    client = source.client = client_for(ResponderTransport(_page), policy=ExecutionPolicy(max_elapsed=3600.0))
    stream = source.stream = client.batch_outcomes(source.commands(), batch_size=1)
    await anext(stream)
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.05):
            await stream.aclose()
    source.release.set()
    await asyncio.wait_for(client.aclose(), timeout=3 * HANG_BOUND)

    assert source.closes == {
        "stream": "returned",
        "client": "refused: client cannot be closed from inside the pull or cleanup of one of its streams",
    }


async def _cancelled_after(stream: OperationStream[Any], turns: int) -> bool:
    """Read ``stream`` until the test cancels the reader ``turns`` loop turns in; return whether it ended cancelled."""

    async def read() -> None:
        while True:
            await anext(stream)
            await asyncio.sleep(0)

    reader = asyncio.create_task(read())
    for _turn in range(turns):
        await asyncio.sleep(0)
    reader.cancel()
    await asyncio.wait((reader,), timeout=HANG_BOUND)
    await stream.aclose()
    return reader.cancelled()


# Enough loop turns to cover several reads, each with its handoff.
_HANDOFF_TURNS = 120


async def _endless_commands() -> AsyncIterator[Command[int]]:
    for index in itertools.count():
        yield _command(index)


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch", "fan-out"])
async def test_a_readers_own_cancellation_ends_its_read_whatever_turn_of_the_handoff_it_comes_in(family: str) -> None:
    # The cancellation comes at every loop turn of a run of reads, the turns where a read's item is made but its
    # reader has not resumed yet included: the reader's task ends cancelled each time, never with the item.
    lost: list[int] = []
    for turns in range(_HANDOFF_TURNS):
        async with client_for(ResponderTransport(_page), policy=ExecutionPolicy(max_elapsed=60.0)) as client:
            stream = (
                client.batch_outcomes(_endless_commands(), batch_size=1)
                if family == "batch"
                else client.fan_out_outcomes(_endless_commands())
            )
            if not await _cancelled_after(stream, turns):
                lost.append(turns)
    assert lost == []


class _StuckSource:
    """An async command source: ``ready`` commands, then a wait nothing ends; its close waits for ``release``."""

    def __init__(self, ready: int) -> None:
        self.ready = ready
        self.taken = 0
        self.release = asyncio.Event()
        self.closing = asyncio.Event()
        self.closed = asyncio.Event()

    def __aiter__(self) -> _StuckSource:
        return self

    async def __anext__(self) -> Command[int]:
        self.taken += 1
        if self.taken > self.ready:
            await asyncio.Event().wait()
        return _command(self.taken)

    async def aclose(self) -> None:
        self.closing.set()
        await self.release.wait()
        self.closed.set()


def _stuck(family: str, ready: int) -> tuple[_StuckSource, Bitrix24, OperationStream[Any]]:
    source = _StuckSource(ready)
    client = client_for(ResponderTransport(_page), policy=ExecutionPolicy(max_elapsed=3600.0))
    if family == "batch":
        return source, client, client.batch_outcomes(source, batch_size=1)
    return source, client, client.fan_out_outcomes(source)


async def _cancelled_within_the_grace(task: asyncio.Task[Any]) -> None:
    """Cancel ``task`` and check that it ends cancelled within the grace (and a little scheduling)."""
    loop = asyncio.get_running_loop()
    started = loop.time()
    task.cancel()
    await asyncio.wait((task,), timeout=HANG_BOUND)
    assert task.cancelled()
    assert loop.time() - started < CANCEL_GRACE_SECONDS + 0.5


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch", "fan-out"])
@pytest.mark.parametrize("first", ["cancel", "close"])
async def test_a_reader_cancelled_while_another_task_closes_waits_for_that_close_only_the_grace(
    family: str, first: str
) -> None:
    # The source's close outlasts the grace. The reader's own cancellation and another task's close come in one loop
    # turn, in either order: the reader waits for the close's end only the grace, then raises its cancellation.
    source, client, stream = _stuck(family, ready=1)
    await anext(stream)
    reader = asyncio.ensure_future(anext(stream))
    for _turn in range(20):
        await asyncio.sleep(0)
    closer = None
    if first == "close":
        closer = asyncio.ensure_future(stream.aclose())
    try:
        await _cancelled_within_the_grace(reader)
    finally:
        source.release.set()
    if closer is not None:
        await asyncio.wait_for(closer, timeout=HANG_BOUND)
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch", "fan-out"])
@pytest.mark.parametrize("owner", ["stream", "client"])
async def test_a_cancelled_exit_behind_another_tasks_close_waits_for_it_only_the_grace(family: str, owner: str) -> None:
    # The body works on an item while another task closes the stream or the client, whose cleanup outlasts the grace.
    # The body's task is then cancelled: its exit joins that close, and waits for it only the grace.
    source, client, stream = _stuck(family, ready=3)
    took = asyncio.Event()

    async def body() -> None:
        async with stream:
            await anext(stream)
            took.set()
            await asyncio.Event().wait()

    task = asyncio.ensure_future(body())
    await took.wait()
    closer = asyncio.ensure_future(stream.aclose() if owner == "stream" else client.aclose())
    for _turn in range(20):
        await asyncio.sleep(0)
    try:
        await _cancelled_within_the_grace(task)
    finally:
        source.release.set()
    await asyncio.wait_for(closer, timeout=HANG_BOUND)
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch", "fan-out"])
async def test_a_client_close_joining_a_stream_close_later_cancelled_still_waits_for_its_cleanup(family: str) -> None:
    # One task closes the stream; the client's close, which nobody cancels, joins it. The stream's close is then
    # cancelled and leaves its cleanup running past the grace: the client's close still waits for that cleanup.
    source, client, stream = _stuck(family, ready=1)
    await anext(stream)
    stream_close = asyncio.ensure_future(stream.aclose())
    for _turn in range(20):
        await asyncio.sleep(0)
    client_close = asyncio.ensure_future(client.aclose())
    for _turn in range(20):
        await asyncio.sleep(0)
    await _cancelled_within_the_grace(stream_close)
    try:
        await asyncio.sleep(0.2)
        assert not client_close.done()
    finally:
        source.release.set()
    await asyncio.wait_for(client_close, timeout=HANG_BOUND)


@pytest.mark.asyncio
async def test_a_cancelled_stream_close_inside_the_clients_async_with_shares_one_grace_with_its_exit() -> None:
    # The cancellation ends the stream's close at the grace, then unwinds the client's exit, whose close waits for the
    # same cleanup: together they defer it one grace, not one each.
    source, client, stream = _stuck("batch", ready=1)

    async def scenario() -> None:
        async with client:
            await anext(stream)
            await stream.aclose()

    task = asyncio.ensure_future(scenario())
    for _turn in range(50):
        await asyncio.sleep(0)
    try:
        await _cancelled_within_the_grace(task)
    finally:
        source.release.set()
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch", "fan-out"])
@pytest.mark.parametrize("helper", ["collect", "first"])
async def test_a_cancelled_collect_or_first_waits_for_cleanup_only_the_grace(family: str, helper: str) -> None:
    # The read waits on a source whose close outlasts the grace; the helper's task is cancelled there. The helper
    # closes as an ``async with`` exit does, so the cancellation leaves after the grace.
    source, client, stream = _stuck(family, ready=0 if helper == "first" else 2)
    task = asyncio.ensure_future(stream.collect(limit=10) if helper == "collect" else stream.first())
    for _turn in range(50):
        await asyncio.sleep(0)
    try:
        await _cancelled_within_the_grace(task)
    finally:
        source.release.set()
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch", "fan-out"])
@pytest.mark.parametrize("outer", ["client", "stream", "timeout"])
async def test_a_cancellation_replayed_behind_a_failed_body_is_deferred_one_grace_by_each_exit_it_reaches(
    family: str, outer: str
) -> None:
    # The body fails, and one cancellation comes while its stream's exit closes a source whose close outlasts the
    # grace. The exit replays it behind the body's exception, and the outer exit (the client's, another stream's, or
    # the client's inside a timeout that fired) waits again: the replay is a new delivery, with a grace of its own,
    # and no exit defers it longer than that.
    source, client, stream = _stuck(family, ready=3)
    other = _StuckSource(ready=3)
    outer_stream = client.batch_outcomes(other, batch_size=1)
    loop = asyncio.get_running_loop()
    scope = asyncio.timeout(None)

    async def body() -> None:
        async with stream:
            await anext(stream)
            raise ValueError("body failed")

    async def scenario() -> None:
        if outer == "stream":
            async with outer_stream:
                await anext(outer_stream)
                await body()
        elif outer == "timeout":
            async with scope, client:
                await body()
        else:
            async with client:
                await body()

    task = asyncio.ensure_future(scenario())
    await reached(source.closing)
    started = loop.time()
    if outer == "timeout":
        scope.reschedule(loop.time())
    else:
        task.cancel()
    try:
        await asyncio.wait((task,), timeout=HANG_BOUND)
        assert isinstance(task.exception(), ValueError)
        assert 2 * CANCEL_GRACE_SECONDS - 0.05 <= loop.time() - started < 2 * CANCEL_GRACE_SECONDS + 0.5
    finally:
        source.release.set()
        other.release.set()
    await client.aclose()


async def _taken_back_by_a_task_group(source: _SlowClose, failing_body: Callable[[], Awaitable[None]]) -> None:
    """Run ``failing_body`` in a ``TaskGroup`` whose child fails while the body's exit closes ``source``.

    The group cancels the task there, then waits for a sibling's cleanup before it takes the request back.
    """
    loop = asyncio.get_running_loop()
    started = asyncio.Event()

    async def failing_child() -> None:
        await source.closing.wait()
        loop.call_later(0, source.release.set)
        raise RuntimeError("child failed")

    async def slow_sibling() -> None:
        try:
            started.set()
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(0.2)
            raise

    with contextlib.suppress(BaseExceptionGroup):
        async with asyncio.TaskGroup() as group:
            group.create_task(failing_child())
            group.create_task(slow_sibling())
            await started.wait()
            await failing_body()


async def _replayed_then_taken_back(phase: str, client: Bitrix24) -> None:
    """Replay a cancellation behind a failed body's exception, which a structured owner then takes back.

    A timeout, or a ``TaskGroup`` whose child fails, cancels the task while the body's stream exit closes its source;
    the exit replays the cancellation behind the body's ``ValueError``. The replay lands at once in the owner's exit
    (``timeout``), in the caller's handler (``handler``), in a close the handler awaits (``client_close``), in the
    client's exit the exception unwinds first (``client_exit``), or while the group waits for a sibling's cleanup
    (``task_group``); the owner then takes the request back.
    """
    loop = asyncio.get_running_loop()
    source = _SlowClose()
    stream = client.batch_outcomes(source.commands(), batch_size=1)

    async def failing_body() -> None:
        async with stream:
            await anext(stream)
            raise ValueError("body failed")

    if phase == "task_group":
        await _taken_back_by_a_task_group(source, failing_body)
        return

    async def expire(scope: asyncio.Timeout) -> None:
        await source.closing.wait()
        scope.reschedule(loop.time())
        loop.call_later(0, source.release.set)

    with contextlib.suppress(ValueError, TimeoutError):
        async with asyncio.timeout(None) as scope:
            expiry = asyncio.ensure_future(expire(scope))
            try:
                if phase == "client_exit":
                    async with client:
                        await failing_body()
                else:
                    await failing_body()
            except ValueError:
                if phase == "handler":
                    await asyncio.sleep(0)
                elif phase == "client_close":
                    await client.aclose()
                raise
    await expiry


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["timeout", "handler", "client_close", "client_exit", "task_group"])
async def test_a_replay_its_owner_took_back_leaves_a_later_cancellation_a_grace_of_its_own(phase: str) -> None:
    # A replayed cancellation is taken back by its owner (``_replayed_then_taken_back``). Once that grace is over, the
    # same task is cancelled again while another stream's exit waits for a source's close that ends within a grace: a
    # new cancellation, it waits for that close, whatever became of the replay.
    loop = asyncio.get_running_loop()
    source, client, later = _stuck("batch", ready=1)
    working = asyncio.Event()

    async def scenario() -> None:
        await _replayed_then_taken_back(phase, client_for(ResponderTransport(_page)))
        await asyncio.sleep(CANCEL_GRACE_SECONDS + 0.1)
        async with later:
            await anext(later)
            working.set()
            await asyncio.Event().wait()

    task = asyncio.ensure_future(scenario())
    await reached(working)
    task.cancel()
    await reached(source.closing)
    loop.call_later(CANCEL_GRACE_SECONDS / 2, source.release.set)
    await asyncio.wait((task,), timeout=HANG_BOUND)
    assert task.cancelled()
    assert source.closed.is_set()
    await client.aclose()


class _Escape(BaseException):
    """A ``BaseException`` that is no interrupt and no cancellation: an escape, raised as Python raises it."""


class _HonoursCancelLate:
    """A runner body: one item, then a read that ends a while after it is cancelled, as the test releases it.

    It ends in that cancellation, in a failure of its own (``ends="failed"``), in an escape (``ends="escaped"``), in an
    interrupt (``ends="interrupted"``), or with an item (``ends="item"``).
    """

    def __init__(self, *, ends: str = "cancelled") -> None:
        self.ends = ends
        self.taken = 0
        self.cancelled = asyncio.Event()
        self.release = asyncio.Event()

    def __aiter__(self) -> _HonoursCancelLate:
        return self

    async def __anext__(self) -> int:
        self.taken += 1
        if self.taken == 1:
            return 1
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            # A close's cancellation of the pull, after its reader's, does not cut this short.
            while not self.release.is_set():
                with contextlib.suppress(asyncio.CancelledError):
                    await self.release.wait()
            if self.ends == "failed":
                raise ValueError("pull failed") from None
            if self.ends == "escaped":
                raise _Escape("pull escaped") from None
            if self.ends == "interrupted":
                raise SystemExit("pull interrupted") from None
            if self.ends == "item":
                return 2
            raise
        raise AssertionError("unreachable")


@pytest.mark.asyncio
@pytest.mark.parametrize("ends", ["cancelled", "failed"])
async def test_a_reader_whose_pull_ends_within_the_grace_shares_that_grace_with_the_cleanup(ends: str) -> None:
    # The reader is cancelled; its pull ends within the grace, in that cancellation or in a failure that absorbs it,
    # then cleanup outlasts it. The read's end defers the reader's cancellation for what is left of its grace, not for
    # a grace of its own: the read raises the pull's cancellation or its failure one grace after the cancel.
    fails = ends == "failed"
    body = _HonoursCancelLate(ends=ends)
    cleaned = asyncio.Event()

    async def cleanup() -> None:
        await cleaned.wait()

    def finalize(cause: TerminalCause, reason: str | None, attempt: CleanupAttempt) -> KernelReport:
        state, terminal_reason = kernel_terminal(cause, reason)
        return with_cleanup_attempt(KernelReport(state=state, terminal_reason=terminal_reason), cause, attempt)

    hooks = LifecycleHooks(finalize=finalize, failure_report=failed_kernel_report, cleanup=cleanup)
    runner = OperationRunner(body, hooks)
    loop = asyncio.get_running_loop()
    assert await anext(runner) == 1
    reader = asyncio.ensure_future(anext(runner))
    for _turn in range(20):
        await asyncio.sleep(0)
    started = loop.time()
    reader.cancel()
    await reached(body.cancelled)
    loop.call_later(CANCEL_GRACE_SECONDS * 0.6, body.release.set)
    try:
        await asyncio.wait((reader,), timeout=HANG_BOUND)
        if fails:
            assert isinstance(reader.exception(), ValueError)
        else:
            assert reader.cancelled()
        assert loop.time() - started < CANCEL_GRACE_SECONDS + 0.4
    finally:
        cleaned.set()


@pytest.mark.asyncio
@pytest.mark.parametrize("ends", ["failed", "item"])
@pytest.mark.parametrize("order", ["reader_first", "close_first"])
async def test_a_reader_whose_pull_absorbed_its_cancellation_behind_another_tasks_close_keeps_it(
    ends: str, order: str
) -> None:
    # Another task closes the stream while the reader is cancelled, in either order; the pull then ends, within the
    # grace, in a failure or with an item, either of which absorbs the reader's cancellation, and cleanup outlasts it.
    # The reader waits for that close only what is left of its grace, and keeps its cancellation: replayed behind the
    # failure the read raises, or raised in place of the item that is not its.
    body = _HonoursCancelLate(ends=ends)
    cleaned = asyncio.Event()

    async def cleanup() -> None:
        await cleaned.wait()

    def finalize(cause: TerminalCause, reason: str | None, attempt: CleanupAttempt) -> KernelReport:
        state, terminal_reason = kernel_terminal(cause, reason)
        return with_cleanup_attempt(KernelReport(state=state, terminal_reason=terminal_reason), cause, attempt)

    hooks = LifecycleHooks(finalize=finalize, failure_report=failed_kernel_report, cleanup=cleanup)
    runner = OperationRunner(body, hooks)
    loop = asyncio.get_running_loop()
    assert await anext(runner) == 1
    raised: list[BaseException] = []

    async def read() -> None:
        try:
            await anext(runner)
        except ValueError as error:
            raised.append(error)
        await asyncio.sleep(CANCEL_GRACE_SECONDS)

    reader = asyncio.ensure_future(read())
    for _turn in range(20):
        await asyncio.sleep(0)
    if order == "close_first":
        closer = asyncio.ensure_future(runner.aclose())
        await reached(body.cancelled)
    started = loop.time()
    reader.cancel()
    if order == "reader_first":
        await reached(body.cancelled)
        closer = asyncio.ensure_future(runner.aclose())
    loop.call_later(CANCEL_GRACE_SECONDS * 0.6, body.release.set)
    try:
        await asyncio.wait((reader,), timeout=HANG_BOUND)
        assert reader.cancelled()
        assert loop.time() - started < CANCEL_GRACE_SECONDS + 0.4
        assert [type(error) for error in raised] == ([ValueError] if ends == "failed" else [])
    finally:
        cleaned.set()
        await asyncio.wait((closer,), timeout=HANG_BOUND)


@pytest.mark.asyncio
async def test_a_reader_whose_read_failed_behind_another_tasks_close_then_cancelled_raises_its_cancellation() -> None:
    # Another task closes the stream; the pull answers the close's cancellation by failing, and the reader, not
    # cancelled, waits for that close to end. Cancelled while it waits, it waits only the grace and raises its
    # cancellation, not the read's failure.
    body = _HonoursCancelLate(ends="failed")
    cleaned = asyncio.Event()

    async def cleanup() -> None:
        await cleaned.wait()

    def finalize(cause: TerminalCause, reason: str | None, attempt: CleanupAttempt) -> KernelReport:
        state, terminal_reason = kernel_terminal(cause, reason)
        return with_cleanup_attempt(KernelReport(state=state, terminal_reason=terminal_reason), cause, attempt)

    hooks = LifecycleHooks(finalize=finalize, failure_report=failed_kernel_report, cleanup=cleanup)
    runner = OperationRunner(body, hooks)
    loop = asyncio.get_running_loop()
    assert await anext(runner) == 1
    reader = asyncio.ensure_future(anext(runner))
    for _turn in range(20):
        await asyncio.sleep(0)
    closer = asyncio.ensure_future(runner.aclose())
    await reached(body.cancelled)
    body.release.set()
    for _turn in range(20):
        await asyncio.sleep(0)
    assert not reader.done()
    started = loop.time()
    reader.cancel()
    try:
        await asyncio.wait((reader,), timeout=HANG_BOUND)
        assert reader.cancelled()
        assert loop.time() - started < CANCEL_GRACE_SECONDS + 0.4
    finally:
        cleaned.set()
        await asyncio.wait((closer,), timeout=HANG_BOUND)


def _runner_with_cleanup(
    body: AsyncIterator[int], cleaned: asyncio.Event, *, ends_in: BaseException | None = None
) -> OperationRunner[int, KernelReport]:
    """Return a runner over ``body`` whose cleanup ends once ``cleaned`` is set, in ``ends_in`` if given."""

    async def cleanup() -> None:
        await cleaned.wait()
        if ends_in is not None:
            raise ends_in

    def finalize(cause: TerminalCause, reason: str | None, attempt: CleanupAttempt) -> KernelReport:
        state, terminal_reason = kernel_terminal(cause, reason)
        return with_cleanup_attempt(KernelReport(state=state, terminal_reason=terminal_reason), cause, attempt)

    hooks = LifecycleHooks(finalize=finalize, failure_report=failed_kernel_report, cleanup=cleanup)
    return OperationRunner(body, hooks)


@pytest.mark.asyncio
@pytest.mark.parametrize("order", ["reader_first", "close_first"])
async def test_a_reader_cancelled_behind_another_tasks_close_gets_its_cancellation_once(order: str) -> None:
    # The reader is cancelled in the same turn another task closes the stream; its pull ends in the cancellation. The
    # read raises it, and it is not replayed as well: the reader's handler can await.
    body = _HonoursCancelLate()
    body.release.set()
    cleaned = asyncio.Event()
    cleaned.set()
    runner = _runner_with_cleanup(body, cleaned)
    assert await anext(runner) == 1
    handled: list[str] = []

    async def read() -> None:
        try:
            await anext(runner)
        except asyncio.CancelledError:
            await asyncio.sleep(0.01)
            handled.append("awaited")
            raise

    reader = asyncio.ensure_future(read())
    for _turn in range(20):
        await asyncio.sleep(0)
    if order == "close_first":
        closer = asyncio.ensure_future(runner.aclose())
        reader.cancel()
    else:
        reader.cancel()
        closer = asyncio.ensure_future(runner.aclose())
    await asyncio.wait((reader, closer), timeout=HANG_BOUND)
    assert reader.cancelled()
    assert handled == ["awaited"]


@pytest.mark.asyncio
async def test_a_replayed_cancellation_landing_in_a_read_another_task_closes_is_still_the_readers() -> None:
    # The reader's task is cancelled while the close behind a failed body runs; that close replays the cancellation
    # behind the body's failure, which leaves the task's cancellation count as it was before the read. It lands in the
    # reader's next read, of another stream, which another task closes meanwhile: the read raises the cancellation, as
    # it would raise one the reader's task took directly, never ``StreamClosedError`` in its place.
    exiting, released = asyncio.Event(), asyncio.Event()
    first = _runner_with_cleanup(_HonoursCancelLate(), released)
    body = _HonoursCancelLate()
    body.release.set()
    always = asyncio.Event()
    always.set()
    second = _runner_with_cleanup(body, always)
    assert await anext(second) == 1
    reading = asyncio.Event()
    raised: list[str] = []

    async def read() -> None:
        with contextlib.suppress(ValueError):
            async with first:
                assert await anext(first) == 1
                exiting.set()
                raise ValueError("body failed")
        reading.set()
        try:
            await anext(second)
        except (StreamClosedError, ValueError) as error:
            raised.append(type(error).__name__)
        await asyncio.sleep(0)
        raised.append("went on")

    async def close() -> None:
        await reading.wait()
        await second.aclose()

    reader = asyncio.ensure_future(read())
    closer = asyncio.ensure_future(close())
    await reached(exiting)
    await asyncio.sleep(0)
    reader.cancel()
    released.set()
    await asyncio.wait((reader, closer), timeout=HANG_BOUND)
    assert reader.cancelled()
    assert raised == []


async def _read_holding_a_cancellation(
    runner: OperationRunner[int, KernelReport], reading: asyncio.Event, raised: list[str]
) -> None:
    """Take a cancellation, set ``reading``, then read ``runner`` once, recording in ``raised`` what the read raised.

    A cancellation the read raises is delivered once: its handler can await.
    """
    task = asyncio.current_task()
    assert task is not None
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.sleep(0)
    reading.set()
    try:
        raised.append(f"item {await anext(runner)}")
    except (StreamClosedError, ValueError, _Escape, SystemExit) as error:
        raised.append(type(error).__name__)
    except asyncio.CancelledError:
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            raised.append("cancelled again")
        raise
    await asyncio.sleep(0)
    raised.append("went on")


async def _read_once(runner: OperationRunner[int, KernelReport], raised: list[str]) -> None:
    """Read ``runner`` once, recording in ``raised`` what the read raised, then await once more.

    A cancellation the read raises is delivered once: its handler can await. One replayed behind a failure the read
    raises comes at the next await.
    """
    try:
        raised.append(f"item {await anext(runner)}")
    except (StreamClosedError, ValueError, _Escape, SystemExit) as error:
        raised.append(type(error).__name__)
    except asyncio.CancelledError:
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            raised.append("cancelled again")
        raise
    await asyncio.sleep(0)
    raised.append("went on")


async def _close_keeping_its_escape(runner: OperationRunner[int, KernelReport]) -> str | None:
    """Close ``runner`` and return the name of the escape the close raised.

    An interrupt a task ends in would leave the event loop, so the close keeps it.
    """
    try:
        await runner.aclose()
    except (_Escape, SystemExit) as error:
        return type(error).__name__
    return None


async def _cancelled_beside_a_close(
    ends: str, first: str, turns: int, *, replayed: bool
) -> tuple[bool, list[str], str | None]:
    """Cancel a reader, directly or by a replay, ``turns`` turns before or after another task's close of its stream.

    The reader's task holds a cancellation it took before the read, so a replay (``uncancel()``, then ``cancel()``)
    leaves its cancellation count as the read found it, where a direct one raises it. The pull ends as ``ends`` says
    once cancelled, and the close's cleanup outlasts the grace. Return whether the reader ended cancelled, what it
    raised and did before that, and what the close raised; the reader ends within its grace either way.
    """
    body = _HonoursCancelLate(ends=ends)
    body.release.set()
    cleaned = asyncio.Event()
    runner = _runner_with_cleanup(body, cleaned)
    assert await anext(runner) == 1
    reading = asyncio.Event()
    raised: list[str] = []

    def cancel() -> None:
        if replayed:
            reader.uncancel()
        reader.cancel()

    reader = asyncio.ensure_future(_read_holding_a_cancellation(runner, reading, raised))
    await reached(reading)
    for _turn in range(20):
        await asyncio.sleep(0)
    loop = asyncio.get_running_loop()
    started = loop.time()
    if first == "close":
        closer = asyncio.ensure_future(_close_keeping_its_escape(runner))
    else:
        cancel()
    for _turn in range(turns):
        await asyncio.sleep(0)
    if first == "close":
        cancel()
    else:
        closer = asyncio.ensure_future(_close_keeping_its_escape(runner))
    try:
        await asyncio.wait((reader,), timeout=HANG_BOUND)
        assert loop.time() - started < CANCEL_GRACE_SECONDS + 0.4
    finally:
        cleaned.set()
        await asyncio.wait((reader, closer), timeout=HANG_BOUND)
    return reader.cancelled(), raised, closer.result()


@pytest.mark.asyncio
@pytest.mark.parametrize("turns", range(4))
@pytest.mark.parametrize("first", ["close", "cancel"])
@pytest.mark.parametrize("ends", ["cancelled", "failed", "escaped", "interrupted", "item"])
async def test_a_replayed_cancellation_is_the_readers_whatever_turn_of_another_tasks_close_it_lands_in(
    ends: str, first: str, turns: int
) -> None:
    # A replayed cancellation ends the read as the reader's own, within its grace, whichever turn of the close it lands
    # in (the pull's, its handoff's, or the wait for the close's end) and however the pull ends: raised, replayed
    # behind the pull's failure, raised in place of the pull's escape or interrupt when the close ended the stream (the
    # escape is the end's, raised once), or in place of an item the close made not the reader's; never
    # ``StreamClosedError``. What the reader caught decides, not its cancellation count, so a direct cancellation
    # takes the same path.
    escape = {"escaped": "_Escape", "interrupted": "SystemExit"}.get(ends)
    cancelled, raised, closed = await _cancelled_beside_a_close(ends, first, turns, replayed=True)
    if ends == "item":
        # The item is the reader's when the pull returned it before the close: as with a direct cancellation.
        assert (cancelled, raised, closed) == await _cancelled_beside_a_close(ends, first, turns, replayed=False)
    elif escape is not None and raised:
        # The read ended the stream, which a close begun first never lets it: it raises the escape, which outranks
        # the cancellation, as Python raises it.
        assert first == "cancel"
        assert (cancelled, raised, closed) == (False, [escape, "went on"], None)
    else:
        assert cancelled
        assert raised == [] or (ends == "failed" and raised == ["ValueError"])
        # The escape is raised once: by the close, which ended the stream.
        assert closed == escape
    assert "StreamClosedError" not in raised


def _runner_escaping_its_cleanup(
    body: AsyncIterator[int], finalized: asyncio.Event
) -> OperationRunner[int, KernelReport]:
    """Return a runner over ``body`` whose cleanup escapes at once; its report is made once ``finalized`` is set."""

    async def cleanup() -> None:
        raise _Escape("cleanup escaped")

    async def finalize(cause: TerminalCause, reason: str | None, attempt: CleanupAttempt) -> KernelReport:
        await finalized.wait()
        state, terminal_reason = kernel_terminal(cause, reason)
        return with_cleanup_attempt(KernelReport(state=state, terminal_reason=terminal_reason), cause, attempt)

    hooks = LifecycleHooks(finalize=finalize, failure_report=failed_kernel_report, cleanup=cleanup)
    return OperationRunner(body, hooks)


@pytest.mark.asyncio
@pytest.mark.parametrize("ends", ["cancelled", "failed", "item"])
async def test_a_cancelled_reader_beside_a_close_whose_cleanup_escapes_leaves_the_escape_to_the_close(
    ends: str,
) -> None:
    # The reader is cancelled, then another task closes the stream; the pull ends in the reader's cancellation, in a
    # failure, or with an item the close made not the reader's (in the read's handoff). The close's cleanup escapes at
    # once and its report outlasts the reader's grace: the escape is latched, the end not published yet. It is the
    # end's, which the close raises, once. The reader waits only its grace and keeps its cancellation (replayed behind
    # the failure the read raises), never the escape.
    body = _HonoursCancelLate(ends=ends)
    finalized = asyncio.Event()
    runner = _runner_escaping_its_cleanup(body, finalized)
    loop = asyncio.get_running_loop()
    assert await anext(runner) == 1
    raised: list[str] = []
    reader = asyncio.ensure_future(_read_once(runner, raised))
    for _turn in range(20):
        await asyncio.sleep(0)
    started = loop.time()
    reader.cancel()
    await reached(body.cancelled)
    closer = asyncio.ensure_future(_close_keeping_its_escape(runner))
    for _turn in range(20):
        await asyncio.sleep(0)
    body.release.set()
    try:
        await asyncio.wait((reader,), timeout=HANG_BOUND)
        assert reader.cancelled()
        assert loop.time() - started < CANCEL_GRACE_SECONDS + 0.4
        assert raised == (["ValueError"] if ends == "failed" else [])
        assert not closer.done()
    finally:
        finalized.set()
        await asyncio.wait((closer,), timeout=HANG_BOUND)
    assert closer.result() == "_Escape"


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup", [0.0, 0.3])
@pytest.mark.parametrize("first", ["close", "cancel"])
async def test_a_cancelled_reader_whose_pull_is_interrupted_beside_a_close_raises_the_interrupt_published_in_its_grace(
    first: str, cleanup: float
) -> None:
    # The reader is cancelled and another task closes the stream, in either order; the pull absorbs the reader's
    # cancellation by raising an interrupt, and the close's cleanup ends within the reader's grace (at once, or a
    # while into it). Every end raises the interrupt: the read raises it once the end is published, in place of its
    # reader's cancellation, as from a ``finally``; so does the close.
    body = _HonoursCancelLate(ends="interrupted")
    cleaned = asyncio.Event()
    runner = _runner_with_cleanup(body, cleaned)
    assert await anext(runner) == 1
    raised: list[str] = []
    reader = asyncio.ensure_future(_read_once(runner, raised))
    for _turn in range(20):
        await asyncio.sleep(0)
    loop = asyncio.get_running_loop()
    started = loop.time()
    closer: asyncio.Future[str | None] | None = None
    if first == "close":
        closer = asyncio.ensure_future(_close_keeping_its_escape(runner))
        await reached(body.cancelled)
    reader.cancel()
    await reached(body.cancelled)
    if closer is None:
        closer = asyncio.ensure_future(_close_keeping_its_escape(runner))
    for _turn in range(20):
        await asyncio.sleep(0)
    body.release.set()
    try:
        await asyncio.sleep(cleanup * CANCEL_GRACE_SECONDS)
        cleaned.set()
        await asyncio.wait((reader, closer), timeout=HANG_BOUND)
        assert loop.time() - started < CANCEL_GRACE_SECONDS
        assert not reader.cancelled()
        assert raised == ["SystemExit", "went on"]
        assert closer.result() == "SystemExit"
    finally:
        cleaned.set()
        await asyncio.wait((reader, closer), timeout=HANG_BOUND)


@pytest.mark.asyncio
@pytest.mark.parametrize("cancellation", ["first", "second"])
async def test_a_cancellation_arriving_while_a_closed_read_waits_gives_way_to_the_interrupt_published_in_its_grace(
    cancellation: str,
) -> None:
    # Another task closes the stream while a read waits, and the pull ends in an interrupt. A cancellation of the
    # reader that arrives while the read waits for the close's end (its first, or a second behind one the pull
    # absorbed) gives way to the interrupt once the end is published within its grace: the read raises the
    # interrupt, as from a ``finally``; so does the close.
    body = _HonoursCancelLate(ends="interrupted")
    cleaned = asyncio.Event()
    runner = _runner_with_cleanup(body, cleaned)
    assert await anext(runner) == 1
    raised: list[str] = []
    reader = asyncio.ensure_future(_read_once(runner, raised))
    for _turn in range(20):
        await asyncio.sleep(0)
    loop = asyncio.get_running_loop()
    closer = asyncio.ensure_future(_close_keeping_its_escape(runner))
    await reached(body.cancelled)
    started = loop.time()
    if cancellation == "second":
        reader.cancel()
    for _turn in range(20):
        await asyncio.sleep(0)
    body.release.set()
    try:
        for _turn in range(20):
            await asyncio.sleep(0)
        assert not reader.done()
        reader.cancel()
        await asyncio.sleep(0.3 * CANCEL_GRACE_SECONDS)
        cleaned.set()
        await asyncio.wait((reader, closer), timeout=HANG_BOUND)
        assert loop.time() - started < CANCEL_GRACE_SECONDS
        assert not reader.cancelled()
        assert raised == ["SystemExit", "went on"]
        assert closer.result() == "SystemExit"
    finally:
        cleaned.set()
        await asyncio.wait((reader, closer), timeout=HANG_BOUND)


@pytest.mark.asyncio
@pytest.mark.parametrize("order", ["reader_first", "close_first"])
async def test_a_second_cancellation_of_a_reader_whose_pull_failed_behind_another_tasks_close_is_raised_once(
    order: str,
) -> None:
    # The reader is cancelled and another task closes the stream, in either order; the pull absorbs the reader's
    # cancellation by failing, and the close's cleanup outlasts the grace. A second cancellation that arrives while the
    # read waits for that close ends with the first one's grace, and is what the read raises, once: the first one is
    # not replayed behind it, nor the failure.
    body = _HonoursCancelLate(ends="failed")
    cleaned = asyncio.Event()
    runner = _runner_with_cleanup(body, cleaned)
    loop = asyncio.get_running_loop()
    assert await anext(runner) == 1
    raised: list[str] = []
    reader = asyncio.ensure_future(_read_once(runner, raised))
    for _turn in range(20):
        await asyncio.sleep(0)
    if order == "close_first":
        closer = asyncio.ensure_future(runner.aclose())
        await reached(body.cancelled)
    started = loop.time()
    reader.cancel()
    if order == "reader_first":
        await reached(body.cancelled)
        closer = asyncio.ensure_future(runner.aclose())
    for _turn in range(20):
        await asyncio.sleep(0)
    body.release.set()
    loop.call_later(CANCEL_GRACE_SECONDS * 0.3, reader.cancel)
    try:
        await asyncio.wait((reader,), timeout=HANG_BOUND)
        assert reader.cancelled()
        assert loop.time() - started < CANCEL_GRACE_SECONDS + 0.4
        assert raised == []
    finally:
        cleaned.set()
        await asyncio.wait((closer,), timeout=HANG_BOUND)


class _ClosedInItsHandoff:
    """A runner body: one item, then a read that starts another task's close of ``runner`` as it returns its item.

    The close starts in the pull's last step, from a context of no stream's work, so it claims the end in the read's
    handoff: its item made, its reader not resumed yet. What the close raises is kept in ``closed``: an interrupt a
    task ends in would leave the event loop.
    """

    def __init__(self) -> None:
        self.taken = 0
        self.runner: OperationRunner[int, KernelReport] | None = None
        self.closer: asyncio.Task[None] | None = None
        self.closed: list[BaseException] = []

    def __aiter__(self) -> _ClosedInItsHandoff:
        return self

    async def __anext__(self) -> int:
        self.taken += 1
        if self.taken == 1:
            return 1
        await asyncio.sleep(0)
        assert self.runner is not None
        self.closer = asyncio.get_running_loop().create_task(self._close(self.runner), context=contextvars.Context())
        return 2

    async def _close(self, runner: OperationRunner[int, KernelReport]) -> None:
        try:
            await runner.aclose()
        except SystemExit as error:
            self.closed.append(error)


@pytest.mark.asyncio
async def test_a_read_a_close_ends_in_its_handoff_raises_the_interrupt_the_close_raises() -> None:
    # Another task's close claims the end in the handoff of a read whose pull returned an item, and the close's
    # cleanup raises an interrupt. The item is not the reader's, and the read raises the interrupt, as every end of the
    # stream does, not ``StreamClosedError``.
    body = _ClosedInItsHandoff()
    cleaned = asyncio.Event()
    cleaned.set()
    runner = body.runner = _runner_with_cleanup(body, cleaned, ends_in=SystemExit("cleanup interrupt"))
    assert await anext(runner) == 1
    with pytest.raises(SystemExit, match="cleanup interrupt"):
        await anext(runner)
    assert body.closer is not None
    await asyncio.wait((body.closer,), timeout=HANG_BOUND)
    assert [type(error) for error in body.closed] == [SystemExit]


@pytest.mark.asyncio
async def test_a_reader_cancelled_in_a_handoff_a_close_ends_raises_the_interrupt_published_in_its_grace() -> None:
    # Another task's close claims the end in the handoff of a read whose pull returned an item, and its cleanup raises
    # an interrupt a while later. The reader, cancelled while it waits for that end, raises the interrupt once the end
    # is published within its grace, in place of its cancellation, as from a ``finally``; so does the close.
    body = _ClosedInItsHandoff()
    cleaned = asyncio.Event()
    runner = body.runner = _runner_with_cleanup(body, cleaned, ends_in=SystemExit("cleanup interrupt"))
    assert await anext(runner) == 1
    raised: list[str] = []
    reader = asyncio.ensure_future(_read_once(runner, raised))
    for _turn in range(20):
        await asyncio.sleep(0)
    closer = body.closer
    assert closer is not None
    loop = asyncio.get_running_loop()
    try:
        assert not reader.done()
        started = loop.time()
        reader.cancel()
        await asyncio.sleep(0.3 * CANCEL_GRACE_SECONDS)
        cleaned.set()
        await asyncio.wait((reader, closer), timeout=HANG_BOUND)
        assert loop.time() - started < CANCEL_GRACE_SECONDS
        assert not reader.cancelled()
        assert raised == ["SystemExit", "went on"]
        assert [type(error) for error in body.closed] == [SystemExit]
    finally:
        cleaned.set()
        await asyncio.wait((reader, closer), timeout=HANG_BOUND)


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["waiting", "cancelled"])
@pytest.mark.parametrize("ends", ["escaped", "interrupted"])
async def test_a_read_ends_as_a_close_whose_coroutine_is_closed_would_end_it(ends: str, reader: str) -> None:
    # Another caller's close ends a read in flight, and the close's coroutine is closed while its cleanup runs, as a
    # loop that stops closes what is pending: a task of the runner's own finishes the end, and it can raise nothing.
    # The read ends as that close's cancellation of it would (StreamClosedError, or its reader's own cancellation), and
    # the escape the cleanup ends in reaches the loop's exception handler once the end is published. An interrupt,
    # which every end raises, the read raises, within its reader's grace in place of its cancellation.
    loop = asyncio.get_running_loop()
    handled: list[object] = []
    loop.set_exception_handler(lambda _loop, context: handled.append((context.get("exception"), context.get("report"))))
    escape = _Escape("cleanup escaped") if ends == "escaped" else SystemExit("cleanup interrupt")
    body = _HonoursCancelLate()
    body.release.set()
    cleaned = asyncio.Event()
    runner = _runner_with_cleanup(body, cleaned, ends_in=escape)
    assert await anext(runner) == 1
    raised: list[str] = []
    reading = asyncio.ensure_future(_read_once(runner, raised))
    try:
        for _turn in range(20):
            await asyncio.sleep(0)
        closing = runner.aclose()
        closing.send(None)
        for _turn in range(20):
            await asyncio.sleep(0)
        if reader == "cancelled":
            reading.cancel()
            for _turn in range(20):
                await asyncio.sleep(0)
        assert not reading.done()
        closing.close()
        cleaned.set()
        await asyncio.wait((reading,), timeout=HANG_BOUND)
        for _turn in range(20):
            await asyncio.sleep(0)
        assert runner.report is not None
        if ends == "interrupted":
            assert (raised, handled) == (["SystemExit", "went on"], [])
        elif reader == "cancelled":
            assert reading.cancelled()
            assert (raised, handled) == ([], [(escape, runner.report)])
        else:
            assert (raised, handled) == (["StreamClosedError", "went on"], [(escape, runner.report)])
    finally:
        cleaned.set()
        await asyncio.wait((reading,), timeout=HANG_BOUND)
        loop.set_exception_handler(None)


async def _drive_until_it_waits(read: Coroutine[Any, Any, object]) -> None:
    """Drive a read coroutine by hand, as its task would, until what it awaits stays pending."""
    while True:
        awaited = read.send(None)
        for _turn in range(20):
            await asyncio.sleep(0)
        if isinstance(awaited, asyncio.Future) and not awaited.done():
            return


async def _close_its_claimant(runner: OperationRunner[int, KernelReport], left_by: str) -> None:
    """Claim ``runner``'s end in a coroutine and close that coroutine while the cleanup runs, as ``left_by`` names."""
    if left_by == "read":
        abandoned = runner.__anext__()
        await _drive_until_it_waits(abandoned)
        abandoned.close()
        return
    raised: list[str] = []
    reader: asyncio.Future[None] | None = None
    reading: Coroutine[Any, Any, int] | None = None
    if left_by == "close_its_reader_left":
        reader = asyncio.ensure_future(_read_once(runner, raised))
        for _turn in range(20):
            await asyncio.sleep(0)
    elif left_by == "close_its_reader_was_closed":
        reading = runner.__anext__()
        await _drive_until_it_waits(reading)
    closing = runner.aclose()
    closing.send(None)
    for _turn in range(20):
        await asyncio.sleep(0)
    if reading is not None:
        await _drive_until_it_waits(reading)
    # The read the close ended still waits for the end as the close's coroutine is closed, then leaves before the end
    # is published.
    closing.close()
    for _turn in range(20):
        await asyncio.sleep(0)
    if reader is not None:
        loop = asyncio.get_running_loop()
        reader.cancel()
        started = loop.time()
        await asyncio.wait((reader,), timeout=CANCEL_GRACE_SECONDS + 0.5)
        assert reader.cancelled()
        assert loop.time() - started >= CANCEL_GRACE_SECONDS - 0.05
        assert raised == []
    if reading is not None:
        reading.close()


@pytest.mark.parametrize("left_by", ["close", "close_its_reader_left", "close_its_reader_was_closed", "read"])
@pytest.mark.parametrize("ends", ["escaped", "interrupted"])
def test_an_end_whose_claimant_is_closed_hands_what_it_raises_to_the_event_loop(ends: str, left_by: str) -> None:
    # The coroutine that claimed the end is closed while its cleanup runs, as a loop that stops closes what is pending:
    # a close's (``close``); one whose ended read, still waiting for the end then, leaves before it is published, its
    # reader's grace run out (``close_its_reader_left``) or its own coroutine closed (``close_its_reader_was_closed``);
    # or a read's, closed in its pull (``read``). No read waits for the end as it is published, so nothing raises what
    # the cleanup ends in: the interrupt leaves asyncio.run, as one
    # between reads does, and any other escape reaches the loop's exception handler once the end is published, not
    # when the stream is collected.
    handled: list[object] = []
    escape = _Escape("cleanup escaped") if ends == "escaped" else SystemExit("cleanup interrupt")
    published: list[bool] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(
            lambda _loop, context: handled.append((context.get("exception"), context.get("report")))
        )
        body = _HonoursCancelLate()
        body.release.set()
        cleaned = asyncio.Event()
        runner = _runner_with_cleanup(body, cleaned, ends_in=escape)
        assert await anext(runner) == 1
        await _close_its_claimant(runner, left_by)
        assert runner.report is None
        cleaned.set()
        for _turn in range(20):
            await asyncio.sleep(0)
            published.append(runner.report is not None)
        assert published[-1]
        assert handled == [(escape, runner.report)]

    if ends == "interrupted":
        with pytest.raises(SystemExit, match="cleanup interrupt"):
            asyncio.run(main())
        assert handled == []
    else:
        asyncio.run(main())
    assert published


class _CountsCloses:
    """A runner body: one item, then a read that waits until it is cancelled; it counts its closes."""

    def __init__(self) -> None:
        self.taken = 0
        self.closes = 0

    def __aiter__(self) -> _CountsCloses:
        return self

    async def __anext__(self) -> int:
        self.taken += 1
        if self.taken == 1:
            return 1
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def aclose(self) -> None:
        self.closes += 1


@pytest.mark.asyncio
async def test_a_read_whose_coroutine_is_closed_while_another_tasks_close_cleans_up_leaves_the_end_to_that_close() -> (
    None
):
    # Another task's close ends a read in flight and its cleanup runs; then the read's coroutine is closed. The close
    # claimed the end, so it runs it alone: the cleanup and the source's close run once.
    cleanups: list[None] = []
    cleaned = asyncio.Event()

    async def cleanup() -> None:
        cleanups.append(None)
        await cleaned.wait()

    def finalize(cause: TerminalCause, reason: str | None, attempt: CleanupAttempt) -> KernelReport:
        state, terminal_reason = kernel_terminal(cause, reason)
        return with_cleanup_attempt(KernelReport(state=state, terminal_reason=terminal_reason), cause, attempt)

    body = _CountsCloses()
    runner = OperationRunner(
        body, LifecycleHooks(finalize=finalize, failure_report=failed_kernel_report, cleanup=cleanup)
    )
    assert await anext(runner) == 1
    reading = runner.__anext__()
    await _drive_until_it_waits(reading)
    closer = asyncio.ensure_future(runner.aclose())
    try:
        for _turn in range(20):
            await asyncio.sleep(0)
        assert cleanups == [None]
        reading.close()
        for _turn in range(20):
            await asyncio.sleep(0)
        cleaned.set()
        await asyncio.wait_for(closer, HANG_BOUND)
        assert runner.report is not None
        assert (len(cleanups), body.closes) == (1, 1)
    finally:
        cleaned.set()
        await asyncio.wait((closer,), timeout=HANG_BOUND)


@pytest.mark.asyncio
async def test_a_reader_nobody_cancelled_waits_for_another_tasks_close_to_its_report() -> None:
    # Another task closes the stream while a read waits; the pull ends at once in the close's cancellation, and cleanup
    # outlasts the grace. The reader, not cancelled, waits for the end to be published, however long the cleanup
    # takes, and raises StreamClosedError carrying the report.
    body = _HonoursCancelLate()
    body.release.set()
    cleaned = asyncio.Event()
    runner = _runner_with_cleanup(body, cleaned)
    loop = asyncio.get_running_loop()
    assert await anext(runner) == 1
    reader = asyncio.ensure_future(anext(runner))
    for _turn in range(20):
        await asyncio.sleep(0)
    closer = asyncio.ensure_future(runner.aclose())
    loop.call_later(CANCEL_GRACE_SECONDS + 0.2, cleaned.set)
    await asyncio.wait((reader,), timeout=HANG_BOUND)
    error = reader.exception()
    assert isinstance(error, StreamClosedError)
    assert cleaned.is_set()
    assert getattr(error, "report", None) is not None
    await asyncio.wait((closer,), timeout=HANG_BOUND)


class _AnswersCancelLate:
    """A runner body: one item, then a read that answers its cancellation late, setting ``returning`` as it answers.

    It answers once ``release`` is set, with an item or, when ``fails``, a failure; further cancellations meanwhile do
    not cut its answer short.
    """

    def __init__(self, *, fails: bool = False) -> None:
        self.fails = fails
        self.taken = 0
        self.cancelled = asyncio.Event()
        self.release = asyncio.Event()
        self.returning = asyncio.Event()

    def __aiter__(self) -> _AnswersCancelLate:
        return self

    async def __anext__(self) -> int:
        self.taken += 1
        if self.taken == 1:
            return 1
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            while not self.release.is_set():
                with contextlib.suppress(asyncio.CancelledError):
                    await self.release.wait()
            self.returning.set()
            if self.fails:
                raise ValueError("rolled back") from None
            return 2
        raise AssertionError("unreachable")


@pytest.mark.asyncio
async def test_a_second_cancellation_as_the_pull_ends_with_an_item_ends_the_read_within_the_first_ones_grace() -> None:
    # The reader's cancellation is handed to its pull, which answers it with an item halfway through the grace; a
    # second cancellation of the reader comes in the very turn the pull returns. It reaches no pull, which has ended:
    # the read raises it, as awaiting the ended pull would, instead of returning the item. The second shares the
    # first's grace, so the cleanup, longer than the grace, holds the reader until that grace ends, no longer.
    body = _AnswersCancelLate()
    cleaned = asyncio.Event()
    runner = _runner_with_cleanup(body, cleaned)
    assert await anext(runner) == 1
    reader = asyncio.ensure_future(anext(runner))
    for _turn in range(20):
        await asyncio.sleep(0)
    loop = asyncio.get_running_loop()
    started = loop.time()
    reader.cancel()
    await reached(body.cancelled)

    async def cancel_again() -> None:
        await body.returning.wait()
        reader.cancel()

    again = asyncio.ensure_future(cancel_again())
    loop.call_later(CANCEL_GRACE_SECONDS / 2, body.release.set)
    try:
        await asyncio.wait((reader, again), timeout=HANG_BOUND)
        took = loop.time() - started
    finally:
        cleaned.set()
    assert reader.cancelled()
    assert CANCEL_GRACE_SECONDS - 0.05 <= took < CANCEL_GRACE_SECONDS * 1.25


@pytest.mark.asyncio
async def test_a_pull_that_fails_after_a_second_cancellation_ends_the_read_in_its_failure() -> None:
    # The reader's cancellation is handed to its pull; a second one comes while the pull answers it, and the pull then
    # fails within the grace. The read still waits for the pull and ends as it ended, in its failure; the stream is
    # failed, and the cancellation is the reader's next await's.
    body = _AnswersCancelLate(fails=True)
    cleaned = asyncio.Event()
    cleaned.set()
    runner = _runner_with_cleanup(body, cleaned)
    assert await anext(runner) == 1
    seen: list[str] = []

    async def read() -> None:
        try:
            await anext(runner)
        except ValueError:
            seen.append("failure")
            await asyncio.sleep(0)
            seen.append("next await not cancelled")

    reader = asyncio.ensure_future(read())
    for _turn in range(20):
        await asyncio.sleep(0)
    reader.cancel()
    await reached(body.cancelled)
    reader.cancel()
    for _turn in range(5):
        await asyncio.sleep(0)
    body.release.set()
    await asyncio.wait((reader,), timeout=HANG_BOUND)
    assert seen == ["failure"]
    assert reader.cancelled()
    report = runner.report
    assert report is not None
    assert report.state is KernelState.FAILED


@pytest.mark.asyncio
async def test_a_read_that_fails_on_its_own_keeps_a_cancellation_its_cleanup_overrun_met_as_a_violation() -> None:
    # A read fails on its own, ending the stream failed; the reader is cancelled while the cleanup runs, which outlasts
    # the grace. Behind the failure, both the overrun and the cancellation are recorded, as secondary violations.
    cleaned = asyncio.Event()
    runner = _runner_with_cleanup(_FailsAtOnce(), cleaned)
    assert await anext(runner) == 1
    reader = asyncio.ensure_future(anext(runner))
    for _turn in range(20):
        await asyncio.sleep(0)
    reader.cancel()
    try:
        await asyncio.wait((reader,), timeout=HANG_BOUND)
    finally:
        cleaned.set()
    assert isinstance(reader.exception(), ValueError)
    report = runner.report
    assert report is not None
    assert report.state is KernelState.FAILED
    messages = [violation.message for violation in report.violations]
    assert any("CleanupOverrunError" in message for message in messages)
    assert any("CancelledError" in message for message in messages)


class _FailsAtOnce:
    """A runner body: one item, then a read that fails on its own."""

    def __init__(self) -> None:
        self.taken = 0

    def __aiter__(self) -> _FailsAtOnce:
        return self

    async def __anext__(self) -> int:
        self.taken += 1
        if self.taken == 1:
            return 1
        raise ValueError("read failed")


@pytest.mark.asyncio
async def test_a_close_around_the_read_that_exhausts_the_stream_never_reports_it_failed() -> None:
    # Another task closes the stream at every loop turn of the read that exhausts it. Exhaustion is no failure: the
    # close ends the stream early, or the read ends it exhausted, never failed.
    async def one(turns: int) -> TerminalState:
        async with client_for(ResponderTransport(_page)) as client:
            stream = client.iter_list(_listing(), page_size=PAGE)
            for _row in range(ROWS):
                await anext(stream)
            reader = asyncio.ensure_future(anext(stream))
            for _turn in range(turns):
                await asyncio.sleep(0)
            closer = asyncio.ensure_future(stream.aclose())
            await asyncio.wait((reader, closer), timeout=HANG_BOUND)
            report = stream.report
            assert report is not None
            return report.state

    states = {await one(turns) for turns in range(16)}
    assert TerminalState.FAILED not in states
    assert TerminalState.EARLY_CLOSED in states


@pytest.mark.asyncio
async def test_a_new_cancellation_in_an_exit_whose_replay_was_taken_back_gets_a_grace_of_its_own() -> None:
    # A timeout fires while the body's stream exit closes a source that outlasts the grace; the exit replays it behind
    # the body's exception, and the timeout takes it back. The exception then unwinds the client's exit, whose close
    # waits for that source's cleanup; a new cancellation there waits for it, not for the spent grace.
    loop = asyncio.get_running_loop()
    source = _StuckSource(ready=3)
    client = client_for(ResponderTransport(_page), policy=ExecutionPolicy(max_elapsed=3600.0))
    stream = client.batch_outcomes(source, batch_size=1)

    async def expire(scope: asyncio.Timeout) -> None:
        await source.closing.wait()
        scope.reschedule(loop.time())

    async def scenario() -> None:
        async with client:
            async with asyncio.timeout(None) as scope:
                expiry = asyncio.ensure_future(expire(scope))
                async with stream:
                    await anext(stream)
                    raise ValueError("body failed")
        await expiry

    task = asyncio.ensure_future(scenario())
    try:
        await reached(source.closing)
        # The stream's exit gives up on the cleanup after the grace; the client's exit then waits for it.
        await asyncio.sleep(CANCEL_GRACE_SECONDS + 0.2)
        assert not task.done()
        task.cancel()
        loop.call_later(CANCEL_GRACE_SECONDS / 2, source.release.set)
        await asyncio.wait((task,), timeout=HANG_BOUND)
        assert source.closed.is_set()
    finally:
        source.release.set()
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch", "fan-out"])
async def test_a_close_cancelled_while_its_source_close_fails_raises_the_failure_then_the_cancellation(
    family: str,
) -> None:
    # aclose(), which nobody cancelled, closes a source whose close then fails; the closing task is cancelled while it
    # waits. The close raises the source's failure, and the cancellation is delivered at the task's next await.
    closing = asyncio.Event()
    release = asyncio.Event()

    async def commands() -> AsyncIterator[Command[int]]:
        try:
            for index in itertools.count():
                yield _command(index)
        finally:
            closing.set()
            await release.wait()
            raise ValueError("the source's close failed")

    raised: list[BaseException] = []
    async with client_for(ResponderTransport(_page)) as client:
        stream = (
            client.batch_outcomes(commands(), batch_size=1)
            if family == "batch"
            else client.fan_out_outcomes(commands(), dispatch=DirectDispatch(concurrency=1))
        )
        await anext(stream)

        async def close() -> None:
            try:
                await stream.aclose()
            except InputSourceError as error:
                raised.append(error)
            await asyncio.sleep(HANG_BOUND)

        task = asyncio.ensure_future(close())
        await reached(closing)
        task.cancel()
        asyncio.get_running_loop().call_soon(release.set)
        await asyncio.wait((task,), timeout=HANG_BOUND)
    assert task.cancelled()
    assert [type(error) for error in raised] == [InputSourceError]


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch", "fan-out"])
@pytest.mark.parametrize("cancellations", [1, 2])
async def test_a_cancelled_body_whose_close_fails_gets_each_cancellation_once(family: str, cancellations: int) -> None:
    # A cancelled body's exit closes a source whose close fails within the grace. The body's cancellation leaves the
    # exit with the failed close noted on it, and is not replayed behind it: its handler can await. A second
    # cancellation that came while the close ran is delivered once too, at that await.
    closing = asyncio.Event()
    release = asyncio.Event()

    async def commands() -> AsyncIterator[Command[int]]:
        try:
            for index in itertools.count():
                yield _command(index)
        finally:
            closing.set()
            while not release.is_set():
                with contextlib.suppress(asyncio.CancelledError):
                    await release.wait()
            raise ValueError("the source's close failed")

    entered = asyncio.Event()
    seen: list[str] = []
    async with client_for(ResponderTransport(_page)) as client:
        stream = (
            client.batch_outcomes(commands(), batch_size=1)
            if family == "batch"
            else client.fan_out_outcomes(commands(), dispatch=DirectDispatch(concurrency=1))
        )

        async def body() -> None:
            try:
                async with stream:
                    await anext(stream)
                    entered.set()
                    await asyncio.Event().wait()
            except asyncio.CancelledError as cancellation:
                seen.extend(getattr(cancellation, "__notes__", ()))
                try:
                    await asyncio.sleep(0)
                except asyncio.CancelledError:
                    seen.append("handler's await cancelled")
                    raise
                seen.append("handler awaited")
                raise

        task = asyncio.ensure_future(body())
        await reached(entered)
        task.cancel()
        await reached(closing)
        if cancellations == len(("first", "second")):
            task.cancel()
        for _turn in range(5):
            await asyncio.sleep(0)
        release.set()
        await asyncio.wait((task,), timeout=HANG_BOUND)
    assert task.cancelled()
    handler = "handler awaited" if cancellations == 1 else "handler's await cancelled"
    assert seen == ["stream cleanup failed (InputSourceError)", handler]


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch", "fan-out"])
@pytest.mark.parametrize("closer", ["async with", "aclose"])
async def test_a_cancelled_close_its_cleanup_overran_raises_the_cancellation_alone_and_reports_the_early_close(
    family: str, closer: str
) -> None:
    # A cancelled close, the `async with` exit of a cancelled body or a cancelled aclose(), closes a source whose
    # close outlasts the grace. The close raises the cancellation with nothing noted on it; only the report records
    # the overrun, and it is the early close's, with its reason, though the source never recorded its end.
    release = asyncio.Event()

    async def commands() -> AsyncIterator[Command[int]]:
        try:
            for index in itertools.count():
                yield _command(index)
        finally:
            await release.wait()

    entered = asyncio.Event()
    notes: list[object] = []
    async with client_for(ResponderTransport(_page)) as client:
        stream = (
            client.batch_outcomes(commands(), batch_size=1)
            if family == "batch"
            else client.fan_out_outcomes(commands(), dispatch=DirectDispatch(concurrency=1))
        )

        async def body() -> None:
            try:
                if closer == "async with":
                    async with stream:
                        await anext(stream)
                        entered.set()
                        await asyncio.Event().wait()
                else:
                    await anext(stream)
                    entered.set()
                    await stream.aclose()
            except asyncio.CancelledError as cancellation:
                notes.append(getattr(cancellation, "__notes__", None))
                raise

        task = asyncio.ensure_future(body())
        await reached(entered)
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.wait((task,), timeout=HANG_BOUND)
        release.set()
    assert task.cancelled()
    assert notes == [None]
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.EARLY_CLOSED
    assert report.terminal_reason == "stream closed before exhaustion"
    assert [violation.message for violation in report.violations] == [
        "stream cleanup also failed (CleanupOverrunError)"
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch", "fan-out"])
@pytest.mark.parametrize("exits", ["stream and client", "client alone"])
async def test_a_cancelled_client_exit_whose_close_overran_raises_the_cancellation_alone(
    family: str, exits: str
) -> None:
    # A cancelled body's `async with client` exit closes a started stream, inside its own `async with` or not, whose
    # source's close outlasts the grace. The client's close raises nothing of its own: the body's cancellation leaves
    # with nothing noted on it, and only the report records the overrun.
    release = asyncio.Event()

    async def commands() -> AsyncIterator[Command[int]]:
        try:
            for index in itertools.count():
                yield _command(index)
        finally:
            await release.wait()

    entered = asyncio.Event()
    notes: list[object] = []

    async def body() -> None:
        try:
            async with client_for(ResponderTransport(_page)) as client:
                stream = (
                    client.batch_outcomes(commands(), batch_size=1)
                    if family == "batch"
                    else client.fan_out_outcomes(commands(), dispatch=DirectDispatch(concurrency=1))
                )
                async with stream if exits == "stream and client" else contextlib.nullcontext(stream):
                    await anext(stream)
                    entered.set()
                    await asyncio.Event().wait()
        except asyncio.CancelledError as cancellation:
            notes.append(getattr(cancellation, "__notes__", None))
            raise

    task = asyncio.ensure_future(body())
    await reached(entered)
    await asyncio.sleep(0)
    task.cancel()
    try:
        await asyncio.wait((task,), timeout=HANG_BOUND)
    finally:
        release.set()
    assert task.cancelled()
    assert notes == [None]


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch", "batch_outcomes", "fan_out_outcomes", "iter_list"])
@pytest.mark.parametrize("closer", ["aclose", "async with"])
async def test_a_stream_closed_before_its_first_read_reports_the_early_close_reason(family: str, closer: str) -> None:
    async def commands() -> AsyncIterator[Command[int]]:
        for index in range(3):
            yield _command(index)

    async with client_for(ResponderTransport(_page)) as client:
        stream: Any
        if family == "batch":
            stream = client.batch(commands(), batch_size=1)
        elif family == "batch_outcomes":
            stream = client.batch_outcomes(commands(), batch_size=1)
        elif family == "fan_out_outcomes":
            stream = client.fan_out_outcomes(commands(), dispatch=DirectDispatch(concurrency=1))
        else:
            stream = client.iter_list(_listing(), page_size=PAGE)
        if closer == "aclose":
            await stream.aclose()
        else:
            async with stream:
                pass
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.EARLY_CLOSED
    assert report.terminal_reason == "stream closed before exhaustion"


@pytest.mark.asyncio
async def test_a_second_cancellation_during_the_grace_ends_with_the_first() -> None:
    # A cancelled body's exits close a source whose close outlasts the grace; a second cancellation comes halfway
    # through. The first cancellation's grace bounds both: the task ends one grace after the first.
    source, client, stream = _stuck("batch", ready=3)
    loop = asyncio.get_running_loop()

    async def scenario() -> None:
        async with client, stream:
            await anext(stream)
            await asyncio.Event().wait()

    task = asyncio.ensure_future(scenario())
    for _turn in range(50):
        await asyncio.sleep(0)
    started = loop.time()
    task.cancel()
    await reached(source.closing)
    await asyncio.sleep(CANCEL_GRACE_SECONDS / 2)
    task.cancel()
    try:
        await asyncio.wait((task,), timeout=HANG_BOUND)
        assert task.cancelled()
        assert loop.time() - started < CANCEL_GRACE_SECONDS + 0.4
    finally:
        source.release.set()
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("second", ["stream", "client"])
async def test_a_close_waiting_for_the_cleanup_a_cancelled_close_left_running_raises_its_own_cancellation(
    second: str,
) -> None:
    # A cancelled close leaves the source's close running past the grace. A later close waits for that cleanup; its
    # own task is cancelled there: it waits only the grace, then raises the cancellation, never returning.
    source, client, stream = _stuck("batch", ready=1)
    await anext(stream)
    first = asyncio.ensure_future(stream.aclose())
    await reached(source.closing)
    await _cancelled_within_the_grace(first)
    later = asyncio.ensure_future(stream.aclose() if second == "stream" else client.aclose())
    for _turn in range(20):
        await asyncio.sleep(0)
    assert not later.done()
    try:
        await _cancelled_within_the_grace(later)
    finally:
        source.release.set()
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch", "fan-out"])
async def test_a_reader_cancelled_while_its_next_read_waits_for_another_tasks_close_raises_its_cancellation(
    family: str,
) -> None:
    # The reader holds an item while another task closes the stream, whose source's close waits for the test. The
    # reader reads again: the read waits for that close's end, and the reader's own cancellation there is raised.
    source, client, stream = _stuck(family, ready=3)
    await anext(stream)
    closer = asyncio.ensure_future(stream.aclose())
    await reached(source.closing)
    reader = asyncio.ensure_future(anext(stream))
    for _turn in range(20):
        await asyncio.sleep(0)
    assert not reader.done()
    reader.cancel()
    source.release.set()
    await asyncio.wait((reader,), timeout=HANG_BOUND)
    assert reader.cancelled()
    await asyncio.wait_for(closer, timeout=HANG_BOUND)
    await client.aclose()


class _ClosesSlowly:
    """An endless command source whose close waits for the test."""

    def __init__(self) -> None:
        self.closing = asyncio.Event()
        self.release = asyncio.Event()

    async def commands(self) -> AsyncIterator[Command[int]]:
        try:
            for index in itertools.count():
                yield _command(index)
        finally:
            self.closing.set()
            await self.release.wait()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch", "fan-out"])
async def test_a_reader_cancelled_while_it_waits_for_another_tasks_close_raises_its_cancellation_whatever_turn(
    family: str,
) -> None:
    # Another task closes the stream at every loop turn of a run of reads, the turns where a read's item is made but
    # its reader has not resumed yet included; the reader's own task is cancelled while it waits for that close's end.
    # Whenever the cancellation is taken, the reader's task ends cancelled, never with ``StreamClosedError``.
    lost: list[int] = []
    for turns in range(_HANDOFF_TURNS):
        source = _ClosesSlowly()
        async with client_for(ResponderTransport(_page), policy=ExecutionPolicy(max_elapsed=60.0)) as client:
            stream = (
                client.batch_outcomes(source.commands(), batch_size=1)
                if family == "batch"
                else client.fan_out_outcomes(source.commands())
            )

            async def read(stream: OperationStream[Any] = stream) -> None:
                while True:
                    await anext(stream)

            reader = asyncio.ensure_future(read())
            for _turn in range(turns):
                await asyncio.sleep(0)
            closer = asyncio.ensure_future(stream.aclose())
            # A close before the first read has no source's close to wait for.
            closing = asyncio.ensure_future(source.closing.wait())
            await asyncio.wait((closer, closing), timeout=HANG_BOUND, return_when=asyncio.FIRST_COMPLETED)
            closing.cancel()
            for _turn in range(20):
                await asyncio.sleep(0)
            taken = reader.cancel()
            source.release.set()
            await asyncio.wait((reader,), timeout=HANG_BOUND)
            if taken and not reader.cancelled():
                lost.append(turns)
            await asyncio.wait_for(closer, timeout=HANG_BOUND)
    assert lost == []


@pytest.mark.asyncio
async def test_a_stream_leaves_its_client_once_the_cleanup_its_cancelled_close_left_running_ends() -> None:
    source, client, stream = _stuck("batch", ready=1)
    await anext(stream)
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.05):
            await stream.aclose()
    # The source's close is the last of the cleanup left running, which ends a few loop turns after it.
    source.release.set()
    await asyncio.wait_for(source.closed.wait(), timeout=HANG_BOUND)
    for _turn in range(20):
        await asyncio.sleep(0)
    released = weakref.ref(stream)
    del stream
    gc.collect()

    assert released() is None
    await client.aclose()


def test_ctrl_c_while_a_stream_closes_on_exit_ends_the_run_with_keyboard_interrupt_after_the_grace() -> None:
    # asyncio.run turns Ctrl-C into a cancellation of its main task. The close defers it only for the grace, and the
    # cancellation, not the overrun, leaves, so the run ends with the interrupt instead of a cleanup error.
    source = _SlowClose()

    async def main() -> None:
        client = client_for(ResponderTransport(_page))
        stream = client.batch_outcomes(source.commands(), batch_size=1)
        loop = asyncio.get_running_loop()
        # Bounds a close that waited for the cleanup instead of the grace.
        loop.call_later(3.0, source.release.set)
        async with stream:
            await anext(stream)
            loop.call_later(0.05, signal.raise_signal, signal.SIGINT)
        await client.aclose()

    # A process started in the background ignores SIGINT; asyncio.run handles it only under the default handler.
    previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    started = time.monotonic()
    try:
        with pytest.raises(KeyboardInterrupt):
            asyncio.run(main())
    finally:
        signal.signal(signal.SIGINT, previous)
    waited = time.monotonic() - started
    assert CANCEL_GRACE_SECONDS <= waited < CANCEL_GRACE_SECONDS + 1.0, waited


def test_ctrl_c_in_the_body_ends_the_run_after_one_grace_for_the_stream_and_the_client_together() -> None:
    # Ctrl-C lands while the body processes an item, not in a read or a close: each exit closes for a cancelled body,
    # so it waits only the grace, and the stream's exit and then the client's defer the one cancellation for one grace
    # together, not one each and not for the operation's time left.
    source = _SlowClose()

    async def main() -> None:
        loop = asyncio.get_running_loop()
        # Bounds a close that waited for the cleanup instead of the grace.
        loop.call_later(4.0, source.release.set)
        async with client_for(ResponderTransport(_page), policy=ExecutionPolicy(max_elapsed=3600.0)) as client:
            stream = client.batch_outcomes(source.commands(), batch_size=1)
            async with stream:
                async for _outcome in stream:
                    loop.call_later(0.05, signal.raise_signal, signal.SIGINT)
                    await asyncio.sleep(HANG_BOUND)

    previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    started = time.monotonic()
    try:
        with pytest.raises(KeyboardInterrupt):
            asyncio.run(main())
    finally:
        signal.signal(signal.SIGINT, previous)
    waited = time.monotonic() - started
    assert CANCEL_GRACE_SECONDS <= waited < CANCEL_GRACE_SECONDS + 1.0, waited


# a cancelled reader waits for work that ignores its cancellation only the grace, not the operation budget.


@pytest.mark.asyncio
async def test_a_cancelled_reader_of_a_blocked_synchronous_source_returns_after_the_grace() -> None:
    release = threading.Event()

    def commands() -> Iterator[Command[int]]:
        yield _command(0)
        release.wait(HANG_BOUND)  # a blocking queue.get() waiting for the next unit of work
        yield _command(1)

    loop = asyncio.get_running_loop()
    client = client_for(ResponderTransport(_page), policy=ExecutionPolicy(max_elapsed=3600.0))
    stream = client.batch_outcomes(commands(), batch_size=1)
    first = asyncio.Event()

    async def read() -> None:
        async with stream:
            async for _outcome in stream:
                first.set()

    reader = asyncio.create_task(read())
    try:
        await reached(first)
        await asyncio.sleep(0)
        cancelled_at = loop.time()
        reader.cancel()
        await asyncio.wait((reader,), timeout=HANG_BOUND)
        waited = loop.time() - cancelled_at
    finally:
        release.set()
    assert reader.cancelled()
    assert waited < CANCEL_GRACE_SECONDS + 1.0, waited
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.CANCELLED
    # The client's close waits for the cleanup left running; once it ran, the stream is still the one that ended.
    await client.aclose()
    assert stream.report is report
    with pytest.raises(RuntimeError, match="already terminated"):
        async with stream:
            pass


# a cancellation nobody requested (raised by a transport or caller code on its own) ends what it reached.


async def _foreign(request: Request) -> object:
    if request.method == "batch" or request.parameters.get("PARENT") == 1 or request.parameters.get("id") == 1:
        raise asyncio.CancelledError("a shared future the transport awaited was cancelled")
    return _page(request)


@pytest.mark.asyncio
async def test_a_transport_raising_a_cancellation_nobody_requested_fails_the_call_as_a_transport_failure() -> None:
    async with client_for(ResponderTransport(_foreign)) as client:
        with pytest.raises(TransportError):
            await client.call(_command(1).request)
        task = asyncio.current_task()
        assert task is not None
        assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_a_cancellation_of_the_sending_task_leaves_the_transport_unchanged() -> None:
    blocker = Blocker()
    async with client_for(ResponderTransport(blocker)) as client:
        call = asyncio.create_task(client.call(_command(0).request))
        await reached(blocker.started)
        call.cancel()
        with pytest.raises(asyncio.CancelledError) as raised:
            await call
        # The caller's own cancellation, never a failure of the send, nor its own cause.
        assert raised.value.__cause__ is not raised.value
        assert blocker.cancelled.is_set()


def _open_foreign(client: Bitrix24, family: str) -> OperationStream[Any]:
    traversal = SequentialTraversal(page_size=PAGE)
    dispatch = BatchDispatch(batch_size=2, coalesce_wait=0.0) if family.endswith("batch") else DirectDispatch()
    if family.startswith("references"):
        return client.iter_reference_outcomes(
            _listing(), [_binding(index) for index in range(3)], traversal=traversal, dispatch=dispatch
        )
    return client.fan_out_outcomes([_command(index) for index in range(3)], dispatch=dispatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["references_direct", "references_batch", "fan_out_direct", "fan_out_batch"])
async def test_a_cancellation_nobody_requested_gives_what_it_reached_an_outcome_and_the_stream_ends(
    family: str,
) -> None:
    async with client_for(ResponderTransport(_foreign)) as client:
        stream = _open_foreign(client, family)
        async with asyncio.timeout(HANG_BOUND):
            outcomes = await _drain(stream)
        assert len(outcomes) >= 1
        report = stream.report
        assert report is not None
        assert report.state is TerminalState.COMPLETED_WITH_FAILURES


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["references_direct", "references_batch", "fan_out_direct", "fan_out_batch"])
async def test_the_reference_scheduler_and_dispatcher_end_every_unit_a_cancellation_reaches_them_on_its_own(
    family: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Behind the transport boundary too: a cancellation that reaches a binding or a batch sender while its operation
    # runs and nobody cancelled it still ends that unit with an outcome, so the reader never waits for it.
    monkeypatch.setattr(boundary, "_cancellation_is_foreign", lambda _mark: False)
    async with client_for(ResponderTransport(_foreign)) as client:
        stream = _open_foreign(client, family)
        async with asyncio.timeout(HANG_BOUND):
            outcomes = await _drain(stream)
        report = stream.report
        assert report is not None
        assert report.state is TerminalState.COMPLETED_WITH_FAILURES
        assert len(outcomes) >= 1


@pytest.mark.asyncio
async def test_a_page_stop_raising_a_cancellation_on_its_own_fails_its_binding_and_the_stream_ends() -> None:
    class _Stop:
        async def on_page(self, boundary: PageBoundary) -> ContinuePage:
            if boundary.binding_id == 1:
                raise asyncio.CancelledError("caller code awaited a future someone else cancelled")
            return ContinuePage.CONTINUE

    async with client_for(ResponderTransport(_page)) as client:
        stream = client.iter_reference_outcomes(
            _listing(),
            [_binding(index) for index in range(3)],
            traversal=SequentialTraversal(page_size=PAGE),
            dispatch=DirectDispatch(concurrency=2),
            page_stop=_Stop(),
        )
        async with asyncio.timeout(HANG_BOUND):
            outcomes = await _drain(stream)
        completed = {outcome.correlation for outcome in outcomes if isinstance(outcome, ReferenceComplete)}
        assert completed == {0, 2}
        report = stream.report
        assert report is not None
        assert report.state is TerminalState.COMPLETED_WITH_FAILURES


@pytest.mark.asyncio
async def test_a_binding_whose_end_fails_still_publishes_its_terminal_event(monkeypatch: pytest.MonkeyPatch) -> None:
    # A defect while a binding ends must not leave the consumer waiting for an event that never comes: the run
    # publishes the terminal event on its way out, and the operation fails with the defect instead of hanging.
    class _Stop:
        async def on_page(self, boundary: PageBoundary) -> ContinuePage:
            if boundary.binding_id == 1:
                raise ValueError("page stop failed")
            return ContinuePage.CONTINUE

    def defect(error: BaseException) -> object:
        raise RuntimeError("ending the binding failed") from error

    monkeypatch.setattr(scheduler, "_failed_page", defect)
    async with client_for(ResponderTransport(_page)) as client:
        stream = client.iter_reference_outcomes(
            _listing(),
            [_binding(index) for index in range(3)],
            traversal=SequentialTraversal(page_size=PAGE),
            dispatch=DirectDispatch(concurrency=2),
            page_stop=_Stop(),
        )
        async with asyncio.timeout(HANG_BOUND):
            with pytest.raises(RuntimeError, match="ending the binding failed"):
                await _drain(stream)
        report = stream.report
        assert report is not None
        assert report.state is TerminalState.FAILED


@pytest.mark.asyncio
@pytest.mark.parametrize("exit_", ["stream", "client"])
async def test_an_exit_whose_task_nobody_cancelled_waits_for_its_close(exit_: str) -> None:
    # The body ends with a CancelledError its task was never asked for: the one an earlier cancelled task ended
    # with, which awaiting that task hands over. The exit is nobody's cancellation: it waits for its close to clean
    # up, and no grace the earlier task's exit spent applies to it.
    release = asyncio.Event()
    closed: list[str] = []

    def commands(name: str, *, slow: bool) -> AsyncIterator[Command[int]]:
        async def source() -> AsyncIterator[Command[int]]:
            try:
                for index in itertools.count():
                    yield _command(index)
            finally:
                if slow:
                    await release.wait()
                closed.append(name)

        return source()

    async with client_for(ResponderTransport(_page)) as client:
        entered = asyncio.Event()

        async def first() -> None:
            stream = client.batch_outcomes(commands("first", slow=False), batch_size=1)
            async with stream:
                await anext(stream)
                entered.set()
                await asyncio.Event().wait()

        cancelled = asyncio.ensure_future(first())
        await entered.wait()
        cancelled.cancel()
        await asyncio.wait((cancelled,))
        await asyncio.sleep(CANCEL_GRACE_SECONDS + 0.2)  # the first exit's grace, had it any, is long over

        async def second() -> None:
            if exit_ == "stream":
                stream = client.batch_outcomes(commands("second", slow=True), batch_size=1)
                async with stream:
                    await anext(stream)
                    await cancelled
            else:
                async with client_for(ResponderTransport(_page)) as own:
                    await anext(own.batch_outcomes(commands("second", slow=True), batch_size=1))
                    await cancelled

        waiter = asyncio.ensure_future(second())
        try:
            await asyncio.sleep(0.3)
            assert not waiter.done(), "the exit stopped waiting for its close"
        finally:
            release.set()
            await asyncio.wait((waiter,), timeout=HANG_BOUND)
        assert closed == ["first", "second"]
        assert waiter.cancelled() or isinstance(waiter.exception(), asyncio.CancelledError)


# the operation's page budget ends the operation, as its request budget does.


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", [ExecutionPolicy(max_pages=4), ExecutionPolicy(max_requests=4)])
async def test_an_operation_budget_ends_the_operation_without_draining_the_source(policy: ExecutionPolicy) -> None:
    pulled: list[int] = []

    async def bindings() -> AsyncIterator[Binding[int]]:
        for index in itertools.count():
            pulled.append(index)
            yield _binding(index)

    async with client_for(ResponderTransport(_page), policy=policy) as client:
        stream = client.iter_reference_outcomes(
            _listing(), bindings(), traversal=SequentialTraversal(page_size=PAGE), dispatch=DirectDispatch()
        )
        async with asyncio.timeout(HANG_BOUND):
            with pytest.raises(BudgetExceededError) as raised:
                await _drain(stream)
        report = stream.report
        assert report is not None
        assert report.state is TerminalState.INCOMPLETE
        assert raised.value.report is report  # type: ignore[attr-defined]
        assert len(pulled) < UNDRAINED


_KEYSET_IDS = tuple(range(1, 31))


def _keyset_page(_method: str, query: Query) -> Envelope:
    """Answer a keyset page over identities 1..30 by its ``ID`` fences, order and limit."""
    selected = _KEYSET_IDS
    if "filter[>ID]" in query:
        selected = tuple(value for value in selected if value > int(query["filter[>ID]"]))
    if "filter[<ID]" in query:
        selected = tuple(value for value in selected if value < int(query["filter[<ID]"]))
    ordered = sorted(selected, reverse=query.get("order[id]") == "DESC")
    return {"result": [{"id": value} for value in ordered[: int(query.get("limit", PAGE))]]}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "execution",
    [
        RangeKeysetExecution(StableIntegerKeysetContract()),
        PartitionedKeysetExecution(StableIntegerKeysetContract()),
        AutoKeysetExecution(StableIntegerKeysetContract()),
    ],
    ids=["range", "partitioned", "auto"],
)
async def test_the_page_budget_ends_a_keyset_reference_operation_without_draining_the_source(
    execution: KeysetExecution,
) -> None:
    # Keyset traversal reserves its page waves at once: a wave the committed pages leave no room for is the
    # operation's page budget refusing work, which ends the operation as a spent budget does.
    pulled: list[int] = []

    async def bindings() -> AsyncIterator[Binding[int]]:
        for index in itertools.count():
            pulled.append(index)
            yield _binding(index)

    traversal = KeysetTraversal(
        selector=ResultSelector.root(),
        identity=IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER),
        page_size=PAGE,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=execution,
    )
    async with client_for(BatchPortal(_keyset_page), policy=ExecutionPolicy(max_pages=6)) as client:
        stream = client.iter_reference_outcomes(
            _listing(), bindings(), traversal=traversal, dispatch=BatchDispatch(batch_size=5)
        )
        async with asyncio.timeout(HANG_BOUND):
            with pytest.raises(BudgetExceededError) as raised:
                await _drain(stream)
        report = stream.report
        assert report is not None
        assert report.state is TerminalState.INCOMPLETE
        assert raised.value.report is report  # type: ignore[attr-defined]
        assert len(pulled) < UNDRAINED


# a client serves the one event loop it was first used in.


_SECOND_LOOP_ENTRIES: dict[str, Callable[[Bitrix24, AsyncIterator[Any]], Awaitable[object]]] = {
    "call": lambda client, _source: client.call(_command(0).request),
    "iter_list": lambda client, _source: _drain(client.iter_list(_listing(), page_size=PAGE)),
    "batch": lambda client, source: _drain(client.batch(source)),
    "batch_outcomes": lambda client, source: _drain(client.batch_outcomes(source)),
    "fan_out": lambda client, source: _drain(client.fan_out(source)),
    "fan_out_outcomes": lambda client, source: _drain(client.fan_out_outcomes(source)),
    "iter_reference_outcomes": lambda client, source: _drain(
        client.iter_reference_outcomes(_listing(), source, traversal=SequentialTraversal(page_size=PAGE))
    ),
    "aclose": lambda client, _source: client.aclose(),
}


@pytest.mark.parametrize("entry", _SECOND_LOOP_ENTRIES)
def test_a_client_used_from_a_second_event_loop_refuses_at_once(entry: str) -> None:
    # Every operation, and the close, refuses a second loop before it does any work: it reads nothing from its source
    # and sends nothing, and the client stays open: a later call refuses the loop, not a closed client.
    transport = ResponderTransport(_page)
    client = client_for(transport)
    assert asyncio.run(client.call(_command(0).request)) == 1
    sent = len(transport.requests)
    pulled: list[int] = []

    async def source() -> AsyncIterator[Any]:
        for index in itertools.count():
            pulled.append(index)
            yield _binding(index) if entry == "iter_reference_outcomes" else _command(index)

    async def second() -> None:
        async with asyncio.timeout(HANG_BOUND):
            await _SECOND_LOOP_ENTRIES[entry](client, source())

    with pytest.raises(RuntimeError, match=FOREIGN_LOOP.split(";", 1)[0]):
        asyncio.run(second())
    assert pulled == []
    assert len(transport.requests) == sent
    with pytest.raises(RuntimeError, match=FOREIGN_LOOP.split(";", 1)[0]):
        asyncio.run(client.call(_command(0).request))


def test_a_client_no_work_used_closes_in_any_loop() -> None:
    client = client_for(ResponderTransport(_page))
    asyncio.run(client.aclose())
    with pytest.raises(ClientClosedError):
        asyncio.run(client.call(_command(0).request))


# a loop that shuts down with a stream nobody closed ends it: the bindings take their own task's cancellation.


def _endless(_method: str, query: Query) -> Envelope:
    """Answer every offset page, direct or a batch command, with two rows and a next page."""
    start = int(query.get("start", "0"))
    return {"result": [{"id": start + 1}, {"id": start + 2}], "next": start + PAGE}


@pytest.mark.parametrize("dispatch", [BatchDispatch(batch_size=10), DirectDispatch(concurrency=4)], ids=repr)
def test_a_loop_shutting_down_with_a_reference_stream_nobody_closed_ends(dispatch: object) -> None:
    # asyncio.run cancels the tasks its main task left. A binding blocked on a full queue nobody reads takes that
    # cancellation as its own stop and ends, rather than failing with it and waiting to publish the failure.
    async def main() -> None:
        client = client_for(BatchPortal(_endless), policy=ExecutionPolicy(max_active_references=4))
        stream = client.iter_reference_outcomes(
            _listing(),
            [_binding(index) for index in range(40)],
            traversal=SequentialTraversal(page_size=PAGE),
            dispatch=dispatch,  # type: ignore[arg-type]
        )
        await anext(stream)
        for _ in range(200):
            await asyncio.sleep(0)

    run = threading.Thread(target=asyncio.run, args=(main(),), daemon=True)
    run.start()
    run.join(HANG_BOUND)
    assert not run.is_alive()
