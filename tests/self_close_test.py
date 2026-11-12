"""A close requested from inside a stream's own pull or cleanup is refused instead of waiting for itself (R2-B9).

Caller code runs inside a pull or a cleanup: a page-stop callback, an async command, binding or
reference source (its ``finally`` too), an injected transport. Closing that stream, or the client
that owns it, from there used to wait for the very work that ran the close, and the cleanup shield
absorbed every cancellation, so the program hung for good. Each scenario runs on its own event loop
in a worker thread joined with a limit, so a regression fails the test instead of hanging the run.
"""

from __future__ import annotations
import asyncio
import contextvars
import gc
import threading
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
    Settings,
    TerminalState,
)
from b24api.contracts import Command, ContinuePage, PageBoundary, ReferenceSource
from b24api.errors import BatchFailed, BudgetExceededError, InputSourceError, ReferenceFailed, TransportError
from tests.scripting import HOST, ResponderTransport

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine

    from b24api.contracts.stream import OperationStream

# Scenarios are event-driven and end in well under a second, the deadline one within its 3 s budget;
# only a deadlock reaches the limit.
HANG_LIMIT = 10.0
ROWS = (1, 2, 3, 4, 5, 6)
PATHS = ("page_stop", "reference_page_stop", "binding_source", "reference_sources", "batch", "fan_out", "transport")
_REFUSALS = {
    "stream": "a stream cannot be closed from inside its own pull or cleanup",
    "client": "client cannot be closed from inside the pull or cleanup of one of its streams",
}
# Each family wraps the refusal as it wraps any failure of that caller code; a page-stop callback's
# own exception leaves a traversal unchanged.
_RAISED: dict[str, type[Exception]] = {
    "page_stop": RuntimeError,
    "reference_page_stop": ReferenceFailed,
    "binding_source": InputSourceError,
    "reference_sources": InputSourceError,
    "batch": BatchFailed,
    "fan_out": BatchFailed,
    "transport": TransportError,
}

type Attempt = Callable[[], Awaitable[None]]


def _bounded[T](scenario: Callable[[], Coroutine[Any, Any, T]]) -> T:
    """Run ``scenario`` on a fresh loop in a daemon thread; fail instead of hanging past the limit."""
    outcome: list[T] = []
    failure: list[BaseException] = []

    def run() -> None:
        try:
            outcome.append(asyncio.run(scenario()))
        except BaseException as error:  # noqa: BLE001 - re-raised in the test thread
            failure.append(error)

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    worker.join(HANG_LIMIT)
    if worker.is_alive():
        pytest.fail("the scenario deadlocked")
    if failure:
        raise failure[0]
    return outcome[0]


class _Portal:
    """Answer offset pages of two rows and batches of single commands; ``before_send`` runs first."""

    def __init__(self) -> None:
        self.before_send: Attempt | None = None
        self.transport = ResponderTransport(self.respond)

    async def respond(self, request: Request) -> object:
        if self.before_send is not None:
            await self.before_send()
        parameters = request.copy_parameters()
        if request.method == "batch":
            commands = parameters["cmd"]
            assert isinstance(commands, dict)
            return {"result": {"result": dict.fromkeys(commands, 1), "result_error": {}}}
        start = parameters.get("start", 0)
        assert isinstance(start, int)
        rows = [{"id": value} for value in ROWS[start : start + 2]]
        return {"result": rows, "next": start + 2 if rows else None}


class _PageStop:
    """Run the attempt on every whole page, then continue."""

    def __init__(self, attempt: Attempt) -> None:
        self.attempt = attempt

    async def on_page(self, boundary: PageBoundary) -> ContinuePage:
        del boundary
        await self.attempt()
        return ContinuePage.CONTINUE


async def _commands(attempt: Attempt) -> AsyncIterator[Command[int]]:
    for index in range(4):
        await attempt()
        yield Command(Request("x.get", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)


def _binding(index: int) -> Binding[int]:
    return Binding("parent", (ParameterUpdate(ParameterPath(("PARENT",)), index),), index)


async def _bindings(attempt: Attempt) -> AsyncIterator[Binding[int]]:
    for index in range(2):
        await attempt()
        yield _binding(index)


async def _sources(attempt: Attempt) -> AsyncIterator[ReferenceSource[int]]:
    for index in range(2):
        await attempt()
        yield ReferenceSource(_listing(), SequentialTraversal(page_size=2), _binding(index))


def _listing() -> Request:
    return Request("offset.list", {"PARENT": 0}, ReplaySafety.SAFE, route=RouteKind.BARE)


def _open(client: Bitrix24, portal: _Portal, path: str, attempt: Attempt) -> OperationStream[Any]:
    """Open the stream of one family with ``attempt`` wired into the caller code its pull runs."""
    traversal = SequentialTraversal(page_size=2)
    direct = DirectDispatch(concurrency=1)
    streams: dict[str, Callable[[], OperationStream[Any]]] = {
        "page_stop": lambda: client.iter_list(_listing(), page_size=2, page_stop=_PageStop(attempt)),
        "reference_page_stop": lambda: client.iter_references(
            _listing(), (_binding(0),), traversal=traversal, dispatch=direct, page_stop=_PageStop(attempt)
        ),
        "binding_source": lambda: client.iter_references(
            _listing(), _bindings(attempt), traversal=traversal, dispatch=direct
        ),
        "reference_sources": lambda: client.iter_reference_sources(_sources(attempt), dispatch=direct),
        "batch": lambda: client.batch(_commands(attempt), batch_size=1),
        "fan_out": lambda: client.fan_out(_commands(attempt), dispatch=BatchDispatch(batch_size=1)),
        "transport": lambda: client.iter_list(_listing(), page_size=2),
    }
    if path == "transport":
        portal.before_send = attempt
    return streams[path]()


def _client(portal: _Portal, policy: ExecutionPolicy | None = None) -> Bitrix24:
    return Bitrix24(Settings(webhook_url=f"https://{HOST}/rest/1/self/"), transport=portal.transport, policy=policy)


def _chain(error: BaseException) -> list[BaseException]:
    seen: list[BaseException] = []
    current: BaseException | None = error
    while current is not None and current not in seen:
        seen.append(current)
        current = current.__cause__ or current.__context__
    return seen


@pytest.mark.parametrize("target", tuple(_REFUSALS))
@pytest.mark.parametrize("path", PATHS)
def test_close_from_inside_the_own_pull_is_refused_and_the_stream_stays_closable(path: str, target: str) -> None:
    async def scenario() -> None:
        portal = _Portal()
        client = _client(portal)
        refusals: list[RuntimeError] = []
        opened: list[OperationStream[Any]] = []

        async def attempt() -> None:
            if refusals:
                return
            try:
                await (opened[0].aclose() if target == "stream" else client.aclose())
            except RuntimeError as refusal:
                refusals.append(refusal)

        stream = _open(client, portal, path, attempt)
        opened.append(stream)
        while not refusals:
            await anext(stream)
        assert _REFUSALS[target] in str(refusals[0])
        # The refusal changed nothing: the stream still reads and the client still accepts work.
        assert stream.report is None
        await anext(stream)
        assert await client.call(_listing()) == [{"id": 1}, {"id": 2}]
        if target == "stream":
            await stream.aclose()
        await client.aclose()
        report = stream.report
        assert report is not None
        assert report.state is TerminalState.EARLY_CLOSED
        assert "cleanup_failure" not in [violation.code for violation in report.violations]
        await stream.aclose()
        assert stream.report is report

    _bounded(scenario)


@pytest.mark.parametrize("target", tuple(_REFUSALS))
@pytest.mark.parametrize("path", PATHS)
def test_an_unhandled_refusal_fails_only_the_stream_whose_pull_raised_it(path: str, target: str) -> None:
    async def scenario() -> None:
        portal = _Portal()
        client = _client(portal)
        opened: list[OperationStream[Any]] = []

        async def attempt() -> None:
            await (opened[0].aclose() if target == "stream" else client.aclose())

        stream = _open(client, portal, path, attempt)
        opened.append(stream)
        with pytest.raises(Exception) as raised:  # noqa: PT011 - each family wraps a caller failure its own way
            async for _item in stream:
                pass
        portal.before_send = None
        assert type(raised.value) is _RAISED[path], raised.value
        refusals = [error for error in _chain(raised.value) if isinstance(error, RuntimeError)]
        assert any(_REFUSALS[target] in str(error) for error in refusals), raised.value
        report = stream.report
        assert report is not None
        assert report.state is TerminalState.FAILED
        await stream.aclose()
        assert stream.report is report
        # The client was never marked closed: it still serves, then closes normally from outside.
        assert await client.call(_listing()) == [{"id": 1}, {"id": 2}]
        await client.aclose()

    _bounded(scenario)


def test_closing_another_stream_from_inside_a_pull_is_allowed() -> None:
    async def scenario() -> None:
        portal = _Portal()
        async with _client(portal) as client:
            sibling = client.iter_list(_listing(), page_size=2)
            assert await anext(sibling) == {"id": 1}

            async def close_sibling() -> None:
                await sibling.aclose()

            stream = client.iter_list(_listing(), page_size=2, page_stop=_PageStop(close_sibling))
            rows = [row async for row in stream]
            assert len(rows) == len(ROWS)
            assert sibling.report is not None
            assert sibling.report.state is TerminalState.EARLY_CLOSED
            report = stream.report
            assert report is not None
            assert report.state is TerminalState.COMPLETED

    _bounded(scenario)


class _StubbornStop:
    """A page-stop callback that ignores cancellation until released."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def on_page(self, boundary: PageBoundary) -> ContinuePage:
        del boundary
        self.entered.set()
        while not self.release.is_set():
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                continue
        return ContinuePage.CONTINUE


def test_a_cancelled_close_stops_waiting_for_a_stuck_pull_at_the_operation_deadline() -> None:
    # The operation budget bounds how long a close defers its caller's cancellation: the close of a
    # pull stuck in caller code that swallows cancellation ends at the deadline with the cleanup
    # reported as failed, and the cleanup it left behind still finishes once the pull lets go.
    async def scenario() -> None:
        portal = _Portal()
        stop = _StubbornStop()
        client = _client(portal, ExecutionPolicy(max_elapsed=3.0))
        stream = client.iter_list(_listing(), page_size=2, page_stop=stop)
        reader = asyncio.create_task(_drain(stream))
        try:
            await stop.entered.wait()
            closer = asyncio.create_task(stream.aclose())
            await asyncio.sleep(0)
            closer.cancel()
            (closed,) = await asyncio.gather(closer, return_exceptions=True)
        finally:
            stop.release.set()
        assert isinstance(closed, BudgetExceededError), closed
        report = stream.report
        assert report is not None
        assert report.state is TerminalState.EARLY_CLOSED
        assert report.terminal_reason == "stream cleanup failed"
        assert "cleanup_failure" in [violation.code for violation in report.violations]
        await asyncio.gather(reader, return_exceptions=True)
        # The cleanup left in the background ends too: nothing but this scenario is left running.
        while others := asyncio.all_tasks() - {asyncio.current_task()}:
            await asyncio.wait(others)
        await stream.aclose()
        assert stream.report is report
        await client.aclose()

    _bounded(scenario)


async def _drain(stream: OperationStream[Any]) -> None:
    async for _item in stream:
        pass


class _Detached:
    """A page-stop callback that starts, once, a task closing the stream and does not await it."""

    def __init__(self, *, context: contextvars.Context | None = None, wait: asyncio.Event | None = None) -> None:
        self.context = context
        self.wait = wait
        self.stream: OperationStream[Any] | None = None
        self.closer: asyncio.Task[None] | None = None

    async def on_page(self, boundary: PageBoundary) -> ContinuePage:
        del boundary
        if self.closer is None:
            self.closer = asyncio.create_task(self._close(), context=self.context)
        return ContinuePage.CONTINUE

    async def _close(self) -> None:
        if self.wait is not None:
            await self.wait.wait()
        assert self.stream is not None
        await self.stream.aclose()


class _CloseDuringNextPage:
    """Start a closing task on the first page; let it try only while the next page's pull is held open."""

    def __init__(self) -> None:
        self.stream: OperationStream[Any] | None = None
        self.closer: asyncio.Task[None] | None = None
        self.go = asyncio.Event()
        self.attempted = asyncio.Event()

    async def on_page(self, boundary: PageBoundary) -> ContinuePage:
        del boundary
        if self.closer is None:
            self.closer = asyncio.create_task(self._close())
        elif not self.go.is_set():
            self.go.set()
            await self.attempted.wait()
        return ContinuePage.CONTINUE

    async def _close(self) -> None:
        await self.go.wait()
        assert self.stream is not None
        try:
            await self.stream.aclose()
        finally:
            self.attempted.set()


def test_a_detached_task_started_by_caller_code_cannot_close_the_stream_while_a_read_runs() -> None:
    # The documented false positive: the task inherits the pull's context, so while a pull is running
    # the close is refused although that pull does not wait for the task.
    async def scenario() -> None:
        stop = _CloseDuringNextPage()
        async with _client(_Portal()) as client:
            stream = stop.stream = client.iter_list(_listing(), page_size=2, page_stop=stop)
            rows = [row async for row in stream]
            assert stop.closer is not None
            with pytest.raises(RuntimeError, match=_REFUSALS["stream"]):
                await stop.closer
            assert len(rows) == len(ROWS)
            report = stream.report
            assert report is not None
            assert report.state is TerminalState.COMPLETED

    _bounded(scenario)


def test_a_task_outliving_a_finished_stream_does_not_keep_it_alive() -> None:
    # A task started during a pull keeps the pull's context; the marker in it must not pin the stream.
    async def scenario() -> None:
        forever = asyncio.Event()
        kept: list[asyncio.Task[bool]] = []

        async def spawn() -> None:
            if not kept:
                kept.append(asyncio.create_task(forever.wait()))

        async with _client(_Portal()) as client:
            stream = client.iter_list(_listing(), page_size=2, page_stop=_PageStop(spawn))
            assert len([row async for row in stream]) == len(ROWS)
            alive = weakref.ref(stream)
            del stream
            gc.collect()
            assert alive() is None
            assert not kept[0].done()
            forever.set()
            await kept[0]

    _bounded(scenario)


def test_a_detached_task_in_a_fresh_context_closes_the_stream() -> None:
    async def scenario() -> None:
        stop = _Detached(context=contextvars.Context())
        async with _client(_Portal()) as client:
            stream = stop.stream = client.iter_list(_listing(), page_size=2, page_stop=stop)
            rows = [row async for row in stream]
            assert stop.closer is not None
            await stop.closer
            assert len(rows) < len(ROWS)
            report = stream.report
            assert report is not None
            assert report.state is TerminalState.EARLY_CLOSED

    _bounded(scenario)


def test_a_detached_task_closes_the_stream_while_no_read_runs() -> None:
    # Between reads a traversal runs nothing of its own, so the close waits for no task and proceeds.
    async def scenario() -> None:
        idle = asyncio.Event()
        stop = _Detached(wait=idle)
        async with _client(_Portal()) as client:
            stream = stop.stream = client.iter_list(_listing(), page_size=2, page_stop=stop)
            for _row in range(3):
                await anext(stream)
            idle.set()
            assert stop.closer is not None
            await stop.closer
            report = stream.report
            assert report is not None
            assert report.state is TerminalState.EARLY_CLOSED

    _bounded(scenario)


def test_a_fan_out_source_cannot_close_its_stream_between_reads() -> None:
    # The scheduler keeps pulling the source in its own tasks while the reader is idle; a close from
    # there would wait for those tasks, so it is refused between reads too.
    async def scenario() -> None:
        idle = asyncio.Event()
        refusals: list[RuntimeError] = []
        tried = asyncio.Event()
        opened: list[OperationStream[Any]] = []

        async def attempt() -> None:
            if not idle.is_set() or tried.is_set():
                return
            try:
                await opened[0].aclose()
            except RuntimeError as refusal:
                refusals.append(refusal)
            tried.set()

        async with _client(_Portal()) as client:
            stream = client.fan_out(_commands(attempt), dispatch=BatchDispatch(batch_size=1))
            opened.append(stream)
            await anext(stream)
            idle.set()
            await tried.wait()
            assert stream.report is None
            assert _REFUSALS["stream"] in str(refusals[0])
            await stream.aclose()
            report = stream.report
            assert report is not None
            assert report.state is TerminalState.EARLY_CLOSED

    _bounded(scenario)


@pytest.mark.parametrize("target", tuple(_REFUSALS))
def test_a_source_closing_from_its_own_finally_is_refused_during_the_stream_cleanup(target: str) -> None:
    # The reader leaves early, so the stream's cleanup closes the source, whose ``finally`` closes again.
    async def scenario() -> None:
        refusals: list[RuntimeError] = []
        opened: list[OperationStream[Any]] = []
        client = _client(_Portal())

        async def commands() -> AsyncIterator[Command[int]]:
            try:
                for index in range(100):
                    yield Command(Request("x.get", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)
            finally:
                try:
                    await (opened[0].aclose() if target == "stream" else client.aclose())
                except RuntimeError as refusal:
                    refusals.append(refusal)
                    raise

        stream = client.batch(commands(), batch_size=1)
        opened.append(stream)
        with pytest.raises(RuntimeError, match=_REFUSALS[target]):
            async with stream:
                await anext(stream)
        assert _REFUSALS[target] in str(refusals[0])
        report = stream.report
        assert report is not None
        assert report.state is TerminalState.EARLY_CLOSED
        assert "cleanup_failure" in [violation.code for violation in report.violations]
        # The client was never marked closed: it still serves, then closes normally.
        assert await client.call(_listing()) == [{"id": 1}, {"id": 2}]
        await client.aclose()

    _bounded(scenario)
