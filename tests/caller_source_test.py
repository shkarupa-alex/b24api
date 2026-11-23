"""How every stream family that reads a caller's source treats that source, the same in all of them.

A close that raised, or that overran its cleanup window (``CleanupOverrunError``), is reported the same
way whichever family read the source and however the stream ended. A close that raised is the source
failing: it raises the family's source failure (``InputSourceError``, wrapped by ``BatchFailed`` in a
fail-fast batch family) chained to the caller's exception, as a failed pull does. When nothing failed before it, the
close is what failed (terminal reason "stream cleanup failed", one ``cleanup_failure`` violation
"stream cleanup failed (X)", no other); behind an earlier failure it is that failure's secondary
("stream cleanup also failed (X)") and the earlier failure stays the terminal reason.

A close concurrent with a read is an early close like any other; an item counts as emitted once its reader has
it; and an asynchronous source runs every pull and its close in one context of its own.
"""

from __future__ import annotations
import asyncio
import contextlib
import contextvars
import gc
import sys
import threading
import time
import warnings
from typing import TYPE_CHECKING, Any

import pytest

from b24api import (
    BatchDispatch,
    Binding,
    CountedTraversal,
    CursorSpec,
    DirectDispatch,
    ExecutionPolicy,
    IdentityCoercion,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    SequentialTraversal,
    TerminalState,
)
from b24api.contracts import Command, ContinuePage, PageBoundary, ReferenceFailure, ReferenceSource
from b24api.errors import (
    BatchFailed,
    BudgetExceededError,
    CapabilityError,
    CleanupOverrunError,
    IncompleteTraversalError,
    InputSourceError,
    PaginationError,
    StreamClosedError,
)
from b24api.execution.context import CANCEL_GRACE_SECONDS, is_interrupt
from tests.scripting import (
    DEADLINE_BUDGET,
    HANG_BOUND,
    BatchPortal,
    ResponderTransport,
    attached_report,
    client_for,
    raised_by_asyncio_run,
    raised_driving_the_loop,
    reached,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Iterable, Iterator

    from b24api import Bitrix24
    from b24api.contracts.report import OperationReport
    from b24api.contracts.stream import OperationStream
    from tests.scripting import Envelope, Query

FAMILIES = (
    "batch",
    "batch_outcomes",
    "fan_out",
    "fan_out_outcomes",
    "iter_references",
    "iter_reference_outcomes",
    "iter_reference_sources",
    "iter_reference_source_outcomes",
    "iter_cursors",
)
# The public error a source's failure ends each family with; fail-fast batch families wrap it.
SOURCE_FAILURE: dict[str, type[Exception]] = {
    family: BatchFailed if family in {"batch", "fan_out"} else InputSourceError for family in FAMILIES
}
ITEMS = 2
LIST_REQUEST = Request("x.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)
TRAVERSAL = SequentialTraversal(selector=ResultSelector(()), page_size=50)
CURSOR = CursorSpec(
    parameter_path=ParameterPath(("LAST_ID",)),
    item_path=("ID",),
    coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
    direction="ascending",
    take="last",
)
# A close that raises at once ends well within this budget; one that blocks overruns the short one.
POLICIES = {
    "returns": ExecutionPolicy(max_elapsed=60.0),
    "raises": ExecutionPolicy(max_elapsed=60.0),
    "overruns": ExecutionPolicy(max_elapsed=DEADLINE_BUDGET),
}
# What the report names: a close that raised is the source failing; an overrun is the library's own error.
CLOSE_FAILURES = {"raises": "InputSourceError", "overruns": "CleanupOverrunError"}


def _raised_for(family: str, close: str) -> type[Exception]:
    return SOURCE_FAILURE[family] if close == "raises" else CleanupOverrunError


def _chain(error: BaseException | None) -> list[type[BaseException]]:
    chain: list[type[BaseException]] = []
    while error is not None:
        chain.append(type(error))
        error = error.__cause__
    return chain


def _answer(request: Request) -> object:
    """One distinct row per binding's first page, then an empty page; one row per command."""
    parameters = request.copy_parameters()
    if request.method == "batch":
        commands = parameters["cmd"]
        assert isinstance(commands, dict)
        return {"result": {"result": {key: {"ID": 1} for key in commands}, "result_error": {}}}
    if request.method != "x.list":
        return {"result": {"ID": 1}}
    if "LAST_ID" in parameters or parameters.get("start"):
        return {"result": [], "total": 0}
    owner = parameters.get("filter")
    assert isinstance(owner, dict)
    index = owner["OWNER"]
    assert isinstance(index, int)
    return {"result": [{"ID": 10 + index}], "total": 1}


def _binding(index: int) -> Binding[int]:
    return Binding(
        summary=f"owner {index}",
        updates=(ParameterUpdate(ParameterPath(("filter", "OWNER")), index),),
        correlation=index,
    )


def _items(family: str, count: int = ITEMS) -> list[Any]:
    if family in {"batch", "batch_outcomes", "fan_out", "fan_out_outcomes"}:
        return [
            Command(Request("x.get", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)
            for index in range(count)
        ]
    if family in {"iter_reference_sources", "iter_reference_source_outcomes"}:
        return [ReferenceSource(LIST_REQUEST, TRAVERSAL, _binding(index)) for index in range(count)]
    return [_binding(index) for index in range(count)]


class _Source:
    """A caller's source whose close returns, raises or blocks until released; it may fail after one item."""

    def __init__(
        self,
        family: str,
        close: str,
        *,
        fail_after: int | None = None,
        failure: BaseException | None = None,
        count: int = ITEMS,
    ) -> None:
        self._items = iter(_items(family, count))
        self._close = close
        self._fail_after = fail_after
        self._failure = failure or ValueError("the caller's source failed")
        self._pulled = 0
        self.release = asyncio.Event()
        self.closing = asyncio.Event()
        self.closes = 0
        # What an ``exits`` close raises.
        self.exit = SystemExit("the caller's close exited")

    def __aiter__(self) -> _Source:
        return self

    async def __anext__(self) -> Any:  # noqa: ANN401 - commands, bindings or reference sources
        if self._pulled == self._fail_after:
            raise self._failure
        self._pulled += 1
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration from None

    async def aclose(self) -> None:
        self.closes += 1
        self.closing.set()
        if self._close == "raises":
            raise RuntimeError("the caller's close failed")
        if self._close == "exits":
            raise self.exit
        if self._close == "overruns":
            await reached(self.release)


class _HeldSource(_Source):
    """``_Source`` whose items start at ``HELD_FIRST``: a stream held open beside the one under test."""

    def __init__(self, family: str) -> None:
        super().__init__(family, "returns")
        self._items = iter(_items(family, HELD_FIRST + ITEMS)[HELD_FIRST:])


class _SyncSource:
    """``_Source`` as a synchronous iterator: pulled and closed in worker threads, its close blocking there."""

    def __init__(
        self, family: str, close: str, *, fail_after: int | None = None, failure: BaseException | None = None
    ) -> None:
        self._items = iter(_items(family))
        self._close = close
        self._fail_after = fail_after
        self._failure = failure or ValueError("the caller's source failed")
        self._pulled = 0
        self.release = threading.Event()
        self.closes = 0

    def __iter__(self) -> _SyncSource:
        return self

    def __next__(self) -> Any:  # noqa: ANN401 - commands, bindings or reference sources
        if self._pulled == self._fail_after:
            raise self._failure
        self._pulled += 1
        return next(self._items)

    def close(self) -> None:
        self.closes += 1
        if self._close == "raises":
            raise RuntimeError("the caller's close failed")
        if self._close == "overruns":
            self.release.wait(HANG_BOUND)


class _SyncPullHeld(_SyncSource):
    """``_SyncSource`` whose failing pull waits in its worker thread (``entered``) until ``release`` is set."""

    def __init__(self, family: str, failure: BaseException) -> None:
        super().__init__(family, "returns", fail_after=1, failure=failure)
        self.entered = threading.Event()

    def __next__(self) -> Any:  # noqa: ANN401 - commands, bindings or reference sources
        if self._pulled == self._fail_after:
            self.entered.set()
            self.release.wait(HANG_BOUND)
        return super().__next__()


def _stream(
    client: Bitrix24,
    family: str,
    source: Iterable[Any] | AsyncIterator[Any],
    dispatch: DirectDispatch | BatchDispatch | None = None,
) -> OperationStream[Any]:
    direct = dispatch or DirectDispatch(concurrency=1)
    if family == "iter_cursors":
        return client.iter_cursors(LIST_REQUEST, source, selector=ResultSelector(()), cursor=CURSOR, dispatch=direct)
    build: Callable[..., OperationStream[Any]] = getattr(client, family)
    if family in {"batch", "batch_outcomes"}:
        return build(source, batch_size=1)
    if family in {"fan_out", "fan_out_outcomes", "iter_reference_sources", "iter_reference_source_outcomes"}:
        return build(source, dispatch=direct)
    return build(LIST_REQUEST, source, traversal=TRAVERSAL, dispatch=direct)


async def _end(stream: OperationStream[Any], ending: str) -> None:
    if ending == "aclose_before_read":
        await stream.aclose()
        return
    async with stream:
        async for _item in stream:
            if ending == "break":
                break


SOURCE_KINDS: dict[str, type[_Source | _SyncSource]] = {"async": _Source, "sync": _SyncSource}


async def _close_fails(
    source: _Source | _SyncSource, family: str, close: str, ending: str
) -> tuple[BaseException, OperationReport]:
    async with client_for(ResponderTransport(_answer), policy=POLICIES[close]) as client:
        stream = _stream(client, family, source)
        try:
            with pytest.raises(Exception) as caught:  # noqa: PT011 - each case checks the error it expects
                await asyncio.wait_for(_end(stream, ending), timeout=HANG_BOUND)
        finally:
            source.release.set()
    report = stream.report
    assert report is not None
    return caught.value, report


def _violations(report: OperationReport) -> list[tuple[str, str]]:
    return [(violation.code, violation.message) for violation in report.violations]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["async", "sync"])
@pytest.mark.parametrize("close", ["raises", "overruns"])
@pytest.mark.parametrize("ending", ["read_to_end", "break", "aclose_before_read"])
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_failed_close_is_what_failed_when_nothing_failed_before_it(
    family: str, ending: str, close: str, kind: str
) -> None:
    # A synchronous source's ``close()`` and an asynchronous one's ``aclose()`` fail the same way.
    error, report = await _close_fails(SOURCE_KINDS[kind](family, close), family, close, ending)

    name = CLOSE_FAILURES[close]
    assert type(error) is _raised_for(family, close)
    if close == "raises":
        assert _chain(error)[-2:] == [InputSourceError, RuntimeError]
    assert report.state is (TerminalState.FAILED if ending == "read_to_end" else TerminalState.EARLY_CLOSED)
    assert report.terminal_reason == "stream cleanup failed"
    # The close's failure alone: no classification of it as the work's failure, no budget end, no "also".
    assert _violations(report) == [("cleanup_failure", f"stream cleanup failed ({name})")]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["async", "sync"])
@pytest.mark.parametrize("close", ["raises", "overruns"])
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_failed_close_behind_an_earlier_failure_is_its_secondary(family: str, close: str, kind: str) -> None:
    source = SOURCE_KINDS[kind](family, close, fail_after=1)
    error, report = await _close_fails(source, family, close, "read_to_end")

    name = CLOSE_FAILURES[close]
    assert type(error) is SOURCE_FAILURE[family]
    assert _chain(error)[-2:] == [InputSourceError, ValueError]
    assert report.state is TerminalState.FAILED
    assert report.terminal_reason == "InputSourceError"
    # The failure is primary in every family: its violation comes first, the failed close behind it.
    assert _violations(report) == [
        ("source_failure", f"{report.operation} failed (InputSourceError)"),
        ("cleanup_failure", f"stream cleanup also failed ({name})"),
    ]
    # The report the error carries says so; no family repeats it as a note on the error or its causes.
    link: BaseException | None = error
    while link is not None:
        assert getattr(link, "__notes__", []) == []
        link = link.__cause__


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["async", "sync"])
@pytest.mark.parametrize("failure", [BudgetExceededError("the caller's budget"), PaginationError("the caller's page")])
@pytest.mark.parametrize("fail_after", [0, 1])
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_source_raising_a_library_error_of_its_own_is_its_failure(
    family: str, fail_after: int, failure: Exception, kind: str
) -> None:
    # Whichever pull raises it, the first of a batch window included, it is neither the operation's budget end
    # nor a failed page: the source failed, and the stream says so with the caller's exception as the cause.
    source = SOURCE_KINDS[kind](family, "raises", fail_after=fail_after, failure=failure)
    error, report = await _close_fails(source, family, "raises", "read_to_end")

    assert type(error) is SOURCE_FAILURE[family]
    assert _chain(error)[-2:] == [InputSourceError, type(failure)]
    assert report.state is TerminalState.FAILED
    assert report.terminal_reason == "InputSourceError"
    assert "budget_exhausted" not in {code for code, _message in _violations(report)}


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["async", "sync"])
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_foreign_item_fails_the_source_with_public_errors_down_to_its_type_error(
    family: str, kind: str
) -> None:
    # The item is not what the family reads: every family refuses it with the same ``TypeError``, on a chain of
    # public errors only.
    async def foreign_async() -> AsyncIterator[str]:
        yield "not an item"

    source: Iterable[Any] | AsyncIterator[Any] = ["not an item"] if kind == "sync" else foreign_async()
    async with client_for(ResponderTransport(_answer), policy=POLICIES["returns"]) as client:
        stream = _stream(client, family, source)
        with pytest.raises(SOURCE_FAILURE[family]) as caught:
            await asyncio.wait_for(_end(stream, "read_to_end"), timeout=HANG_BOUND)

    chain = _chain(caught.value)
    assert chain[-2:] == [InputSourceError, TypeError]
    assert [link.__name__ for link in chain if link.__name__.startswith("_")] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_stream_read_out_and_cancelled_in_its_close_reports_the_cancellation_reason(family: str) -> None:
    # The source was read to its end, but the reader's cancellation ended the stream: its state and its reason
    # are the cancellation's, in every family, not the read-out source's ("input exhausted").
    source = _Source(family, "overruns")

    async def consume(stream: OperationStream[Any]) -> None:
        async for _item in stream:
            pass

    async with client_for(ResponderTransport(_answer), policy=POLICIES["returns"]) as client:
        stream = _stream(client, family, source)
        reader = asyncio.create_task(consume(stream))
        try:
            await reached(source.closing)
            reader.cancel()
            await asyncio.sleep(0)
        finally:
            source.release.set()
        assert isinstance(await _settled(reader), asyncio.CancelledError)

    report = stream.report
    assert report is not None
    assert report.state is TerminalState.CANCELLED
    assert report.terminal_reason == "iteration cancelled"


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_callers_own_stream_read_under_a_closed_read_is_cancelled_not_early_closed(family: str) -> None:
    # The transport the outer stream's read runs reads a stream of the caller's own, in a task the read started.
    # Closing the outer stream cancels that task, and so that inner read: the inner stream was cancelled, not
    # closed early by a close of its own, though it ran under the outer read the close ended.
    inner_started = asyncio.Event()

    async def inner_blocks(_request: Request) -> object:
        inner_started.set()
        return await asyncio.Event().wait()

    async with client_for(ResponderTransport(inner_blocks), policy=POLICIES["returns"]) as inner_client:
        inner = _stream(inner_client, "fan_out_outcomes", _items("fan_out_outcomes"))

        async def outer_reads_inner(request: Request) -> object:
            await anext(inner)
            return _answer(request)

        async with client_for(ResponderTransport(outer_reads_inner), policy=POLICIES["returns"]) as client:
            outer = _stream(client, family, _Source(family, "returns"))
            read = asyncio.create_task(anext(outer))
            await reached(inner_started)
            assert await _settled(outer.aclose()) is None
            assert isinstance(await _settled(read), StreamClosedError)

        assert outer.report is not None
        assert outer.report.state is TerminalState.EARLY_CLOSED
        inner_report = inner.report
        assert inner_report is not None
        assert inner_report.state is TerminalState.CANCELLED
        assert inner_report.terminal_reason == "iteration cancelled"


class _Blocks:
    """Answer the first ``answered`` sends; block each later one until released.

    A send that ``resists`` ignores its cancellation: work that outlasts the cleanup window.
    """

    def __init__(self, *, answered: int = 0, resists: bool = False) -> None:
        self._answered = answered
        self._resists = resists
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, request: Request) -> object:
        if self._answered:
            self._answered -= 1
            return _answer(request)
        self.started.set()
        while True:
            try:
                await reached(self.release)
                return _answer(request)
            except asyncio.CancelledError:
                if not self._resists:
                    raise


async def _settled(awaitable: Awaitable[object]) -> BaseException | None:
    """Await ``awaitable`` within the hang bound; return what it raised, ``None`` when it returned."""
    try:
        await asyncio.wait_for(awaitable, timeout=HANG_BOUND)
    except BaseException as error:  # noqa: BLE001 - the test inspects whichever error each side got
        return error
    return None


@pytest.mark.asyncio
@pytest.mark.parametrize("close", ["returns", "raises", "overruns"])
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_close_while_a_read_is_in_flight_is_an_early_close_like_any_other(family: str, close: str) -> None:
    # Another task closes the stream while its reader waits for a send. No caller cancelled it, and neither the
    # budget nor a failure ended it first: it is an early close, whose failed close is what failed, as after break.
    source = _Source(family, close)
    transport = _Blocks()
    async with client_for(ResponderTransport(transport), policy=POLICIES[close]) as client:
        stream = _stream(client, family, source)
        read = asyncio.create_task(anext(stream))
        try:
            await reached(transport.started)
            closed = await _settled(stream.aclose())
            read_end = await _settled(read)
        finally:
            source.release.set()
            transport.release.set()

    report = stream.report
    assert report is not None
    assert report.state is TerminalState.EARLY_CLOSED
    assert source.closes == 1
    if close == "returns":
        # The read the close ended raises that the stream was closed while it read; the close itself succeeded.
        assert closed is None
        assert isinstance(read_end, StreamClosedError)
        assert report.terminal_reason == "stream closed before exhaustion"
        assert _violations(report) == []
        assert attached_report(read_end) is report
        return
    # The closer gets the close's failure, and so does the read the close ended in it.
    assert type(closed) is _raised_for(family, close)
    assert type(read_end) is _raised_for(family, close)
    assert report.terminal_reason == "stream cleanup failed"
    assert _violations(report) == [("cleanup_failure", f"stream cleanup failed ({CLOSE_FAILURES[close]})")]


class _Commands:
    """Two commands, then the end: a source the stream reads out while the work it fed still runs.

    Its end runs no cleanup, as a generator's would: ``closed`` records that its ``aclose()`` ran, which only the
    stream's cleanup calls, once no pull is left to close it after.
    """

    def __init__(self) -> None:
        self._items: Iterator[Command[int]] = iter(_items("fan_out"))
        self.closed = False

    def __aiter__(self) -> _Commands:
        return self

    async def __anext__(self) -> Command[int]:
        for item in self._items:
            return item
        raise StopAsyncIteration

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("dispatch", "overrun"),
    [
        # The reference task sends a command itself: it is the task the scheduler's cleanup waits for.
        (DirectDispatch(concurrency=2), "reference task cleanup exceeded operation time budget"),
        # The dispatcher's worker sends it: the dispatcher's cleanup waits for the worker.
        (BatchDispatch(batch_size=1, concurrency=2), "batch dispatcher cleanup exceeded operation time budget"),
    ],
    ids=["direct", "batch"],
)
async def test_reference_work_that_outlasts_the_cleanup_window_fails_the_close_and_the_source_is_still_closed(
    dispatch: DirectDispatch | BatchDispatch, overrun: str
) -> None:
    # A direct fan-out reads ahead, so the second command is in flight once the first outcome is read; a batch
    # fan-out sends only while it is read, so its first command is in flight when another task closes the stream.
    direct = isinstance(dispatch, DirectDispatch)
    transport = _Blocks(answered=1 if direct else 0, resists=True)
    source = _Commands()
    async with client_for(ResponderTransport(transport), policy=POLICIES["overruns"]) as client:
        stream = client.fan_out_outcomes(source, dispatch=dispatch)
        read = None
        try:
            if direct:
                await anext(stream)
            else:
                read = asyncio.create_task(anext(stream))
            await reached(transport.started)
            closed = await _settled(stream.aclose())
            if read is not None:
                assert type(await _settled(read)) is CleanupOverrunError
        finally:
            transport.release.set()

    # The work's overrun is the close's failure: no budget end, no cancellation, nothing failed before it.
    assert type(closed) is CleanupOverrunError
    assert str(closed) == overrun
    # The source is closed although the work it fed was still running when the window ended.
    assert source.closed
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.EARLY_CLOSED
    assert report.terminal_reason == "stream cleanup failed"
    assert _violations(report) == [("cleanup_failure", "stream cleanup failed (CleanupOverrunError)")]


class _CommandsWhoseCloseIsInterrupted(_Commands):
    """``_Commands`` whose close, once run, raises an interrupt."""

    def __init__(self, interrupt: type[BaseException]) -> None:
        super().__init__()
        self._interrupt = interrupt

    async def aclose(self) -> None:
        await super().aclose()
        raise self._interrupt("close interrupted")


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize(
    "dispatch", [DirectDispatch(concurrency=2), BatchDispatch(batch_size=1, concurrency=2)], ids=["direct", "batch"]
)
async def test_an_interrupt_from_the_close_outranks_reference_work_that_outlasts_the_cleanup_window(
    dispatch: DirectDispatch | BatchDispatch, interrupt: type[BaseException]
) -> None:
    # The work's overrun is a cleanup failure found first; the source's close then raises an interrupt, which is
    # never behind another failure: the close raises it, and so does the read the close ended. The overrun stays
    # in the report, ahead of the interrupt.
    direct = isinstance(dispatch, DirectDispatch)
    transport = _Blocks(answered=1 if direct else 0, resists=True)
    source = _CommandsWhoseCloseIsInterrupted(interrupt)
    async with client_for(ResponderTransport(transport), policy=POLICIES["overruns"]) as client:
        stream = client.fan_out_outcomes(source, dispatch=dispatch)
        read = None
        try:
            if direct:
                await anext(stream)
            else:
                # The read settles inside its task: a task of the test's own ending in an interrupt would stop the loop.
                read = asyncio.create_task(_settled(anext(stream)))
            await reached(transport.started)
            closed = await _settled(stream.aclose())
            if read is not None:
                assert type(await read) is interrupt
        finally:
            transport.release.set()

    assert type(closed) is interrupt
    assert source.closed
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.FAILED
    assert report.terminal_reason == "operation interrupted"
    assert _violations(report) == [
        ("cleanup_failure", "stream cleanup failed (CleanupOverrunError)"),
        ("interrupted", f"operation interrupted ({interrupt.__name__})"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dispatch", [DirectDispatch(concurrency=2), BatchDispatch(batch_size=1, concurrency=2)], ids=["direct", "batch"]
)
async def test_a_cancellation_outranks_reference_work_that_outlasts_the_cleanup_window(
    dispatch: DirectDispatch | BatchDispatch,
) -> None:
    transport = _Blocks(resists=True)
    source = _Commands()
    async with client_for(ResponderTransport(transport), policy=POLICIES["overruns"]) as client:
        stream = client.fan_out_outcomes(source, dispatch=dispatch)
        read = asyncio.create_task(anext(stream))
        try:
            await reached(transport.started)
            read.cancel()
            read_end = await _settled(read)
        finally:
            transport.release.set()

    assert isinstance(read_end, asyncio.CancelledError)
    assert source.closed
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.CANCELLED
    assert report.terminal_reason == "iteration cancelled"
    assert _violations(report) == [("cleanup_failure", "stream cleanup also failed (CleanupOverrunError)")]


# How many loop turns after a send is answered its reader's cancellation may land: enough to reach past the turn in
# which the read hands its item over, in every family.
HANDOVER_TURNS = 40


async def _cancelled_turns_after_an_answer(family: str, turns: int) -> tuple[int, int]:
    """Cancel the reader ``turns`` loop turns after the first send that follows its first item is answered."""
    loop = asyncio.get_running_loop()
    received: list[object] = []
    armed = False

    def cancel_after(left: int) -> None:
        if left:
            loop.call_soon(cancel_after, left - 1)
        else:
            consumer.cancel()

    def answer(request: Request) -> object:
        nonlocal armed
        if armed:
            armed = False
            loop.call_soon(cancel_after, turns)
        return _answer(request)

    async def consume(stream: OperationStream[Any]) -> None:
        nonlocal armed
        async for item in stream:
            received.append(item)
            armed = True

    async with client_for(ResponderTransport(answer), policy=POLICIES["returns"]) as client:
        stream = _stream(client, family, _Source(family, "returns"))
        consumer = asyncio.create_task(consume(stream))
        await _settled(consumer)
        await stream.aclose()
    report = stream.report
    assert report is not None
    return report.emitted, len(received)


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_an_item_counts_as_emitted_only_once_its_reader_has_it(family: str) -> None:
    # At some turn the read has produced its item but the reader, cancelled, never takes it: no item was emitted.
    for turns in range(HANDOVER_TURNS):
        emitted, received = await _cancelled_turns_after_an_answer(family, turns)
        assert emitted == received, f"cancelled {turns} turns after an answer"


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_pull_cancelled_before_it_starts_leaves_no_coroutine_unawaited(family: str) -> None:
    # Some of these cancellations land before a pull's task has started: its never-run coroutine is closed, not
    # left for the collector to report as never awaited.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for turns in range(HANDOVER_TURNS):
            await _cancelled_turns_after_an_answer(family, turns)
        gc.collect()
    assert [str(warning.message) for warning in caught if "never awaited" in str(warning.message)] == []


_TENANT: contextvars.ContextVar[str] = contextvars.ContextVar("tenant", default="caller")


async def _tenant_scoped(family: str, seen: list[tuple[str, str]]) -> AsyncIterator[Any]:
    """A caller's asynchronous generator that sets a context variable around each yield and resets it after."""
    for index, item in enumerate(_items(family)):
        token = _TENANT.set(f"tenant-{index}")
        seen.append(("set", _TENANT.get()))
        try:
            yield item
            seen.append(("resumed", _TENANT.get()))
        finally:
            _TENANT.reset(token)
            seen.append(("reset", _TENANT.get()))


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["read_to_end", "break"])
@pytest.mark.parametrize("family", FAMILIES)
async def test_an_asynchronous_source_keeps_one_context_across_its_pulls_and_its_close(
    family: str, ending: str
) -> None:
    # A pull ran in a fresh copy of the reader's context, so a token set before a yield belonged to another
    # context when the generator resumed or closed: resetting it raised ValueError, the source failed.
    seen: list[tuple[str, str]] = []
    async with client_for(ResponderTransport(_answer), policy=POLICIES["returns"]) as client:
        stream = _stream(client, family, _tenant_scoped(family, seen))
        await asyncio.wait_for(_end(stream, ending), timeout=HANG_BOUND)
    report = stream.report
    assert report is not None
    assert report.state is (TerminalState.COMPLETED if ending == "read_to_end" else TerminalState.EARLY_CLOSED)
    assert report.violations == ()
    tokens = [value for step, value in seen if step == "set"]
    assert tokens == [f"tenant-{index}" for index in range(len(tokens))]
    # Every resumption sees its own token, and every token is reset, back to the caller's value.
    resumed = [value for step, value in seen if step == "resumed"]
    assert resumed == tokens[: len(resumed)]
    assert len(resumed) >= len(tokens) - 1
    assert [value for step, value in seen if step == "reset"] == ["caller"] * len(tokens)
    assert _TENANT.get() == "caller"


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_source_cancelled_in_its_pull_resets_its_context_in_that_context(family: str) -> None:
    # The cancellation enters the generator where its pull waits; its ``finally`` runs in the source's context, as
    # every other step does, so the token resets and the stream ends cancelled, not failed by a ValueError.
    pulling = asyncio.Event()
    reset: list[str] = []

    async def scoped() -> AsyncIterator[Any]:
        token = _TENANT.set("tenant")
        try:
            first, _second = _items(family)
            yield first
            pulling.set()
            await asyncio.Event().wait()
        finally:
            _TENANT.reset(token)
            reset.append(_TENANT.get())

    async def consume(stream: OperationStream[Any]) -> None:
        async for _item in stream:
            pass

    async with client_for(ResponderTransport(_answer), policy=POLICIES["returns"]) as client:
        stream = _stream(client, family, scoped())
        reader = asyncio.create_task(consume(stream))
        await reached(pulling)
        reader.cancel()
        assert isinstance(await _settled(reader), asyncio.CancelledError)

    assert reset == ["caller"]
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.CANCELLED
    assert report.terminal_reason == "iteration cancelled"
    assert report.violations == ()


_SPAN: contextvars.ContextVar[str] = contextvars.ContextVar("span", default="unset")


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["async", "sync"])
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_source_sees_the_context_of_the_call_that_created_its_stream(family: str, kind: str) -> None:
    # The source's context is copied when the stream is created: a variable the caller changes afterwards (a
    # tracing span around the loop, say) is not what the source sees, in any pull or in its close.
    seen: list[str] = []

    async def async_source() -> AsyncIterator[Any]:
        for item in _items(family):
            seen.append(_SPAN.get())
            yield item
        seen.append(_SPAN.get())

    def sync_source() -> Iterator[Any]:
        for item in _items(family):
            seen.append(_SPAN.get())
            yield item
        seen.append(_SPAN.get())

    async with client_for(ResponderTransport(_answer), policy=POLICIES["returns"]) as client:
        token = _SPAN.set("at creation")
        try:
            stream = _stream(client, family, async_source() if kind == "async" else sync_source())
            _SPAN.set("after creation")
            await asyncio.wait_for(_end(stream, "read_to_end"), timeout=HANG_BOUND)
        finally:
            _SPAN.reset(token)

    assert seen == ["at creation"] * (ITEMS + 1)


def _interrupting(
    family: str, kind: str, where: str, interrupt: type[BaseException]
) -> Iterable[Any] | AsyncIterator[Any]:
    """A caller's generator that raises ``interrupt`` from its second pull or from its close."""
    items = _items(family)

    def sync_source() -> Iterator[Any]:
        try:
            for index, item in enumerate(items):
                if where == "pull" and index == 1:
                    raise interrupt
                yield item
        finally:
            if where == "close":
                raise interrupt

    async def async_source() -> AsyncIterator[Any]:
        try:
            for index, item in enumerate(items):
                await asyncio.sleep(0)
                if where == "pull" and index == 1:
                    raise interrupt
                yield item
        finally:
            if where == "close":
                raise interrupt

    return sync_source() if kind == "sync" else async_source()


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize(("where", "ending"), [("pull", "read_to_end"), ("close", "read_to_end"), ("close", "break")])
@pytest.mark.parametrize("kind", ["async", "sync"])
@pytest.mark.parametrize("family", FAMILIES)
def test_an_interrupt_a_source_raises_leaves_the_loop_with_no_task_behind(
    family: str, kind: str, where: str, ending: str, interrupt: type[BaseException]
) -> None:
    # The interrupt reaches the caller's own task, which raises it out of asyncio.run: no library task ends in it
    # unretrieved, and none is left pending when the loop closes.
    reported: list[str] = []

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
        async with client_for(ResponderTransport(_answer), policy=POLICIES["returns"]) as client:
            await _end(_stream(client, family, _interrupting(family, kind, where, interrupt)), ending)

    with pytest.raises(interrupt):
        asyncio.run(main())
    gc.collect()
    assert reported == []


@pytest.mark.parametrize("kind", ["async", "sync"])
@pytest.mark.parametrize("family", FAMILIES)
def test_an_interrupt_from_a_close_the_client_runs_leaves_the_loop_with_no_task_behind(family: str, kind: str) -> None:
    # A stream left open is closed by its client's close; its source's close raising an interrupt reaches the
    # caller's own task through the client's close task, which does not end in it.
    reported: list[str] = []
    items = _items(family)

    def sync_source() -> Iterator[Any]:
        try:
            while True:
                yield from items
        finally:
            raise KeyboardInterrupt

    async def async_source() -> AsyncIterator[Any]:
        try:
            while True:
                for item in items:
                    yield item
        finally:
            raise KeyboardInterrupt

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
        async with client_for(ResponderTransport(_answer), policy=POLICIES["returns"]) as client:
            stream = _stream(client, family, sync_source() if kind == "sync" else async_source())
            await anext(stream)

    with pytest.raises(KeyboardInterrupt):
        asyncio.run(main())
    gc.collect()
    assert reported == []


class _ClosesInterrupted:
    """A caller's source whose close raises ``interrupt``; its second pull fails first when ``fail_pull``."""

    def __init__(self, family: str, interrupt: type[BaseException], *, fail_pull: bool) -> None:
        self._items = iter(_items(family))
        self._interrupt = interrupt
        self._fail_pull = fail_pull
        self._pulled = 0

    def _next(self) -> Any:  # noqa: ANN401 - commands, bindings or reference sources
        if self._fail_pull and self._pulled == 1:
            raise ValueError("the caller's pull failed")
        self._pulled += 1
        return next(self._items)

    def __iter__(self) -> _ClosesInterrupted:
        return self

    def __next__(self) -> Any:  # noqa: ANN401 - commands, bindings or reference sources
        return self._next()

    def close(self) -> None:
        raise self._interrupt


class _AsyncClosesInterrupted(_ClosesInterrupted):
    def __aiter__(self) -> _AsyncClosesInterrupted:
        return self

    async def __anext__(self) -> Any:  # noqa: ANN401 - commands, bindings or reference sources
        try:
            return self._next()
        except StopIteration:
            raise StopAsyncIteration from None

    async def aclose(self) -> None:
        self.close()


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("before", ["nothing", "failure", "cancellation_between_reads", "cancellation_during_a_read"])
@pytest.mark.parametrize("kind", ["async", "sync"])
@pytest.mark.parametrize("family", FAMILIES)
def test_an_interrupt_from_a_close_outranks_whatever_ended_the_stream_before_it(
    family: str, kind: str, before: str, interrupt: type[BaseException]
) -> None:
    # An interrupt is never behind anything: after a read to the end, a failure or the reader's cancellation (between
    # reads or while a send blocks), the close's interrupt is what leaves. The report has one shape whatever came
    # before: failed, interrupted, an earlier failure's violation first and the interrupt's last; the close is no
    # cleanup failure.
    reported: list[str] = []
    pending: list[str] = []
    streams: list[OperationStream[Any]] = []
    source_type = _AsyncClosesInterrupted if kind == "async" else _ClosesInterrupted
    transport = _Blocks(answered=0 if before == "cancellation_during_a_read" else 1_000)

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
        try:
            async with client_for(ResponderTransport(transport), policy=POLICIES["returns"]) as client:
                stream = _stream(client, family, source_type(family, interrupt, fail_pull=before == "failure"))
                streams.append(stream)

                async def read() -> None:
                    async for _item in stream:
                        if before == "cancellation_between_reads":
                            task = asyncio.current_task()
                            assert task is not None
                            task.cancel()

                reader = asyncio.create_task(_settled(read()))
                if before == "cancellation_during_a_read":
                    await reached(transport.started)
                    reader.cancel()
                got = await reader
        finally:
            pending.extend(_others())
        if got is not None:
            raise got

    with pytest.raises(interrupt):
        asyncio.run(main())
    gc.collect()
    assert reported == []
    assert pending == []
    report = streams[0].report
    assert report is not None
    assert report.state is TerminalState.FAILED
    assert report.terminal_reason == "operation interrupted"
    earlier = [("source_failure", f"{report.operation} failed (InputSourceError)")] if before == "failure" else []
    assert _violations(report) == [*earlier, ("interrupted", f"operation interrupted ({interrupt.__name__})")]


@pytest.mark.asyncio
@pytest.mark.parametrize("timing", ["between_reads", "during_a_read"])
@pytest.mark.parametrize("kind", ["async", "sync"])
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_cancelled_reader_whose_close_then_fails_is_cancelled_whenever_the_cancellation_lands(
    family: str, kind: str, timing: str
) -> None:
    # The reader's cancellation came first, so it outranks the close's failure, whether it lands between reads or
    # while a read waits for a request: the reader gets CancelledError and the close is a secondary violation.
    sending = asyncio.Event()

    async def answer(request: Request) -> object:
        if timing == "during_a_read":
            sending.set()
            await asyncio.Event().wait()
        return _answer(request)

    raised: list[BaseException] = []

    async def read(stream: OperationStream[Any]) -> None:
        try:
            async for _item in stream:
                if timing == "between_reads":
                    task = asyncio.current_task()
                    assert task is not None
                    task.cancel()
        except BaseException as error:  # noqa: BLE001 - the test inspects what the reader got
            raised.append(error)

    async with client_for(ResponderTransport(answer), policy=POLICIES["raises"]) as client:
        stream = _stream(client, family, SOURCE_KINDS[kind](family, "raises"))
        reader = asyncio.create_task(read(stream))
        if timing == "during_a_read":
            await reached(sending)
            reader.cancel()
        await _settled(reader)

    assert [type(error) for error in raised] == [asyncio.CancelledError]
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.CANCELLED
    assert report.terminal_reason == "iteration cancelled"
    assert _violations(report) == [("cleanup_failure", "stream cleanup also failed (InputSourceError)")]
    # The cancellation carries the close's failure as its cause, and the report.
    cause = raised[0].__cause__
    assert isinstance(cause, InputSourceError)
    assert type(cause.__cause__) is RuntimeError
    assert str(cause.__cause__) == "the caller's close failed"
    assert getattr(raised[0], "report", None) is report


class _WaitsThenClosesInterrupted:
    """A caller's source whose second pull waits for good and whose close raises ``interrupt``."""

    def __init__(self, family: str, interrupt: type[BaseException]) -> None:
        self._items = iter(_items(family))
        self._interrupt = interrupt
        self._pulled = 0
        self.waiting = asyncio.Event()

    def __aiter__(self) -> _WaitsThenClosesInterrupted:
        return self

    async def __anext__(self) -> Any:  # noqa: ANN401 - commands, bindings or reference sources
        self._pulled += 1
        if self._pulled == ITEMS:
            self.waiting.set()
            await asyncio.Event().wait()
        return next(self._items)

    async def aclose(self) -> None:
        raise self._interrupt


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_read_a_close_ends_gets_the_interrupt_the_close_raises(
    family: str, interrupt: type[BaseException]
) -> None:
    # Another task closes the stream while a read waits; the source's close raises an interrupt. The read the
    # close ended raises the close's end as it does a close failure, and an interrupt is never turned into its
    # cancellation.
    source = _WaitsThenClosesInterrupted(family, interrupt)
    got: list[BaseException] = []

    async def read(stream: OperationStream[Any]) -> None:
        try:
            async for _item in stream:
                pass
        except BaseException as error:  # noqa: BLE001 - the test inspects what the reader got
            got.append(error)

    async with client_for(ResponderTransport(_answer), policy=POLICIES["returns"]) as client:
        stream = _stream(client, family, source)
        reader = asyncio.create_task(read(stream))
        await reached(source.waiting)
        with pytest.raises(interrupt):
            await stream.aclose()
        await reader

    assert [type(error) for error in got] == [interrupt]


@pytest.mark.parametrize("call", [1, 2])
@pytest.mark.parametrize(
    ("family", "dispatch"),
    # A batch dispatch's sender sends the fan-out families' commands; the others' list pages go direct here.
    [(family, "direct") for family in FAMILIES] + [("fan_out", "batch"), ("fan_out_outcomes", "batch")],
)
def test_an_interrupt_a_transport_raises_leaves_the_loop_with_no_task_behind(
    family: str, dispatch: str, call: int
) -> None:
    # The transport runs in the library's own tasks (a binding's run, a batch sender); its interrupt reaches the
    # caller's task, which raises it out of asyncio.run, with no library task ending in it. Each of the two items is
    # sent alone, so both calls come.
    reported: list[str] = []
    pending: list[str] = []
    calls = 0

    def answer(request: Request) -> object:
        nonlocal calls
        calls += 1
        if calls == call:
            raise KeyboardInterrupt
        return _answer(request)

    plan = DirectDispatch(concurrency=2) if dispatch == "direct" else BatchDispatch(batch_size=1, concurrency=2)

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
        try:
            async with client_for(ResponderTransport(answer), policy=POLICIES["returns"]) as client:
                await _end(_stream(client, family, _items(family), plan), "read_to_end")
        finally:
            pending.extend(_others())

    with pytest.raises(KeyboardInterrupt):
        asyncio.run(main())
    gc.collect()
    assert reported == []
    assert pending == []


def _others() -> list[str]:
    """Return the tasks still running besides the current one."""
    current = asyncio.current_task()
    return [task.get_name() for task in asyncio.all_tasks() if task is not current]


REFERENCE_FAMILIES = tuple(family for family in FAMILIES if family not in {"batch", "batch_outcomes"})
# The first item of a stream held open beside the one under test, whose sends the scripted transport answers.
HELD_FIRST = 100


class _Interrupting:
    """A transport that interrupts the operation one of three ways (the reference families' routes to drop it).

    ``queued``: the first send blocks while the second raises ``KeyboardInterrupt`` at once, so the interrupt is in a
    binding the reader does not read yet (``interrupted`` is set as it is raised). ``converted``: every send blocks
    and turns the cancellation that ends it into ``KeyboardInterrupt``. ``after_failure``: the first send fails once
    the second has started, and the second raises ``KeyboardInterrupt`` behind that failure: when the stream the
    failure ended cancels it, or a few loop turns later in a stream the failure does not end.
    """

    def __init__(self, scenario: str) -> None:
        self._scenario = scenario
        self._calls = 0
        self.started = asyncio.Event()
        self.interrupted = asyncio.Event()

    async def __call__(self, request: Request) -> object:
        del request
        self._calls += 1
        if self._scenario == "after_failure":
            return await self._after_failure()
        if self._scenario == "queued" and self._calls == 2:  # noqa: PLR2004 - the second binding's send
            self.interrupted.set()
            raise KeyboardInterrupt
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if self._scenario == "converted":
                raise KeyboardInterrupt from None
            raise
        raise AssertionError("never answered")

    async def _after_failure(self) -> object:
        if self._calls == 1:
            await reached(self.started)
            raise RuntimeError("the first send failed")
        self.started.set()
        try:
            for _turn in range(HANDOVER_TURNS):
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise KeyboardInterrupt from None
        raise KeyboardInterrupt


def _failure_before(family: str, dispatch: str, scenario: str, operation: str) -> list[tuple[str, str]]:
    """Return the violation the first send's failure leaves ahead of the interrupt (none unless ``after_failure``)."""
    if scenario != "after_failure":
        return []
    if family.endswith("_outcomes"):
        return [("reference_failure", "one reference produced a typed failure outcome")]
    if dispatch == "direct":
        return [("transport_failure", f"{operation} failed (TransportError)")]
    return [("ambiguous_execution", f"{operation} failed (AmbiguousExecutionError)")]


@pytest.mark.parametrize(
    ("scenario", "ending"),
    [
        *((scenario, ending) for scenario in ("queued", "converted") for ending in ("cancel", "timeout", "aclose")),
        ("after_failure", "read_to_end"),
    ],
)
@pytest.mark.parametrize("dispatch", ["direct", "batch"])
@pytest.mark.parametrize("family", REFERENCE_FAMILIES)
def test_an_interrupt_on_any_route_ends_the_operation_and_leaves_asyncio_run(
    family: str, dispatch: str, scenario: str, ending: str
) -> None:
    # Wherever the interrupt is caught (a binding's end waiting in its queue, a task the cleanup cancels, a failure
    # that ended the stream first), it is latched and the operation raises it: the reader gets it, whatever ends its
    # read (its cancellation, a timeout, another task's close), and no task is left. The loop reports nothing but
    # another interrupt (``converted`` turns each send's cancellation into one), never the one the reader got.
    reported: list[BaseException] = []
    pending: list[str] = []
    transport = _Interrupting(scenario)
    plan = DirectDispatch(concurrency=2) if dispatch == "direct" else BatchDispatch(batch_size=1, concurrency=2)
    got: list[BaseException] = []
    streams: list[OperationStream[Any]] = []

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["exception"]))
        try:
            async with client_for(ResponderTransport(transport), policy=POLICIES["returns"]) as client:
                stream = _stream(client, family, _items(family), plan)
                streams.append(stream)

                async def read() -> None:
                    try:
                        if ending == "timeout":
                            async with asyncio.timeout(0.2):
                                await _end(stream, "read_to_end")
                        else:
                            await _end(stream, "read_to_end")
                    except BaseException as error:  # noqa: BLE001 - the test inspects what the reader got
                        got.append(error)

                reader = asyncio.create_task(read())
                if scenario != "after_failure":
                    await reached(transport.started)
                if scenario == "queued":
                    await reached(transport.interrupted)
                if ending == "cancel":
                    reader.cancel()
                elif ending == "aclose":
                    with contextlib.suppress(KeyboardInterrupt):
                        await stream.aclose()
                await reader
        finally:
            pending.extend(_others())
        raise got[0]

    with pytest.raises(KeyboardInterrupt):
        asyncio.run(main())
    gc.collect()
    assert [type(error) for error in got] == [KeyboardInterrupt]
    if scenario == "converted":
        assert all(type(error) is KeyboardInterrupt and error is not got[0] for error in reported)
    else:
        assert reported == []
    assert pending == []
    report = streams[0].report
    assert report is not None
    # One report shape on every route: failed, interrupted; the failure that came first keeps its violation, first.
    assert report.state is TerminalState.FAILED
    assert report.terminal_reason == "operation interrupted"
    assert _violations(report) == [
        *_failure_before(family, dispatch, scenario, report.operation),
        ("interrupted", "operation interrupted (KeyboardInterrupt)"),
    ]


def _owner(request: Request) -> object:
    """The binding or command a send is for; a batch family's envelope of its one command 1 is 1's."""
    parameters = request.copy_parameters()
    owner_filter = parameters.get("filter")
    owner = parameters.get("id", owner_filter.get("OWNER") if isinstance(owner_filter, dict) else None)
    commands = parameters.get("cmd")
    if isinstance(commands, dict) and any(str(command).endswith("id=1") for command in commands.values()):
        return 1
    return owner


def _sends_for(request: Request, owner: int) -> bool:
    """Whether ``request`` sends ``owner``'s binding or command: alone, or in a batch with others."""
    if owner == 1:
        return _owner(request) == 1
    commands = request.copy_parameters().get("cmd")
    return isinstance(commands, dict) and any(str(command).endswith(f"id={owner}") for command in commands.values())


class _InterruptsBetweenReads:
    """Answer the first binding; the second binding's send raises the interrupt once the reader has an item.

    ``owner`` names another binding or command whose send raises it instead.
    """

    def __init__(self, interrupt: type[BaseException] | None, owner: int = 1) -> None:
        self.interrupt = interrupt
        self.owner = owner
        self.between = asyncio.Event()
        # Set as the interrupt is raised: a reader waiting for it reads again in the same loop turn as the latch's wake.
        self.raising = asyncio.Event()
        self.raised: list[BaseException] = []

    async def __call__(self, request: Request) -> object:
        if _sends_for(request, self.owner) and self.interrupt is not None:
            await reached(self.between)
            error = self.interrupt("between reads")
            self.raised.append(error)
            self.raising.set()
            raise error
        return _answer(request)


BETWEEN_READS = ("next", "noclose", "abandon", "runloop")


async def _read_and_close(stream: OperationStream[Any], after: list[object]) -> None:
    """Read an ended stream once more and close it, recording what each did."""
    try:
        await anext(stream)
    except StopAsyncIteration as error:
        after.append(type(error))
    await stream.aclose()
    after.append("closed")


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("reader", BETWEEN_READS)
@pytest.mark.parametrize("family", REFERENCE_FAMILIES)
def test_an_interrupt_latched_between_reads_goes_to_the_event_loop_at_once(
    family: str, reader: str, interrupt: type[BaseException]
) -> None:
    # The reader holds an item and works elsewhere while another binding's send raises the interrupt: no read is in
    # flight to raise it, so it goes to the event loop at once, which raises it, once, before the reader's work is
    # over: whether the reader would read again (``next``), never reads nor closes again (``noclose``), never closes
    # its client either (``abandon``), or drives the loop itself (``runloop``). The operation's end follows, in a task
    # started after the interrupt was handed on. Under ``asyncio.run`` the shutdown cancels that task before it
    # begins, and the end is the close the shutdown runs (the client's): it closes the source, once, and reports the
    # interrupt alone. With no close to run (``abandon``) the stream gets no end: its source is never closed, and
    # there is no report. On a loop that runs on, the end is over before a later read finds the stream ended or a
    # close returns, neither raising the interrupt again. A report is attached to the interrupt.
    reported: list[str] = []
    worked: list[bool] = []
    after: list[object] = []
    streams: list[OperationStream[Any]] = []
    transport = _InterruptsBetweenReads(interrupt)
    source = _Source(family, "returns")

    async def body(client: Bitrix24) -> None:
        stream = _stream(client, family, source, DirectDispatch(concurrency=2))
        streams.append(stream)
        await anext(stream)
        transport.between.set()
        await asyncio.sleep(HANG_BOUND)
        worked.append(True)
        if reader == "next":
            await anext(stream)

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
        if reader == "abandon":
            await body(await client_for(ResponderTransport(transport), policy=POLICIES["returns"]).__aenter__())
            return
        async with client_for(ResponderTransport(transport), policy=POLICIES["returns"]) as client:
            await body(client)

    if reader == "runloop":
        error = raised_driving_the_loop(main, lambda: _read_and_close(streams[0], after))
        assert after == [StopAsyncIteration, "closed"]
    else:
        error = raised_by_asyncio_run(main)
    gc.collect()
    assert error is transport.raised[0]
    assert worked == []
    assert reported == []
    report = streams[0].report
    if reader == "abandon":
        assert (report, source.closes) == (None, 0)
        assert not hasattr(error, "report")
        return
    assert source.closes == 1
    assert report is not None
    assert report.state is TerminalState.FAILED
    assert report.terminal_reason == "operation interrupted"
    assert _violations(report) == [("interrupted", f"operation interrupted ({interrupt.__name__})")]
    assert getattr(error, "report", None) is report


@pytest.mark.parametrize("family", REFERENCE_FAMILIES)
def test_a_read_woken_with_the_interrupt_between_reads_closes_the_source_under_asyncio_run(family: str) -> None:
    # The reader holds an item and waits for the moment another binding's send raises the interrupt: it reads again
    # in the very turn the interrupt is latched, ahead of the event loop's raise, and that read runs the end. The
    # shutdown ``asyncio.run`` begins at the raise cancels every task, the cleanup the read started among them before
    # it took a step: the read starts it again, past the shutdown's cancellations, and the source is closed, once.
    # The report is the interrupt's alone: a cleanup that never began failed nothing.
    reported: list[str] = []
    streams: list[OperationStream[Any]] = []
    transport = _InterruptsBetweenReads(KeyboardInterrupt)
    source = _Source(family, "returns")

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
        async with client_for(ResponderTransport(transport), policy=POLICIES["returns"]) as client:
            stream = _stream(client, family, source, DirectDispatch(concurrency=2))
            streams.append(stream)
            await anext(stream)
            transport.between.set()
            await reached(transport.raising)
            await anext(stream)

    error = raised_by_asyncio_run(main)
    gc.collect()
    assert error is transport.raised[0]
    assert reported == []
    assert source.closes == 1
    report = streams[0].report
    assert report is not None
    assert report.terminal_reason == "operation interrupted"
    assert _violations(report) == [("interrupted", "operation interrupted (KeyboardInterrupt)")]
    assert getattr(error, "report", None) is report


def _interrupt_between_reads(family: str) -> tuple[BaseException, float, _InterruptsBetweenReads, _Source]:
    """Run a stream whose reader holds an item as another binding's send raises an interrupt, under its own loop.

    Return what ``asyncio.run`` raised, how long after the interrupt, the transport and the source.
    """
    transport = _InterruptsBetweenReads(KeyboardInterrupt)
    source = _Source(family, "overruns")
    raised_at: list[float] = []

    async def main() -> None:
        # Bounds an end that waited for the cleanup instead of the grace.
        asyncio.get_running_loop().call_later(4.0, source.release.set)
        async with client_for(ResponderTransport(transport), policy=POLICIES["returns"]) as client:
            stream = _stream(client, family, source, DirectDispatch(concurrency=2))
            async with stream:
                await anext(stream)
                transport.between.set()
                await reached(transport.raising)
                raised_at.append(time.monotonic())
                await asyncio.sleep(HANG_BOUND)

    error = raised_by_asyncio_run(main)
    return error, time.monotonic() - raised_at[0], transport, source


@pytest.mark.parametrize("family", ["iter_reference_outcomes", "fan_out_outcomes"])
def test_an_interrupt_between_reads_ended_by_a_cancelled_exit_waits_for_cleanup_only_the_grace(family: str) -> None:
    # The reader holds an item while another binding's send raises the interrupt; the shutdown ``asyncio.run`` then
    # begins cancels the main task, whose stream exit runs the interrupt's end holding that cancellation. The source's
    # close outlasts the grace: the exit, and the client's after it, wait for it one grace in all.
    error, waited, transport, source = _interrupt_between_reads(family)
    assert error is transport.raised[0]
    assert source.closes == 1
    assert waited < CANCEL_GRACE_SECONDS + 1.0, waited


@pytest.mark.parametrize("family", ["iter_reference_outcomes", "fan_out_outcomes"])
def test_a_stream_its_loop_left_behind_starts_no_work_in_a_later_loop_that_collects_it(family: str) -> None:
    # The interrupt's loop ends with the stream's reader coroutine still pending. Collected later inside another
    # loop, that coroutine's close hands nothing over to that loop: its shutdown would wait on the stream's end.
    _interrupt_between_reads(family)
    tasks: list[str] = []

    async def later() -> None:
        gc.collect()
        for _ in range(5):
            await asyncio.sleep(0)
        tasks.extend(repr(task) for task in asyncio.all_tasks() if task is not asyncio.current_task())

    loop = threading.Thread(target=asyncio.run, args=(later(),), daemon=True)
    loop.start()
    loop.join(HANG_BOUND)
    assert not loop.is_alive(), "the later loop waits on a stream it never ran"
    assert tasks == []


class _FailingPageStop:
    """A page stop whose callback raises its own ``Exception``."""

    def __init__(self) -> None:
        self.raised: list[Exception] = []

    async def on_page(self, boundary: PageBoundary) -> ContinuePage:
        del boundary
        error = ValueError("the caller's page stop failed")
        self.raised.append(error)
        raise error


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["iter_reference_outcomes", "iter_reference_source_outcomes"])
async def test_a_failure_outcome_keeps_the_exception_of_a_callers_page_stop_as_its_cause(family: str) -> None:
    # A page stop's own ``Exception`` fails its binding's traversal: the failure outcome's error is the traversal's,
    # and the exception is kept in its chain, carried by the one rule every outcome family follows: a
    # ``CapabilityError`` "reference failed" whose ``__cause__`` it is.
    stop = _FailingPageStop()
    dispatch = DirectDispatch(concurrency=1)
    async with client_for(ResponderTransport(_answer), policy=POLICIES["returns"]) as client:
        if family == "iter_reference_outcomes":
            stream = client.iter_reference_outcomes(
                LIST_REQUEST, [_binding(0)], traversal=TRAVERSAL, dispatch=dispatch, page_stop=stop
            )
        else:
            stream = client.iter_reference_source_outcomes(
                [ReferenceSource(LIST_REQUEST, TRAVERSAL, _binding(0), page_stop=stop)], dispatch=dispatch
            )
        async with stream:
            failures = [outcome async for outcome in stream if isinstance(outcome, ReferenceFailure)]
    assert len(failures) == 1
    error = failures[0].error
    assert isinstance(error, IncompleteTraversalError)
    carried = error.__cause__
    assert type(carried) is CapabilityError
    assert str(carried) == "reference failed"
    assert carried.__cause__ is stop.raised[0]


class _NotAnInterruptError(BaseException):
    """A ``BaseException`` that is neither an interrupt nor a cancellation: no failure, so it propagates as it is."""


class _CloseFails(_Source):
    """``_Source`` whose close, once counted, raises ``close_failure``."""

    def __init__(
        self,
        family: str,
        close_failure: BaseException,
        *,
        fail_after: int | None = None,
        failure: Exception | None = None,
    ) -> None:
        super().__init__(family, "returns", fail_after=fail_after, failure=failure)
        self.close_failure = close_failure

    async def aclose(self) -> None:
        await super().aclose()
        raise self.close_failure


class _RaisesWhenCancelled(_InterruptsBetweenReads):
    """Hold binding 1's send (``sending`` is set first); raise ``interrupt`` from it once it is cancelled."""

    def __init__(self, interrupt: type[BaseException], owner: int = 1) -> None:
        super().__init__(interrupt, owner)
        self.sending = asyncio.Event()

    async def __call__(self, request: Request) -> object:
        if not _sends_for(request, self.owner):
            return _answer(request)
        self.sending.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            error = self.interrupt("cancelled send") if self.interrupt is not None else None
            if error is None:
                raise
            self.raised.append(error)
            raise error from None
        raise AssertionError("unreachable")


# Where it is raised. Binding 1's send: while the reader's read waits (``in_read``); while the reader holds an item,
# which then reads on (``between``) or closes, by ``async with`` (``between_close``) or ``aclose()``
# (``between_aclose``); or once the close cancels it, the reader holding an item (``cancelled_send``) or reading in
# another task (``conc_close_in_read``). The source's second pull, on the loop (``async_pull``) or in its worker thread
# (``sync_pull``), or there once the close cancelled it, a read waiting in another task (``sync_pull_cancelled``).
# The source's close: at the end (``close``), behind the source's own failure (``behind_failure``), or behind the
# caller's own exception, from the stream's ``async with`` (``behind_body``) or the client's (``client_close``).
RAW_POINTS = (
    "in_read",
    "between",
    "between_close",
    "between_aclose",
    "cancelled_send",
    "conc_close_in_read",
    "async_pull",
    "sync_pull",
    "sync_pull_cancelled",
    "close",
    "behind_failure",
    "behind_body",
    "client_close",
)
# The points where the stream's close, not a read, raises it.
CLOSE_RAISES = {
    "between_close",
    "between_aclose",
    "cancelled_send",
    "conc_close_in_read",
    "sync_pull_cancelled",
    "behind_body",
}
# A batch family sends only while it is read: nothing of it runs between reads or waits for a close.
SENT_BETWEEN_READS = {"between", "between_close", "between_aclose", "cancelled_send", "conc_close_in_read"}
# The commands' sends: one by one, or in batches that a sender task sends for every binding, of one command or of
# two: four commands, the reader's first in the first batch and the one that raises it in the second, with another.
RAW_DISPATCHES: dict[str, DirectDispatch | BatchDispatch] = {
    "direct": DirectDispatch(concurrency=2),
    "batch1": BatchDispatch(batch_size=1, concurrency=2),
    "batch2": BatchDispatch(batch_size=2, concurrency=2),
}
# Per dispatch: how many commands the source has, and which one's send raises it.
RAW_SENDS = {"direct": (ITEMS, 1), "batch1": (ITEMS, 1), "batch2": (4, 3)}
RAW_CASES = [
    *(
        (family, point, "direct")
        for family in FAMILIES
        for point in RAW_POINTS
        if not (point in SENT_BETWEEN_READS and family in {"batch", "batch_outcomes"})
    ),
    *(
        (family, point, dispatch)
        for dispatch in ("batch1", "batch2")
        for family in ("fan_out", "fan_out_outcomes")
        for point in ("in_read", *sorted(SENT_BETWEEN_READS))
    ),
]


def _raw_source(family: str, point: str, count: int) -> _Source | _SyncSource:
    raw = _NotAnInterruptError(point)
    if point == "async_pull":
        return _Source(family, "returns", fail_after=1, failure=raw)
    if point == "sync_pull":
        return _SyncSource(family, "returns", fail_after=1, failure=raw)
    if point == "sync_pull_cancelled":
        return _SyncPullHeld(family, raw)
    if point == "behind_failure":
        return _CloseFails(family, raw, fail_after=1, failure=ValueError("pull"))
    if point in {"close", "behind_body", "client_close"}:
        return _CloseFails(family, raw)
    return _Source(family, "returns", count=count)


def _raw_transport(point: str, owner: int) -> _InterruptsBetweenReads:
    if point in {"cancelled_send", "conc_close_in_read"}:
        return _RaisesWhenCancelled(_NotAnInterruptError, owner)
    raises = _NotAnInterruptError if point in {"in_read", *SENT_BETWEEN_READS} else None
    return _InterruptsBetweenReads(raises, owner)


class _RawEnd:
    """One run of ``family`` with the ``BaseException`` raised at ``point``; it records where the reader got it."""

    def __init__(self, family: str, point: str, dispatch: str = "direct") -> None:
        self.family = family
        self.point = point
        self.dispatch = RAW_DISPATCHES[dispatch]
        count, owner = RAW_SENDS[dispatch]
        self.transport = _raw_transport(point, owner)
        self.source = _raw_source(family, point, count)
        self.own = _OwnError("body")
        self.reported: list[str] = []
        self.items: list[object] = []
        # Each place the reader got it: ``read`` or ``close``.
        self.seen: list[tuple[str, BaseException]] = []
        # What a read the close ended in another task raised.
        self.readers: list[BaseException | None] = []

    async def main(self) -> None:
        asyncio.get_running_loop().set_exception_handler(
            lambda _loop, context: self.reported.append(context["message"])
        )
        async with client_for(ResponderTransport(self.transport), policy=POLICIES["returns"]) as client:
            stream = _stream(client, self.family, self.source, self.dispatch)
            if self.point == "client_close":
                self.items.append(await anext(stream))
                raise self.own
            try:
                async with stream:
                    try:
                        await self._drive(stream)
                    except _NotAnInterruptError as error:
                        self.seen.append(("read", error))
            except _NotAnInterruptError as error:
                self.seen.append(("close", error))
            await self._close(stream)  # a later close raises nothing
        raise self.seen[0][1]

    async def _drive(self, stream: OperationStream[Any]) -> None:
        self.items.append(await anext(stream))
        if self.point == "behind_body":
            raise self.own
        if isinstance(self.source, _SyncPullHeld):
            await self._close_held_pull(stream, self.source)
            return
        if isinstance(self.transport, _RaisesWhenCancelled):
            reader = asyncio.ensure_future(self._read_on(stream)) if self.point == "conc_close_in_read" else None
            await reached(self.transport.sending)
            await _turns()
            await self._close(stream)
            if reader is not None:
                self.readers.append(await _settled(reader))
            return
        self.transport.between.set()
        if self.point in SENT_BETWEEN_READS:
            await reached(self.transport.raising)
            await _turns()
            if self.point == "between_aclose":
                await self._close(stream)
            if self.point != "between":
                return
        await self._read_on(stream)

    async def _close_held_pull(self, stream: OperationStream[Any], source: _SyncPullHeld) -> None:
        """Close while the source's pull waits in its thread and a read in another task waits for it; then let it go."""
        reader = asyncio.ensure_future(self._read_on(stream))
        await asyncio.to_thread(source.entered.wait, HANG_BOUND)
        closing = asyncio.ensure_future(self._close(stream))
        await _turns()  # the close has cancelled what waits for the pull
        source.release.set()
        await closing
        self.readers.append(await _settled(reader))

    async def _read_on(self, stream: OperationStream[Any]) -> None:
        async for item in stream:
            self.items.append(item)  # one by one: the items read before it is raised count too

    async def _close(self, stream: OperationStream[Any]) -> None:
        try:
            await stream.aclose()
        except _NotAnInterruptError as error:
            self.seen.append(("close", error))


async def _turns() -> None:
    """Let the stream's own work run on: the end it hands over reaches its tasks."""
    for _turn in range(HANDOVER_TURNS):
        await asyncio.sleep(0)


@pytest.mark.parametrize(("family", "point", "dispatch"), RAW_CASES)
def test_a_base_exception_that_is_no_interrupt_reaches_the_reader_as_python_raises_it(
    family: str, point: str, dispatch: str
) -> None:
    # It is no failure: no family classifies, wraps or ranks it, nor makes it an outcome, and it is never lost. The
    # reader gets it as it is, exactly once: from the read it ended, or else from the close, as a ``finally`` raises
    # it, the close a read waiting in another task makes included (that read is cancelled), whatever caught it on the
    # way (a binding, a batch sender whose pages the close cancelled, a pull's worker thread). The source is closed
    # once, nothing hangs, and nothing reaches the event loop: no handler call, no task left with its exception
    # unretrieved. Behind the caller's own exception it has that as its context. Neither its report nor its rank is
    # promised.
    run = _RawEnd(family, point, dispatch)
    error = raised_by_asyncio_run(run.main)
    gc.collect()  # a task left with its exception unretrieved reports it as it is collected
    assert isinstance(error, _NotAnInterruptError)
    if point != "client_close":
        assert run.seen == [("close" if point in CLOSE_RAISES else "read", error)]
    assert run.reported == []
    assert run.source.closes == 1
    assert not any(hasattr(item, "error") for item in run.items)  # never an outcome, nor any read before it
    if run.transport.raised:
        assert error is run.transport.raised[0]
    else:
        assert str(error) == point
    if point in {"conc_close_in_read", "sync_pull_cancelled"}:
        assert len(run.readers) == 1
        assert isinstance(run.readers[0], StreamClosedError)
    if point in {"behind_body", "client_close"}:
        assert error.__context__ is run.own


# Loop turns between binding 1's send raising it and the close: the close comes before the read's task ends in it,
# after that but before the reader resumes, and after.
RACE_TURNS = 60


class _RawRace:
    """A read on in another task, binding 1's send raising it, and a close ``turns`` loop turns later."""

    def __init__(self, family: str, turns: int) -> None:
        self.family = family
        self.turns = turns
        self.transport = _InterruptsBetweenReads(_NotAnInterruptError)
        self.source = _Source(family, "returns")
        self.reported: list[str] = []
        self.read: BaseException | None = None
        self.closed: BaseException | None = None

    async def main(self) -> None:
        asyncio.get_running_loop().set_exception_handler(
            lambda _loop, context: self.reported.append(context["message"])
        )
        async with client_for(ResponderTransport(self.transport), policy=POLICIES["returns"]) as client:
            stream = _stream(client, self.family, self.source, DirectDispatch(concurrency=2))
            await anext(stream)
            reader = asyncio.ensure_future(_settled(self._read_on(stream)))
            self.transport.between.set()
            for _turn in range(self.turns):
                await asyncio.sleep(0)
            self.closed = await _settled(stream.aclose())
            self.read = await reader

    @staticmethod
    async def _read_on(stream: OperationStream[Any]) -> None:
        async for _item in stream:
            pass


@pytest.mark.parametrize("family", REFERENCE_FAMILIES)
def test_a_base_exception_a_read_ends_in_reaches_that_read_or_a_concurrent_close_once(family: str) -> None:
    # A read waits in another task when binding 1's send raises it, and the stream is closed after one number of loop
    # turns, then the next: the read's task may have ended in it already, its reader not resumed yet. Whoever comes
    # first gets it, exactly once: the read, or else the close, whose read then raises that the stream was closed
    # while it read. Both come first in the sweep.
    # The source is closed once, and nothing reaches the event loop.
    firsts: set[str] = set()
    for turns in range(RACE_TURNS):
        run = _RawRace(family, turns)
        asyncio.run(run.main())
        gc.collect()
        assert run.reported == [], turns
        assert run.source.closes == 1, turns
        raised = run.transport.raised
        got = [end for end, error in (("read", run.read), ("close", run.closed)) if raised and error is raised[0]]
        assert len(got) == len(raised) == 1, (turns, run.read, run.closed)
        firsts.update(got)
        if got == ["close"]:
            assert run.read is None or isinstance(run.read, StreamClosedError), turns
        else:
            assert run.closed is None, turns
    assert firsts == {"read", "close"}


class _TailWavesRaiseWhenCancelled:
    """Serve a counted traversal's head page and its first tail wave; hold every later page, raising once cancelled.

    A batch is answered command by command, so a held page holds its whole wave: the first tail wave (``batch_size``
    pages, 50 to 150) is served whole for the reader to get past the head.
    """

    ROWS = 1001
    WIDTH = 50
    SERVED = frozenset({0, 50, 100, 150})
    # The counted window sends page 200 once the reader is past the rows of pages 0 and 50.
    READ_AHEAD_AT = 2 * WIDTH

    def __init__(self) -> None:
        self.raised: list[BaseException] = []

    async def __call__(self, method: str, query: Query) -> Envelope:
        del method
        offset = int(str(query.get("start", "0")))
        if offset not in self.SERVED:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                error = _NotAnInterruptError(f"cancelled tail page {offset}")
                self.raised.append(error)
                raise error from None
        rows = [{"ID": row + 1} for row in range(offset, min(offset + self.WIDTH, self.ROWS))]
        envelope: Envelope = {"result": rows, "total": self.ROWS}
        if offset + self.WIDTH < self.ROWS:
            envelope["next"] = offset + self.WIDTH
        return envelope


@pytest.mark.parametrize("turns", [3, 30])
def test_a_base_exception_a_counted_tail_page_the_close_cancelled_raises_is_the_closes(turns: int) -> None:
    # A counted traversal's dense tail sends its pages in batches ahead of the reader, delivered as they are ready. The
    # reader stops reading with tail pages in flight and closes the stream; a page the close cancels raises it. No
    # read gets it, so the close raises it, once; the source is closed once, and nothing reaches the event loop.
    answer = _TailWavesRaiseWhenCancelled()
    source = _Source("iter_references", "returns", count=1)
    reported: list[str] = []
    read: list[object] = []

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
        async with client_for(BatchPortal(answer)) as client:
            stream = client.iter_references(
                LIST_REQUEST,
                source,
                traversal=CountedTraversal(page_size=answer.WIDTH),
                dispatch=BatchDispatch(batch_size=3, coalesce_wait=0.0),
                policy=ExecutionPolicy(max_active_references=1, max_elapsed=60.0),
            )
            async for item in stream:
                read.append(item)
                # Past the first tail wave's first page: the next tail page is out and held.
                if len(read) > answer.READ_AHEAD_AT:
                    break
            for _turn in range(turns):
                await asyncio.sleep(0)
            await stream.aclose()

    error = raised_by_asyncio_run(main)
    gc.collect()
    assert len(answer.raised) == 1
    assert error is answer.raised[0]
    assert reported == []
    assert source.closes == 1


class _Abandoned:
    """A stream nothing closes: binding 1's send raises it between reads, or once ``asyncio.run``'s shutdown cancels it.

    ``shutdown``: ``main`` returns with the stream and its client open. ``gc``: the stream is dropped and collected
    while the loop runs on, then the client is closed.
    """

    def __init__(self, family: str, raised: str, end: str) -> None:
        self.family = family
        self.raised = raised
        self.end = end
        if raised == "held":
            self.transport: _InterruptsBetweenReads = _RaisesWhenCancelled(_NotAnInterruptError)
        else:
            self.transport = _InterruptsBetweenReads(_NotAnInterruptError)
        self.handler = _Handler()
        self.kept: list[object] = []
        # The escape's identity alone: whoever holds the escape holds its traceback, and through it the operation.
        self.raised_id: int | None = None

    def take_raised(self) -> int:
        """Let go of the escape the transport raised, keeping its identity."""
        if self.raised_id is None:
            assert len(self.transport.raised) == 1
            self.raised_id = id(self.transport.raised.pop())
        return self.raised_id

    async def main(self) -> None:
        asyncio.get_running_loop().set_exception_handler(self.handler)
        client = client_for(ResponderTransport(self.transport), policy=POLICIES["returns"])
        await client.__aenter__()
        stream = _stream(client, self.family, _Source(self.family, "returns"), DirectDispatch(concurrency=2))
        await anext(stream)
        if isinstance(self.transport, _RaisesWhenCancelled):
            await reached(self.transport.sending)
        else:
            self.transport.between.set()
            await reached(self.transport.raising)
            await _turns()
            self.take_raised()
        if self.end == "shutdown":
            self.kept.append((client, stream))
            return
        del stream
        for _collection in range(3):
            gc.collect()
            await asyncio.sleep(0)
        await client.__aexit__(None, None, None)


# Where nothing ends the operation: the escape raised between reads or by a send the shutdown cancels, the stream
# left open as ``asyncio.run`` returns or dropped while the loop runs on.
ABANDONED = [
    *((family, "held", "shutdown") for family in REFERENCE_FAMILIES),
    *(
        (family, raised, end)
        for family in ("fan_out", "iter_references")
        for raised, end in (("held", "gc"), ("between", "shutdown"), ("between", "gc"))
    ),
]


@pytest.mark.filterwarnings("ignore:coroutine .* was never awaited:RuntimeWarning")
@pytest.mark.parametrize(("family", "raised", "end"), ABANDONED)
def test_an_escape_of_an_operation_nothing_ends_reaches_the_loops_handler_once(
    family: str, raised: str, end: str
) -> None:
    # Nothing ends the operation: no read or close is there to raise the escape. As asyncio reports a task's exception
    # nobody retrieved, the operation reports it to the loop's exception handler, once, with that very object, when it
    # is collected after asyncio.run returned, on its closed loop. A started stream the application dropped while the
    # loop runs on is still its client's: the client's close ends it and raises its escape, once.
    run = _Abandoned(family, raised, end)
    escaped: list[int] = []
    try:
        asyncio.run(run.main())
    except _NotAnInterruptError as error:
        escaped.append(id(error))
    error_id = run.take_raised()
    run.kept.clear()
    gc.collect()
    handed = [id(context["exception"]) for context in run.handler.contexts if context.get("exception") is not None]
    if end == "gc":
        assert (escaped, handed) == ([error_id], [])
    else:
        assert (escaped, handed) == ([], [error_id])


@pytest.mark.filterwarnings("ignore:coroutine .* was never awaited:RuntimeWarning")
@pytest.mark.parametrize("family", ["fan_out_outcomes", "iter_references", "iter_reference_source_outcomes"])
def test_closing_the_binding_coroutines_a_dropped_loop_left_raises_nothing_and_logs_nothing(
    family: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A loop that stops with an operation's tasks pending leaves their coroutines to be closed, by hand or at
    # collection: a ``GeneratorExit`` is raised where each one waits. It is no binding's end, so no binding publishes
    # a terminal event from a coroutine being closed, which must not await again: every close returns quietly, and
    # nothing is reported as ignored.
    unraisable: list[object] = []
    monkeypatch.setattr(sys, "unraisablehook", unraisable.append)
    handled: list[str] = []
    loop = asyncio.new_event_loop()
    loop.set_exception_handler(lambda _loop, context: handled.append(context["message"]))
    kept: list[object] = []

    async def main() -> None:
        client = client_for(
            ResponderTransport(_answer), policy=ExecutionPolicy(max_elapsed=60.0, max_active_references=ITEMS)
        )
        await client.__aenter__()
        # Every binding is admitted and sent at once; all but the one read wait for the reader with their page.
        stream = _stream(client, family, _Source(family, "returns"), DirectDispatch(concurrency=ITEMS))
        kept.extend((client, stream))
        await anext(stream)
        for _turn in range(HANDOVER_TURNS):
            await asyncio.sleep(0)

    try:
        loop.run_until_complete(main())
        pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
        # The source's producer and every binding's run.
        assert len(pending) == 1 + ITEMS
        closed: list[BaseException | None] = []
        for task in pending:
            try:
                task.get_coro().close()  # type: ignore[union-attr]
            except BaseException as error:  # noqa: BLE001 - the test collects what each close raised
                closed.append(error)
            else:
                closed.append(None)
        assert closed == [None] * len(pending)
    finally:
        loop.close()
        kept.clear()
        gc.collect()
    assert unraisable == []
    assert [message for message in handled if message != "Task was destroyed but it is pending!"] == []


class _ClosesWhenReleased:
    """A caller's source whose close waits until the test releases it (``closing`` is set as it starts waiting)."""

    def __init__(self, family: str) -> None:
        self._items = iter(_items(family))
        self.closing = asyncio.Event()
        self.release = asyncio.Event()

    def __aiter__(self) -> _ClosesWhenReleased:
        return self

    async def __anext__(self) -> Any:  # noqa: ANN401 - the family's items
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration from None

    async def aclose(self) -> None:
        self.closing.set()
        await self.release.wait()


@pytest.mark.parametrize("call", ["race", "read", "close"])
@pytest.mark.parametrize("family", REFERENCE_FAMILIES)
def test_a_read_or_close_that_finds_the_operation_ending_sees_its_end_over_and_raises_no_interrupt(
    family: str, call: str
) -> None:
    # An interrupt between reads goes to the event loop at once, and the operation's end follows. A read that starts
    # in the very loop turn of the latch's wake (``race``) runs that end itself, ahead of the end's own task; one that
    # starts while that end waits for the source's close (``read``), or a close then (``close``), waits for it. Each
    # sees the end over, its report published, before it returns: never the stream ended without a report. None of
    # them raises the interrupt: the loop has, once, before any of them returned.
    reported: list[str] = []
    seen: list[object] = []
    seen_as_the_loop_raised: list[list[object]] = []
    after: list[object] = []
    streams: list[OperationStream[Any]] = []
    transport = _InterruptsBetweenReads(KeyboardInterrupt)
    source = _ClosesWhenReleased(family)
    done = asyncio.Event()
    releasing: list[asyncio.Task[None]] = []

    async def release_later() -> None:
        # The source's close goes on waiting a few loop turns after the read or close started: long enough for one
        # that does not wait for the end to have found the stream ended already.
        for _turn in range(HANDOVER_TURNS):
            await asyncio.sleep(0)
        source.release.set()

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
        client = await client_for(ResponderTransport(transport), policy=POLICIES["returns"]).__aenter__()
        stream = _stream(client, family, source, DirectDispatch(concurrency=2))
        streams.append(stream)
        await anext(stream)
        transport.between.set()
        if call == "race":
            source.release.set()
            await reached(transport.raising)
        else:
            await reached(source.closing)
            releasing.append(asyncio.create_task(release_later()))
        try:
            if call == "close":
                await stream.aclose()
                seen.append("closed")
            else:
                await anext(stream)
        except StopAsyncIteration as error:
            seen.append(type(error))
        seen.append(stream.report)
        done.set()

    async def finish() -> None:
        seen_as_the_loop_raised.append(list(seen))
        await reached(done)
        await _read_and_close(streams[0], after)

    error = raised_driving_the_loop(main, finish)
    gc.collect()
    assert error is transport.raised[0]
    report = streams[0].report
    assert report is not None
    assert seen_as_the_loop_raised == [[]]
    assert seen == ["closed" if call == "close" else StopAsyncIteration, report]
    assert after == [StopAsyncIteration, "closed"]
    assert reported == []
    assert (report.state, _violations(report)) == (
        TerminalState.FAILED,
        [("interrupted", "operation interrupted (KeyboardInterrupt)")],
    )
    assert getattr(error, "report", None) is report


@pytest.mark.parametrize("family", REFERENCE_FAMILIES)
def test_a_close_overrunning_after_an_interrupt_between_reads_is_recorded_behind_it(family: str) -> None:
    # The source's close would wait until the test gives up. The event loop raises an interrupt between reads at
    # once; a close then waits for the operation's end, in which the operation's budget ends the source's close. The
    # report puts that close behind the interrupt, as it would for an interrupt in a read ("stream cleanup also
    # failed").
    reported: list[str] = []
    worked: list[bool] = []
    closed: list[bool] = []
    streams: list[OperationStream[Any]] = []
    transport = _InterruptsBetweenReads(KeyboardInterrupt)
    source = _Source(family, "overruns")

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
        client = await client_for(ResponderTransport(transport), policy=POLICIES["overruns"]).__aenter__()
        stream = _stream(client, family, source, DirectDispatch(concurrency=2))
        streams.append(stream)
        await anext(stream)
        transport.between.set()
        await asyncio.sleep(HANG_BOUND)
        worked.append(True)

    async def close() -> None:
        await streams[0].aclose()
        closed.append(True)

    error = raised_driving_the_loop(main, close)
    gc.collect()
    assert error is transport.raised[0]
    assert worked == []
    assert closed == [True]
    assert source.closes == 1
    assert not source.release.is_set()
    assert reported == []
    report = streams[0].report
    assert report is not None
    assert report.state is TerminalState.FAILED
    assert report.terminal_reason == "operation interrupted"
    assert _violations(report) == [
        ("cleanup_failure", "stream cleanup also failed (CleanupOverrunError)"),
        ("interrupted", "operation interrupted (KeyboardInterrupt)"),
    ]
    assert getattr(error, "report", None) is report


class _InterruptsAtShutdown:
    """Answer binding 0 (one row, then none) on direct or batch routes; binding 1's send raises the interrupt.

    ``cancel``: that send waits until ``asyncio.run``'s shutdown cancels it and raises the interrupt instead. Any
    other case: it raises it once the test sets ``go``. Every other binding's send waits for good.
    """

    def __init__(self, case: str, interrupt: type[BaseException]) -> None:
        self._case = case
        self._interrupt = interrupt
        self.sending = asyncio.Event()
        self.go = asyncio.Event()
        self.raised: list[BaseException] = []

    async def __call__(self, method: str, query: Query) -> Envelope:
        key = int(query.get("id") or query.get("filter[OWNER]") or 0)
        if key == 1:
            self.sending.set()
            if self._case == "cancel":
                try:
                    await asyncio.sleep(HANG_BOUND)
                except asyncio.CancelledError:
                    raise self._raise() from None
            await self.go.wait()
            raise self._raise()
        if key:
            await asyncio.sleep(HANG_BOUND)
        if method == "x.get":
            return {"result": {"ID": 1}}
        if "LAST_ID" in query or int(query.get("start") or 0):
            return {"result": [], "total": 0}
        return {"result": [{"ID": 10}], "total": 1}

    def _raise(self) -> BaseException:
        error = self._interrupt("binding 1")
        self.raised.append(error)
        return error


# A loop that stops on the interrupt during its shutdown never runs again the tasks it had left: the garbage collector
# closes them, and asyncio warns about each; a source pull it closes asks for its source's close with no loop to run it.
@pytest.mark.filterwarnings("ignore:coroutine .* was never awaited:RuntimeWarning")
@pytest.mark.filterwarnings("ignore:There is no current event loop:DeprecationWarning")
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("dispatch", ["direct", "batch"])
@pytest.mark.parametrize("family", REFERENCE_FAMILIES)
@pytest.mark.parametrize("case", ["cancel", "same_turn", "waiter", "control"])
def test_an_interrupt_between_reads_leaves_asyncio_run_however_its_shutdown_meets_it(
    case: str, family: str, dispatch: str, interrupt: type[BaseException]
) -> None:
    # The reader holds an item when binding 1's send raises the interrupt. It leaves ``asyncio.run``, once and without
    # hanging, however the loop's shutdown meets the operation's end that follows: the shutdown's own cancellation of
    # the send raised it (``cancel``), it was raised in the loop turn that stops the loop as ``main`` returned
    # (``same_turn``), a consumer waits between reads inside ``async with stream``, whose exit the shutdown would run
    # (``waiter``), or ``main`` still runs as it is raised (``control``). Only in ``control`` does the loop raise it
    # while ``main`` runs: the shutdown then runs the consumer's close, which is the end, closing the source once and
    # reporting the interrupt alone. In every other case the loop raises it from the shutdown itself, which stops
    # there: the end never begins, the source is never closed, and there is no report.
    reported: list[str] = []
    streams: list[OperationStream[Any]] = []
    transport = _InterruptsAtShutdown(case, interrupt)
    plan = DirectDispatch(concurrency=2) if dispatch == "direct" else BatchDispatch(batch_size=1, concurrency=2)
    holding = asyncio.Event()
    source = _Source(family, "returns")

    async def consume(stream: OperationStream[Any]) -> None:
        async with stream:
            await anext(stream)
            holding.set()
            await asyncio.sleep(HANG_BOUND)

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
        client = await client_for(BatchPortal(transport), policy=POLICIES["returns"]).__aenter__()
        stream = _stream(client, family, source, plan)
        streams.append(stream)
        if case in {"waiter", "control"}:
            consumer = asyncio.create_task(consume(stream))
            await reached(holding)
        else:
            await anext(stream)
        await reached(transport.sending)
        if case == "cancel":
            return
        transport.go.set()
        if case == "control":
            await asyncio.sleep(HANG_BOUND)
            del consumer

    error = raised_by_asyncio_run(main)
    gc.collect()
    assert error is transport.raised[0]
    assert reported == []
    report = streams[0].report
    if case == "control":
        assert report is not None
        assert report.terminal_reason == "operation interrupted"
        assert _violations(report) == [("interrupted", f"operation interrupted ({interrupt.__name__})")]
        assert getattr(error, "report", None) is report
        assert source.closes == 1
    else:
        assert (report, source.closes) == (None, 0)


SHAPE_FAMILIES = (*FAMILIES, "iter_list", "iter_list_counted")


@pytest.mark.parametrize("ending", ["cancel", "aclose"])
@pytest.mark.parametrize("family", SHAPE_FAMILIES)
def test_an_interrupt_a_cancelled_send_raises_ends_every_family_in_one_shape(family: str, ending: str) -> None:
    # The reader's cancellation, or a close from another task, cancels the read while its send waits, and the
    # cancelled send raises KeyboardInterrupt instead. That cancellation is the reader's or the close's own, never a
    # failed close: every family reports the interrupt alone, a kernel reading inline in the read included.
    reported: list[str] = []
    got: list[BaseException] = []
    streams: list[OperationStream[Any]] = []
    transport = _Interrupting("converted")

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
        async with client_for(ResponderTransport(transport), policy=POLICIES["returns"]) as client:
            if family.startswith("iter_list"):
                stream = getattr(client, family)(LIST_REQUEST)
            else:
                stream = _stream(client, family, _items(family))
            streams.append(stream)

            async def read() -> None:
                try:
                    async for _item in stream:
                        pass
                except BaseException as error:  # noqa: BLE001 - the test inspects what the reader got
                    got.append(error)

            reader = asyncio.create_task(read())
            await reached(transport.started)
            if ending == "cancel":
                reader.cancel()
            elif (closed := await _settled(stream.aclose())) is not None:
                got.append(closed)
            await reader
        raise got[0]

    with pytest.raises(KeyboardInterrupt):
        asyncio.run(main())
    gc.collect()
    assert {type(error) for error in got} == {KeyboardInterrupt}
    assert reported == []
    report = streams[0].report
    assert report is not None
    assert report.state is TerminalState.FAILED
    assert report.terminal_reason == "operation interrupted"
    assert _violations(report) == [("interrupted", "operation interrupted (KeyboardInterrupt)")]


@pytest.mark.parametrize("family", REFERENCE_FAMILIES)
def test_a_close_that_fails_after_an_interrupt_ended_the_read_is_behind_it(family: str) -> None:
    # The interrupt ends the read in flight (it cancels that read) and so the stream, before its close: the close
    # that then fails is behind it ("stream cleanup also failed"), as behind any end that came first, and the
    # interrupt's violation comes last.
    pending: list[str] = []
    got: list[BaseException] = []
    streams: list[OperationStream[Any]] = []
    transport = _Interrupting("queued")

    async def main() -> None:
        try:
            async with client_for(ResponderTransport(transport), policy=POLICIES["raises"]) as client:
                stream = _stream(client, family, _Source(family, "raises"), DirectDispatch(concurrency=2))
                streams.append(stream)

                async def read() -> None:
                    try:
                        await _end(stream, "read_to_end")
                    except BaseException as error:  # noqa: BLE001 - the test inspects what the reader got
                        got.append(error)

                await asyncio.create_task(read())
        finally:
            pending.extend(_others())
        raise got[0]

    with pytest.raises(KeyboardInterrupt):
        asyncio.run(main())
    gc.collect()
    assert pending == []
    report = streams[0].report
    assert report is not None
    assert report.state is TerminalState.FAILED
    assert report.terminal_reason == "operation interrupted"
    assert _violations(report) == [
        ("cleanup_failure", "stream cleanup also failed (InputSourceError)"),
        ("interrupted", "operation interrupted (KeyboardInterrupt)"),
    ]


@pytest.mark.parametrize("family", REFERENCE_FAMILIES)
def test_an_interrupt_a_close_raises_behind_the_one_that_ended_the_read_is_reported_not_raised(family: str) -> None:
    # The interrupt ends the read, and the source's close then raises a second one (``SystemExit``): the stream raises
    # the first, out of ``asyncio.run``, and reports the second to the loop's exception handler, once, raw. Raised
    # from the loop, it would stop it again before the client's close ran: that close still closes the client's other
    # open stream, and the source is closed once.
    reported: list[BaseException] = []
    got: list[BaseException] = []
    source = _Source(family, "exits")
    other = _HeldSource(family)
    interrupting = _Interrupting("queued")

    async def transport(request: Request) -> object:
        owner = _owner(request)
        if isinstance(owner, int) and owner >= HELD_FIRST:
            return _answer(request)
        return await interrupting(request)

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["exception"]))
        async with client_for(ResponderTransport(transport), policy=POLICIES["raises"]) as client:
            held = _stream(client, family, other, DirectDispatch(concurrency=1))
            await anext(held)
            stream = _stream(client, family, source, DirectDispatch(concurrency=2))

            async def read() -> None:
                try:
                    await _end(stream, "read_to_end")
                except BaseException as error:  # noqa: BLE001 - the test inspects what the reader got
                    got.append(error)

            await asyncio.create_task(read())
        raise got[0]

    with pytest.raises(KeyboardInterrupt) as raised:
        asyncio.run(main())
    gc.collect()
    assert raised.value is got[0]
    assert reported == [source.exit]
    assert (source.closes, other.closes) == (1, 1)


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
def test_an_interrupt_the_clients_close_raises_leaves_ahead_of_the_body_exception(
    interrupt: type[BaseException],
) -> None:
    # The client's exit closes a stream still open, whose source's close raises an interrupt: it leaves ahead of the
    # exception the body raised, which is not put in front of it.
    reported: list[str] = []

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
        async with client_for(ResponderTransport(_answer), policy=POLICIES["returns"]) as client:
            stream = _stream(client, "fan_out", _AsyncClosesInterrupted("fan_out", interrupt, fail_pull=False))
            await anext(stream)
            raise _OwnError

    with pytest.raises(interrupt):
        asyncio.run(main())
    gc.collect()
    assert reported == []


class _PullInterruptedInItsThread:
    """A synchronous source whose second pull, in its worker thread, waits for ``release`` and then is interrupted."""

    def __init__(self, family: str) -> None:
        self._items = iter(_items(family))
        self._pulled = 0
        self.pulling = threading.Event()
        self.release = threading.Event()

    def __iter__(self) -> _PullInterruptedInItsThread:
        return self

    def __next__(self) -> Any:  # noqa: ANN401 - commands, bindings or reference sources
        self._pulled += 1
        if self._pulled == ITEMS:
            self.pulling.set()
            self.release.wait(HANG_BOUND)
            raise KeyboardInterrupt
        return next(self._items)


@pytest.mark.parametrize("family", FAMILIES)
def test_an_interrupt_a_pull_cancelled_in_its_thread_ends_in_reaches_the_reader(family: str) -> None:
    # The reader is cancelled while a synchronous pull runs in its worker thread, which the cancellation waits for:
    # the pull then ends in an interrupt, which is not dropped with the cancelled pull's result but ends the operation.
    reported: list[str] = []
    pending: list[str] = []
    got: list[BaseException] = []
    source = _PullInterruptedInItsThread(family)

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
        try:
            async with client_for(ResponderTransport(_answer), policy=POLICIES["returns"]) as client:
                stream = _stream(client, family, source)

                async def read() -> None:
                    try:
                        await _end(stream, "read_to_end")
                    except BaseException as error:  # noqa: BLE001 - the test inspects what the reader got
                        got.append(error)

                reader = asyncio.create_task(read())
                while not source.pulling.is_set():  # noqa: ASYNC110 - the pull waits in a worker thread
                    await asyncio.sleep(0.001)
                reader.cancel()
                await asyncio.sleep(0)
                source.release.set()
                await reader
        finally:
            source.release.set()
            pending.extend(_others())
        raise got[0]

    with pytest.raises(KeyboardInterrupt):
        asyncio.run(main())
    gc.collect()
    assert [type(error) for error in got] == [KeyboardInterrupt]
    assert reported == []
    assert pending == []


INLINE_CASES = [
    ("iter_list", "returns"),
    ("iter_list_counted", "returns"),
    ("batch", "returns"),
    ("batch", "raises"),
    ("batch_outcomes", "returns"),
    ("batch_outcomes", "raises"),
]


@pytest.mark.parametrize(("family", "close"), INLINE_CASES)
def test_an_interrupt_an_inline_kernels_send_raises_ends_the_operation_in_one_shape(family: str, close: str) -> None:
    # A kernel reading inline in its stream's read sends on the reader's own path: its read is where the interrupt
    # is first caught, and it is latched there, also when the kernel's cleanup then fails (the source's close
    # raises), which is what the kernel ends with. The operation raises the interrupt, with its report attached, in
    # the one shape: no cleanup failure of its own, the source's failed close behind it.
    reported: list[str] = []
    streams: list[OperationStream[Any]] = []

    async def interrupting(request: Request) -> object:
        del request
        raise KeyboardInterrupt

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
        async with client_for(ResponderTransport(interrupting), policy=POLICIES[close]) as client:
            if family.startswith("iter_list"):
                stream = getattr(client, family)(LIST_REQUEST)
            else:
                stream = _stream(client, family, _Source(family, close))
            streams.append(stream)
            async for _item in stream:
                pass

    with pytest.raises(KeyboardInterrupt) as raised:
        asyncio.run(main())
    gc.collect()
    assert reported == []
    report = streams[0].report
    assert report is not None
    assert report.state is TerminalState.FAILED
    assert report.terminal_reason == "operation interrupted"
    behind = [("cleanup_failure", "stream cleanup also failed (InputSourceError)")] if close == "raises" else []
    assert _violations(report) == [*behind, ("interrupted", "operation interrupted (KeyboardInterrupt)")]
    # The interrupt carries the report, as a cancellation does.
    attached = getattr(raised.value, "report", None)
    assert attached is report


class _AsyncClosesLate:
    """A caller's source whose close outlives the cleanup window, resisting its cancellation, then raises ``escape``."""

    def __init__(self, family: str, escape: type[BaseException]) -> None:
        self._items = iter(_items(family))
        self._escape = escape
        self.release = asyncio.Event()
        self.raised: list[BaseException] = []

    def __aiter__(self) -> _AsyncClosesLate:
        return self

    async def __anext__(self) -> Any:  # noqa: ANN401 - commands
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration from None

    async def aclose(self) -> None:
        while not self.release.is_set():
            with contextlib.suppress(asyncio.CancelledError):
                await self.release.wait()
        self.raised.append(self._escape("late close"))
        raise self.raised[-1]


class _SyncClosesLate:
    """``_AsyncClosesLate`` as a synchronous iterator: its close waits in its worker thread, then raises ``escape``."""

    def __init__(self, family: str, escape: type[BaseException]) -> None:
        self._items = iter(_items(family))
        self._escape = escape
        self.release = threading.Event()
        self.raised: list[BaseException] = []

    def __iter__(self) -> _SyncClosesLate:
        return self

    def __next__(self) -> Any:  # noqa: ANN401 - commands
        return next(self._items)

    def close(self) -> None:
        self.release.wait(HANG_BOUND)
        self.raised.append(self._escape("late close"))
        raise self.raised[-1]


class _Handler:
    """The event loop's exception handler: it keeps every context it gets."""

    def __init__(self) -> None:
        self.contexts: list[dict[str, object]] = []
        self.arrived = asyncio.Event()

    def __call__(self, _loop: asyncio.AbstractEventLoop, context: dict[str, object]) -> None:
        self.contexts.append(context)
        self.arrived.set()


async def _handed_on(handler: _Handler) -> None:
    """Wait until the event loop's exception handler got something, then a few turns more for anything after it.

    An interrupt stops the loop instead, out of ``asyncio.run``.
    """
    async with asyncio.timeout(HANG_BOUND):
        await handler.arrived.wait()
    await _turns()


def _handed_late(
    escape: type[BaseException], main: Callable[[], Coroutine[Any, Any, None]], raised: list[BaseException]
) -> None:
    """Run ``main``, which ends an operation and then lets its work raise ``escape``: no read or close raises it.

    An interrupt leaves the event loop out of ``asyncio.run``, as any task's interrupt does. Any other escape goes to
    the loop's exception handler, once, with that very object.
    """
    if is_interrupt(escape("probe")):
        with pytest.raises(escape, match="late"):
            asyncio.run(main())
        return
    asyncio.run(main())
    assert len(raised) == 1


@pytest.mark.parametrize("escape", [KeyboardInterrupt, SystemExit, _NotAnInterruptError])
@pytest.mark.parametrize("kind", ["async", "sync"])
@pytest.mark.parametrize("family", ["fan_out", "batch"])
def test_an_escape_raised_after_the_operation_ended_goes_to_the_event_loop_once(
    family: str, kind: str, escape: type[BaseException]
) -> None:
    # The source's close outlives the cleanup window: the stream ends without it (a cleanup overrun) and its operation
    # raises nothing more. The close then raises an escape, which nobody would raise any longer: it is not kept but
    # goes to the event loop: an interrupt leaves it out of asyncio.run, and any other reaches its exception handler,
    # once, with the stream's report.
    handler = _Handler()
    got: list[type[BaseException]] = []
    streams: list[OperationStream[Any]] = []
    source = _AsyncClosesLate(family, escape) if kind == "async" else _SyncClosesLate(family, escape)

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(handler)
        try:
            async with client_for(ResponderTransport(_answer), policy=POLICIES["overruns"]) as client:
                stream = _stream(client, family, source)
                streams.append(stream)
                try:
                    async for _item in stream:
                        pass
                except Exception as error:  # noqa: BLE001 - the test inspects what the reader got
                    got.append(type(error))
                source.release.set()
                await _handed_on(handler)
        finally:
            source.release.set()

    _handed_late(escape, main, source.raised)
    gc.collect()
    assert got == [_raised_for(family, "overruns")]
    _assert_handed(handler.contexts, escape, source.raised, streams[0].report)


def _assert_handed(
    handed: list[dict[str, object]], escape: type[BaseException], raised: list[BaseException], report: object
) -> None:
    """Nothing reached the loop's exception handler but a late escape that is no interrupt, once, with the report."""
    if is_interrupt(escape("probe")):
        assert handed == []
        return
    assert [context["exception"] for context in handed] == raised
    assert handed[0]["report"] is report


class _SendInterruptedLate:
    """Answer the first command; hold the second's send, ignoring its cancellation, until released; raise ``escape``."""

    def __init__(self, escape: type[BaseException]) -> None:
        self._escape = escape
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.raised: list[BaseException] = []

    async def __call__(self, request: Request) -> object:
        if request.method != "batch" and request.copy_parameters().get("id") == 0:
            return _answer(request)
        self.started.set()
        while not self.release.is_set():
            with contextlib.suppress(asyncio.CancelledError):
                await self.release.wait()
        self.raised.append(self._escape("late send"))
        raise self.raised[-1]


@pytest.mark.parametrize("escape", [KeyboardInterrupt, SystemExit, _NotAnInterruptError])
@pytest.mark.parametrize(
    "dispatch", [DirectDispatch(concurrency=2), BatchDispatch(batch_size=1, concurrency=2)], ids=["direct", "batch"]
)
def test_an_escape_a_send_raises_after_the_operation_ended_goes_to_the_event_loop_once(
    dispatch: DirectDispatch | BatchDispatch, escape: type[BaseException]
) -> None:
    # A send outlives the cleanup window (its binding's task, or the batch sender, ignores the cancellation): the
    # stream's close ends without it (a cleanup overrun), and the escape it raises afterwards goes to the event loop:
    # an interrupt leaves it out of asyncio.run, and any other reaches its exception handler, once.
    handler = _Handler()
    got: list[type[BaseException]] = []
    streams: list[OperationStream[Any]] = []
    transport = _SendInterruptedLate(escape)

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(handler)
        try:
            async with client_for(ResponderTransport(transport), policy=POLICIES["overruns"]) as client:
                stream = client.fan_out_outcomes(_items("fan_out_outcomes"), dispatch=dispatch)
                streams.append(stream)
                read = asyncio.create_task(_settled(anext(stream)))
                await reached(transport.started)
                closed = await _settled(stream.aclose())
                assert closed is not None
                got.append(type(closed))
                await read
                transport.release.set()
                await _handed_on(handler)
        finally:
            transport.release.set()

    _handed_late(escape, main, transport.raised)
    gc.collect()
    assert got == [CleanupOverrunError]
    _assert_handed(handler.contexts, escape, transport.raised, streams[0].report)


class _OwnError(Exception):
    """The caller's own code failed inside ``async with stream``."""


@pytest.mark.asyncio
@pytest.mark.parametrize("close", ["raises", "overruns"])
@pytest.mark.parametrize("kind", ["async", "sync"])
@pytest.mark.parametrize("family", FAMILIES)
async def test_the_callers_own_exception_carries_the_reports_record_of_a_failed_close(
    family: str, kind: str, close: str
) -> None:
    # The caller's own exception stays what is raised; its one note is the report's violation, word for word.
    source = SOURCE_KINDS[kind](family, close)
    async with client_for(ResponderTransport(_answer), policy=POLICIES[close]) as client:
        stream = _stream(client, family, source)

        async def body() -> None:
            async with stream:
                await anext(stream)
                raise _OwnError

        try:
            with pytest.raises(_OwnError) as raised:
                await asyncio.wait_for(body(), timeout=HANG_BOUND)
        finally:
            source.release.set()
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.EARLY_CLOSED
    assert report.terminal_reason == "stream cleanup failed"
    assert _violations(report) == [("cleanup_failure", f"stream cleanup failed ({CLOSE_FAILURES[close]})")]
    assert raised.value.__notes__ == [report.violations[0].message]


class _FailsWhenCut:
    """Its second pull waits until cancelled and then fails; its close fails too."""

    def __init__(self, family: str) -> None:
        self._items = iter(_items(family))
        self._pulled = 0
        self.pulling = asyncio.Event()

    def __aiter__(self) -> _FailsWhenCut:
        return self

    async def __anext__(self) -> Any:  # noqa: ANN401 - commands, bindings or reference sources
        self._pulled += 1
        if self._pulled == ITEMS:
            self.pulling.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise ValueError("the caller's pull failed as it was cut") from None
        return next(self._items)

    async def aclose(self) -> None:
        raise RuntimeError("the caller's close failed")


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_failed_close_after_a_pull_the_close_cut_keeps_its_own_cause(family: str) -> None:
    # A close cuts a pull, which fails as it is cut; the source's close then fails as well. The close failure
    # already chains to its own cause, the caller's close exception: the pull's failure does not replace it.
    source = _FailsWhenCut(family)
    async with client_for(ResponderTransport(_answer), policy=POLICIES["returns"]) as client:
        stream = _stream(client, family, source)
        read = asyncio.create_task(_end(stream, "read_to_end"))
        await reached(source.pulling)
        closed = await _settled(stream.aclose())
        read_end = await _settled(read)

    for raised in (closed, read_end):
        assert type(raised) is SOURCE_FAILURE[family]
        assert _chain(raised)[-2:] == [InputSourceError, RuntimeError]
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.EARLY_CLOSED
    assert _violations(report) == [("cleanup_failure", "stream cleanup failed (InputSourceError)")]


class _Gate:
    """Where a source's second pull waits, and the release that only the event loop's shutdown gives it.

    An asynchronous source waits on the loop, so the shutdown's cancellation reaches it there. A synchronous one
    waits in its worker thread, which no cancellation reaches: a task of the test's own releases it as the shutdown
    cancels that task, after it has asked every task to cancel.
    """

    def __init__(self) -> None:
        self.pulling = threading.Event()
        self._release = threading.Event()

    def sync_source(self, family: str) -> Iterator[Any]:
        first, second = _items(family)
        try:
            yield first
            self.pulling.set()
            self._release.wait(HANG_BOUND)
            yield second
        finally:
            raise RuntimeError("the caller's close failed")

    async def async_source(self, family: str) -> AsyncIterator[Any]:
        first, _second = _items(family)
        try:
            yield first
            self.pulling.set()
            await asyncio.Event().wait()
        finally:
            raise RuntimeError("the caller's close failed")

    async def released_at_shutdown(self) -> None:
        try:
            await asyncio.Event().wait()
        finally:
            self._release.set()


@pytest.mark.parametrize("ending", ["shutdown_in_pull", "shutdown_in_close"])
@pytest.mark.parametrize("kind", ["sync", "async"])
@pytest.mark.parametrize("family", FAMILIES)
def test_no_library_task_ends_in_a_failure_when_the_event_loop_shuts_down(family: str, kind: str, ending: str) -> None:
    # ``asyncio.run`` cancels every task left when its coroutine returns, then reports each that ended in an
    # exception, retrieved or not. The source fails as it is cancelled: its ``finally`` raises. Only the reader takes
    # that failure; no task of the library's own (a pull, a close, a stream's read or cleanup) may end in it too.
    reports: list[dict[str, Any]] = []
    gate = _Gate()

    async def consume(stream: OperationStream[Any]) -> None:
        with contextlib.suppress(BaseException):
            async for _item in stream:
                pass

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reports.append(context))
        holder = asyncio.create_task(gate.released_at_shutdown())
        client = await contextlib.AsyncExitStack().enter_async_context(
            client_for(ResponderTransport(_answer), policy=POLICIES["returns"])
        )
        source = gate.sync_source(family) if kind == "sync" else gate.async_source(family)
        consumer = asyncio.create_task(consume(_stream(client, family, source)))
        await asyncio.wait_for(asyncio.to_thread(gate.pulling.wait, HANG_BOUND), timeout=HANG_BOUND)
        if ending == "shutdown_in_close":
            # The read the reader's cancellation ends closes the source after the pull it cut: shutdown meets that.
            consumer.cancel()
            for _turn in range(HANDOVER_TURNS):
                await asyncio.sleep(0)
        assert not holder.done()

    asyncio.run(main())

    assert [(context["message"], context.get("task")) for context in reports] == []


@pytest.mark.parametrize("family", REFERENCE_FAMILIES)
@pytest.mark.asyncio
async def test_a_close_of_a_stream_never_read_whose_coroutine_is_closed_still_ends_it(family: str) -> None:
    # The close is the stream's first step on its loop. Its coroutine is closed while the source's close waits, as a
    # loop that stops closes what is pending: the end goes on in a task of that loop and publishes its report.
    source = _ClosesWhenReleased(family)
    client = client_for(ResponderTransport(_answer), policy=POLICIES["returns"])
    async with asyncio.timeout(HANG_BOUND), client:
        stream = _stream(client, family, source, DirectDispatch(concurrency=2))
        closing = stream.aclose()
        closing.send(None)
        await reached(source.closing)
        closing.close()
        source.release.set()
        await _turns()
        assert stream.report is not None
        assert stream.report.state is TerminalState.EARLY_CLOSED
