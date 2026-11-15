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
import threading
import warnings
from typing import TYPE_CHECKING, Any

import pytest

from b24api import (
    BatchDispatch,
    Binding,
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
from b24api.contracts import Command, ReferenceSource
from b24api.errors import BatchFailed, BudgetExceededError, CleanupOverrunError, InputSourceError, PaginationError
from tests.scripting import DEADLINE_BUDGET, HANG_BOUND, ResponderTransport, client_for, reached

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Iterable, Iterator

    from b24api import Bitrix24
    from b24api.contracts.report import OperationReport
    from b24api.contracts.stream import OperationStream

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


def _items(family: str) -> list[Any]:
    if family in {"batch", "batch_outcomes", "fan_out", "fan_out_outcomes"}:
        return [
            Command(Request("x.get", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)
            for index in range(ITEMS)
        ]
    if family in {"iter_reference_sources", "iter_reference_source_outcomes"}:
        return [ReferenceSource(LIST_REQUEST, TRAVERSAL, _binding(index)) for index in range(ITEMS)]
    return [_binding(index) for index in range(ITEMS)]


class _Source:
    """A caller's source whose close returns, raises or blocks until released; it may fail after one item."""

    def __init__(
        self, family: str, close: str, *, fail_after: int | None = None, failure: Exception | None = None
    ) -> None:
        self._items = iter(_items(family))
        self._close = close
        self._fail_after = fail_after
        self._failure = failure or ValueError("the caller's source failed")
        self._pulled = 0
        self.release = asyncio.Event()
        self.closing = asyncio.Event()
        self.closed = False

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
        self.closed = True
        self.closing.set()
        if self._close == "raises":
            raise RuntimeError("the caller's close failed")
        if self._close == "overruns":
            await reached(self.release)


class _SyncSource:
    """``_Source`` as a synchronous iterator: pulled and closed in worker threads, its close blocking there."""

    def __init__(
        self, family: str, close: str, *, fail_after: int | None = None, failure: Exception | None = None
    ) -> None:
        self._items = iter(_items(family))
        self._close = close
        self._fail_after = fail_after
        self._failure = failure or ValueError("the caller's source failed")
        self._pulled = 0
        self.release = threading.Event()

    def __iter__(self) -> _SyncSource:
        return self

    def __next__(self) -> Any:  # noqa: ANN401 - commands, bindings or reference sources
        if self._pulled == self._fail_after:
            raise self._failure
        self._pulled += 1
        return next(self._items)

    def close(self) -> None:
        if self._close == "raises":
            raise RuntimeError("the caller's close failed")
        if self._close == "overruns":
            self.release.wait(HANG_BOUND)


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
            assert isinstance(await _settled(read), asyncio.CancelledError)

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
    assert source.closed
    if close == "returns":
        # The read the close ended raises the close's cancellation of it; the close itself succeeded.
        assert closed is None
        assert isinstance(read_end, asyncio.CancelledError)
        assert report.terminal_reason == "stream closed before exhaustion"
        assert _violations(report) == []
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
    # read (its cancellation, a timeout, another task's close), the loop reports nothing and no task is left.
    reported: list[str] = []
    pending: list[str] = []
    transport = _Interrupting(scenario)
    plan = DirectDispatch(concurrency=2) if dispatch == "direct" else BatchDispatch(batch_size=1, concurrency=2)
    got: list[BaseException] = []
    streams: list[OperationStream[Any]] = []

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
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


class _InterruptsBetweenReads:
    """Answer the first binding; the second binding's send raises the interrupt once the reader has an item."""

    def __init__(self, interrupt: type[BaseException]) -> None:
        self.interrupt = interrupt
        self.between = asyncio.Event()
        self.raised: list[BaseException] = []

    async def __call__(self, request: Request) -> object:
        parameters = request.copy_parameters()
        owner_filter = parameters.get("filter")
        owner = parameters.get("id", owner_filter.get("OWNER") if isinstance(owner_filter, dict) else None)
        if owner == 1:
            await reached(self.between)
            error = self.interrupt("between reads")
            self.raised.append(error)
            raise error
        return _answer(request)


BETWEEN_READS = ("next", "noclose", "abandon", "runloop")


def _raised_driving_the_loop(
    main: Callable[[], Coroutine[Any, Any, None]], after: Callable[[], Coroutine[Any, Any, None]]
) -> BaseException:
    """Drive ``main`` on a loop of the test's own until the loop raises; then run ``after`` there, raising nothing.

    Return what the loop raised. Running the loop again shows the interrupt was handed to it once.
    """
    loop = asyncio.new_event_loop()
    try:
        work = loop.create_task(main())
        try:
            loop.run_until_complete(work)
        except BaseException as error:  # noqa: BLE001 - the test inspects what the loop raised
            raised = error
        else:
            raise AssertionError("the loop raised nothing")
        loop.run_until_complete(after())
        work.cancel()
        loop.run_until_complete(asyncio.gather(work, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
    finally:
        loop.close()
    return raised


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("reader", BETWEEN_READS)
@pytest.mark.parametrize("family", REFERENCE_FAMILIES)
def test_an_interrupt_latched_between_reads_ends_the_operation_at_once(
    family: str, reader: str, interrupt: type[BaseException]
) -> None:
    # The reader holds an item and works elsewhere while another binding's send raises the interrupt: no read is in
    # flight to raise it, so the operation ends at once, its report in the interrupt's shape, and the event loop
    # raises it, once, before the reader's work is over: whether the reader would read again (``next``), never reads
    # nor closes again (``noclose``), never closes its client either (``abandon``), or drives the loop itself
    # (``runloop``). The stream is ended: a later read finds it so, a close returns, and neither raises it again.
    reported: list[str] = []
    worked: list[bool] = []
    after: list[object] = []
    streams: list[OperationStream[Any]] = []
    transport = _InterruptsBetweenReads(interrupt)

    async def body(client: Bitrix24) -> None:
        stream = _stream(client, family, _items(family), DirectDispatch(concurrency=2))
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

    async def read_after() -> None:
        stream = streams[0]
        try:
            await anext(stream)
        except StopAsyncIteration as error:
            after.append(type(error))
        await stream.aclose()
        after.append("closed")

    if reader == "runloop":
        error = _raised_driving_the_loop(main, read_after)
        assert after == [StopAsyncIteration, "closed"]
    else:
        with pytest.raises(interrupt) as raised:
            asyncio.run(main())
        error = raised.value
    gc.collect()
    assert error is transport.raised[0]
    assert worked == []
    assert reported == []
    report = streams[0].report
    assert report is not None
    assert report.state is TerminalState.FAILED
    assert report.terminal_reason == "operation interrupted"
    assert _violations(report) == [("interrupted", f"operation interrupted ({interrupt.__name__})")]
    assert getattr(error, "report", None) is report


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
    """A caller's source whose close outlives the cleanup window, resisting its cancellation, then is interrupted."""

    def __init__(self, family: str, interrupt: type[BaseException]) -> None:
        self._items = iter(_items(family))
        self._interrupt = interrupt
        self.release = asyncio.Event()

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
        raise self._interrupt("late close")


class _SyncClosesLate:
    """``_AsyncClosesLate`` as a synchronous iterator: its close waits in its worker thread, then is interrupted."""

    def __init__(self, family: str, interrupt: type[BaseException]) -> None:
        self._items = iter(_items(family))
        self._interrupt = interrupt
        self.release = threading.Event()

    def __iter__(self) -> _SyncClosesLate:
        return self

    def __next__(self) -> Any:  # noqa: ANN401 - commands
        return next(self._items)

    def close(self) -> None:
        self.release.wait(HANG_BOUND)
        raise self._interrupt("late close")


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("kind", ["async", "sync"])
@pytest.mark.parametrize("family", ["fan_out", "batch"])
def test_an_interrupt_raised_after_the_operation_ended_stops_the_loop(
    family: str, kind: str, interrupt: type[BaseException]
) -> None:
    # The source's close outlives the cleanup window: the stream ends without it (a cleanup overrun) and its operation
    # raises nothing more. The close then raises an interrupt, which nobody would raise any longer: it is not kept
    # but leaves the event loop out of asyncio.run, as any task's interrupt does.
    reported: list[str] = []
    got: list[type[BaseException]] = []
    source = _AsyncClosesLate(family, interrupt) if kind == "async" else _SyncClosesLate(family, interrupt)

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
        try:
            async with client_for(ResponderTransport(_answer), policy=POLICIES["overruns"]) as client:
                stream = _stream(client, family, source)
                try:
                    async for _item in stream:
                        pass
                except Exception as error:  # noqa: BLE001 - the test inspects what the reader got
                    got.append(type(error))
                source.release.set()
                await asyncio.sleep(HANG_BOUND)
        finally:
            source.release.set()

    with pytest.raises(interrupt, match="late close"):
        asyncio.run(main())
    gc.collect()
    assert got == [_raised_for(family, "overruns")]
    assert reported == []


class _SendInterruptedLate:
    """Answer the first command; block the second's send, ignoring its cancellation, until released, then interrupt."""

    def __init__(self, interrupt: type[BaseException]) -> None:
        self._interrupt = interrupt
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, request: Request) -> object:
        if request.method != "batch" and request.copy_parameters().get("id") == 0:
            return _answer(request)
        self.started.set()
        while not self.release.is_set():
            with contextlib.suppress(asyncio.CancelledError):
                await self.release.wait()
        raise self._interrupt("late send")


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize(
    "dispatch", [DirectDispatch(concurrency=2), BatchDispatch(batch_size=1, concurrency=2)], ids=["direct", "batch"]
)
def test_an_interrupt_a_send_raises_after_the_operation_ended_stops_the_loop(
    dispatch: DirectDispatch | BatchDispatch, interrupt: type[BaseException]
) -> None:
    # A send outlives the cleanup window (its binding's task, or the batch sender, ignores the cancellation): the
    # stream's close ends without it (a cleanup overrun), and the interrupt it raises afterwards leaves the event loop
    # out of asyncio.run.
    reported: list[str] = []
    got: list[type[BaseException]] = []
    transport = _SendInterruptedLate(interrupt)

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context["message"]))
        try:
            async with client_for(ResponderTransport(transport), policy=POLICIES["overruns"]) as client:
                stream = client.fan_out_outcomes(_items("fan_out_outcomes"), dispatch=dispatch)
                read = asyncio.create_task(_settled(anext(stream)))
                await reached(transport.started)
                closed = await _settled(stream.aclose())
                assert closed is not None
                got.append(type(closed))
                await read
                transport.release.set()
                await asyncio.sleep(HANG_BOUND)
        finally:
            transport.release.set()

    with pytest.raises(interrupt, match="late send"):
        asyncio.run(main())
    gc.collect()
    assert got == [CleanupOverrunError]
    assert reported == []


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
