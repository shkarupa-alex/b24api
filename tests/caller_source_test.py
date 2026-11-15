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
import threading
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
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator

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


def _stream(client: Bitrix24, family: str, source: Iterable[Any] | AsyncIterator[Any]) -> OperationStream[Any]:
    direct = DirectDispatch(concurrency=1)
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
    assert sorted(_violations(report)) == [
        ("cleanup_failure", f"stream cleanup also failed ({name})"),
        ("source_failure", f"{report.operation} failed (InputSourceError)"),
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
