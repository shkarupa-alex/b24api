"""One report for a failed close, in every stream family that reads a caller's source.

A close that raised, or that overran its cleanup window (``CleanupOverrunError``), is reported the same
way whichever family read the source and however the stream ended. A close that raised is the source
failing: it raises the family's source failure (``InputSourceError``, wrapped by ``BatchFailed`` in a
fail-fast batch family) chained to the caller's exception, as a failed pull does. When nothing failed before it, the
close is what failed (terminal reason "stream cleanup failed", one ``cleanup_failure`` violation
"stream cleanup failed (X)", no other); behind an earlier failure it is that failure's secondary
("stream cleanup also failed (X)") and the earlier failure stays the terminal reason.
"""

from __future__ import annotations
import asyncio
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
    from collections.abc import AsyncIterator, Awaitable, Callable

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
        if self._close == "raises":
            raise RuntimeError("the caller's close failed")
        if self._close == "overruns":
            await reached(self.release)


def _stream(client: Bitrix24, family: str, source: _Source) -> OperationStream[Any]:
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


async def _close_fails(
    family: str, close: str, ending: str, *, fail_after: int | None = None, failure: Exception | None = None
) -> tuple[BaseException, OperationReport]:
    source = _Source(family, close, fail_after=fail_after, failure=failure)
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
@pytest.mark.parametrize("close", ["raises", "overruns"])
@pytest.mark.parametrize("ending", ["read_to_end", "break", "aclose_before_read"])
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_failed_close_is_what_failed_when_nothing_failed_before_it(
    family: str, ending: str, close: str
) -> None:
    error, report = await _close_fails(family, close, ending)

    name = CLOSE_FAILURES[close]
    assert type(error) is _raised_for(family, close)
    if close == "raises":
        assert _chain(error)[-2:] == [InputSourceError, RuntimeError]
    assert report.state is (TerminalState.FAILED if ending == "read_to_end" else TerminalState.EARLY_CLOSED)
    assert report.terminal_reason == "stream cleanup failed"
    # The close's failure alone: no classification of it as the work's failure, no budget end, no "also".
    assert _violations(report) == [("cleanup_failure", f"stream cleanup failed ({name})")]


@pytest.mark.asyncio
@pytest.mark.parametrize("close", ["raises", "overruns"])
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_failed_close_behind_an_earlier_failure_is_its_secondary(family: str, close: str) -> None:
    error, report = await _close_fails(family, close, "read_to_end", fail_after=1)

    name = CLOSE_FAILURES[close]
    assert type(error) is SOURCE_FAILURE[family]
    assert _chain(error)[-2:] == [InputSourceError, ValueError]
    assert report.state is TerminalState.FAILED
    assert report.terminal_reason == "InputSourceError"
    assert sorted(_violations(report)) == [
        ("cleanup_failure", f"stream cleanup also failed ({name})"),
        ("source_failure", f"{report.operation} failed (InputSourceError)"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [BudgetExceededError("the caller's budget"), PaginationError("the caller's page")])
@pytest.mark.parametrize("fail_after", [0, 1])
@pytest.mark.parametrize("family", FAMILIES)
async def test_a_source_raising_a_library_error_of_its_own_is_its_failure(
    family: str, fail_after: int, failure: Exception
) -> None:
    # Whichever pull raises it, the first of a batch window included, it is neither the operation's budget end
    # nor a failed page: the source failed, and the stream says so with the caller's exception as the cause.
    error, report = await _close_fails(family, "raises", "read_to_end", fail_after=fail_after, failure=failure)

    assert type(error) is SOURCE_FAILURE[family]
    assert _chain(error)[-2:] == [InputSourceError, type(failure)]
    assert report.state is TerminalState.FAILED
    assert report.terminal_reason == "InputSourceError"
    assert "budget_exhausted" not in {code for code, _message in _violations(report)}


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


def _commands(source_closed: asyncio.Event) -> AsyncIterator[Command[int]]:
    async def commands() -> AsyncIterator[Command[int]]:
        try:
            for item in _items("fan_out"):
                yield item
        finally:
            source_closed.set()

    return commands()


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
    source_closed = asyncio.Event()
    async with client_for(ResponderTransport(transport), policy=POLICIES["overruns"]) as client:
        stream = client.fan_out_outcomes(_commands(source_closed), dispatch=dispatch)
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
    assert source_closed.is_set()
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
    source_closed = asyncio.Event()
    async with client_for(ResponderTransport(transport), policy=POLICIES["overruns"]) as client:
        stream = client.fan_out_outcomes(_commands(source_closed), dispatch=dispatch)
        read = asyncio.create_task(anext(stream))
        try:
            await reached(transport.started)
            read.cancel()
            read_end = await _settled(read)
        finally:
            transport.release.set()

    assert isinstance(read_end, asyncio.CancelledError)
    assert source_closed.is_set()
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.CANCELLED
    assert report.terminal_reason == "iteration cancelled"
    assert _violations(report) == [("cleanup_failure", "stream cleanup also failed (CleanupOverrunError)")]
