"""Stream lifecycle: cleanup failures publish a report, repeated closes are no-ops, reasons are public.

The rows follow the §3.1 transition table on the logical batch family (``batch``/``batch_outcomes``)
and on the reference-backed fan-out family (``fan_out``/``fan_out_outcomes``): the report is
published after cleanup even when the caller's source fails to close (A11), an early close is
``stream closed before exhaustion`` rather than ``GeneratorExit`` (A12), and a report never names a
private carrier exception (A14).
"""

from __future__ import annotations
import asyncio
import json
from typing import TYPE_CHECKING, Any

import pytest

from b24api import (
    BatchDispatch,
    Binding,
    Bitrix24,
    OffsetSpec,
    ReplaySafety,
    Request,
    RouteKind,
    SequentialTraversal,
    Settings,
    TerminalState,
)
from b24api.contracts import Command
from b24api.contracts.report import OperationReport
from b24api.errors import B24ApiError, BatchFailed, InputSourceError
from b24api.transport import WireResponse

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from b24api.contracts.stream import OperationStream

HOST = "fixture.invalid"
COMMANDS = 6
BATCH = 2
FAILING_COMMAND = 3
FAMILIES = ("batch", "batch_outcomes", "fan_out", "fan_out_outcomes")
LOGICAL = ("batch", "batch_outcomes")


class _Portal:
    """Answer batch commands; command ``FAILING_COMMAND`` fails, others return their id."""

    host = HOST

    def __init__(self, *, block: bool = False) -> None:
        self.block = block
        self.entered = asyncio.Event()

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        if self.block:
            self.entered.set()
            await asyncio.Event().wait()
        parameters = request.copy_parameters()
        if request.method == "batch":
            commands = parameters["cmd"]
            assert isinstance(commands, dict)
            results = {key: 1 for key, command in commands.items() if f"id={FAILING_COMMAND}" not in str(command)}
            errors = {
                key: {"error": "NOT_FOUND", "error_description": "missing"}
                for key, command in commands.items()
                if f"id={FAILING_COMMAND}" in str(command)
            }
            body: object = {"result": {"result": results, "result_error": errors}}
        else:
            body = {"result": [], "total": 0}
        return WireResponse(200, (("content-type", "application/json"),), json.dumps(body).encode())

    async def aclose(self) -> None:
        return None


class _CloseFailedError(Exception):
    """The caller's source failed to close."""


class _SourceFailedError(Exception):
    """The caller's source failed to produce the next command."""


class _Source:
    """An async command source whose ``aclose`` can fail, observable by tests."""

    def __init__(self, *, fail_at: int | None = None, close_fails: bool = False, failing: bool = False) -> None:
        self.produced = 0
        self.fail_at = fail_at
        self.close_fails = close_fails
        self.failing = failing
        self.closes = 0

    def __aiter__(self) -> _Source:
        return self

    async def __anext__(self) -> Command[int]:
        if self.fail_at is not None and self.produced == self.fail_at:
            raise _SourceFailedError
        if self.produced >= COMMANDS:
            raise StopAsyncIteration
        self.produced += 1
        identity = self.produced if self.failing else self.produced + COMMANDS
        return Command(Request("x.get", {"id": identity}, ReplaySafety.SAFE, route=RouteKind.BARE), self.produced)

    async def aclose(self) -> None:
        self.closes += 1
        if self.close_fails:
            raise _CloseFailedError


def _client(portal: _Portal) -> Bitrix24:
    return Bitrix24(Settings(webhook_url=f"https://{HOST}/rest/1/life/"), transport=portal)


def _stream(client: Bitrix24, family: str, source: _Source) -> OperationStream[Any]:
    if family == "batch":
        return client.batch(source, batch_size=BATCH)
    if family == "batch_outcomes":
        return client.batch_outcomes(source, batch_size=BATCH)
    dispatch = BatchDispatch(batch_size=BATCH)
    if family == "fan_out":
        return client.fan_out(source, dispatch=dispatch)
    return client.fan_out_outcomes(source, dispatch=dispatch)


def _codes(report: OperationReport) -> list[str]:
    return [violation.code for violation in report.violations]


def _published(error: BaseException) -> OperationReport:
    report = error.__dict__.get("report")
    assert isinstance(report, OperationReport), f"{type(error).__name__} carries no report"
    return report


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_early_close_with_failing_source_close_publishes_one_report(family: str) -> None:
    source = _Source(close_fails=True)
    async with _client(_Portal()) as client:
        stream = _stream(client, family, source)
        await anext(stream)
        with pytest.raises(_CloseFailedError) as raised:
            await stream.aclose()

        report = stream.report
        assert report is not None
        assert _published(raised.value) is report
        assert report.state is TerminalState.EARLY_CLOSED
        assert report.terminal_reason == "stream cleanup failed"
        assert "cleanup_failure" in _codes(report)
        # A repeated close after the finalized one does nothing and raises nothing.
        await stream.aclose()
        assert stream.report is report
    assert source.closes == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_source_close_failure_after_exhaustion_is_a_cleanup_failure_not_a_source_failure(family: str) -> None:
    # The owned source is closed by its owner's cleanup, not inside the final pull (§3.2), so every
    # command is still delivered and the close failure never masquerades as a failed input source.
    source = _Source(close_fails=True)
    async with _client(_Portal()) as client:
        stream = _stream(client, family, source)
        for _ in range(COMMANDS):
            await anext(stream)
        with pytest.raises(_CloseFailedError) as raised:
            await anext(stream)

    report = _published(raised.value)
    assert report.state is TerminalState.FAILED
    assert "cleanup_failure" in _codes(report)
    assert "source_failure" not in _codes(report)
    if family in LOGICAL:
        assert report.terminal_reason == "stream cleanup failed"
    assert source.closes == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_cancelled_pull_with_failing_source_close_raises_the_cleanup_failure(family: str) -> None:
    portal = _Portal(block=True)
    source = _Source(close_fails=True)
    async with _client(portal) as client:
        stream = _stream(client, family, source)
        pull = asyncio.ensure_future(anext(stream))
        await portal.entered.wait()
        pull.cancel()
        with pytest.raises(_CloseFailedError) as raised:
            await pull

        report = stream.report
        assert report is not None
        assert _published(raised.value) is report
        assert report.state is TerminalState.FAILED
        assert "cleanup_failure" in _codes(report)
        await stream.aclose()
        assert stream.report is report


@pytest.mark.asyncio
@pytest.mark.parametrize("family", LOGICAL)
async def test_logical_batch_cancel_cleanup_failure_is_a_terminal_cleanup_failure(family: str) -> None:
    portal = _Portal(block=True)
    async with _client(portal) as client:
        stream = _stream(client, family, _Source(close_fails=True))
        pull = asyncio.ensure_future(anext(stream))
        await portal.entered.wait()
        pull.cancel()
        with pytest.raises(_CloseFailedError):
            await pull
    assert stream.report is not None
    assert stream.report.terminal_reason == "stream cleanup failed"


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_early_close_is_reported_as_closed_before_exhaustion(family: str) -> None:
    async with _client(_Portal()) as client:
        stream = _stream(client, family, _Source())
        await anext(stream)
        await stream.aclose()

    assert stream.report is not None
    assert stream.report.state is TerminalState.EARLY_CLOSED
    assert stream.report.terminal_reason == "stream closed before exhaustion"
    # The terminal event follows cleanup, so work cancelled by the close never lands after it (N1).
    assert "completion_after_terminal" not in _codes(stream.report)


@pytest.mark.asyncio
async def test_fail_fast_batch_reports_the_command_failure_not_its_carrier() -> None:
    async with _client(_Portal()) as client:
        stream = client.batch(_Source(failing=True), batch_size=BATCH)
        with pytest.raises(BatchFailed) as raised:
            _ = [outcome async for outcome in stream]

    report = raised.value.report
    assert report.state is TerminalState.FAILED
    assert report.terminal_reason == "BatchCommandError"
    assert _codes(report) == ["batch_command_failure"]


@pytest.mark.asyncio
@pytest.mark.parametrize(("family", "error"), [("batch", BatchFailed), ("fan_out_outcomes", InputSourceError)])
async def test_source_failure_is_reported_as_an_input_source_failure(
    family: str,
    error: type[B24ApiError],
) -> None:
    async with _client(_Portal()) as client:
        stream = _stream(client, family, _Source(fail_at=FAILING_COMMAND))
        with pytest.raises(error) as raised:
            _ = [outcome async for outcome in stream]

    report = _published(raised.value)
    assert report.terminal_reason == "InputSourceError"
    assert "source_failure" in _codes(report)
    assert "internal_failure" not in _codes(report)


async def _failing_bindings() -> AsyncIterator[Binding[int]]:
    yield Binding("first", (), 1)
    raise _SourceFailedError


@pytest.mark.asyncio
async def test_reference_binding_source_failure_is_reported_as_an_input_source_failure() -> None:
    async with _client(_Portal()) as client:
        stream = client.iter_reference_outcomes(
            Request("x.list", route=RouteKind.BARE),
            _failing_bindings(),
            traversal=SequentialTraversal(offset=OffsetSpec()),
            dispatch=BatchDispatch(batch_size=BATCH),
        )
        with pytest.raises(InputSourceError) as raised:
            _ = [outcome async for outcome in stream]

    report = _published(raised.value)
    assert report.terminal_reason == "InputSourceError"
    assert "source_failure" in _codes(report)
