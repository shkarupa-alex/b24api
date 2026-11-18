"""П-8: a batch stream ended before its outcomes were delivered tells in flight from never sent in its report."""

from __future__ import annotations
import asyncio
from typing import TYPE_CHECKING

import pytest

from b24api import ReplaySafety, Request, RouteKind, TerminalState
from b24api.batch.correlation import _Command
from b24api.batch.engine import BatchExecutor, BatchProgress
from b24api.contracts import Command, ExecutionPolicy
from b24api.errors import BatchFailed
from b24api.execution import Executor, RateCoordinator
from b24api.execution.facts import Execution
from b24api.testing import ScriptedTransport
from b24api.transport import WireResponse
from tests.scripting import client_for, json_response

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from b24api.contracts import CommandSuccess, OperationReport
    from b24api.contracts.stream import OperationStream
    from b24api.transport import WireRequest


class _HangingTransport(ScriptedTransport):
    """Answer every envelope but the ``hang_at``-th, which never returns; REST 3.0 envelopes included."""

    def __init__(self, hang_at: int) -> None:
        super().__init__(())
        self.sends = 0
        self.hang_at = hang_at
        self.hanging = asyncio.Event()

    async def send_wire(self, wire: WireRequest, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        self.sends += 1
        if self.sends == self.hang_at:
            self.hanging.set()
            await asyncio.Future()
        if wire.array_body is not None:
            return json_response({"result": [{"item": 1} for _ in wire.array_body.commands]})
        commands = wire.parameters["cmd"]
        assert isinstance(commands, dict)
        return json_response({"result": {"result": dict.fromkeys(commands, 1), "result_error": []}})


async def _cancelled_report(transport: _HangingTransport, commands: list[Command[int]]) -> OperationReport:
    """Cancel the stream's consumer once the hanging envelope is in flight; return the stream's report."""
    delivered = 0
    async with client_for(transport) as client:
        stream = client.batch_outcomes(commands)

        async def consume() -> None:
            nonlocal delivered
            async with stream:
                async for _outcome in stream:
                    delivered += 1

        task = asyncio.ensure_future(consume())
        await transport.hanging.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    report = stream.report
    assert report is not None
    assert delivered == report.emitted
    return report


@pytest.mark.asyncio
@pytest.mark.parametrize("route", [RouteKind.BARE, RouteKind.API_V3])
async def test_commands_in_flight_when_the_stream_is_cancelled_are_reported_possibly_run(route: RouteKind) -> None:
    transport = _HangingTransport(hang_at=2)
    commands = [
        Command(Request("x.item.add", {"id": index}, ReplaySafety.UNSAFE, route=route), index) for index in range(120)
    ]

    report = await _cancelled_report(transport, commands)

    assert report.state is TerminalState.CANCELLED
    assert (report.admitted, report.emitted) == (100, 50)
    assert (report.undelivered_possibly_run, report.undelivered_not_sent) == (50, 0)
    assert report.unknown == 0


@pytest.mark.asyncio
async def test_commands_of_an_envelope_never_sent_when_the_stream_is_cancelled_are_reported_not_sent() -> None:
    # One chunk of two envelopes, classic then REST 3.0: the first hangs, so the second is never sent.
    transport = _HangingTransport(hang_at=1)
    commands = [
        Command(Request("x.item.add", {"id": index}, ReplaySafety.UNSAFE, route=route), index)
        for index, route in enumerate([RouteKind.BARE] * 20 + [RouteKind.API_V3] * 30)
    ]

    report = await _cancelled_report(transport, commands)

    assert (report.admitted, report.emitted) == (50, 0)
    assert (report.undelivered_possibly_run, report.undelivered_not_sent) == (20, 30)


class _GatewayTransport(ScriptedTransport):
    """Answer every envelope with a 502 from a gateway: each command of it may have run."""

    def __init__(self) -> None:
        super().__init__(())

    async def send_wire(self, wire: WireRequest, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del wire, attempt_timeout, max_response_bytes
        return WireResponse(502, (("content-type", "text/html"),), b"<html>bad gateway</html>")


@pytest.mark.asyncio
@pytest.mark.parametrize("taken", [1, 2])
async def test_an_outcome_the_consumer_received_before_it_stopped_is_not_counted_undelivered(taken: int) -> None:
    # The consumer stops right after it received ``taken`` outcomes of an envelope a gateway refused: only the
    # others are undelivered, and each of them may have run.
    commands = [Command(Request.bare("x.item.add", {"id": index}, ReplaySafety.UNSAFE), index) for index in range(3)]
    received = 0
    async with client_for(_GatewayTransport()) as client:
        stream = client.batch_outcomes(commands)
        async with stream:
            async for _outcome in stream:
                received += 1
                if received == taken:
                    break
    report = stream.report
    assert report is not None

    assert (report.admitted, report.emitted) == (3, taken)
    assert (report.undelivered_possibly_run, report.undelivered_not_sent) == (3 - taken, 0)


@pytest.mark.asyncio
async def test_answered_outcomes_the_consumer_did_not_take_are_neither_possibly_run_nor_unsent() -> None:
    # The envelope was answered in full: what each command did is known, so the outcomes the consumer left behind
    # are in neither count, though they are undelivered.
    commands = [Command(Request.bare("x.item.add", {"id": index}, ReplaySafety.UNSAFE), index) for index in range(3)]
    async with client_for(_HangingTransport(hang_at=0)) as client:
        stream = client.batch_outcomes(commands)
        async with stream:
            async for _outcome in stream:
                break
    report = stream.report
    assert report is not None

    assert (report.admitted, report.emitted) == (3, 1)
    assert (report.undelivered_possibly_run, report.undelivered_not_sent) == (0, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("route", [RouteKind.BARE, RouteKind.API_V3])
async def test_commands_of_an_envelope_waiting_for_its_permit_when_the_chunk_is_cancelled_were_not_sent(
    route: RouteKind,
) -> None:
    # The host is held, so the chunk's envelope waits for its permit; the chunk is cancelled there. Nothing of it
    # left, in either dialect: each of its commands was not sent.

    async def never(_seconds: float) -> None:
        await asyncio.Future()

    transport = _HangingTransport(hang_at=0)
    coordinator = RateCoordinator(max_concurrency=2, sleep=never)
    executor = Executor(transport, coordinator=coordinator)
    await coordinator.observe_throttle(30.0, reason="quota")
    commands = tuple(
        _Command(index, f"c{index:012d}", Request("x.item.add", {"id": index}, ReplaySafety.UNSAFE, route=route), None)
        for index in range(5)
    )
    progress = BatchProgress()
    chunk = asyncio.ensure_future(
        BatchExecutor(executor).execute_chunk(
            commands, context=executor.context(ExecutionPolicy(max_elapsed=120.0)), halt=False, progress=progress
        )
    )
    waiting = False
    for _turn in range(100):
        if waiting := any(count for _work_class, count in (await coordinator.snapshot()).queued):
            break
        await asyncio.sleep(0)
    assert waiting
    chunk.cancel()
    with pytest.raises(asyncio.CancelledError):
        await chunk
    await coordinator.close()

    assert transport.sends == 0
    assert [progress.execution(command.index) for command in commands] == [Execution.NOT_SENT] * 5


async def _failing_source(count: int) -> AsyncIterator[Command[int]]:
    for index in range(count):
        yield Command(Request.bare("x.item.add", {"id": index}, ReplaySafety.UNSAFE), index)
    raise _SourceFailedError


class _SourceFailedError(RuntimeError):
    """The caller's command source failed."""


@pytest.mark.asyncio
async def test_commands_admitted_from_a_source_that_then_failed_are_reported_not_sent() -> None:
    # The source yields three commands, then fails: none of them is sent. The consumer stops after the first
    # outcome; the two it left behind were never sent.
    async with client_for(_HangingTransport(hang_at=0)) as client:
        stream = client.batch_outcomes(_failing_source(3))
        async with stream:
            async for _outcome in stream:
                break
    report = stream.report
    assert report is not None

    assert (report.admitted, report.emitted) == (3, 1)
    assert (report.undelivered_possibly_run, report.undelivered_not_sent) == (0, 2)


async def _drain(stream: OperationStream[CommandSuccess[int]]) -> None:
    async with stream:
        async for _outcome in stream:
            pass


@pytest.mark.asyncio
async def test_a_fail_fast_batch_whose_source_failed_reports_its_admitted_commands_not_sent() -> None:
    async with client_for(_HangingTransport(hang_at=0)) as client:
        stream = client.batch(_failing_source(3))
        with pytest.raises(BatchFailed):
            await _drain(stream)
    report = stream.report
    assert report is not None

    assert (report.admitted, report.emitted) == (3, 0)
    assert (report.undelivered_possibly_run, report.undelivered_not_sent) == (0, 3)
