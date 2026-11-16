"""§3.1 transition table on every runner family, driven by barriers.

Each case builds a real family stream: the logical batch kernel, the public operation stream, the
batch outcome kernel, the reference kernel and the traversal kernel. It keeps the family's own
finalize, fallback, attach and propagate hooks and scripts only the two things the table varies: how
the body ends (exhausted, closed early, cancelled or failed) and how cleanup ends (success, failure
``E`` or a cancellation ``C`` that arrives while cleanup runs). Barriers make every interleaving
deterministic; no case sleeps for time.
"""

from __future__ import annotations
import asyncio
import dataclasses
import json
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NoReturn

import pytest

from b24api import Bitrix24, ReplaySafety, Request, RouteKind, Settings, TerminalState
from b24api.batch.engine import BatchExecutor
from b24api.batch.logical import LogicalBatchKernelStream
from b24api.batch.stream import batch_outcome_stream
from b24api.contracts import Command
from b24api.contracts.policy import ExecutionPolicy, IdentityCoercion, KernelState
from b24api.contracts.request import IdentitySpec
from b24api.execution import Executor, WireResponse
from b24api.execution.context import latch_escape
from b24api.execution.lifecycle import CleanupAttempt, LifecycleHooks, OperationRunner, TerminalCause, kernel_terminal
from b24api.execution.snapshot import KernelReport
from b24api.references.outcome import ReferenceRequest
from b24api.references.stream import iter_references
from b24api.traversal.plans import KernelDirectDispatch, SingleResponsePlan
from b24api.traversal.stream import iter_list
from tests.scripting import HANG_BOUND, ResponderTransport, client_for, raised_driving_the_loop

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from b24api.execution.context import Settled


HOST = "fixture.invalid"
FAMILIES = ("logical_batch", "operation_stream", "batch_outcomes", "references", "traversal")
type Family = Literal["logical_batch", "operation_stream", "batch_outcomes", "references", "traversal"]
type CleanupMode = Literal["ok", "fail", "cancel"]
type BodyMode = Literal["exhaust", "fail", "block"]


class _BodyFailedError(Exception):
    """The body failed with a foreign exception (the table's ``X``)."""


class _CleanupFailedError(Exception):
    """Cleanup failed (the table's ``E``)."""


class _CallerError(Exception):
    """The caller's own failure inside ``async with``."""


def _raise_caller_error() -> NoReturn:
    raise _CallerError


class _FinalizeFailedError(Exception):
    """The family's finalizer raised."""


class _Portal:
    """Answer batch commands with their ids and list reads with two rows."""

    host = HOST
    sent: ClassVar[list[str]] = []  # every portal's sends; a test compares its own before/after lengths

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        _Portal.sent.append(request.method)
        if request.method == "batch":
            commands = request.copy_parameters()["cmd"]
            assert isinstance(commands, dict)
            body: object = {"result": {"result": dict.fromkeys(commands, 1), "result_error": {}}}
        else:
            body = {"result": [{"ID": 1}, {"ID": 2}]}
        return WireResponse(200, (("content-type", "application/json"),), json.dumps(body).encode())

    async def aclose(self) -> None:
        return None


async def _commands() -> AsyncIterator[Command[int]]:
    for index in (1, 2, 3):
        yield Command(Request("x.get", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)


def _identity() -> IdentitySpec:
    return IdentitySpec(item_path=("ID",), filter_key="ID", order_key="ID", coercion=IdentityCoercion.EXACT_INTEGER)


def _build(family: Family, client: Bitrix24) -> Any:  # noqa: ANN401 - each family has its own stream type
    executor = Executor(_Portal())
    if family == "logical_batch":
        return LogicalBatchKernelStream(executor, _commands(), batch_size=2, fail_fast=False, policy=ExecutionPolicy())
    if family == "operation_stream":
        return client.batch(_commands(), batch_size=2)
    if family == "batch_outcomes":
        return batch_outcome_stream(
            BatchExecutor(executor),
            [Request("x.get", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE) for index in (1, 2, 3)],
            batch_size=2,
        )
    if family == "references":
        return iter_references(
            executor,
            [ReferenceRequest(Request("crm.item.list", {"ref": key}, route=RouteKind.BARE), key) for key in "ab"],
            plan=SingleResponsePlan(),
            dispatch=KernelDirectDispatch(),
            identity=_identity(),
        )
    return iter_list(
        executor,
        Request("crm.item.list", route=RouteKind.BARE),
        plan=SingleResponsePlan(),
        identity=_identity(),
    )


@dataclasses.dataclass
class _Harness:
    """One family stream with a scripted body end, a scripted cleanup end and their barriers."""

    stream: Any
    runner: OperationRunner[Any, Any]
    body_blocked: asyncio.Event
    cleanup_blocked: asyncio.Event
    cleanup_release: asyncio.Event
    finalize_calls: int = 0
    fallback_calls: int = 0

    def codes(self) -> list[str]:
        report = self.runner.report
        assert report is not None
        return [violation.code for violation in report.violations]

    def assert_published_once(self, *errors: BaseException) -> None:
        assert self.runner.report is not None
        assert self.finalize_calls == 1
        assert self.fallback_calls == 0
        for error in errors:
            assert getattr(error, "report", None) is self.runner.report


async def _scripted(body: AsyncIterator[Any], mode: BodyMode, blocked: asyncio.Event) -> AsyncIterator[Any]:
    """Yield the family's first real item, then end the body the way the case needs."""
    try:
        yield await anext(body)
        if mode == "exhaust":
            async for item in body:
                yield item
        elif mode == "fail":
            raise _BodyFailedError
        else:
            blocked.set()
            await asyncio.Event().wait()
    finally:
        close = getattr(body, "aclose", None)
        if close is not None:
            await close()


def _harness(
    family: Family,
    client: Bitrix24,
    *,
    body: BodyMode,
    cleanup: CleanupMode,
    finalize_fails: bool = False,
) -> _Harness:
    stream = _build(family, client)
    runner: OperationRunner[Any, Any] = stream._runner  # noqa: SLF001 - scripts the family's own runner
    harness = _Harness(stream, runner, asyncio.Event(), asyncio.Event(), asyncio.Event())
    hooks = runner._hooks  # noqa: SLF001 - keeps the family's hooks, scripts cleanup
    original_cleanup: Callable[[], Awaitable[None]] = hooks.cleanup

    async def scripted_cleanup() -> None:
        await original_cleanup()
        if cleanup == "fail":
            raise _CleanupFailedError
        if cleanup == "cancel":
            harness.cleanup_blocked.set()
            await harness.cleanup_release.wait()

    def counted_finalize(*args: Any) -> Any:  # noqa: ANN401 - forwards the family's own report type
        harness.finalize_calls += 1
        if finalize_fails:
            raise _FinalizeFailedError
        return hooks.finalize(*args)

    def counted_fallback(*args: Any) -> Any:  # noqa: ANN401 - forwards the family's own report type
        harness.fallback_calls += 1
        return hooks.failure_report(*args)

    runner._hooks = dataclasses.replace(  # noqa: SLF001 - installs the scripted cleanup
        hooks,
        cleanup=scripted_cleanup,
        finalize=counted_finalize,
        failure_report=counted_fallback,
    )
    runner._body = _scripted(runner._body, body, harness.body_blocked)  # noqa: SLF001 - scripts the body end
    return harness


@pytest.fixture
def client() -> Bitrix24:
    return Bitrix24(Settings(webhook_url=f"https://{HOST}/rest/1/token/"), transport=_Portal())


async def _drain(stream: Any) -> None:  # noqa: ANN401
    while True:
        try:
            await anext(stream)
        except StopAsyncIteration:
            return


async def _cancel_during_cleanup(harness: _Harness, task: asyncio.Task[Any], message: str = "C") -> None:
    """Cancel the task while cleanup waits on its barrier, then let cleanup finish."""
    await harness.cleanup_blocked.wait()
    task.cancel(message)
    await asyncio.sleep(0)
    harness.cleanup_release.set()


async def _outcome(task: asyncio.Task[Any]) -> BaseException | None:
    try:
        await task
    except BaseException as error:  # noqa: BLE001 - the raised exception is what the table specifies
        return error
    return None


def _is_body_failure(error: BaseException | None) -> bool:
    """The FAILED primary, or the family's propagated carrier of it."""
    return isinstance(error, _BodyFailedError) or isinstance(getattr(error, "__cause__", None), _BodyFailedError)


# Row: body exhausted.


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_exhausted_with_clean_cleanup_publishes_and_stops(family: Family, client: Bitrix24) -> None:
    harness = _harness(family, client, body="exhaust", cleanup="ok")

    assert await _outcome(asyncio.create_task(_drain(harness.stream))) is None

    harness.assert_published_once()
    assert "cleanup_failure" not in harness.codes()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_exhausted_with_failing_cleanup_raises_the_cleanup_failure(family: Family, client: Bitrix24) -> None:
    harness = _harness(family, client, body="exhaust", cleanup="fail")

    error = await _outcome(asyncio.create_task(_drain(harness.stream)))

    assert isinstance(error, _CleanupFailedError)
    harness.assert_published_once(error)
    assert "cleanup_failure" in harness.codes()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_exhausted_with_cancelled_cleanup_raises_that_cancellation(family: Family, client: Bitrix24) -> None:
    harness = _harness(family, client, body="exhaust", cleanup="cancel")
    task = asyncio.create_task(_drain(harness.stream))

    await _cancel_during_cleanup(harness, task)
    error = await _outcome(task)

    assert isinstance(error, asyncio.CancelledError)
    assert error.args == ("C",)
    harness.assert_published_once(error)


# Row: aclose() before exhaustion.


async def _read_then_close(stream: Any) -> None:  # noqa: ANN401
    await anext(stream)
    await stream.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_early_close_with_clean_cleanup_returns_quietly(family: Family, client: Bitrix24) -> None:
    harness = _harness(family, client, body="exhaust", cleanup="ok")

    assert await _outcome(asyncio.create_task(_read_then_close(harness.stream))) is None

    harness.assert_published_once()
    with pytest.raises(StopAsyncIteration):
        await anext(harness.stream)
    await harness.stream.aclose()
    assert harness.finalize_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_early_close_with_failing_cleanup_raises_the_cleanup_failure(family: Family, client: Bitrix24) -> None:
    harness = _harness(family, client, body="exhaust", cleanup="fail")

    error = await _outcome(asyncio.create_task(_read_then_close(harness.stream)))

    assert isinstance(error, _CleanupFailedError)
    harness.assert_published_once(error)
    assert "cleanup_failure" in harness.codes()
    await harness.stream.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_early_close_with_cancelled_cleanup_raises_that_cancellation(family: Family, client: Bitrix24) -> None:
    harness = _harness(family, client, body="exhaust", cleanup="cancel")
    task = asyncio.create_task(_read_then_close(harness.stream))

    await _cancel_during_cleanup(harness, task)
    error = await _outcome(task)

    assert isinstance(error, asyncio.CancelledError)
    assert error.args == ("C",)
    harness.assert_published_once(error)


# Row: the consumer is cancelled during a read (the table's ``P``).


async def _cancel_blocked_read(harness: _Harness, task: asyncio.Task[Any]) -> None:
    await harness.body_blocked.wait()
    task.cancel("P")


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_cancelled_read_with_clean_cleanup_raises_the_cancellation(family: Family, client: Bitrix24) -> None:
    harness = _harness(family, client, body="block", cleanup="ok")
    task = asyncio.create_task(_drain(harness.stream))

    await _cancel_blocked_read(harness, task)
    error = await _outcome(task)

    assert isinstance(error, asyncio.CancelledError)
    assert error.args == ("P",)
    harness.assert_published_once(error)


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_cancelled_read_with_failing_cleanup_raises_the_cancellation_and_reports_the_cleanup(
    family: Family,
    client: Bitrix24,
) -> None:
    # The cancellation came first, so it outranks the cleanup failure: the read raises it, the report records both.
    harness = _harness(family, client, body="block", cleanup="fail")
    task = asyncio.create_task(_drain(harness.stream))
    await _cancel_blocked_read(harness, task)
    error = await _outcome(task)

    assert isinstance(error, asyncio.CancelledError)
    harness.assert_published_once(error)
    assert "cleanup_failure" in harness.codes()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_cancelled_read_with_cancelled_cleanup_raises_the_second_cancellation_from_the_first(
    family: Family,
    client: Bitrix24,
) -> None:
    harness = _harness(family, client, body="block", cleanup="cancel")
    task = asyncio.create_task(_drain(harness.stream))

    await _cancel_blocked_read(harness, task)
    await _cancel_during_cleanup(harness, task)
    error = await _outcome(task)

    assert isinstance(error, asyncio.CancelledError)
    assert error.args == ("C",)
    assert isinstance(error.__cause__, asyncio.CancelledError)
    assert error.__cause__.args == ("P",)
    harness.assert_published_once(error)


# Row: the body fails with a foreign exception ``X``.


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_failed_body_with_clean_cleanup_raises_the_failure(family: Family, client: Bitrix24) -> None:
    harness = _harness(family, client, body="fail", cleanup="ok")

    error = await _outcome(asyncio.create_task(_drain(harness.stream)))

    assert error is not None
    assert _is_body_failure(error)
    harness.assert_published_once(error)
    assert "cleanup_failure" not in harness.codes()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_failed_body_with_failing_cleanup_keeps_the_failure_and_reports_the_cleanup(
    family: Family,
    client: Bitrix24,
) -> None:
    harness = _harness(family, client, body="fail", cleanup="fail")

    error = await _outcome(asyncio.create_task(_drain(harness.stream)))

    assert error is not None
    assert _is_body_failure(error)
    primary = error if isinstance(error, _BodyFailedError) else error.__cause__
    assert primary is not None
    # The report the error carries records the cleanup failure; no note repeats it, in any family.
    assert getattr(primary, "__notes__", []) == []
    assert getattr(error, "__notes__", []) == []
    harness.assert_published_once(error)
    assert "cleanup_failure" in harness.codes()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_failed_body_with_cancelled_cleanup_raises_the_failure_then_rearms_the_cancellation(
    family: Family,
    client: Bitrix24,
) -> None:
    harness = _harness(family, client, body="fail", cleanup="cancel")
    seen: list[BaseException] = []

    async def consume() -> None:
        try:
            await _drain(harness.stream)
        except Exception as error:  # noqa: BLE001 - the family may carry X in its own exception
            seen.append(error)
        await asyncio.sleep(0)
        pytest.fail("the deferred cancellation was not re-armed")

    task = asyncio.create_task(consume())
    await _cancel_during_cleanup(harness, task)
    error = await _outcome(task)

    assert isinstance(error, asyncio.CancelledError)
    assert len(seen) == 1
    assert _is_body_failure(seen[0])
    harness.assert_published_once(seen[0])
    assert "cleanup_failure" in harness.codes()


# Concurrency, repeated close and finalization failure.


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_second_concurrent_read_is_refused_without_work(family: Family, client: Bitrix24) -> None:
    harness = _harness(family, client, body="block", cleanup="ok")
    await anext(harness.stream)
    blocked = asyncio.create_task(anext(harness.stream))
    await harness.body_blocked.wait()

    with pytest.raises(RuntimeError, match="concurrent stream pull"):
        await anext(harness.stream)

    blocked.cancel()
    assert isinstance(await _outcome(blocked), asyncio.CancelledError)
    harness.assert_published_once()


@pytest.mark.asyncio
async def test_close_during_an_isolated_read_cancels_it_and_terminates_once(client: Bitrix24) -> None:
    harness = _harness("operation_stream", client, body="block", cleanup="ok")
    await anext(harness.stream)
    reader = asyncio.create_task(anext(harness.stream))
    await harness.body_blocked.wait()

    closer = asyncio.create_task(harness.stream.aclose())
    second_closer = asyncio.create_task(harness.stream.aclose())

    assert await _outcome(closer) is None
    assert await _outcome(second_closer) is None
    read_error = await _outcome(reader)
    assert isinstance(read_error, asyncio.CancelledError)
    harness.assert_published_once(read_error)
    with pytest.raises(StopAsyncIteration):
        await anext(harness.stream)


@pytest.mark.asyncio
@pytest.mark.parametrize("family", [family for family in FAMILIES if family != "operation_stream"])
async def test_inline_read_cannot_be_closed_from_another_task(family: Family, client: Bitrix24) -> None:
    harness = _harness(family, client, body="block", cleanup="ok")
    await anext(harness.stream)
    reader = asyncio.create_task(anext(harness.stream))
    await harness.body_blocked.wait()

    with pytest.raises(RuntimeError, match="inline stream read cannot be closed from another task"):
        await harness.stream.aclose()

    reader.cancel()
    assert isinstance(await _outcome(reader), asyncio.CancelledError)
    harness.assert_published_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_failing_finalizer_publishes_one_fallback_report_and_raises(family: Family, client: Bitrix24) -> None:
    harness = _harness(family, client, body="exhaust", cleanup="ok", finalize_fails=True)

    error = await _outcome(asyncio.create_task(_drain(harness.stream)))

    assert isinstance(error, _FinalizeFailedError)
    assert harness.finalize_calls == 1
    assert harness.fallback_calls == 1
    assert getattr(error, "report", None) is harness.runner.report
    assert "report_finalize_failure" in harness.codes()
    with pytest.raises(StopAsyncIteration):
        await anext(harness.stream)
    await harness.stream.aclose()
    assert harness.finalize_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_failing_finalizer_behind_a_body_failure_keeps_the_body_failure(family: Family, client: Bitrix24) -> None:
    harness = _harness(family, client, body="fail", cleanup="ok", finalize_fails=True)

    error = await _outcome(asyncio.create_task(_drain(harness.stream)))

    assert error is not None
    assert _is_body_failure(error)
    primary = error if isinstance(error, _BodyFailedError) else error.__cause__
    assert "stream report finalization failed (_FinalizeFailedError)" in getattr(primary, "__notes__", [])
    assert harness.fallback_calls == 1
    assert getattr(error, "report", None) is harness.runner.report
    assert "report_finalize_failure" in harness.codes()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["body", "cleanup", "finalize"])
@pytest.mark.parametrize("family", FAMILIES)
async def test_terminal_failure_is_raised_once_and_later_reads_end_the_iteration(
    family: Family, failure: str, client: Bitrix24
) -> None:
    # §3.1: the read that ends the body raises its failure with the report; any later read, before or after
    # aclose(), starts no work and ends the iteration.
    harness = _harness(
        family,
        client,
        body="fail" if failure == "body" else "exhaust",
        cleanup="fail" if failure == "cleanup" else "ok",
        finalize_fails=failure == "finalize",
    )

    error = await _outcome(asyncio.create_task(_drain(harness.stream)))

    report = harness.runner.report
    assert report is not None
    assert getattr(error, "report", None) is report
    if failure == "body":
        assert _is_body_failure(error)
    else:
        assert isinstance(error, _CleanupFailedError if failure == "cleanup" else _FinalizeFailedError)
    sent = len(_Portal.sent)
    calls = (harness.finalize_calls, harness.fallback_calls)
    with pytest.raises(StopAsyncIteration):
        await anext(harness.stream)
    await harness.stream.aclose()
    with pytest.raises(StopAsyncIteration):
        await anext(harness.stream)
    assert len(_Portal.sent) == sent
    assert (harness.finalize_calls, harness.fallback_calls) == calls
    assert harness.runner.report is report


@pytest.mark.asyncio
@pytest.mark.parametrize("caller", ["fails", "returns"])
@pytest.mark.parametrize("family", FAMILIES)
async def test_cancellation_during_context_exit_cleanup_keeps_the_callers_exception(
    family: Family, caller: str, client: Bitrix24
) -> None:
    # §3.1 X + C: the caller's exception leaves ``async with`` and the cancellation lands on its next await;
    # without a caller exception the cancellation is raised at once. Either way the report is published once.
    harness = _harness(family, client, body="exhaust", cleanup="cancel")
    # Internal kernels have no context protocol; the public adapter delegates it to the same runner.
    stream = harness.stream if hasattr(harness.stream, "__aenter__") else harness.runner
    caught: list[BaseException] = []
    resumed: list[bool] = []

    async def use() -> None:
        try:
            async with stream:
                await anext(stream)
                if caller == "fails":
                    _raise_caller_error()
        except _CallerError as error:
            caught.append(error)
        resumed.append(True)
        await asyncio.sleep(0)
        resumed.append(True)

    task = asyncio.create_task(use())
    await _cancel_during_cleanup(harness, task)

    assert isinstance(await _outcome(task), asyncio.CancelledError)
    assert len(caught) == (1 if caller == "fails" else 0)
    assert resumed == ([True] if caller == "fails" else [])
    harness.assert_published_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
@pytest.mark.parametrize("body", ["exhaust", "fail"])
async def test_failing_finalizer_keeps_a_simultaneous_cleanup_failure(
    family: Family, body: BodyMode, client: Bitrix24
) -> None:
    harness = _harness(family, client, body=body, cleanup="fail", finalize_fails=True)

    error = await _outcome(asyncio.create_task(_drain(harness.stream)))

    if body == "exhaust":
        assert isinstance(error, _FinalizeFailedError)
        # A library error carries the cleanup failure in its report, never as a note.
        assert getattr(error, "__notes__", []) == []
    else:
        assert _is_body_failure(error)
    assert (harness.finalize_calls, harness.fallback_calls) == (1, 1)
    report = harness.runner.report
    assert report is not None
    assert report.state.value == "failed"
    assert getattr(error, "report", None) is report
    assert {"report_finalize_failure", "cleanup_failure"} <= set(harness.codes())
    await harness.stream.aclose()
    assert harness.runner.report is report
    assert harness.finalize_calls == 1


def _public(family: str, client: Bitrix24) -> Any:  # noqa: ANN401 - each public family has its own stream type
    if family == "batch_outcomes":
        return client.batch_outcomes(_commands(), batch_size=2)
    if family == "fan_out_outcomes":
        return client.fan_out_outcomes(_commands())
    return client.iter_list(Request("crm.item.list", route=RouteKind.BARE))


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["batch_outcomes", "fan_out_outcomes", "iter_list"])
@pytest.mark.parametrize("cleanup", ["ok", "fail"])
async def test_failing_kernel_finalizer_still_publishes_one_public_fallback_report(
    family: str, cleanup: CleanupMode, client: Bitrix24
) -> None:
    # The source kernel's own finalizer raises before it records terminal or cleanup evidence on the shared
    # completion gate; the public stream must still publish one FAILED report and close promptly.
    stream = _public(family, client)
    kernel_runner: OperationRunner[Any, Any] = stream._source._runner  # noqa: SLF001 - scripts the source kernel
    hooks = kernel_runner._hooks  # noqa: SLF001
    original_cleanup: Callable[[], Awaitable[None]] = hooks.cleanup

    async def scripted_cleanup() -> None:
        await original_cleanup()
        if cleanup == "fail":
            raise _CleanupFailedError

    def failing_finalize(*_args: Any) -> Any:  # noqa: ANN401
        raise _FinalizeFailedError

    kernel_runner._hooks = dataclasses.replace(hooks, cleanup=scripted_cleanup, finalize=failing_finalize)  # noqa: SLF001

    await anext(stream)
    error = await _outcome(asyncio.create_task(_drain(stream)))

    if family == "iter_list":
        # The scripted portal repeats its page, so the traversal's own rejection is the primary failure and
        # the finalization failure is secondary; the primary stays the raised error.
        assert error is not None
        assert not isinstance(error, RuntimeError)
    else:
        assert isinstance(error, _FinalizeFailedError)
    report = stream.report
    assert report is not None
    assert report.state.value in {"failed", "incomplete"}
    codes = {violation.code for violation in report.violations}
    assert getattr(error, "report", None) is report
    assert "report_finalize_failure" in codes
    assert ("cleanup_failure" in codes) is (cleanup == "fail")
    await asyncio.wait_for(stream.aclose(), timeout=1)
    assert stream.report is report


@pytest.mark.parametrize("family", ["batch_outcomes", "fan_out_outcomes", "iter_list"])
def test_a_failing_public_finalizer_publishes_its_fallback_in_the_interrupts_shape(family: str) -> None:
    # The transport interrupts the operation and the public stream's own finalizer then raises: the fallback report
    # is what is published, in the one shape of an interrupted operation, and the interrupt leaves carrying it.
    got: list[BaseException] = []
    streams: list[Any] = []

    async def respond(_request: Request) -> object:
        raise KeyboardInterrupt("transport")

    def failing_finalize(*_args: Any) -> Any:  # noqa: ANN401
        raise _FinalizeFailedError

    async def main() -> None:
        async with client_for(ResponderTransport(respond)) as client:
            stream = _public(family, client)
            streams.append(stream)
            runner: OperationRunner[Any, Any] = stream._runner  # noqa: SLF001 - scripts the public stream's finalizer
            runner._hooks = dataclasses.replace(runner._hooks, finalize=failing_finalize)  # noqa: SLF001
            try:
                async for _item in stream:
                    pass
            except KeyboardInterrupt as error:
                got.append(error)

    asyncio.run(main())
    report = streams[0].report
    assert report is not None
    assert [type(error) for error in got] == [KeyboardInterrupt]
    assert report.state is TerminalState.FAILED
    assert report.terminal_reason == "operation interrupted"
    codes = [violation.code for violation in report.violations]
    assert "report_finalize_failure" in codes
    assert codes[-1] == "interrupted"
    assert report.violations[-1].message == "operation interrupted (KeyboardInterrupt)"
    assert getattr(got[0], "report", None) is report


@pytest.mark.asyncio
@pytest.mark.parametrize("family", FAMILIES)
async def test_failing_fallback_hook_still_ends_publication(family: Family, client: Bitrix24) -> None:
    harness = _harness(family, client, body="exhaust", cleanup="ok", finalize_fails=True)

    def failing_fallback(*_args: Any) -> Any:  # noqa: ANN401
        raise _CleanupFailedError

    harness.runner._hooks = dataclasses.replace(harness.runner._hooks, failure_report=failing_fallback)  # noqa: SLF001

    error = await _outcome(asyncio.create_task(_drain(harness.stream)))

    assert error is not None
    # Publication ended with the failure, so a later close returns instead of waiting for a report.
    await asyncio.wait_for(harness.stream.aclose(), timeout=1)


def test_a_read_whose_own_task_latched_an_interrupt_raises_it_instead_of_its_item() -> None:
    # The read's own task caught the interrupt and went on to an item (a wake cancels no read from inside it): the
    # read in flight is what raises it, so it ends the stream with it rather than deliver the item and hold it.
    interrupt = KeyboardInterrupt("in the read")
    delivered: list[bool] = []
    causes: list[TerminalCause] = []
    got: list[object] = []

    async def body() -> AsyncIterator[int]:
        latch_escape(interrupt)
        yield 1

    async def cleanup() -> None:
        return None

    def finalize(cause: TerminalCause, reason: str | None, _attempt: CleanupAttempt) -> KernelReport:
        causes.append(cause)
        state, terminal_reason = kernel_terminal(cause, reason)
        return KernelReport(state=state, terminal_reason=terminal_reason)

    def fallback(_cause: TerminalCause, reason: str, _attempt: CleanupAttempt) -> KernelReport:
        return KernelReport(state=KernelState.FAILED, terminal_reason=reason)

    runner: OperationRunner[int, KernelReport] = OperationRunner(
        body(),
        LifecycleHooks(
            finalize=finalize, failure_report=fallback, cleanup=cleanup, delivered=lambda: delivered.append(True)
        ),
    )

    async def main() -> None:
        for _read in range(2):
            try:
                got.append(await anext(runner))
            except StopAsyncIteration:
                break
            except KeyboardInterrupt as error:
                got.append(error)

    asyncio.run(main())
    assert got == [interrupt]
    assert delivered == []
    assert causes == [TerminalCause.INTERRUPTED]
    report = runner.report
    assert report is not None
    assert (report.state, report.terminal_reason) == (KernelState.FAILED, "operation interrupted")
    assert getattr(interrupt, "report", None) is report


class _BetweenReads:
    """A runner whose body starts work latching an interrupt between reads, and whose cleanup waits for a release.

    The work goes on, in the very step that latched the interrupt, with the steps the test adds (``after_latch``).
    """

    def __init__(self, cleanup_budget: Callable[[], float] | None = None) -> None:
        self.interrupt = KeyboardInterrupt("between reads")
        self.go = asyncio.Event()
        # Set just before the interrupt is latched: a read waiting for it resumes ahead of the end's own task.
        self.latching = asyncio.Event()
        self.cleaning = asyncio.Event()
        self.cleanups = 0
        self.finalized: list[TerminalCause] = []
        self.attempts: list[CleanupAttempt] = []
        self.release = asyncio.Event()
        self.after_latch: list[Callable[[], Awaitable[None]]] = []
        # Interrupts the cleanup latches once released, as more of the operation's own work fails.
        self.latched_in_cleanup: list[BaseException] = []
        self.runner: OperationRunner[int, KernelReport] = OperationRunner(
            self._body(),
            LifecycleHooks(
                finalize=self._finalize, failure_report=_fallback, cleanup=self._cleanup, cleanup_budget=cleanup_budget
            ),
            works_between_reads=True,
        )
        self.work: list[asyncio.Future[None]] = []

    async def _body(self) -> AsyncIterator[int]:
        # Started in the read, the work is the operation's: it inherits the operation's latch.
        self.work.append(asyncio.ensure_future(self._latch_later()))
        yield 1
        await asyncio.Event().wait()

    async def _latch_later(self) -> None:
        await self.go.wait()
        self.latching.set()
        latch_escape(self.interrupt)
        for step in self.after_latch:
            await step()

    def _finalize(self, cause: TerminalCause, reason: str | None, attempt: CleanupAttempt) -> KernelReport:
        self.finalized.append(cause)
        self.attempts.append(attempt)
        return _finalized(cause, reason, attempt)

    async def _cleanup(self) -> None:
        self.cleanups += 1
        self.cleaning.set()
        await self.release.wait()
        for interrupt in self.latched_in_cleanup:
            latch_escape(interrupt)

    async def read_one(self) -> None:
        """Read the one item, let the work latch the interrupt, and go on working past the event loop's raise."""
        assert await anext(self.runner) == 1
        self.go.set()
        await asyncio.sleep(HANG_BOUND)


def _finalized(cause: TerminalCause, reason: str | None, _attempt: CleanupAttempt) -> KernelReport:
    state, terminal_reason = kernel_terminal(cause, reason)
    return KernelReport(state=state, terminal_reason=terminal_reason)


def _fallback(_cause: TerminalCause, reason: str, _attempt: CleanupAttempt) -> KernelReport:
    return KernelReport(state=KernelState.FAILED, terminal_reason=reason)


def _ending(runner: OperationRunner[int, KernelReport]) -> asyncio.Future[Settled[None]]:
    ending = runner._ending  # noqa: SLF001 - the task the interrupt started for the operation's end
    assert ending is not None
    return ending


def test_an_interrupt_between_reads_goes_to_the_event_loop_before_its_end_is_over() -> None:
    # Work the read started latches an interrupt between reads: the event loop raises it at once, while the end that
    # follows waits for its cleanup. Released, that end publishes the report, in the interrupt's shape and attached
    # to it; a read then finds the stream ended and a close returns, neither raising the interrupt again.
    operation = _BetweenReads()
    after: list[object] = []

    async def finish() -> None:
        after.append(operation.runner.report)
        operation.release.set()
        try:
            await anext(operation.runner)
        except StopAsyncIteration as error:
            after.append(type(error))
        await operation.runner.aclose()
        after.append("closed")

    error = raised_driving_the_loop(operation.read_one, finish)
    assert error is operation.interrupt
    assert after == [None, StopAsyncIteration, "closed"]
    report = operation.runner.report
    assert report is not None
    assert (report.state, report.terminal_reason) == (KernelState.FAILED, "operation interrupted")
    assert getattr(error, "report", None) is report


def test_a_read_that_resumes_ahead_of_the_ends_task_runs_the_end_once() -> None:
    # The read resumes in the very turn the interrupt is latched, ahead of the end's own task, and runs the end
    # itself; the task finds it begun and leaves it. Cleanup runs once, and the report is published once.
    operation = _BetweenReads()
    got: list[object] = []
    read_over = asyncio.Event()

    async def main() -> None:
        assert await anext(operation.runner) == 1
        operation.go.set()
        await operation.latching.wait()
        try:
            await anext(operation.runner)
        except StopAsyncIteration as error:
            got.append(type(error))
        read_over.set()

    async def finish() -> None:
        operation.release.set()
        await read_over.wait()
        await _ending(operation.runner)

    error = raised_driving_the_loop(main, finish)
    assert error is operation.interrupt
    assert got == [StopAsyncIteration]
    assert (operation.cleanups, operation.finalized) == (1, [TerminalCause.INTERRUPTED])
    report = operation.runner.report
    assert report is not None
    assert getattr(error, "report", None) is report


def test_a_read_running_the_end_between_reads_waits_for_its_cleanup_past_the_operations_deadline() -> None:
    # The end of an operation an interrupt ended between reads has no deadline of its own: the operation's budget is
    # spent, and the read that runs the end is cancelled while the cleanup waits. The read still waits for the
    # cleanup, however long it takes, then raises its cancellation, recorded as cleanup's; nothing overran.
    operation = _BetweenReads(cleanup_budget=lambda: 0.0)
    readers: list[asyncio.Task[Any]] = []

    async def main() -> None:
        reader = asyncio.current_task()
        assert reader is not None
        readers.append(reader)
        assert await anext(operation.runner) == 1
        operation.go.set()
        await operation.latching.wait()
        # Resumed in the turn the interrupt is latched, ahead of the end's own task: this read runs the end.
        await anext(operation.runner)

    async def cancel_the_reader() -> None:
        await operation.cleaning.wait()
        readers[0].cancel()
        await asyncio.sleep(0.05)  # more than enough for a read that does not wait for the cleanup to end
        assert not readers[0].done()
        operation.release.set()
        await asyncio.gather(readers[0], return_exceptions=True)

    error = raised_driving_the_loop(main, cancel_the_reader)
    assert error is operation.interrupt
    assert readers[0].cancelled()
    assert (operation.cleanups, operation.finalized) == (1, [TerminalCause.INTERRUPTED])
    [attempt] = operation.attempts
    assert (attempt.error, attempt.finished) == (None, True)
    assert isinstance(attempt.cancellation, asyncio.CancelledError)


def test_a_read_from_the_operations_own_work_once_an_interrupt_ended_it_between_reads_ends_the_iteration() -> None:
    # The work that latched the interrupt reads at once, before the end has begun: the operation is over, so the read
    # ends the iteration rather than wait for an item the operation never yields. The end then runs once.
    operation = _BetweenReads()
    operation.release.set()
    got: list[object] = []

    async def read_from_the_work() -> None:
        try:
            async with asyncio.timeout(HANG_BOUND):
                await anext(operation.runner)
        except StopAsyncIteration as error:
            got.append(type(error))

    async def wait_for_the_end() -> None:
        await operation.runner.aclose()

    operation.after_latch.append(read_from_the_work)
    error = raised_driving_the_loop(operation.read_one, wait_for_the_end)
    assert error is operation.interrupt
    assert got == [StopAsyncIteration]
    assert (operation.cleanups, operation.finalized) == (1, [TerminalCause.INTERRUPTED])


def test_a_close_waiting_for_an_end_whose_fallback_report_fails_still_returns() -> None:
    # The end of an operation an interrupt ended between reads fails to build any report: its finalizer and its
    # fallback both fail. Publication still ends, so a close that waits for it while the cleanup runs returns.
    operation = _BetweenReads()

    def failing(*_args: object) -> NoReturn:
        raise _FinalizeFailedError

    operation.runner._hooks = dataclasses.replace(  # noqa: SLF001 - both report hooks fail
        operation.runner._hooks,  # noqa: SLF001
        finalize=failing,
        failure_report=failing,
    )
    closed: list[object] = []

    async def close_while_the_end_cleans_up() -> None:
        await operation.cleaning.wait()
        closing = asyncio.ensure_future(operation.runner.aclose())
        await asyncio.sleep(0)
        operation.release.set()
        async with asyncio.timeout(HANG_BOUND):
            closed.extend(await asyncio.gather(closing, return_exceptions=True))
        await asyncio.gather(_ending(operation.runner), return_exceptions=True)

    error = raised_driving_the_loop(operation.read_one, close_while_the_end_cleans_up)
    assert error is operation.interrupt
    assert closed == [None]
    assert operation.cleanups == 1


def test_a_close_runs_the_end_whose_own_task_was_cancelled_before_it_ran() -> None:
    # The task the interrupt started for the operation's end is cancelled before its first step, as a loop's shutdown
    # may cancel it: the end never began, so the close that comes next runs it, and the report is published all the
    # same, attached to the interrupt the event loop raised.
    operation = _BetweenReads()
    operation.release.set()
    closed: list[object] = []

    async def cancel_the_end() -> None:
        _ending(operation.runner).cancel()

    async def close() -> None:
        closed.append(operation.runner.report)
        await operation.runner.aclose()
        closed.append("closed")

    operation.after_latch.append(cancel_the_end)
    error = raised_driving_the_loop(operation.read_one, close)
    assert error is operation.interrupt
    assert _ending(operation.runner).cancelled()
    assert closed == [None, "closed"]
    report = operation.runner.report
    assert report is not None
    assert report.terminal_reason == "operation interrupted"
    assert getattr(error, "report", None) is report


def test_a_close_cancelled_while_it_runs_the_end_raises_its_cancellation_once_the_end_is_over() -> None:
    # The close runs the end nobody began (its task was cancelled before its first step) and is cancelled while the
    # cleanup waits: the end still finishes and publishes, the cancellation recorded as cleanup's, and the close
    # raises that cancellation rather than return as if nobody had cancelled it.
    operation = _BetweenReads()
    got: list[object] = []

    async def cancel_the_end() -> None:
        _ending(operation.runner).cancel()

    async def close_cancelled() -> None:
        closing = asyncio.create_task(operation.runner.aclose())
        await operation.cleaning.wait()
        closing.cancel()
        await asyncio.sleep(0)
        operation.release.set()
        got.extend(await asyncio.gather(closing, return_exceptions=True))
        got.append(operation.runner.report)

    operation.after_latch.append(cancel_the_end)
    error = raised_driving_the_loop(operation.read_one, close_cancelled)
    assert error is operation.interrupt
    report = operation.runner.report
    assert report is not None
    assert [type(item) for item in got] == [asyncio.CancelledError, KernelReport]
    assert got[1] is report
    assert report.terminal_reason == "operation interrupted"


def test_the_operations_own_work_cannot_close_it_once_an_interrupt_ended_it_between_reads() -> None:
    # The work that latched the interrupt closes the stream at once, before the end has begun: that close would run
    # an end that waits for the operation's work, its own task included, so it is refused, as from the pull or cleanup.
    operation = _BetweenReads()
    operation.release.set()
    refused: list[str] = []

    async def close_from_the_work() -> None:
        try:
            await operation.runner.aclose()
        except RuntimeError as refusal:
            refused.append(str(refusal))

    async def wait_for_the_end() -> None:
        await operation.runner.aclose()

    operation.after_latch.append(close_from_the_work)
    error = raised_driving_the_loop(operation.read_one, wait_for_the_end)
    assert error is operation.interrupt
    assert len(refused) == 1
    assert refused[0].startswith("a stream cannot be closed from inside its own pull or cleanup")
    report = operation.runner.report
    assert report is not None
    assert report.terminal_reason == "operation interrupted"


def test_a_second_interrupt_latched_in_the_ends_cleanup_goes_to_the_event_loop_after_the_first() -> None:
    # The end that follows an interrupt between reads begins once the event loop raised it. Its cleanup latches
    # another interrupt, twice, and the first again: nobody reads to raise them, so the event loop raises the other
    # one, once, as it raised the first; the first is never raised again.
    operation = _BetweenReads()
    second = SystemExit("in cleanup")
    operation.latched_in_cleanup.extend([second, second, operation.interrupt])
    operation.release.set()
    raised: list[BaseException] = []
    loop = asyncio.new_event_loop()
    try:
        work = loop.create_task(operation.read_one())
        for step in (lambda: work, lambda: _ending(operation.runner)):
            try:
                loop.run_until_complete(step())
            except BaseException as error:  # noqa: BLE001 - the test inspects what the loop raised
                raised.append(error)
        loop.run_until_complete(asyncio.wait_for(operation.runner.aclose(), HANG_BOUND))
        work.cancel()
        loop.run_until_complete(asyncio.gather(work, return_exceptions=True))
    finally:
        loop.close()
    assert raised == [operation.interrupt, second]
    assert (operation.cleanups, operation.finalized) == (1, [TerminalCause.INTERRUPTED])
    report = operation.runner.report
    assert report is not None
    assert getattr(operation.interrupt, "report", None) is report
