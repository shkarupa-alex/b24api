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

from b24api import Bitrix24, ReplaySafety, Request, RouteKind, Settings
from b24api.batch.engine import BatchExecutor
from b24api.batch.logical import LogicalBatchKernelStream
from b24api.batch.stream import batch_outcome_stream
from b24api.contracts import Command
from b24api.contracts.policy import ExecutionPolicy, IdentityCoercion
from b24api.contracts.request import IdentitySpec
from b24api.execution import Executor, WireResponse
from b24api.references.outcome import ReferenceRequest
from b24api.references.stream import iter_references
from b24api.traversal.plans import KernelDirectDispatch, SingleResponsePlan
from b24api.traversal.stream import iter_list

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from b24api.execution.lifecycle import OperationRunner

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
async def test_cancelled_read_with_failing_cleanup_raises_it_and_rearms_the_cancellation(
    family: Family,
    client: Bitrix24,
) -> None:
    harness = _harness(family, client, body="block", cleanup="fail")
    seen: list[BaseException] = []

    async def consume() -> None:
        try:
            await _drain(harness.stream)
        except _CleanupFailedError as error:
            seen.append(error)
        await asyncio.sleep(0)
        pytest.fail("the deferred cancellation was not re-armed")

    task = asyncio.create_task(consume())
    await _cancel_blocked_read(harness, task)
    error = await _outcome(task)

    assert isinstance(error, asyncio.CancelledError)
    assert len(seen) == 1
    harness.assert_published_once(seen[0])
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
async def test_failed_body_with_failing_cleanup_keeps_the_failure_and_notes_the_cleanup(
    family: Family,
    client: Bitrix24,
) -> None:
    harness = _harness(family, client, body="fail", cleanup="fail")

    error = await _outcome(asyncio.create_task(_drain(harness.stream)))

    assert error is not None
    assert _is_body_failure(error)
    primary = error if isinstance(error, _BodyFailedError) else error.__cause__
    assert primary is not None
    assert "stream cleanup also failed (_CleanupFailedError)" in getattr(primary, "__notes__", [])
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
