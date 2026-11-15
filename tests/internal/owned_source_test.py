"""The single caller-source owner: bounded observation, one close and cancellation-safe pulls (§3.2)."""

from __future__ import annotations
import asyncio
import contextvars
import threading
from typing import TYPE_CHECKING

import pytest

from b24api import ReplaySafety, Request, RouteKind
from b24api._sources import EXHAUSTED, OwnedSource
from b24api.contracts import Command
from b24api.contracts.report import Violation, ViolationSeverity
from b24api.contracts.violation import MAX_RETAINED_VIOLATIONS
from b24api.execution.context import CleanupResult
from tests.scripting import HANG_BOUND, WorkerStarts, reached

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

COMMANDS = 20_000


def _commands() -> Iterator[Command[int]]:
    request = Request("x.get", replay_safety=ReplaySafety.UNKNOWN, route=RouteKind.BARE)
    return (Command(request, index) for index in range(COMMANDS))


def _unknown(_command: object) -> Violation:
    return Violation(ViolationSeverity.WARNING, "unknown_request", "request was not declared")


def _identity(item: object, _index: int) -> object:
    return item


async def _drain(source: OwnedSource[object]) -> int:
    return len([item async for item in source])


@pytest.mark.asyncio
async def test_sync_source_retains_bounded_violations() -> None:
    source = OwnedSource.adapt(_commands(), accept=_identity, observe=_unknown)
    assert await _drain(source) == COMMANDS
    assert len(source.violations) == MAX_RETAINED_VIOLATIONS
    assert source.violations[-1].code == "violations_truncated"


@pytest.mark.asyncio
async def test_async_source_retains_bounded_violations() -> None:
    async def commands() -> AsyncIterator[Command[int]]:
        for command in _commands():
            yield command

    source = OwnedSource.adapt(commands(), accept=_identity, observe=_unknown)
    assert await _drain(source) == COMMANDS
    assert len(source.violations) == MAX_RETAINED_VIOLATIONS
    assert source.violations[-1].code == "violations_truncated"


class _ClosableIterator:
    def __init__(self, items: list[int], *, close_error: Exception | None = None) -> None:
        self._items = iter(items)
        self._close_error = close_error
        self.closes = 0

    def __iter__(self) -> _ClosableIterator:
        return self

    def __next__(self) -> int:
        return next(self._items)

    def close(self) -> None:
        self.closes += 1
        if self._close_error is not None:
            raise self._close_error


@pytest.mark.asyncio
async def test_exhaustion_leaves_closing_to_the_owner_and_close_happens_once() -> None:
    iterator = _ClosableIterator([1], close_error=RuntimeError("close failed"))
    source = OwnedSource.adapt(iterator, accept=lambda item, index: (index, item))
    assert await source.next() == (0, 1)
    assert await source.next() is EXHAUSTED
    assert iterator.closes == 0

    first = await source.aclose()
    second = await source.aclose()
    assert iterator.closes == 1
    assert isinstance(first.error, RuntimeError)
    assert second.error is first.error
    assert first.cancellation is None


@pytest.mark.asyncio
async def test_failure_maps_pull_and_admission_errors_onto_the_family_carrier() -> None:
    class CarrierError(Exception):
        pass

    def reject(item: object, _index: int) -> object:
        raise TypeError(f"foreign {item!r}")

    source = OwnedSource.adapt([1], accept=reject, failure=lambda _error: CarrierError())
    with pytest.raises(CarrierError) as raised:
        await source.next()
    assert isinstance(raised.value.__cause__, TypeError)

    def broken() -> Iterator[int]:
        raise ValueError("source failed")
        yield 1

    source = OwnedSource.adapt(broken(), accept=_identity, failure=lambda _error: CarrierError())
    with pytest.raises(CarrierError) as raised:
        await source.next()
    assert isinstance(raised.value.__cause__, ValueError)


@pytest.mark.asyncio
async def test_cancelled_sync_pull_finishes_before_the_iterator_is_closed() -> None:
    entered = threading.Event()
    release = threading.Event()
    trace: list[str] = []

    def items() -> Iterator[int]:
        try:
            entered.set()
            release.wait(5)
            trace.append("pulled")
            yield 1
        finally:
            trace.append("closed")

    source = OwnedSource.adapt(items(), accept=_identity)
    pull = asyncio.create_task(source.next())
    await asyncio.to_thread(entered.wait, 5)
    pull.cancel()
    await asyncio.sleep(0)
    assert not pull.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await pull

    result = await source.aclose()
    assert result.error is None
    assert trace == ["pulled", "closed"]


_TENANT: contextvars.ContextVar[str] = contextvars.ContextVar("tenant", default="caller")


def _tenant_scoped(seen: list[tuple[str, str]]) -> Iterator[int]:
    """Set a context variable around each yield and reset it with its token, as a scoped generator does."""
    for index in range(3):
        token = _TENANT.set(f"tenant-{index}")
        try:
            yield index
            seen.append(("resumed", _TENANT.get()))
        finally:
            _TENANT.reset(token)
            seen.append(("reset", _TENANT.get()))


@pytest.mark.asyncio
async def test_sync_source_keeps_one_context_across_its_pulls_and_close() -> None:
    # Each pull used to run in a fresh copy of the caller's context, so a token set before a yield belonged
    # to another context when the generator resumed, and resetting it raised ValueError.
    seen: list[tuple[str, str]] = []
    drained = OwnedSource.adapt(_tenant_scoped(seen), accept=_identity)
    assert await _drain(drained) == 3  # noqa: PLR2004
    assert seen == [
        (step, value) for index in range(3) for step, value in (("resumed", f"tenant-{index}"), ("reset", "caller"))
    ]
    assert await drained.aclose() == CleanupResult(None, None)

    # A close that interrupts the generator runs its cleanup in the same context as its pulls.
    seen.clear()
    interrupted = OwnedSource.adapt(_tenant_scoped(seen), accept=_identity)
    assert await interrupted.next() == 0
    assert await interrupted.aclose() == CleanupResult(None, None)
    assert seen == [("reset", "caller")]
    # The source's context is its own: the caller's context never sees what the generator sets.
    assert _TENANT.get() == "caller"


_PULL: contextvars.ContextVar[str] = contextvars.ContextVar("pull", default="caller")


@pytest.mark.asyncio
async def test_a_close_requested_while_a_sync_pull_runs_waits_for_it_in_the_same_context() -> None:
    # Only the owner's asyncio ordering kept the close from the pull's context: once that ordering broke (the
    # loop shutting down cancels every task, the pull's worker task included), the close entered the context
    # while the pull still ran in it and failed with "cannot enter context ... already entered".
    loop = asyncio.get_running_loop()
    workers = WorkerStarts(loop)
    loop.set_default_executor(workers)
    release = threading.Event()
    trace: list[tuple[str, str]] = []

    def items() -> Iterator[int]:
        try:
            _PULL.set("pull 1")
            yield 1
            _PULL.set("pull 2")
            release.wait(HANG_BOUND)
            trace.append(("pulled", _PULL.get()))
            yield 2
        finally:
            trace.append(("closed", _PULL.get()))

    source = OwnedSource.adapt(items(), accept=_identity)
    assert await source.next() == 1
    pull = asyncio.create_task(source.next())
    try:
        await reached(workers.started(1))
        close = asyncio.create_task(source.aclose())
        # The close's worker call has started while the pull still runs: it waits for the pull.
        await reached(workers.started(2))
        assert not close.done()
    finally:
        release.set()
    outcome = await asyncio.wait_for(close, timeout=HANG_BOUND)
    workers.shutdown(wait=True)

    assert await pull == 2  # noqa: PLR2004
    assert outcome == CleanupResult(None, None)
    assert trace == [("pulled", "pull 2"), ("closed", "pull 2")]
