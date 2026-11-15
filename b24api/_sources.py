"""One owner for caller item sources: lazy pulls, item admission, bounded observation and close.

Every stream family adapts its caller source here instead of keeping its own sync and async
iterator wrappers. A source's pulls and its close all run in one ``contextvars.Context``, copied
when the source is adapted, as a generator iterated in one place runs: a context variable it sets
before a ``yield`` is still set, and its token still resettable, when it resumes. An asynchronous
iterator's steps are driven by the task that reads or closes it, each resumption entering that
context. A synchronous iterator is pulled and closed in a worker thread, and a pull that is
cancelled still finishes before the cancellation propagates. Its steps run one at a time,
serialized by a lock taken in the worker thread: a close that the operation's end starts while an
abandoned pull still runs (its deadline passed, or the loop cancelled every task as it shut down)
waits there for that pull to return, so the iterator is never closed while it runs and the context
is never entered twice at once. The wait is bounded on the event loop by the operation's cleanup
window; past it the close finishes in the background. ``accept`` maps each raw item to the
family's item and raises for a foreign one, ``observe`` records a bounded violation per admitted
item, and ``failure`` maps a pull, admission or close failure onto the family's carrier.
``aclose`` never raises: it returns the cleanup outcome and the lifecycle owner decides what to
report.
"""

from __future__ import annotations
import asyncio
import contextlib
import contextvars
import functools
import threading
import types
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Generator, Iterable, Iterator
from enum import Enum
from typing import TYPE_CHECKING, Protocol, Self, runtime_checkable

from b24api.contracts.violation import retain_violations
from b24api.execution.context import (
    WORK_OWNERS,
    CleanupResult,
    Settled,
    await_cleanup_resistant,
    spawn_settled,
)

if TYPE_CHECKING:
    from b24api.contracts.report import Violation


@runtime_checkable
class SyncClosable(Protocol):
    """A synchronous iterator that owns resources."""

    def close(self) -> None:
        """Release the iterator's resources."""
        ...


@runtime_checkable
class AsyncClosable(Protocol):
    """An asynchronous iterator that owns resources."""

    async def aclose(self) -> None:
        """Release the iterator's resources."""
        ...


class Exhausted(Enum):
    """The source has no further items."""

    EXHAUSTED = "exhausted"


EXHAUSTED = Exhausted.EXHAUSTED


def _next_or_exhausted(iterator: Iterator[object]) -> object:
    # StopIteration cannot cross a future, so exhaustion comes back as a value.
    return next(iterator, EXHAUSTED)


class _InContext[R](Generator[object, object, R]):
    """The steps of an awaitable, each resumption run in one context, driven by the task that awaits them.

    As ``yield from`` delegates each send, throw and close to it, the awaitable runs as if awaited directly (no task of
    its own, no extra loop turn, the same current task) except that every resumption enters ``context``.
    """

    __slots__ = ("_context", "_steps")

    def __init__(self, step: Awaitable[R], context: contextvars.Context) -> None:
        self._steps = step.__await__()
        self._context = context

    def send(self, value: object, /) -> object:
        return self._context.run(self._steps.send, value)

    def throw(self, error: BaseException, /, *_: object) -> object:  # type: ignore[override]
        return self._context.run(self._steps.throw, error)

    def close(self) -> None:
        self._context.run(self._steps.close)


@types.coroutine
def _in_context[R](step: Awaitable[R], context: contextvars.Context) -> Generator[object, object, R]:
    return (yield from _InContext(step, context))


async def _finish_in_thread[R](call: Callable[[], R]) -> R:
    """Run ``call`` in a worker thread that a cancellation waits for instead of abandoning.

    The cancelled call's failure is dropped, but not an interrupt (``KeyboardInterrupt``, ``SystemExit``): it is raised.
    """
    work = spawn_settled(asyncio.to_thread(call))
    try:
        return (await asyncio.shield(work)).unwrap()
    except asyncio.CancelledError:
        ended: Settled[R] | None = None
        with contextlib.suppress(asyncio.CancelledError):
            ended = await work
        if ended is not None and isinstance(ended.error, KeyboardInterrupt | SystemExit):
            raise ended.error from None
        raise


class OwnedSource[T](AsyncIterator[T]):
    """Own one caller iterator from its first pull until it is closed exactly once."""

    def __init__(
        self,
        iterator: Iterator[object] | AsyncIterator[object],
        *,
        inline: bool,
        accept: Callable[[object, int], T],
        observe: Callable[[T], Violation | None] | None,
        failure: Callable[[Exception], Exception] | None,
    ) -> None:
        """Hold an already opened iterator; use :meth:`adapt` to build one."""
        self._iterator = iterator
        self._inline = inline
        self._context = contextvars.copy_context()
        # Held by the worker thread running a pull or the close in ``_context``: one call at a time.
        self._context_lock = threading.Lock()
        self._accept = accept
        self._observe = observe
        self._failure = failure
        self._admitted = 0
        # What ``accept`` raised for an item the source yielded, when that refusal is how the source ended.
        self.rejected: Exception | None = None
        self._violations: tuple[Violation, ...] = ()
        self._close: asyncio.Future[Settled[None]] | None = None

    @classmethod
    def adapt(
        cls,
        source: Iterable[object] | AsyncIterable[object],
        *,
        accept: Callable[[object, int], T],
        observe: Callable[[T], Violation | None] | None = None,
        failure: Callable[[Exception], Exception] | None = None,
        inline_sequences: bool = False,
    ) -> OwnedSource[T]:
        """Open ``source`` now; ``accept`` receives each raw item with its admission index.

        A synchronous source is pulled in a worker thread. With ``inline_sequences`` a built-in
        list or tuple, which cannot block, is read on the event loop instead; kernels adopting
        their own raw input use it, while public families keep the worker-thread pacing.
        """
        if isinstance(source, AsyncIterable):
            return cls(aiter(source), inline=True, accept=accept, observe=observe, failure=failure)
        inline = inline_sequences and (source.__class__ is list or source.__class__ is tuple)
        return cls(iter(source), inline=inline, accept=accept, observe=observe, failure=failure)

    @property
    def violations(self) -> tuple[Violation, ...]:
        """Return the retained observation violations, bounded like a report."""
        return self._violations

    def __aiter__(self) -> Self:
        """Return this asynchronous iterator."""
        return self

    async def __anext__(self) -> T:
        """Return the next admitted item."""
        item = await self.next()
        if item is EXHAUSTED:
            raise StopAsyncIteration
        return item

    async def next(self) -> T | Exhausted:
        """Pull and admit one item, or return :data:`EXHAUSTED`."""
        admitting = False
        try:
            raw = await self._pull()
            if raw is EXHAUSTED:
                return EXHAUSTED
            admitting = True
            item = self._accept(raw, self._admitted)
        except Exception as error:
            if admitting:
                self.rejected = error
            if self._failure is None:
                raise
            raise self._failure(error) from error
        self._admitted += 1
        if self._observe is not None:
            violation = self._observe(item)
            if violation is not None:
                self._violations = retain_violations((*self._violations, violation))
        return item

    async def _pull(self) -> object:
        iterator = self._iterator
        if isinstance(iterator, AsyncIterator):
            return await self._run_step(anext(iterator, EXHAUSTED))
        if self._inline:
            return _next_or_exhausted(iterator)
        return await _finish_in_thread(functools.partial(self._run_in_context, _next_or_exhausted, iterator))

    async def aclose(self) -> CleanupResult:
        """Close the caller iterator once; every call returns the same outcome and none raises."""
        if self._close is None:
            self._close = spawn_settled(self._close_iterator())
        return await await_cleanup_resistant(self._close)

    async def _close_iterator(self) -> None:
        iterator = self._iterator
        try:
            if isinstance(iterator, AsyncIterator):
                if isinstance(iterator, AsyncClosable):
                    await self._run_step(iterator.aclose())
            elif isinstance(iterator, SyncClosable):
                await _finish_in_thread(functools.partial(self._run_in_context, iterator.close))
        except Exception as error:
            # The caller's close failing is its source failing, mapped as a failed pull is.
            if self._failure is None:
                raise
            raise self._failure(error) from error

    async def _run_step[R](self, step: Awaitable[R]) -> R:
        """Run one step of an asynchronous source, a pull or its close, in the source's context.

        The task driving the step drives it, and the context is given that task's work marker (``WORK_OWNERS``), so
        a close the source requests of its own stream is still refused as it would be in the driving task.
        """
        self._context.run(WORK_OWNERS.set, WORK_OWNERS.get())
        return await _in_context(step, self._context)

    def _run_in_context[R](self, call: Callable[..., R], *args: object) -> R:
        """Run ``call`` in the source's context in a worker thread, after any call still running there returns."""
        with self._context_lock:
            return self._context.run(call, *args)


__all__ = ["EXHAUSTED", "AsyncClosable", "Exhausted", "OwnedSource", "SyncClosable"]
