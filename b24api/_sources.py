"""One owner for caller item sources: lazy pulls, item admission, bounded observation and close.

Every stream family adapts its caller source here instead of keeping its own sync and async
iterator wrappers. A synchronous iterator is pulled and closed in a worker thread, and a pull
that is cancelled still finishes before the cancellation propagates, so the iterator is never
closed while it runs. ``accept`` maps each raw item to the family's item and raises for a
foreign one, ``observe`` records a bounded violation per admitted item, and ``failure`` maps a
pull or admission failure onto the family's carrier. ``aclose`` never raises: it returns the
cleanup outcome and the lifecycle owner decides what to report.
"""

from __future__ import annotations
import asyncio
import contextlib
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable, Iterator
from enum import Enum
from typing import TYPE_CHECKING, Protocol, Self, runtime_checkable

from b24api.contracts.violation import retain_violations
from b24api.execution.context import CleanupResult, await_cleanup_resistant

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


async def _finish_in_thread[R](call: Callable[[], R]) -> R:
    """Run ``call`` in a worker thread that a cancellation waits for instead of abandoning."""
    work = asyncio.create_task(asyncio.to_thread(call))
    try:
        return await asyncio.shield(work)
    except asyncio.CancelledError:
        with contextlib.suppress(BaseException):
            await work
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
        self._accept = accept
        self._observe = observe
        self._failure = failure
        self._admitted = 0
        self._violations: tuple[Violation, ...] = ()
        self._close: asyncio.Future[None] | None = None

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
        try:
            raw = await self._pull()
            if raw is EXHAUSTED:
                return EXHAUSTED
            item = self._accept(raw, self._admitted)
        except Exception as error:
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
            try:
                return await anext(iterator)
            except StopAsyncIteration:
                return EXHAUSTED
        if self._inline:
            return _next_or_exhausted(iterator)
        return await _finish_in_thread(lambda: _next_or_exhausted(iterator))

    async def aclose(self) -> CleanupResult:
        """Close the caller iterator once; every call returns the same outcome and none raises."""
        if self._close is None:
            self._close = asyncio.ensure_future(self._close_iterator())
        return await await_cleanup_resistant(self._close)

    async def _close_iterator(self) -> None:
        iterator = self._iterator
        if isinstance(iterator, AsyncIterator):
            if isinstance(iterator, AsyncClosable):
                await iterator.aclose()
        elif isinstance(iterator, SyncClosable):
            await _finish_in_thread(iterator.close)


__all__ = ["EXHAUSTED", "AsyncClosable", "Exhausted", "OwnedSource", "SyncClosable"]
