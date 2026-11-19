"""Cancellation-independent cleanup of client-owned resources."""

from __future__ import annotations
import functools
import weakref
from typing import TYPE_CHECKING, Protocol

from b24api.execution.context import OperationLatch

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable


class CloseableResource(Protocol):
    """Minimal internal resource ownership contract."""

    async def aclose(self) -> None:
        """Close the resource."""
        ...


class OwnedStream(CloseableResource, Protocol):
    """A client-registered stream that knows whether the current task runs inside its own work."""

    @property
    def inside_own_work(self) -> bool:
        """Whether closing the stream from the current task would wait for that task itself."""
        ...

    def when_started(self, hook: Callable[[], None]) -> None:
        """Call ``hook`` once, when the stream's first read starts its work."""
        ...


class StreamRegistry:
    """The streams a client owns: each open one weakly, and each started one strongly until it ends.

    A started stream the application dropped still runs tasks that nothing else keeps, so the registry keeps it, and
    the client's close ends it; one never started holds nothing and is let go with its last reference.
    """

    __slots__ = ("_open", "_started")

    def __init__(self) -> None:
        """Hold no stream yet."""
        self._open: weakref.WeakSet[OwnedStream] = weakref.WeakSet()
        self._started: set[OwnedStream] = set()

    def add(self, stream: OwnedStream) -> None:
        """Own ``stream``, strongly once its work starts."""
        self._open.add(stream)
        stream.when_started(functools.partial(self._started.add, stream))

    def discard(self, stream: OwnedStream) -> None:
        """Let go of a stream that ended."""
        self._open.discard(stream)
        self._started.discard(stream)

    def snapshot(self) -> tuple[OwnedStream, ...]:
        """Return the streams owned now."""
        return tuple(self._open)

    def __len__(self) -> int:
        """Return how many streams are owned now."""
        return len(self._open)


async def close_owned_resources(
    streams: Iterable[CloseableResource],
    transport: CloseableResource | None,
    coordinator: CloseableResource | None = None,
) -> None:
    """Attempt every owned close and preserve the first cleanup failure; an escape outranks it (``OperationLatch``).

    Order: streams, then the owned rate coordinator (its cooldown wake task is awaited), then the
    owned transport. The interrupt, else the first other escape, is raised; any other escape goes to the event loop.
    """
    ordered: list[tuple[CloseableResource, str]] = [(stream, "additional stream") for stream in streams]
    ordered.extend(
        (resource, label)
        for resource, label in ((coordinator, "coordinator"), (transport, "transport"))
        if resource is not None
    )
    escapes = OperationLatch()
    primary: BaseException | None = None
    for resource, label in ordered:
        try:
            await resource.aclose()
        except BaseException as error:  # noqa: BLE001 - every owned resource must still be closed
            escapes.record(error)
            if primary is None:
                primary = error
            else:
                primary.add_note(f"{label} cleanup failure: {type(error).__name__}")
    if (escape := escapes.close()) is not None:
        raise escape
    if primary is not None:
        raise primary


__all__ = ["CloseableResource", "OwnedStream", "StreamRegistry", "close_owned_resources"]
