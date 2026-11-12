"""Cancellation-independent cleanup of client-owned resources."""

from __future__ import annotations
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterable


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


async def close_owned_resources(
    streams: Iterable[CloseableResource],
    transport: CloseableResource | None,
    coordinator: CloseableResource | None = None,
) -> None:
    """Attempt every owned close and preserve the first cleanup failure.

    Order: streams, then the owned rate coordinator (its cooldown wake task is awaited), then the
    owned transport.
    """
    ordered: list[tuple[CloseableResource, str]] = [(stream, "additional stream") for stream in streams]
    ordered.extend(
        (resource, label)
        for resource, label in ((coordinator, "coordinator"), (transport, "transport"))
        if resource is not None
    )
    primary: BaseException | None = None
    for resource, label in ordered:
        try:
            await resource.aclose()
        except BaseException as error:  # noqa: BLE001 - every owned resource must still be closed
            if primary is None:
                primary = error
            else:
                primary.add_note(f"{label} cleanup failure: {type(error).__name__}")
    if primary is not None:
        raise primary


__all__ = ["CloseableResource", "OwnedStream", "close_owned_resources"]
