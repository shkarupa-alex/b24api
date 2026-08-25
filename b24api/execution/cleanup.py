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


async def close_owned_resources(
    streams: Iterable[CloseableResource],
    transport: CloseableResource | None,
) -> None:
    """Attempt every owned close and preserve the first cleanup failure."""
    primary: BaseException | None = None
    for stream in streams:
        try:
            await stream.aclose()
        except BaseException as error:  # noqa: BLE001 - every owned resource must still be closed
            if primary is None:
                primary = error
            else:
                primary.add_note(f"additional stream cleanup failure: {type(error).__name__}")
    if transport is not None:
        try:
            await transport.aclose()
        except BaseException as error:  # noqa: BLE001 - retain failure after ordered cleanup
            if primary is None:
                primary = error
            else:
                primary.add_note(f"transport cleanup failure: {type(error).__name__}")
    if primary is not None:
        raise primary


__all__ = ["CloseableResource", "close_owned_resources"]
