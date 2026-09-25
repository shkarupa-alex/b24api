"""Deterministic suspension of one execution ledger for cancellation-race regressions.

The ledger has no lock (C8), so these tests can no longer park a task by holding one. ``LedgerHold``
reproduces what a held lock did: while held, every async ledger call on that one instance waits for
the release, which lets a test cancel a task at exactly that await.
"""

from __future__ import annotations
import asyncio
import functools
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from b24api.execution.context import ExecutionContext

_ASYNC_LEDGER_CALLS = (
    "start",
    "reserve_attempt",
    "record_retry",
    "record_cooldown",
    "reserve_page",
    "reserve_pages",
    "set_buffered_rows",
    "adjust_buffered_rows",
    "snapshot",
)


class LedgerHold:
    """Make every async call on one ledger wait while the hold is taken."""

    def __init__(self, context: ExecutionContext) -> None:
        """Wrap the ledger's async calls; the hold starts released."""
        self._released = asyncio.Event()
        self._released.set()
        self.blocked = asyncio.Event()
        for name in _ASYNC_LEDGER_CALLS:
            setattr(context, name, self._gated(getattr(context, name)))

    def acquire(self) -> None:
        """Park every later async ledger call until :meth:`release`."""
        self._released.clear()
        self.blocked.clear()

    def release(self) -> None:
        """Let parked and later calls proceed."""
        self._released.set()

    def _gated[**P, R](self, call: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @functools.wraps(call)
        async def gated(*args: P.args, **kwargs: P.kwargs) -> R:
            if not self._released.is_set():
                self.blocked.set()
                await self._released.wait()
            return await call(*args, **kwargs)

        return gated
