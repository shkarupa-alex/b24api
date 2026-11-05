"""The execution seams a fast keyset runtime runs on: its budget context and its wave engine.

A flat traversal runs on the operation ``ExecutionContext`` and a private ``BatchExecutor``. A
reference binding runs on a per-binding budget view of the shared operation context and on the
shared reference dispatcher. Traversal never imports references: both sides meet in these protocols.
"""

from __future__ import annotations
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from b24api.batch.outcome import BatchOutcome
    from b24api.contracts.policy import ExecutionPolicy
    from b24api.contracts.request import Request
    from b24api.execution.context import ExecutionContext, ExecutionSnapshot, PageReservation


class KeysetContext(Protocol):
    """Exactly the context members the fast keyset modules use, with ``ExecutionContext`` signatures."""

    @property
    def policy(self) -> ExecutionPolicy:
        """Return the policy planning and every execution fit are computed from."""
        ...

    @property
    def execution_context(self) -> ExecutionContext:
        """Return the operation context the engine and the executor charge requests to."""
        ...

    async def start(self) -> None:
        """Start the operation clock once."""
        ...

    async def snapshot(self) -> ExecutionSnapshot:
        """Return the current operation counters."""
        ...

    async def reserve_page(self, *, reference: str | None = None) -> PageReservation:
        """Reserve one logical page before I/O."""
        ...

    async def reserve_pages(self, count: int) -> tuple[PageReservation, ...]:
        """Atomically reserve a page wave."""
        ...

    def commit_page(self, reservation: PageReservation) -> None:
        """Charge one decoded page."""
        ...

    def release_page(self, reservation: PageReservation) -> None:
        """Release a page that returned no decoded response."""
        ...

    async def adjust_buffered_rows(self, delta: int) -> None:
        """Account retained decoded rows by a signed delta."""
        ...


class WaveEngine(Protocol):
    """Execute one atomic correlated wave of fast keyset commands."""

    @property
    def portal_command_cap(self) -> int:
        """Return the most commands one wave may carry."""
        ...

    async def execute_requests(
        self,
        requests: tuple[Request, ...],
        *,
        context: ExecutionContext,
        advisory_totals: bool = False,
        strict_envelope: bool = False,
        strict_json_members: bool = False,
    ) -> tuple[BatchOutcome, ...]:
        """Return one correlated outcome per request, in request order."""
        ...


__all__ = ["KeysetContext", "WaveEngine"]
