"""A fast keyset reference binding's budget view of the shared operation context.

The binding holds one fixed row lease ``G`` in the shared row pool from activation to terminal. Its
runtime plans and fits every wave against a projection of the operation policy that differs only in
``max_buffered_rows=G``; requests, attempts, pages, deadline and bytes stay on the shared context,
which is the only context the engine and the executor ever receive.
"""

from __future__ import annotations
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from b24api.errors import BudgetExceededError

if TYPE_CHECKING:
    from b24api.contracts.policy import ExecutionPolicy
    from b24api.execution.context import ExecutionContext, ExecutionSnapshot, PageReservation
    from b24api.references.dispatch import _Reservation


@dataclass(slots=True, eq=False)
class RowLease:
    """A fixed capacity in the shared row pool and the rows currently held against it."""

    capacity: int
    held: int = field(default=0, init=False)
    reservation: _Reservation | None = field(default=None, repr=False)

    @classmethod
    def of(cls, reservation: _Reservation) -> RowLease:
        """Lease the fixed capacity of one pool reservation."""
        return cls(reservation.amount, reservation)

    @property
    def revoked(self) -> bool:
        """Return whether closing the shared pool already reset its capacity and the shared row counter."""
        return self.reservation is not None and self.reservation.amount == 0

    def __post_init__(self) -> None:
        """Validate the fixed capacity."""
        if isinstance(self.capacity, bool) or not isinstance(self.capacity, int) or self.capacity < 1:
            raise ValueError("row lease capacity must be a positive integer")

    def charge(self, rows: int) -> None:
        """Hold more rows; a charge beyond the owned free capacity fails without changing state."""
        if rows < 0:
            raise ValueError("row lease charge must be non-negative")
        if self.held + rows > self.capacity:
            raise BudgetExceededError("fast keyset rows exceed the binding's row lease")
        self.held += rows

    def receipt(self, rows: int) -> None:
        """Return held rows to the lease."""
        if rows < 0 or rows > self.held:
            raise RuntimeError("fast keyset lease returned rows it does not hold")
        self.held -= rows


@dataclass(frozen=True, slots=True)
class BindingBudgetView:
    """``KeysetContext`` of one fast keyset reference binding over the shared operation context."""

    shared: ExecutionContext
    lease: RowLease
    projection: ExecutionPolicy

    @classmethod
    def over(cls, shared: ExecutionContext, lease: RowLease) -> BindingBudgetView:
        """Project the operation policy onto the lease: only ``max_buffered_rows`` changes."""
        return cls(shared, lease, replace(shared.policy, max_buffered_rows=lease.capacity))

    @property
    def policy(self) -> ExecutionPolicy:
        """Return the projection planning and every execution fit use."""
        return self.projection

    @property
    def execution_context(self) -> ExecutionContext:
        """Return the shared operation context requests are charged to."""
        return self.shared

    async def start(self) -> None:
        """Start the shared operation clock."""
        await self.shared.start()

    async def snapshot(self) -> ExecutionSnapshot:
        """Return the shared operation counters."""
        return await self.shared.snapshot()

    async def reserve_page(self, *, reference: str | None = None) -> PageReservation:
        """Reserve one page of the shared page budget."""
        return await self.shared.reserve_page(reference=reference)

    async def reserve_pages(self, count: int) -> tuple[PageReservation, ...]:
        """Reserve a page wave of the shared page budget."""
        return await self.shared.reserve_pages(count)

    def commit_page(self, reservation: PageReservation) -> None:
        """Charge one decoded page to the shared budget."""
        self.shared.commit_page(reservation)

    def release_page(self, reservation: PageReservation) -> None:
        """Release one unused page reservation."""
        self.shared.release_page(reservation)

    async def adjust_buffered_rows(self, delta: int) -> None:
        """Charge or return lease rows, then apply exactly one delta to the shared row counter."""
        if isinstance(delta, bool) or not isinstance(delta, int):
            raise TypeError("buffer delta must be an integer")
        if delta >= 0:
            self.lease.charge(delta)
        else:
            self.lease.receipt(-delta)
            if self.lease.revoked:
                # The closed pool zeroed the shared counter with every lease in it; nothing remains to return.
                return
        try:
            await self.shared.adjust_buffered_rows(delta)
        except BaseException:
            # The shared counter refused the delta: the lease keeps its previous holding.
            if delta >= 0:
                self.lease.receipt(delta)
            else:
                self.lease.charge(-delta)
            raise


__all__ = ["BindingBudgetView", "RowLease"]
