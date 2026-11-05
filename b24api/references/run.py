"""Mutable state of one running reference binding, shared by the scheduler and its I/O seams."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncio

    from b24api.completion.reference_recorder import ReferenceBindingRecorder
    from b24api.references.dispatch import _Event, _FailureEvent, _Reservation, _Work
    from b24api.references.program import SourceProgram
    from b24api.traversal import PaginationDriver


@dataclass(slots=True)
class _ReferenceRun:
    """Mutable state of one reference transaction, shared by the driver's fetch hook and the page loop."""

    work: _Work
    output: asyncio.Queue[_Event]
    completion: ReferenceBindingRecorder
    producer_key: str
    program: SourceProgram
    driver: PaginationDriver = field(init=False)
    reservation: _Reservation | None = None
    partial_rows: int = 0
    page_state: int = 0
    violation_offset: int = 0
    trace_offset: int = 0
    scheduled_sequences: list[int] = field(default_factory=list)
    stopped_reason: str | None = None
    page_admission: asyncio.Future[None] | None = None
    settlement: asyncio.Future[None] | None = None
    tail_active: bool = False
    # A fast keyset binding prepares its own terminal failure before re-raising into the scheduler.
    failure: _FailureEvent | None = None

    def admit_page(self) -> None:
        """Release the dispatcher's admission slot once the page is in the row buffer."""
        if self.page_admission is not None and not self.page_admission.done():
            self.page_admission.set_result(None)
        self.page_admission = None

    def settle_page(self) -> None:
        """Release the dispatcher's settlement wait for the current page."""
        if self.settlement is not None and not self.settlement.done():
            self.settlement.set_result(None)
        self.settlement = None


__all__ = ["_ReferenceRun"]
