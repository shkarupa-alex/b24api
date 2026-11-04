"""One lowered traversal program per reference source.

A homogeneous reference stream runs every binding on the scheduler's single default program; a
heterogeneous stream lowers each admitted source to its own program, so sources with different
methods, traversals, selectors and page caps share one dispatcher, one row pool and one gate.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from b24api.contracts.page import PageAdapter
    from b24api.contracts.page_stop import PageStopPolicy
    from b24api.contracts.report import TraversalAssurance
    from b24api.contracts.request import ResultSelector, TraversalIdentity
    from b24api.traversal.plans import ListPlan


@dataclass(frozen=True, slots=True, eq=False)
class SourceProgram:
    """The lowered plan, selection, identity, row cap and stop policy one reference runs on.

    ``counted_page_size`` is set only for a counted traversal on the shared batch queue: its head
    and dense tail then travel through the shared dispatcher (``SharedCountedWaveIO``) instead of
    the sequential counted strategy.
    """

    plan: ListPlan
    selector: ResultSelector | None
    identity: TraversalIdentity | None
    page_adapter: PageAdapter
    page_cap: int
    page_cap_hint: int | None = None
    page_stop: PageStopPolicy | None = None
    whole_result: bool = False
    counted_page_size: int | None = None
    assurance: TraversalAssurance | None = None

    def __post_init__(self) -> None:
        """Validate the row cap every page of this program reserves."""
        if not isinstance(self.page_cap, int) or isinstance(self.page_cap, bool) or self.page_cap < 1:
            raise ValueError("source program page cap must be a positive integer")


__all__ = ["SourceProgram"]
