"""One lowered traversal program per reference source.

A homogeneous reference stream runs every binding on the scheduler's single default program; a
heterogeneous stream lowers each admitted source to its own program, so sources with different
methods, traversals, selectors and page caps share one dispatcher, one row pool and one gate.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING

from b24api.contracts.keyset_execution import AutoKeysetExecution, PartitionedKeysetExecution

if TYPE_CHECKING:
    from b24api.contracts.keyset_execution import RangeKeysetExecution
    from b24api.contracts.page import PageAdapter
    from b24api.contracts.page_stop import PageStopPolicy
    from b24api.contracts.report import KeysetSelectionSummary, TraversalAssurance
    from b24api.contracts.request import IdentitySpec, ResultSelector, TraversalIdentity
    from b24api.contracts.response import ResultCollectionShape
    from b24api.contracts.traversal import KeysetSpec
    from b24api.traversal.plans import ListPlan

# A bounded fast keyset retains both boundary pages and one full probe or body page (3C), plus one
# retained row per occupied anchor; one more page of slack (4C) lets it run a two-command wave.
_FLOOR_PAGES = 3
_QUOTA_PAGES = 4


@dataclass(frozen=True, slots=True, eq=False)
class FastKeysetProgram:
    """One reference binding's fast keyset runtime inputs; the runtime itself is per binding."""

    identity: IdentitySpec
    keyset: KeysetSpec
    execution: RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution
    selector: ResultSelector
    collection_shape: ResultCollectionShape
    page_size: int
    effective_page_cap: int

    @property
    def anchors(self) -> int:
        """Return the anchor rows the plan may retain (A): target lanes of AUTO/Partitioned, 0 for Range."""
        if isinstance(self.execution, AutoKeysetExecution | PartitionedKeysetExecution):
            return self.execution.target_lanes
        return 0

    @property
    def floor(self) -> int:
        """Return P = 3C + A, the smallest lease a bounded plan can run in."""
        return _FLOOR_PAGES * self.effective_page_cap + self.anchors

    @property
    def quota(self) -> int:
        """Return Q = 4C + A, the lease a binding asks for when the pool allows it."""
        return _QUOTA_PAGES * self.effective_page_cap + self.anchors

    @property
    def automatic(self) -> bool:
        """Return whether the execution is AUTO (the only one admitted below the floor, exclusively)."""
        return isinstance(self.execution, AutoKeysetExecution)


@dataclass(frozen=True, slots=True, eq=False)
class SourceProgram:
    """The lowered plan, selection, identity, row cap and stop policy one reference runs on.

    ``counted_page_size`` is set only for a counted traversal on the shared batch queue: its head
    and dense tail then travel through the shared dispatcher (``SharedCountedWaveIO``) instead of
    the sequential counted strategy. ``fast`` is set only for a fast or automatic keyset: the binding
    then runs its own keyset runtime under a fixed row lease and sends its waves through the shared
    dispatcher.
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
    fast: FastKeysetProgram | None = None
    keyset_selection: KeysetSelectionSummary | None = None

    def __post_init__(self) -> None:
        """Validate the row cap every page of this program reserves."""
        if not isinstance(self.page_cap, int) or isinstance(self.page_cap, bool) or self.page_cap < 1:
            raise ValueError("source program page cap must be a positive integer")


__all__ = ["FastKeysetProgram", "SourceProgram"]
