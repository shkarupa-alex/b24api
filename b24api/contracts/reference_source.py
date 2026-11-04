"""One heterogeneous reference source."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from b24api.contracts.reference import Binding
from b24api.contracts.request import Request
from b24api.contracts.traversal import (
    ContinuationTraversal,
    CountedTraversal,
    CursorTraversal,
    InclusiveWindowTraversal,
    KeysetTraversal,
    SequentialTraversal,
)

if TYPE_CHECKING:
    from b24api.contracts.page_stop import PageStopPolicy
    from b24api.contracts.traversal import TraversalSpec

_TRAVERSAL_TYPES = (
    SequentialTraversal,
    CountedTraversal,
    KeysetTraversal,
    CursorTraversal,
    ContinuationTraversal,
    InclusiveWindowTraversal,
)


@dataclass(frozen=True, slots=True)
class ReferenceSource[C]:
    """One heterogeneous reference source: its own request template, traversal, binding and stop policy.

    Sources of one ``iter_reference_sources`` operation share its dispatcher, row pool, execution
    budgets and completion gate; each keeps its own identities, page state and terminal event. A
    valid source the operation cannot run (for example a counted traversal under DirectDispatch)
    becomes a correlated local non-execution before any HTTP request.
    """

    request: Request = field(repr=False)
    traversal: TraversalSpec
    binding: Binding[C]
    page_stop: PageStopPolicy | None = field(default=None, kw_only=True, repr=False)

    def __post_init__(self) -> None:
        """Validate the source's parts without lowering or reading anything."""
        if not isinstance(self.request, Request):
            raise TypeError("request must be a Request")
        if not isinstance(self.traversal, _TRAVERSAL_TYPES):
            raise TypeError("traversal must be a TraversalSpec")
        if not isinstance(self.binding, Binding):
            raise TypeError("binding must be a Binding")
        if self.page_stop is not None and not callable(getattr(self.page_stop, "on_page", None)):
            raise TypeError("page_stop must implement on_page(boundary) or be None")


__all__ = ["ReferenceSource"]
