"""The one pure source-compatibility rule of reference operations (S6).

Homogeneous facades call it before any binding is read and raise its error; a heterogeneous
operation calls it for each admitted source and turns its error into a correlated local
non-execution of that source, before any HTTP request.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.contracts.dispatch import DirectDispatch
from b24api.contracts.traversal import CountedTraversal
from b24api.contracts.wire import BodyEncoding
from b24api.errors import CapabilityError

if TYPE_CHECKING:
    from b24api.contracts.dispatch import DispatchSpec
    from b24api.contracts.page_stop import PageStopPolicy
    from b24api.contracts.request import Request
    from b24api.contracts.traversal import TraversalSpec


def source_incompatibility(
    request: Request,
    traversal: TraversalSpec,
    dispatch: DispatchSpec,
    page_stop: PageStopPolicy | None,
) -> CapabilityError | None:
    """Return why one source cannot run under the operation's dispatch, or None when it can."""
    if not isinstance(dispatch, DirectDispatch) and (
        request.encoding is not BodyEncoding.JSON or request.headers.items
    ):
        return CapabilityError("physical batch supports JSON requests without scoped headers; use direct dispatch")
    if isinstance(traversal, CountedTraversal) and isinstance(dispatch, DirectDispatch):
        return CapabilityError("counted reference traversal requires BatchDispatch")
    if isinstance(traversal, CountedTraversal) and page_stop is not None:
        return CapabilityError("counted physical batch tail does not support page stop")
    return None


__all__ = ["source_incompatibility"]
