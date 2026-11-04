"""The one pure source-compatibility rule of reference operations.

Homogeneous facades call it before any binding is read and raise its error; a heterogeneous
operation calls it for each admitted source and turns its error into a correlated local
non-execution of that source, before any HTTP request.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.contracts.dispatch import DirectDispatch
from b24api.contracts.keyset_execution import AutoKeysetExecution, SequentialKeysetExecution
from b24api.contracts.request import RouteKind
from b24api.contracts.traversal import CountedTraversal, KeysetTraversal
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
    *,
    v3_batch: bool,
) -> CapabilityError | None:
    """Return why one source cannot run under the operation's dispatch, or None when it can.

    ``v3_batch`` is the operation transport's declared support for native REST v3 batch envelopes.
    """
    if not isinstance(dispatch, DirectDispatch):
        unbatchable = _batch_incompatibility(request, v3_batch=v3_batch)
        if unbatchable is not None:
            return unbatchable
    if isinstance(traversal, CountedTraversal) and isinstance(dispatch, DirectDispatch):
        return CapabilityError("counted reference traversal requires BatchDispatch")
    if isinstance(traversal, CountedTraversal) and page_stop is not None:
        return CapabilityError("counted physical batch tail does not support page stop")
    if isinstance(traversal, KeysetTraversal) and not isinstance(traversal.execution, SequentialKeysetExecution):
        # AUTO with a page stop settles on sequential execution before planning; an explicit fast
        # execution cannot honour a stop decided between ordered pages.
        automatic = isinstance(traversal.execution, AutoKeysetExecution)
        if page_stop is not None and not automatic:
            return CapabilityError("page stop requires sequential keyset execution")
        if page_stop is None and isinstance(dispatch, DirectDispatch):
            return CapabilityError("fast keyset reference traversal requires BatchDispatch")
    return None


def _batch_incompatibility(request: Request, *, v3_batch: bool) -> CapabilityError | None:
    """Return why one request cannot travel in a physical batch envelope, or None when it can."""
    if request.encoding is not BodyEncoding.JSON or request.headers.items:
        return CapabilityError("physical batch supports JSON requests without scoped headers; use direct dispatch")
    if request.route is RouteKind.API_V3 and not v3_batch:
        return CapabilityError("transport does not support native REST v3 batch envelopes; use direct dispatch")
    return None


def source_route_incompatibility(request: Request, dispatch: DispatchSpec) -> CapabilityError | None:
    """Return why a heterogeneous source's request no batch envelope can carry, or None when one can.

    Physical batch carries only the classic route and native REST v3, without positional slots. A
    heterogeneous source refused here is a correlated local non-execution before any reservation. The
    homogeneous facades keep the 2.5.2 contract instead: such a template fails each binding at dispatch.
    """
    if isinstance(dispatch, DirectDispatch):
        return None
    if request.route not in {RouteKind.BARE, RouteKind.API_V3} or request.positional is not None:
        return CapabilityError(
            "physical batch carries classic and REST v3 requests without positional slots; use direct dispatch"
        )
    return None


__all__ = ["source_incompatibility", "source_route_incompatibility"]
