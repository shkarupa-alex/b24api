"""Pure source-compatibility rules of reference operations.

Homogeneous facades call ``source_incompatibility`` before any binding is read and raise its
error; a heterogeneous operation calls it for each admitted source and turns its error into a
correlated local non-execution of that source, before any HTTP request. ``source_route_incompatibility``
is heterogeneous-only: it refuses a request no batch envelope carries, which the homogeneous
facades keep failing per binding at dispatch, as in 2.5.2.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.contracts.dispatch import BatchDispatch, DirectDispatch
from b24api.contracts.keyset_execution import AutoKeysetExecution, SequentialKeysetExecution
from b24api.contracts.request import RouteKind
from b24api.contracts.traversal import CountedTraversal, KeysetTraversal, SequentialTraversal
from b24api.contracts.wire import BodyEncoding
from b24api.errors import CapabilityError
from b24api.traversal.keyset_eligibility import BOUNDARY_WAVE_COMMANDS
from b24api.traversal.offset_rules import offset_route_incompatibility

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
    """Return why one source cannot run under the operation's dispatch or on its route, or None when it can.

    ``v3_batch`` is the operation transport's declared support for native REST v3 batch envelopes.
    """
    if not isinstance(dispatch, DirectDispatch):
        unbatchable = _batch_incompatibility(request, v3_batch=v3_batch)
        if unbatchable is not None:
            return unbatchable
    if isinstance(traversal, SequentialTraversal | CountedTraversal):
        unreadable = offset_route_incompatibility(traversal.offset, request.route)
        if unreadable is not None:
            return unreadable
    if isinstance(traversal, CountedTraversal) and isinstance(dispatch, DirectDispatch):
        return CapabilityError("counted reference traversal requires BatchDispatch")
    if isinstance(traversal, CountedTraversal) and page_stop is not None:
        return CapabilityError("counted physical batch tail does not support page stop")
    if isinstance(traversal, KeysetTraversal) and not isinstance(traversal.execution, SequentialKeysetExecution):
        return _fast_keyset_incompatibility(traversal, dispatch, page_stop)
    return None


def _fast_keyset_incompatibility(
    traversal: KeysetTraversal, dispatch: DispatchSpec, page_stop: PageStopPolicy | None
) -> CapabilityError | None:
    """Return why a fast or automatic keyset source cannot run under the dispatch, or None when it can."""
    # AUTO with a page stop settles on sequential execution before planning; an explicit fast
    # execution cannot honour a stop decided between ordered pages.
    if page_stop is not None:
        if isinstance(traversal.execution, AutoKeysetExecution):
            return None
        return CapabilityError("page stop requires sequential keyset execution")
    if isinstance(dispatch, DirectDispatch):
        return CapabilityError("fast keyset reference traversal requires BatchDispatch")
    # Every wave travels in one shared physical batch, and the first one always holds both boundary probes.
    if isinstance(dispatch, BatchDispatch) and dispatch.batch_size < BOUNDARY_WAVE_COMMANDS:
        return CapabilityError(
            f"fast keyset reference traversal requires BatchDispatch batch_size of at least {BOUNDARY_WAVE_COMMANDS}"
        )
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
