"""Small method-agnostic selectors shared by traversal entry points."""

from __future__ import annotations
from typing import TYPE_CHECKING, Literal

from b24api.contracts.identity_store import IdentityStore
from b24api.contracts.policy import DuplicatePolicy, IdentityRequirement, OrderSemantics
from b24api.contracts.request import _MappingValuesResultSelector, _TolerantMappingValuesResultSelector
from b24api.contracts.response import ResultCollectionShape
from b24api.contracts.traversal import ContinuationTraversal, InclusiveWindowTraversal
from b24api.traversal.plans import ContinuationPlan, InclusiveWindowPlan

if TYPE_CHECKING:
    from b24api.contracts.request import ResultSelector, TraversalIdentity
    from b24api.contracts.traversal import CursorSpec


def _direction(value: str) -> Literal["asc", "desc"]:
    return "asc" if value == "ascending" else "desc"


def _collection_selector(selector: ResultSelector, shape: ResultCollectionShape) -> ResultSelector:
    """Lower a declared collection shape into its private selector, exactly once per operation."""
    if not isinstance(shape, ResultCollectionShape):
        raise TypeError("collection_shape must be a ResultCollectionShape")
    if shape is ResultCollectionShape.SEQUENCE:
        return selector
    already_lowered = isinstance(selector, _MappingValuesResultSelector)
    if already_lowered:
        raise ValueError("selector is already lowered to a mapping collection; pass a plain ResultSelector")
    if shape is ResultCollectionShape.MAPPING_VALUES_OR_EMPTY:
        return _TolerantMappingValuesResultSelector(selector.path)
    return _MappingValuesResultSelector(selector.path)


def _checked_identity_store(store: object, identity: TraversalIdentity | None) -> IdentityStore | None:
    """Admit a caller-owned ledger only where it can witness declared identities."""
    if store is None:
        return None
    if identity is None:
        raise ValueError("identity_store requires an identity declaration")
    if not isinstance(store, IdentityStore):
        raise TypeError("identity_store must implement add_if_absent(key) -> bool")
    return store


def declared_traversal_plan(
    traversal: ContinuationTraversal | InclusiveWindowTraversal,
) -> tuple[ContinuationPlan | InclusiveWindowPlan, ResultSelector, TraversalIdentity, int]:
    """Lower a continuation or window traversal for flat and reference streams alike.

    This is the one lowering point of both traversals: the declared collection shape is applied to
    the selector here exactly once. The returned page cap is the decoded rows one request may hold.
    """
    if not isinstance(traversal, ContinuationTraversal | InclusiveWindowTraversal):
        raise TypeError("traversal must be a ContinuationTraversal or InclusiveWindowTraversal")
    selector = _collection_selector(traversal.selector, traversal.collection_shape)
    if isinstance(traversal, ContinuationTraversal):
        continuation = ContinuationPlan(
            controls=traversal.controls,
            rule=traversal.rule,
            limit_path=traversal.limit_path,
            requested_page_size=traversal.page_size if traversal.limit_path is not None else None,
            allow_create_controls=traversal.allow_create_controls,
            identity_requirement=IdentityRequirement.REQUIRED,
            duplicate_policy=DuplicatePolicy.ERROR,
        )
        return continuation, selector, traversal.identity, traversal.page_size
    window = InclusiveWindowPlan(
        timestamp_path=traversal.timestamp_path,
        parameter_path=traversal.parameter_path,
        terminal=traversal.terminal,
        classifier=traversal.classifier,
        max_decoded_rows=traversal.max_decoded_rows,
        limit_path=traversal.limit_path,
        requested_window_size=traversal.requested_window_size,
        allow_create_controls=traversal.allow_create_controls,
        identity_requirement=IdentityRequirement.REQUIRED,
        duplicate_policy=DuplicatePolicy.ERROR,
    )
    return window, selector, traversal.identity, traversal.max_decoded_rows


def _cursor_page_contract(cursor: CursorSpec) -> tuple[Literal["asc", "desc"] | None, OrderSemantics]:
    """Return a cursor's declared in-page order and the global row order its plan may verify.

    Pages ordered against the boundary direction have no single global row order; the cursor
    strategy then checks each page and every value against the boundary itself.
    """
    direction = _direction(cursor.direction)
    page_order = None if cursor.page_order is None else _direction(cursor.page_order)
    if page_order is not None and page_order != direction:
        return page_order, OrderSemantics.UNORDERED
    return page_order, OrderSemantics.ASCENDING if direction == "asc" else OrderSemantics.DESCENDING
