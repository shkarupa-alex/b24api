"""Behavior-neutral construction and progression for one sequential keyset page."""

from __future__ import annotations
from typing import TYPE_CHECKING, Literal

from b24api.contracts.bounded_range import filter_fingerprint
from b24api.contracts.policy import (
    DuplicatePolicy,
    IdentityCoercion,
    IdentityRequirement,
    OrderSemantics,
    TotalSemantics,
)
from b24api.errors import CapabilityError, PaginationError
from b24api.traversal.identity import _child_path, _request_with_controls
from b24api.traversal.plans import KeysetPlan, KeysetTerminalRule
from b24api.traversal.values import IdentityValue, _compare_identities

if TYPE_CHECKING:
    from collections.abc import Sequence

    from b24api.contracts.request import IdentitySpec, ParameterPath, Request
    from b24api.contracts.traversal import KeysetSpec


def sequential_keyset_plan(keyset: KeysetSpec, page_size: int) -> KeysetPlan:
    """Describe the unchanged sequential wire and completion contract for a keyset."""
    direction: Literal["asc", "desc"] = "asc" if keyset.direction == "ascending" else "desc"
    return KeysetPlan(
        direction=direction,
        filter_path=keyset.filter_path,
        order_path=keyset.order_path,
        split_order=keyset.split_order,
        start_suppression_path=keyset.start_suppression_path,
        limit_path=keyset.limit_path,
        requested_page_size=page_size if keyset.limit_path is not None else None,
        terminal=KeysetTerminalRule.EMPTY_CONFIRMATION,
        boundary=keyset.boundary,
        allow_create_controls=keyset.allow_create_controls,
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING if direction == "asc" else OrderSemantics.DESCENDING,
        duplicate_policy=DuplicatePolicy.ERROR,
        total_semantics=TotalSemantics.IGNORE,
    )


def validate_bounded_keyset(request: Request, plan: KeysetPlan, identity: IdentitySpec) -> None:
    """Prove a declared exact boundary belongs to this filter before dispatch."""
    boundary = plan.boundary
    if boundary is None:
        return
    if identity.coercion is not IdentityCoercion.EXACT_INTEGER:
        raise CapabilityError("bounded keyset requires exact integer identities")
    expected_fence = _child_path(plan.filter_path, f"<={identity.filter_key}")
    if boundary.fence_path != expected_fence:
        raise CapabilityError("bounded keyset fence path does not match the identity filter")
    if filter_fingerprint(request, plan.filter_path) != boundary.filter_fingerprint:
        raise CapabilityError("bounded keyset base filter differs from its captured fingerprint")


def validate_bounded_keyset_page(plan: KeysetPlan, identities: Sequence[IdentityValue]) -> None:
    """Require the server to enforce the exact upper fence on every returned row."""
    if plan.boundary is not None and any(
        type(value) is not int or value > plan.boundary.upper_id for value in identities
    ):
        raise PaginationError("bounded keyset page crossed its enforced upper fence")


def keyset_page_request(
    request: Request,
    *,
    plan: KeysetPlan,
    identity: IdentitySpec,
    cursor: IdentityValue | None,
) -> Request:
    """Build one page request without mutating the caller's request or traversal state."""
    updates: dict[ParameterPath, object]
    if plan.split_order is None:
        if plan.order_path is None:
            raise RuntimeError("keyset plan lacks ordering controls")
        updates = {
            _child_path(plan.order_path, identity.order_key): "ASC" if plan.direction == "asc" else "DESC",
        }
    else:
        updates = {
            plan.split_order.field_path: plan.split_order.field_value or identity.order_key,
            plan.split_order.direction_path: (
                plan.split_order.ascending if plan.direction == "asc" else plan.split_order.descending
            ),
        }
    if plan.limit_path is not None and plan.requested_page_size is not None:
        updates[plan.limit_path] = plan.requested_page_size
    if plan.start_suppression_path is not None:
        updates[plan.start_suppression_path] = -1
    if cursor is not None:
        operator = ">" if plan.direction == "asc" else "<"
        updates[_child_path(plan.filter_path, f"{operator}{identity.filter_key}")] = cursor
    if plan.boundary is not None:
        updates[plan.boundary.fence_path] = plan.boundary.upper_id
    return _request_with_controls(request, updates, allow_create=plan.allow_create_controls)


def bounded_keyset_request(  # noqa: PLR0913
    request: Request,
    *,
    keyset: KeysetSpec,
    identity: IdentitySpec,
    direction: str,
    lower: int | None = None,
    upper: int | None = None,
    limit: int | None = None,
    advisory_start: bool = False,
) -> Request:
    """Build bounded fast-path controls without reading or mutating scheduler state."""
    updates: dict[ParameterPath, object] = {}
    if keyset.split_order is None:
        if keyset.order_path is None:
            raise RuntimeError("keyset ordering contract is missing")
        updates[_child_path(keyset.order_path, identity.order_key)] = direction
    else:
        updates[keyset.split_order.field_path] = keyset.split_order.field_value or identity.order_key
        updates[keyset.split_order.direction_path] = (
            keyset.split_order.ascending if direction == "ASC" else keyset.split_order.descending
        )
    if lower is not None:
        updates[_child_path(keyset.filter_path, f">{identity.filter_key}")] = lower
    if upper is not None:
        updates[_child_path(keyset.filter_path, f"<{identity.filter_key}")] = upper
    if keyset.start_suppression_path is not None:
        updates[keyset.start_suppression_path] = 0 if advisory_start else -1
    if keyset.limit_path is not None and limit is not None:
        updates[keyset.limit_path] = limit
    return _request_with_controls(request, updates, allow_create=keyset.allow_create_controls)


def validate_keyset_continuation(
    plan: KeysetPlan,
    cursor: IdentityValue | None,
    identities: Sequence[IdentityValue],
) -> None:
    """Reject a non-empty page whose first identity does not honor its cursor bound."""
    if cursor is None or not identities:
        return
    if plan.direction == "asc" and _compare_identities(identities[0], cursor) <= 0:
        raise PaginationError("keyset page ignored its lower bound")
    if plan.direction == "desc" and _compare_identities(identities[0], cursor) >= 0:
        raise PaginationError("keyset page ignored its upper bound")


def keyset_page_terminal(
    plan: KeysetPlan,
    page_size: int,
    identities: Sequence[IdentityValue] = (),
) -> str | None:
    """Return the declared completion reason for one validated keyset page."""
    if plan.boundary is not None:
        if plan.boundary.upper_id in identities:
            return "exact admitted upper boundary reached"
        if page_size == 0:
            raise PaginationError("bounded keyset ended before its exact upper boundary")
        return None
    if plan.terminal is KeysetTerminalRule.EMPTY_CONFIRMATION and page_size == 0:
        return "empty keyset confirmation"
    return None


def next_keyset_cursor[IdentityT: IdentityValue](
    cursor: IdentityT | None,
    identities: Sequence[IdentityT],
) -> IdentityT:
    """Advance after admission while rejecting empty or repeated cursor evidence."""
    if not identities:
        raise PaginationError("keyset cursor could not advance")
    next_cursor = identities[-1]
    if cursor is not None and next_cursor == cursor:
        raise PaginationError("keyset cursor repeated")
    return next_cursor


__all__: list[str] = []
