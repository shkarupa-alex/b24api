"""Prove all traversal-owned controls are writable before network dispatch."""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.errors import CapabilityError
from b24api.traversal import offset_rules
from b24api.traversal.cursor_domain import cursor_controls_replace, cursor_probe_updates
from b24api.traversal.identity import _child_path, _request_with_controls
from b24api.traversal.inclusive_window import initial_window_boundary
from b24api.traversal.keyset_step import reject_caller_order
from b24api.traversal.plans import (
    ContinuationPlan,
    CountedOffsetPlan,
    InclusiveWindowPlan,
    ItemCursorPlan,
    KeysetPlan,
    OffsetSequentialPlan,
    OffsetTerminalRule,
)

if TYPE_CHECKING:
    from b24api.contracts.request import ParameterPath
    from b24api.traversal.strategy_context import StrategyContext


def _aligned_initial_offset(driver: StrategyContext, plan: OffsetSequentialPlan) -> int:
    """Return the first wire offset, refusing one a rounding server would move to another window."""
    initial_offset = offset_rules.initial_offset(driver.request, plan.offset_path, default=plan.initial_control)
    stride = plan.page_stride or (plan.sparse_raw_bound.stride if plan.sparse_raw_bound is not None else None)
    if stride is not None and initial_offset % stride.server_granularity:
        # The server would silently serve the floor window, repeating or skipping raw rows.
        raise CapabilityError("initial offset must align with the qualified server page granularity")
    if initial_offset != 0 and OffsetTerminalRule.DECLARED_SHORT_PAGE in plan.terminal:
        # A declared short page closes only the collection read from its first window.
        raise CapabilityError("declared short-page traversal must start at offset zero")
    if initial_offset != plan.initial_control and OffsetTerminalRule.QUALIFIED_TOTAL in plan.terminal:
        # The exact total counts the whole collection; a suffix never reaches it and would end incomplete.
        raise CapabilityError(
            "initial offset must equal the plan's initial control"
            " when a caller-qualified exact total closes the collection"
        )
    return initial_offset


# Structurally valid probe values for a window boundary: two distinct timestamps of the declared grammar.
_WINDOW_PROBES = ("2000-01-02T00:00:00Z", "2000-01-01T00:00:00Z")


def _declared_probes(
    plan: ContinuationPlan | InclusiveWindowPlan,
) -> tuple[dict[ParameterPath, object], dict[ParameterPath, object], frozenset[ParameterPath]]:
    """Return two successor control sets for SDK-owned continuation or window controls and the owned paths."""
    if isinstance(plan, ContinuationPlan):
        first: dict[ParameterPath, object] = dict.fromkeys(plan.controls, 0)
        second: dict[ParameterPath, object] = dict.fromkeys(plan.controls, 1)
        limit, size, owned = plan.limit_path, plan.requested_page_size, frozenset(plan.controls)
    else:
        first = {plan.parameter_path: _WINDOW_PROBES[0]}
        second = {plan.parameter_path: _WINDOW_PROBES[1]}
        limit, size, owned = plan.limit_path, plan.requested_window_size, frozenset({plan.parameter_path})
    if limit is not None and size is not None:
        first[limit] = size
        second[limit] = size
    return first, second, owned


def preflight_controls(driver: StrategyContext) -> None:
    """Validate first and successor wire controls against the caller request."""
    plan = driver.plan
    if not isinstance(plan, ContinuationPlan | InclusiveWindowPlan):
        _preflight_traversal_controls(driver)
        return
    if driver.request.positional is not None:
        raise CapabilityError("continuation and window traversals require named request parameters")
    if isinstance(plan, InclusiveWindowPlan):
        initial_window_boundary(driver.request, plan)
    first, second, owned = _declared_probes(plan)
    _request_with_controls(driver.request, first, allow_create=plan.allow_create_controls, replace=owned)
    _request_with_controls(driver.request, second, allow_create=plan.allow_create_controls, replace=owned)


def _preflight_traversal_controls(driver: StrategyContext) -> None:
    """Probe the offset, keyset and item-cursor controls the traversal writes."""
    plan = driver.plan
    first: dict[ParameterPath, object] = {}
    second: dict[ParameterPath, object] = {}
    allow_create = getattr(plan, "allow_create_controls", True)
    if isinstance(plan, OffsetSequentialPlan):
        initial_offset = _aligned_initial_offset(driver, plan)
        first[plan.offset_path] = initial_offset
        second[plan.offset_path] = initial_offset + 1
    elif isinstance(plan, CountedOffsetPlan):
        first[plan.offset_path] = 0
        second[plan.offset_path] = 1
    elif isinstance(plan, KeysetPlan):
        identity = driver.require_identity("keyset")
        reject_caller_order(driver.request, plan, identity)
        if plan.split_order is None:
            if plan.order_path is None:
                raise RuntimeError("keyset plan lacks ordering controls")
            order_updates: dict[ParameterPath, object] = {
                _child_path(plan.order_path, identity.order_key): "ASC" if plan.direction == "asc" else "DESC",
            }
        else:
            order_updates = {
                plan.split_order.field_path: plan.split_order.field_value or identity.order_key,
                plan.split_order.direction_path: (
                    plan.split_order.ascending if plan.direction == "asc" else plan.split_order.descending
                ),
            }
        operator = ">" if plan.direction == "asc" else "<"
        filter_path = _child_path(plan.filter_path, f"{operator}{identity.filter_key}")
        first.update(order_updates)
        second.update(order_updates)
        first[filter_path] = 0
        second[filter_path] = 1
        if plan.start_suppression_path is not None:
            first[plan.start_suppression_path] = -1
            second[plan.start_suppression_path] = -1
    elif isinstance(plan, ItemCursorPlan):
        first, second = cursor_probe_updates(driver.request, plan)
    if (
        isinstance(plan, OffsetSequentialPlan | CountedOffsetPlan | KeysetPlan | ItemCursorPlan)
        and plan.limit_path is not None
        and plan.requested_page_size is not None
    ):
        first[plan.limit_path] = plan.requested_page_size
        second[plan.limit_path] = plan.requested_page_size
    if first:
        replace = (
            frozenset({plan.offset_path})
            if isinstance(plan, OffsetSequentialPlan)
            else frozenset({plan.cursor_request_path})
            if isinstance(plan, ItemCursorPlan) and cursor_controls_replace(plan, driver.initial_cursor)
            else frozenset()
        )
        _request_with_controls(driver.request, first, allow_create=allow_create, replace=replace)
        _request_with_controls(driver.request, second, allow_create=allow_create, replace=replace)
