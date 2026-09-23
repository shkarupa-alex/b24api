"""Canonical sequential-offset plan composition."""

from __future__ import annotations

from b24api.contracts.policy import DuplicatePolicy, IdentityRequirement, TotalSemantics
from b24api.contracts.traversal import OffsetContinuation, OffsetSpec, TotalTermination
from b24api.traversal.plans import OffsetSequentialPlan, OffsetTerminalRule


def offset_terminal_rules(offset: OffsetSpec) -> frozenset[OffsetTerminalRule]:
    """Keep sparse raw closure separate from selected-row emptiness."""
    if offset.sparse_raw_bound is not None:
        return frozenset({OffsetTerminalRule.SPARSE_RAW_BOUND})
    if offset.total_termination is TotalTermination.DISABLED:
        return frozenset({OffsetTerminalRule.EMPTY_PAGE})
    return frozenset({OffsetTerminalRule.EMPTY_PAGE, OffsetTerminalRule.QUALIFIED_TOTAL})


def sequential_offset_plan(
    offset: OffsetSpec,
    *,
    page_size: int,
    duplicate_policy: DuplicatePolicy,
) -> OffsetSequentialPlan:
    """Build the one complete plan used by direct and reference traversal."""
    if offset.continuation is OffsetContinuation.FIXED_STEP and offset.page_stride is None and offset.step != page_size:
        raise ValueError("fixed-step traversal requires page_size equal to step")
    stride = offset.page_stride
    if stride is not None and page_size != stride.max_decoded_rows:
        raise ValueError("page_size must match page_stride max_decoded_rows")
    if stride is not None and stride.requested_wire_limit is not None and offset.limit_path is None:
        raise ValueError("requested wire limit requires a limit_path")
    if (
        stride is not None
        and offset.limit_path is not None
        and stride.requested_wire_limit is None
        and stride.max_decoded_rows != stride.wire_increment
    ):
        raise ValueError("requested wire limit is required when the decoded row cap differs from the wire increment")
    page_index = offset.page_index
    if page_index is not None and page_size != page_index.max_rows:
        raise ValueError("page_size must match page_index max_rows")
    wire_limit = (
        stride.requested_wire_limit
        if stride is not None and stride.requested_wire_limit is not None
        else stride.wire_increment
        if stride is not None
        else page_size
    )
    return OffsetSequentialPlan(
        offset_path=offset.parameter_path,
        limit_path=offset.limit_path,
        requested_page_size=wire_limit if offset.limit_path is not None else None,
        continuation=OffsetContinuation.FIXED_STEP if page_index else offset.continuation,
        fixed_step=page_index.increment if page_index else offset.step,
        initial_control=page_index.initial if page_index else 0,
        sparse_raw_bound=offset.sparse_raw_bound,
        page_stride=stride,
        short_page_width=(
            page_index.max_rows
            if page_index is not None
            else stride.max_decoded_rows
            if stride is not None
            else offset.step
        ),
        terminal=offset_terminal_rules(offset),
        allow_create_controls=offset.allow_create_controls,
        identity_requirement=IdentityRequirement.OPTIONAL,
        duplicate_policy=duplicate_policy,
        total_semantics=(
            TotalSemantics.IGNORE
            if offset.total_termination is TotalTermination.DISABLED
            else TotalSemantics.FILTERED_EXACT
        ),
    )
