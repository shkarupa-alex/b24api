"""Canonical sequential-offset plan composition and offset continuation rules."""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.completion.closure import QUALIFIED_TOTAL_REACHED
from b24api.contracts.policy import ConfirmationPolicy, DuplicatePolicy, IdentityRequirement, TotalSemantics
from b24api.contracts.traversal import OffsetContinuation, OffsetSpec, TotalTermination
from b24api.errors import CapabilityError, PaginationError
from b24api.traversal.plans import OffsetSequentialPlan, OffsetTerminalRule

if TYPE_CHECKING:
    from b24api.contracts.request import ParameterPath, Request
    from b24api.contracts.response import Response
    from b24api.traversal.plans import CountedOffsetPlan


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
        allow_empty_after_short_window=page_index is not None,
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


def initial_offset(request: Request, path: ParameterPath, *, default: int = 0) -> int:
    """Return a caller-supplied lexical control or its qualified default."""
    positional = request.positional is not None
    current: object = (
        request.positional.to_wire_slots() if request.positional is not None else request.copy_parameters()
    )
    for part in path.path:
        if isinstance(part, str):
            if not isinstance(current, dict):
                return default
            matches = (
                [part]
                if positional and part in current
                else ([] if positional else [key for key in current if key.casefold() == part.casefold()])
            )
            if len(matches) > 1:
                raise CapabilityError("request contains an ambiguous initial offset path")
            if not matches:
                return default
            current = current[matches[0]]
        else:
            if not isinstance(current, list) or part >= len(current):
                return default
            current = current[part]
    if not isinstance(current, int) or isinstance(current, bool) or current < default:
        raise CapabilityError("initial traversal control is outside its admitted range")
    return current


def offset_terminal(
    plan: OffsetSequentialPlan,
    response: Response,
    *,
    page_size: int,
    accepted: int,
    confirmation: ConfirmationPolicy,
) -> str | None:
    """Return the terminal reason a committed offset page proves, if any."""
    if page_size == 0 and OffsetTerminalRule.EMPTY_PAGE in plan.terminal:
        return "empty page confirmed terminal"
    if confirmation is ConfirmationPolicy.EMPTY_AFTER_BOUNDARY:
        return None
    if (
        OffsetTerminalRule.QUALIFIED_TOTAL in plan.terminal
        and response.total is not None
        and response.total >= 0
        and accepted == response.total
        and (response.next is None or plan.continuation is OffsetContinuation.FIXED_STEP)
    ):
        return QUALIFIED_TOTAL_REACHED
    return None


def next_offset(
    plan: OffsetSequentialPlan | CountedOffsetPlan,
    response: Response,
    *,
    current: int,
    observed: int,
) -> int:
    """Return the next offset control under the plan's continuation rule."""
    if plan.continuation is OffsetContinuation.FIXED_STEP:
        step = plan.fixed_step if isinstance(plan, OffsetSequentialPlan) else plan.fixed_stride
        if step is None:
            raise RuntimeError("fixed-step plan lacks its validated step")
        return current + step
    if plan.continuation is OffsetContinuation.SERVER_NEXT:
        if response.next is None:
            raise PaginationError("server-next traversal has no continuation")
        return response.next
    if plan.continuation is OffsetContinuation.SERVER_NEXT_OR_OBSERVED_COUNT and response.next is not None:
        return response.next
    return current + observed
