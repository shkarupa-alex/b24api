"""Single-response, sequential offset and sequential counted traversal strategies."""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.completion.closure import QUALIFIED_TOTAL_REACHED, SINGLE_RESPONSE_COMPLETE
from b24api.contracts.report import PageDispatch, PageRejectionCode
from b24api.contracts.response import result_snapshot
from b24api.contracts.traversal import OffsetContinuation
from b24api.errors import BudgetExceededError, CapabilityError, PaginationError
from b24api.traversal import offset_rules
from b24api.traversal.counted_rules import CountedContradiction, CountedPageFacts, judge_counted_page
from b24api.traversal.identity import (
    _Page,
    _PageRejectionError,
    _request_with_controls,
)
from b24api.traversal.plans import OffsetSequentialPlan
from b24api.traversal.sparse import sparse_page_terminal
from b24api.traversal.strategy_context import PageStop, PageVerdict

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from b24api.contracts.json import FrozenJson
    from b24api.contracts.request import ParameterPath, Request
    from b24api.contracts.response import Response
    from b24api.traversal.plans import CountedOffsetPlan, SingleResponsePlan
    from b24api.traversal.strategy_context import StrategyContext
    from b24api.traversal.values import IdentityValue

_SHORT_WINDOW_UNPROVEN = "fixed-step traversal cannot prove closure after a short page"


def _opens_short_window(plan: OffsetSequentialPlan, rows: int) -> bool:
    """Report whether a non-terminal fixed-step page is shorter than its window, leaving closure unproven."""
    width = plan.short_page_width or plan.fixed_step
    return (
        plan.page_stride is None
        and plan.continuation is OffsetContinuation.FIXED_STEP
        and width is not None
        and rows < width
    )


def _judge_single(plan: SingleResponsePlan, response: Response, qualified_count: int) -> None:
    if plan.reject_continuation and response.next is not None:
        raise CapabilityError("single-response plan observed a continuation")
    if plan.reject_positive_total_over_result and response.total is not None and response.total > qualified_count:
        raise CapabilityError("single-response plan observed a larger qualified total")


class SingleResponseStrategy:
    """One whole response: the page flow and its terminal reason are fixed before the page is yielded."""

    def __init__(self, plan: SingleResponsePlan) -> None:
        """Hold the single-response plan."""
        self._plan = plan

    async def pages(self, ctx: StrategyContext) -> AsyncGenerator[_Page]:
        """Fetch, judge and yield the one page, even when it holds no rows."""
        plan = self._plan
        response = await ctx.fetch(ctx.request)
        trace_count = ctx.page_trace_count
        items: tuple[FrozenJson, ...] = ()
        whole = ctx.single_result_as_item and ctx.selector.path == ()
        try:
            frozen_result = result_snapshot(response)
            qualified_count = len(frozen_result) if whole and isinstance(frozen_result, tuple) else None
            items = (frozen_result,) if whole else ctx.select_page(response, single=True)
            if qualified_count is None:
                qualified_count = len(items)
            _judge_single(plan, response, qualified_count)
            ctx.validate_page(items, response=response, qualified_count=qualified_count, terminal=True)
        except BaseException as error:
            if ctx.page_trace_count == trace_count:
                ctx.reject_external_page(items, response, error)
            raise
        ctx.terminal_reason = SINGLE_RESPONSE_COMPLETE
        item_weights = (qualified_count,) if ctx.single_result_as_item else (1,) * len(items)
        yield _Page(tuple(items), response, item_weights, continuing=False)


def _offset_request(ctx: StrategyContext, plan: OffsetSequentialPlan | CountedOffsetPlan, offset: int) -> Request:
    ctx.schedule_page(offset=offset, dispatch=PageDispatch.DIRECT)
    updates: dict[ParameterPath, object] = {plan.offset_path: offset}
    if plan.limit_path is not None and plan.requested_page_size is not None:
        updates[plan.limit_path] = plan.requested_page_size
    replace = frozenset({plan.offset_path}) if isinstance(plan, OffsetSequentialPlan) else frozenset()
    return _request_with_controls(ctx.request, updates, allow_create=plan.allow_create_controls, replace=replace)


class OffsetStrategy:
    """Sequential offset pages closed by the plan's terminal rules or a sparse raw bound."""

    def __init__(self, plan: OffsetSequentialPlan) -> None:
        """Hold the plan and the traversal's offset progress."""
        self._plan = plan
        self._visited: set[int] = set()
        self._offset = 0
        self._next_offset: int | None = None
        self._terminal: str | None = None
        self._expected_raw_total: int | None = None
        self._pending_short_window = False

    def first_request(self, ctx: StrategyContext) -> Request:
        """Start at the request's own offset; a sparse raw traversal must cover the range from zero."""
        plan = self._plan
        offset = offset_rules.initial_offset(ctx.request, plan.offset_path, default=plan.initial_control)
        if plan.sparse_raw_bound is not None and offset != 0:
            raise CapabilityError("sparse raw traversal requires the complete range from offset zero")
        ctx.cursor_state = offset
        return self._request(ctx, offset)

    def _request(self, ctx: StrategyContext, offset: int) -> Request:
        sparse = self._plan.sparse_raw_bound
        if sparse is not None and len(self._visited) >= sparse.max_pages:
            raise BudgetExceededError("sparse raw page budget exhausted")
        if offset in self._visited:
            raise PaginationError("offset cycle detected")
        self._visited.add(offset)
        self._offset = offset
        return _offset_request(ctx, self._plan, offset)

    def judge(self, ctx: StrategyContext, response: Response, items: tuple[FrozenJson, ...]) -> PageVerdict:
        """Decide closure and the successor offset from the selected page."""
        plan, offset, sparse = self._plan, self._offset, self._plan.sparse_raw_bound
        if self._pending_short_window and items:
            raise PaginationError(_SHORT_WINDOW_UNPROVEN)
        if sparse is None:
            terminal = offset_rules.offset_terminal(
                plan,
                response,
                page_size=len(items),
                accepted=ctx.validated_rows + len(items),
                confirmation=ctx.confirmation_policy,
            )
        else:
            terminal, self._expected_raw_total = sparse_page_terminal(
                sparse,
                response,
                offset=offset,
                selected=len(items),
                previous_total=self._expected_raw_total,
            )
        stride = plan.page_stride if sparse is None and terminal is None else None
        if stride is not None and len(items) < stride.max_decoded_rows:
            raise PaginationError("fixed-stride traversal observed an unexplained short page")
        self._pending_short_window = self._pending_short_window or (
            sparse is None and terminal is None and _opens_short_window(plan, len(items))
        )
        next_offset = (
            None
            if terminal is not None
            else offset_rules.next_offset(plan, response, current=offset, observed=len(items))
        )
        if next_offset is not None and next_offset <= offset:
            raise PaginationError("offset did not advance")
        self._terminal, self._next_offset = terminal, next_offset
        return PageVerdict(terminal is not None)

    def advance(self, ctx: StrategyContext, identities: list[IdentityValue]) -> Request | PageStop:
        """Stop on a terminal page, refuse an unprovable short window, else request the next offset."""
        del identities
        if self._terminal is not None:
            return PageStop(self._terminal)
        if self._pending_short_window and not self._plan.allow_empty_after_short_window:
            # Only an empty page could close the window and this plan accepts none: refuse without asking.
            raise PaginationError(_SHORT_WINDOW_UNPROVEN)
        if self._next_offset is None:
            raise RuntimeError("non-terminal offset page lacks its validated next offset")
        ctx.cursor_state = self._next_offset
        return self._request(ctx, self._next_offset)


class CountedStrategy:
    """Sequential counted pages judged by the shared counted rules (C12, §3.5)."""

    def __init__(self, plan: CountedOffsetPlan) -> None:
        """Hold the plan and the traversal's offset progress."""
        self._plan = plan
        self._visited: set[int] = set()
        self._offset = 0
        self._next_offset: int | None = None

    def first_request(self, ctx: StrategyContext) -> Request:
        """Start at offset zero."""
        ctx.cursor_state = 0
        return self._request(ctx, 0)

    def _request(self, ctx: StrategyContext, offset: int) -> Request:
        if offset in self._visited:
            raise PaginationError("counted offset cycle detected")
        self._visited.add(offset)
        self._offset = offset
        return _offset_request(ctx, self._plan, offset)

    def judge(self, ctx: StrategyContext, response: Response, items: tuple[FrozenJson, ...]) -> PageVerdict:
        """Judge the page against the exact total and the continuation."""
        plan, offset = self._plan, self._offset
        effective_total = response.total if response.total is not None and response.total >= 0 else ctx.expected_total
        verdict = judge_counted_page(
            CountedPageFacts(offset, len(items), ctx.validated_rows, effective_total, response.next, plan.continuation),
        )
        terminal = verdict.terminal
        if not terminal and not items:
            raise _PageRejectionError(
                "counted traversal ended before its exact total",
                PageRejectionCode.RANGE_CONTRADICTION,
            )
        if verdict.contradiction is CountedContradiction.CONTINUATION_AFTER_TOTAL:
            raise CapabilityError("counted traversal completed while continuation remained")
        next_offset = (
            None if terminal else offset_rules.next_offset(plan, response, current=offset, observed=len(items))
        )
        if next_offset is not None and next_offset <= offset:
            raise PaginationError("counted offset did not advance")
        self._next_offset = next_offset
        return PageVerdict(terminal)

    def advance(self, ctx: StrategyContext, identities: list[IdentityValue]) -> Request | PageStop:
        """Stop once the committed rows reach the exact total, else request the next offset."""
        del identities
        if ctx.expected_total is not None and ctx.validated_rows == ctx.expected_total:
            return PageStop(QUALIFIED_TOTAL_REACHED)
        if self._next_offset is None:
            raise RuntimeError("non-terminal counted page lacks its validated next offset")
        ctx.cursor_state = self._next_offset
        return self._request(ctx, self._next_offset)


__all__ = ["CountedStrategy", "OffsetStrategy", "SingleResponseStrategy"]
