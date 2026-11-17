"""Single-response, sequential offset and sequential counted traversal strategies."""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.completion.closure import QUALIFIED_TOTAL_REACHED, SINGLE_RESPONSE_COMPLETE
from b24api.contracts.completion import EMPTY_SOURCE_WITNESS
from b24api.contracts.policy import TotalSemantics
from b24api.contracts.report import PageDispatch, PageRejectionCode
from b24api.contracts.response import result_snapshot
from b24api.contracts.traversal import OffsetContinuation
from b24api.errors import BudgetExceededError, CapabilityError, PaginationError
from b24api.traversal import offset_rules
from b24api.traversal.counted_rules import CountedContradiction, CountedPageFacts, judge_counted_page
from b24api.traversal.identity import (
    _Page,
    _request_with_controls,
)
from b24api.traversal.plans import OffsetSequentialPlan, OffsetTerminalRule
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
        self._from_first_page = True
        self._reported_total: int | None = None

    def first_request(self, ctx: StrategyContext) -> Request:
        """Start at the request's own offset; a sparse raw traversal must cover the range from zero."""
        plan = self._plan
        offset = offset_rules.initial_offset(ctx.request, plan.offset_path, default=plan.initial_control)
        if plan.sparse_raw_bound is not None and offset != 0:
            raise CapabilityError("sparse raw traversal requires the complete range from offset zero")
        self._from_first_page = offset == plan.initial_control
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
        if OffsetTerminalRule.DECLARED_SHORT_PAGE in plan.terminal:
            # The declared window explains a short page before the stride's unexplained-short refusal.
            terminal = offset_rules.declared_short_page_terminal(plan, response, offset=offset, rows=len(items))
            self._terminal, self._next_offset = (
                terminal,
                (
                    None
                    if terminal is not None
                    else offset_rules.next_offset(plan, response, current=offset, observed=len(items))
                ),
            )
            return PageVerdict(terminal is not None)
        empty_source = False
        if sparse is None:
            accepted = ctx.validated_rows + len(items)
            terminal = offset_rules.offset_terminal(
                plan,
                response,
                page_size=len(items),
                accepted=accepted,
                confirmation=ctx.confirmation_policy,
            )
            empty_source = terminal is not None and ctx.empty_source_head_eligible(
                response, ctx.source_page.current(items), items
            )
            if empty_source:
                terminal = EMPTY_SOURCE_WITNESS.terminal_reason
            elif terminal is not None:
                self._check_reported_total(ctx, response, accepted)
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
        if response.total is not None and response.total >= 0:
            self._reported_total = response.total
        self._terminal, self._next_offset = terminal, next_offset
        return PageVerdict(terminal is not None, empty_source=empty_source)

    def _check_reported_total(self, ctx: StrategyContext, response: Response, accepted: int) -> None:
        """Refuse a server-next end whose row count contradicts the reported total, when nothing else checks it.

        A server-next traversal reads the ordinary Bitrix list envelope, whose ``total`` counts the
        filter's rows: read from its first page, it must end with that many. The page's own total is
        used, or the latest one an earlier page reported when the end carries none. Under ``IGNORE``
        total semantics no total closes or qualifies the traversal, so this end check is its only use;
        an exact total is checked by the driver on every page, and an advisory one only warns. An
        observed-count or fixed-step endpoint has no such envelope, and its total is never read
        unless the caller qualifies it.
        """
        if (
            not self._from_first_page
            or OffsetTerminalRule.NO_CONTINUATION not in self._plan.terminal
            or ctx.total_semantics is not TotalSemantics.IGNORE
        ):
            return
        total = response.total if response.total is not None and response.total >= 0 else self._reported_total
        contradiction = offset_rules.reported_total_contradiction(total=total, delivered=accepted)
        if contradiction is not None:
            raise contradiction

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
        self._terminal = QUALIFIED_TOTAL_REACHED

    def first_request(self, ctx: StrategyContext) -> Request:
        """Start at offset zero."""
        ctx.cursor_state = 0
        return self._request(ctx, 0)

    def seed_head(self, ctx: StrategyContext) -> None:
        """Adopt an already dispatched offset-zero head instead of requesting it again."""
        if self._visited:
            raise RuntimeError("counted sequential strategy was already started")
        ctx.cursor_state = 0
        self._visited.add(0)
        self._offset = 0

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
        if verdict.contradiction is CountedContradiction.CONTINUATION_AFTER_TOTAL:
            raise PaginationError(
                "counted traversal completed while continuation remained",
                rejection_code=PageRejectionCode.RANGE_CONTRADICTION,
            )
        # An empty page, or a server-next page without next, ends the source; the driver then refuses an
        # end short of the exact total, as it does for every traversal closed by one.
        ended = not items or (plan.continuation is OffsetContinuation.SERVER_NEXT and response.next is None)
        terminal = verdict.terminal or ended
        empty_source = terminal and ctx.empty_source_head_eligible(response, ctx.source_page.current(items), items)
        next_offset = (
            None if terminal else offset_rules.next_offset(plan, response, current=offset, observed=len(items))
        )
        if next_offset is not None and next_offset <= offset:
            raise PaginationError("counted offset did not advance")
        self._next_offset = next_offset
        self._terminal = EMPTY_SOURCE_WITNESS.terminal_reason if empty_source else QUALIFIED_TOTAL_REACHED
        return PageVerdict(terminal, empty_source=empty_source)

    def advance(self, ctx: StrategyContext, identities: list[IdentityValue]) -> Request | PageStop:
        """Stop at a terminal page, else request the next offset."""
        del identities
        if self._next_offset is None:
            return PageStop(self._terminal)
        ctx.cursor_state = self._next_offset
        return self._request(ctx, self._next_offset)


__all__ = ["CountedStrategy", "OffsetStrategy", "SingleResponseStrategy"]
