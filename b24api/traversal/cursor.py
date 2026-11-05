"""Strict dependent item-cursor traversal strategy."""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.completion.closure import RESPONSE_FLAG_DECLARED_TERMINAL
from b24api.contracts.continuation import ContinuationReason
from b24api.errors import ContinuationError, PaginationError
from b24api.traversal.continuation import response_more
from b24api.traversal.cursor_domain import validate_cursor_progression, validate_cursor_value, validate_initial_cursor
from b24api.traversal.identity import _cursor_terminal, _request_with_controls
from b24api.traversal.strategy_context import PageStop, PageVerdict
from b24api.traversal.values import (
    _compare_identities,
    _cursor_values,
    _take_cursor,
    _validate_order,
)

if TYPE_CHECKING:
    from b24api.contracts.json import FrozenJson
    from b24api.contracts.request import ParameterPath, Request
    from b24api.contracts.response import Response
    from b24api.traversal.plans import (
        ItemCursorPlan,
    )
    from b24api.traversal.strategy_context import StrategyContext
    from b24api.traversal.values import IdentityValue


class ItemCursorStrategy:
    """Strict dependent item-cursor pages: each page's items carry the next page's cursor."""

    def __init__(self, plan: ItemCursorPlan) -> None:
        """Hold the plan; the cursor is resolved from the binding or the request on the first request."""
        self._plan = plan
        self._cursor: IdentityValue | None = None
        self._request_cursor: IdentityValue | None = None
        self._cursor_values: list[IdentityValue] = []
        self._terminal: str | None = None

    def first_request(self, ctx: StrategyContext) -> Request:
        """Validate the starting cursor; the first page is requested as the caller wrote it."""
        plan = self._plan
        ctx.require_identity("item cursor")
        self._request_cursor = validate_initial_cursor(ctx.request, plan)
        self._cursor = ctx.initial_cursor if ctx.initial_cursor is not None else self._request_cursor
        if self._cursor is not None:
            validate_cursor_value(self._cursor, plan.domain)
        return self._request(ctx, with_cursor=False)

    def _request(self, ctx: StrategyContext, *, with_cursor: bool) -> Request:
        plan = self._plan
        updates: dict[ParameterPath, object] = {}
        if with_cursor and self._cursor is not None:
            updates[plan.cursor_request_path] = self._cursor
        if plan.limit_path is not None and plan.requested_page_size is not None:
            updates[plan.limit_path] = plan.requested_page_size
        if not updates:
            return ctx.request
        replaces_cursor = (
            ctx.initial_cursor is not None or self._request_cursor is not None or not plan.allow_create_controls
        )
        return _request_with_controls(
            ctx.request,
            updates,
            allow_create=plan.allow_create_controls,
            replace=frozenset({plan.cursor_request_path}) if replaces_cursor else frozenset(),
        )

    def judge(self, ctx: StrategyContext, response: Response, items: tuple[FrozenJson, ...]) -> PageVerdict:
        """Check the page's cursors are valid, ordered and strictly past the current cursor."""
        plan, cursor = self._plan, self._cursor
        cursor_values = self._cursor_values = _cursor_values(ctx.source_page.current(items), plan)
        for value in cursor_values:
            validate_cursor_progression(value, plan.domain)
        _validate_order(cursor_values, plan.page_order or plan.direction)
        if cursor is not None and cursor_values:
            # A declared page order checks every row: the boundary row need not lead the page.
            for value in cursor_values if plan.page_order is not None else cursor_values[:1]:
                comparison = _compare_identities(value, cursor)
                if plan.direction == "asc" and comparison <= 0:
                    raise PaginationError("item cursor page ignored its lower bound")
                if plan.direction == "desc" and comparison >= 0:
                    raise PaginationError("item cursor page ignored its upper bound")
        self._terminal = self._page_terminal(response, len(items))
        return PageVerdict(self._terminal is not None)

    def _page_terminal(self, response: Response, row_count: int) -> str | None:
        """Close on the declared response flag when one exists, else on the empty confirmation page."""
        flag = self._plan.response_terminal
        if flag is None:
            return _cursor_terminal(self._plan, row_count)
        if not response_more(response, flag):
            return RESPONSE_FLAG_DECLARED_TERMINAL
        if not row_count:
            raise ContinuationError(
                ContinuationReason.MISSING_CONTINUATION,
                "response flag declares more pages but the page carries no cursor",
            )
        return None

    def advance(self, ctx: StrategyContext, identities: list[IdentityValue]) -> Request | PageStop:
        """Stop on the terminal page, else take the next cursor, which must move strictly forward."""
        del identities
        plan, cursor = self._plan, self._cursor
        if self._terminal is not None:
            return PageStop(self._terminal)
        if not self._cursor_values:
            raise PaginationError("item cursor is absent before terminal confirmation")
        next_cursor = _take_cursor(self._cursor_values, plan.cursor_take)
        if cursor is not None:
            if next_cursor == cursor:
                raise PaginationError("item cursor repeated")
            if plan.direction == "asc" and _compare_identities(next_cursor, cursor) < 0:
                raise PaginationError("item cursor moved in the wrong direction")
            if plan.direction == "desc" and _compare_identities(next_cursor, cursor) > 0:
                raise PaginationError("item cursor moved in the wrong direction")
        self._cursor = next_cursor
        ctx.cursor_state = next_cursor
        return self._request(ctx, with_cursor=True)


__all__ = ["ItemCursorStrategy"]
