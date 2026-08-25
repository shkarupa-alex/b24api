"""Strict dependent item-cursor traversal strategy."""

from __future__ import annotations
from typing import TYPE_CHECKING, Any

from b24api.error import PaginationError
from b24api.traversal.identity import (
    IdentityValue,
    _compare_identities,
    _cursor_terminal,
    _cursor_values,
    _Page,
    _request_with_controls,
    _response_items,
    _take_cursor,
    _validate_order,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from b24api.models import (
        JsonValue,
        ParameterPath,
    )
    from b24api.plans import (
        ItemCursorPlan,
    )


class _CursorMixin:
    terminal_reason: str | None
    cursor_state: JsonValue

    async def _cursor(self: Any, plan: ItemCursorPlan) -> AsyncGenerator[_Page]:  # noqa: C901, PLR0912
        self._require_identity("item cursor")
        cursor: IdentityValue | None = None
        while True:
            updates: dict[ParameterPath, object] = {}
            if cursor is not None:
                updates[plan.cursor_request_path] = cursor
            if plan.limit_path is not None and plan.requested_page_size is not None:
                updates[plan.limit_path] = plan.requested_page_size
            request = (
                self.request
                if not updates
                else _request_with_controls(
                    self.request,
                    updates,
                    allow_create=plan.allow_create_controls,
                )
            )
            response = await self._fetch(request)
            items = _response_items(response, self.selector)
            self._validate_page(items, response=response)
            cursor_values = _cursor_values(items, plan)
            _validate_order(cursor_values, plan.direction)
            if cursor is not None and cursor_values:
                comparison = _compare_identities(cursor_values[0], cursor)
                if plan.direction == "asc" and comparison <= 0:
                    raise PaginationError("item cursor page ignored its lower bound")
                if plan.direction == "desc" and comparison >= 0:
                    raise PaginationError("item cursor page ignored its upper bound")
            terminal = _cursor_terminal(plan, len(items))
            if terminal is not None:
                self._validate_terminal_total()
            if items:
                yield _Page(tuple(items), response, (1,) * len(items))
            if terminal is not None:
                self.terminal_reason = terminal
                return
            if not cursor_values:
                raise PaginationError("item cursor is absent before terminal confirmation")
            next_cursor = _take_cursor(cursor_values, plan.cursor_take)
            if cursor is not None:
                if next_cursor == cursor:
                    raise PaginationError("item cursor repeated")
                if plan.direction == "asc" and _compare_identities(next_cursor, cursor) < 0:
                    raise PaginationError("item cursor moved in the wrong direction")
                if plan.direction == "desc" and _compare_identities(next_cursor, cursor) > 0:
                    raise PaginationError("item cursor moved in the wrong direction")
            cursor = next_cursor
            self.cursor_state = cursor
