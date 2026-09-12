"""Strict dependent item-cursor traversal strategy."""

# ruff: noqa: TRY301 - rejected-page evidence is recorded at this transaction boundary

from __future__ import annotations
from typing import TYPE_CHECKING, Any

from b24api.errors import PaginationError
from b24api.traversal.identity import (
    _cursor_terminal,
    _Page,
    _request_with_controls,
)
from b24api.traversal.values import (
    IdentityValue,
    _compare_identities,
    _cursor_values,
    _take_cursor,
    _validate_order,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from b24api.contracts.json import FrozenJson, JsonValue
    from b24api.contracts.request import ParameterPath
    from b24api.traversal.plans import (
        ItemCursorPlan,
    )


class _CursorMixin:
    terminal_reason: str | None
    cursor_state: JsonValue
    initial_cursor: IdentityValue | None

    async def _cursor(self: Any, plan: ItemCursorPlan) -> AsyncGenerator[_Page]:  # noqa: C901, PLR0912
        self._require_identity("item cursor")
        cursor = self.initial_cursor
        first_page = True
        while True:
            updates: dict[ParameterPath, object] = {}
            if not first_page and cursor is not None:
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
                    replace=(frozenset({plan.cursor_request_path}) if self.initial_cursor is not None else frozenset()),
                )
            )
            response = await self._fetch(request)
            first_page = False
            trace_count = self.page_trace_count
            items: tuple[FrozenJson, ...] = ()
            try:
                items = self.select_page(response)
                source = items if self._selected_source_items is None else self._selected_source_items
                cursor_values = _cursor_values(source, plan)
                _validate_order(cursor_values, plan.direction)
                if cursor is not None and cursor_values:
                    comparison = _compare_identities(cursor_values[0], cursor)
                    if plan.direction == "asc" and comparison <= 0:
                        raise PaginationError("item cursor page ignored its lower bound")
                    if plan.direction == "desc" and comparison >= 0:
                        raise PaginationError("item cursor page ignored its upper bound")
                terminal = _cursor_terminal(plan, len(items))
                self._validate_page(items, response=response, terminal=terminal is not None)
            except BaseException as error:
                if self.page_trace_count == trace_count:
                    self.reject_external_page(items, response, error)
                raise
            if items:
                yield _Page(tuple(items), response, (1,) * len(items), terminal is None)
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
