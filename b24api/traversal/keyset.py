"""Sequential exact keyset traversal strategy."""

from __future__ import annotations
from typing import TYPE_CHECKING, Any

from b24api.errors import PaginationError
from b24api.traversal.identity import (
    _child_path,
    _keyset_terminal,
    _Page,
    _request_with_controls,
)
from b24api.traversal.values import IdentityValue, _compare_identities, _response_items

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from b24api.contracts.json import JsonValue
    from b24api.contracts.request import ParameterPath
    from b24api.traversal.plans import (
        KeysetPlan,
    )


class _KeysetMixin:
    terminal_reason: str | None
    cursor_state: JsonValue

    async def _keyset(self: Any, plan: KeysetPlan) -> AsyncGenerator[_Page]:  # noqa: C901
        identity = self._require_identity("keyset")
        cursor: IdentityValue | None = None
        while True:
            updates: dict[ParameterPath, object] = {
                _child_path(plan.order_path, identity.order_key): "ASC" if plan.direction == "asc" else "DESC",
            }
            if plan.limit_path is not None and plan.requested_page_size is not None:
                updates[plan.limit_path] = plan.requested_page_size
            if plan.start_suppression_path is not None:
                updates[plan.start_suppression_path] = -1
            if cursor is not None:
                operator = ">" if plan.direction == "asc" else "<"
                updates[_child_path(plan.filter_path, f"{operator}{identity.filter_key}")] = cursor
            response = await self._fetch(
                _request_with_controls(
                    self.request,
                    updates,
                    allow_create=plan.allow_create_controls,
                ),
            )
            items = _response_items(response, self.selector)
            identities = self._validate_page(items, response=response)
            if cursor is not None and identities:
                if plan.direction == "asc" and _compare_identities(identities[0], cursor) <= 0:
                    raise PaginationError("keyset page ignored its lower bound")
                if plan.direction == "desc" and _compare_identities(identities[0], cursor) >= 0:
                    raise PaginationError("keyset page ignored its upper bound")
            terminal = _keyset_terminal(plan, len(items))
            if terminal is not None:
                self._validate_terminal_total()
            if items:
                yield _Page(tuple(items), response, (1,) * len(items))
            if terminal is not None:
                self.terminal_reason = terminal
                return
            if not identities:
                raise PaginationError("keyset cursor could not advance")
            next_cursor = identities[-1]
            if cursor is not None and next_cursor == cursor:
                raise PaginationError("keyset cursor repeated")
            cursor = next_cursor
            self.cursor_state = cursor
