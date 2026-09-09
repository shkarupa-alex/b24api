"""Sequential exact keyset traversal strategy."""

# ruff: noqa: TRY301 - rejected-page evidence is recorded at this transaction boundary

from __future__ import annotations
from typing import TYPE_CHECKING, Any

from b24api.errors import PaginationError
from b24api.traversal.identity import (
    _child_path,
    _keyset_terminal,
    _Page,
    _request_with_controls,
)
from b24api.traversal.values import IdentityValue, _compare_identities

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

    async def _keyset(self: Any, plan: KeysetPlan) -> AsyncGenerator[_Page]:  # noqa: C901, PLR0912
        identity = self._require_identity("keyset")
        cursor: IdentityValue | None = None
        while True:
            if plan.split_order is None:
                if plan.order_path is None:
                    raise RuntimeError("keyset plan lacks ordering controls")
                updates: dict[ParameterPath, object] = {
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
            response = await self._fetch(
                _request_with_controls(
                    self.request,
                    updates,
                    allow_create=plan.allow_create_controls,
                ),
            )
            trace_count = self.page_trace_count
            items: list[JsonValue] = []
            try:
                items = self.select_page(response)
                candidate_identities = self._extract_identities(items)
                if cursor is not None and candidate_identities:
                    if plan.direction == "asc" and _compare_identities(candidate_identities[0], cursor) <= 0:
                        raise PaginationError("keyset page ignored its lower bound")
                    if plan.direction == "desc" and _compare_identities(candidate_identities[0], cursor) >= 0:
                        raise PaginationError("keyset page ignored its upper bound")
                terminal = _keyset_terminal(plan, len(items))
                identities = self._validate_page(
                    items,
                    response=response,
                    terminal=terminal is not None,
                    identities=candidate_identities,
                )
            except BaseException as error:
                if self.page_trace_count == trace_count:
                    self.reject_external_page(items, response, error)
                raise
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
