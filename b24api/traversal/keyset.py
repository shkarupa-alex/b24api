"""Sequential exact keyset traversal strategy."""

from __future__ import annotations
from typing import TYPE_CHECKING, Any

from b24api.traversal.identity import _Page
from b24api.traversal.keyset_step import (
    keyset_page_request,
    keyset_page_terminal,
    next_keyset_cursor,
    validate_keyset_continuation,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from b24api.contracts.json import FrozenJson, JsonValue
    from b24api.traversal.plans import (
        KeysetPlan,
    )
    from b24api.traversal.values import IdentityValue


class _KeysetMixin:
    terminal_reason: str | None
    cursor_state: JsonValue

    async def _keyset(self: Any, plan: KeysetPlan) -> AsyncGenerator[_Page]:
        identity = self._require_identity("keyset")
        cursor: IdentityValue | None = None
        while True:
            response = await self._fetch(
                keyset_page_request(
                    self.request,
                    plan=plan,
                    identity=identity,
                    cursor=cursor,
                ),
            )
            trace_count = self.page_trace_count
            items: tuple[FrozenJson, ...] = ()
            try:
                items = self.select_page(response)
                candidate_identities = self._extract_identities(items)
                validate_keyset_continuation(plan, cursor, candidate_identities)
                terminal = keyset_page_terminal(plan, len(items))
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
            cursor = next_keyset_cursor(cursor, identities)
            self.cursor_state = cursor
