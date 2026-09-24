"""Sequential exact keyset traversal strategy."""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.traversal.keyset_step import (
    keyset_page_request,
    keyset_page_terminal,
    next_keyset_cursor,
    validate_bounded_keyset,
    validate_bounded_keyset_page,
    validate_keyset_continuation,
)
from b24api.traversal.strategy_context import PageStop, PageVerdict

if TYPE_CHECKING:
    from b24api.contracts.json import FrozenJson
    from b24api.contracts.request import IdentitySpec, Request
    from b24api.contracts.response import Response
    from b24api.traversal.plans import (
        KeysetPlan,
    )
    from b24api.traversal.strategy_context import StrategyContext
    from b24api.traversal.values import IdentityValue


class KeysetStrategy:
    """Sequential exact keyset pages: each committed page moves the cursor past its last identity."""

    def __init__(self, plan: KeysetPlan) -> None:
        """Hold the plan; the cursor starts at the boundary's exclusive lower bound."""
        self._plan = plan
        self._cursor: IdentityValue | None = plan.boundary.lower_exclusive if plan.boundary else None
        self._identity: IdentitySpec | None = None
        self._terminal: str | None = None

    def _request(self, ctx: StrategyContext, identity: IdentitySpec) -> Request:
        return keyset_page_request(ctx.request, plan=self._plan, identity=identity, cursor=self._cursor)

    def first_request(self, ctx: StrategyContext) -> Request:
        """Require a scalar identity and a bounded keyset request."""
        identity = self._identity = ctx.require_identity("keyset")
        validate_bounded_keyset(ctx.request, self._plan, identity)
        return self._request(ctx, identity)

    def judge(self, ctx: StrategyContext, response: Response, items: tuple[FrozenJson, ...]) -> PageVerdict:
        """Check the page continues the cursor within its bounds, reading identities from the source rows."""
        del response
        plan = self._plan
        candidate_identities = ctx.extract_identities(ctx.source_page.current(items))
        validate_keyset_continuation(plan, self._cursor, candidate_identities)
        validate_bounded_keyset_page(plan, candidate_identities)
        self._terminal = keyset_page_terminal(plan, len(items), candidate_identities)
        return PageVerdict(self._terminal is not None, candidate_identities)

    def advance(self, ctx: StrategyContext, identities: list[IdentityValue]) -> Request | PageStop:
        """Stop on the terminal page, else continue after the committed identities."""
        if self._terminal is not None:
            return PageStop(self._terminal)
        if self._identity is None:
            raise RuntimeError("keyset traversal advanced before its first request")
        self._cursor = next_keyset_cursor(self._cursor, identities)
        ctx.cursor_state = self._cursor
        return self._request(ctx, self._identity)


__all__ = ["KeysetStrategy"]
