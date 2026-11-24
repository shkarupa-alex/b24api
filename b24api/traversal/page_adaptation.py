"""Shared enforcement for synchronous immutable page adapters."""

from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING

from b24api.contracts.page import AdaptedPage, PageAdapter, PageView, _PageOutputTypeError
from b24api.contracts.response import result_snapshot
from b24api.errors import PageAdaptationError, PageAdaptationViolation, PaginationError
from b24api.traversal.values import _cursor_value, _row_identity

if TYPE_CHECKING:
    from b24api.contracts.json import FrozenJson
    from b24api.contracts.policy import IdentityCoercion
    from b24api.contracts.request import IdentityComponent, IdentitySpec
    from b24api.contracts.request_summary import RequestSummary
    from b24api.contracts.response import Response
    from b24api.traversal.values import IdentityValue


@dataclass(frozen=True, slots=True)
class PageTokens:
    """The traversal tokens every row of a page must carry: identity components and an optional cursor.

    A composite identity reports its component index and label on failure. A cursor read by an
    identity component (same path, same coercion) is that component, so it is read once, as identity.
    """

    identity: tuple[IdentitySpec | IdentityComponent, ...] = ()
    composite: bool = False
    cursor: tuple[tuple[str | int, ...], IdentityCoercion] | None = None

    def read(
        self,
        rows: tuple[FrozenJson, ...],
        *,
        row_offset: int,
        request_summary: RequestSummary | None,
    ) -> list[tuple[IdentityValue, ...]]:
        """Return each row's tokens, or raise the typed error of the first row breaking its contract.

        Identities are read for the whole page before cursors, as the traversal itself reads them, so a
        row breaking its identity raises ``IdentityContractError`` and a bad cursor the cursor's
        ``PaginationError``.
        """
        tokens = [
            tuple(
                _row_identity(
                    row,
                    component,
                    row_offset=row_offset + index,
                    request_summary=request_summary,
                    component_index=component_index if self.composite else None,
                )
                for component_index, component in enumerate(self.identity)
            )
            for index, row in enumerate(rows)
        ]
        cursor = self.cursor
        if cursor is None or any((part.item_path, part.coercion) == cursor for part in self.identity):
            return tokens
        return [(*row_tokens, _cursor_value(row, *cursor)) for row_tokens, row in zip(tokens, rows, strict=True)]


class _SourcePageState:
    """Retain source rows until the adapted page transaction settles."""

    def __init__(self) -> None:
        self.selected: tuple[FrozenJson, ...] | None = None

    def remember(self, source: tuple[FrozenJson, ...]) -> None:
        self.selected = source

    def current(self, fallback: tuple[FrozenJson, ...]) -> tuple[FrozenJson, ...]:
        return fallback if self.selected is None else self.selected

    def take(self, fallback: tuple[FrozenJson, ...]) -> tuple[FrozenJson, ...]:
        source, self.selected = self.current(fallback), None
        return source


def adapt_page(  # noqa: PLR0913 - explicit public-contract evidence at the shared seam
    response: Response,
    source: tuple[FrozenJson, ...],
    *,
    adapter: PageAdapter,
    tokens: PageTokens,
    row_offset: int,
    request_summary: RequestSummary | None,
    page_offset: int | None,
) -> tuple[FrozenJson, ...]:
    """Adapt one selected page while preserving its traversal tokens.

    The source rows' tokens are read first, with ``row_offset`` the position of the page's first row,
    so a row breaking its identity or cursor contract raises that typed error before any adapter code
    runs on it. The adapter then has to leave every token unchanged.
    """
    adapter_name = f"{type(adapter).__module__}.{type(adapter).__qualname__}"

    def failure(violation: PageAdaptationViolation, row: int | None = None) -> PageAdaptationError:
        return PageAdaptationError(
            violation=violation,
            adapter=adapter_name,
            row_offset=row,
            page_offset=page_offset,
            request_summary=request_summary,
        )

    source_tokens = tokens.read(source, row_offset=row_offset, request_summary=request_summary)
    try:
        adapted = adapter.adapt(PageView(result_snapshot(response), source))
    except _PageOutputTypeError as error:
        raise failure(PageAdaptationViolation.NON_JSON_VALUE, error.row_offset) from error
    except Exception as error:
        raise failure(PageAdaptationViolation.ADAPTER_RAISED) from error
    if not isinstance(adapted, AdaptedPage):
        raise failure(PageAdaptationViolation.NOT_ADAPTED_PAGE)
    if len(adapted.items) != len(source):
        raise failure(PageAdaptationViolation.CARDINALITY_CHANGED, min(len(source), len(adapted.items)))
    if not tokens.identity and tokens.cursor is None:
        return adapted.items
    for index, (before, item) in enumerate(zip(source_tokens, adapted.items, strict=True)):
        try:
            after = tokens.read((item,), row_offset=row_offset + index, request_summary=request_summary)[0]
        except PaginationError as error:
            raise failure(PageAdaptationViolation.ORDER_OR_IDENTITY_CHANGED, index) from error
        if before != after:
            raise failure(PageAdaptationViolation.ORDER_OR_IDENTITY_CHANGED, index)
    return adapted.items


__all__: list[str] = []
