"""Shared enforcement for synchronous immutable page adapters."""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.contracts.page import AdaptedPage, PageAdapter, PageView, _PageOutputTypeError
from b24api.contracts.policy import IdentityCoercion
from b24api.contracts.response import result_snapshot
from b24api.errors import PageAdaptationError, PageAdaptationViolation, PaginationError
from b24api.traversal.values import IdentityValue, _coerce_identity, _extract_path

if TYPE_CHECKING:
    from collections.abc import Sequence

    from b24api.contracts.json import FrozenJson
    from b24api.contracts.request import RequestSummary
    from b24api.contracts.response import Response

type _IdentityTokenSpec = tuple[tuple[str | int, ...], IdentityCoercion]


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
    identities: Sequence[_IdentityTokenSpec],
    request_summary: RequestSummary | None,
    page_offset: int | None,
) -> tuple[FrozenJson, ...]:
    """Adapt one selected page while preserving its traversal tokens."""
    adapter_name = f"{type(adapter).__module__}.{type(adapter).__qualname__}"

    def failure(violation: PageAdaptationViolation, row_offset: int | None = None) -> PageAdaptationError:
        return PageAdaptationError(
            violation=violation,
            adapter=adapter_name,
            row_offset=row_offset,
            page_offset=page_offset,
            request_summary=request_summary,
        )

    source_tokens = _tokens(source, identities)
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
    if source_tokens:
        for index, (before, item) in enumerate(zip(source_tokens, adapted.items, strict=True)):
            try:
                after = _tokens((item,), identities)[0]
            except PaginationError as error:
                raise failure(PageAdaptationViolation.ORDER_OR_IDENTITY_CHANGED, index) from error
            if before != after:
                raise failure(PageAdaptationViolation.ORDER_OR_IDENTITY_CHANGED, index)
    return adapted.items


def _tokens(
    items: tuple[FrozenJson, ...],
    identities: Sequence[_IdentityTokenSpec],
) -> tuple[tuple[IdentityValue, ...], ...]:
    return tuple(
        tuple(_coerce_identity(_extract_path(item, path), coercion) for path, coercion in identities) for item in items
    )


__all__: list[str] = []
