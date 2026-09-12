"""Immutable page-adaptation contracts for list traversals."""

from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, Self

from b24api.contracts.json import FrozenJson, _freeze_json

if TYPE_CHECKING:
    from collections.abc import Iterable


class _PageOutputTypeError(TypeError):
    """Adapter output could not be represented by the JSON contract."""

    def __init__(self, row_offset: int | None = None) -> None:
        self.row_offset = row_offset
        super().__init__("adapter output must contain only finite JSON values")


@dataclass(frozen=True, slots=True)
class PageView:
    """One immutable server result and its declared row selection."""

    result: FrozenJson
    items: tuple[FrozenJson, ...]


@dataclass(frozen=True, slots=True, init=False)
class AdaptedPage:
    """Immutable rows returned by an application page adapter."""

    items: tuple[FrozenJson, ...]

    def __init__(self, items: Iterable[object]) -> None:
        """Freeze application-owned output at the trust boundary."""
        frozen: list[FrozenJson] = []
        for index, item in enumerate(items):
            try:
                frozen.append(_freeze_json(item))
            except (TypeError, ValueError) as error:
                raise _PageOutputTypeError(index) from error
        object.__setattr__(self, "items", tuple(frozen))

    @classmethod
    def _from_frozen(cls, items: tuple[FrozenJson, ...], /) -> Self:
        """Build a page from trusted already-frozen rows without copying."""
        page = object.__new__(cls)
        object.__setattr__(page, "items", items)
        return page


class PageAdapter(Protocol):
    """Synchronous, side-effect-free page adaptation strategy."""

    def adapt(self, page: PageView, /) -> AdaptedPage:
        """Return rows preserving cardinality, order, and traversal identities."""
        ...


@dataclass(frozen=True, slots=True)
class IdentityPageAdapter:
    """Zero-copy default page adapter."""

    def adapt(self, page: PageView, /) -> AdaptedPage:
        """Return the selected immutable rows unchanged."""
        return AdaptedPage._from_frozen(page.items)  # noqa: SLF001 - trusted built-in zero-copy path


__all__ = ["AdaptedPage", "IdentityPageAdapter", "PageAdapter", "PageView"]
