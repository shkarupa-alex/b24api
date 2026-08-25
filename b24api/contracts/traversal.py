"""Method-agnostic public list traversal mechanics."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Literal

from b24api.models import IdentityCoercion, IdentitySpec, ParameterPath, ResultSelector

_START = ParameterPath(("start",))
_FILTER = ParameterPath(("filter",))
_ORDER = ParameterPath(("order",))
_ROOT_SELECTOR = ResultSelector.root()


def _positive_page_size(value: object) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError("page_size must be a positive integer")


@dataclass(frozen=True, slots=True)
class OffsetSpec:
    """Offset and optional page-limit parameter locations."""

    parameter_path: ParameterPath = _START
    limit_path: ParameterPath | None = None
    allow_create_controls: bool = True


@dataclass(frozen=True, slots=True)
class KeysetSpec:
    """Strict sequential keyset control paths."""

    filter_path: ParameterPath = _FILTER
    order_path: ParameterPath = _ORDER
    start_suppression_path: ParameterPath | None = _START
    limit_path: ParameterPath | None = None
    direction: Literal["ascending", "descending"] = "ascending"
    allow_create_controls: bool = True

    def __post_init__(self) -> None:
        """Validate keyset direction."""
        if self.direction not in {"ascending", "descending"}:
            raise ValueError("direction must be ascending or descending")


@dataclass(frozen=True, slots=True)
class CursorSpec:
    """Strict dependent cursor progression contract."""

    parameter_path: ParameterPath
    item_path: tuple[str | int, ...]
    coercion: IdentityCoercion
    direction: Literal["ascending", "descending"]
    take: Literal["first", "last"]
    limit_path: ParameterPath | None = None
    allow_create_controls: bool = True

    def __post_init__(self) -> None:
        """Validate and freeze cursor mechanics."""
        object.__setattr__(self, "item_path", tuple(self.item_path))
        ParameterPath(self.item_path)
        if not isinstance(self.coercion, IdentityCoercion):
            raise TypeError("coercion must be an IdentityCoercion")
        if self.direction not in {"ascending", "descending"}:
            raise ValueError("direction must be ascending or descending")
        if self.take not in {"first", "last"}:
            raise ValueError("take must be first or last")


@dataclass(frozen=True, slots=True)
class SequentialTraversal:
    """Conservative sequential offset traversal."""

    selector: ResultSelector = _ROOT_SELECTOR
    identity: IdentitySpec | None = None
    page_size: int = 50
    offset: OffsetSpec = OffsetSpec()

    def __post_init__(self) -> None:
        """Validate page cap."""
        _positive_page_size(self.page_size)


@dataclass(frozen=True, slots=True)
class CountedTraversal:
    """Direct-head plus physically batched exact counted traversal."""

    identity: IdentitySpec
    selector: ResultSelector = _ROOT_SELECTOR
    page_size: int = 50
    offset: OffsetSpec = OffsetSpec()

    def __post_init__(self) -> None:
        """Validate required identity and page cap."""
        if not isinstance(self.identity, IdentitySpec):
            raise TypeError("identity must be an IdentitySpec")
        _positive_page_size(self.page_size)


@dataclass(frozen=True, slots=True)
class KeysetTraversal:
    """Exact sequential no-count traversal."""

    selector: ResultSelector
    identity: IdentitySpec
    page_size: int = 50
    keyset: KeysetSpec = KeysetSpec()

    def __post_init__(self) -> None:
        """Validate page cap."""
        _positive_page_size(self.page_size)


@dataclass(frozen=True, slots=True)
class CursorTraversal:
    """Strict dependent cursor traversal."""

    selector: ResultSelector
    cursor: CursorSpec
    identity: IdentitySpec | None = None
    page_size: int = 50

    def __post_init__(self) -> None:
        """Validate page cap."""
        _positive_page_size(self.page_size)


type TraversalSpec = SequentialTraversal | CountedTraversal | KeysetTraversal | CursorTraversal

__all__ = [
    "CountedTraversal",
    "CursorSpec",
    "CursorTraversal",
    "KeysetSpec",
    "KeysetTraversal",
    "OffsetSpec",
    "SequentialTraversal",
    "TraversalSpec",
]
