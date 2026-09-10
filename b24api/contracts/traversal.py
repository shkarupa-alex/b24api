"""Method-agnostic public list traversal mechanics."""

from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from b24api.contracts.keyset_execution import (
    AutoKeysetExecution,
    KeysetExecution,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    SequentialKeysetExecution,
)
from b24api.contracts.policy import IdentityCoercion
from b24api.contracts.request import IdentitySpec, ParameterPath, ResultSelector, TraversalIdentity

_START = ParameterPath(("start",))
_FILTER = ParameterPath(("filter",))
_ORDER = ParameterPath(("order",))
_ROOT_SELECTOR = ResultSelector.root()
_SEQUENTIAL_KEYSET_EXECUTION = SequentialKeysetExecution()


class OffsetContinuation(StrEnum):
    """Caller-declared offset progression source."""

    SERVER_NEXT = "server_next"
    SERVER_NEXT_OR_OBSERVED_COUNT = "server_next_or_observed_count"
    OBSERVED_COUNT = "observed_count"
    FIXED_STEP = "fixed_step"


class TotalTermination(StrEnum):
    """Whether a caller-qualified exact total may prove completion."""

    DISABLED = "disabled"
    EXACT_QUALIFIED = "exact_qualified"


def _positive_page_size(value: object) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError("page_size must be a positive integer")


def _require_disjoint_paths(*paths: ParameterPath) -> None:
    normalized = [tuple(part.casefold() if isinstance(part, str) else part for part in path.path) for path in paths]
    for index, left in enumerate(normalized):
        for right in normalized[index + 1 :]:
            shared = min(len(left), len(right))
            if left[:shared] == right[:shared]:
                raise ValueError("traversal control paths must be distinct and non-overlapping")


@dataclass(frozen=True, slots=True)
class OffsetSpec:
    """Offset and optional page-limit parameter locations."""

    parameter_path: ParameterPath = _START
    limit_path: ParameterPath | None = None
    allow_create_controls: bool = True
    continuation: OffsetContinuation = OffsetContinuation.SERVER_NEXT_OR_OBSERVED_COUNT
    step: int | None = None
    total_termination: TotalTermination = TotalTermination.DISABLED

    def __post_init__(self) -> None:
        """Validate offset mechanics and completion semantics."""
        if not isinstance(self.continuation, OffsetContinuation) or not isinstance(
            self.total_termination,
            TotalTermination,
        ):
            raise TypeError("offset controls must use their declared enum types")
        if self.continuation is OffsetContinuation.FIXED_STEP:
            if not isinstance(self.step, int) or isinstance(self.step, bool) or self.step < 1:
                raise ValueError("fixed-step offset requires a positive integer step")
        elif self.step is not None:
            raise ValueError("step is valid only for fixed-step continuation")


_COUNTED_OFFSET = OffsetSpec(total_termination=TotalTermination.EXACT_QUALIFIED)


@dataclass(frozen=True, slots=True)
class SplitOrderSpec:
    """Separate flat controls for keyset sort field and direction."""

    field_path: ParameterPath
    direction_path: ParameterPath
    ascending: str = "ASC"
    descending: str = "DESC"
    field_value: str | None = None

    def __post_init__(self) -> None:
        """Validate split ordering literals."""
        strings = (self.ascending, self.descending)
        if any(not isinstance(value, str) or not value for value in strings):
            raise ValueError("split-order direction values must be non-empty strings")
        if self.field_value is not None and (not isinstance(self.field_value, str) or not self.field_value):
            raise ValueError("split-order field_value must be a non-empty string")
        _require_disjoint_paths(self.field_path, self.direction_path)


@dataclass(frozen=True, slots=True)
class KeysetSpec:
    """Strict sequential keyset control paths."""

    filter_path: ParameterPath = _FILTER
    order_path: ParameterPath | None = _ORDER
    start_suppression_path: ParameterPath | None = _START
    limit_path: ParameterPath | None = None
    direction: Literal["ascending", "descending"] = "ascending"
    allow_create_controls: bool = True
    split_order: SplitOrderSpec | None = None

    def __post_init__(self) -> None:
        """Validate keyset direction."""
        if self.direction not in {"ascending", "descending"}:
            raise ValueError("direction must be ascending or descending")
        if (self.order_path is None) == (self.split_order is None):
            raise ValueError("exactly one of order_path and split_order must be set")
        order_paths = (
            (self.order_path,)
            if self.split_order is None
            else (self.split_order.field_path, self.split_order.direction_path)
        )
        _require_disjoint_paths(
            *(path for path in (self.filter_path, *order_paths, self.start_suppression_path, self.limit_path) if path),
        )


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
    identity: TraversalIdentity | None = None
    page_size: int = 50
    offset: OffsetSpec = OffsetSpec()

    def __post_init__(self) -> None:
        """Validate page cap."""
        _positive_page_size(self.page_size)
        if self.offset.continuation is OffsetContinuation.FIXED_STEP and self.offset.step != self.page_size:
            raise ValueError("fixed-step traversal requires page_size equal to step")


@dataclass(frozen=True, slots=True)
class CountedTraversal:
    """Direct-head plus physically batched exact counted traversal."""

    identity: TraversalIdentity | None = None
    selector: ResultSelector = _ROOT_SELECTOR
    page_size: int = 50
    offset: OffsetSpec = _COUNTED_OFFSET

    def __post_init__(self) -> None:
        """Validate required identity and page cap."""
        _positive_page_size(self.page_size)
        if self.offset.total_termination is not TotalTermination.EXACT_QUALIFIED:
            raise ValueError("counted traversal requires exact-qualified total termination")
        if self.offset.continuation is OffsetContinuation.FIXED_STEP and self.offset.step != self.page_size:
            raise ValueError("fixed-step counted traversal requires page_size equal to step")


@dataclass(frozen=True, slots=True)
class KeysetTraversal:
    """Exact sequential no-count traversal."""

    selector: ResultSelector
    identity: IdentitySpec
    page_size: int = 50
    keyset: KeysetSpec = KeysetSpec()
    execution: KeysetExecution = _SEQUENTIAL_KEYSET_EXECUTION

    def __post_init__(self) -> None:
        """Validate page cap."""
        _positive_page_size(self.page_size)
        if not isinstance(
            self.execution,
            SequentialKeysetExecution | RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution,
        ):
            raise TypeError("execution must be a supported KeysetExecution")


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


def traversal_control_paths(spec: TraversalSpec) -> tuple[ParameterPath, ...]:
    """Return every request path written by a traversal contract."""
    if isinstance(spec, SequentialTraversal | CountedTraversal):
        return tuple(path for path in (spec.offset.parameter_path, spec.offset.limit_path) if path is not None)
    if isinstance(spec, KeysetTraversal):
        order_paths = (
            (spec.keyset.order_path,)
            if spec.keyset.split_order is None
            else (spec.keyset.split_order.field_path, spec.keyset.split_order.direction_path)
        )
        return tuple(
            path
            for path in (
                spec.keyset.filter_path,
                *order_paths,
                spec.keyset.start_suppression_path,
                spec.keyset.limit_path,
            )
            if path is not None
        )
    return tuple(path for path in (spec.cursor.parameter_path, spec.cursor.limit_path) if path is not None)


__all__ = [
    "CountedTraversal",
    "CursorSpec",
    "CursorTraversal",
    "KeysetSpec",
    "KeysetTraversal",
    "OffsetContinuation",
    "OffsetSpec",
    "SequentialTraversal",
    "SplitOrderSpec",
    "TotalTermination",
    "TraversalSpec",
    "traversal_control_paths",
]
