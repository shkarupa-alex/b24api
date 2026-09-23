"""Method-agnostic public list traversal mechanics."""

from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from b24api.contracts.bounded_range import BoundedIdentityRange
from b24api.contracts.keyset_execution import (
    AutoKeysetExecution,
    KeysetExecution,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    SequentialKeysetExecution,
)
from b24api.contracts.page import IdentityPageAdapter, PageAdapter
from b24api.contracts.policy import IdentityCoercion
from b24api.contracts.request import IdentitySpec, ParameterPath, ResultSelector, TraversalIdentity

_START = ParameterPath(("start",))
_FILTER = ParameterPath(("filter",))
_ORDER = ParameterPath(("order",))
_ROOT_SELECTOR = ResultSelector.root()
_SEQUENTIAL_KEYSET_EXECUTION = SequentialKeysetExecution()
_IDENTITY_PAGE_ADAPTER = IdentityPageAdapter()


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


class CursorDomain(StrEnum):
    """How a cursor control is interpreted by its qualified endpoint."""

    OPAQUE = "opaque"
    EXCLUSIVE_POSITIVE_INTEGER = "exclusive_positive_integer"


@dataclass(frozen=True, slots=True)
class PageIndex:
    """One-based or arbitrary page control independent of decoded row count."""

    control_path: ParameterPath
    initial: int = 1
    increment: int = 1
    max_rows: int = 10

    def __post_init__(self) -> None:
        """Require a finite forward page sequence and a bounded decoded page."""
        if not isinstance(self.control_path, ParameterPath):
            raise TypeError("control_path must be a ParameterPath")
        if any(type(value) is not int or value < 1 for value in (self.initial, self.increment, self.max_rows)):
            raise ValueError("page index initial, increment, and max_rows must be positive integers")


@dataclass(frozen=True, slots=True)
class PageStride:
    """Qualified server offset granularity and independent decoded row cap."""

    server_granularity: int
    wire_increment: int
    max_decoded_rows: int
    requested_wire_limit: int | None = None

    def __post_init__(self) -> None:
        """Reject controls that could alias the same rounded server page."""
        values = (self.server_granularity, self.wire_increment, self.max_decoded_rows)
        if any(type(value) is not int or value < 1 for value in values):
            raise ValueError("page stride fields must be positive integers")
        if self.wire_increment % self.server_granularity:
            raise ValueError("wire increment must align with server page granularity")
        if self.requested_wire_limit is not None and (
            type(self.requested_wire_limit) is not int
            or self.requested_wire_limit < self.server_granularity
            or self.requested_wire_limit % self.server_granularity
        ):
            raise ValueError("requested wire limit must align with server page granularity")


@dataclass(frozen=True, slots=True)
class SparseRawBound:
    """Explicit closure using stable raw total despite empty selected pages."""

    total_path: ResultSelector
    stride: PageStride
    max_pages: int
    order_contract: str

    def __post_init__(self) -> None:
        """Require a finite qualified raw range and declared stable order."""
        if not isinstance(self.total_path, ResultSelector) or not self.total_path.path:
            raise ValueError("sparse raw total needs a non-root result path")
        if not isinstance(self.stride, PageStride):
            raise TypeError("sparse stride must be a PageStride")
        if type(self.max_pages) is not int or self.max_pages < 1:
            raise ValueError("sparse max_pages must be positive")
        if not isinstance(self.order_contract, str) or not self.order_contract:
            raise ValueError("sparse raw bound requires a stable order contract")


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


def _validate_offset_extensions(spec: OffsetSpec) -> None:  # noqa: C901 - closed optional plan variants
    """Reject contradictory page-index, stride, and sparse closure declarations."""
    if spec.page_index is not None:
        if not isinstance(spec.page_index, PageIndex) or spec.parameter_path != spec.page_index.control_path:
            raise ValueError("page_index control_path must equal the offset parameter_path")
        if spec.continuation is not OffsetContinuation.SERVER_NEXT_OR_OBSERVED_COUNT or spec.step is not None:
            raise ValueError("page_index owns progression and cannot combine with offset continuation")
        if spec.total_termination is not TotalTermination.DISABLED:
            raise ValueError("page_index requires explicit empty-page closure")
    if spec.page_stride is not None:
        if not isinstance(spec.page_stride, PageStride) or spec.page_index is not None:
            raise ValueError("page_stride cannot combine with page_index")
        if spec.continuation is not OffsetContinuation.FIXED_STEP or spec.step != spec.page_stride.wire_increment:
            raise ValueError("page_stride requires matching fixed-step continuation")
    if spec.sparse_raw_bound is not None:
        if not isinstance(spec.sparse_raw_bound, SparseRawBound) or spec.page_stride != spec.sparse_raw_bound.stride:
            raise ValueError("sparse raw bound requires its declared page_stride")
        if spec.total_termination is not TotalTermination.DISABLED:
            raise ValueError("sparse raw bound owns closure independently of selected count")


@dataclass(frozen=True, slots=True)
class OffsetSpec:
    """Offset and optional page-limit parameter locations."""

    parameter_path: ParameterPath = _START
    limit_path: ParameterPath | None = None
    allow_create_controls: bool = True
    continuation: OffsetContinuation = OffsetContinuation.SERVER_NEXT_OR_OBSERVED_COUNT
    step: int | None = None
    total_termination: TotalTermination = TotalTermination.DISABLED
    page_index: PageIndex | None = None
    page_stride: PageStride | None = None
    sparse_raw_bound: SparseRawBound | None = None

    def __post_init__(self) -> None:
        """Validate offset mechanics and completion semantics."""
        if not isinstance(self.continuation, OffsetContinuation) or not isinstance(
            self.total_termination,
            TotalTermination,
        ):
            raise TypeError("offset controls must use their declared enum types")
        _validate_offset_extensions(self)
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
    boundary: BoundedIdentityRange | None = None

    def __post_init__(self) -> None:
        """Validate keyset direction."""
        if self.direction not in {"ascending", "descending"}:
            raise ValueError("direction must be ascending or descending")
        if self.boundary is not None and (
            not isinstance(self.boundary, BoundedIdentityRange) or self.direction != self.boundary.order
        ):
            raise ValueError("keyset boundary requires a matching ascending direction")
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
    domain: CursorDomain = CursorDomain.OPAQUE

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
        if not isinstance(self.domain, CursorDomain):
            raise TypeError("domain must be a CursorDomain")
        if self.domain is CursorDomain.EXCLUSIVE_POSITIVE_INTEGER and self.coercion not in {
            IdentityCoercion.EXACT_INTEGER,
            IdentityCoercion.DECIMAL_STRING_INTEGER,
        }:
            raise ValueError("exclusive positive range requires integer cursor coercion")


@dataclass(frozen=True, slots=True)
class SequentialTraversal:
    """Conservative sequential offset traversal."""

    selector: ResultSelector = _ROOT_SELECTOR
    identity: TraversalIdentity | None = None
    page_size: int = 50
    offset: OffsetSpec = OffsetSpec()
    page_adapter: PageAdapter = _IDENTITY_PAGE_ADAPTER

    def __post_init__(self) -> None:
        """Validate page cap."""
        _positive_page_size(self.page_size)
        if self.offset.page_stride is not None and self.page_size != self.offset.page_stride.max_decoded_rows:
            raise ValueError("page_size must match page_stride max_decoded_rows")
        if self.offset.page_index is not None and self.page_size != self.offset.page_index.max_rows:
            raise ValueError("page_size must match page_index max_rows")
        if (
            self.offset.continuation is OffsetContinuation.FIXED_STEP
            and self.offset.page_stride is None
            and self.offset.step != self.page_size
        ):
            raise ValueError("fixed-step traversal requires page_size equal to step")


@dataclass(frozen=True, slots=True)
class CountedTraversal:
    """Direct-head plus physically batched exact counted traversal."""

    identity: TraversalIdentity | None = None
    selector: ResultSelector = _ROOT_SELECTOR
    page_size: int = 50
    offset: OffsetSpec = _COUNTED_OFFSET
    page_adapter: PageAdapter = _IDENTITY_PAGE_ADAPTER

    def __post_init__(self) -> None:
        """Validate required identity and page cap."""
        _positive_page_size(self.page_size)
        if self.offset.page_index is not None:
            raise ValueError("counted physical batch does not support page_index")
        if self.offset.total_termination is not TotalTermination.EXACT_QUALIFIED:
            raise ValueError("counted traversal requires exact-qualified total termination")
        if self.offset.continuation is OffsetContinuation.FIXED_STEP and self.offset.step != self.page_size:
            raise ValueError("fixed-step counted traversal requires page_size equal to step")
        stride = self.offset.page_stride
        if stride is not None and stride.max_decoded_rows != self.page_size:
            raise ValueError("counted page_size must match page_stride max_decoded_rows")
        if stride is not None and stride.requested_wire_limit is not None:
            raise ValueError("counted traversal does not support page_stride requested_wire_limit")


@dataclass(frozen=True, slots=True)
class KeysetTraversal:
    """Exact sequential no-count traversal."""

    selector: ResultSelector
    identity: IdentitySpec
    page_size: int = 50
    keyset: KeysetSpec = KeysetSpec()
    execution: KeysetExecution = _SEQUENTIAL_KEYSET_EXECUTION
    page_adapter: PageAdapter = _IDENTITY_PAGE_ADAPTER

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
    page_adapter: PageAdapter = _IDENTITY_PAGE_ADAPTER

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
