"""Frozen plan and dispatch descriptions with pre-I/O validation."""

from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from b24api.models import (
    DuplicatePolicy,
    IdentityRequirement,
    OrderSemantics,
    ParameterPath,
    ResultSelector,
    TotalSemantics,
)

PORTAL_BATCH_CAP = 50
MINIMUM_PARTITION_LANES = 2
_START_PATH = ParameterPath(("start",))
_FILTER_PATH = ParameterPath(("filter",))
_ORDER_PATH = ParameterPath(("order",))
_LAST_ID_PATH = ParameterPath(("LAST_ID",))


class OffsetContinuation(StrEnum):
    """How an offset plan computes the next offset."""

    SERVER_NEXT = "server_next"
    SERVER_NEXT_OR_OBSERVED_COUNT = "server_next_or_observed_count"
    OBSERVED_COUNT = "observed_count"


class OffsetTerminalRule(StrEnum):
    """Explicit offset termination evidence."""

    EMPTY_PAGE = "empty_page"
    QUALIFIED_TOTAL = "qualified_total"
    PROFILE_ABSENT_NEXT = "profile_absent_next"
    PROFILE_SHORT_PAGE = "profile_short_page"


class CountedOffsetMode(StrEnum):
    """Sequential or fixed-stride counted traversal."""

    SEQUENTIAL_NEXT = "sequential_next"
    PARALLEL_FIXED_STRIDE = "parallel_fixed_stride"


class KeysetTerminalRule(StrEnum):
    """Explicit keyset terminal evidence."""

    BOUNDARY_ID_SEEN = "boundary_id_seen"
    EMPTY_CONFIRMATION = "empty_confirmation"
    PROFILE_SHORT_PAGE = "profile_short_page"


class CursorTerminalRule(StrEnum):
    """Explicit item-cursor terminal evidence."""

    EMPTY_CONFIRMATION = "empty_confirmation"
    PROFILE_SHORT_PAGE = "profile_short_page"
    PROFILE_CURSOR_EXHAUSTED = "profile_cursor_exhausted"


class ReferenceOutputOrder(StrEnum):
    """Cross-reference delivery order."""

    READY = "ready"
    INPUT = "input"


@dataclass(frozen=True, slots=True, kw_only=True)
class PlanContract:
    """Correctness contract shared by every traversal plan."""

    selector: ResultSelector | None = None
    identity_requirement: IdentityRequirement = IdentityRequirement.OPTIONAL
    order_semantics: OrderSemantics = OrderSemantics.UNORDERED
    duplicate_policy: DuplicatePolicy = DuplicatePolicy.ERROR
    total_semantics: TotalSemantics = TotalSemantics.IGNORE
    invariants: tuple[str, ...] = ("bounded", "terminal_explicit")

    def __post_init__(self) -> None:
        object.__setattr__(self, "invariants", tuple(self.invariants))
        if not self.invariants or any(not invariant for invariant in self.invariants):
            raise ValueError("plans must declare non-empty local invariants")
        if self.selector is not None and not isinstance(self.selector, ResultSelector):
            raise TypeError("selector must be a ResultSelector or None")
        expected = (
            (self.identity_requirement, IdentityRequirement),
            (self.order_semantics, OrderSemantics),
            (self.duplicate_policy, DuplicatePolicy),
            (self.total_semantics, TotalSemantics),
        )
        if any(not isinstance(value, enum_type) for value, enum_type in expected):
            raise TypeError("plan contracts must use their declared enum types")


@dataclass(frozen=True, slots=True, kw_only=True)
class SingleResponsePlan(PlanContract):
    """Exactly one response with contradiction checks."""

    reject_continuation: bool = True
    reject_positive_total_over_result: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class OffsetSequentialPlan(PlanContract):
    """Sequential offset traversal with explicit continuation and terminal rules."""

    offset_path: ParameterPath = _START_PATH
    limit_path: ParameterPath | None = None
    requested_page_size: int | None = None
    continuation: OffsetContinuation = OffsetContinuation.SERVER_NEXT_OR_OBSERVED_COUNT
    terminal: frozenset[OffsetTerminalRule] = frozenset({OffsetTerminalRule.EMPTY_PAGE})
    allow_create_controls: bool = True

    def __post_init__(self) -> None:
        super(OffsetSequentialPlan, self).__post_init__()
        if not isinstance(self.continuation, OffsetContinuation):
            raise TypeError("continuation must be an OffsetContinuation")
        object.__setattr__(self, "terminal", frozenset(self.terminal))
        if any(not isinstance(rule, OffsetTerminalRule) for rule in self.terminal):
            raise TypeError("terminal rules must be OffsetTerminalRule values")
        _validate_page_size(self.limit_path, self.requested_page_size)
        if not self.terminal:
            raise ValueError("offset plan requires at least one terminal rule")
        if OffsetTerminalRule.PROFILE_SHORT_PAGE in self.terminal and self.requested_page_size is None:
            raise ValueError("short-page terminal requires a requested_page_size")
        if (
            OffsetTerminalRule.QUALIFIED_TOTAL in self.terminal
            and self.total_semantics is not TotalSemantics.FILTERED_EXACT
        ):
            raise ValueError("qualified-total terminal requires filtered exact total semantics")


@dataclass(frozen=True, slots=True, kw_only=True)
class CountedOffsetPlan(PlanContract):
    """Counted offset traversal whose parallel mode has an explicit stride."""

    total_semantics: TotalSemantics = TotalSemantics.FILTERED_EXACT
    mode: CountedOffsetMode = CountedOffsetMode.SEQUENTIAL_NEXT
    offset_path: ParameterPath = _START_PATH
    limit_path: ParameterPath | None = None
    requested_page_size: int | None = None
    fixed_stride: int | None = None
    allow_create_controls: bool = True

    def __post_init__(self) -> None:
        super(CountedOffsetPlan, self).__post_init__()
        if not isinstance(self.mode, CountedOffsetMode):
            raise TypeError("mode must be a CountedOffsetMode")
        if self.total_semantics is not TotalSemantics.FILTERED_EXACT:
            raise ValueError("counted offset requires filtered exact total semantics")
        _validate_page_size(self.limit_path, self.requested_page_size)
        if self.mode is CountedOffsetMode.PARALLEL_FIXED_STRIDE:
            if self.fixed_stride is None or not _is_plain_int(self.fixed_stride) or self.fixed_stride < 1:
                raise ValueError("parallel counted offset requires a positive fixed_stride")
            if self.requested_page_size is not None and self.fixed_stride != self.requested_page_size:
                raise ValueError("fixed_stride must equal requested_page_size")
        elif self.fixed_stride is not None:
            raise ValueError("sequential counted offset cannot declare fixed_stride")


@dataclass(frozen=True, slots=True, kw_only=True)
class KeysetPlan(PlanContract):
    """Sequential exact keyset traversal."""

    direction: Literal["asc", "desc"] = "asc"
    filter_path: ParameterPath = _FILTER_PATH
    order_path: ParameterPath = _ORDER_PATH
    limit_path: ParameterPath | None = None
    requested_page_size: int | None = None
    start_suppression_path: ParameterPath | None = _START_PATH
    terminal: KeysetTerminalRule = KeysetTerminalRule.EMPTY_CONFIRMATION
    allow_create_controls: bool = True

    def __post_init__(self) -> None:
        super(KeysetPlan, self).__post_init__()
        if self.direction not in {"asc", "desc"}:
            raise ValueError("keyset direction must be asc or desc")
        if not isinstance(self.terminal, KeysetTerminalRule):
            raise TypeError("terminal must be a KeysetTerminalRule")
        _validate_page_size(self.limit_path, self.requested_page_size)
        if self.terminal is KeysetTerminalRule.PROFILE_SHORT_PAGE and self.requested_page_size is None:
            raise ValueError("short-page terminal requires a requested_page_size")
        _require_distinct_paths(self.filter_path, self.order_path)
        if self.order_semantics is OrderSemantics.UNORDERED:
            raise ValueError("keyset plan requires declared ascending or descending order")
        expected = OrderSemantics.ASCENDING if self.direction == "asc" else OrderSemantics.DESCENDING
        if self.order_semantics is not expected:
            raise ValueError("keyset direction contradicts order_semantics")
        if self.identity_requirement is not IdentityRequirement.REQUIRED:
            raise ValueError("keyset plan requires identity")


@dataclass(frozen=True, slots=True, kw_only=True)
class ItemCursorPlan(PlanContract):
    """Sequential cursor derived from an item in the preceding page."""

    cursor_request_path: ParameterPath = _LAST_ID_PATH
    cursor_item_path: tuple[str | int, ...] = ("id",)
    direction: Literal["asc", "desc"] = "asc"
    cursor_take: Literal["first", "last", "min", "max"] = "max"
    limit_path: ParameterPath | None = None
    requested_page_size: int | None = None
    terminal: CursorTerminalRule = CursorTerminalRule.EMPTY_CONFIRMATION
    allow_create_controls: bool = True

    def __post_init__(self) -> None:
        super(ItemCursorPlan, self).__post_init__()
        object.__setattr__(self, "cursor_item_path", tuple(self.cursor_item_path))
        if not self.cursor_item_path:
            raise ValueError("cursor_item_path must not be empty")
        ParameterPath(self.cursor_item_path)
        if self.direction not in {"asc", "desc"}:
            raise ValueError("cursor direction must be asc or desc")
        if self.cursor_take not in {"first", "last", "min", "max"}:
            raise ValueError("cursor_take is invalid")
        if not isinstance(self.terminal, CursorTerminalRule):
            raise TypeError("terminal must be a CursorTerminalRule")
        _validate_page_size(self.limit_path, self.requested_page_size)
        if self.terminal is CursorTerminalRule.PROFILE_SHORT_PAGE and self.requested_page_size is None:
            raise ValueError("short-page terminal requires a requested_page_size")
        if self.identity_requirement is not IdentityRequirement.REQUIRED:
            raise ValueError("item cursor plan requires identity")
        expected = OrderSemantics.ASCENDING if self.direction == "asc" else OrderSemantics.DESCENDING
        if self.order_semantics is not expected:
            raise ValueError("cursor direction contradicts order_semantics")


@dataclass(frozen=True, slots=True, kw_only=True)
class PartitionedKeysetPlan(PlanContract):
    """Internal fixed-lane partitioned keyset evidence candidate."""

    lane_count: int = 2
    direction: Literal["asc", "desc"] = "asc"
    filter_path: ParameterPath = _FILTER_PATH
    order_path: ParameterPath = _ORDER_PATH
    limit_path: ParameterPath | None = None
    requested_page_size: int | None = None
    merge_order: ReferenceOutputOrder = ReferenceOutputOrder.READY

    def __post_init__(self) -> None:
        super(PartitionedKeysetPlan, self).__post_init__()
        if self.direction not in {"asc", "desc"}:
            raise ValueError("partition direction must be asc or desc")
        if not isinstance(self.merge_order, ReferenceOutputOrder):
            raise TypeError("merge_order must be a ReferenceOutputOrder")
        if not _is_plain_int(self.lane_count) or not MINIMUM_PARTITION_LANES <= self.lane_count <= PORTAL_BATCH_CAP:
            raise ValueError("partition lane_count must be between 2 and the hard batch cap 50")
        _validate_page_size(self.limit_path, self.requested_page_size)
        _require_distinct_paths(self.filter_path, self.order_path)
        if self.identity_requirement is not IdentityRequirement.REQUIRED:
            raise ValueError("partitioned keyset requires identity")
        expected = OrderSemantics.ASCENDING if self.direction == "asc" else OrderSemantics.DESCENDING
        if self.order_semantics is not expected:
            raise ValueError("partition direction contradicts order_semantics")


@dataclass(frozen=True, slots=True, kw_only=True)
class BatchDispatch:
    """Explicit bounded batch dispatch."""

    batch_size: int = 50
    output_order: ReferenceOutputOrder = ReferenceOutputOrder.READY
    fallback_failed: Literal["none", "direct"] = "none"

    def __post_init__(self) -> None:
        if not _is_plain_int(self.batch_size) or not 1 <= self.batch_size <= PORTAL_BATCH_CAP:
            raise ValueError("batch_size must be between 1 and 50")
        if not isinstance(self.output_order, ReferenceOutputOrder):
            raise TypeError("output_order must be a ReferenceOutputOrder")
        if self.fallback_failed not in {"none", "direct"}:
            raise ValueError("fallback_failed must be none or direct")


@dataclass(frozen=True, slots=True, kw_only=True)
class DirectDispatch:
    """Explicit bounded direct dispatch."""

    concurrency: int = 10
    output_order: ReferenceOutputOrder = ReferenceOutputOrder.READY

    def __post_init__(self) -> None:
        if not _is_plain_int(self.concurrency) or self.concurrency < 1:
            raise ValueError("direct concurrency must be positive")
        if not isinstance(self.output_order, ReferenceOutputOrder):
            raise TypeError("output_order must be a ReferenceOutputOrder")


type ListPlan = (
    SingleResponsePlan | OffsetSequentialPlan | CountedOffsetPlan | KeysetPlan | ItemCursorPlan | PartitionedKeysetPlan
)
type DispatchPlan = BatchDispatch | DirectDispatch


def _validate_page_size(limit_path: ParameterPath | None, requested_page_size: int | None) -> None:
    if requested_page_size is not None and (not _is_plain_int(requested_page_size) or requested_page_size < 1):
        raise ValueError("requested_page_size must be positive")
    if requested_page_size is not None and limit_path is None:
        raise ValueError("requested_page_size requires limit_path")


def _require_distinct_paths(left: ParameterPath, right: ParameterPath) -> None:
    left_normalized = tuple(item.casefold() if isinstance(item, str) else item for item in left.path)
    right_normalized = tuple(item.casefold() if isinstance(item, str) else item for item in right.path)
    if left_normalized == right_normalized:
        raise ValueError("filter and order paths must be case-insensitively distinct")


def _is_plain_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


__all__ = [
    "BatchDispatch",
    "CountedOffsetMode",
    "CountedOffsetPlan",
    "CursorTerminalRule",
    "DirectDispatch",
    "DispatchPlan",
    "ItemCursorPlan",
    "KeysetPlan",
    "KeysetTerminalRule",
    "ListPlan",
    "OffsetContinuation",
    "OffsetSequentialPlan",
    "OffsetTerminalRule",
    "PartitionedKeysetPlan",
    "PlanContract",
    "ReferenceOutputOrder",
    "SingleResponsePlan",
]
