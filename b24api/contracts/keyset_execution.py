"""Typed execution contracts for no-count integer-keyset traversal."""

from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, TypedDict

PORTAL_BATCH_CAP = 50
MIN_WINDOW_WIDTH = 2
MIN_TARGET_LANES = 2


class KeysetPageCompletion(StrEnum):
    """Caller-asserted evidence that closes one bounded keyset lane."""

    EMPTY_CONFIRMATION = "empty_confirmation"
    SHORT_PAGE_EXHAUSTS = "short_page_exhausts"


class TotalHintMode(StrEnum):
    """Whether boundary requests may ask for an advisory total."""

    IGNORE = "ignore"
    REQUEST_ADVISORY = "request_advisory"


class KeysetExecutionKind(StrEnum):
    """Requested or selected keyset execution strategy."""

    SEQUENTIAL = "sequential"
    RANGE = "range"
    PARTITIONED = "partitioned"
    AUTO = "auto"
    BOUNDARY_ONLY = "boundary_only"


class KeysetSelectionReason(StrEnum):
    """Closed, report-safe planner decision reasons."""

    EXPLICIT_RANGE = "explicit_range"
    EXPLICIT_PARTITIONED = "explicit_partitioned"
    EMPTY_SELECTION = "empty_selection"
    BOUNDARY_OVERLAP = "boundary_overlap"
    ADJACENT_BOUNDARIES = "adjacent_boundaries"
    SMALL_SELECTION = "small_selection"
    INSUFFICIENT_PREDICTED_GAIN = "insufficient_predicted_gain"
    RANGE_WITHIN_WAVE_BUDGET = "range_within_wave_budget"
    WIDE_SPAN_PARTITIONING = "wide_span_partitioning"
    POST_PROBE_RANGE_PREFERRED = "post_probe_range_preferred"
    POST_PROBE_GAIN_LOST = "post_probe_gain_lost"
    DEGENERATE_SINGLE_LANE = "degenerate_single_lane"


class KeysetAssuranceSource(StrEnum):
    """Evidence source for the selected fast plan."""

    ORDERED_PREFIX_ONLY = "ordered_prefix_only"
    CANARY_VERIFIED_BOUNDS = "canary_verified_bounds"
    CALLER_ASSERTED_BOUNDS = "caller_asserted_bounds"


class KeysetPhase(StrEnum):
    """Fast traversal state-machine phase."""

    BOUNDARY = "boundary"
    CANARY = "canary"
    ANCHOR_PROBE = "anchor_probe"
    BODY = "body"
    FINISH = "finish"


class ClosureWitness(StrEnum):
    """Independent evidence that one bounded lane is exhausted."""

    EMPTY = "empty"
    TOP = "top"
    LATTICE_FULL = "lattice_full"
    SHORT_PAGE = "short_page"
    ANCHOR_FENCE = "anchor_fence"


class TraceClass(StrEnum):
    """Deterministic bounded trace-retention class."""

    ANOMALY = "anomaly"
    PLANNING = "planning"
    TERMINAL = "terminal"
    BODY = "body"


class KeysetExecutionJson(TypedDict, total=False):
    """Closed JSON representation accepted by the CLI."""

    kind: Literal["sequential", "range", "partitioned", "auto"]
    page_completion: Literal["empty_confirmation", "short_page_exhausts"]
    batch_size: int
    window_width: int
    range_window_width: int
    target_lanes: int
    max_range_waves: int
    total_hint: Literal["ignore", "request_advisory"]
    endpoint_page_cap: int


def _optional_positive(value: object, *, field: str, maximum: int | None = None) -> None:
    if value is None:
        return
    if not isinstance(value, int) or isinstance(value, bool) or value < 1 or (maximum is not None and value > maximum):
        suffix = f" within 1..{maximum}" if maximum is not None else " positive"
        raise ValueError(f"{field} must be a{suffix} integer")


@dataclass(frozen=True, slots=True)
class StableIntegerKeysetContract:
    """Caller assertion supporting exact bounded integer traversal."""

    page_completion: KeysetPageCompletion = KeysetPageCompletion.EMPTY_CONFIRMATION
    endpoint_page_cap: int | None = None

    def __post_init__(self) -> None:
        """Validate completion and endpoint cap."""
        if not isinstance(self.page_completion, KeysetPageCompletion):
            raise TypeError("page_completion must be a KeysetPageCompletion")
        _optional_positive(self.endpoint_page_cap, field="endpoint_page_cap")


@dataclass(frozen=True, slots=True)
class SequentialKeysetExecution:
    """Preserve the existing sequential keyset mechanics."""


@dataclass(frozen=True, slots=True)
class RangeKeysetExecution:
    """Traverse the captured interior through numeric windows."""

    contract: StableIntegerKeysetContract
    batch_size: int | None = None
    window_width: int | None = None

    def __post_init__(self) -> None:
        """Validate range controls."""
        if not isinstance(self.contract, StableIntegerKeysetContract):
            raise TypeError("contract must be a StableIntegerKeysetContract")
        _optional_positive(self.batch_size, field="batch_size", maximum=PORTAL_BATCH_CAP)
        if self.window_width is not None and (
            not isinstance(self.window_width, int)
            or isinstance(self.window_width, bool)
            or self.window_width < MIN_WINDOW_WIDTH
        ):
            raise ValueError("window_width must be an integer of at least 2")


@dataclass(frozen=True, slots=True)
class PartitionedKeysetExecution:
    """Traverse the captured interior through occupied-anchor lanes."""

    contract: StableIntegerKeysetContract
    batch_size: int | None = None
    target_lanes: int = 20

    def __post_init__(self) -> None:
        """Validate partition controls."""
        if not isinstance(self.contract, StableIntegerKeysetContract):
            raise TypeError("contract must be a StableIntegerKeysetContract")
        _optional_positive(self.batch_size, field="batch_size", maximum=PORTAL_BATCH_CAP)
        if (
            not isinstance(self.target_lanes, int)
            or isinstance(self.target_lanes, bool)
            or not MIN_TARGET_LANES <= self.target_lanes <= PORTAL_BATCH_CAP
        ):
            raise ValueError("target_lanes must be an integer within 2..50")


@dataclass(frozen=True, slots=True)
class AutoKeysetExecution:
    """Deterministically select sequential, range, or partitioned execution."""

    contract: StableIntegerKeysetContract
    batch_size: int | None = None
    target_lanes: int = 20
    range_window_width: int | None = None
    max_range_waves: int = 2
    total_hint: TotalHintMode = TotalHintMode.IGNORE

    def __post_init__(self) -> None:
        """Validate automatic selector controls."""
        if not isinstance(self.contract, StableIntegerKeysetContract):
            raise TypeError("contract must be a StableIntegerKeysetContract")
        _optional_positive(self.batch_size, field="batch_size", maximum=PORTAL_BATCH_CAP)
        if (
            not isinstance(self.target_lanes, int)
            or isinstance(self.target_lanes, bool)
            or not MIN_TARGET_LANES <= self.target_lanes <= PORTAL_BATCH_CAP
        ):
            raise ValueError("target_lanes must be an integer within 2..50")
        if self.range_window_width is not None and (
            not isinstance(self.range_window_width, int)
            or isinstance(self.range_window_width, bool)
            or self.range_window_width < MIN_WINDOW_WIDTH
        ):
            raise ValueError("range_window_width must be an integer of at least 2")
        if (
            not isinstance(self.max_range_waves, int)
            or isinstance(self.max_range_waves, bool)
            or self.max_range_waves < 1
        ):
            raise ValueError("max_range_waves must be a positive integer")
        if not isinstance(self.total_hint, TotalHintMode):
            raise TypeError("total_hint must be a TotalHintMode")


type KeysetExecution = (
    SequentialKeysetExecution | RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution
)


__all__ = [
    "AutoKeysetExecution",
    "ClosureWitness",
    "KeysetAssuranceSource",
    "KeysetExecution",
    "KeysetExecutionJson",
    "KeysetExecutionKind",
    "KeysetPageCompletion",
    "KeysetPhase",
    "KeysetSelectionReason",
    "PartitionedKeysetExecution",
    "RangeKeysetExecution",
    "SequentialKeysetExecution",
    "StableIntegerKeysetContract",
    "TotalHintMode",
    "TraceClass",
]
