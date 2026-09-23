"""Immutable, correlated evidence for operation completion."""

from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum

_MAX_ID_LENGTH = 128


class CommandSettlement(StrEnum):
    """Physical command outcome relevant to page completion."""

    SUCCESS = "success"
    FAILURE = "failure"
    UNKNOWN = "unknown"
    NOT_EXECUTED = "not_executed"


class BindingClosure(StrEnum):
    """One qualified terminal witness for an admitted binding."""

    SOURCE_EMPTY = "source_empty"
    QUALIFIED_TOTAL = "qualified_total"
    RAW_RANGE_COVERED = "raw_range_covered"
    SINGLE_RESPONSE = "single_response"
    BOUNDARY_SEEN = "boundary_seen"
    KEYSET_PLAN_COVERED = "keyset_plan_covered"
    CALLER_STOP = "caller_stop"
    FAILURE = "failure"
    UNKNOWN = "unknown"


class StreamClosure(StrEnum):
    """Global producer outcome before cleanup."""

    NATURAL = "natural"
    EARLY_CLOSE = "early_close"
    CANCELLED = "cancelled"


class CleanupState(StrEnum):
    """Final owned-resource cleanup outcome."""

    SUCCESS = "success"
    FAILURE = "failure"


@dataclass(frozen=True, slots=True, kw_only=True)
class CompletionEvent:
    """Common correlation and strictly increasing operation sequence."""

    operation_id: str
    sequence: int

    def __post_init__(self) -> None:
        """Reject invalid or unbounded correlation values."""
        if not isinstance(self.operation_id, str) or not 0 < len(self.operation_id) <= _MAX_ID_LENGTH:
            raise ValueError("operation_id must be a bounded non-empty string")
        if type(self.sequence) is not int or self.sequence < 0:
            raise ValueError("sequence must be a non-negative integer")


@dataclass(frozen=True, slots=True, kw_only=True)
class BindingAdmitted(CompletionEvent):
    """Register one binding before its first page schedule."""

    binding_id: int


@dataclass(frozen=True, slots=True, kw_only=True)
class PageScheduled(CompletionEvent):
    """Register one unique logical page before dispatch."""

    binding_id: int
    page_id: int


@dataclass(frozen=True, slots=True, kw_only=True)
class PageCommandOutcome(CompletionEvent):
    """Settle the physical command that produced one logical page."""

    binding_id: int
    page_id: int
    outcome: CommandSettlement


@dataclass(frozen=True, slots=True, kw_only=True)
class PageValidated(CompletionEvent):
    """Record validation of one successful page without retaining rows."""

    binding_id: int
    page_id: int
    identity_digest: str
    row_count: int


@dataclass(frozen=True, slots=True, kw_only=True)
class PageDelivered(CompletionEvent):
    """Record delivery of the whole validated page to the consumer."""

    binding_id: int
    page_id: int


@dataclass(frozen=True, slots=True, kw_only=True)
class PageAcknowledged(CompletionEvent):
    """Record successful durable acknowledgement of the delivered page."""

    binding_id: int
    page_id: int


@dataclass(frozen=True, slots=True, kw_only=True)
class PageRejected(CompletionEvent):
    """Settle a page without admitting its rows."""

    binding_id: int
    page_id: int
    reason: str


@dataclass(frozen=True, slots=True, kw_only=True)
class BindingTerminal(CompletionEvent):
    """Settle one binding after all its pages are accounted for."""

    binding_id: int
    closure: BindingClosure
    qualified_total: int | None = None
    qualified_witnesses: int | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class StreamTerminal(CompletionEvent):
    """Settle the operation producer once."""

    closure: StreamClosure
    empty_source: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class CleanupOutcome(CompletionEvent):
    """Settle owned-resource cleanup after the stream terminal."""

    state: CleanupState


type CompletionEvidence = (
    BindingAdmitted
    | PageScheduled
    | PageCommandOutcome
    | PageValidated
    | PageDelivered
    | PageAcknowledged
    | PageRejected
    | BindingTerminal
    | StreamTerminal
    | CleanupOutcome
)
