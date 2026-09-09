"""Immutable public operation terminal evidence."""

from __future__ import annotations
import math
from dataclasses import dataclass
from enum import StrEnum

from b24api.contracts.keyset_execution import (
    ClosureWitness,
    KeysetAssuranceSource,
    KeysetExecutionKind,
    KeysetPhase,
    KeysetSelectionReason,
    TraceClass,
)
from b24api.redaction import DEFAULT_REDACTOR

VIOLATION_CODE_MAXIMUM = 100
VIOLATION_MESSAGE_MAXIMUM = 500


class ViolationSeverity(StrEnum):
    """Whether a bounded report violation blocks completion."""

    WARNING = "warning"
    BLOCKING = "blocking"


@dataclass(frozen=True, slots=True)
class Violation:
    """Typed bounded report violation."""

    severity: ViolationSeverity
    code: str
    message: str
    field: str | None = None

    def __post_init__(self) -> None:
        """Redact and validate bounded diagnostic text."""
        if not isinstance(self.severity, ViolationSeverity):
            raise TypeError("severity must be a ViolationSeverity")
        object.__setattr__(self, "code", DEFAULT_REDACTOR.redact_text(self.code))
        object.__setattr__(self, "message", DEFAULT_REDACTOR.redact_text(self.message))
        if self.field is not None:
            object.__setattr__(self, "field", DEFAULT_REDACTOR.redact_text(self.field))
        if not self.code or len(self.code) > VIOLATION_CODE_MAXIMUM:
            raise ValueError("violation code must be 1..100 characters")
        if not self.message or len(self.message) > VIOLATION_MESSAGE_MAXIMUM:
            raise ValueError("violation message must be 1..500 characters")


class TerminalState(StrEnum):
    """Closed operation terminal states."""

    COMPLETED = "completed"
    COMPLETED_WITH_FAILURES = "completed_with_failures"
    EARLY_CLOSED = "early_closed"
    CANCELLED = "cancelled"
    FAILED = "failed"
    INCOMPLETE = "incomplete"


class TraversalAssurance(StrEnum):
    """Strength of list traversal completion evidence."""

    MECHANICS_ONLY = "mechanics_only"
    IDENTITY_EXACT = "identity_exact"
    COUNT_MATCHED = "count_matched"
    IDENTITY_AND_COUNT_MATCHED = "identity_and_count_matched"


class PageDispatch(StrEnum):
    """How one logical traversal page was dispatched."""

    DIRECT = "direct"
    BATCH = "batch"


class PageOutcome(StrEnum):
    """Whether a scheduled page was committed, rejected, or unknown."""

    COMMITTED = "committed"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


class PageRejectionCode(StrEnum):
    """Bounded page-level rejection categories."""

    DUPLICATE_IDENTITY = "duplicate_identity"
    REPEATED_FINGERPRINT = "repeated_fingerprint"
    TOTAL_DRIFT = "total_drift"
    RANGE_CONTRADICTION = "range_contradiction"
    COMMAND_FAILURE = "command_failure"
    SHAPE_CONTRACT = "shape_contract"
    IDENTITY_CONTRACT = "identity_contract"
    AMBIGUOUS_EXECUTION = "ambiguous_execution"
    BATCH_ENVELOPE = "batch_envelope"
    NOT_EXECUTED = "not_executed"
    TRANSACTION_ABORTED = "transaction_aborted"


@dataclass(frozen=True, slots=True)
class PageRecord:
    """Value-free provenance for one logical traversal page."""

    sequence: int
    offset: int | None
    dispatch: PageDispatch
    batch_index: int | None
    rows_selected: int
    rows_admitted: int
    reported_total: int | None
    reported_next: int | None
    outcome: PageOutcome
    rejection_code: PageRejectionCode | None
    reference_index: int | None = None
    phase: KeysetPhase | None = None
    lane_ordinal: int | None = None

    def __post_init__(self) -> None:
        """Validate page evidence."""
        integers = (self.sequence, self.rows_selected, self.rows_admitted)
        optional = (self.offset, self.batch_index, self.reported_total, self.reported_next, self.reference_index)
        if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in integers):
            raise ValueError("page counters must be non-negative integers")
        if any(
            value is not None and (not isinstance(value, int) or isinstance(value, bool) or value < 0)
            for value in optional
        ):
            raise ValueError("optional page counters must be non-negative integers")
        if not isinstance(self.dispatch, PageDispatch) or not isinstance(self.outcome, PageOutcome):
            raise TypeError("page record enums must use their declared types")
        if self.rejection_code is not None and not isinstance(self.rejection_code, PageRejectionCode):
            raise TypeError("rejection_code must be a PageRejectionCode")
        if self.outcome is PageOutcome.COMMITTED and self.rejection_code is not None:
            raise ValueError("committed pages cannot carry a rejection code")
        if self.outcome is not PageOutcome.COMMITTED and self.rows_admitted:
            raise ValueError("uncommitted pages cannot admit rows")
        if self.phase is not None and not isinstance(self.phase, KeysetPhase):
            raise TypeError("phase must be a KeysetPhase or None")
        if self.lane_ordinal is not None and (
            not isinstance(self.lane_ordinal, int) or isinstance(self.lane_ordinal, bool) or self.lane_ordinal < 0
        ):
            raise ValueError("lane_ordinal must be a non-negative integer or None")


@dataclass(frozen=True, slots=True)
class KeysetExecutionReport:
    """Immutable aggregate evidence for one fast keyset execution."""

    requested_kind: KeysetExecutionKind
    selected_kind: KeysetExecutionKind
    preselection_reason: KeysetSelectionReason
    final_selection_reason: KeysetSelectionReason | None
    assurance_source: KeysetAssuranceSource
    planning_requests: int
    boundary_requests: int
    canary_requests: int
    anchor_probe_requests: int
    canary_commands: int
    canary_rows: int
    anchor_probe_commands: int
    anchor_count: int
    empty_anchor_probes: int
    probe_rows_discarded: int
    boundary_overlap_rows: int
    head_page_admitted: bool
    sequential_requests_estimate: int | None
    selected_requests_estimate: int | None
    head_rows: int
    tail_rows: int
    interior_span: int | None
    interior_rows_estimate: int | None
    total_rows_estimate: int | None
    density_numerator: int | None
    density_denominator: int | None
    effective_window_width: int | None
    range_window_count: int | None
    target_lanes: int | None
    actual_lanes: int | None
    continuation_count: int
    closure_witness_counts: tuple[tuple[ClosureWitness, int], ...]
    effective_batch_capacity: int
    total_hint_requested: bool
    total_hint_observed: int | None
    total_hint_plausible: bool
    total_hint_used: bool
    trace_retained_by_class: tuple[tuple[TraceClass, int], ...]
    trace_dropped_by_class: tuple[tuple[TraceClass, int], ...]
    raw_rows: int = 0

    def __post_init__(self) -> None:
        """Validate closed aggregate report fields."""
        enum_values = (
            (self.requested_kind, KeysetExecutionKind),
            (self.selected_kind, KeysetExecutionKind),
            (self.preselection_reason, KeysetSelectionReason),
            (self.assurance_source, KeysetAssuranceSource),
        )
        if any(not isinstance(value, kind) for value, kind in enum_values):
            raise TypeError("keyset report enums must use their declared types")
        if self.final_selection_reason is not None and not isinstance(
            self.final_selection_reason,
            KeysetSelectionReason,
        ):
            raise TypeError("final_selection_reason must be a KeysetSelectionReason or None")
        integers = (
            self.planning_requests,
            self.boundary_requests,
            self.canary_requests,
            self.anchor_probe_requests,
            self.canary_commands,
            self.canary_rows,
            self.anchor_probe_commands,
            self.anchor_count,
            self.empty_anchor_probes,
            self.probe_rows_discarded,
            self.boundary_overlap_rows,
            self.head_rows,
            self.tail_rows,
            self.continuation_count,
            self.effective_batch_capacity,
            self.raw_rows,
        )
        optional = (
            self.sequential_requests_estimate,
            self.selected_requests_estimate,
            self.interior_span,
            self.interior_rows_estimate,
            self.total_rows_estimate,
            self.density_numerator,
            self.density_denominator,
            self.effective_window_width,
            self.range_window_count,
            self.target_lanes,
            self.actual_lanes,
        )
        if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in integers):
            raise ValueError("keyset report counters must be non-negative integers")
        if any(
            value is not None and (not isinstance(value, int) or isinstance(value, bool) or value < 0)
            for value in optional
        ):
            raise ValueError("optional keyset report counters must be non-negative integers")
        flags = (
            self.head_page_admitted,
            self.total_hint_requested,
            self.total_hint_plausible,
            self.total_hint_used,
        )
        if any(not isinstance(value, bool) for value in flags):
            raise TypeError("keyset report flags must be booleans")
        object.__setattr__(self, "closure_witness_counts", tuple(self.closure_witness_counts))
        object.__setattr__(self, "trace_retained_by_class", tuple(self.trace_retained_by_class))
        object.__setattr__(self, "trace_dropped_by_class", tuple(self.trace_dropped_by_class))
        self._validate_counts(self.closure_witness_counts, ClosureWitness, "closure_witness_counts")
        self._validate_counts(self.trace_retained_by_class, TraceClass, "trace_retained_by_class")
        self._validate_counts(self.trace_dropped_by_class, TraceClass, "trace_dropped_by_class")

    @staticmethod
    def _validate_counts(values: tuple[tuple[object, int], ...], kind: type[StrEnum], field: str) -> None:
        if any(
            not isinstance(key, kind) or not isinstance(count, int) or isinstance(count, bool) or count < 0
            for key, count in values
        ):
            raise ValueError(f"{field} must contain declared enums and non-negative integer counts")


@dataclass(frozen=True, slots=True)
class OperationReport:
    """Bounded redacted counters frozen after cleanup."""

    state: TerminalState
    operation: str
    terminal_reason: str
    assurance: TraversalAssurance | None = None
    admitted: int = 0
    emitted: int = 0
    successes: int = 0
    failures: int = 0
    not_executed: int = 0
    unknown: int = 0
    unique_rows: int = 0
    physical_requests: int = 0
    logical_pages: int = 0
    batch_requests: int = 0
    batch_commands: int = 0
    retries: int = 0
    cooldown_seconds: float = 0.0
    buffered_commands_high_water: int = 0
    buffered_rows_high_water: int = 0
    active_references_high_water: int = 0
    violations: tuple[Violation, ...] = ()
    page_trace: tuple[PageRecord, ...] = ()
    page_trace_truncated: bool = False
    keyset_execution: KeysetExecutionReport | None = None

    def __post_init__(self) -> None:
        """Validate bounded terminal evidence."""
        if not isinstance(self.state, TerminalState):
            raise TypeError("state must be a TerminalState")
        if self.assurance is not None and not isinstance(self.assurance, TraversalAssurance):
            raise TypeError("assurance must be a TraversalAssurance or None")
        if not self.operation or not self.terminal_reason:
            raise ValueError("operation and terminal_reason must be non-empty")
        counters = (
            self.admitted,
            self.emitted,
            self.successes,
            self.failures,
            self.not_executed,
            self.unknown,
            self.unique_rows,
            self.physical_requests,
            self.logical_pages,
            self.batch_requests,
            self.batch_commands,
            self.retries,
            self.buffered_commands_high_water,
            self.buffered_rows_high_water,
            self.active_references_high_water,
        )
        if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in counters):
            raise ValueError("report counters must be non-negative integers")
        if not math.isfinite(self.cooldown_seconds) or self.cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must be finite and non-negative")
        object.__setattr__(self, "violations", tuple(self.violations))
        object.__setattr__(self, "page_trace", tuple(self.page_trace))
        if any(not isinstance(record, PageRecord) for record in self.page_trace):
            raise TypeError("page_trace must contain PageRecord values")
        if not isinstance(self.page_trace_truncated, bool):
            raise TypeError("page_trace_truncated must be a bool")
        if self.keyset_execution is not None and not isinstance(self.keyset_execution, KeysetExecutionReport):
            raise TypeError("keyset_execution must be a KeysetExecutionReport or None")
        if self.successful and any(item.severity is ViolationSeverity.BLOCKING for item in self.violations):
            raise ValueError("successful report cannot contain blocking violations")

    @property
    def successful(self) -> bool:
        """Whether the operation completed without negative outcomes."""
        return self.state is TerminalState.COMPLETED

    @property
    def exhausted(self) -> bool:
        """Whether the declared source was naturally exhausted."""
        return self.state in {TerminalState.COMPLETED, TerminalState.COMPLETED_WITH_FAILURES}

    @property
    def partial(self) -> bool:
        """Whether the operation ended before complete success/exhaustion."""
        return not self.exhausted


def retain_page_trace(records: tuple[PageRecord, ...], limit: int) -> tuple[tuple[PageRecord, ...], bool]:
    """Retain bounded page provenance with anomaly priority."""
    if limit <= 0:
        return (), bool(records)
    if len(records) <= limit:
        return records, False
    anomalies = tuple(record for record in records if record.outcome is not PageOutcome.COMMITTED)
    normal = tuple(record for record in records if record.outcome is PageOutcome.COMMITTED)
    if len(anomalies) >= limit:
        early = (limit + 1) // 2
        kept = (*anomalies[:early], *anomalies[-(limit - early) :]) if limit > early else anomalies[:early]
    else:
        remaining = limit - len(anomalies)
        early = min(len(normal), max(1, remaining // 4))
        recent_count = remaining - early
        recent = normal[-recent_count:] if recent_count else ()
        kept = (*anomalies, *normal[:early], *recent)
    return tuple(sorted(set(kept), key=lambda record: record.sequence)), True


__all__ = [
    "KeysetExecutionReport",
    "OperationReport",
    "PageDispatch",
    "PageOutcome",
    "PageRecord",
    "PageRejectionCode",
    "TerminalState",
    "TraversalAssurance",
    "Violation",
    "ViolationSeverity",
]
