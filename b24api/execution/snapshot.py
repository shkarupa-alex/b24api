"""Immutable public values shared by execution and evidence layers."""

from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING

from b24api.contracts.completion import EmptySourceWitness
from b24api.contracts.json import _is_plain_int
from b24api.contracts.policy import CompletionAssurance, KernelState, SnapshotState
from b24api.contracts.report import KeysetExecutionReport, PageRecord, Violation, ViolationSeverity
from b24api.redaction import DEFAULT_REDACTOR

if TYPE_CHECKING:
    from b24api.contracts.response import ResponseEvidence


@dataclass(frozen=True, slots=True)
class KernelReport:
    """Immutable terminal report snapshot."""

    state: KernelState = KernelState.NOT_STARTED
    assurance: CompletionAssurance = CompletionAssurance.CALLER_ASSERTED
    snapshot: SnapshotState = SnapshotState.NOT_REQUESTED
    plan_id: str | None = None
    dispatch_id: str | None = None
    emitted_rows: int = 0
    unique_rows: int = 0
    duplicate_identities: int = 0
    physical_requests: int = 0
    logical_pages: int = 0
    batch_requests: int = 0
    batch_commands: int = 0
    retries: int = 0
    cooldown_seconds: float = 0.0
    buffered_rows_high_water: int = 0
    violations: tuple[Violation, ...] = ()
    terminal_reason: str | None = None
    caller_stopped: bool = False
    evidence: tuple[ResponseEvidence, ...] = ()
    page_trace: tuple[PageRecord, ...] = ()
    page_trace_truncated: bool = False
    keyset_execution: KeysetExecutionReport | None = None
    empty_source_witness: EmptySourceWitness | None = None

    def __post_init__(self) -> None:  # noqa: C901 - one flat validation of every report field
        """Validate and normalize instance state."""
        if not isinstance(self.state, KernelState):
            raise TypeError("state must be a KernelState")
        if not isinstance(self.assurance, CompletionAssurance):
            raise TypeError("assurance must be a CompletionAssurance")
        if not isinstance(self.snapshot, SnapshotState):
            raise TypeError("snapshot must be a SnapshotState")
        object.__setattr__(self, "violations", tuple(self.violations))
        object.__setattr__(self, "evidence", tuple(self.evidence))
        object.__setattr__(self, "page_trace", tuple(self.page_trace))
        if self.terminal_reason is not None:
            object.__setattr__(self, "terminal_reason", DEFAULT_REDACTOR.redact_text(self.terminal_reason))
        if not isinstance(self.caller_stopped, bool):
            raise TypeError("caller_stopped must be a bool")
        counters = (
            self.emitted_rows,
            self.unique_rows,
            self.duplicate_identities,
            self.physical_requests,
            self.logical_pages,
            self.batch_requests,
            self.batch_commands,
            self.retries,
            self.buffered_rows_high_water,
        )
        if any(not _is_plain_int(value) or value < 0 for value in counters) or self.cooldown_seconds < 0:
            raise ValueError("report counters must be non-negative")
        if self.unique_rows > self.emitted_rows:
            raise ValueError("unique_rows cannot exceed emitted_rows")
        if self.completed and any(item.severity is ViolationSeverity.BLOCKING for item in self.violations):
            raise ValueError("completed report cannot contain blocking violations")
        if self.keyset_execution is not None and not isinstance(self.keyset_execution, KeysetExecutionReport):
            raise TypeError("keyset_execution must be a KeysetExecutionReport or None")
        if self.empty_source_witness is not None and (
            not isinstance(self.empty_source_witness, EmptySourceWitness) or not self.completed or self.emitted_rows
        ):
            raise ValueError("an empty-source witness requires a completed report without rows")

    @property
    def completed(self) -> bool:
        """Return the completed."""
        return self.state is KernelState.COMPLETED
