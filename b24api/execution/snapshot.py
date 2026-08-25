"""Immutable public values shared by execution and evidence layers."""

from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING

from b24api.contracts.json import _is_plain_int
from b24api.contracts.policy import CompletionAssurance, KernelState, SnapshotState
from b24api.contracts.report import Violation, ViolationSeverity
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
    physical_requests: int = 0
    logical_pages: int = 0
    batch_requests: int = 0
    batch_commands: int = 0
    retries: int = 0
    cooldown_seconds: float = 0.0
    buffered_rows_high_water: int = 0
    violations: tuple[Violation, ...] = ()
    terminal_reason: str | None = None
    evidence: tuple[ResponseEvidence, ...] = ()

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        if not isinstance(self.state, KernelState):
            raise TypeError("state must be a KernelState")
        if not isinstance(self.assurance, CompletionAssurance):
            raise TypeError("assurance must be a CompletionAssurance")
        if not isinstance(self.snapshot, SnapshotState):
            raise TypeError("snapshot must be a SnapshotState")
        object.__setattr__(self, "violations", tuple(self.violations))
        object.__setattr__(self, "evidence", tuple(self.evidence))
        if self.terminal_reason is not None:
            object.__setattr__(self, "terminal_reason", DEFAULT_REDACTOR.redact_text(self.terminal_reason))
        counters = (
            self.emitted_rows,
            self.unique_rows,
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

    @property
    def completed(self) -> bool:
        """Return the completed."""
        return self.state is KernelState.COMPLETED
