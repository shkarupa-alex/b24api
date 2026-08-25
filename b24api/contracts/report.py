"""Immutable public operation terminal evidence."""

from __future__ import annotations
import math
from dataclasses import dataclass
from enum import StrEnum

from b24api.models import Violation, ViolationSeverity


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


__all__ = ["OperationReport", "TerminalState", "TraversalAssurance"]
