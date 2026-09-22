"""Measured result returned by every offline recipe."""

from __future__ import annotations
from dataclasses import dataclass

from b24api import OperationReport


@dataclass(frozen=True, slots=True)
class RecipeEvidence:
    """Actual recipe result and its public terminal reports."""

    observed_count: int
    primary_report: OperationReport | None = None
    reports: tuple[OperationReport, ...] = ()

    def __post_init__(self) -> None:
        """Reject asserted or malformed evidence before the runner serializes it."""
        if not isinstance(self.observed_count, int) or isinstance(self.observed_count, bool) or self.observed_count < 0:
            raise ValueError("observed_count must be a non-negative integer")
        reports = tuple(self.reports)
        if self.primary_report is not None and not isinstance(self.primary_report, OperationReport):
            raise TypeError("primary_report must be an OperationReport or None")
        if any(not isinstance(report, OperationReport) for report in reports):
            raise TypeError("reports must contain only OperationReport values")
        if self.primary_report is not None and self.primary_report not in reports:
            reports = (*reports, self.primary_report)
        object.__setattr__(self, "reports", reports)

    @property
    def report_state(self) -> str:
        """Return the measured primary state or an explicit no-report marker."""
        return self.primary_report.state.value if self.primary_report is not None else "not_applicable"

    @property
    def assurance(self) -> str:
        """Return the measured assurance or an explicit absent marker."""
        if self.primary_report is None or self.primary_report.assurance is None:
            return "not_applicable"
        return self.primary_report.assurance.value

    def high_water(self, name: str) -> int:
        """Return the measured maximum bounded-resource counter."""
        return max((getattr(report, name) for report in self.reports), default=0)
