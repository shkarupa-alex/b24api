"""Immutable reports for explicit keyset capability verification."""

from __future__ import annotations
import dataclasses
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from b24api.contracts.json import FrozenJson, _freeze_json, _thaw_json
from b24api.contracts.report import PageRecord, Violation, ViolationSeverity

if TYPE_CHECKING:
    from collections.abc import Iterable

_EVIDENCE_LIMIT = 8


def _validate_report_metadata(report: KeysetCapabilityReport) -> None:
    if not isinstance(report.verdict, KeysetCapabilityVerdict):
        raise TypeError("verdict must be a KeysetCapabilityVerdict")
    if report.inconclusive_reason is not None and not isinstance(
        report.inconclusive_reason,
        KeysetInconclusiveReason,
    ):
        raise TypeError("inconclusive_reason must be a KeysetInconclusiveReason or None")
    for name in ("physical_requests", "batch_waves", "logical_commands"):
        value = getattr(report, name)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError(f"{name} must be a non-negative integer")
    if not isinstance(report.cross_digit_pair_exercised, bool) or not isinstance(report.page_trace_truncated, bool):
        raise TypeError("report flags must be booleans")


class KeysetCapabilityVerdict(StrEnum):
    """Semantic result of one explicit capability verification."""

    VERIFIED = "verified"
    UNSUPPORTED = "unsupported"
    INCONCLUSIVE = "inconclusive"


class KeysetCapabilityCheckName(StrEnum):
    """The five fixed strict-bound checks."""

    LOWER_EMPTY = "lower_empty"
    UPPER_EMPTY = "upper_empty"
    SINGLETON = "singleton"
    TWO_ROW_ASC = "two_row_asc"
    TWO_ROW_DESC = "two_row_desc"


class KeysetCapabilityCheckOutcome(StrEnum):
    """Closed observation classes for one capability check."""

    PASSED = "passed"
    OUT_OF_INTERVAL_ROWS = "out_of_interval_rows"
    ORDER_INVALID = "order_invalid"
    CAP_EXCEEDED = "cap_exceeded"
    SHAPE_INVALID = "shape_invalid"
    IN_INTERVAL_DRIFT = "in_interval_drift"
    CROSS_CANARY_CONTRADICTION = "cross_canary_contradiction"
    NOT_EXECUTED = "not_executed"


class KeysetInconclusiveReason(StrEnum):
    """Actionable reasons a verifier could not establish a verdict."""

    INSUFFICIENT_ROWS = "insufficient_rows"
    NO_USABLE_IDENTITY_PAIR = "no_usable_identity_pair"
    PAGE_CAP_TOO_SMALL = "page_cap_too_small"
    CONCURRENT_MUTATION = "concurrent_mutation"
    IN_RANGE_DRIFT = "in_range_drift"
    BOUND_OVER_RESTRICTIVE_SUSPECTED = "bound_over_restrictive_suspected"
    UNSTABLE_BOUNDARY = "unstable_boundary"


def _freeze_evidence(values: Iterable[object]) -> tuple[tuple[FrozenJson, ...], bool]:
    original = tuple(values)
    return tuple(_freeze_json(value) for value in original[:_EVIDENCE_LIMIT]), len(original) > _EVIDENCE_LIMIT


@dataclass(frozen=True, slots=True)
class MembershipRecheck:
    """One bounded exact-identity diagnostic wave."""

    identities: tuple[FrozenJson, ...]
    still_observed: tuple[FrozenJson, ...]
    no_longer_observed: tuple[FrozenJson, ...]
    contradictory: tuple[FrozenJson, ...] = ()
    truncated: bool = False

    def __post_init__(self) -> None:
        """Enforce the finite partition contract."""
        if not isinstance(self.truncated, bool):
            raise TypeError("truncated must be a boolean")
        truncated = self.truncated
        frozen: list[tuple[FrozenJson, ...]] = []
        for values in (self.identities, self.still_observed, self.no_longer_observed, self.contradictory):
            bounded, was_truncated = _freeze_evidence(values)
            frozen.append(bounded)
            truncated = truncated or was_truncated
        sent, still, gone, contradictions = (set(values) for values in frozen)
        if still & gone or still | gone != sent:
            raise ValueError("membership recheck must partition every sent identity")
        if not contradictions <= sent:
            raise ValueError("contradictory identities must be included in the recheck")
        object.__setattr__(self, "identities", frozen[0])
        object.__setattr__(self, "still_observed", frozen[1])
        object.__setattr__(self, "no_longer_observed", frozen[2])
        object.__setattr__(self, "contradictory", frozen[3])
        object.__setattr__(self, "truncated", truncated)


@dataclass(frozen=True, slots=True)
class KeysetCapabilityCheckResult:
    """Value-bounded evidence for one fixed capability check."""

    name: KeysetCapabilityCheckName
    outcome: KeysetCapabilityCheckOutcome
    rows_selected: int = 0
    out_of_interval_identities: tuple[FrozenJson, ...] = ()
    missing_in_interval_identities: tuple[FrozenJson, ...] = ()
    extra_in_interval_identities: tuple[FrozenJson, ...] = ()
    contradictory_identities: tuple[FrozenJson, ...] = ()
    recheck: MembershipRecheck | None = None

    def __post_init__(self) -> None:
        """Normalize bounded immutable evidence."""
        if not isinstance(self.name, KeysetCapabilityCheckName) or not isinstance(
            self.outcome,
            KeysetCapabilityCheckOutcome,
        ):
            raise TypeError("capability check fields must use their declared enum types")
        if not isinstance(self.rows_selected, int) or isinstance(self.rows_selected, bool) or self.rows_selected < 0:
            raise ValueError("rows_selected must be a non-negative integer")
        if self.recheck is not None and not isinstance(self.recheck, MembershipRecheck):
            raise TypeError("recheck must be a MembershipRecheck or None")
        for name in (
            "out_of_interval_identities",
            "missing_in_interval_identities",
            "extra_in_interval_identities",
            "contradictory_identities",
        ):
            frozen, _truncated = _freeze_evidence(getattr(self, name))
            object.__setattr__(self, name, frozen)


@dataclass(frozen=True, slots=True)
class KeysetCapabilityReport:
    """Complete immutable evidence for one explicit verifier run."""

    verdict: KeysetCapabilityVerdict
    checks: tuple[KeysetCapabilityCheckResult, ...]
    physical_requests: int
    batch_waves: int
    logical_commands: int
    cross_digit_pair_exercised: bool
    inconclusive_reason: KeysetInconclusiveReason | None
    inconclusive_detail: str | None
    violations: tuple[Violation, ...]
    page_trace: tuple[PageRecord, ...]
    page_trace_truncated: bool

    def __post_init__(self) -> None:
        """Validate verdict and fixed-check invariants."""
        _validate_report_metadata(self)
        object.__setattr__(self, "checks", tuple(self.checks))
        object.__setattr__(self, "violations", tuple(self.violations))
        object.__setattr__(self, "page_trace", tuple(self.page_trace))
        if any(not isinstance(check, KeysetCapabilityCheckResult) for check in self.checks):
            raise TypeError("checks must contain only KeysetCapabilityCheckResult values")
        if any(not isinstance(violation, Violation) for violation in self.violations):
            raise TypeError("violations must contain only Violation values")
        if any(not isinstance(record, PageRecord) for record in self.page_trace):
            raise TypeError("page_trace must contain only PageRecord values")
        if tuple(check.name for check in self.checks) != tuple(KeysetCapabilityCheckName):
            raise ValueError("checks must contain every capability check exactly once in enum order")
        proof = {
            KeysetCapabilityCheckOutcome.OUT_OF_INTERVAL_ROWS,
            KeysetCapabilityCheckOutcome.ORDER_INVALID,
            KeysetCapabilityCheckOutcome.CAP_EXCEEDED,
            KeysetCapabilityCheckOutcome.SHAPE_INVALID,
        }
        outcomes = {check.outcome for check in self.checks}
        if self.verdict is KeysetCapabilityVerdict.VERIFIED:
            blocking = any(violation.severity is ViolationSeverity.BLOCKING for violation in self.violations)
            if outcomes != {KeysetCapabilityCheckOutcome.PASSED} or self.inconclusive_reason is not None or blocking:
                raise ValueError("verified report requires five passed checks and no blocking violations")
        elif self.verdict is KeysetCapabilityVerdict.UNSUPPORTED:
            if not outcomes & proof or self.inconclusive_reason is not None:
                raise ValueError("unsupported report requires proof-class evidence")
        elif self.inconclusive_reason is None or outcomes & proof:
            raise ValueError("inconclusive report requires a reason and no proof-class evidence")

    def to_dict(self) -> dict[str, object]:
        """Return the public JSON-safe report representation."""
        return {
            "verdict": self.verdict,
            "checks": tuple(
                {
                    "name": check.name,
                    "outcome": check.outcome,
                    "rows_selected": check.rows_selected,
                    "out_of_interval_identities": tuple(
                        _thaw_json(value) for value in check.out_of_interval_identities
                    ),
                    "missing_in_interval_identities": tuple(
                        _thaw_json(value) for value in check.missing_in_interval_identities
                    ),
                    "extra_in_interval_identities": tuple(
                        _thaw_json(value) for value in check.extra_in_interval_identities
                    ),
                    "contradictory_identities": tuple(_thaw_json(value) for value in check.contradictory_identities),
                    "recheck": None
                    if check.recheck is None
                    else {
                        "identities": tuple(_thaw_json(value) for value in check.recheck.identities),
                        "still_observed": tuple(_thaw_json(value) for value in check.recheck.still_observed),
                        "no_longer_observed": tuple(_thaw_json(value) for value in check.recheck.no_longer_observed),
                        "contradictory": tuple(_thaw_json(value) for value in check.recheck.contradictory),
                        "truncated": check.recheck.truncated,
                    },
                }
                for check in self.checks
            ),
            "physical_requests": self.physical_requests,
            "batch_waves": self.batch_waves,
            "logical_commands": self.logical_commands,
            "cross_digit_pair_exercised": self.cross_digit_pair_exercised,
            "inconclusive_reason": self.inconclusive_reason,
            "inconclusive_detail": self.inconclusive_detail,
            "violations": tuple(violation.to_safe_dict() for violation in self.violations),
            "page_trace": tuple(dataclasses.asdict(record) for record in self.page_trace),
            "page_trace_truncated": self.page_trace_truncated,
        }


__all__ = [
    "KeysetCapabilityCheckName",
    "KeysetCapabilityCheckOutcome",
    "KeysetCapabilityCheckResult",
    "KeysetCapabilityReport",
    "KeysetCapabilityVerdict",
    "KeysetInconclusiveReason",
    "MembershipRecheck",
]
