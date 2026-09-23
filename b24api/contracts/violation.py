"""Typed bounded operation violations with retained safe causes."""

from __future__ import annotations
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from enum import StrEnum
from typing import TYPE_CHECKING

from b24api.contracts.policy import ReplayDisposition
from b24api.redaction import DEFAULT_REDACTOR

if TYPE_CHECKING:
    from collections.abc import Iterable

    from b24api.errors import B24ApiError

VIOLATION_CODE_MAXIMUM = 100
VIOLATION_MESSAGE_MAXIMUM = 500
MAX_RETAINED_VIOLATIONS = 128
_TRUNCATED_CODE = "violations_truncated"
_MINIMUM_RETENTION_LIMIT = 2


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
    error: B24ApiError | None = dataclass_field(default=None, repr=False)
    replay_disposition: ReplayDisposition = ReplayDisposition.NOT_ELIGIBLE

    def __post_init__(self) -> None:
        """Redact and validate bounded diagnostic text."""
        if not isinstance(self.severity, ViolationSeverity):
            raise TypeError("severity must be a ViolationSeverity")
        object.__setattr__(self, "code", DEFAULT_REDACTOR.redact_text(self.code))
        object.__setattr__(self, "message", DEFAULT_REDACTOR.redact_text(self.message))
        if self.field is not None:
            object.__setattr__(self, "field", DEFAULT_REDACTOR.redact_text(self.field))
        if self.error is not None:
            from b24api.errors import B24ApiError  # noqa: PLC0415 - breaks the report/error import cycle

            if not isinstance(self.error, B24ApiError):
                raise TypeError("violation error must be a B24ApiError or None")
        if not isinstance(self.replay_disposition, ReplayDisposition):
            raise TypeError("replay_disposition must be a ReplayDisposition")
        if not self.code or len(self.code) > VIOLATION_CODE_MAXIMUM:
            raise ValueError("violation code must be 1..100 characters")
        if not self.message or len(self.message) > VIOLATION_MESSAGE_MAXIMUM:
            raise ValueError("violation message must be 1..500 characters")

    def to_safe_dict(self) -> dict[str, object]:
        """Return redacted structured cause and the closed replay decision."""
        return {
            "severity": self.severity.value,
            "code": self.code,
            "message": self.message,
            "field": self.field,
            "error": self.error.to_safe_dict() if self.error is not None else None,
            "replay_disposition": self.replay_disposition.value,
        }


def retain_violations(
    values: Iterable[Violation],
    *,
    limit: int = MAX_RETAINED_VIOLATIONS,
) -> tuple[Violation, ...]:
    """Retain bounded evidence while preserving late blocking causes."""
    items = tuple(values)
    if any(not isinstance(item, Violation) for item in items):
        raise TypeError("violation evidence must contain Violation values")
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < _MINIMUM_RETENTION_LIMIT:
        raise ValueError("violation retention limit must be at least two")
    truncated = any(item.code == _TRUNCATED_CODE for item in items)
    evidence = tuple(item for item in items if item.code != _TRUNCATED_CODE)
    if len(evidence) <= limit and not truncated:
        return evidence
    blocking = tuple(item for item in evidence if item.severity is ViolationSeverity.BLOCKING)
    warnings = tuple(item for item in evidence if item.severity is ViolationSeverity.WARNING)
    capacity = limit - 1
    retained_blocking = blocking[-capacity:]
    retained_warnings = warnings[: capacity - len(retained_blocking)]
    marker = Violation(
        ViolationSeverity.WARNING,
        _TRUNCATED_CODE,
        "additional operation violations were omitted by the evidence retention limit",
    )
    return (*retained_blocking, *retained_warnings, marker)


__all__ = ["Violation", "ViolationSeverity"]
