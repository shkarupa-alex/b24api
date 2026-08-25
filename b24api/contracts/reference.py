"""Reference binding contracts and correlated terminal events."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from b24api.models import FrozenJson, JsonValue, ParameterPath, _freeze_json, _thaw_json
from b24api.redaction import DEFAULT_REDACTOR

if TYPE_CHECKING:
    from collections.abc import Iterable

    from b24api.contracts.command import NotExecutedReason
    from b24api.error import B24ApiError

_SUMMARY_MAXIMUM = 256


@dataclass(frozen=True, slots=True, init=False)
class ParameterUpdate:
    """One exact immutable parameter-leaf replacement."""

    path: ParameterPath
    _value: FrozenJson = field(repr=False)

    def __init__(self, path: ParameterPath, value: object) -> None:
        """Freeze a JSON update value."""
        if not isinstance(path, ParameterPath):
            raise TypeError("path must be a ParameterPath")
        object.__setattr__(self, "path", path)
        object.__setattr__(self, "_value", _freeze_json(value))

    @property
    def value(self) -> JsonValue:
        """Return a detached update value."""
        return _thaw_json(self._value)


@dataclass(frozen=True, slots=True)
class Binding[C]:
    """One bound traversal input with opaque off-wire correlation."""

    summary: str
    updates: tuple[ParameterUpdate, ...]
    correlation: C = field(repr=False)

    def __post_init__(self) -> None:
        """Bound and redact diagnostics without touching correlation."""
        if not isinstance(self.summary, str):
            raise TypeError("summary must be a string")
        safe = DEFAULT_REDACTOR.redact_text(self.summary)
        if not safe or len(safe) > _SUMMARY_MAXIMUM:
            raise ValueError("summary must contain 1..256 code points")
        updates = tuple(self.updates)
        if any(not isinstance(update, ParameterUpdate) for update in updates):
            raise TypeError("updates must contain only ParameterUpdate values")
        normalized = [
            tuple(part.casefold() if isinstance(part, str) else part for part in update.path.path) for update in updates
        ]
        for index, left in enumerate(normalized):
            for right in normalized[index + 1 :]:
                shared = min(len(left), len(right))
                if left[:shared] == right[:shared]:
                    raise ValueError("binding update paths must be distinct and non-overlapping")
        object.__setattr__(self, "summary", safe)
        object.__setattr__(self, "updates", updates)


@dataclass(frozen=True, slots=True)
class ReferenceItem[C]:
    """One item delivered for a binding."""

    binding_index: int
    correlation: C = field(repr=False)
    item_index: int
    item: JsonValue = field(repr=False)


@dataclass(frozen=True, slots=True)
class ReferenceComplete[C]:
    """Successful binding terminal event, including zero-row results."""

    binding_index: int
    correlation: C = field(repr=False)
    row_count: int


@dataclass(frozen=True, slots=True)
class ReferenceFailure[C]:
    """Conclusive or fail-closed failure for one binding."""

    binding_index: int
    correlation: C = field(repr=False)
    error: B24ApiError
    partial_rows: int


@dataclass(frozen=True, slots=True)
class ReferenceNotExecuted[C]:
    """A binding proven not to have executed."""

    binding_index: int
    correlation: C = field(repr=False)
    reason: NotExecutedReason


@dataclass(frozen=True, slots=True)
class ReferenceOutcomeUnknown[C]:
    """A binding whose server acceptance cannot be excluded."""

    binding_index: int
    correlation: C = field(repr=False)
    error: B24ApiError
    partial_rows: int


type ReferenceEvent[C] = ReferenceItem[C] | ReferenceComplete[C]
type ReferenceOutcome[C] = (
    ReferenceItem[C] | ReferenceComplete[C] | ReferenceFailure[C] | ReferenceNotExecuted[C] | ReferenceOutcomeUnknown[C]
)


@dataclass(frozen=True, slots=True)
class ReferenceOutcomeBuckets[C]:
    """Finite pure partition of every reference outcome variant."""

    items: tuple[ReferenceItem[C], ...]
    completions: tuple[ReferenceComplete[C], ...]
    failures: tuple[ReferenceFailure[C], ...]
    not_executed: tuple[ReferenceNotExecuted[C], ...]
    unknown: tuple[ReferenceOutcomeUnknown[C], ...]


def partition_reference_outcomes[C](outcomes: Iterable[ReferenceOutcome[C]]) -> ReferenceOutcomeBuckets[C]:
    """Partition a finite iterable without discarding terminal states."""
    buckets: tuple[list[object], ...] = ([], [], [], [], [])
    for outcome in outcomes:
        if isinstance(outcome, ReferenceItem):
            buckets[0].append(outcome)
        elif isinstance(outcome, ReferenceComplete):
            buckets[1].append(outcome)
        elif isinstance(outcome, ReferenceFailure):
            buckets[2].append(outcome)
        elif isinstance(outcome, ReferenceNotExecuted):
            buckets[3].append(outcome)
        elif isinstance(outcome, ReferenceOutcomeUnknown):
            buckets[4].append(outcome)
        else:
            raise TypeError("outcomes must contain only ReferenceOutcome values")
    return ReferenceOutcomeBuckets(
        cast("tuple[ReferenceItem[C], ...]", tuple(buckets[0])),
        cast("tuple[ReferenceComplete[C], ...]", tuple(buckets[1])),
        cast("tuple[ReferenceFailure[C], ...]", tuple(buckets[2])),
        cast("tuple[ReferenceNotExecuted[C], ...]", tuple(buckets[3])),
        cast("tuple[ReferenceOutcomeUnknown[C], ...]", tuple(buckets[4])),
    )


__all__ = [
    "Binding",
    "ParameterUpdate",
    "ReferenceComplete",
    "ReferenceEvent",
    "ReferenceFailure",
    "ReferenceItem",
    "ReferenceNotExecuted",
    "ReferenceOutcome",
    "ReferenceOutcomeBuckets",
    "ReferenceOutcomeUnknown",
    "partition_reference_outcomes",
]
