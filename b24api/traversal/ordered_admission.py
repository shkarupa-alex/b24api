"""Atomic global ordering, duplicate, and counter ownership for fast keysets."""

from __future__ import annotations
import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from b24api.errors import PaginationError

if TYPE_CHECKING:
    from b24api.contracts.json import JsonValue
    from b24api.contracts.request import IdentitySpec
    from b24api.traversal.page_validation import LaneReceipt


@dataclass(frozen=True, slots=True)
class AdmissionCommit:
    """Rows atomically accepted into global output order."""

    rows: tuple[JsonValue, ...]
    identities: tuple[int, ...]
    unique_rows: int


@dataclass(slots=True)
class FastCounters:
    """Fast-path row provenance counters."""

    raw_rows: int = 0
    admitted_rows: int = 0
    emitted_rows: int = 0
    unique_rows: int = 0
    probe_rows_discarded: int = 0
    boundary_overlap_rows: int = 0


class OrderedAdmissionState:
    """Sole atomic owner of global traversal identity state."""

    def __init__(self, *, direction: Literal["asc", "desc"], identity: IdentitySpec) -> None:
        """Initialize isolated admission state."""
        self.direction = direction
        self.identity = identity
        self._last: int | None = None
        self._identities: set[int] = set()
        self._fingerprints: set[str] = set()
        self._counters = FastCounters()

    @property
    def last_identity(self) -> int | None:
        """Return the last globally admitted identity."""
        return self._last

    @property
    def seen_identities(self) -> frozenset[int]:
        """Return an immutable identity snapshot for boundary normalization."""
        return frozenset(self._identities)

    def validate_and_commit(self, receipt: LaneReceipt) -> AdmissionCommit:
        """Atomically validate and commit an entire ordered receipt."""
        identities = receipt.identities
        if len(set(identities)) != len(identities) or any(value in self._identities for value in identities):
            raise PaginationError("fast keyset traversal observed a duplicate identity")
        last = self._last
        for value in identities:
            if last is not None and self.direction == "asc" and value <= last:
                raise PaginationError("fast keyset global order did not advance")
            if last is not None and self.direction == "desc" and value >= last:
                raise PaginationError("descending fast keyset global order did not advance")
            last = value
        fingerprints = tuple(
            hashlib.sha256(
                json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(),
            ).hexdigest()
            for row in receipt.rows
        )
        if any(value in self._fingerprints for value in fingerprints):
            raise PaginationError("fast keyset traversal repeated an admitted row fingerprint")
        self._last = last
        self._identities.update(identities)
        self._fingerprints.update(fingerprints)
        self._counters.admitted_rows += len(receipt.rows)
        self._counters.unique_rows += len(receipt.rows)
        return AdmissionCommit(receipt.rows, identities, len(receipt.rows))

    def record_raw(self, count: int, *, discarded: bool = False) -> None:
        """Record validated reads that may not own output."""
        self._counters.raw_rows += count
        if discarded:
            self._counters.probe_rows_discarded += count

    def record_discarded(self, count: int) -> None:
        """Record already-counted raw rows that do not own output."""
        if count < 0:
            raise ValueError("discarded count cannot be negative")
        self._counters.probe_rows_discarded += count

    def record_boundary_overlap(self, count: int) -> None:
        """Record boundary rows normalized out before admission."""
        self._counters.boundary_overlap_rows += count

    def mark_emitted(self, count: int) -> None:
        """Record consumer-visible output delivery."""
        if count < 0:
            raise ValueError("emitted count cannot be negative")
        self._counters.emitted_rows += count

    def snapshot_counters(self) -> FastCounters:
        """Return a detached mutable-counter snapshot."""
        return FastCounters(**{name: getattr(self._counters, name) for name in FastCounters.__dataclass_fields__})

    def assert_clean(self) -> None:
        """Validate counter relationships at terminal cleanup."""
        if self._counters.emitted_rows > self._counters.admitted_rows:
            raise RuntimeError("fast keyset emitted rows exceed admitted rows")


__all__ = ["AdmissionCommit", "FastCounters", "OrderedAdmissionState"]
