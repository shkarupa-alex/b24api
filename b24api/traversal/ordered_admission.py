"""Atomic global ordering, duplicate, and counter ownership for fast keysets."""

from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from b24api.errors import PaginationError
from b24api.traversal.keyset_fast_plan import LaneState, LaneStatus, LazyRangePlan
from b24api.traversal.page_validation import LaneReceipt

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.contracts.json import JsonValue
    from b24api.contracts.request import IdentitySpec


def drain_complete_lanes(  # noqa: PLR0913
    lane_index: int,
    lanes: list[LaneState],
    rows_by_lane: dict[int, list[JsonValue]],
    identities_by_lane: dict[int, list[int]],
    commands_by_lane: dict[int, list[tuple[str, int]]],
    admit: Callable[[LaneReceipt], None],
    trace_admit: Callable[[str, int], None],
    lazy_range: LazyRangePlan | None,
) -> int:
    """Drain only complete frontier lanes and advance lazy range geometry."""
    while lane_index < len(lanes):
        lane = lanes[lane_index]
        identities, rows, commands = (
            identities_by_lane[lane.spec.ordinal],
            rows_by_lane[lane.spec.ordinal],
            commands_by_lane[lane.spec.ordinal],
        )
        if rows or commands:
            admit(
                LaneReceipt(
                    lane_ordinal=lane.spec.ordinal,
                    command_id=f"body-admit-{lane.spec.ordinal}",
                    rows=tuple(rows),
                    identities=tuple(identities),
                    page_full=False,
                    last_identity=identities[-1] if identities else None,
                    witness=lane.witness,
                    warnings=(),
                ),
            )
            anchor = lane.spec.retained_upper_anchor if lane.status is LaneStatus.CLOSED else None
            for index, (command_id, count) in enumerate(commands):
                trace_admit(command_id, count + int(anchor is not None and index == len(commands) - 1))
            rows.clear()
            identities.clear()
            commands.clear()
        if lane.status is LaneStatus.OPEN:
            return lane_index
        lane_index += 1
        if lazy_range is not None:
            rows_by_lane.pop(lane.spec.ordinal, None)
            identities_by_lane.pop(lane.spec.ordinal, None)
            commands_by_lane.pop(lane.spec.ordinal, None)
            lanes.pop(0)
            lane_index = 0
            lazy_range.fill(len(lanes) + 1, lanes, rows_by_lane, identities_by_lane, commands_by_lane)
    return lane_index


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
        self._counters = FastCounters()

    @property
    def last_identity(self) -> int | None:
        """Return the last globally admitted identity."""
        return self._last

    def has_seen(self, identity: int) -> bool:
        """Return whether monotonic admission has already crossed an identity."""
        if self._last is None:
            return False
        return identity <= self._last if self.direction == "asc" else identity >= self._last

    def validate_and_commit(self, receipt: LaneReceipt) -> AdmissionCommit:
        """Atomically validate and commit an entire ordered receipt."""
        identities = receipt.identities
        if len(set(identities)) != len(identities):
            raise PaginationError("fast keyset traversal observed a duplicate identity")
        last = self._last
        for value in identities:
            if last is not None and self.direction == "asc" and value <= last:
                raise PaginationError("fast keyset global order did not advance")
            if last is not None and self.direction == "desc" and value >= last:
                raise PaginationError("descending fast keyset global order did not advance")
            last = value
        self._last = last
        self._counters.admitted_rows += len(receipt.rows)
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
        self._counters.unique_rows += count

    def snapshot_counters(self) -> FastCounters:
        """Return a detached mutable-counter snapshot."""
        return FastCounters(**{name: getattr(self._counters, name) for name in FastCounters.__dataclass_fields__})

    def assert_clean(self) -> None:
        """Validate counter relationships at terminal cleanup."""
        if self._counters.emitted_rows > self._counters.admitted_rows:
            raise RuntimeError("fast keyset emitted rows exceed admitted rows")

    def close(self) -> None:
        """Release identity state after terminal evidence has been frozen."""
        self._last = None


__all__ = ["AdmissionCommit", "FastCounters", "OrderedAdmissionState", "drain_complete_lanes"]
