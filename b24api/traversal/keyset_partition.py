"""Pure occupied-anchor guess and lane planning."""

from __future__ import annotations

from b24api.traversal.keyset_fast_plan import LaneSpec, plan_lanes_from_anchors

_MINIMUM_LANES = 2


def anchor_guesses(*, lo: int, upper_exclusive: int, target_lanes: int) -> tuple[int, ...]:
    """Return the exact deduplicated integer-floor anchor guesses."""
    if any(not isinstance(value, int) or isinstance(value, bool) for value in (lo, upper_exclusive, target_lanes)):
        raise TypeError("anchor guess operands must be exact integers")
    if target_lanes < _MINIMUM_LANES:
        raise ValueError("target_lanes must be at least 2")
    span = max(0, upper_exclusive - lo - 1)
    return tuple(sorted({lo + (index * span) // target_lanes for index in range(target_lanes)}))


def normalize_anchors(*, lo: int, upper_exclusive: int, anchors: tuple[int, ...]) -> tuple[int, ...]:
    """Validate and normalize occupied anchors inside the captured fence."""
    if any(not isinstance(value, int) or isinstance(value, bool) for value in anchors):
        raise TypeError("anchors must be exact integers")
    return tuple(sorted({value for value in anchors if lo < value < upper_exclusive}))


__all__ = ["LaneSpec", "anchor_guesses", "normalize_anchors", "plan_lanes_from_anchors"]
