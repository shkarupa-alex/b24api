"""Pure range-window planning and closure evidence."""

from __future__ import annotations

from b24api.contracts.keyset_execution import ClosureWitness, KeysetPageCompletion
from b24api.traversal.keyset_fast_plan import LaneBounds, LaneSpec, plan_windows


def range_window_width(  # noqa: PLR0913
    *,
    completion: KeysetPageCompletion,
    page_cap: int,
    span: int,
    density_numerator: int,
    density_denominator: int,
    explicit: int | None,
) -> int:
    """Return the frozen integer-only width formula used by range planning."""
    if explicit is not None:
        return explicit
    if completion is KeysetPageCompletion.EMPTY_CONFIRMATION:
        return max(2, min(page_cap, span + 1))
    density_width = page_cap * density_denominator // max(1, density_numerator)
    return max(2, min(max(density_width, page_cap), span + 1))


def closure_witness(
    *,
    bounds: LaneBounds,
    cursor: int,
    identities: tuple[int, ...],
    page_cap: int,
    completion: KeysetPageCompletion,
) -> ClosureWitness | None:
    """Classify independently sufficient lane closure evidence."""
    if not identities:
        return ClosureWitness.EMPTY
    upper = bounds.upper_exclusive
    if upper is not None and identities[-1] == upper - 1:
        return ClosureWitness.TOP
    if upper is not None and identities == tuple(range(cursor + 1, upper)):
        return ClosureWitness.LATTICE_FULL
    if completion is KeysetPageCompletion.SHORT_PAGE_EXHAUSTS and len(identities) < page_cap:
        return ClosureWitness.SHORT_PAGE
    return None


__all__ = ["LaneSpec", "closure_witness", "plan_windows", "range_window_width"]
