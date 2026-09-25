"""Counted-traversal contradiction rules shared by the batched and sequential paths.

Only contradictions the batched path already enforces, and which hold for any page width, live
here. Rules both paths already share through ``PaginationDriver`` (a missing or negative total, rows
beyond the total, total drift) stay in the driver, and prerequisites of parallel planning (a known
stride, fixed page length, request budgets) stay in the batched path, so the sequential path keeps
accepting variable-width pages it legitimately accepts today.
"""

from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum

from b24api.contracts.traversal import OffsetContinuation


def expected_counted_next(offset: int, stride: int, total: int) -> int | None:
    """Return the continuation a qualified fixed-stride page at ``offset`` must report."""
    following = offset + stride
    return following if following < total else None


@dataclass(frozen=True, slots=True)
class CountedPageFacts:
    """Observed facts of one counted page, counted in adapted rows as the batched path counts them."""

    offset: int
    rows: int
    accepted_before: int
    total: int | None
    next: int | None
    continuation: OffsetContinuation


class CountedContradiction(StrEnum):
    """A closed set of width-independent counted contradictions."""

    CONTINUATION_AFTER_TOTAL = "continuation_after_total"


@dataclass(frozen=True, slots=True)
class CountedVerdict:
    """Whether the page reaches the exact total, and the contradiction it carries, if any."""

    terminal: bool
    contradiction: CountedContradiction | None


def judge_counted_page(facts: CountedPageFacts) -> CountedVerdict:
    """Judge one counted page against the shared contradiction rules.

    R2: a page that reaches the exact total while a positive continuation remains is contradictory,
    except under ``FIXED_STEP``, whose plan ignores the server continuation. A terminal ``next`` of
    ``None`` or ``0`` is accepted, as on the batched head.
    """
    terminal = facts.total is not None and facts.accepted_before + facts.rows == facts.total
    contradiction = (
        CountedContradiction.CONTINUATION_AFTER_TOTAL
        if terminal
        and facts.next is not None
        and facts.next > 0
        and facts.continuation is not OffsetContinuation.FIXED_STEP
        else None
    )
    return CountedVerdict(terminal, contradiction)


__all__ = [
    "CountedContradiction",
    "CountedPageFacts",
    "CountedVerdict",
    "expected_counted_next",
    "judge_counted_page",
]
