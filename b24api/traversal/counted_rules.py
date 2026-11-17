"""Counted-traversal rules shared by the flat batched, reference and sequential paths.

Only contradictions the batched path already enforces, and which hold for any page width, live
here. Rules both paths already share through ``PaginationDriver`` (a missing or negative total, rows
beyond the total, total drift) stay in the driver, and prerequisites of parallel planning (a known
stride, fixed page length, request budgets) stay in the batched path, so the sequential path keeps
accepting variable-width pages it legitimately accepts today.
"""

from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum

from b24api.contracts.report import PageRejectionCode
from b24api.contracts.traversal import OffsetContinuation
from b24api.errors import CapabilityError, PaginationError

ENDED_BEFORE_TOTAL = "traversal terminated before its exact total"


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


@dataclass(frozen=True, slots=True)
class EmptyHeadFacts:
    """Pre-commit facts that decide whether one counted head may witness an empty source."""

    first_page: bool
    offset: int | None
    validated_rows: int
    expected_total: int | None
    fixed_step: bool
    qualified_total_required: bool
    source_rows: int
    adapted_rows: int
    total: int | None
    next: int | None


def empty_source_head_eligible(facts: EmptyHeadFacts) -> bool:
    """Return whether an unvalidated head may witness an empty source.

    Only the first page at offset zero of an exact-total traversal (counted or sequential) qualifies, with empty source and adapted rows, no
    usable total, no continuation, no fixed step, no earlier exact total and no caller-declared
    qualified-total confirmation. ``-1`` is the unknown-total sentinel and never counts as zero, and
    ``next=0`` is a continuation, not its absence.
    """
    return (
        not facts.fixed_step
        and not facts.qualified_total_required
        and facts.offset == 0
        and facts.first_page
        and facts.validated_rows == 0
        and facts.expected_total is None
        and facts.source_rows == 0
        and facts.adapted_rows == 0
        and facts.total in {None, -1}
        and facts.next is None
    )


@dataclass(frozen=True, slots=True)
class CountedHeadPlan:
    """The plan fields the counted head range rules read."""

    continuation: OffsetContinuation
    fixed_stride: int | None


def _head_stride(
    plan: CountedHeadPlan,
    *,
    total: int,
    next_offset: int | None,
    head_rows: int,
    page_size: int,
) -> int | None:
    """Return the tail width a head names: declared, the whole range, its continuation or its row count."""
    if plan.fixed_stride is not None:
        return plan.fixed_stride
    if total == head_rows:
        # The head holds the whole range: no tail is planned, so no continuation has to name its width.
        return max(page_size, head_rows)
    if plan.continuation is OffsetContinuation.SERVER_NEXT:
        if next_offset is None:
            # A server-next page without next ends the source, here short of its exact total.
            raise PaginationError(ENDED_BEFORE_TOTAL, rejection_code=PageRejectionCode.TOTAL_DRIFT)
        return next_offset
    return head_rows


def counted_head_range(
    plan: CountedHeadPlan,
    *,
    total: int | None,
    next_offset: int | None,
    head_rows: int,
    page_size: int,
) -> tuple[int, int]:
    """Return the exact total and stride a counted head qualifies for dense tails, or reject the head."""
    if total is None or total < 0:
        raise CapabilityError("parallel counted traversal requires a non-negative total")
    if total < head_rows:
        raise PaginationError(
            "parallel counted traversal observed total below the head page",
            rejection_code=PageRejectionCode.TOTAL_DRIFT,
        )
    stride = _head_stride(plan, total=total, next_offset=next_offset, head_rows=head_rows, page_size=page_size)
    if not isinstance(stride, int) or isinstance(stride, bool) or stride < 1:
        raise CapabilityError("parallel counted traversal requires a positive in-band stride")
    if head_rows != min(stride, total):
        raise PaginationError(
            "parallel counted head length contradicts the planned exact range",
            rejection_code=PageRejectionCode.RANGE_CONTRADICTION,
        )
    if (
        plan.continuation is OffsetContinuation.SERVER_NEXT
        and next_offset is not None
        and next_offset > 0
        and next_offset != stride
    ):
        raise PaginationError(
            "parallel counted head continuation contradicts its row count",
            rejection_code=PageRejectionCode.RANGE_CONTRADICTION,
        )
    verdict = judge_counted_page(CountedPageFacts(0, head_rows, 0, total, next_offset, plan.continuation))
    if verdict.contradiction is CountedContradiction.CONTINUATION_AFTER_TOTAL:
        raise PaginationError(
            "parallel counted traversal completed while continuation remained",
            rejection_code=PageRejectionCode.RANGE_CONTRADICTION,
        )
    return total, stride


def dense_width_proven(plan: CountedHeadPlan, *, total: int, stride: int, head_rows: int, page_size: int) -> bool:
    """Return whether a reference head proves the tail width as the flat dense plan does.

    A fixed stride is declared by the caller; otherwise the head must already close the range or
    carry one full declared page whose continuation, if any, agrees with it.
    """
    return plan.fixed_stride is not None or total <= head_rows or (head_rows == page_size and stride == page_size)


def check_tail_page(  # noqa: PLR0913 - the planned range of one tail page
    continuation: OffsetContinuation,
    *,
    rows: int,
    total_reported: int | None,
    next_reported: int | None,
    start: int,
    stride: int,
    total: int,
) -> None:
    """Reject a tail page whose rows, total or continuation contradict the planned exact range."""
    if rows != min(stride, total - start):
        raise PaginationError(
            "parallel counted page length contradicts the planned exact range",
            rejection_code=PageRejectionCode.RANGE_CONTRADICTION,
        )
    if total_reported not in {None, -1} and total_reported != total:
        raise PaginationError(
            "parallel counted page total contradicts the head total", rejection_code=PageRejectionCode.TOTAL_DRIFT
        )
    expected_next = expected_counted_next(start, stride, total)
    if continuation is OffsetContinuation.SERVER_NEXT and next_reported != expected_next:
        raise PaginationError(
            "parallel counted continuation contradicts the planned exact range",
            rejection_code=PageRejectionCode.RANGE_CONTRADICTION,
        )


__all__ = [
    "ENDED_BEFORE_TOTAL",
    "CountedContradiction",
    "CountedHeadPlan",
    "CountedPageFacts",
    "CountedVerdict",
    "EmptyHeadFacts",
    "check_tail_page",
    "counted_head_range",
    "dense_width_proven",
    "empty_source_head_eligible",
    "expected_counted_next",
    "judge_counted_page",
]
