"""Descending inclusive-timestamp windows: overlap delivered once, a stationary boundary fails, never nudged."""

from __future__ import annotations
import itertools
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import TYPE_CHECKING

from b24api.completion.closure import RESPONSE_FLAG_DECLARED_TERMINAL
from b24api.contracts.continuation import ContinuationReason, WindowRows
from b24api.contracts.page import PageView
from b24api.contracts.report import PageRejectionCode
from b24api.contracts.response import result_snapshot
from b24api.errors import CapabilityError, ContinuationError, PaginationError
from b24api.traversal.continuation import response_more
from b24api.traversal.identity import _PageRejectionError, _request_with_controls
from b24api.traversal.keyset_eligibility import _path_lookup
from b24api.traversal.strategy_context import PageStop, PageVerdict
from b24api.traversal.values import _extract_optional_path

if TYPE_CHECKING:
    from b24api.contracts.json import FrozenJson
    from b24api.contracts.request import ParameterPath, Request
    from b24api.contracts.response import Response
    from b24api.traversal.plans import InclusiveWindowPlan
    from b24api.traversal.strategy_context import StrategyContext
    from b24api.traversal.values import IdentityValue

# YYYY-MM-DDTHH:MM:SS[.F{1,6}](Z|+HH:MM|-HH:MM) with ASCII digits only; ranges are checked numerically.
_TIMESTAMP = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]{1,6}))?(Z|[+-][0-9]{2}:[0-9]{2})",
)
_MAXIMUM_OFFSET_HOURS = 23
_MAXIMUM_MINUTES = 59
_FRACTION_DIGITS = 6


@dataclass(frozen=True, slots=True)
class WindowBoundary:
    """One validated window boundary: its comparable UTC instant and the raw string sent back unchanged."""

    instant: datetime
    raw: str


def parse_window_timestamp(value: object) -> WindowBoundary:
    """Parse the exact window grammar to a UTC instant or raise the closed timestamp reason."""
    if not isinstance(value, str):
        raise ContinuationError(ContinuationReason.WINDOW_TIMESTAMP_INVALID, "window timestamp is not a string")
    match = _TIMESTAMP.fullmatch(value)
    if match is None:
        raise ContinuationError(ContinuationReason.WINDOW_TIMESTAMP_INVALID, "window timestamp has an invalid form")
    year, month, day, hour, minute, second, fraction, zone = match.groups()
    offset = timedelta(0)
    if zone != "Z":
        offset_hours, offset_minutes = int(zone[1:3]), int(zone[4:6])
        if offset_hours > _MAXIMUM_OFFSET_HOURS or offset_minutes > _MAXIMUM_MINUTES:
            raise ContinuationError(ContinuationReason.WINDOW_TIMESTAMP_INVALID, "window timestamp offset is invalid")
        offset = timedelta(hours=offset_hours, minutes=offset_minutes) * (-1 if zone[0] == "-" else 1)
    try:
        local = datetime(
            int(year),
            int(month),
            int(day),
            int(hour),
            int(minute),
            int(second),
            int((fraction or "0").ljust(_FRACTION_DIGITS, "0")),
            tzinfo=timezone(offset),
        )
        instant = local.astimezone(UTC)
    except (OverflowError, ValueError):
        raise ContinuationError(
            ContinuationReason.WINDOW_TIMESTAMP_INVALID,
            "window timestamp is outside the representable calendar",
        ) from None
    return WindowBoundary(instant, value)


def initial_window_boundary(request: Request, plan: InclusiveWindowPlan) -> WindowBoundary | None:
    """Return the caller's boundary; only a head may omit it, and the SDK never invents one."""
    if request.positional is not None:
        raise CapabilityError("inclusive window traversal requires named request parameters")
    present, raw = _path_lookup(request.copy_parameters(), plan.parameter_path)
    if not present:
        return None
    try:
        return parse_window_timestamp(raw)
    except ContinuationError as error:
        raise CapabilityError("window boundary control has an invalid timestamp") from error


@dataclass(frozen=True, slots=True)
class _JudgedWindow:
    terminal: bool
    outgoing: WindowBoundary | None


class InclusiveWindowStrategy:
    """Validate each classified window page in one transaction and move the boundary strictly down."""

    def __init__(self, plan: InclusiveWindowPlan) -> None:
        """Hold the plan; the incoming boundary is read from the request on the first request."""
        self._plan = plan
        self._incoming: WindowBoundary | None = None
        self._judged: _JudgedWindow | None = None

    def first_request(self, ctx: StrategyContext) -> Request:
        """Accept the caller's boundary when present (only a head may omit it) and send the head as written."""
        self._incoming = initial_window_boundary(ctx.request, self._plan)
        return self._request(ctx, None)

    def _request(self, ctx: StrategyContext, boundary: str | None) -> Request:
        plan = self._plan
        updates: dict[ParameterPath, object] = {}
        if boundary is not None:
            updates[plan.parameter_path] = boundary
        if plan.limit_path is not None and plan.requested_window_size is not None:
            updates[plan.limit_path] = plan.requested_window_size
        if not updates:
            return ctx.request
        return _request_with_controls(
            ctx.request,
            updates,
            allow_create=plan.allow_create_controls,
            replace=frozenset({plan.parameter_path}),
        )

    def judge(self, ctx: StrategyContext, response: Response, items: tuple[FrozenJson, ...]) -> PageVerdict:
        """Run the pre-commit window transaction and admit only first observations, in source order."""
        plan = self._plan
        source = ctx.source_page.current(items)
        if len(source) > plan.max_decoded_rows:
            raise PaginationError("window page exceeded its decoded row ceiling")
        identities = ctx.extract_identities(source)
        if len(set(identities)) != len(identities):
            raise _PageRejectionError("duplicate identity within a window page", PageRejectionCode.DUPLICATE_IDENTITY)
        paging = self._classified(PageView(result_snapshot(response), source))
        boundaries = [self._timestamp(source[index]) for index in paging]
        self._check_window(boundaries)
        more = response_more(response, plan.terminal)
        outgoing = boundaries[-1] if boundaries else None
        if more:
            self._check_progress(outgoing)
        self._judged = _JudgedWindow(terminal=not more, outgoing=outgoing)
        admitted = tuple(index for index, identity in enumerate(identities) if not ctx.identity_seen(identity))
        return PageVerdict(
            terminal=not more,
            identities=[identities[index] for index in admitted],
            admitted=admitted,
            deliver_empty=True,
        )

    def _classified(self, page: PageView) -> tuple[int, ...]:
        """Call the classifier once and require an exact partition; return paging indexes in source order."""
        try:
            rows = self._plan.classifier.classify(page)
        except Exception as error:
            raise ContinuationError(ContinuationReason.WINDOW_UNQUALIFIED, "window classifier raised") from error
        if not isinstance(rows, WindowRows):
            raise ContinuationError(
                ContinuationReason.WINDOW_CLASSIFICATION_INVALID,
                "window classifier did not return WindowRows",
            )
        indexes = (*rows.paging_indexes, *rows.supplemental_indexes)
        if any(type(index) is not int for index in indexes) or sorted(indexes) != list(range(len(page.items))):
            raise ContinuationError(
                ContinuationReason.WINDOW_CLASSIFICATION_INVALID,
                "window rows must split every page index exactly once",
            )
        limit = self._plan.requested_window_size
        if limit is not None and len(rows.paging_indexes) > limit:
            raise ContinuationError(
                ContinuationReason.WINDOW_CLASSIFICATION_INVALID,
                "window paging rows exceed the requested window size",
            )
        return tuple(sorted(rows.paging_indexes))

    def _timestamp(self, row: FrozenJson) -> WindowBoundary:
        value = _extract_optional_path(row, self._plan.timestamp_path)
        return parse_window_timestamp(value)

    def _check_window(self, boundaries: list[WindowBoundary]) -> None:
        """Require paging timestamps to be non-increasing and inside the inclusive incoming boundary."""
        for previous, current in itertools.pairwise(boundaries):
            if current.instant > previous.instant:
                raise ContinuationError(
                    ContinuationReason.WINDOW_CLASSIFICATION_INVALID,
                    "window paging rows are not in descending timestamp order",
                )
        incoming = self._incoming
        if incoming is not None and boundaries and boundaries[0].instant > incoming.instant:
            raise ContinuationError(
                ContinuationReason.WINDOW_OUTSIDE_BOUND,
                "window paging row is above the incoming boundary",
            )

    def _check_progress(self, outgoing: WindowBoundary | None) -> None:
        """A continuing page needs paging rows and a boundary strictly below the incoming one."""
        if outgoing is None:
            raise ContinuationError(
                ContinuationReason.WINDOW_UNQUALIFIED,
                "a continuing window page has no paging rows",
            )
        incoming = self._incoming
        if incoming is not None and outgoing.instant >= incoming.instant:
            # The next request would repeat this one: the tied cohort beyond the server cap is unreachable.
            raise ContinuationError(ContinuationReason.WINDOW_NONPROGRESS, "window boundary did not move")

    def advance(self, ctx: StrategyContext, identities: list[IdentityValue]) -> Request | PageStop:
        """Stop on the declared terminal page, else request the raw minimum paging timestamp."""
        del identities
        judged, self._judged = self._judged, None
        if judged is None:
            raise RuntimeError("window advanced without a judged page")
        if judged.terminal:
            return PageStop(RESPONSE_FLAG_DECLARED_TERMINAL)
        if judged.outgoing is None:
            raise RuntimeError("continuing window page lacks its outgoing boundary")
        self._incoming = judged.outgoing
        return self._request(ctx, judged.outgoing.raw)


__all__ = ["InclusiveWindowStrategy", "WindowBoundary", "initial_window_boundary", "parse_window_timestamp"]
