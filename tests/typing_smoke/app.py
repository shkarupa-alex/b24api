"""A consumer module type-checked against the installed wheel, not the source tree.

The ``wheel-typing`` CI job copies this file into an empty directory, installs the built wheel
into a clean virtual environment and runs ``mypy --strict`` on it. The job requires the revealed
type below to be exactly ``b24api.contracts.request.Request`` and refuses any ``import-untyped``
report, which is what a wheel without ``b24api/py.typed`` produces. pytest never collects this
module: its name does not match the test file pattern.
"""

from typing import reveal_type

from b24api import Bitrix24, OffsetContinuation, OffsetSpec, ReplaySafety, Request, ResultSelector, RouteKind
from b24api.contracts import PageStride, ShortPageTermination, TraversalAssurance

reveal_type(Request("user.get", route=RouteKind.BARE))

BOOKING_PAGE = 50


async def declared_short_page_recipe(api: Bitrix24, date_from: str, date_to: str) -> bool:
    """The documented booking recipe (docs/recipes.md, "Declared short-page closure") type-checks."""
    booking_offset = OffsetSpec(
        continuation=OffsetContinuation.FIXED_STEP,
        step=BOOKING_PAGE,
        page_stride=PageStride(
            server_granularity=BOOKING_PAGE,
            wire_increment=BOOKING_PAGE,
            max_decoded_rows=BOOKING_PAGE,
        ),
        short_page_termination=ShortPageTermination.DECLARED_TERMINAL,
    )
    stream = api.iter_list(
        Request(
            "booking.v1.booking.list",
            {
                "filter": {"within": {"dateFrom": date_from, "dateTo": date_to}},
                "order": {"id": "ASC"},
            },
            replay_safety=ReplaySafety.UNKNOWN,
            route=RouteKind.BARE,
        ),
        selector=ResultSelector(("booking",)),
        page_size=BOOKING_PAGE,
        offset=booking_offset,
    )
    async with stream:
        async for _booking in stream:
            pass
    report = stream.report
    return (
        report is not None
        and report.successful
        and report.exhausted is True
        and report.assurance is TraversalAssurance.MECHANICS_ONLY
    )
