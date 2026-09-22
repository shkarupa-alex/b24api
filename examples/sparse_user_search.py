"""Scenario 11: sparse raw SQL offset continues after an empty selected page.

Offline fixture for `im.search.user`: mapping values at raw offsets 0 and 100,
no visible users at 50, qualified raw total 150 and stable raw order. The
independent expected IDs are fixed before traversal. This does not prove that
all live portals expose a stable raw bound. Run:
`uv run python -m examples.sparse_user_search`.
"""

from __future__ import annotations
import asyncio

from b24api import (
    Bitrix24,
    OffsetContinuation,
    OffsetSpec,
    PageStride,
    ReplaySafety,
    Request,
    ResultCollectionShape,
    ResultSelector,
    RouteKind,
    Settings,
    SparseRawBound,
    TraversalAssurance,
)
from b24api.testing import ScriptedExchange, ScriptedTransport

METHOD = "im.search.user"
EXPECTED_IDS = (17, 83)
RAW_TOTAL = 150
STRIDE = 50


def _request(offset: int) -> Request:
    return Request(METHOD, {"start": offset}, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)


def _fixture() -> ScriptedTransport:
    pages = (
        (0, {"17": {"id": 17}}),
        (50, {}),
        (100, {"83": {"id": 83}}),
    )
    return ScriptedTransport(
        tuple(
            ScriptedExchange.json(_request(offset), {"result": {"items": users, "rawTotal": RAW_TOTAL}})
            for offset, users in pages
        )
    )


async def run() -> None:
    """Verify the selected-empty middle page does not terminate traversal."""
    transport = _fixture()
    stride = PageStride(server_granularity=STRIDE, wire_increment=STRIDE, max_decoded_rows=STRIDE)
    offset = OffsetSpec(
        continuation=OffsetContinuation.FIXED_STEP,
        step=STRIDE,
        page_stride=stride,
        sparse_raw_bound=SparseRawBound(
            ResultSelector(("rawTotal",)),
            stride,
            3,
            "qualified stable raw ID order",
        ),
    )
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    async with Bitrix24(settings, transport=transport) as client:
        stream = client.iter_list(
            Request(METHOD, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
            selector=ResultSelector(("items",)),
            collection_shape=ResultCollectionShape.MAPPING_VALUES,
            page_size=STRIDE,
            offset=offset,
        )
        observed = tuple([int(row["id"]) async for row in stream])
        if observed != EXPECTED_IDS:
            raise AssertionError("scenario 11 omitted the visible user after a sparse empty page")
        if stream.report is None or not stream.report.exhausted:
            raise AssertionError("scenario 11 lacked raw-bound exhaustion evidence")
        if stream.report.assurance is not TraversalAssurance.RAW_RANGE_COVERED:
            raise AssertionError("scenario 11 claimed the wrong assurance")
    transport.assert_exhausted()
    if tuple(request.copy_parameters()["start"] for request in transport.calls) != (0, 50, 100):
        raise AssertionError("scenario 11 did not cover every raw offset window")


if __name__ == "__main__":
    asyncio.run(run())
