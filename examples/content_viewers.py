"""Scenario 18: direct JSON `params` object and two content-view result shapes.

Offline fixture: a readable extranet content ID has 10/3/empty `items` pages;
a readable ordinary content ID returns a user-ID mapping. An unknown ID gives
an empty success, which is not accepted as evidence of a complete fixture.
Physical batch query encoding is deliberately not selected for this method.
Run: `uv run python -m examples.content_viewers`.
"""

from __future__ import annotations
import asyncio

from b24api import (
    Bitrix24,
    OffsetSpec,
    PageIndex,
    ParameterPath,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    Settings,
)
from b24api.testing import ScriptedExchange, ScriptedTransport

METHOD = "socialnetwork.contentview.getlist"
EXPECTED_EXTRANET_IDS = tuple(range(1, 14))
EXPECTED_ORDINARY_IDS = (42,)
PAGE_SIZE = 10


def _request(content_id: str, page: int) -> Request:
    return Request(
        METHOD, {"params": {"contentId": content_id, "page": page}},
        replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE,
    )


def _fixture() -> ScriptedTransport:
    pages = ((1, EXPECTED_EXTRANET_IDS[:10]), (2, EXPECTED_EXTRANET_IDS[10:]), (3, ()))
    return ScriptedTransport((
        *(ScriptedExchange.json(
            _request("TASK-42", page),
            {"result": {"items": [{"ID": str(value)} for value in ids], "itemsCount": len(ids)}},
        ) for page, ids in pages),
        ScriptedExchange.json(_request("TASK-7", 1), {"result": {"42": {"ID": "42"}}}),
        ScriptedExchange.json(_request("TASK-999", 1), {"result": {"items": [], "itemsCount": 0}}),
    ))


async def run() -> None:
    """Verify nested wire shape and independent IDs for each readable mode."""
    transport = _fixture()
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    async with Bitrix24(settings, transport=transport) as client:
        path = ParameterPath(("params", "page"))
        stream = client.iter_list(
            _request("TASK-42", 1),
            selector=ResultSelector(("items",)),
            page_size=PAGE_SIZE,
            offset=OffsetSpec(parameter_path=path, page_index=PageIndex(path, max_rows=PAGE_SIZE)),
        )
        extranet = tuple([int(row["ID"]) async for row in stream])
        if extranet != EXPECTED_EXTRANET_IDS:
            raise AssertionError("scenario 18 paged extranet IDs differed from oracle")
        if stream.report is None or not stream.report.exhausted:
            raise AssertionError("scenario 18 paged content lacked exhaustion evidence")
        ordinary = await client.call(_request("TASK-7", 1))
        if not isinstance(ordinary, dict) or tuple(int(key) for key in ordinary) != EXPECTED_ORDINARY_IDS:
            raise AssertionError("scenario 18 ordinary mapping IDs differed from oracle")
        unknown = await client.call(_request("TASK-999", 1))
        if unknown != {"items": [], "itemsCount": 0}:
            raise AssertionError("scenario 18 unknown content did not return expected empty success")
    transport.assert_exhausted()
    if any(not isinstance(request.copy_parameters().get("params"), dict) for request in transport.calls):
        raise AssertionError("scenario 18 lost the required direct JSON params object")
    pages = tuple(request.copy_parameters()["params"]["page"] for request in transport.calls
                  if request.copy_parameters()["params"]["contentId"] == "TASK-42")
    if pages != (1, 2, 3):
        raise AssertionError("scenario 18 nested page index did not advance")


if __name__ == "__main__":
    asyncio.run(run())
