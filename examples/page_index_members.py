"""Scenario 12: one-based `page` and `pageNum` controls with 10/3/0 rows.

Offline fixture for `socialnetwork.workgroup.getgridpopupmembers` and
`socialnetwork.workgroup.getlistincomingusers`. The expected 13 user IDs are
declared before traversal. Wire indexes advance 1/2/3 independently of the
decoded row count; live portal qualification remains separate. Run:
`uv run python -m examples.page_index_members`.
"""

from __future__ import annotations
import asyncio

from b24api import Bitrix24, OffsetSpec, PageIndex, ParameterPath, ReplaySafety, Request, RouteKind, Settings
from b24api.testing import ScriptedExchange, ScriptedTransport

METHODS = (
    ("socialnetwork.workgroup.getgridpopupmembers", "page"),
    ("socialnetwork.workgroup.getlistincomingusers", "pageNum"),
)
EXPECTED_IDS = tuple(range(1, 14))
PAGE_SIZE = 10


def _request(method: str, control: str, index: int) -> Request:
    return Request(method, {control: index}, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)


def _fixture() -> ScriptedTransport:
    exchanges = []
    for method, control in METHODS:
        for index, ids in ((1, EXPECTED_IDS[:10]), (2, EXPECTED_IDS[10:]), (3, ())):
            exchanges.append(ScriptedExchange.json(
                _request(method, control, index), {"result": [{"id": value} for value in ids]},
            ))
    return ScriptedTransport(tuple(exchanges))


async def run() -> None:
    """Verify exact wire controls and 13 IDs for both public traversals."""
    transport = _fixture()
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/offline/")
    async with Bitrix24(settings, transport=transport) as client:
        for method, control in METHODS:
            path = ParameterPath((control,))
            stream = client.iter_list(
                Request(method, route=RouteKind.BARE, replay_safety=ReplaySafety.SAFE),
                page_size=PAGE_SIZE,
                offset=OffsetSpec(
                    parameter_path=path,
                    page_index=PageIndex(path, initial=1, increment=1, max_rows=PAGE_SIZE),
                ),
            )
            observed = tuple([int(row["id"]) async for row in stream])
            if observed != EXPECTED_IDS:
                raise AssertionError("scenario 12 user IDs differed from independent oracle")
            if stream.report is None or not stream.report.exhausted:
                raise AssertionError("scenario 12 page-index traversal lacked exhaustion evidence")
            controls = tuple(
                request.copy_parameters()[control]
                for request in transport.calls if request.method == method
            )
            if controls != (1, 2, 3):
                raise AssertionError("scenario 12 wire page indexes did not advance 1/2/3")
    transport.assert_exhausted()


if __name__ == "__main__":
    asyncio.run(run())
