"""Scenario 13: five-slot task-scoped positional elapsed-item traversal.

Offline fixture: task 42 has 53 elapsed items in pages of 50/3/empty.
`task.elapseditem.getlist` receives exactly `taskId,order,filter,select,params`
as top-level JSON slots; page control is `NAV_PARAMS.iNumPage`, not `start`.
Each row's task scope is checked independently of HTTP success and total.
Direct JSON only; physical batch/form encoding is not qualified. Run:
`uv run python -m examples.elapsed_task_items`.
"""

from __future__ import annotations
import asyncio

from b24api import (
    Bitrix24,
    EmptyArray,
    EmptyObject,
    OffsetSpec,
    PageIndex,
    ParameterPath,
    PositionalArguments,
    PositionalLayout,
    Present,
    ReplaySafety,
    Request,
    RouteKind,
    Settings,
    SlotContract,
    SlotShape,
)
from b24api.testing import ScriptedExchange, ScriptedTransport

METHOD = "task.elapseditem.getlist"
TASK_ID = 42
PAGE_SIZE = 50
EXPECTED_IDS = tuple(range(1, 54))
EXPECTED_PAGES = (1, 2, 3)
SLOT_COUNT = 5
CONTROL = ParameterPath((4, "NAV_PARAMS", "iNumPage"))
LAYOUT = PositionalLayout(
    "task.elapseditem.getlist.five.v1",
    (
        SlotContract("taskId", SlotShape.SCALAR, fixed=True),
        SlotContract("order", SlotShape.OBJECT),
        SlotContract("filter", SlotShape.OBJECT),
        SlotContract("select", SlotShape.ARRAY),
        SlotContract("params", SlotShape.OBJECT),
    ),
    control_paths=frozenset({CONTROL.path}),
)


def _request(page: int) -> Request:
    arguments = PositionalArguments(
        (
            Present(TASK_ID), EmptyObject(), EmptyObject(), EmptyArray(),
            Present({"NAV_PARAMS": {"nPageSize": PAGE_SIZE, "iNumPage": page}}),
        ),
        LAYOUT.layout_id, layout=LAYOUT,
    )
    return Request(METHOD, arguments, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)


def _fixture() -> ScriptedTransport:
    pages = ((1, EXPECTED_IDS[:50]), (2, EXPECTED_IDS[50:]), (3, ()))
    return ScriptedTransport(tuple(
        ScriptedExchange.json(
            _request(page), {"result": [{"id": identity, "taskId": TASK_ID} for identity in ids], "total": 53},
        )
        for page, ids in pages
    ))


async def run() -> None:
    """Check exact slot order, task scope, IDs, and page index progression."""
    transport = _fixture()
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/offline/")
    async with Bitrix24(settings, transport=transport) as client:
        stream = client.iter_list(
            _request(1),
            page_size=PAGE_SIZE,
            offset=OffsetSpec(
                parameter_path=CONTROL,
                page_index=PageIndex(CONTROL, initial=1, increment=1, max_rows=PAGE_SIZE),
            ),
        )
        rows = [row async for row in stream]
        if tuple(int(row["id"]) for row in rows) != EXPECTED_IDS:
            raise AssertionError("scenario 13 elapsed IDs differed from independent oracle")
        if any(int(row["taskId"]) != TASK_ID for row in rows):
            raise AssertionError("scenario 13 lost the task scope despite successful HTTP")
        if stream.report is None or not stream.report.exhausted:
            raise AssertionError("scenario 13 positional traversal did not prove exhaustion")
    transport.assert_exhausted()
    slots = tuple(request.positional.to_wire_slots() for request in transport.calls if request.positional is not None)
    if len(slots) != len(EXPECTED_PAGES) or any(len(page) != SLOT_COUNT or page[0] != TASK_ID for page in slots):
        raise AssertionError("scenario 13 changed the five-slot task-scoped ABI")
    if tuple(page[4]["NAV_PARAMS"]["iNumPage"] for page in slots) != EXPECTED_PAGES:
        raise AssertionError("scenario 13 did not advance the positional page index")


if __name__ == "__main__":
    asyncio.run(run())
