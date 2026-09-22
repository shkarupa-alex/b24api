"""Scenario 3: im.recent.list offset pages can repeat a chat identity.

Frozen OFFSET/LIMIT source has five rows, including chat 20 on two adjacent
pages. The public traversal reports that duplicate; caller-keyed storage has
the independent four-chat oracle. `total=-1` is not used for closure. A
same-date LAST_MESSAGE_DATE boundary needs separate live qualification.
Run: `uv run python -m examples.recent_dialogs`.
"""

from __future__ import annotations
import asyncio

from b24api import (
    Bitrix24,
    ConsistencyPolicy,
    DuplicatePolicy,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    OffsetSpec,
    ParameterPath,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    Settings,
)
from b24api.testing import ScriptedExchange, ScriptedTransport

METHOD = "im.recent.list"
EXPECTED_IDS = (10, 20, 30, 40)
EMITTED_IDS = (10, 20, 20, 30, 40)
PAGE_SIZE = 2


def _chat_id(row: object) -> int:
    if not isinstance(row, dict):
        raise TypeError("recent dialog row must be an object")
    value = row.get("chat_id")
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise TypeError("chat_id must be a string or integer")
    return int(value)


def _request(offset: int) -> Request:
    return Request(
        METHOD, {"OFFSET": offset, "LIMIT": PAGE_SIZE},
        replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE,
    )


def _fixture() -> ScriptedTransport:
    pages = ((0, (10, 20), 2), (2, (20, 30), 4), (4, (40,), None), (5, (), None))
    return ScriptedTransport(tuple(
        ScriptedExchange.json(
            _request(offset),
            {
                "result": {"items": [{"chat_id": chat_id} for chat_id in ids]},
                "total": -1,
                **({"next": next_offset} if next_offset is not None else {}),
            },
        )
        for offset, ids, next_offset in pages
    ))


async def run() -> None:
    """Record a duplicate warning while reconciling chat identities."""
    transport = _fixture()
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    async with Bitrix24(settings, transport=transport) as client:
        stream = client.iter_list(
            _request(0),
            selector=ResultSelector(("items",)),
            identity=IdentitySpec(("chat_id",), "chat_id", "chat_id", IdentityCoercion.EXACT_INTEGER),
            page_size=PAGE_SIZE,
            offset=OffsetSpec(parameter_path=ParameterPath(("OFFSET",))),
            policy=ExecutionPolicy(consistency=ConsistencyPolicy(duplicate_policy=DuplicatePolicy.REPORT)),
        )
        rows = [row async for row in stream]
        ids = tuple(_chat_id(row) for row in rows)
        if ids != EMITTED_IDS:
            raise AssertionError("scenario 3 emitted rows differ from frozen source")
        if stream.report is None or not stream.report.exhausted:
            raise AssertionError("scenario 3 lacked empty-page closure")
        if stream.report.emitted != len(EMITTED_IDS) or stream.report.unique_rows != len(EXPECTED_IDS):
            raise AssertionError("scenario 3 report lost duplicate accounting")
        if not any(violation.code == "duplicate_identity" for violation in stream.report.violations):
            raise AssertionError("scenario 3 report omitted duplicate warning")
        if tuple(sorted({_chat_id(row) for row in rows})) != EXPECTED_IDS:
            raise AssertionError("scenario 3 keyed chat set differs from independent oracle")
    transport.assert_exhausted()
    if tuple(request.copy_parameters()["OFFSET"] for request in transport.calls) != (0, 2, 4, 5):
        raise AssertionError("scenario 3 did not follow envelope continuation")


if __name__ == "__main__":
    asyncio.run(run())
