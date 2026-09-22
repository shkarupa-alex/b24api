"""Scenario 2: FIRST_ID omission versus qualified LAST_ID descending traversal.

Frozen source oracle has five message IDs. The synthetic FIRST_ID response
jumps over two middle IDs and then returns empty; clean HTTP alone cannot
certify completeness. Public LAST_ID traversal returns 5/5 at limits 1, 3,
and 50 with empty confirmation. This fixture records the observed method
hazard; it does not claim every page was captured live. Run:
`uv run python -m examples.message_cursor_direction`.
"""

from __future__ import annotations
import asyncio

from b24api import (
    Bitrix24,
    CursorDomain,
    CursorSpec,
    IdentityCoercion,
    ParameterPath,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    Settings,
)
from b24api.testing import ScriptedExchange, ScriptedTransport

METHOD = "im.dialog.messages.get"
EXPECTED_IDS = (3573, 3575, 12561, 29991, 30211)
ASC_IDS = (3573, 3575, 30211)
HEAD = 99999
LIMITS = (1, 3, 50)


def _request(control: str, cursor: int, limit: int) -> Request:
    return Request(
        METHOD,
        {"DIALOG_ID": "chat-1", control: cursor, "LIMIT": limit},
        replay_safety=ReplaySafety.SAFE,
        route=RouteKind.BARE,
    )


def _descending_fixture(limit: int) -> ScriptedTransport:
    remaining = tuple(reversed(EXPECTED_IDS))
    cursor = HEAD
    exchanges: list[ScriptedExchange] = []
    while True:
        page = tuple(value for value in remaining if value < cursor)[:limit]
        exchanges.append(
            ScriptedExchange.json(
                _request("LAST_ID", cursor, limit),
                {"result": {"messages": [{"id": value} for value in page]}},
            )
        )
        if not page:
            return ScriptedTransport(tuple(exchanges))
        cursor = page[-1]


async def run() -> None:
    """Compare a false clean ASC end with three complete DESC traversals."""
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    asc = ScriptedTransport(
        (
            ScriptedExchange.json(
                _request("FIRST_ID", 0, 3),
                {"result": {"messages": [{"id": value} for value in ASC_IDS]}},
            ),
            ScriptedExchange.json(_request("FIRST_ID", 30211, 3), {"result": {"messages": []}}),
        )
    )
    async with Bitrix24(settings, transport=asc) as client:
        first = await client.call(_request("FIRST_ID", 0, 3))
        second = await client.call(_request("FIRST_ID", 30211, 3))
        seen = tuple(int(item["id"]) for item in first["messages"] + second["messages"])
        if seen != ASC_IDS or set(seen) == set(EXPECTED_IDS):
            raise AssertionError("scenario 2 ASC fixture did not expose its false clean end")
    asc.assert_exhausted()
    for limit in LIMITS:
        transport = _descending_fixture(limit)
        async with Bitrix24(settings, transport=transport) as client:
            stream = client.iter_list_cursor(
                _request("LAST_ID", HEAD, limit),
                selector=ResultSelector(("messages",)),
                cursor=CursorSpec(
                    ParameterPath(("LAST_ID",)),
                    ("id",),
                    IdentityCoercion.EXACT_INTEGER,
                    "descending",
                    "last",
                    domain=CursorDomain.EXCLUSIVE_POSITIVE_INTEGER,
                    limit_path=ParameterPath(("LIMIT",)),
                ),
                page_size=limit,
            )
            observed = tuple([int(item["id"]) async for item in stream])
            if observed != tuple(reversed(EXPECTED_IDS)):
                raise AssertionError("scenario 2 LAST_ID omitted a source ID")
            if stream.report is None or not stream.report.exhausted:
                raise AssertionError("scenario 2 LAST_ID lacked empty confirmation")
        transport.assert_exhausted()


if __name__ == "__main__":
    asyncio.run(run())
