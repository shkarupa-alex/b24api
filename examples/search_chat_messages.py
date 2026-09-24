"""Scenario 4: correlated message search across readable, empty, denied chats.

Frozen fixture qualifies `CHAT_ID`, date window, and descending LAST_ID only
for the offline source. One chat has two nonempty pages, one ends empty, and
one yields typed ACCESS_ERROR. Run: `uv run python -m examples.search_chat_messages`.
"""

from __future__ import annotations
import asyncio

from b24api import (
    ApiResponseError,
    Binding,
    Bitrix24,
    CursorSpec,
    CursorTraversal,
    DirectDispatch,
    IdentityCoercion,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    Settings,
    TerminalState,
)
from b24api.contracts import CursorDomain, ReferenceComplete, ReferenceFailure, ReferenceItem
from b24api.testing import ScriptedExchange, ScriptedTransport
from examples._support.evidence import RecipeEvidence

METHOD = "im.dialog.messages.search"
START = "2026-09-01T00:00:00+03:00"
END = "2026-09-22T23:59:59+03:00"
EXPECTED_IDS = {101: (10009, 10008, 3), 102: ()}


def _message_id(row: object) -> int:
    if not isinstance(row, dict):
        raise TypeError("message row must be an object")
    value = row.get("id")
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise TypeError("message id must be a string or integer")
    return int(value)


def _request(chat_id: int, cursor: int | None) -> Request:
    params = {
        "CHAT_ID": chat_id,
        "DATE_FROM": START,
        "DATE_TO": END,
        "ORDER": {"ID": "DESC"},
        "LIMIT": 2,
    }
    if cursor is not None:
        params["LAST_ID"] = cursor
    return Request(
        METHOD,
        params,
        replay_safety=ReplaySafety.SAFE,
        route=RouteKind.BARE,
    )


def _fixture() -> ScriptedTransport:
    pages = ((101, None, (10009, 10008)), (101, 10008, (3,)), (101, 3, ()), (102, None, ()))
    exchanges = [
        ScriptedExchange.json(
            _request(chat_id, cursor),
            {"result": {"messages": [{"id": value, "chat_id": chat_id} for value in ids]}},
        )
        for chat_id, cursor, ids in pages
    ]
    exchanges.append(
        ScriptedExchange.json(
            _request(103, None),
            {"error": "ACCESS_ERROR", "error_description": "chat denied"},
        )
    )
    return ScriptedTransport(tuple(exchanges))


async def run() -> RecipeEvidence:  # noqa: C901 - scenario intentionally exercises every terminal variant
    """Retain successful rows and all three correlated terminal outcomes."""
    transport = _fixture()
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    async with Bitrix24(settings, transport=transport) as client:
        stream = client.iter_reference_outcomes(
            _request(101, None),
            tuple(
                Binding(str(chat_id), (ParameterUpdate(ParameterPath(("CHAT_ID",)), chat_id),), chat_id)
                for chat_id in (101, 102, 103)
            ),
            traversal=CursorTraversal(
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
                page_size=2,
            ),
            dispatch=DirectDispatch(concurrency=1),
        )
        rows: dict[int, list[int]] = {101: [], 102: []}
        completed: set[int] = set()
        failures: dict[int, ReferenceFailure[int]] = {}
        async for outcome in stream:
            if isinstance(outcome, ReferenceItem):
                rows[outcome.correlation].append(_message_id(outcome.item))
            elif isinstance(outcome, ReferenceComplete):
                completed.add(outcome.correlation)
            elif isinstance(outcome, ReferenceFailure):
                failures[outcome.correlation] = outcome
        if {chat: tuple(ids) for chat, ids in rows.items()} != EXPECTED_IDS:
            raise AssertionError("scenario 4 successful chat IDs differed from oracle")
        if completed != {101, 102} or set(failures) != {103}:
            raise AssertionError("scenario 4 lost a correlated terminal outcome")
        error = failures[103].error
        if not isinstance(error, ApiResponseError) or error.original_code != "ACCESS_ERROR":
            raise AssertionError("scenario 4 access denial lost its typed code")
        if stream.report is None or stream.report.state is not TerminalState.COMPLETED_WITH_FAILURES:
            raise AssertionError("scenario 4 should complete with one binding failure")
        if stream.report.exhausted:
            raise AssertionError("scenario 4 failure cannot prove global exhaustion")
    transport.assert_exhausted()
    report = stream.report
    if report is None:
        raise AssertionError("scenario 4 lost its terminal report")
    return RecipeEvidence(sum(len(ids) for ids in rows.values()), report, (report,))


if __name__ == "__main__":
    asyncio.run(run())
