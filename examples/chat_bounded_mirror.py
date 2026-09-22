"""Scenario 1: stop individual chat bindings after whole committed pages.

Offline fixture: three readable chats, `im.dialog.messages.get`, `DIALOG_ID`,
exclusive descending `LAST_ID`, `LIMIT=2`, selector `messages`. The expected
IDs below are declared before client construction. A and B stop on an old
page; C reaches an empty confirmation. This proves bounded prefixes, not a
snapshot of mutable chat history. Run: `uv run python -m examples.chat_bounded_mirror`.
"""

from __future__ import annotations
import asyncio

from b24api import (
    Binding,
    Bitrix24,
    CallerStop,
    ContinuePage,
    CursorDomain,
    CursorSpec,
    DirectDispatch,
    IdentityCoercion,
    PageBoundary,
    ParameterPath,
    ParameterUpdate,
    ReferenceComplete,
    ReferenceItem,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    Settings,
    TerminalState,
    TraversalAssurance,
)
from b24api.testing import ScriptedExchange, ScriptedTransport

METHOD = "im.dialog.messages.get"
EXPECTED_IDS = {"A": (9, 8, 3, 2), "B": (15, 14, 13, 12, 1), "C": (20, 19)}
EXPECTED_STOPS = {"A": "cutoff reached", "B": "cutoff reached", "C": None}
HEAD = 100
CUTOFF_ID = 3


def _request(chat: str, cursor: int) -> Request:
    return Request(
        METHOD,
        {"DIALOG_ID": chat, "LAST_ID": cursor, "LIMIT": 2},
        replay_safety=ReplaySafety.SAFE,
        route=RouteKind.BARE,
    )


def _fixture() -> ScriptedTransport:
    pages = (
        ("A", 100, (9, 8)),
        ("A", 8, (3, 2)),
        ("B", 100, (15, 14)),
        ("B", 14, (13, 12)),
        ("B", 12, (1,)),
        ("C", 100, (20, 19)),
        ("C", 19, ()),
    )
    return ScriptedTransport(
        tuple(
            ScriptedExchange.json(_request(chat, cursor), {"result": {"messages": [{"id": value} for value in ids]}})
            for chat, cursor, ids in pages
        )
    )


class _Cutoff:
    """Commit the complete page before returning a per-binding stop decision."""

    def __init__(self) -> None:
        self.committed: dict[str, list[int]] = {chat: [] for chat in EXPECTED_IDS}

    def on_page(self, boundary: PageBoundary) -> ContinuePage | CallerStop:
        chat = ("A", "B", "C")[boundary.binding_id]
        ids = [int(row["id"]) for row in boundary.rows]
        self.committed[chat].extend(ids)
        if chat in {"A", "B"} and ids and min(ids) <= CUTOFF_ID:
            return CallerStop("cutoff reached")
        return ContinuePage.CONTINUE


def _expect(actual: object, expected: object, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"scenario 1 oracle mismatch: {label}")


async def run() -> None:
    """Execute the frozen fixture and verify the independent per-chat oracle."""
    transport = _fixture()
    stop = _Cutoff()
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    async with Bitrix24(settings, transport=transport) as client:
        stream = client.iter_cursors(
            _request("", HEAD),
            tuple(
                Binding(chat, (ParameterUpdate(ParameterPath(("DIALOG_ID",)), chat),), chat) for chat in EXPECTED_IDS
            ),
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
            dispatch=DirectDispatch(concurrency=3),
            page_stop=stop,
        )
        observed: dict[str, list[int]] = {chat: [] for chat in EXPECTED_IDS}
        completions: dict[str, ReferenceComplete] = {}
        async for event in stream:
            if isinstance(event, ReferenceItem):
                observed[event.correlation].append(int(event.item["id"]))
            elif isinstance(event, ReferenceComplete):
                completions[event.correlation] = event
        _expect({chat: tuple(ids) for chat, ids in observed.items()}, EXPECTED_IDS, "delivered IDs")
        _expect({chat: tuple(ids) for chat, ids in stop.committed.items()}, EXPECTED_IDS, "committed IDs")
        _expect({chat: done.stop_reason for chat, done in completions.items()}, EXPECTED_STOPS, "binding stops")
        report = stream.report
        if report is None:
            raise AssertionError("scenario 1 lacked a terminal report")
        _expect(report.state, TerminalState.COMPLETED, "terminal state")
        _expect(report.assurance, TraversalAssurance.BOUNDED_PREFIX, "assurance")
        if report.exhausted:
            raise AssertionError("scenario 1 falsely claimed source exhaustion")
        _expect(report.physical_requests, len(transport.calls), "physical requests")
    transport.assert_exhausted()


if __name__ == "__main__":
    asyncio.run(run())
