"""Scenario 1: stop individual chat bindings after whole committed pages.

Offline fixture: four readable chats, `im.dialog.messages.get`, `DIALOG_ID`,
exclusive descending `LAST_ID`, `LIMIT=2`, selector `messages`. The expected
IDs below are declared before client construction. A, B and D stop on an old
page; C reaches an empty confirmation. The same bindings without the page stop
read every chat to its empty confirmation, and the bounded mirror must use
strictly fewer requests than that exhaust-to-first baseline. This proves
bounded prefixes, not a snapshot of mutable chat history.
Run: `uv run python -m examples.chat_bounded_mirror`.
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
from examples._support.evidence import RecipeEvidence

METHOD = "im.dialog.messages.get"
# Complete frozen histories, newest first; the bounded mirror reads only their prefixes.
HISTORY = {"A": (109, 108, 3, 2, 1), "B": (115, 114, 113, 112, 1), "C": (120, 119), "D": (130, 129, 128, 127, 2, 1)}
EXPECTED_IDS = {"A": (109, 108, 3, 2), "B": (115, 114, 113, 112, 1), "C": (120, 119), "D": (130, 129, 128, 127, 2, 1)}
EXPECTED_STOPS = {"A": "cutoff reached", "B": "cutoff reached", "C": None, "D": "cutoff reached"}
EXPECTED_REQUESTS = 10
EXPECTED_BASELINE_REQUESTS = 14
CUTOFF_ID = 3
STOPPING_CHATS = frozenset({"A", "B", "D"})


def _request(chat: str, cursor: int | None) -> Request:
    params = {"DIALOG_ID": chat, "LIMIT": 2}
    if cursor is not None:
        params["LAST_ID"] = cursor
    return Request(
        METHOD,
        params,
        replay_safety=ReplaySafety.SAFE,
        route=RouteKind.BARE,
    )


def _pages(chat: str, *, bounded: bool) -> list[tuple[int | None, tuple[int, ...]]]:
    """Split one history into LIMIT=2 pages, ending at the cutoff page or the empty confirmation."""
    pages: list[tuple[int | None, tuple[int, ...]]] = []
    rest, cursor = HISTORY[chat], None
    while True:
        page, rest = rest[:2], rest[2:]
        pages.append((cursor, page))
        if not page or (bounded and chat in STOPPING_CHATS and min(page) <= CUTOFF_ID):
            return pages
        cursor = page[-1]


def _fixture(*, bounded: bool) -> ScriptedTransport:
    return ScriptedTransport(
        tuple(
            ScriptedExchange.json(_request(chat, cursor), {"result": {"messages": [{"id": value} for value in ids]}})
            for chat in HISTORY
            for cursor, ids in _pages(chat, bounded=bounded)
        )
    )


class _Cutoff:
    """Commit the complete page before returning a per-binding stop decision."""

    def __init__(self) -> None:
        self.committed: dict[str, list[int]] = {chat: [] for chat in EXPECTED_IDS}

    def on_page(self, boundary: PageBoundary) -> ContinuePage | CallerStop:
        chat = tuple(HISTORY)[boundary.binding_id]
        ids = [int(row["id"]) for row in boundary.rows]
        self.committed[chat].extend(ids)
        if chat in STOPPING_CHATS and ids and min(ids) <= CUTOFF_ID:
            return CallerStop("cutoff reached")
        return ContinuePage.CONTINUE


def _expect(actual: object, expected: object, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"scenario 1 oracle mismatch: {label}")


def _stream(client: Bitrix24, stop: _Cutoff | None):  # noqa: ANN202 - public stream type is internal here
    return client.iter_cursors(
        _request("", None),
        tuple(Binding(chat, (ParameterUpdate(ParameterPath(("DIALOG_ID",)), chat),), chat) for chat in HISTORY),
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
        dispatch=DirectDispatch(concurrency=len(HISTORY)),
        page_stop=stop,
    )


async def _baseline_requests(settings: Settings) -> int:
    """Read every chat to its empty confirmation, as a mirror without page stop would."""
    transport = _fixture(bounded=False)
    async with Bitrix24(settings, transport=transport) as client:
        stream = _stream(client, None)
        async for _event in stream:
            pass
        if stream.report is None or stream.report.state is not TerminalState.COMPLETED:
            raise AssertionError("scenario 1 baseline did not complete")
    transport.assert_exhausted()
    return len(transport.calls)


async def run() -> RecipeEvidence:
    """Execute the frozen fixture and verify the independent per-chat oracle."""
    transport = _fixture(bounded=True)
    stop = _Cutoff()
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    async with Bitrix24(settings, transport=transport) as client:
        stream = _stream(client, stop)
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
        _expect(report.physical_requests, EXPECTED_REQUESTS, "bounded request count")
    transport.assert_exhausted()
    baseline = await _baseline_requests(settings)
    _expect(baseline, EXPECTED_BASELINE_REQUESTS, "exhaust-to-first baseline")
    if not report.physical_requests < baseline:
        raise AssertionError("scenario 1 page stop saved no requests over exhaust-to-first")
    return RecipeEvidence(sum(len(ids) for ids in observed.values()), report, (report,))


if __name__ == "__main__":
    asyncio.run(run())
