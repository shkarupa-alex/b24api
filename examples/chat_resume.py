"""Scenario 7: SQLite page commit, global early close, bounded overlap resume.

Offline fixture: two readable chats on `im.dialog.messages.get`, descending
exclusive `LAST_ID`, exact `messages` selector, two rows per page. The first
run stops A after a committed page and closes the operation before B. The
second run resumes A from its durable one-row overlap and B from head; the
keyed sink absorbs the re-read overlap row. That row may have been deleted
between runs, so its absence is not an error: resume fails only on a
contradiction, a returned ID at or above the committed exclusive bound. This
demonstrates keyed deduplication; it does not claim snapshot consistency on a
mutable live portal. Run: `uv run python -m examples.chat_resume`.
"""

from __future__ import annotations
import asyncio
import tempfile
from pathlib import Path

from b24api import (
    Binding,
    Bitrix24,
    CursorSpec,
    DirectDispatch,
    IdentityCoercion,
    OperationReport,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    Settings,
    TerminalState,
)
from b24api.contracts import CallerStop, ContinuePage, CursorDomain, DeliveryOrder, PageBoundary, ReferenceComplete
from b24api.testing import ScriptedExchange, ScriptedTransport
from examples._support.evidence import RecipeEvidence
from examples._support.sqlite_sink import SqliteMirror

METHOD = "im.dialog.messages.get"
EXPECTED = {"A": (105, 104, 103, 102, 101), "B": (115, 114, 113)}
FIRST_OVERLAP = 105
RESUMED_PAGES = (
    ("A", 105, (104, 103)),
    ("A", 103, (102, 101)),
    ("A", 101, ()),
    ("B", None, (115, 114)),
    ("B", 114, (113,)),
    ("B", 113, ()),
)


def _request(parent: str, cursor: int | None) -> Request:
    params = {"DIALOG_ID": parent, "LIMIT": 2}
    if cursor is not None:
        params["LAST_ID"] = cursor
    return Request(
        METHOD,
        params,
        replay_safety=ReplaySafety.SAFE,
        route=RouteKind.BARE,
    )


def _transport(pages: tuple[tuple[str, int | None, tuple[int, ...]], ...]) -> ScriptedTransport:
    return ScriptedTransport(
        tuple(
            ScriptedExchange.json(
                _request(parent, cursor),
                {"result": {"messages": [{"id": value} for value in ids]}},
            )
            for parent, cursor, ids in pages
        )
    )


def _bindings(sink: SqliteMirror) -> tuple[Binding[str], ...]:
    return tuple(
        Binding(
            parent,
            (ParameterUpdate(ParameterPath(("DIALOG_ID",)), parent),),
            parent,
            start_cursor=sink.checkpoint(parent),
        )
        for parent in EXPECTED
    )


class _Commit:
    """Commit every full page before the scheduler acknowledges it."""

    def __init__(self, sink: SqliteMirror, *, pause_a: bool) -> None:
        self.sink = sink
        self.pause_a = pause_a
        self.resume_bound = {
            parent: checkpoint for parent in EXPECTED if (checkpoint := sink.checkpoint(parent)) is not None
        }

    def on_page(self, boundary: PageBoundary) -> ContinuePage | CallerStop:
        parent = ("A", "B")[boundary.binding_id]
        ids = tuple(int(row["id"]) for row in boundary.rows)
        bound = self.resume_bound.get(parent)
        if bound is not None and any(value >= bound for value in ids):
            raise ValueError("scenario 7 resumed page contradicts the committed exclusive bound")
        self.sink.commit_page(parent, ids)
        if self.pause_a and parent == "A":
            return CallerStop("persisted pause")
        return ContinuePage.CONTINUE


async def _run_once(sink: SqliteMirror, transport: ScriptedTransport, *, pause_a: bool) -> OperationReport:
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    commit = _Commit(sink, pause_a=pause_a)
    async with Bitrix24(settings, transport=transport) as client:
        stream = client.iter_cursors(
            _request("", None),
            _bindings(sink),
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
            dispatch=DirectDispatch(concurrency=1, output_order=DeliveryOrder.INPUT),
            page_stop=commit,
        )
        async for event in stream:
            if pause_a and isinstance(event, ReferenceComplete) and event.correlation == "A":
                await stream.aclose()
                break
        report = stream.report
        if report is None:
            raise AssertionError("scenario 7 lacked a terminal report")
    transport.assert_exhausted()
    return report


async def pause_and_resume(
    resumed_pages: tuple[tuple[str, int | None, tuple[int, ...]], ...] = RESUMED_PAGES,
) -> tuple[OperationReport, OperationReport, dict[str, tuple[int, ...]]]:
    """Pause A after one committed page, then resume both chats against the given source pages."""
    with tempfile.TemporaryDirectory(prefix="b24api-example-") as temporary:
        sink = SqliteMirror(Path(temporary) / "mirror.sqlite3")
        try:
            first = _transport((("A", None, (105, 104)),))
            first_report = await _run_once(sink, first, pause_a=True)
            if (
                first_report.state is not TerminalState.EARLY_CLOSED
                or first_report.exhausted
                or sink.checkpoint("A") != FIRST_OVERLAP
            ):
                raise AssertionError("scenario 7 did not preserve the committed early-close checkpoint")
            second_report = await _run_once(sink, _transport(resumed_pages), pause_a=False)
            mirrored = {parent: sink.ids(parent) for parent in EXPECTED}
        finally:
            sink.close()
    return first_report, second_report, mirrored


async def run() -> RecipeEvidence:
    """Verify exact keyed IDs and the two distinct terminal meanings."""
    first_report, second_report, mirrored = await pause_and_resume()
    if second_report.state is not TerminalState.COMPLETED or not second_report.exhausted:
        raise AssertionError("scenario 7 resume did not complete")
    if mirrored != EXPECTED:
        raise AssertionError("scenario 7 keyed sink differs from uninterrupted oracle")
    observed_count = sum(len(ids) for ids in mirrored.values())
    return RecipeEvidence(observed_count, second_report, (first_report, second_report))


if __name__ == "__main__":
    asyncio.run(run())
