"""Scenario 9: source-qualified calendar delta with recurring tombstone folding.

The frozen source returns a live event, then repeated DELETED=Y rows for one
recurring ID. SQLite commits event changes and the inclusive ISO timestamp in
one transaction. This proves the recipe mechanically, not portal behavior.
Run: `uv run python -m examples.calendar_delta`.
"""

from __future__ import annotations
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from b24api import Bitrix24, ReplaySafety, Request, RouteKind, Settings
from b24api.testing import ScriptedExchange, ScriptedTransport
from examples._support.calendar_sink import CalendarSink

METHOD = "calendar.event.get"
INITIAL = "2026-09-01T09:00:00+03:00"
NEXT = "2026-09-02T10:00:00+03:00"
WINDOW = {"type": "user", "ownerId": 1, "from": "2026-09-01", "to": "2026-09-30"}
LIVE_ROWS = [
    {"ID": "101", "NAME": "Recurring meeting", "DELETED": "N", "TIMESTAMP_X_ISO": INITIAL},
    {"ID": "102", "NAME": "Other event", "DELETED": "N", "TIMESTAMP_X_ISO": INITIAL},
]
DELETED_ROWS = [
    {"ID": "101", "DELETED": "Y", "TIMESTAMP_X_ISO": NEXT, "ORIGINAL_DATE_FROM": day}
    for day in ("2026-09-03", "2026-09-10", "2026-09-17")
]


def _request(stamp: str) -> Request:
    return Request(
        METHOD, {**WINDOW, "modified_since": stamp},
        replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE,
    )


async def run() -> None:
    """Replay an inclusive deletion border and compare the keyed oracle."""
    transport = ScriptedTransport((
        ScriptedExchange.json(_request(INITIAL), {"result": LIVE_ROWS}),
        ScriptedExchange.json(_request(INITIAL), {"result": DELETED_ROWS}),
        ScriptedExchange.json(_request(NEXT), {"result": DELETED_ROWS}),
    ))
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    with TemporaryDirectory() as directory:
        sink = CalendarSink(Path(directory) / "calendar.sqlite3")
        try:
            async with Bitrix24(settings, transport=transport) as client:
                first = await client.call(_request(INITIAL))
                sink.apply_delta(first)
                if sink.ids() != (101, 102) or sink.checkpoint() != INITIAL:
                    raise AssertionError("scenario 9 initial commit differs from oracle")
                # Same inclusive border can contain a later deletion. The checkpoint
                # remains at the last committed event mark until the transaction ends.
                deletion = await client.call(_request(sink.checkpoint() or INITIAL))
                if sink.ids() != (101, 102) or sink.checkpoint() != INITIAL:
                    raise AssertionError("scenario 9 advanced before durable commit")
                sink.apply_delta(deletion)
                if sink.ids() != (102,) or sink.checkpoint() != NEXT:
                    raise AssertionError("scenario 9 tombstone folding differs from oracle")
                replay = await client.call(_request(sink.checkpoint() or NEXT))
                sink.apply_delta(replay)
                if sink.ids() != (102,) or sink.checkpoint() != NEXT:
                    raise AssertionError("scenario 9 inclusive replay was not idempotent")
        finally:
            sink.close()
    transport.assert_exhausted()


if __name__ == "__main__":
    asyncio.run(run())
