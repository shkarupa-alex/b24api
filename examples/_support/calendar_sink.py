"""Atomic, keyed calendar delta sink for the offline recipe.

An inclusive ``modified_since`` replay can return versions the mirror already applied, in any order
within one response. The sink therefore keeps the newest applied version per event (instant plus
tombstone flag) and ignores older ones, so a replayed live row never resurrects a deleted event and a
replayed older response never moves the checkpoint backwards.
"""

from __future__ import annotations
import sqlite3
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from pathlib import Path


def _instant(stamp: str) -> float:
    moment = datetime.fromisoformat(stamp)
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("calendar timestamp must include a timezone")
    return moment.timestamp()


class CalendarSink:
    """Apply a whole delta and its inclusive timestamp checkpoint together."""

    def __init__(self, path: Path) -> None:
        self._db = sqlite3.connect(path)
        with self._db:
            self._db.execute(
                "CREATE TABLE IF NOT EXISTS events ("
                "id INTEGER PRIMARY KEY, name TEXT NOT NULL, instant REAL NOT NULL, deleted INTEGER NOT NULL)",
            )
            self._db.execute(
                "CREATE TABLE IF NOT EXISTS cursor ("
                "slot INTEGER PRIMARY KEY CHECK(slot=1), stamp TEXT NOT NULL, instant REAL NOT NULL)",
            )

    def apply_delta(self, response: object) -> None:
        """Commit newer tombstones, newer live rows, and a non-decreasing mark atomically."""
        if not isinstance(response, list) or any(not isinstance(row, dict) for row in response):
            raise ValueError("calendar response must be an array of event objects")
        rows = cast("list[dict[str, Any]]", response)
        prepared: list[tuple[int, str, bool, str, float]] = []
        for row in rows:
            event_id = int(row["ID"])
            if event_id < 1 or row["DELETED"] not in ("Y", "N"):
                raise ValueError("invalid event identity or deletion flag")
            stamp = str(row["TIMESTAMP_X_ISO"])
            prepared.append((event_id, str(row.get("NAME", "")), row["DELETED"] == "Y", stamp, _instant(stamp)))
        with self._db:
            for event_id, name, deleted, _, instant in prepared:
                current = self._db.execute("SELECT instant, deleted FROM events WHERE id=?", (event_id,)).fetchone()
                if current is not None and (
                    current[0] > instant or (current[0] == instant and (bool(current[1]) or not deleted))
                ):
                    continue
                self._db.execute(
                    "INSERT INTO events(id, name, instant, deleted) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(id) DO UPDATE SET name=excluded.name, instant=excluded.instant, "
                    "deleted=excluded.deleted",
                    (event_id, name, instant, int(deleted)),
                )
            if prepared:
                _, _, _, stamp, instant = max(prepared, key=lambda item: item[4])
                self._db.execute(
                    "INSERT INTO cursor(slot, stamp, instant) VALUES (1, ?, ?) "
                    "ON CONFLICT(slot) DO UPDATE SET stamp=excluded.stamp, instant=excluded.instant "
                    "WHERE excluded.instant > cursor.instant",
                    (stamp, instant),
                )

    def checkpoint(self) -> str | None:
        row = self._db.execute("SELECT stamp FROM cursor WHERE slot=1").fetchone()
        return str(row[0]) if row else None

    def ids(self) -> tuple[int, ...]:
        return tuple(int(row[0]) for row in self._db.execute("SELECT id FROM events WHERE deleted=0 ORDER BY id"))

    def close(self) -> None:
        self._db.close()
