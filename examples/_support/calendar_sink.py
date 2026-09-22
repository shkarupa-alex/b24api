"""Atomic, keyed calendar delta sink for the offline recipe."""

from __future__ import annotations
import sqlite3
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from pathlib import Path


class CalendarSink:
    """Apply a whole delta and its inclusive timestamp checkpoint together."""

    def __init__(self, path: Path) -> None:
        self._db = sqlite3.connect(path)
        with self._db:
            self._db.execute("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            self._db.execute(
                "CREATE TABLE IF NOT EXISTS cursor (slot INTEGER PRIMARY KEY CHECK(slot=1), stamp TEXT NOT NULL)",
            )

    def apply_delta(self, response: object) -> None:
        """Commit tombstones, live rows, and the highest observed mark atomically."""
        if not isinstance(response, list) or any(not isinstance(row, dict) for row in response):
            raise ValueError("calendar response must be an array of event objects")
        rows = cast("list[dict[str, Any]]", response)
        prepared: list[tuple[int, str, str, str]] = []
        for row in rows:
            event_id = int(row["ID"])
            if event_id < 1 or row["DELETED"] not in ("Y", "N"):
                raise ValueError("invalid event identity or deletion flag")
            stamp = str(row["TIMESTAMP_X_ISO"])
            moment = datetime.fromisoformat(stamp)
            if moment.tzinfo is None or moment.utcoffset() is None:
                raise ValueError("calendar timestamp must include a timezone")
            prepared.append((event_id, str(row.get("NAME", "")), row["DELETED"], stamp))
        with self._db:
            for event_id, name, deleted, _ in prepared:
                if deleted == "Y":
                    self._db.execute("DELETE FROM events WHERE id=?", (event_id,))
                else:
                    self._db.execute(
                        "INSERT INTO events(id, name) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET name=excluded.name",
                        (event_id, name),
                    )
            if prepared:
                latest = max(prepared, key=lambda item: datetime.fromisoformat(item[3]))[3]
                self._db.execute(
                    "INSERT INTO cursor(slot, stamp) VALUES (1, ?) "
                    "ON CONFLICT(slot) DO UPDATE SET stamp=excluded.stamp",
                    (latest,),
                )

    def checkpoint(self) -> str | None:
        row = self._db.execute("SELECT stamp FROM cursor WHERE slot=1").fetchone()
        return str(row[0]) if row else None

    def ids(self) -> tuple[int, ...]:
        return tuple(int(row[0]) for row in self._db.execute("SELECT id FROM events ORDER BY id"))

    def close(self) -> None:
        self._db.close()
