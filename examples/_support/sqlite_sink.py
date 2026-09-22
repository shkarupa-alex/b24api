"""Tiny durable keyed sink with an atomic per-parent overlap checkpoint."""

from __future__ import annotations
import sqlite3
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


class SqliteMirror:
    """Commit one whole cursor page and its resume control in one transaction."""

    def __init__(self, path: Path) -> None:
        self._db = sqlite3.connect(path)
        with self._db:
            self._db.execute("CREATE TABLE IF NOT EXISTS rows (parent TEXT, id INTEGER, PRIMARY KEY(parent, id))")
            self._db.execute("CREATE TABLE IF NOT EXISTS checkpoints (parent TEXT PRIMARY KEY, overlap_cursor INTEGER)")

    def commit_page(self, parent: str, ids: tuple[int, ...]) -> None:
        """Persist keyed rows and a one-row overlap before acknowledging the page."""
        if not ids:
            return
        if any(value < 1 for value in ids):
            raise ValueError("cursor identities must be positive")
        with self._db:
            self._db.executemany(
                "INSERT OR IGNORE INTO rows(parent, id) VALUES (?, ?)",
                ((parent, value) for value in ids),
            )
            self._db.execute(
                "INSERT INTO checkpoints(parent, overlap_cursor) VALUES (?, ?) "
                "ON CONFLICT(parent) DO UPDATE SET overlap_cursor=excluded.overlap_cursor",
                (parent, ids[-1] + 1),
            )

    def checkpoint(self, parent: str) -> int | None:
        """Read the last durable exclusive cursor with one-row overlap."""
        row = self._db.execute(
            "SELECT overlap_cursor FROM checkpoints WHERE parent=?", (parent,),
        ).fetchone()
        return int(row[0]) if row is not None else None

    def ids(self, parent: str) -> tuple[int, ...]:
        """Read the keyed final oracle in descending identity order."""
        return tuple(
            int(row[0]) for row in self._db.execute(
                "SELECT id FROM rows WHERE parent=? ORDER BY id DESC", (parent,),
            )
        )

    def close(self) -> None:
        """Close the owned SQLite connection."""
        self._db.close()
