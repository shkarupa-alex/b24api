"""Calendar delta folds recurring tombstones before an atomic checkpoint."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from examples._support.calendar_sink import CalendarSink

if TYPE_CHECKING:
    from collections.abc import Iterator


def test_calendar_delta_recipe_folds_tombstones_and_replays_border() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.calendar_delta"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


T0 = "2026-09-01T09:00:00+03:00"
T1 = "2026-09-02T10:00:00+03:00"


def _live(stamp: str) -> dict[str, str]:
    return {"ID": "101", "NAME": "Recurring meeting", "DELETED": "N", "TIMESTAMP_X_ISO": stamp}


def _tombstone(stamp: str) -> dict[str, str]:
    return {"ID": "101", "DELETED": "Y", "TIMESTAMP_X_ISO": stamp}


@pytest.fixture
def sink(tmp_path: Path) -> Iterator[CalendarSink]:
    value = CalendarSink(tmp_path / "calendar.sqlite3")
    yield value
    value.close()


def test_older_live_replay_after_tombstone_does_not_resurrect(sink: CalendarSink) -> None:
    sink.apply_delta([_live(T0)])
    sink.apply_delta([_tombstone(T1)])
    sink.apply_delta([_live(T0)])

    assert sink.ids() == ()
    assert sink.checkpoint() == T1


def test_tombstone_before_older_live_row_in_one_response_stays_deleted(sink: CalendarSink) -> None:
    sink.apply_delta([_tombstone(T1), _live(T0)])

    assert sink.ids() == ()
    assert sink.checkpoint() == T1


def test_equal_time_live_replay_loses_to_tombstone(sink: CalendarSink) -> None:
    sink.apply_delta([_tombstone(T1)])
    sink.apply_delta([_live(T1)])

    assert sink.ids() == ()


def test_older_response_never_moves_the_checkpoint_backwards(sink: CalendarSink) -> None:
    sink.apply_delta([_live(T1)])
    sink.apply_delta([{"ID": "102", "NAME": "Other", "DELETED": "N", "TIMESTAMP_X_ISO": T0}])

    assert sink.checkpoint() == T1
    assert sink.ids() == (101, 102)
