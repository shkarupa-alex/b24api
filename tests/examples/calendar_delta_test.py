"""Calendar delta folds recurring tombstones before an atomic checkpoint."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_calendar_delta_recipe_folds_tombstones_and_replays_border() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.calendar_delta"],
        cwd=root, check=False, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
