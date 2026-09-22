"""FIRST_ID false completion and three LAST_ID limits are replayed offline."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_message_cursor_direction_recipe_retains_all_descending_ids() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.message_cursor_direction"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
