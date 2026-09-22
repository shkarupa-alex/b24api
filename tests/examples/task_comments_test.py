"""Task comment recipe distinguishes modern, legacy, empty and denied states."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_task_comments_recipe_uses_correlated_batch_and_legacy_slots() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.task_comments"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
