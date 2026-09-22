"""Task-scoped positional traversal proves IDs, scope, and page controls."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_elapsed_task_items_recipe_uses_five_json_slots() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.elapsed_task_items"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
