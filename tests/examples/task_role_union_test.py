"""Four-role task union uses aligned wire offsets and a keyed oracle."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_task_role_union_recipe_avoids_rounded_offset_repeat() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.task_role_union"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
