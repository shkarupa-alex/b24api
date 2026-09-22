"""Public V3 route and object-valued API error recipe runs offline."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_v3_task_results_recipe_preserves_failed_parent_checkpoint() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.v3_task_results"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
