"""Disk mirror recipe bounds the folder queue and folds REAL_OBJECT_ID links."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_disk_mirror_recipe_reconciles_all_objects_without_cycle() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.disk_mirror"],
        cwd=root, check=False, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
