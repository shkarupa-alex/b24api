"""Sparse mapping-value search traverses an empty selected middle page."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_sparse_user_search_recipe_covers_all_raw_offset_windows() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.sparse_user_search"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
