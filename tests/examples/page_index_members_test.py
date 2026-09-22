"""Both public page-index controls run against exact offline fixture pages."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_one_based_member_page_indexes_reach_all_expected_users() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.page_index_members"],
        cwd=root, check=False, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
