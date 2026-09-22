"""Counted requisite links compare exact direct/batch keys and reject a gap."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_requisite_links_recipe_checks_complete_windows() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.requisite_links"],
        cwd=root, check=False, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
