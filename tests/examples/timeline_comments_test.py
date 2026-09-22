"""Timeline comment recipe preserves shared storage identity and terminals."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_timeline_comments_recipe_checks_two_pages_and_access_denial() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.timeline_comments"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
