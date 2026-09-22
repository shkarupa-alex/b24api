"""Recent-dialog recipe reports duplicate chat IDs without trusting total=-1."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_recent_dialogs_recipe_reconciles_cross_page_duplicate() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.recent_dialogs"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
