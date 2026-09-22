"""CRM item delta recipe replays an edited old ID and a new ID."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_crm_item_delta_recipe_reconciles_old_and_new_ids() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.crm_item_delta"],
        cwd=root, check=False, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
