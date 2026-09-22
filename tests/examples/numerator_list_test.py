"""Page-local totals cannot shorten the public fixed-step numerator recipe."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_numerator_recipe_covers_offsets_despite_page_local_totals() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.numerator_list"],
        cwd=root, check=False, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
