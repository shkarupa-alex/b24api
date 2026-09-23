"""Direct nested JSON content-view recipe retains both documented shapes."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_content_viewer_recipe_uses_nested_params_and_readable_ids() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.content_viewers"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
