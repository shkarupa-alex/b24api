"""Exact binary and typed JSON error recipe runs without a live portal."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_binary_recipe_preserves_bytes_media_and_typed_error() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.binary_download"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
