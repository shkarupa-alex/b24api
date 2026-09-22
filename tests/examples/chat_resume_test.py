"""Durable cursor checkpoint and keyed overlap recipe runs offline."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_chat_resume_recipe_reconciles_after_global_early_close() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.chat_resume"],
        cwd=root, check=False, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
