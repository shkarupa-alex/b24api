"""Message-search recipe retains three independent reference outcomes."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path


def test_search_chat_messages_recipe_retains_access_failure() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.search_chat_messages"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
