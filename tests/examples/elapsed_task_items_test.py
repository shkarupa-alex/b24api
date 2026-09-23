"""Task-scoped positional traversal proves IDs, scope, and page controls."""

from __future__ import annotations
import asyncio
import subprocess
import sys
from pathlib import Path

import pytest

from b24api import TraversalAssurance
from b24api.errors import IncompleteTraversalError
from examples import elapsed_task_items


def test_elapsed_task_items_recipe_uses_five_json_slots() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.elapsed_task_items"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_elapsed_task_items_recipe_reports_identity_exact() -> None:
    evidence = asyncio.run(elapsed_task_items.run())
    assert evidence.primary_report is not None
    assert evidence.primary_report.assurance is TraversalAssurance.IDENTITY_EXACT


def test_elapsed_task_items_recipe_rejects_a_repeated_first_page() -> None:
    first = elapsed_task_items.EXPECTED_IDS[:50]
    with pytest.raises(IncompleteTraversalError):
        asyncio.run(elapsed_task_items.run(((1, first), (2, first), (3, ()))))
