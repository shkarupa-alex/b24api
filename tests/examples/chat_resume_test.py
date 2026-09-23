"""Durable cursor checkpoint and keyed overlap recipe runs offline."""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path

import pytest

from b24api import TerminalState
from b24api.errors import ReferenceFailed
from examples import chat_resume


def test_chat_resume_recipe_reconciles_after_global_early_close() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.chat_resume"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


B_PAGES = (("B", None, (115, 114)), ("B", 114, (113,)), ("B", 113, ()))


@pytest.mark.asyncio
async def test_resume_tolerates_a_deleted_overlap_row() -> None:
    deleted_104 = (("A", 105, (103, 102)), ("A", 102, (101,)), ("A", 101, ()), *B_PAGES)

    _, report, mirrored = await chat_resume.pause_and_resume(deleted_104)

    assert report.state is TerminalState.COMPLETED
    assert report.exhausted
    assert mirrored == chat_resume.EXPECTED


@pytest.mark.asyncio
async def test_resume_rejects_an_identity_at_the_committed_exclusive_bound() -> None:
    contradicting = (("A", 105, (105, 104)), *B_PAGES)

    with pytest.raises(ReferenceFailed) as captured:
        await chat_resume.pause_and_resume(contradicting)

    report = captured.value.report  # type: ignore[attr-defined]
    assert report.state is not TerminalState.COMPLETED
    assert not report.exhausted
