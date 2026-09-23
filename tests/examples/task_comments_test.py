"""Task comment recipe distinguishes modern, legacy, empty and denied states."""

from __future__ import annotations
import os
import subprocess
import sys
from pathlib import Path

import pytest

from examples import task_comments


def test_task_comments_recipe_uses_correlated_batch_and_legacy_slots() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.task_comments"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "ENV": "PROD"},
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_task_comments_non_production_verifies_keyset_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def verify(_self: object, request: object, **_kwargs: object) -> object:
        method = request.method
        assert isinstance(method, str)
        calls.append(method)
        return object()

    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.setattr(task_comments.Bitrix24, "verify_keyset_capability", verify)

    await task_comments.run()
    assert calls == [task_comments.LEGACY_METHOD]
