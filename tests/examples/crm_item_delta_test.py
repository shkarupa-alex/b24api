"""CRM item delta recipe replays an edited old ID and a new ID."""

from __future__ import annotations
import os
import subprocess
import sys
from pathlib import Path

import pytest

from examples import crm_item_delta


def test_crm_item_delta_recipe_reconciles_old_and_new_ids() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.crm_item_delta"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "ENV": "PROD"},
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_crm_item_delta_non_production_verifies_each_keyset_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    borders: list[str] = []

    async def verify(_self: object, request: object, **_kwargs: object) -> object:
        copy_parameters = request.copy_parameters
        parameters = copy_parameters()
        border = parameters["filter"][">=updatedTime"]
        assert isinstance(border, str)
        borders.append(border)
        return object()

    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.setattr(crm_item_delta.Bitrix24, "verify_keyset_capability", verify)

    await crm_item_delta.run()
    assert borders == [crm_item_delta.INITIAL_BORDER, crm_item_delta.REPLAY_BORDER]
