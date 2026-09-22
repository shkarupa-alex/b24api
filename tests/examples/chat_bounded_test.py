"""The public multi-chat recipe runs without network or hidden fixture fallbacks."""

from __future__ import annotations
import ast
import subprocess
import sys
from pathlib import Path

import pytest

from b24api import Request, RouteKind
from b24api.testing import ScriptedExchange, ScriptedTransport

ROOT = Path(__file__).resolve().parents[2]


def test_recipe_uses_only_public_client_exports() -> None:
    for recipe in (
        "chat_bounded_mirror.py", "chat_resume.py", "binary_download.py", "v3_task_results.py",
        "page_index_members.py",
        "sparse_user_search.py",
        "elapsed_task_items.py",
        "numerator_list.py",
        "content_viewers.py",
    ):
        source = (ROOT / "examples" / recipe).read_text()
        imports = (node.module for node in ast.walk(ast.parse(source)) if isinstance(node, ast.ImportFrom))
        assert all(module in {"b24api", "b24api.testing"}
                   for module in imports if module is not None and module.startswith("b24api"))


def test_chat_bounded_recipe_matches_its_independent_oracle() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "examples.chat_bounded_mirror"],
        cwd=ROOT, check=False, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_scripted_transport_rejects_an_unlisted_request() -> None:
    transport = ScriptedTransport((ScriptedExchange.json(Request("listed", route=RouteKind.BARE), {"result": []}),))
    with pytest.raises(AssertionError, match="unexpected scripted request"):
        await transport.send(Request("other", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024)
    with pytest.raises(AssertionError, match="scripted exchange"):
        transport.assert_exhausted()
