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
MAX_SUPPORT_DECLARATIONS = 10
SCENARIO_COUNT = 19


def test_recipe_uses_only_public_client_exports() -> None:
    recipe_paths = sorted(
        path for path in (ROOT / "examples").glob("*.py") if path.name not in {"__init__.py", "run.py"}
    )
    assert len(recipe_paths) == SCENARIO_COUNT
    recipe_paths += sorted((ROOT / "examples" / "_support").glob("*.py"))
    for path in recipe_paths:
        source = path.read_text()
        imports = (node.module for node in ast.walk(ast.parse(source)) if isinstance(node, ast.ImportFrom))
        assert all(
            module in {"b24api", "b24api.testing"}
            for module in imports
            if module is not None and module.startswith("b24api")
        )


def test_example_support_stays_below_the_public_helper_limit() -> None:
    declarations: list[str] = []
    for path in sorted((ROOT / "examples" / "_support").glob("*.py")):
        tree = ast.parse(path.read_text())
        declarations.extend(
            node.name
            for node in tree.body
            if isinstance(node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef) and not node.name.startswith("_")
        )
    assert len(declarations) <= MAX_SUPPORT_DECLARATIONS


def test_chat_bounded_recipe_matches_its_independent_oracle() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "examples.chat_bounded_mirror"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_scripted_transport_rejects_an_unlisted_request() -> None:
    transport = ScriptedTransport((ScriptedExchange.json(Request("listed", route=RouteKind.BARE), {"result": []}),))
    with pytest.raises(AssertionError, match="unexpected scripted request"):
        await transport.send(Request("other", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024)
    with pytest.raises(AssertionError, match="scripted exchange"):
        transport.assert_exhausted()
