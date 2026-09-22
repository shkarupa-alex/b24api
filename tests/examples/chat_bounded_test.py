"""The public multi-chat recipe runs without network or hidden fixture fallbacks."""

from __future__ import annotations
import ast
import re
import subprocess
import sys
from pathlib import Path

import pytest

import b24api as public_client
import b24api.testing as public_testing
from b24api import Request, RouteKind
from b24api.testing import ScriptedExchange, ScriptedTransport

ROOT = Path(__file__).resolve().parents[2]
MAX_SUPPORT_DECLARATIONS = 10
SCENARIO_COUNT = 19


def _assert_public_client_imports(source: str) -> None:  # noqa: C901, PLR0912
    tree = ast.parse(source)
    root_aliases: set[str] = set()
    importlib_aliases: set[str] = set()
    dynamic_import_aliases: set[str] = {"__import__"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "b24api":
                    root_aliases.add(alias.asname or "b24api")
                elif alias.name == "importlib":
                    importlib_aliases.add(alias.asname or "importlib")
        elif isinstance(node, ast.ImportFrom) and node.module == "importlib":
            dynamic_import_aliases.update(
                alias.asname or alias.name for alias in node.names if alias.name == "import_module"
            )

    public_exports = {
        "b24api": frozenset(public_client.__all__),
        "b24api.testing": frozenset(public_testing.__all__),
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("b24api") and alias.name != "b24api":
                    raise AssertionError(f"private client import: {alias.name}")
        elif isinstance(node, ast.ImportFrom) and node.module is not None and node.module.startswith("b24api"):
            if node.module not in public_exports:
                raise AssertionError(f"private client import: {node.module}")
            if any(alias.name not in public_exports[node.module] for alias in node.names):
                raise AssertionError("private client export imported")
        elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id in root_aliases:
            if node.attr not in public_exports["b24api"]:
                raise AssertionError(f"private client attribute: {node.attr}")
        elif isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Constant):
            target = node.args[0].value
            dynamic = isinstance(node.func, ast.Name) and node.func.id in dynamic_import_aliases
            dynamic = dynamic or (
                isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id in importlib_aliases
                and node.func.attr == "import_module"
            )
            if dynamic and isinstance(target, str) and target.startswith("b24api."):
                raise AssertionError(f"private client dynamic import: {target}")


def test_recipe_uses_only_public_client_exports() -> None:
    recipe_paths = sorted(path for path in (ROOT / "examples").glob("*.py") if path.name != "__init__.py")
    assert len(recipe_paths) == SCENARIO_COUNT + 1
    recipe_paths += sorted((ROOT / "examples" / "_support").glob("*.py"))
    for path in recipe_paths:
        _assert_public_client_imports(path.read_text())


@pytest.mark.parametrize(
    "source",
    [
        "import b24api.traversal",
        "import b24api.batch as hidden",
        "from b24api.execution import Executor",
        "from b24api import _stream",
        "from b24api import traversal",
        "from b24api import batch, references, execution",
        "import b24api\nb24api.traversal.facade",
        'import importlib\nimportlib.import_module("b24api.traversal")',
        'from importlib import import_module\nimport_module("b24api.batch")',
        '__import__("b24api.references")',
    ],
)
def test_private_client_import_forms_are_rejected(source: str) -> None:
    with pytest.raises(AssertionError, match="private client"):
        _assert_public_client_imports(source)


def test_examples_contain_no_real_portal_url_or_webhook_secret() -> None:
    url = re.compile(r"https?://([^/\s`\"']+)(/[^\s`\"']*)?")
    webhook = re.compile(r"/rest/[1-9]\d*/(?!test(?:/|$))[A-Za-z0-9_-]{6,}/")
    for path in [*sorted((ROOT / "examples").rglob("*.py")), ROOT / "examples" / "README.md"]:
        text = path.read_text()
        for host, route in url.findall(text):
            assert host == "fixture.invalid", f"real URL in {path.relative_to(ROOT)}"
            assert webhook.search(route or "") is None, f"credential-shaped webhook in {path.relative_to(ROOT)}"


def test_readme_separates_offline_passes_from_unrecorded_live_gates() -> None:
    text = (ROOT / "examples" / "README.md").read_text()
    rows = [line for line in text.splitlines() if re.match(r"\| \d+ \|", line)]

    assert len(rows) == SCENARIO_COUNT
    assert all("| passing | not recorded |" in row for row in rows)
    assert "| supported |" not in text


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
