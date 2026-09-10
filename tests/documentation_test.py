"""Integrity checks for the compact maintained documentation set."""

from __future__ import annotations
import ast
import re
from pathlib import Path
from types import SimpleNamespace

import b24api

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
DOCS = ROOT / "docs"
MIGRATION = DOCS / "migration.md"
PYTHON_BLOCK = re.compile(r"```python\n(.*?)\n```", re.DOTALL)


def test_docs_are_flat_compact_and_linked_from_readme() -> None:
    tracked_docs = sorted(path.relative_to(ROOT).as_posix() for path in DOCS.rglob("*") if path.is_file())

    assert tracked_docs == [
        "docs/architecture.md",
        "docs/migration.md",
        "docs/performance.md",
        "docs/recipes.md",
    ]
    text = README.read_text(encoding="utf-8")
    assert "docs/architecture.md" in text
    assert "docs/performance.md" in text
    assert "docs/migration.md" in text
    assert "docs/recipes.md" in text
    assert MIGRATION.is_file()


def test_user_documentation_contains_no_internal_issue_identifiers() -> None:
    text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (README, MIGRATION, DOCS / "architecture.md", DOCS / "performance.md", DOCS / "recipes.md")
    )

    assert re.search(r"\b[BC]\d+[a-z]?\b", text) is None


def test_architecture_document_names_the_complete_public_capability_family() -> None:
    text = (DOCS / "architecture.md").read_text(encoding="utf-8")

    for operation in (
        "call()",
        "call_response()",
        "call_bytes()",
        "batch()",
        "fan_out()",
        "iter_list()",
        "iter_list_counted()",
        "iter_list_keyset()",
        "iter_list_cursor()",
        "iter_references()",
    ):
        assert operation in text


def test_migration_python_examples_execute_without_io() -> None:
    blocks = PYTHON_BLOCK.findall(MIGRATION.read_text(encoding="utf-8"))
    assert blocks
    for source in blocks:
        namespace: dict[str, object] = {
            "client": SimpleNamespace(iter_list_keyset=lambda *_args, **kwargs: kwargs),
            "identity": object(),
            "keyset": object(),
            "request": object(),
            "selector": object(),
        }
        code = compile(source, str(MIGRATION), "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
        exec(code, namespace)  # noqa: S102 - exact trusted repository documentation source
        stream = namespace["stream"]
        assert isinstance(stream, dict)
        assert isinstance(stream["execution"], b24api.SequentialKeysetExecution)
