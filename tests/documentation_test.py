"""Integrity checks for the compact maintained documentation set."""

from __future__ import annotations
import ast
import re
from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace
from typing import Self, cast

import pytest

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


class _EmptyStream(AsyncIterator[object]):
    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> object:
        raise StopAsyncIteration

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None


class _MigrationClient:
    def iter_list_keyset(self, *_args: object, **kwargs: object) -> dict[str, object]:
        return kwargs

    def iter_cursors(self, *_args: object, **_kwargs: object) -> _EmptyStream:
        return _EmptyStream()

    async def verify_keyset_capability(self, *_args: object, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(verdict="verified")


@pytest.mark.asyncio
async def test_migration_python_examples_execute_without_io() -> None:
    blocks = PYTHON_BLOCK.findall(MIGRATION.read_text(encoding="utf-8"))
    assert blocks
    for source in blocks:
        client = _MigrationClient()
        namespace: dict[str, object] = {
            "AdaptedPage": b24api.AdaptedPage,
            "Binding": b24api.Binding,
            "PageView": b24api.PageView,
            "ParameterPath": b24api.ParameterPath,
            "ParameterUpdate": b24api.ParameterUpdate,
            "ResultSelector": b24api.ResultSelector,
            "api": client,
            "bindings": (),
            "checkpoints": {42: 7300},
            "client": client,
            "consume": lambda *_args: None,
            "cursor": object(),
            "identity": object(),
            "keyset": object(),
            "request": object(),
            "selector": object(),
        }
        code = compile(source, str(MIGRATION), "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
        result = eval(code, namespace)  # noqa: S307 - exact trusted repository documentation source
        if result is not None:
            await cast("object", result)
        stream = namespace.get("stream")
        if isinstance(stream, dict) and "execution" in stream:
            assert isinstance(stream["execution"], b24api.SequentialKeysetExecution)
