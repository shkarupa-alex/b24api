"""Integrity checks for the compact maintained documentation set."""

from __future__ import annotations
import ast
import json
import re
from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace
from typing import Self, cast

import pytest

import b24api
from b24api.execution import Executor, WireResponse

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
        "docs/specifications/b24api-issues-architecture/decision-ledger.md",
        "docs/specifications/b24api-issues-architecture/examples-contracts.md",
        "docs/specifications/b24api-issues-architecture/registry-contracts.md",
        "docs/specifications/b24api-issues-architecture/specification.md",
        "docs/specifications/b24api-issues-architecture/synthesis.md",
        "docs/specifications/b24api-issues-architecture/transport-and-errors.md",
        "docs/specifications/b24api-issues-architecture/traversal-contracts.md",
    ]
    text = README.read_text(encoding="utf-8")
    assert "docs/architecture.md" in text
    assert "docs/performance.md" in text
    assert "docs/migration.md" in text
    assert "docs/recipes.md" in text
    assert MIGRATION.is_file()


def test_every_documented_keyset_traversal_keeps_the_non_production_guard_beside_it() -> None:
    for path in (README, DOCS / "recipes.md", MIGRATION):
        for source in PYTHON_BLOCK.findall(path.read_text(encoding="utf-8")):
            traversal = source.find("iter_list_keyset(")
            if traversal < 0:
                continue
            verifier = source.find("verify_keyset_capability(")
            assert 'os.environ.get("ENV") != "PROD"' in source
            assert "Accepting an ID filter does not prove strict bounds or ordering." in source
            assert 0 <= verifier < traversal


def test_user_documentation_contains_no_internal_issue_identifiers() -> None:
    text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (README, MIGRATION, DOCS / "architecture.md", DOCS / "performance.md", DOCS / "recipes.md")
    )

    assert re.search(r"\b[BC]\d+[a-z]?\b", text) is None


def test_migration_covers_changed_completion_stop_keyset_and_replay_contracts() -> None:
    text = MIGRATION.read_text(encoding="utf-8")
    for public_contract in ("exhausted", "partial", "page_stop", "BoundedIdentityRange", "ReplayDisposition"):
        assert public_contract in text
    bounded = next(paragraph for paragraph in text.split("\n\n") if "BoundedIdentityRange" in paragraph)
    assert "SequentialKeysetExecution" in bounded
    assert "consumes that\nboundary" in bounded
    assert "RangeKeysetExecution`, `PartitionedKeysetExecution`, and auto execution reject" in bounded


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


class _FixedStepTransport:
    host = "test.invalid"

    def __init__(self, pages: dict[int, list[int]]) -> None:
        self.pages = pages

    async def send(self, request: b24api.Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        rows = [{"ID": value} for value in self.pages[request.copy_parameters().get("start", 0)]]
        return WireResponse(200, (), json.dumps({"result": rows}).encode())


_Outcome = tuple[b24api.OperationReport | None, BaseException | None]


async def _fixed_step_outcome(pages: dict[int, list[int]]) -> _Outcome:
    client = b24api.Bitrix24._from_executor(Executor(_FixedStepTransport(pages)))  # noqa: SLF001
    stream = client.iter_list(
        b24api.Request("example.list", replay_safety=b24api.ReplaySafety.SAFE, route=b24api.RouteKind.BARE),
        page_size=2,
        offset=b24api.OffsetSpec(continuation=b24api.OffsetContinuation.FIXED_STEP, step=2),
    )
    try:
        _ = [row async for row in stream]
    except b24api.IncompleteTraversalError as error:
        return stream.report, error
    return stream.report, None


@pytest.mark.asyncio
async def test_fixed_step_recipe_documents_the_actual_unqualified_outcomes() -> None:
    recipe = " ".join((DOCS / "recipes.md").read_text(encoding="utf-8").split())
    migration = " ".join(MIGRATION.read_text(encoding="utf-8").split())

    report, error = await _fixed_step_outcome({0: [1, 2], 2: [3, 4], 4: []})
    assert error is None
    assert report is not None
    assert report.assurance is b24api.TraversalAssurance.MECHANICS_ONLY
    assert "`mechanics_only` assurance" in recipe

    report, error = await _fixed_step_outcome({0: [1, 2], 2: [3], 4: []})
    assert error is not None
    assert report is not None
    assert not report.exhausted
    assert str(error.__cause__) in recipe
    assert "no longer accepts an empty page after a short page as closure" in migration
