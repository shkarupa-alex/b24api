"""Integrity checks for the compact maintained documentation set."""

from __future__ import annotations
import ast
import re
import tomllib
from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace
from typing import Self, cast

import pytest
from packaging.requirements import Requirement

import b24api
import b24api.contracts
from tests.real_signature import real_signature
from tests.scripting import ResponderTransport, client_for

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
DOCS = ROOT / "docs"
MIGRATION = DOCS / "migration.md"
PYTHON_BLOCK = re.compile(r"```python\n(.*?)\n```", re.DOTALL)


_MARKDOWN_LINK = re.compile(r"\]\((?P<target>[^)\s]+)\)")
# README links are absolute so they also work on PyPI (B19); these prefixes name files of this repository.
_REPOSITORY_URLS = (
    "https://github.com/shkarupa-alex/b24api/blob/master/",
    "https://raw.githubusercontent.com/shkarupa-alex/b24api/master/",
)


def _local_links(path: Path) -> set[Path]:
    """Return the repository files a Markdown document links to, ignoring URLs and anchors."""
    targets = set()
    for match in _MARKDOWN_LINK.finditer(path.read_text(encoding="utf-8")):
        target = match["target"].split("#", 1)[0]
        repository = next(
            (target.removeprefix(prefix) for prefix in _REPOSITORY_URLS if target.startswith(prefix)), None
        )
        if repository is not None:
            targets.add((ROOT / repository).resolve())
        elif target and "://" not in target and not target.startswith("mailto:"):
            targets.add((path.parent / target).resolve())
    return targets


def test_every_local_documentation_link_resolves() -> None:
    for path in (README, *sorted(DOCS.glob("*.md"))):
        for target in _local_links(path):
            assert target.exists(), f"{path.relative_to(ROOT)} links to missing {target}"


def test_readme_links_are_absolute_so_they_work_on_pypi() -> None:
    relative = [
        match["target"]
        for match in _MARKDOWN_LINK.finditer(README.read_text(encoding="utf-8"))
        if "://" not in match["target"] and not match["target"].startswith(("#", "mailto:"))
    ]

    assert relative == []


def test_every_maintained_doc_is_linked_and_specifications_are_an_archive() -> None:
    linked = _local_links(README)
    for path in DOCS.glob("*.md"):
        linked |= _local_links(path)
    for path in sorted(DOCS.rglob("*")):
        if not path.is_file() or path.is_relative_to(DOCS / "specifications"):
            continue
        assert path.resolve() in linked, f"{path.relative_to(ROOT)} is neither linked nor archived"
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
    paragraphs = [" ".join(paragraph.split()) for paragraph in text.split("\n\n")]
    bounded = next(paragraph for paragraph in paragraphs if "BoundedIdentityRange" in paragraph)
    assert "SequentialKeysetExecution" in bounded
    assert "consumes that boundary" in bounded
    assert "RangeKeysetExecution`, `PartitionedKeysetExecution`, and auto execution reject" in bounded
    reference = next(paragraph for paragraph in paragraphs if "per-reference `IncompleteTraversalError`" in paragraph)
    assert "`report=None`" in reference
    assert "partial_rows" in reference


# Each review ID a "Breaking (3.0.0)" release note names -> the migration items that tell the caller what to
# change. The guide carries no internal IDs, so the items are matched by their bold titles.
_BREAKING_MIGRATION_ITEMS = {
    "C7": ("Root imports.",),
    "A2": ("Compressed responses.",),
    "A3": ("Possibly accepted batches are not replayed.", "Permanent transport refusals."),
    "A13": ("Exceptions from an injected transport.",),
    "B29": ("Oversized responses.",),
    "A7": ("Error text.",),
    "A9": ("Mid-collection start with an exact total.",),
    "A10": ("Keyset verification.",),
    "A11": ("Stream lifecycle.",),
    "A12": ("A logical batch closed early says so.",),
    "A14": ("Reports name the public failure.",),
    "B10": ("Fixed step refuses at once.",),
    "B11": ("Removed report vocabulary.",),
    "B12": ("Removed report vocabulary.",),
    "B8": ("`EnvelopeContractError` is a `ProtocolError`.",),
    "B20": ("`EnvelopeContractError` is a `ProtocolError`.",),
}


def test_every_breaking_release_note_has_a_migration_item() -> None:
    notes = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8").split("## Unreleased (3.0.0)")[1].split("\n## ")[0]
    breaking = {
        identifier
        for bullet in re.split(r"\n- ", f"\n{notes}")
        if bullet.startswith("**Breaking (3.0.0):**")
        for identifier in re.findall(r"\(([A-D]\d+)\)", bullet)
    }
    upgrade = MIGRATION.read_text(encoding="utf-8").split("## Upgrading from 2.3 to 3.0")[1].split("\n## ")[0]
    titles = set(re.findall(r"^\s*\d+\. \*\*(.+?)\*\*", upgrade, re.MULTILINE))

    assert breaking == set(_BREAKING_MIGRATION_ITEMS)
    for identifier, items in _BREAKING_MIGRATION_ITEMS.items():
        assert set(items) <= titles, f"{identifier}: {sorted(set(items) - titles)}"


@pytest.mark.parametrize("name", ["h2", "hpack"])
def test_release_notes_and_migration_guide_state_the_hpack_stack_bounds(name: str) -> None:
    # 3.0.0 made both direct requirements (C2); a consumer pinned outside them learns it before resolving.
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    line = next(dependency for dependency in project["project"]["dependencies"] if Requirement(dependency).name == name)
    notes = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8").split("## Unreleased (3.0.0)")[1].split("\n## ")[0]

    assert f"`{line}`" in notes
    assert f"`{line}`" in MIGRATION.read_text(encoding="utf-8")


# Report fields and enum members 3.0.0 removed (B11, B12); only the migration guide, which describes their
# removal, and the release notes may still name them.
_REMOVED_REPORT_VOCABULARY = (
    "canary_requests",
    "canary_commands",
    "canary_rows",
    "canary counters",
    "CANARY_VERIFIED_BOUNDS",
    "identity_digest",
    "SCHEDULER_STOPPED",
    "REPLAYED_DIRECT",
    "ORACLE_VERIFIED",
)


def test_maintained_documentation_does_not_describe_removed_report_vocabulary_as_current() -> None:
    for path in (README, *sorted(DOCS.glob("*.md"))):
        if path == MIGRATION:
            continue
        text = path.read_text(encoding="utf-8")
        for term in _REMOVED_REPORT_VOCABULARY:
            assert term not in text, f"{path.relative_to(ROOT)} names removed {term}"


def test_every_test_the_review_outcome_registry_cites_exists() -> None:
    # The registry is the evidence map of the astra-fable review; a citation of a missing or renamed test
    # would claim evidence that nothing runs. ``::name`` continues the file named earlier on the same line.
    registry = DOCS / "specifications" / "review-astra-fable" / "outcomes.md"
    citation = re.compile(r"`(?P<path>(?:tests|tools)/[\w/]+\.py)?::(?P<name>test_\w+)`")
    missing: list[str] = []
    cited = 0
    for line in registry.read_text(encoding="utf-8").splitlines():
        path: str | None = None
        for match in citation.finditer(line):
            path = match.group("path") or path
            assert path is not None, f"continuation without a file: {line}"
            functions = {
                node.name
                for node in ast.walk(ast.parse((ROOT / path).read_text(encoding="utf-8")))
                if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
            }
            cited += 1
            if match.group("name") not in functions:
                missing.append(f"{path}::{match.group('name')}")
    assert cited > 0
    assert missing == []


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
    """No-I/O stand-in whose calls are bound against the real ``Bitrix24`` methods."""

    @real_signature
    def iter_list_keyset(self, *_args: object, **kwargs: object) -> dict[str, object]:
        return kwargs

    @real_signature
    def iter_cursors(self, *_args: object, **_kwargs: object) -> _EmptyStream:
        return _EmptyStream()

    @real_signature
    async def verify_keyset_capability(self, *_args: object, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(verdict="verified")


@pytest.mark.asyncio
async def test_migration_python_examples_execute_without_io() -> None:
    blocks = PYTHON_BLOCK.findall(MIGRATION.read_text(encoding="utf-8"))
    assert blocks
    for source in blocks:
        client = _MigrationClient()
        namespace: dict[str, object] = {
            "AdaptedPage": b24api.contracts.AdaptedPage,
            "Binding": b24api.Binding,
            "PageView": b24api.contracts.PageView,
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


def _fixed_step(pages: dict[int, list[int]]) -> ResponderTransport:
    return ResponderTransport(
        lambda request: {"result": [{"ID": value} for value in pages[request.copy_parameters().get("start", 0)]]}
    )


_Outcome = tuple[b24api.OperationReport | None, BaseException | None]


async def _fixed_step_outcome(pages: dict[int, list[int]]) -> _Outcome:
    client = client_for(_fixed_step(pages))
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
