"""Integrity checks for the compact maintained documentation set."""

from __future__ import annotations
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
MIGRATION = ROOT / "MIGRATION.md"
DOCS = ROOT / "docs"


def test_docs_are_flat_compact_and_linked_from_readme() -> None:
    tracked_docs = sorted(path.relative_to(ROOT).as_posix() for path in DOCS.rglob("*") if path.is_file())

    assert tracked_docs == ["docs/ARCHITECTURE.md", "docs/PERFORMANCE.md"]
    text = README.read_text(encoding="utf-8")
    assert "docs/ARCHITECTURE.md" in text
    assert "docs/PERFORMANCE.md" in text
    assert "MIGRATION.md" in text
    assert MIGRATION.is_file()


def test_user_documentation_contains_no_internal_issue_identifiers() -> None:
    text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (README, MIGRATION, DOCS / "ARCHITECTURE.md", DOCS / "PERFORMANCE.md")
    )

    assert re.search(r"\b[BC]\d+[a-z]?\b", text) is None


def test_architecture_document_names_the_complete_public_capability_family() -> None:
    text = (DOCS / "ARCHITECTURE.md").read_text(encoding="utf-8")

    for operation in (
        "call()",
        "call_response()",
        "batch()",
        "fan_out()",
        "iter_list()",
        "iter_list_counted()",
        "iter_list_keyset()",
        "iter_list_cursor()",
        "iter_references()",
    ):
        assert operation in text
