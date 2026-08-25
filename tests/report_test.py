"""Integrity checks for the final capability and architecture report."""

from __future__ import annotations
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "docs/bitrix24-client-2.0/w13/capability-architecture-report.md"
TEST_REFERENCE = re.compile(r"`(test_[a-zA-Z0-9_]+)`")
EXPECTED_MATRIX_LINES = 6


def _section(text: str, heading: str, next_heading: str) -> str:
    return text.split(heading, maxsplit=1)[1].split(next_heading, maxsplit=1)[0]


def test_final_report_has_exact_five_way_matrix_and_empty_blocking_lists() -> None:
    text = REPORT.read_text(encoding="utf-8")
    matrix = _section(text, "## Five-way business capability matrix", "## Detailed capability-preservation ledger")
    rows = [line for line in matrix.splitlines() if line.startswith("| ")]

    assert len(rows) == EXPECTED_MATRIX_LINES  # header and exactly five business rows
    assert "missing=[]" in text
    assert "regressed=[]" in text
    assert "| Yes |" not in _section(
        text,
        "## Detailed capability-preservation ledger",
        "## Sampled issue traceability",
    )


def test_final_report_names_all_sampled_issues_and_resolves_every_test_reference() -> None:
    text = REPORT.read_text(encoding="utf-8")
    issues = _section(text, "## Sampled issue traceability", "## Business failure ownership")
    for issue in ("B1", "B2", "B3", "B4", "B5", "C1b", "C5", "C34", "C35"):
        assert f"| {issue}" in issues

    test_corpus = "\n".join(path.read_text(encoding="utf-8") for path in ROOT.rglob("*_test.py"))
    for test_name in TEST_REFERENCE.findall(text):
        assert re.search(rf"^(?:async )?def {re.escape(test_name)}\b", test_corpus, re.MULTILINE)
