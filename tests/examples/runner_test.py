"""The shared recipe runner emits bound structured evidence and fails closed for LIVE."""

from __future__ import annotations
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LIVE_SKIPPED = 3


def test_offline_runner_emits_required_structured_summary() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "examples.run", "--scenario", "1"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    summary = json.loads(result.stdout)
    assert set(summary) == {
        "client_sha",
        "expected_count",
        "logical_requests",
        "method_card_sha",
        "observed_count",
        "physical_requests",
        "provenance",
        "report_state",
        "scenario",
        "status",
    }
    assert summary["scenario"] == 1
    assert summary["expected_count"] == summary["observed_count"]
    assert summary["physical_requests"] > 0
    assert summary["provenance"] == "fixture"
    assert summary["status"] == "passed"


def test_live_runner_never_promotes_missing_evidence_to_success(tmp_path: Path) -> None:
    result = subprocess.run(  # noqa: S603 - fixed interpreter and repository module
        [sys.executable, "-m", "examples.run", "--scenario", "1", "--live-evidence", str(tmp_path / "missing")],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == LIVE_SKIPPED
    summary = json.loads(result.stdout)
    assert summary["provenance"] == "live"
    assert summary["status"] == "skipped"
    assert summary["reason"]
