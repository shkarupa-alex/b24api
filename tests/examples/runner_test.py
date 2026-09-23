"""The shared recipe runner emits bound structured evidence and fails closed for LIVE."""

from __future__ import annotations
import base64
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from examples import run as recipe_runner
from examples._support.evidence import RecipeEvidence

ROOT = Path(__file__).resolve().parents[2]
LIVE_SKIPPED = 3
SCENARIO_COUNT = 19
LIVE_SIGNING_KEY = Ed25519PrivateKey.generate()
LIVE_PUBLIC_KEY = LIVE_SIGNING_KEY.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)


def _offline_record(scenario: int = 1) -> dict[str, object]:
    result = subprocess.run(  # noqa: S603 - fixed interpreter and repository module
        [sys.executable, "-m", "examples.run", "--scenario", str(scenario)],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def _live_record() -> dict[str, object]:
    record = _offline_record()
    physical_requests = record["physical_requests"]
    logical_requests = record["logical_requests"]
    assert isinstance(physical_requests, int)
    assert isinstance(logical_requests, int)
    requests = [
        {
            "method": f"captured.method.{index}",
            "http_status": 200,
            "logical_requests": logical_requests - physical_requests + 1 if index == 0 else 1,
            "response_sha256": hashlib.sha256(f"response-{index}".encode()).hexdigest(),
        }
        for index in range(physical_requests)
    ]
    fixture_id = "disposable-portal-fixture-1"
    record.update(
        {
            "provenance": "live",
            "fixture_id": fixture_id,
            "capture": {
                "kind": "b24api-live-capture-v1",
                "captured_at": "2026-09-23T12:00:00+00:00",
                "portal_fingerprint": hashlib.sha256(b"disposable-portal").hexdigest(),
                "fixture_id": fixture_id,
                "requests": requests,
            },
        },
    )
    signature = LIVE_SIGNING_KEY.sign(recipe_runner._live_attestation_payload(record))  # noqa: SLF001
    record["attestation"] = f"ed25519:{base64.b64encode(signature).decode()}"
    return record


def _attest(record: dict[str, object]) -> None:
    signature = LIVE_SIGNING_KEY.sign(recipe_runner._live_attestation_payload(record))  # noqa: SLF001
    record["attestation"] = f"ed25519:{base64.b64encode(signature).decode()}"


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
        "assurance",
        "active_references_high_water",
        "buffered_commands_high_water",
        "buffered_rows_high_water",
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
    assert summary["assurance"] == "bounded_prefix"
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


def test_live_runner_accepts_only_a_capture_from_the_pinned_recorder(monkeypatch: pytest.MonkeyPatch) -> None:
    record = _live_record()
    monkeypatch.setattr(recipe_runner, "LIVE_RECORDER_PUBLIC_KEY", LIVE_PUBLIC_KEY)

    assert (
        recipe_runner._validate_live_record(  # noqa: SLF001
            record,
            recipe_runner.SCENARIOS[0],
            str(record["client_sha"]),
        )
        == record
    )


@pytest.mark.parametrize(
    ("method", "logical_requests"),
    [("batch", 51), ("profile", 2)],
)
def test_live_runner_rejects_signed_physically_impossible_request_counts(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    logical_requests: int,
) -> None:
    record = _live_record()
    capture = record["capture"]
    assert isinstance(capture, dict)
    requests = capture["requests"]
    assert isinstance(requests, list)
    request = requests[0]
    assert isinstance(request, dict)
    request.update(method=method, logical_requests=logical_requests)
    record["logical_requests"] = sum(item["logical_requests"] for item in requests if isinstance(item, dict))
    _attest(record)
    monkeypatch.setattr(recipe_runner, "LIVE_RECORDER_PUBLIC_KEY", LIVE_PUBLIC_KEY)

    with pytest.raises(ValueError, match="impossible request capture"):
        recipe_runner._validate_live_record(  # noqa: SLF001
            record,
            recipe_runner.SCENARIOS[0],
            str(record["client_sha"]),
        )


@pytest.mark.parametrize(("method", "logical_requests"), [("batch", 50), ("profile", 1)])
def test_live_runner_accepts_signed_feasible_request_counts(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    logical_requests: int,
) -> None:
    record = _live_record()
    capture = record["capture"]
    assert isinstance(capture, dict)
    requests = capture["requests"]
    assert isinstance(requests, list)
    request = requests[0]
    assert isinstance(request, dict)
    request.update(method=method, logical_requests=logical_requests)
    record["logical_requests"] = sum(item["logical_requests"] for item in requests if isinstance(item, dict))
    _attest(record)
    monkeypatch.setattr(recipe_runner, "LIVE_RECORDER_PUBLIC_KEY", LIVE_PUBLIC_KEY)

    assert (
        recipe_runner._validate_live_record(  # noqa: SLF001
            record,
            recipe_runner.SCENARIOS[0],
            str(record["client_sha"]),
        )
        == record
    )


def test_live_runner_rejects_capture_signed_by_the_caller(tmp_path: Path) -> None:
    record = _live_record()
    evidence = tmp_path / "caller-signed.jsonl"
    evidence.write_text(json.dumps(record) + "\n")

    result = subprocess.run(  # noqa: S603 - fixed interpreter and repository module
        [sys.executable, "-m", "examples.run", "--scenario", "1", "--live-evidence", str(evidence)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "no valid capture attestation" in result.stderr


def test_live_runner_rejects_a_relabelled_offline_summary(tmp_path: Path) -> None:
    record = _offline_record()
    record.update({"provenance": "live", "fixture_id": "disposable-portal-fixture-1"})
    evidence = tmp_path / "relabelled.jsonl"
    evidence.write_text(json.dumps(record) + "\n")

    result = subprocess.run(  # noqa: S603 - fixed interpreter and repository module
        [sys.executable, "-m", "examples.run", "--scenario", "1", "--live-evidence", str(evidence)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "missing required fields" in result.stderr


def test_live_runner_rejects_capture_modified_after_attestation(tmp_path: Path) -> None:
    record = _live_record()
    capture = record["capture"]
    assert isinstance(capture, dict)
    requests = capture["requests"]
    assert isinstance(requests, list)
    request = requests[0]
    assert isinstance(request, dict)
    request["method"] = "relabeled.offline.method"
    evidence = tmp_path / "tampered.jsonl"
    evidence.write_text(json.dumps(record) + "\n")

    result = subprocess.run(  # noqa: S603 - fixed interpreter and repository module
        [sys.executable, "-m", "examples.run", "--scenario", "1", "--live-evidence", str(evidence)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "no valid capture attestation" in result.stderr


@pytest.mark.asyncio
async def test_offline_runner_rejects_a_recipe_result_that_disagrees_with_the_oracle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = __import__("examples.chat_bounded_mirror", fromlist=["run"])

    async def contradictory_run() -> RecipeEvidence:
        return RecipeEvidence(0)

    monkeypatch.setattr(module, "run", contradictory_run)
    with pytest.raises(AssertionError, match="observed 0 rows, expected 11"):
        await recipe_runner._offline(recipe_runner.SCENARIOS[0])  # noqa: SLF001 - direct contract test


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("method_card_sha", "0" * 40, "not bound"),
        ("observed_count", 0, "independent oracle"),
        ("physical_requests", 0, "no request evidence"),
        ("report_state", "incomplete", "wrong report state"),
        ("assurance", "made_up", "wrong assurance"),
    ],
)
def test_live_runner_rejects_contradictory_passing_evidence(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    record = _live_record()
    record[field] = value
    evidence = tmp_path / "evidence.jsonl"
    evidence.write_text(json.dumps(record) + "\n")

    result = subprocess.run(  # noqa: S603 - fixed interpreter and repository module
        [sys.executable, "-m", "examples.run", "--scenario", "1", "--live-evidence", str(evidence)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert message in result.stderr


def test_live_runner_rejects_duplicate_and_wrong_selected_scenario(tmp_path: Path) -> None:
    record = _live_record()
    duplicate = tmp_path / "duplicate.jsonl"
    duplicate.write_text(f"{json.dumps(record)}\n{json.dumps(record)}\n")
    wrong = tmp_path / "wrong.jsonl"
    wrong.write_text(json.dumps({**record, "scenario": 2}) + "\n")

    duplicated = subprocess.run(  # noqa: S603 - fixed interpreter and repository module
        [sys.executable, "-m", "examples.run", "--scenario", "1", "--live-evidence", str(duplicate)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    selected = subprocess.run(  # noqa: S603 - fixed interpreter and repository module
        [sys.executable, "-m", "examples.run", "--scenario", "1", "--live-evidence", str(wrong)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert duplicated.returncode != 0
    assert "duplicate scenario" in duplicated.stderr
    assert selected.returncode != 0
    assert "missing required fields" in selected.stderr


def test_all_offline_recipes_emit_measured_passing_evidence() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "examples.run", "--scenario", "all"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    records = [json.loads(line) for line in result.stdout.splitlines()]

    assert result.returncode == 0, result.stderr
    assert len(records) == SCENARIO_COUNT
    assert all(record["expected_count"] == record["observed_count"] for record in records)
    assert all(record["status"] == "passed" for record in records)
