"""Regression gates for W9 evidence contracts and offline execution."""

from __future__ import annotations
import base64
import copy
import json
import os
import subprocess
import sys
import uuid
import zipfile
from argparse import Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

import pytest

from . import cli as cli_module
from .contracts import (
    FINGERPRINT_ALGORITHM,
    FINGERPRINT_KEY_FORMAT,
    REVIEWED_MAX_ENTITIES_PER_CELL,
    REVIEWED_PROFILE_SET_ID,
    REVIEWED_PROFILE_SET_SHA256,
    SCHEMA_VERSION,
    ContractError,
    ExitCode,
    ManifestLineage,
    PortalIdentity,
    SecretLeakError,
    append_manifest_record,
    build_manifest_record,
    content_sha256,
    derive_drift_controls,
    git_sha,
    load_manifest,
    marker_sha256,
    marker_value,
    parse_fingerprint_key,
    portal_identity,
    scan_bytes_for_secrets,
    strict_json_loads,
    validate_benchmark_plan,
    validate_dataset_plan,
    validate_evidence_artifact,
    validate_manifest_against_plan,
    validate_manifest_record,
    validate_oracle_record,
    validate_probe_envelope,
    validate_reviewed_profile_set,
)
from .live import ADAPTERS, LivePreflight
from .model import DeterministicPortal, exact_model_cases, run_exact_matrix_sync

if TYPE_CHECKING:
    from collections.abc import Callable

ROOT = Path(__file__).resolve().parents[3]
ENTRYPOINT = ROOT / "tools/b24api_evidence.py"
PROFILE_SET = ROOT / "docs/bitrix24-client-2.0/w0/disposable-entity-profiles.json"
RUN_ID = "00000000-0000-4000-8000-000000000011"
LINEAGE_ID = "00000000-0000-4000-8000-000000000012"
SHA = git_sha(ROOT)
SHA256 = "2" * 64
FINGERPRINT_KEY_BYTES = 32
LARGE_CASE_ROWS = 10_000
SPARSE_BASE_MINIMUM = 100_000
EXPECTED_MUTATION_RETRIES = 3
EXPECTED_SCHEMA_COUNT = 6
LEAK_FIXTURE = b"https://example.invalid/rest/1/realisticToken123/"


def _plan(*, count: int = 5) -> dict[str, Any]:
    profile = validate_reviewed_profile_set(PROFILE_SET)["profiles"][0]
    requests = count * 5 + 4 if count else 0
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": RUN_ID,
        "lineage_id": LINEAGE_ID,
        "candidate_sha": SHA,
        "disposable_profile_set_id": REVIEWED_PROFILE_SET_ID,
        "disposable_profiles_content_hash": REVIEWED_PROFILE_SET_SHA256,
        "portal": {
            "host": "model.local",
            "role": "model",
            "fingerprint": SHA256,
            "fingerprint_algorithm": "sha256-public-model-v1",
            "fingerprint_key_format": "not_applicable_model",
            "build": "model",
            "scope_hash": SHA256,
        },
        "namespace": f"b24api-evidence-{RUN_ID}",
        "cells": [
            {
                "id": "CELL",
                "disposable_profile_id": profile["id"],
                "entity_family": profile["entity_family"],
                "target_count": count,
                "base_count": 0,
                "relationship_count": 0,
                "distribution": "boundary" if count else "empty",
                "marker_field": profile["marker_field"],
                "create_method": profile["create_method"],
                "read_method": profile["read_method"],
                "delete_method": profile["delete_method"],
                "required_scopes": profile["required_scopes"],
            },
        ],
        "estimated": {
            "entities": count,
            "relationships": 0,
            "create_strategy": "direct",
            "delete_strategy": "direct",
            "requests": requests,
            "batch_commands": 0,
            "duration_seconds": requests * 0.1,
            "quota_impact": float(requests),
        },
        "cleanup": {
            "feasible": True,
            "dependency_order": ["CELL"],
            "absence_verification": "exact_id_point_read",
        },
        "authorization": {
            "state": "preview",
            "live": False,
            "allow_writes": False,
            "plan_review_sha": None,
            "approved_by_user": False,
            "max_entities_per_cell": REVIEWED_MAX_ENTITIES_PER_CELL,
        },
    }


def _approved_live_plan(*, count: int = 1) -> dict[str, Any]:
    plan = _plan(count=count)
    plan["portal"].update(
        host="portal.invalid",
        role="admin_full",
        fingerprint=SHA256,
        fingerprint_algorithm=FINGERPRINT_ALGORITHM,
        fingerprint_key_format=FINGERPRINT_KEY_FORMAT,
        build="build-1",
        scope_hash=content_sha256(["task"]),
    )
    plan["authorization"].update(
        state="approved_for_seed",
        live=True,
        allow_writes=True,
        approved_by_user=True,
        plan_review_sha=SHA,
        approved_at="2026-08-20T00:00:00+00:00",
    )
    validate_dataset_plan(plan)
    return plan


def _manifest_base(plan: dict[str, Any]) -> dict[str, Any]:
    correlation = content_sha256([RUN_ID, "CELL", 0])
    marker = marker_value(plan["namespace"], correlation)
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": RUN_ID,
        "lineage_id": LINEAGE_ID,
        "dataset_plan_content_hash": content_sha256(plan),
        "portal_fingerprint": SHA256,
        "candidate_sha": SHA,
        "namespace": plan["namespace"],
        "event": "planned",
        "cell_id": "CELL",
        "entity_family": "task",
        "correlation_key": correlation,
        "entity_id": None,
        "marker_hash": marker_sha256(marker),
        "marker_value": marker,
        "request_fingerprint": None,
        "recorded_at": "2026-08-20T00:00:00Z",
        "safe_error": None,
        "parent_correlation_keys": [],
    }


def _benchmark_plan(*, state: str = "admission_ready") -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "plan_id": "00000000-0000-4000-8000-000000000013",
        "lineage_id": LINEAGE_ID,
        "admission_state": state,
        "thresholds_normative": state == "admission_ready",
        "candidate_sha": SHA,
        "dataset_plan_content_hash": SHA256,
        "controls": {
            "warmups": 1,
            "advisory_runs": 5,
            "blocking_pairs": 9,
            "interleaving": True,
            "drift": {"status": "preregistered", "max_rtt_ratio": 1.2, "max_operating_ratio": 1.2},
        },
        "cases": [
            {
                "id": "MODEL-MATRIX",
                "compared_plans": ["baseline", "candidate"],
                "benefit_gate": {
                    "blocking": True,
                    "minimum_median_improvement": 0.15,
                    "paired_95_interval_excludes_parity": True,
                    "maximum_small_p95_ratio": 1.05,
                    "maximum_server_operating_ratio": 1.1,
                },
            },
        ],
    }


def _benchmark_artifact() -> dict[str, Any]:
    plan = _plan(count=0)
    controls = derive_drift_controls(
        rtt_before=0.01,
        rtt_after=0.011,
        operating_before=0.02,
        operating_after=0.021,
        max_rtt_ratio=1.2,
        max_operating_ratio=1.2,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": RUN_ID,
        "lineage_id": LINEAGE_ID,
        "portal_fingerprint": SHA256,
        "host": "model.local",
        "candidate_sha": SHA,
        "command": "benchmark",
        "phase": "complete",
        "case_id": "MODEL-MATRIX",
        "manifest_content_hash": None,
        "profile_versions": [REVIEWED_PROFILE_SET_ID],
        "plan_versions": [SCHEMA_VERSION],
        "runtime": {"python": "3.12.10", "b24api": "test", "httpx": "0.28.1"},
        "started_at": "2026-08-20T00:00:00Z",
        "finished_at": "2026-08-20T00:00:01Z",
        "outcome": "PASS",
        "terminal_state": "completed",
        "assurance": "oracle_verified",
        "snapshot_requirement": "traversal_only",
        "snapshot_state": "not_requested",
        "benchmark_plan_content_hash": SHA256,
        "controls": controls,
        "metrics": {
            "kind": "benchmark",
            "http_attempts": 1,
            "logical_pages": 1,
            "batch_requests": 0,
            "batch_commands": 0,
            "time_to_first_row_seconds": 0.001,
            "wall_seconds": 0.01,
            "server_operating_seconds": 0.001,
            "retries": 0,
            "cooldown_seconds": 0.0,
            "buffered_rows_high_water": 1,
            "rss_delta_bytes": None,
            "raw_rows": 10,
            "unique_rows": 9,
            "overlap": 9,
            "duplicates": 1,
            "shortfall": 0,
            "overfetch": 0,
            "reference_failures": 0,
        },
        "safe_violations": [],
        "evidence_refs": [],
        "dataset_plan_content_hash": content_sha256(plan),
    }


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity", "1e400", "-1e400"])
def test_strict_json_rejects_every_nonfinite_form(literal: str) -> None:
    with pytest.raises(ContractError, match="non-finite"):
        strict_json_loads(f'{{"value":{literal}}}')


def test_strict_json_rejects_duplicate_object_keys() -> None:
    with pytest.raises(ContractError, match="duplicate"):
        strict_json_loads('{"value":1,"value":2}')


@pytest.mark.parametrize(
    "raw",
    ["[" * 1_500 + "]" * 1_500, '{"value":' + "9" * 5_000 + "}"],
)
def test_strict_json_wraps_parser_resource_failures_as_invalid_contract(raw: str) -> None:
    with pytest.raises(ContractError, match="invalid JSON"):
        strict_json_loads(raw)


def test_fingerprint_requires_exact_random_key_shape_and_hides_principal() -> None:
    key = base64.urlsafe_b64encode(bytes(range(FINGERPRINT_KEY_BYTES))).decode().rstrip("=")
    assert len(parse_fingerprint_key(key)) == FINGERPRINT_KEY_BYTES
    identity = portal_identity(
        "https://portal.invalid/rest/13/not-a-real-token/",
        role="employee_full",
        fingerprint_key=key,
    )
    assert identity.host == "portal.invalid"
    assert identity.principal_id == "13"
    assert (
        identity.fingerprint
        == portal_identity(
            "https://portal.invalid/rest/13/different-dummy-token/",
            role="employee_full",
            fingerprint_key=key,
        ).fingerprint
    )
    assert "13" not in identity.fingerprint
    with pytest.raises(ContractError, match="43-character"):
        parse_fingerprint_key("A")
    with pytest.raises(ContractError, match="diversity safety"):
        parse_fingerprint_key("A" * 43)
    with pytest.raises(ContractError, match="HTTPS"):
        portal_identity(
            "https://user:password@portal.invalid/rest/13/not-a-real-token/",
            role="employee_full",
            fingerprint_key=key,
        )
    with pytest.raises(ContractError, match="invalid port"):
        portal_identity(
            "https://[" + "::1/rest/1/" + "abcdef",
            role="employee_full",
            fingerprint_key=key,
        )
    for invalid_host in (".bad.example", "bad_host", "-bad.example", "bad-.example", "[::1]"):
        with pytest.raises(ContractError, match="host"):
            portal_identity(
                f"https://{invalid_host}/rest/13/abcdef",
                role="employee_full",
                fingerprint_key=key,
            )


def test_reviewed_profile_set_is_anchored_by_id_and_immutable_hash(tmp_path: Path) -> None:
    loaded = validate_reviewed_profile_set(PROFILE_SET)
    assert loaded["profile_set_id"] == REVIEWED_PROFILE_SET_ID
    changed = tmp_path / "profiles.json"
    changed.write_text(PROFILE_SET.read_text().replace("tasks.task.add", "user.add"))
    with pytest.raises(ContractError, match="immutable SHA"):
        validate_reviewed_profile_set(changed)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda plan: plan.update(disposable_profiles_content_hash="f" * 64),
        lambda plan: plan["cells"][0].update(target_count=501),
        lambda plan: plan["authorization"].update(max_entities_per_cell=10**9),
        lambda plan: plan["cells"][0].update(create_method="user.add"),
        lambda plan: plan["cells"][0].update(required_scopes=["crm"]),
        lambda plan: plan["cells"][0].update(relationship_count=1),
        lambda plan: plan["cells"][0].update(distribution="reference_graph"),
        lambda plan: plan["cleanup"].update(dependency_order=[]),
        lambda plan: plan["estimated"].update(requests=0),
        lambda plan: plan["estimated"].update(batch_commands=1),
        lambda plan: plan["estimated"].update(duration_seconds=0),
        lambda plan: plan["estimated"].update(quota_impact=0),
    ],
)
def test_dataset_plan_rejects_self_authorized_or_underestimated_writes(
    mutation: Callable[[dict[str, Any]], None],
) -> None:
    plan = _plan()
    mutation(plan)
    with pytest.raises(ContractError):
        validate_dataset_plan(plan)


def test_manifest_hash_chain_marker_and_resume_lineage(tmp_path: Path) -> None:
    plan = _plan()
    first = build_manifest_record(_manifest_base(plan))
    path = tmp_path / "manifest.jsonl"
    append_manifest_record(path, first)
    second_base = {**_manifest_base(plan), "event": "verified", "entity_id": "42"}
    second = build_manifest_record(second_base, previous=first)
    append_manifest_record(path, second)
    lineage = ManifestLineage(RUN_ID, LINEAGE_ID, content_sha256(plan), SHA256, SHA, plan["namespace"])
    assert load_manifest(path, expected=lineage) == [first, second]
    mismatched = copy.deepcopy(second)
    mismatched["marker_hash"] = "f" * 64
    mismatched_without_hash = {key: value for key, value in mismatched.items() if key != "record_hash"}
    mismatched["record_hash"] = content_sha256(mismatched_without_hash)
    with pytest.raises(ContractError, match="marker"):
        validate_manifest_record(mismatched, previous=first)
    with pytest.raises(ContractError, match="run_id"):
        load_manifest(
            path,
            expected=ManifestLineage(
                str(uuid.uuid4()),
                LINEAGE_ID,
                content_sha256(plan),
                SHA256,
                SHA,
                plan["namespace"],
            ),
        )


def test_manifest_semantics_reject_forged_terminal_and_unknown_correlation() -> None:
    plan = _plan(count=1)
    forged = build_manifest_record({**_manifest_base(plan), "event": "verified", "entity_id": "42"})
    with pytest.raises(ContractError, match="transition"):
        validate_manifest_against_plan([forged], plan)
    unknown_marker = marker_value(plan["namespace"], "4" * 64)
    unknown = build_manifest_record(
        {
            **_manifest_base(plan),
            "correlation_key": "4" * 64,
            "marker_value": unknown_marker,
            "marker_hash": marker_sha256(unknown_marker),
        },
    )
    with pytest.raises(ContractError, match="not a member"):
        validate_manifest_against_plan([unknown], plan)


def test_manifest_rejects_a_stale_concurrent_genesis_writer(tmp_path: Path) -> None:
    plan = _plan()
    first = build_manifest_record(_manifest_base(plan))
    stale_marker = marker_value(plan["namespace"], "4" * 64)
    stale = build_manifest_record(
        {
            **_manifest_base(plan),
            "correlation_key": "4" * 64,
            "marker_value": stale_marker,
            "marker_hash": marker_sha256(stale_marker),
        },
    )
    path = tmp_path / "manifest.jsonl"
    append_manifest_record(path, first)
    with pytest.raises(ContractError, match="sequence"):
        append_manifest_record(path, stale)
    assert load_manifest(path) == [first]


@pytest.mark.parametrize("requirement", ["frozen_manifest", "independent_pre_post_oracle"])
def test_oracle_snapshot_pass_requires_equal_nonnull_hashes(requirement: str) -> None:
    oracle = {
        "schema_version": SCHEMA_VERSION,
        "run_id": RUN_ID,
        "lineage_id": LINEAGE_ID,
        "case_id": "MODEL-offset",
        "candidate_sha": SHA,
        "dataset_plan_content_hash": SHA256,
        "manifest_content_hash": SHA256,
        "expected_result_hash": SHA256,
        "actual_result_hash": SHA256,
        "outcome": "PASS",
        "qualification": "immutable_manifest",
        "snapshot_requirement": requirement,
        "snapshot_state": "verified",
        "pre_hash": SHA256,
        "post_hash": "3" * 64,
        "mutation_retries": 0,
        "raw_count": 1,
        "unique_count": 1,
    }
    with pytest.raises(ContractError, match="equal non-null"):
        validate_oracle_record(oracle)
    oracle.update(snapshot_state="changed", outcome="PASS", mutation_retries=3)
    with pytest.raises(ContractError):
        validate_oracle_record(oracle)
    oracle.update(snapshot_state="verified", outcome="PASS", pre_hash=SHA256, post_hash=SHA256)
    oracle.update(raw_count=0, unique_count=999)
    with pytest.raises(ContractError, match="unique_count"):
        validate_oracle_record(oracle)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda plan: plan["cases"][0]["benefit_gate"].update(minimum_median_improvement=0),
        lambda plan: plan["cases"][0]["benefit_gate"].update(paired_95_interval_excludes_parity=False),
        lambda plan: plan["cases"][0]["benefit_gate"].update(maximum_small_p95_ratio=999),
        lambda plan: plan["cases"][0]["benefit_gate"].update(maximum_server_operating_ratio=999),
        lambda plan: plan["cases"][0].update(compared_plans=["candidate"]),
        lambda plan: plan["controls"]["drift"].update(status="tbd_live"),
    ],
)
def test_admission_benchmark_gates_cannot_be_weakened(
    mutation: Callable[[dict[str, Any]], None],
) -> None:
    plan = _benchmark_plan()
    mutation(plan)
    with pytest.raises(ContractError):
        validate_benchmark_plan(plan)


def test_draft_thresholds_are_explicitly_nonnormative() -> None:
    plan = _benchmark_plan(state="draft")
    plan["controls"]["drift"] = {
        "status": "tbd_live",
        "max_rtt_ratio": "TBD-LIVE",
        "max_operating_ratio": "TBD-LIVE",
    }
    validate_benchmark_plan(plan)
    plan["thresholds_normative"] = True
    with pytest.raises(ContractError):
        validate_benchmark_plan(plan)


def test_benchmark_artifact_derives_drift_and_metric_algebra() -> None:
    artifact = _benchmark_artifact()
    validate_evidence_artifact(artifact)
    artifact["controls"]["rtt_ratio"] = 1.0
    with pytest.raises(ContractError, match="asserted instead of derived"):
        validate_evidence_artifact(artifact)
    artifact = _benchmark_artifact()
    artifact["metrics"]["duplicates"] = 999
    with pytest.raises(ContractError, match="duplicates"):
        validate_evidence_artifact(artifact)
    artifact = _benchmark_artifact()
    artifact["metrics"]["shortfall"] = 1
    with pytest.raises(ContractError, match="zero correctness"):
        validate_evidence_artifact(artifact)


def test_every_pass_rejects_blocking_violation_and_cleanup_requires_absence() -> None:
    artifact = _benchmark_artifact()
    artifact["command"] = "cleanup"
    artifact["controls"] = None
    artifact["metrics"] = {
        "kind": "operation",
        "http_attempts": 1,
        "wall_seconds": 0.01,
        "orphan_count": 0,
        "absence_verified": True,
        "orphan_refs": [],
    }
    artifact.pop("benchmark_plan_content_hash")
    artifact["safe_violations"] = [
        {"severity": "blocking", "code": "orphan", "message": "verified orphan", "field": None},
    ]
    with pytest.raises(ContractError, match="blocking"):
        validate_evidence_artifact(artifact)
    artifact["safe_violations"] = []
    artifact["metrics"].update(
        orphan_count=1,
        absence_verified=False,
        orphan_refs=[f"sha256:{SHA256}"],
    )
    with pytest.raises(ContractError, match="zero orphans"):
        validate_evidence_artifact(artifact)


@pytest.mark.parametrize(
    "result_error",
    [None, ["bad"], "bad", 1],
)
def test_probe_requires_present_php_aware_result_error_shape(
    result_error: list[str] | str | int | None,
) -> None:
    envelope: dict[str, Any] = {"result": {"dependent": [{"ID": "13"}]}}
    if result_error is not None:
        envelope["result_error"] = result_error
    with pytest.raises(ContractError):
        validate_probe_envelope(envelope, who_id="13")


def test_probe_requires_exact_one_dependent_identity() -> None:
    assert validate_probe_envelope(
        {"result": {"dependent": [{"ID": "13"}]}, "result_error": []},
        who_id="13",
    ) == {
        "result_error_shape": "empty_array",
        "dependent_ids": ["13"],
        "unexpected_error_keys": [],
        "matched": True,
    }
    assert not validate_probe_envelope(
        {"result": {"dependent": [{"ID": "13"}, {"ID": "14"}]}, "result_error": {}},
        who_id="13",
    )["matched"]
    assert not validate_probe_envelope(
        {"result": {"dependent": [{"ID": "13"}, {"junk": "x"}]}, "result_error": []},
        who_id="13",
    )["matched"]
    assert not validate_probe_envelope(
        {"result": {"dependent": [{"ID": "13"}], "unexpected": []}, "result_error": []},
        who_id="13",
    )["matched"]


def test_exact_model_matrix_covers_all_scales_and_expected_mutation() -> None:
    cases = exact_model_cases()
    assert {case.distribution for case in cases} >= {
        "empty",
        "single",
        "boundary",
        "dense",
        "uniform_sparse",
        "clustered_sparse",
        "skewed",
        "deleted_id",
        "mutation",
    }
    assert any(len(case.identities) == LARGE_CASE_ROWS and case.base_count > SPARSE_BASE_MINIMUM for case in cases)
    runs = run_exact_matrix_sync()
    assert len(runs) == len(cases) * 2
    assert all(run.actual_hash == run.expected_hash for run in runs)
    assert {run.outcome for run in runs if run.case_id == "mutation"} == {"INCONCLUSIVE"}
    assert all(run.pre_hash != run.post_hash for run in runs if run.case_id == "mutation")
    assert all(run.pre_hash == run.post_hash for run in runs if run.case_id != "mutation")
    assert all(run.mutation_retries == EXPECTED_MUTATION_RETRIES for run in runs if run.case_id == "mutation")
    mutation = next(case for case in cases if case.mutation)
    portal = DeterministicPortal(mutation)
    snapshots = [portal.oracle_snapshot() for _ in range(EXPECTED_MUTATION_RETRIES + 1)]
    assert len(set(snapshots)) == EXPECTED_MUTATION_RETRIES + 1


def test_schema_documents_are_finite_draft_2020_12_objects() -> None:
    schema_dir = ROOT / "tools/b24api_evidence/schemas"
    schemas = sorted(schema_dir.glob("*.schema.json"))
    assert len(schemas) == EXPECTED_SCHEMA_COUNT
    for path in schemas:
        schema = strict_json_loads(path.read_bytes())
        assert isinstance(schema, dict)
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert schema["additionalProperties"] is False


def test_secret_scanner_rejects_realistic_forms_without_echoing_value() -> None:
    with pytest.raises(SecretLeakError) as captured:
        scan_bytes_for_secrets(LEAK_FIXTURE, source="artifact.json")
    assert "realisticToken123" not in str(captured.value)
    for escaped in (
        b'"https:\\/\\/example.invalid\\/rest\\/1\\/realisticToken123\\/"',
        b'"https:\\u002f\\u002fexample.invalid\\u002frest\\u002f1\\u002frealisticToken123\\u002f"',
    ):
        with pytest.raises(SecretLeakError):
            scan_bytes_for_secrets(escaped, source="artifact.json")
    scan_bytes_for_secrets(b"https://bitrix24.com/rest/0/test/", source="fixture")


def test_cli_plan_and_benchmark_are_offline_and_live_writes_are_flag_gated(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "artifacts"
    environment = dict(os.environ)
    environment.pop("BITRIX24_API_WEBHOOK_URL", None)
    environment.pop("BITRIX24_EVIDENCE_FINGERPRINT_KEY", None)
    plan = _run_cli(
        "plan",
        "--artifact-dir",
        str(artifact_dir),
        "--run-id",
        RUN_ID,
        "--lineage-id",
        LINEAGE_ID,
        environment=environment,
    )
    assert plan.returncode == 0
    benchmark = _run_cli(
        "benchmark",
        "--artifact-dir",
        str(artifact_dir),
        "--plan",
        str(artifact_dir / "dataset-plan.json"),
        environment=environment,
    )
    assert benchmark.returncode == 0
    seed = _run_cli("seed", "--artifact-dir", str(artifact_dir), environment=environment)
    cleanup = _run_cli("cleanup", "--artifact-dir", str(artifact_dir), environment=environment)
    assert seed.returncode == cleanup.returncode == ExitCode.INVALID
    assert "requires both --live and --allow-writes" in seed.stderr


def test_every_live_command_refuses_under_ordinary_pytest(tmp_path: Path) -> None:
    environment = {**os.environ, "PYTEST_CURRENT_TEST": "ordinary::test"}
    result = _run_cli("plan", "--artifact-dir", str(tmp_path), "--live", environment=environment)
    assert result.returncode == ExitCode.INVALID
    assert "forbidden under ordinary pytest" in result.stderr


def test_live_empty_plan_refuses_before_credential_setup(tmp_path: Path) -> None:
    environment = dict(os.environ)
    environment.pop("PYTEST_CURRENT_TEST", None)
    environment.pop("BITRIX24_API_WEBHOOK_URL", None)
    environment.pop("BITRIX24_EVIDENCE_FINGERPRINT_KEY", None)
    result = _run_cli(
        "plan",
        "--artifact-dir",
        str(tmp_path),
        "--live",
        "--count",
        "0",
        environment=environment,
    )
    assert result.returncode == ExitCode.INVALID
    assert "at least one disposable entity" in result.stderr


def test_live_benchmark_and_live_resume_never_silently_run_offline(tmp_path: Path) -> None:
    environment = dict(os.environ)
    environment.pop("PYTEST_CURRENT_TEST", None)
    benchmark = _run_cli("benchmark", "--artifact-dir", str(tmp_path), "--live", environment=environment)
    resume = _run_cli("resume", "--artifact-dir", str(tmp_path), "--live", environment=environment)
    assert benchmark.returncode == ExitCode.UNAVAILABLE
    assert "not admitted" in benchmark.stderr
    assert resume.returncode == ExitCode.INVALID
    assert "read-only validation" in resume.stderr


def test_benchmark_parent_hashes_detect_oracle_tampering(tmp_path: Path) -> None:
    result = _run_cli("benchmark", "--artifact-dir", str(tmp_path))
    assert result.returncode == ExitCode.COMPLETED, result.stderr
    oracle_path = tmp_path / "model-oracles/empty-offset.json"
    oracle_path.write_text("{}")
    with pytest.raises(ContractError, match="content hash"):
        cli_module._scan_bundle(tmp_path)  # noqa: SLF001 - bundle-integrity regression


def test_resume_rejects_incompatible_run_lineage(tmp_path: Path) -> None:
    plan = _plan()
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    record = build_manifest_record(_manifest_base(plan))
    manifest_path = tmp_path / "manifest.jsonl"
    append_manifest_record(manifest_path, record)
    result = _run_cli(
        "resume",
        "--artifact-dir",
        str(tmp_path),
        "--plan",
        str(plan_path),
        "--manifest",
        str(manifest_path),
        "--run-id",
        str(uuid.uuid4()),
    )
    assert result.returncode == ExitCode.INVALID
    assert "run_id" in result.stderr


def test_verify_and_resume_accept_one_exact_manifest_lineage(tmp_path: Path) -> None:
    plan = _plan(count=1)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    manifest_path = tmp_path / "manifest.jsonl"
    previous = build_manifest_record(_manifest_base(plan))
    append_manifest_record(manifest_path, previous)
    for event, entity_id in (("create_dispatched", None), ("created", "42"), ("verified", "42")):
        previous = build_manifest_record(
            {
                **_manifest_base(plan),
                "event": event,
                "entity_id": entity_id,
                "request_fingerprint": SHA256,
            },
            previous=previous,
        )
        append_manifest_record(manifest_path, previous)
    for command in ("verify", "resume"):
        result = _run_cli(
            command,
            "--artifact-dir",
            str(tmp_path),
            "--plan",
            str(plan_path),
            "--manifest",
            str(manifest_path),
        )
        assert result.returncode == ExitCode.COMPLETED, result.stderr
    assert (tmp_path / "oracle.json").exists()
    assert (tmp_path / "resume-evidence.json").exists()


def test_recover_manifest_is_read_only_but_requires_explicit_live_mode(tmp_path: Path) -> None:
    result = _run_cli("recover-manifest", "--artifact-dir", str(tmp_path))
    assert result.returncode == ExitCode.INVALID
    assert "requires --live" in result.stderr


def test_cleanup_reconciles_an_ambiguous_create_before_claiming_absence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _approved_live_plan()
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    manifest_path = tmp_path / "manifest.jsonl"
    previous = build_manifest_record(_manifest_base(plan))
    append_manifest_record(manifest_path, previous)
    for event in ("create_dispatched", "ambiguous"):
        previous = build_manifest_record(
            {**_manifest_base(plan), "event": event, "request_fingerprint": SHA256},
            previous=previous,
        )
        append_manifest_record(manifest_path, previous)

    class FakeAdapter:
        delete_method = "tasks.task.delete"
        id_parameter = "taskId"
        deleted = False

        def find_exact_marker(self, _portal: object, _marker: str) -> list[str]:
            return ["owned-123"]

        def read(self, _portal: object, _entity_id: str) -> dict[str, Any] | None:
            return None if self.deleted else {"TITLE": previous["marker_value"]}

        def delete(self, _portal: object, _entity_id: str) -> None:
            self.deleted = True

    adapter = FakeAdapter()

    class FakePortal:
        identity = PortalIdentity("portal.invalid", "admin_full", "1", SHA256)
        attempts = 0

        def __init__(self, *, role: str) -> None:
            assert role == "admin_full"

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def preflight(self, *, required_scopes: set[str]) -> LivePreflight:
            assert required_scopes == {"task"}
            return LivePreflight(self.identity, "build-1", frozenset({"task"}))

    monkeypatch.delenv("PYTEST_CURRENT_TEST")
    monkeypatch.setattr(cli_module, "LivePortal", FakePortal)
    monkeypatch.setitem(ADAPTERS, "tasks-task-v1", adapter)
    result = cli_module._cleanup(  # noqa: SLF001 - direct offline orchestration regression
        Namespace(
            live=True,
            allow_writes=True,
            plan=plan_path,
            manifest=manifest_path,
            artifact_dir=tmp_path,
            run_id=None,
            lineage_id=None,
            credential_role="admin_full",
        ),
    )
    assert result == ExitCode.COMPLETED
    assert adapter.deleted
    latest = load_manifest(manifest_path)[-1]
    assert latest["event"] == "absence_verified"
    validate_manifest_against_plan(load_manifest(manifest_path), plan)


def test_seed_never_redispatches_an_unresolved_create(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _approved_live_plan()
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    manifest_path = tmp_path / "manifest.jsonl"
    previous = build_manifest_record(_manifest_base(plan))
    append_manifest_record(manifest_path, previous)
    previous = build_manifest_record(
        {**_manifest_base(plan), "event": "create_dispatched", "request_fingerprint": SHA256},
        previous=previous,
    )
    append_manifest_record(manifest_path, previous)

    class FakeAdapter:
        create_method = "tasks.task.add"
        create_calls = 0

        def find_exact_marker(self, _portal: object, _marker: str) -> list[str]:
            return []

        def create(self, _portal: object, _marker: str) -> str:
            self.create_calls += 1
            return "unexpected"

    adapter = FakeAdapter()

    class FakePortal:
        identity = PortalIdentity("portal.invalid", "admin_full", "1", SHA256)
        attempts = 0

        def __init__(self, *, role: str) -> None:
            assert role == "admin_full"

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def preflight(self, *, required_scopes: set[str]) -> LivePreflight:
            assert required_scopes == {"task"}
            return LivePreflight(self.identity, "build-1", frozenset({"task"}))

    monkeypatch.delenv("PYTEST_CURRENT_TEST")
    monkeypatch.setattr(cli_module, "LivePortal", FakePortal)
    monkeypatch.setitem(ADAPTERS, "tasks-task-v1", adapter)
    result = cli_module._seed(  # noqa: SLF001 - direct no-network ambiguity regression
        Namespace(
            live=True,
            allow_writes=True,
            plan=plan_path,
            manifest=manifest_path,
            artifact_dir=tmp_path,
            run_id=None,
            lineage_id=None,
            credential_role="admin_full",
        ),
    )
    assert result == ExitCode.INCOMPLETE
    assert adapter.create_calls == 0
    records = load_manifest(manifest_path)
    assert records[-1]["event"] == "create_dispatched"
    validate_manifest_against_plan(records, plan)


def test_wheel_contains_library_but_no_evidence_or_live_tooling(tmp_path: Path) -> None:
    uv = os.environ.get("UV", "uv")
    result = subprocess.run(  # noqa: S603 - fixed uv build command in isolated test directory
        [uv, "build", "--wheel", "--out-dir", str(tmp_path)],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    wheel = next(tmp_path.glob("*.whl"))
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
    assert any(name.startswith("b24api/") for name in names)
    assert not any("b24api_evidence" in name or name.startswith("tools/") for name in names)
    assert not any(name.endswith(("live.py", "cli.py")) for name in names)


def _run_cli(*arguments: str, environment: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed interpreter/entrypoint with test-controlled arguments
        [sys.executable, str(ENTRYPOINT), *arguments],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
