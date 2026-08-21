"""Regression gates for W9 evidence contracts and offline execution."""

from __future__ import annotations
import base64
import copy
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import uuid
import zipfile
from argparse import Namespace
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self, cast

import pytest

from . import cli as cli_module
from . import contracts as contracts_module
from .contracts import (
    FINGERPRINT_ALGORITHM,
    FINGERPRINT_KEY_FORMAT,
    FIXED_1X_SHA,
    INSTRUMENTATION_REVIEW_SHA,
    ORIGINAL_HEAD_SHA,
    REVIEWED_MAX_ENTITIES_PER_CELL,
    REVIEWED_PROFILE_SET_ID,
    REVIEWED_PROFILE_SET_SHA256,
    SCHEMA_VERSION,
    SKILLS_CORPUS_SHA,
    SKILLS_RECIPE_TREE_SHA256,
    ContractError,
    ExitCode,
    ManifestLineage,
    PortalIdentity,
    SecretLeakError,
    append_manifest_record,
    atomic_write_json,
    build_manifest_record,
    content_sha256,
    derive_drift_controls,
    git_sha,
    load_manifest,
    marker_sha256,
    marker_value,
    parse_fingerprint_key,
    portal_identity,
    reviewed_dataset_plan_sha256,
    scan_bytes_for_secrets,
    scan_paths_for_secrets,
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
from .live import ADAPTERS, LivePreflight, LiveUnavailableError
from .model import DeterministicPortal, exact_model_cases, run_exact_matrix_sync

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

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
EXPECTED_STABLE_MODEL_RUNS = 90
EXPECTED_BENCHMARK_REFS = EXPECTED_STABLE_MODEL_RUNS + 1
EXPECTED_SCHEMA_COUNT = 6
SECOND_CALL = 2
BUNDLE_OVERFLOW_FILES = 513
LEAK_FIXTURE = b"https://example.invalid/rest/1/realisticToken123/"


def _plan(*, count: int = 5) -> dict[str, Any]:
    profile = validate_reviewed_profile_set(PROFILE_SET)["profiles"][0]
    requests = count * 5 + 4 if count else 0
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": RUN_ID,
        "lineage_id": LINEAGE_ID,
        "original_head_sha": ORIGINAL_HEAD_SHA,
        "candidate_sha": SHA,
        "generator_sha": SHA,
        "skills_corpus_sha": SKILLS_CORPUS_SHA,
        "skills_recipe_tree_sha256": SKILLS_RECIPE_TREE_SHA256,
        "credential_role": "model",
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
    plan["credential_role"] = "admin_full"
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


def _approval_arguments(plan: dict[str, Any]) -> dict[str, str]:
    return {
        "confirm_plan_review_sha": str(plan["authorization"]["plan_review_sha"]),
        "confirm_plan_content_sha256": content_sha256(plan),
    }


def test_approved_plan_self_claims_do_not_authorize_live_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _approved_live_plan()
    with pytest.raises(ContractError, match="external exact plan review"):
        cli_module._require_approved_plan(plan, args=Namespace())  # noqa: SLF001
    with pytest.raises(ContractError, match="content hash"):
        cli_module._require_approved_plan(  # noqa: SLF001
            plan,
            args=Namespace(confirm_plan_review_sha=SHA, confirm_plan_content_sha256="f" * 64),
        )
    forged = copy.deepcopy(plan)
    forged["authorization"]["plan_review_sha"] = "f" * 40
    with pytest.raises(ContractError, match="existing review commit"):
        cli_module._require_approved_plan(  # noqa: SLF001
            forged,
            args=Namespace(
                confirm_plan_review_sha="f" * 40,
                confirm_plan_content_sha256=content_sha256(forged),
            ),
        )
    review_root = tmp_path / "review-root"
    review_root.mkdir()
    git = shutil.which("git")
    assert git is not None
    subprocess.run(  # noqa: S603 - fixed git operation in an isolated test repository
        [git, "init", "--quiet"],
        cwd=review_root,
        check=True,
    )
    trailer_hash = reviewed_dataset_plan_sha256(plan)
    review_blob = subprocess.run(  # noqa: S603 - isolated non-commit regression fixture
        [git, "hash-object", "-w", "--stdin"],
        cwd=review_root,
        check=True,
        capture_output=True,
        input=f"Dataset-Plan-SHA256: {trailer_hash}\n",
        text=True,
    ).stdout.strip()
    plan["authorization"]["plan_review_sha"] = review_blob
    monkeypatch.setattr(cli_module, "ROOT", review_root)
    with pytest.raises(ContractError, match="commit object"):
        cli_module._require_approved_plan(  # noqa: SLF001
            plan,
            args=Namespace(**_approval_arguments(plan)),
        )
    subprocess.run(  # noqa: S603 - isolated exact review-commit integration fixture
        [
            git,
            "-c",
            "user.name=Evidence Reviewer",
            "-c",
            "user.email=reviewer@example.invalid",
            "commit",
            "--allow-empty",
            "--quiet",
            "-m",
            f"approve dataset plan\n\nDataset-Plan-SHA256: {trailer_hash}",
        ],
        cwd=review_root,
        check=True,
    )
    review_sha = subprocess.run(  # noqa: S603 - fixed git query in an isolated test repository
        [git, "rev-parse", "HEAD"],
        cwd=review_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    plan["authorization"]["plan_review_sha"] = review_sha
    assert reviewed_dataset_plan_sha256(plan) == trailer_hash
    cli_module._require_approved_plan(  # noqa: SLF001
        plan,
        args=Namespace(**_approval_arguments(plan)),
    )


@pytest.mark.parametrize("command_name", ["_seed", "_cleanup"])
def test_live_write_refuses_unapproved_plan_before_writing_artifact_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command_name: str,
) -> None:
    external_plan = tmp_path / "external-plan.json"
    plan = cli_module._model_dataset_plan(run_id=RUN_ID, lineage_id=LINEAGE_ID)  # noqa: SLF001
    atomic_write_json(external_plan, plan)
    artifact_dir = tmp_path / "empty-bundle"
    monkeypatch.delenv("PYTEST_CURRENT_TEST")
    command = getattr(cli_module, command_name)

    with pytest.raises(ContractError, match="human-reviewed"):
        command(
            Namespace(
                live=True,
                allow_writes=True,
                plan=external_plan,
                artifact_dir=artifact_dir,
                run_id=None,
                lineage_id=None,
                manifest=None,
                credential_role="admin_full",
                confirm_plan_review_sha=None,
                confirm_plan_content_sha256=None,
            ),
        )

    assert not artifact_dir.exists()


def _manifest_base(plan: dict[str, Any]) -> dict[str, Any]:
    correlation = content_sha256([RUN_ID, "CELL", 0])
    marker = marker_value(plan["namespace"], correlation)
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": RUN_ID,
        "lineage_id": LINEAGE_ID,
        "original_head_sha": ORIGINAL_HEAD_SHA,
        "fixed_1x_sha": FIXED_1X_SHA,
        "skills_corpus_sha": SKILLS_CORPUS_SHA,
        "skills_recipe_tree_sha256": SKILLS_RECIPE_TREE_SHA256,
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
        "original_head_sha": ORIGINAL_HEAD_SHA,
        "fixed_1x_sha": FIXED_1X_SHA,
        "candidate_sha": SHA,
        "skills_corpus_sha": SKILLS_CORPUS_SHA,
        "skills_recipe_tree_sha256": SKILLS_RECIPE_TREE_SHA256,
        "dataset_plan_content_hash": SHA256,
        "manifest_content_hash": SHA256,
        "instrumentation_review_sha": INSTRUMENTATION_REVIEW_SHA,
        "controls": {
            "warmups": 1,
            "advisory_runs": 5,
            "blocking_pairs": 9,
            "interleaving": True,
            "timing_model": {"kind": "deterministic_request_cost", "seconds_per_request": 0.001},
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
        "original_head_sha": ORIGINAL_HEAD_SHA,
        "fixed_1x_sha": FIXED_1X_SHA,
        "portal_fingerprint": SHA256,
        "host": "model.local",
        "candidate_sha": SHA,
        "skills_corpus_sha": SKILLS_CORPUS_SHA,
        "skills_recipe_tree_sha256": SKILLS_RECIPE_TREE_SHA256,
        "command": "benchmark",
        "phase": "complete",
        "case_id": "MODEL-MATRIX",
        "manifest_content_hash": SHA256,
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


@pytest.mark.parametrize("encoding", ["utf-16", "utf-32"])
def test_json_inputs_are_utf8_only_and_encoded_secrets_remain_detectable(encoding: str) -> None:
    webhook = "https://real.example" + "/rest/13/realisticToken123/"
    raw = json.dumps({"value": webhook}).encode(encoding)
    with pytest.raises(ContractError, match="invalid JSON"):
        strict_json_loads(raw)
    with pytest.raises(SecretLeakError):
        scan_bytes_for_secrets(raw, source="encoded-artifact.json")


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
                f"https://{invalid_host}" + "/rest/13/abcdef",
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
        lambda plan: plan["estimated"].update(
            create_strategy="batch",
            delete_strategy="batch",
            requests=1,
            batch_commands=10,
        ),
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


def test_dataset_plan_cannot_multiply_the_reviewed_scale_across_cells() -> None:
    plan = _plan(count=300)
    second = copy.deepcopy(plan["cells"][0])
    second.update(
        id="CRM",
        disposable_profile_id="crm-deal-v1",
        entity_family="crm_deal",
        create_method="crm.deal.add",
        read_method="crm.deal.get",
        delete_method="crm.deal.delete",
        required_scopes=["crm"],
    )
    plan["cells"].append(second)
    plan["cleanup"]["dependency_order"].append("CRM")
    plan["estimated"].update(entities=500, requests=3004, duration_seconds=300.4, quota_impact=3004.0)
    with pytest.raises(ContractError, match="aggregate entity ceiling"):
        validate_dataset_plan(plan)


def test_candidate_cleanliness_is_rechecked_before_parent_artifact_persistence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_dirty(_root: Path) -> None:
        raise ContractError("tracked repository changed during evidence execution")

    monkeypatch.setattr(cli_module, "require_clean_tracked_tree", reject_dirty)
    path = tmp_path / "benchmark-evidence.json"
    with pytest.raises(ContractError, match="changed during"):
        cli_module._write_validated_artifact(path, _benchmark_artifact())  # noqa: SLF001
    assert not path.exists()


def test_candidate_drift_after_atomic_replace_rolls_back_pass_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = _benchmark_artifact()
    candidate_sha = str(artifact["candidate_sha"])
    calls = 0

    def drifting_sha(_root: Path) -> str:
        nonlocal calls
        calls += 1
        return candidate_sha if calls == 1 else "f" * 40

    monkeypatch.setattr(cli_module, "require_clean_tracked_tree", lambda _root: None)
    monkeypatch.setattr(cli_module, "git_sha", drifting_sha)
    path = tmp_path / "benchmark-evidence.json"

    with pytest.raises(ContractError, match="candidate SHA differs"):
        cli_module._write_validated_artifact(  # noqa: SLF001
            path,
            artifact,
            candidate_sha=candidate_sha,
        )

    assert not path.exists()


def test_terminal_bundle_scan_failure_rolls_back_pass_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = _benchmark_artifact()
    candidate_sha = str(artifact["candidate_sha"])
    monkeypatch.setattr(cli_module, "_require_evidence_candidate", lambda _candidate: None)
    monkeypatch.setattr(
        cli_module,
        "_scan_bundle",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ContractError("final scan refused")),
    )
    path = tmp_path / "benchmark-evidence.json"

    with pytest.raises(ContractError, match="final scan refused"):
        cli_module._write_validated_artifact(  # noqa: SLF001
            path,
            artifact,
            candidate_sha=candidate_sha,
            scan_bundle=True,
        )

    assert not path.exists()


def test_directory_fsync_failure_after_replace_restores_previous_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = _benchmark_artifact()
    candidate_sha = str(artifact["candidate_sha"])
    path = tmp_path / "benchmark-evidence.json"
    previous = {"previous": "accepted"}
    atomic_write_json(path, previous)
    monkeypatch.setattr(cli_module, "_require_evidence_candidate", lambda _candidate: None)
    fsync_calls = 0

    def fail_artifact_fsync(_directory: Path) -> None:
        nonlocal fsync_calls
        fsync_calls += 1
        if fsync_calls == SECOND_CALL:
            raise OSError("directory fsync failed after replace")

    monkeypatch.setattr(contracts_module, "_fsync_directory", fail_artifact_fsync)

    with pytest.raises(OSError, match="directory fsync failed after replace"):
        cli_module._write_validated_artifact(  # noqa: SLF001
            path,
            artifact,
            candidate_sha=candidate_sha,
        )

    assert json.loads(path.read_text()) == previous


def test_failed_owned_restore_leaves_bundle_transaction_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "plan-evidence.json"
    accepted = {"outcome": "PASS", "metrics": {"http_attempts": 11}}
    refused = {"outcome": "PASS", "metrics": {"http_attempts": 22}}
    atomic_write_json(path, accepted)
    candidate_checks = 0

    def refuse_after_write(_candidate_sha: str) -> None:
        nonlocal candidate_checks
        candidate_checks += 1
        if candidate_checks == SECOND_CALL:
            raise ContractError("candidate SHA differs")

    real_atomic_write_json = contracts_module.atomic_write_json

    def fail_predecessor_restore(target: Path, value: Mapping[str, Any]) -> None:
        if target == path and value == accepted:
            raise OSError("predecessor restore refused")
        real_atomic_write_json(target, value)

    monkeypatch.setattr(cli_module, "_require_evidence_candidate", refuse_after_write)
    monkeypatch.setattr(cli_module, "atomic_write_json", fail_predecessor_restore)

    with pytest.raises(ContractError, match="candidate SHA differs") as captured:
        cli_module._write_candidate_json(  # noqa: SLF001
            path,
            refused,
            candidate_sha="a" * 40,
        )

    assert json.loads(path.read_text()) == refused
    assert any("bundle fail-closed" in note for note in captured.value.__notes__)
    markers = tuple(tmp_path.glob(".b24api-transaction-*.pending"))
    assert len(markers) == 1
    monkeypatch.setattr(cli_module, "require_clean_tracked_tree", lambda _root: None)
    with pytest.raises(ContractError, match="incomplete publication transaction"):
        cli_module._scan_bundle(tmp_path)  # noqa: SLF001


def test_rollback_base_exception_does_not_mask_primary_bundle_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = _benchmark_artifact()
    candidate_sha = str(artifact["candidate_sha"])
    path = tmp_path / "benchmark-evidence.json"
    with monkeypatch.context() as patch:
        patch.setattr(cli_module, "_require_evidence_candidate", lambda _candidate: None)
        patch.setattr(
            cli_module,
            "_scan_bundle",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(ContractError("primary final scan failure")),
        )
        patch.setattr(Path, "unlink", lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt()))

        with pytest.raises(ContractError, match="primary final scan failure"):
            cli_module._write_validated_artifact(  # noqa: SLF001
                path,
                artifact,
                candidate_sha=candidate_sha,
                scan_bundle=True,
            )

    assert json.loads(path.read_text())["outcome"] == "PASS"
    markers = tuple(tmp_path.glob(".b24api-transaction-*.pending"))
    assert len(markers) == 1
    monkeypatch.setattr(cli_module, "require_clean_tracked_tree", lambda _root: None)
    with pytest.raises(ContractError, match="incomplete publication transaction"):
        cli_module._scan_bundle(tmp_path)  # noqa: SLF001
    path.unlink()
    markers[0].unlink()


def test_bundle_scan_never_deletes_caller_owned_refused_lookalikes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_lookalike = tmp_path / ".caller-owned.refused-backup"
    nested_dir = tmp_path / "model-oracles"
    nested_dir.mkdir()
    nested_lookalike = nested_dir / ".caller-owned.json.refused-backup"
    root_lookalike.write_text("caller owned")
    nested_lookalike.write_text("caller owned")
    monkeypatch.setattr(cli_module, "require_clean_tracked_tree", lambda _root: None)

    cli_module._scan_bundle(tmp_path)  # noqa: SLF001

    assert root_lookalike.read_text() == "caller owned"
    assert nested_lookalike.read_text() == "caller owned"


def test_interrupted_nested_owned_restore_is_visible_and_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oracle_dir = tmp_path / "model-oracles"
    oracle_dir.mkdir()
    path = oracle_dir / "case.json"
    atomic_write_json(path, {"outcome": "PASS"})

    with monkeypatch.context() as patch:
        patch.setattr(Path, "unlink", lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt()))
        cli_module._restore_candidate_json(path, None)  # noqa: SLF001

    assert json.loads(path.read_text()) == {"outcome": "PASS"}
    markers = tuple(tmp_path.rglob(".b24api-transaction-*.pending"))
    assert len(markers) == 1
    monkeypatch.setattr(cli_module, "require_clean_tracked_tree", lambda _root: None)
    with pytest.raises(ContractError, match="incomplete publication transaction"):
        cli_module._scan_bundle(tmp_path)  # noqa: SLF001
    path.unlink()
    markers[0].unlink()


@pytest.mark.parametrize("with_previous", [False, True])
def test_verify_bundle_failure_rolls_back_oracle_dependency(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    with_previous: bool,
) -> None:
    candidate_sha = "d9cd6969415f571ce64ae16d7aab160b8a3a7d42"
    oracle_path = tmp_path / "oracle.json"
    previous = {"case_id": "old"}
    if with_previous:
        atomic_write_json(oracle_path, previous)
    monkeypatch.setattr(cli_module, "_require_evidence_candidate", lambda _candidate: None)
    monkeypatch.setattr(
        cli_module,
        "_write_validated_artifact",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ContractError("terminal publication refused")),
    )

    with pytest.raises(ContractError, match="terminal publication refused"):
        cli_module._persist_verify_bundle(  # noqa: SLF001
            artifact_dir=tmp_path,
            oracle={"case_id": "new"},
            artifact=_benchmark_artifact(),
            candidate_sha=candidate_sha,
        )

    if with_previous:
        assert json.loads(oracle_path.read_text()) == previous
    else:
        assert not oracle_path.exists()


def test_plan_bundle_failure_rolls_back_every_new_dependency(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan(count=0)
    benchmark_plan = _benchmark_plan()
    artifact = _benchmark_artifact()
    candidate_sha = str(plan["candidate_sha"])
    monkeypatch.setattr(cli_module, "_require_evidence_candidate", lambda _candidate: None)
    monkeypatch.setattr(
        cli_module,
        "_write_validated_artifact",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ContractError("terminal publication refused")),
    )

    with pytest.raises(ContractError, match="terminal publication refused"):
        cli_module._persist_plan_bundle(  # noqa: SLF001
            artifact_dir=tmp_path,
            dataset_plan=plan,
            benchmark_plan=benchmark_plan,
            artifact=artifact,
            candidate_sha=candidate_sha,
        )

    assert {path.name for path in tmp_path.iterdir()} == {".b24api-transaction-bundle.lock"}


def test_plan_reuses_a_directory_containing_only_its_persistent_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = cli_module._begin_candidate_transaction(tmp_path / "bundle")  # noqa: SLF001
    assert cli_module._rollback_candidate_json_log([], transaction=marker)  # noqa: SLF001
    assert {path.name for path in tmp_path.iterdir()} == {".b24api-transaction-bundle.lock"}
    monkeypatch.setattr(cli_module, "_persist_plan_bundle", lambda **_kwargs: None)
    monkeypatch.setattr(cli_module, "_safe_message", lambda _message: None)
    args = cli_module._parser().parse_args(  # noqa: SLF001
        ["plan", "--artifact-dir", str(tmp_path), "--count", "0"],
    )

    result = cli_module._plan(args)  # noqa: SLF001

    assert result == ExitCode.COMPLETED


def test_bundle_transaction_lock_is_owner_only(tmp_path: Path) -> None:
    marker = cli_module._begin_candidate_transaction(tmp_path / "bundle")  # noqa: SLF001
    lock_path = tmp_path / ".b24api-transaction-bundle.lock"
    owner_only_mode = 0o600

    assert lock_path.stat().st_mode & 0o777 == owner_only_mode
    assert cli_module._rollback_candidate_json_log([], transaction=marker)  # noqa: SLF001


def test_plan_never_exempts_or_follows_a_symlinked_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    caller_mode = 0o644
    outside = tmp_path / "caller-owned"
    artifact_dir = tmp_path / "bundle"
    artifact_dir.mkdir()
    outside.write_text("caller owned")
    outside.chmod(caller_mode)
    (artifact_dir / ".b24api-transaction-bundle.lock").symlink_to(outside)
    monkeypatch.setattr(cli_module, "_persist_plan_bundle", lambda **_kwargs: None)
    args = cli_module._parser().parse_args(  # noqa: SLF001
        ["plan", "--artifact-dir", str(artifact_dir), "--count", "0"],
    )

    with pytest.raises(ContractError, match="requires an empty artifact directory"):
        cli_module._plan(args)  # noqa: SLF001

    assert outside.read_text() == "caller owned"
    assert outside.stat().st_mode & 0o777 == caller_mode


def test_transaction_lock_rejects_symlink_and_hardlink_aliases(tmp_path: Path) -> None:
    owner_only_mode = 0o600
    outside = tmp_path / "caller-owned"
    outside.write_text("caller owned")
    outside.chmod(owner_only_mode)
    symlink_dir = tmp_path / "symlink-bundle"
    hardlink_dir = tmp_path / "hardlink-bundle"
    symlink_dir.mkdir()
    hardlink_dir.mkdir()
    (symlink_dir / ".b24api-transaction-bundle.lock").symlink_to(outside)
    os.link(outside, hardlink_dir / ".b24api-transaction-bundle.lock")

    with pytest.raises(ContractError, match="not a secure regular file"):
        cli_module._begin_candidate_transaction(symlink_dir / "bundle")  # noqa: SLF001
    with pytest.raises(ContractError, match="not a secure regular file"):
        cli_module._begin_candidate_transaction(hardlink_dir / "bundle")  # noqa: SLF001

    assert outside.read_text() == "caller owned"
    assert outside.stat().st_mode & 0o777 == owner_only_mode


def test_plan_bundle_uses_one_marker_created_before_all_dependency_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan(count=0)
    benchmark_plan = _benchmark_plan()
    artifact = _benchmark_artifact()
    candidate_sha = str(plan["candidate_sha"])
    real_begin = cli_module._begin_candidate_transaction  # noqa: SLF001
    begin_calls = 0

    def refuse_any_late_marker(path: Path) -> Path:
        nonlocal begin_calls
        begin_calls += 1
        if begin_calls > 1:
            raise PermissionError("late marker creation refused")
        return real_begin(path)

    monkeypatch.setattr(cli_module, "_require_evidence_candidate", lambda _candidate: None)
    monkeypatch.setattr(cli_module, "_begin_candidate_transaction", refuse_any_late_marker)
    monkeypatch.setattr(
        cli_module,
        "_scan_bundle",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ContractError("terminal publication refused")),
    )

    with pytest.raises(ContractError, match="terminal publication refused"):
        cli_module._persist_plan_bundle(  # noqa: SLF001
            artifact_dir=tmp_path,
            dataset_plan=plan,
            benchmark_plan=benchmark_plan,
            artifact=artifact,
            candidate_sha=candidate_sha,
        )

    assert begin_calls == 1
    assert {path.name for path in tmp_path.iterdir()} == {".b24api-transaction-bundle.lock"}


def test_bundle_transaction_excludes_a_concurrent_publisher(tmp_path: Path) -> None:
    first = cli_module._begin_candidate_transaction(tmp_path / "bundle")  # noqa: SLF001

    with pytest.raises(ContractError, match="owned by another active publication"):
        cli_module._begin_candidate_transaction(tmp_path / "bundle")  # noqa: SLF001

    assert cli_module._rollback_candidate_json_log([], transaction=first)  # noqa: SLF001
    second = cli_module._begin_candidate_transaction(tmp_path / "bundle")  # noqa: SLF001
    assert cli_module._rollback_candidate_json_log([], transaction=second)  # noqa: SLF001


def test_stale_transaction_journal_restores_only_its_owned_content(tmp_path: Path) -> None:
    path = tmp_path / "dataset-plan.json"
    previous = {"tag": "accepted"}
    refused = {"tag": "refused"}
    atomic_write_json(path, previous)
    marker = cli_module._begin_candidate_transaction(tmp_path / "bundle")  # noqa: SLF001
    cli_module._register_transaction_write(marker, path, previous, refused)  # noqa: SLF001
    atomic_write_json(path, refused)
    cli_module._finish_transaction_lock(marker)  # noqa: SLF001 - simulate process death

    cli_module._recover_stale_candidate_transaction(tmp_path)  # noqa: SLF001

    assert json.loads(path.read_text()) == previous
    assert not marker.exists()


def test_stale_transaction_never_overwrites_foreign_canonical_content(tmp_path: Path) -> None:
    path = tmp_path / "dataset-plan.json"
    previous = {"tag": "accepted"}
    refused = {"tag": "refused"}
    foreign = {"tag": "foreign"}
    atomic_write_json(path, previous)
    marker = cli_module._begin_candidate_transaction(tmp_path / "bundle")  # noqa: SLF001
    cli_module._register_transaction_write(marker, path, previous, refused)  # noqa: SLF001
    atomic_write_json(path, refused)
    cli_module._finish_transaction_lock(marker)  # noqa: SLF001 - simulate process death
    atomic_write_json(path, foreign)

    with pytest.raises(ContractError, match="could not be recovered safely"):
        cli_module._recover_stale_candidate_transaction(tmp_path)  # noqa: SLF001

    assert json.loads(path.read_text()) == foreign
    assert marker.exists()


def test_standalone_rollback_reads_predecessor_only_after_lock_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "plan-evidence.json"
    old = {"tag": "old"}
    committed = {"tag": "intervening-commit"}
    candidate = {"tag": "refused-candidate"}
    atomic_write_json(path, old)
    real_begin = cli_module._begin_candidate_transaction  # noqa: SLF001
    candidate_checks = 0

    def commit_before_lock(target: Path) -> Path:
        atomic_write_json(path, committed)
        return real_begin(target)

    def refuse_after_write(_candidate_sha: str) -> None:
        nonlocal candidate_checks
        candidate_checks += 1
        if candidate_checks == SECOND_CALL:
            raise ContractError("post-write refusal")

    monkeypatch.setattr(cli_module, "_begin_candidate_transaction", commit_before_lock)
    monkeypatch.setattr(cli_module, "_require_evidence_candidate", refuse_after_write)

    with pytest.raises(ContractError, match="post-write refusal"):
        cli_module._write_candidate_json(  # noqa: SLF001
            path,
            candidate,
            candidate_sha="a" * 40,
        )

    assert json.loads(path.read_text()) == committed
    assert not tuple(tmp_path.glob(".b24api-transaction-*.pending"))


def test_transaction_refuses_a_symlinked_canonical_path_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    predecessor = tmp_path / "caller-owned-predecessor.json"
    canonical = tmp_path / "dataset-plan.json"
    accepted = {"tag": "accepted"}
    refused = {"tag": "refused"}
    atomic_write_json(predecessor, accepted)
    canonical.symlink_to(predecessor.name)
    monkeypatch.setattr(cli_module, "_require_evidence_candidate", lambda _candidate: None)

    with pytest.raises(ContractError, match="cannot contain a symlink"):
        cli_module._write_candidate_json(  # noqa: SLF001
            canonical,
            refused,
            candidate_sha="a" * 40,
        )

    assert canonical.is_symlink()
    assert json.loads(predecessor.read_text()) == accepted
    assert not tuple(tmp_path.glob(".b24api-transaction-*.pending"))


def test_transaction_refuses_a_symlinked_parent_before_creating_a_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_dir = tmp_path / "bundle"
    outside = tmp_path / "caller-owned"
    artifact_dir.mkdir()
    outside.mkdir()
    (artifact_dir / "nested").symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr(cli_module, "_require_evidence_candidate", lambda _candidate: None)

    with pytest.raises(ContractError, match="artifact directory cannot contain a symlink"):
        cli_module._write_candidate_json(  # noqa: SLF001
            artifact_dir / "nested" / "case-evidence.json",
            {"outcome": "PASS"},
            candidate_sha="a" * 40,
        )

    assert tuple(outside.iterdir()) == ()
    assert not (artifact_dir / ".b24api-transaction-bundle.lock").exists()


def test_stale_transaction_refuses_a_symlinked_journal_path(tmp_path: Path) -> None:
    predecessor = tmp_path / "caller-owned-predecessor.json"
    canonical = tmp_path / "dataset-plan.json"
    accepted = {"tag": "accepted"}
    refused = {"tag": "refused"}
    atomic_write_json(predecessor, accepted)
    canonical.symlink_to(predecessor.name)
    marker = cli_module._begin_candidate_transaction(tmp_path / "bundle")  # noqa: SLF001
    atomic_write_json(
        marker,
        {
            "kind": "b24api-evidence-transaction-v2",
            "target": "bundle",
            "entries": [
                {
                    "path": canonical.name,
                    "previous": accepted,
                    "written_sha256": content_sha256(refused),
                },
            ],
        },
    )
    cli_module._finish_transaction_lock(marker)  # noqa: SLF001 - simulate process death

    with pytest.raises(ContractError, match="could not be recovered safely"):
        cli_module._recover_stale_candidate_transaction(tmp_path)  # noqa: SLF001

    assert canonical.is_symlink()
    assert json.loads(predecessor.read_text()) == accepted
    assert marker.exists()


@pytest.mark.parametrize("target_is_directory", [False, True])
def test_dangling_or_directory_marker_symlink_is_always_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    target_is_directory: bool,
) -> None:
    marker = tmp_path / ".b24api-transaction-bundle.pending"
    target = tmp_path / "caller-target"
    if target_is_directory:
        target.mkdir()
    else:
        target = tmp_path / "missing-caller-target"
    marker.symlink_to(target, target_is_directory=target_is_directory)

    with pytest.raises(ContractError, match="could not be recovered safely"):
        cli_module._recover_stale_candidate_transaction(tmp_path)  # noqa: SLF001

    assert marker.is_symlink()
    monkeypatch.setattr(cli_module, "require_clean_tracked_tree", lambda _root: None)
    with pytest.raises(ContractError, match="incomplete publication transaction"):
        cli_module._scan_bundle(tmp_path)  # noqa: SLF001
    assert marker.is_symlink()


def test_begin_never_replaces_a_dangling_marker_symlink(tmp_path: Path) -> None:
    marker = tmp_path / ".b24api-transaction-bundle.pending"
    marker.symlink_to("missing-caller-target")

    with pytest.raises(ContractError, match="could not be recovered safely"):
        cli_module._begin_candidate_transaction(tmp_path / "bundle")  # noqa: SLF001

    assert marker.is_symlink()


def test_post_unlink_error_is_reported_after_bundle_commit_without_false_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan(count=0)
    benchmark_plan = _benchmark_plan()
    artifact = _benchmark_artifact()
    candidate_sha = str(plan["candidate_sha"])
    real_unlink = Path.unlink
    marker_removed = False

    def remove_then_interrupt(path: Path, *, missing_ok: bool = False) -> None:
        nonlocal marker_removed
        real_unlink(path, missing_ok=missing_ok)
        if path.name.startswith(".b24api-transaction-") and not marker_removed:
            marker_removed = True
            raise OSError("interrupted after marker unlink")

    monkeypatch.setattr(cli_module, "_require_evidence_candidate", lambda _candidate: None)
    monkeypatch.setattr(cli_module, "_scan_bundle", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(Path, "unlink", remove_then_interrupt)

    with pytest.raises(OSError, match="after marker unlink") as captured:
        cli_module._persist_plan_bundle(  # noqa: SLF001
            artifact_dir=tmp_path,
            dataset_plan=plan,
            benchmark_plan=benchmark_plan,
            artifact=artifact,
            candidate_sha=candidate_sha,
        )

    assert marker_removed is True
    assert not tuple(tmp_path.glob(".b24api-transaction-*.pending"))
    assert {path.name for path in tmp_path.iterdir()} == {
        "benchmark-plan.json",
        "dataset-plan.json",
        "model-fixture-manifest.json",
        "plan-evidence.json",
        ".b24api-transaction-bundle.lock",
    }
    assert getattr(captured.value, "__notes__", ()) == ()


def test_safe_error_renders_redacted_rollback_notes(capsys: pytest.CaptureFixture[str]) -> None:
    error = ContractError("candidate publication refused")
    error.add_note("rollback failed; bundle is fail-closed")

    cli_module._safe_error(error)  # noqa: SLF001

    assert capsys.readouterr().err == (
        "error: ContractError: candidate publication refused\nnote: rollback failed; bundle is fail-closed\n"
    )


def test_stdout_failure_after_benchmark_commit_never_rolls_back_dependencies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependency = tmp_path / "model-matrix.json"
    terminal = tmp_path / "benchmark-evidence.json"

    def publish(
        _args: Namespace,
        **kwargs: object,
    ) -> ExitCode:
        rollback_log = cast("list[tuple[Path, Mapping[str, Any] | None]]", kwargs["rollback_log"])
        atomic_write_json(dependency, {"dependency": "committed"})
        rollback_log.append((dependency, None))
        atomic_write_json(terminal, {"outcome": "PASS"})
        return ExitCode.COMPLETED

    monkeypatch.setattr(cli_module, "_benchmark_runs_and_artifact_inner", publish)
    monkeypatch.setattr(
        cli_module,
        "_safe_message",
        lambda _message: (_ for _ in ()).throw(BrokenPipeError("stdout closed")),
    )

    with pytest.raises(BrokenPipeError, match="stdout closed"):
        cli_module._benchmark_runs_and_artifact(  # noqa: SLF001
            Namespace(artifact_dir=tmp_path),
            plan={},
            benchmark_plan={},
            runs=(),
        )

    assert json.loads(dependency.read_text()) == {"dependency": "committed"}
    assert json.loads(terminal.read_text()) == {"outcome": "PASS"}


def test_bundle_scan_rechecks_candidate_cleanliness_after_reading(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def become_dirty(_root: Path) -> None:
        nonlocal calls
        calls += 1
        if calls > 1:
            raise ContractError("tracked repository changed during evidence execution")

    monkeypatch.setattr(cli_module, "require_clean_tracked_tree", become_dirty)
    with pytest.raises(ContractError, match="changed during"):
        cli_module._scan_bundle(tmp_path)  # noqa: SLF001


def test_real_mid_benchmark_tracked_mutation_cannot_emit_pass(tmp_path: Path) -> None:
    worktree = tmp_path / "candidate"
    artifact_dir = tmp_path / "artifacts"
    git = shutil.which("git")
    assert git is not None
    add = subprocess.run(  # noqa: S603 - fixed git operation in an isolated test directory
        [git, "worktree", "add", "--detach", "--quiet", str(worktree), "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    assert add.returncode == 0
    process: subprocess.Popen[str] | None = None
    try:
        process = subprocess.Popen(  # noqa: S603 - fixed interpreter and detached entrypoint
            [
                sys.executable,
                str(worktree / "tools/b24api_evidence.py"),
                "benchmark",
                "--artifact-dir",
                str(artifact_dir),
            ],
            cwd=worktree,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 10
        plan_path = artifact_dir / "dataset-plan.json"
        while not plan_path.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.001)
        assert plan_path.exists(), "benchmark finished before its immutable plan became observable"
        assert process.poll() is None, "benchmark completed before the mid-run mutation window"
        readme = worktree / "README.md"
        readme.write_text(readme.read_text() + "\nmid-benchmark dirty-tree regression\n")

        stdout, stderr = process.communicate(timeout=30)

        assert process.returncode == ExitCode.INVALID, (stdout, stderr)
        assert "clean tracked tree" in stderr
        assert not (artifact_dir / "benchmark-evidence.json").exists()
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.communicate()
        subprocess.run(  # noqa: S603 - exact disposable worktree cleanup
            [git, "worktree", "remove", "--force", str(worktree)],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )


def test_real_mid_benchmark_clean_head_switch_cannot_emit_pass(tmp_path: Path) -> None:
    worktree = tmp_path / "candidate-head-switch"
    artifact_dir = tmp_path / "artifacts-head-switch"
    git = shutil.which("git")
    assert git is not None
    subprocess.run(  # noqa: S603 - fixed git operation in an isolated test directory
        [git, "worktree", "add", "--detach", "--quiet", str(worktree), "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    process: subprocess.Popen[str] | None = None
    try:
        process = subprocess.Popen(  # noqa: S603 - fixed interpreter and detached entrypoint
            [
                sys.executable,
                str(worktree / "tools/b24api_evidence.py"),
                "benchmark",
                "--artifact-dir",
                str(artifact_dir),
            ],
            cwd=worktree,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 10
        plan_path = artifact_dir / "dataset-plan.json"
        while not plan_path.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.001)
        assert plan_path.exists(), "benchmark finished before its immutable plan became observable"
        assert process.poll() is None, "benchmark completed before the clean-HEAD switch window"
        os.kill(process.pid, signal.SIGSTOP)
        try:
            subprocess.run(  # noqa: S603 - exact clean commit switch in a disposable worktree
                [git, "checkout", "--detach", "--quiet", "HEAD^"],
                cwd=worktree,
                check=True,
                capture_output=True,
                text=True,
            )
        finally:
            os.kill(process.pid, signal.SIGCONT)

        stdout, stderr = process.communicate(timeout=30)

        assert process.returncode == ExitCode.INVALID, (stdout, stderr)
        assert "candidate SHA differs" in stderr
        assert not (artifact_dir / "benchmark-evidence.json").exists()
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.communicate()
        subprocess.run(  # noqa: S603 - exact disposable worktree cleanup
            [git, "worktree", "remove", "--force", str(worktree)],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )


def test_manifest_hash_chain_marker_and_resume_lineage(tmp_path: Path) -> None:
    plan = _plan()
    first = build_manifest_record(_manifest_base(plan))
    path = tmp_path / "manifest.jsonl"
    append_manifest_record(path, first)
    second_base = {**_manifest_base(plan), "event": "verified", "entity_id": "42"}
    second = build_manifest_record(second_base, previous=first)
    append_manifest_record(path, second)
    lineage = ManifestLineage(
        run_id=RUN_ID,
        lineage_id=LINEAGE_ID,
        original_head_sha=ORIGINAL_HEAD_SHA,
        fixed_1x_sha=FIXED_1X_SHA,
        skills_corpus_sha=SKILLS_CORPUS_SHA,
        skills_recipe_tree_sha256=SKILLS_RECIPE_TREE_SHA256,
        dataset_plan_content_hash=content_sha256(plan),
        portal_fingerprint=SHA256,
        candidate_sha=SHA,
        namespace=str(plan["namespace"]),
    )
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
            expected=replace(lineage, run_id=str(uuid.uuid4())),
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


def test_manifest_append_failure_preserves_the_last_complete_chain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan()
    first = build_manifest_record(_manifest_base(plan))
    path = tmp_path / "manifest.jsonl"
    append_manifest_record(path, first)
    original = path.read_bytes()
    second = build_manifest_record(
        {**_manifest_base(plan), "event": "create_dispatched", "request_fingerprint": "f" * 64},
        previous=first,
    )

    def fail_replace(_source: Path, _destination: Path) -> Path:
        raise OSError("simulated atomic replace failure")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated"):
        append_manifest_record(path, second)
    assert path.read_bytes() == original
    assert load_manifest(path) == [first]


def test_atomic_write_cleanup_never_masks_the_primary_replace_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "artifact.json"

    def fail_replace(_source: Path, _destination: Path) -> Path:
        raise RuntimeError("primary replace failure")

    def fail_unlink(_path: Path, *, missing_ok: bool = False) -> None:
        del missing_ok
        raise OSError("cleanup unlink failure")

    monkeypatch.setattr(Path, "replace", fail_replace)
    monkeypatch.setattr(Path, "unlink", fail_unlink)
    with pytest.raises(RuntimeError, match="primary replace failure"):
        atomic_write_json(target, {"safe": "value"})


@pytest.mark.parametrize("requirement", ["frozen_manifest", "independent_pre_post_oracle"])
def test_oracle_snapshot_pass_requires_equal_nonnull_hashes(requirement: str) -> None:
    oracle = {
        "schema_version": SCHEMA_VERSION,
        "run_id": RUN_ID,
        "lineage_id": LINEAGE_ID,
        "case_id": "MODEL-offset",
        "original_head_sha": ORIGINAL_HEAD_SHA,
        "fixed_1x_sha": FIXED_1X_SHA,
        "candidate_sha": SHA,
        "skills_corpus_sha": SKILLS_CORPUS_SHA,
        "skills_recipe_tree_sha256": SKILLS_RECIPE_TREE_SHA256,
        "dataset_plan_content_hash": SHA256,
        "manifest_content_hash": SHA256,
        "portal_fingerprint": SHA256,
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


@pytest.mark.parametrize("filename", ["models_test.py", "protocol_test.py", "redaction_test.py"])
def test_secret_scanner_never_grants_allowance_by_basename(tmp_path: Path, filename: str) -> None:
    path = tmp_path / filename
    path.write_text("EXAMPLE_CREDENTIAL " + "https://real.example" + "/rest/13/realisticToken123/")
    with pytest.raises(SecretLeakError):
        scan_paths_for_secrets([path])


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


def test_concurrent_plan_publishers_cannot_rollback_the_committed_bundle(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "artifacts"
    commands = [
        [
            sys.executable,
            str(ENTRYPOINT),
            "plan",
            "--artifact-dir",
            str(artifact_dir),
            "--count",
            "0",
            "--run-id",
            str(uuid.uuid4()),
            "--lineage-id",
            str(uuid.uuid4()),
        ]
        for _index in range(2)
    ]
    processes = [
        subprocess.Popen(  # noqa: S603 - fixed interpreter and entrypoint
            command,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for command in commands
    ]
    results = [process.communicate(timeout=30) for process in processes]

    assert sorted(process.returncode for process in processes) == [ExitCode.COMPLETED, ExitCode.INVALID]
    assert sum("plan completed" in stdout for stdout, _stderr in results) == 1
    assert (artifact_dir / "dataset-plan.json").exists()
    assert (artifact_dir / "plan-evidence.json").exists()
    assert not tuple(artifact_dir.glob(".b24api-transaction-*.pending"))
    cli_module._scan_bundle(artifact_dir, expected_candidate_sha=SHA)  # noqa: SLF001


def test_cli_benchmark_refuses_and_preserves_a_dangling_transaction_marker(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "artifacts"
    plan = _run_cli(
        "plan",
        "--artifact-dir",
        str(artifact_dir),
        "--count",
        "0",
        "--run-id",
        RUN_ID,
        "--lineage-id",
        LINEAGE_ID,
    )
    assert plan.returncode == ExitCode.COMPLETED
    marker = artifact_dir / ".b24api-transaction-bundle.pending"
    marker.symlink_to("missing-caller-target")

    benchmark = _run_cli(
        "benchmark",
        "--artifact-dir",
        str(artifact_dir),
        "--plan",
        str(artifact_dir / "dataset-plan.json"),
    )

    assert benchmark.returncode == ExitCode.INVALID
    assert "could not be recovered safely" in benchmark.stderr
    assert marker.is_symlink()
    assert not (artifact_dir / "benchmark-evidence.json").exists()


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


def test_live_plan_refuses_missing_build_before_artifact_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakePortal:
        identity = PortalIdentity("portal.invalid", "admin_full", "1", SHA256)

        def __init__(self, *, role: str) -> None:
            assert role == "admin_full"

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def preflight(self, *, required_scopes: set[str]) -> LivePreflight:
            assert required_scopes == {"task"}
            return LivePreflight(self.identity, None, frozenset({"task"}))

    monkeypatch.setattr(cli_module, "LivePortal", FakePortal)
    args = cli_module._parser().parse_args(  # noqa: SLF001 - direct refusal-before-write regression
        [
            "plan",
            "--artifact-dir",
            str(tmp_path),
            "--live",
            "--credential-role",
            "admin_full",
            "--entity-profile",
            "tasks-task-v1",
            "--count",
            "5",
        ],
    )

    with pytest.raises(LiveUnavailableError, match="exact build identifier"):
        cli_module._plan(args)  # noqa: SLF001 - direct refusal-before-write regression

    assert list(tmp_path.iterdir()) == []


def test_live_benchmark_and_live_resume_never_silently_run_offline(tmp_path: Path) -> None:
    environment = dict(os.environ)
    environment.pop("PYTEST_CURRENT_TEST", None)
    benchmark = _run_cli("benchmark", "--artifact-dir", str(tmp_path), "--live", environment=environment)
    resume = _run_cli("resume", "--artifact-dir", str(tmp_path), "--live", environment=environment)
    assert benchmark.returncode == ExitCode.UNAVAILABLE
    assert "not admitted" in benchmark.stderr
    assert resume.returncode == ExitCode.INVALID
    assert "read-only validation" in resume.stderr
    live_plan = _approved_live_plan()
    live_plan_path = tmp_path / "live-plan.json"
    live_plan_path.write_text(json.dumps(live_plan))
    implicit_model = _run_cli(
        "benchmark",
        "--artifact-dir",
        str(tmp_path / "live-benchmark"),
        "--plan",
        str(live_plan_path),
        environment=environment,
    )
    assert implicit_model.returncode == ExitCode.UNAVAILABLE
    assert "non-model dataset plans" in implicit_model.stderr
    assert not (tmp_path / "live-benchmark/model-matrix.json").exists()


def test_benchmark_parent_hashes_detect_oracle_tampering(tmp_path: Path) -> None:
    result = _run_cli("benchmark", "--artifact-dir", str(tmp_path))
    assert result.returncode == ExitCode.COMPLETED, result.stderr
    artifact = json.loads((tmp_path / "benchmark-evidence.json").read_text())
    matrix = json.loads((tmp_path / "model-matrix.json").read_text())
    stable = [run for run in matrix["runs"] if run["outcome"] == "PASS"]
    assert len(artifact["evidence_refs"]) == EXPECTED_BENCHMARK_REFS
    assert artifact["metrics"]["http_attempts"] == sum(run["requests"] for run in stable)
    assert artifact["metrics"]["logical_pages"] == sum(run["logical_pages"] for run in stable)
    assert artifact["metrics"]["server_operating_seconds"] == pytest.approx(
        sum(run["operating_seconds"] for run in stable),
    )
    assert artifact["safe_violations"][0]["code"] == "mutation_diagnostic_inconclusive"
    assert f"sha256:{content_sha256(matrix)}" in artifact["evidence_refs"]
    assert all(run["outcome"] == "PASS" for run in matrix["runs"])
    artifact_path = tmp_path / "benchmark-evidence.json"
    original_artifact = artifact_path.read_text()
    arbitrary = {"schema_version": SCHEMA_VERSION, "value": "not an oracle"}
    arbitrary_path = tmp_path / "arbitrary.json"
    arbitrary_path.write_text(json.dumps(arbitrary))
    artifact["evidence_refs"] = [f"sha256:{content_sha256(arbitrary)}"]
    artifact_path.write_text(json.dumps(artifact))
    with pytest.raises(ContractError, match="benchmark PASS"):
        cli_module._scan_bundle(tmp_path)  # noqa: SLF001 - dependency-type regression
    artifact["evidence_refs"] = []
    artifact_path.write_text(json.dumps(artifact))
    with pytest.raises(ContractError, match="requires immutable"):
        cli_module._scan_bundle(tmp_path)  # noqa: SLF001 - missing-dependency regression
    artifact_path.write_text(original_artifact)
    oracle_path = tmp_path / "model-oracles/empty-offset-run-1.json"
    oracle_path.write_text("{}")
    with pytest.raises(ContractError, match="content hash"):
        cli_module._scan_bundle(tmp_path)  # noqa: SLF001 - bundle-integrity regression


def test_benchmark_rejects_per_observation_timing_redistribution(tmp_path: Path) -> None:
    result = _run_cli("benchmark", "--artifact-dir", str(tmp_path))
    assert result.returncode == ExitCode.COMPLETED, result.stderr
    matrix_path = tmp_path / "model-matrix.json"
    artifact_path = tmp_path / "benchmark-evidence.json"
    matrix = json.loads(matrix_path.read_text())
    artifact = json.loads(artifact_path.read_text())
    old_hash = content_sha256(matrix)
    first, second = [run for run in matrix["runs"] if run["identities"]][:2]
    shift = min(first["wall_seconds"], second["wall_seconds"]) / 4
    first["wall_seconds"] -= shift
    second["wall_seconds"] += shift
    new_hash = content_sha256(matrix)
    artifact["evidence_refs"] = [
        f"sha256:{new_hash}" if value == f"sha256:{old_hash}" else value for value in artifact["evidence_refs"]
    ]
    matrix_path.write_text(json.dumps(matrix))
    artifact_path.write_text(json.dumps(artifact))
    with pytest.raises(ContractError, match="timing contradict"):
        cli_module._scan_bundle(tmp_path)  # noqa: SLF001 - per-observation timing binding


def test_benchmark_observation_counters_reject_boolean_integer_aliases(tmp_path: Path) -> None:
    result = _run_cli("benchmark", "--artifact-dir", str(tmp_path))
    assert result.returncode == ExitCode.COMPLETED, result.stderr
    matrix_path = tmp_path / "model-matrix.json"
    artifact_path = tmp_path / "benchmark-evidence.json"
    matrix = json.loads(matrix_path.read_text())
    artifact = json.loads(artifact_path.read_text())
    old_hash = content_sha256(matrix)
    observation = next(run for run in matrix["runs"] if run["requests"] == 1)
    observation["requests"] = True
    new_hash = content_sha256(matrix)
    artifact["evidence_refs"] = [
        f"sha256:{new_hash}" if value == f"sha256:{old_hash}" else value for value in artifact["evidence_refs"]
    ]
    matrix_path.write_text(json.dumps(matrix))
    artifact_path.write_text(json.dumps(artifact))
    with pytest.raises(ContractError, match="exact non-negative integers"):
        cli_module._scan_bundle(tmp_path)  # noqa: SLF001 - bool/int alias regression


def test_bundle_rejects_evidence_from_two_runs_even_when_each_document_is_valid(tmp_path: Path) -> None:
    first = _run_cli(
        "plan",
        "--artifact-dir",
        str(tmp_path),
        "--run-id",
        RUN_ID,
        "--lineage-id",
        LINEAGE_ID,
    )
    assert first.returncode == ExitCode.COMPLETED, first.stderr
    benchmark = _run_cli("benchmark", "--artifact-dir", str(tmp_path))
    assert benchmark.returncode == ExitCode.COMPLETED, benchmark.stderr
    second = _run_cli(
        "plan",
        "--artifact-dir",
        str(tmp_path),
        "--run-id",
        str(uuid.uuid4()),
        "--lineage-id",
        str(uuid.uuid4()),
    )
    assert second.returncode == ExitCode.INVALID
    assert "never overwrites" in second.stderr


def test_plan_refusal_never_overwrites_an_existing_bundle(tmp_path: Path) -> None:
    first = _run_cli(
        "plan",
        "--artifact-dir",
        str(tmp_path),
        "--run-id",
        RUN_ID,
        "--lineage-id",
        LINEAGE_ID,
    )
    assert first.returncode == ExitCode.COMPLETED, first.stderr
    benchmark = _run_cli("benchmark", "--artifact-dir", str(tmp_path))
    assert benchmark.returncode == ExitCode.COMPLETED, benchmark.stderr
    plan_path = tmp_path / "dataset-plan.json"
    before = plan_path.read_bytes()
    second = _run_cli(
        "plan",
        "--artifact-dir",
        str(tmp_path),
        "--run-id",
        str(uuid.uuid4()),
        "--lineage-id",
        str(uuid.uuid4()),
    )
    assert second.returncode == ExitCode.INVALID
    assert "never overwrites" in second.stderr
    assert plan_path.read_bytes() == before
    cli_module._scan_bundle(tmp_path)  # noqa: SLF001 - prior bundle remains valid


def test_external_plan_conflict_never_overwrites_an_existing_bundle(tmp_path: Path) -> None:
    initial = _run_cli(
        "plan",
        "--artifact-dir",
        str(tmp_path),
        "--run-id",
        RUN_ID,
        "--lineage-id",
        LINEAGE_ID,
    )
    assert initial.returncode == ExitCode.COMPLETED, initial.stderr
    bundled_plan_path = tmp_path / "dataset-plan.json"
    original = bundled_plan_path.read_bytes()
    conflicting = _plan()
    conflicting["run_id"] = str(uuid.uuid4())
    conflicting["lineage_id"] = str(uuid.uuid4())
    conflicting["namespace"] = f"b24api-evidence-{conflicting['run_id']}"
    external_path = tmp_path.parent / "conflicting-plan.json"
    external_path.write_text(json.dumps(conflicting))
    result = _run_cli(
        "resume",
        "--artifact-dir",
        str(tmp_path),
        "--plan",
        str(external_path),
        "--manifest",
        str(tmp_path / "missing-manifest.jsonl"),
    )
    assert result.returncode == ExitCode.INVALID
    assert bundled_plan_path.read_bytes() == original


def test_bundle_refuses_unbounded_file_count_before_parsing_documents(tmp_path: Path) -> None:
    for index in range(BUNDLE_OVERFLOW_FILES):
        (tmp_path / f"unreferenced-{index}.json").write_text("{}")
    with pytest.raises(ContractError, match="file-count ceiling"):
        cli_module._scan_bundle(tmp_path)  # noqa: SLF001 - aggregate-bound regression


def test_admission_ready_benchmark_never_runs_the_model_matrix_as_a_substitute(tmp_path: Path) -> None:
    dataset_plan = _plan(count=0)
    benchmark_plan = _benchmark_plan()
    benchmark_plan["dataset_plan_content_hash"] = content_sha256(dataset_plan)
    benchmark_plan["cases"][0]["id"] = "FAKE-UNEXECUTED-LIVE-CELL"
    plan_path = tmp_path / "dataset-plan-input.json"
    benchmark_path = tmp_path / "benchmark-plan-input.json"
    plan_path.write_text(json.dumps(dataset_plan))
    benchmark_path.write_text(json.dumps(benchmark_plan))
    result = _run_cli(
        "benchmark",
        "--artifact-dir",
        str(tmp_path / "artifacts"),
        "--plan",
        str(plan_path),
        "--benchmark-plan",
        str(benchmark_path),
    )
    assert result.returncode == ExitCode.UNAVAILABLE
    assert "admission-ready" in result.stderr


def test_model_benchmark_rejects_run_controls_it_cannot_execute_exactly(tmp_path: Path) -> None:
    initial = _run_cli("plan", "--artifact-dir", str(tmp_path))
    assert initial.returncode == ExitCode.COMPLETED, initial.stderr
    benchmark_path = tmp_path / "benchmark-plan.json"
    benchmark_plan = json.loads(benchmark_path.read_text())
    benchmark_plan["controls"]["warmups"] = 3
    benchmark_path.write_text(json.dumps(benchmark_plan))
    result = _run_cli(
        "benchmark",
        "--artifact-dir",
        str(tmp_path / "output"),
        "--plan",
        str(tmp_path / "dataset-plan.json"),
        "--benchmark-plan",
        str(benchmark_path),
    )
    assert result.returncode == ExitCode.INVALID
    assert "exact bounded draft run controls" in result.stderr
    assert not (tmp_path / "output/model-matrix.json").exists()


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
    monkeypatch.setattr(cli_module, "_require_review_commit", lambda *_args, **_kwargs: None)
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
            **_approval_arguments(plan),
        ),
    )
    assert result == ExitCode.COMPLETED
    assert adapter.deleted
    latest = load_manifest(manifest_path)[-1]
    assert latest["event"] == "absence_verified"
    validate_manifest_against_plan(load_manifest(manifest_path), plan)


def test_cleanup_resumes_after_delete_dispatch_without_corrupting_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _approved_live_plan()
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    manifest_path = tmp_path / "manifest.jsonl"
    previous = build_manifest_record(_manifest_base(plan))
    append_manifest_record(manifest_path, previous)
    for event in ("create_dispatched", "created", "verified", "delete_dispatched"):
        previous = build_manifest_record(
            {
                **_manifest_base(plan),
                "event": event,
                "entity_id": None if event == "create_dispatched" else "owned-123",
                "request_fingerprint": SHA256,
            },
            previous=previous,
        )
        append_manifest_record(manifest_path, previous)

    class FakeAdapter:
        delete_method = "tasks.task.delete"
        id_parameter = "taskId"
        deleted = False
        delete_calls = 0

        def read(self, _portal: object, _entity_id: str) -> dict[str, Any] | None:
            return None if self.deleted else {"TITLE": previous["marker_value"]}

        def delete(self, _portal: object, _entity_id: str) -> None:
            self.delete_calls += 1
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
    monkeypatch.setattr(cli_module, "_require_review_commit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli_module, "_bind_plan_to_bundle", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli_module, "_require_exact_candidate", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli_module, "require_clean_tracked_tree", lambda *_args, **_kwargs: None)
    result = cli_module._cleanup(  # noqa: SLF001 - direct crash-window regression
        Namespace(
            live=True,
            allow_writes=True,
            plan=plan_path,
            manifest=manifest_path,
            artifact_dir=tmp_path,
            run_id=None,
            lineage_id=None,
            credential_role="admin_full",
            **_approval_arguments(plan),
        ),
    )
    assert result == ExitCode.COMPLETED
    records = load_manifest(manifest_path)
    assert records[-1]["event"] == "absence_verified"
    assert [record["event"] for record in records][-4:] == [
        "delete_dispatched",
        "delete_dispatched",
        "deleted",
        "absence_verified",
    ]
    assert adapter.delete_calls == 1
    validate_manifest_against_plan(records, plan)


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
    monkeypatch.setattr(cli_module, "_require_review_commit", lambda *_args, **_kwargs: None)
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
            **_approval_arguments(plan),
        ),
    )
    assert result == ExitCode.INCOMPLETE
    assert adapter.create_calls == 0
    records = load_manifest(manifest_path)
    assert records[-1]["event"] == "create_dispatched"
    validate_manifest_against_plan(records, plan)


@pytest.mark.parametrize("drift_kind", ["dirty", "clean_head"])
@pytest.mark.parametrize("drift_timing", ["preflight", "journal"])
def test_seed_rechecks_exact_candidate_and_remains_resumable_before_create(  # noqa: C901
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    drift_kind: str,
    drift_timing: str,
) -> None:
    plan = _approved_live_plan()
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    dirty = False
    head_changed = False
    induce_drift = True

    class FakeAdapter:
        create_method = "tasks.task.add"
        create_calls = 0
        marker: str | None = None

        def find_exact_marker(self, _portal: object, _marker: str) -> list[str]:
            return []

        def create(self, _portal: object, marker: str) -> str:
            self.create_calls += 1
            self.marker = marker
            return "42"

        def read(self, _portal: object, _entity_id: str) -> dict[str, str] | None:
            return {"TITLE": self.marker} if self.marker is not None else None

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
            nonlocal dirty, head_changed
            assert required_scopes == {"task"}
            if induce_drift and drift_timing == "preflight":
                dirty = drift_kind == "dirty"
                head_changed = drift_kind == "clean_head"
            return LivePreflight(self.identity, "build-1", frozenset({"task"}))

    def reject_after_preflight(_root: Path) -> None:
        if dirty:
            raise ContractError("tracked repository changed before live mutation")

    real_append = append_manifest_record

    def append_with_optional_drift(path: Path, record: Mapping[str, Any]) -> None:
        nonlocal dirty, head_changed
        real_append(path, record)
        if induce_drift and drift_timing == "journal" and record["event"] == "create_dispatched":
            dirty = drift_kind == "dirty"
            head_changed = drift_kind == "clean_head"

    monkeypatch.delenv("PYTEST_CURRENT_TEST")
    monkeypatch.setattr(cli_module, "LivePortal", FakePortal)
    monkeypatch.setattr(cli_module, "require_clean_tracked_tree", reject_after_preflight)
    monkeypatch.setattr(cli_module, "git_sha", lambda _root: "f" * 40 if head_changed else SHA)
    monkeypatch.setattr(cli_module, "append_manifest_record", append_with_optional_drift)
    monkeypatch.setitem(ADAPTERS, "tasks-task-v1", adapter)
    monkeypatch.setattr(cli_module, "_require_review_commit", lambda *_args, **_kwargs: None)
    arguments = Namespace(
        live=True,
        allow_writes=True,
        plan=plan_path,
        manifest=None,
        artifact_dir=tmp_path / "artifacts",
        run_id=None,
        lineage_id=None,
        credential_role="admin_full",
        **_approval_arguments(plan),
    )
    with pytest.raises(ContractError, match=r"before live mutation|candidate SHA differs"):
        cli_module._seed(arguments)  # noqa: SLF001 - exact pre-mutation race regression
    assert adapter.create_calls == 0
    records = load_manifest(tmp_path / "artifacts" / "manifest.jsonl")
    expected_event = "planned" if drift_timing == "preflight" else "create_cancelled"
    assert records[-1]["event"] == expected_event

    induce_drift = False
    dirty = False
    head_changed = False
    assert cli_module._seed(arguments) == ExitCode.COMPLETED  # noqa: SLF001
    assert adapter.create_calls == 1


@pytest.mark.parametrize("drift_timing", ["preflight", "journal"])
def test_cleanup_rechecks_clean_candidate_immediately_before_delete(  # noqa: C901
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    drift_timing: str,
) -> None:
    plan = _approved_live_plan()
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
    dirty = False
    induce_drift = True

    class FakeAdapter:
        delete_method = "tasks.task.delete"
        id_parameter = "taskId"
        delete_calls = 0
        deleted = False

        def read(self, _portal: object, _entity_id: str) -> dict[str, str] | None:
            return None if self.deleted else {"TITLE": str(previous["marker_value"])}

        def delete(self, _portal: object, _entity_id: str) -> None:
            self.delete_calls += 1
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
            nonlocal dirty
            assert required_scopes == {"task"}
            if induce_drift and drift_timing == "preflight":
                dirty = True
            return LivePreflight(self.identity, "build-1", frozenset({"task"}))

    def reject_after_preflight(_root: Path) -> None:
        if dirty:
            raise ContractError("tracked repository changed before live mutation")

    real_append = append_manifest_record

    def append_with_optional_drift(path: Path, record: Mapping[str, Any]) -> None:
        nonlocal dirty
        real_append(path, record)
        if induce_drift and drift_timing == "journal" and record["event"] == "delete_dispatched":
            dirty = True

    monkeypatch.delenv("PYTEST_CURRENT_TEST")
    monkeypatch.setattr(cli_module, "LivePortal", FakePortal)
    monkeypatch.setattr(cli_module, "require_clean_tracked_tree", reject_after_preflight)
    monkeypatch.setattr(cli_module, "append_manifest_record", append_with_optional_drift)
    monkeypatch.setitem(ADAPTERS, "tasks-task-v1", adapter)
    monkeypatch.setattr(cli_module, "_require_review_commit", lambda *_args, **_kwargs: None)
    with pytest.raises(ContractError, match="before live mutation"):
        cli_module._cleanup(  # noqa: SLF001 - exact pre-mutation race regression
            Namespace(
                live=True,
                allow_writes=True,
                plan=plan_path,
                manifest=manifest_path,
                artifact_dir=tmp_path / "artifacts",
                run_id=None,
                lineage_id=None,
                credential_role="admin_full",
                **_approval_arguments(plan),
            ),
        )
    assert adapter.delete_calls == 0
    records = load_manifest(manifest_path)
    expected_event = "verified" if drift_timing == "preflight" else "delete_cancelled"
    assert records[-1]["event"] == expected_event

    induce_drift = False
    dirty = False
    assert (
        cli_module._cleanup(  # noqa: SLF001
            Namespace(
                live=True,
                allow_writes=True,
                plan=plan_path,
                manifest=manifest_path,
                artifact_dir=tmp_path / "artifacts",
                run_id=None,
                lineage_id=None,
                credential_role="admin_full",
                **_approval_arguments(plan),
            ),
        )
        == ExitCode.COMPLETED
    )
    assert adapter.delete_calls == 1


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
    assert not any(name.startswith("b24api/") and name.endswith("_test.py") for name in names)


def _run_cli(*arguments: str, environment: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed interpreter/entrypoint with test-controlled arguments
        [sys.executable, str(ENTRYPOINT), *arguments],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
