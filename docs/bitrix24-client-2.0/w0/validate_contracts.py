# ruff: noqa: INP001
"""Validate W0 evidence schemas and their cross-field semantic contracts."""

from __future__ import annotations
import argparse
import hashlib
import json
import math
import re
import sys
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator  # type: ignore[import-untyped]

ROOT = Path(__file__).parent
SCHEMAS = {
    "disposable-entity-profiles": "disposable-entity-profiles.schema.json",
    "dataset-plan": "dataset-plan.schema.json",
    "dataset-manifest-record": "dataset-manifest-record.schema.json",
    "oracle-record": "oracle-record.schema.json",
    "benchmark-plan": "benchmark-plan.schema.json",
    "evidence-artifact": "evidence-artifact.schema.json",
    "batch-chaining-probe": "batch-chaining-probe.schema.json",
}
SECRET_PATTERNS = (
    re.compile(r"https?://[^\s]+/rest/[0-9]+/[A-Za-z0-9_-]{8,}", re.IGNORECASE),
    re.compile(r"/rest/[0-9]+/[A-Za-z0-9_-]{8,}", re.IGNORECASE),
    re.compile(r"(?:[?&](?:auth|access_token)=|bearer\s+)[^\s&]+", re.IGNORECASE),
    re.compile(r"(?:client_secret|refresh_token)\s*[:=]\s*[^\s,}]+", re.IGNORECASE),
    re.compile(r"[\"']?auth[\"']?\s*[:=]\s*[\"']?[A-Za-z0-9_-]{8,}", re.IGNORECASE),
    re.compile(r"(?:AUTH_ID|APPLICATION_TOKEN)\s*=\s*[^\s,}]+", re.IGNORECASE),
)


class ContractError(ValueError):
    """Raised when structural or semantic evidence validation fails."""


def _reject_json_constant(value: str) -> None:
    raise ContractError(f"non-standard or non-finite JSON number: {value}")


def _strict_json_loads(text: str) -> Any:  # noqa: ANN401
    return json.loads(text, parse_constant=_reject_json_constant)


def _load_schema(name: str) -> dict[str, Any]:
    return cast("dict[str, Any]", _strict_json_loads((ROOT / SCHEMAS[name]).read_text()))


def _content_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _walk_strings(value: Any, path: str = "$") -> list[tuple[str, str]]:  # noqa: ANN401
    if isinstance(value, str):
        return [(path, value)]
    if isinstance(value, list):
        found: list[tuple[str, str]] = []
        for index, item in enumerate(value):
            found.extend(_walk_strings(item, f"{path}[{index}]"))
        return found
    if isinstance(value, dict):
        found = []
        for key, item in value.items():
            found.extend(_walk_strings(item, f"{path}.{key}"))
        return found
    return []


def _scan_secrets(document: dict[str, Any]) -> list[str]:
    errors = []
    for path, value in _walk_strings(document):
        if any(pattern.search(value) for pattern in SECRET_PATTERNS):
            errors.append(f"{path}: secret-like value")
    return errors


def _finite_number_errors(value: Any, path: str = "$") -> list[str]:  # noqa: ANN401
    if isinstance(value, float) and not math.isfinite(value):
        return [f"{path}: number must be finite"]
    if isinstance(value, list):
        errors: list[str] = []
        for index, item in enumerate(value):
            errors.extend(_finite_number_errors(item, f"{path}[{index}]"))
        return errors
    if isinstance(value, dict):
        errors = []
        for key, item in value.items():
            errors.extend(_finite_number_errors(item, f"{path}.{key}"))
        return errors
    return []


def _manifest_record_hash(document: dict[str, Any]) -> str:
    payload = {key: value for key, value in document.items() if key != "record_hash"}
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _profile_map() -> dict[str, dict[str, Any]]:
    profile_set = cast(
        "dict[str, Any]",
        _strict_json_loads((ROOT / "disposable-entity-profiles.json").read_text()),
    )
    validate_document("disposable-entity-profiles", profile_set)
    return {profile["id"]: profile for profile in profile_set["profiles"]}


def _disposable_profile_errors(document: dict[str, Any]) -> list[str]:
    ids = [profile["id"] for profile in document["profiles"]]
    triples = [
        (profile["create_method"], profile["read_method"], profile["delete_method"])
        for profile in document["profiles"]
    ]
    errors = []
    if len(ids) != len(set(ids)):
        errors.append("disposable profile ids must be unique")
    if len(triples) != len(set(triples)):
        errors.append("disposable method triples must be unique")
    errors.extend(
        f"{profile['id']}: create and delete methods must differ"
        for profile in document["profiles"]
        if profile["create_method"] == profile["delete_method"]
    )
    return errors


def _dataset_plan_errors(document: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if document["namespace"] != f"b24api-evidence-{document['run_id']}":
        errors.append("namespace must be derived exactly from run_id")
    cell_ids = [cell["id"] for cell in document["cells"]]
    if len(cell_ids) != len(set(cell_ids)):
        errors.append("dataset cell ids must be unique")
    profile_path = ROOT / "disposable-entity-profiles.json"
    if document["disposable_profiles_content_hash"] != _content_sha256(profile_path):
        errors.append("disposable profile content hash does not match reviewed profile set")
    profiles = _profile_map()
    bounded_entities = 0
    ceiling = document["authorization"]["max_entities_per_cell"]
    profile_fields = (
        "entity_family",
        "create_method",
        "read_method",
        "delete_method",
        "marker_field",
        "required_scopes",
    )
    for cell in document["cells"]:
        profile = profiles.get(cell["disposable_profile_id"])
        if profile is None:
            errors.append(f"{cell['id']}: unknown disposable profile")
            continue
        errors.extend(
            f"{cell['id']}: {field} does not match disposable profile"
            for field in profile_fields
            if cell.get(field) != profile[field]
        )
        cell_entities = max(cell["target_count"], cell.get("base_count", 0))
        bounded_entities += cell_entities
        if cell_entities > ceiling:
            errors.append(f"{cell['id']}: entity count exceeds reviewed per-cell ceiling")
    if document["estimated"]["entities"] < bounded_entities:
        errors.append("estimated entities understate the bounded dataset size")
    return errors


def _manifest_errors(document: dict[str, Any]) -> list[str]:
    errors = []
    if document["namespace"] != f"b24api-evidence-{document['run_id']}":
        errors.append("namespace must be derived exactly from run_id")
    if document["record_hash"] != _manifest_record_hash(document):
        errors.append("record_hash does not match canonical record content")
    marker = document["marker_value"]
    if marker is not None and not marker.startswith(f"{document['namespace']}:"):
        errors.append("marker_value must be derived from the exact run namespace")
    return errors


def _oracle_errors(document: dict[str, Any]) -> list[str]:
    errors = []
    expected = document["expected"]
    if expected["unique_count"] > expected["raw_count"]:
        errors.append("unique_count cannot exceed raw_count")
    snapshot = document["snapshot"]
    if (
        document["outcome"] == "PASS"
        and snapshot["requirement"] == "independent_pre_post_oracle"
        and snapshot["pre_hash"] != snapshot["post_hash"]
    ):
        errors.append("PASS requires equal independent pre/post hashes")
    return errors


def _benchmark_errors(document: dict[str, Any]) -> list[str]:
    errors = []
    case_ids = [case["id"] for case in document["cases"]]
    if len(case_ids) != len(set(case_ids)):
        errors.append("benchmark case ids must be unique")
    errors.extend(
        f"{case['id']}: dataset plan content hash does not match plan lineage"
        for case in document["cases"]
        if case["dataset_plan_content_hash"] != document["dataset_plan_content_hash"]
    )
    for case in document["cases"]:
        if case["dataset_plan_ref"] != f"sha256:{case['dataset_plan_content_hash']}":
            errors.append(f"{case['id']}: dataset plan ref does not match content hash")
        if case["oracle_ref"] != f"sha256:{case['oracle_content_hash']}":
            errors.append(f"{case['id']}: oracle ref does not match content hash")
    if document["admission_state"] == "admission_ready" and not all(
        case["benefit_gate"]["blocking"] for case in document["cases"]
    ):
        errors.append("admission_ready requires every case to use a blocking benefit gate")
    return errors


def _evidence_errors(document: dict[str, Any]) -> list[str]:
    errors = []
    started = datetime.fromisoformat(document["started_at"])
    finished = datetime.fromisoformat(document["finished_at"])
    if finished < started:
        errors.append("finished_at cannot precede started_at")
    metrics = document["metrics"]
    if metrics["kind"] == "benchmark" and metrics["unique_rows"] > metrics["raw_rows"]:
        errors.append("unique_rows cannot exceed raw_rows")
    return errors


SEMANTIC_VALIDATORS = {
    "disposable-entity-profiles": _disposable_profile_errors,
    "dataset-plan": _dataset_plan_errors,
    "dataset-manifest-record": _manifest_errors,
    "oracle-record": _oracle_errors,
    "benchmark-plan": _benchmark_errors,
    "evidence-artifact": _evidence_errors,
}


def _semantic_errors(name: str, document: dict[str, Any]) -> list[str]:
    validator = SEMANTIC_VALIDATORS.get(name)
    return validator(document) if validator is not None else []


def validate_document(name: str, document: dict[str, Any]) -> None:
    """Validate one artifact against its schema and semantic invariants."""
    schema = _load_schema(name)
    structural = sorted(Draft202012Validator(schema).iter_errors(document), key=lambda error: list(error.path))
    errors = [f"{'.'.join(map(str, error.path)) or '$'}: {error.message}" for error in structural]
    errors.extend(_finite_number_errors(document))
    if not structural:
        errors.extend(_semantic_errors(name, document))
    errors.extend(_scan_secrets(document))
    if errors:
        raise ContractError("; ".join(errors))


UUID = "11111111-1111-4111-8111-111111111111"
LINEAGE_UUID = "22222222-2222-4222-8222-222222222222"
SHA = "a" * 40
SHA256 = "b" * 64
OTHER_SHA256 = "c" * 64
SECRET_EXAMPLE = "https://portal.test/" + "rest/1/" + "abcdef1234567890/"


def _dataset_plan() -> dict[str, Any]:
    return {
        "schema_version": "1.2",
        "run_id": UUID,
        "lineage_id": LINEAGE_UUID,
        "original_head_sha": SHA,
        "candidate_sha": SHA,
        "generator_sha": SHA,
        "skills_corpus_sha": SHA,
        "skills_recipe_tree_sha256": SHA256,
        "disposable_profiles_content_hash": _content_sha256(ROOT / "disposable-entity-profiles.json"),
        "portal": {
            "host": "example.bitrix24.test",
            "fingerprint": SHA256,
            "fingerprint_algorithm": "hmac-sha256-portal-role-principal-v1",
            "build": None,
            "scope_hash": SHA256,
        },
        "credential_role": "admin_full",
        "namespace": f"b24api-evidence-{UUID}",
        "cells": [
            {
                "id": "T-S",
                "disposable_profile_id": "tasks-task-v1",
                "entity_family": "task",
                "target_count": 1,
                "base_count": 1,
                "distribution": "boundary",
                "marker_field": "TITLE",
                "create_method": "tasks.task.add",
                "read_method": "tasks.task.get",
                "delete_method": "tasks.task.delete",
                "dependencies": [],
                "required_scopes": ["task"],
            },
        ],
        "estimated": {"entities": 1, "relationships": 0, "requests": 3, "batch_commands": 0, "duration_seconds": 1},
        "cleanup": {"feasible": True, "dependency_order": ["task"], "absence_verification": "exact point read"},
        "authorization": {
            "state": "preview",
            "live": True,
            "allow_writes": False,
            "plan_review_sha": None,
            "approved_by_user": False,
            "max_entities_per_cell": 1,
        },
    }


def _manifest_record() -> dict[str, Any]:
    document = {
        "schema_version": "1.2",
        "sequence": 1,
        "run_id": UUID,
        "lineage_id": LINEAGE_UUID,
        "dataset_plan_content_hash": SHA256,
        "skills_corpus_sha": SHA,
        "skills_recipe_tree_sha256": SHA256,
        "portal_fingerprint": SHA256,
        "candidate_sha": SHA,
        "namespace": f"b24api-evidence-{UUID}",
        "event": "created",
        "cell_id": "T-S",
        "entity_family": "task",
        "correlation_key": SHA256,
        "entity_id": "1",
        "parent_correlation_keys": [],
        "marker_hash": SHA256,
        "marker_value": f"b24api-evidence-{UUID}:task-1",
        "request_fingerprint": SHA256,
        "previous_record_hash": SHA256,
        "record_hash": "",
        "recorded_at": "2026-08-19T20:00:00Z",
        "safe_error": None,
    }
    document["record_hash"] = _manifest_record_hash(document)
    return document


def _oracle_record() -> dict[str, Any]:
    return {
        "schema_version": "1.2",
        "run_id": UUID,
        "lineage_id": LINEAGE_UUID,
        "case_id": "T-S",
        "candidate_sha": SHA,
        "skills_corpus_sha": SHA,
        "skills_recipe_tree_sha256": SHA256,
        "dataset_plan_content_hash": SHA256,
        "manifest_content_hash": SHA256,
        "portal_fingerprint": SHA256,
        "qualification": {
            "kind": "immutable_manifest",
            "cross_check": "exact point reads",
            "defects": [],
            "visibility_differences": [],
            "missing_row_behavior": "not found",
        },
        "normalization": {
            "identity_mode": "exact_integer",
            "ordering": "ascending",
            "duplicate_semantics": "ordered_sequence",
        },
        "expected": {
            "raw_count": 1,
            "unique_count": 1,
            "identity_hash": SHA256,
            "multiset_hash": SHA256,
            "order_hash": SHA256,
        },
        "snapshot": {
            "requirement": "independent_pre_post_oracle",
            "state": "verified",
            "pre_hash": SHA256,
            "post_hash": SHA256,
            "quarantine_attempts": 0,
        },
        "outcome": "PASS",
        "safe_notes": [],
    }


def _benchmark_plan() -> dict[str, Any]:
    return {
        "schema_version": "1.2",
        "plan_id": UUID,
        "lineage_id": LINEAGE_UUID,
        "admission_state": "admission_ready",
        "original_head_sha": SHA,
        "fixed_1x_sha": SHA,
        "candidate_sha": SHA,
        "skills_corpus_sha": SHA,
        "skills_recipe_tree_sha256": SHA256,
        "dataset_plan_content_hash": SHA256,
        "manifest_content_hash": SHA256,
        "instrumentation_review_sha": SHA,
        "controls": {
            "python_version": "3.12.10",
            "tool_versions": {"b24api": "2.0.dev", "httpx": "0.28.1", "pytest": "9.0.3"},
            "warmups": 1,
            "advisory_runs": 5,
            "blocking_pairs": 9,
            "interleaving": True,
            "drift": {"status": "preregistered", "max_rtt_ratio": 1.2, "max_operating_ratio": 1.2},
        },
        "cases": [
            {
                "id": "T-S",
                "method": "tasks.task.list",
                "dataset_plan_ref": "sha256:" + SHA256,
                "dataset_plan_content_hash": SHA256,
                "compared_plans": ["keyset", "partitioned_keyset"],
                "oracle_ref": "sha256:" + SHA256,
                "oracle_content_hash": SHA256,
                "consistency_contract": "exact ordered identities",
                "correctness_gate": {
                    "exact_result_contract": True,
                    "zero_unresolved_failures": True,
                    "budget_formula": "bounded by pages",
                },
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


def _evidence_artifact() -> dict[str, Any]:
    return {
        "schema_version": "1.2",
        "run_id": UUID,
        "lineage_id": LINEAGE_UUID,
        "portal_fingerprint": SHA256,
        "host": "example.bitrix24.test",
        "command": "benchmark",
        "phase": "candidate",
        "case_id": "T-S",
        "original_head_sha": SHA,
        "fixed_1x_sha": SHA,
        "candidate_sha": SHA,
        "skills_corpus_sha": SHA,
        "skills_recipe_tree_sha256": SHA256,
        "dataset_plan_content_hash": SHA256,
        "manifest_content_hash": SHA256,
        "profile_versions": [],
        "plan_versions": ["keyset-1"],
        "runtime": {"python": "3.12.10", "b24api": "2.0.dev", "httpx": "0.28.1"},
        "started_at": "2026-08-19T20:00:00Z",
        "finished_at": "2026-08-19T20:00:01Z",
        "outcome": "PASS",
        "terminal_state": "completed",
        "assurance": "oracle_verified",
        "snapshot_requirement": "independent_pre_post_oracle",
        "snapshot_state": "verified",
        "metrics": {
            "kind": "benchmark",
            "http_attempts": 1,
            "logical_pages": 1,
            "batch_requests": 0,
            "batch_commands": 0,
            "time_to_first_row_seconds": 0.1,
            "wall_seconds": 1.0,
            "server_operating_seconds": 0.1,
            "retries": 0,
            "cooldown_seconds": 0,
            "buffered_rows_high_water": 1,
            "rss_delta_bytes": 0,
            "raw_rows": 1,
            "unique_rows": 1,
            "overlap": 1,
            "duplicates": 0,
            "shortfall": 0,
            "overfetch": 0,
            "reference_failures": 0,
        },
        "controls": {
            "rtt_before_seconds": 0.1,
            "rtt_after_seconds": 0.1,
            "operating_before_seconds": 0.1,
            "operating_after_seconds": 0.1,
            "drift_quarantined": False,
        },
        "evidence_refs": ["sha256:" + SHA256],
        "safe_violations": [],
    }


def _probe_artifact() -> dict[str, Any]:
    return {
        "schema_version": "1.2",
        "observed_at": "2026-08-19T20:00:00Z",
        "runner": {
            "kind": "committed_python_httpx",
            "repository_head_sha": SHA,
            "source_sha256": SHA256,
            "python": "3.12.10",
            "httpx": "0.28.1",
        },
        "host": "example.bitrix24.test",
        "portal_fingerprint": SHA256,
        "portal_fingerprint_algorithm": "hmac-sha256-portal-role-principal-v1",
        "credential_role": "admin_full",
        "read_only": True,
        "request_count": 1,
        "command_keys": ["who", "dependent"],
        "request_shape": {"halt": 0, "cmd": {"who": "profile", "dependent": "user.get?ID=$result[who][ID]"}},
        "response_summary": {
            "http_status": 200,
            "envelope_shape_valid": True,
            "structured_error": False,
            "command_error_keys": [],
            "unexpected_command_error_count": 0,
            "profile_identity_present": True,
            "dependent_row_count": 1,
            "dependent_identity_matched": True,
        },
        "outcome": "PASS",
        "conclusion": "supported_for_observed_query_shape_only",
        "authorization_effect": "none",
    }


def _must_reject(name: str, document: dict[str, Any], label: str) -> None:
    try:
        validate_document(name, document)
    except ContractError:
        return
    raise AssertionError(f"accepted invalid case: {label}")


def _rehash_manifest(document: dict[str, Any]) -> None:
    document["record_hash"] = _manifest_record_hash(document)


def self_test() -> None:  # noqa: PLR0915
    """Exercise positive contracts and every review regression."""
    valid = {
        "disposable-entity-profiles": cast(
            "dict[str, Any]",
            _strict_json_loads((ROOT / "disposable-entity-profiles.json").read_text()),
        ),
        "dataset-plan": _dataset_plan(),
        "dataset-manifest-record": _manifest_record(),
        "oracle-record": _oracle_record(),
        "benchmark-plan": _benchmark_plan(),
        "evidence-artifact": _evidence_artifact(),
        "batch-chaining-probe": _probe_artifact(),
    }
    for name, document in valid.items():
        Draft202012Validator.check_schema(_load_schema(name))
        validate_document(name, document)

    draft_benchmark = deepcopy(valid["benchmark-plan"])
    draft_benchmark["admission_state"] = "draft"
    draft_benchmark["controls"]["drift"] = {
        "status": "tbd_live",
        "max_rtt_ratio": "TBD-LIVE",
        "max_operating_ratio": "TBD-LIVE",
    }
    validate_document("benchmark-plan", draft_benchmark)

    churn_oracle = deepcopy(valid["oracle-record"])
    churn_oracle["outcome"] = "INCONCLUSIVE"
    churn_oracle["snapshot"].update(
        {"state": "changed", "pre_hash": SHA256, "post_hash": OTHER_SHA256, "quarantine_attempts": 3},
    )
    validate_document("oracle-record", churn_oracle)

    operation_artifact = deepcopy(valid["evidence-artifact"])
    operation_artifact.update(
        {
            "command": "cleanup",
            "case_id": None,
            "assurance": "caller_asserted",
            "snapshot_requirement": "traversal_only",
            "snapshot_state": "not_requested",
            "metrics": {"kind": "operation", "http_attempts": 1, "wall_seconds": 0.1},
            "controls": None,
        },
    )
    validate_document("evidence-artifact", operation_artifact)

    empty_oracle = deepcopy(valid["oracle-record"])
    empty_oracle["expected"].update({"raw_count": 0, "unique_count": 0})
    empty_oracle["safe_notes"] = ["Empty boundary result is an intentional exact PASS case"]
    validate_document("oracle-record", empty_oracle)

    warning_artifact = deepcopy(valid["evidence-artifact"])
    warning_artifact["safe_violations"] = [
        {"severity": "warning", "code": "advisory", "message": "non-blocking observation", "field": None},
    ]
    validate_document("evidence-artifact", warning_artifact)

    try:
        _strict_json_loads('{"value": NaN}')
    except ContractError:
        pass
    else:
        raise AssertionError("strict JSON parser accepted NaN")

    invalid: list[tuple[str, dict[str, Any], str]] = []
    case = deepcopy(valid["dataset-plan"])
    case["run_id"] = "x"
    invalid.append(("dataset-plan", case, "annotative UUID"))
    case = deepcopy(valid["dataset-plan"])
    case["namespace"] = "b24api-evidence-" + "-" * 36
    invalid.append(("dataset-plan", case, "degenerate namespace"))
    case = deepcopy(valid["dataset-plan"])
    case["namespace"] = f"b24api-evidence-{LINEAGE_UUID}"
    invalid.append(("dataset-plan", case, "namespace/run mismatch"))
    case = deepcopy(valid["dataset-plan"])
    case["cells"].append(deepcopy(case["cells"][0]))
    invalid.append(("dataset-plan", case, "duplicate cell id"))
    case = deepcopy(valid["dataset-plan"])
    cell = case["cells"][0]
    cell.update(
        {
            "disposable_profile_id": "crm-deal-v1",
            "entity_family": "crm_deal",
            "create_method": "crm.deal.delete",
            "read_method": "crm.deal.get",
            "delete_method": "crm.deal.add",
            "required_scopes": ["crm"],
        },
    )
    invalid.append(("dataset-plan", case, "reversed disposable methods"))
    case = deepcopy(valid["dataset-plan"])
    case["disposable_profiles_content_hash"] = OTHER_SHA256
    invalid.append(("dataset-plan", case, "unbound disposable profile set"))
    case = deepcopy(valid["dataset-plan"])
    case["cells"][0]["target_count"] = 2
    case["authorization"]["max_entities_per_cell"] = 2
    invalid.append(("dataset-plan", case, "understated entity estimate"))
    case = deepcopy(valid["dataset-plan"])
    case["cells"][0]["target_count"] = 2
    invalid.append(("dataset-plan", case, "cell exceeds reviewed ceiling"))

    case = deepcopy(valid["dataset-manifest-record"])
    case["entity_id"] = None
    invalid.append(("dataset-manifest-record", case, "created without entity id"))
    case = deepcopy(valid["dataset-manifest-record"])
    case["event"] = "ambiguous"
    case["entity_id"] = None
    case["marker_hash"] = None
    case["marker_value"] = None
    _rehash_manifest(case)
    invalid.append(("dataset-manifest-record", case, "ambiguous without reconciliation marker"))
    case = deepcopy(valid["dataset-manifest-record"])
    case["event"] = "create_dispatched"
    case["entity_id"] = None
    case["marker_hash"] = None
    case["marker_value"] = None
    _rehash_manifest(case)
    invalid.append(("dataset-manifest-record", case, "dispatched create without reconciliation marker"))
    case = deepcopy(valid["dataset-manifest-record"])
    case["sequence"] = 0
    _rehash_manifest(case)
    invalid.append(("dataset-manifest-record", case, "genesis with previous hash"))
    case = deepcopy(valid["dataset-manifest-record"])
    case["previous_record_hash"] = None
    _rehash_manifest(case)
    invalid.append(("dataset-manifest-record", case, "non-genesis without previous hash"))
    case = deepcopy(valid["dataset-manifest-record"])
    case["marker_value"] = f"b24api-evidence-{LINEAGE_UUID}:task-1"
    _rehash_manifest(case)
    invalid.append(("dataset-manifest-record", case, "marker from different namespace"))
    case = deepcopy(valid["dataset-manifest-record"])
    case["safe_error"] = {"url": SECRET_EXAMPLE + "user.get.json"}
    invalid.append(("dataset-manifest-record", case, "untyped secret-bearing error"))
    case = deepcopy(valid["dataset-manifest-record"])
    case["record_hash"] = OTHER_SHA256
    invalid.append(("dataset-manifest-record", case, "incorrect canonical record hash"))

    case = deepcopy(valid["oracle-record"])
    case["snapshot"].update({"state": "changed", "pre_hash": None, "post_hash": None})
    invalid.append(("oracle-record", case, "PASS with changed snapshot"))
    case = deepcopy(valid["oracle-record"])
    case["snapshot"]["post_hash"] = OTHER_SHA256
    invalid.append(("oracle-record", case, "PASS with unequal pre/post hashes"))
    case = deepcopy(valid["oracle-record"])
    case["expected"]["unique_count"] = 2
    invalid.append(("oracle-record", case, "unique exceeds raw"))
    case = deepcopy(valid["oracle-record"])
    case["expected"].update({"identity_hash": None, "multiset_hash": None, "order_hash": None})
    invalid.append(("oracle-record", case, "PASS without hashes"))
    case = deepcopy(valid["oracle-record"])
    case["safe_notes"] = [SECRET_EXAMPLE]
    invalid.append(("oracle-record", case, "secret-bearing note"))
    case = deepcopy(valid["oracle-record"])
    case["safe_notes"] = ['{"auth": "abcdef1234567890"}']
    invalid.append(("oracle-record", case, "JSON auth secret-bearing note"))
    case = deepcopy(valid["oracle-record"])
    case["safe_notes"] = ["AUTH_ID=abcdef1234567890 APPLICATION_TOKEN=zyxwvutsrqponmlk"]
    invalid.append(("oracle-record", case, "application token secret-bearing note"))
    case = deepcopy(valid["oracle-record"])
    del case["lineage_id"]
    invalid.append(("oracle-record", case, "missing lineage id"))
    case = deepcopy(valid["oracle-record"])
    case["normalization"]["identity_mode"] = "none"
    invalid.append(("oracle-record", case, "none identity mode with identity hash"))

    case = deepcopy(valid["benchmark-plan"])
    case["cases"][0]["benefit_gate"].update(
        {
            "minimum_median_improvement": 0,
            "paired_95_interval_excludes_parity": False,
            "maximum_small_p95_ratio": 999,
            "maximum_server_operating_ratio": 999,
        },
    )
    invalid.append(("benchmark-plan", case, "weak blocking gates"))
    case = deepcopy(valid["benchmark-plan"])
    case["controls"]["drift"] = {
        "status": "preregistered",
        "max_rtt_ratio": "TBD-LIVE",
        "max_operating_ratio": "TBD-LIVE",
    }
    invalid.append(("benchmark-plan", case, "preregistered TBD-LIVE"))
    case = deepcopy(valid["benchmark-plan"])
    case["controls"]["drift"] = {"status": "tbd_live", "max_rtt_ratio": "TBD-LIVE", "max_operating_ratio": "TBD-LIVE"}
    invalid.append(("benchmark-plan", case, "admission with TBD-LIVE"))
    case = deepcopy(valid["benchmark-plan"])
    case["cases"].append(deepcopy(case["cases"][0]))
    invalid.append(("benchmark-plan", case, "duplicate benchmark id"))
    case = deepcopy(valid["benchmark-plan"])
    case["cases"][0]["dataset_plan_content_hash"] = OTHER_SHA256
    invalid.append(("benchmark-plan", case, "cross-lineage dataset ref"))
    case = deepcopy(valid["benchmark-plan"])
    case["cases"][0]["dataset_plan_ref"] = "sha256:" + OTHER_SHA256
    invalid.append(("benchmark-plan", case, "dataset ref/content mismatch"))
    case = deepcopy(valid["benchmark-plan"])
    case["cases"][0]["oracle_ref"] = "sha256:" + OTHER_SHA256
    invalid.append(("benchmark-plan", case, "oracle ref/content mismatch"))
    case = deepcopy(valid["benchmark-plan"])
    case["cases"][0]["benefit_gate"]["blocking"] = False
    invalid.append(("benchmark-plan", case, "admission with advisory case"))
    case = deepcopy(valid["benchmark-plan"])
    case["cases"][0]["compared_plans"] = ["keyset"]
    invalid.append(("benchmark-plan", case, "blocking case with one compared plan"))

    case = deepcopy(valid["evidence-artifact"])
    case["terminal_state"] = "incomplete"
    invalid.append(("evidence-artifact", case, "PASS incomplete"))
    case = deepcopy(valid["evidence-artifact"])
    case["snapshot_state"] = "changed"
    invalid.append(("evidence-artifact", case, "PASS changed snapshot"))
    case = deepcopy(valid["evidence-artifact"])
    case["assurance"] = "caller_asserted"
    invalid.append(("evidence-artifact", case, "benchmark caller asserted"))
    case = deepcopy(valid["evidence-artifact"])
    case["case_id"] = None
    invalid.append(("evidence-artifact", case, "benchmark missing case"))
    case = deepcopy(valid["evidence-artifact"])
    del case["metrics"]["overlap"]
    invalid.append(("evidence-artifact", case, "benchmark missing overlap"))
    case = deepcopy(valid["evidence-artifact"])
    case["finished_at"] = "2026-08-19T19:59:59Z"
    invalid.append(("evidence-artifact", case, "reversed timestamps"))
    case = deepcopy(valid["evidence-artifact"])
    case["evidence_refs"] = [SECRET_EXAMPLE]
    invalid.append(("evidence-artifact", case, "secret-bearing evidence ref"))
    case = deepcopy(valid["evidence-artifact"])
    case["safe_violations"] = [
        {
            "code": "unsafe",
            "message": "unsafe",
            "field": None,
            "url": SECRET_EXAMPLE,
        },
    ]
    invalid.append(("evidence-artifact", case, "untyped secret-bearing violation"))
    case = deepcopy(valid["evidence-artifact"])
    del case["skills_recipe_tree_sha256"]
    invalid.append(("evidence-artifact", case, "missing dirty corpus hash"))
    for metric in ("shortfall", "overfetch", "reference_failures"):
        case = deepcopy(valid["evidence-artifact"])
        case["metrics"][metric] = 10
        invalid.append(("evidence-artifact", case, f"PASS with nonzero {metric}"))
    case = deepcopy(valid["evidence-artifact"])
    case["safe_violations"] = [
        {"severity": "blocking", "code": "oracle_mismatch", "message": "oracle mismatch", "field": None},
    ]
    invalid.append(("evidence-artifact", case, "PASS with blocking violation"))
    case = deepcopy(valid["evidence-artifact"])
    case["controls"].update(
        {
            "rtt_before_seconds": None,
            "rtt_after_seconds": None,
            "operating_before_seconds": None,
            "operating_after_seconds": None,
        },
    )
    invalid.append(("evidence-artifact", case, "PASS without drift measurements"))
    case = deepcopy(valid["evidence-artifact"])
    case["controls"]["drift_quarantined"] = True
    invalid.append(("evidence-artifact", case, "PASS with quarantined drift"))
    case = deepcopy(valid["evidence-artifact"])
    case["metrics"]["wall_seconds"] = float("nan")
    invalid.append(("evidence-artifact", case, "NaN metric"))
    case = deepcopy(valid["evidence-artifact"])
    case["metrics"]["wall_seconds"] = float("inf")
    invalid.append(("evidence-artifact", case, "infinite metric"))

    case = deepcopy(valid["batch-chaining-probe"])
    case["command_keys"] = ["profile", "dependent_user_get"]
    invalid.append(("batch-chaining-probe", case, "mismatched command keys"))
    case = deepcopy(valid["batch-chaining-probe"])
    case["response_summary"]["dependent_row_count"] = 0
    invalid.append(("batch-chaining-probe", case, "PASS with zero dependent rows"))
    case = deepcopy(valid["batch-chaining-probe"])
    case["response_summary"]["dependent_row_count"] = 7
    invalid.append(("batch-chaining-probe", case, "PASS with multiple dependent rows"))
    case = deepcopy(valid["batch-chaining-probe"])
    case["response_summary"]["unexpected_command_error_count"] = 1
    invalid.append(("batch-chaining-probe", case, "PASS with unexpected command error"))
    case = deepcopy(valid["batch-chaining-probe"])
    case["response_summary"]["envelope_shape_valid"] = False
    invalid.append(("batch-chaining-probe", case, "PASS with malformed envelope"))

    for name, document, label in invalid:
        _must_reject(name, document, label)
    positive_count = len(valid) + 5
    sys.stdout.write(f"validated {positive_count} positive and rejected {len(invalid)} negative contract cases\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", choices=sorted(SCHEMAS))
    parser.add_argument("path", nargs="?", type=Path)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test or not args.schema:
        self_test()
        return 0
    if args.path is None:
        parser.error("path is required with --schema")
    validate_document(args.schema, cast("dict[str, Any]", _strict_json_loads(args.path.read_text())))
    sys.stdout.write(f"valid {args.schema}: {args.path}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
