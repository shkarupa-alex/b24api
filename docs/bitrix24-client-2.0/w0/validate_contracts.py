# ruff: noqa: INP001
"""Validate W0 evidence schemas and their cross-field semantic contracts."""

from __future__ import annotations
import argparse
import hashlib
import json
import re
import sys
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator  # type: ignore[import-untyped]

ROOT = Path(__file__).parent
SCHEMAS = {
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
)


class ContractError(ValueError):
    """Raised when structural or semantic evidence validation fails."""


def _load_schema(name: str) -> dict[str, Any]:
    return cast("dict[str, Any]", json.loads((ROOT / SCHEMAS[name]).read_text()))


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


def _manifest_record_hash(document: dict[str, Any]) -> str:
    payload = {key: value for key, value in document.items() if key != "record_hash"}
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _dataset_plan_errors(document: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if document["namespace"] != f"b24api-evidence-{document['run_id']}":
        errors.append("namespace must be derived exactly from run_id")
    cell_ids = [cell["id"] for cell in document["cells"]]
    if len(cell_ids) != len(set(cell_ids)):
        errors.append("dataset cell ids must be unique")
    return errors


def _manifest_errors(document: dict[str, Any]) -> list[str]:
    errors = []
    if document["namespace"] != f"b24api-evidence-{document['run_id']}":
        errors.append("namespace must be derived exactly from run_id")
    if document["record_hash"] != _manifest_record_hash(document):
        errors.append("record_hash does not match canonical record content")
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
    if document["admission_state"] == "admission_ready" and not any(
        case["benefit_gate"]["blocking"] for case in document["cases"]
    ):
        errors.append("admission_ready requires at least one blocking benefit gate")
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
        "schema_version": "1.1",
        "run_id": UUID,
        "lineage_id": LINEAGE_UUID,
        "original_head_sha": SHA,
        "candidate_sha": SHA,
        "generator_sha": SHA,
        "skills_corpus_sha": SHA,
        "skills_recipe_tree_sha256": SHA256,
        "portal": {
            "host": "example.bitrix24.test",
            "fingerprint": SHA256,
            "fingerprint_algorithm": "hmac-sha256-v1",
            "build": None,
            "scope_hash": SHA256,
        },
        "credential_role": "admin_full",
        "namespace": f"b24api-evidence-{UUID}",
        "cells": [
            {
                "id": "T-S",
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
        },
    }


def _manifest_record() -> dict[str, Any]:
    document = {
        "schema_version": "1.1",
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
        "request_fingerprint": SHA256,
        "previous_record_hash": None,
        "record_hash": "",
        "recorded_at": "2026-08-19T20:00:00Z",
        "safe_error": None,
    }
    document["record_hash"] = _manifest_record_hash(document)
    return document


def _oracle_record() -> dict[str, Any]:
    return {
        "schema_version": "1.1",
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
        "schema_version": "1.1",
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
        "schema_version": "1.1",
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
        "schema_version": "1.1",
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
        "portal_fingerprint_algorithm": "hmac-sha256-v1",
        "credential_role": "admin_full",
        "read_only": True,
        "request_count": 1,
        "command_keys": ["who", "dependent"],
        "request_shape": {"halt": 0, "cmd": {"who": "profile", "dependent": "user.get?ID=$result[who][ID]"}},
        "response_summary": {
            "http_status": 200,
            "structured_error": False,
            "command_error_keys": [],
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


def self_test() -> None:  # noqa: PLR0915
    """Exercise positive contracts and every review regression."""
    valid = {
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

    case = deepcopy(valid["dataset-manifest-record"])
    case["entity_id"] = None
    invalid.append(("dataset-manifest-record", case, "created without entity id"))
    case = deepcopy(valid["dataset-manifest-record"])
    case["event"] = "ambiguous"
    case["entity_id"] = None
    case["marker_hash"] = None
    case["request_fingerprint"] = None
    invalid.append(("dataset-manifest-record", case, "ambiguous without request fingerprint"))
    case = deepcopy(valid["dataset-manifest-record"])
    case["safe_error"] = {"url": SECRET_EXAMPLE + "user.get.json"}
    invalid.append(("dataset-manifest-record", case, "untyped secret-bearing error"))

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
    del case["lineage_id"]
    invalid.append(("oracle-record", case, "missing lineage id"))

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

    case = deepcopy(valid["batch-chaining-probe"])
    case["command_keys"] = ["profile", "dependent_user_get"]
    invalid.append(("batch-chaining-probe", case, "mismatched command keys"))

    for name, document, label in invalid:
        _must_reject(name, document, label)
    positive_count = len(valid) + 3
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
    validate_document(args.schema, json.loads(args.path.read_text()))
    sys.stdout.write(f"valid {args.schema}: {args.path}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
