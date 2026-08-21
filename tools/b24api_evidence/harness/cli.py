"""Normative CLI orchestration for offline and explicitly enabled live evidence."""

from __future__ import annotations
import argparse
import contextlib
import hashlib
import math
import os
import platform
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn, cast

import httpx

from b24api.redaction import DEFAULT_REDACTOR

from .contracts import (
    FINGERPRINT_ALGORITHM,
    FINGERPRINT_KEY_FORMAT,
    FIXED_1X_SHA,
    INSTRUMENTATION_REVIEW_SHA,
    MAX_MUTATION_RETRIES,
    NORMATIVE_MAXIMUM_OPERATING_RATIO,
    NORMATIVE_MAXIMUM_SMALL_P95_RATIO,
    NORMATIVE_MINIMUM_MEDIAN_IMPROVEMENT,
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
    SecretLeakError,
    append_manifest_record,
    atomic_write_json,
    build_manifest_record,
    content_sha256,
    derive_drift_controls,
    file_sha256,
    git_sha,
    load_manifest,
    manifest_content_hash,
    marker_sha256,
    marker_value,
    read_json_object,
    require_clean_tracked_tree,
    reviewed_dataset_plan_sha256,
    scan_paths_for_secrets,
    tracked_repository_paths,
    validate_benchmark_plan,
    validate_dataset_plan,
    validate_evidence_artifact,
    validate_manifest_against_plan,
    validate_oracle_record,
    validate_reviewed_profile_set,
    validate_schema,
)
from .live import ADAPTERS, LiveCorrectnessError, LivePortal, LivePreflight, LiveUnavailableError
from .model import MODEL_REQUEST_SECONDS, ModelRun, exact_model_cases, run_exact_matrix_sync

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[3]
PROFILE_SET_PATH = ROOT / "docs/bitrix24-client-2.0/w0/disposable-entity-profiles.json"
DEFAULT_ARTIFACT_DIR = Path(".b24api-evidence")
_ROLE_CHOICES = ("admin_full", "admin_limited", "employee_full", "employee_limited")
_MODEL_WARMUPS = 1
_MODEL_ADVISORY_RUNS = 5
_MODEL_BLOCKING_PAIRS = 9
_MAX_BUNDLE_FILES = 512
_MAX_BUNDLE_BYTES = 128 * 1024 * 1024


def main(argv: Sequence[str] | None = None) -> int:
    """Parse one command, execute it, and return a stable normative exit code."""
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        return int(_dispatch(args))
    except SecretLeakError as error:
        _safe_error(error)
        return int(ExitCode.SECRET_LEAK)
    except ContractError as error:
        _safe_error(error)
        return int(ExitCode.INVALID)
    except LiveUnavailableError as error:
        _safe_error(error)
        return int(ExitCode.UNAVAILABLE)
    except LiveCorrectnessError as error:
        _safe_error(error)
        return int(ExitCode.CORRECTNESS)
    except Exception as error:  # noqa: BLE001 - normative boundary suppresses secret-bearing tracebacks
        sys.stderr.write(f"error: internal {type(error).__name__}\n")
        return int(ExitCode.CORRECTNESS)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="b24api_evidence.py")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("plan", "seed", "verify", "benchmark", "resume", "cleanup", "recover-manifest"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--manifest", type=Path)
        subparser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
        subparser.add_argument("--run-id")
        subparser.add_argument("--lineage-id")
        subparser.add_argument("--plan", type=Path)
        subparser.add_argument("--benchmark-plan", type=Path)
        subparser.add_argument("--live", action="store_true")
        subparser.add_argument("--allow-writes", action="store_true")
        subparser.add_argument("--credential-role", choices=_ROLE_CHOICES, default="admin_full")
        subparser.add_argument("--entity-profile", choices=tuple(ADAPTERS), default="tasks-task-v1")
        subparser.add_argument("--count", type=int, default=5)
        subparser.add_argument("--confirm-recovery", action="store_true")
        subparser.add_argument("--recovery-preview-sha256")
        subparser.add_argument("--confirm-plan-review-sha")
        subparser.add_argument("--confirm-plan-content-sha256")
    return parser


def _dispatch(args: argparse.Namespace) -> ExitCode:
    require_clean_tracked_tree(ROOT)
    if args.live and os.environ.get("PYTEST_CURRENT_TEST"):
        raise ContractError("live evidence commands are forbidden under ordinary pytest")
    handlers: dict[str, Callable[[argparse.Namespace], ExitCode]] = {
        "plan": _plan,
        "seed": _seed,
        "verify": _verify,
        "benchmark": _benchmark,
        "resume": _resume,
        "cleanup": _cleanup,
        "recover-manifest": _recover_manifest,
    }
    return handlers[args.command](args)


def _plan(args: argparse.Namespace) -> ExitCode:
    started = time.monotonic()
    if args.allow_writes:
        raise ContractError("plan is read-only; --allow-writes is invalid")
    artifact_dir = args.artifact_dir.resolve()
    if artifact_dir.exists() and next(artifact_dir.iterdir(), None) is not None:
        raise ContractError("plan requires an empty artifact directory and never overwrites an evidence bundle")
    run_id = _uuid_or_new(args.run_id, "run_id")
    lineage_id = _uuid_or_new(args.lineage_id, "lineage_id")
    if not 0 <= args.count <= REVIEWED_MAX_ENTITIES_PER_CELL:
        raise ContractError(f"count must be between 0 and {REVIEWED_MAX_ENTITIES_PER_CELL}")
    if args.live and args.count == 0:
        raise ContractError("live dataset plans require at least one disposable entity")
    profile_set = validate_reviewed_profile_set(PROFILE_SET_PATH)
    profile = _profile(profile_set, args.entity_profile)
    if args.live:
        with LivePortal(role=args.credential_role) as portal:
            preflight = portal.preflight(required_scopes=set(profile["required_scopes"]))
        portal_data = {
            "host": preflight.identity.host,
            "role": preflight.identity.role,
            "fingerprint": preflight.identity.fingerprint,
            "fingerprint_algorithm": FINGERPRINT_ALGORITHM,
            "fingerprint_key_format": FINGERPRINT_KEY_FORMAT,
            "build": preflight.build,
            "scope_hash": content_sha256(sorted(preflight.scopes)),
        }
    else:
        portal_data = _model_portal()
    candidate_sha = git_sha(ROOT)
    namespace = f"b24api-evidence-{run_id}"
    cell = {
        "id": "LIVE-SMALL",
        "disposable_profile_id": profile["id"],
        "entity_family": profile["entity_family"],
        "target_count": args.count,
        "base_count": 0,
        "relationship_count": 0,
        "distribution": "boundary" if args.count else "empty",
        "marker_field": profile["marker_field"],
        "create_method": profile["create_method"],
        "read_method": profile["read_method"],
        "delete_method": profile["delete_method"],
        "required_scopes": profile["required_scopes"],
    }
    requests = args.count * 5 + 4 if args.count else 0
    dataset_plan = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "lineage_id": lineage_id,
        "original_head_sha": ORIGINAL_HEAD_SHA,
        "candidate_sha": candidate_sha,
        "generator_sha": candidate_sha,
        "skills_corpus_sha": SKILLS_CORPUS_SHA,
        "skills_recipe_tree_sha256": SKILLS_RECIPE_TREE_SHA256,
        "credential_role": portal_data["role"],
        "disposable_profile_set_id": REVIEWED_PROFILE_SET_ID,
        "disposable_profiles_content_hash": REVIEWED_PROFILE_SET_SHA256,
        "portal": portal_data,
        "namespace": namespace,
        "cells": [cell],
        "estimated": {
            "entities": args.count,
            "relationships": 0,
            "create_strategy": "direct",
            "delete_strategy": "direct",
            "requests": requests,
            "batch_commands": 0,
            "duration_seconds": float(requests) * 0.1 if args.count else 0.0,
            "quota_impact": float(requests) if args.count else 0.0,
        },
        "cleanup": {
            "feasible": True,
            "dependency_order": ["LIVE-SMALL"],
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
    validate_dataset_plan(dataset_plan)
    plan_path = artifact_dir / "dataset-plan.json"
    benchmark_plan = _default_benchmark_plan(dataset_plan)
    validate_benchmark_plan(benchmark_plan)
    artifact = _operation_artifact(
        command="plan",
        dataset_plan=dataset_plan,
        manifest_hash=None,
        metrics={
            "kind": "operation",
            "http_attempts": int(args.live) * 2,
            "wall_seconds": max(time.monotonic() - started, 0.000001),
        },
    )
    _persist_plan_bundle(
        artifact_dir=artifact_dir,
        dataset_plan=dataset_plan,
        benchmark_plan=benchmark_plan,
        artifact=artifact,
        candidate_sha=candidate_sha,
    )
    _safe_message(f"plan completed: {plan_path}")
    return ExitCode.COMPLETED


def _seed(args: argparse.Namespace) -> ExitCode:  # noqa: C901, PLR0912, PLR0915
    _require_live_write_flags(args, "seed")
    _required_path(args.plan, "--plan")
    plan = _read_plan(args)
    _require_approved_plan(plan, args=args)
    _bind_plan_to_bundle(args, plan)
    validate_reviewed_profile_set(PROFILE_SET_PATH)
    manifest_path = _manifest_path(args)
    artifact_dir = args.artifact_dir.resolve()
    previous: dict[str, Any] | None = None
    existing: list[dict[str, Any]] = []
    if manifest_path.exists():
        existing = load_manifest(manifest_path, expected=_lineage(plan))
        validate_manifest_against_plan(existing, plan)
        previous = existing[-1]
    terminal_by_key = {record["correlation_key"]: record for record in existing}
    http_attempts = 0
    started = time.monotonic()
    with LivePortal(role=args.credential_role) as portal:
        _require_portal_match(plan, portal)
        required_scopes = {scope for cell in plan["cells"] for scope in cell.get("required_scopes", [])}
        preflight = portal.preflight(required_scopes=required_scopes)
        _require_preflight_match(plan, preflight)
        http_attempts += 2
        for cell in plan["cells"]:
            adapter = ADAPTERS[str(cell["disposable_profile_id"])]
            for index in range(int(cell["target_count"])):
                correlation = content_sha256([plan["run_id"], cell["id"], index])
                current = terminal_by_key.get(correlation)
                if current is not None and current["event"] == "verified":
                    continue
                if current is not None and current["event"] in {
                    "delete_dispatched",
                    "delete_cancelled",
                    "deleted",
                    "absence_verified",
                    "orphan",
                }:
                    raise ContractError("seed cannot resume a correlation after cleanup has started")
                marker = marker_value(str(plan["namespace"]), correlation)
                base = _manifest_base(plan, cell=cell, correlation=correlation, marker=marker)
                recovered_id: str | None = None
                write_reconciled_event = False
                if current is not None and current["event"] in {"created", "reconciled"}:
                    recovered_id = str(current["entity_id"])
                    recovered = adapter.read(portal, recovered_id)
                    http_attempts += 1
                    if recovered is None or _entity_marker(recovered, str(cell["marker_field"])) != marker:
                        matches = adapter.find_exact_marker(portal, marker)
                        http_attempts += 1
                        recovered_id = matches[0] if len(matches) == 1 else None
                        if len(matches) > 1:
                            raise LiveCorrectnessError("resume found multiple exact-marker entities")
                        if recovered_id is None:
                            return _seed_inconclusive(
                                artifact_dir=artifact_dir,
                                plan=plan,
                                manifest_path=manifest_path,
                                http_attempts=portal.attempts,
                                started=started,
                            )
                        write_reconciled_event = current["event"] == "created"
                elif current is not None and current["event"] in {"planned", "create_dispatched", "ambiguous"}:
                    matches = adapter.find_exact_marker(portal, marker)
                    http_attempts += 1
                    if len(matches) > 1:
                        raise LiveCorrectnessError("resume found multiple exact-marker entities")
                    recovered_id = matches[0] if matches else None
                    write_reconciled_event = recovered_id is not None
                    if recovered_id is None and current["event"] in {"create_dispatched", "ambiguous"}:
                        return _seed_inconclusive(
                            artifact_dir=artifact_dir,
                            plan=plan,
                            manifest_path=manifest_path,
                            http_attempts=portal.attempts,
                            started=started,
                        )
                elif current is not None and current["event"] == "create_cancelled":
                    # The post-journal guard refused before adapter.create(), so
                    # no marker reconciliation is necessary on retry.
                    pass
                if recovered_id is not None:
                    request_fingerprint = content_sha256(["resume-exact-marker", correlation])
                    if write_reconciled_event:
                        reconciled = build_manifest_record(
                            {
                                **base,
                                "event": "reconciled",
                                "entity_id": recovered_id,
                                "request_fingerprint": request_fingerprint,
                            },
                            previous=previous,
                        )
                        append_manifest_record(manifest_path, reconciled)
                        previous = reconciled
                    recovered = adapter.read(portal, recovered_id)
                    http_attempts += 1
                    if recovered is None or _entity_marker(recovered, str(cell["marker_field"])) != marker:
                        raise LiveCorrectnessError("reconciled entity failed exact marker read-back")
                    verified = build_manifest_record(
                        {
                            **base,
                            "event": "verified",
                            "entity_id": recovered_id,
                            "request_fingerprint": request_fingerprint,
                        },
                        previous=previous,
                    )
                    append_manifest_record(manifest_path, verified)
                    previous = verified
                    terminal_by_key[correlation] = verified
                    continue
                if current is None:
                    record = build_manifest_record(
                        {**base, "event": "planned", "entity_id": None, "request_fingerprint": None},
                        previous=previous,
                    )
                    append_manifest_record(manifest_path, record)
                    previous = record
                request_fingerprint = content_sha256([adapter.create_method, ["fields"], correlation])
                _require_exact_candidate(plan)
                record = build_manifest_record(
                    {
                        **base,
                        "event": "create_dispatched",
                        "entity_id": None,
                        "request_fingerprint": request_fingerprint,
                    },
                    previous=previous,
                )
                append_manifest_record(manifest_path, record)
                previous = record
                try:
                    _require_exact_candidate(plan)
                except ContractError:
                    cancelled = build_manifest_record(
                        {
                            **base,
                            "event": "create_cancelled",
                            "entity_id": None,
                            "request_fingerprint": request_fingerprint,
                        },
                        previous=previous,
                    )
                    append_manifest_record(manifest_path, cancelled)
                    previous = cancelled
                    raise
                try:
                    entity_id = adapter.create(portal, marker)
                    http_attempts += 1
                    event = "created"
                except (LiveUnavailableError, LiveCorrectnessError):
                    ambiguous = build_manifest_record(
                        {**base, "event": "ambiguous", "entity_id": None, "request_fingerprint": request_fingerprint},
                        previous=previous,
                    )
                    append_manifest_record(manifest_path, ambiguous)
                    previous = ambiguous
                    matches = adapter.find_exact_marker(portal, marker)
                    http_attempts += 1
                    if len(matches) != 1:
                        return _seed_inconclusive(
                            artifact_dir=artifact_dir,
                            plan=plan,
                            manifest_path=manifest_path,
                            http_attempts=portal.attempts,
                            started=started,
                        )
                    entity_id = matches[0]
                    event = "reconciled"
                created = build_manifest_record(
                    {**base, "event": event, "entity_id": entity_id, "request_fingerprint": request_fingerprint},
                    previous=previous,
                )
                append_manifest_record(manifest_path, created)
                previous = created
                entity = adapter.read(portal, entity_id)
                http_attempts += 1
                if entity is None or _entity_marker(entity, str(cell["marker_field"])) != marker:
                    raise LiveCorrectnessError("created entity failed exact marker read-back")
                verified = build_manifest_record(
                    {**base, "event": "verified", "entity_id": entity_id, "request_fingerprint": request_fingerprint},
                    previous=previous,
                )
                append_manifest_record(manifest_path, verified)
                previous = verified
                terminal_by_key[correlation] = verified
        http_attempts = portal.attempts
    artifact = _operation_artifact(
        command="seed",
        dataset_plan=plan,
        manifest_hash=manifest_content_hash(manifest_path),
        metrics={"kind": "operation", "http_attempts": http_attempts, "wall_seconds": time.monotonic() - started},
    )
    _write_validated_artifact(
        artifact_dir / "seed-evidence.json",
        artifact,
        candidate_sha=str(plan["candidate_sha"]),
        scan_bundle=True,
    )
    _safe_message(f"seed completed: {manifest_path}")
    return ExitCode.COMPLETED


def _seed_inconclusive(
    *,
    artifact_dir: Path,
    plan: Mapping[str, Any],
    manifest_path: Path,
    http_attempts: int,
    started: float,
) -> ExitCode:
    artifact = _operation_artifact(
        command="seed",
        dataset_plan=plan,
        manifest_hash=manifest_content_hash(manifest_path),
        metrics={
            "kind": "operation",
            "http_attempts": http_attempts,
            "wall_seconds": time.monotonic() - started,
        },
        outcome="INCONCLUSIVE",
        terminal_state="incomplete",
    )
    _write_validated_artifact(
        artifact_dir / "seed-evidence.json",
        artifact,
        candidate_sha=str(plan["candidate_sha"]),
        scan_bundle=True,
    )
    return ExitCode.INCOMPLETE


def _verify(args: argparse.Namespace) -> ExitCode:
    if args.allow_writes:
        raise ContractError("verify is read-only; --allow-writes is invalid")
    plan = _load_plan(args)
    manifest_path = _manifest_path(args)
    records = load_manifest(manifest_path, expected=_lineage(plan))
    validate_manifest_against_plan(records, plan)
    manifest_hash = manifest_content_hash(manifest_path)
    latest = _latest_records(records)
    expected_count = sum(int(cell["target_count"]) for cell in plan["cells"])
    if len(latest) != expected_count or any(record["event"] != "verified" for record in latest.values()):
        raise ContractError("verify requires exactly one verified terminal entity for every planned correlation")
    started = time.monotonic()
    retries = 0
    attempts = 0
    snapshot_state = "verified"
    if args.live:
        if plan["portal"]["role"] == "model":
            raise ContractError("model dataset plans cannot be verified with --live")
        with LivePortal(role=args.credential_role) as portal:
            _require_portal_match(plan, portal)
            required_scopes = {scope for cell in plan["cells"] for scope in cell.get("required_scopes", [])}
            preflight = portal.preflight(required_scopes=required_scopes)
            _require_preflight_match(plan, preflight)
            pre_hash = _live_manifest_snapshot(portal, plan=plan, latest=latest)
            post_hash = _live_manifest_snapshot(portal, plan=plan, latest=latest)
            while pre_hash != post_hash and retries < MAX_MUTATION_RETRIES:
                retries += 1
                pre_hash = post_hash
                post_hash = _live_manifest_snapshot(portal, plan=plan, latest=latest)
            attempts = portal.attempts
        if pre_hash != post_hash:
            snapshot_state = "changed"
    elif plan["portal"]["role"] != "model":
        raise ContractError("a live dataset plan requires --live independent verification")
    else:
        identities = sorted(
            [str(record["correlation_key"]), str(record["entity_id"]), str(record["marker_hash"])]
            for record in latest.values()
        )
        pre_hash = post_hash = content_sha256(identities)
    outcome = "PASS" if snapshot_state == "verified" else "INCONCLUSIVE"
    oracle = {
        "schema_version": SCHEMA_VERSION,
        "run_id": plan["run_id"],
        "lineage_id": plan["lineage_id"],
        "case_id": "MANIFEST",
        "original_head_sha": ORIGINAL_HEAD_SHA,
        "fixed_1x_sha": FIXED_1X_SHA,
        "candidate_sha": plan["candidate_sha"],
        "skills_corpus_sha": SKILLS_CORPUS_SHA,
        "skills_recipe_tree_sha256": SKILLS_RECIPE_TREE_SHA256,
        "dataset_plan_content_hash": content_sha256(plan),
        "manifest_content_hash": manifest_hash,
        "portal_fingerprint": plan["portal"]["fingerprint"],
        "expected_result_hash": pre_hash,
        "actual_result_hash": post_hash,
        "qualification": "bounded_point_read" if args.live else "immutable_manifest",
        "snapshot_requirement": "frozen_manifest",
        "snapshot_state": snapshot_state,
        "pre_hash": pre_hash,
        "post_hash": post_hash,
        "mutation_retries": retries,
        "raw_count": len(latest),
        "unique_count": len({str(record["entity_id"]) for record in latest.values()}),
        "outcome": outcome,
    }
    validate_oracle_record(oracle)
    artifact_dir = args.artifact_dir.resolve()
    artifact = _operation_artifact(
        command="verify",
        dataset_plan=plan,
        manifest_hash=manifest_hash,
        metrics={"kind": "operation", "http_attempts": attempts, "wall_seconds": time.monotonic() - started},
        snapshot_requirement="frozen_manifest",
        snapshot_state=snapshot_state,
        assurance="oracle_verified",
        outcome=outcome,
        terminal_state="completed" if outcome == "PASS" else "incomplete",
        extra={"evidence_refs": [f"sha256:{content_sha256(oracle)}"]},
    )
    _persist_verify_bundle(
        artifact_dir=artifact_dir,
        oracle=oracle,
        artifact=artifact,
        candidate_sha=str(plan["candidate_sha"]),
    )
    _safe_message(f"verify completed: {artifact_dir / 'oracle.json'}")
    return ExitCode.COMPLETED if outcome == "PASS" else ExitCode.INCOMPLETE


def _benchmark(args: argparse.Namespace) -> ExitCode:
    if args.allow_writes:
        raise ContractError("benchmark is read-only; --allow-writes is invalid")
    if args.live:
        raise LiveUnavailableError(
            "live benchmark execution is not admitted until a reviewed live benchmark cell exists",
        )
    plan = _load_plan(args, allow_generated=True)
    if plan["portal"]["role"] != "model":
        raise LiveUnavailableError("non-model dataset plans require the unavailable reviewed live benchmark runner")
    benchmark_plan = (
        read_json_object(args.benchmark_plan) if args.benchmark_plan is not None else _default_benchmark_plan(plan)
    )
    validate_benchmark_plan(benchmark_plan)
    if (
        benchmark_plan["candidate_sha"] != plan["candidate_sha"]
        or benchmark_plan["lineage_id"] != plan["lineage_id"]
        or benchmark_plan["dataset_plan_content_hash"] != content_sha256(plan)
    ):
        raise ContractError("benchmark plan lineage does not match the dataset plan and candidate")
    if benchmark_plan["admission_state"] != "draft":
        raise LiveUnavailableError("admission-ready benchmark plans require the unavailable reviewed live runner")
    _require_exact_model_benchmark_plan(benchmark_plan)
    controls_config = benchmark_plan["controls"]

    run_exact_matrix_sync()  # one declared warmup; its observations are intentionally discarded
    runs = tuple(run for _ in range(int(controls_config["advisory_runs"])) for run in run_exact_matrix_sync())
    return _benchmark_runs_and_artifact(args, plan=plan, benchmark_plan=benchmark_plan, runs=runs)


def _require_exact_model_benchmark_plan(benchmark_plan: Mapping[str, Any]) -> None:
    """Refuse draft controls the deterministic runner cannot execute exactly."""
    cases = benchmark_plan["cases"]
    if (
        not isinstance(cases, list)
        or len(cases) != 1
        or cases[0].get("id") != "MODEL-MATRIX"
        or cases[0].get("compared_plans") != ["offset", "keyset"]
    ):
        raise ContractError("the deterministic runner accepts only its exact MODEL-MATRIX offset/keyset draft")
    controls_config = benchmark_plan["controls"]
    if (
        controls_config["warmups"] != _MODEL_WARMUPS
        or controls_config["advisory_runs"] != _MODEL_ADVISORY_RUNS
        or controls_config["blocking_pairs"] != _MODEL_BLOCKING_PAIRS
        or controls_config["interleaving"] is not True
        or controls_config["timing_model"]
        != {"kind": "deterministic_request_cost", "seconds_per_request": MODEL_REQUEST_SECONDS}
    ):
        raise ContractError("the deterministic runner accepts only its exact bounded draft run controls")
    if benchmark_plan["manifest_content_hash"] != content_sha256(_model_fixture_manifest()):
        raise ContractError("the deterministic runner requires its exact immutable model fixture manifest")


def _benchmark_runs_and_artifact(
    args: argparse.Namespace,
    *,
    plan: Mapping[str, Any],
    benchmark_plan: Mapping[str, Any],
    runs: tuple[ModelRun, ...],
) -> ExitCode:
    rollback_log: list[tuple[Path, Mapping[str, Any] | None]] = []
    try:
        result = _benchmark_runs_and_artifact_inner(
            args,
            plan=plan,
            benchmark_plan=benchmark_plan,
            runs=runs,
            rollback_log=rollback_log,
        )
    except BaseException:
        _rollback_candidate_json_log(rollback_log)
        raise
    artifact_path = args.artifact_dir.resolve() / "benchmark-evidence.json"
    _safe_message(f"benchmark completed: {artifact_path}")
    return result


def _benchmark_runs_and_artifact_inner(
    args: argparse.Namespace,
    *,
    plan: Mapping[str, Any],
    benchmark_plan: Mapping[str, Any],
    runs: tuple[ModelRun, ...],
    rollback_log: list[tuple[Path, Mapping[str, Any] | None]],
) -> ExitCode:
    """Persist the already executed exact model matrix and its qualified evidence."""
    controls_config = benchmark_plan["controls"]
    stable_runs = [run for run in runs if run.outcome == "PASS"]
    rows = sum(len(run.identities) for run in stable_runs)
    requests = sum(run.requests for run in stable_runs)
    logical_pages = sum(run.logical_pages for run in stable_runs)
    stable_offset = [run for run in stable_runs if run.plan == "offset"]
    stable_keyset = [run for run in stable_runs if run.plan == "keyset"]
    matrix_width = len(runs) // int(controls_config["advisory_runs"])
    offset_control = sum(run.operating_seconds for run in stable_offset) / sum(run.requests for run in stable_offset)
    keyset_control = sum(run.operating_seconds for run in stable_keyset) / sum(run.requests for run in stable_keyset)
    controls = derive_drift_controls(
        rtt_before=offset_control,
        rtt_after=keyset_control,
        operating_before=offset_control,
        operating_after=keyset_control,
        max_rtt_ratio=1.20,
        max_operating_ratio=1.20,
    )
    metrics = {
        "kind": "benchmark",
        "http_attempts": requests,
        "logical_pages": logical_pages,
        "batch_requests": 0,
        "batch_commands": 0,
        "time_to_first_row_seconds": min(
            run.time_to_first_row_seconds for run in stable_runs if run.time_to_first_row_seconds is not None
        ),
        "wall_seconds": max(sum(run.wall_seconds for run in stable_runs), 0.000001),
        "server_operating_seconds": sum(run.operating_seconds for run in stable_runs),
        "retries": 0,
        "cooldown_seconds": 0.0,
        "buffered_rows_high_water": max(run.buffered_rows_high_water for run in stable_runs),
        "rss_delta_bytes": None,
        "raw_rows": rows,
        "unique_rows": rows,
        "overlap": rows,
        "duplicates": 0,
        "shortfall": 0,
        "overfetch": 0,
        "reference_failures": 0,
    }
    artifact_dir = args.artifact_dir.resolve()
    oracle_dir = artifact_dir / "model-oracles"
    oracle_refs: list[str] = []
    for run_index, run in enumerate(runs):
        oracle_case_id = f"{run.case_id}-{run.plan}-run-{run_index // matrix_width + 1}"
        oracle = {
            "schema_version": SCHEMA_VERSION,
            "run_id": plan["run_id"],
            "lineage_id": plan["lineage_id"],
            "case_id": oracle_case_id,
            "original_head_sha": ORIGINAL_HEAD_SHA,
            "fixed_1x_sha": FIXED_1X_SHA,
            "candidate_sha": plan["candidate_sha"],
            "skills_corpus_sha": SKILLS_CORPUS_SHA,
            "skills_recipe_tree_sha256": SKILLS_RECIPE_TREE_SHA256,
            "dataset_plan_content_hash": content_sha256(plan),
            "manifest_content_hash": run.expected_hash,
            "portal_fingerprint": plan["portal"]["fingerprint"],
            "expected_result_hash": run.expected_hash,
            "actual_result_hash": run.actual_hash,
            "qualification": "independent_cross_method",
            "snapshot_requirement": "independent_pre_post_oracle",
            "snapshot_state": run.snapshot_state,
            "pre_hash": run.pre_hash,
            "post_hash": run.post_hash,
            "mutation_retries": run.mutation_retries,
            "raw_count": len(run.identities),
            "unique_count": len(set(run.identities)),
            "outcome": run.outcome,
        }
        validate_oracle_record(oracle)
        _write_candidate_json(
            oracle_dir / f"{oracle_case_id}.json",
            oracle,
            candidate_sha=str(plan["candidate_sha"]),
            rollback_log=rollback_log,
        )
        if run.outcome == "PASS":
            oracle_refs.append(f"sha256:{content_sha256(oracle)}")
    stable_observations = [
        {"iteration": index // matrix_width + 1, **asdict(run)}
        for index, run in enumerate(runs)
        if run.outcome == "PASS"
    ]
    model_matrix = {
        "schema_version": SCHEMA_VERSION,
        "run_id": plan["run_id"],
        "lineage_id": plan["lineage_id"],
        "original_head_sha": ORIGINAL_HEAD_SHA,
        "fixed_1x_sha": FIXED_1X_SHA,
        "candidate_sha": plan["candidate_sha"],
        "skills_corpus_sha": SKILLS_CORPUS_SHA,
        "skills_recipe_tree_sha256": SKILLS_RECIPE_TREE_SHA256,
        "dataset_plan_content_hash": content_sha256(plan),
        "benchmark_plan_content_hash": content_sha256(benchmark_plan),
        "runs": stable_observations,
    }
    _write_candidate_json(
        artifact_dir / "model-matrix.json",
        model_matrix,
        candidate_sha=str(plan["candidate_sha"]),
        rollback_log=rollback_log,
    )
    model_diagnostics = {
        "schema_version": SCHEMA_VERSION,
        "runs": [asdict(run) for run in runs if run.outcome != "PASS"],
    }
    _write_candidate_json(
        artifact_dir / "model-diagnostics.json",
        model_diagnostics,
        candidate_sha=str(plan["candidate_sha"]),
        rollback_log=rollback_log,
    )
    matrix_ref = f"sha256:{content_sha256(model_matrix)}"
    artifact = _operation_artifact(
        command="benchmark",
        dataset_plan=plan,
        manifest_hash=str(benchmark_plan["manifest_content_hash"]),
        metrics=metrics,
        assurance="oracle_verified",
        extra={
            "benchmark_plan_content_hash": content_sha256(benchmark_plan),
            "controls": controls,
            "case_id": "MODEL-MATRIX",
            "evidence_refs": [*oracle_refs, matrix_ref],
            "safe_violations": [
                {
                    "severity": "warning",
                    "code": "mutation_diagnostic_inconclusive",
                    "message": "persistent-mutation runs are diagnostic and are not dependencies of stable-case PASS",
                    "field": "model-diagnostics.json",
                },
                {
                    "severity": "warning",
                    "code": "deterministic_latency_model",
                    "message": "offline timings use the preregistered 1 ms/request model, not host wall clock",
                    "field": "benchmark-plan.json.controls.timing_model",
                },
            ],
        },
    )
    candidate_sha = str(plan["candidate_sha"])
    _write_candidate_json(
        artifact_dir / "benchmark-plan.json",
        benchmark_plan,
        candidate_sha=candidate_sha,
        rollback_log=rollback_log,
    )
    _write_candidate_json(
        artifact_dir / "dataset-plan.json",
        plan,
        candidate_sha=candidate_sha,
        rollback_log=rollback_log,
    )
    _write_candidate_json(
        artifact_dir / "model-fixture-manifest.json",
        _model_fixture_manifest(),
        candidate_sha=candidate_sha,
        rollback_log=rollback_log,
    )
    _write_validated_artifact(
        artifact_dir / "benchmark-evidence.json",
        artifact,
        candidate_sha=candidate_sha,
        scan_bundle=True,
    )
    return ExitCode.COMPLETED


def _resume(args: argparse.Namespace) -> ExitCode:
    started = time.monotonic()
    if args.live or args.allow_writes:
        raise ContractError(
            "resume is read-only validation; continue writes with the idempotent seed or cleanup command",
        )
    plan = _load_plan(args)
    records = load_manifest(_manifest_path(args), expected=_lineage(plan))
    validate_manifest_against_plan(records, plan)
    artifact = _operation_artifact(
        command="resume",
        dataset_plan=plan,
        manifest_hash=manifest_content_hash(_manifest_path(args)),
        metrics={
            "kind": "operation",
            "http_attempts": 0,
            "wall_seconds": max(time.monotonic() - started, 0.000001),
            "records": len(records),
        },
    )
    artifact_path = args.artifact_dir.resolve() / "resume-evidence.json"
    candidate_sha = str(plan["candidate_sha"])
    _write_validated_artifact(
        artifact_path,
        artifact,
        candidate_sha=candidate_sha,
        scan_bundle=True,
    )
    _safe_message(f"resume validation completed: {artifact_path}")
    return ExitCode.COMPLETED


def _cleanup(args: argparse.Namespace) -> ExitCode:  # noqa: C901, PLR0912, PLR0915
    _require_live_write_flags(args, "cleanup")
    plan = _read_plan(args)
    _require_approved_plan(plan, args=args)
    _bind_plan_to_bundle(args, plan)
    manifest_path = _manifest_path(args)
    records = load_manifest(manifest_path, expected=_lineage(plan))
    validate_manifest_against_plan(records, plan)
    previous = records[-1]
    latest = _latest_records(records)
    cell_by_id = {str(cell["id"]): cell for cell in plan["cells"]}
    dependency_order = [str(cell_id) for cell_id in plan["cleanup"]["dependency_order"]]
    ordered = sorted(
        latest.values(),
        key=lambda record: dependency_order.index(str(record["cell_id"])),
        reverse=True,
    )
    orphans: list[str] = []
    portal_attempts = 0
    started = time.monotonic()
    with LivePortal(role=args.credential_role) as portal:
        _require_portal_match(plan, portal)
        required_scopes = {scope for cell in plan["cells"] for scope in cell.get("required_scopes", [])}
        preflight = portal.preflight(required_scopes=required_scopes)
        _require_preflight_match(plan, preflight)
        for current in ordered:
            active_record = current
            if current["event"] == "absence_verified":
                continue
            cell = cell_by_id[str(current["cell_id"])]
            adapter = ADAPTERS[str(cell["disposable_profile_id"])]
            base = _manifest_base(
                plan,
                cell=cell,
                correlation=str(current["correlation_key"]),
                marker=str(current["marker_value"]),
            )
            if current["entity_id"] is None:
                if current["event"] in {"planned", "create_cancelled"}:
                    continue
                if current["event"] not in {"create_dispatched", "ambiguous"}:
                    raise ContractError("cleanup found an entity-less manifest state that cannot be reconciled")
                matches = adapter.find_exact_marker(portal, str(current["marker_value"]))
                if len(matches) > 1:
                    raise LiveCorrectnessError("cleanup found multiple exact-marker entities")
                recovery_fingerprint = content_sha256(
                    ["cleanup-exact-marker", current["correlation_key"]],
                )
                recovered = build_manifest_record(
                    {
                        **base,
                        "event": "reconciled" if matches else "absence_verified",
                        "entity_id": matches[0] if matches else None,
                        "request_fingerprint": recovery_fingerprint,
                    },
                    previous=previous,
                )
                append_manifest_record(manifest_path, recovered)
                previous = recovered
                active_record = recovered
                if not matches:
                    continue
            entity_id = str(active_record["entity_id"])
            request_fingerprint = content_sha256(
                [adapter.delete_method, [adapter.id_parameter], active_record["correlation_key"]],
            )
            owned = adapter.read(portal, entity_id)
            if owned is None:
                checked = build_manifest_record(
                    {
                        **base,
                        "event": "absence_verified",
                        "entity_id": entity_id,
                        "request_fingerprint": request_fingerprint,
                    },
                    previous=previous,
                )
                append_manifest_record(manifest_path, checked)
                previous = checked
                continue
            if _entity_marker(owned, str(cell["marker_field"])) != active_record["marker_value"]:
                if active_record["event"] == "orphan":
                    orphans.append(str(active_record["correlation_key"]))
                    continue
                checked = build_manifest_record(
                    {
                        **base,
                        "event": "orphan",
                        "entity_id": entity_id,
                        "request_fingerprint": request_fingerprint,
                    },
                    previous=previous,
                )
                append_manifest_record(manifest_path, checked)
                previous = checked
                orphans.append(str(active_record["correlation_key"]))
                continue
            _require_exact_candidate(plan)
            dispatched = build_manifest_record(
                {
                    **base,
                    "event": "delete_dispatched",
                    "entity_id": entity_id,
                    "request_fingerprint": request_fingerprint,
                },
                previous=previous,
            )
            append_manifest_record(manifest_path, dispatched)
            previous = dispatched
            try:
                _require_exact_candidate(plan)
            except ContractError:
                cancelled = build_manifest_record(
                    {
                        **base,
                        "event": "delete_cancelled",
                        "entity_id": entity_id,
                        "request_fingerprint": request_fingerprint,
                    },
                    previous=previous,
                )
                append_manifest_record(manifest_path, cancelled)
                previous = cancelled
                raise
            adapter.delete(portal, entity_id)
            deleted = build_manifest_record(
                {**base, "event": "deleted", "entity_id": entity_id, "request_fingerprint": request_fingerprint},
                previous=previous,
            )
            append_manifest_record(manifest_path, deleted)
            previous = deleted
            absent = adapter.read(portal, entity_id) is None
            event = "absence_verified" if absent else "orphan"
            checked = build_manifest_record(
                {**base, "event": event, "entity_id": entity_id, "request_fingerprint": request_fingerprint},
                previous=previous,
            )
            append_manifest_record(manifest_path, checked)
            previous = checked
            if not absent:
                orphans.append(str(active_record["correlation_key"]))
        portal_attempts = portal.attempts
    outcome = "PASS" if not orphans else "FAIL"
    artifact = _operation_artifact(
        command="cleanup",
        dataset_plan=plan,
        manifest_hash=manifest_content_hash(manifest_path),
        metrics={
            "kind": "operation",
            "http_attempts": portal_attempts,
            "wall_seconds": time.monotonic() - started,
            "orphan_count": len(orphans),
            "absence_verified": not orphans,
            "orphan_refs": [f"sha256:{value}" for value in orphans],
        },
        outcome=outcome,
        terminal_state="completed" if not orphans else "failed",
    )
    artifact_path = args.artifact_dir.resolve() / "cleanup-evidence.json"
    candidate_sha = str(plan["candidate_sha"])
    _write_validated_artifact(
        artifact_path,
        artifact,
        candidate_sha=candidate_sha,
        scan_bundle=True,
    )
    if orphans:
        _safe_error(RuntimeError(f"cleanup left {len(orphans)} verified orphan(s)"))
        return ExitCode.ORPHANS
    _safe_message(f"cleanup completed: {artifact_path}")
    return ExitCode.COMPLETED


def _recover_manifest(args: argparse.Namespace) -> ExitCode:  # noqa: C901, PLR0912, PLR0915
    started = time.monotonic()
    if not args.live:
        raise ContractError("recover-manifest requires --live")
    if args.allow_writes:
        raise ContractError("recover-manifest is read-only and rejects --allow-writes")
    plan = _load_plan(args)
    if args.run_id is not None and args.run_id != plan["run_id"]:
        raise ContractError("recovery run_id does not match the reviewed plan")
    artifact_dir = args.artifact_dir.resolve()
    preview_path = artifact_dir / "recovery-preview.json"
    prior_preview: dict[str, Any] | None = None
    if args.confirm_recovery:
        if args.recovery_preview_sha256 is None or not preview_path.exists():
            raise ContractError("confirmed recovery requires the prior preview and --recovery-preview-sha256")
        if file_sha256(preview_path) != args.recovery_preview_sha256:
            raise ContractError("recovery preview content hash does not match the explicit confirmation")
        prior_preview = read_json_object(preview_path)
        validate_schema(prior_preview, "recovery-preview")
        for field in (
            "run_id",
            "lineage_id",
            "original_head_sha",
            "fixed_1x_sha",
            "skills_corpus_sha",
            "skills_recipe_tree_sha256",
            "dataset_plan_content_hash",
            "candidate_sha",
            "portal_fingerprint",
            "namespace",
        ):
            expected = content_sha256(plan) if field == "dataset_plan_content_hash" else plan.get(field)
            if field == "original_head_sha":
                expected = ORIGINAL_HEAD_SHA
            if field == "fixed_1x_sha":
                expected = FIXED_1X_SHA
            if field == "skills_corpus_sha":
                expected = SKILLS_CORPUS_SHA
            if field == "skills_recipe_tree_sha256":
                expected = SKILLS_RECIPE_TREE_SHA256
            if field == "portal_fingerprint":
                expected = plan["portal"]["fingerprint"]
            if prior_preview.get(field) != expected:
                raise ContractError(f"recovery preview {field} does not match the reviewed plan")
    candidates: list[dict[str, Any]] = []
    with LivePortal(role=args.credential_role) as portal:
        _require_portal_match(plan, portal)
        required_scopes = {scope for cell in plan["cells"] for scope in cell.get("required_scopes", [])}
        preflight = portal.preflight(required_scopes=required_scopes)
        _require_preflight_match(plan, preflight)
        for cell in plan["cells"]:
            adapter = ADAPTERS[str(cell["disposable_profile_id"])]
            for index in range(int(cell["target_count"])):
                correlation = content_sha256([plan["run_id"], cell["id"], index])
                marker = marker_value(str(plan["namespace"]), correlation)
                matches = adapter.find_exact_marker(portal, marker)
                if len(matches) > 1:
                    raise LiveCorrectnessError("exact-marker recovery found multiple owned entities")
                if matches:
                    candidates.append({"cell_id": cell["id"], "correlation_key": correlation, "entity_id": matches[0]})
        portal_attempts = portal.attempts
    preview = {
        "schema_version": SCHEMA_VERSION,
        "run_id": plan["run_id"],
        "lineage_id": plan["lineage_id"],
        "original_head_sha": ORIGINAL_HEAD_SHA,
        "fixed_1x_sha": FIXED_1X_SHA,
        "skills_corpus_sha": SKILLS_CORPUS_SHA,
        "skills_recipe_tree_sha256": SKILLS_RECIPE_TREE_SHA256,
        "dataset_plan_content_hash": content_sha256(plan),
        "candidate_sha": plan["candidate_sha"],
        "portal_fingerprint": plan["portal"]["fingerprint"],
        "namespace": plan["namespace"],
        "exact_marker_candidates": candidates,
        "confirmation_required": True,
    }
    validate_schema(preview, "recovery-preview")
    if not args.confirm_recovery:
        _write_candidate_json(preview_path, preview, candidate_sha=str(plan["candidate_sha"]))
        preview_hash = file_sha256(preview_path)
        _safe_message(
            f"recovery preview completed: {preview_path}; explicit confirmation digest: {preview_hash}",
        )
        return ExitCode.INCOMPLETE
    if prior_preview is None or prior_preview["exact_marker_candidates"] != candidates:
        _write_candidate_json(preview_path, preview, candidate_sha=str(plan["candidate_sha"]))
        _safe_message(
            f"recovery candidates changed; review the new preview SHA-256 {file_sha256(preview_path)}",
        )
        return ExitCode.INCOMPLETE
    if not candidates:
        _safe_message("recovery found no exact-marker entities; no candidate manifest was written")
        return ExitCode.INCOMPLETE
    manifest_path = _manifest_path(args)
    if manifest_path.exists():
        raise ContractError("confirmed recovery refuses to overwrite an existing manifest")
    previous: dict[str, Any] | None = None
    cell_by_id = {str(cell["id"]): cell for cell in plan["cells"]}
    for candidate in candidates:
        cell = cell_by_id[str(candidate["cell_id"])]
        marker = marker_value(str(plan["namespace"]), str(candidate["correlation_key"]))
        base = _manifest_base(plan, cell=cell, correlation=str(candidate["correlation_key"]), marker=marker)
        record = build_manifest_record(
            {
                **base,
                "event": "reconciled",
                "entity_id": candidate["entity_id"],
                "request_fingerprint": content_sha256(["recover-manifest", candidate["correlation_key"]]),
            },
            previous=previous,
        )
        append_manifest_record(manifest_path, record)
        previous = record
    artifact = _operation_artifact(
        command="recover-manifest",
        dataset_plan=plan,
        manifest_hash=manifest_content_hash(manifest_path),
        metrics={
            "kind": "operation",
            "http_attempts": portal_attempts,
            "wall_seconds": max(time.monotonic() - started, 0.000001),
            "records": len(candidates),
        },
        outcome="INCONCLUSIVE",
        terminal_state="incomplete",
    )
    candidate_sha = str(plan["candidate_sha"])
    _write_validated_artifact(
        artifact_dir / "recovery-evidence.json",
        artifact,
        candidate_sha=candidate_sha,
        scan_bundle=True,
    )
    _safe_message(f"confirmed candidate manifest written: {manifest_path}")
    return ExitCode.INCOMPLETE


def _operation_artifact(  # noqa: PLR0913
    *,
    command: str,
    dataset_plan: Mapping[str, Any],
    manifest_hash: str | None,
    metrics: Mapping[str, Any],
    outcome: str = "PASS",
    terminal_state: str = "completed",
    assurance: str = "caller_asserted",
    snapshot_requirement: str = "traversal_only",
    snapshot_state: str = "not_requested",
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    finished = datetime.now(UTC)
    wall_seconds = metrics.get("wall_seconds", 0.0)
    duration = float(wall_seconds) if isinstance(wall_seconds, int | float) else 0.0
    started = finished - timedelta(seconds=max(duration, 0.0))
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "run_id": dataset_plan["run_id"],
        "lineage_id": dataset_plan["lineage_id"],
        "original_head_sha": ORIGINAL_HEAD_SHA,
        "fixed_1x_sha": FIXED_1X_SHA,
        "portal_fingerprint": dataset_plan["portal"]["fingerprint"],
        "host": dataset_plan["portal"]["host"],
        "command": command,
        "phase": "complete",
        "case_id": None,
        "candidate_sha": dataset_plan["candidate_sha"],
        "skills_corpus_sha": SKILLS_CORPUS_SHA,
        "skills_recipe_tree_sha256": SKILLS_RECIPE_TREE_SHA256,
        "dataset_plan_content_hash": content_sha256(dataset_plan),
        "manifest_content_hash": manifest_hash,
        "profile_versions": [dataset_plan["disposable_profile_set_id"]],
        "plan_versions": [SCHEMA_VERSION],
        "runtime": {
            "python": platform.python_version(),
            "b24api": "working-tree-candidate",
            "httpx": httpx.__version__,
        },
        "started_at": started.isoformat().replace("+00:00", "Z"),
        "finished_at": finished.isoformat().replace("+00:00", "Z"),
        "outcome": outcome,
        "terminal_state": terminal_state,
        "assurance": assurance,
        "snapshot_requirement": snapshot_requirement,
        "snapshot_state": snapshot_state,
        "metrics": dict(metrics),
        "controls": None,
        "evidence_refs": [],
        "safe_violations": [],
    }
    artifact.update(extra or {})
    return artifact


def _write_candidate_json(
    path: Path,
    value: Mapping[str, Any],
    *,
    candidate_sha: str,
    scan_bundle: bool = False,
    rollback_log: list[tuple[Path, Mapping[str, Any] | None]] | None = None,
) -> None:
    """Atomically write only while the clean executing HEAD stays the exact candidate."""
    _require_evidence_candidate(candidate_sha)
    previous = read_json_object(path) if path.exists() else None
    try:
        atomic_write_json(path, value)
        _require_evidence_candidate(candidate_sha)
        if scan_bundle:
            _scan_bundle(path.parent, expected_candidate_sha=candidate_sha)
        if rollback_log is not None:
            rollback_log.append((path, previous))
    except BaseException:
        _restore_candidate_json(path, previous)
        raise


def _restore_candidate_json(path: Path, previous: Mapping[str, Any] | None) -> None:
    """Best-effort rollback without masking the candidate or bundle failure."""
    quarantine: Path | None = None
    try:
        if path.exists():
            bundle_root = path.parent.parent if path.parent.name == "model-oracles" else path.parent
            quarantine = bundle_root.parent / (f".{bundle_root.name}.{path.name}.refused-{uuid.uuid4().hex}")
            path.replace(quarantine)
        if previous is not None:
            atomic_write_json(path, previous)
    except BaseException:  # noqa: BLE001, S110 - rollback must never mask the primary refusal
        pass
    finally:
        if quarantine is not None:
            with contextlib.suppress(BaseException):
                quarantine.unlink(missing_ok=True)


def _rollback_candidate_json_log(log: list[tuple[Path, Mapping[str, Any] | None]]) -> None:
    for path, previous in reversed(log):
        _restore_candidate_json(path, previous)


def _persist_verify_bundle(
    *,
    artifact_dir: Path,
    oracle: Mapping[str, Any],
    artifact: Mapping[str, Any],
    candidate_sha: str,
) -> None:
    """Publish one verify dependency and terminal artifact as a transaction."""
    rollback_log: list[tuple[Path, Mapping[str, Any] | None]] = []
    try:
        _write_candidate_json(
            artifact_dir / "oracle.json",
            oracle,
            candidate_sha=candidate_sha,
            rollback_log=rollback_log,
        )
        _write_validated_artifact(
            artifact_dir / "verify-evidence.json",
            artifact,
            candidate_sha=candidate_sha,
            scan_bundle=True,
        )
    except BaseException:
        _rollback_candidate_json_log(rollback_log)
        raise


def _persist_plan_bundle(
    *,
    artifact_dir: Path,
    dataset_plan: Mapping[str, Any],
    benchmark_plan: Mapping[str, Any],
    artifact: Mapping[str, Any],
    candidate_sha: str,
) -> None:
    rollback_log: list[tuple[Path, Mapping[str, Any] | None]] = []
    try:
        _write_candidate_json(
            artifact_dir / "dataset-plan.json",
            dataset_plan,
            candidate_sha=candidate_sha,
            rollback_log=rollback_log,
        )
        _write_candidate_json(
            artifact_dir / "benchmark-plan.json",
            benchmark_plan,
            candidate_sha=candidate_sha,
            rollback_log=rollback_log,
        )
        _write_candidate_json(
            artifact_dir / "model-fixture-manifest.json",
            _model_fixture_manifest(),
            candidate_sha=candidate_sha,
            rollback_log=rollback_log,
        )
        _write_validated_artifact(
            artifact_dir / "plan-evidence.json",
            artifact,
            candidate_sha=candidate_sha,
            scan_bundle=True,
        )
    except BaseException:
        _rollback_candidate_json_log(rollback_log)
        raise


def _write_validated_artifact(
    path: Path,
    artifact: Mapping[str, Any],
    *,
    candidate_sha: str | None = None,
    scan_bundle: bool = False,
) -> None:
    if candidate_sha is None:
        require_clean_tracked_tree(ROOT)
        validate_evidence_artifact(artifact)
        atomic_write_json(path, artifact)
        return
    validate_evidence_artifact(artifact)
    _write_candidate_json(
        path,
        artifact,
        candidate_sha=candidate_sha,
        scan_bundle=scan_bundle,
    )


def _default_benchmark_plan(dataset_plan: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "plan_id": str(uuid.uuid4()),
        "lineage_id": dataset_plan["lineage_id"],
        "admission_state": "draft",
        "thresholds_normative": False,
        "original_head_sha": ORIGINAL_HEAD_SHA,
        "fixed_1x_sha": FIXED_1X_SHA,
        "candidate_sha": dataset_plan["candidate_sha"],
        "skills_corpus_sha": SKILLS_CORPUS_SHA,
        "skills_recipe_tree_sha256": SKILLS_RECIPE_TREE_SHA256,
        "dataset_plan_content_hash": content_sha256(dataset_plan),
        "manifest_content_hash": content_sha256(_model_fixture_manifest()),
        "instrumentation_review_sha": INSTRUMENTATION_REVIEW_SHA,
        "controls": {
            "warmups": _MODEL_WARMUPS,
            "advisory_runs": _MODEL_ADVISORY_RUNS,
            "blocking_pairs": _MODEL_BLOCKING_PAIRS,
            "interleaving": True,
            "timing_model": {
                "kind": "deterministic_request_cost",
                "seconds_per_request": MODEL_REQUEST_SECONDS,
            },
            "drift": {"status": "tbd_live", "max_rtt_ratio": "TBD-LIVE", "max_operating_ratio": "TBD-LIVE"},
        },
        "cases": [
            {
                "id": "MODEL-MATRIX",
                "compared_plans": ["offset", "keyset"],
                "benefit_gate": {
                    "blocking": False,
                    "minimum_median_improvement": NORMATIVE_MINIMUM_MEDIAN_IMPROVEMENT,
                    "paired_95_interval_excludes_parity": False,
                    "maximum_small_p95_ratio": NORMATIVE_MAXIMUM_SMALL_P95_RATIO,
                    "maximum_server_operating_ratio": NORMATIVE_MAXIMUM_OPERATING_RATIO,
                },
            },
        ],
    }


def _model_fixture_manifest() -> dict[str, Any]:
    """Return the immutable deterministic fixture lineage used by the offline matrix."""
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "deterministic-model-fixture",
        "cases": [
            {
                "case_id": case.case_id,
                "expected_hash": case.expected_hash,
                "raw_count": len(case.identities),
                "base_count": case.base_count,
                "mutation": case.mutation,
            }
            for case in exact_model_cases()
        ],
    }


def _manifest_base(
    plan: Mapping[str, Any],
    *,
    cell: Mapping[str, Any],
    correlation: str,
    marker: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": plan["run_id"],
        "lineage_id": plan["lineage_id"],
        "original_head_sha": ORIGINAL_HEAD_SHA,
        "fixed_1x_sha": FIXED_1X_SHA,
        "skills_corpus_sha": SKILLS_CORPUS_SHA,
        "skills_recipe_tree_sha256": SKILLS_RECIPE_TREE_SHA256,
        "dataset_plan_content_hash": content_sha256(plan),
        "portal_fingerprint": plan["portal"]["fingerprint"],
        "candidate_sha": plan["candidate_sha"],
        "namespace": plan["namespace"],
        "cell_id": cell["id"],
        "entity_family": cell["entity_family"],
        "correlation_key": correlation,
        "marker_hash": marker_sha256(marker),
        "marker_value": marker,
        "recorded_at": _timestamp(),
        "safe_error": None,
        "parent_correlation_keys": [],
    }


def _lineage(plan: Mapping[str, Any]) -> ManifestLineage:
    return ManifestLineage(
        run_id=str(plan["run_id"]),
        lineage_id=str(plan["lineage_id"]),
        original_head_sha=ORIGINAL_HEAD_SHA,
        fixed_1x_sha=FIXED_1X_SHA,
        skills_corpus_sha=SKILLS_CORPUS_SHA,
        skills_recipe_tree_sha256=SKILLS_RECIPE_TREE_SHA256,
        dataset_plan_content_hash=content_sha256(plan),
        portal_fingerprint=str(plan["portal"]["fingerprint"]),
        candidate_sha=str(plan["candidate_sha"]),
        namespace=str(plan["namespace"]),
    )


def _load_plan(args: argparse.Namespace, *, allow_generated: bool = False) -> dict[str, Any]:
    plan = _read_plan(args, allow_generated=allow_generated)
    _bind_plan_to_bundle(args, plan)
    return plan


def _read_plan(args: argparse.Namespace, *, allow_generated: bool = False) -> dict[str, Any]:
    """Load and validate a plan without mutating its artifact bundle."""
    if args.plan is None:
        default = args.artifact_dir.resolve() / "dataset-plan.json"
        if not default.exists() and allow_generated:
            run_id = _uuid_or_new(args.run_id, "run_id")
            lineage_id = _uuid_or_new(args.lineage_id, "lineage_id")
            plan = _model_dataset_plan(run_id=run_id, lineage_id=lineage_id)
            validate_dataset_plan(plan)
        else:
            plan = read_json_object(default)
    else:
        plan = read_json_object(args.plan)
    validate_dataset_plan(plan)
    if plan["candidate_sha"] != git_sha(ROOT):
        raise ContractError("dataset plan candidate_sha does not match the executing evidence code")
    if args.run_id is not None and args.run_id != plan["run_id"]:
        raise ContractError("requested run_id does not match dataset plan")
    if args.lineage_id is not None and args.lineage_id != plan["lineage_id"]:
        raise ContractError("requested lineage_id does not match dataset plan")
    return plan


def _bind_plan_to_bundle(args: argparse.Namespace, plan: Mapping[str, Any]) -> None:
    """Persist an admitted plan into its immutable artifact bundle."""
    artifact_dir = args.artifact_dir.resolve()
    bundled_plan_path = artifact_dir / "dataset-plan.json"
    candidate_sha = str(plan["candidate_sha"])
    _scan_bundle(artifact_dir, expected_candidate_sha=candidate_sha)
    if bundled_plan_path.exists():
        bundled_plan = read_json_object(bundled_plan_path)
        validate_dataset_plan(bundled_plan)
        if content_sha256(bundled_plan) != content_sha256(plan):
            raise ContractError("external dataset plan conflicts with the existing immutable evidence bundle")
    else:
        _write_candidate_json(bundled_plan_path, plan, candidate_sha=candidate_sha)
    _scan_bundle(artifact_dir, expected_candidate_sha=candidate_sha)


def _model_dataset_plan(*, run_id: str, lineage_id: str) -> dict[str, Any]:
    profile_set = validate_reviewed_profile_set(PROFILE_SET_PATH)
    profile = _profile(profile_set, "tasks-task-v1")
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "lineage_id": lineage_id,
        "original_head_sha": ORIGINAL_HEAD_SHA,
        "candidate_sha": git_sha(ROOT),
        "generator_sha": git_sha(ROOT),
        "skills_corpus_sha": SKILLS_CORPUS_SHA,
        "skills_recipe_tree_sha256": SKILLS_RECIPE_TREE_SHA256,
        "credential_role": "model",
        "disposable_profile_set_id": REVIEWED_PROFILE_SET_ID,
        "disposable_profiles_content_hash": REVIEWED_PROFILE_SET_SHA256,
        "portal": _model_portal(),
        "namespace": f"b24api-evidence-{run_id}",
        "cells": [
            {
                "id": "MODEL",
                "disposable_profile_id": profile["id"],
                "entity_family": profile["entity_family"],
                "target_count": 0,
                "base_count": 0,
                "relationship_count": 0,
                "distribution": "empty",
                "marker_field": profile["marker_field"],
                "create_method": profile["create_method"],
                "read_method": profile["read_method"],
                "delete_method": profile["delete_method"],
                "required_scopes": profile["required_scopes"],
            },
        ],
        "estimated": {
            "entities": 0,
            "relationships": 0,
            "create_strategy": "direct",
            "delete_strategy": "direct",
            "requests": 0,
            "batch_commands": 0,
            "duration_seconds": 0.0,
            "quota_impact": 0.0,
        },
        "cleanup": {"feasible": True, "dependency_order": ["MODEL"], "absence_verification": "exact_id_point_read"},
        "authorization": {
            "state": "preview",
            "live": False,
            "allow_writes": False,
            "plan_review_sha": None,
            "approved_by_user": False,
            "max_entities_per_cell": REVIEWED_MAX_ENTITIES_PER_CELL,
        },
    }


def _model_portal() -> dict[str, Any]:
    return {
        "host": "model.local",
        "role": "model",
        "fingerprint": hashlib.sha256(b"b24api-deterministic-model-v1").hexdigest(),
        "fingerprint_algorithm": "sha256-public-model-v1",
        "fingerprint_key_format": "not_applicable_model",
        "build": "deterministic-v1",
        "scope_hash": content_sha256([]),
    }


def _profile(profile_set: Mapping[str, Any], profile_id: str) -> dict[str, Any]:
    profiles = profile_set.get("profiles")
    if isinstance(profiles, list):
        for profile in profiles:
            if isinstance(profile, dict) and profile.get("id") == profile_id:
                return profile
    raise ContractError("requested disposable entity profile is not in the reviewed set")


def _require_approved_plan(plan: Mapping[str, Any], *, args: argparse.Namespace) -> None:
    authorization = plan["authorization"]
    if not isinstance(authorization, dict) or authorization.get("state") != "approved_for_seed":
        raise ContractError("live writes require a human-reviewed approved_for_seed plan")
    review_sha = authorization.get("plan_review_sha")
    if getattr(args, "confirm_plan_review_sha", None) != review_sha:
        raise ContractError("live writes require an external exact plan review SHA confirmation")
    plan_content_sha256 = content_sha256(plan)
    if getattr(args, "confirm_plan_content_sha256", None) != plan_content_sha256:
        raise ContractError("live writes require an external exact plan content hash confirmation")
    _require_review_commit(
        str(review_sha),
        reviewed_plan_sha256=reviewed_dataset_plan_sha256(plan),
    )
    if plan["portal"]["role"] == "model":
        raise ContractError("approved live-write plan cannot target the deterministic model portal")
    if sum(int(cell["target_count"]) for cell in plan["cells"]) == 0:
        raise ContractError("approved live-write plans require at least one disposable entity")


def _require_review_commit(review_sha: str, *, reviewed_plan_sha256: str) -> None:
    git = shutil.which("git")
    if git is None:
        raise ContractError("git executable is unavailable")
    try:
        object_type = subprocess.run(  # noqa: S603 - fixed git and validated hash-only revision
            [git, "cat-file", "-t", review_sha],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
        message = subprocess.run(  # noqa: S603 - fixed git and validated hash-only revision
            [git, "show", "--no-patch", "--format=%B", review_sha],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        raise ContractError("plan_review_sha must name an existing review commit") from error
    if object_type != "commit":
        raise ContractError("plan_review_sha must name a commit object")
    expected = f"Dataset-Plan-SHA256: {reviewed_plan_sha256}"
    if expected not in {line.strip() for line in message.splitlines()}:
        raise ContractError("plan review commit does not bind the exact dataset plan content hash")


def _require_exact_candidate(plan: Mapping[str, Any]) -> None:
    """Reject tracked changes and clean-HEAD drift immediately before live mutation."""
    _require_evidence_candidate(str(plan["candidate_sha"]), purpose="live mutation")


def _require_evidence_candidate(candidate_sha: str, *, purpose: str = "evidence execution") -> None:
    """Bind filesystem effects to one exact clean candidate commit."""
    require_clean_tracked_tree(ROOT)
    if git_sha(ROOT) != candidate_sha:
        raise ContractError(f"{purpose} candidate SHA differs from the reviewed dataset plan")


def _require_portal_match(plan: Mapping[str, Any], portal: LivePortal) -> None:
    planned = plan["portal"]
    if (
        planned["host"] != portal.identity.host
        or planned["role"] != portal.identity.role
        or planned["fingerprint"] != portal.identity.fingerprint
    ):
        raise ContractError("live portal identity does not match the reviewed plan")


def _require_preflight_match(plan: Mapping[str, Any], preflight: LivePreflight) -> None:
    """Reject reviewed-build or scope drift before any entity operation."""
    planned = plan["portal"]
    if not isinstance(preflight.build, str) or not preflight.build:
        raise LiveUnavailableError("live portal did not provide an exact build identifier")
    if planned.get("build") != preflight.build:
        raise LiveUnavailableError("live portal build differs from the reviewed plan")
    if planned.get("scope_hash") != content_sha256(sorted(preflight.scopes)):
        raise LiveUnavailableError("live portal scope set differs from the reviewed plan")


def _require_live_write_flags(args: argparse.Namespace, command: str) -> None:
    if not args.live or not args.allow_writes:
        raise ContractError(f"{command} requires both --live and --allow-writes")
    if os.environ.get("PYTEST_CURRENT_TEST"):
        raise ContractError(f"{command} live writes are forbidden under ordinary pytest")


def _manifest_path(args: argparse.Namespace) -> Path:
    manifest = cast("Path | None", args.manifest)
    artifact_dir = cast("Path", args.artifact_dir)
    return (manifest or (artifact_dir / "manifest.jsonl")).resolve()


def _required_path(path: Path | None, option: str) -> Path:
    if path is None:
        raise ContractError(f"{option} is required")
    return path.resolve()


def _uuid_or_new(value: str | None, field: str) -> str:
    if value is None:
        return str(uuid.uuid4())
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise ContractError(f"{field} must be a UUID") from error
    if str(parsed) != value:
        raise ContractError(f"{field} must use lowercase canonical UUID form")
    return value


def _timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _latest_records(records: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    return {str(record["correlation_key"]): record for record in records}


def _live_manifest_snapshot(
    portal: LivePortal,
    *,
    plan: Mapping[str, Any],
    latest: Mapping[str, Mapping[str, Any]],
) -> str:
    """Point-read every manifest identity and hash its exact ownership relation."""
    cells = {str(cell["id"]): cell for cell in plan["cells"]}
    qualified: list[list[str]] = []
    for correlation, record in sorted(latest.items()):
        cell = cells[str(record["cell_id"])]
        adapter = ADAPTERS[str(cell["disposable_profile_id"])]
        entity_id = str(record["entity_id"])
        entity = adapter.read(portal, entity_id)
        marker = str(record["marker_value"])
        if entity is None or _entity_marker(entity, str(cell["marker_field"])) != marker:
            raise LiveCorrectnessError("manifest identity failed independent exact-marker point-read")
        qualified.append([correlation, entity_id, marker_sha256(marker)])
    return content_sha256(qualified)


def _entity_marker(entity: Mapping[str, Any], marker_field: str) -> object:
    return entity.get(marker_field, entity.get(marker_field.casefold()))


def _scan_bundle(  # noqa: C901, PLR0912, PLR0915
    artifact_dir: Path,
    *,
    expected_candidate_sha: str | None = None,
) -> None:
    if expected_candidate_sha is not None:
        _require_evidence_candidate(expected_candidate_sha)
    else:
        require_clean_tracked_tree(ROOT)
    scan_paths_for_secrets(tracked_repository_paths(ROOT))
    artifact_paths: list[Path] = []
    total_bytes = 0
    for path in artifact_dir.rglob("*"):
        if not path.is_file():
            continue
        artifact_paths.append(path)
        if len(artifact_paths) > _MAX_BUNDLE_FILES:
            raise ContractError("evidence bundle exceeds the reviewed file-count ceiling")
        try:
            total_bytes += path.stat().st_size
        except OSError as error:
            raise ContractError(f"cannot inspect evidence bundle file: {path.name}") from error
        if total_bytes > _MAX_BUNDLE_BYTES:
            raise ContractError("evidence bundle exceeds the reviewed aggregate byte ceiling")
    scan_paths_for_secrets(artifact_paths)
    json_documents: dict[str, list[tuple[Path, dict[str, Any]]]] = {}
    for path in artifact_paths:
        if path.suffix == ".json" and not path.name.endswith("-evidence.json"):
            value = read_json_object(path)
            json_documents.setdefault(content_sha256(value), []).append((path, value))
    artifacts: list[dict[str, Any]] = []
    for path in artifact_paths:
        if not path.name.endswith("-evidence.json"):
            continue
        artifact = read_json_object(path)
        validate_evidence_artifact(artifact)
        artifacts.append(artifact)
        for reference in artifact["evidence_refs"]:
            if (
                isinstance(reference, str)
                and reference.startswith("sha256:")
                and reference.removeprefix("sha256:") not in json_documents
            ):
                raise ContractError(f"evidence content hash has no matching immutable JSON in {artifact_dir}")
    if not artifacts:
        if expected_candidate_sha is not None:
            _require_evidence_candidate(expected_candidate_sha)
        else:
            require_clean_tracked_tree(ROOT)
        return
    lineage_fields = (
        "run_id",
        "lineage_id",
        "original_head_sha",
        "fixed_1x_sha",
        "candidate_sha",
        "skills_corpus_sha",
        "skills_recipe_tree_sha256",
        "dataset_plan_content_hash",
        "portal_fingerprint",
    )
    lineages = {tuple(artifact[field] for field in lineage_fields) for artifact in artifacts}
    if len(lineages) != 1:
        raise ContractError("evidence bundle mixes run, lineage, candidate, dataset plan, or portal identity")
    lineage = next(iter(lineages))
    expected = dict(zip(lineage_fields, lineage, strict=True))
    plan_hash = str(expected["dataset_plan_content_hash"])
    plan_candidates = json_documents.get(plan_hash, [])
    if not plan_candidates:
        raise ContractError("evidence dataset plan content hash has no matching immutable JSON")
    dataset_plan = plan_candidates[0][1]
    validate_dataset_plan(dataset_plan)
    if (
        dataset_plan["run_id"] != expected["run_id"]
        or dataset_plan["lineage_id"] != expected["lineage_id"]
        or dataset_plan["original_head_sha"] != expected["original_head_sha"]
        or dataset_plan["candidate_sha"] != expected["candidate_sha"]
        or dataset_plan["skills_corpus_sha"] != expected["skills_corpus_sha"]
        or dataset_plan["skills_recipe_tree_sha256"] != expected["skills_recipe_tree_sha256"]
        or dataset_plan["portal"]["fingerprint"] != expected["portal_fingerprint"]
    ):
        raise ContractError("evidence bundle lineage does not match its immutable dataset plan")
    qualified_oracles: dict[str, dict[str, Any]] = {}
    for document_hash, documents in json_documents.items():
        for path, document in documents:
            if path.name != "oracle.json" and path.parent.name != "model-oracles":
                continue
            validate_oracle_record(document)
            if any(
                document[field] != expected[field]
                for field in (
                    "run_id",
                    "lineage_id",
                    "original_head_sha",
                    "fixed_1x_sha",
                    "candidate_sha",
                    "skills_corpus_sha",
                    "skills_recipe_tree_sha256",
                    "dataset_plan_content_hash",
                    "portal_fingerprint",
                )
            ):
                raise ContractError("oracle lineage does not match the evidence bundle")
            if document["outcome"] == "PASS":
                qualified_oracles[document_hash] = document
    for artifact in artifacts:
        if artifact["outcome"] != "PASS" or artifact["assurance"] != "oracle_verified":
            continue
        references = artifact["evidence_refs"]
        referenced_hashes = {
            reference.removeprefix("sha256:")
            for reference in references
            if isinstance(reference, str) and reference.startswith("sha256:")
        }
        if not references or len(referenced_hashes) != len(references):
            raise ContractError("oracle-verified PASS requires immutable SHA-256 oracle dependencies")
        if artifact["command"] == "verify":
            if not referenced_hashes <= qualified_oracles.keys():
                raise ContractError("oracle-verified PASS references a document that is not a qualified PASS oracle")
            matching = [
                document
                for document_hash in referenced_hashes
                for _path, document in json_documents[document_hash]
                if document.get("case_id") == "MANIFEST"
                and document.get("manifest_content_hash") == artifact["manifest_content_hash"]
            ]
            if len(matching) != 1:
                raise ContractError("verify PASS requires exactly one matching MANIFEST oracle")
        elif artifact["command"] == "benchmark":
            _validate_benchmark_pass_dependencies(
                artifact,
                json_documents=json_documents,
                qualified_oracles=qualified_oracles,
            )
        else:
            raise ContractError("oracle_verified PASS is unsupported for this command")
    if expected_candidate_sha is not None:
        if expected["candidate_sha"] != expected_candidate_sha:
            raise ContractError("evidence bundle candidate differs from the executing candidate")
        _require_evidence_candidate(expected_candidate_sha)
    else:
        require_clean_tracked_tree(ROOT)


def _validate_benchmark_pass_dependencies(  # noqa: C901
    artifact: Mapping[str, Any],
    *,
    json_documents: Mapping[str, list[tuple[Path, dict[str, Any]]]],
    qualified_oracles: Mapping[str, dict[str, Any]],
) -> None:
    """Bind a benchmark PASS to its exact plan, stable matrix, oracle set, and metric algebra."""
    benchmark_plan_hash = str(artifact.get("benchmark_plan_content_hash"))
    plan_documents = json_documents.get(benchmark_plan_hash, [])
    if not plan_documents:
        raise ContractError("benchmark PASS has no immutable benchmark plan")
    benchmark_plan = plan_documents[0][1]
    validate_benchmark_plan(benchmark_plan)
    if benchmark_plan["admission_state"] != "draft":
        raise ContractError("model benchmark PASS requires its exact draft benchmark plan")
    _require_exact_model_benchmark_plan(benchmark_plan)
    if (
        benchmark_plan["candidate_sha"] != artifact["candidate_sha"]
        or benchmark_plan["lineage_id"] != artifact["lineage_id"]
        or benchmark_plan["dataset_plan_content_hash"] != artifact["dataset_plan_content_hash"]
        or benchmark_plan["manifest_content_hash"] != artifact["manifest_content_hash"]
    ):
        raise ContractError("benchmark plan lineage does not match its PASS artifact")
    fixture_documents = [
        document
        for path, document in json_documents.get(str(benchmark_plan["manifest_content_hash"]), [])
        if path.name == "model-fixture-manifest.json"
    ]
    if len(fixture_documents) != 1 or fixture_documents[0] != _model_fixture_manifest():
        raise ContractError("benchmark PASS has no exact immutable model fixture manifest")

    referenced_hashes = {reference.removeprefix("sha256:") for reference in artifact["evidence_refs"]}
    matrix_hashes = referenced_hashes - qualified_oracles.keys()
    if len(matrix_hashes) != 1:
        raise ContractError("benchmark PASS requires exactly one stable model-matrix dependency")
    matrix_hash = next(iter(matrix_hashes))
    matrix_candidates = [
        document for path, document in json_documents.get(matrix_hash, []) if path.name == "model-matrix.json"
    ]
    if len(matrix_candidates) != 1:
        raise ContractError("benchmark PASS matrix hash does not resolve to model-matrix.json")
    matrix = matrix_candidates[0]
    if set(matrix) != {
        "schema_version",
        "run_id",
        "lineage_id",
        "original_head_sha",
        "fixed_1x_sha",
        "candidate_sha",
        "skills_corpus_sha",
        "skills_recipe_tree_sha256",
        "dataset_plan_content_hash",
        "benchmark_plan_content_hash",
        "runs",
    }:
        raise ContractError("benchmark model matrix has an unexpected shape")
    for field in (
        "run_id",
        "lineage_id",
        "original_head_sha",
        "fixed_1x_sha",
        "candidate_sha",
        "skills_corpus_sha",
        "skills_recipe_tree_sha256",
        "dataset_plan_content_hash",
    ):
        if matrix[field] != artifact[field]:
            raise ContractError("benchmark model matrix lineage does not match its PASS artifact")
    if matrix["benchmark_plan_content_hash"] != benchmark_plan_hash:
        raise ContractError("benchmark model matrix does not match its benchmark plan")
    observations = matrix["runs"]
    if not isinstance(observations, list):
        raise ContractError("benchmark model matrix runs must be a list")
    _validate_model_observations(
        observations,
        artifact=artifact,
        referenced_hashes=referenced_hashes,
        matrix_hash=matrix_hash,
        qualified_oracles=qualified_oracles,
    )


def _validate_model_observations(
    observations: list[Any],
    *,
    artifact: Mapping[str, Any],
    referenced_hashes: set[str],
    matrix_hash: str,
    qualified_oracles: Mapping[str, dict[str, Any]],
) -> None:
    """Enforce the exact deterministic stable-run set and recompute parent metrics."""
    cases = {case.case_id: case for case in exact_model_cases() if not case.mutation}
    reference_runs = {(run.case_id, run.plan): run for run in run_exact_matrix_sync() if run.outcome == "PASS"}
    expected_keys = {
        (iteration, case_id, plan)
        for iteration in range(1, _MODEL_ADVISORY_RUNS + 1)
        for case_id in cases
        for plan in ("offset", "keyset")
    }
    run_fields = {
        "iteration",
        "case_id",
        "plan",
        "identities",
        "expected_hash",
        "actual_hash",
        "pre_hash",
        "post_hash",
        "requests",
        "logical_pages",
        "operating_seconds",
        "time_to_first_row_seconds",
        "wall_seconds",
        "buffered_rows_high_water",
        "outcome",
        "snapshot_state",
        "mutation_retries",
    }
    observed_keys: set[tuple[int, str, str]] = set()
    required_oracle_hashes: set[str] = set()
    oracle_by_case = {str(oracle["case_id"]): (value, oracle) for value, oracle in qualified_oracles.items()}
    for observation in observations:
        if not isinstance(observation, dict) or set(observation) != run_fields:
            raise ContractError("benchmark model observation has an unexpected shape")
        iteration = observation["iteration"]
        case_id = observation["case_id"]
        plan_name = observation["plan"]
        if (
            isinstance(iteration, bool)
            or not isinstance(iteration, int)
            or not isinstance(case_id, str)
            or not isinstance(plan_name, str)
        ):
            raise ContractError("benchmark model observation identity is malformed")
        _validate_observation_value_types(observation)
        key = (iteration, case_id, plan_name)
        observed_keys.add(key)
        case = cases.get(case_id)
        reference_run = reference_runs.get((case_id, plan_name))
        oracle_entry = oracle_by_case.get(f"{case_id}-{plan_name}-run-{iteration}")
        if case is None or reference_run is None or oracle_entry is None:
            raise ContractError("benchmark model observation has no exact reviewed case oracle")
        oracle_hash, oracle = oracle_entry
        identities = observation["identities"]
        if identities != list(case.identities) or observation["expected_hash"] != case.expected_hash:
            raise ContractError("benchmark model observation does not match the deterministic case oracle")
        if (
            observation["outcome"] != "PASS"
            or observation["snapshot_state"] != "verified"
            or observation["mutation_retries"] != 0
            or observation["pre_hash"] != oracle["pre_hash"]
            or observation["post_hash"] != oracle["post_hash"]
            or observation["actual_hash"] != oracle["actual_result_hash"]
            or observation["expected_hash"] != oracle["expected_result_hash"]
            or oracle["qualification"] != "independent_cross_method"
            or oracle["snapshot_requirement"] != "independent_pre_post_oracle"
            or oracle["manifest_content_hash"] != case.expected_hash
            or len(identities) != oracle["raw_count"]
            or len(set(identities)) != oracle["unique_count"]
        ):
            raise ContractError("benchmark model observation contradicts its PASS oracle")
        if (
            observation["requests"] != reference_run.requests
            or observation["logical_pages"] != reference_run.logical_pages
            or observation["buffered_rows_high_water"] != reference_run.buffered_rows_high_water
            or not math.isclose(
                float(observation["operating_seconds"]),
                reference_run.operating_seconds,
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
            or not math.isclose(
                float(observation["wall_seconds"]),
                reference_run.wall_seconds,
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
            or observation["time_to_first_row_seconds"] != reference_run.time_to_first_row_seconds
        ):
            raise ContractError("benchmark model observation counters or timing contradict deterministic execution")
        _validate_observation_timing(observation)
        required_oracle_hashes.add(oracle_hash)
    if len(observations) != len(expected_keys) or observed_keys != expected_keys:
        raise ContractError("benchmark PASS does not contain the exact stable model run set")
    if referenced_hashes != required_oracle_hashes | {matrix_hash}:
        raise ContractError("benchmark PASS references do not equal its exact matrix and oracle set")
    _validate_benchmark_metric_algebra(observations, artifact)


def _validate_observation_timing(observation: Mapping[str, Any]) -> None:
    wall = observation["wall_seconds"]
    first = observation["time_to_first_row_seconds"]
    if isinstance(wall, bool) or not isinstance(wall, int | float) or not math.isfinite(wall) or wall <= 0:
        raise ContractError("benchmark observation wall time must be positive and finite")
    if observation["identities"]:
        if (
            isinstance(first, bool)
            or not isinstance(first, int | float)
            or not math.isfinite(first)
            or not 0 <= first <= wall
        ):
            raise ContractError("benchmark observation first-row time is invalid")
    elif first is not None:
        raise ContractError("empty benchmark observation cannot report a first-row time")


def _validate_observation_value_types(observation: Mapping[str, Any]) -> None:
    identities = observation["identities"]
    if not isinstance(identities, list) or any(type(value) is not int or value < 0 for value in identities):
        raise ContractError("benchmark observation identities must be exact non-negative integers")
    integer_fields = ("requests", "logical_pages", "buffered_rows_high_water", "mutation_retries")
    if any(type(observation[field]) is not int or observation[field] < 0 for field in integer_fields):
        raise ContractError("benchmark observation counters must be exact non-negative integers")
    operating = observation["operating_seconds"]
    if (
        isinstance(operating, bool)
        or not isinstance(operating, int | float)
        or not math.isfinite(operating)
        or operating < 0
    ):
        raise ContractError("benchmark observation operating time must be finite and non-negative")


def _validate_benchmark_metric_algebra(observations: list[dict[str, Any]], artifact: Mapping[str, Any]) -> None:
    metrics = artifact["metrics"]
    rows = sum(len(run["identities"]) for run in observations)
    exact_metrics = {
        "http_attempts": sum(run["requests"] for run in observations),
        "logical_pages": sum(run["logical_pages"] for run in observations),
        "buffered_rows_high_water": max(run["buffered_rows_high_water"] for run in observations),
        "raw_rows": rows,
        "unique_rows": rows,
        "overlap": rows,
    }
    if any(metrics[field] != value for field, value in exact_metrics.items()):
        raise ContractError("benchmark PASS integer metrics do not match its stable observations")
    float_metrics = {
        "time_to_first_row_seconds": min(
            run["time_to_first_row_seconds"] for run in observations if run["time_to_first_row_seconds"] is not None
        ),
        "wall_seconds": sum(run["wall_seconds"] for run in observations),
        "server_operating_seconds": sum(run["operating_seconds"] for run in observations),
    }
    if any(
        not math.isclose(float(metrics[field]), float(value), rel_tol=1e-12, abs_tol=1e-12)
        for field, value in float_metrics.items()
    ):
        raise ContractError("benchmark PASS timing metrics do not match its stable observations")
    offset = [run for run in observations if run["plan"] == "offset"]
    keyset = [run for run in observations if run["plan"] == "keyset"]
    offset_control = sum(run["operating_seconds"] for run in offset) / sum(run["requests"] for run in offset)
    keyset_control = sum(run["operating_seconds"] for run in keyset) / sum(run["requests"] for run in keyset)
    controls = derive_drift_controls(
        rtt_before=offset_control,
        rtt_after=keyset_control,
        operating_before=offset_control,
        operating_after=keyset_control,
        max_rtt_ratio=1.20,
        max_operating_ratio=1.20,
    )
    if content_sha256(artifact["controls"]) != content_sha256(controls):
        raise ContractError("benchmark PASS drift controls do not match its stable observations")


def _safe_message(message: str) -> None:
    sys.stdout.write(f"{DEFAULT_REDACTOR.redact_text(message)}\n")


def _safe_error(error: BaseException) -> None:
    rendered = DEFAULT_REDACTOR.redact_text(str(error))
    sys.stderr.write(f"error: {type(error).__name__}: {rendered}\n")


def _abort(message: str) -> NoReturn:
    raise ContractError(message)


if __name__ == "__main__":
    raise SystemExit(main())
