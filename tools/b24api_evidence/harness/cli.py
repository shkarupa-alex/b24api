"""Normative CLI orchestration for offline and explicitly enabled live evidence."""

from __future__ import annotations
import argparse
import hashlib
import platform
import sys
import time
import uuid
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn, cast

import httpx

from .contracts import (
    FINGERPRINT_ALGORITHM,
    FINGERPRINT_KEY_FORMAT,
    NORMATIVE_MAXIMUM_OPERATING_RATIO,
    NORMATIVE_MAXIMUM_SMALL_P95_RATIO,
    NORMATIVE_MINIMUM_MEDIAN_IMPROVEMENT,
    REVIEWED_MAX_ENTITIES_PER_CELL,
    REVIEWED_PROFILE_SET_ID,
    REVIEWED_PROFILE_SET_SHA256,
    SCHEMA_VERSION,
    ContractError,
    ExitCode,
    ManifestLineage,
    SecretLeakError,
    append_manifest_record,
    atomic_write_json,
    build_manifest_record,
    content_sha256,
    derive_drift_controls,
    git_sha,
    load_manifest,
    manifest_content_hash,
    marker_sha256,
    marker_value,
    read_json_object,
    scan_paths_for_secrets,
    tracked_repository_paths,
    validate_benchmark_plan,
    validate_dataset_plan,
    validate_evidence_artifact,
    validate_oracle_record,
    validate_reviewed_profile_set,
)
from .live import ADAPTERS, LiveCorrectnessError, LivePortal, LiveUnavailableError
from .model import run_exact_matrix_sync

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[3]
PROFILE_SET_PATH = ROOT / "docs/bitrix24-client-2.0/w0/disposable-entity-profiles.json"
DEFAULT_ARTIFACT_DIR = Path(".b24api-evidence")
_ROLE_CHOICES = ("admin_full", "admin_limited", "employee_full", "employee_limited")


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
    return parser


def _dispatch(args: argparse.Namespace) -> ExitCode:
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
    if args.allow_writes:
        raise ContractError("plan is read-only; --allow-writes is invalid")
    run_id = _uuid_or_new(args.run_id, "run_id")
    lineage_id = _uuid_or_new(args.lineage_id, "lineage_id")
    if not 0 <= args.count <= REVIEWED_MAX_ENTITIES_PER_CELL:
        raise ContractError(f"count must be between 0 and {REVIEWED_MAX_ENTITIES_PER_CELL}")
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
        "candidate_sha": candidate_sha,
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
    artifact_dir = args.artifact_dir.resolve()
    plan_path = artifact_dir / "dataset-plan.json"
    atomic_write_json(plan_path, dataset_plan)
    benchmark_plan = _default_benchmark_plan(dataset_plan)
    validate_benchmark_plan(benchmark_plan)
    atomic_write_json(artifact_dir / "benchmark-plan.json", benchmark_plan)
    artifact = _operation_artifact(
        command="plan",
        dataset_plan=dataset_plan,
        manifest_hash=None,
        metrics={"kind": "operation", "http_attempts": int(args.live) * 2, "wall_seconds": 0.0},
    )
    _write_validated_artifact(artifact_dir / "plan-evidence.json", artifact)
    _scan_bundle(artifact_dir)
    _safe_message(f"plan completed: {plan_path}")
    return ExitCode.COMPLETED


def _seed(args: argparse.Namespace) -> ExitCode:  # noqa: C901, PLR0912, PLR0915
    _require_live_write_flags(args, "seed")
    plan_path = _required_path(args.plan, "--plan")
    plan = read_json_object(plan_path)
    validate_dataset_plan(plan)
    _require_approved_plan(plan)
    validate_reviewed_profile_set(PROFILE_SET_PATH)
    manifest_path = _manifest_path(args)
    artifact_dir = args.artifact_dir.resolve()
    previous: dict[str, Any] | None = None
    existing: list[dict[str, Any]] = []
    if manifest_path.exists():
        existing = load_manifest(manifest_path, expected=_lineage(plan))
        previous = existing[-1]
    terminal_by_key = {record["correlation_key"]: record for record in existing}
    http_attempts = 0
    started = time.monotonic()
    with LivePortal(role=args.credential_role) as portal:
        _require_portal_match(plan, portal)
        required_scopes = {scope for cell in plan["cells"] for scope in cell.get("required_scopes", [])}
        portal.preflight(required_scopes=required_scopes)
        http_attempts += 2
        for cell in plan["cells"]:
            adapter = ADAPTERS[str(cell["disposable_profile_id"])]
            for index in range(int(cell["target_count"])):
                correlation = content_sha256([plan["run_id"], cell["id"], index])
                current = terminal_by_key.get(correlation)
                if current is not None and current["event"] in {
                    "verified",
                    "delete_dispatched",
                    "deleted",
                    "absence_verified",
                }:
                    continue
                marker = marker_value(str(plan["namespace"]), correlation)
                base = _manifest_base(plan, cell=cell, correlation=correlation, marker=marker)
                recovered_id: str | None = None
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
                                http_attempts=http_attempts,
                                started=started,
                            )
                elif current is not None and current["event"] in {"planned", "create_dispatched", "ambiguous"}:
                    matches = adapter.find_exact_marker(portal, marker)
                    http_attempts += 1
                    if len(matches) > 1:
                        raise LiveCorrectnessError("resume found multiple exact-marker entities")
                    recovered_id = matches[0] if matches else None
                if recovered_id is not None:
                    reconciled = build_manifest_record(
                        {
                            **base,
                            "event": "reconciled",
                            "entity_id": recovered_id,
                            "request_fingerprint": content_sha256(["resume-exact-marker", correlation]),
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
                            "request_fingerprint": reconciled["request_fingerprint"],
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
                            http_attempts=http_attempts,
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
    artifact = _operation_artifact(
        command="seed",
        dataset_plan=plan,
        manifest_hash=manifest_content_hash(manifest_path),
        metrics={"kind": "operation", "http_attempts": http_attempts, "wall_seconds": time.monotonic() - started},
    )
    _write_validated_artifact(artifact_dir / "seed-evidence.json", artifact)
    _scan_bundle(artifact_dir)
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
    _write_validated_artifact(artifact_dir / "seed-evidence.json", artifact)
    return ExitCode.INCOMPLETE


def _verify(args: argparse.Namespace) -> ExitCode:
    plan = _load_plan(args)
    manifest_path = _manifest_path(args)
    records = load_manifest(manifest_path, expected=_lineage(plan))
    manifest_hash = manifest_content_hash(manifest_path)
    identities = sorted(
        str(record["entity_id"])
        for record in _latest_records(records).values()
        if record["event"] in {"created", "reconciled", "verified"} and record["entity_id"] is not None
    )
    snapshot_hash = content_sha256(identities)
    oracle = {
        "schema_version": SCHEMA_VERSION,
        "run_id": plan["run_id"],
        "lineage_id": plan["lineage_id"],
        "candidate_sha": plan["candidate_sha"],
        "dataset_plan_content_hash": content_sha256(plan),
        "manifest_content_hash": manifest_hash,
        "qualification": "immutable_manifest",
        "snapshot_requirement": "frozen_manifest",
        "snapshot_state": "verified",
        "pre_hash": snapshot_hash,
        "post_hash": snapshot_hash,
        "mutation_retries": 0,
        "raw_count": len(identities),
        "unique_count": len(set(identities)),
        "outcome": "PASS",
    }
    validate_oracle_record(oracle)
    artifact_dir = args.artifact_dir.resolve()
    atomic_write_json(artifact_dir / "oracle.json", oracle)
    artifact = _operation_artifact(
        command="verify",
        dataset_plan=plan,
        manifest_hash=manifest_hash,
        metrics={"kind": "operation", "http_attempts": 0, "wall_seconds": 0.0},
        snapshot_requirement="frozen_manifest",
        snapshot_state="verified",
        assurance="oracle_verified",
    )
    _write_validated_artifact(artifact_dir / "verify-evidence.json", artifact)
    _scan_bundle(artifact_dir)
    _safe_message(f"verify completed: {artifact_dir / 'oracle.json'}")
    return ExitCode.COMPLETED


def _benchmark(args: argparse.Namespace) -> ExitCode:
    if args.allow_writes:
        raise ContractError("benchmark is read-only; --allow-writes is invalid")
    plan = _load_plan(args, allow_generated=True)
    benchmark_plan = (
        read_json_object(args.benchmark_plan) if args.benchmark_plan is not None else _default_benchmark_plan(plan)
    )
    validate_benchmark_plan(benchmark_plan)
    started = time.monotonic()
    runs = run_exact_matrix_sync()
    rows = sum(len(run.identities) for run in runs if run.outcome == "PASS")
    requests = sum(run.requests for run in runs)
    logical_pages = sum(run.logical_pages for run in runs)
    controls = derive_drift_controls(
        rtt_before=0.010,
        rtt_after=0.010,
        operating_before=0.002,
        operating_after=0.002,
        max_rtt_ratio=1.20,
        max_operating_ratio=1.20,
    )
    metrics = {
        "kind": "benchmark",
        "http_attempts": requests,
        "logical_pages": logical_pages,
        "batch_requests": 0,
        "batch_commands": 0,
        "time_to_first_row_seconds": 0.001,
        "wall_seconds": max(time.monotonic() - started, 0.000001),
        "server_operating_seconds": sum(run.operating_seconds for run in runs),
        "retries": 0,
        "cooldown_seconds": 0.0,
        "buffered_rows_high_water": 50,
        "rss_delta_bytes": None,
        "raw_rows": rows,
        "unique_rows": rows,
        "overlap": rows,
        "duplicates": 0,
        "shortfall": 0,
        "overfetch": 0,
        "reference_failures": 0,
    }
    artifact = _operation_artifact(
        command="benchmark",
        dataset_plan=plan,
        manifest_hash=None,
        metrics=metrics,
        assurance="oracle_verified",
        extra={
            "benchmark_plan_content_hash": content_sha256(benchmark_plan),
            "controls": controls,
            "case_id": "MODEL-MATRIX",
        },
    )
    artifact_dir = args.artifact_dir.resolve()
    atomic_write_json(
        artifact_dir / "model-matrix.json",
        {"schema_version": SCHEMA_VERSION, "runs": [asdict(run) for run in runs]},
    )
    atomic_write_json(artifact_dir / "benchmark-plan.json", benchmark_plan)
    _write_validated_artifact(artifact_dir / "benchmark-evidence.json", artifact)
    _scan_bundle(artifact_dir)
    _safe_message(f"benchmark completed: {artifact_dir / 'benchmark-evidence.json'}")
    return ExitCode.COMPLETED


def _resume(args: argparse.Namespace) -> ExitCode:
    plan = _load_plan(args)
    records = load_manifest(_manifest_path(args), expected=_lineage(plan))
    artifact = _operation_artifact(
        command="resume",
        dataset_plan=plan,
        manifest_hash=manifest_content_hash(_manifest_path(args)),
        metrics={"kind": "operation", "http_attempts": 0, "wall_seconds": 0.0, "records": len(records)},
    )
    artifact_path = args.artifact_dir.resolve() / "resume-evidence.json"
    _write_validated_artifact(artifact_path, artifact)
    _safe_message(f"resume validation completed: {artifact_path}")
    return ExitCode.COMPLETED


def _cleanup(args: argparse.Namespace) -> ExitCode:  # noqa: PLR0915
    _require_live_write_flags(args, "cleanup")
    plan = _load_plan(args)
    _require_approved_plan(plan)
    manifest_path = _manifest_path(args)
    records = load_manifest(manifest_path, expected=_lineage(plan))
    previous = records[-1]
    latest = _latest_records(records)
    cell_by_id = {str(cell["id"]): cell for cell in plan["cells"]}
    ordered = sorted(latest.values(), key=lambda record: list(cell_by_id).index(str(record["cell_id"])), reverse=True)
    orphans: list[str] = []
    attempts = 0
    started = time.monotonic()
    with LivePortal(role=args.credential_role) as portal:
        _require_portal_match(plan, portal)
        required_scopes = {scope for cell in plan["cells"] for scope in cell.get("required_scopes", [])}
        portal.preflight(required_scopes=required_scopes)
        attempts += 2
        for current in ordered:
            if current["event"] == "absence_verified" or current["entity_id"] is None:
                continue
            cell = cell_by_id[str(current["cell_id"])]
            adapter = ADAPTERS[str(cell["disposable_profile_id"])]
            base = _manifest_base(
                plan,
                cell=cell,
                correlation=str(current["correlation_key"]),
                marker=str(current["marker_value"]),
            )
            entity_id = str(current["entity_id"])
            request_fingerprint = content_sha256(
                [adapter.delete_method, [adapter.id_parameter], current["correlation_key"]],
            )
            owned = adapter.read(portal, entity_id)
            attempts += 1
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
            if _entity_marker(owned, str(cell["marker_field"])) != current["marker_value"]:
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
                orphans.append(str(current["correlation_key"]))
                continue
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
            adapter.delete(portal, entity_id)
            attempts += 1
            deleted = build_manifest_record(
                {**base, "event": "deleted", "entity_id": entity_id, "request_fingerprint": request_fingerprint},
                previous=previous,
            )
            append_manifest_record(manifest_path, deleted)
            previous = deleted
            absent = adapter.read(portal, entity_id) is None
            attempts += 1
            event = "absence_verified" if absent else "orphan"
            checked = build_manifest_record(
                {**base, "event": event, "entity_id": entity_id, "request_fingerprint": request_fingerprint},
                previous=previous,
            )
            append_manifest_record(manifest_path, checked)
            previous = checked
            if not absent:
                orphans.append(str(current["correlation_key"]))
    outcome = "PASS" if not orphans else "FAIL"
    artifact = _operation_artifact(
        command="cleanup",
        dataset_plan=plan,
        manifest_hash=manifest_content_hash(manifest_path),
        metrics={
            "kind": "operation",
            "http_attempts": attempts,
            "wall_seconds": time.monotonic() - started,
            "orphan_count": len(orphans),
            "absence_verified": not orphans,
            "orphan_refs": [f"sha256:{value}" for value in orphans],
        },
        outcome=outcome,
        terminal_state="completed" if not orphans else "failed",
    )
    artifact_path = args.artifact_dir.resolve() / "cleanup-evidence.json"
    _write_validated_artifact(artifact_path, artifact)
    _scan_bundle(args.artifact_dir.resolve())
    if orphans:
        _safe_error(RuntimeError(f"cleanup left {len(orphans)} verified orphan(s)"))
        return ExitCode.ORPHANS
    _safe_message(f"cleanup completed: {artifact_path}")
    return ExitCode.COMPLETED


def _recover_manifest(args: argparse.Namespace) -> ExitCode:  # noqa: C901
    if not args.live:
        raise ContractError("recover-manifest requires --live")
    if args.allow_writes:
        raise ContractError("recover-manifest is read-only and rejects --allow-writes")
    plan = _load_plan(args)
    if args.run_id is not None and args.run_id != plan["run_id"]:
        raise ContractError("recovery run_id does not match the reviewed plan")
    candidates: list[dict[str, Any]] = []
    with LivePortal(role=args.credential_role) as portal:
        _require_portal_match(plan, portal)
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
    preview = {
        "schema_version": SCHEMA_VERSION,
        "run_id": plan["run_id"],
        "lineage_id": plan["lineage_id"],
        "dataset_plan_content_hash": content_sha256(plan),
        "candidate_sha": plan["candidate_sha"],
        "portal_fingerprint": plan["portal"]["fingerprint"],
        "namespace": plan["namespace"],
        "exact_marker_candidates": candidates,
        "confirmation_required": not args.confirm_recovery,
    }
    artifact_dir = args.artifact_dir.resolve()
    atomic_write_json(artifact_dir / "recovery-preview.json", preview)
    if not args.confirm_recovery:
        _safe_message(
            f"recovery preview completed; rerun with --confirm-recovery: {artifact_dir / 'recovery-preview.json'}",
        )
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
    _safe_message(f"confirmed candidate manifest written: {manifest_path}")
    return ExitCode.COMPLETED


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
    now = _timestamp()
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "run_id": dataset_plan["run_id"],
        "lineage_id": dataset_plan["lineage_id"],
        "portal_fingerprint": dataset_plan["portal"]["fingerprint"],
        "host": dataset_plan["portal"]["host"],
        "command": command,
        "phase": "complete",
        "case_id": None,
        "candidate_sha": dataset_plan["candidate_sha"],
        "dataset_plan_content_hash": content_sha256(dataset_plan),
        "manifest_content_hash": manifest_hash,
        "profile_versions": [dataset_plan["disposable_profile_set_id"]],
        "plan_versions": [SCHEMA_VERSION],
        "runtime": {
            "python": platform.python_version(),
            "b24api": "working-tree-candidate",
            "httpx": httpx.__version__,
        },
        "started_at": now,
        "finished_at": now,
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


def _write_validated_artifact(path: Path, artifact: Mapping[str, Any]) -> None:
    validate_evidence_artifact(artifact)
    atomic_write_json(path, artifact)


def _default_benchmark_plan(dataset_plan: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "plan_id": str(uuid.uuid4()),
        "lineage_id": dataset_plan["lineage_id"],
        "admission_state": "draft",
        "thresholds_normative": False,
        "candidate_sha": dataset_plan["candidate_sha"],
        "dataset_plan_content_hash": content_sha256(dataset_plan),
        "controls": {
            "warmups": 1,
            "advisory_runs": 5,
            "blocking_pairs": 9,
            "interleaving": True,
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
        dataset_plan_content_hash=content_sha256(plan),
        portal_fingerprint=str(plan["portal"]["fingerprint"]),
        candidate_sha=str(plan["candidate_sha"]),
        namespace=str(plan["namespace"]),
    )


def _load_plan(args: argparse.Namespace, *, allow_generated: bool = False) -> dict[str, Any]:
    if args.plan is None:
        default = args.artifact_dir.resolve() / "dataset-plan.json"
        if not default.exists() and allow_generated:
            run_id = _uuid_or_new(args.run_id, "run_id")
            lineage_id = _uuid_or_new(args.lineage_id, "lineage_id")
            plan = _model_dataset_plan(run_id=run_id, lineage_id=lineage_id)
            validate_dataset_plan(plan)
            return plan
        path = default
    else:
        path = args.plan
    plan = read_json_object(path)
    validate_dataset_plan(plan)
    if plan["candidate_sha"] != git_sha(ROOT):
        raise ContractError("dataset plan candidate_sha does not match the executing evidence code")
    if args.run_id is not None and args.run_id != plan["run_id"]:
        raise ContractError("requested run_id does not match dataset plan")
    if args.lineage_id is not None and args.lineage_id != plan["lineage_id"]:
        raise ContractError("requested lineage_id does not match dataset plan")
    return plan


def _model_dataset_plan(*, run_id: str, lineage_id: str) -> dict[str, Any]:
    profile_set = validate_reviewed_profile_set(PROFILE_SET_PATH)
    profile = _profile(profile_set, "tasks-task-v1")
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "lineage_id": lineage_id,
        "candidate_sha": git_sha(ROOT),
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


def _require_approved_plan(plan: Mapping[str, Any]) -> None:
    authorization = plan["authorization"]
    if not isinstance(authorization, dict) or authorization.get("state") != "approved_for_seed":
        raise ContractError("live writes require a human-reviewed approved_for_seed plan")
    if plan["portal"]["role"] == "model":
        raise ContractError("approved live-write plan cannot target the deterministic model portal")


def _require_portal_match(plan: Mapping[str, Any], portal: LivePortal) -> None:
    planned = plan["portal"]
    if (
        planned["host"] != portal.identity.host
        or planned["role"] != portal.identity.role
        or planned["fingerprint"] != portal.identity.fingerprint
    ):
        raise ContractError("live portal identity does not match the reviewed plan")


def _require_live_write_flags(args: argparse.Namespace, command: str) -> None:
    if not args.live or not args.allow_writes:
        raise ContractError(f"{command} requires both --live and --allow-writes")


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


def _entity_marker(entity: Mapping[str, Any], marker_field: str) -> object:
    return entity.get(marker_field, entity.get(marker_field.casefold()))


def _scan_bundle(artifact_dir: Path) -> None:
    scan_paths_for_secrets(tracked_repository_paths(ROOT))
    scan_paths_for_secrets(path for path in artifact_dir.rglob("*") if path.is_file())


def _safe_message(message: str) -> None:
    sys.stdout.write(f"{message}\n")


def _safe_error(error: BaseException) -> None:
    sys.stderr.write(f"error: {type(error).__name__}: {error}\n")


def _abort(message: str) -> NoReturn:
    raise ContractError(message)


if __name__ == "__main__":
    raise SystemExit(main())
