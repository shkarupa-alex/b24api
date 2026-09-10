"""Deterministic evaluation of batched-keyset sandwich artifacts."""

from __future__ import annotations
import math
from collections import defaultdict
from typing import Any, cast

SCHEMA_VERSION = 1
SMALL_MAX_REQUESTS = 3
INTERMEDIATE_MAX_REQUESTS = 10
LARGE_REQUEST_RATIO = 0.60
MAX_SANDWICH_WINDOW_SECONDS = 120.0
LIVE_ATTEMPT_WINDOWS = 3
REQUIRED_LIVE_MATRIX_FEATURES = frozenset(
    {
        "unfiltered",
        "filtered",
        "two_large_selections",
        "small_selection",
        "density_below_5_percent",
        "density_5_to_25_percent",
        "density_above_50_percent",
        "clustered_or_skewed",
        "tasks_split_roles",
        "same_case_endpoint",
        "total_hint_ignore",
        "total_hint_advisory",
    },
)


def lower_median(values: list[float]) -> float:
    """Return the deterministic lower median."""
    if not values:
        raise ValueError("median requires at least one value")
    ordered = sorted(values)
    return ordered[(len(ordered) - 1) // 2]


def nearest_rank_p95(values: list[float]) -> float:
    """Return the nearest-rank 95th percentile."""
    if not values:
        raise ValueError("p95 requires at least one value")
    ordered = sorted(values)
    return ordered[math.ceil(0.95 * len(ordered)) - 1]


def _exclusion(sample: dict[str, Any]) -> str | None:
    if sample.get("window_seconds", 0.0) > MAX_SANDWICH_WINDOW_SECONDS:
        return "window_exceeded"
    before, candidate, after = (sample[role] for role in ("sequential_before", "candidate", "sequential_after"))
    if before["digest"] != after["digest"]:
        return "mutation_invalid"
    if any(
        run["retries"] or run["cooldown_seconds"] or run.get("transport_fault") for run in (before, candidate, after)
    ):
        return "transport_perturbed"
    slower = max(before["wall_seconds"], after["wall_seconds"])
    faster = min(before["wall_seconds"], after["wall_seconds"])
    if slower > 2 * max(faster, 1e-12):
        return "control_drift"
    return None


def _raw_ceiling(sample: dict[str, Any]) -> int:
    run = sample["candidate"]
    page_cap = sample["page_size"]
    selected = run["selected_kind"]
    overlap = run["boundary_overlap_rows"]
    if selected == "range":
        return cast("int", run["admitted"] + 5 * page_cap + overlap)
    if selected == "partitioned":
        probe_term = sample["target_lanes"] if sample["writable_limit"] else sample["target_lanes"] * page_cap
        return cast("int", run["admitted"] + 5 * page_cap + probe_term + overlap)
    if selected == "sequential":
        return cast("int", run["admitted"] + run["tail_rows"] + overlap)
    return cast("int", run["admitted"] + overlap)


def analyze_artifact(  # noqa: C901, PLR0912, PLR0915
    artifact: dict[str, Any],
    *,
    _fixture_substitution: bool = False,
) -> dict[str, Any]:
    """Evaluate correctness, stability, sampling, and paired performance gates."""
    if artifact.get("source") == "combined_live_fixture":
        return _analyze_combined_artifact(artifact)
    if artifact.get("schema_version") != SCHEMA_VERSION or not isinstance(artifact.get("samples"), list):
        raise ValueError("unsupported keyset admission artifact")
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    correctness_failures: list[dict[str, str]] = []
    manifest = artifact.get("manifest")
    if not isinstance(manifest, dict):
        raise TypeError("keyset admission artifact requires a manifest")
    raw_correctness_scope = manifest.get("correctness_scope")
    if not isinstance(raw_correctness_scope, list) or not raw_correctness_scope:
        raise ValueError("manifest requires a non-empty correctness_scope")
    correctness_scope = set(map(tuple, raw_correctness_scope))
    expected_auto = manifest.get("expected_auto_selections")
    if not isinstance(expected_auto, dict):
        raise TypeError("expected_auto_selections must be an object")
    expected_auto_cells = {cell for cell, mode in correctness_scope if mode == "auto"}
    correctness_failures.extend(
        {"cell": cell, "mode": "auto", "check": "missing_expected_selection"}
        for cell in sorted(expected_auto_cells - set(expected_auto))
    )
    for sample in artifact["samples"]:
        key = (sample["cell"], sample["mode"])
        exclusion = _exclusion(sample)
        sample["exclusion_reason"] = exclusion
        groups[key].append(sample)
        before, candidate, after = (sample[role] for role in ("sequential_before", "candidate", "sequential_after"))
        checks = {
            "stable_digest": before["digest"] == after["digest"] == candidate["digest"],
            "omissions": candidate["omissions"] == 0,
            "duplicates": candidate["duplicates"] == 0,
            "output_overfetch": candidate["output_overfetch"] == 0,
            "false_completion": candidate["false_completion"] == 0,
            "raw_ceiling": candidate["raw_rows"] <= _raw_ceiling(sample),
            "resources": not any(run["resources_leaked"] for run in (before, candidate, after)),
        }
        oracle_dependent = {"stable_digest", "omissions", "output_overfetch"}
        correctness_failures.extend(
            {"cell": key[0], "mode": key[1], "check": name}
            for name, passed in checks.items()
            if not passed and not (exclusion == "mutation_invalid" and name in oracle_dependent)
        )
        expected = expected_auto.get(key[0]) if key[1] == "auto" else None
        if expected is not None and candidate["selected_kind"] != expected:
            correctness_failures.append({"cell": key[0], "mode": key[1], "check": "auto_selection"})

    group_results: list[dict[str, Any]] = []
    for (cell, mode), samples in sorted(groups.items()):
        accepted = [sample for sample in samples if not sample["warmup"] and sample["exclusion_reason"] is None]
        baseline = accepted[0]["control_requests"] if accepted else 0
        required = 20 if baseline <= SMALL_MAX_REQUESTS else 5
        exclusions = sum(not sample["warmup"] and sample["exclusion_reason"] is not None for sample in samples)
        unstable = exclusions > 3 * max(1, len(accepted)) / 5
        ratios_req = [sample["candidate"]["requests"] / sample["control_requests"] for sample in accepted]
        ratios_time = [sample["candidate"]["wall_seconds"] / sample["control_wall_seconds"] for sample in accepted]
        median_req = lower_median(ratios_req) if ratios_req else None
        median_time = lower_median(ratios_time) if ratios_time else None
        p95_time = nearest_rank_p95(ratios_time) if ratios_time else None
        if baseline <= SMALL_MAX_REQUESTS:
            threshold_req, threshold_time, use_p95 = 1.0, 1.10, True
        elif baseline <= INTERMEDIATE_MAX_REQUESTS:
            threshold_req, threshold_time, use_p95 = 1.0, 1.05, True
        else:
            threshold_req, threshold_time, use_p95 = 0.60, 0.85, False
        time_stat = p95_time if use_p95 else median_time
        parity = baseline <= INTERMEDIATE_MAX_REQUESTS
        improved = sum(
            (request_ratio <= 1.0 and time_ratio <= threshold_time)
            if parity
            else (request_ratio < 1.0 and time_ratio < 1.0)
            for request_ratio, time_ratio in zip(ratios_req, ratios_time, strict=True)
        )
        performance_passed = (
            len(accepted) >= required
            and not unstable
            and median_req is not None
            and median_req <= threshold_req
            and time_stat is not None
            and time_stat <= threshold_time
            and improved >= math.ceil(0.8 * len(accepted))
        )
        group_results.append(
            {
                "cell": cell,
                "mode": mode,
                "accepted": len(accepted),
                "required": required,
                "exclusions": exclusions,
                "unstable": unstable,
                "median_request_ratio": median_req,
                "median_time_ratio": median_time,
                "p95_time_ratio": p95_time,
                "performance_passed": performance_passed,
            },
        )
    observed_groups = set(groups)
    correctness_failures.extend(
        {"cell": cell, "mode": mode, "check": "missing_group"}
        for cell, mode in sorted(correctness_scope - observed_groups)
    )
    scoped = set(map(tuple, manifest["performance_scope"]))
    observed = {(result["cell"], result["mode"]) for result in group_results}
    performance_failures = [
        {"cell": result["cell"], "mode": result["mode"]}
        for result in group_results
        if (result["cell"], result["mode"]) in scoped and not result["performance_passed"]
    ]
    performance_failures.extend(
        {"cell": cell, "mode": mode}
        for cell, mode in sorted(scoped - observed)
    )
    material_range = any(
        result["mode"] == "range"
        and (result["cell"], result["mode"]) in scoped
        and result["performance_passed"]
        and result["median_request_ratio"] is not None
        and result["median_request_ratio"] <= LARGE_REQUEST_RATIO
        for result in group_results
    )
    if artifact.get("source") == "live_read_only":
        matrix = artifact["manifest"].get("live_matrix", {})
        covered = frozenset(matrix.get("covered_features", ())) if isinstance(matrix, dict) else frozenset()
        declared_complete = isinstance(matrix, dict) and matrix.get("complete") is True
        declared_modes = frozenset(artifact["manifest"].get("modes", ()))
        if (
            not declared_complete
            or not REQUIRED_LIVE_MATRIX_FEATURES.issubset(covered)
            or declared_modes != {"range", "partitioned", "auto"}
        ):
            performance_failures.append({"cell": "__live_matrix__", "mode": "all"})
    elif artifact.get("source") == "deterministic_fixture" and not _fixture_substitution:
        performance_failures.append({"cell": "__live_shortfall__", "mode": "all"})
    return {
        "schema_version": SCHEMA_VERSION,
        "correctness_passed": not correctness_failures,
        "performance_passed": not performance_failures and material_range,
        "material_range_speedup": material_range,
        "correctness_failures": correctness_failures,
        "performance_failures": performance_failures,
        "groups": group_results,
    }


def _analyze_combined_artifact(artifact: dict[str, Any]) -> dict[str, Any]:  # noqa: C901, PLR0912, PLR0915
    """Admit fixture performance only for proven three-window live shortfalls."""
    live = artifact.get("live_artifact")
    fixture = artifact.get("fixture_artifact")
    substitutions = artifact.get("substitutions")
    if not isinstance(live, dict) or live.get("source") != "live_read_only":
        raise ValueError("combined admission requires one live_read_only artifact")
    if not isinstance(fixture, dict) or fixture.get("source") != "deterministic_fixture":
        raise ValueError("combined admission requires one deterministic_fixture artifact")
    if not isinstance(substitutions, list):
        raise TypeError("combined admission requires a substitutions list")
    if live.get("sha") != fixture.get("sha") or artifact.get("sha") != live.get("sha"):
        raise ValueError("combined admission evidence must bind one candidate SHA")

    live_result = analyze_artifact(live)
    fixture_result = analyze_artifact(fixture, _fixture_substitution=True)
    live_groups = {(group["cell"], group["mode"]): group for group in live_result["groups"]}
    fixture_groups = {(group["cell"], group["mode"]): group for group in fixture_result["groups"]}
    live_samples: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for sample in live["samples"]:
        live_samples[(sample["cell"], sample["mode"])].append(sample)

    declared: dict[tuple[str, str], dict[str, Any]] = {}
    substitution_failures: list[dict[str, str]] = []
    for entry in substitutions:
        if not isinstance(entry, dict):
            raise TypeError("each substitution must be an object")
        live_key = (entry.get("live_cell"), entry.get("mode"))
        fixture_key = (entry.get("fixture_cell"), entry.get("mode"))
        if not all(isinstance(value, str) for value in (*live_key, fixture_key[0])):
            raise ValueError("substitution cells and mode must be strings")
        if live_key in declared:
            raise ValueError("duplicate live substitution")
        declared[live_key] = entry
        live_group = live_groups.get(live_key)
        fixture_group = fixture_groups.get(fixture_key)
        windows = {
            sample.get("attempt_window")
            for sample in live_samples.get(live_key, ())
            if not sample.get("warmup")
        }
        reasons = entry.get("reason_codes")
        deficient = live_group is not None and (
            live_group["accepted"] < live_group["required"] or live_group["unstable"]
        )
        expected_reasons = set()
        if live_group is not None and live_group["accepted"] < live_group["required"]:
            expected_reasons.add("insufficient_accepted_samples")
        if live_group is not None and live_group["unstable"]:
            expected_reasons.add("unstable")
        valid = (
            deficient
            and live["manifest"].get("attempt_windows") == LIVE_ATTEMPT_WINDOWS
            and windows == set(range(1, LIVE_ATTEMPT_WINDOWS + 1))
            and isinstance(reasons, list)
            and set(reasons) == expected_reasons
            and fixture_group is not None
            and fixture_group["performance_passed"]
        )
        if not valid:
            substitution_failures.append({"cell": str(live_key[0]), "mode": str(live_key[1])})

    live_scope = set(map(tuple, live["manifest"]["performance_scope"]))
    unresolved = []
    for key in sorted(live_scope):
        group = live_groups.get(key)
        if (group is None or not group["performance_passed"]) and key not in declared:
            unresolved.append({"cell": key[0], "mode": key[1]})
    matrix_failed = any(
        failure["cell"] == "__live_matrix__" for failure in live_result["performance_failures"]
    )
    performance_failures = [*substitution_failures, *unresolved]
    if matrix_failed:
        performance_failures.append({"cell": "__live_matrix__", "mode": "all"})
    material_range = live_result["material_range_speedup"] or fixture_result["material_range_speedup"]
    return {
        "schema_version": SCHEMA_VERSION,
        "correctness_passed": live_result["correctness_passed"] and fixture_result["correctness_passed"],
        "performance_passed": not performance_failures and material_range,
        "material_range_speedup": material_range,
        "correctness_failures": [
            *live_result["correctness_failures"],
            *fixture_result["correctness_failures"],
        ],
        "performance_failures": performance_failures,
        "groups": {"live": live_result["groups"], "fixture": fixture_result["groups"]},
    }


__all__ = [
    "LIVE_ATTEMPT_WINDOWS",
    "REQUIRED_LIVE_MATRIX_FEATURES",
    "SCHEMA_VERSION",
    "analyze_artifact",
    "lower_median",
    "nearest_rank_p95",
]
