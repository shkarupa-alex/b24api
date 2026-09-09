"""Deterministic evaluation of batched-keyset sandwich artifacts."""

from __future__ import annotations
import math
from collections import defaultdict
from typing import Any, cast

SCHEMA_VERSION = 1
SMALL_MAX_REQUESTS = 3
INTERMEDIATE_MAX_REQUESTS = 10
LARGE_REQUEST_RATIO = 0.60


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


def analyze_artifact(artifact: dict[str, Any]) -> dict[str, Any]:
    """Evaluate correctness, stability, sampling, and paired performance gates."""
    if artifact.get("schema_version") != SCHEMA_VERSION or not isinstance(artifact.get("samples"), list):
        raise ValueError("unsupported keyset admission artifact")
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    correctness_failures: list[dict[str, str]] = []
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
            "resources": not candidate["resources_leaked"],
        }
        correctness_failures.extend(
            {"cell": key[0], "mode": key[1], "check": name}
            for name, passed in checks.items()
            if not passed and exclusion != "mutation_invalid"
        )

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
        improved = sum(ratio <= 1.0 if parity else ratio < 1.0 for ratio in ratios_req)
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
    scoped = set(map(tuple, artifact["manifest"]["performance_scope"]))
    performance_failures = [
        {"cell": result["cell"], "mode": result["mode"]}
        for result in group_results
        if (result["cell"], result["mode"]) in scoped and not result["performance_passed"]
    ]
    material_range = any(
        result["mode"] == "range"
        and result["median_request_ratio"] is not None
        and result["median_request_ratio"] <= LARGE_REQUEST_RATIO
        for result in group_results
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "correctness_passed": not correctness_failures,
        "performance_passed": not performance_failures and material_range,
        "material_range_speedup": material_range,
        "correctness_failures": correctness_failures,
        "performance_failures": performance_failures,
        "groups": group_results,
    }


__all__ = ["SCHEMA_VERSION", "analyze_artifact", "lower_median", "nearest_rank_p95"]
