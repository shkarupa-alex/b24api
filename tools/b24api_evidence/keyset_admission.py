"""Deterministic evaluation of batched-keyset sandwich artifacts."""

from __future__ import annotations
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, cast

from .harness.contracts import clean_candidate_sha

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_VERSION = 1
GIT_SHA_HEX_LENGTH = 40
PORTAL_FINGERPRINT_LENGTH = 64
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
_BAND_RULES = {
    "small": (20, 1.0, 1.10, True),
    "intermediate": (5, 1.0, 1.05, True),
    "large": (5, 0.60, 0.85, False),
}
_SOURCES = frozenset({"deterministic_fixture", "live_read_only", "combined_live_fixture"})


def _current_candidate_sha() -> str:
    return clean_candidate_sha(ROOT)


def _validate_artifact_binding(artifact: dict[str, Any], candidate_sha: str) -> None:
    source, sha = artifact.get("source"), artifact.get("sha")
    if source not in _SOURCES:
        raise ValueError("keyset admission artifact source is missing or unsupported")
    if (
        not isinstance(sha, str)
        or len(sha) != GIT_SHA_HEX_LENGTH
        or any(character not in "0123456789abcdef" for character in sha)
    ):
        raise ValueError("keyset admission artifact requires an exact lowercase candidate SHA")
    if sha != candidate_sha:
        raise ValueError("keyset admission artifact does not match the current candidate SHA")
    required = {"python_version", "portal_fingerprint", "wall_clock_unix"}
    if not required.issubset(artifact):
        raise ValueError("keyset admission artifact is missing required provenance")
    fingerprint = artifact["portal_fingerprint"]
    if (
        not isinstance(fingerprint, str)
        or len(fingerprint) != PORTAL_FINGERPRINT_LENGTH
        or any(character not in "0123456789abcdef" for character in fingerprint)
    ):
        raise ValueError("keyset admission artifact requires a lowercase portal fingerprint")
    if not isinstance(artifact["python_version"], str) or not artifact["python_version"]:
        raise ValueError("keyset admission artifact requires a Python version")
    wall_clock = artifact["wall_clock_unix"]
    if (
        not isinstance(wall_clock, int | float)
        or isinstance(wall_clock, bool)
        or not math.isfinite(wall_clock)
        or wall_clock <= 0
    ):
        raise ValueError("keyset admission artifact requires a finite wall clock")


def _request_band(requests: int) -> str:
    if requests <= SMALL_MAX_REQUESTS:
        return "small"
    if requests <= INTERMEDIATE_MAX_REQUESTS:
        return "intermediate"
    return "large"


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


def analyze_artifact(  # noqa: C901, PLR0912, PLR0915
    artifact: dict[str, Any],
    *,
    _fixture_substitution: bool = False,
    _candidate_sha: str | None = None,
) -> dict[str, Any]:
    """Evaluate correctness, stability, sampling, and paired performance gates."""
    candidate_sha = _candidate_sha or _current_candidate_sha()
    _validate_artifact_binding(artifact, candidate_sha)
    if artifact.get("source") == "combined_live_fixture":
        return _analyze_combined_artifact(artifact, candidate_sha=candidate_sha)
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
        required_fields = {
            "cell",
            "mode",
            "warmup",
            "sha",
            "schema_version",
            "python_version",
            "portal_fingerprint",
            "wall_clock_unix",
            "window_seconds",
            "page_size",
            "batch_size",
            "target_lanes",
            "writable_limit",
            "contract",
            "total_hint",
            "rotation_offset",
            "control_requests",
            "control_wall_seconds",
            "sequential_before",
            "candidate",
            "sequential_after",
        }
        if not required_fields.issubset(sample):
            raise ValueError("keyset admission sample is missing required metadata")
        if sample["sha"] != candidate_sha:
            raise ValueError("keyset admission sample does not match the current candidate SHA")
        if (
            sample["schema_version"] != SCHEMA_VERSION
            or sample["python_version"] != artifact["python_version"]
            or sample["portal_fingerprint"] != artifact["portal_fingerprint"]
        ):
            raise ValueError("keyset admission sample provenance does not match its artifact")
        sample_wall_clock = sample["wall_clock_unix"]
        if (
            not isinstance(sample_wall_clock, int | float)
            or isinstance(sample_wall_clock, bool)
            or not math.isfinite(sample_wall_clock)
            or sample_wall_clock <= 0
        ):
            raise ValueError("keyset admission sample requires a finite wall clock")
        for role in ("sequential_before", "candidate", "sequential_after"):
            if "first_row_seconds" not in sample[role]:
                raise ValueError("keyset admission run is missing first-row timing")
        window_seconds = sample["window_seconds"]
        if (
            not isinstance(window_seconds, int | float)
            or isinstance(window_seconds, bool)
            or not math.isfinite(window_seconds)
            or window_seconds <= 0
        ):
            raise ValueError("keyset admission sample requires positive finite contemporaneity")
        key = (sample["cell"], sample["mode"])
        exclusion = _exclusion(sample)
        sample["exclusion_reason"] = exclusion
        groups[key].append(sample)
        before, candidate, after = (sample[role] for role in ("sequential_before", "candidate", "sequential_after"))
        checks = {
            "contemporaneous": window_seconds <= MAX_SANDWICH_WINDOW_SECONDS,
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
        observed_bands = {_request_band(sample["control_requests"]) for sample in samples if not sample["warmup"]}
        band = next(iter(observed_bands)) if len(observed_bands) == 1 else None
        required, threshold_req, threshold_time, use_p95 = (
            _BAND_RULES[band] if band is not None else (20, 0.0, 0.0, True)
        )
        exclusions = sum(not sample["warmup"] and sample["exclusion_reason"] is not None for sample in samples)
        unstable = exclusions > 3 * max(1, len(accepted)) / 5
        ratios_req = [sample["candidate"]["requests"] / sample["control_requests"] for sample in accepted]
        ratios_time = [sample["candidate"]["wall_seconds"] / sample["control_wall_seconds"] for sample in accepted]
        median_req = lower_median(ratios_req) if ratios_req else None
        median_time = lower_median(ratios_time) if ratios_time else None
        p95_time = nearest_rank_p95(ratios_time) if ratios_time else None
        time_stat = p95_time if use_p95 else median_time
        parity = band != "large"
        improved = sum(
            (request_ratio <= 1.0 and time_ratio <= threshold_time)
            if parity
            else (request_ratio < 1.0 and time_ratio < 1.0)
            for request_ratio, time_ratio in zip(ratios_req, ratios_time, strict=True)
        )
        performance_passed = (
            band is not None
            and len(accepted) >= required
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
                "band": band,
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
    performance_failures.extend({"cell": cell, "mode": mode} for cell, mode in sorted(scoped - observed))
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


def _analyze_combined_artifact(  # noqa: C901, PLR0912, PLR0915
    artifact: dict[str, Any],
    *,
    candidate_sha: str,
) -> dict[str, Any]:
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

    live_result = analyze_artifact(live, _candidate_sha=candidate_sha)
    fixture_result = analyze_artifact(fixture, _fixture_substitution=True, _candidate_sha=candidate_sha)
    live_groups = {(group["cell"], group["mode"]): group for group in live_result["groups"]}
    fixture_groups = {(group["cell"], group["mode"]): group for group in fixture_result["groups"]}
    live_samples: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for sample in live["samples"]:
        live_samples[(sample["cell"], sample["mode"])].append(sample)

    declared: dict[tuple[str, str], dict[str, Any]] = {}
    valid_substitutions: dict[tuple[str, str], tuple[str, str]] = {}
    substitution_failures: list[dict[str, str]] = []
    for entry in substitutions:
        if not isinstance(entry, dict):
            raise TypeError("each substitution must be an object")
        live_cell, fixture_cell, mode = entry.get("live_cell"), entry.get("fixture_cell"), entry.get("mode")
        if not isinstance(live_cell, str) or not isinstance(fixture_cell, str) or not isinstance(mode, str):
            raise TypeError("substitution cells and mode must be strings")
        live_key = (live_cell, mode)
        fixture_key = (fixture_cell, mode)
        if live_key in declared:
            raise ValueError("duplicate live substitution")
        declared[live_key] = entry
        live_group = live_groups.get(live_key)
        fixture_group = fixture_groups.get(fixture_key)
        windows = {
            sample.get("attempt_window") for sample in live_samples.get(live_key, ()) if not sample.get("warmup")
        }
        reasons = entry.get("reason_codes")
        valid = False
        if live_group is not None and fixture_group is not None:
            deficient = live_group["accepted"] < live_group["required"] or live_group["unstable"]
            expected_reasons = set()
            if live_group["accepted"] < live_group["required"]:
                expected_reasons.add("insufficient_accepted_samples")
            if live_group["unstable"]:
                expected_reasons.add("unstable")
            valid = (
                deficient
                and live["manifest"].get("attempt_windows") == LIVE_ATTEMPT_WINDOWS
                and windows == set(range(1, LIVE_ATTEMPT_WINDOWS + 1))
                and isinstance(reasons, list)
                and set(reasons) == expected_reasons
                and fixture_key in set(map(tuple, fixture["manifest"]["performance_scope"]))
                and live_group["band"] is not None
                and live_group["band"] == fixture_group["band"]
                and fixture_group["performance_passed"]
            )
        if not valid:
            substitution_failures.append({"cell": str(live_key[0]), "mode": str(live_key[1])})
        else:
            valid_substitutions[live_key] = fixture_key

    live_scope = set(map(tuple, live["manifest"]["performance_scope"]))
    unresolved = []
    for key in sorted(live_scope):
        group = live_groups.get(key)
        if (group is None or not group["performance_passed"]) and key not in declared:
            unresolved.append({"cell": key[0], "mode": key[1]})
    matrix_failed = any(failure["cell"] == "__live_matrix__" for failure in live_result["performance_failures"])
    performance_failures = [*substitution_failures, *unresolved]
    if matrix_failed:
        performance_failures.append({"cell": "__live_matrix__", "mode": "all"})
    substituted_material_range = any(
        live_key[1] == "range"
        and fixture_groups[fixture_key]["median_request_ratio"] is not None
        and fixture_groups[fixture_key]["median_request_ratio"] <= LARGE_REQUEST_RATIO
        for live_key, fixture_key in valid_substitutions.items()
    )
    material_range = live_result["material_range_speedup"] or substituted_material_range
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
