"""Admission analyzer aggregation and fail-closed gates."""

# ruff: noqa: PLR2004

from __future__ import annotations
import shutil
import subprocess
from copy import deepcopy
from typing import Any

import pytest

from .keyset_admission import REQUIRED_LIVE_MATRIX_FEATURES, analyze_artifact, lower_median, nearest_rank_p95

GIT = shutil.which("git")
assert GIT is not None
CANDIDATE_SHA = subprocess.run(  # noqa: S603 - resolved fixed test executable
    [GIT, "rev-parse", "HEAD"], check=True, capture_output=True, text=True,
).stdout.strip()


def _run(*, digest: str = "same", requests: int = 10, seconds: float = 1.0) -> dict[str, Any]:
    return {
        "digest": digest,
        "requests": requests,
        "wall_seconds": seconds,
        "retries": 0,
        "cooldown_seconds": 0.0,
        "transport_fault": False,
        "omissions": 0,
        "duplicates": 0,
        "output_overfetch": 0,
        "false_completion": 0,
        "resources_leaked": False,
        "raw_rows": 120,
        "admitted": 100,
        "selected_kind": "range",
        "boundary_overlap_rows": 0,
        "tail_rows": 50,
    }


def _artifact() -> dict[str, Any]:
    samples = []
    for index in range(6):
        before = _run()
        after = _run(seconds=1.1)
        candidate = _run(requests=4, seconds=0.7)
        samples.append(
            {
                "cell": "large",
                "mode": "range",
                "warmup": index == 0,
                "sha": CANDIDATE_SHA,
                "window_seconds": 1.0,
                "rotation_offset": index % 3,
                "page_size": 50,
                "batch_size": 50,
                "target_lanes": 20,
                "writable_limit": True,
                "contract": "empty_confirmation",
                "total_hint": "ignore",
                "control_requests": 10,
                "control_wall_seconds": 1.0,
                "sequential_before": before,
                "candidate": candidate,
                "sequential_after": after,
            },
        )
    return {
        "schema_version": 1,
        "source": "live_read_only",
        "sha": CANDIDATE_SHA,
        "manifest": {
            "performance_scope": [["large", "range"]],
            "correctness_scope": [["large", "range"]],
            "expected_auto_selections": {},
            "modes": ["range", "partitioned", "auto"],
            "live_matrix": {
                "complete": True,
                "covered_features": sorted(REQUIRED_LIVE_MATRIX_FEATURES),
            },
        },
        "samples": samples,
    }


def test_order_statistics_use_lower_median_and_nearest_rank() -> None:
    assert lower_median([4.0, 1.0, 3.0, 2.0]) == 2.0
    assert nearest_rank_p95(list(map(float, range(1, 21)))) == 19.0


def test_analyzer_accepts_paired_large_cell_and_material_range_gain() -> None:
    result = analyze_artifact(_artifact())

    assert result["correctness_passed"] is True
    assert result["performance_passed"] is True
    assert result["material_range_speedup"] is True


def test_analyzer_rejects_unbound_or_incomplete_release_metadata() -> None:
    missing_source = deepcopy(_artifact())
    missing_source.pop("source")
    with pytest.raises(ValueError, match="source"):
        analyze_artifact(missing_source)

    wrong_sha = deepcopy(_artifact())
    wrong_sha["sha"] = "0" * 40
    with pytest.raises(ValueError, match="current candidate"):
        analyze_artifact(wrong_sha)

    missing_window = deepcopy(_artifact())
    missing_window["samples"][0].pop("window_seconds")
    with pytest.raises(ValueError, match="required metadata"):
        analyze_artifact(missing_window)


def test_standalone_fixture_cannot_supply_release_performance_evidence() -> None:
    artifact = _artifact()
    artifact["source"] = "deterministic_fixture"

    result = analyze_artifact(artifact)

    assert result["performance_passed"] is False
    assert {tuple(failure.values()) for failure in result["performance_failures"]} >= {
        ("__live_shortfall__", "all"),
    }


def test_combined_evidence_requires_three_live_windows_and_exact_shortfall_reasons() -> None:
    fixture = _artifact()
    fixture.update({"source": "deterministic_fixture", "sha": CANDIDATE_SHA})
    live = deepcopy(_artifact())
    live.update({"source": "live_read_only", "sha": CANDIDATE_SHA})
    live["manifest"]["modes"] = ["range", "partitioned", "auto"]
    live["manifest"]["attempt_windows"] = 3
    live["manifest"]["live_matrix"] = {
        "complete": True,
        "covered_features": sorted(
            {
                "unfiltered", "filtered", "two_large_selections", "small_selection",
                "density_below_5_percent", "density_5_to_25_percent", "density_above_50_percent",
                "clustered_or_skewed", "tasks_split_roles", "same_case_endpoint",
                "total_hint_ignore", "total_hint_advisory",
            },
        ),
    }
    live["samples"] = live["samples"][:4]
    for index, sample in enumerate(live["samples"]):
        sample["attempt_window"] = 0 if sample["warmup"] else index
    combined: dict[str, Any] = {
        "schema_version": 1,
        "source": "combined_live_fixture",
        "sha": CANDIDATE_SHA,
        "live_artifact": live,
        "fixture_artifact": fixture,
        "substitutions": [{
            "live_cell": "large",
            "fixture_cell": "large",
            "mode": "range",
            "reason_codes": ["insufficient_accepted_samples"],
        }],
    }

    result = analyze_artifact(combined)

    assert result["correctness_passed"] is True
    assert result["performance_passed"] is True

    band_mismatch = deepcopy(combined)
    for sample in band_mismatch["live_artifact"]["samples"]:
        sample["control_requests"] = 11
        sample["sequential_before"]["requests"] = 11
        sample["sequential_after"]["requests"] = 11
    assert analyze_artifact(band_mismatch)["performance_passed"] is False

    excluded_large = deepcopy(combined)
    for sample in excluded_large["live_artifact"]["samples"]:
        sample["control_requests"] = 11
        sample["sequential_before"]["requests"] = 11
        sample["sequential_after"]["requests"] = 11
        sample["candidate"]["transport_fault"] = True
    result = analyze_artifact(excluded_large)
    assert result["groups"]["live"][0]["accepted"] == 0
    assert result["groups"]["live"][0]["band"] == "large"
    assert result["performance_passed"] is False

    no_substitution = deepcopy(combined)
    no_substitution["substitutions"] = []
    no_substitution["live_artifact"]["samples"].extend(
        deepcopy(no_substitution["live_artifact"]["samples"][-1:]) * 2,
    )
    for sample in no_substitution["live_artifact"]["samples"]:
        sample["warmup"] = False
        sample["candidate"]["requests"] = 10
        sample["candidate"]["wall_seconds"] = 1.0
    assert analyze_artifact(no_substitution)["performance_passed"] is False

    live["samples"][3]["attempt_window"] = 2
    assert analyze_artifact(combined)["performance_passed"] is False


def test_analyzer_retains_mutation_exclusion_and_blocks_digest_mismatch() -> None:
    mutation = deepcopy(_artifact())
    mutation["samples"][1]["sequential_after"]["digest"] = "changed"
    mutation_result = analyze_artifact(mutation)
    assert mutation_result["groups"][0]["exclusions"] == 1
    assert mutation_result["correctness_passed"] is True

    mutation_with_leak = deepcopy(mutation)
    mutation_with_leak["samples"][1]["sequential_after"]["resources_leaked"] = True
    leaked_result = analyze_artifact(mutation_with_leak)
    assert {failure["check"] for failure in leaked_result["correctness_failures"]} == {"resources"}

    mismatch = deepcopy(_artifact())
    mismatch["samples"][1]["candidate"]["digest"] = "wrong"
    mismatch_result = analyze_artifact(mismatch)
    assert mismatch_result["correctness_passed"] is False
    assert mismatch_result["correctness_failures"][0]["check"] == "stable_digest"


def test_analyzer_rejects_overlong_sandwich_and_any_run_resource_leak() -> None:
    artifact = deepcopy(_artifact())
    artifact["samples"][1]["window_seconds"] = 120.1
    artifact["samples"][2]["sequential_before"]["resources_leaked"] = True

    result = analyze_artifact(artifact)

    assert result["groups"][0]["exclusions"] == 0
    assert {failure["check"] for failure in result["correctness_failures"]} == {
        "contemporaneous",
        "resources",
    }


def test_material_range_requires_a_scoped_passing_range_group() -> None:
    artifact = deepcopy(_artifact())
    artifact["manifest"]["performance_scope"] = []

    result = analyze_artifact(artifact)

    assert result["material_range_speedup"] is False
    assert result["performance_passed"] is False


def test_analyzer_rejects_a_declared_scope_with_no_samples() -> None:
    artifact = deepcopy(_artifact())
    artifact["manifest"]["performance_scope"].append(["missing", "auto"])

    result = analyze_artifact(artifact)

    assert {tuple(failure.values()) for failure in result["performance_failures"]} >= {("missing", "auto")}


def test_analyzer_rejects_missing_correctness_groups_and_auto_selection_drift() -> None:
    artifact = deepcopy(_artifact())
    artifact["manifest"]["correctness_scope"].extend(
        [["missing", "partitioned"], ["large", "auto"]],
    )
    artifact["manifest"]["expected_auto_selections"] = {"large": "range"}
    for sample in artifact["samples"]:
        sample["mode"] = "auto"
        sample["candidate"]["selected_kind"] = "partitioned"

    result = analyze_artifact(artifact)

    failures = {(failure["cell"], failure["mode"], failure["check"]) for failure in result["correctness_failures"]}
    assert ("missing", "partitioned", "missing_group") in failures
    assert ("large", "auto", "auto_selection") in failures


def test_analyzer_requires_a_frozen_expectation_for_every_auto_cell() -> None:
    artifact = deepcopy(_artifact())
    artifact["manifest"]["correctness_scope"].append(["large", "auto"])

    result = analyze_artifact(artifact)

    assert {failure["check"] for failure in result["correctness_failures"]} >= {
        "missing_expected_selection",
    }


def test_analyzer_rejects_an_artifact_without_a_frozen_correctness_manifest() -> None:
    artifact = deepcopy(_artifact())
    del artifact["manifest"]["correctness_scope"]

    with pytest.raises(ValueError, match="correctness_scope"):
        analyze_artifact(artifact)


def test_individual_improvement_gate_requires_request_and_wall_improvement() -> None:
    artifact = deepcopy(_artifact())
    times = iter((0.7, 0.7, 0.8, 1.1, 1.1))
    for index, sample in enumerate(artifact["samples"]):
        sample["control_requests"] = 11
        sample["sequential_before"]["requests"] = 11
        sample["sequential_after"]["requests"] = 11
        if index:
            sample["candidate"]["wall_seconds"] = next(times)

    result = analyze_artifact(artifact)

    assert result["groups"][0]["median_request_ratio"] < 0.6
    assert result["groups"][0]["median_time_ratio"] < 0.85
    assert result["groups"][0]["performance_passed"] is False


def test_intermediate_individual_gate_uses_the_declared_wall_tolerance() -> None:
    artifact = deepcopy(_artifact())
    for sample in artifact["samples"]:
        sample["candidate"]["requests"] = 10
        sample["candidate"]["wall_seconds"] = 1.04

    result = analyze_artifact(artifact)

    assert result["groups"][0]["median_request_ratio"] == 1.0
    assert result["groups"][0]["p95_time_ratio"] == 1.04
    assert result["groups"][0]["performance_passed"] is True


def test_live_admission_fails_closed_without_the_required_matrix() -> None:
    artifact = deepcopy(_artifact())
    artifact["source"] = "live_read_only"
    artifact["manifest"]["live_matrix"] = {
        "complete": False,
        "covered_features": ["unfiltered", "total_hint_ignore"],
    }

    result = analyze_artifact(artifact)

    assert {tuple(failure.values()) for failure in result["performance_failures"]} >= {
        ("__live_matrix__", "all"),
    }
    assert result["performance_passed"] is False
