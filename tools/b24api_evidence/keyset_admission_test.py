"""Admission analyzer aggregation and fail-closed gates."""

# ruff: noqa: PLR2004

from __future__ import annotations
from copy import deepcopy

import pytest

from .keyset_admission import analyze_artifact, lower_median, nearest_rank_p95


def _run(*, digest: str = "same", requests: int = 10, seconds: float = 1.0) -> dict[str, object]:
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


def _artifact() -> dict[str, object]:
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
                "page_size": 50,
                "target_lanes": 20,
                "writable_limit": True,
                "control_requests": 10,
                "control_wall_seconds": 1.0,
                "sequential_before": before,
                "candidate": candidate,
                "sequential_after": after,
            },
        )
    return {
        "schema_version": 1,
        "manifest": {
            "performance_scope": [["large", "range"]],
            "correctness_scope": [["large", "range"]],
            "expected_auto_selections": {},
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


def test_analyzer_retains_mutation_exclusion_and_blocks_digest_mismatch() -> None:
    mutation = deepcopy(_artifact())
    mutation["samples"][1]["sequential_after"]["digest"] = "changed"  # type: ignore[index]
    mutation_result = analyze_artifact(mutation)  # type: ignore[arg-type]
    assert mutation_result["groups"][0]["exclusions"] == 1
    assert mutation_result["correctness_passed"] is True

    mutation_with_leak = deepcopy(mutation)
    mutation_with_leak["samples"][1]["sequential_after"]["resources_leaked"] = True  # type: ignore[index]
    leaked_result = analyze_artifact(mutation_with_leak)  # type: ignore[arg-type]
    assert {failure["check"] for failure in leaked_result["correctness_failures"]} == {"resources"}

    mismatch = deepcopy(_artifact())
    mismatch["samples"][1]["candidate"]["digest"] = "wrong"  # type: ignore[index]
    mismatch_result = analyze_artifact(mismatch)  # type: ignore[arg-type]
    assert mismatch_result["correctness_passed"] is False
    assert mismatch_result["correctness_failures"][0]["check"] == "stable_digest"


def test_analyzer_rejects_overlong_sandwich_and_any_run_resource_leak() -> None:
    artifact = deepcopy(_artifact())
    artifact["samples"][1]["window_seconds"] = 120.1  # type: ignore[index]
    artifact["samples"][2]["sequential_before"]["resources_leaked"] = True  # type: ignore[index]

    result = analyze_artifact(artifact)  # type: ignore[arg-type]

    assert result["groups"][0]["exclusions"] == 1
    assert {failure["check"] for failure in result["correctness_failures"]} == {"resources"}


def test_material_range_requires_a_scoped_passing_range_group() -> None:
    artifact = deepcopy(_artifact())
    artifact["manifest"]["performance_scope"] = []  # type: ignore[index]

    result = analyze_artifact(artifact)  # type: ignore[arg-type]

    assert result["material_range_speedup"] is False
    assert result["performance_passed"] is False


def test_analyzer_rejects_a_declared_scope_with_no_samples() -> None:
    artifact = deepcopy(_artifact())
    artifact["manifest"]["performance_scope"].append(["missing", "auto"])  # type: ignore[index]

    result = analyze_artifact(artifact)  # type: ignore[arg-type]

    assert {tuple(failure.values()) for failure in result["performance_failures"]} >= {("missing", "auto")}


def test_analyzer_rejects_missing_correctness_groups_and_auto_selection_drift() -> None:
    artifact = deepcopy(_artifact())
    artifact["manifest"]["correctness_scope"].extend(  # type: ignore[index]
        [["missing", "partitioned"], ["large", "auto"]],
    )
    artifact["manifest"]["expected_auto_selections"] = {"large": "range"}  # type: ignore[index]
    for sample in artifact["samples"]:  # type: ignore[index]
        sample["mode"] = "auto"
        sample["candidate"]["selected_kind"] = "partitioned"

    result = analyze_artifact(artifact)  # type: ignore[arg-type]

    failures = {(failure["cell"], failure["mode"], failure["check"]) for failure in result["correctness_failures"]}
    assert ("missing", "partitioned", "missing_group") in failures
    assert ("large", "auto", "auto_selection") in failures


def test_analyzer_requires_a_frozen_expectation_for_every_auto_cell() -> None:
    artifact = deepcopy(_artifact())
    artifact["manifest"]["correctness_scope"].append(["large", "auto"])  # type: ignore[index]

    result = analyze_artifact(artifact)  # type: ignore[arg-type]

    assert {failure["check"] for failure in result["correctness_failures"]} >= {
        "missing_expected_selection",
    }


def test_analyzer_rejects_an_artifact_without_a_frozen_correctness_manifest() -> None:
    artifact = deepcopy(_artifact())
    del artifact["manifest"]["correctness_scope"]  # type: ignore[index]

    with pytest.raises(ValueError, match="correctness_scope"):
        analyze_artifact(artifact)  # type: ignore[arg-type]


def test_individual_improvement_gate_requires_request_and_wall_improvement() -> None:
    artifact = deepcopy(_artifact())
    times = iter((0.7, 0.7, 0.8, 1.1, 1.1))
    for index, sample in enumerate(artifact["samples"]):  # type: ignore[index]
        sample["control_requests"] = 11
        sample["sequential_before"]["requests"] = 11
        sample["sequential_after"]["requests"] = 11
        if index:
            sample["candidate"]["wall_seconds"] = next(times)

    result = analyze_artifact(artifact)  # type: ignore[arg-type]

    assert result["groups"][0]["median_request_ratio"] < 0.6
    assert result["groups"][0]["median_time_ratio"] < 0.85
    assert result["groups"][0]["performance_passed"] is False


def test_live_admission_fails_closed_without_the_required_matrix() -> None:
    artifact = deepcopy(_artifact())
    artifact["source"] = "live_read_only"
    artifact["manifest"]["live_matrix"] = {  # type: ignore[index]
        "complete": False,
        "covered_features": ["unfiltered", "total_hint_ignore"],
    }

    result = analyze_artifact(artifact)  # type: ignore[arg-type]

    assert {tuple(failure.values()) for failure in result["performance_failures"]} >= {
        ("__live_matrix__", "all"),
    }
    assert result["performance_passed"] is False
