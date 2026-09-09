"""Admission analyzer aggregation and fail-closed gates."""

# ruff: noqa: PLR2004

from __future__ import annotations
from copy import deepcopy

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
        "manifest": {"performance_scope": [["large", "range"]]},
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

    mismatch = deepcopy(_artifact())
    mismatch["samples"][1]["candidate"]["digest"] = "wrong"  # type: ignore[index]
    mismatch_result = analyze_artifact(mismatch)  # type: ignore[arg-type]
    assert mismatch_result["correctness_passed"] is False
    assert mismatch_result["correctness_failures"][0]["check"] == "stable_digest"
