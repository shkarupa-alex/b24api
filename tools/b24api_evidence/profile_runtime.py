# ruff: noqa: INP001
"""Reproducible local CPU, wall-time, and allocation profiling scenarios."""

from __future__ import annotations
import argparse
import asyncio
import contextlib
import hashlib
import importlib
import json
import math
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from harness.model import ModelCase, ModelRun, exact_model_cases, run_model_case

_PLANS = ("fixed_1x_batch", "counted_batch")
_DEFAULT_CASES = (
    "nineteen",
    "five-hundred",
    "dense-10k",
    "uniform-sparse-10k",
    "clustered-sparse-10k",
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=tuple(case.case_id for case in exact_model_cases()), action="append")
    parser.add_argument("--plan", choices=_PLANS, action="append")
    parser.add_argument("--samples", type=int, default=15)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument(
        "--memray-output",
        type=Path,
        help="track measured samples only; requires Memray and exactly one case/plan",
    )
    return parser


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _distribution(values: list[float]) -> dict[str, float]:
    return {
        "minimum": min(values),
        "median": statistics.median(values),
        "p95": _percentile(values, 0.95),
        "maximum": max(values),
    }


def _result(  # noqa: PLR0913 - explicit benchmark sample inputs
    case: ModelCase,
    *,
    plan: str,
    warmups: int,
    samples: int,
    wall_samples: list[float],
    cpu_samples: list[float],
    last_run: ModelRun,
) -> dict[str, Any]:
    return {
        "case_id": case.case_id,
        "distribution": case.distribution,
        "rows": len(case.identities),
        "plan": plan,
        "samples": samples,
        "warmups": warmups,
        "wall_seconds": _distribution(wall_samples),
        "cpu_seconds": _distribution(cpu_samples),
        "requests": last_run.requests,
        "logical_pages": last_run.logical_pages,
        "batch_requests": last_run.batch_requests,
        "batch_commands": last_run.batch_commands,
        "buffered_rows_high_water": last_run.buffered_rows_high_water,
        "identity_sha256": hashlib.sha256(
            json.dumps(last_run.identities, separators=(",", ":")).encode(),
        ).hexdigest(),
    }


async def _measure_pair(
    case: ModelCase,
    *,
    plans: tuple[str, ...],
    warmups: int,
    samples: int,
    memray_output: Path | None = None,
) -> tuple[dict[str, Any], ...]:
    for index in range(warmups):
        order = plans if index % 2 == 0 else tuple(reversed(plans))
        for plan in order:
            await run_model_case(case, plan_name=plan)
    wall_samples: dict[str, list[float]] = {plan: [] for plan in plans}
    cpu_samples: dict[str, list[float]] = {plan: [] for plan in plans}
    last_runs: dict[str, ModelRun] = {}
    tracker: Any = contextlib.nullcontext()
    if memray_output is not None:
        memray = importlib.import_module("memray")
        tracker = memray.Tracker(str(memray_output), trace_python_allocators=True)
    with tracker:
        for index in range(samples):
            order = plans if index % 2 == 0 else tuple(reversed(plans))
            for plan in order:
                wall_start = time.perf_counter_ns()
                cpu_start = time.process_time_ns()
                last_runs[plan] = await run_model_case(case, plan_name=plan)
                cpu_samples[plan].append((time.process_time_ns() - cpu_start) / 1_000_000_000)
                wall_samples[plan].append((time.perf_counter_ns() - wall_start) / 1_000_000_000)
    return tuple(
        _result(
            case,
            plan=plan,
            warmups=warmups,
            samples=samples,
            wall_samples=wall_samples[plan],
            cpu_samples=cpu_samples[plan],
            last_run=last_runs[plan],
        )
        for plan in plans
    )


def _main() -> None:
    args = _parser().parse_args()
    if args.samples <= 0 or args.warmups < 0:
        raise SystemExit("samples must be positive and warmups cannot be negative")
    selected = set(args.case or _DEFAULT_CASES)
    cases = tuple(case for case in exact_model_cases() if case.case_id in selected)
    plans = tuple(dict.fromkeys(args.plan or _PLANS))
    if args.memray_output is not None and (len(cases) != 1 or len(plans) != 1):
        raise SystemExit("--memray-output requires exactly one --case and one --plan")
    if args.memray_output is not None and args.memray_output.exists():
        raise SystemExit(f"Memray output already exists: {args.memray_output}")
    git = shutil.which("git")
    if git is None:
        raise SystemExit("git is required to bind profiling output to a candidate")
    candidate_sha = subprocess.run(  # noqa: S603 - resolved git binary, fixed arguments
        (git, "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    results: list[dict[str, Any]] = []
    for case in cases:
        results.extend(
            asyncio.run(
                _measure_pair(
                    case,
                    plans=plans,
                    warmups=args.warmups,
                    samples=args.samples,
                    memray_output=args.memray_output,
                ),
            ),
        )
    sys.stdout.write(
        json.dumps(
            {
                "schema_version": 1,
                "candidate_sha": candidate_sha,
                "python": sys.version.split()[0],
                "results": results,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )


if __name__ == "__main__":
    _main()
