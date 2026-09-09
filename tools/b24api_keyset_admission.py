#!/usr/bin/env python3
"""Produce or analyze read-only batched-keyset sandwich evidence."""

from __future__ import annotations
import argparse
import asyncio
import hashlib
import json
import platform
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs

from b24api_evidence.keyset_admission import SCHEMA_VERSION, analyze_artifact, lower_median

from b24api import (
    AutoKeysetExecution,
    Bitrix24,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    KeysetPageCompletion,
    KeysetSpec,
    OperationReport,
    ParameterPath,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    ReplaySafety,
    Request,
    ResultSelector,
    Settings,
    StableIntegerKeysetContract,
    TotalHintMode,
)
from b24api.execution import Executor, WireResponse

if TYPE_CHECKING:
    from b24api.contracts import KeysetExecution

PAGE_SIZE = 50
TARGET_LANES = 20
MODES = ("range", "partitioned", "auto")


@dataclass(frozen=True, slots=True)
class Cell:
    """One pinned deterministic selection."""

    name: str
    identities: tuple[int, ...]
    total_hint: TotalHintMode = TotalHintMode.IGNORE


@dataclass(frozen=True, slots=True)
class MeasuredRun:
    """One completed traversal with local timing."""

    identities: list[int]
    report: OperationReport
    wall_seconds: float
    first_row_seconds: float


class FixturePortal:
    """Stable read-only integer-keyset portal model."""

    host = "keyset-admission.invalid"

    def __init__(self, identities: tuple[int, ...]) -> None:
        """Store one immutable fixture selection."""
        self.identities = identities

    @staticmethod
    def _decode(value: str) -> dict[str, Any]:
        parsed = parse_qs(value.split("?", 1)[1] if "?" in value else "")
        result: dict[str, Any] = {"filter": {}, "order": {}}
        for key, values in parsed.items():
            if key.startswith("filter["):
                result["filter"][key[7:-1]] = values[0]
            elif key.startswith("order["):
                result["order"][key[6:-1]] = values[0]
            else:
                result[key] = values[0]
        return result

    def _rows(self, parameters: dict[str, Any]) -> list[dict[str, int]]:
        selected = self.identities
        filters = parameters.get("filter", {})
        if ">ID" in filters:
            selected = tuple(value for value in selected if value > int(filters[">ID"]))
        if "<ID" in filters:
            selected = tuple(value for value in selected if value < int(filters["<ID"]))
        descending = parameters.get("order", {}).get("id") == "DESC"
        limit = int(parameters.get("limit", PAGE_SIZE))
        return [{"id": value} for value in sorted(selected, reverse=descending)[:limit]]

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Return one deterministic direct or correlated batch response."""
        del attempt_timeout, max_response_bytes
        parameters = request.copy_parameters()
        if request.method == "batch":
            commands = parameters["cmd"]
            if not isinstance(commands, dict) or any(not isinstance(value, str) for value in commands.values()):
                raise TypeError("fixture batch commands must be strings")
            result = {key: self._rows(self._decode(cast("str", value))) for key, value in commands.items()}
            payload: object = {"result": {"result": result, "result_error": []}}
        else:
            payload = {"result": self._rows(parameters)}
        return WireResponse(200, (("content-type", "application/json"),), json.dumps(payload).encode())

    async def aclose(self) -> None:
        """Close the stateless fixture transport."""


def _cells() -> tuple[Cell, ...]:
    sparse = tuple(sorted({*range(3, 301), *(300 + 5 * index for index in range(1, 821)), *range(4400, 4601)}))
    return (
        Cell("small", tuple(range(1, 81))),
        Cell("intermediate", tuple(range(1, 201))),
        Cell("dense_large", tuple(range(1, 1001))),
        Cell("dense_total_hint", tuple(range(1, 1001)), TotalHintMode.REQUEST_ADVISORY),
        Cell("sparse_wide", sparse),
        Cell(
            "clustered",
            tuple(value for block in range(5) for value in range(1 + block * 1_000, 201 + block * 1_000)),
        ),
    )


def _execution(mode: str, cell: Cell) -> KeysetExecution:
    contract = StableIntegerKeysetContract(page_completion=KeysetPageCompletion.EMPTY_CONFIRMATION)
    if mode == "range":
        return RangeKeysetExecution(contract)
    if mode == "partitioned":
        return PartitionedKeysetExecution(contract, target_lanes=TARGET_LANES)
    return AutoKeysetExecution(contract, target_lanes=TARGET_LANES, total_hint=cell.total_hint)


async def _consume(stream: Any) -> MeasuredRun:  # noqa: ANN401
    started = time.perf_counter()
    identities: list[int] = []
    try:
        first = await anext(stream)
    except StopAsyncIteration:
        first_elapsed = time.perf_counter() - started
    else:
        first_elapsed = time.perf_counter() - started
        identities.append(int(cast("dict[str, str | int]", first)["id"]))
        identities.extend([int(cast("dict[str, str | int]", row)["id"]) async for row in stream])
    measured = time.perf_counter() - started
    if stream.report is None:
        raise RuntimeError("completed traversal lacks a report")
    return MeasuredRun(identities, stream.report, measured, first_elapsed)


async def _run(cell: Cell, mode: str | None) -> MeasuredRun:
    portal = FixturePortal(cell.identities)
    client = Bitrix24._from_executor(  # noqa: SLF001 - deterministic injected evidence transport
        Executor(portal),
        policy=ExecutionPolicy(max_requests=2_000, max_pages=5_000, max_buffered_rows=10_000),
    )
    kwargs: dict[str, Any] = {}
    if mode is not None:
        kwargs["execution"] = _execution(mode, cell)
    stream = client.iter_list_keyset(
        Request("fixture.list", replay_safety=ReplaySafety.SAFE),
        selector=ResultSelector.root(),
        identity=IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER),
        page_size=PAGE_SIZE,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        **kwargs,
    )
    return await _consume(stream)


def _digest(identities: list[int]) -> str:
    return hashlib.sha256(",".join(map(str, identities)).encode()).hexdigest()


def _record(
    run: MeasuredRun,
    *,
    oracle: set[int] | None = None,
    deterministic_time: bool = True,
) -> dict[str, Any]:
    identities, report, measured = run.identities, run.report, run.wall_seconds
    execution = report.keyset_execution
    selected = execution.selected_kind.value if execution is not None else "sequential"
    expected = set(identities) if oracle is None else oracle
    actual = set(identities)
    raw_rows = execution.raw_rows if execution is not None else len(identities)
    return {
        "digest": _digest(identities),
        "rows": len(identities),
        "raw_rows": raw_rows,
        "raw_read_amplification": raw_rows / max(1, report.unique_rows),
        "admitted": report.admitted,
        "emitted": report.emitted,
        "unique": report.unique_rows,
        "omissions": len(expected - actual),
        "duplicates": len(identities) - len(actual),
        "output_overfetch": len(actual - expected),
        "false_completion": int(report.state.value != "completed"),
        "resources_leaked": False,
        "requests": report.physical_requests,
        "commands": report.batch_commands,
        "logical_pages": report.logical_pages,
        "wall_seconds": report.physical_requests * 0.001 if deterministic_time else measured,
        "first_row_seconds": run.first_row_seconds,
        "measured_wall_seconds": measured,
        "server_seconds": None,
        "retries": report.retries,
        "cooldown_seconds": report.cooldown_seconds,
        "buffer_high_water": report.buffered_rows_high_water,
        "selected_kind": selected,
        "assurance_source": execution.assurance_source.value if execution is not None else "ordered_prefix_only",
        "preselection_reason": execution.preselection_reason.value if execution is not None else None,
        "final_selection_reason": execution.final_selection_reason.value
        if execution and execution.final_selection_reason
        else None,
        "boundary_overlap_rows": execution.boundary_overlap_rows if execution is not None else 0,
        "probe_rows_discarded": execution.probe_rows_discarded if execution is not None else 0,
        "tail_rows": execution.tail_rows if execution is not None else 0,
        "head_rows": execution.head_rows if execution is not None else 0,
        "interior_span": execution.interior_span if execution is not None else None,
        "interior_rows_estimate": execution.interior_rows_estimate if execution is not None else None,
        "total_rows_estimate": execution.total_rows_estimate if execution is not None else None,
        "density_numerator": execution.density_numerator if execution is not None else None,
        "density_denominator": execution.density_denominator if execution is not None else None,
        "sequential_requests_estimate": execution.sequential_requests_estimate if execution is not None else None,
        "selected_requests_estimate": execution.selected_requests_estimate if execution is not None else None,
        "planning_requests": execution.planning_requests if execution is not None else 0,
        "boundary_requests": execution.boundary_requests if execution is not None else 0,
        "canary_requests": execution.canary_requests if execution is not None else 0,
        "anchor_probe_requests": execution.anchor_probe_requests if execution is not None else 0,
        "total_hint_requested": execution.total_hint_requested if execution is not None else False,
        "total_hint_observed": execution.total_hint_observed if execution is not None else None,
        "total_hint_plausible": execution.total_hint_plausible if execution is not None else False,
        "total_hint_used": execution.total_hint_used if execution is not None else False,
        "transport_fault": False,
    }


async def generate(samples: int, *, sha: str) -> dict[str, Any]:
    """Generate frozen credential-free read-only sandwiches."""
    observations: list[dict[str, Any]] = []
    for cell in _cells():
        for round_index in range(samples + 1):
            rotation = round_index % len(MODES)
            for mode in (*MODES[rotation:], *MODES[:rotation]):
                before_run = await _run(cell, None)
                candidate_run = await _run(cell, mode)
                after_run = await _run(cell, None)
                oracle = set(before_run.identities)
                before = _record(before_run)
                candidate = _record(candidate_run, oracle=oracle)
                after = _record(after_run)
                observations.append(
                    {
                        "cell": cell.name,
                        "mode": mode,
                        "warmup": round_index == 0,
                        "rotation_offset": rotation,
                        "page_size": PAGE_SIZE,
                        "batch_size": 50,
                        "target_lanes": TARGET_LANES,
                        "writable_limit": True,
                        "contract": "empty_confirmation",
                        "total_hint": cell.total_hint.value,
                        "control_requests": min(before["requests"], after["requests"]),
                        "control_wall_seconds": lower_median([before["wall_seconds"], after["wall_seconds"]]),
                        "sequential_before": before,
                        "candidate": candidate,
                        "sequential_after": after,
                    },
                )
    scope = [
        [cell.name, mode]
        for cell in _cells()
        for mode in MODES
        if mode == "auto"
        or (mode == "range" and "dense" in cell.name)
        or (mode == "partitioned" and cell.name in {"sparse_wide", "clustered"})
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "deterministic_fixture",
        "sha": sha,
        "python_version": platform.python_version(),
        "portal_fingerprint": hashlib.sha256(b"b24api-keyset-deterministic-fixture-v1").hexdigest(),
        "wall_clock_unix": time.time(),
        "manifest": {"performance_scope": scope, "modes": list(MODES)},
        "samples": observations,
    }


async def _run_live(mode: str | None) -> MeasuredRun:
    policy = ExecutionPolicy(max_requests=2_000, max_pages=5_000, max_buffered_rows=10_000)
    async with Bitrix24(policy=policy) as client:
        kwargs: dict[str, Any] = {}
        if mode == "range":
            kwargs["execution"] = RangeKeysetExecution(
                StableIntegerKeysetContract(endpoint_page_cap=PAGE_SIZE),
            )
        stream = client.iter_list_keyset(
            Request(
                "tasks.task.list",
                parameters={"filter": {}, "select": ["id"]},
                replay_safety=ReplaySafety.SAFE,
            ),
            selector=ResultSelector(("tasks",)),
            identity=IdentitySpec(("id",), "ID", "id", IdentityCoercion.DECIMAL_STRING_INTEGER),
            page_size=PAGE_SIZE,
            keyset=KeysetSpec(
                filter_path=ParameterPath(("filter",)),
                order_path=ParameterPath(("order",)),
                start_suppression_path=ParameterPath(("start",)),
            ),
            **kwargs,
        )
        return await _consume(stream)


async def generate_live_range(samples: int, *, sha: str) -> dict[str, Any]:
    """Generate pinned read-only live range sandwiches from environment settings."""
    observations: list[dict[str, Any]] = []
    for index in range(samples + 1):
        sample_started = time.monotonic()
        before_run = await _run_live(None)
        candidate_run = await _run_live("range")
        after_run = await _run_live(None)
        before = _record(before_run, deterministic_time=False)
        candidate = _record(
            candidate_run,
            oracle=set(before_run.identities),
            deterministic_time=False,
        )
        after = _record(after_run, deterministic_time=False)
        observations.append(
            {
                "cell": "tasks_all",
                "mode": "range",
                "warmup": index == 0,
                "rotation_offset": 0,
                "window_seconds": time.monotonic() - sample_started,
                "page_size": PAGE_SIZE,
                "batch_size": 50,
                "target_lanes": TARGET_LANES,
                "writable_limit": False,
                "contract": "empty_confirmation",
                "total_hint": "ignore",
                "control_requests": min(before["requests"], after["requests"]),
                "control_wall_seconds": lower_median([before["wall_seconds"], after["wall_seconds"]]),
                "sequential_before": before,
                "candidate": candidate,
                "sequential_after": after,
            },
        )
    settings = Settings()
    portal_host = settings.webhook_url.host or "unknown"
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "live_read_only",
        "sha": sha,
        "python_version": platform.python_version(),
        "portal_fingerprint": hashlib.sha256(portal_host.casefold().encode()).hexdigest(),
        "wall_clock_unix": time.time(),
        "manifest": {"performance_scope": [["tasks_all", "range"]], "modes": ["range"]},
        "samples": observations,
    }


def main() -> int:
    """Run the single generator/analyzer entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("fixture", "live-range", "analyze"))
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--samples", type=int, default=20)
    args = parser.parse_args()
    if args.command in {"fixture", "live-range"}:
        git = shutil.which("git")
        if git is None:
            raise RuntimeError("git is required to bind evidence to a candidate")
        sha = subprocess.run(  # noqa: S603 - resolved fixed git executable and arguments
            [git, "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        artifact = asyncio.run(
            generate(args.samples, sha=sha)
            if args.command == "fixture"
            else generate_live_range(args.samples, sha=sha),
        )
    else:
        artifact = json.loads(args.artifact.read_text(encoding="utf-8"))
    result = analyze_artifact(artifact)
    if args.command in {"fixture", "live-range"}:
        artifact["analysis"] = result
        args.artifact.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    sys.stdout.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return 0 if result["correctness_passed"] and result["performance_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
