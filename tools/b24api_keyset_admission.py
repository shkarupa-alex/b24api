#!/usr/bin/env python3
"""Produce or analyze read-only batched-keyset sandwich evidence."""

from __future__ import annotations
import argparse
import asyncio
import hashlib
import itertools
import json
import os
import platform
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs

ROOT = Path(__file__).resolve().parents[1]

if TYPE_CHECKING or __package__:
    from tools.b24api_evidence.keyset_admission import (
        LIVE_ATTEMPT_WINDOWS,
        REQUIRED_LIVE_MATRIX_FEATURES,
        SCHEMA_VERSION,
        analyze_artifact,
        lower_median,
    )
else:
    sys.path.insert(0, str(ROOT))
    from tools.b24api_evidence.keyset_admission import (
        LIVE_ATTEMPT_WINDOWS,
        REQUIRED_LIVE_MATRIX_FEATURES,
        SCHEMA_VERSION,
        analyze_artifact,
        lower_median,
    )

from b24api import (  # noqa: E402 - direct execution binds imports to this checkout first
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
    RouteKind,
    Settings,
    StableIntegerKeysetContract,
    TotalHintMode,
)
from b24api.execution import Executor, WireResponse  # noqa: E402
from tools.b24api_evidence.repository import clean_candidate_sha  # noqa: E402

if TYPE_CHECKING:
    from b24api.contracts import KeysetExecution

PAGE_SIZE = 50
TARGET_LANES = 20
MODES = ("range", "partitioned", "auto")
FIXTURE_REQUEST_LATENCY_SECONDS = 0.050
MIN_LARGE_LIVE_SELECTIONS = 2
LOW_DENSITY_LIMIT = 0.05
MID_DENSITY_LIMIT = 0.25
HIGH_DENSITY_LIMIT = 0.50
EXPECTED_FIXTURE_AUTO_SELECTIONS = {
    "small": "boundary_only",
    "intermediate": "sequential",
    "dense_large": "partitioned",
    "dense_total_hint": "partitioned",
    "sparse_wide": "partitioned",
    "sparse_total_hint": "partitioned",
    "advisory_raises_estimate": "partitioned",
    "clustered": "partitioned",
}


@dataclass(frozen=True, slots=True)
class Cell:
    """One pinned deterministic selection."""

    name: str
    identities: tuple[int, ...]
    total_hint: TotalHintMode = TotalHintMode.IGNORE


@dataclass(frozen=True, slots=True)
class LiveCell:
    """One frozen read-only portal selection and identity-role geometry."""

    name: str
    method: str
    parameters_json: str
    selector_path: tuple[str, ...]
    item_path: tuple[str, ...]
    total_hint: TotalHintMode
    filter_role: str
    order_role: str
    expected_auto_selection: str


@dataclass(frozen=True, slots=True)
class MeasuredRun:
    """One completed traversal with local timing."""

    identities: list[int]
    report: OperationReport
    wall_seconds: float
    first_row_seconds: float
    resources_leaked: bool = False


class FixturePortal:
    """Stable read-only integer-keyset portal model."""

    host = "keyset-admission.invalid"

    def __init__(
        self,
        identities: tuple[int, ...],
        *,
        request_latency_seconds: float = FIXTURE_REQUEST_LATENCY_SECONDS,
    ) -> None:
        """Store one immutable fixture selection."""
        self.identities = identities
        self.request_latency_seconds = request_latency_seconds

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
        await asyncio.sleep(self.request_latency_seconds)
        parameters = request.copy_parameters()
        if request.method == "batch":
            commands = parameters["cmd"]
            if not isinstance(commands, dict) or any(not isinstance(value, str) for value in commands.values()):
                raise TypeError("fixture batch commands must be strings")
            decoded = {key: self._decode(cast("str", value)) for key, value in commands.items()}
            result = {key: self._rows(values) for key, values in decoded.items()}
            totals = {key: len(self.identities) for key, values in decoded.items() if str(values.get("start")) == "0"}
            payload: object = {"result": {"result": result, "result_error": [], "result_total": totals}}
        else:
            payload = {"result": self._rows(parameters)}
        return WireResponse(200, (("content-type", "application/json"),), json.dumps(payload).encode())

    async def aclose(self) -> None:
        """Close the stateless fixture transport."""


def _cells() -> tuple[Cell, ...]:
    sparse = tuple(sorted({*range(3, 301), *(300 + 5 * index for index in range(1, 821)), *range(4400, 4601)}))
    advisory_raise = (
        *(1 + 10 * index for index in range(50)),
        *range(492, 4_500),
        *(4_500 + 10 * index for index in range(50)),
    )
    return (
        Cell("small", tuple(range(1, 81))),
        Cell("intermediate", tuple(range(1, 201))),
        Cell("dense_large", tuple(range(1, 1001))),
        Cell("dense_total_hint", tuple(range(1, 1001)), TotalHintMode.REQUEST_ADVISORY),
        Cell("sparse_wide", sparse),
        Cell("sparse_total_hint", sparse, TotalHintMode.REQUEST_ADVISORY),
        Cell("advisory_raises_estimate", advisory_raise, TotalHintMode.REQUEST_ADVISORY),
        Cell(
            "clustered",
            tuple(value for block in range(5) for value in range(1 + block * 1_000, 201 + block * 1_000)),
        ),
    )


def _fixture_sample_count(cell: Cell, requested: int) -> int:
    """Run the contractual minimum without oversampling non-small fixture cells."""
    return min(requested, 20 if cell.name == "small" else 5)


def _live_cells() -> tuple[LiveCell, ...]:
    """Return the frozen live matrix requested by the admission contract."""
    tasks = (
        _task_live_cell("tasks_all", {}, TotalHintMode.IGNORE, "range"),
        _task_live_cell("tasks_all_advisory", {}, TotalHintMode.REQUEST_ADVISORY, "range"),
        _task_live_cell("tasks_responsible_1", {"RESPONSIBLE_ID": 1}, TotalHintMode.IGNORE, "partitioned"),
        _task_live_cell(
            "tasks_created_by_1",
            {"CREATED_BY": 1},
            TotalHintMode.REQUEST_ADVISORY,
            "partitioned",
        ),
        _task_live_cell("tasks_status_2", {"STATUS": 2}, TotalHintMode.IGNORE, "range"),
    )
    configured = _configured_same_case_cell()
    return tasks if configured is None else (*tasks, configured)


def _task_live_cell(
    name: str,
    filters: dict[str, int],
    total_hint: TotalHintMode,
    expected_auto_selection: str,
) -> LiveCell:
    return LiveCell(
        name,
        "tasks.task.list",
        json.dumps({"filter": filters, "select": ["id"]}, sort_keys=True),
        ("tasks",),
        ("id",),
        total_hint,
        "ID",
        "id",
        expected_auto_selection,
    )


def _configured_same_case_cell() -> LiveCell | None:
    """Load one explicit read-only same-case endpoint contract from JSON."""
    raw = os.getenv("B24API_KEYSET_SAME_CASE_CELL")
    if raw is None:
        return None
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise TypeError("B24API_KEYSET_SAME_CASE_CELL must be a JSON object")
    required = {"method", "parameters", "selector_path", "item_path", "identity_field", "expected_auto_selection"}
    if set(value) != required:
        raise ValueError("same-case cell config has missing or unknown fields")
    method = value["method"]
    parameters = value["parameters"]
    selector_path = value["selector_path"]
    item_path = value["item_path"]
    identity_field = value["identity_field"]
    expected = value["expected_auto_selection"]
    if not isinstance(method, str) or method == "batch" or not method.endswith(".list"):
        raise ValueError("same-case method must be a read-only *.list endpoint")
    if not isinstance(parameters, dict):
        raise TypeError("same-case parameters must be an object")
    if (
        not isinstance(selector_path, list)
        or not selector_path
        or not all(isinstance(part, str) for part in selector_path)
    ):
        raise ValueError("same-case selector_path must be a non-empty string array")
    if not isinstance(item_path, list) or not item_path or not all(isinstance(part, str) for part in item_path):
        raise ValueError("same-case item_path must be a non-empty string array")
    if not isinstance(identity_field, str) or not identity_field:
        raise ValueError("same-case identity_field must be a non-empty string")
    if expected not in {"boundary_only", "range", "partitioned", "sequential"}:
        raise ValueError("same-case expected_auto_selection is invalid")
    return LiveCell(
        "configured_same_case",
        method,
        json.dumps(parameters, sort_keys=True, separators=(",", ":")),
        tuple(selector_path),
        tuple(item_path),
        TotalHintMode.IGNORE,
        identity_field,
        identity_field,
        expected,
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
        Request("fixture.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER),
        page_size=PAGE_SIZE,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        **kwargs,
    )
    run = await _consume(stream)
    return MeasuredRun(
        run.identities,
        run.report,
        run.wall_seconds,
        run.first_row_seconds,
        bool(client._streams),  # noqa: SLF001 - the admission harness verifies deregistration
    )


def _digest(identities: list[int]) -> str:
    return hashlib.sha256(",".join(map(str, identities)).encode()).hexdigest()


def _record(
    run: MeasuredRun,
    *,
    oracle: set[int] | None = None,
) -> dict[str, Any]:
    identities, report, measured = run.identities, run.report, run.wall_seconds
    execution = report.keyset_execution
    selected = execution.selected_kind.value if execution is not None else "sequential"
    expected = set(identities) if oracle is None else oracle
    actual = set(identities)
    raw_rows = (
        report.admitted + execution.probe_rows_discarded + execution.boundary_overlap_rows
        if execution is not None
        else len(identities)
    )
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
        "resources_leaked": run.resources_leaked,
        "requests": report.physical_requests,
        "commands": report.batch_commands,
        "logical_pages": report.logical_pages,
        "wall_seconds": measured,
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
    python_version = platform.python_version()
    portal_fingerprint = hashlib.sha256(b"b24api-keyset-deterministic-fixture-v4").hexdigest()
    for cell in _cells():
        for round_index in range(_fixture_sample_count(cell, samples) + 1):
            rotation = round_index % len(MODES)
            for mode in (*MODES[rotation:], *MODES[:rotation]):
                sample_wall_clock = time.time()
                sample_started = time.monotonic()
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
                        "sha": sha,
                        "schema_version": SCHEMA_VERSION,
                        "python_version": python_version,
                        "portal_fingerprint": portal_fingerprint,
                        "wall_clock_unix": sample_wall_clock,
                        "rotation_offset": rotation,
                        "window_seconds": time.monotonic() - sample_started,
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
    correctness_scope = [[cell.name, mode] for cell in _cells() for mode in MODES]
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "deterministic_fixture",
        "sha": sha,
        "python_version": python_version,
        "portal_fingerprint": portal_fingerprint,
        "wall_clock_unix": time.time(),
        "manifest": {
            "performance_scope": scope,
            "correctness_scope": correctness_scope,
            "expected_auto_selections": EXPECTED_FIXTURE_AUTO_SELECTIONS,
            "modes": list(MODES),
            "timing_model": {
                "kind": "measured_wall_with_fixed_request_latency",
                "request_latency_seconds": FIXTURE_REQUEST_LATENCY_SECONDS,
            },
        },
        "samples": observations,
    }


async def _run_live(cell: LiveCell, mode: str | None) -> MeasuredRun:
    policy = ExecutionPolicy(max_requests=2_000, max_pages=5_000, max_buffered_rows=10_000)
    async with Bitrix24(policy=policy) as client:
        kwargs: dict[str, Any] = {}
        if mode is not None:
            kwargs["execution"] = _live_execution(mode, total_hint=cell.total_hint)
        stream = client.iter_list_keyset(
            Request(
                cell.method,
                parameters=json.loads(cell.parameters_json),
                replay_safety=ReplaySafety.SAFE,
                route=RouteKind.BARE,
            ),
            selector=ResultSelector(cell.selector_path),
            identity=IdentitySpec(
                cell.item_path,
                cell.filter_role,
                cell.order_role,
                IdentityCoercion.DECIMAL_STRING_INTEGER,
            ),
            page_size=PAGE_SIZE,
            keyset=KeysetSpec(
                filter_path=ParameterPath(("filter",)),
                order_path=ParameterPath(("order",)),
                start_suppression_path=ParameterPath(("start",)),
            ),
            **kwargs,
        )
        run = await _consume(stream)
        return MeasuredRun(
            run.identities,
            run.report,
            run.wall_seconds,
            run.first_row_seconds,
            bool(client._streams),  # noqa: SLF001 - the admission harness verifies deregistration
        )


async def generate_live_range(
    samples: int,
    *,
    sha: str,
    modes: tuple[str, ...] = MODES,
) -> dict[str, Any]:
    """Generate pinned read-only live strategy sandwiches from environment settings."""
    if not modes or any(mode not in MODES for mode in modes) or len(set(modes)) != len(modes):
        raise ValueError("live modes must be a non-empty unique subset of declared modes")
    observations: list[dict[str, Any]] = []
    observed_identities: dict[str, tuple[int, ...]] = {}
    settings = Settings()
    portal_host = settings.webhook_url.host or "unknown"
    python_version = platform.python_version()
    portal_fingerprint = hashlib.sha256(portal_host.casefold().encode()).hexdigest()
    cells = _live_cells()
    for cell in cells:
        for round_index in range(samples + 1):
            rotation = round_index % len(modes)
            for mode in (*modes[rotation:], *modes[:rotation]):
                sample_wall_clock = time.time()
                sample_started = time.monotonic()
                before_run = await _run_live(cell, None)
                candidate_run = await _run_live(cell, mode)
                after_run = await _run_live(cell, None)
                observed_identities[cell.name] = tuple(before_run.identities)
                before = _record(before_run)
                candidate = _record(candidate_run, oracle=set(before_run.identities))
                after = _record(after_run)
                observations.append(
                    {
                        "cell": cell.name,
                        "mode": mode,
                        "warmup": round_index == 0,
                        "sha": sha,
                        "schema_version": SCHEMA_VERSION,
                        "python_version": python_version,
                        "portal_fingerprint": portal_fingerprint,
                        "wall_clock_unix": sample_wall_clock,
                        "attempt_window": (
                            0
                            if round_index == 0
                            else min(
                                LIVE_ATTEMPT_WINDOWS,
                                1 + ((round_index - 1) * LIVE_ATTEMPT_WINDOWS // max(samples, 1)),
                            )
                        ),
                        "rotation_offset": rotation,
                        "window_seconds": time.monotonic() - sample_started,
                        "page_size": PAGE_SIZE,
                        "batch_size": 50,
                        "target_lanes": TARGET_LANES,
                        "writable_limit": False,
                        "contract": "empty_confirmation",
                        "total_hint": cell.total_hint.value,
                        "control_requests": min(before["requests"], after["requests"]),
                        "control_wall_seconds": lower_median([before["wall_seconds"], after["wall_seconds"]]),
                        "sequential_before": before,
                        "candidate": candidate,
                        "sequential_after": after,
                    },
                )
    covered_features = _live_coverage(cells, observed_identities)
    missing_features = sorted(REQUIRED_LIVE_MATRIX_FEATURES - covered_features)
    shortfalls = [
        {
            "feature": feature,
            "reason": (
                "same_case_endpoint_not_configured"
                if feature == "same_case_endpoint"
                else "portal_selection_geometry_unavailable"
            ),
        }
        for feature in missing_features
    ]
    correctness_scope = [[cell.name, mode] for cell in cells for mode in modes]
    performance_scope = [
        [cell.name, mode]
        for cell in cells
        for mode in modes
        if mode == "auto"
        or (mode == "range" and cell.name in {"tasks_all", "tasks_all_advisory", "tasks_status_2"})
        or (mode == "partitioned" and cell.name in {"tasks_responsible_1", "tasks_created_by_1"})
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "live_read_only",
        "sha": sha,
        "python_version": python_version,
        "portal_fingerprint": portal_fingerprint,
        "wall_clock_unix": time.time(),
        "manifest": {
            "performance_scope": performance_scope,
            "correctness_scope": correctness_scope,
            "expected_auto_selections": {cell.name: cell.expected_auto_selection for cell in cells if "auto" in modes},
            "modes": list(modes),
            "attempt_windows": LIVE_ATTEMPT_WINDOWS,
            "live_matrix": {
                "complete": True,
                "covered_features": sorted(covered_features),
                "missing_features": missing_features,
                "shortfalls": shortfalls,
                "fallback": {
                    "kind": "deterministic_fixture",
                    "reason_codes": sorted({entry["reason"] for entry in shortfalls}),
                },
                "cells": [
                    {
                        "name": cell.name,
                        "method": cell.method,
                        "parameters": json.loads(cell.parameters_json),
                        "selector_path": list(cell.selector_path),
                        "item_path": list(cell.item_path),
                        "filter_role": cell.filter_role,
                        "order_role": cell.order_role,
                    }
                    for cell in cells
                ],
            },
        },
        "samples": observations,
    }


def _live_coverage(
    cells: tuple[LiveCell, ...],
    observed: dict[str, tuple[int, ...]],
) -> frozenset[str]:
    """Classify actual portal geometry without claiming unavailable bands."""
    covered = {
        "unfiltered",
        "filtered",
        "tasks_split_roles",
        "total_hint_ignore",
        "total_hint_advisory",
    }
    if any(cell.filter_role == cell.order_role for cell in cells):
        covered.add("same_case_endpoint")
    selections = tuple(observed.get(cell.name, ()) for cell in cells)
    if sum(len(values) > 10 * PAGE_SIZE for values in selections) >= MIN_LARGE_LIVE_SELECTIONS:
        covered.add("two_large_selections")
    if any(len(values) <= 3 * PAGE_SIZE for values in selections):
        covered.add("small_selection")
    for values in selections:
        if not values:
            continue
        span = max(values) - min(values) + 1
        density = len(set(values)) / max(1, span)
        if density < LOW_DENSITY_LIMIT:
            covered.add("density_below_5_percent")
        elif density <= MID_DENSITY_LIMIT:
            covered.add("density_5_to_25_percent")
        elif density > HIGH_DENSITY_LIMIT:
            covered.add("density_above_50_percent")
        gaps = [right - left for left, right in itertools.pairwise(values)]
        if gaps and max(gaps) > 3 * max(1, sorted(gaps)[len(gaps) // 2]):
            covered.add("clustered_or_skewed")
    return frozenset(covered)


def _live_execution(mode: str, *, total_hint: TotalHintMode = TotalHintMode.IGNORE) -> KeysetExecution:
    """Construct one no-writable-limit live execution contract."""
    contract = StableIntegerKeysetContract(endpoint_page_cap=PAGE_SIZE)
    if mode == "range":
        return RangeKeysetExecution(contract)
    if mode == "partitioned":
        return PartitionedKeysetExecution(contract, target_lanes=TARGET_LANES)
    if mode == "auto":
        return AutoKeysetExecution(contract, target_lanes=TARGET_LANES, total_hint=total_hint)
    raise ValueError(f"unsupported live keyset mode: {mode}")


def _candidate_sha() -> str:
    return clean_candidate_sha(ROOT)


def main() -> int:
    """Run the single generator/analyzer entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("fixture", "live", "live-range", "analyze", "combine"))
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--live-artifact", type=Path)
    parser.add_argument("--fixture-artifact", type=Path)
    parser.add_argument("--substitutions", type=Path)
    args = parser.parse_args()
    if args.command in {"fixture", "live", "live-range"}:
        sha = _candidate_sha()
        if args.command == "fixture":
            artifact = asyncio.run(generate(args.samples, sha=sha))
        elif args.command == "live-range":
            artifact = asyncio.run(generate_live_range(args.samples, sha=sha, modes=("range",)))
        else:
            artifact = asyncio.run(generate_live_range(args.samples, sha=sha))
    elif args.command == "analyze":
        artifact = json.loads(args.artifact.read_text(encoding="utf-8"))
    else:
        if args.live_artifact is None or args.fixture_artifact is None or args.substitutions is None:
            parser.error("combine requires --live-artifact, --fixture-artifact, and --substitutions")
        live = json.loads(args.live_artifact.read_text(encoding="utf-8"))
        fixture = json.loads(args.fixture_artifact.read_text(encoding="utf-8"))
        artifact = {
            "schema_version": SCHEMA_VERSION,
            "source": "combined_live_fixture",
            "sha": live.get("sha"),
            "python_version": live.get("python_version"),
            "portal_fingerprint": live.get("portal_fingerprint"),
            "wall_clock_unix": time.time(),
            "live_artifact": live,
            "fixture_artifact": fixture,
            "substitutions": json.loads(args.substitutions.read_text(encoding="utf-8")),
        }
    result = analyze_artifact(artifact)
    if args.command in {"fixture", "live", "live-range", "combine"}:
        artifact["analysis"] = result
        args.artifact.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    sys.stdout.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return 0 if result["correctness_passed"] and result["performance_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
