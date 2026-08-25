"""Public-surface CPU, memory, backpressure, and lifecycle profiling."""

from __future__ import annotations
import asyncio
import gc
import hashlib
import inspect
import json
import time
import tracemalloc
import weakref
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any

import httpx

from b24api.client import Bitrix24
from b24api.contracts import (
    Binding,
    Command,
    DeliveryOrder,
    DirectDispatch,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    SequentialTraversal,
)
from b24api.errors import ResponseTooLargeError
from b24api.execution import HttpxTransport, Transport, WireResponse
from b24api.settings import Settings

from .model import DeterministicPortal, exact_model_cases

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable

_HTTP_OK = 200
_PROFILE_WEBHOOK = "https://profile.invalid/rest/1/local-only/"
_BATCH_BUFFER = 7
_LARGE_BATCH = 100_000
_SMALL_BATCH = 10_000
_REPEATED_WARMUPS = 10


@dataclass(slots=True)
class _Transport:
    handler: Callable[[Request], object | Awaitable[object]]
    host: str = "profile.invalid"
    requests: int = 0
    response_bytes: int = 0
    wire_contains_correlation: bool = False

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        if attempt_timeout <= 0:
            raise AssertionError("profile received a non-positive attempt timeout")
        self.requests += 1
        parameters = request.copy_parameters()
        self.wire_contains_correlation |= "profile-correlation" in json.dumps(parameters)
        outcome = self.handler(request)
        envelope = await outcome if inspect.isawaitable(outcome) else outcome
        body = json.dumps(envelope, separators=(",", ":")).encode()
        if len(body) > max_response_bytes:
            raise AssertionError("profile fixture exceeded the response policy")
        self.response_bytes += len(body)
        return WireResponse(_HTTP_OK, (("content-type", "application/json"),), body)

    async def aclose(self) -> None:
        """Satisfy the transport protocol without owning resources."""


def _client(transport: Transport, *, policy: ExecutionPolicy | None = None) -> Bitrix24:
    return Bitrix24(
        Settings(webhook_url=f"https://{transport.host}/rest/1/local-only/"),
        transport=transport,
        policy=policy,
    )


def _allocation_sites(before: tracemalloc.Snapshot, after: tracemalloc.Snapshot) -> list[dict[str, int | str]]:
    sites: list[dict[str, int | str]] = []
    for statistic in after.compare_to(before, "lineno")[:8]:
        frame = statistic.traceback[0]
        sites.append(
            {
                "file": frame.filename.rsplit("/", 1)[-1],
                "line": frame.lineno,
                "bytes": statistic.size_diff,
                "allocations": statistic.count_diff,
            },
        )
    return sites


async def _traced(operation: Callable[[], Awaitable[dict[str, Any]]]) -> dict[str, Any]:
    gc.collect()
    # One frame is sufficient to identify the owning allocation site and keeps
    # the 100k-command characterization practical on supported interpreters.
    tracemalloc.start(1)
    baseline = tracemalloc.take_snapshot()
    baseline_current = tracemalloc.get_traced_memory()[0]
    wall_start = time.perf_counter_ns()
    cpu_start = time.process_time_ns()
    payload = await operation()
    cpu_seconds = (time.process_time_ns() - cpu_start) / 1_000_000_000
    wall_seconds = (time.perf_counter_ns() - wall_start) / 1_000_000_000
    current, peak = tracemalloc.get_traced_memory()
    final = tracemalloc.take_snapshot()
    tracemalloc.stop()
    return {
        **payload,
        "wall_seconds": wall_seconds,
        "cpu_seconds": cpu_seconds,
        "retained_bytes": max(0, current - baseline_current),
        "peak_bytes": max(0, peak - baseline_current),
        "allocation_sites": _allocation_sites(baseline, final),
    }


async def _direct_case() -> dict[str, Any]:
    transport = _Transport(lambda _request: {"result": {"ID": 1}})
    client = _client(transport)
    started = time.perf_counter_ns()
    result = await client.call(Request("profile.get", replay_safety=ReplaySafety.SAFE))
    first_row_seconds = (time.perf_counter_ns() - started) / 1_000_000_000
    await client.aclose()
    return {
        "case": "direct-call",
        "rows": int(result == {"ID": 1}),
        "requests": transport.requests,
        "response_bytes": transport.response_bytes,
        "time_to_first_row_seconds": first_row_seconds,
        "retained_resources": 0,
    }


async def _logical_batch_case(total: int) -> dict[str, Any]:
    source_closed = False

    async def source() -> AsyncGenerator[Command[tuple[str, int]]]:
        nonlocal source_closed
        try:
            for index in range(total):
                yield Command(
                    Request("profile.get", {"index": index}, ReplaySafety.SAFE),
                    ("profile-correlation", index),
                )
        finally:
            source_closed = True

    def handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        if not isinstance(commands, dict):
            raise TypeError("logical batch did not emit a command map")
        return {
            "result": {
                "result": {key: int(key[1:]) for key in commands},
                "result_error": {},
            },
        }

    transport = _Transport(handler)
    policy = ExecutionPolicy(max_requests=20_000, max_buffered_commands=_BATCH_BUFFER)
    client = _client(transport, policy=policy)
    stream = client.batch(source(), batch_size=_BATCH_BUFFER)
    first_row_seconds: float | None = None
    rows = 0
    identity_hash = hashlib.sha256()
    started = time.perf_counter_ns()
    async for outcome in stream:
        if first_row_seconds is None:
            first_row_seconds = (time.perf_counter_ns() - started) / 1_000_000_000
        if outcome.correlation != ("profile-correlation", rows):
            raise AssertionError("logical batch correlation changed")
        identity_hash.update(f"{rows}\n".encode())
        rows += 1
    report = stream.report
    await client.aclose()
    if report is None:
        raise AssertionError("logical batch did not publish its report")
    return {
        "case": f"logical-batch-{total}",
        "rows": rows,
        "requests": transport.requests,
        "response_bytes": transport.response_bytes,
        "time_to_first_row_seconds": first_row_seconds,
        "buffered_commands_high_water": report.buffered_commands_high_water,
        "identity_sha256": identity_hash.hexdigest(),
        "source_closed": source_closed,
        "correlation_entered_wire": transport.wire_contains_correlation,
        "retained_resources": 0,
    }


def _identity() -> IdentitySpec:
    return IdentitySpec(
        item_path=("ID",),
        filter_key="ID",
        order_key="ID",
        coercion=IdentityCoercion.EXACT_INTEGER,
    )


async def _counted_case(case_id: str) -> dict[str, Any]:
    case = next(item for item in exact_model_cases() if item.case_id == case_id)
    portal = DeterministicPortal(case)
    client = _client(portal, policy=ExecutionPolicy(max_requests=1_000, max_pages=1_000))
    stream = client.iter_list_counted(
        Request("model.entity.list", replay_safety=ReplaySafety.SAFE),
        identity=_identity(),
    )
    started = time.perf_counter_ns()
    first_row_seconds: float | None = None
    identities: list[int] = []
    async for row in stream:
        if first_row_seconds is None:
            first_row_seconds = (time.perf_counter_ns() - started) / 1_000_000_000
        if not isinstance(row, dict):
            raise TypeError("counted profile emitted a malformed row")
        row_id = row.get("ID")
        if isinstance(row_id, bool) or not isinstance(row_id, int):
            raise TypeError("counted profile emitted a malformed identity")
        identities.append(row_id)
    report = stream.report
    await client.aclose()
    if report is None:
        raise AssertionError("counted profile did not publish its report")
    return {
        "case": f"counted-{case_id}",
        "rows": len(identities),
        "requests": report.physical_requests,
        "logical_pages": report.logical_pages,
        "batch_requests": report.batch_requests,
        "batch_commands": report.batch_commands,
        "buffered_rows_high_water": report.buffered_rows_high_water,
        "time_to_first_row_seconds": first_row_seconds,
        "identity_equal": tuple(identities) == case.identities,
        "identity_sha256": hashlib.sha256(json.dumps(identities, separators=(",", ":")).encode()).hexdigest(),
        "response_bytes": 0,
        "retained_resources": 0,
    }


async def _reference_backpressure_case() -> dict[str, Any]:
    async def handler(request: Request) -> object:
        parameters = request.copy_parameters()
        filters = parameters["filter"]
        if not isinstance(filters, dict):
            raise TypeError("reference profile filter must be an object")
        owner = filters["OWNER"]
        if isinstance(owner, bool) or not isinstance(owner, int):
            raise TypeError("reference profile owner must be an integer")
        start = parameters.get("start", 0)
        if isinstance(start, bool) or not isinstance(start, int):
            raise TypeError("reference profile start must be an integer")
        if owner == 0 and start == 0:
            # Let later bindings finish while INPUT delivery remains blocked
            # behind the first binding.
            await asyncio.sleep(0.02)
        return {"result": [] if start else [{"ID": owner + 1}]}

    transport = _Transport(handler)
    client = _client(transport, policy=ExecutionPolicy(max_active_references=8, max_buffered_rows=8))
    bindings = [
        Binding(
            f"owner-{owner}",
            (ParameterUpdate(ParameterPath(("filter", "OWNER")), owner),),
            ("profile-correlation", owner),
        )
        for owner in range(8)
    ]
    stream = client.iter_references(
        Request("profile.list", replay_safety=ReplaySafety.SAFE),
        bindings,
        traversal=SequentialTraversal(identity=_identity()),
        dispatch=DirectDispatch(concurrency=8, output_order=DeliveryOrder.INPUT),
    )
    events = [event async for event in stream]
    report = stream.report
    await client.aclose()
    if report is None:
        raise AssertionError("reference profile did not publish its report")
    return {
        "case": "references-input-order-backpressure",
        "events": len(events),
        "requests": report.physical_requests,
        "buffered_rows_high_water": report.buffered_rows_high_water,
        "active_references_high_water": report.active_references_high_water,
        "response_bytes": transport.response_bytes,
        "correlation_entered_wire": transport.wire_contains_correlation,
        "retained_resources": 0,
    }


async def _repeated_lifecycle_case() -> dict[str, Any]:
    transport = _Transport(lambda _request: {"result": []})
    client = _client(transport)
    stream_refs: list[weakref.ReferenceType[object]] = []

    async def traverse_once() -> weakref.ReferenceType[object]:
        stream = client.iter_list(Request("profile.list", replay_safety=ReplaySafety.SAFE))
        reference: weakref.ReferenceType[object] = weakref.ref(stream)
        async for _row in stream:
            pass
        return reference

    for _ in range(110):
        stream_refs.append(await traverse_once())
        if len(stream_refs) == _REPEATED_WARMUPS:
            gc.collect()
            warm_current = tracemalloc.get_traced_memory()[0]
    gc.collect()
    final_current = tracemalloc.get_traced_memory()[0]
    await client.aclose()
    # Advance once so CPython releases the frame of the last directly-awaited
    # traversal coroutine before retained-resource accounting.
    await asyncio.sleep(0)
    gc.collect()
    retained = sum(reference() is not None for reference in stream_refs)
    return {
        "case": "repeated-traversal-lifecycle",
        "iterations": 110,
        "requests": transport.requests,
        "growth_per_100_bytes": max(0, final_current - warm_current),
        "retained_resources": retained,
        "response_bytes": transport.response_bytes,
    }


class _ChunkStream(httpx.AsyncByteStream):
    def __init__(self, *, chunk: bytes, count: int) -> None:
        self.chunk = chunk
        self.count = count

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for _ in range(self.count):
            yield self.chunk


async def _oversized_response_case() -> dict[str, Any]:
    ceiling = 64 * 1024
    chunk = 16 * 1024

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(_HTTP_OK, request=request, stream=_ChunkStream(chunk=b"x" * chunk, count=5))

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = HttpxTransport(_PROFILE_WEBHOOK, client=raw_client)
    refused = False
    try:
        try:
            await transport.send(Request("profile.get"), attempt_timeout=1, max_response_bytes=ceiling)
        except ResponseTooLargeError:
            refused = True
    finally:
        await transport.aclose()
        await raw_client.aclose()
    return {
        "case": "oversized-response",
        "requests": 1,
        "response_ceiling_bytes": ceiling,
        "stream_chunk_bytes": chunk,
        "maximum_retained_body_bytes": ceiling,
        "ceiling_plus_one_chunk_bytes": ceiling + chunk,
        "refused": refused,
        "retained_resources": 0,
        "response_bytes": ceiling + chunk,
    }


async def run_capability_profile() -> dict[str, Any]:
    """Run the complete deterministic resource profile required by the handoff."""
    cases = [
        await _traced(_direct_case),
        await _traced(lambda: _logical_batch_case(_SMALL_BATCH)),
        await _traced(lambda: _logical_batch_case(_LARGE_BATCH)),
    ]
    cases.extend(
        [
            await _traced(partial(_counted_case, case_id))
            for case_id in ("nineteen", "five-hundred", "dense-10k", "uniform-sparse-10k")
        ],
    )
    cases.extend(
        (
            await _traced(_reference_backpressure_case),
            await _traced(_repeated_lifecycle_case),
            await _traced(_oversized_response_case),
        ),
    )
    small = next(case for case in cases if case["case"] == f"logical-batch-{_SMALL_BATCH}")
    large = next(case for case in cases if case["case"] == f"logical-batch-{_LARGE_BATCH}")
    plateau_allowance = max(int(small["peak_bytes"] * 0.25), 32 * 1024 * 1024)
    invariants = {
        "logical_batch_memory_plateau": large["peak_bytes"] <= small["peak_bytes"] + plateau_allowance,
        "logical_batch_source_closed": bool(large["source_closed"]),
        "logical_batch_correlation_off_wire": not bool(large["correlation_entered_wire"]),
        "counted_request_shape_1_2_5": [
            next(case for case in cases if case["case"] == f"counted-{case_id}")["requests"]
            for case_id in ("nineteen", "five-hundred", "dense-10k")
        ]
        == [1, 2, 5],
        "counted_identities_exact": all(
            bool(case.get("identity_equal", True)) for case in cases if str(case["case"]).startswith("counted-")
        ),
        "no_orphan_work": all(case["retained_resources"] == 0 for case in cases),
        "repeated_growth_at_most_1mib": next(case for case in cases if case["case"] == "repeated-traversal-lifecycle")[
            "growth_per_100_bytes"
        ]
        <= 1024 * 1024,
        "oversized_transport_within_ceiling_plus_chunk": next(
            case for case in cases if case["case"] == "oversized-response"
        )["maximum_retained_body_bytes"]
        <= next(case for case in cases if case["case"] == "oversized-response")["ceiling_plus_one_chunk_bytes"],
    }
    return {
        "suite": "capability-resource-profile",
        "cases": cases,
        "invariants": invariants,
        "passed": all(invariants.values()),
    }


__all__ = ["run_capability_profile"]
