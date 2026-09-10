"""Deterministic correctness tests for opt-in fast integer keysets."""

# ruff: noqa: ANN202, D102, D107, FBT003, PLR2004, SLF001

from __future__ import annotations
import asyncio
import json
import random
from dataclasses import replace
from typing import TYPE_CHECKING
from urllib.parse import parse_qs

import pytest

from b24api import (
    AutoKeysetExecution,
    Bitrix24,
    ClosureWitness,
    ConsistencyPolicy,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    KeysetAssuranceSource,
    KeysetExecutionKind,
    KeysetPageCompletion,
    KeysetPhase,
    KeysetSelectionReason,
    KeysetSpec,
    PageOutcome,
    PageRejectionCode,
    ParameterPath,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    Request,
    ResultSelector,
    StableIntegerKeysetContract,
    TerminalState,
    TotalHintMode,
    TraceClass,
)
from b24api.cli import _report_json
from b24api.contracts.policy import IdentityRequirement, OrderSemantics, TotalSemantics
from b24api.contracts.report import PageDispatch
from b24api.errors import CapabilityError, IncompleteTraversalError
from b24api.execution import Executor, WireResponse
from b24api.traversal import keyset_scheduler
from b24api.traversal.keyset_auto import AnchorFacts, BoundaryFacts, Preselected, SelectorInputs, finalize, preselect
from b24api.traversal.keyset_fast_plan import plan_lanes_from_anchors, plan_windows
from b24api.traversal.keyset_fast_stream import FastTraceRecorder
from b24api.traversal.keyset_observation import PageObservation
from b24api.traversal.keyset_partition import anchor_guesses

if TYPE_CHECKING:
    from b24api.contracts import JsonValue

PAGE_SIZE = 5


def _decode_command(value: str) -> dict[str, JsonValue]:
    parsed = parse_qs(value.split("?", 1)[1] if "?" in value else "")
    result: dict[str, JsonValue] = {"filter": {}, "order": {}}
    for key, values in parsed.items():
        if key.startswith("filter["):
            result["filter"][key[7:-1]] = values[0]  # type: ignore[index]
        elif key.startswith("order["):
            result["order"][key[6:-1]] = values[0]  # type: ignore[index]
        else:
            result[key] = values[0]
    return result


class KeysetTransport:
    """Serve a stable integer selection and expose requested controls."""

    host = "test.invalid"

    def __init__(
        self,
        identities: tuple[int, ...],
        *,
        ignore_direction: bool = False,
        ignore_bounds: bool = False,
        boundary_total: object | None = None,
        default_limit: int = PAGE_SIZE,
    ) -> None:
        self.identities = identities
        self.ignore_direction = ignore_direction
        self.ignore_bounds = ignore_bounds
        self.boundary_total = boundary_total
        self.default_limit = default_limit
        self.requests: list[Request] = []

    def _rows(self, parameters: dict[str, JsonValue]) -> list[dict[str, int]]:
        selected = self.identities
        filters = parameters.get("filter", {})
        if isinstance(filters, dict) and not self.ignore_bounds:
            if ">ID" in filters:
                selected = tuple(value for value in selected if value > int(filters[">ID"]))
            if "<ID" in filters:
                selected = tuple(value for value in selected if value < int(filters["<ID"]))
        order = parameters.get("order", {})
        descending = isinstance(order, dict) and order.get("id") == "DESC" and not self.ignore_direction
        limit = int(parameters.get("limit", self.default_limit))
        return [{"id": value} for value in sorted(selected, reverse=descending)[:limit]]

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        self.requests.append(request)
        if request.method == "batch":
            commands = request.copy_parameters()["cmd"]
            assert isinstance(commands, dict)
            results = {
                key: self._rows(_decode_command(value)) for key, value in commands.items() if isinstance(value, str)
            }
            envelope: dict[str, JsonValue] = {"result": results, "result_error": []}
            if self.boundary_total is not None:
                envelope["result_total"] = {
                    key: self.boundary_total  # type: ignore[dict-item]
                    for key, value in commands.items()
                    if isinstance(value, str)
                    and not any(bound in _decode_command(value).get("filter", {}) for bound in (">ID", "<ID"))
                }
            payload = {"result": envelope}
        else:
            payload = {"result": self._rows(request.copy_parameters())}
        return WireResponse(
            200,
            (("content-type", "application/json"),),
            json.dumps(payload, separators=(",", ":")).encode(),
        )

    async def aclose(self) -> None:
        pass


class MalformedBatchEnvelopeTransport(KeysetTransport):
    """Inject one deterministic correlation-envelope fault into every batch."""

    def __init__(self, identities: tuple[int, ...], *, fault: str) -> None:
        super().__init__(identities)
        self.fault = fault

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        response = await super().send(
            request,
            attempt_timeout=attempt_timeout,
            max_response_bytes=max_response_bytes,
        )
        if request.method != "batch":
            return response
        payload = json.loads(response.body)
        result = payload["result"]["result"]
        if self.fault == "extra":
            result["unexpected"] = []
        elif self.fault == "missing":
            result.pop(next(iter(result)))
        elif self.fault == "row_duplicate":
            encoded = json.dumps(payload, separators=(",", ":"))
            return WireResponse(
                response.status_code,
                response.headers,
                encoded.replace('{"id":', '{"name":"first","name":"last","id":', 1).encode(),
            )
        else:
            encoded = json.dumps(payload, separators=(",", ":"))
            key_prefix = f"{json.dumps(next(iter(result)))}:"
            return WireResponse(
                response.status_code,
                response.headers,
                encoded.replace(key_prefix, f"{key_prefix}[],{key_prefix}", 1).encode(),
            )
        return WireResponse(
            response.status_code,
            response.headers,
            json.dumps(payload, separators=(",", ":")).encode(),
        )


class EmptyDescendingBoundaryTransport(KeysetTransport):
    """Contradict the ascending boundary by hiding only the descending boundary page."""

    def _rows(self, parameters: dict[str, JsonValue]) -> list[dict[str, int]]:
        filters = parameters.get("filter", {})
        order = parameters.get("order", {})
        bounded = isinstance(filters, dict) and any(key in filters for key in (">ID", "<ID"))
        if not bounded and isinstance(order, dict) and order.get("id") == "DESC":
            return []
        return super()._rows(parameters)


class AppendOnFinishTransport(KeysetTransport):
    """Append identities only when the post-boundary finishing sweep begins."""

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        if request.method != "batch" and len(self.identities) == 30:
            self.identities = (*self.identities, *range(31, 36))
        return await super().send(
            request,
            attempt_timeout=attempt_timeout,
            max_response_bytes=max_response_bytes,
        )


class OneRowBoundedTransport(KeysetTransport):
    """Serve adversarial one-row bounded pages while preserving full boundary prefixes."""

    def _rows(self, parameters: dict[str, JsonValue]) -> list[dict[str, int]]:
        rows = super()._rows(parameters)
        filters = parameters.get("filter", {})
        bounded = isinstance(filters, dict) and any(key in filters for key in (">ID", "<ID"))
        body_started = sum(request.method == "batch" for request in self.requests) >= 3
        return rows[:1] if bounded and body_started else rows


class ShortBoundaryTransport(KeysetTransport):
    """Return short non-overlapping boundary prefixes despite a larger selection."""

    def _rows(self, parameters: dict[str, JsonValue]) -> list[dict[str, int]]:
        rows = super()._rows(parameters)
        filters = parameters.get("filter", {})
        bounded = isinstance(filters, dict) and any(key in filters for key in (">ID", "<ID"))
        return rows if bounded else rows[:2]


class AsymmetricBoundaryTransport(KeysetTransport):
    """Return one ascending boundary row and a full descending prefix."""

    def _rows(self, parameters: dict[str, JsonValue]) -> list[dict[str, int]]:
        rows = super()._rows(parameters)
        filters = parameters.get("filter", {})
        order = parameters.get("order", {})
        bounded = isinstance(filters, dict) and any(key in filters for key in (">ID", "<ID"))
        descending = isinstance(order, dict) and order.get("id") == "DESC"
        return rows[:1] if not bounded and not descending else rows


class NthBatchMissingResultTransport(KeysetTransport):
    """Drop one result only from a selected batch request."""

    def __init__(self, identities: tuple[int, ...], *, batch_ordinal: int) -> None:
        super().__init__(identities)
        self.batch_ordinal = batch_ordinal
        self.batch_count = 0

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        response = await super().send(
            request,
            attempt_timeout=attempt_timeout,
            max_response_bytes=max_response_bytes,
        )
        if request.method != "batch":
            return response
        self.batch_count += 1
        if self.batch_count != self.batch_ordinal:
            return response
        payload = json.loads(response.body)
        results = payload["result"]["result"]
        results.pop(next(iter(results)))
        return WireResponse(
            response.status_code,
            response.headers,
            json.dumps(payload, separators=(",", ":")).encode(),
        )


def _client(transport: KeysetTransport, *, policy: ExecutionPolicy | None = None) -> Bitrix24:
    return Bitrix24._from_executor(Executor(transport), policy=policy)


def _identity() -> IdentitySpec:
    return IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER)


def _stream(
    transport: KeysetTransport,
    execution: RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution,
    *,
    direction: str = "ascending",
    writable_limit: bool = True,
):
    return _client(transport).iter_list_keyset(
        Request("item.list", parameters={"filter": {"STATUS": "open"}}),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE_SIZE,
        keyset=KeysetSpec(
            limit_path=ParameterPath(("limit",)) if writable_limit else None,
            direction=direction,  # type: ignore[arg-type]
        ),
        execution=execution,
    )


@pytest.mark.parametrize(
    "factory",
    [
        lambda: RangeKeysetExecution(StableIntegerKeysetContract(), window_width=1),
        lambda: RangeKeysetExecution(StableIntegerKeysetContract(), batch_size=True),
        lambda: PartitionedKeysetExecution(StableIntegerKeysetContract(), target_lanes=1),
        lambda: AutoKeysetExecution(StableIntegerKeysetContract(), max_range_waves=0),
        lambda: StableIntegerKeysetContract(endpoint_page_cap=0),
    ],
)
def test_execution_contracts_reject_invalid_integer_controls(factory: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        factory()  # type: ignore[operator]


def test_window_and_anchor_algebra_is_disjoint_and_exact() -> None:
    windows = plan_windows(lo=-3, upper_exclusive=19, width=4)
    owned = [value for lane in windows for value in range(lane.bounds.lower_exclusive + 1, lane.bounds.upper_exclusive)]  # type: ignore[operator]
    assert owned == list(range(-2, 19))
    guesses = anchor_guesses(lo=5, upper_exclusive=106, target_lanes=20)
    assert guesses == tuple(sorted(set(guesses)))
    lanes = plan_lanes_from_anchors(lo=5, upper_exclusive=106, anchors=(20, 20, 70))
    assert tuple(lane.retained_upper_anchor for lane in lanes) == (20, 70, None)


def test_window_algebra_fixed_seed_property_tier() -> None:
    generator = random.Random(20260910)
    for _ in range(250):
        lo = generator.randint(-(10**12), 10**12)
        span = generator.randint(1, 500)
        upper = lo + span + 1
        width = generator.randint(2, 80)
        windows = plan_windows(lo=lo, upper_exclusive=upper, width=width)
        owned = [
            value
            for lane in windows
            for value in range(lane.bounds.lower_exclusive + 1, lane.bounds.upper_exclusive)  # type: ignore[operator]
        ]
        assert owned == list(range(lo + 1, upper))
        assert len(owned) == len(set(owned))


@pytest.mark.parametrize(
    ("boundary", "completion", "capacity", "expected"),
    [
        (
            BoundaryFacts(0, 0, None, None, None, None, False, False),
            KeysetPageCompletion.EMPTY_CONFIRMATION,
            50,
            (Preselected.BOUNDARY_ONLY, KeysetSelectionReason.EMPTY_SELECTION, 0, 0, 0, None, None, None, None),
        ),
        (
            BoundaryFacts(42, 42, 1, 42, 1, 42, True, False),
            KeysetPageCompletion.EMPTY_CONFIRMATION,
            50,
            (Preselected.BOUNDARY_ONLY, KeysetSelectionReason.BOUNDARY_OVERLAP, 0, 0, 42, None, None, None, None),
        ),
        (
            BoundaryFacts(50, 50, 3, 300, 4400, 4600, False, False),
            KeysetPageCompletion.SHORT_PAGE_EXHAUSTS,
            50,
            (Preselected.RANGE, KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET, 4099, 822, 922, 19, 3, 249, 17),
        ),
        (
            BoundaryFacts(50, 50, 3, 300, 4400, 4600, False, False),
            KeysetPageCompletion.EMPTY_CONFIRMATION,
            50,
            (Preselected.PROBE_ANCHORS, KeysetSelectionReason.WIDE_SPAN_PARTITIONING, 4099, 822, 922, 19, 6, 50, 84),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 71, 120, False, False),
            KeysetPageCompletion.EMPTY_CONFIRMATION,
            50,
            (Preselected.SEQUENTIAL, KeysetSelectionReason.SMALL_SELECTION, 20, 20, 120, 3, 4, 21, 1),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 151, 200, False, False),
            KeysetPageCompletion.EMPTY_CONFIRMATION,
            50,
            (Preselected.SEQUENTIAL, KeysetSelectionReason.INSUFFICIENT_PREDICTED_GAIN, 100, 100, 200, 4, 4, 50, 3),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 151, 200, False, False),
            KeysetPageCompletion.SHORT_PAGE_EXHAUSTS,
            50,
            (Preselected.SEQUENTIAL, KeysetSelectionReason.INSUFFICIENT_PREDICTED_GAIN, 100, 100, 200, 4, 3, 50, 3),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 301, 350, False, False),
            KeysetPageCompletion.SHORT_PAGE_EXHAUSTS,
            50,
            (Preselected.RANGE, KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET, 250, 250, 350, 7, 3, 50, 6),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 301, 350, False, False),
            KeysetPageCompletion.EMPTY_CONFIRMATION,
            50,
            (Preselected.PROBE_ANCHORS, KeysetSelectionReason.WIDE_SPAN_PARTITIONING, 250, 250, 350, 7, 4, 50, 6),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 510000, 1000000, False, False),
            KeysetPageCompletion.SHORT_PAGE_EXHAUSTS,
            50,
            (Preselected.RANGE, KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET, 509949, 105, 205, 5, 3, 245025, 3),
        ),
        (
            BoundaryFacts(50, 50, 3, 300, 4400, 4600, False, False),
            KeysetPageCompletion.SHORT_PAGE_EXHAUSTS,
            1,
            (
                Preselected.SEQUENTIAL,
                KeysetSelectionReason.INSUFFICIENT_PREDICTED_GAIN,
                4099,
                822,
                922,
                19,
                23,
                249,
                17,
            ),
        ),
    ],
)
def test_frozen_selector_decision_table(
    boundary: BoundaryFacts,
    completion: KeysetPageCompletion,
    capacity: int,
    expected: tuple[object, ...],
) -> None:
    inputs = SelectorInputs(boundary, 50, capacity, 20, 2, None, completion, None)
    selection = preselect(inputs)
    estimate = selection.range_estimate
    assert estimate is not None
    partition = selection.partition_estimate
    assert partition is not None
    has_estimates = selection.plan is not Preselected.BOUNDARY_ONLY
    assert (
        selection.plan,
        selection.reason,
        selection.interior_span,
        selection.interior_rows_estimate,
        selection.total_rows_estimate,
        selection.sequential_estimate.requests if has_estimates else None,
        estimate.requests if has_estimates else None,
        estimate.window_width if has_estimates else None,
        estimate.window_count if has_estimates else None,
    ) == expected
    expected_details = {
        (BoundaryFacts(0, 0, None, None, None, None, False, False), KeysetPageCompletion.EMPTY_CONFIRMATION, 50): (
            (0, 2, 0, 1, True),
            (3, 1, 1, 1, 1),
        ),
        (BoundaryFacts(42, 42, 1, 42, 1, 42, True, False), KeysetPageCompletion.EMPTY_CONFIRMATION, 50): (
            (1, 2, 0, 1, True),
            (3, 1, 1, 1, 1),
        ),
        (BoundaryFacts(50, 50, 3, 300, 4400, 4600, False, False), KeysetPageCompletion.SHORT_PAGE_EXHAUSTS, 50): (
            (50, 1, 1, 1, True),
            (3, 17, 1, 1, 1),
        ),
        (BoundaryFacts(50, 50, 3, 300, 4400, 4600, False, False), KeysetPageCompletion.EMPTY_CONFIRMATION, 50): (
            (10, 2, 2, 1, False),
            (3, 17, 1, 1, 1),
        ),
        (BoundaryFacts(50, 50, 1, 50, 71, 120, False, False), KeysetPageCompletion.EMPTY_CONFIRMATION, 50): (
            (20, 2, 1, 1, True),
            (3, 1, 1, 1, 1),
        ),
        (BoundaryFacts(50, 50, 1, 50, 151, 200, False, False), KeysetPageCompletion.EMPTY_CONFIRMATION, 50): (
            (49, 2, 1, 1, True),
            (3, 2, 1, 1, 1),
        ),
        (BoundaryFacts(50, 50, 1, 50, 151, 200, False, False), KeysetPageCompletion.SHORT_PAGE_EXHAUSTS, 50): (
            (49, 1, 1, 1, True),
            (3, 2, 1, 1, 1),
        ),
        (BoundaryFacts(50, 50, 1, 50, 301, 350, False, False), KeysetPageCompletion.SHORT_PAGE_EXHAUSTS, 50): (
            (49, 1, 1, 1, True),
            (3, 5, 1, 1, 1),
        ),
        (BoundaryFacts(50, 50, 1, 50, 301, 350, False, False), KeysetPageCompletion.EMPTY_CONFIRMATION, 50): (
            (49, 2, 1, 1, True),
            (3, 5, 1, 1, 1),
        ),
        (BoundaryFacts(50, 50, 1, 50, 510000, 1000000, False, False), KeysetPageCompletion.SHORT_PAGE_EXHAUSTS, 50): (
            (50, 1, 1, 1, True),
            (3, 3, 1, 1, 1),
        ),
        (BoundaryFacts(50, 50, 3, 300, 4400, 4600, False, False), KeysetPageCompletion.SHORT_PAGE_EXHAUSTS, 1): (
            (50, 1, 17, 5, False),
            (43, 17, 1, 17, 25),
        ),
    }
    expected_range, expected_partition = expected_details[(boundary, completion, capacity)]
    assert (
        estimate.rows_per_window,
        estimate.depth,
        estimate.groups,
        estimate.planning_waves,
        estimate.eligible,
    ) == expected_range
    assert (
        partition.requests,
        partition.lane_count,
        partition.depth,
        partition.groups,
        partition.planning_waves,
    ) == expected_partition


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["ascending", "descending"])
@pytest.mark.parametrize("kind", ["range", "partitioned"])
async def test_explicit_modes_match_sparse_ordered_oracle(direction: str, kind: str) -> None:
    identities = (1, 2, 3, 4, 5, 10, 20, 40, 80, 160, 320, 640, 1000, 1001, 1002, 1003, 1004)
    transport = KeysetTransport(identities)
    contract = StableIntegerKeysetContract()
    execution = (
        RangeKeysetExecution(contract) if kind == "range" else PartitionedKeysetExecution(contract, target_lanes=3)
    )
    stream = _stream(transport, execution, direction=direction)
    result = [item["id"] async for item in stream]
    assert result == sorted(identities, reverse=direction == "descending")
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.selected_kind is KeysetExecutionKind(kind)
    assert stream.report.keyset_execution.canary_commands == 5
    assert stream.report.keyset_execution.assurance_source is KeysetAssuranceSource.CANARY_VERIFIED_BOUNDS
    assert all(
        record.rows_admitted == record.rows_selected
        for record in stream.report.page_trace
        if record.phase is KeysetPhase.BODY and record.rows_selected
    )
    if kind == "partitioned":
        assert dict(stream.report.keyset_execution.closure_witness_counts)[ClosureWitness.ANCHOR_FENCE] > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["ascending", "descending"])
async def test_short_page_contract_closes_sparse_windows_in_both_directions(direction: str) -> None:
    identities = (*range(1, 6), 10, 20, *range(100, 105))
    stream = _stream(
        KeysetTransport(identities),
        RangeKeysetExecution(
            StableIntegerKeysetContract(page_completion=KeysetPageCompletion.SHORT_PAGE_EXHAUSTS),
            window_width=50,
        ),
        direction=direction,
    )

    assert [item["id"] async for item in stream] == sorted(identities, reverse=direction == "descending")
    assert stream.report is not None
    assert stream.report.keyset_execution is not None
    closures = dict(stream.report.keyset_execution.closure_witness_counts)
    assert closures[ClosureWitness.SHORT_PAGE] == 1
    assert closures[ClosureWitness.EMPTY] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["ascending", "descending"])
async def test_empty_confirmation_handles_repeated_one_row_pages(direction: str) -> None:
    identities = tuple(range(1, 31))
    stream = _stream(
        OneRowBoundedTransport(identities),
        RangeKeysetExecution(StableIntegerKeysetContract()),
        direction=direction,
    )

    assert [row["id"] async for row in stream] == sorted(identities, reverse=direction == "descending")


@pytest.mark.asyncio
async def test_ascending_full_integer_window_reports_lattice_closure() -> None:
    identities = tuple(range(1, 31))
    stream = _stream(
        KeysetTransport(identities),
        RangeKeysetExecution(StableIntegerKeysetContract(), window_width=6),
    )

    assert [item["id"] async for item in stream] == list(identities)
    assert stream.report is not None
    assert stream.report.keyset_execution is not None
    closures = dict(stream.report.keyset_execution.closure_witness_counts)
    assert closures[ClosureWitness.LATTICE_FULL] == 4
    assert closures[ClosureWitness.TOP] == 0


@pytest.mark.parametrize(
    ("capacity", "quotas"),
    [
        (64, {TraceClass.PLANNING: 8, TraceClass.TERMINAL: 8, TraceClass.BODY: 16, TraceClass.ANOMALY: 32}),
        (13, {TraceClass.PLANNING: 1, TraceClass.TERMINAL: 1, TraceClass.BODY: 3, TraceClass.ANOMALY: 8}),
    ],
)
def test_fast_trace_uses_exact_deterministic_class_quotas(
    capacity: int,
    quotas: dict[TraceClass, int],
) -> None:
    def populated_recorder() -> tuple[FastTraceRecorder, set[int]]:
        recorder = FastTraceRecorder(capacity)
        expected_sequences: set[int] = set()
        ordinal = 0
        phases = {
            TraceClass.PLANNING: KeysetPhase.BOUNDARY,
            TraceClass.TERMINAL: KeysetPhase.FINISH,
            TraceClass.BODY: KeysetPhase.BODY,
            TraceClass.ANOMALY: KeysetPhase.BOUNDARY,
        }
        for trace_class, quota in quotas.items():
            sequences: list[int] = []
            for _ in range(quota + 3):
                sequences.append(ordinal)
                anomaly = trace_class is TraceClass.ANOMALY
                recorder.record(
                    PageObservation(
                        ordinal=ordinal,
                        phase=phases[trace_class],
                        lane_ordinal=0,
                        command_id=f"command-{ordinal}",
                        dispatch=PageDispatch.BATCH,
                        batch_index=ordinal,
                        rows_selected=0,
                        rows_admitted=0,
                        reported_total=None,
                        reported_next=None,
                        page_full=False,
                        witness=None,
                        outcome=PageOutcome.REJECTED if anomaly else PageOutcome.COMMITTED,
                        rejection_code=PageRejectionCode.RANGE_CONTRADICTION if anomaly else None,
                        violation=None,
                        trace_class=TraceClass.BODY,
                    ),
                )
                ordinal += 1
            head = (quota + 1) // 2
            tail = quota // 2
            expected_sequences.update(sequences[:head])
            if tail:
                expected_sequences.update(sequences[-tail:])
        return recorder, expected_sequences

    recorder, expected_sequences = populated_recorder()
    records, dropped = recorder.snapshot()
    repeated, _ = populated_recorder()

    assert len(records) == capacity
    assert {record.sequence for record in records} == expected_sequences
    assert dict(recorder.class_counts()) == quotas
    assert dropped == dict.fromkeys(TraceClass, 3)
    assert repeated.snapshot() == recorder.snapshot()


@pytest.mark.asyncio
async def test_auto_sequential_has_request_parity_and_no_canaries() -> None:
    transport = KeysetTransport(tuple(range(1, 21)))
    stream = _stream(
        transport,
        AutoKeysetExecution(StableIntegerKeysetContract(), target_lanes=5),
    )
    assert [item["id"] async for item in stream] == list(range(1, 21))
    assert stream.report is not None
    assert stream.report.keyset_execution is not None
    report = stream.report.keyset_execution
    assert report.selected_kind is KeysetExecutionKind.SEQUENTIAL
    assert report.preselection_reason is KeysetSelectionReason.INSUFFICIENT_PREDICTED_GAIN
    assert report.canary_commands == 0
    assert stream.report.physical_requests == 5


@pytest.mark.asyncio
async def test_boundary_only_estimate_excludes_the_spent_boundary_wave() -> None:
    empty = _stream(KeysetTransport(()), AutoKeysetExecution(StableIntegerKeysetContract()))
    assert [row async for row in empty] == []
    assert empty.report.keyset_execution is not None
    assert empty.report.keyset_execution.selected_requests_estimate == 0

    overlap = _stream(KeysetTransport(tuple(range(1, 8))), AutoKeysetExecution(StableIntegerKeysetContract()))
    assert [row["id"] async for row in overlap] == list(range(1, 8))
    assert overlap.report.keyset_execution is not None
    assert overlap.report.keyset_execution.selected_requests_estimate == 1


@pytest.mark.asyncio
async def test_adjacent_boundaries_select_prefix_assured_boundary_only() -> None:
    stream = _stream(
        KeysetTransport(tuple(range(1, 11))),
        AutoKeysetExecution(StableIntegerKeysetContract()),
    )

    assert [row["id"] async for row in stream] == list(range(1, 11))
    report = stream.report.keyset_execution
    assert report is not None
    assert report.selected_kind is KeysetExecutionKind.BOUNDARY_ONLY
    assert report.preselection_reason is KeysetSelectionReason.ADJACENT_BOUNDARIES
    assert report.assurance_source is KeysetAssuranceSource.ORDERED_PREFIX_ONLY
    assert report.canary_commands == 0


def test_post_probe_range_preferred_transition_is_pinned() -> None:
    inputs = SelectorInputs(
        BoundaryFacts(50, 50, 1, 50, 301, 350, False, False),
        50,
        50,
        20,
        2,
        None,
        KeysetPageCompletion.EMPTY_CONFIRMATION,
        None,
    )
    selection = preselect(inputs)

    final = finalize(inputs, selection, AnchorFacts((), 20, 20))

    assert selection.plan is Preselected.PROBE_ANCHORS
    assert final.kind is KeysetExecutionKind.RANGE
    assert final.reason is KeysetSelectionReason.POST_PROBE_RANGE_PREFERRED


@pytest.mark.asyncio
async def test_boundary_overlap_releases_duplicate_row_capacity_immediately() -> None:
    stream = _stream(
        KeysetTransport(tuple(range(1, 8))),
        AutoKeysetExecution(StableIntegerKeysetContract()),
    )
    source = stream._source

    assert (await anext(source))["id"] == 1

    scheduler = source._scheduler
    assert scheduler.counters.boundary_overlap_rows == 3
    assert scheduler._buffer_balance == 7
    await source.aclose()


@pytest.mark.asyncio
async def test_explicit_partitioned_reports_degenerate_single_lane() -> None:
    stream = _stream(
        KeysetTransport((*range(1, 6), *range(100, 105))),
        PartitionedKeysetExecution(StableIntegerKeysetContract(), target_lanes=3),
    )

    assert [row["id"] async for row in stream] == [*range(1, 6), *range(100, 105)]
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.selected_kind is KeysetExecutionKind.PARTITIONED
    assert stream.report.keyset_execution.preselection_reason is KeysetSelectionReason.DEGENERATE_SINGLE_LANE
    assert stream.report.keyset_execution.actual_lanes == 1


@pytest.mark.asyncio
async def test_auto_keeps_feasible_range_when_anchor_retention_does_not_fit() -> None:
    identities = (*range(1, 6), 50, 100, 200, 300, 400, *range(507, 512))
    transport = KeysetTransport(identities)
    policy = ExecutionPolicy(max_buffered_rows=30)
    stream = _client(transport, policy=policy).iter_list_keyset(
        Request("item.list"),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE_SIZE,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=AutoKeysetExecution(
            StableIntegerKeysetContract(page_completion=KeysetPageCompletion.SHORT_PAGE_EXHAUSTS),
            target_lanes=20,
            max_range_waves=50,
        ),
    )

    assert [row["id"] async for row in stream] == list(identities)
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.selected_kind is KeysetExecutionKind.RANGE


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "execution",
    [
        RangeKeysetExecution(StableIntegerKeysetContract()),
        PartitionedKeysetExecution(StableIntegerKeysetContract(), target_lanes=3),
        AutoKeysetExecution(StableIntegerKeysetContract(), target_lanes=3),
    ],
)
async def test_finishing_sweep_includes_append_growth(
    execution: RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution,
) -> None:
    stream = _stream(AppendOnFinishTransport(tuple(range(1, 31))), execution)

    assert [row["id"] async for row in stream] == list(range(1, 36))


@pytest.mark.asyncio
async def test_partition_transfers_anchor_ownership_without_retaining_payload() -> None:
    stream = _stream(
        KeysetTransport(tuple(range(1, 101))),
        PartitionedKeysetExecution(StableIntegerKeysetContract(), target_lanes=3),
    )
    scheduler = stream._source._scheduler
    await scheduler.plan_barrier()
    assert scheduler._anchor_rows

    while rows := await scheduler.next_rows():
        scheduler.mark_emitted(len(rows))

    assert scheduler._anchor_rows == {}
    assert scheduler._anchor_commands == {}
    await scheduler.aclose()


@pytest.mark.asyncio
async def test_auto_discards_precharged_anchor_objects_when_post_probe_gain_is_lost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = keyset_scheduler.finalize

    def force_sequential(*args: object, **kwargs: object):
        return replace(
            original(*args, **kwargs),
            kind=KeysetExecutionKind.SEQUENTIAL,
            reason=KeysetSelectionReason.POST_PROBE_GAIN_LOST,
        )

    monkeypatch.setattr(keyset_scheduler, "finalize", force_sequential)
    stream = _stream(
        KeysetTransport(tuple(range(1, 5_001))),
        AutoKeysetExecution(StableIntegerKeysetContract(), target_lanes=20),
    )

    assert (await anext(stream))["id"] == 1
    scheduler = stream._source._scheduler
    assert scheduler._selected is KeysetExecutionKind.SEQUENTIAL
    assert scheduler._anchor_count > 0
    assert scheduler._anchor_rows == scheduler._anchor_commands == {}
    assert scheduler._boundary_totals == {}
    assert scheduler._buffer_balance == PAGE_SIZE
    await stream.aclose()


@pytest.mark.asyncio
async def test_empty_auto_selection_completes_in_boundary_wave() -> None:
    transport = KeysetTransport(())
    stream = _stream(transport, AutoKeysetExecution(StableIntegerKeysetContract()))

    assert [item async for item in stream] == []
    assert stream.report is not None
    assert stream.report.physical_requests == 1
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.selected_kind is KeysetExecutionKind.BOUNDARY_ONLY
    assert stream.report.keyset_execution.preselection_reason is KeysetSelectionReason.EMPTY_SELECTION


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["extra", "duplicate"])
async def test_fast_wave_invalid_correlation_rejects_every_observation_as_batch_envelope(fault: str) -> None:
    stream = _stream(
        MalformedBatchEnvelopeTransport(tuple(range(1, 21)), fault=fault),
        RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    with pytest.raises(IncompleteTraversalError):
        _ = [item async for item in stream]

    assert stream.report is not None
    assert len(stream.report.page_trace) == 2
    assert {(record.outcome, record.rejection_code) for record in stream.report.page_trace} == {
        (PageOutcome.REJECTED, PageRejectionCode.BATCH_ENVELOPE),
    }


@pytest.mark.asyncio
async def test_fast_wave_missing_correlation_rejects_command_and_aborts_decoded_sibling() -> None:
    stream = _stream(
        MalformedBatchEnvelopeTransport(tuple(range(1, 21)), fault="missing"),
        RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    with pytest.raises(IncompleteTraversalError):
        _ = [item async for item in stream]

    assert stream.report is not None
    assert len(stream.report.page_trace) == 2
    assert {(record.outcome, record.rejection_code) for record in stream.report.page_trace} == {
        (PageOutcome.REJECTED, PageRejectionCode.COMMAND_FAILURE),
        (PageOutcome.REJECTED, PageRejectionCode.TRANSACTION_ABORTED),
    }


@pytest.mark.asyncio
async def test_fast_wave_tolerates_duplicate_non_correlation_row_member() -> None:
    identities = tuple(range(1, 21))
    stream = _stream(
        MalformedBatchEnvelopeTransport(identities, fault="row_duplicate"),
        RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    assert [item["id"] async for item in stream] == list(identities)


@pytest.mark.asyncio
async def test_partition_anchor_probe_without_writable_limit_discards_extra_rows() -> None:
    identities = tuple(range(1, 61)) + tuple(range(100, 141))
    stream = _stream(
        KeysetTransport(identities),
        PartitionedKeysetExecution(StableIntegerKeysetContract(endpoint_page_cap=PAGE_SIZE), target_lanes=3),
        writable_limit=False,
    )

    assert [item["id"] async for item in stream] == list(identities)
    assert stream.report is not None
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.probe_rows_discarded > 0


@pytest.mark.asyncio
async def test_boundary_direction_contradiction_fails_before_emission() -> None:
    transport = KeysetTransport(tuple(range(1, 31)), ignore_direction=True)
    stream = _stream(transport, RangeKeysetExecution(StableIntegerKeysetContract()))
    with pytest.raises(IncompleteTraversalError):
        await anext(stream)
    assert stream.report is not None
    assert stream.report.emitted == 0
    assert stream.report.state is TerminalState.INCOMPLETE
    assert stream._source._scheduler._boundary_totals == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "execution",
    [
        RangeKeysetExecution(StableIntegerKeysetContract()),
        PartitionedKeysetExecution(StableIntegerKeysetContract(), target_lanes=3),
    ],
)
async def test_asymmetric_empty_boundary_is_classified_incomplete(
    execution: RangeKeysetExecution | PartitionedKeysetExecution,
) -> None:
    stream = _stream(EmptyDescendingBoundaryTransport(tuple(range(1, 31))), execution)

    with pytest.raises(IncompleteTraversalError):
        await anext(stream)

    assert stream.report.state is TerminalState.INCOMPLETE
    assert stream.report.emitted == 0


@pytest.mark.asyncio
async def test_ignored_numeric_bounds_fail_canaries_before_emission() -> None:
    transport = KeysetTransport(tuple(range(1, 31)), ignore_bounds=True)
    stream = _stream(transport, RangeKeysetExecution(StableIntegerKeysetContract()))

    with pytest.raises(IncompleteTraversalError):
        await anext(stream)
    assert stream.report is not None
    assert stream.report.emitted == 0
    assert stream.report.state is TerminalState.INCOMPLETE
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.canary_rows > 0
    assert stream.report.keyset_execution.probe_rows_discarded > 0
    assert stream.report.keyset_execution.selected_kind is KeysetExecutionKind.RANGE
    assert stream.report.keyset_execution.preselection_reason is KeysetSelectionReason.EXPLICIT_RANGE
    assert stream.report.keyset_execution.assurance_source is KeysetAssuranceSource.ORDERED_PREFIX_ONLY
    canaries = [record for record in stream.report.page_trace if record.phase is KeysetPhase.CANARY]
    assert canaries
    assert all(record.outcome is PageOutcome.REJECTED for record in canaries)


@pytest.mark.asyncio
async def test_chunked_canary_failure_finalizes_prior_staged_evidence() -> None:
    stream = _stream(
        KeysetTransport(tuple(range(1, 31)), ignore_bounds=True),
        RangeKeysetExecution(StableIntegerKeysetContract(), batch_size=2),
    )

    with pytest.raises(IncompleteTraversalError):
        await anext(stream)

    canaries = [record for record in stream.report.page_trace if record.phase is KeysetPhase.CANARY]
    assert canaries
    assert all(record.outcome is not PageOutcome.COMMITTED for record in canaries)
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.probe_rows_discarded > 0


@pytest.mark.asyncio
async def test_short_boundary_cap_contradiction_never_records_committed_planning_pages() -> None:
    stream = _stream(
        ShortBoundaryTransport(tuple(range(1, 31))),
        RangeKeysetExecution(StableIntegerKeysetContract(page_completion=KeysetPageCompletion.SHORT_PAGE_EXHAUSTS)),
    )

    with pytest.raises(IncompleteTraversalError):
        await anext(stream)

    boundaries = [record for record in stream.report.page_trace if record.phase is KeysetPhase.BOUNDARY]
    assert len(boundaries) == 2
    assert all(record.outcome is PageOutcome.REJECTED for record in boundaries)
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.assurance_source is KeysetAssuranceSource.ORDERED_PREFIX_ONLY


def test_fast_ineligible_requests_fail_synchronously_without_io() -> None:
    transport = KeysetTransport((1, 2, 3))
    client = _client(transport)
    with pytest.raises(CapabilityError):
        client.iter_list_keyset(
            Request("item.list", parameters={"filter": {">id": 1}}),
            selector=ResultSelector.root(),
            identity=_identity(),
            execution=RangeKeysetExecution(StableIntegerKeysetContract()),
        )
    with pytest.raises(CapabilityError):
        client.iter_list_keyset(
            Request("item.list"),
            selector=ResultSelector.root(),
            identity=_identity(),
            execution=RangeKeysetExecution(
                StableIntegerKeysetContract(page_completion=KeysetPageCompletion.SHORT_PAGE_EXHAUSTS),
            ),
        )
    assert transport.requests == []


@pytest.mark.parametrize(
    "consistency",
    [
        ConsistencyPolicy(total_semantics=TotalSemantics.FILTERED_EXACT),
        ConsistencyPolicy(order_semantics=OrderSemantics.DESCENDING),
        ConsistencyPolicy(identity_requirement=IdentityRequirement.COMPOSITE),
    ],
)
def test_fast_rejects_incompatible_universal_consistency_without_io(
    consistency: ConsistencyPolicy,
) -> None:
    transport = KeysetTransport(tuple(range(1, 31)))
    client = _client(transport, policy=ExecutionPolicy(consistency=consistency))

    with pytest.raises(CapabilityError):
        client.iter_list_keyset(
            Request("item.list"),
            selector=ResultSelector.root(),
            identity=_identity(),
            page_size=PAGE_SIZE,
            keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
            execution=RangeKeysetExecution(StableIntegerKeysetContract()),
        )

    assert transport.requests == []


@pytest.mark.asyncio
async def test_fast_context_enter_is_io_free_and_request_is_immutable() -> None:
    transport = KeysetTransport(tuple(range(1, 8)))
    request = Request("item.list", parameters={"filter": {"STATUS": "open"}})
    stream = _client(transport).iter_list_keyset(
        request,
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE_SIZE,
        execution=RangeKeysetExecution(StableIntegerKeysetContract()),
    )
    async with stream:
        assert transport.requests == []
    assert request.copy_parameters() == {"filter": {"STATUS": "open"}}


@pytest.mark.asyncio
@pytest.mark.parametrize(("operation", "expected"), [("first", 1), ("collect", 3)])
async def test_fast_bounded_consumption_freezes_an_early_close_report(operation: str, expected: int) -> None:
    stream = _stream(
        KeysetTransport(tuple(range(1, 80))),
        RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    partial = await stream.first() if operation == "first" else await stream.collect(limit=expected)

    assert len(partial.value) == expected
    assert partial.report.state is TerminalState.EARLY_CLOSED
    assert partial.report.emitted == expected
    assert partial.report.unique_rows == expected
    assert partial.report.buffered_rows_high_water <= ExecutionPolicy().max_buffered_rows
    assert stream._source._scheduler._plan_outcome is None
    assert not stream._source._buffer
    assert stream._source._scheduler._anchor_rows == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", [ExecutionPolicy(max_pages=2), ExecutionPolicy(max_requests=1)])
async def test_fast_runtime_budget_is_loud_incomplete(policy: ExecutionPolicy) -> None:
    transport = KeysetTransport(tuple(range(1, 80)))
    stream = _client(transport, policy=policy).iter_list_keyset(
        Request("item.list"),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE_SIZE,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    with pytest.raises(IncompleteTraversalError):
        await anext(stream)

    assert stream.report.state is TerminalState.INCOMPLETE
    assert stream.report.emitted == 0


@pytest.mark.asyncio
async def test_fast_close_finishes_cleanup_before_propagating_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = _stream(
        KeysetTransport(tuple(range(1, 80))),
        RangeKeysetExecution(StableIntegerKeysetContract()),
    )
    source = stream._source
    await anext(source)
    scheduler = source._scheduler
    original = scheduler._adjust_buffer
    entered = asyncio.Event()
    release = asyncio.Event()

    async def pause_cleanup(delta: int) -> None:
        if delta < 0:
            entered.set()
            await release.wait()
        await original(delta)

    monkeypatch.setattr(scheduler, "_adjust_buffer", pause_cleanup)
    closing = asyncio.create_task(source.aclose())
    await entered.wait()
    closing.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await closing

    assert scheduler._closed is True
    assert source._closed is True
    assert scheduler._buffer_balance == 0
    assert not source._buffer


@pytest.mark.asyncio
@pytest.mark.parametrize("malformed_total", [-1, True, "79", 79.5])
async def test_advisory_boundary_total_is_ignored_when_not_a_non_negative_integer(malformed_total: object) -> None:
    stream = _stream(
        KeysetTransport(tuple(range(1, 80)), boundary_total=malformed_total),
        AutoKeysetExecution(
            StableIntegerKeysetContract(),
            total_hint=TotalHintMode.REQUEST_ADVISORY,
        ),
    )

    assert [row["id"] async for row in stream] == list(range(1, 80))
    assert stream.report is not None
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.total_hint_observed is None
    assert stream.report.keyset_execution.total_hint_plausible is False


@pytest.mark.asyncio
async def test_keyset_report_rejects_boolean_total_hint_observation() -> None:
    stream = _stream(
        KeysetTransport(tuple(range(1, 31)), boundary_total=30),
        AutoKeysetExecution(StableIntegerKeysetContract(), total_hint=TotalHintMode.REQUEST_ADVISORY),
    )
    assert [row["id"] async for row in stream] == list(range(1, 31))
    assert stream.report.keyset_execution is not None

    with pytest.raises(ValueError, match="optional keyset report counters"):
        replace(stream.report.keyset_execution, total_hint_observed=True)
    nested = _report_json(stream.report)["keyset_execution"]
    assert isinstance(nested, dict)
    assert nested["selected_kind"] == stream.report.keyset_execution.selected_kind


@pytest.mark.asyncio
async def test_partition_body_wave_accounts_for_pinned_tail_and_anchors() -> None:
    page_size = 50
    identities = tuple(range(1, 5_001))
    transport = KeysetTransport(identities)
    stream = _client(transport).iter_list_keyset(
        Request("item.list"),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=page_size,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=PartitionedKeysetExecution(StableIntegerKeysetContract(), target_lanes=50),
    )

    assert [row["id"] async for row in stream] == list(identities)
    assert stream.report is not None
    assert stream.report.buffered_rows_high_water <= ExecutionPolicy().max_buffered_rows


@pytest.mark.asyncio
async def test_partition_planning_accounts_for_free_rows_without_a_writable_limit() -> None:
    page_size = 50
    identities = tuple(range(1, 5_001))
    transport = KeysetTransport(identities, default_limit=page_size)
    stream = _client(transport).iter_list_keyset(
        Request("item.list"),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=page_size,
        keyset=KeysetSpec(),
        execution=PartitionedKeysetExecution(
            StableIntegerKeysetContract(endpoint_page_cap=page_size),
            target_lanes=44,
        ),
    )

    assert [row["id"] async for row in stream] == list(identities)
    assert stream.report is not None
    assert stream.report.buffered_rows_high_water <= ExecutionPolicy().max_buffered_rows


@pytest.mark.asyncio
async def test_partition_anchor_waves_discard_unneeded_rows_before_the_next_wave() -> None:
    page_size = 50
    identities = tuple(range(1, 5_001))
    policy = ExecutionPolicy(max_buffered_rows=200)
    transport = KeysetTransport(identities, default_limit=page_size)
    stream = _client(transport, policy=policy).iter_list_keyset(
        Request("item.list"),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=page_size,
        keyset=KeysetSpec(),
        execution=PartitionedKeysetExecution(
            StableIntegerKeysetContract(endpoint_page_cap=page_size),
            target_lanes=50,
            batch_size=1,
        ),
    )
    scheduler = stream._source._scheduler
    await scheduler.plan_barrier()

    assert scheduler._boundary_totals == {}
    assert scheduler._buffer_balance == page_size * 2 + 50
    assert [row["id"] async for row in stream] == list(identities)
    assert stream.report is not None
    assert stream.report.buffered_rows_high_water <= policy.max_buffered_rows


def test_explicit_partitioned_rejects_policy_that_cannot_retain_anchors_and_a_body_page() -> None:
    page_size = 5
    execution = PartitionedKeysetExecution(StableIntegerKeysetContract(), target_lanes=10)
    transport = KeysetTransport(tuple(range(1, 401)))
    client = _client(transport, policy=ExecutionPolicy(max_buffered_rows=3 * page_size))

    with pytest.raises(CapabilityError, match="retain boundaries"):
        client.iter_list_keyset(
            Request("item.list"),
            selector=ResultSelector.root(),
            identity=_identity(),
            page_size=page_size,
            keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
            execution=execution,
        )

    assert transport.requests == []


@pytest.mark.parametrize(
    "execution",
    [RangeKeysetExecution(StableIntegerKeysetContract()), PartitionedKeysetExecution(StableIntegerKeysetContract())],
)
def test_explicit_bounded_mode_rejects_policy_without_post_boundary_capacity(
    execution: RangeKeysetExecution | PartitionedKeysetExecution,
) -> None:
    transport = KeysetTransport(tuple(range(1, 31)))
    client = _client(transport, policy=ExecutionPolicy(max_buffered_rows=3 * PAGE_SIZE - 1))

    with pytest.raises(CapabilityError, match="retain boundaries"):
        client.iter_list_keyset(
            Request("item.list"),
            selector=ResultSelector.root(),
            identity=_identity(),
            page_size=PAGE_SIZE,
            keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
            execution=execution,
        )

    assert transport.requests == []


@pytest.mark.asyncio
async def test_auto_uses_post_boundary_capacity_and_selects_sequential_when_none_remains() -> None:
    identities = (*range(1, 6), *range(1_006, 1_011))
    transport = KeysetTransport(identities)
    stream = _client(transport, policy=ExecutionPolicy(max_buffered_rows=2 * PAGE_SIZE)).iter_list_keyset(
        Request("item.list"),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE_SIZE,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=AutoKeysetExecution(StableIntegerKeysetContract(), target_lanes=20),
    )

    assert [row["id"] async for row in stream] == list(identities)
    assert stream.report is not None
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.selected_kind is KeysetExecutionKind.SEQUENTIAL
    assert stream.report.keyset_execution.effective_batch_capacity == 0


@pytest.mark.asyncio
async def test_descending_full_integer_window_reports_lattice_closure() -> None:
    identities = tuple(range(1, 31))
    stream = _stream(
        KeysetTransport(identities),
        RangeKeysetExecution(StableIntegerKeysetContract(), window_width=6),
        direction="descending",
    )

    assert [item["id"] async for item in stream] == list(reversed(identities))
    assert stream.report is not None
    assert stream.report.keyset_execution is not None
    closures = dict(stream.report.keyset_execution.closure_witness_counts)
    assert closures[ClosureWitness.LATTICE_FULL] == 4
    assert closures[ClosureWitness.TOP] == 0


@pytest.mark.asyncio
async def test_partial_page_budget_rejects_a_wave_without_waiting_for_elapsed_budget() -> None:
    policy = ExecutionPolicy(max_pages=3, max_elapsed=20.0)
    transport = KeysetTransport(tuple(range(1, 80)))
    stream = _client(transport, policy=policy).iter_list_keyset(
        Request("item.list"),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE_SIZE,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    with pytest.raises(IncompleteTraversalError):
        async with asyncio.timeout(0.5):
            await anext(stream)

    assert stream.report.state is TerminalState.INCOMPLETE
    assert stream.report.emitted == 0


@pytest.mark.asyncio
async def test_range_windows_are_materialized_one_bounded_group_at_a_time() -> None:
    identities = (*range(1, 6), *range(1_000_001, 1_000_006))
    stream = _stream(
        KeysetTransport(identities),
        RangeKeysetExecution(StableIntegerKeysetContract()),
    )
    scheduler = stream._source._scheduler

    await scheduler.plan_barrier()

    assert scheduler._window_count == 249_999
    assert len(scheduler._lanes) == scheduler.batch_capacity
    assert len(scheduler._lane_rows) == scheduler.batch_capacity
    await stream.aclose()


@pytest.mark.asyncio
async def test_short_page_range_preserves_grouped_wave_cost() -> None:
    identities = tuple(range(1, 351))
    stream = _client(KeysetTransport(identities, default_limit=50)).iter_list_keyset(
        Request("item.list"),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=50,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=AutoKeysetExecution(
            StableIntegerKeysetContract(page_completion=KeysetPageCompletion.SHORT_PAGE_EXHAUSTS),
        ),
    )

    assert [row["id"] async for row in stream] == list(identities)
    assert stream.report.physical_requests == 4
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.selected_kind is KeysetExecutionKind.RANGE
    assert stream.report.keyset_execution.selected_requests_estimate == 3


@pytest.mark.asyncio
async def test_auto_reports_sequential_when_boundary_cannot_supply_canaries() -> None:
    identities = tuple(range(1, 41))
    stream = _client(KeysetTransport(identities, default_limit=1)).iter_list_keyset(
        Request("item.list"),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=1,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=AutoKeysetExecution(StableIntegerKeysetContract()),
    )

    assert [row["id"] async for row in stream] == list(identities)
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.selected_kind is KeysetExecutionKind.SEQUENTIAL
    assert stream.report.keyset_execution.canary_commands == 0


@pytest.mark.asyncio
async def test_failed_canary_construction_finalizes_boundary_evidence() -> None:
    stream = _client(KeysetTransport(tuple(range(1, 41)), default_limit=1)).iter_list_keyset(
        Request("item.list"),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=1,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    with pytest.raises(IncompleteTraversalError):
        await anext(stream)
    assert stream.report.logical_pages == 2
    assert len(stream.report.page_trace) == 2
    assert all(record.outcome is PageOutcome.REJECTED for record in stream.report.page_trace)
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.probe_rows_discarded == 2


@pytest.mark.asyncio
async def test_dense_multi_round_range_keeps_grouped_continuations() -> None:
    identities = tuple(range(1, 126))
    stream = _client(KeysetTransport(identities, default_limit=5)).iter_list_keyset(
        Request("item.list"),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=5,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=AutoKeysetExecution(
            StableIntegerKeysetContract(page_completion=KeysetPageCompletion.SHORT_PAGE_EXHAUSTS),
            range_window_width=11,
            target_lanes=2,
        ),
    )

    assert [row["id"] async for row in stream] == list(identities)
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.selected_kind is KeysetExecutionKind.RANGE
    assert stream.report.physical_requests == 5


@pytest.mark.asyncio
async def test_auto_uses_available_canary_pair_from_asymmetric_boundary() -> None:
    identities = tuple(range(1, 1_001))
    stream = _client(AsymmetricBoundaryTransport(identities, default_limit=5)).iter_list_keyset(
        Request("item.list"),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=5,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=AutoKeysetExecution(StableIntegerKeysetContract(), target_lanes=2),
    )

    assert [row["id"] async for row in stream] == list(identities)
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.selected_kind is not KeysetExecutionKind.SEQUENTIAL
    assert stream.report.keyset_execution.canary_commands == 5


@pytest.mark.asyncio
async def test_coscheduled_canary_failure_accounts_for_every_staged_probe_row() -> None:
    stream = _stream(
        KeysetTransport(tuple(range(1, 101)), ignore_bounds=True),
        PartitionedKeysetExecution(StableIntegerKeysetContract(), target_lanes=3),
    )

    with pytest.raises(IncompleteTraversalError):
        await anext(stream)
    assert stream.report.keyset_execution is not None
    selected = sum(record.rows_selected for record in stream.report.page_trace)
    assert stream.report.keyset_execution.probe_rows_discarded == selected


@pytest.mark.asyncio
async def test_separate_anchor_failure_discards_previously_validated_boundaries() -> None:
    stream = _stream(
        NthBatchMissingResultTransport(tuple(range(1, 101)), batch_ordinal=3),
        PartitionedKeysetExecution(StableIntegerKeysetContract(), batch_size=5, target_lanes=3),
    )

    with pytest.raises(IncompleteTraversalError):
        await anext(stream)
    assert stream.report.keyset_execution is not None
    selected = sum(record.rows_selected for record in stream.report.page_trace)
    assert stream.report.keyset_execution.probe_rows_discarded == selected


@pytest.mark.asyncio
async def test_rejected_finish_page_records_selected_and_discarded_rows() -> None:
    stream = _client(KeysetTransport((1, 2, 3), ignore_bounds=True, default_limit=1)).iter_list_keyset(
        Request("item.list"),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=1,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=AutoKeysetExecution(StableIntegerKeysetContract()),
    )

    with pytest.raises(IncompleteTraversalError):
        _ = [row async for row in stream]
    finish = [record for record in stream.report.page_trace if record.phase is KeysetPhase.FINISH]
    assert len(finish) == 1
    assert finish[0].rows_selected == 1
    assert finish[0].outcome is PageOutcome.REJECTED
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.probe_rows_discarded == 2


@pytest.mark.asyncio
async def test_default_partitioned_streams_selection_larger_than_row_buffer() -> None:
    identities = tuple(range(1, 50_001))
    stream = _client(KeysetTransport(identities, default_limit=50)).iter_list_keyset(
        Request("item.list"),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=50,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=PartitionedKeysetExecution(StableIntegerKeysetContract(), target_lanes=20),
    )

    assert [row["id"] async for row in stream] == list(identities)
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.buffered_rows_high_water <= ExecutionPolicy().max_buffered_rows


@pytest.mark.asyncio
async def test_later_partition_lanes_are_not_rescheduled_while_frontier_is_open() -> None:
    stream = _stream(
        KeysetTransport(tuple(range(1, 201))),
        PartitionedKeysetExecution(StableIntegerKeysetContract(), target_lanes=3),
    )
    scheduler = stream._source._scheduler
    await scheduler.plan_barrier()

    await scheduler._body_wave()
    frontier = scheduler._lane_index
    frontier_ordinal = scheduler._lanes[frontier].spec.ordinal
    first_rounds = {lane.spec.ordinal: lane.rounds for lane in scheduler._lanes}
    assert scheduler._lanes[frontier].status.value == "open"
    await scheduler._body_wave()
    second_rounds = {lane.spec.ordinal: lane.rounds for lane in scheduler._lanes}

    assert second_rounds[frontier_ordinal] > first_rounds[frontier_ordinal]
    assert all(rounds <= second_rounds[frontier_ordinal] for rounds in second_rounds.values())
    await stream.aclose()
