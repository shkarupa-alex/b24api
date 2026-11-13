"""Deterministic correctness tests for opt-in fast integer keysets."""

# ruff: noqa: ANN202, D102, D107, FBT003, PLR2004, SLF001

from __future__ import annotations
import asyncio
import dataclasses
import json
from dataclasses import replace
from typing import TYPE_CHECKING, cast
from urllib.parse import parse_qs

import pytest

from b24api import (
    AutoKeysetExecution,
    Bitrix24,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    ParameterPath,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    Request,
    ResultSelector,
    SequentialKeysetExecution,
    StableIntegerKeysetContract,
    TerminalState,
)
from b24api.cli import _report_json
from b24api.completion import CompletionGate
from b24api.contracts import (
    ClosureWitness,
    ConsistencyPolicy,
    KeysetAssuranceSource,
    KeysetExecutionKind,
    KeysetPageCompletion,
    KeysetPhase,
    KeysetSelectionReason,
    KeysetSelectionSummary,
    PageAcknowledged,
    PageDelivered,
    PageOutcome,
    PageRejectionCode,
    PageScheduled,
    PageValidated,
    TotalHintMode,
    TraceClass,
)
from b24api.contracts.page import AdaptedPage, PageView
from b24api.contracts.policy import IdentityRequirement, OrderSemantics, TotalSemantics
from b24api.contracts.report import OperationReport, PageDispatch
from b24api.contracts.request import RouteKind
from b24api.errors import (
    AmbiguousExecutionError,
    BudgetExceededError,
    CapabilityError,
    EnvelopeContractError,
    IdentityContractError,
    IncompleteTraversalError,
    PaginationError,
    ProtocolError,
    ResultShapeError,
)
from b24api.execution import WireResponse
from b24api.traversal import keyset_page_validation, keyset_plan
from b24api.traversal.keyset_auto import AnchorFacts, BoundaryFacts, Preselected, SelectorInputs, finalize, preselect
from b24api.traversal.keyset_fast_plan import plan_lanes_from_anchors, plan_windows
from b24api.traversal.keyset_geometry import anchor_guesses, anchor_probe_count, estimates, partition_estimate
from b24api.traversal.keyset_observation import FastTraceRecorder, PageObservation
from tests.scripting import client_for

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
        if self.fault == "no_envelope":
            return WireResponse(response.status_code, response.headers, b'{"foo":1}')
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


class MalformedCommandResultTransport(KeysetTransport):
    """Replace one correlated command result with a non-collection value."""

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        response = await super().send(
            request,
            attempt_timeout=attempt_timeout,
            max_response_bytes=max_response_bytes,
        )
        if request.method != "batch":
            return response
        payload = json.loads(response.body)
        results = payload["result"]["result"]
        results[next(iter(results))] = {"not": "a sequence"}
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


class BrokenIdentityTransport(KeysetTransport):
    """Serve one identity as a non-canonical decimal string wherever it is selected."""

    def __init__(self, identities: tuple[int, ...], *, broken: int) -> None:
        super().__init__(identities)
        self.broken = broken

    def _rows(self, parameters: dict[str, JsonValue]) -> list[dict[str, int]]:
        rows = super()._rows(parameters)
        broken = cast("dict[str, int]", {"id": f"0{self.broken}"})
        return [broken if row["id"] == self.broken else row for row in rows]


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


def _sends_no_canary(report: OperationReport | None) -> bool:
    """Fast traversal never schedules a canary page: every recorded page belongs to another phase."""
    assert report is not None
    return all(record.phase is not KeysetPhase.CANARY for record in report.page_trace)


def _client(transport: KeysetTransport, *, policy: ExecutionPolicy | None = None) -> Bitrix24:
    return client_for(transport, policy=policy)


def _identity() -> IdentitySpec:
    return IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER)


def _stream(
    transport: KeysetTransport,
    execution: RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution,
    *,
    direction: str = "ascending",
    writable_limit: bool = True,
    policy: ExecutionPolicy | None = None,
):
    return _client(transport, policy=policy).iter_list_keyset(
        Request("item.list", parameters={"filter": {"STATUS": "open"}}, route=RouteKind.BARE),
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


@pytest.mark.parametrize("capacity", range(1, 11))
@pytest.mark.parametrize("target_lanes", range(2, 21))
def test_zero_canary_partition_planning_always_has_a_positive_exact_wave_count(
    capacity: int,
    target_lanes: int,
) -> None:
    inputs = SelectorInputs(
        BoundaryFacts(5, 5, 1, 5, 996, 1000, False, False),
        effective_page_cap=5,
        batch_capacity=capacity,
        available_rows=10_000,
        target_lanes=target_lanes,
        max_range_waves=20,
        range_window_width=None,
        completion=KeysetPageCompletion.EMPTY_CONFIRMATION,
        advisory_total=None,
    )
    _sequential, range_estimate, partition, _geometry = estimates(inputs, finish_requests=1)
    assert range_estimate.planning_waves == 0
    expected = 1 if target_lanes <= capacity else (target_lanes + capacity - 1) // capacity
    assert partition.planning_waves == expected


@pytest.mark.asyncio
async def test_sequential_keyset_deep_pagination_stays_on_the_direct_no_coalescer_path() -> None:
    transport = KeysetTransport(tuple(range(1, 121)))
    stream = _client(transport).iter_list_keyset(
        Request("item.list", parameters={"filter": {"STATUS": "open"}}, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=1,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=SequentialKeysetExecution(),
    )
    assert len([row async for row in stream]) == 120
    assert len(transport.requests) == 121
    assert all(request.method == "item.list" for request in transport.requests)


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
            (Preselected.RANGE, KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET, 4099, 822, 922, 19, 2, 249, 17),
        ),
        (
            BoundaryFacts(50, 50, 3, 300, 4400, 4600, False, False),
            KeysetPageCompletion.EMPTY_CONFIRMATION,
            50,
            (Preselected.PROBE_ANCHORS, KeysetSelectionReason.WIDE_SPAN_PARTITIONING, 4099, 822, 922, 19, 5, 50, 84),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 71, 120, False, False),
            KeysetPageCompletion.EMPTY_CONFIRMATION,
            50,
            (Preselected.SEQUENTIAL, KeysetSelectionReason.SMALL_SELECTION, 20, 20, 120, 3, 3, 21, 1),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 151, 200, False, False),
            KeysetPageCompletion.EMPTY_CONFIRMATION,
            50,
            (Preselected.SEQUENTIAL, KeysetSelectionReason.INSUFFICIENT_PREDICTED_GAIN, 100, 100, 200, 4, 3, 50, 3),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 151, 200, False, False),
            KeysetPageCompletion.SHORT_PAGE_EXHAUSTS,
            50,
            (Preselected.RANGE, KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET, 100, 100, 200, 4, 2, 50, 3),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 301, 350, False, False),
            KeysetPageCompletion.SHORT_PAGE_EXHAUSTS,
            50,
            (Preselected.RANGE, KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET, 250, 250, 350, 7, 2, 50, 6),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 301, 350, False, False),
            KeysetPageCompletion.EMPTY_CONFIRMATION,
            50,
            (Preselected.RANGE, KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET, 250, 250, 350, 7, 3, 50, 6),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 510000, 1000000, False, False),
            KeysetPageCompletion.SHORT_PAGE_EXHAUSTS,
            50,
            (Preselected.RANGE, KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET, 509949, 105, 205, 5, 2, 245025, 3),
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
                18,
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
    inputs = SelectorInputs(boundary, 50, capacity, 50 * capacity, 20, 2, None, completion, None)
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
            (0, 2, 0, 0, True),
            (4, 21, 2, 1, 1),
        ),
        (BoundaryFacts(42, 42, 1, 42, 1, 42, True, False), KeysetPageCompletion.EMPTY_CONFIRMATION, 50): (
            (1, 2, 0, 0, True),
            (4, 21, 2, 1, 1),
        ),
        (BoundaryFacts(50, 50, 3, 300, 4400, 4600, False, False), KeysetPageCompletion.SHORT_PAGE_EXHAUSTS, 50): (
            (50, 1, 1, 0, True),
            (3, 21, 1, 1, 1),
        ),
        (BoundaryFacts(50, 50, 3, 300, 4400, 4600, False, False), KeysetPageCompletion.EMPTY_CONFIRMATION, 50): (
            (10, 2, 2, 0, False),
            (4, 21, 2, 1, 1),
        ),
        (BoundaryFacts(50, 50, 1, 50, 71, 120, False, False), KeysetPageCompletion.EMPTY_CONFIRMATION, 50): (
            (20, 2, 1, 0, True),
            (4, 21, 2, 1, 1),
        ),
        (BoundaryFacts(50, 50, 1, 50, 151, 200, False, False), KeysetPageCompletion.EMPTY_CONFIRMATION, 50): (
            (49, 2, 1, 0, True),
            (4, 21, 2, 1, 1),
        ),
        (BoundaryFacts(50, 50, 1, 50, 151, 200, False, False), KeysetPageCompletion.SHORT_PAGE_EXHAUSTS, 50): (
            (49, 1, 1, 0, True),
            (3, 21, 1, 1, 1),
        ),
        (BoundaryFacts(50, 50, 1, 50, 301, 350, False, False), KeysetPageCompletion.SHORT_PAGE_EXHAUSTS, 50): (
            (49, 1, 1, 0, True),
            (3, 21, 1, 1, 1),
        ),
        (BoundaryFacts(50, 50, 1, 50, 301, 350, False, False), KeysetPageCompletion.EMPTY_CONFIRMATION, 50): (
            (49, 2, 1, 0, True),
            (4, 21, 2, 1, 1),
        ),
        (BoundaryFacts(50, 50, 1, 50, 510000, 1000000, False, False), KeysetPageCompletion.SHORT_PAGE_EXHAUSTS, 50): (
            (50, 1, 1, 0, True),
            (3, 21, 1, 1, 1),
        ),
        (BoundaryFacts(50, 50, 3, 300, 4400, 4600, False, False), KeysetPageCompletion.SHORT_PAGE_EXHAUSTS, 1): (
            (50, 1, 17, 0, False),
            (42, 21, 1, 21, 20),
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
async def test_fast_keyset_schedules_pages_before_send_and_finishes_through_gate() -> None:
    class ProbeTransport(KeysetTransport):
        def __init__(self) -> None:
            super().__init__(tuple(range(1, 18)))
            self.events: list[type[object]] = []

        async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
            assert self.events
            assert self.events[-1] is PageScheduled
            return await super().send(request, attempt_timeout=attempt_timeout, max_response_bytes=max_response_bytes)

    transport = ProbeTransport()
    stream = _stream(transport, RangeKeysetExecution(StableIntegerKeysetContract()))
    gate = stream._source.completion_gate
    assert isinstance(gate, CompletionGate)
    original_emit = gate.emit

    def observe(event: object) -> None:
        transport.events.append(type(event))
        original_emit(event)

    gate.emit = observe
    assert [row["id"] async for row in stream] == list(range(1, 18))
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert gate.decision().pages_scheduled == gate.decision().pages_acknowledged
    assert gate.decision().pages_scheduled == len([event for event in transport.events if event is PageScheduled])
    assert gate.decision().pages_acknowledged == len([event for event in transport.events if event is PageAcknowledged])
    assert gate.finish() == stream.report


@pytest.mark.asyncio
async def test_fast_page_is_acknowledged_only_after_its_last_row_is_yielded() -> None:
    stream = _stream(KeysetTransport(tuple(range(1, 18))), RangeKeysetExecution(StableIntegerKeysetContract()))
    gate = stream._source.completion_gate
    events: list[object] = []
    original_emit = gate.emit

    def observe(event: object) -> None:
        events.append(event)
        original_emit(event)

    gate.emit = observe
    iterator = stream.__aiter__()
    assert (await anext(iterator))["id"] == 1
    first_page = next(event.page_id for event in events if isinstance(event, PageValidated) and event.row_count == 5)
    assert not any(
        isinstance(event, PageDelivered | PageAcknowledged) and event.page_id == first_page for event in events
    )
    for expected in range(2, 6):
        assert (await anext(iterator))["id"] == expected
    assert any(isinstance(event, PageAcknowledged) and event.page_id == first_page for event in events)
    await stream.aclose()


@pytest.mark.asyncio
async def test_fast_partial_page_close_retires_gate_pages_without_protocol_violation() -> None:
    stream = _stream(KeysetTransport(tuple(range(1, 18))), RangeKeysetExecution(StableIntegerKeysetContract()))
    assert (await anext(stream))["id"] == 1
    await stream.aclose()
    assert stream.report is not None
    assert stream.report.state is TerminalState.EARLY_CLOSED
    gate = stream._source.completion_gate
    decision = gate.decision()
    assert decision.pages_scheduled > decision.pages_acknowledged
    assert not decision.violations
    assert gate.finish() == stream.report


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
    assert _sends_no_canary(stream.report)
    assert stream.report.keyset_execution.assurance_source is KeysetAssuranceSource.CALLER_ASSERTED_BOUNDS
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
    assert _sends_no_canary(stream.report)
    assert stream.report.physical_requests == 5


@pytest.mark.asyncio
async def test_omitted_execution_defaults_to_auto() -> None:
    identities = tuple(range(1, 80))
    stream = _client(KeysetTransport(identities)).iter_list_keyset(
        Request("item.list", route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE_SIZE,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
    )

    assert [row["id"] async for row in stream] == list(identities)
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.requested_kind is KeysetExecutionKind.AUTO


def test_omitted_execution_rejects_fast_ineligible_request_synchronously() -> None:
    transport = KeysetTransport((1, 2, 3))
    client = _client(transport)

    with pytest.raises(CapabilityError):
        client.iter_list_keyset(
            Request("item.list", parameters={"filter": {">id": 1}}, route=RouteKind.BARE),
            selector=ResultSelector.root(),
            identity=_identity(),
        )

    assert transport.requests == []


@pytest.mark.asyncio
async def test_omitted_execution_fails_closed_on_direction_contradiction() -> None:
    transport = KeysetTransport(tuple(range(1, 31)), ignore_direction=True)
    stream = _client(transport).iter_list_keyset(
        Request("item.list", parameters={"filter": {"STATUS": "open"}}, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE_SIZE,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
    )

    with pytest.raises(IncompleteTraversalError, match="range_contradiction"):
        await anext(stream)

    assert stream.report.emitted == 0
    assert stream.report.state is TerminalState.INCOMPLETE


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
    assert _sends_no_canary(stream.report)


def test_post_probe_range_preferred_transition_is_pinned() -> None:
    inputs = SelectorInputs(
        BoundaryFacts(50, 50, 1, 50, 5_051, 5_100, False, False),
        50,
        30,
        100_000,
        20,
        10,
        None,
        KeysetPageCompletion.EMPTY_CONFIRMATION,
        None,
    )
    selection = preselect(inputs)

    final = finalize(inputs, selection, AnchorFacts((), 20))

    assert selection.plan is Preselected.PROBE_ANCHORS
    assert final.kind is KeysetExecutionKind.RANGE
    assert final.reason is KeysetSelectionReason.POST_PROBE_RANGE_PREFERRED
    assert (final.estimate.requests, final.estimate.pages) == (9, 207)  # 4 groups of 103 windows, 2 pages deep
    # 103 windows and the finish page no longer fit once the probes spent the budget.
    assert finalize(replace(inputs, remaining_pages=104), selection, AnchorFacts((), 20)) == final
    short = finalize(replace(inputs, remaining_pages=103), selection, AnchorFacts((), 20))
    assert short.reason is KeysetSelectionReason.POST_PROBE_GAIN_LOST


def _two_eras(rows: int) -> tuple[int, ...]:
    """Two dense halves of ``rows`` identities that a single jump separates."""
    half = rows // 2
    return (*range(1, half + 1), *range(half + 100_000, half + 100_000 + half))


def _head_dense(rows: int) -> tuple[int, ...]:
    """Dense boundary pages around an interior that holds every twentieth identity."""
    interior = range(41, 41 + 20 * (rows - 40), 20)
    return (*range(1, 21), *interior, *range(interior[-1] + 1, interior[-1] + 21))


@pytest.mark.parametrize(
    ("page_cap", "available_rows", "capacity", "tail_min", "expected"),
    [
        (50, 100_000, 48, 20_001, 20),  # target lanes fit a wave of the buffer
        (50, 30_000, 48, 20_001, 32),  # finer lanes, one wave of probes
        (50, 2_400, 48, 20_001, 96),  # two waves of probes bound the count
        (50, 2_400, 48, 4_001, 79),  # a wave of 47-row lanes fits, but a lane never holds less than a page
        (50, 500, 10, 20_001, 20),  # the ceiling never drops below target lanes
        (2, 30, 15, 20_001, 28),  # the buffer retains 28 anchors beside one page
        (5, 2_400, 48, 4_006, 84),  # 96 retained anchors leave 47 lanes 48 rows each beside a page
    ],
)
def test_auto_probes_anchors_until_a_wave_of_lanes_fits_the_buffer(
    page_cap: int,
    available_rows: int,
    capacity: int,
    tail_min: int,
    expected: int,
) -> None:
    inputs = SelectorInputs(
        BoundaryFacts(page_cap, page_cap, 1, page_cap, tail_min, tail_min - 1 + page_cap, False, False),
        page_cap,
        capacity,
        available_rows,
        20,
        2,
        None,
        KeysetPageCompletion.EMPTY_CONFIRMATION,
        None,
    )
    selection = preselect(inputs)
    partition = selection.partition_estimate
    assert partition is not None
    assert anchor_probe_count(inputs, selection.interior_rows_estimate, finish_requests=1) == expected
    assert (selection.anchor_probes, partition.probes) == (expected, expected)
    assert partition.planning_waves == -(-expected // capacity)


@pytest.mark.parametrize(
    ("anchors", "empty_below", "kind", "requests"),
    [
        ((400, 1_600), (), KeysetExecutionKind.SEQUENTIAL, 41),  # deepest lane 1,199 rows
        ((400, 1_600), ((1_600, 1_000),), KeysetExecutionKind.PARTITIONED, 15),  # a probe saw 1,000..1,600 empty
        ((1_000, 1_100), (), KeysetExecutionKind.PARTITIONED, 21),  # 949 rows below the first anchor: 18 full pages
    ],
)
def test_post_probe_estimate_follows_the_deepest_lane_the_probes_left(
    anchors: tuple[int, ...],
    empty_below: tuple[tuple[int, int], ...],
    kind: KeysetExecutionKind,
    requests: int,
) -> None:
    inputs = SelectorInputs(
        BoundaryFacts(50, 50, 1, 50, 2_001, 2_050, False, False),
        50,
        10,
        1_000,
        20,
        2,
        None,
        KeysetPageCompletion.EMPTY_CONFIRMATION,
        None,
    )
    selection = preselect(inputs)

    final = finalize(inputs, selection, AnchorFacts(anchors, 0, empty_below))

    assert selection.plan is Preselected.PROBE_ANCHORS
    assert (final.kind, final.estimate.requests) == (kind, requests)


@pytest.mark.parametrize(
    ("tail_min", "capacity", "budget", "plan"),
    [
        (551, 10, (None, 11), Preselected.SEQUENTIAL),  # 11 windows and the finish page cannot fit
        (551, 10, (None, 12), Preselected.RANGE),
        (551, 10, (2, None), Preselected.SEQUENTIAL),  # two waves of windows and the finish
        (551, 10, (3, None), Preselected.RANGE),
        (5_051, 30, (None, 21), Preselected.SEQUENTIAL),  # 20 probes, a lane and the finish page cannot fit
        (5_051, 30, (None, 22), Preselected.PROBE_ANCHORS),
        (5_051, 30, (2, None), Preselected.SEQUENTIAL),  # a probe wave, a lane, the finish
        (5_051, 30, (3, None), Preselected.PROBE_ANCHORS),
    ],
)
def test_auto_refuses_only_fast_plans_whose_certain_cost_exceeds_the_budget(
    tail_min: int,
    capacity: int,
    budget: tuple[int | None, int | None],
    plan: Preselected,
) -> None:
    inputs = SelectorInputs(
        BoundaryFacts(50, 50, 1, 50, tail_min, tail_min + 49, False, False),
        50,
        capacity,
        100_000,
        20,
        10,
        None,
        KeysetPageCompletion.EMPTY_CONFIRMATION,
        None,
    )
    free = preselect(inputs)
    estimate = free.range_estimate if plan is Preselected.RANGE else free.partition_estimate
    assert estimate is not None
    assert estimate.pages > 22  # the estimate plays no part

    assert preselect(replace(inputs, remaining_requests=budget[0], remaining_pages=budget[1])).plan is plan


@pytest.mark.parametrize(
    ("budget", "kind"),
    [
        ((None, 4), KeysetExecutionKind.PARTITIONED),  # 3 lanes and the finish page
        ((None, 3), KeysetExecutionKind.SEQUENTIAL),
        ((2, None), KeysetExecutionKind.PARTITIONED),  # a wave of lanes and the finish
        ((1, None), KeysetExecutionKind.SEQUENTIAL),
    ],
)
def test_post_probe_partitioning_needs_a_page_for_every_lane_it_found(
    budget: tuple[int | None, int | None],
    kind: KeysetExecutionKind,
) -> None:
    inputs = SelectorInputs(
        BoundaryFacts(50, 50, 1, 50, 2_001, 2_050, False, False),
        50,
        10,
        1_000,
        20,
        2,
        None,
        KeysetPageCompletion.EMPTY_CONFIRMATION,
        None,
    )
    final = finalize(
        replace(inputs, remaining_requests=budget[0], remaining_pages=budget[1]),
        preselect(inputs),
        AnchorFacts((1_000, 1_100), 0),
    )

    assert final.kind is kind


@pytest.mark.parametrize(
    ("anchors", "empty_below", "budget", "kind"),
    [
        # 3 lanes as deep as the 600 rows left between the anchors: 12 full and 2 end pages each, and
        # the finish. Sequential traversal of what the probes left, with the lane ends, takes only 37.
        ((400, 1_600), ((1_600, 1_000),), (None, 43), KeysetExecutionKind.PARTITIONED),
        ((400, 1_600), ((1_600, 1_000),), (None, 42), KeysetExecutionKind.SEQUENTIAL),
        # Sequential traversal of the 1,199 rows the probes left, the 2 anchors and the tail page takes
        # 26 pages and the finish; the 3 lanes add 2 end pages each, the partitioned plan its finish.
        # Its own estimate, 3 lanes of 425 rows, is 31.
        ((475, 1_650), ((1_650, 900),), (None, 34), KeysetExecutionKind.PARTITIONED),
        ((475, 1_650), ((1_650, 900),), (None, 33), KeysetExecutionKind.SEQUENTIAL),
        ((400, 1_600), ((1_600, 1_000),), (2, None), KeysetExecutionKind.PARTITIONED),  # a lane wave, the finish
        ((400, 1_600), ((1_600, 1_000),), (1, None), KeysetExecutionKind.SEQUENTIAL),
    ],
)
def test_post_probe_switch_to_partitioning_needs_the_pages_of_sequential_traversal_and_of_its_own_plan(
    anchors: tuple[int, ...],
    empty_below: tuple[tuple[int, int], ...],
    budget: tuple[int | None, int | None],
    kind: KeysetExecutionKind,
) -> None:
    inputs = SelectorInputs(
        BoundaryFacts(50, 50, 1, 50, 2_001, 2_050, False, False),
        50,
        10,
        1_000,
        20,
        2,
        None,
        KeysetPageCompletion.EMPTY_CONFIRMATION,
        None,
    )
    selection = preselect(inputs)
    # At boundary density the rows between the anchors take too many waves; the probe that saw the
    # range below the second anchor empty is what makes partitioning pay off.
    blind = finalize(inputs, selection, AnchorFacts(anchors, 0))
    assert blind.kind is KeysetExecutionKind.SEQUENTIAL

    final = finalize(
        replace(inputs, remaining_requests=budget[0], remaining_pages=budget[1]),
        selection,
        AnchorFacts(anchors, 0, empty_below),
    )

    assert final.kind is kind


def _sparse_edged_eras(step: int, rows: int, gap: int) -> tuple[int, ...]:
    """Two dense eras of ``rows`` identities ``gap`` apart, between boundary pages of every ``step``-th."""
    head = range(1, PAGE_SIZE * step + 1, step)
    first = range(head[-1] + 1, head[-1] + 1 + rows)
    second = range(first[-1] + gap, first[-1] + gap + rows)
    tail = range(second[-1] + step, second[-1] + step + PAGE_SIZE * step, step)
    return (*head, *first, *second, *tail)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("identities", "max_pages"),
    [
        (_sparse_edged_eras(10, 500, 100_000), 224),
        (_sparse_edged_eras(10, 300, 50_000), 144),
        (_sparse_edged_eras(4, 400, 20_000), 184),
    ],
)
async def test_auto_keeps_sequential_traversal_the_budget_fits_when_the_boundary_pages_undercount_the_interior(
    identities: tuple[int, ...],
    max_pages: int,
) -> None:
    # The sparse boundary pages put the eras at a tenth or a quarter of their rows: sequential
    # traversal of what the probes left looks cheap, but the deepest lane's estimate does not fit.
    transport = KeysetTransport(identities)
    stream = _stream(
        transport,
        AutoKeysetExecution(StableIntegerKeysetContract()),
        policy=ExecutionPolicy(max_buffered_rows=250, max_pages=max_pages),
    )

    assert [cast("dict[str, int]", row)["id"] async for row in stream] == list(identities)
    report = stream.report
    assert report is not None
    keyset = report.keyset_execution
    assert keyset is not None
    assert (keyset.selected_kind, keyset.final_selection_reason, keyset.anchor_probe_commands) == (
        KeysetExecutionKind.SEQUENTIAL,
        KeysetSelectionReason.POST_PROBE_GAIN_LOST,
        20,
    )
    assert report.logical_pages == max_pages


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("max_pages", "kind", "requests"),
    [
        # The probes left 3,064 pages: the partitioned plan's own estimate, 3 lanes as deep as the
        # 5,098 rows the boundary density puts below the second anchor (1,019 full and 2 end pages
        # each) and the finish. It takes 423 pages in all; sequential traversal takes 422.
        (3_086, KeysetExecutionKind.PARTITIONED, 355),
        (3_085, KeysetExecutionKind.SEQUENTIAL, 402),
    ],
)
async def test_auto_switches_to_partitioning_after_the_probes_only_while_the_budget_covers_its_estimate(
    max_pages: int,
    kind: KeysetExecutionKind,
    requests: int,
) -> None:
    identities = _two_eras(2_000)
    transport = KeysetTransport(identities)
    stream = _stream(
        transport,
        AutoKeysetExecution(StableIntegerKeysetContract()),
        policy=ExecutionPolicy(max_buffered_rows=250, max_pages=max_pages),
    )

    assert [cast("dict[str, int]", row)["id"] async for row in stream] == list(identities)
    report = stream.report
    assert report is not None
    keyset = report.keyset_execution
    assert keyset is not None
    assert (keyset.selected_kind, keyset.anchor_probe_commands, len(transport.requests)) == (kind, 20, requests)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("max_pages", "probes", "requests"),
    [
        # Two boundary pages leave 690: the 20-lane plan's 462 and the 228 more pages of finer lanes.
        (692, 96, 41),
        (691, 20, 227),
    ],
)
async def test_auto_cuts_lanes_finer_at_the_exact_page_budget_edge(max_pages: int, probes: int, requests: int) -> None:
    transport = KeysetTransport(tuple(range(1, 2_001)))
    stream = _stream(
        transport,
        AutoKeysetExecution(StableIntegerKeysetContract()),
        policy=ExecutionPolicy(max_buffered_rows=250, max_pages=max_pages),
    )

    assert len([row async for row in stream]) == 2_000
    report = stream.report
    assert report is not None
    keyset = report.keyset_execution
    assert keyset is not None
    assert (keyset.anchor_probe_commands, len(transport.requests)) == (probes, requests)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("max_requests", "probes"),
    [
        # The boundary request leaves 28: the 20-lane plan's 23 and 5 more of finer lanes. Neither plan
        # finishes in so few, but the boundary request already spent counts against the budget.
        (29, 96),
        (28, 20),
    ],
)
async def test_auto_cuts_lanes_finer_at_the_exact_request_budget_edge(max_requests: int, probes: int) -> None:
    stream = _stream(
        KeysetTransport(tuple(range(1, 2_001))),
        AutoKeysetExecution(StableIntegerKeysetContract()),
        policy=ExecutionPolicy(max_buffered_rows=250, max_requests=max_requests),
    )

    with pytest.raises(IncompleteTraversalError, match="command_failure") as captured:
        _ = [row async for row in stream]
    assert isinstance(captured.value.__cause__, BudgetExceededError)
    report = stream.report
    assert report is not None
    keyset = report.keyset_execution
    assert keyset is not None
    assert keyset.anchor_probe_commands == probes


@pytest.mark.parametrize(
    ("budget", "expected"),
    [
        ((None, None), 96),
        # 20 probes and 21 lanes of 19 full pages, 2 end pages each, and the finish: 462 pages, then
        # 76 more probes and 76 more lanes of 2 end pages each.
        ((None, 690), 96),
        ((None, 689), 20),
        # A probe wave, a group of 19 full and 2 end waves, the finish: 23 requests, then a probe
        # wave and 2 more groups of 2 end waves each.
        ((28, None), 96),
        ((27, None), 20),
    ],
)
def test_auto_cuts_lanes_finer_only_while_the_budget_covers_the_target_lanes_plan_and_their_overhead(
    budget: tuple[int | None, int | None],
    expected: int,
) -> None:
    inputs = SelectorInputs(
        BoundaryFacts(50, 50, 1, 50, 20_001, 20_050, False, False),
        50,
        48,
        2_400,
        20,
        2,
        None,
        KeysetPageCompletion.EMPTY_CONFIRMATION,
        None,
        remaining_requests=budget[0],
        remaining_pages=budget[1],
    )
    selection = preselect(inputs)
    coarse = partition_estimate(inputs, lanes=21, lane_rows=950, probes=20, finish_requests=1)

    assert (selection.interior_rows_estimate, coarse.requests, coarse.pages) == (19_950, 23, 462)
    assert selection.anchor_probes == expected
    assert anchor_probe_count(inputs, selection.interior_rows_estimate, finish_requests=1) == expected


@pytest.mark.parametrize(("descending", "requests"), [(False, 79), (True, 80)])
def test_sequential_estimate_leaves_out_the_boundary_page_already_admitted(
    *,
    descending: bool,
    requests: int,
) -> None:
    inputs = SelectorInputs(
        BoundaryFacts(50, 5, 1, 50, 4_001, 4_006, False, False),
        50,
        48,
        2_400,
        20,
        2,
        None,
        KeysetPageCompletion.EMPTY_CONFIRMATION,
        None,
        descending=descending,
    )
    selection = preselect(inputs)

    # 3,935 rows: ascending traversal admitted the 50-row head page, descending the 5-row tail page.
    assert (selection.total_rows_estimate, selection.sequential_estimate.requests) == (3_935, requests)


@pytest.mark.parametrize(("remaining_pages", "expected"), [(582, 96), (581, 20)])
def test_auto_cuts_lanes_finer_only_while_the_finer_plan_estimate_fits_too(remaining_pages: int, expected: int) -> None:
    inputs = SelectorInputs(
        BoundaryFacts(50, 50, 1, 50, 19_951, 20_000, False, False),
        50,
        48,
        2_400,
        20,
        2,
        None,
        KeysetPageCompletion.SHORT_PAGE_EXHAUSTS,
        None,
        remaining_pages=remaining_pages,
    )
    selection = preselect(inputs)
    # 21 lanes of 948 rows round down to 18 full pages, 97 lanes of 206 rows to 4: the 20-lane plan's
    # 420 pages and 76 more probes and lane ends come to 572, the 96-anchor plan's own estimate to 582.
    coarse = partition_estimate(inputs, lanes=21, lane_rows=948, probes=20, finish_requests=1)
    finer = partition_estimate(inputs, lanes=97, lane_rows=206, probes=96, finish_requests=1)

    assert (selection.interior_rows_estimate, coarse.pages, finer.pages) == (19_900, 420, 582)
    assert selection.anchor_probes == expected


@pytest.mark.parametrize(("remaining_requests", "expected"), [(15, 44), (14, 20)])
def test_auto_cuts_lanes_finer_only_while_the_finer_plan_request_estimate_fits_too(
    remaining_requests: int,
    expected: int,
) -> None:
    inputs = SelectorInputs(
        BoundaryFacts(50, 50, 1, 50, 8_001, 8_050, False, False),
        50,
        22,
        300,
        20,
        2,
        None,
        KeysetPageCompletion.SHORT_PAGE_EXHAUSTS,
        None,
        remaining_requests=remaining_requests,
    )
    selection = preselect(inputs)
    # One group of 21 lanes, 7 full and 1 end page deep, after a probe wave: 10 requests, and a probe
    # wave and two groups of lane ends more come to 13. The 45 lanes of 177 rows fill three groups
    # 3 full pages deep, so the finer plan's own estimate is 15.
    coarse = partition_estimate(inputs, lanes=21, lane_rows=379, probes=20, finish_requests=1)
    finer = partition_estimate(inputs, lanes=45, lane_rows=177, probes=44, finish_requests=1)

    assert (selection.interior_rows_estimate, coarse.requests, finer.requests) == (7_950, 10, 15)
    assert selection.anchor_probes == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("identities", "kind", "probes", "ceiling"),
    [
        (tuple(range(1, 2_001)), KeysetExecutionKind.PARTITIONED, 96, 60),  # D20, 227 requests with 20 big lanes
        (_head_dense(800), KeysetExecutionKind.PARTITIONED, 96, 40),  # 54 with 20 lanes
        (_two_eras(2_000), KeysetExecutionKind.PARTITIONED, 20, 360),  # 402 sequentially after 20 probes
    ],
)
async def test_auto_plans_a_selection_larger_than_the_buffer_in_lanes_a_wave_holds(
    identities: tuple[int, ...],
    kind: KeysetExecutionKind,
    probes: int,
    ceiling: int,
) -> None:
    transport = KeysetTransport(identities)
    stream = _stream(
        transport,
        AutoKeysetExecution(StableIntegerKeysetContract()),
        policy=ExecutionPolicy(max_buffered_rows=250),
    )

    assert [cast("dict[str, int]", row)["id"] async for row in stream] == list(identities)
    report = stream.report
    assert report is not None
    keyset = report.keyset_execution
    assert keyset is not None
    assert (keyset.selected_kind, keyset.anchor_probe_commands) == (kind, probes)
    assert len(transport.requests) <= ceiling


@pytest.mark.asyncio
async def test_auto_partition_estimate_holds_while_a_wave_of_lanes_fits_the_buffer() -> None:
    transport = KeysetTransport(tuple(range(1, 2_001)))
    stream = _stream(
        transport,
        AutoKeysetExecution(StableIntegerKeysetContract()),
        policy=ExecutionPolicy(max_buffered_rows=2_500),
    )

    assert len([row async for row in stream]) == 2_000
    report = stream.report
    assert report is not None
    keyset = report.keyset_execution
    assert keyset is not None
    assert keyset.selected_kind is KeysetExecutionKind.PARTITIONED
    assert keyset.selected_requests_estimate == 12  # the lanes after the boundary page and the probe wave
    assert len(transport.requests) == 13


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("identities", "max_pages"),
    [
        (_head_dense(800), 300),  # estimated at thousands of pages; 20 lanes take 209, finer ones 393
        (tuple(range(1, 2_001)), 485),  # 20 lanes take 424 pages, finer ones 484; the gate asks 692
    ],
)
async def test_auto_keeps_target_lanes_when_finer_lanes_would_not_fit_the_page_budget(
    identities: tuple[int, ...],
    max_pages: int,
) -> None:
    transport = KeysetTransport(identities)
    stream = _stream(
        transport,
        AutoKeysetExecution(StableIntegerKeysetContract()),
        policy=ExecutionPolicy(max_buffered_rows=250, max_pages=max_pages),
    )

    assert [cast("dict[str, int]", row)["id"] async for row in stream] == list(identities)
    report = stream.report
    assert report is not None
    keyset = report.keyset_execution
    assert keyset is not None
    assert (keyset.selected_kind, keyset.anchor_probe_commands) == (KeysetExecutionKind.PARTITIONED, 20)


@pytest.mark.asyncio
async def test_auto_rechecks_the_certain_cost_after_the_probes_spent_pages() -> None:
    stream = _stream(
        KeysetTransport(tuple(range(1, 2_001))),
        AutoKeysetExecution(StableIntegerKeysetContract()),
        policy=ExecutionPolicy(max_buffered_rows=250, max_pages=32),
    )

    with pytest.raises(IncompleteTraversalError, match="budget_exhausted"):
        _ = [row async for row in stream]
    report = stream.report
    assert report is not None
    keyset = report.keyset_execution
    assert keyset is not None
    assert keyset.anchor_probe_commands == 20  # 20 probes, a lane and the finish fit 30 pages
    assert keyset.final_selection_reason is KeysetSelectionReason.POST_PROBE_GAIN_LOST  # 21 lanes do not fit 10


@pytest.mark.asyncio
async def test_auto_leaves_out_the_identity_range_empty_anchor_probes_saw() -> None:
    identities = (*range(1, 2_001), *range(1_000_000, 1_000_005))
    stream = _stream(
        KeysetTransport(identities),
        AutoKeysetExecution(StableIntegerKeysetContract()),
        policy=ExecutionPolicy(max_buffered_rows=250, max_pages=500_000, max_requests=500_000),
    )

    assert [cast("dict[str, int]", row)["id"] async for row in stream] == list(identities)
    report = stream.report
    assert report is not None
    keyset = report.keyset_execution
    assert keyset is not None
    assert keyset.empty_anchor_probes == 95
    assert keyset.selected_kind is KeysetExecutionKind.PARTITIONED
    assert keyset.selected_requests_estimate == 2_086


@pytest.mark.asyncio
async def test_boundary_overlap_releases_duplicate_row_capacity_immediately() -> None:
    stream = _stream(
        KeysetTransport(tuple(range(1, 8))),
        AutoKeysetExecution(StableIntegerKeysetContract()),
    )
    source = stream._source

    assert (await anext(source))["id"] == 1

    scheduler = source._runtime
    assert scheduler.counters.boundary_overlap_rows == 3
    assert scheduler.transactions.buffer_balance == 7
    await source.aclose()


@pytest.mark.asyncio
async def test_automatic_partitioned_keyset_adapts_the_anchor_rows_its_lanes_publish() -> None:
    """A partition lane publishes its retained upper anchor row as a fence; that row is adapted too."""

    class Tag:
        def adapt(self, page: PageView, /) -> AdaptedPage:
            return AdaptedPage([{**dict(cast("dict[str, object]", row)), "adapted": True} for row in page.items])

    stream = _client(KeysetTransport(tuple(range(1, 5001)))).iter_list_keyset(
        Request("item.list", route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=50,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        page_adapter=Tag(),
    )
    rows = [cast("dict[str, object]", row) async for row in stream]

    report = stream.report
    assert report is not None
    assert report.keyset_execution is not None
    assert report.keyset_execution.selected_kind is KeysetExecutionKind.PARTITIONED
    assert report.state is TerminalState.COMPLETED
    assert [row["id"] for row in rows] == list(range(1, 5001))
    assert [row["id"] for row in rows if "adapted" not in row] == []


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
    assert stream.report.keyset_selection == KeysetSelectionSummary(
        KeysetExecutionKind.PARTITIONED,
        KeysetExecutionKind.PARTITIONED,
        KeysetSelectionReason.DEGENERATE_SINGLE_LANE,
    )
    disagreeing = KeysetSelectionSummary(
        KeysetExecutionKind.AUTO,
        KeysetExecutionKind.SEQUENTIAL,
        KeysetSelectionReason.PAGE_STOP,
    )
    with pytest.raises(ValueError, match="same requested and selected kinds"):
        dataclasses.replace(stream.report, keyset_selection=disagreeing)


@pytest.mark.asyncio
async def test_auto_keeps_feasible_range_when_anchor_retention_does_not_fit() -> None:
    identities = (*range(1, 6), 50, 100, 200, 300, 400, *range(507, 512))
    transport = KeysetTransport(identities)
    policy = ExecutionPolicy(max_buffered_rows=30)
    stream = _client(transport, policy=policy).iter_list_keyset(
        Request("item.list", route=RouteKind.BARE),
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
    scheduler = stream._source._runtime
    await scheduler.plan_barrier()
    assert scheduler.transactions.anchor_rows

    while rows := await scheduler.next_rows():
        scheduler.mark_emitted(len(rows))

    assert scheduler.transactions.anchor_rows == {}
    assert scheduler.transactions.anchor_commands == {}
    await scheduler.aclose()


@pytest.mark.asyncio
async def test_auto_discards_precharged_anchor_objects_when_post_probe_gain_is_lost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = keyset_plan.finalize

    def force_sequential(*args: object, **kwargs: object):
        return replace(
            original(*args, **kwargs),
            kind=KeysetExecutionKind.SEQUENTIAL,
            reason=KeysetSelectionReason.POST_PROBE_GAIN_LOST,
        )

    monkeypatch.setattr(keyset_plan, "finalize", force_sequential)
    stream = _stream(
        KeysetTransport(tuple(range(1, 5_001))),
        AutoKeysetExecution(StableIntegerKeysetContract(), target_lanes=20),
    )

    assert (await anext(stream))["id"] == 1
    scheduler = stream._source._runtime
    assert scheduler.planner.facts.selected is KeysetExecutionKind.SEQUENTIAL
    assert scheduler.planner.facts.anchor_count > 0
    assert scheduler.transactions.anchor_rows == scheduler.transactions.anchor_commands == {}
    assert scheduler.transactions.boundary_totals == {}
    assert scheduler.transactions.buffer_balance == PAGE_SIZE
    await stream.aclose()
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.assurance_source is KeysetAssuranceSource.ORDERED_PREFIX_ONLY


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
async def test_fast_wave_invalid_correlation_leaves_every_observation_unknown_as_batch_envelope(fault: str) -> None:
    # An answer that cannot be attributed to its commands proves nothing about them: they may have run.
    stream = _stream(
        MalformedBatchEnvelopeTransport(tuple(range(1, 21)), fault=fault),
        RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    with pytest.raises(AmbiguousExecutionError) as captured:
        _ = [item async for item in stream]

    assert isinstance(captured.value.__cause__, ProtocolError)
    assert stream.report is not None
    assert len(stream.report.page_trace) == 2
    assert {(record.outcome, record.rejection_code) for record in stream.report.page_trace} == {
        (PageOutcome.UNKNOWN, PageRejectionCode.BATCH_ENVELOPE),
    }


@pytest.mark.asyncio
async def test_fast_wave_success_status_without_an_envelope_leaves_every_observation_unknown() -> None:
    # EnvelopeContractError is a ProtocolError since 3.0, so a 2xx physical batch without the Bitrix envelope
    # is recorded as BATCH_ENVELOPE (2.3 recorded COMMAND_FAILURE); the sent wave may have run, so its pages
    # are unknown. The migration guide says so.
    stream = _stream(
        MalformedBatchEnvelopeTransport(tuple(range(1, 21)), fault="no_envelope"),
        RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    with pytest.raises(AmbiguousExecutionError) as captured:
        _ = [item async for item in stream]

    assert isinstance(captured.value.__cause__, EnvelopeContractError)
    assert stream.report is not None
    assert stream.report.page_trace
    assert {(record.outcome, record.rejection_code) for record in stream.report.page_trace} == {
        (PageOutcome.UNKNOWN, PageRejectionCode.BATCH_ENVELOPE),
    }


@pytest.mark.asyncio
async def test_fast_wave_missing_correlation_leaves_command_unknown_and_aborts_decoded_sibling() -> None:
    # Nothing answers for the sent command without an entry, so it may have run.
    stream = _stream(
        MalformedBatchEnvelopeTransport(tuple(range(1, 21)), fault="missing"),
        RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    with pytest.raises(AmbiguousExecutionError):
        _ = [item async for item in stream]

    assert stream.report is not None
    assert len(stream.report.page_trace) == 2
    assert {(record.outcome, record.rejection_code) for record in stream.report.page_trace} == {
        (PageOutcome.UNKNOWN, PageRejectionCode.AMBIGUOUS_EXECUTION),
        (PageOutcome.REJECTED, PageRejectionCode.TRANSACTION_ABORTED),
    }


@pytest.mark.asyncio
async def test_fast_wave_preserves_typed_result_shape_failure() -> None:
    stream = _stream(
        MalformedCommandResultTransport(tuple(range(1, 21))),
        RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    with pytest.raises(ResultShapeError):
        await anext(stream)


@pytest.mark.parametrize(
    ("execution", "selected", "broken", "lane_offset"),
    [
        # Range window (9, 14) holds 10..13: the broken row is the fourth of its lane's first page.
        (RangeKeysetExecution(StableIntegerKeysetContract(), window_width=5), 20, 13, 3),
        # Partition lane (21, 36): 22..26, then 27..31, so 30 is the lane's ninth row.
        (PartitionedKeysetExecution(StableIntegerKeysetContract(), target_lanes=2), 40, 30, 8),
        # Auto picks the sequential finish after the head 1..5: 6..10, then 11..15, so 12 is its seventh row.
        (AutoKeysetExecution(StableIntegerKeysetContract()), 18, 12, 6),
    ],
)
@pytest.mark.asyncio
async def test_fast_lane_row_breaking_its_identity_raises_identity_contract_error(
    execution: RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution,
    selected: int,
    broken: int,
    lane_offset: int,
) -> None:
    stream = _stream(BrokenIdentityTransport(tuple(range(1, selected + 1)), broken=broken), execution)

    with pytest.raises(IncompleteTraversalError, match="identity_contract") as captured:
        _ = [row async for row in stream]

    cause = captured.value.__cause__
    assert isinstance(cause, IdentityContractError)
    assert isinstance(cause, PaginationError)
    assert (cause.path, cause.coercion, cause.observed_type) == (("id",), IdentityCoercion.EXACT_INTEGER, "string")
    assert cause.row_offset == lane_offset
    assert stream.report is not None
    rejected = [record for record in stream.report.page_trace if record.outcome is PageOutcome.REJECTED]
    assert rejected[0].rejection_code is PageRejectionCode.IDENTITY_CONTRACT


class _SeenIdentities:
    """Record every identity value the adapter is shown."""

    def __init__(self) -> None:
        self.seen: list[object] = []

    def adapt(self, page: PageView, /) -> AdaptedPage:
        self.seen.extend(cast("dict[str, object]", item).get("id") for item in page.items)
        return AdaptedPage(page.items)


@pytest.mark.asyncio
async def test_fast_lane_adapter_never_sees_a_row_breaking_its_identity() -> None:
    adapter = _SeenIdentities()
    stream = _client(BrokenIdentityTransport(tuple(range(1, 21)), broken=13)).iter_list_keyset(
        Request("item.list", parameters={"filter": {"STATUS": "open"}}, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE_SIZE,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=RangeKeysetExecution(StableIntegerKeysetContract(), window_width=5),
        page_adapter=adapter,
    )

    with pytest.raises(IncompleteTraversalError, match="identity_contract") as captured:
        _ = [row async for row in stream]

    assert isinstance(captured.value.__cause__, IdentityContractError)
    assert captured.value.__cause__.row_offset == 3
    assert adapter.seen
    assert all(isinstance(value, int) for value in adapter.seen)


@pytest.mark.asyncio
async def test_fast_boundary_row_breaking_its_identity_raises_identity_contract_error() -> None:
    stream = _stream(
        BrokenIdentityTransport(tuple(range(1, 21)), broken=2),
        RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    with pytest.raises(IncompleteTraversalError, match="identity_contract") as captured:
        await anext(stream)

    cause = captured.value.__cause__
    assert isinstance(cause, IdentityContractError)
    assert cause.row_offset == 1


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
    assert stream._source._runtime.transactions.boundary_totals == {}


@pytest.mark.asyncio
async def test_internal_lane_validation_error_is_not_reclassified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = _stream(
        KeysetTransport(tuple(range(1, 20))),
        RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    def fail_selection(*_args: object, **_kwargs: object) -> list[JsonValue]:
        raise RuntimeError("internal validation defect")

    monkeypatch.setattr(keyset_page_validation, "_response_items", fail_selection)

    with pytest.raises(RuntimeError, match="internal validation defect"):
        await anext(stream)


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

    with pytest.raises(IncompleteTraversalError) as captured:
        await anext(stream)

    assert isinstance(captured.value.error, PaginationError)
    assert stream.report.state is TerminalState.INCOMPLETE
    assert stream.report.emitted == 0


@pytest.mark.asyncio
async def test_ignored_numeric_bounds_fail_late_without_runtime_canaries() -> None:
    transport = KeysetTransport(tuple(range(1, 31)), ignore_bounds=True)
    stream = _stream(transport, RangeKeysetExecution(StableIntegerKeysetContract()))

    async def consume() -> list[int]:
        return [row["id"] async for row in stream]

    with pytest.raises(IncompleteTraversalError):
        await consume()
    assert stream.report is not None
    assert stream.report.emitted > 0
    assert stream.report.state is TerminalState.INCOMPLETE
    assert stream.report.keyset_execution is not None
    assert _sends_no_canary(stream.report)
    assert stream.report.keyset_execution.selected_kind is KeysetExecutionKind.RANGE
    assert stream.report.keyset_execution.preselection_reason is KeysetSelectionReason.EXPLICIT_RANGE
    assert stream.report.keyset_execution.assurance_source is KeysetAssuranceSource.CALLER_ASSERTED_BOUNDS
    canaries = [record for record in stream.report.page_trace if record.phase is KeysetPhase.CANARY]
    assert canaries == []


@pytest.mark.asyncio
async def test_small_batch_runtime_still_emits_no_canary_observations() -> None:
    stream = _stream(
        KeysetTransport(tuple(range(1, 31)), ignore_bounds=True),
        RangeKeysetExecution(StableIntegerKeysetContract(), batch_size=2),
    )

    with pytest.raises(IncompleteTraversalError):
        _ = [row async for row in stream]

    canaries = [record for record in stream.report.page_trace if record.phase is KeysetPhase.CANARY]
    assert canaries == []
    assert stream.report.keyset_execution is not None
    assert _sends_no_canary(stream.report)


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
    assert stream.report.keyset_execution.assurance_source is KeysetAssuranceSource.CALLER_ASSERTED_BOUNDS


def test_fast_ineligible_requests_fail_synchronously_without_io() -> None:
    transport = KeysetTransport((1, 2, 3))
    client = _client(transport)
    with pytest.raises(CapabilityError):
        client.iter_list_keyset(
            Request("item.list", parameters={"filter": {">id": 1}}, route=RouteKind.BARE),
            selector=ResultSelector.root(),
            identity=_identity(),
            execution=RangeKeysetExecution(StableIntegerKeysetContract()),
        )
    with pytest.raises(CapabilityError):
        client.iter_list_keyset(
            Request("item.list", route=RouteKind.BARE),
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
            Request("item.list", route=RouteKind.BARE),
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
    request = Request("item.list", parameters={"filter": {"STATUS": "open"}}, route=RouteKind.BARE)
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
    assert stream._source._runtime.plan is None
    assert not stream._source._buffer
    assert stream._source._runtime.transactions.anchor_rows == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", [ExecutionPolicy(max_pages=2), ExecutionPolicy(max_requests=1)])
async def test_fast_runtime_budget_is_loud_incomplete(policy: ExecutionPolicy) -> None:
    transport = KeysetTransport(tuple(range(1, 80)))
    stream = _client(transport, policy=policy).iter_list_keyset(
        Request("item.list", route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE_SIZE,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    with pytest.raises(IncompleteTraversalError):
        _ = [row async for row in stream]

    assert stream.report.state is TerminalState.INCOMPLETE
    assert stream.report.emitted > 0


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
    scheduler = source._runtime
    original = scheduler.adjust_buffer
    entered = asyncio.Event()
    release = asyncio.Event()

    async def pause_cleanup(delta: int) -> None:
        if delta < 0:
            entered.set()
            await release.wait()
        await original(delta)

    monkeypatch.setattr(scheduler, "adjust_buffer", pause_cleanup)
    closing = asyncio.create_task(source.aclose())
    await entered.wait()
    closing.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await closing

    assert scheduler.closed is True
    assert source._closed is True
    assert scheduler.transactions.buffer_balance == 0
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
        Request("item.list", route=RouteKind.BARE),
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
        Request("item.list", route=RouteKind.BARE),
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
        Request("item.list", route=RouteKind.BARE),
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
    scheduler = stream._source._runtime
    await scheduler.plan_barrier()

    assert scheduler.transactions.boundary_totals == {}
    assert scheduler.transactions.buffer_balance == page_size * 2 + 50
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
            Request("item.list", route=RouteKind.BARE),
            selector=ResultSelector.root(),
            identity=_identity(),
            page_size=page_size,
            keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
            execution=execution,
        )

    assert transport.requests == []


@pytest.mark.parametrize("route", [RouteKind.JSON, RouteKind.API_V3])
@pytest.mark.parametrize(
    "execution",
    [RangeKeysetExecution(StableIntegerKeysetContract()), PartitionedKeysetExecution(StableIntegerKeysetContract())],
)
def test_explicit_fast_keyset_rejects_non_bare_route_before_io(
    route: RouteKind,
    execution: RangeKeysetExecution | PartitionedKeysetExecution,
) -> None:
    transport = KeysetTransport(tuple(range(1, 31)))

    with pytest.raises(CapabilityError, match="JSON requests without scoped headers"):
        _client(transport).iter_list_keyset(
            Request("item.list", route=route),
            selector=ResultSelector.root(),
            identity=_identity(),
            page_size=PAGE_SIZE,
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
            Request("item.list", route=RouteKind.BARE),
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
        Request("item.list", route=RouteKind.BARE),
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
        Request("item.list", route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE_SIZE,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    with pytest.raises(IncompleteTraversalError):
        async with asyncio.timeout(0.5):
            _ = [row async for row in stream]

    assert stream.report.state is TerminalState.INCOMPLETE
    assert stream.report.emitted > 0


@pytest.mark.asyncio
async def test_range_windows_are_materialized_one_bounded_group_at_a_time() -> None:
    identities = (*range(1, 6), *range(1_000_001, 1_000_006))
    stream = _stream(
        KeysetTransport(identities),
        RangeKeysetExecution(StableIntegerKeysetContract()),
    )
    scheduler = stream._source._runtime

    await scheduler.plan_barrier()

    assert scheduler.plan is not None
    assert scheduler.plan.range_bounds is not None
    assert scheduler.plan.range_bounds.count == 249_999
    assert len(scheduler.transactions.lanes) == scheduler.batch_capacity
    assert len(scheduler.transactions.lane_rows) == scheduler.batch_capacity
    await stream.aclose()


@pytest.mark.asyncio
async def test_short_page_range_preserves_grouped_wave_cost() -> None:
    identities = tuple(range(1, 351))
    stream = _client(KeysetTransport(identities, default_limit=50)).iter_list_keyset(
        Request("item.list", route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=50,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=AutoKeysetExecution(
            StableIntegerKeysetContract(page_completion=KeysetPageCompletion.SHORT_PAGE_EXHAUSTS),
        ),
    )

    assert [row["id"] async for row in stream] == list(identities)
    assert stream.report.physical_requests == 3
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.selected_kind is KeysetExecutionKind.RANGE
    assert stream.report.keyset_execution.selected_requests_estimate == 2


@pytest.mark.asyncio
async def test_page_cap_one_can_select_range_without_canary_prefix_requirement() -> None:
    identities = tuple(range(1, 41))
    stream = _client(KeysetTransport(identities, default_limit=1)).iter_list_keyset(
        Request("item.list", route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=1,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=AutoKeysetExecution(StableIntegerKeysetContract()),
    )

    assert [row["id"] async for row in stream] == list(identities)
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.selected_kind is KeysetExecutionKind.RANGE
    assert _sends_no_canary(stream.report)


@pytest.mark.asyncio
async def test_explicit_range_with_page_cap_one_needs_no_canary_pair() -> None:
    stream = _client(KeysetTransport(tuple(range(1, 41)), default_limit=1)).iter_list_keyset(
        Request("item.list", route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=1,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    assert [row["id"] async for row in stream] == list(range(1, 41))
    assert stream.report.keyset_execution is not None
    assert _sends_no_canary(stream.report)


@pytest.mark.asyncio
async def test_dense_multi_round_range_keeps_grouped_continuations() -> None:
    identities = tuple(range(1, 126))
    stream = _client(KeysetTransport(identities, default_limit=5)).iter_list_keyset(
        Request("item.list", route=RouteKind.BARE),
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
    assert stream.report.physical_requests == 4


@pytest.mark.asyncio
async def test_auto_uses_asymmetric_boundary_without_runtime_canaries() -> None:
    identities = tuple(range(1, 1_001))
    stream = _client(AsymmetricBoundaryTransport(identities, default_limit=5)).iter_list_keyset(
        Request("item.list", route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=5,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=AutoKeysetExecution(StableIntegerKeysetContract(), target_lanes=2),
    )

    assert [row["id"] async for row in stream] == list(identities)
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.selected_kind is not KeysetExecutionKind.SEQUENTIAL
    assert _sends_no_canary(stream.report)


@pytest.mark.asyncio
async def test_anchor_failure_accounts_for_every_staged_probe_row() -> None:
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
        NthBatchMissingResultTransport(tuple(range(1, 101)), batch_ordinal=2),
        PartitionedKeysetExecution(StableIntegerKeysetContract(), batch_size=5, target_lanes=3),
    )

    with pytest.raises(AmbiguousExecutionError):
        await anext(stream)
    assert stream.report.keyset_execution is not None
    selected = sum(record.rows_selected for record in stream.report.page_trace)
    assert stream.report.keyset_execution.probe_rows_discarded == selected


@pytest.mark.asyncio
async def test_later_anchor_wave_failure_accounts_for_prior_compacted_rows() -> None:
    stream = _stream(
        NthBatchMissingResultTransport(tuple(range(1, 101)), batch_ordinal=3),
        PartitionedKeysetExecution(StableIntegerKeysetContract(), batch_size=2, target_lanes=5),
    )

    with pytest.raises(AmbiguousExecutionError):
        await anext(stream)
    assert stream.report.keyset_execution is not None
    assert stream._source._runtime.counters.raw_rows == 13
    assert stream.report.keyset_execution.probe_rows_discarded == 13


@pytest.mark.asyncio
async def test_rejected_finish_page_records_selected_and_discarded_rows() -> None:
    stream = _client(KeysetTransport((1, 2, 3), ignore_bounds=True, default_limit=1)).iter_list_keyset(
        Request("item.list", route=RouteKind.BARE),
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


@pytest.mark.slow
@pytest.mark.asyncio
async def test_default_partitioned_streams_selection_larger_than_row_buffer() -> None:
    identities = tuple(range(1, 50_001))
    stream = _client(KeysetTransport(identities, default_limit=50)).iter_list_keyset(
        Request("item.list", route=RouteKind.BARE),
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
    scheduler = stream._source._runtime
    await scheduler.plan_barrier()

    await scheduler.body_wave()
    frontier = scheduler.transactions.lane_index
    frontier_ordinal = scheduler.transactions.lanes[frontier].spec.ordinal
    first_rounds = {lane.spec.ordinal: lane.rounds for lane in scheduler.transactions.lanes}
    assert scheduler.transactions.lanes[frontier].status.value == "open"
    await scheduler.body_wave()
    second_rounds = {lane.spec.ordinal: lane.rounds for lane in scheduler.transactions.lanes}

    assert second_rounds[frontier_ordinal] > first_rounds[frontier_ordinal]
    assert all(rounds <= second_rounds[frontier_ordinal] for rounds in second_rounds.values())
    await stream.aclose()
