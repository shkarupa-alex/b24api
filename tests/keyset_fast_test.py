"""Deterministic correctness tests for opt-in fast integer keysets."""

# ruff: noqa: ANN202, D102, D107, FBT003, PLR2004, SLF001

from __future__ import annotations
import asyncio
import dataclasses
import json
import math
import random
import threading
import time
from dataclasses import replace
from fractions import Fraction
from typing import TYPE_CHECKING, Literal, cast
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
    BindingClosure,
    BindingTerminal,
    ClosureWitness,
    CommandSettlement,
    ConsistencyPolicy,
    KeysetAssuranceSource,
    KeysetExecutionKind,
    KeysetPageCompletion,
    KeysetPhase,
    KeysetSelectionReason,
    KeysetSelectionSummary,
    PageAcknowledged,
    PageCommandOutcome,
    PageDelivered,
    PageOutcome,
    PageRejectionCode,
    PageScheduled,
    PageValidated,
    TotalHintMode,
    TraceClass,
)
from b24api.contracts.json import FrozenMapping
from b24api.contracts.page import AdaptedPage, PageView
from b24api.contracts.policy import IdentityRequirement, OrderSemantics, RetryPolicy, TotalSemantics
from b24api.contracts.report import KeysetExecutionReport, OperationReport, PageDispatch
from b24api.contracts.request import RouteKind
from b24api.errors import (
    AmbiguousExecutionError,
    ApiResponseError,
    BudgetExceededError,
    CapabilityError,
    EnvelopeContractError,
    IdentityContractError,
    IncompleteTraversalError,
    PaginationError,
    ProtocolError,
    ResultShapeError,
)
from b24api.execution import ExecutionContext, Executor, RateCoordinator, WireResponse, WorkClass
from b24api.traversal import keyset_auto, keyset_page_validation, keyset_plan
from b24api.traversal.identity import _request_with_controls
from b24api.traversal.keyset_auto import (
    AnchorFacts,
    BoundaryFacts,
    Preselected,
    Preselection,
    SelectorInputs,
    constrain_anchor_preselection,
    finalize,
    preselect,
    probed_runs,
)
from b24api.traversal.keyset_capability import compact_anchor_receipts
from b24api.traversal.keyset_fast_plan import plan_lanes_from_anchors, plan_windows
from b24api.traversal.keyset_geometry import (
    CostEstimate,
    anchor_guesses,
    anchor_probe_count,
    estimates,
    partition_estimate,
)
from b24api.traversal.keyset_observation import FastTraceRecorder, PageObservation
from b24api.traversal.keyset_ordered_admission import OrderedAdmissionState
from b24api.traversal.keyset_page_validation import LaneReceipt
from b24api.traversal.keyset_waves import (
    LanePages,
    LaneShape,
    WaveCost,
    WaveRules,
    lane_pages,
    simulate_lanes,
    wave_members,
    wave_room,
)
from tests.scripting import HANG_BOUND, ResponderTransport, client_for, json_response

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable
    from typing import Any

    from b24api.contracts import JsonValue
    from b24api.contracts.completion import CompletionEvidence
    from b24api.contracts.stream import OperationStream
    from b24api.execution.executor import _Pause, _RequestAttempts

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
        limit = self._limit(parameters)
        return [{"id": value} for value in sorted(selected, reverse=descending)[:limit]]

    def _limit(self, parameters: dict[str, JsonValue]) -> int:
        return int(parameters.get("limit", self.default_limit))

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


class _CappedKeysetTransport(KeysetTransport):
    """A server that returns at most ``cap`` rows a page, whatever the limit asked."""

    def __init__(self, identities: tuple[int, ...], *, cap: int) -> None:
        super().__init__(identities)
        self.cap = cap

    def _limit(self, parameters: dict[str, JsonValue]) -> int:
        return min(super()._limit(parameters), self.cap)


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
    """Replace a command result of the ``corrupt_at``-th batch and of every later one with a non-collection value."""

    corrupt_at = 1

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        response = await super().send(
            request,
            attempt_timeout=attempt_timeout,
            max_response_bytes=max_response_bytes,
        )
        if request.method != "batch" or sum(sent.method == "batch" for sent in self.requests) < self.corrupt_at:
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


def _client(transport: KeysetTransport | ResponderTransport, *, policy: ExecutionPolicy | None = None) -> Bitrix24:
    return client_for(transport, policy=policy)


def _identity() -> IdentitySpec:
    return IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER)


def _stream(
    transport: KeysetTransport | ResponderTransport,
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
        lambda: AutoKeysetExecution(StableIntegerKeysetContract(), target_lanes=1),
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
def test_partition_planning_counts_whole_probe_waves_and_range_planning_none(
    capacity: int,
    target_lanes: int,
) -> None:
    inputs = SelectorInputs(
        BoundaryFacts(5, 5, 1, 5, 996, 1000, False, False),
        effective_page_cap=5,
        batch_capacity=capacity,
        available_rows=10_000,
        target_lanes=target_lanes,
        range_window_width=None,
        completion=KeysetPageCompletion.EMPTY_CONFIRMATION,
        advisory_total=None,
    )
    _sequential, range_estimate, partition, _geometry = estimates(inputs)
    assert range_estimate.probes == 0
    assert partition.probes >= target_lanes
    probe_waves = -(-partition.probes // capacity)
    # Every probe wave, one lane page once the anchors are known, and the finish page.
    assert partition.min_requests == probe_waves + 2
    assert partition.min_pages == partition.probes + 2
    assert not partition.eligible or partition.requests >= partition.min_requests


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


_WIDE = BoundaryFacts(50, 50, 3, 300, 4400, 4600, False, False)


@pytest.mark.parametrize(
    ("boundary", "completion", "capacity", "expected"),
    [
        (
            BoundaryFacts(0, 0, None, None, None, None, False, False),
            KeysetPageCompletion.EMPTY_CONFIRMATION,
            50,
            (Preselected.BOUNDARY_ONLY, KeysetSelectionReason.EMPTY_SELECTION, 0, 0, 0, 2, None, None),
        ),
        (
            BoundaryFacts(42, 42, 1, 42, 1, 42, True, False),
            KeysetPageCompletion.EMPTY_CONFIRMATION,
            50,
            (Preselected.BOUNDARY_ONLY, KeysetSelectionReason.BOUNDARY_OVERLAP, 0, 0, 42, 2, None, None),
        ),
        (
            _WIDE,
            KeysetPageCompletion.SHORT_PAGE_EXHAUSTS,
            50,
            (
                Preselected.RANGE,
                KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET,
                4099,
                822,
                922,
                19,
                (3, 31, 249, 17, True),
                (3, 42, 21, 20, True),
            ),
        ),
        (
            _WIDE,
            KeysetPageCompletion.EMPTY_CONFIRMATION,
            50,
            (
                Preselected.RANGE,
                KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET,
                4099,
                822,
                922,
                19,
                (3, 32, 249, 17, True),
                (4, 59, 21, 20, True),
            ),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 71, 120, False, False),
            KeysetPageCompletion.EMPTY_CONFIRMATION,
            50,
            (
                Preselected.SEQUENTIAL,
                KeysetSelectionReason.SMALL_SELECTION,
                20,
                20,
                120,
                3,
                (2, 2, 21, 1, True),
                (3, 42, 21, 20, True),
            ),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 151, 200, False, False),
            KeysetPageCompletion.EMPTY_CONFIRMATION,
            50,
            (
                Preselected.RANGE,
                KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET,
                100,
                100,
                200,
                4,
                (2, 4, 50, 3, True),
                (3, 42, 21, 20, True),
            ),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 151, 200, False, False),
            KeysetPageCompletion.SHORT_PAGE_EXHAUSTS,
            50,
            (
                Preselected.RANGE,
                KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET,
                100,
                100,
                200,
                4,
                (2, 4, 50, 3, True),
                (3, 42, 21, 20, True),
            ),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 301, 350, False, False),
            KeysetPageCompletion.EMPTY_CONFIRMATION,
            50,
            (
                Preselected.RANGE,
                KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET,
                250,
                250,
                350,
                7,
                (2, 7, 50, 6, True),
                (3, 42, 21, 20, True),
            ),
        ),
        (
            BoundaryFacts(50, 50, 1, 50, 510000, 1000000, False, False),
            KeysetPageCompletion.SHORT_PAGE_EXHAUSTS,
            50,
            (
                Preselected.RANGE,
                KeysetSelectionReason.RANGE_WITHIN_WAVE_BUDGET,
                509949,
                105,
                205,
                5,
                (3, 6, 245025, 3, True),
                (3, 42, 21, 20, True),
            ),
        ),
        (
            _WIDE,
            KeysetPageCompletion.SHORT_PAGE_EXHAUSTS,
            1,
            (
                Preselected.SEQUENTIAL,
                KeysetSelectionReason.INSUFFICIENT_PREDICTED_GAIN,
                4099,
                822,
                922,
                19,
                (18, 18, 249, 17, False),
                (22, 22, 21, 20, False),
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
    """Range and partition costs come from running the body-wave rule over the planned lanes.

    One wave of a 50-command batch holds 50 pages of 50 rows here, so the 17 windows of the wide case
    cost one body wave and one finish request, not the near-sequential count a command-cap model gives.
    Each of the first 16 windows holds a page of rows over 248 values: it needs an empty page to confirm
    its end unless its last row sits right below its fence, at four chances in five, 13 pages in all.
    The last window spans 131 identities, about 27 rows: one short page closes it under short-page
    completion; under empty confirmation it needs that page too, at the same chance, and takes the 14th.
    With one command per batch, every page is its own request and no fast plan pays off.
    """
    selection = preselect(SelectorInputs(boundary, 50, capacity, 50 * capacity, 20, None, completion, None))
    range_estimate = selection.range_estimate
    partition = selection.partition_estimate
    assert range_estimate is not None
    assert partition is not None
    has_estimates = selection.plan is not Preselected.BOUNDARY_ONLY
    assert (
        selection.plan,
        selection.reason,
        selection.interior_span,
        selection.interior_rows_estimate,
        selection.total_rows_estimate,
        selection.sequential_estimate.requests,
        (
            range_estimate.requests,
            range_estimate.pages,
            range_estimate.window_width,
            range_estimate.window_count,
            range_estimate.eligible,
        )
        if has_estimates
        else None,
        (partition.requests, partition.pages, partition.lane_count, partition.probes, partition.eligible)
        if has_estimates
        else None,
    ) == expected


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
    transport = KeysetTransport(tuple(range(1, 16)))
    stream = _stream(
        transport,
        AutoKeysetExecution(StableIntegerKeysetContract(), target_lanes=5),
    )
    assert [item["id"] async for item in stream] == list(range(1, 16))
    assert stream.report is not None
    assert stream.report.keyset_execution is not None
    report = stream.report.keyset_execution
    assert report.selected_kind is KeysetExecutionKind.SEQUENTIAL
    assert report.preselection_reason is KeysetSelectionReason.SMALL_SELECTION
    assert _sends_no_canary(stream.report)
    # Three full pages and the empty one: the requests explicit sequential traversal sends.
    assert stream.report.physical_requests == 4


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
        BoundaryFacts(50, 50, 1, 246, 20_001, 20_246, False, False),
        50,
        10,
        2_500,
        20,
        None,
        KeysetPageCompletion.EMPTY_CONFIRMATION,
        None,
    )
    selection = preselect(inputs)
    empty = AnchorFacts((), 20)

    final = finalize(inputs, selection, empty)

    assert selection.plan is Preselected.PROBE_ANCHORS
    assert final.kind is KeysetExecutionKind.RANGE
    assert final.reason is KeysetSelectionReason.POST_PROBE_RANGE_PREFERRED
    # 81 windows of about 49 rows over 245 values, 10 a wave, and the finish: a short page each, and an empty
    # confirmation unless the window's last row sits right below its fence, at four chances in five.
    assert (final.estimate.requests, final.estimate.pages, final.estimate.min_pages) == (18, 146, 82)
    # Where sequential traversal's 83 pages fit and the range estimate does not, sequential traversal wins.
    assert finalize(replace(inputs, remaining_pages=83), selection, empty).kind is KeysetExecutionKind.SEQUENTIAL
    # Where neither estimate fits, range windows are kept while every window and the finish page fit.
    assert finalize(replace(inputs, remaining_pages=82), selection, empty) == final
    short = finalize(replace(inputs, remaining_pages=81), selection, empty)
    assert short.reason is KeysetSelectionReason.POST_PROBE_GAIN_LOST


def _two_eras(rows: int, gap: int = 100_000) -> tuple[int, ...]:
    """Two dense halves of ``rows`` identities that a single jump of ``gap`` separates."""
    half = rows // 2
    return (*range(1, half + 1), *range(half + gap, half + gap + half))


def _head_dense(rows: int) -> tuple[int, ...]:
    """Dense boundary pages around an interior that holds every twentieth identity."""
    interior = range(41, 41 + 20 * (rows - 40), 20)
    return (*range(1, 21), *interior, *range(interior[-1] + 1, interior[-1] + 21))


@pytest.mark.parametrize(
    ("page_cap", "available_rows", "capacity", "tail_min", "expected", "waves"),
    [
        (50, 100_000, 48, 20_001, 96, 2),  # two waves of probes cut lanes the cost model prices cheaper
        # A probe reads a page: a buffer of two pages carries two probes a wave (the runtime caps the
        # batch capacity at the pages the buffer holds), and the anchors they keep leave one page of
        # room from the eleventh wave on.
        (50, 120, 2, 20_001, 20, 10),
        # Two waves of 10 probes are no finer than the target lanes; the ten anchors the first wave
        # keeps leave room for nine probes, so the twentieth goes in a third wave.
        (50, 500, 10, 20_001, 20, 3),
        (2, 30, 15, 20_001, 20, 2),  # 28 finer lanes cost no fewer requests: the target lanes stay
        (10, 580, 25, 516, 20, 1),  # 50 finer lanes cost as many requests (6) and more pages: the target lanes stay
    ],
)
def test_auto_probes_anchors_until_a_wave_of_lanes_fits_the_buffer(  # noqa: PLR0913
    page_cap: int,
    available_rows: int,
    capacity: int,
    tail_min: int,
    expected: int,
    waves: int,
) -> None:
    inputs = SelectorInputs(
        BoundaryFacts(page_cap, page_cap, 1, page_cap, tail_min, tail_min - 1 + page_cap, False, False),
        page_cap,
        capacity,
        available_rows,
        20,
        None,
        KeysetPageCompletion.EMPTY_CONFIRMATION,
        None,
    )
    selection = preselect(inputs)
    partition = selection.partition_estimate
    assert partition is not None
    probed = anchor_probe_count(
        inputs,
        selection.interior_span,
        selection.interior_rows_estimate,
        request_limit=selection.sequential_estimate.requests,
    )
    assert probed == partition
    assert (selection.anchor_probes, partition.probes) == (expected, expected)
    assert (inputs.probe_waves(expected), partition.min_requests) == (waves, waves + 2)


def _post_probe_inputs() -> SelectorInputs:
    """A 2,050-identity selection whose 30-page budget leaves range windows out and anchor probes in."""
    return SelectorInputs(
        BoundaryFacts(50, 50, 1, 50, 2_001, 2_050, False, False),
        50,
        10,
        1_000,
        20,
        None,
        KeysetPageCompletion.EMPTY_CONFIRMATION,
        None,
        remaining_pages=30,
    )


@pytest.mark.parametrize(
    ("anchors", "empty_below", "kind", "requests"),
    [
        ((400, 1_600), (), KeysetExecutionKind.SEQUENTIAL, 41),  # deepest lane 1,199 rows
        ((400, 1_600), ((1_600, 1_000),), KeysetExecutionKind.PARTITIONED, 13),  # a probe saw 1,000..1,600 empty
        ((1_000, 1_100), (), KeysetExecutionKind.PARTITIONED, 21),  # 949 rows below the first anchor: 19 pages
    ],
)
def test_post_probe_estimate_follows_the_deepest_lane_the_probes_left(
    anchors: tuple[int, ...],
    empty_below: tuple[tuple[int, int], ...],
    kind: KeysetExecutionKind,
    requests: int,
) -> None:
    inputs = _post_probe_inputs()
    selection = preselect(inputs)

    final = finalize(inputs, selection, AnchorFacts(anchors, 0, empty_below))

    assert selection.plan is Preselected.PROBE_ANCHORS
    assert (final.kind, final.estimate.requests) == (kind, requests)


def test_auto_after_probes_takes_partitioning_over_range_windows_that_cost_as_many_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The anchors already fence partitioning's lanes: when range windows cost as many requests, the lanes are kept.
    inputs = _post_probe_inputs()
    selection = preselect(inputs)

    def priced(kind: KeysetExecutionKind) -> Callable[..., CostEstimate]:
        return lambda *_args, **_kwargs: CostEstimate(kind, 5, eligible=True, pages=5)

    monkeypatch.setattr(keyset_auto, "partition_estimate", priced(KeysetExecutionKind.PARTITIONED))
    monkeypatch.setattr(keyset_auto, "range_estimate", priced(KeysetExecutionKind.RANGE))

    final = finalize(inputs, selection, AnchorFacts((400, 1_600), 0))

    assert (final.kind, final.estimate.requests) == (KeysetExecutionKind.PARTITIONED, 5)


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
        None,
        KeysetPageCompletion.EMPTY_CONFIRMATION,
        None,
    )
    free = preselect(inputs)
    estimate = free.range_estimate if tail_min == 551 else free.partition_estimate
    assert estimate is not None
    budgeted = replace(inputs, remaining_requests=budget[0], remaining_pages=budget[1])
    # The plan's minimum cost decides; the estimate, which extrapolates the density, does not.
    assert budgeted.admit(estimate).eligible is (plan is not Preselected.SEQUENTIAL)

    assert preselect(budgeted).plan is plan


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
    inputs = _post_probe_inputs()
    final = finalize(
        replace(inputs, remaining_requests=budget[0], remaining_pages=budget[1]),
        preselect(inputs),
        AnchorFacts((1_000, 1_100), 0),
    )

    assert final.kind is kind


@pytest.mark.parametrize(
    ("remaining_pages", "kind"),
    [
        (45, KeysetExecutionKind.PARTITIONED),  # the partitioned estimate fits
        (44, KeysetExecutionKind.SEQUENTIAL),  # only the sequential estimate fits
        (41, KeysetExecutionKind.SEQUENTIAL),
        (40, KeysetExecutionKind.PARTITIONED),  # neither fits: the plan whose minimum fits and pays off
        (23, KeysetExecutionKind.PARTITIONED),  # a page for each of the 22 lanes and the finish
        (22, KeysetExecutionKind.SEQUENTIAL),
    ],
)
def test_post_probe_switch_to_partitioning_needs_the_pages_of_sequential_traversal_and_of_its_own_plan(
    remaining_pages: int,
    kind: KeysetExecutionKind,
) -> None:
    # 21 anchors leave 22 lanes of about 89 rows: 45 pages, two a lane and the finish; sequential
    # traversal of the same rows takes 41. Windows two identities wide leave range windows out.
    inputs = replace(_post_probe_inputs(), range_window_width=2)
    anchors = AnchorFacts(tuple(range(140, 2_000, 90)), 0)
    final = finalize(replace(inputs, remaining_pages=remaining_pages), preselect(inputs), anchors)

    assert (final.kind, final.estimate.pages) == (kind, 45 if kind is KeysetExecutionKind.PARTITIONED else 41)


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
    # The sparse boundary pages put the eras at a tenth or a quarter of their rows: no plan is taken
    # blind on that density; after the probes, sequential traversal takes exactly the pages left.
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


def _gap_stream(policy: ExecutionPolicy, rows: int = 1_000) -> OperationStream[JsonValue]:
    """Auto over two dense eras 10,000 identities apart, whose empty gap range windows read in full."""
    stream = _stream(
        KeysetTransport(_two_eras(rows, 10_000)), AutoKeysetExecution(StableIntegerKeysetContract()), policy=policy
    )
    return cast("OperationStream[JsonValue]", stream)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("max_pages", "probes", "requests"),
    [
        # Range windows would read the gap too: 2,749 windows in 58 requests. The probes find it first.
        (None, 100, 26),
        # Two boundary pages leave 2,323: the estimate of 100 finer lanes, which cost 24 requests, not 107.
        (2_325, 100, 26),
        (2_324, 20, 103),
    ],
)
async def test_auto_cuts_lanes_finer_at_the_exact_page_budget_edge(
    max_pages: int | None, probes: int, requests: int
) -> None:
    stream = _gap_stream(ExecutionPolicy(max_buffered_rows=2_500, max_pages=max_pages or 10_000))

    assert [cast("dict[str, int]", row)["id"] async for row in stream] == list(_two_eras(1_000, 10_000))
    report = stream.report
    assert report is not None
    keyset = report.keyset_execution
    assert keyset is not None
    assert (keyset.selected_kind, keyset.anchor_probe_commands, report.physical_requests) == (
        KeysetExecutionKind.PARTITIONED,
        probes,
        requests,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("max_requests", "kind"),
    [
        # The boundary request leaves 3: a probe wave, a wave of lanes and the finish page.
        (4, KeysetExecutionKind.PARTITIONED),
        (3, KeysetExecutionKind.SEQUENTIAL),
    ],
)
async def test_auto_cuts_lanes_finer_at_the_exact_request_budget_edge(
    max_requests: int, kind: KeysetExecutionKind
) -> None:
    stream = _gap_stream(ExecutionPolicy(max_buffered_rows=2_500, max_requests=max_requests))

    with pytest.raises(IncompleteTraversalError, match="budget_exhausted") as captured:
        _ = [row async for row in stream]
    assert isinstance(captured.value.__cause__, BudgetExceededError)
    report = stream.report
    assert report is not None
    keyset = report.keyset_execution
    assert keyset is not None
    assert (keyset.selected_kind, report.physical_requests) == (kind, max_requests)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rows", "max_pages", "max_requests", "kind", "reason"),
    [
        # Too few requests for any gain: sequential traversal from the boundary wave, whose next page
        # the request budget refuses.
        (1_000, None, 2, KeysetExecutionKind.SEQUENTIAL, KeysetSelectionReason.INSUFFICIENT_PREDICTED_GAIN),
        # The probes leave no gain within 160 pages: sequential traversal after them, refused likewise.
        (400, 160, 6, KeysetExecutionKind.SEQUENTIAL, KeysetSelectionReason.POST_PROBE_GAIN_LOST),
        # Partitioned: a wave command, then the finish page, are the ones refused.
        (1_000, 2_324, 30, KeysetExecutionKind.PARTITIONED, KeysetSelectionReason.WIDE_SPAN_PARTITIONING),
        (1_000, 2_324, 102, KeysetExecutionKind.PARTITIONED, KeysetSelectionReason.WIDE_SPAN_PARTITIONING),
    ],
    ids=["sequential", "sequential-after-probe", "partitioned-wave", "partitioned-finish"],
)
async def test_auto_ends_with_the_request_budget_as_explicit_sequential_traversal_does(
    rows: int,
    max_pages: int | None,
    max_requests: int,
    kind: KeysetExecutionKind,
    reason: KeysetSelectionReason,
) -> None:
    # A page whose send the budget refused was never settled, and closing the binding rejected it as if
    # it had been: a completion protocol violation instead of the budget's end. A refused wave command
    # was a command failure.
    policy = ExecutionPolicy(max_buffered_rows=2_500, max_requests=max_requests, max_pages=max_pages or 10_000)
    auto = _gap_stream(policy, rows)
    sequential = _stream(KeysetTransport(_two_eras(rows, 10_000)), SequentialKeysetExecution(), policy=policy)  # type: ignore[arg-type]

    with pytest.raises(IncompleteTraversalError) as captured:
        _ = [row async for row in auto]
    with pytest.raises(BudgetExceededError):
        _ = [row async for row in sequential]

    assert isinstance(captured.value.__cause__, BudgetExceededError)
    assert str(captured.value.__cause__) == "physical request budget exhausted"
    report, expected = auto.report, sequential.report
    assert report is not None
    assert expected is not None
    assert (report.state, report.terminal_reason, [item.code for item in report.violations]) == (
        TerminalState.INCOMPLETE,
        "BudgetExceededError",
        ["budget_exhausted"],
    )
    assert (expected.state, expected.terminal_reason) == (report.state, report.terminal_reason)
    assert [(item.code, item.message) for item in expected.violations] == [
        (item.code, item.message) for item in report.violations
    ]
    keyset = report.keyset_execution
    assert keyset is not None
    assert (keyset.selected_kind, keyset.final_selection_reason or keyset.preselection_reason) == (kind, reason)
    # A page the budget kept from its send left no request to record: none is traced as a failed command.
    assert [record for record in report.page_trace if record.outcome is not PageOutcome.COMMITTED] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "duplicate"])
async def test_an_ambiguous_fast_wave_closes_its_binding_unknown(fault: str) -> None:
    # A command that may have run settles unknown, so the binding's closure is unknown, not failed.
    stream = _stream(
        MalformedBatchEnvelopeTransport(tuple(range(1, 21)), fault=fault),
        RangeKeysetExecution(StableIntegerKeysetContract()),
    )

    with pytest.raises(AmbiguousExecutionError):
        _ = [item async for item in stream]

    assert stream.report is not None
    assert [item.code for item in stream.report.violations if item.code.startswith("completion_")] == []


@pytest.mark.parametrize(
    ("budget", "expected"),
    [
        ((None, None), 96),
        ((None, 582), 96),
        ((None, 581), 20),
        ((67, None), 96),
        ((66, None), 20),
    ],
)
def test_auto_cuts_lanes_finer_only_while_the_finer_plan_estimate_fits_too(
    budget: tuple[int | None, int | None],
    expected: int,
) -> None:
    inputs = SelectorInputs(
        BoundaryFacts(50, 50, 1, 50, 20_001, 20_050, False, False),
        50,
        48,
        2_400,
        20,
        None,
        KeysetPageCompletion.EMPTY_CONFIRMATION,
        None,
    )
    # 21 lanes of 950 rows outgrow what a wave of the buffer holds and read nearly one page a request;
    # 97 lanes of 206 rows read 5 pages each, about 48 a wave. The 96 probes take three waves: the 48
    # anchors the first keeps leave room for 47 pages.
    coarse = partition_estimate(inputs, (LaneShape(950, 950, 21),), probes=20, anchors=20, request_limit=10**6)
    finer = partition_estimate(inputs, (LaneShape(206, 206, 97),), probes=96, anchors=96, request_limit=10**6)
    assert ((coarse.requests, coarse.pages), (finer.requests, finer.pages)) == ((218, 420), (67, 582))

    selection = preselect(replace(inputs, remaining_requests=budget[0], remaining_pages=budget[1]))

    assert selection.interior_rows_estimate == 19_950
    assert selection.anchor_probes == expected


@pytest.mark.asyncio
async def test_anchor_probes_take_the_waves_the_cost_model_counts() -> None:
    # 96 probes of a page each beside the default 2,500 buffered rows: 48 go in the first wave, and
    # the 48 anchors it keeps leave room for 47 pages, so the last probe takes a third wave.
    stream = _client(KeysetTransport(_two_eras(20_000, 100_000))).iter_list_keyset(
        Request("item.list", parameters={"filter": {"STATUS": "open"}}, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=50,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
    )
    _ = [row async for row in stream]
    report = stream.report
    assert report is not None
    keyset = report.keyset_execution
    assert keyset is not None
    assert (keyset.anchor_probe_commands, keyset.anchor_probe_requests) == (96, 3)


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
        None,
        KeysetPageCompletion.EMPTY_CONFIRMATION,
        None,
        descending=descending,
    )
    selection = preselect(inputs)

    # 3,935 rows: ascending traversal admitted the 50-row head page, descending the 5-row tail page.
    assert (selection.total_rows_estimate, selection.sequential_estimate.requests) == (3_935, requests)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("identities", "kind", "probes", "ceiling"),
    [
        # 227 requests with 20 big lanes; after 96 probes, windows a wave of the buffer holds.
        (tuple(range(1, 2_001)), KeysetExecutionKind.RANGE, 96, 16),
        (_head_dense(800), KeysetExecutionKind.PARTITIONED, 96, 30),  # 54 with 20 lanes
        # Two lanes of a thousand rows each: the buffer holds 50 of their pages, so the second lane
        # waits for the first and partitioning would save an eighth of the requests, not the gain
        # auto requires. Sequential traversal after the 20 probes.
        (_two_eras(2_000), KeysetExecutionKind.SEQUENTIAL, 20, 402),
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
    assert keyset.selected_requests_estimate == 9  # the lanes after the boundary page and two probe waves
    assert len(transport.requests) == 12


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("identities", "max_pages"),
    [
        # 96 probes and range windows would take 298 pages; 20 lanes take 203.
        (tuple(range(1, 16_001, 20)), 300),
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
    # 95 probes found nothing below the outliers: the estimate prices the one lane up to the lowest of
    # them (about 10,400 values at the density the probe at the head measured), not the million
    # values the boundary density spans. One lane holds every row, so no fast plan gains.
    assert keyset.empty_anchor_probes == 95
    assert (keyset.selected_kind, keyset.final_selection_reason) == (
        KeysetExecutionKind.SEQUENTIAL,
        KeysetSelectionReason.POST_PROBE_GAIN_LOST,
    )
    # The report carries the estimates the plan was chosen on, not the boundary pages' million values.
    assert (keyset.sequential_requests_estimate, keyset.selected_requests_estimate) == (2_086, 2_086)
    assert keyset.interior_rows_estimate is not None
    assert keyset.interior_rows_estimate < 11_000
    # The total is the boundary pages' rows and that interior, not the boundary pages' million values.
    assert keyset.total_rows_estimate is not None
    assert 0 < keyset.total_rows_estimate - keyset.interior_rows_estimate <= 2 * PAGE_SIZE


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

    # Identities scattered at about one in seven: auto partitions them after its probes.
    identities = tuple(sorted(random.Random(11).sample(range(1, 35_000), 5_000)))
    stream = _client(KeysetTransport(identities)).iter_list_keyset(
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
    assert [row["id"] for row in rows] == list(identities)
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
    # The probes found anchors; sequential traversal keeps none of them, and the report counts none.
    assert (scheduler.planner.facts.anchor_probe_commands, scheduler.planner.facts.anchor_count) == (100, 0)
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
async def test_a_fast_traversal_admits_its_finish_page_as_traversal_work(monkeypatch: pytest.MonkeyPatch) -> None:
    # The finish page is a page of the traversal: it waits in the traversal class, as every sequential
    # keyset page does, and does not take the caller's interactive share of the rate.
    classes: list[WorkClass] = []
    acquire = RateCoordinator.acquire

    async def record(coordinator: RateCoordinator, work_class: WorkClass, *args: object, **kwargs: object) -> object:
        classes.append(work_class)
        return await acquire(coordinator, work_class, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(RateCoordinator, "acquire", record)
    transport = KeysetTransport(tuple(range(1, 41)))
    stream = _stream(transport, RangeKeysetExecution(StableIntegerKeysetContract()))

    assert len([row async for row in stream]) == 40
    direct = [request for request in transport.requests if request.method != "batch"]
    assert len(direct) == 1  # the finish page
    assert WorkClass.TRAVERSAL_DIRECT in classes
    assert WorkClass.INTERACTIVE_DIRECT not in classes


def _controlled(parameters: dict[str, JsonValue], execution: object) -> OperationStream[JsonValue]:
    return _client(KeysetTransport(tuple(range(1, 41)))).iter_list_keyset(
        Request("item.list", parameters={"filter": {"STATUS": "open"}, **parameters}, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE_SIZE,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=execution,  # type: ignore[arg-type]
    )


_EXECUTIONS = (
    SequentialKeysetExecution(),
    RangeKeysetExecution(StableIntegerKeysetContract()),
    PartitionedKeysetExecution(StableIntegerKeysetContract(), target_lanes=2),
    AutoKeysetExecution(StableIntegerKeysetContract()),
)


@pytest.mark.asyncio
@pytest.mark.parametrize("execution", _EXECUTIONS, ids=["sequential", "range", "partitioned", "auto"])
async def test_every_keyset_execution_accepts_the_controls_sequential_traversal_writes(execution: object) -> None:
    # A caller may set the order, start and limit controls to what sequential traversal writes on every
    # page; a fast command then writes its own value there (its direction, a probe's page).
    stream = _controlled({"order": {"id": "ASC"}, "start": -1, "limit": PAGE_SIZE}, execution)

    assert [cast("dict[str, int]", row)["id"] async for row in stream] == list(range(1, 41))


@pytest.mark.asyncio
@pytest.mark.parametrize("execution", _EXECUTIONS, ids=["sequential", "range", "partitioned", "auto"])
@pytest.mark.parametrize(
    "parameters",
    [{"order": {"id": "DESC"}}, {"order": {"date": "ASC"}}, {"start": 0}, {"limit": PAGE_SIZE - 1}],
    ids=["direction", "other-order", "start", "limit"],
)
async def test_every_keyset_execution_refuses_a_control_sequential_traversal_would_overwrite(
    execution: object, parameters: dict[str, JsonValue]
) -> None:
    # Fast executions refuse it before returning the stream, sequential traversal at its first page.
    with pytest.raises(CapabilityError):
        _ = [row async for row in _controlled(parameters, execution)]


def test_owned_controls_a_request_lacks_are_written_in_the_order_the_traversal_lists_them() -> None:
    # The wire form of a command must not depend on the interpreter's hash seed.
    paths = tuple(ParameterPath(("filter", f"control{index}")) for index in range(16))
    request = Request("crm.item.list", {"filter": {}}, route=RouteKind.BARE)

    written = _request_with_controls(
        request, dict.fromkeys(paths, 1), allow_create=True, replace=frozenset(paths)
    ).copy_parameters()

    assert list(cast("dict[str, object]", written["filter"])) == [path.path[1] for path in paths]


def _sent_bounds(transport: KeysetTransport) -> list[tuple[object, object]]:
    """Return the identity bounds of every page the transport was asked for, batched or direct."""
    parameters: list[dict[str, JsonValue]] = []
    for request in transport.requests:
        if request.method == "batch":
            commands = request.copy_parameters()["cmd"]
            assert isinstance(commands, dict)
            parameters.extend(_decode_command(value) for value in commands.values() if isinstance(value, str))
        else:
            parameters.append(request.copy_parameters())
    filters = [cast("dict[str, object]", page.get("filter", {})) for page in parameters]
    return [(page.get(">ID"), page.get("<ID")) for page in filters]


@pytest.mark.asyncio
async def test_auto_prices_a_server_that_caps_its_pages_as_one_that_does_not() -> None:
    # Under empty confirmation a short page proves nothing beyond its last row. A server that caps its pages one row
    # below the requested size leaves every probe page short: Auto must price the rows those pages read, as it does
    # for full pages, not take each one as the end of the values below its fence.
    identities = tuple(range(1, 10_001))

    async def traverse(cap: int | None) -> tuple[object, int, int]:
        transport = KeysetTransport(identities) if cap is None else _CappedKeysetTransport(identities, cap=cap)
        stream = _stream(transport, AutoKeysetExecution(StableIntegerKeysetContract()))
        assert [cast("dict[str, int]", row)["id"] async for row in stream] == list(identities)
        keyset = stream.report.keyset_execution
        assert keyset is not None
        assert keyset.selected_requests_estimate is not None
        return keyset.selected_kind, keyset.selected_requests_estimate, len(transport.requests)

    full = await traverse(None)
    capped = await traverse(PAGE_SIZE - 1)

    assert full[0] is KeysetExecutionKind.RANGE
    assert capped == full


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["ascending", "descending"])
async def test_partitioning_sends_nothing_for_the_values_its_first_probe_found_empty(direction: str) -> None:
    # The first probe starts at the lower boundary page, in either direction: the row it finds opens
    # the first lane that can hold rows, and the values between that page and that row are known
    # empty. That lane closes without a request, and the probe also measured the lane above it.
    identities = tuple(range(10, 801, 10))
    transport = KeysetTransport(identities)
    execution = PartitionedKeysetExecution(StableIntegerKeysetContract(), target_lanes=4)
    stream = _stream(transport, execution, direction=direction)

    expected = list(identities) if direction == "ascending" else list(reversed(identities))
    assert [cast("dict[str, int]", row)["id"] async for row in stream] == expected
    bounds = _sent_bounds(transport)
    assert ("50", "760") in bounds  # the probe at the head page
    assert ("50", "60") not in bounds  # no page for the values it found empty
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.actual_lanes == 5


@pytest.mark.asyncio
async def test_auto_whose_boundary_wave_fails_reports_planning_incomplete() -> None:
    # Planning ended before any plan was chosen: the report names no cost decision.
    stream = _stream(
        MalformedCommandResultTransport(tuple(range(1, 21))),
        AutoKeysetExecution(StableIntegerKeysetContract()),
    )

    with pytest.raises(ResultShapeError):
        await anext(stream)
    keyset = stream.report.keyset_execution
    assert keyset is not None
    assert (keyset.selected_kind, keyset.preselection_reason, keyset.final_selection_reason, keyset.anchor_count) == (
        KeysetExecutionKind.AUTO,
        KeysetSelectionReason.PLANNING_INCOMPLETE,
        None,
        0,
    )


class _ProbeWaveMalformedTransport(MalformedCommandResultTransport):
    """Answer the boundary wave, then corrupt the first probe wave."""

    corrupt_at = 2


@pytest.mark.asyncio
async def test_auto_whose_probe_wave_fails_keeps_its_preselection_and_reports_no_final_reason() -> None:
    # The boundary pages were read and Auto chose to probe: that decision stands in the report, and the final one,
    # never made, is absent.
    identities = (*range(1, 51), *range(1_000_000, 1_000_050))
    stream = _stream(_ProbeWaveMalformedTransport(identities), AutoKeysetExecution(StableIntegerKeysetContract()))

    with pytest.raises(ResultShapeError):
        await anext(stream)
    keyset = stream.report.keyset_execution
    assert keyset is not None
    assert (keyset.selected_kind, keyset.preselection_reason, keyset.final_selection_reason) == (
        KeysetExecutionKind.AUTO,
        KeysetSelectionReason.WIDE_SPAN_PARTITIONING,
        None,
    )
    assert keyset.anchor_probe_commands > 0


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
        # Auto reads the interior 6..13 in range windows of four values: 12 is the third row of (9, 14).
        (AutoKeysetExecution(StableIntegerKeysetContract()), 18, 12, 2),
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


class _AscendingViewHidesTheLeast(KeysetTransport):
    """Serve a selection whose ascending view lacks the identities below ``hidden_below``, which descending has."""

    def __init__(self, identities: tuple[int, ...], *, hidden_below: int) -> None:
        super().__init__(identities)
        self.hidden_below = hidden_below

    def _rows(self, parameters: dict[str, JsonValue]) -> list[dict[str, int]]:
        order = parameters.get("order", {})
        if isinstance(order, dict) and order.get("id") == "DESC":
            return super()._rows(parameters)
        visible = self.identities
        self.identities = tuple(value for value in visible if value >= self.hidden_below)
        try:
            return super()._rows(parameters)
        finally:
            self.identities = visible


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["ascending", "descending"])
async def test_a_descending_boundary_below_the_ascending_head_fails_before_emission(direction: str) -> None:
    # The ascending boundary page starts at the selection's least identity, 5, and ends short, so it holds the whole
    # selection; the descending one holds 4 too. Both maxima agree, but one selection cannot have both pages: the
    # boundary refuses them, whichever way the traversal runs, before a row is delivered or a further page is sent.
    transport = _AscendingViewHidesTheLeast(tuple(range(1, 9)), hidden_below=5)
    stream = _stream(transport, AutoKeysetExecution(StableIntegerKeysetContract()), direction=direction)

    with pytest.raises(IncompleteTraversalError, match="descending boundary passes head"):
        await anext(stream)

    assert stream.report is not None
    assert stream.report.emitted == 0
    assert len(transport.requests) == 1  # the boundary wave alone


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
    assert stream._source._runtime.transactions.buffer_balance == 0
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
    assert scheduler.transactions.buffer_balance == 0
    with pytest.raises(StopAsyncIteration):
        await anext(source)


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
    # The boundary pages (10 rows), the first probe wave's two pages (10) and the page the failed
    # wave did return (5): every row read is counted and discarded.
    assert sum(record.rows_selected for record in stream.report.page_trace) == 25
    assert stream.report.keyset_execution.probe_rows_discarded == 25


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


def _fast_execution(kind: str) -> RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution:
    contract = StableIntegerKeysetContract()
    if kind == "range":
        return RangeKeysetExecution(contract)
    return PartitionedKeysetExecution(contract) if kind == "partitioned" else AutoKeysetExecution(contract)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["range", "partitioned", "auto"])
@pytest.mark.parametrize("refused_at", [2, 3, 4])
@pytest.mark.parametrize("refused", ["at_its_plan", "at_its_send"])
async def test_a_wave_retry_the_deadline_refuses_ends_the_traversal_with_the_budget(
    monkeypatch: pytest.MonkeyPatch, kind: str, refused_at: int, refused: str
) -> None:
    # A wave is answered 429 (proven not run, so replayed) and the deadline passes: while the replay round is
    # planned, or once it is planned, as its send is granted its permit. The retry window is as long as the
    # operation, so the operation's deadline refuses the round: the stopped commands are the operation's end, as
    # the refusal they restate is, not failed commands.
    skew = [0.0]
    armed = [False]

    def context(executor: Executor, policy: ExecutionPolicy | None = None) -> ExecutionContext:
        return ExecutionContext(
            policy or ExecutionPolicy(), executor.coordinator, clock=lambda: time.monotonic() + skew[0]
        )

    monkeypatch.setattr(Executor, "context", context)
    acquire = RateCoordinator.acquire

    async def late_grant(coordinator: RateCoordinator, *args: object, **kwargs: object) -> object:
        permit = await acquire(coordinator, *args, **kwargs)  # type: ignore[arg-type]
        if armed[0]:
            skew[0] = 10_000.0
        return permit

    monkeypatch.setattr(RateCoordinator, "acquire", late_grant)
    keyset = KeysetTransport(tuple(range(1, 3001)))

    async def respond(request: Request) -> WireResponse:
        if request.method == "batch" and sum(sent.method == "batch" for sent in transport.requests) == refused_at:
            if refused == "at_its_plan":
                skew[0] = 10_000.0
            armed[0] = True
            return WireResponse(429, (("content-type", "text/plain"),), b"slow down")
        return await keyset.send(request, attempt_timeout=1.0, max_response_bytes=1 << 20)

    transport = ResponderTransport(respond, host=keyset.host)
    policy = ExecutionPolicy(
        max_elapsed=600.0,
        max_retry_elapsed_per_request=600.0,
        retry=RetryPolicy(initial_delay=0.001, maximum_delay=0.001, jitter=0),
    )
    stream = _stream(transport, _fast_execution(kind), policy=policy)

    with pytest.raises(IncompleteTraversalError) as raised:
        _ = [row async for row in stream]

    assert isinstance(raised.value.__cause__, BudgetExceededError)
    if refused == "at_its_plan":
        assert str(raised.value.__cause__) == "execution time budget exhausted before the retry"
    assert sum(sent.method == "batch" for sent in transport.requests) == refused_at
    assert stream.report is not None
    assert stream.report.terminal_reason == "BudgetExceededError"
    assert [item.code for item in stream.report.violations] == ["budget_exhausted"]


def _refused_or_lost(body: dict[str, dict[str, dict[str, object]]], order: list[str], moment: str) -> None:
    """Answer a wave's batch with one command refused (QUERY_LIMIT_EXCEEDED), so the wave waits to replay it.

    ``order`` is the batch's commands in envelope order. ``final_answers``: the answers of its two first commands
    are lost (each may have run) and the third is refused. ``access_denied``: the second command is also answered
    with a final failure. Otherwise the first command is refused.
    """
    result = body["result"]["result"]
    errors = body["result"]["result_error"] = {}
    refused = order[2] if moment == "final_answers" else order[0]
    for key in order[:3] if moment == "final_answers" else (refused,):
        del result[key]
    errors[refused] = {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "slow"}
    if moment == "access_denied":
        del result[order[1]]
        errors[order[1]] = {"error": "ACCESS_DENIED", "error_description": "no"}


_U, _F, _N = CommandSettlement.UNKNOWN, CommandSettlement.FAILURE, CommandSettlement.NOT_EXECUTED
_CANCELLED_WAVE: dict[str, tuple[tuple[CommandSettlement, ...] | CommandSettlement, BindingClosure]] = {
    # moment: (how the wave's first pages settle, in envelope order, before its answered successes, or how every
    # page of it settles; the closure)
    "before_the_send": (_N, BindingClosure.FAILURE),
    "on_the_wire": (_U, BindingClosure.UNKNOWN),
    "before_the_replay": ((_F,), BindingClosure.FAILURE),
    "replay_on_the_wire": ((_U,), BindingClosure.UNKNOWN),
    "final_answers": ((_U, _U, _F), BindingClosure.UNKNOWN),
    "access_denied": ((_F, _F), BindingClosure.FAILURE),
}


def _cancellable_wave(
    monkeypatch: pytest.MonkeyPatch, moment: str, cancelled_at: int
) -> tuple[ResponderTransport, asyncio.Event]:
    """Serve a keyset whose ``cancelled_at``-th batch reaches ``moment``; the event is set once it has."""
    reached = asyncio.Event()
    replay_after = Executor.replay_after

    async def observed_replay_after(
        self: Executor, context: ExecutionContext, *, attempts: _RequestAttempts, pause: _Pause
    ) -> None:
        if moment not in {"before_the_send", "on_the_wire", "replay_on_the_wire"}:
            reached.set()
        await replay_after(self, context, attempts=attempts, pause=pause)

    monkeypatch.setattr(Executor, "replay_after", observed_replay_after)
    acquire = RateCoordinator.acquire

    async def held_acquire(coordinator: RateCoordinator, *args: object, **kwargs: object) -> object:
        if moment == "before_the_send" and _batches(transport) == cancelled_at - 1 and not reached.is_set():
            reached.set()
            await asyncio.Event().wait()
        return await acquire(coordinator, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(RateCoordinator, "acquire", held_acquire)
    keyset = KeysetTransport(tuple(range(1, 3001)))

    async def respond(request: Request) -> WireResponse:
        answer = await keyset.send(request, attempt_timeout=1.0, max_response_bytes=1 << 20)
        sent = _batches(transport) if request.method == "batch" else 0
        if (moment, sent) in {("on_the_wire", cancelled_at), ("replay_on_the_wire", cancelled_at + 1)}:
            reached.set()
            await asyncio.Event().wait()
        if sent != cancelled_at:
            return answer
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        body = json.loads(answer.body)
        _refused_or_lost(body, list(commands), moment)
        return WireResponse(200, answer.headers, json.dumps(body).encode())

    transport = ResponderTransport(respond, host=keyset.host)
    return transport, reached


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["range", "partitioned", "auto"])
@pytest.mark.parametrize("cancelled_at", [2, 3])
@pytest.mark.parametrize("moment", list(_CANCELLED_WAVE))
async def test_a_wave_cancelled_at_any_stage_settles_each_page_from_its_own_command(
    monkeypatch: pytest.MonkeyPatch, kind: str, cancelled_at: int, moment: str
) -> None:
    # A wave cancelled before its batch is sent ran nothing: no page is claimed answered. Cancelled while its batch
    # is on the wire, its every page may have run. Cancelled once the batch was answered, while the wave waits to
    # send again the one command the server refused (QUERY_LIMIT_EXCEEDED), each page keeps its own answer: the
    # answered pages succeeded, a refused one is an answered failure, as it is when the budget ends its replay, a
    # final failure is one, and a lost answer may have run: each page settles from its own command. Cancelled while the
    # replay is on the wire, only the replayed page may have run. A page that may have run makes the closure unknown.
    transport, reached = _cancellable_wave(monkeypatch, moment, cancelled_at)
    events: list[object] = []
    emit = CompletionGate.emit

    def observe(gate: CompletionGate, event: CompletionEvidence) -> None:
        events.append(event)
        emit(gate, event)

    monkeypatch.setattr(CompletionGate, "emit", observe)
    stream = _stream(transport, _fast_execution(kind))

    async def consume() -> None:
        async for _ in stream:
            pass

    task = asyncio.create_task(consume())
    await asyncio.wait_for(reached.wait(), HANG_BOUND)
    settled_before = {event.page_id for event in events if isinstance(event, PageCommandOutcome)}
    # Every page of the wave is scheduled, in its batch's envelope order, before the batch is sent, and settled
    # once the wave has its outcomes.
    wave = [
        event.page_id for event in events if isinstance(event, PageScheduled) and event.page_id not in settled_before
    ]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert stream.report is not None
    assert stream.report.state is TerminalState.CANCELLED
    assert not stream.report.violations
    settled = {event.page_id: event.outcome for event in events if isinstance(event, PageCommandOutcome)}
    first, closure = _CANCELLED_WAVE[moment]
    expected = (
        [first] * len(wave)
        if isinstance(first, CommandSettlement)
        else [*first, *[CommandSettlement.SUCCESS] * (len(wave) - len(first))]
    )
    assert [settled[page] for page in wave] == expected
    assert [event.closure for event in events if isinstance(event, BindingTerminal)] == [closure]


def _batches(transport: ResponderTransport) -> int:
    return sum(sent.method == "batch" for sent in transport.requests)


def _hold_the_budget_clock(monkeypatch: pytest.MonkeyPatch) -> Callable[[], None]:
    """Stand every operation budget's clock still until the returned call, then run it on from where it stood."""
    started = time.monotonic()
    resumed: list[float] = []

    def clock() -> float:
        return started if not resumed else started + time.monotonic() - resumed[0]

    def context(executor: Executor, policy: ExecutionPolicy | None = None) -> ExecutionContext:
        return ExecutionContext(policy or ExecutionPolicy(), executor.coordinator, clock=clock)

    def resume() -> None:
        if not resumed:
            resumed.append(time.monotonic())

    monkeypatch.setattr(Executor, "context", context)
    return resume


@pytest.mark.parametrize("send", [1, 2, 3, "finish"])
@pytest.mark.parametrize("end", ["cancel", "interrupt", "budget", "transport"])
def test_a_traversal_ended_while_a_page_is_on_the_wire_settles_every_page_before_rejecting_it(
    monkeypatch: pytest.MonkeyPatch, end: str, send: int | str
) -> None:
    # The first send is the boundary wave, the next ones are range waves, and the last a direct finish page.
    # Whatever ends the traversal while one is on the wire (its reader's cancellation, an interrupt, the elapsed
    # budget, a transport failure), every page is settled before the ledger rejects it, and the pages that may
    # have run make the binding's closure unknown. The budget's clock stands still until the held page is on the wire,
    # so the pages before it are sent within the budget however slow the runner (the responder never pauses before
    # them), and its whole budget is the held one's.
    events: list[object] = []
    emit = CompletionGate.emit

    def observe(gate: CompletionGate, event: CompletionEvidence) -> None:
        events.append(event)
        emit(gate, event)

    monkeypatch.setattr(CompletionGate, "emit", observe)
    keyset = KeysetTransport(tuple(range(1, 2000)))
    on_the_wire = asyncio.Event()
    resume_the_clock = _hold_the_budget_clock(monkeypatch)

    async def respond(request: Request) -> object:
        if len(transport.requests) == send or (send == "finish" and request.method != "batch"):
            on_the_wire.set()
            resume_the_clock()
            if end == "interrupt":
                raise KeyboardInterrupt
            if end == "transport":
                raise RuntimeError("transport")
            await asyncio.Future[None]()
        return await keyset.send(request, attempt_timeout=1.0, max_response_bytes=1 << 20)

    transport = ResponderTransport(respond, host=keyset.host)
    policy = ExecutionPolicy(max_elapsed=0.3) if end == "budget" else None
    stream = _stream(transport, RangeKeysetExecution(StableIntegerKeysetContract()), policy=policy)

    async def main() -> None:
        reader = asyncio.create_task(_drain_rows(stream))
        if end == "cancel":
            await asyncio.wait_for(on_the_wire.wait(), HANG_BOUND)
            reader.cancel()
        await asyncio.wait_for(asyncio.gather(reader, return_exceptions=end != "interrupt"), HANG_BOUND)

    if end == "interrupt":
        with pytest.raises(KeyboardInterrupt):
            asyncio.run(main())
    else:
        asyncio.run(main())

    assert on_the_wire.is_set()
    assert stream.report is not None
    assert not [violation.code for violation in stream.report.violations if violation.code.startswith("completion")]
    scheduled = sum(isinstance(event, PageScheduled) for event in events)
    assert sum(isinstance(event, PageCommandOutcome) for event in events) == scheduled
    assert [event.closure for event in events if isinstance(event, BindingTerminal)] == [BindingClosure.UNKNOWN]


@pytest.mark.asyncio
async def test_a_finish_page_the_server_refuses_settles_as_a_failure_and_fails_its_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The direct finish page is answered with a final failure: it ran and failed, so its command settles as a
    # failure, not as one that may have run, and the binding closes as failed.
    events: list[object] = []
    emit = CompletionGate.emit

    def observe(gate: CompletionGate, event: CompletionEvidence) -> None:
        events.append(event)
        emit(gate, event)

    monkeypatch.setattr(CompletionGate, "emit", observe)
    keyset = KeysetTransport(tuple(range(1, 3001)))

    async def respond(request: Request) -> WireResponse:
        if request.method != "batch":
            return json_response({"error": "ACCESS_DENIED", "error_description": "denied"})
        return await keyset.send(request, attempt_timeout=1.0, max_response_bytes=1 << 20)

    transport = ResponderTransport(respond, host=keyset.host)
    stream = _stream(transport, RangeKeysetExecution(StableIntegerKeysetContract()))

    with pytest.raises(ApiResponseError):
        await _drain_rows(stream)

    assert transport.requests[-1].method != "batch"
    settled = [event.outcome for event in events if isinstance(event, PageCommandOutcome)]
    assert settled[-1] is CommandSettlement.FAILURE
    assert CommandSettlement.UNKNOWN not in settled
    assert [event.closure for event in events if isinstance(event, BindingTerminal)] == [BindingClosure.FAILURE]


async def _drain_rows(stream: AsyncIterator[object]) -> None:
    async for _ in stream:
        pass


def _fail_runtime_cleanup(monkeypatch: pytest.MonkeyPatch, stream: object) -> None:
    def fail() -> None:
        raise RuntimeError("runtime cleanup failed")

    monkeypatch.setattr(cast("Any", stream)._source._runtime.admission, "discard_unadmitted_raw", fail)


@pytest.mark.asyncio
async def test_fast_early_close_whose_cleanup_fails_reports_that_failure_only(monkeypatch: pytest.MonkeyPatch) -> None:
    # The close is what failed: the report says so once, and nothing else about how the stream ended.
    stream = _stream(KeysetTransport(tuple(range(1, 80))), RangeKeysetExecution(StableIntegerKeysetContract()))
    await anext(stream)
    _fail_runtime_cleanup(monkeypatch, stream)

    with pytest.raises(RuntimeError, match="runtime cleanup failed") as raised:
        await stream.aclose()

    report = stream.report
    assert report is not None
    assert report.state is TerminalState.EARLY_CLOSED
    assert report.terminal_reason == "stream cleanup failed"
    assert [(item.code, item.message) for item in report.violations] == [
        ("cleanup_failure", "stream cleanup failed (RuntimeError)")
    ]
    assert getattr(raised.value, "report", None) is report
    assert not hasattr(raised.value, "__notes__")


@pytest.mark.asyncio
async def test_fast_failure_keeps_a_failed_cleanup_in_its_report_not_in_a_note(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = _stream(
        KeysetTransport(tuple(range(1, 80))),
        RangeKeysetExecution(StableIntegerKeysetContract()),
        policy=ExecutionPolicy(max_requests=1),
    )
    _fail_runtime_cleanup(monkeypatch, stream)

    with pytest.raises(IncompleteTraversalError) as raised:
        _ = [row async for row in stream]

    report = stream.report
    assert report is not None
    assert report.state is TerminalState.INCOMPLETE
    # The failure is primary: its violation comes first, the failed cleanup behind it last.
    assert [(item.code, item.message) for item in report.violations[-2:]] == [
        ("budget_exhausted", "iter_list_keyset failed (BudgetExceededError)"),
        ("cleanup_failure", "stream cleanup also failed (RuntimeError)"),
    ]
    assert raised.value.report is report
    assert not hasattr(raised.value, "__notes__")


@pytest.mark.asyncio
@pytest.mark.parametrize("timing", ["between_reads", "during_a_read"])
async def test_fast_cancelled_reader_whose_cleanup_then_fails_is_cancelled_whenever_the_cancellation_lands(
    monkeypatch: pytest.MonkeyPatch, timing: str
) -> None:
    # As in every family: the reader's cancellation came first, so the failed cleanup is behind it.
    transport = KeysetTransport(tuple(range(1, 80)))
    stream = _stream(transport, RangeKeysetExecution(StableIntegerKeysetContract()))
    _fail_runtime_cleanup(monkeypatch, stream)
    sending = asyncio.Event()
    if timing == "during_a_read":

        async def blocked(request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
            del request, attempt_timeout, max_response_bytes
            sending.set()
            await asyncio.Event().wait()
            raise AssertionError("never answered")

        monkeypatch.setattr(transport, "send", blocked)
    raised: list[BaseException] = []

    async def read() -> None:
        try:
            async for _row in stream:
                if timing == "between_reads":
                    task = asyncio.current_task()
                    assert task is not None
                    task.cancel()
        except BaseException as error:  # noqa: BLE001 - the test inspects what the reader got
            raised.append(error)

    reader = asyncio.create_task(read())
    if timing == "during_a_read":
        await sending.wait()
        reader.cancel()
    await reader

    assert [type(error) for error in raised] == [asyncio.CancelledError]
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.CANCELLED
    assert report.terminal_reason == "iteration cancelled"
    # The cleanup the test breaks stops before it settles a page in flight, which the gate records besides.
    assert [item.message for item in report.violations if item.code == "cleanup_failure"] == [
        "stream cleanup also failed (RuntimeError)"
    ]


@pytest.mark.parametrize(("direction", "admitted", "behind"), [("asc", (3, 4), (5, 3)), ("desc", (4, 3), (2, 4))])
def test_the_ordered_admission_refuses_a_receipt_that_does_not_advance_past_what_it_admitted(
    direction: Literal["asc", "desc"],
    admitted: tuple[int, ...],
    behind: tuple[int, ...],
) -> None:
    # The page checks upstream refuse such a receipt first today; the one owner of the global order still refuses
    # it on its own, and admits nothing of it.
    state = OrderedAdmissionState(
        direction=direction,
        identity=IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER),
    )

    def receipt(identities: tuple[int, ...]) -> keyset_page_validation.LaneReceipt:
        rows = tuple(FrozenMapping({"id": value}) for value in identities)
        return keyset_page_validation.LaneReceipt(0, "lane", rows, identities, None, ())

    assert state.validate_and_commit(receipt(admitted)).identities == admitted
    with pytest.raises(PaginationError, match="global order did not advance"):
        state.validate_and_commit(receipt(behind))
    assert state.has_seen(admitted[-1])
    assert not state.has_seen(behind[0])


def test_a_body_wave_advances_the_first_open_lanes_in_output_order() -> None:
    # Every wave is a prefix of the open lanes, so no lane has read more pages than an open lane before it.
    lanes = ("frontier", "second", "third")

    assert wave_members(lanes, room=2) == ("frontier", "second")
    assert wave_members(lanes, room=5) == lanes
    assert wave_members(lanes, room=0) == ()
    assert wave_members((), room=4) == ()


def test_sliding_lanes_open_no_more_lanes_than_a_wave_carries() -> None:
    # Two lanes a wave: the second closes after one page, but the third opens only once the deep frontier closes.
    rules = WaveRules(50, KeysetPageCompletion.EMPTY_CONFIRMATION, capacity=2, row_budget=10_000)
    shapes = (LaneShape(150, 150), LaneShape(50, 50, 2))

    assert simulate_lanes(shapes, rules, sliding=True, request_limit=100) == WaveCost(4, 5)
    assert simulate_lanes(shapes, rules, sliding=False, request_limit=100) == WaveCost(3, 5)


def test_a_lane_needs_a_confirmation_page_at_the_chance_its_top_value_holds_no_row() -> None:
    # A full page or, under empty confirmation, a short one closes the lane when its last row sits right below
    # the upper fence; otherwise an empty page confirms the end. Rows spread over a third of the values leave
    # the top value empty two times in three, and the simulation spreads those pages evenly over the lanes.
    empty, short = KeysetPageCompletion.EMPTY_CONFIRMATION, KeysetPageCompletion.SHORT_PAGE_EXHAUSTS

    assert lane_pages(rows=50, ids=150, page_cap=50, completion=short) == LanePages(1, Fraction(2, 3))
    assert lane_pages(rows=40, ids=120, page_cap=50, completion=empty) == LanePages(1, Fraction(2, 3))
    assert lane_pages(rows=40, ids=120, page_cap=50, completion=short) == LanePages(1, Fraction(0))
    assert lane_pages(rows=0, ids=10, page_cap=50, completion=short) == LanePages(0, Fraction(1))
    assert lane_pages(rows=120, ids=120, page_cap=50, completion=empty) == LanePages(3, Fraction(0))
    assert lane_pages(rows=0, ids=0, page_cap=50, completion=empty) == LanePages(0, Fraction(0))
    rules = WaveRules(50, empty, capacity=50, row_budget=10_000)
    assert simulate_lanes((LaneShape(50, 150, 3),), rules, sliding=False, request_limit=100) == WaveCost(2, 5)
    # A lane alone takes the page it more likely needs than not.
    assert simulate_lanes((LaneShape(50, 250),), rules, sliding=False, request_limit=100) == WaveCost(2, 2)
    assert simulate_lanes((LaneShape(50, 70),), rules, sliding=False, request_limit=100) == WaveCost(1, 1)


def _reference_waves(shapes: tuple[LaneShape, ...], rules: WaveRules, *, sliding: bool) -> WaveCost | None:
    """The body-wave rule run one wave and one lane at a time, with no wave taken at once."""
    chance, given = Fraction(0), 0
    pending: list[list[int]] = []  # [rows left, pages left, held]
    for shape in shapes:
        cost = lane_pages(rows=shape.rows, ids=shape.ids, page_cap=rules.page_cap, completion=rules.completion)
        for _ in range(shape.count):
            chance += cost.confirmation
            confirmation = math.floor(chance + Fraction(1, 2)) - given
            given += confirmation
            if cost.pages + confirmation:
                pending.append([shape.rows, cost.pages + confirmation, 0])
    group = rules.capacity if sliding else len(pending)
    active, waiting = pending[:group], pending[group:]
    held = requests = pages = 0
    while active:
        room = wave_room(capacity=rules.capacity, free_rows=rules.row_budget - held, reserve=rules.page_cap)
        members = wave_members([lane for lane in active if lane[1]], room=room)
        if not members:
            return None
        requests += 1
        pages += len(members)
        for lane in members:
            read = min(rules.page_cap, lane[0])
            lane[0] -= read
            lane[2] += read
            held += read
            lane[1] -= 1
        while active:
            held -= active[0][2]
            active[0][2] = 0
            if active[0][1]:
                break
            active.pop(0)
            if waiting:
                active.append(waiting.pop(0))
    return WaveCost(requests, pages)


@pytest.mark.parametrize("seed", range(120))
def test_the_cost_model_takes_waves_at_once_exactly_as_it_would_one_by_one(seed: int) -> None:
    # Runs of windows that repeat their confirmation pattern many times, waves only the frontier reads, buffers
    # that hold fewer pages than a wave carries: whatever the model takes at once costs what running every wave in
    # turn does.
    rng = random.Random(seed)
    page_cap = rng.choice((2, 5, 10))
    shapes = tuple(
        LaneShape(
            rows,
            max(rows, rng.choice((rows, rows + 1, 3 * rows + 2, page_cap)), 1),
            rng.choice((rng.randint(1, 40), rng.randint(100, 600))),
        )
        for rows in (rng.choice((0, 1, page_cap - 1, page_cap, 3 * page_cap)) for _ in range(rng.randint(1, 6)))
    )
    completion = rng.choice(tuple(KeysetPageCompletion))
    rules = WaveRules(page_cap, completion, capacity=rng.randint(1, 8), row_budget=rng.randint(1, 12) * page_cap)

    for sliding in (False, True):
        expected = _reference_waves(shapes, rules, sliding=sliding)
        assert simulate_lanes(shapes, rules, sliding=sliding, request_limit=10**6) == expected


@pytest.mark.parametrize(
    ("shapes", "rules"),
    [
        (
            tuple(shape for run in range(40) for shape in (LaneShape(49, 343, 2_000), LaneShape(run, 343))),
            WaveRules(50, KeysetPageCompletion.EMPTY_CONFIRMATION, capacity=48, row_budget=2_450),
        ),
        (
            (*(LaneShape(rows, 343) for rows in range(1, 101)), LaneShape(253, 1_772, 20_001)),
            WaveRules(50, KeysetPageCompletion.EMPTY_CONFIRMATION, capacity=48, row_budget=2_450),
        ),
        (
            (LaneShape(100, 100, 3_141), LaneShape(100, 49_441, 8_429), LaneShape(100, 203, 4_980)),
            WaveRules(50, KeysetPageCompletion.SHORT_PAGE_EXHAUSTS, capacity=10, row_budget=100),
        ),
        (
            (LaneShape(0, 49_441, 5_944), LaneShape(20, 21, 5_989), LaneShape(20, 141, 8_215)),
            WaveRules(10, KeysetPageCompletion.EMPTY_CONFIRMATION, capacity=48, row_budget=120),
        ),
        (
            (LaneShape(253, 253, 20_000), LaneShape(24, 343), LaneShape(253, 1_772, 20_000)),
            WaveRules(50, KeysetPageCompletion.EMPTY_CONFIRMATION, capacity=48, row_budget=2_450),
        ),
        (
            tuple(shape for _ in range(20) for shape in (LaneShape(20_000, 200_000, 47), LaneShape(1, 343))),
            WaveRules(50, KeysetPageCompletion.EMPTY_CONFIRMATION, capacity=48, row_budget=2_450),
        ),
        ((LaneShape(183, 252, 20_000),), WaveRules(10, KeysetPageCompletion.EMPTY_CONFIRMATION, 25, 5_000)),
        ((LaneShape(100, 701, 20_000),), WaveRules(50, KeysetPageCompletion.SHORT_PAGE_EXHAUSTS, 50, 600)),
    ],
    ids=[
        "forty-runs",
        "straddles-first",
        "tight-buffer",
        "shallow",
        "two-deep-spans",
        "deep-short-runs",
        "dense",
        "short",
    ],
)
def test_the_cost_model_prices_long_runs_of_windows_exactly(shapes: tuple[LaneShape, ...], rules: WaveRules) -> None:
    # Long runs of windows, many short runs of one kind, windows of the same rows over other spans, deep windows
    # behind a tight buffer: the estimate is what running every wave in turn costs, requests and pages alike.
    expected = _reference_waves(shapes, rules, sliding=True)

    assert expected is not None
    assert simulate_lanes(shapes, rules, sliding=True, request_limit=10**9) == expected


def test_the_cost_model_takes_the_waves_a_frontier_reads_alone_at_once() -> None:
    # A lane of a trillion rows behind a buffer of one page: a page a wave would take forever to simulate.
    one_page = WaveRules(50, KeysetPageCompletion.EMPTY_CONFIRMATION, capacity=50, row_budget=50)
    deep = (LaneShape(10**12, 10**12, 2),)

    assert simulate_lanes(deep, one_page, sliding=False, request_limit=10**12) == WaveCost(4 * 10**10, 4 * 10**10)
    assert simulate_lanes(deep, one_page, sliding=False, request_limit=10**6) is None
    roomy = dataclasses.replace(one_page, row_budget=2_500)
    # The second lane reads 49 pages beside the frontier before its rows fill the buffer.
    assert simulate_lanes(deep, roomy, sliding=False, request_limit=10**12) == WaveCost(4 * 10**10 - 49, 4 * 10**10)


@pytest.mark.parametrize(
    ("completion", "short_ids"),
    [(KeysetPageCompletion.SHORT_PAGE_EXHAUSTS, 499), (KeysetPageCompletion.EMPTY_CONFIRMATION, 10)],
    ids=["short-page-exhausts", "empty-confirmation"],
)
def test_a_probe_page_short_of_the_cap_measures_up_to_its_fence_only_when_a_short_page_exhausts(
    completion: KeysetPageCompletion, short_ids: int
) -> None:
    # A full page measures up to its last row. A short page read every row below the fence when a short page exhausts
    # what it reads, so the lane above its anchor spans every value up to it. Under empty confirmation a server may
    # cap its pages below the requested size: a short page measures up to its last row, as a full one does.
    def receipt(*ids: int) -> LaneReceipt:
        rows = tuple(FrozenMapping({"ID": str(i)}) for i in ids)
        return LaneReceipt(0, "probe", rows, ids, None, ())

    _, samples, discarded, retained = compact_anchor_receipts(
        (receipt(100, 110, 120), receipt(500, 510)), page_cap=3, completion=completion, upper_exclusive=1_000
    )

    assert [(sample.fence, sample.rows, sample.ids) for sample in samples] == [(100, 2, 20), (500, 1, short_ids)]
    assert (discarded, retained) == (3, 2)


@pytest.mark.parametrize(
    ("range_requests", "range_pages", "remaining_pages", "plan"),
    [
        (60, 60, None, Preselected.RANGE),
        (61, 61, None, Preselected.SEQUENTIAL),
        (60, 150, 120, Preselected.SEQUENTIAL),
        (60, 150, 90, Preselected.RANGE),
    ],
    ids=["pays-off", "no-gain", "range-over-budget-sequential-within", "both-over-budget"],
)
def test_a_buffer_that_cannot_hold_the_anchors_keeps_range_windows_only_when_they_pay_off(
    range_requests: int, range_pages: int, remaining_pages: int | None, plan: Preselected
) -> None:
    # The probes were chosen, but the buffer has no room for their anchors: range windows replace them only when
    # they pay off against sequential traversal (at most 3/5 of its requests, within the budget left unless
    # sequential traversal does not fit it either); otherwise sequential traversal.
    inputs = SelectorInputs(
        BoundaryFacts(50, 50, 1, 50, 20_001, 20_050, False, False),
        effective_page_cap=50,
        batch_capacity=50,
        available_rows=49,
        target_lanes=4,
        range_window_width=None,
        completion=KeysetPageCompletion.EMPTY_CONFIRMATION,
        advisory_total=None,
        remaining_pages=remaining_pages,
    )
    sequential = CostEstimate(KeysetExecutionKind.SEQUENTIAL, 100, eligible=True, pages=100)
    windows = CostEstimate(KeysetExecutionKind.RANGE, range_requests, eligible=True, pages=range_pages)
    probing = Preselection(
        Preselected.PROBE_ANCHORS, KeysetSelectionReason.WIDE_SPAN_PARTITIONING, 400, 500, sequential, windows, None, 2
    )

    assert constrain_anchor_preselection(inputs, probing, anchor_capacity=0).plan is plan
    assert constrain_anchor_preselection(inputs, probing, anchor_capacity=1) is probing


def test_a_lane_whose_probe_page_measured_no_span_takes_both_boundary_pages_density() -> None:
    # The probe at 1,000 read a page of one row under empty confirmation, which measures no span: the lane above it
    # is priced at the density of both boundary pages together (100 rows over 540 values), not at the density the
    # previous lane's probe measured (49 rows over 98 values).
    boundary = BoundaryFacts(50, 50, 1, 50, 10_001, 10_490, False, False)
    anchors = AnchorFacts((400, 1_000), 0, (), ((400, 49, 98), (1_000, 0, 0)))

    lanes, _ = probed_runs(boundary, anchors)

    unknown = 10_001 - 1_000 - 1
    assert lanes[2] == LaneShape(math.ceil(unknown * Fraction(100, 540)), unknown)


def test_the_runs_after_probing_span_every_interior_value_the_anchors_included() -> None:
    # Range windows are cut over these runs: each anchor's own value is a run of one row, so the runs
    # account for every value between the boundary pages and the windows hold the rows they span.
    boundary = BoundaryFacts(50, 50, 1, 50, 2_001, 2_050, False, False)
    anchors = AnchorFacts((400, 1_000, 1_600), 0, (), ((400, 49, 50), (1_000, 49, 98), (1_600, 49, 50)))

    lanes, runs = probed_runs(boundary, anchors)

    assert sum(run.ids for run in runs) == 2_001 - 50 - 1
    assert sum(run.ids * run.density for run in runs) >= len(anchors.anchors)
    assert len(lanes) == len(anchors.anchors) + 1


@pytest.mark.asyncio
async def test_explicit_partitioning_closes_a_lane_whose_fences_leave_no_value_without_a_request() -> None:
    # Probes past the dense head all find 199, right below the tail boundary page: the lane between them
    # holds no identity value, so it closes as full without the empty page that would confirm it.
    identities = (*range(1, 101), 199, *range(200, 205))
    transport = KeysetTransport(identities)
    stream = _stream(transport, PartitionedKeysetExecution(StableIntegerKeysetContract(), target_lanes=4))

    assert [cast("dict[str, int]", row)["id"] async for row in stream] == list(identities)
    report = stream.report
    assert report is not None
    keyset = report.keyset_execution
    assert keyset is not None
    witnesses = dict(keyset.closure_witness_counts)
    assert (witnesses[ClosureWitness.LATTICE_FULL], witnesses[ClosureWitness.EMPTY]) == (1, 0)
    assert report.logical_pages == 28


@pytest.mark.asyncio
async def test_auto_prices_range_windows_by_the_waves_that_slide_over_them() -> None:
    # A dense head and a sparse tail: the first windows are deep, so a wave opens the next window only
    # as one closes. Priced as if every window were open at once, the estimate falls a third short.
    identities = (*range(1, 601), *range(601, 80_000, 97))
    transport = KeysetTransport(identities)
    stream = _stream(transport, AutoKeysetExecution(StableIntegerKeysetContract()))

    assert [cast("dict[str, int]", row)["id"] async for row in stream] == list(identities)
    report = stream.report
    assert report is not None
    keyset = report.keyset_execution
    assert keyset is not None
    assert keyset.selected_kind is KeysetExecutionKind.RANGE
    sent = len(transport.requests)
    assert keyset.selected_requests_estimate is not None
    assert abs(keyset.selected_requests_estimate - sent) <= sent // 10, (keyset.selected_requests_estimate, sent)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("hint", "pricings"),
    [
        (TotalHintMode.IGNORE, ["preselect", "finalize"]),
        # A plausible advisory total is weighed against a preselection priced without it.
        (TotalHintMode.REQUEST_ADVISORY, ["preselect", "preselect", "finalize"]),
    ],
)
async def test_auto_prices_its_plans_off_the_event_loop(
    hint: TotalHintMode, pricings: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pricing steps through every wave of each plan it prices: every preselection and the choice after the probes run
    # in a worker thread, so the event loop is never held for them.
    priced: list[tuple[str, bool]] = []

    def off_the_loop[**P, R](name: str, price: Callable[P, R]) -> Callable[P, R]:
        def run(*args: P.args, **kwargs: P.kwargs) -> R:
            priced.append((name, threading.current_thread() is threading.main_thread()))
            return price(*args, **kwargs)

        return run

    monkeypatch.setattr(keyset_plan, "preselect", off_the_loop("preselect", keyset_auto.preselect))
    monkeypatch.setattr(keyset_plan, "finalize", off_the_loop("finalize", keyset_auto.finalize))
    identities = (*range(1, 80_000, 97), *range(80_000, 83_000))
    stream = _client(KeysetTransport(identities, boundary_total=len(identities))).iter_list_keyset(
        Request("item.list", parameters={"filter": {"STATUS": "open"}}, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=50,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",)), direction="descending"),
        execution=AutoKeysetExecution(StableIntegerKeysetContract(), total_hint=hint),
    )

    assert [cast("dict[str, int]", row)["id"] async for row in stream] == sorted(identities, reverse=True)
    assert stream.report is not None
    assert stream.report.keyset_execution is not None
    assert stream.report.keyset_execution.total_hint_plausible is (hint is TotalHintMode.REQUEST_ADVISORY)
    assert priced == [(name, False) for name in pricings]


@pytest.mark.asyncio
async def test_a_total_the_server_sends_unasked_steers_no_plan() -> None:
    # Without REQUEST_ADVISORY the total is evidence only: reported as observed, never plausible, never priced.
    identities = (*range(1, 80_000, 97), *range(80_000, 83_000))

    async def plan(total: int | None) -> KeysetExecutionReport:
        stream = _client(KeysetTransport(identities, boundary_total=total)).iter_list_keyset(
            Request("item.list", parameters={"filter": {"STATUS": "open"}}, route=RouteKind.BARE),
            selector=ResultSelector.root(),
            identity=_identity(),
            page_size=50,
            keyset=KeysetSpec(limit_path=ParameterPath(("limit",)), direction="descending"),
            execution=AutoKeysetExecution(StableIntegerKeysetContract()),
        )
        assert [cast("dict[str, int]", row)["id"] async for row in stream] == sorted(identities, reverse=True)
        assert stream.report is not None
        assert stream.report.keyset_execution is not None
        return stream.report.keyset_execution

    unasked, absent = await plan(len(identities)), await plan(None)

    assert (unasked.total_hint_observed, unasked.total_hint_plausible, unasked.total_hint_used) == (
        len(identities),
        False,
        False,
    )
    assert (unasked.selected_kind, unasked.selected_requests_estimate, unasked.final_selection_reason) == (
        absent.selected_kind,
        absent.selected_requests_estimate,
        absent.final_selection_reason,
    )


@pytest.mark.asyncio
async def test_a_descending_auto_prices_its_lanes_in_the_order_it_reads_them() -> None:
    # Read from the top, the dense block drains first and the sparse lanes below it read ahead freely: range
    # windows over that order pay off. Priced in ascending order, the dense block would sit behind the sparse
    # lanes, and Auto would take partitioning at nearly twice the requests.
    identities = (*range(1, 80_000, 97), *range(80_000, 83_000))
    transport = KeysetTransport(identities)
    stream = _client(transport).iter_list_keyset(
        Request("item.list", parameters={"filter": {"STATUS": "open"}}, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=50,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",)), direction="descending"),
        execution=AutoKeysetExecution(StableIntegerKeysetContract()),
    )

    assert [cast("dict[str, int]", row)["id"] async for row in stream] == sorted(identities, reverse=True)
    report = stream.report
    assert report is not None
    keyset = report.keyset_execution
    assert keyset is not None
    assert (keyset.selected_kind, keyset.final_selection_reason) == (
        KeysetExecutionKind.RANGE,
        KeysetSelectionReason.POST_PROBE_RANGE_PREFERRED,
    )
    assert len(transport.requests) <= 40


@pytest.mark.parametrize(("direction", "requests"), [("ascending", 5), ("descending", 6)])
def test_post_probe_partitioning_is_priced_in_the_order_the_lanes_are_read(direction: str, requests: int) -> None:
    # The lane above the first anchor is dense, the others sparse. Read upwards it drains first; read downwards
    # it comes last, behind the rows the sparse lanes above it hold, and costs a wave more.
    inputs = dataclasses.replace(_post_probe_inputs(), available_rows=200, descending=direction == "descending")
    anchors = AnchorFacts((200, 400, 1_800), 0, (), ((200, 49, 49), (400, 2, 98), (1_800, 2, 98)))

    final = finalize(inputs, preselect(inputs), anchors)

    assert (final.kind, final.estimate.requests) == (KeysetExecutionKind.PARTITIONED, requests)
