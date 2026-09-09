"""Deterministic correctness tests for opt-in fast integer keysets."""

# ruff: noqa: ANN202, D102, D107, FBT003, PLR2004, SLF001

from __future__ import annotations
import json
from typing import TYPE_CHECKING
from urllib.parse import parse_qs

import pytest

from b24api import (
    AutoKeysetExecution,
    Bitrix24,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    KeysetExecutionKind,
    KeysetPageCompletion,
    KeysetSelectionReason,
    KeysetSpec,
    ParameterPath,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    Request,
    ResultSelector,
    StableIntegerKeysetContract,
    TerminalState,
)
from b24api.errors import CapabilityError, IncompleteTraversalError
from b24api.execution import Executor, WireResponse
from b24api.traversal.keyset_auto import BoundaryFacts, Preselected, SelectorInputs, preselect
from b24api.traversal.keyset_fast_plan import plan_lanes_from_anchors, plan_windows
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

    def __init__(self, identities: tuple[int, ...], *, ignore_direction: bool = False) -> None:
        self.identities = identities
        self.ignore_direction = ignore_direction
        self.requests: list[Request] = []

    def _rows(self, parameters: dict[str, JsonValue]) -> list[dict[str, int]]:
        selected = self.identities
        filters = parameters.get("filter", {})
        if isinstance(filters, dict):
            if ">ID" in filters:
                selected = tuple(value for value in selected if value > int(filters[">ID"]))
            if "<ID" in filters:
                selected = tuple(value for value in selected if value < int(filters["<ID"]))
        order = parameters.get("order", {})
        descending = isinstance(order, dict) and order.get("id") == "DESC" and not self.ignore_direction
        limit = int(parameters.get("limit", PAGE_SIZE))
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
            payload = {"result": {"result": results, "result_error": []}}
        else:
            payload = {"result": self._rows(request.copy_parameters())}
        return WireResponse(
            200,
            (("content-type", "application/json"),),
            json.dumps(payload, separators=(",", ":")).encode(),
        )

    async def aclose(self) -> None:
        pass


def _client(transport: KeysetTransport, *, policy: ExecutionPolicy | None = None) -> Bitrix24:
    return Bitrix24._from_executor(Executor(transport), policy=policy)


def _identity() -> IdentitySpec:
    return IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER)


def _stream(
    transport: KeysetTransport,
    execution: RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution,
    *,
    direction: str = "ascending",
):
    return _client(transport).iter_list_keyset(
        Request("item.list", parameters={"filter": {"STATUS": "open"}}),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE_SIZE,
        keyset=KeysetSpec(
            limit_path=ParameterPath(("limit",)),
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
    selection = preselect(SelectorInputs(boundary, 50, capacity, 20, 2, None, completion, None))
    estimate = selection.range_estimate
    assert estimate is not None
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
async def test_boundary_direction_contradiction_fails_before_emission() -> None:
    transport = KeysetTransport(tuple(range(1, 31)), ignore_direction=True)
    stream = _stream(transport, RangeKeysetExecution(StableIntegerKeysetContract()))
    with pytest.raises(IncompleteTraversalError):
        await anext(stream)
    assert stream.report is not None
    assert stream.report.emitted == 0
    assert stream.report.state is TerminalState.INCOMPLETE


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
