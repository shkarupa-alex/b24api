"""Acceptance coverage for cursor seeds, page adapters, and keyset verification."""

# ruff: noqa: ANN202, D101, D102, D107, FBT003, PLR2004, SLF001

from __future__ import annotations
import hashlib
import json
from collections import deque
from dataclasses import FrozenInstanceError
from typing import TYPE_CHECKING
from urllib.parse import parse_qs

import pytest

from b24api import (
    AdaptedPage,
    BatchDispatch,
    Binding,
    Bitrix24,
    CapabilityError,
    CursorSpec,
    CursorTraversal,
    ExecutionPolicy,
    IdentityCoercion,
    IdentityPageAdapter,
    IdentitySpec,
    IncompleteTraversalError,
    KeysetCapabilityCheckName,
    KeysetCapabilityCheckOutcome,
    KeysetCapabilityCheckResult,
    KeysetCapabilityError,
    KeysetCapabilityReport,
    KeysetCapabilityVerdict,
    KeysetInconclusiveReason,
    KeysetSpec,
    MembershipRecheck,
    PageAdaptationError,
    PageAdaptationViolation,
    PageAdapter,
    ParameterPath,
    ParameterUpdate,
    ReferenceComplete,
    ReferenceFailed,
    ReferenceFailure,
    ReferenceItem,
    ReferenceNotExecuted,
    Request,
    ResultSelector,
    SequentialTraversal,
)
from b24api.batch.outcome import BatchSuccess
from b24api.contracts.json import FrozenMapping
from b24api.contracts.keyset_execution import KeysetPageCompletion, KeysetPhase
from b24api.contracts.response import Response, ResultCollectionShape
from b24api.execution import Executor, WireResponse
from b24api.references.dispatch import _ProducerState, _RowBuffer
from b24api.traversal.keyset_fast_plan import LaneBounds, LaneKind, LaneSpec, LaneState, LaneStatus
from b24api.traversal.page_validation import LaneCommandPlan, ReceiptRejection, validate_lane_receipt
from b24api.traversal.values import _page_fingerprint, _response_items

if TYPE_CHECKING:
    from collections.abc import Mapping

    from b24api.contracts import JsonValue
    from b24api.contracts.page import PageView


def _client(transport: object, *, policy: ExecutionPolicy | None = None) -> Bitrix24:
    return Bitrix24._from_executor(Executor(transport), policy=policy)  # type: ignore[arg-type]


def _identity() -> IdentitySpec:
    return IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER)


def _cursor() -> CursorSpec:
    return CursorSpec(
        ParameterPath(("after",)),
        ("id",),
        IdentityCoercion.EXACT_INTEGER,
        "ascending",
        "last",
        ParameterPath(("limit",)),
    )


def _decode_command(command: str) -> dict[str, JsonValue]:
    parsed = parse_qs(command.split("?", 1)[1] if "?" in command else "")
    result: dict[str, JsonValue] = {}
    for key, values in parsed.items():
        if key.startswith("filter["):
            nested = result.setdefault("filter", {})
            assert isinstance(nested, dict)
            nested[key[7:-1]] = values[0]
        elif key.startswith("order["):
            nested = result.setdefault("order", {})
            assert isinstance(nested, dict)
            nested[key[6:-1]] = values[0]
        else:
            result[key] = values[0]
    return result


class CursorBatchTransport:
    host = "test.invalid"

    def __init__(self, rows: Mapping[str, tuple[int, ...]]) -> None:
        self.rows = rows
        self.requests: list[Request] = []
        self.commands: list[dict[str, JsonValue]] = []

    def _page(self, parameters: dict[str, JsonValue]) -> list[dict[str, object]]:
        parent = str(parameters["parent"])
        cursor = int(parameters.get("after", 0))
        limit = int(parameters.get("limit", 50))
        return [{"id": value, "parent": parent} for value in self.rows[parent] if value > cursor][:limit]

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        self.requests.append(request)
        parameters = request.copy_parameters()
        if request.method == "batch":
            commands = parameters["cmd"]
            assert isinstance(commands, dict)
            decoded = {key: _decode_command(value) for key, value in commands.items() if isinstance(value, str)}
            self.commands.extend(decoded.values())
            result = {key: self._page(value) for key, value in decoded.items()}
            payload = {"result": {"result": result, "result_error": []}}
        else:
            payload = {"result": self._page(parameters)}
        return WireResponse(200, (("content-type", "application/json"),), json.dumps(payload).encode())

    async def aclose(self) -> None:
        return


@pytest.mark.asyncio
async def test_iter_cursors_normalizes_independent_seeds_and_keeps_correlation_off_wire() -> None:
    transport = CursorBatchTransport({"a": (1, 2, 3, 4), "b": (10, 11, 12)})
    correlations = ({"secret": "alpha"}, {"secret": "beta"})
    bindings = [
        Binding("a", (ParameterUpdate(ParameterPath(("parent",)), "a"),), correlations[0], start_cursor=2),
        Binding("b", (ParameterUpdate(ParameterPath(("parent",)), "b"),), correlations[1], start_cursor=10),
    ]
    stream = _client(transport).iter_cursors(
        Request("item.list", {"parent": "base"}),
        bindings,
        selector=ResultSelector.root(),
        cursor=_cursor(),
        identity=_identity(),
        page_size=2,
        dispatch=BatchDispatch(batch_size=2, coalesce_wait=0),
    )

    events = [event async for event in stream]

    items = [event for event in events if isinstance(event, ReferenceItem)]
    completions = [event for event in events if isinstance(event, ReferenceComplete)]
    assert [(item.binding_index, item.item["id"]) for item in items] == [(0, 3), (0, 4), (1, 11), (1, 12)]
    assert [event.row_count for event in completions] == [2, 2]
    assert items[0].correlation is correlations[0]
    assert items[-1].correlation is correlations[1]
    initial = {str(command["parent"]): int(command["after"]) for command in transport.commands[:2]}
    assert initial == {"a": 2, "b": 10}
    wire = "".join(request.summary.method for request in transport.requests) + repr(transport.requests)
    assert "alpha" not in wire
    assert "beta" not in wire


@pytest.mark.asyncio
async def test_cursor_seed_invalid_or_used_with_non_cursor_traversal_is_local() -> None:
    invalid_transport = CursorBatchTransport({"a": (1, 2)})
    invalid = _client(invalid_transport).iter_cursors(
        Request("item.list", {"parent": "base"}),
        [Binding("a", (ParameterUpdate(ParameterPath(("parent",)), "a"),), "opaque", start_cursor="bad")],
        selector=ResultSelector.root(),
        cursor=_cursor(),
    )
    with pytest.raises(ReferenceFailed) as raised:
        await anext(invalid)
    assert len(raised.value.outcomes) == 1
    assert isinstance(raised.value.outcomes[0], ReferenceNotExecuted)
    assert raised.value.outcomes[0].correlation == "opaque"
    assert invalid_transport.requests == []

    tolerant_transport = CursorBatchTransport({"a": (1, 2)})
    tolerant = _client(tolerant_transport).iter_reference_outcomes(
        Request("item.list", {"parent": "base"}),
        [Binding("a", (), "opaque", start_cursor=1)],
        traversal=SequentialTraversal(),
    )
    outcomes = [outcome async for outcome in tolerant]
    assert len(outcomes) == 1
    assert isinstance(outcomes[0], ReferenceNotExecuted)
    assert outcomes[0].correlation == "opaque"
    assert tolerant_transport.requests == []


@pytest.mark.asyncio
async def test_cursor_seed_must_advance_on_first_page_and_cursor_update_path_is_reserved() -> None:
    transport = CursorBatchTransport({"a": (1, 2, 3)})
    stream = _client(transport).iter_cursors(
        Request("item.list", {"parent": "a"}),
        [Binding("a", (), None, start_cursor=3)],
        selector=ResultSelector.root(),
        cursor=_cursor(),
        dispatch=BatchDispatch(coalesce_wait=0),
    )
    assert [event async for event in stream] == [ReferenceComplete(0, None, 0)]

    blocked = _client(transport).iter_cursors(
        Request("item.list", {"parent": "a"}),
        [Binding("a", (ParameterUpdate(ParameterPath(("after",)), 1),), None, start_cursor=2)],
        selector=ResultSelector.root(),
        cursor=_cursor(),
    )
    with pytest.raises(ReferenceFailed) as raised:
        await anext(blocked)
    assert isinstance(raised.value.outcomes[0], ReferenceNotExecuted)


@pytest.mark.asyncio
async def test_cursor_seed_replaces_existing_control_when_creation_is_forbidden() -> None:
    transport = CursorBatchTransport({"a": (1, 2)})
    cursor = CursorSpec(
        ParameterPath(("after",)),
        ("id",),
        IdentityCoercion.EXACT_INTEGER,
        "ascending",
        "last",
        ParameterPath(("limit",)),
        allow_create_controls=False,
    )
    stream = _client(transport).iter_cursors(
        Request("item.list", {"parent": "a", "after": 0, "limit": 1}),
        [Binding("a", (), None, start_cursor=1)],
        selector=ResultSelector.root(),
        cursor=cursor,
        page_size=1,
        dispatch=BatchDispatch(coalesce_wait=0),
    )
    events = [event async for event in stream]
    assert [event.item["id"] for event in events if isinstance(event, ReferenceItem)] == [2]
    assert isinstance(events[-1], ReferenceComplete)

    singular = _client(CursorBatchTransport({"a": (1, 2)})).iter_list_cursor(
        Request("item.list", {"parent": "a", "after": 0, "limit": 1}),
        selector=ResultSelector.root(),
        cursor=cursor,
        page_size=1,
    )
    assert [row["id"] async for row in singular] == [1, 2]


class PageTransport:
    host = "test.invalid"

    def __init__(self) -> None:
        self.requests: list[Request] = []

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        self.requests.append(request)
        cursor = int(request.copy_parameters().get("after", 0))
        messages = [] if cursor else [{"id": 1, "author_id": 7}, {"id": 2, "author_id": 8}]
        result = {"chat_id": 42, "messages": messages, "users": {"7": "Ada", "8": "Lin"}, "files": []}
        return WireResponse(
            200,
            (("content-type", "application/json"),),
            json.dumps({"result": result}).encode(),
        )


class EnrichMessages:
    def __init__(self) -> None:
        self.calls = 0

    def adapt(self, page: PageView, /) -> AdaptedPage:
        self.calls += 1
        assert isinstance(page.result, FrozenMapping)
        users = page.result["users"]
        assert isinstance(users, FrozenMapping)
        output = []
        for item in page.items:
            assert isinstance(item, FrozenMapping)
            output.append({"id": item["id"], "author_id": item["author_id"], "author": users[str(item["author_id"])]})
        return AdaptedPage(output)


class ReorderingIdentityAdapter(IdentityPageAdapter):
    def adapt(self, page: PageView, /) -> AdaptedPage:
        return AdaptedPage(tuple(reversed(page.items)))


@pytest.mark.asyncio
async def test_page_adapter_enriches_from_frozen_siblings_and_runs_on_empty_confirmation() -> None:
    adapter = EnrichMessages()
    stream = _client(PageTransport()).iter_list_cursor(
        Request("item.list"),
        selector=ResultSelector(("messages",)),
        cursor=_cursor(),
        identity=_identity(),
        page_adapter=adapter,
    )
    rows = [row async for row in stream]
    assert rows == [
        {"id": 1, "author_id": 7, "author": "Ada"},
        {"id": 2, "author_id": 8, "author": "Lin"},
    ]
    assert adapter.calls == 2
    with pytest.raises((FrozenInstanceError, TypeError)):
        IdentityPageAdapter().unexpected = True  # type: ignore[misc]


@pytest.mark.asyncio
async def test_identity_adapter_subclass_is_custom_and_requires_provable_order() -> None:
    transport = PageTransport()
    stream = _client(transport).iter_list(
        Request("item.list"),
        selector=ResultSelector(("messages",)),
        page_adapter=ReorderingIdentityAdapter(),
    )
    with pytest.raises(CapabilityError, match="requires a traversal identity"):
        await anext(stream)
    assert transport.requests == []


class BrokenAdapter:
    def __init__(self, violation: PageAdaptationViolation) -> None:
        self.violation = violation

    def adapt(self, page: PageView, /) -> object:
        if self.violation is PageAdaptationViolation.NOT_ADAPTED_PAGE:
            return page.items
        if self.violation is PageAdaptationViolation.CARDINALITY_CHANGED:
            return AdaptedPage(page.items[:-1])
        if self.violation is PageAdaptationViolation.ORDER_OR_IDENTITY_CHANGED:
            rows = [dict(item.items()) for item in page.items if isinstance(item, FrozenMapping)]
            rows[0]["id"] = 99
            return AdaptedPage(rows)
        if self.violation is PageAdaptationViolation.NON_JSON_VALUE:
            return AdaptedPage([object(), *page.items[1:]])
        raise RuntimeError("sensitive application detail")


@pytest.mark.asyncio
@pytest.mark.parametrize("violation", list(PageAdaptationViolation))
async def test_page_adapter_violations_are_typed_atomic_and_value_free(violation: PageAdaptationViolation) -> None:
    stream = _client(PageTransport()).iter_list_cursor(
        Request("item.list"),
        selector=ResultSelector(("messages",)),
        cursor=_cursor(),
        identity=_identity(),
        page_adapter=BrokenAdapter(violation),
    )
    with pytest.raises(PageAdaptationError) as raised:
        await anext(stream)
    error = raised.value
    assert error.violation is violation
    assert error.retryable is False
    assert error.page_offset == 0
    assert stream.report.emitted == 0
    safe = repr(error.to_safe_dict())
    assert "author_id" not in safe
    assert "sensitive application detail" not in safe
    assert isinstance(error.__cause__, RuntimeError) is (violation is PageAdaptationViolation.ADAPTER_RAISED)


class FailOnCall:
    def __init__(self, ordinal: int) -> None:
        self.ordinal = ordinal
        self.calls = 0

    def adapt(self, page: PageView, /) -> AdaptedPage:
        self.calls += 1
        if self.calls == self.ordinal:
            raise RuntimeError("private adapter exception")
        return IdentityPageAdapter().adapt(page)


@pytest.mark.asyncio
@pytest.mark.parametrize("ordinal", [1, 2])
async def test_reference_adapter_failure_preserves_first_vs_partial_mapping(ordinal: int) -> None:
    transport = CursorBatchTransport({"a": (1,)})
    adapter = FailOnCall(ordinal)
    outcomes = _client(transport).iter_reference_outcomes(
        Request("item.list", {"parent": "a"}),
        [Binding("a", (), "opaque")],
        traversal=CursorTraversal(ResultSelector.root(), _cursor(), _identity(), 1, adapter),
        dispatch=BatchDispatch(coalesce_wait=0),
    )
    collected = [outcome async for outcome in outcomes]
    failure = next(outcome for outcome in collected if isinstance(outcome, ReferenceFailure))
    if ordinal == 1:
        assert isinstance(failure.error, PageAdaptationError)
        assert failure.partial_rows == 0
    else:
        assert isinstance(collected[0], ReferenceItem)
        assert isinstance(failure.error, IncompleteTraversalError)
        assert isinstance(failure.error.__cause__, PageAdaptationError)
        assert failure.partial_rows == 1
    assert failure.correlation == "opaque"


class VerifierTransport:
    host = "test.invalid"

    def __init__(
        self,
        identities: tuple[int, ...],
        *,
        ignore_bounds: bool = False,
        mutation: str | None = None,
        observed_cap: int | None = None,
    ) -> None:
        self.identities = identities
        self.ignore_bounds = ignore_bounds
        self.mutation = mutation
        self.observed_cap = observed_cap
        self.batch_ordinal = 0
        self.requests: list[Request] = []

    def _rows(self, parameters: dict[str, JsonValue]) -> list[dict[str, int]]:
        selected = self.identities
        filters = parameters.get("filter", {})
        assert isinstance(filters, dict)
        if "ID" in filters:
            selected = tuple(value for value in selected if value == int(filters["ID"]))
            if self.mutation == "deleted" and int(filters["ID"]) == 2:
                selected = ()
        elif not self.ignore_bounds:
            if ">ID" in filters:
                selected = tuple(value for value in selected if value > int(filters[">ID"]))
            if "<ID" in filters:
                selected = tuple(value for value in selected if value < int(filters["<ID"]))
        order = parameters.get("order", {})
        descending = isinstance(order, dict) and order.get("id") == "DESC"
        if (
            self.batch_ordinal == 2
            and filters.get(">ID") == "0"
            and filters.get("<ID") == "3"
            and (self.mutation in {"missing", "deleted"} or (self.mutation == "contradiction" and descending))
        ):
            selected = tuple(value for value in selected if value != 2)
        if (
            self.batch_ordinal == 2
            and self.mutation == "extra"
            and filters.get(">ID") == "10"
            and filters.get("<ID") == "16"
        ):
            selected = (*selected, 13)
        limit = int(parameters.get("limit", 50))
        if self.observed_cap is not None:
            limit = min(limit, self.observed_cap)
        return [{"id": value} for value in sorted(set(selected), reverse=descending)[:limit]]

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        self.requests.append(request)
        assert request.method == "batch"
        self.batch_ordinal += 1
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        results = {key: self._rows(_decode_command(value)) for key, value in commands.items() if isinstance(value, str)}
        payload = {"result": {"result": results, "result_error": []}}
        return WireResponse(200, (("content-type", "application/json"),), json.dumps(payload).encode())


async def _verify(transport: VerifierTransport):
    return await _client(transport).verify_keyset_capability(
        Request("item.list", {"filter": {"STATUS": "open"}, "select": ["id"]}),
        selector=ResultSelector.root(),
        identity=_identity(),
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
    )


@pytest.mark.asyncio
async def test_keyset_verifier_verified_and_unsupported_cost_and_safe_error() -> None:
    verified = await _verify(VerifierTransport((1, 2, 3, 4)))
    assert verified.verdict is KeysetCapabilityVerdict.VERIFIED
    assert tuple(check.name for check in verified.checks) == tuple(KeysetCapabilityCheckName)
    assert {check.outcome for check in verified.checks} == {KeysetCapabilityCheckOutcome.PASSED}
    assert (verified.logical_commands, verified.batch_waves, verified.physical_requests) == (7, 2, 2)
    assert len(verified.page_trace) == 7
    assert [record.phase for record in verified.page_trace[:2]] == [KeysetPhase.BOUNDARY] * 2
    assert {record.phase for record in verified.page_trace[2:]} == {KeysetPhase.CANARY}
    assert verified.violations == ()

    with pytest.raises(KeysetCapabilityError) as raised:
        await _verify(VerifierTransport((553, 555, 711, 723), ignore_bounds=True))
    error = raised.value
    assert error.verdict is KeysetCapabilityVerdict.UNSUPPORTED
    assert all(check.outcome is KeysetCapabilityCheckOutcome.OUT_OF_INTERVAL_ROWS for check in error.report.checks)
    assert error.retryable is False
    safe = error.to_safe_dict()
    assert safe["logical_commands"] == 7
    assert "553" not in repr(safe)
    assert len(error.report.violations) == 5


@pytest.mark.asyncio
async def test_keyset_verifier_rejects_owned_controls_before_io_and_detects_hidden_cap() -> None:
    blocked_transport = VerifierTransport((1, 2, 3, 4))
    with pytest.raises(CapabilityError, match="control conflicts"):
        await _client(blocked_transport).verify_keyset_capability(
            Request("item.list", {"limit": 2}),
            selector=ResultSelector.root(),
            identity=_identity(),
            keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        )
    assert blocked_transport.requests == []

    with pytest.raises(KeysetCapabilityError) as raised:
        await _verify(VerifierTransport((1, 2, 3, 4), observed_cap=1))
    assert raised.value.report.inconclusive_reason is KeysetInconclusiveReason.PAGE_CAP_TOO_SMALL


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ("missing", KeysetInconclusiveReason.BOUND_OVER_RESTRICTIVE_SUSPECTED),
        ("deleted", KeysetInconclusiveReason.CONCURRENT_MUTATION),
        ("contradiction", KeysetInconclusiveReason.UNSTABLE_BOUNDARY),
        ("extra", KeysetInconclusiveReason.IN_RANGE_DRIFT),
    ],
)
async def test_keyset_verifier_drift_is_never_misclassified_as_unsupported(
    mutation: str,
    reason: KeysetInconclusiveReason,
) -> None:
    identities = (11, 15, 19, 20) if mutation == "extra" else (1, 2, 3, 4)
    with pytest.raises(KeysetCapabilityError) as raised:
        await _verify(VerifierTransport(identities, mutation=mutation))
    report = raised.value.report
    assert report.verdict is KeysetCapabilityVerdict.INCONCLUSIVE
    assert report.inconclusive_reason is reason
    assert report.logical_commands > 7
    rechecks = [check.recheck for check in report.checks if check.recheck is not None]
    assert rechecks
    assert all(recheck is rechecks[0] for recheck in rechecks)


def test_membership_recheck_accepts_json_values_and_enforces_partition() -> None:
    value = {"id": [1]}
    record = MembershipRecheck((value,), (value,), ())
    assert record.still_observed == (value,)
    with pytest.raises(ValueError, match="partition"):
        MembershipRecheck((value,), (), ())
    bounded = MembershipRecheck(tuple(range(10)), tuple(range(10)), ())
    assert bounded.identities == tuple(range(8))
    assert bounded.truncated is True


def test_keyset_capability_error_rejects_verified_report_and_exposes_only_safe_summary() -> None:
    report = KeysetCapabilityReport(
        KeysetCapabilityVerdict.VERIFIED,
        tuple(
            KeysetCapabilityCheckResult(name, KeysetCapabilityCheckOutcome.PASSED) for name in KeysetCapabilityCheckName
        ),
        2,
        2,
        7,
        False,
        None,
        None,
        (),
        (),
        False,
    )
    with pytest.raises(ValueError, match="returned"):
        KeysetCapabilityError(report=report)
    with pytest.raises(ValueError, match="physical_requests"):
        KeysetCapabilityReport(
            KeysetCapabilityVerdict.VERIFIED,
            report.checks,
            -1,
            2,
            7,
            False,
            None,
            None,
            (),
            (),
            False,
        )


def test_batch_dispatch_coalesce_wait_is_closed() -> None:
    assert BatchDispatch().coalesce_wait == 0.020
    assert BatchDispatch(coalesce_wait=0).coalesce_wait == 0
    for value in (-0.1, 1.1, float("inf"), float("nan")):
        with pytest.raises(ValueError, match="coalesce_wait"):
            BatchDispatch(coalesce_wait=value)


def test_page_adapter_protocol_is_structural() -> None:
    adapter: PageAdapter = EnrichMessages()
    assert callable(adapter.adapt)
    traversal = CursorTraversal(ResultSelector.root(), _cursor())
    assert isinstance(traversal.page_adapter, IdentityPageAdapter)


def test_frozen_selection_and_fingerprint_preserve_legacy_json_semantics() -> None:
    rows = [{"id": 1, "nested": [True, None, "кириллица"]}, {"id": 2, "number": 1.5}]
    response = Response({"items": rows, "sibling": {"large": [1, 2, 3]}})
    selected = _response_items(response, ResultSelector(("items",)))
    legacy = hashlib.sha256(
        json.dumps(rows, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(),
    ).hexdigest()
    assert _page_fingerprint(selected) == legacy

    mutable = [{"id": 1, "nested": ["before"]}]
    adapted = AdaptedPage(mutable)
    mutable[0]["id"] = 9
    mutable[0]["nested"].append("after")
    assert adapted.items[0]["id"] == 1  # type: ignore[index]
    assert adapted.items[0]["nested"] == ("before",)  # type: ignore[index]


def test_fast_keyset_lane_applies_adapter_only_to_publishable_phases() -> None:
    request = Request("item.list")
    response = Response([{"id": 1}])
    outcome = BatchSuccess(0, "body:0", request, response.result, response=response)
    lane = LaneState(
        LaneSpec(0, LaneKind.LANE, LaneBounds(0, 2), False, True, None),
        0,
        LaneStatus.OPEN,
        None,
        0,
        1,
        deque(),
    )

    class RaisingAdapter:
        def adapt(self, _page: PageView, /) -> AdaptedPage:
            raise RuntimeError("application-only detail")

    def validate(phase: KeysetPhase):
        return validate_lane_receipt(
            plan=LaneCommandPlan(0, "body:0", phase, request, 1, False),
            lane=lane,
            outcome=outcome,
            identity=_identity(),
            collection_shape=ResultCollectionShape.SEQUENCE,
            effective_page_cap=1,
            completion=KeysetPageCompletion.EMPTY_CONFIRMATION,
            page_adapter=RaisingAdapter(),
        )

    assert not isinstance(validate(KeysetPhase.ANCHOR_PROBE), ReceiptRejection)
    assert not isinstance(validate(KeysetPhase.CANARY), ReceiptRejection)
    rejected = validate(KeysetPhase.BODY)
    assert isinstance(rejected, ReceiptRejection)
    assert isinstance(rejected.error, PageAdaptationError)


@pytest.mark.asyncio
async def test_fast_source_fills_initial_and_continuation_batches() -> None:
    count = 50
    transport = CursorBatchTransport({str(index): (index + 1,) for index in range(count)})
    bindings = [
        Binding(
            str(index),
            (ParameterUpdate(ParameterPath(("parent",)), str(index)),),
            index,
        )
        for index in range(count)
    ]
    policy = ExecutionPolicy(
        max_active_references=count,
        max_buffered_rows=2 * count,
        max_pages=3 * count,
        max_pages_per_reference=3,
    )
    stream = _client(transport, policy=policy).iter_cursors(
        Request("item.list", {"parent": "base"}),
        bindings,
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=1,
        dispatch=BatchDispatch(batch_size=count),
    )

    assert len([event async for event in stream]) == 2 * count
    assert [len(request.copy_parameters()["cmd"]) for request in transport.requests] == [count, count]


@pytest.mark.asyncio
async def test_producer_state_broadcast_and_capacity_predicate_parity() -> None:
    state = _ProducerState(set(), {})
    seen = state.revision
    first, second = state.changed(seen), state.changed(seen)
    state.touch()
    assert first.done()
    assert second.done()
    cancelled = state.changed(state.revision)
    cancelled.cancel()
    state.touch()
    assert cancelled.cancelled()
    state.closing = True
    assert state.changed(state.revision).done()

    context = Executor(CursorBatchTransport({"a": ()})).context(
        ExecutionPolicy(max_pages=1, max_pages_per_reference=1, max_buffered_rows=2),
    )
    await context.start()
    assert context.can_reserve_page(reference="r0")
    page_reservation = await context.reserve_page(reference="r0")
    assert not context.can_reserve_page(reference="r0")
    context.release_page(page_reservation)
    assert context.can_reserve_page(reference="r0")

    buffer = _RowBuffer(2, context)
    assert buffer.can_reserve(0, 2)
    row_reservation = await buffer.reserve(0, 2)
    assert not buffer.can_reserve(1, 1)
    await buffer.abort(row_reservation)
    assert buffer.can_reserve(1, 1)
    await buffer.close()
    assert not buffer.can_reserve(0, 1)
