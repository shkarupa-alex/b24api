"""Acceptance coverage for cursor seeds, page adapters, and keyset verification."""

# ruff: noqa: ANN202, D101, D102, D107, FBT003, PLR2004, SLF001

from __future__ import annotations
import asyncio
import hashlib
import json
import random
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
    DeliveryOrder,
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
    PageRejectionCode,
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
from b24api.contracts.report import Violation, ViolationSeverity
from b24api.contracts.request import RouteKind
from b24api.contracts.response import Response, ResultCollectionShape
from b24api.execution import Executor, WireResponse
from b24api.references import dispatch as dispatch_module
from b24api.references.dispatch import _BatchPageDispatcher, _ProducerState, _RowBuffer
from b24api.traversal.keyset_fast_plan import LaneBounds, LaneKind, LaneSpec, LaneState, LaneStatus
from b24api.traversal.keyset_page_validation import LaneCommandPlan, ReceiptRejection, validate_lane_receipt
from b24api.traversal.values import _page_fingerprint, _response_items

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Mapping

    from b24api.contracts import JsonValue
    from b24api.contracts.page import PageView
    from b24api.contracts.report import OperationReport

# Coalescing tests run structurally: waits end by an event within bounded loop turns or by a timer.
# The contract caps coalesce_wait at one second; a wave that waited for that timer is a regression.
_UNREACHABLE_COALESCE_WAIT = 1
_REGRESSION_GUARD_SECONDS = 30
_EVENT_TURNS = 64
_CONSUMER_TURNS = 4
_COALESCE_BINDINGS = 8
_COALESCE_BATCH = 4


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


class JitterCursorBatchTransport(CursorBatchTransport):
    def __init__(self, rows: Mapping[str, tuple[int, ...]], seed: int) -> None:
        super().__init__(rows)
        self._random = random.Random(seed)

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        await asyncio.sleep(self._random.choice((0, 0.0005, 0.001)))
        return await super().send(
            request,
            attempt_timeout=attempt_timeout,
            max_response_bytes=max_response_bytes,
        )


@pytest.mark.asyncio
async def test_iter_cursors_normalizes_independent_seeds_and_keeps_correlation_off_wire() -> None:
    transport = CursorBatchTransport({"a": (1, 2, 3, 4), "b": (10, 11, 12)})
    correlations = ({"secret": "alpha"}, {"secret": "beta"})
    bindings = [
        Binding("a", (ParameterUpdate(ParameterPath(("parent",)), "a"),), correlations[0], start_cursor=2),
        Binding("b", (ParameterUpdate(ParameterPath(("parent",)), "b"),), correlations[1], start_cursor=10),
    ]
    stream = _client(transport).iter_cursors(
        Request("item.list", {"parent": "base"}, route=RouteKind.BARE),
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
        Request("item.list", {"parent": "base"}, route=RouteKind.BARE),
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
        Request("item.list", {"parent": "base"}, route=RouteKind.BARE),
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
        Request("item.list", {"parent": "a"}, route=RouteKind.BARE),
        [Binding("a", (), None, start_cursor=3)],
        selector=ResultSelector.root(),
        cursor=_cursor(),
        dispatch=BatchDispatch(coalesce_wait=0),
    )
    assert [event async for event in stream] == [ReferenceComplete(0, None, 0)]

    blocked = _client(transport).iter_cursors(
        Request("item.list", {"parent": "a"}, route=RouteKind.BARE),
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
        Request("item.list", {"parent": "a", "after": 0, "limit": 1}, route=RouteKind.BARE),
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
        Request("item.list", {"parent": "a", "after": 0, "limit": 1}, route=RouteKind.BARE),
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
        Request("item.list", route=RouteKind.BARE),
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
async def test_custom_adapter_without_configured_identity_preserves_generic_list_support() -> None:
    transport = PageTransport()
    adapter = EnrichMessages()
    stream = _client(transport).iter_list(
        Request("item.list", route=RouteKind.BARE),
        selector=ResultSelector(("messages",)),
        page_adapter=adapter,
    )
    assert await anext(stream) == {"id": 1, "author_id": 7, "author": "Ada"}
    assert adapter.calls == 1
    await stream.aclose()


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
        Request("item.list", route=RouteKind.BARE),
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
    assert stream.report.page_trace[-1].rejection_code is PageRejectionCode.PAGE_ADAPTATION
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
        Request("item.list", {"parent": "a"}, route=RouteKind.BARE),
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
        Request("item.list", {"filter": {"STATUS": "open"}, "select": ["id"]}, route=RouteKind.BARE),
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
            Request("item.list", {"limit": 2}, route=RouteKind.BARE),
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
    assert isinstance(record.still_observed[0], FrozenMapping)
    value["id"].append(2)
    assert record.identities[0]["id"] == (1,)  # type: ignore[index]
    with pytest.raises(TypeError):
        record.still_observed[0]["id"] = ()  # type: ignore[index]
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
    blocking = Violation(ViolationSeverity.BLOCKING, "keyset_capability_shape_invalid", "shape invalid")
    with pytest.raises(ValueError, match="no blocking violations"):
        KeysetCapabilityReport(
            KeysetCapabilityVerdict.VERIFIED,
            report.checks,
            2,
            2,
            7,
            False,
            None,
            None,
            (blocking,),
            (),
            False,
        )


def test_keyset_capability_check_evidence_is_deeply_immutable_and_json_safe() -> None:
    value = {"id": [1]}
    check = KeysetCapabilityCheckResult(
        KeysetCapabilityCheckName.LOWER_EMPTY,
        KeysetCapabilityCheckOutcome.OUT_OF_INTERVAL_ROWS,
        out_of_interval_identities=(value,),
    )
    value["id"].append(2)
    assert isinstance(check.out_of_interval_identities[0], FrozenMapping)
    assert check.out_of_interval_identities[0]["id"] == (1,)  # type: ignore[index]
    with pytest.raises(TypeError):
        check.out_of_interval_identities[0]["id"] = ()  # type: ignore[index]
    checks = tuple(
        check
        if name is KeysetCapabilityCheckName.LOWER_EMPTY
        else KeysetCapabilityCheckResult(name, KeysetCapabilityCheckOutcome.PASSED)
        for name in KeysetCapabilityCheckName
    )
    report = KeysetCapabilityReport(
        KeysetCapabilityVerdict.UNSUPPORTED,
        checks,
        1,
        1,
        1,
        False,
        None,
        None,
        (Violation(ViolationSeverity.BLOCKING, "keyset_capability_out_of_interval_rows", "outside"),),
        (),
        False,
    )
    assert json.loads(json.dumps(report.to_dict()))["checks"][0]["out_of_interval_identities"] == [{"id": [1]}]


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
    request = Request("item.list", route=RouteKind.BARE)
    response = Response([{"id": 1}])
    outcome = BatchSuccess(0, "body:0", request, response.result, response=response)
    lane = LaneState(
        LaneSpec(0, LaneKind.LANE, LaneBounds(0, 2), False, None),
        0,
        LaneStatus.OPEN,
        None,
        0,
        1,
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
    rejected = validate(KeysetPhase.BODY)
    assert isinstance(rejected, ReceiptRejection)
    assert isinstance(rejected.error, PageAdaptationError)
    assert rejected.violation.code == "page_adaptation"


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
        Request("item.list", {"parent": "base"}, route=RouteKind.BARE),
        bindings,
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=1,
        # Full waves dispatch at once; the widest wait keeps a slow host (coverage, CI) from splitting them.
        dispatch=BatchDispatch(batch_size=count, coalesce_wait=_UNREACHABLE_COALESCE_WAIT),
    )

    assert len([event async for event in stream]) == 2 * count
    assert [len(request.copy_parameters()["cmd"]) for request in transport.requests] == [count, count]
    state = stream._source._scheduler.producer_state
    assert state.runnable == state.admitting == state.pending_continuations == set()
    assert not state.source_pull_in_flight


@pytest.mark.asyncio
async def test_input_order_does_not_wait_for_an_unacknowledgeable_continuation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    waves: list[tuple[int, float]] = []

    def observe_wave(_dispatcher: _BatchPageDispatcher, commands: int, delay: float) -> None:
        waves.append((commands, delay))

    monkeypatch.setattr(_BatchPageDispatcher, "_observe_wave", observe_wave)
    transport = CursorBatchTransport({"slow": tuple(range(1, 11)), "fast": (100,)})
    bindings = [
        Binding(
            parent,
            (ParameterUpdate(ParameterPath(("parent",)), parent),),
            parent,
        )
        for parent in ("slow", "fast")
    ]
    stream = _client(transport).iter_cursors(
        Request("item.list", {"parent": "base"}, route=RouteKind.BARE),
        bindings,
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=1,
        dispatch=BatchDispatch(
            batch_size=2,
            concurrency=1,
            coalesce_wait=_UNREACHABLE_COALESCE_WAIT,
            output_order=DeliveryOrder.INPUT,
        ),
    )

    # The guard only bounds a regression; the assertion is structural: no wave waited for its timer.
    async with asyncio.timeout(_REGRESSION_GUARD_SECONDS):
        events = [event async for event in stream]
    assert len(events) == 13
    assert waves
    assert all(delay < _UNREACHABLE_COALESCE_WAIT for _commands, delay in waves)


@pytest.mark.asyncio
@pytest.mark.parametrize("output_order", [DeliveryOrder.READY, DeliveryOrder.INPUT])
async def test_sender_capacity_is_released_before_downstream_acknowledgement(
    output_order: DeliveryOrder,
) -> None:
    transport = CursorBatchTransport({"a": (1,), "b": (2,)})
    stream = _client(transport).iter_cursors(
        Request("item.list", {"parent": "base"}, route=RouteKind.BARE),
        [Binding(key, (ParameterUpdate(ParameterPath(("parent",)), key),), key) for key in transport.rows],
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=1,
        dispatch=BatchDispatch(batch_size=2, concurrency=1, output_order=output_order),
    )

    first = await anext(stream)
    assert isinstance(first, ReferenceItem)
    for _ in range(3):
        await asyncio.sleep(0)
    dispatcher = stream._source._scheduler.dispatcher
    assert isinstance(dispatcher, _BatchPageDispatcher)
    assert dispatcher._active_sends == 0

    assert len([event async for event in stream]) == 3


class _VirtualClock:
    """Deterministic coalescing clock: a wait ends by an event within bounded loop turns or by its timer."""

    def __init__(self) -> None:
        self.now = 0.0
        self.timer_firings = 0

    def time(self) -> float:
        return self.now

    async def wait(self, futures: tuple[asyncio.Future[object], ...], seconds: float) -> None:
        for _turn in range(_EVENT_TURNS):
            if any(future.done() for future in futures):
                return
            await asyncio.sleep(0)
        self.now += seconds
        self.timer_firings += 1


class _TimedCursorBatchTransport(CursorBatchTransport):
    def __init__(self, rows: Mapping[str, tuple[int, ...]], clock: Callable[[], float]) -> None:
        super().__init__(rows)
        self._clock = clock
        self.first_request_at: float | None = None

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        if self.first_request_at is None:
            self.first_request_at = self._clock()
        return await super().send(request, attempt_timeout=attempt_timeout, max_response_bytes=max_response_bytes)


async def _coalescing_run(
    output_order: DeliveryOrder,
    wait: float,
    clock: Callable[[], float],
    observations: list[tuple[int, float]],
) -> tuple[list[object], _TimedCursorBatchTransport, OperationReport | None, tuple[tuple[int, float], ...], float]:
    observations.clear()
    transport = _TimedCursorBatchTransport({str(index): (1, 2, 3) for index in range(_COALESCE_BINDINGS)}, clock)

    async def bindings() -> AsyncIterator[Binding[str]]:
        # An asynchronous source keeps binding admission on the event loop: a synchronous source is
        # pulled through a worker thread, whose wall-clock timing would reorder admission.
        for key in transport.rows:
            yield Binding(key, (ParameterUpdate(ParameterPath(("parent",)), key),), key)

    stream = _client(transport).iter_cursors(
        Request("item.list", {"parent": "base"}, route=RouteKind.BARE),
        bindings(),
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=1,
        dispatch=BatchDispatch(
            batch_size=_COALESCE_BATCH,
            concurrency=1,
            coalesce_wait=wait,
            output_order=output_order,
        ),
    )
    started = clock()
    events: list[object] = []
    async for event in stream:
        events.append(event)
        # A slow consumer hands control back to the producer instead of sleeping on the wall clock.
        for _turn in range(_CONSUMER_TURNS):
            await asyncio.sleep(0)
    assert transport.first_request_at is not None
    return events, transport, stream.report, tuple(observations), transport.first_request_at - started


@pytest.mark.asyncio
@pytest.mark.parametrize("output_order", [DeliveryOrder.READY, DeliveryOrder.INPUT])
async def test_slow_consumer_coalescing_is_bounded_per_wave_on_a_virtual_clock(
    monkeypatch: pytest.MonkeyPatch,
    output_order: DeliveryOrder,
) -> None:
    observations: list[tuple[int, float]] = []

    def observe_wave(_dispatcher: _BatchPageDispatcher, commands: int, delay: float) -> None:
        observations.append((commands, delay))

    monkeypatch.setattr(_BatchPageDispatcher, "_observe_wave", observe_wave)
    clock = _VirtualClock()
    monkeypatch.setattr(dispatch_module, "_loop_time", clock.time)
    monkeypatch.setattr(dispatch_module, "_wait_first", clock.wait)
    coalesce_wait = 0.005

    _baseline_events, _baseline_transport, _baseline_report, baseline_waves, _ = await _coalescing_run(
        output_order, 0, clock.time, observations
    )
    assert clock.timer_firings == 0
    events, transport, report, waves, first_request_latency = await _coalescing_run(
        output_order, coalesce_wait, clock.time, observations
    )

    assert len([event for event in events if isinstance(event, ReferenceItem)]) == 3 * _COALESCE_BINDINGS
    command_counts = sorted(commands for commands, _delay in waves)
    underfilled = [delay for commands, delay in waves if commands < _COALESCE_BATCH]
    assert len(waves) == len(transport.requests)
    assert sum(command_counts) == len(transport.commands) == 4 * _COALESCE_BINDINGS
    if output_order is DeliveryOrder.READY:
        assert len(underfilled) < len(waves)
        assert command_counts[-1] == _COALESCE_BATCH
    else:
        assert len(waves) > _COALESCE_BINDINGS
        assert len(underfilled) == len(waves)
        assert command_counts[-1] < _COALESCE_BATCH
    # At most one timer firing per underfilled wave, and no wave waits longer than its window.
    assert clock.timer_firings <= len(underfilled)
    assert all(delay <= coalesce_wait for _commands, delay in waves)
    assert sum(underfilled) <= coalesce_wait * len(underfilled)
    assert all(delay == 0 for _commands, delay in baseline_waves)
    assert first_request_latency <= coalesce_wait
    assert report is not None
    # The row buffer is bounded by the active bindings; the exact peak depends on consumer interleaving.
    assert 1 <= report.buffered_rows_high_water <= _COALESCE_BINDINGS
    assert report.active_references_high_water == _COALESCE_BINDINGS


@pytest.mark.benchmark
@pytest.mark.asyncio
@pytest.mark.parametrize("output_order", [DeliveryOrder.READY, DeliveryOrder.INPUT])
async def test_slow_consumer_records_bounded_per_wave_coalescing_cost(
    monkeypatch: pytest.MonkeyPatch,
    record_property: Callable[[str, object], None],
    output_order: DeliveryOrder,
) -> None:
    """Wall-clock coalescing benchmark; recorded for trend review and never part of the blocking gate."""
    observations: list[tuple[int, float]] = []

    def observe_wave(_dispatcher: _BatchPageDispatcher, commands: int, delay: float) -> None:
        observations.append((commands, delay))

    monkeypatch.setattr(_BatchPageDispatcher, "_observe_wave", observe_wave)
    coalesce_wait = 0.005
    clock = asyncio.get_running_loop().time

    started = clock()
    await _coalescing_run(output_order, 0, clock, observations)
    baseline_wall_clock = clock() - started
    started = clock()
    _events, transport, report, waves, first_request_latency = await _coalescing_run(
        output_order, coalesce_wait, clock, observations
    )
    wall_clock = clock() - started

    wave_count = len(waves)
    command_counts = sorted(commands for commands, _delay in waves)
    delays = sorted(delay for _commands, delay in waves)
    mean_delay = sum(delays) / wave_count
    p95_delay = delays[(95 * wave_count + 99) // 100 - 1]
    underfilled_delay = sum(delay for commands, delay in waves if commands < _COALESCE_BATCH)
    assert mean_delay <= 0.020
    assert p95_delay <= 0.020
    assert first_request_latency <= 0.020
    assert report is not None
    record_property(
        "coalescing_benchmark",
        json.dumps(
            {
                "output_order": output_order.value,
                "physical_waves": wave_count,
                "physical_requests": len(transport.requests),
                "underfilled_waves": sum(commands < _COALESCE_BATCH for commands in command_counts),
                "commands_mean": sum(command_counts) / wave_count,
                "commands_p50": command_counts[len(command_counts) // 2],
                "commands_p95": command_counts[(95 * wave_count + 99) // 100 - 1],
                "time_to_first_request": first_request_latency,
                "coalescing_mean": mean_delay,
                "coalescing_p95": p95_delay,
                "coalescing_total": sum(delays),
                "coalescing_underfilled_total": underfilled_delay,
                "wall_clock": wall_clock,
                "baseline_wall_clock": baseline_wall_clock,
                "wall_clock_ratio": wall_clock / baseline_wall_clock,
                "buffered_rows_high_water": report.buffered_rows_high_water,
                "active_references_high_water": report.active_references_high_water,
            },
            sort_keys=True,
        ),
    )


@pytest.mark.asyncio
async def test_zero_coalesce_never_subscribes_to_producer_waits(monkeypatch: pytest.MonkeyPatch) -> None:
    def reject_wait(_state: _ProducerState, _seen: int) -> object:
        raise AssertionError("zero coalesce must not wait for producer changes")

    monkeypatch.setattr(_ProducerState, "changed", reject_wait)
    stream = _client(CursorBatchTransport({"a": (1, 2)})).iter_cursors(
        Request("item.list", {"parent": "a"}, route=RouteKind.BARE),
        [Binding("a", (), "a")],
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=1,
        dispatch=BatchDispatch(coalesce_wait=0),
    )

    assert len([event async for event in stream]) == 3


@pytest.mark.asyncio
async def test_exhausted_single_producer_skips_wait_for_one_hundred_sequential_waves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_wait(_state: _ProducerState, _seen: int) -> object:
        raise AssertionError("an exhausted producer must not arm a coalescing wait")

    delays: list[float] = []

    def observe_wave(_dispatcher: _BatchPageDispatcher, _commands: int, delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(_ProducerState, "changed", reject_wait)
    monkeypatch.setattr(_BatchPageDispatcher, "_observe_wave", observe_wave)
    executor = Executor(CursorBatchTransport({"a": ()}))
    context = executor.context(
        ExecutionPolicy(max_pages=150, max_pages_per_reference=150, max_buffered_rows=2),
    )
    await context.start()
    state = _ProducerState({"r0"}, {"r0": 0})
    buffer = _RowBuffer(2, context, producer_state=state)
    dispatcher = _BatchPageDispatcher(
        executor,
        context,
        BatchDispatch(batch_size=50, concurrency=1, coalesce_wait=0.020),
        producer_state=state,
        buffer=buffer,
        page_cap=1,
    )
    for _ in range(101):
        page = await dispatcher.fetch(Request("item.list", {"parent": "a"}, route=RouteKind.BARE), "r0")
        assert page.admission is not None
        assert page.settlement is not None
        page.admission.set_result(None)
        page.settlement.set_result(None)
    assert len(delays) == 101
    assert all(delay == 0 for delay in delays)
    await dispatcher.aclose()
    await buffer.close()


@pytest.mark.asyncio
async def test_singular_cursor_deep_pagination_stays_on_the_direct_no_coalescer_path() -> None:
    transport = CursorBatchTransport({"a": tuple(range(1, 121))})
    stream = _client(transport).iter_list_cursor(
        Request("item.list", {"parent": "a"}, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=1,
    )
    assert len([row async for row in stream]) == 120
    assert len(transport.requests) == 121
    assert all(request.method == "item.list" for request in transport.requests)


@pytest.mark.asyncio
async def test_exhausted_ten_parent_deep_pagination_fills_every_wave_without_deadline_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observations: list[tuple[int, float]] = []

    def observe_wave(_dispatcher: _BatchPageDispatcher, commands: int, delay: float) -> None:
        observations.append((commands, delay))

    monkeypatch.setattr(_BatchPageDispatcher, "_observe_wave", observe_wave)
    rows = {str(index): tuple(range(index * 1000 + 1, index * 1000 + 101)) for index in range(10)}
    transport = CursorBatchTransport(rows)
    stream = _client(
        transport,
        policy=ExecutionPolicy(
            max_active_references=10,
            max_buffered_rows=20,
            max_pages=1200,
            max_pages_per_reference=120,
        ),
    ).iter_cursors(
        Request("item.list", {"parent": "base"}, route=RouteKind.BARE),
        [Binding(key, (ParameterUpdate(ParameterPath(("parent",)), key),), key) for key in rows],
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=1,
        dispatch=BatchDispatch(batch_size=10, concurrency=1, coalesce_wait=0.020),
    )

    events = [event async for event in stream]
    assert len(events) == 1010
    assert len(observations) >= 100
    assert sum(commands for commands, _delay in observations) == 1010
    assert all(commands == 10 for commands, _delay in observations)
    assert all(delay < 0.020 for _commands, delay in observations)


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(4))
async def test_capacity_saturated_jitter_preserves_every_page_and_releases_state(seed: int) -> None:
    count = 12
    rows = {str(index): tuple(range(index * 10 + 1, index * 10 + 5)) for index in range(count)}
    transport = JitterCursorBatchTransport(rows, seed)
    bindings = [
        Binding(
            key,
            (ParameterUpdate(ParameterPath(("parent",)), key),),
            key,
        )
        for key in rows
    ]
    stream = _client(
        transport,
        policy=ExecutionPolicy(
            max_active_references=count,
            max_buffered_rows=8,
            max_pages=80,
            max_pages_per_reference=6,
        ),
    ).iter_cursors(
        Request("item.list", {"parent": "base"}, route=RouteKind.BARE),
        bindings,
        selector=ResultSelector.root(),
        cursor=_cursor(),
        page_size=1,
        dispatch=BatchDispatch(batch_size=4, concurrency=3, coalesce_wait=0.005),
    )

    async with asyncio.timeout(2):
        events = [event async for event in stream]
    items = [event for event in events if isinstance(event, ReferenceItem)]
    assert sorted((event.correlation, event.item["id"]) for event in items) == sorted(
        (key, value) for key, values in rows.items() for value in values
    )
    scheduler = stream._source._scheduler
    state = scheduler.producer_state
    assert state.runnable == state.admitting == state.pending_continuations == set()
    assert not state.source_pull_in_flight
    assert scheduler.buffer._reservations == []
    assert scheduler.context._page_reservations == {}


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

    executor = Executor(CursorBatchTransport({"a": ()}))
    context = executor.context(
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

    blocked_state = _ProducerState(set(), {"r0": 0, "r1": 1}, admitting={"r1"})
    blocked_buffer = _RowBuffer(1, context, producer_state=blocked_state)
    held = await blocked_buffer.reserve(1, 1)
    dispatcher = _BatchPageDispatcher(
        executor,
        context,
        BatchDispatch(batch_size=2, coalesce_wait=1),
        producer_state=blocked_state,
        buffer=blocked_buffer,
        page_cap=1,
    )
    assert dispatcher._potential({"r0"}) == 0
    await blocked_buffer.abort(held)
    await blocked_buffer.close()

    settling_state = _ProducerState(set(), {"r0": 0})
    settling_buffer = _RowBuffer(1, context, producer_state=settling_state)
    settling_dispatcher = _BatchPageDispatcher(
        executor,
        context,
        BatchDispatch(batch_size=2, coalesce_wait=1),
        producer_state=settling_state,
        buffer=settling_buffer,
        page_cap=1,
    )
    settling_dispatcher._settling_waves = 1
    assert settling_dispatcher._potential({"r0"}) == 0
    settling_state.admitting.add("r0")
    assert settling_dispatcher._potential({"other"}) == 1
    await settling_buffer.close()

    queued_pull = _ProducerState(set(), {"r0": 0}, next_key="r1", next_index=1)
    pull_buffer = _RowBuffer(2, context, producer_state=queued_pull)
    pull_dispatcher = _BatchPageDispatcher(
        executor,
        context,
        BatchDispatch(batch_size=2, coalesce_wait=1),
        producer_state=queued_pull,
        buffer=pull_buffer,
        page_cap=1,
    )
    assert pull_dispatcher._potential({"r0"}) == 0
    queued_pull.source_pull_in_flight = True
    assert pull_dispatcher._potential({"r0"}) == 1
    await pull_buffer.close()


@pytest.mark.asyncio
async def test_settling_wave_counts_only_a_concrete_capacity_blocked_admission() -> None:
    executor = Executor(CursorBatchTransport({"a": ()}))
    context = executor.context(
        ExecutionPolicy(max_pages=2, max_pages_per_reference=2, max_buffered_rows=1),
    )
    await context.start()
    state = _ProducerState(set(), {"r0": 0}, admitting={"r0"})
    buffer = _RowBuffer(1, context, producer_state=state)
    held = await buffer.reserve(0, 1)
    dispatcher = _BatchPageDispatcher(
        executor,
        context,
        BatchDispatch(batch_size=2, coalesce_wait=1),
        producer_state=state,
        buffer=buffer,
        page_cap=1,
    )
    assert dispatcher._potential({"other"}) == 0
    dispatcher._settling_waves = 1
    assert dispatcher._potential({"other"}) == 1
    state.admitting.clear()
    assert dispatcher._potential({"other"}) == 0
    await buffer.abort(held)
    await buffer.close()
