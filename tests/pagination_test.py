"""W5 tests for lazy traversal lifecycle and sequential pagination proofs."""

from __future__ import annotations
import asyncio
import json
from typing import TYPE_CHECKING

import pytest

from b24api.error import BudgetExceededError, CapabilityError, PaginationError, ProtocolError
from b24api.execution import ExecutionContext, Executor, WireResponse
from b24api.models import (
    CompletionAssurance,
    ConfirmationPolicy,
    ConsistencyPolicy,
    DuplicatePolicy,
    ExecutionPolicy,
    IdentityCoercion,
    IdentityRequirement,
    IdentitySpec,
    JsonValue,
    OrderSemantics,
    ParameterPath,
    Request,
    ResultSelector,
    SnapshotRequirement,
    SnapshotState,
    TerminalState,
    TotalSemantics,
)
from b24api.pagination import iter_list
from b24api.plans import (
    CountedOffsetMode,
    CountedOffsetPlan,
    CursorTerminalRule,
    ItemCursorPlan,
    KeysetPlan,
    KeysetTerminalRule,
    ListPlan,
    OffsetContinuation,
    OffsetSequentialPlan,
    OffsetTerminalRule,
    PartitionedKeysetPlan,
    SingleResponsePlan,
)

if TYPE_CHECKING:
    from collections.abc import Callable

PAGE_SIZE = 2
THREE_ROWS = 3


class FunctionTransport:
    """Provide a deterministic test helper."""

    def __init__(self, handler: Callable[[Request], object]) -> None:
        """Initialize instance state."""
        self.handler = handler
        self.requests: list[Request] = []

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Send one transport request attempt."""
        del attempt_timeout, max_response_bytes
        self.requests.append(request)
        body = json.dumps(self.handler(request), separators=(",", ":")).encode()
        return WireResponse(200, (("content-type", "application/json"),), body)


class BlockingTransport:
    """Provide a deterministic test helper."""

    def __init__(self) -> None:
        """Initialize instance state."""
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Send one transport request attempt."""
        del request, attempt_timeout, max_response_bytes
        self.started.set()
        try:
            return await asyncio.Future[WireResponse]()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


def _identity() -> IdentitySpec:
    return IdentitySpec(
        item_path=("ID",),
        filter_key="ID",
        order_key="ID",
        coercion=IdentityCoercion.EXACT_INTEGER,
    )


def _integer_parameter(request: Request, name: str) -> int:
    value = request.copy_parameters()[name]
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    return value


def _optional_integer_parameter(request: Request, name: str) -> int | None:
    value = request.copy_parameters().get(name)
    if value is not None and (not isinstance(value, int) or isinstance(value, bool)):
        raise TypeError(f"{name} must be an integer or absent")
    return value


def _offset_plan(*, duplicate_policy: DuplicatePolicy = DuplicatePolicy.ERROR) -> OffsetSequentialPlan:
    return OffsetSequentialPlan(
        limit_path=ParameterPath(("limit",)),
        requested_page_size=PAGE_SIZE,
        continuation=OffsetContinuation.OBSERVED_COUNT,
        terminal=frozenset({OffsetTerminalRule.EMPTY_PAGE}),
        duplicate_policy=duplicate_policy,
    )


def _keyset_plan() -> KeysetPlan:
    return KeysetPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
        limit_path=ParameterPath(("limit",)),
        requested_page_size=PAGE_SIZE,
    )


async def _collect(stream: object) -> list[JsonValue]:
    return [item async for item in stream]  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_single_stream_is_lazy_and_reports_scalar_completion() -> None:
    transport = FunctionTransport(lambda _request: {"result": {"ID": 7}})
    stream = iter_list(Executor(transport), Request("crm.item.get"), plan=SingleResponsePlan())

    assert transport.requests == []
    assert await _collect(stream) == [{"ID": 7}]
    assert len(transport.requests) == 1
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.logical_pages == 1
    assert stream.report.emitted_rows == 1


@pytest.mark.asyncio
async def test_async_context_entry_starts_execution_without_delivering_prefetched_item() -> None:
    transport = FunctionTransport(lambda _request: {"result": {"ID": 7}})
    stream = iter_list(Executor(transport), Request("crm.item.get"), plan=SingleResponsePlan())

    async with stream as entered:
        assert len(transport.requests) == 1
        assert entered.report.state is TerminalState.NOT_STARTED
        assert await _collect(entered) == [{"ID": 7}]

    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.emitted_rows == 1


@pytest.mark.asyncio
async def test_single_rejects_continuation_and_records_failure() -> None:
    transport = FunctionTransport(lambda _request: {"result": [], "next": 2})
    stream = iter_list(Executor(transport), Request("crm.item.list"), plan=SingleResponsePlan())

    with pytest.raises(CapabilityError, match="continuation") as captured:
        await _collect(stream)

    assert stream.report.state is TerminalState.FAILED
    assert stream.report.logical_pages == 1
    assert captured.value.__dict__["report"] is stream.report


@pytest.mark.asyncio
async def test_offset_does_not_treat_arbitrary_short_page_as_terminal_or_mutate_caller() -> None:
    original = {"filter": {"ACTIVE": "Y"}}

    def handler(request: Request) -> dict[str, object]:
        start = _integer_parameter(request, "start")
        pages = {
            0: [{"ID": 1}, {"ID": 2}],
            2: [{"ID": 3}],
            3: [],
        }
        return {"result": pages[start]}

    transport = FunctionTransport(handler)
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", original),
        plan=_offset_plan(),
        identity=_identity(),
    )

    assert await _collect(stream) == [{"ID": 1}, {"ID": 2}, {"ID": 3}]
    assert [request.copy_parameters()["start"] for request in transport.requests] == [0, 2, 3]
    assert original == {"filter": {"ACTIVE": "Y"}}
    assert stream.report.unique_rows == THREE_ROWS
    assert stream.report.terminal_reason == "empty page confirmed terminal"


@pytest.mark.asyncio
async def test_offset_detects_ignored_control_by_repeated_page_fingerprint() -> None:
    transport = FunctionTransport(lambda _request: {"result": [{"ID": 1}]})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=_offset_plan(),
        identity=_identity(),
    )

    with pytest.raises(PaginationError, match="repeated page"):
        await _collect(stream)

    assert len(transport.requests) == PAGE_SIZE
    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_required_identity_and_oversized_pages_are_rejected() -> None:
    no_identity_transport = FunctionTransport(lambda _request: {"result": []})
    required = OffsetSequentialPlan(identity_requirement=IdentityRequirement.REQUIRED)
    stream = iter_list(Executor(no_identity_transport), Request("crm.item.list"), plan=required)
    with pytest.raises(CapabilityError, match="IdentitySpec"):
        await _collect(stream)
    assert no_identity_transport.requests == []

    oversized_transport = FunctionTransport(
        lambda _request: {"result": [{"ID": 1}, {"ID": 2}, {"ID": 3}]},
    )
    oversized = iter_list(
        Executor(oversized_transport),
        Request("crm.item.list"),
        plan=_offset_plan(),
        identity=_identity(),
    )
    with pytest.raises(PaginationError, match="page cap"):
        await _collect(oversized)
    assert len(oversized_transport.requests) == 1


@pytest.mark.asyncio
async def test_offset_exact_total_must_be_present_stable_and_not_overshot() -> None:
    responses = [
        {"result": [{"ID": 1}], "total": 2},
        {"result": [{"ID": 2}], "total": 3},
    ]
    transport = FunctionTransport(lambda _request: responses.pop(0))
    plan = OffsetSequentialPlan(
        continuation=OffsetContinuation.OBSERVED_COUNT,
        terminal=frozenset({OffsetTerminalRule.QUALIFIED_TOTAL}),
        total_semantics=TotalSemantics.FILTERED_EXACT,
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=plan,
        identity=_identity(),
    )

    with pytest.raises(PaginationError, match="total drifted"):
        await _collect(stream)


@pytest.mark.asyncio
async def test_empty_page_cannot_override_an_unreached_exact_total() -> None:
    responses = [
        {"result": [{"ID": 1}], "total": 2},
        {"result": [], "total": 2},
    ]
    transport = FunctionTransport(lambda _request: responses.pop(0))
    plan = OffsetSequentialPlan(
        continuation=OffsetContinuation.OBSERVED_COUNT,
        terminal=frozenset({OffsetTerminalRule.EMPTY_PAGE, OffsetTerminalRule.QUALIFIED_TOTAL}),
        total_semantics=TotalSemantics.FILTERED_EXACT,
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=plan,
        identity=_identity(),
    )

    with pytest.raises(PaginationError, match="before its exact total"):
        await _collect(stream)


@pytest.mark.asyncio
async def test_short_page_does_not_escape_before_exact_total_validation() -> None:
    transport = FunctionTransport(lambda _request: {"result": [{"ID": 1}], "total": 2})
    plan = OffsetSequentialPlan(
        limit_path=ParameterPath(("limit",)),
        requested_page_size=2,
        continuation=OffsetContinuation.OBSERVED_COUNT,
        terminal=frozenset(
            {OffsetTerminalRule.PROFILE_SHORT_PAGE, OffsetTerminalRule.QUALIFIED_TOTAL},
        ),
        total_semantics=TotalSemantics.FILTERED_EXACT,
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=plan,
        identity=_identity(),
    )

    with pytest.raises(PaginationError, match="before its exact total"):
        await anext(stream)
    assert stream.report.emitted_rows == 0


@pytest.mark.asyncio
async def test_page_budget_refuses_continuation_before_network_io() -> None:
    transport = FunctionTransport(lambda _request: {"result": [{"ID": 1}]})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=_offset_plan(),
        identity=_identity(),
        policy=ExecutionPolicy(max_pages=1),
    )

    with pytest.raises(BudgetExceededError, match="page budget"):
        await _collect(stream)

    assert [_integer_parameter(request, "start") for request in transport.requests] == [0]
    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_selector_shape_failure_is_typed_and_reported() -> None:
    transport = FunctionTransport(lambda _request: {"result": {"items": {"ID": 1}}})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=SingleResponsePlan(selector=None),
        selector=ResultSelector(("items",)),
    )

    with pytest.raises(CapabilityError, match="selector") as captured:
        await _collect(stream)

    assert captured.value.__dict__["report"] is stream.report


@pytest.mark.asyncio
async def test_counted_offset_requires_one_stable_non_negative_exact_total() -> None:
    def handler(request: Request) -> dict[str, object]:
        start = _integer_parameter(request, "start")
        return (
            {"result": [{"ID": 1}, {"ID": 2}], "total": 3, "next": 2}
            if start == 0
            else {"result": [{"ID": 3}], "total": 3}
        )

    transport = FunctionTransport(handler)
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=CountedOffsetPlan(),
        identity=_identity(),
    )

    assert await _collect(stream) == [{"ID": 1}, {"ID": 2}, {"ID": 3}]
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.terminal_reason == "qualified total reached"


@pytest.mark.asyncio
async def test_counted_offset_detects_repeated_items_when_continuation_metadata_changes() -> None:
    responses = [
        {"result": [{"ID": 1}, {"ID": 2}], "total": 4, "next": 2},
        {"result": [{"ID": 1}, {"ID": 2}], "total": 4},
    ]
    transport = FunctionTransport(lambda _request: responses.pop(0))
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=CountedOffsetPlan(),
    )

    with pytest.raises(PaginationError, match="repeated page"):
        await _collect(stream)

    assert len(transport.requests) == PAGE_SIZE
    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "plan",
    [
        SingleResponsePlan(
            identity_requirement=IdentityRequirement.REQUIRED,
            order_semantics=OrderSemantics.ASCENDING,
        ),
        OffsetSequentialPlan(
            identity_requirement=IdentityRequirement.REQUIRED,
            order_semantics=OrderSemantics.ASCENDING,
        ),
        CountedOffsetPlan(
            identity_requirement=IdentityRequirement.REQUIRED,
            order_semantics=OrderSemantics.ASCENDING,
        ),
    ],
)
async def test_declared_order_is_enforced_for_single_and_offset_plans(plan: ListPlan) -> None:
    transport = FunctionTransport(
        lambda _request: {"result": [{"ID": 2}, {"ID": 1}], "total": PAGE_SIZE},
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=plan,
        identity=_identity(),
    )

    with pytest.raises(PaginationError, match="strictly ascending"):
        await _collect(stream)

    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_consistency_policy_requires_identity_before_io() -> None:
    transport = FunctionTransport(lambda _request: {"result": []})
    policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(identity_requirement=IdentityRequirement.REQUIRED),
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=SingleResponsePlan(),
        policy=policy,
    )

    with pytest.raises(CapabilityError, match="IdentitySpec"):
        await _collect(stream)

    assert transport.requests == []


@pytest.mark.asyncio
async def test_consistency_policy_enforces_duplicates_order_total_and_confirmation() -> None:
    duplicate_transport = FunctionTransport(
        lambda _request: {"result": [{"ID": 1}, {"ID": 1}]},
    )
    duplicate_stream = iter_list(
        Executor(duplicate_transport),
        Request("crm.item.list"),
        plan=SingleResponsePlan(duplicate_policy=DuplicatePolicy.ALLOW_DECLARED_MULTISET),
        identity=_identity(),
    )
    with pytest.raises(PaginationError, match="duplicate identity"):
        await _collect(duplicate_stream)

    order_transport = FunctionTransport(
        lambda _request: {"result": [{"ID": 2}, {"ID": 1}]},
    )
    order_policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(order_semantics=OrderSemantics.ASCENDING),
    )
    order_stream = iter_list(
        Executor(order_transport),
        Request("crm.item.list"),
        plan=SingleResponsePlan(),
        identity=_identity(),
        policy=order_policy,
    )
    with pytest.raises(PaginationError, match="strictly ascending"):
        await _collect(order_stream)

    total_transport = FunctionTransport(
        lambda _request: {"result": [{"ID": 1}, {"ID": 2}], "total": 1},
    )
    total_policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(total_semantics=TotalSemantics.FILTERED_EXACT),
    )
    total_stream = iter_list(
        Executor(total_transport),
        Request("crm.item.list"),
        plan=SingleResponsePlan(),
        policy=total_policy,
    )
    with pytest.raises(PaginationError, match="exact total"):
        await _collect(total_stream)

    confirmation_transport = FunctionTransport(lambda _request: {"result": []})
    confirmation_policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(confirmation_policy=ConfirmationPolicy.EMPTY_AFTER_BOUNDARY),
    )
    confirmation_stream = iter_list(
        Executor(confirmation_transport),
        Request("crm.item.list"),
        plan=SingleResponsePlan(),
        policy=confirmation_policy,
    )
    with pytest.raises(CapabilityError, match="confirmation"):
        await _collect(confirmation_stream)
    assert confirmation_transport.requests == []


@pytest.mark.asyncio
async def test_empty_boundary_policy_requires_the_empty_offset_confirmation() -> None:
    def handler(request: Request) -> dict[str, object]:
        start = _integer_parameter(request, "start")
        rows = [{"ID": 1}, {"ID": 2}] if start == 0 else []
        return {"result": rows, "total": PAGE_SIZE}

    transport = FunctionTransport(handler)
    plan = OffsetSequentialPlan(
        continuation=OffsetContinuation.OBSERVED_COUNT,
        terminal=frozenset({OffsetTerminalRule.QUALIFIED_TOTAL, OffsetTerminalRule.EMPTY_PAGE}),
        total_semantics=TotalSemantics.FILTERED_EXACT,
    )
    policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(
            total_semantics=TotalSemantics.FILTERED_EXACT,
            confirmation_policy=ConfirmationPolicy.EMPTY_AFTER_BOUNDARY,
        ),
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=plan,
        identity=_identity(),
        policy=policy,
    )

    assert await _collect(stream) == [{"ID": 1}, {"ID": 2}]
    assert [_integer_parameter(request, "start") for request in transport.requests] == [0, PAGE_SIZE]
    assert stream.report.terminal_reason == "empty page confirmed terminal"


@pytest.mark.asyncio
async def test_advisory_total_mismatch_is_reported_without_blocking_completion() -> None:
    transport = FunctionTransport(
        lambda _request: {"result": [{"ID": 1}, {"ID": 2}], "total": 99},
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=SingleResponsePlan(reject_positive_total_over_result=False),
        policy=ExecutionPolicy(
            consistency=ConsistencyPolicy(total_semantics=TotalSemantics.ADVISORY),
        ),
    )

    assert await _collect(stream) == [{"ID": 1}, {"ID": 2}]
    assert stream.report.state is TerminalState.COMPLETED
    assert [violation.code for violation in stream.report.violations] == ["advisory_total_mismatch"]


@pytest.mark.asyncio
async def test_conflicting_policy_and_plan_semantics_refuse_before_io() -> None:
    transport = FunctionTransport(lambda _request: {"result": []})
    plan = CountedOffsetPlan(order_semantics=OrderSemantics.ASCENDING)
    policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(
            total_semantics=TotalSemantics.GLOBAL,
            order_semantics=OrderSemantics.DESCENDING,
        ),
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=plan,
        identity=_identity(),
        policy=policy,
    )

    with pytest.raises(CapabilityError, match=r"total semantics|order semantics"):
        await _collect(stream)
    assert transport.requests == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("responses", "message"),
    [
        (({"result": [], "total": -1},), "non-negative"),
        (
            (
                {"result": [{"ID": 1}], "total": 2, "next": 1},
                {"result": [{"ID": 2}], "total": 3},
            ),
            "drifted",
        ),
        (
            (
                {"result": [{"ID": 1}], "total": 2, "next": 1},
                {"result": [], "total": 2},
            ),
            "before its exact total",
        ),
    ],
)
async def test_counted_offset_rejects_unproven_totals(
    responses: tuple[dict[str, object], ...],
    message: str,
) -> None:
    pending = list(responses)
    transport = FunctionTransport(lambda _request: pending.pop(0))
    stream = iter_list(Executor(transport), Request("crm.item.list"), plan=CountedOffsetPlan())

    with pytest.raises((CapabilityError, PaginationError), match=message):
        await _collect(stream)

    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_unreviewed_parallel_and_partitioned_strategies_refuse_before_io() -> None:
    transport = FunctionTransport(lambda _request: {"result": []})
    parallel = CountedOffsetPlan(
        mode=CountedOffsetMode.PARALLEL_FIXED_STRIDE,
        limit_path=ParameterPath(("limit",)),
        requested_page_size=PAGE_SIZE,
        fixed_stride=PAGE_SIZE,
    )
    partitioned = PartitionedKeysetPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
    )
    boundary = KeysetPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
        terminal=KeysetTerminalRule.BOUNDARY_ID_SEEN,
    )

    for plan in (parallel, partitioned, boundary):
        stream = iter_list(Executor(transport), Request("crm.item.list"), plan=plan, identity=_identity())
        with pytest.raises(CapabilityError):
            await _collect(stream)

    assert transport.requests == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("plan", "policy", "message"),
    [
        (
            SingleResponsePlan(),
            ExecutionPolicy(
                consistency=ConsistencyPolicy(
                    confirmation_policy=ConfirmationPolicy.INDEPENDENT_ORACLE,
                ),
            ),
            "independent oracle",
        ),
        (
            SingleResponsePlan(),
            ExecutionPolicy(
                consistency=ConsistencyPolicy(
                    confirmation_policy=ConfirmationPolicy.BOUNDARY_ID_SEEN,
                ),
            ),
            "boundary identity",
        ),
        (
            SingleResponsePlan(order_semantics=OrderSemantics.INPUT),
            ExecutionPolicy(),
            "input order semantics",
        ),
        (
            SingleResponsePlan(),
            ExecutionPolicy(
                consistency=ConsistencyPolicy(order_semantics=OrderSemantics.INPUT),
            ),
            "input order semantics",
        ),
    ],
)
async def test_unadmitted_consistency_controls_refuse_before_io(
    plan: ListPlan,
    policy: ExecutionPolicy,
    message: str,
) -> None:
    transport = FunctionTransport(lambda _request: {"result": []})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=plan,
        policy=policy,
    )

    with pytest.raises(CapabilityError, match=message):
        await anext(stream)
    assert transport.requests == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("plan", "row", "terminal_reason"),
    [
        (
            KeysetPlan(
                identity_requirement=IdentityRequirement.REQUIRED,
                order_semantics=OrderSemantics.ASCENDING,
                limit_path=ParameterPath(("limit",)),
                requested_page_size=PAGE_SIZE,
                terminal=KeysetTerminalRule.PROFILE_SHORT_PAGE,
            ),
            {"ID": 1},
            "profile-authorized short keyset page",
        ),
        (
            ItemCursorPlan(
                identity_requirement=IdentityRequirement.REQUIRED,
                cursor_item_path=("cursor",),
                limit_path=ParameterPath(("limit",)),
                requested_page_size=PAGE_SIZE,
                terminal=CursorTerminalRule.PROFILE_SHORT_PAGE,
            ),
            {"ID": 1, "cursor": 10},
            "profile-authorized short cursor page",
        ),
    ],
)
async def test_profile_short_page_terminates_keyset_and_cursor_traversal(
    plan: ListPlan,
    row: JsonValue,
    terminal_reason: str,
) -> None:
    transport = FunctionTransport(lambda _request: {"result": [row]})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=plan,
        identity=_identity(),
    )

    assert await _collect(stream) == [row]
    assert len(transport.requests) == 1
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.assurance is CompletionAssurance.CALLER_ASSERTED
    assert stream.report.violations == ()
    assert stream.report.terminal_reason == terminal_reason


@pytest.mark.asyncio
async def test_keyset_injects_exact_controls_and_requires_empty_confirmation() -> None:
    def handler(request: Request) -> dict[str, object]:
        parameters = request.copy_parameters()
        cursor = parameters.get("filter", {}).get(">ID")  # type: ignore[union-attr]
        if cursor is None:
            return {"result": [{"ID": 1}, {"ID": 2}]}
        if cursor == PAGE_SIZE:
            return {"result": [{"ID": 3}]}
        return {"result": []}

    transport = FunctionTransport(handler)
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=_keyset_plan(),
        identity=_identity(),
    )

    assert await _collect(stream) == [{"ID": 1}, {"ID": 2}, {"ID": 3}]
    sent = [request.copy_parameters() for request in transport.requests]
    assert sent[0] == {"order": {"ID": "ASC"}, "limit": 2, "start": -1}
    assert sent[1]["filter"] == {">ID": 2}
    assert sent[2]["filter"] == {">ID": 3}
    assert stream.report.terminal_reason == "empty keyset confirmation"


@pytest.mark.asyncio
async def test_keyset_rejects_page_that_does_not_respect_previous_bound() -> None:
    responses = [
        {"result": [{"ID": 1}, {"ID": 2}]},
        {"result": [{"ID": 2}, {"ID": 3}]},
    ]
    transport = FunctionTransport(lambda _request: responses.pop(0))
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=_keyset_plan(),
        identity=_identity(),
    )

    with pytest.raises(PaginationError, match=r"advance|lower bound"):
        await _collect(stream)


@pytest.mark.asyncio
async def test_item_cursor_advances_from_items_until_empty_confirmation() -> None:
    def handler(request: Request) -> dict[str, object]:
        cursor = _optional_integer_parameter(request, "LAST_ID")
        pages = {None: [{"ID": 1}, {"ID": 2}], 2: [{"ID": 3}], 3: []}
        return {"result": pages[cursor]}

    transport = FunctionTransport(handler)
    plan = ItemCursorPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
        cursor_item_path=("ID",),
        cursor_take="max",
        terminal=CursorTerminalRule.EMPTY_CONFIRMATION,
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=plan,
        identity=_identity(),
    )

    assert await _collect(stream) == [{"ID": 1}, {"ID": 2}, {"ID": 3}]
    assert [request.copy_parameters().get("LAST_ID") for request in transport.requests] == [None, 2, 3]


@pytest.mark.asyncio
async def test_item_cursor_orders_cursor_values_independently_from_row_identity() -> None:
    responses = [
        {"result": [{"ID": 20, "cursor": 1}, {"ID": 10, "cursor": 2}]},
        {"result": [{"ID": 10, "cursor": 3}]},
        {"result": []},
    ]
    transport = FunctionTransport(lambda _request: responses.pop(0))
    plan = ItemCursorPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        cursor_item_path=("cursor",),
        direction="asc",
        cursor_take="max",
        duplicate_policy=DuplicatePolicy.REPORT,
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=plan,
        identity=_identity(),
        policy=ExecutionPolicy(
            consistency=ConsistencyPolicy(duplicate_policy=DuplicatePolicy.REPORT),
        ),
    )

    assert await _collect(stream) == [
        {"ID": 20, "cursor": 1},
        {"ID": 10, "cursor": 2},
        {"ID": 10, "cursor": 3},
    ]
    assert [request.copy_parameters().get("LAST_ID") for request in transport.requests] == [None, 2, 3]
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.unique_rows == PAGE_SIZE
    assert [violation.code for violation in stream.report.violations] == ["duplicate_identity"]


@pytest.mark.asyncio
async def test_item_cursor_uses_independent_cursor_coercion() -> None:
    responses = [
        {"result": [{"uuid": "a", "cursor": 1}, {"uuid": "b", "cursor": 2}]},
        {"result": []},
    ]
    transport = FunctionTransport(lambda _request: responses.pop(0))
    plan = ItemCursorPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        cursor_item_path=("cursor",),
        cursor_coercion=IdentityCoercion.EXACT_INTEGER,
    )
    identity = IdentitySpec(
        item_path=("uuid",),
        filter_key="uuid",
        order_key="uuid",
        coercion=IdentityCoercion.EXACT_STRING,
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=plan,
        identity=identity,
    )

    assert await _collect(stream) == [
        {"uuid": "a", "cursor": 1},
        {"uuid": "b", "cursor": 2},
    ]
    assert [request.copy_parameters().get("LAST_ID") for request in transport.requests] == [None, 2]
    assert stream.report.state is TerminalState.COMPLETED


@pytest.mark.asyncio
@pytest.mark.parametrize("row", [{"ID": 1}, {"ID": 1, "cursor": None}])
async def test_profile_cursor_exhaustion_delivers_last_page_without_cursor(row: dict[str, JsonValue]) -> None:
    transport = FunctionTransport(lambda _request: {"result": [row]})
    plan = ItemCursorPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        cursor_item_path=("cursor",),
        terminal=CursorTerminalRule.PROFILE_CURSOR_EXHAUSTED,
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=plan,
        identity=_identity(),
    )

    assert await _collect(stream) == [row]
    assert len(transport.requests) == 1
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.terminal_reason == "profile-authorized cursor exhaustion"


@pytest.mark.asyncio
async def test_profile_cursor_exhaustion_rejects_mixed_cursor_presence() -> None:
    transport = FunctionTransport(
        lambda _request: {"result": [{"ID": 1, "cursor": 1}, {"ID": 2}]},
    )
    plan = ItemCursorPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        cursor_item_path=("cursor",),
        terminal=CursorTerminalRule.PROFILE_CURSOR_EXHAUSTED,
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=plan,
        identity=_identity(),
    )

    with pytest.raises(PaginationError, match="cursor exhaustion is inconsistent"):
        await _collect(stream)
    assert stream.report.emitted_rows == 0
    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("plan", "policy"),
    [
        (
            SingleResponsePlan(identity_requirement=IdentityRequirement.COMPOSITE),
            ExecutionPolicy(),
        ),
        (
            SingleResponsePlan(),
            ExecutionPolicy(
                consistency=ConsistencyPolicy(identity_requirement=IdentityRequirement.COMPOSITE),
            ),
        ),
    ],
)
async def test_composite_identity_refuses_before_io(
    plan: ListPlan,
    policy: ExecutionPolicy,
) -> None:
    transport = FunctionTransport(lambda _request: {"result": []})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=plan,
        policy=policy,
    )

    with pytest.raises(CapabilityError, match="composite identity"):
        await anext(stream)
    assert transport.requests == []


@pytest.mark.asyncio
async def test_item_cursor_rejects_wrong_order_within_first_page() -> None:
    transport = FunctionTransport(lambda _request: {"result": [{"ID": 2}, {"ID": 1}]})
    plan = ItemCursorPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
        cursor_item_path=("ID",),
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=plan,
        identity=_identity(),
    )

    with pytest.raises(PaginationError, match="strictly ascending"):
        await _collect(stream)


@pytest.mark.asyncio
async def test_item_cursor_uses_internal_monotonic_tracking() -> None:
    def handler(request: Request) -> object:
        if "LAST_ID" in request.parameters:
            return {"result": []}
        return {"result": [{"ID": 1, "cursor": 1}]}

    transport = FunctionTransport(handler)
    plan = ItemCursorPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
        cursor_item_path=("cursor",),
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=plan,
        identity=_identity(),
    )

    assert await _collect(stream) == [{"ID": 1, "cursor": 1}]
    assert len(transport.requests) == PAGE_SIZE


@pytest.mark.asyncio
async def test_latent_keyset_filter_collision_refuses_before_io() -> None:
    transport = FunctionTransport(lambda _request: {"result": [{"ID": 1}]})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", {"filter": {">ID": 99}}),
        plan=_keyset_plan(),
        identity=_identity(),
    )

    with pytest.raises(CapabilityError, match="conflict"):
        await anext(stream)
    assert transport.requests == []


@pytest.mark.asyncio
async def test_latent_item_cursor_collision_refuses_before_io() -> None:
    transport = FunctionTransport(lambda _request: {"result": [{"ID": 1}]})
    plan = ItemCursorPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
        cursor_item_path=("ID",),
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", {"LAST_ID": 99}),
        plan=plan,
        identity=_identity(),
    )

    with pytest.raises(CapabilityError, match="conflict"):
        await anext(stream)
    assert transport.requests == []


@pytest.mark.asyncio
async def test_duplicate_report_preserves_multiset_and_exact_unique_count() -> None:
    def handler(request: Request) -> dict[str, object]:
        start = _integer_parameter(request, "start")
        pages = {0: [{"ID": 1}, {"ID": 2}], 2: [{"ID": 2}], 3: []}
        return {"result": pages[start]}

    transport = FunctionTransport(handler)
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=_offset_plan(duplicate_policy=DuplicatePolicy.REPORT),
        identity=_identity(),
        policy=ExecutionPolicy(
            consistency=ConsistencyPolicy(duplicate_policy=DuplicatePolicy.REPORT),
        ),
    )

    assert await _collect(stream) == [{"ID": 1}, {"ID": 2}, {"ID": 2}]
    assert stream.report.emitted_rows == THREE_ROWS
    assert stream.report.unique_rows == PAGE_SIZE
    assert [violation.code for violation in stream.report.violations] == ["duplicate_identity"]


@pytest.mark.asyncio
async def test_early_close_counts_only_unique_rows_delivered_from_later_page() -> None:
    def handler(request: Request) -> object:
        start = _integer_parameter(request, "start")
        pages = {
            0: [{"ID": 1, "revision": "a"}],
            1: [{"ID": 1, "revision": "b"}, {"ID": 2}],
        }
        return {"result": pages.get(start, [])}

    stream = iter_list(
        Executor(FunctionTransport(handler)),
        Request("crm.item.list"),
        plan=_offset_plan(duplicate_policy=DuplicatePolicy.REPORT),
        identity=_identity(),
        policy=ExecutionPolicy(
            consistency=ConsistencyPolicy(duplicate_policy=DuplicatePolicy.REPORT),
        ),
    )

    assert await anext(stream) == {"ID": 1, "revision": "a"}
    assert await anext(stream) == {"ID": 1, "revision": "b"}
    await stream.aclose()

    assert stream.report.emitted_rows == PAGE_SIZE
    assert stream.report.unique_rows == 1
    assert [violation.code for violation in stream.report.violations] == ["duplicate_identity"]


@pytest.mark.asyncio
async def test_large_exact_identity_tracking_warns_once_and_continues() -> None:
    distinct = 100_001
    rows = [{"ID": index} for index in range(distinct)]
    rows.append({"ID": 0})
    transport = FunctionTransport(lambda _request: {"result": rows})
    policy = ExecutionPolicy(
        max_buffered_rows=len(rows),
        consistency=ConsistencyPolicy(duplicate_policy=DuplicatePolicy.REPORT),
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=SingleResponsePlan(duplicate_policy=DuplicatePolicy.REPORT),
        identity=_identity(),
        policy=policy,
    )

    with pytest.warns(RuntimeWarning, match="exact duplicate/loss detection") as captured:
        result = await _collect(stream)

    matching = [warning for warning in captured if "exact duplicate/loss detection" in str(warning.message)]
    assert len(matching) == 1
    assert len(result) == len(rows)
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.unique_rows == distinct
    assert [violation.code for violation in stream.report.violations] == ["duplicate_identity"]


@pytest.mark.asyncio
async def test_buffer_budget_blocks_page_before_any_row_is_emitted() -> None:
    transport = FunctionTransport(lambda _request: {"result": [{"ID": 1}, {"ID": 2}]})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=SingleResponsePlan(),
        policy=ExecutionPolicy(max_buffered_rows=1),
    )

    with pytest.raises(BudgetExceededError, match="buffered"):
        await _collect(stream)

    assert stream.report.emitted_rows == 0
    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_early_close_is_idempotent_and_reports_cancelled_with_buffer_high_water() -> None:
    transport = FunctionTransport(lambda _request: {"result": [{"ID": 1}, {"ID": 2}]})
    stream = iter_list(Executor(transport), Request("crm.item.list"), plan=SingleResponsePlan())

    assert await anext(stream) == {"ID": 1}
    await stream.aclose()
    await stream.aclose()

    assert stream.report.state is TerminalState.CANCELLED
    assert stream.report.emitted_rows == 1
    assert stream.report.buffered_rows_high_water == PAGE_SIZE


@pytest.mark.asyncio
async def test_task_cancellation_propagates_to_transport_and_finalizes_report() -> None:
    transport = BlockingTransport()
    stream = iter_list(Executor(transport), Request("crm.item.list"), plan=SingleResponsePlan())
    task = asyncio.create_task(anext(stream))
    await transport.started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert transport.cancelled.is_set()
    assert stream.report.state is TerminalState.CANCELLED


@pytest.mark.asyncio
async def test_cancellation_after_decoded_response_cannot_rollback_logical_page() -> None:
    class CancelAfterResponseTransport:
        def __init__(self) -> None:
            self.context: ExecutionContext | None = None
            self.locked = asyncio.Event()

        async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
            del request, attempt_timeout, max_response_bytes
            assert self.context is not None
            await self.context._lock.acquire()  # noqa: SLF001 - deterministic commit-race regression
            self.locked.set()
            body = json.dumps({"result": [{"ID": 1}]}).encode()
            return WireResponse(200, (("content-type", "application/json"),), body)

    transport = CancelAfterResponseTransport()
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=SingleResponsePlan(),
        identity=_identity(),
    )
    transport.context = stream._context  # noqa: SLF001 - deterministic commit-race regression
    task = asyncio.create_task(anext(stream))
    await transport.locked.wait()
    for _ in range(10):
        await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    assert transport.context is not None
    transport.context._lock.release()  # noqa: SLF001 - deterministic commit-race regression

    with pytest.raises(asyncio.CancelledError) as captured:
        await task

    assert captured.value.__dict__["report"] is stream.report
    assert stream.report.logical_pages == 1
    assert stream.report.state is TerminalState.CANCELLED


@pytest.mark.asyncio
async def test_cancellation_during_failed_finalization_preserves_failure_report() -> None:
    class MalformedAfterLockTransport:
        def __init__(self) -> None:
            self.context: ExecutionContext | None = None
            self.locked = asyncio.Event()

        async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
            del request, attempt_timeout, max_response_bytes
            assert self.context is not None
            await self.context._lock.acquire()  # noqa: SLF001 - deterministic finalize-race regression
            self.locked.set()
            return WireResponse(200, (("content-type", "application/json"),), b"{")

    transport = MalformedAfterLockTransport()
    stream = iter_list(Executor(transport), Request("crm.item.list"), plan=SingleResponsePlan())
    transport.context = stream._context  # noqa: SLF001 - deterministic finalize-race regression
    primary: list[ProtocolError] = []
    post_failure_executed = False

    async def observe_replayed_cancellation() -> None:
        nonlocal post_failure_executed
        try:
            await anext(stream)
        except ProtocolError as error:
            primary.append(error)
        await asyncio.sleep(0)
        post_failure_executed = True

    task = asyncio.create_task(observe_replayed_cancellation())
    await transport.locked.wait()
    for _ in range(10):
        await asyncio.sleep(0)
    task.cancel()
    assert transport.context is not None
    transport.context._lock.release()  # noqa: SLF001 - deterministic finalize-race regression

    with pytest.raises(asyncio.CancelledError):
        await task

    assert primary[0].__dict__["report"] is stream.report
    assert post_failure_executed is False
    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_non_traversal_snapshot_requirement_is_unverified_and_incomplete() -> None:
    transport = FunctionTransport(lambda _request: {"result": []})
    policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(snapshot_requirement=SnapshotRequirement.FROZEN_MANIFEST),
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list"),
        plan=SingleResponsePlan(),
        policy=policy,
    )

    assert await _collect(stream) == []
    assert stream.report.assurance is CompletionAssurance.CALLER_ASSERTED
    assert stream.report.snapshot is SnapshotState.UNVERIFIED
    assert stream.report.state is TerminalState.INCOMPLETE
    assert not stream.report.completed
    assert [violation.code for violation in stream.report.violations] == ["snapshot_unverified"]


@pytest.mark.asyncio
async def test_case_insensitive_control_conflict_fails_before_network_io() -> None:
    transport = FunctionTransport(lambda _request: {"result": []})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", {"START": 99}),
        plan=_offset_plan(),
    )

    with pytest.raises(CapabilityError, match="conflict"):
        await _collect(stream)

    assert transport.requests == []
