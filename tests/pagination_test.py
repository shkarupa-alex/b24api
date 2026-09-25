"""W5 tests for lazy traversal lifecycle and sequential pagination proofs."""

from __future__ import annotations
import asyncio
import json
import re
from typing import TYPE_CHECKING

import pytest

from b24api.contracts.policy import (
    CompletionAssurance,
    ConfirmationPolicy,
    ConsistencyPolicy,
    DuplicatePolicy,
    ExecutionPolicy,
    IdentityCoercion,
    IdentityRequirement,
    KernelState,
    OrderSemantics,
    SnapshotRequirement,
    SnapshotState,
    TotalSemantics,
)
from b24api.contracts.report import PageDispatch, PageOutcome, PageRejectionCode
from b24api.contracts.request import IdentitySpec, ParameterPath, Request, ResultSelector, RouteKind
from b24api.contracts.traversal import KeysetSpec
from b24api.errors import (
    ApiResponseError,
    BudgetExceededError,
    CapabilityError,
    IncompleteTraversalError,
    PaginationError,
    ProtocolError,
)
from b24api.execution import Executor, RateCoordinator, WireResponse, WorkClass
from b24api.traversal import iter_list
from b24api.traversal.keyset_step import (
    keyset_page_request,
    keyset_page_terminal,
    next_keyset_cursor,
    sequential_keyset_plan,
    validate_keyset_continuation,
)
from b24api.traversal.plans import (
    CountedOffsetPlan,
    CursorTerminalRule,
    ItemCursorPlan,
    KeysetPlan,
    KeysetTerminalRule,
    ListPlan,
    OffsetContinuation,
    OffsetSequentialPlan,
    OffsetTerminalRule,
    SingleResponsePlan,
)
from tests.ledger_hold import LedgerHold
from tests.scripting import Blocker, ResponderTransport, attached_report

if TYPE_CHECKING:
    from b24api.contracts.json import JsonValue

PAGE_SIZE = 2
THREE_ROWS = 3


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


@pytest.mark.parametrize(
    ("direction", "wire_direction", "operator"),
    [("ascending", "ASC", ">ID"), ("descending", "DESC", "<ID")],
)
def test_shared_keyset_page_step_preserves_sequential_controls(
    direction: str,
    wire_direction: str,
    operator: str,
) -> None:
    keyset = KeysetSpec(direction=direction, limit_path=ParameterPath(("limit",)))  # type: ignore[arg-type]
    plan = sequential_keyset_plan(keyset, PAGE_SIZE)
    original = Request("item.list", {"filter": {"ACTIVE": "Y"}}, route=RouteKind.BARE)

    request = keyset_page_request(original, plan=plan, identity=_identity(), cursor=7)

    assert original.copy_parameters() == {"filter": {"ACTIVE": "Y"}}
    assert request.copy_parameters() == {
        "filter": {"ACTIVE": "Y", operator: 7},
        "order": {"ID": wire_direction},
        "start": -1,
        "limit": PAGE_SIZE,
    }
    validate_keyset_continuation(plan, 7, [8] if direction == "ascending" else [6])
    assert next_keyset_cursor(7, [8] if direction == "ascending" else [6]) == (8 if direction == "ascending" else 6)
    assert keyset_page_terminal(plan, 1) is None
    assert keyset_page_terminal(plan, 0) == "empty keyset confirmation"


async def _collect(stream: object) -> list[JsonValue]:
    return [item async for item in stream]  # type: ignore[attr-defined]


async def _assert_incomplete_pagination(stream: object, pattern: str) -> IncompleteTraversalError:
    with pytest.raises(IncompleteTraversalError) as captured:
        await _collect(stream)
    cause = captured.value.__cause__
    assert isinstance(cause, PaginationError)
    assert re.search(pattern, str(cause))
    assert captured.value.report is stream.report  # type: ignore[attr-defined]
    return captured.value


@pytest.mark.asyncio
async def test_single_stream_is_lazy_and_reports_scalar_completion() -> None:
    transport = ResponderTransport(lambda _request: {"result": {"ID": 7}})
    stream = iter_list(Executor(transport), Request("crm.item.get", route=RouteKind.BARE), plan=SingleResponsePlan())

    assert transport.requests == []
    assert await _collect(stream) == [{"ID": 7}]
    assert len(transport.requests) == 1
    assert stream.report.state is KernelState.COMPLETED
    assert stream.report.logical_pages == 1
    assert stream.report.emitted_rows == 1


@pytest.mark.asyncio
async def test_async_context_entry_reads_nothing_until_the_first_pull() -> None:
    transport = ResponderTransport(lambda _request: {"result": {"ID": 7}})
    stream = iter_list(Executor(transport), Request("crm.item.get", route=RouteKind.BARE), plan=SingleResponsePlan())

    async with stream as entered:
        assert transport.requests == []
        assert entered.report.state is KernelState.NOT_STARTED
        assert await _collect(entered) == [{"ID": 7}]

    assert stream.report.state is KernelState.COMPLETED
    assert stream.report.emitted_rows == 1


@pytest.mark.asyncio
async def test_single_rejects_continuation_and_records_failure() -> None:
    transport = ResponderTransport(lambda _request: {"result": [], "next": 2})
    stream = iter_list(Executor(transport), Request("crm.item.list", route=RouteKind.BARE), plan=SingleResponsePlan())

    with pytest.raises(CapabilityError, match="continuation") as captured:
        await _collect(stream)

    assert stream.report.state is KernelState.FAILED
    assert stream.report.logical_pages == 1
    assert attached_report(captured.value) is stream.report


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

    transport = ResponderTransport(handler)
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", original, route=RouteKind.BARE),
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
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 1}]})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=_offset_plan(),
        identity=_identity(),
    )

    await _assert_incomplete_pagination(stream, "repeated page")

    assert len(transport.requests) == PAGE_SIZE
    assert stream.report.state is KernelState.INCOMPLETE


@pytest.mark.asyncio
async def test_required_identity_and_oversized_pages_are_rejected() -> None:
    no_identity_transport = ResponderTransport(lambda _request: {"result": []})
    required = OffsetSequentialPlan(identity_requirement=IdentityRequirement.REQUIRED)
    stream = iter_list(Executor(no_identity_transport), Request("crm.item.list", route=RouteKind.BARE), plan=required)
    with pytest.raises(CapabilityError, match="IdentitySpec"):
        await _collect(stream)
    assert no_identity_transport.requests == []

    oversized_transport = ResponderTransport(
        lambda _request: {"result": [{"ID": 1}, {"ID": 2}, {"ID": 3}]},
    )
    oversized = iter_list(
        Executor(oversized_transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=_offset_plan(),
        identity=_identity(),
    )
    await _assert_incomplete_pagination(oversized, "page cap")
    assert len(oversized_transport.requests) == 1


@pytest.mark.asyncio
async def test_offset_exact_total_must_be_present_stable_and_not_overshot() -> None:
    responses = [
        {"result": [{"ID": 1}], "total": 2},
        {"result": [{"ID": 2}], "total": 3},
    ]
    transport = ResponderTransport(lambda _request: responses.pop(0))
    plan = OffsetSequentialPlan(
        continuation=OffsetContinuation.OBSERVED_COUNT,
        terminal=frozenset({OffsetTerminalRule.QUALIFIED_TOTAL}),
        total_semantics=TotalSemantics.FILTERED_EXACT,
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=plan,
        identity=_identity(),
    )

    await _assert_incomplete_pagination(stream, "total drifted")


@pytest.mark.asyncio
async def test_empty_page_cannot_override_an_unreached_exact_total() -> None:
    responses = [
        {"result": [{"ID": 1}], "total": 2},
        {"result": [], "total": 2},
    ]
    transport = ResponderTransport(lambda _request: responses.pop(0))
    plan = OffsetSequentialPlan(
        continuation=OffsetContinuation.OBSERVED_COUNT,
        terminal=frozenset({OffsetTerminalRule.EMPTY_PAGE, OffsetTerminalRule.QUALIFIED_TOTAL}),
        total_semantics=TotalSemantics.FILTERED_EXACT,
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=plan,
        identity=_identity(),
    )

    await _assert_incomplete_pagination(stream, "before its exact total")
    assert stream.report.page_trace[-1].outcome is PageOutcome.REJECTED
    assert stream.report.page_trace[-1].rejection_code is PageRejectionCode.TOTAL_DRIFT
    assert stream.report.page_trace[-1].rows_admitted == 0
    assert stream.report.terminal_reason == "PaginationError"


@pytest.mark.asyncio
async def test_page_budget_refuses_continuation_before_network_io() -> None:
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 1}]})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=_offset_plan(),
        identity=_identity(),
        policy=ExecutionPolicy(max_pages=1),
    )

    with pytest.raises(BudgetExceededError, match="page budget"):
        await _collect(stream)

    assert [_integer_parameter(request, "start") for request in transport.requests] == [0]
    assert stream.report.state is KernelState.FAILED
    assert len(stream.report.page_trace) == 1
    assert stream.report.page_trace[0].outcome is PageOutcome.COMMITTED


@pytest.mark.asyncio
async def test_selector_shape_failure_is_typed_and_reported() -> None:
    transport = ResponderTransport(lambda _request: {"result": {"items": {"ID": 1}}})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=SingleResponsePlan(selector=None),
        selector=ResultSelector(("items",)),
    )

    with pytest.raises(CapabilityError, match="must be a sequence") as captured:
        await _collect(stream)

    assert attached_report(captured.value) is stream.report


@pytest.mark.asyncio
async def test_counted_offset_requires_one_stable_non_negative_exact_total() -> None:
    def handler(request: Request) -> dict[str, object]:
        start = _integer_parameter(request, "start")
        return (
            {"result": [{"ID": 1}, {"ID": 2}], "total": 3, "next": 2}
            if start == 0
            else {"result": [{"ID": 3}], "total": 3}
        )

    transport = ResponderTransport(handler)
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=CountedOffsetPlan(),
        identity=_identity(),
    )

    assert await _collect(stream) == [{"ID": 1}, {"ID": 2}, {"ID": 3}]
    assert stream.report.state is KernelState.COMPLETED
    assert stream.report.terminal_reason == "qualified total reached"


@pytest.mark.asyncio
async def test_counted_offset_detects_repeated_items_when_continuation_metadata_changes() -> None:
    responses = [
        {"result": [{"ID": 1}, {"ID": 2}], "total": 4, "next": 2},
        {"result": [{"ID": 1}, {"ID": 2}], "total": 4},
    ]
    transport = ResponderTransport(lambda _request: responses.pop(0))
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=CountedOffsetPlan(),
    )

    await _assert_incomplete_pagination(stream, "repeated page")

    assert len(transport.requests) == PAGE_SIZE
    assert stream.report.state is KernelState.INCOMPLETE


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
    transport = ResponderTransport(
        lambda _request: {"result": [{"ID": 2}, {"ID": 1}], "total": PAGE_SIZE},
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=plan,
        identity=_identity(),
    )

    await _assert_incomplete_pagination(stream, "strictly ascending")

    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_consistency_policy_requires_identity_before_io() -> None:
    transport = ResponderTransport(lambda _request: {"result": []})
    policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(identity_requirement=IdentityRequirement.REQUIRED),
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=SingleResponsePlan(),
        policy=policy,
    )

    with pytest.raises(CapabilityError, match="IdentitySpec"):
        await _collect(stream)

    assert transport.requests == []


@pytest.mark.asyncio
async def test_consistency_policy_enforces_duplicates_order_total_and_confirmation() -> None:
    duplicate_transport = ResponderTransport(
        lambda _request: {"result": [{"ID": 1}, {"ID": 1}]},
    )
    duplicate_stream = iter_list(
        Executor(duplicate_transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=SingleResponsePlan(duplicate_policy=DuplicatePolicy.ALLOW_DECLARED_MULTISET),
        identity=_identity(),
    )
    await _assert_incomplete_pagination(duplicate_stream, "duplicate identity")

    order_transport = ResponderTransport(
        lambda _request: {"result": [{"ID": 2}, {"ID": 1}]},
    )
    order_policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(order_semantics=OrderSemantics.ASCENDING),
    )
    order_stream = iter_list(
        Executor(order_transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=SingleResponsePlan(),
        identity=_identity(),
        policy=order_policy,
    )
    await _assert_incomplete_pagination(order_stream, "strictly ascending")

    total_transport = ResponderTransport(
        lambda _request: {"result": [{"ID": 1}, {"ID": 2}], "total": 1},
    )
    total_policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(total_semantics=TotalSemantics.FILTERED_EXACT),
    )
    total_stream = iter_list(
        Executor(total_transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=SingleResponsePlan(),
        policy=total_policy,
    )
    await _assert_incomplete_pagination(total_stream, "exact total")

    confirmation_transport = ResponderTransport(lambda _request: {"result": []})
    confirmation_policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(confirmation_policy=ConfirmationPolicy.EMPTY_AFTER_BOUNDARY),
    )
    confirmation_stream = iter_list(
        Executor(confirmation_transport),
        Request("crm.item.list", route=RouteKind.BARE),
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

    transport = ResponderTransport(handler)
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
        Request("crm.item.list", route=RouteKind.BARE),
        plan=plan,
        identity=_identity(),
        policy=policy,
    )

    assert await _collect(stream) == [{"ID": 1}, {"ID": 2}]
    assert [_integer_parameter(request, "start") for request in transport.requests] == [0, PAGE_SIZE]
    assert stream.report.terminal_reason == "empty page confirmed terminal"


@pytest.mark.asyncio
async def test_advisory_total_mismatch_is_reported_without_blocking_completion() -> None:
    transport = ResponderTransport(
        lambda _request: {"result": [{"ID": 1}, {"ID": 2}], "total": 99},
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=SingleResponsePlan(reject_positive_total_over_result=False),
        policy=ExecutionPolicy(
            consistency=ConsistencyPolicy(total_semantics=TotalSemantics.ADVISORY),
        ),
    )

    assert await _collect(stream) == [{"ID": 1}, {"ID": 2}]
    assert stream.report.state is KernelState.COMPLETED
    assert [violation.code for violation in stream.report.violations] == ["advisory_total_mismatch"]


@pytest.mark.asyncio
async def test_conflicting_policy_and_plan_semantics_refuse_before_io() -> None:
    transport = ResponderTransport(lambda _request: {"result": []})
    plan = CountedOffsetPlan(order_semantics=OrderSemantics.ASCENDING)
    policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(
            total_semantics=TotalSemantics.GLOBAL,
            order_semantics=OrderSemantics.DESCENDING,
        ),
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
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
        (({"result": [], "total": -1},), "before its exact total"),
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
    transport = ResponderTransport(lambda _request: pending.pop(0))
    stream = iter_list(Executor(transport), Request("crm.item.list", route=RouteKind.BARE), plan=CountedOffsetPlan())

    await _assert_incomplete_pagination(stream, message)

    assert stream.report.state is KernelState.INCOMPLETE
    expected_code = PageRejectionCode.TOTAL_DRIFT if message == "drifted" else PageRejectionCode.RANGE_CONTRADICTION
    assert stream.report.page_trace[-1].rejection_code is expected_code


@pytest.mark.asyncio
async def test_page_trace_limit_bounds_live_driver_evidence() -> None:
    responses = [
        {"result": [{"ID": 1}], "next": 1},
        {"result": [{"ID": 2}], "next": 2},
        {"result": [{"ID": 3}], "next": 3},
        {"result": []},
    ]
    stream = iter_list(
        Executor(ResponderTransport(lambda _request: responses.pop(0))),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=OffsetSequentialPlan(
            continuation=OffsetContinuation.SERVER_NEXT,
            terminal=frozenset({OffsetTerminalRule.EMPTY_PAGE}),
        ),
        identity=_identity(),
        policy=ExecutionPolicy(page_trace_limit=1),
    )

    assert len([item async for item in stream]) == THREE_ROWS
    assert len(stream._driver.page_trace) == 1  # noqa: SLF001
    assert stream._driver.page_trace_truncated  # noqa: SLF001
    assert len(stream.report.page_trace) == 1
    assert stream.report.page_trace_truncated


@pytest.mark.asyncio
async def test_direct_fetch_failure_records_unknown_scheduled_page() -> None:
    stream = iter_list(
        Executor(
            ResponderTransport(
                lambda _request: {"error": "ACCESS_DENIED", "error_description": "denied"},
            ),
        ),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=_offset_plan(),
        identity=_identity(),
    )

    with pytest.raises(ApiResponseError) as captured:
        await _collect(stream)

    assert attached_report(captured.value) is stream.report
    assert len(stream.report.page_trace) == 1
    record = stream.report.page_trace[0]
    assert record.outcome is PageOutcome.UNKNOWN
    assert record.dispatch is PageDispatch.DIRECT
    assert record.rejection_code is PageRejectionCode.COMMAND_FAILURE


@pytest.mark.asyncio
async def test_cancellation_while_waiting_for_dispatch_records_no_unknown_page() -> None:
    coordinator = RateCoordinator(max_concurrency=1)
    held = await coordinator.acquire(WorkClass.INTERACTIVE_DIRECT, methods=frozenset({"profile"}))
    stream = iter_list(
        Executor(ResponderTransport(lambda _request: {"result": []}), coordinator=coordinator),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=_offset_plan(),
        identity=_identity(),
    )
    pending = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)
    pending.cancel()

    with pytest.raises(asyncio.CancelledError):
        await pending

    assert stream.report.page_trace == ()
    await held.release()
    await coordinator.close()


@pytest.mark.asyncio
async def test_boundary_keyset_strategy_refuses_before_io() -> None:
    transport = ResponderTransport(lambda _request: {"result": []})
    boundary = KeysetPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
        terminal=KeysetTerminalRule.BOUNDARY_ID_SEEN,
    )

    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=boundary,
        identity=_identity(),
    )
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
    transport = ResponderTransport(lambda _request: {"result": []})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=plan,
        policy=policy,
    )

    with pytest.raises(CapabilityError, match=message):
        await anext(stream)
    assert transport.requests == []


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

    transport = ResponderTransport(handler)
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
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
    transport = ResponderTransport(lambda _request: responses.pop(0))
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=_keyset_plan(),
        identity=_identity(),
    )

    await _assert_incomplete_pagination(stream, r"advance|lower bound")


@pytest.mark.asyncio
async def test_item_cursor_advances_from_items_until_empty_confirmation() -> None:
    def handler(request: Request) -> dict[str, object]:
        cursor = _optional_integer_parameter(request, "LAST_ID")
        pages = {None: [{"ID": 1}, {"ID": 2}], 2: [{"ID": 3}], 3: []}
        return {"result": pages[cursor]}

    transport = ResponderTransport(handler)
    plan = ItemCursorPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
        cursor_item_path=("ID",),
        cursor_take="last",
        terminal=CursorTerminalRule.EMPTY_CONFIRMATION,
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
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
    transport = ResponderTransport(lambda _request: responses.pop(0))
    plan = ItemCursorPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        cursor_item_path=("cursor",),
        direction="asc",
        cursor_take="last",
        duplicate_policy=DuplicatePolicy.REPORT,
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
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
    assert stream.report.state is KernelState.COMPLETED
    assert stream.report.unique_rows == PAGE_SIZE
    assert [violation.code for violation in stream.report.violations] == ["duplicate_identity"]


@pytest.mark.asyncio
async def test_item_cursor_uses_independent_cursor_coercion() -> None:
    responses = [
        {"result": [{"uuid": "a", "cursor": 1}, {"uuid": "b", "cursor": 2}]},
        {"result": []},
    ]
    transport = ResponderTransport(lambda _request: responses.pop(0))
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
        Request("crm.item.list", route=RouteKind.BARE),
        plan=plan,
        identity=identity,
    )

    assert await _collect(stream) == [
        {"uuid": "a", "cursor": 1},
        {"uuid": "b", "cursor": 2},
    ]
    assert [request.copy_parameters().get("LAST_ID") for request in transport.requests] == [None, 2]
    assert stream.report.state is KernelState.COMPLETED


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
    transport = ResponderTransport(lambda _request: {"result": []})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=plan,
        policy=policy,
    )

    with pytest.raises(CapabilityError, match="composite identity"):
        await anext(stream)
    assert transport.requests == []


@pytest.mark.asyncio
async def test_item_cursor_rejects_wrong_order_within_first_page() -> None:
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 2}, {"ID": 1}]})
    plan = ItemCursorPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
        cursor_item_path=("ID",),
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=plan,
        identity=_identity(),
    )

    await _assert_incomplete_pagination(stream, "strictly ascending")


@pytest.mark.asyncio
async def test_item_cursor_uses_internal_monotonic_tracking() -> None:
    def handler(request: Request) -> object:
        if "LAST_ID" in request.parameters:
            return {"result": []}
        return {"result": [{"ID": 1, "cursor": 1}]}

    transport = ResponderTransport(handler)
    plan = ItemCursorPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
        cursor_item_path=("cursor",),
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=plan,
        identity=_identity(),
    )

    assert await _collect(stream) == [{"ID": 1, "cursor": 1}]
    assert len(transport.requests) == PAGE_SIZE


@pytest.mark.asyncio
async def test_latent_keyset_filter_collision_refuses_before_io() -> None:
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 1}]})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", {"filter": {">ID": 99}}, route=RouteKind.BARE),
        plan=_keyset_plan(),
        identity=_identity(),
    )

    with pytest.raises(CapabilityError, match="conflict"):
        await anext(stream)
    assert transport.requests == []


@pytest.mark.asyncio
async def test_latent_item_cursor_collision_refuses_before_io() -> None:
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 1}]})
    plan = ItemCursorPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
        cursor_item_path=("ID",),
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", {"LAST_ID": 99}, route=RouteKind.BARE),
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

    transport = ResponderTransport(handler)
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
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
        Executor(ResponderTransport(handler)),
        Request("crm.item.list", route=RouteKind.BARE),
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
async def test_large_exact_identity_tracking_uses_declared_finite_budget() -> None:
    distinct = 100_001
    rows = [{"ID": index} for index in range(distinct)]
    rows.insert(50_000, {"ID": 0})
    rows.append({"ID": distinct - 1})
    transport = ResponderTransport(lambda _request: {"result": rows})
    policy = ExecutionPolicy(
        max_buffered_rows=len(rows),
        max_identity_keys=distinct,
        consistency=ConsistencyPolicy(duplicate_policy=DuplicatePolicy.REPORT),
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=SingleResponsePlan(duplicate_policy=DuplicatePolicy.REPORT),
        identity=_identity(),
        policy=policy,
    )

    result = await _collect(stream)
    assert len(result) == len(rows)
    assert stream.report.state is KernelState.COMPLETED
    assert stream.report.unique_rows == distinct
    assert [violation.code for violation in stream.report.violations] == ["duplicate_identity"]
    assert "observed 2 duplicate identities" in stream.report.violations[0].message


@pytest.mark.asyncio
async def test_buffer_budget_blocks_page_before_any_row_is_emitted() -> None:
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 1}, {"ID": 2}]})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=SingleResponsePlan(),
        policy=ExecutionPolicy(max_buffered_rows=1),
    )

    with pytest.raises(BudgetExceededError, match="buffered"):
        await _collect(stream)

    assert stream.report.emitted_rows == 0
    assert stream.report.state is KernelState.FAILED


@pytest.mark.asyncio
async def test_early_close_is_idempotent_and_reports_cancelled_with_buffer_high_water() -> None:
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 1}, {"ID": 2}]})
    stream = iter_list(Executor(transport), Request("crm.item.list", route=RouteKind.BARE), plan=SingleResponsePlan())

    assert await anext(stream) == {"ID": 1}
    await stream.aclose()
    await stream.aclose()

    assert stream.report.state is KernelState.CANCELLED
    assert stream.report.emitted_rows == 1
    assert stream.report.buffered_rows_high_water == PAGE_SIZE


@pytest.mark.asyncio
async def test_task_cancellation_propagates_to_transport_and_finalizes_report() -> None:
    blocker = Blocker()
    transport = ResponderTransport(blocker)
    stream = iter_list(Executor(transport), Request("crm.item.list", route=RouteKind.BARE), plan=SingleResponsePlan())
    task = asyncio.create_task(anext(stream))
    await blocker.started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert blocker.cancelled.is_set()
    assert stream.report.state is KernelState.CANCELLED


@pytest.mark.asyncio
async def test_cancellation_after_decoded_response_cannot_rollback_logical_page() -> None:
    class CancelAfterResponseTransport:
        host = "fixture.invalid"

        def __init__(self) -> None:
            self.hold: LedgerHold | None = None

        async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
            del request, attempt_timeout, max_response_bytes
            assert self.hold is not None
            self.hold.acquire()
            body = json.dumps({"result": [{"ID": 1}]}).encode()
            return WireResponse(200, (("content-type", "application/json"),), body)

    transport = CancelAfterResponseTransport()
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=SingleResponsePlan(),
        identity=_identity(),
    )
    hold = transport.hold = LedgerHold(stream._context)  # noqa: SLF001 - deterministic commit-race regression
    task = asyncio.create_task(anext(stream))
    await hold.blocked.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    hold.release()

    with pytest.raises(asyncio.CancelledError) as captured:
        await task

    assert attached_report(captured.value) is stream.report
    assert stream.report.logical_pages == 1
    assert stream.report.state is KernelState.CANCELLED


@pytest.mark.asyncio
async def test_cancellation_during_failed_finalization_preserves_failure_report() -> None:
    class MalformedAfterHoldTransport:
        host = "fixture.invalid"

        def __init__(self) -> None:
            self.hold: LedgerHold | None = None

        async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
            del request, attempt_timeout, max_response_bytes
            assert self.hold is not None
            self.hold.acquire()
            return WireResponse(200, (("content-type", "application/json"),), b"{")

    transport = MalformedAfterHoldTransport()
    stream = iter_list(Executor(transport), Request("crm.item.list", route=RouteKind.BARE), plan=SingleResponsePlan())
    hold = transport.hold = LedgerHold(stream._context)  # noqa: SLF001 - deterministic finalize-race regression
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
    await hold.blocked.wait()
    task.cancel()
    hold.release()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert attached_report(primary[0]) is stream.report
    assert post_failure_executed is False
    assert stream.report.state is KernelState.FAILED


@pytest.mark.asyncio
async def test_non_traversal_snapshot_requirement_is_unverified_and_incomplete() -> None:
    transport = ResponderTransport(lambda _request: {"result": []})
    policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(snapshot_requirement=SnapshotRequirement.FROZEN_MANIFEST),
    )
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", route=RouteKind.BARE),
        plan=SingleResponsePlan(),
        policy=policy,
    )

    assert await _collect(stream) == []
    assert stream.report.assurance is CompletionAssurance.CALLER_ASSERTED
    assert stream.report.snapshot is SnapshotState.UNVERIFIED
    assert stream.report.state is KernelState.INCOMPLETE
    assert not stream.report.completed
    assert [violation.code for violation in stream.report.violations] == ["snapshot_unverified"]


@pytest.mark.asyncio
async def test_case_insensitive_control_ambiguity_fails_before_network_io() -> None:
    transport = ResponderTransport(lambda _request: {"result": []})
    stream = iter_list(
        Executor(transport),
        Request("crm.item.list", {"start": 99, "START": 99}, route=RouteKind.BARE),
        plan=_offset_plan(),
    )

    with pytest.raises(CapabilityError, match="ambiguous"):
        await _collect(stream)

    assert transport.requests == []
