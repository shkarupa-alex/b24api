"""Counted references on the shared batch queue (#20) and the empty counted head (#11).

Acceptance A11, A11-STRICT, A20, A20-COST, A20-N, A20-VARIABLE, A20-PROVENANCE, A20-SIBLING,
A20-LOAD, AFLAT and AX: one counted head transaction serves the flat stream and the reference
queue; a reference binding's head and dense tail ride the shared dispatcher and row pool.
"""

from __future__ import annotations
import asyncio
from collections import Counter
from typing import TYPE_CHECKING, Any

import pytest

from b24api import (
    BatchDispatch,
    Binding,
    Bitrix24,
    CountedTraversal,
    ExecutionPolicy,
    OffsetContinuation,
    OffsetSpec,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    RetryPolicy,
    Settings,
    TerminalState,
    TotalTermination,
)
from b24api.contracts import (
    ConsistencyPolicy,
    DeliveryOrder,
    PageDispatch,
    PageOutcome,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ReferenceOutcomeUnknown,
    TraversalAssurance,
)
from b24api.contracts.completion import BindingClosure
from b24api.contracts.policy import ConfirmationPolicy, SnapshotRequirement
from b24api.errors import BudgetExceededError
from b24api.execution import ExecutionContext
from tests.scripting import BatchPortal, Envelope, Query

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from b24api.contracts import OperationReport, OperationStream, ReferenceOutcome

WIDTH = 50
ROWS = 1001
_ABSENT = object()
_COUNTED = OffsetSpec(total_termination=TotalTermination.EXACT_QUALIFIED)


def _client(portal: BatchPortal) -> Bitrix24:
    return Bitrix24(Settings(webhook_url="https://fixture.invalid/rest/1/token/"), transport=portal)


def _request() -> Request:
    return Request.bare("crm.item.list", {}, ReplaySafety.SAFE)


def _bindings(*parents: int) -> list[Binding[int]]:
    return [Binding("parent", (ParameterUpdate(ParameterPath(("PARENT",)), parent),), parent) for parent in parents]


def _report(stream: OperationStream[Any]) -> OperationReport:
    report = stream.report
    assert report is not None
    return report


def _dense(rows: int, width: int = WIDTH) -> Callable[[str, Query], Envelope]:
    """Serve ``rows`` rows per parent in ``width`` pages with an exact total and a server next."""

    def answer(method: str, query: Query) -> Envelope:
        assert method == "crm.item.list"
        parent, offset = int(query["PARENT"]), int(query.get("start", "0"))
        page = [{"ID": parent * 100_000 + row} for row in range(offset, min(offset + width, rows))]
        envelope: Envelope = {"result": page, "total": rows}
        if offset + width < rows:
            envelope["next"] = offset + width
        return envelope

    return answer


async def _outcomes(
    portal: BatchPortal,
    parents: tuple[int, ...],
    *,
    traversal: CountedTraversal | None = None,
    dispatch: BatchDispatch | None = None,
    policy: ExecutionPolicy | None = None,
) -> tuple[list[ReferenceOutcome[int]], OperationReport]:
    async with _client(portal) as api:
        stream = api.iter_reference_outcomes(
            _request(),
            _bindings(*parents),
            traversal=traversal or CountedTraversal(page_size=WIDTH),
            dispatch=dispatch or BatchDispatch(),
            policy=policy,
        )
        async with stream:
            events = [event async for event in stream]
    return events, _report(stream)


def _offsets(portal: BatchPortal) -> Counter[tuple[int, int]]:
    return Counter((int(query["PARENT"]), int(query.get("start", "0"))) for query in portal.queries)


def _completes(events: list[ReferenceOutcome[int]]) -> dict[int, ReferenceComplete[int]]:
    return {event.correlation: event for event in events if isinstance(event, ReferenceComplete)}


def _items(events: list[ReferenceOutcome[int]], parent: int) -> list[object]:
    return [
        event.item["ID"] if isinstance(event.item, dict) else None
        for event in events
        if isinstance(event, ReferenceItem) and event.correlation == parent
    ]


# --- A20 / A20-COST -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a20_two_counted_parents_travel_in_two_http_requests() -> None:
    portal = BatchPortal(_dense(ROWS))
    events, report = await _outcomes(portal, (1, 2))

    assert portal.waves == [2, 40]
    assert report.physical_requests == len(portal.waves)
    assert report.logical_pages == 42  # noqa: PLR2004 - 2 heads plus 2 x 20 tail pages
    assert sum(isinstance(event, ReferenceItem) for event in events) == 2 * ROWS
    completes = _completes(events)
    assert sorted(completes) == [1, 2]
    assert all(complete.exhausted and complete.row_count == ROWS for complete in completes.values())
    assert {complete.closure for complete in completes.values()} == {BindingClosure.QUALIFIED_TOTAL}
    assert report.state is TerminalState.COMPLETED
    for parent in (1, 2):
        assert _items(events, parent) == [parent * 100_000 + row for row in range(ROWS)]


@pytest.mark.asyncio
async def test_a20_cost_keeps_the_sequential_logical_request_multiset_without_count_probes() -> None:
    portal = BatchPortal(_dense(ROWS))
    await _outcomes(portal, (1, 2))

    # The sequential counted baseline asks each offset of each parent exactly once: packing removes
    # HTTP round trips, never a logical request, and adds no COUNT probe.
    baseline = Counter((parent, offset) for parent in (1, 2) for offset in range(0, ROWS, WIDTH))
    assert _offsets(portal) == baseline
    assert {method for wave in portal.methods for method in wave} == {"crm.item.list"}


# --- A11 / A11-STRICT ---------------------------------------------------------------------------


def _head_cases(empty: Envelope) -> Callable[[str, Query], Envelope]:
    def answer(method: str, query: Query) -> Envelope:
        del method
        assert query.get("start", "0") == "0"
        if query["PARENT"] == "1":
            return {"result": [{"ID": 1}], "total": 1}
        return dict(empty)

    return answer


def _empty(total: object) -> Envelope:
    envelope: Envelope = {"result": []}
    if total is not _ABSENT:
        envelope["total"] = total
    return envelope


@pytest.mark.asyncio
@pytest.mark.parametrize("total", [_ABSENT, None, -1], ids=["missing", "null", "unknown-sentinel"])
@pytest.mark.parametrize("tolerant", [True, False], ids=["outcomes", "fail-fast"])
async def test_a11_empty_counted_head_closes_as_source_empty_in_one_http_request(
    total: object,
    tolerant: bool,  # noqa: FBT001 - parametrized facade switch
) -> None:
    portal = BatchPortal(_head_cases(_empty(total)))
    async with _client(portal) as api:
        traversal = CountedTraversal(page_size=WIDTH)
        stream = (
            api.iter_reference_outcomes(_request(), _bindings(1, 2), traversal=traversal)
            if tolerant
            else api.iter_references(_request(), _bindings(1, 2), traversal=traversal)
        )
        async with stream:
            events = [event async for event in stream]

    assert portal.waves == [2]
    assert sum(isinstance(event, ReferenceItem) for event in events) == 1
    completes = {event.correlation: event for event in events if isinstance(event, ReferenceComplete)}
    assert completes[1].closure is BindingClosure.QUALIFIED_TOTAL
    assert (completes[2].row_count, completes[2].exhausted, completes[2].closure) == (
        0,
        True,
        BindingClosure.SOURCE_EMPTY,
    )
    report = _report(stream)
    assert report.state is TerminalState.COMPLETED
    assert report.exhausted
    # The counted neighbour does not lend its count to the empty source's operation assurance.
    assert report.assurance not in {TraversalAssurance.COUNT_MATCHED, TraversalAssurance.IDENTITY_AND_COUNT_MATCHED}
    empty_head = [record for record in report.page_trace if record.reference_index == 1]
    assert [(record.offset, record.outcome, record.reported_total) for record in empty_head] == [
        (0, PageOutcome.COMMITTED, None),
    ]


@pytest.mark.asyncio
async def test_a11_explicit_zero_total_keeps_the_qualified_total_closure() -> None:
    portal = BatchPortal(_head_cases(_empty(0)))
    events, report = await _outcomes(portal, (1, 2))

    completes = _completes(events)
    assert (completes[2].row_count, completes[2].closure) == (0, BindingClosure.QUALIFIED_TOTAL)
    assert report.state is TerminalState.COMPLETED
    assert portal.waves == [2]


_FIXED_STEP = OffsetSpec(
    continuation=OffsetContinuation.FIXED_STEP,
    step=WIDTH,
    total_termination=TotalTermination.EXACT_QUALIFIED,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("empty", "offset", "policy"),
    [
        pytest.param({"result": [{"ID": 7}]}, _COUNTED, None, id="rows-without-total"),
        pytest.param({"result": [{"ID": 7}], "total": -1}, _COUNTED, None, id="rows-unknown-total"),
        pytest.param({"result": [], "next": 0}, _COUNTED, None, id="empty-next-zero"),
        pytest.param({"result": [], "next": WIDTH}, _COUNTED, None, id="empty-positive-next"),
        pytest.param({"result": [], "total": 3}, _COUNTED, None, id="positive-total-empty"),
        pytest.param({"result": []}, _FIXED_STEP, None, id="fixed-step"),
        pytest.param(
            {"result": []},
            _COUNTED,
            ExecutionPolicy(consistency=ConsistencyPolicy(confirmation_policy=ConfirmationPolicy.QUALIFIED_TOTAL)),
            id="qualified-total",
        ),
    ],
)
async def test_a11_strict_heads_outside_the_empty_source_contract_fail_typed(
    empty: Envelope,
    offset: OffsetSpec,
    policy: ExecutionPolicy | None,
) -> None:
    portal = BatchPortal(_head_cases(empty))
    events, report = await _outcomes(
        portal,
        (1, 2),
        traversal=CountedTraversal(page_size=WIDTH, offset=offset),
        policy=policy,
    )

    completes = _completes(events)
    assert sorted(completes) == [1]
    failures = [event for event in events if isinstance(event, ReferenceFailure | ReferenceOutcomeUnknown)]
    assert [failure.correlation for failure in failures] == [2]
    assert report.state is TerminalState.COMPLETED_WITH_FAILURES
    assert not report.exhausted
    # No restart: the failing head is the binding's only offset-zero request.
    assert _offsets(portal)[2, 0] == 1


@pytest.mark.asyncio
async def test_a11_strict_a_later_page_without_total_fails_the_binding() -> None:
    def answer(method: str, query: Query) -> Envelope:
        envelope = _dense(2 * WIDTH)(method, query)
        if query.get("start") == str(WIDTH):
            del envelope["total"]
        return envelope

    portal = BatchPortal(answer)
    events, report = await _outcomes(portal, (1,))

    failures = [event for event in events if isinstance(event, ReferenceFailure)]
    assert len(failures) == 1
    assert failures[0].partial_rows == WIDTH
    assert report.state is TerminalState.COMPLETED_WITH_FAILURES


@pytest.mark.asyncio
async def test_a11_strict_an_empty_source_still_requires_a_verified_snapshot() -> None:
    policy = ExecutionPolicy(consistency=ConsistencyPolicy(snapshot_requirement=SnapshotRequirement.FROZEN_MANIFEST))
    portal = BatchPortal(_head_cases(_empty(_ABSENT)))
    _events, report = await _outcomes(portal, (2,), policy=policy)

    assert report.state is TerminalState.INCOMPLETE
    assert not report.exhausted


# --- A20-VARIABLE / A20-PROVENANCE ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a20_variable_a_short_head_continues_sequentially_then_fails_without_restart() -> None:
    def answer(method: str, query: Query) -> Envelope:
        del method
        offset = int(query.get("start", "0"))
        if offset == 0:
            return {"result": [{"ID": 1}], "total": 4, "next": 1}
        return {"result": [{"ID": 2}], "total": 5, "next": 2}  # drifted total

    portal = BatchPortal(answer)
    events, _ = await _outcomes(portal, (1,), traversal=CountedTraversal(page_size=2))

    assert _items(events, 1) == [1]
    assert [failure.partial_rows for failure in events if isinstance(failure, ReferenceFailure)] == [1]
    # The head is fetched, adapted and committed once; the next offset is the head's row count.
    assert [int(query.get("start", "0")) for query in portal.queries] == [0, 1]


@pytest.mark.asyncio
async def test_a20_provenance_a_sent_head_is_unknown_at_the_deadline_and_a_queued_one_is_not_recorded() -> None:
    """A head whose batch was sent may have executed; a head still queued at the deadline was never sent."""

    async def answer(method: str, query: Query) -> Envelope:
        del method, query
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    portal = BatchPortal(answer)
    async with _client(portal) as api:
        stream = api.iter_reference_outcomes(
            _request(),
            _bindings(1, 2),
            traversal=CountedTraversal(page_size=WIDTH),
            dispatch=BatchDispatch(batch_size=1, concurrency=1),
            policy=ExecutionPolicy(max_elapsed=0.2),
        )
        with pytest.raises(BudgetExceededError):
            async with asyncio.timeout(5), stream:
                _ = [event async for event in stream]

    assert portal.waves == [1]
    assert [query["PARENT"] for query in portal.queries] == ["1"]
    trace = _report(stream).page_trace
    assert [(record.reference_index, record.offset, record.dispatch, record.outcome) for record in trace] == [
        (0, 0, PageDispatch.BATCH, PageOutcome.UNKNOWN)
    ]


@pytest.mark.asyncio
async def test_a20_provenance_heads_are_batch_pages_with_their_command_index() -> None:
    def answer(method: str, query: Query) -> Envelope:
        if query["PARENT"] == "2":
            return {"error": "INTERNAL_SERVER_ERROR", "error_description": "scripted"}
        return _dense(10)(method, query)

    portal = BatchPortal(answer)
    events, report = await _outcomes(portal, (1, 2))

    assert portal.waves == [2]
    heads = {record.reference_index: record for record in report.page_trace if record.offset == 0}
    assert (heads[0].dispatch, heads[0].batch_index, heads[0].outcome) == (PageDispatch.BATCH, 0, PageOutcome.COMMITTED)
    assert (heads[1].dispatch, heads[1].batch_index, heads[1].outcome) == (PageDispatch.BATCH, 1, PageOutcome.REJECTED)
    assert [event.correlation for event in events if isinstance(event, ReferenceFailure | ReferenceOutcomeUnknown)] == [
        2
    ]


@pytest.mark.asyncio
async def test_a20_provenance_the_flat_counted_head_stays_direct() -> None:
    portal = BatchPortal(_dense(2 * WIDTH))
    async with _client(portal) as api:
        stream = api.iter_list_counted(
            Request.bare("crm.item.list", {"PARENT": 1}, ReplaySafety.SAFE),
            page_size=WIDTH,
        )
        async with stream:
            rows = [row async for row in stream]

    assert len(rows) == 2 * WIDTH
    assert portal.waves == [1, 1]
    assert [record.dispatch for record in _report(stream).page_trace] == [PageDispatch.DIRECT, PageDispatch.BATCH]


# --- A20-N --------------------------------------------------------------------------------------


def _slow_first_tail_wave(rows: int) -> Callable[[str, Query], Awaitable[Envelope]]:
    async def answer(method: str, query: Query) -> Envelope:
        if int(query.get("start", "0")) == WIDTH:
            await asyncio.sleep(0.05)
        return _dense(rows)(method, query)

    return answer


@pytest.mark.asyncio
async def test_a20_n_out_of_order_receipts_commit_in_offset_order() -> None:
    portal = BatchPortal(_slow_first_tail_wave(5 * WIDTH))
    events, report = await _outcomes(portal, (1,), dispatch=BatchDispatch(batch_size=2, concurrency=2))

    assert _items(events, 1) == [100_000 + row for row in range(5 * WIDTH)]
    assert report.state is TerminalState.COMPLETED
    assert [record.offset for record in report.page_trace] == [0, 50, 100, 150, 200]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("damage", "partial"),
    [
        pytest.param(lambda envelope: envelope.update(result=envelope["result"][:-1]), 2 * WIDTH, id="gap"),
        pytest.param(lambda envelope: envelope.update(total=ROWS + 1), 2 * WIDTH, id="drift"),
        pytest.param(lambda envelope: envelope.update(next=7), 2 * WIDTH, id="contradictory-next"),
    ],
)
async def test_a20_n_a_damaged_tail_page_stops_the_binding_with_its_ordered_prefix(
    damage: Callable[[Envelope], None],
    partial: int,
) -> None:
    def answer(method: str, query: Query) -> Envelope:
        envelope = _dense(ROWS)(method, query)
        if query["PARENT"] == "1" and query.get("start") == str(2 * WIDTH):
            damage(envelope)
        return envelope

    portal = BatchPortal(answer)
    events, report = await _outcomes(portal, (1, 2))

    failures = [event for event in events if isinstance(event, ReferenceFailure)]
    assert [(failure.correlation, failure.partial_rows) for failure in failures] == [(1, partial)]
    assert _items(events, 1) == [100_000 + row for row in range(partial)]
    assert _completes(events)[2].row_count == ROWS
    assert report.state is TerminalState.COMPLETED_WITH_FAILURES
    assert all(not violation.code.startswith("completion_") for violation in report.violations)


@pytest.mark.asyncio
async def test_a20_n_a_buffer_of_one_page_still_completes_in_order() -> None:
    portal = BatchPortal(_dense(4 * WIDTH))
    events, report = await _outcomes(portal, (1, 2), policy=ExecutionPolicy(max_buffered_rows=WIDTH))

    for parent in (1, 2):
        assert _items(events, parent) == [parent * 100_000 + row for row in range(4 * WIDTH)]
    assert report.state is TerminalState.COMPLETED
    assert report.buffered_rows_high_water <= WIDTH


# --- A20-SIBLING --------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a20_sibling_a_counted_head_never_replaces_a_siblings_buffered_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    balance = [0]
    observed: list[int] = []
    resets: list[int] = []
    adjust = ExecutionContext.adjust_buffered_rows
    reset = ExecutionContext.set_buffered_rows

    async def tracked_adjust(self: ExecutionContext, delta: int) -> None:
        await adjust(self, delta)
        balance[0] += delta
        observed.append(balance[0])

    async def tracked_reset(self: ExecutionContext, rows: int) -> None:
        resets.append(rows)
        await reset(self, rows)

    monkeypatch.setattr(ExecutionContext, "adjust_buffered_rows", tracked_adjust)
    monkeypatch.setattr(ExecutionContext, "set_buffered_rows", tracked_reset)

    def answer(method: str, query: Query) -> Envelope:
        rows = 30 if query["PARENT"] == "1" else 2 * WIDTH
        return _dense(rows)(method, query)

    portal = BatchPortal(answer)
    async with _client(portal) as api:
        stream = api.iter_reference_outcomes(
            _request(),
            _bindings(1, 2),
            traversal=CountedTraversal(page_size=WIDTH),
            dispatch=BatchDispatch(output_order=DeliveryOrder.INPUT),
        )
        async with stream:
            first = await anext(stream)
            assert isinstance(first, ReferenceItem)
            await asyncio.sleep(0.05)  # the counted head lands while the sibling's page stays buffered
            assert balance[0] == 30 + WIDTH
            events = [first, *[event async for event in stream]]

    assert sum(isinstance(event, ReferenceItem) for event in events) == 30 + 2 * WIDTH
    assert max(observed) >= 30 + WIDTH
    # The reference path never replaces the operation counter; only the pool's close clears it.
    assert resets == [0]
    assert balance[0] == 0


# --- A20-LOAD -----------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a20_load_an_overloaded_tail_fails_finitely_with_an_honest_partial() -> None:
    def answer(method: str, query: Query) -> Envelope:
        if int(query.get("start", "0")) >= 2 * WIDTH:
            return {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "Too many requests"}
        return _dense(ROWS)(method, query)

    portal = BatchPortal(answer)
    policy = ExecutionPolicy(retry=RetryPolicy(initial_delay=0.001, maximum_delay=0.002, jitter=0.0))
    async with asyncio.timeout(30):
        events, report = await _outcomes(portal, (1,), dispatch=BatchDispatch(batch_size=3), policy=policy)

    failures = [event for event in events if isinstance(event, ReferenceFailure | ReferenceOutcomeUnknown)]
    assert len(failures) == 1
    assert failures[0].partial_rows == 2 * WIDTH
    assert not report.exhausted
    # Bounded backoff retries of the overloaded wave only: no hot loop of resends.
    attempts = policy.max_attempts_per_request
    assert report.physical_requests <= 2 + attempts
    assert len(portal.queries) <= 2 + 3 * attempts


# --- AFLAT --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aflat_flat_counted_keeps_its_direct_head_wire_and_counter_sequence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resets: list[int] = []
    reset = ExecutionContext.set_buffered_rows

    async def tracked_reset(self: ExecutionContext, rows: int) -> None:
        resets.append(rows)
        await reset(self, rows)

    monkeypatch.setattr(ExecutionContext, "set_buffered_rows", tracked_reset)
    portal = BatchPortal(_dense(ROWS))
    async with _client(portal) as api:
        stream = api.iter_list_counted(
            Request.bare("crm.item.list", {"PARENT": 3}, ReplaySafety.SAFE),
            page_size=WIDTH,
            batch_size=10,
        )
        async with stream:
            rows = [row async for row in stream]

    assert len(rows) == ROWS
    assert portal.waves == [1, 10, 10]
    assert resets[:2] == [WIDTH, 0]
    report = _report(stream)
    assert [record.dispatch for record in report.page_trace] == [PageDispatch.DIRECT] + [PageDispatch.BATCH] * 20
    assert report.state is TerminalState.COMPLETED


# --- AX -----------------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("order", [DeliveryOrder.INPUT, DeliveryOrder.READY])
async def test_ax_closing_mid_tail_cleans_up_once_without_deadlock(order: DeliveryOrder) -> None:
    portal = BatchPortal(_slow_first_tail_wave(ROWS))
    async with _client(portal) as api:
        stream = api.iter_reference_outcomes(
            _request(),
            _bindings(1, 2, 3),
            traversal=CountedTraversal(page_size=WIDTH),
            dispatch=BatchDispatch(batch_size=7, output_order=order),
        )
        async with asyncio.timeout(10):
            async with stream:
                taken = 0
                async for event in stream:
                    taken += isinstance(event, ReferenceItem)
                    if taken == 75:  # noqa: PLR2004 - inside the first tail page
                        break
    report = _report(stream)
    assert report.state is TerminalState.EARLY_CLOSED
    assert not report.exhausted
    assert all(not violation.code.startswith("completion_") for violation in report.violations)


@pytest.mark.asyncio
async def test_ax_cancelling_the_consumer_keeps_the_primary_cancellation() -> None:
    """The cancellation reaches the consumer while it waits inside the stream, with a tail wave in flight."""
    tail_sent = asyncio.Event()

    async def answer(method: str, query: Query) -> Envelope:
        if int(query.get("start", "0")) == WIDTH:
            tail_sent.set()
            await asyncio.sleep(0.05)
        return _dense(ROWS)(method, query)

    portal = BatchPortal(answer)
    async with _client(portal) as api:
        stream = api.iter_reference_outcomes(_request(), _bindings(1, 2), traversal=CountedTraversal(page_size=WIDTH))
        waiting = False

        async def consume() -> None:
            nonlocal waiting
            async with stream:
                events = aiter(stream)
                while True:
                    waiting = True
                    try:
                        await anext(events)
                    except StopAsyncIteration:
                        return
                    finally:
                        waiting = False
                    await asyncio.sleep(0)

        task = asyncio.create_task(consume())
        await tail_sent.wait()
        # Cancel only once the consumer is suspended inside the stream, not in its own loop body.
        while not waiting:  # noqa: ASYNC110 - an Event wake-up could arrive after the consumer left the stream
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert _report(stream).state is TerminalState.CANCELLED
