"""Counted references on the shared batch queue (#20) and the empty counted head (#11).

Acceptance A11, A11-STRICT, A20, A20-COST, A20-N, A20-VARIABLE, A20-PROVENANCE, A20-SIBLING,
A20-LOAD, AFLAT and AX: one counted head transaction serves the flat stream and the reference
queue; a reference binding's head and dense tail ride the shared dispatcher and row pool.
"""

from __future__ import annotations
import asyncio
import json
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
    SequentialTraversal,
    Settings,
    TerminalState,
    TotalTermination,
)
from b24api.completion import CompletionGate
from b24api.contracts import (
    ConsistencyPolicy,
    DeliveryOrder,
    PageDispatch,
    PageOutcome,
    PageRejectionCode,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ReferenceOutcomeUnknown,
    TraversalAssurance,
)
from b24api.contracts.completion import (
    BindingClosure,
    BindingTerminal,
    CommandSettlement,
    CompletionEvidence,
    PageCommandOutcome,
    PageRejected,
    PageScheduled,
)
from b24api.contracts.policy import ConfirmationPolicy, ReplayDisposition, SnapshotRequirement
from b24api.errors import BatchCommandError, BudgetExceededError, IncompleteTraversalError
from b24api.execution import ExecutionContext
from b24api.transport import WireResponse
from tests.scripting import (
    DEADLINE_BUDGET,
    HANG_BOUND,
    UNREACHABLE_COALESCE_WAIT,
    BatchPortal,
    Envelope,
    Query,
    ResponderTransport,
    VirtualClock,
    assert_fired_near_deadline,
    client_for,
    deadline_started,
    on_loop,
    reached,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from b24api.contracts import OperationReport, OperationStream, ReferenceOutcome

WIDTH = 50
ROWS = 1001
_ABSENT = object()
_COUNTED = OffsetSpec(total_termination=TotalTermination.EXACT_QUALIFIED)


# Tests that assert exact waves: a wave closes by its size or by the end of input, never by the timer.
_WAVES = BatchDispatch(coalesce_wait=UNREACHABLE_COALESCE_WAIT)


def _client(portal: BatchPortal) -> Bitrix24:
    return Bitrix24(Settings(webhook_url="https://fixture.invalid/rest/1/token/"), transport=portal)


def _request() -> Request:
    return Request.bare("crm.item.list", {}, ReplaySafety.SAFE)


def _bindings(*parents: int) -> AsyncIterator[Binding[int]]:
    return on_loop(
        Binding("parent", (ParameterUpdate(ParameterPath(("PARENT",)), parent),), parent) for parent in parents
    )


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
            dispatch=dispatch or _WAVES,
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
async def test_a20_two_counted_parents_travel_in_two_http_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    # On a virtual clock each wave closes by its size or by the end of input, after a bounded number of
    # loop turns, and every wait its deadline ended is counted: the waves hold however the host runs.
    clock = VirtualClock().install(monkeypatch)
    portal = BatchPortal(_dense(ROWS))
    events, report = await _outcomes(portal, (1, 2), dispatch=BatchDispatch())

    assert portal.waves == [2, 40]
    assert clock.timer_firings == 0
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
            api.iter_reference_outcomes(_request(), _bindings(1, 2), traversal=traversal, dispatch=_WAVES)
            if tolerant
            else api.iter_references(_request(), _bindings(1, 2), traversal=traversal, dispatch=_WAVES)
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
        budget_started = deadline_started()
        stream = api.iter_reference_outcomes(
            _request(),
            _bindings(1, 2),
            traversal=CountedTraversal(page_size=WIDTH),
            dispatch=BatchDispatch(batch_size=1, concurrency=1),
            policy=ExecutionPolicy(max_elapsed=DEADLINE_BUDGET),
        )
        with pytest.raises(BudgetExceededError):
            async with asyncio.timeout(HANG_BOUND), stream:
                _ = [event async for event in stream]
        assert_fired_near_deadline(budget_started)

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
    sibling_buffered = asyncio.Event()
    adjust = ExecutionContext.adjust_buffered_rows
    reset = ExecutionContext.set_buffered_rows

    async def tracked_adjust(self: ExecutionContext, delta: int) -> None:
        await adjust(self, delta)
        balance[0] += delta
        observed.append(balance[0])
        if balance[0] == 30 + 2 * WIDTH:
            sibling_buffered.set()

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
            # The sibling's head and tail land, read ahead, while the first binding's page stays buffered.
            await reached(sibling_buffered)
            assert balance[0] == 30 + 2 * WIDTH
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
    # The bounds below count full waves: every tail page is known up front, so none waits for the timer.
    dispatch = BatchDispatch(batch_size=3, coalesce_wait=UNREACHABLE_COALESCE_WAIT)
    async with asyncio.timeout(30):
        events, report = await _outcomes(portal, (1,), dispatch=dispatch, policy=policy)

    failures = [event for event in events if isinstance(event, ReferenceFailure | ReferenceOutcomeUnknown)]
    assert len(failures) == 1
    assert failures[0].partial_rows == 2 * WIDTH
    assert not report.exhausted
    # Bounded backoff retries of the overloaded waves only, no hot loop of resends: after the head, the
    # first tail wave and the next page's wave, which is already on its way when the binding fails. How
    # often that second wave retries before the failure reaches it is a race with its backoff timer, so
    # only its bound is fixed.
    attempts = policy.max_attempts_per_request
    assert report.physical_requests <= 1 + 2 * attempts
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
    # Each tail page keeps its command's index in its own batch of ten, not its ordinal in the tail.
    assert [record.batch_index for record in report.page_trace] == [None, *range(10), *range(10)]
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


# --- What a failed or abandoned page reports and how it settles ------------------------------------


def _observed(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    events: list[object] = []
    emit = CompletionGate.emit

    def observe(gate: CompletionGate, event: CompletionEvidence) -> None:
        events.append(event)
        emit(gate, event)

    monkeypatch.setattr(CompletionGate, "emit", observe)
    return events


@pytest.mark.asyncio
@pytest.mark.parametrize(("stage", "refused_at"), [("heads", 1), ("tail", 2)])
async def test_counted_bindings_cancelled_while_a_replay_is_on_the_wire_settle_each_page_from_what_its_command_did(
    monkeypatch: pytest.MonkeyPatch, stage: str, refused_at: int
) -> None:
    # Two counted bindings share the dispatcher. One batch (the heads' or a tail chunk) answers every command with a
    # success but its first, which it refuses (QUERY_LIMIT_EXCEEDED); the reader is cancelled while that command's
    # replay is on the wire. Each page settles from what its own command did: one answered with a success
    # succeeded, the replayed one may have run, and one never sent was not executed.
    events = _observed(monkeypatch)
    portal = BatchPortal(_dense(301))
    sends = [0]
    refused: list[object] = []
    answered = [0]
    replaying = asyncio.Event()

    async def respond(request: Request) -> WireResponse:
        sends[0] += 1
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        if refused and refused[0] in commands.values():
            replaying.set()
            await asyncio.Future()
        answer = await portal.send(request, attempt_timeout=1.0, max_response_bytes=1 << 22)
        if sends[0] != refused_at:
            answered[0] += len(commands)
            return answer
        first = next(iter(commands))
        refused.append(commands[first])
        answered[0] += len(commands) - 1
        body = json.loads(answer.body)
        result = body["result"]
        del result["result"][first]
        for key in ("result_total", "result_next"):
            if isinstance(result.get(key), dict):
                result[key].pop(first, None)
        result["result_error"] = {first: {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "slow"}}
        return WireResponse(200, answer.headers, json.dumps(body).encode())

    async with client_for(ResponderTransport(respond, host=portal.host)) as client:
        stream = client.iter_reference_outcomes(
            _request(),
            _bindings(1, 2),
            traversal=CountedTraversal(page_size=WIDTH),
            dispatch=BatchDispatch(batch_size=4),
        )

        async def drain() -> None:
            async with stream:
                async for _ in stream:
                    pass

        reader = asyncio.create_task(drain())
        await asyncio.wait_for(replaying.wait(), HANG_BOUND)
        reader.cancel()
        done, _pending = await asyncio.wait({reader}, timeout=HANG_BOUND)
        assert done == {reader}

    scheduled = sum(isinstance(event, PageScheduled) for event in events)
    settled = Counter(event.outcome for event in events if isinstance(event, PageCommandOutcome))
    assert answered[0] > 0
    expected = {CommandSettlement.SUCCESS: answered[0], CommandSettlement.UNKNOWN: 1}
    if scheduled > answered[0] + 1:
        expected[CommandSettlement.NOT_EXECUTED] = scheduled - answered[0] - 1
    assert settled == expected
    # Only the tail case has pages never sent: the heads are both sent in the one batch.
    assert (CommandSettlement.NOT_EXECUTED in settled) is (stage == "tail")


_TRAVERSALS: dict[str, CountedTraversal | SequentialTraversal] = {
    "counted": CountedTraversal(page_size=WIDTH),
    "sequential": SequentialTraversal(page_size=WIDTH),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_send", [1, 2], ids=["first-pages", "second-pages"])
@pytest.mark.parametrize("traversal", list(_TRAVERSALS))
@pytest.mark.parametrize(
    ("status", "body", "outcome", "variant"),
    [
        # 200 with no Bitrix envelope: each page may have run.
        (200, b'{"foo": 1}', PageOutcome.UNKNOWN, ReferenceOutcomeUnknown),
        # 400 whose error object is malformed: the batch was answered and refused.
        (400, b'{"error": [1]}', PageOutcome.REJECTED, ReferenceFailure),
    ],
)
async def test_a_reference_batch_whose_envelope_fails_traces_each_page_by_its_envelope_failure(  # noqa: PLR0913
    failing_send: int, traversal: str, status: int, body: bytes, outcome: PageOutcome, variant: type[object]
) -> None:
    # A batch answer is one the batch envelope's own protocol failure explains, which each page's trace reports: the
    # first pages' batch (a counted head, or a plain sequential page), or the second's (a counted tail page, or the
    # next sequential page). Unknown when the page may have run, rejected when the batch was answered.
    portal = BatchPortal(_dense(301))
    sends = [0]

    async def respond(request: Request) -> WireResponse:
        sends[0] += 1
        answer = await portal.send(request, attempt_timeout=1.0, max_response_bytes=1 << 22)
        if sends[0] != failing_send:
            return answer
        return WireResponse(status, (("content-type", "application/json"),), body)

    async with client_for(ResponderTransport(respond, host=portal.host)) as client:
        stream = client.iter_reference_outcomes(
            _request(), _bindings(1, 2), traversal=_TRAVERSALS[traversal], dispatch=BatchDispatch()
        )
        async with stream:
            outcomes = [event async for event in stream]

    traced = sorted(
        (record.reference_index, record.offset, record.outcome, record.rejection_code)
        for record in _report(stream).page_trace
        if record.outcome is not PageOutcome.COMMITTED
    )
    offset = (failing_send - 1) * WIDTH
    assert traced == [(index, offset, outcome, PageRejectionCode.BATCH_ENVELOPE) for index in (0, 1)]
    assert [type(event) for event in outcomes if not isinstance(event, ReferenceItem)] == [variant, variant]


_NESTED = 100_000
_TAIL_PAGES = 6  # the tail pages of a 301-row binding in 50-row pages
_TAIL_CHUNK, _REPLAY = 2, 3  # the sends of the first tail chunk and of its refused command's replay


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["undecodable", "refuse_first"])
async def test_a_counted_tail_page_that_fails_with_its_chunk_is_traced_the_way_it_settles(
    monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    # The first tail chunk, which carries both bindings' tail pages, fails as a whole: its answer cannot be
    # decoded, or its first command is refused (QUERY_LIMIT_EXCEEDED) and the answer of that command's replay
    # cannot be decoded. Each binding fails at its first tail page, which is traced the way it settles: one that may
    # have run is unknown, one whose own command succeeded is rejected with the chunk (``transaction_aborted``). Each
    # page settles the same way, on the completion events.
    events = _observed(monkeypatch)
    portal = BatchPortal(_dense(301))
    undecodable = WireResponse(200, (("content-type", "application/json"),), b"[" * _NESTED + b"]" * _NESTED)
    sends = [0]

    async def respond(request: Request) -> WireResponse:
        sends[0] += 1
        if sends[0] == (_TAIL_CHUNK if mode == "undecodable" else _REPLAY):
            return undecodable
        answer = await portal.send(request, attempt_timeout=1.0, max_response_bytes=1 << 22)
        if sends[0] != _TAIL_CHUNK:
            return answer
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        first = next(iter(commands))
        body = json.loads(answer.body)
        result = body["result"]
        del result["result"][first]
        for key in ("result_total", "result_next"):
            if isinstance(result.get(key), dict):
                result[key].pop(first, None)
        result["result_error"] = {first: {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "slow"}}
        return WireResponse(200, answer.headers, json.dumps(body).encode())

    async with client_for(ResponderTransport(respond, host=portal.host)) as client:
        stream = client.iter_reference_outcomes(
            _request(), _bindings(1, 2), traversal=CountedTraversal(page_size=WIDTH), dispatch=BatchDispatch()
        )
        async with stream:
            outcomes = [event async for event in stream]

    traced = sorted(
        (record.reference_index, record.offset, record.outcome, record.rejection_code)
        for record in _report(stream).page_trace
        if record.outcome is not PageOutcome.COMMITTED
    )
    unknown = (PageOutcome.UNKNOWN, PageRejectionCode.AMBIGUOUS_EXECUTION)
    second = unknown if mode == "undecodable" else (PageOutcome.REJECTED, PageRejectionCode.TRANSACTION_ABORTED)
    assert traced == [(0, WIDTH, *unknown), (1, WIDTH, *second)]
    assert [event.correlation for event in outcomes if isinstance(event, ReferenceFailure)] == [1, 2]
    # The tail pages: every one may have run when the chunk's answer is lost; otherwise only the refused command's,
    # whose replay's answer is lost, and the rest of the chunk, its commands answered with successes, succeeded.
    settled = Counter((event.binding_id, event.outcome) for event in events if isinstance(event, PageCommandOutcome))
    tail = _TAIL_PAGES
    if mode == "undecodable":
        assert settled == {
            (0, CommandSettlement.SUCCESS): 1,
            (0, CommandSettlement.UNKNOWN): tail,
            (1, CommandSettlement.SUCCESS): 1,
            (1, CommandSettlement.UNKNOWN): tail,
        }
    else:
        assert settled == {
            (0, CommandSettlement.SUCCESS): tail,
            (0, CommandSettlement.UNKNOWN): 1,
            (1, CommandSettlement.SUCCESS): 1 + tail,
        }
    assert not any(isinstance(event, ReferenceComplete) for event in outcomes)


@pytest.mark.asyncio
async def test_a_counted_tail_page_answered_with_a_failure_is_traced_as_rejected_by_its_command() -> None:
    # A tail page whose command the batch answers with a failure ends its binding, and is traced: rejected, by its
    # command's failure, at its own offset.
    def answer(method: str, query: Query) -> Envelope:
        if query["PARENT"] == "1" and query.get("start") == str(2 * WIDTH):
            return {"error": "ACCESS_DENIED", "error_description": "denied"}
        return _dense(ROWS)(method, query)

    events, report = await _outcomes(BatchPortal(answer), (1, 2))

    failed = [
        (record.offset, record.outcome, record.rejection_code)
        for record in report.page_trace
        if record.outcome is not PageOutcome.COMMITTED
    ]
    assert failed == [(2 * WIDTH, PageOutcome.REJECTED, PageRejectionCode.COMMAND_FAILURE)]
    # It keeps its command's index in its batch: the next after the binding's first tail page, in the same batch.
    by_offset = {record.offset: record.batch_index for record in report.page_trace if record.reference_index == 0}
    first = by_offset[WIDTH]
    assert first is not None
    assert by_offset[2 * WIDTH] == first + 1
    assert [event.correlation for event in events if isinstance(event, ReferenceFailure)] == [1]
    assert _completes(events)[2].row_count == ROWS


def _refuse_once(refused: tuple[int, int]) -> Callable[[str, Query], Envelope]:
    """Serve 301 rows per parent, refusing the page at ``refused`` (parent, offset) once (QUERY_LIMIT_EXCEEDED)."""
    seen: set[tuple[int, int]] = set()
    dense = _dense(301)

    def answer(method: str, query: Query) -> Envelope:
        page = (int(query["PARENT"]), int(query.get("start", "0")))
        if page == refused and page not in seen:
            seen.add(page)
            return {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "slow"}
        return dense(method, query)

    return answer


async def _budgeted(
    portal: BatchPortal, max_requests: int
) -> tuple[list[ReferenceOutcome[int]], OperationReport, BaseException | None]:
    retry = RetryPolicy(initial_delay=0.001, maximum_delay=0.002, jitter=0.0)
    outcomes: list[ReferenceOutcome[int]] = []
    raised: BaseException | None = None
    async with _client(portal) as api:
        stream = api.iter_reference_outcomes(
            _request(),
            _bindings(1, 2),
            traversal=CountedTraversal(page_size=WIDTH),
            dispatch=_WAVES,
            policy=ExecutionPolicy(retry=retry, max_requests=max_requests),
        )
        try:
            async with stream:
                async for event in stream:
                    if not isinstance(event, ReferenceItem):
                        outcomes.append(event)  # noqa: PERF401 - the stream raises: keep what it yielded first
        except BudgetExceededError as error:
            raised = error
    return outcomes, _report(stream), raised


def _failed_trace(report: OperationReport) -> list[tuple[object, ...]]:
    return [
        (
            record.reference_index,
            record.offset,
            record.dispatch,
            record.batch_index,
            record.outcome,
            record.rejection_code,
        )
        for record in report.page_trace
        if record.outcome is not PageOutcome.COMMITTED
    ]


@pytest.mark.asyncio
async def test_a_counted_tail_page_refused_and_kept_from_its_replay_by_the_budget_fails_eligible_for_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The tail batch refuses the first binding's first tail page (QUERY_LIMIT_EXCEEDED), and the budget (two
    # requests) keeps it from its replay. The binding fails with the refusal, which a replay may still retry; its
    # page was sent and answered, so it is traced, rejected by its command, and settles as a failure. The other
    # binding, answered in full by the same batch, completes.
    events = _observed(monkeypatch)
    portal = BatchPortal(_refuse_once((1, WIDTH)))

    outcomes, report, raised = await _budgeted(portal, max_requests=2)

    assert isinstance(raised, BudgetExceededError)
    failure, complete = outcomes
    assert isinstance(failure, ReferenceFailure)
    assert failure.correlation == 1
    assert isinstance(failure.error, BatchCommandError)
    assert failure.error.normalized_code == "query_limit_exceeded"
    assert failure.replay_disposition is ReplayDisposition.ELIGIBLE
    assert isinstance(complete, ReferenceComplete)
    assert (complete.correlation, complete.row_count) == (2, 301)
    assert _failed_trace(report) == [
        (0, WIDTH, PageDispatch.BATCH, 0, PageOutcome.REJECTED, PageRejectionCode.COMMAND_FAILURE)
    ]
    settled = Counter((event.binding_id, event.outcome) for event in events if isinstance(event, PageCommandOutcome))
    assert settled == {
        (0, CommandSettlement.SUCCESS): _TAIL_PAGES,
        (0, CommandSettlement.FAILURE): 1,
        (1, CommandSettlement.SUCCESS): 1 + _TAIL_PAGES,
    }


@pytest.mark.asyncio
async def test_counted_tail_pages_the_budget_keeps_from_their_send_are_not_traced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The budget (one request) is spent on the heads' batch, so the tail batch is never sent. Its pages left no
    # request to record: neither is traced, and each settles as not executed; both bindings fail with the budget.
    events = _observed(monkeypatch)
    portal = BatchPortal(_refuse_once((1, WIDTH)))

    outcomes, report, raised = await _budgeted(portal, max_requests=1)

    assert isinstance(raised, BudgetExceededError)
    assert portal.waves == [2]
    assert [(type(event), event.correlation) for event in outcomes] == [(ReferenceFailure, 1), (ReferenceFailure, 2)]
    assert all(
        isinstance(event.error, BudgetExceededError) for event in outcomes if isinstance(event, ReferenceFailure)
    )
    assert _failed_trace(report) == []
    settled = Counter((event.binding_id, event.outcome) for event in events if isinstance(event, PageCommandOutcome))
    assert settled == {
        (binding, outcome): count
        for binding in (0, 1)
        for outcome, count in ((CommandSettlement.SUCCESS, 1), (CommandSettlement.NOT_EXECUTED, _TAIL_PAGES))
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "body", "outcome"),
    [(200, b'{"foo": 1}', PageOutcome.UNKNOWN), (400, b'{"error": [1]}', PageOutcome.REJECTED)],
)
async def test_a_flat_counted_tail_batch_whose_envelope_fails_traces_its_first_page_by_the_envelope_failure(
    status: int, body: bytes, outcome: PageOutcome
) -> None:
    # The flat counted head is direct; its tail batch is answered with a failure its envelope explains. The tail's
    # first page is traced by it, with its command's index in the batch.
    portal = BatchPortal(_dense(301))
    sends = [0]

    async def respond(request: Request) -> WireResponse:
        sends[0] += 1
        answer = await portal.send(request, attempt_timeout=1.0, max_response_bytes=1 << 22)
        if sends[0] == 1:
            return answer
        return WireResponse(status, (("content-type", "application/json"),), body)

    async with client_for(ResponderTransport(respond, host=portal.host)) as client:
        stream = client.iter_list_counted(
            Request.bare("crm.item.list", {"PARENT": 3}, ReplaySafety.SAFE), page_size=WIDTH
        )

        async def drain() -> None:
            async with stream:
                async for _ in stream:
                    pass

        with pytest.raises(IncompleteTraversalError):
            await drain()

    assert sends[0] == 2  # noqa: PLR2004
    assert _failed_trace(_report(stream)) == [
        (None, WIDTH, PageDispatch.BATCH, 0, outcome, PageRejectionCode.BATCH_ENVELOPE)
    ]


def _flat_settlement(events: list[object]) -> tuple[dict[int, CommandSettlement], list[int], BindingClosure]:
    """Return the flat binding's settlement by page id, the pages rejected as aborted, and its closure."""
    settled = {event.page_id: event.outcome for event in events if isinstance(event, PageCommandOutcome)}
    aborted = [
        event.page_id
        for event in events
        if isinstance(event, PageRejected) and event.reason == PageRejectionCode.TRANSACTION_ABORTED.value
    ]
    (closure,) = [event.closure for event in events if isinstance(event, BindingTerminal)]
    return settled, aborted, closure


@pytest.mark.asyncio
async def test_flat_counted_tail_pages_the_budget_keeps_from_their_send_are_not_traced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The budget (three requests) covers the direct head and both tail batches as planned, but the first tail
    # batch refuses its first page (QUERY_LIMIT_EXCEEDED) and that page's replay spends the third request. The
    # second tail batch is never sent: the traversal ends with the budget, and its first page, which no request
    # carried, is not traced. Each of its pages, the two the traversal never reached included, settles as not
    # executed, so the binding failed rather than ended unknown.
    events = _observed(monkeypatch)
    retry = RetryPolicy(initial_delay=0.001, maximum_delay=0.002, jitter=0.0)
    portal = BatchPortal(_refuse_once((3, WIDTH)))
    async with _client(portal) as api:
        stream = api.iter_list_counted(
            Request.bare("crm.item.list", {"PARENT": 3}, ReplaySafety.SAFE),
            page_size=WIDTH,
            batch_size=3,
            policy=ExecutionPolicy(retry=retry, max_requests=3),
        )

        async def drain() -> None:
            async with stream:
                async for _ in stream:
                    pass

        with pytest.raises(IncompleteTraversalError) as raised:
            await drain()

    assert isinstance(raised.value.__cause__, BudgetExceededError)
    assert portal.waves == [1, 3, 1]
    report = _report(stream)
    assert _failed_trace(report) == []
    assert [record.offset for record in report.page_trace] == [0, WIDTH, 2 * WIDTH, 3 * WIDTH]
    settled, aborted, closure = _flat_settlement(events)
    assert settled == dict.fromkeys(range(4), CommandSettlement.SUCCESS) | dict.fromkeys(
        range(4, 7), CommandSettlement.NOT_EXECUTED
    )
    assert aborted == []
    assert closure is BindingClosure.FAILURE


@pytest.mark.asyncio
async def test_a_flat_counted_tail_page_answered_with_a_failure_keeps_its_index_in_its_own_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The second tail batch (three pages each) answers its first page with a failure. That page is traced rejected by
    # its command, with its index in its own batch, as every committed tail page is. The batch answered its two
    # later pages with a success: the traversal stops before them, so each settles as a success and is rejected as
    # aborted with the traversal, and the binding failed rather than ended unknown.
    events = _observed(monkeypatch)

    def answer(method: str, query: Query) -> Envelope:
        if query.get("start") == str(4 * WIDTH):
            return {"error": "ACCESS_DENIED", "error_description": "denied"}
        return _dense(301)(method, query)

    portal = BatchPortal(answer)
    async with _client(portal) as api:
        stream = api.iter_list_counted(
            Request.bare("crm.item.list", {"PARENT": 3}, ReplaySafety.SAFE), page_size=WIDTH, batch_size=3
        )

        async def drain() -> None:
            async with stream:
                async for _ in stream:
                    pass

        with pytest.raises(IncompleteTraversalError):
            await drain()

    report = _report(stream)
    assert _failed_trace(report) == [
        (None, 4 * WIDTH, PageDispatch.BATCH, 0, PageOutcome.REJECTED, PageRejectionCode.COMMAND_FAILURE)
    ]
    committed = [
        (record.offset, record.batch_index) for record in report.page_trace if record.outcome is PageOutcome.COMMITTED
    ]
    assert committed == [(0, None), (WIDTH, 0), (2 * WIDTH, 1), (3 * WIDTH, 2)]
    settled, aborted, closure = _flat_settlement(events)
    assert settled == dict.fromkeys(range(7), CommandSettlement.SUCCESS) | {4: CommandSettlement.FAILURE}
    assert aborted == [5, 6]
    assert closure is BindingClosure.FAILURE


@pytest.mark.asyncio
async def test_flat_counted_tail_pages_cancelled_while_a_replay_is_on_the_wire_settle_from_what_each_command_did(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The first tail batch (three pages) answers its last two pages with a success and refuses its first
    # (QUERY_LIMIT_EXCEEDED); the reader is cancelled while that page's replay is on the wire, before the traversal
    # reached any of the three. Each settles from where its command stood: the two answered with a success
    # succeeded and are rejected as aborted, the replayed one may have run, so the binding ended unknown.
    events = _observed(monkeypatch)
    portal = BatchPortal(_dense(301))
    sends = [0]
    replaying = asyncio.Event()

    async def respond(request: Request) -> WireResponse:
        sends[0] += 1
        if sends[0] == 3:  # noqa: PLR2004 - the refused page's replay
            replaying.set()
            await asyncio.Future()
        answer = await portal.send(request, attempt_timeout=1.0, max_response_bytes=1 << 22)
        if sends[0] == 1:
            return answer
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        first = next(iter(commands))
        body = json.loads(answer.body)
        result = body["result"]
        del result["result"][first]
        for key in ("result_total", "result_next"):
            if isinstance(result.get(key), dict):
                result[key].pop(first, None)
        result["result_error"] = {first: {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "slow"}}
        return WireResponse(200, answer.headers, json.dumps(body).encode())

    retry = RetryPolicy(initial_delay=0.001, maximum_delay=0.002, jitter=0.0)
    async with client_for(ResponderTransport(respond, host=portal.host)) as client:
        stream = client.iter_list_counted(
            Request.bare("crm.item.list", {"PARENT": 3}, ReplaySafety.SAFE),
            page_size=WIDTH,
            batch_size=3,
            policy=ExecutionPolicy(retry=retry),
        )

        async def drain() -> None:
            async with stream:
                async for _ in stream:
                    pass

        reader = asyncio.create_task(drain())
        await asyncio.wait_for(replaying.wait(), HANG_BOUND)
        reader.cancel()
        done, _pending = await asyncio.wait({reader}, timeout=HANG_BOUND)
        assert done == {reader}

    settled, aborted, closure = _flat_settlement(events)
    assert settled == {
        0: CommandSettlement.SUCCESS,
        1: CommandSettlement.UNKNOWN,
        2: CommandSettlement.SUCCESS,
        3: CommandSettlement.SUCCESS,
    }
    assert aborted == [2, 3]
    assert closure is BindingClosure.UNKNOWN


@pytest.mark.asyncio
async def test_a_flat_counted_tail_page_whose_result_was_dropped_settles_as_its_outcome_did(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The row budget (101 rows) fits two pages per tail batch. The last batch answers its first page with a failure,
    # where the traversal stops, and its second with more rows than the budget has left, which the stream drops as
    # answered: that page, never reached, settles as its outcome did, a failure, not as the success its command was
    # answered with.
    events = _observed(monkeypatch)

    def answer(method: str, query: Query) -> Envelope:
        start = int(query.get("start", "0"))
        if start == 5 * WIDTH:
            return {"error": "ACCESS_DENIED", "error_description": "denied"}
        if start == 6 * WIDTH:
            return {"result": [{"ID": 600_000 + row} for row in range(120)], "total": 301}
        return _dense(301)(method, query)

    portal = BatchPortal(answer)
    async with _client(portal) as api:
        stream = api.iter_list_counted(
            Request.bare("crm.item.list", {"PARENT": 3}, ReplaySafety.SAFE),
            page_size=WIDTH,
            batch_size=3,
            policy=ExecutionPolicy(max_buffered_rows=101),
        )

        async def drain() -> None:
            async with stream:
                async for _ in stream:
                    pass

        with pytest.raises(IncompleteTraversalError):
            await drain()

    assert portal.waves == [1, 2, 2, 2]
    settled, aborted, closure = _flat_settlement(events)
    assert settled == dict.fromkeys(range(5), CommandSettlement.SUCCESS) | dict.fromkeys(
        (5, 6), CommandSettlement.FAILURE
    )
    assert aborted == []
    assert closure is BindingClosure.FAILURE
