"""Final confirmations of fast keyset references share the batch queue (#33).

A cohort of small parents under automatic execution plans each parent with a boundary wave and then
confirms its end with a sequential finish page. On the shared queue both travel as commands of the
shared dispatcher: no binding confirms its parent with a direct request, a binding awaiting the
consumer is a continuation the dispatcher waits for, and a plan that bounds the rows it can still
hold gives the rest of its lease back so the bindings waiting for one start in the same batches.
"""

# ruff: noqa: PLR2004

from __future__ import annotations
import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import pytest

from b24api import (
    AutoKeysetExecution,
    BatchDispatch,
    Binding,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    KeysetTraversal,
    ParameterPath,
    ParameterUpdate,
    RangeKeysetExecution,
    Request,
    ResultSelector,
    SequentialKeysetExecution,
    StableIntegerKeysetContract,
    TerminalState,
)
from b24api.contracts import (
    DeliveryOrder,
    KeysetExecutionKind,
    KeysetPhase,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
)
from b24api.contracts.report import PageDispatch, PageOutcome, PageRejectionCode
from b24api.contracts.request import RouteKind
from b24api.errors import IncompleteTraversalError
from b24api.execution import ExecutionContext, RateCoordinator
from b24api.references.budget_view import BindingBudgetView, RowLease
from b24api.references.dispatch import _RowBuffer
from tests.scripting import BatchPortal, client_for

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from b24api.contracts import ReferenceOutcome
    from b24api.contracts.report import OperationReport, PageRecord
    from b24api.contracts.stream import OperationStream
    from tests.scripting import Envelope, Query

PAGE = 10
PARENTS = 20
_CONTRACT = StableIntegerKeysetContract()
_POLICY = ExecutionPolicy(page_trace_limit=1024)

type Execution = AutoKeysetExecution | RangeKeysetExecution | SequentialKeysetExecution


def _answer(data: Mapping[str, tuple[int, ...]], *, failing: frozenset[str] = frozenset()) -> Callable[..., Envelope]:
    """Serve each parent's stable integer selection; a failing parent refuses every page past a cursor."""

    def answer(method: str, query: Query) -> Envelope:
        del method
        parent = query.get("filter[=parent]", "")
        if parent in failing and "filter[>ID]" in query:
            return {"error": "INTERNAL_SERVER_ERROR", "error_description": "finish refused"}
        selected = data[parent]
        if "filter[>ID]" in query:
            selected = tuple(value for value in selected if value > int(query["filter[>ID]"]))
        if "filter[<ID]" in query:
            selected = tuple(value for value in selected if value < int(query["filter[<ID]"]))
        descending = query.get("order[id]") == "DESC"
        limit = int(query.get("limit", PAGE))
        return {"result": [{"id": value} for value in sorted(selected, reverse=descending)[:limit]]}

    return answer


def _traversal(execution: Execution) -> KeysetTraversal:
    return KeysetTraversal(
        selector=ResultSelector.root(),
        identity=IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER),
        page_size=PAGE,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=execution,
    )


def _parent(name: str) -> Binding[object]:
    return Binding(name, (ParameterUpdate(ParameterPath(("filter", "=parent")), name),), name)


def _base() -> Request:
    return Request("item.list", {"filter": {"STATUS": "open"}}, route=RouteKind.BARE)


async def _drain[T](stream: OperationStream[T]) -> tuple[list[T], OperationReport]:
    async with stream:
        items = [item async for item in stream]
    report = stream.report
    assert report is not None
    return items, report


@dataclass(frozen=True)
class _Run:
    outcomes: list[ReferenceOutcome[object]]
    report: OperationReport
    portal: BatchPortal

    @property
    def rows(self) -> set[tuple[object, int]]:
        return {
            (outcome.correlation, cast("dict[str, int]", outcome.item)["id"])
            for outcome in self.outcomes
            if isinstance(outcome, ReferenceItem)
        }

    @property
    def completes(self) -> dict[object, ReferenceComplete[object]]:
        return {outcome.correlation: outcome for outcome in self.outcomes if isinstance(outcome, ReferenceComplete)}

    def finish_pages(self) -> list[PageRecord]:
        return [record for record in self.report.page_trace if record.phase is KeysetPhase.FINISH]


async def _run(
    execution: Execution,
    data: Mapping[str, tuple[int, ...]],
    *,
    failing: frozenset[str] = frozenset(),
) -> _Run:
    portal = BatchPortal(_answer(data, failing=failing))
    async with client_for(portal, policy=_POLICY) as client:
        outcomes, report = await _drain(
            client.iter_reference_outcomes(
                _base(),
                [_parent(name) for name in data],
                traversal=_traversal(execution),
                dispatch=BatchDispatch(),
            )
        )
    return _Run(outcomes, report, portal)


def _direct(report: OperationReport) -> int:
    """Return the physical requests sent outside a batch envelope."""
    return report.physical_requests - report.batch_requests


def _cohort(rows: int) -> dict[str, tuple[int, ...]]:
    return {f"p{index}": tuple(range(1, rows + 1)) for index in range(PARENTS)}


@dataclass
class _Trims:
    """Spy on the shared pool: every lease granted and every trim of one."""

    granted: list[int] = field(default_factory=list)
    trimmed: list[tuple[int, int]] = field(default_factory=list)


@pytest.fixture
def trims(monkeypatch: pytest.MonkeyPatch) -> _Trims:
    spy = _Trims()
    lease, trim = _RowBuffer.lease, _RowBuffer.settle_lease

    async def tracked_lease(self: _RowBuffer, index: int, grant: Callable[[int], int | None]) -> object:
        reservation = await lease(self, index, grant)
        spy.granted.append(reservation.amount)
        return reservation

    async def tracked_trim(self: _RowBuffer, reservation: object, amount: int) -> None:
        before = cast("int", getattr(reservation, "amount", 0))
        await trim(self, reservation, amount)  # type: ignore[arg-type]
        if amount < before:
            spy.trimmed.append((before, amount))

    monkeypatch.setattr(_RowBuffer, "lease", tracked_lease)
    monkeypatch.setattr(_RowBuffer, "settle_lease", tracked_trim)
    return spy


# The issue's cohort: one-row parents plan boundary-only, 25-row parents plan sequential.


@pytest.mark.parametrize(
    ("rows", "kind"), [(1, KeysetExecutionKind.BOUNDARY_ONLY), (25, KeysetExecutionKind.SEQUENTIAL)]
)
@pytest.mark.asyncio
async def test_a_small_parent_cohort_confirms_every_parent_inside_shared_batches(
    rows: int,
    kind: KeysetExecutionKind,
) -> None:
    data = _cohort(rows)
    auto = await _run(AutoKeysetExecution(_CONTRACT), data)
    sequential = await _run(SequentialKeysetExecution(), data)

    expected = {(name, value) for name, ids in data.items() for value in ids}
    assert auto.rows == sequential.rows == expected
    assert set(auto.completes) == set(data)
    assert all(complete.exhausted and complete.row_count == rows for complete in auto.completes.values())
    assert auto.report.state is TerminalState.COMPLETED
    assert {
        complete.keyset_execution.selected_kind
        for complete in auto.completes.values()
        if complete.keyset_execution is not None
    } == {kind}
    # Every parent ends on a final confirmation, and every one of them is a command of a shared batch.
    finish = auto.finish_pages()
    assert {record.reference_index for record in finish} == set(range(PARENTS))
    assert {record.dispatch for record in finish} == {PageDispatch.BATCH}
    assert _direct(auto.report) == 0
    assert auto.report.physical_requests == len(auto.portal.waves)
    # Before #33 each finish page was a direct request: 24 and 64 requests against 2 and 4 sequential.
    assert auto.report.physical_requests <= 2 * sequential.report.physical_requests + 2
    assert max(auto.portal.waves) > PARENTS


@pytest.mark.asyncio
async def test_a_flat_traversal_still_confirms_its_end_with_one_direct_request() -> None:
    portal = BatchPortal(_answer({"": (1,)}))
    async with client_for(portal, policy=_POLICY) as client:
        stream = client.iter_list_keyset(
            _base(),
            selector=ResultSelector.root(),
            identity=IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER),
            page_size=PAGE,
            keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
            execution=AutoKeysetExecution(_CONTRACT),
        )
        rows, report = await _drain(stream)

    assert rows == [{"id": 1}]
    finish = [record for record in report.page_trace if record.phase is KeysetPhase.FINISH]
    assert [record.dispatch for record in finish] == [PageDispatch.DIRECT]
    assert _direct(report) == 1
    assert report.physical_requests == 2


@pytest.mark.asyncio
async def test_a_refused_shared_finish_fails_its_binding_closed_while_the_cohort_completes() -> None:
    data = _cohort(1)
    run = await _run(AutoKeysetExecution(_CONTRACT), data, failing=frozenset({"p3"}))

    failures = [outcome for outcome in run.outcomes if isinstance(outcome, ReferenceFailure)]
    assert [failure.correlation for failure in failures] == ["p3"]
    assert isinstance(failures[0].error, IncompleteTraversalError)
    # The row the boundary wave admitted reached the caller; the binding never claims exhaustion.
    assert failures[0].partial_rows == 1
    assert set(run.completes) == set(data) - {"p3"}
    assert run.report.state is TerminalState.COMPLETED_WITH_FAILURES
    assert _direct(run.report) == 0
    refused = [record for record in run.finish_pages() if record.reference_index == 3]
    assert [(record.dispatch, record.outcome, record.rejection_code) for record in refused] == [
        (PageDispatch.BATCH, PageOutcome.REJECTED, PageRejectionCode.COMMAND_FAILURE),
    ]


# A plan that bounds the rows it can still hold gives the rest of its lease back.


@pytest.mark.asyncio
async def test_a_bounded_plan_trims_its_lease_and_a_lane_plan_keeps_it(trims: _Trims) -> None:
    small = await _run(AutoKeysetExecution(_CONTRACT), _cohort(1))
    assert small.report.state is TerminalState.COMPLETED
    assert len(trims.granted) == PARENTS
    # A boundary-only plan holds its head row, then one finish page at a time: one page cap.
    assert len(trims.trimmed) == PARENTS
    assert all(after == PAGE < before for before, after in trims.trimmed)

    trims.granted.clear()
    trims.trimmed.clear()
    dense = await _run(RangeKeysetExecution(_CONTRACT), {"dense": tuple(range(1, 201))})
    assert dense.report.state is TerminalState.COMPLETED
    assert len(trims.granted) == 1
    assert trims.trimmed == []


def _shared(rows: int) -> ExecutionContext:
    return ExecutionContext(ExecutionPolicy(max_buffered_rows=rows), RateCoordinator(max_concurrency=1))


@pytest.mark.asyncio
async def test_a_trimmed_view_names_its_capacity_and_never_drops_below_its_held_rows() -> None:
    view = BindingBudgetView.over(_shared(100), RowLease(80))
    await view.adjust_buffered_rows(15)
    view.trim(20)
    assert view.lease.capacity == view.policy.max_buffered_rows == 20
    with pytest.raises(RuntimeError, match="trims only"):
        view.trim(14)
    with pytest.raises(RuntimeError, match="trims only"):
        view.trim(21)
    with pytest.raises(ValueError, match="positive"):
        view.trim(0)
    with pytest.raises(Exception, match="row lease"):
        await view.adjust_buffered_rows(6)
    await view.adjust_buffered_rows(5)
    assert view.lease.held == view.lease.capacity == 20


@pytest.mark.asyncio
async def test_a_trimmed_lease_wakes_the_binding_waiting_for_rows() -> None:
    pool = _RowBuffer(100, _shared(100))
    first = await pool.lease(0, lambda free: free)
    waiting = asyncio.create_task(pool.lease(1, lambda free: 40 if free >= 40 else None))
    await asyncio.sleep(0)
    assert not waiting.done()
    await pool.settle_lease(first, 60)
    second = await asyncio.wait_for(waiting, 1)
    assert (first.amount, second.amount) == (60, 40)
    with pytest.raises(RuntimeError, match="cannot grow"):
        await pool.settle_lease(first, 61)
    page = await pool.reserve(2, 0)
    with pytest.raises(RuntimeError, match="only a lease settles"):
        await pool.settle_lease(page, 1)
    await pool.close()
    await pool.settle_lease(first, 1)
    assert first.amount == 0


@pytest.mark.asyncio
async def test_a_page_charged_to_a_lease_is_admissible_behind_the_input_frontier() -> None:
    pool = _RowBuffer(100, _shared(100), head_reserve=30)
    await pool.lease(1, lambda free: free - 30)
    await pool.lease(2, lambda free: free)
    # The free pool is exhausted, so a pooled page behind the frontier waits; a leased page reserves nothing.
    assert not pool.can_reserve(1, PAGE)
    assert pool.can_reserve(1, 0)


# Under INPUT delivery a trimmed lease must not strand the next frontier.


def _ids(start: int, size: int, step: int) -> tuple[int, ...]:
    return tuple(range(start, start + size * step, step))


_TIGHT_INPUT_COHORTS = {
    # A binding behind the frontier leased first; the next frontier must wait for it to plan and trim.
    "behind_the_frontier_not_yet_planned": {"p0": (5,), "p1": _ids(20, 19, 7), "p2": (), "p3": _ids(37, 5, 1)},
    # Freed rows must go to the earlier waiting binding, which becomes the frontier first.
    "earlier_waiter_first": {
        "p0": _ids(44, 2, 7),
        "p1": _ids(34, 5, 1),
        "p2": _ids(13, 20, 1),
        "p3": _ids(46, 40, 7),
        "p4": _ids(3, 11, 2),
        "p5": _ids(25, 21, 1),
        "p6": _ids(27, 15, 2),
    },
}


@pytest.mark.parametrize("cohort", sorted(_TIGHT_INPUT_COHORTS))
@pytest.mark.asyncio
async def test_a_tight_input_pool_completes_every_parent_in_binding_order(cohort: str) -> None:
    data = _TIGHT_INPUT_COHORTS[cohort]
    portal = BatchPortal(_answer(data))
    policy = ExecutionPolicy(max_buffered_rows=40, page_trace_limit=1024)
    async with client_for(portal, policy=policy) as client:
        outcomes, report = await _drain(
            client.iter_reference_outcomes(
                _base(),
                [_parent(name) for name in data],
                traversal=_traversal(RangeKeysetExecution(_CONTRACT)),
                dispatch=BatchDispatch(batch_size=2, output_order=DeliveryOrder.INPUT),
            )
        )

    assert [outcome for outcome in outcomes if isinstance(outcome, ReferenceFailure)] == []
    assert report.state is TerminalState.COMPLETED
    delivered = [
        (outcome.correlation, cast("dict[str, int]", outcome.item)["id"])
        for outcome in outcomes
        if isinstance(outcome, ReferenceItem)
    ]
    assert delivered == [(name, value) for name, ids in data.items() for value in sorted(ids)]


@pytest.mark.asyncio
async def test_an_unsettled_lease_is_releasable_and_lease_waiters_are_known_in_order() -> None:
    pool = _RowBuffer(100, _shared(100), head_reserve=10)
    behind = await pool.lease(3, lambda _free: 60)
    # A lease whose binding has not planned yet may still be trimmed: the frontier waits for it.
    assert pool.releasable_elsewhere(2)
    await pool.settle_lease(behind, 60)
    assert not pool.releasable_elsewhere(2)
    assert not pool.lease_waiting_before(5)
    later = asyncio.create_task(pool.lease(5, lambda _free: None))
    await asyncio.sleep(0)
    assert pool.lease_waiting_before(6)
    assert not pool.lease_waiting_before(5)
    later.cancel()
    with pytest.raises(asyncio.CancelledError):
        await later
    assert not pool.lease_waiting_before(6)
