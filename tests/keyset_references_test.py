"""Fast and automatic keyset references on the shared queue (#27).

Acceptance: A27, A27-SHARED, A27-N, A27-VIEW, A27-RETAIN, A27-WAVE1, A27-GRANT-LOW, A27-GRANT-OK,
A27-INPUT-TIGHT, A27-INPUT-OK.

Every binding runs its own keyset runtime under a fixed row lease ``G`` of the shared row pool and
sends its waves through the shared dispatcher. The oracle for a binding is the flat traversal of
the same selection under ``max_buffered_rows=G``: selection, geometry and cost must match it.
"""

# ruff: noqa: PLR2004

from __future__ import annotations
import asyncio
import dataclasses
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
    PartitionedKeysetExecution,
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
    KeysetSelectionReason,
    KeysetSelectionSummary,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ReferenceNotExecuted,
    ReferenceSource,
)
from b24api.contracts.page_stop import ContinuePage
from b24api.contracts.request import RouteKind
from b24api.errors import CapabilityError
from b24api.execution import ExecutionContext
from b24api.references import wave_engine
from b24api.references.budget_view import BindingBudgetView, RowLease
from b24api.references.dispatch import _RowBuffer
from b24api.traversal.keyset_fast_plan import fit_wave
from b24api.traversal.keyset_runtime import KeysetRuntime
from tests.scripting import BatchPortal, client_for

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from b24api.contracts import KeysetExecutionReport, ReferenceOutcome
    from b24api.contracts.report import OperationReport
    from b24api.contracts.stream import OperationStream
    from b24api.traversal.keyset_context import KeysetContext
    from tests.scripting import Envelope, Query

PAGE = 10
LANES = 4
_CONTRACT = StableIntegerKeysetContract()

type Execution = RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution


def _range() -> RangeKeysetExecution:
    return RangeKeysetExecution(_CONTRACT)


def _partitioned(lanes: int = LANES) -> PartitionedKeysetExecution:
    return PartitionedKeysetExecution(_CONTRACT, target_lanes=lanes)


def _auto(lanes: int = LANES) -> AutoKeysetExecution:
    return AutoKeysetExecution(_CONTRACT, target_lanes=lanes)


DISTRIBUTIONS: dict[str, tuple[int, ...]] = {
    "dense": tuple(range(1, 201)),
    "skewed": (*range(1, 151), *range(10_000, 10_051)),
    "sparse": tuple(range(37, 5_000, 37)),
}


def _rows(ids: tuple[int, ...], query: Query, *, ignore_bounds: bool = False) -> list[dict[str, int]]:
    selected = ids
    if not ignore_bounds:
        if "filter[>ID]" in query:
            selected = tuple(value for value in selected if value > int(query["filter[>ID]"]))
        if "filter[<ID]" in query:
            selected = tuple(value for value in selected if value < int(query["filter[<ID]"]))
    descending = query.get("order[id]") == "DESC"
    limit = int(query.get("limit", PAGE))
    return [{"id": value} for value in sorted(selected, reverse=descending)[:limit]]


def _answer(
    data: Mapping[str, tuple[int, ...]], *, ignore_bounds: frozenset[str] = frozenset()
) -> Callable[..., Envelope]:
    """Serve each parent's own stable integer selection from ``filter[=parent]``."""

    def answer(method: str, query: Query) -> Envelope:
        del method
        parent = query.get("filter[=parent]", "")
        return {"result": _rows(data[parent], query, ignore_bounds=parent in ignore_bounds)}

    return answer


def _flat_answer(ids: tuple[int, ...]) -> Callable[..., Envelope]:
    def answer(method: str, query: Query) -> Envelope:
        del method
        return {"result": _rows(ids, query)}

    return answer


def _identity() -> IdentitySpec:
    return IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER)


def _keyset() -> KeysetSpec:
    return KeysetSpec(limit_path=ParameterPath(("limit",)))


def _traversal(execution: Execution | SequentialKeysetExecution) -> KeysetTraversal:
    return KeysetTraversal(
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE,
        keyset=_keyset(),
        execution=execution,
    )


def _base() -> Request:
    return Request("item.list", {"filter": {"STATUS": "open"}}, route=RouteKind.BARE)


def _parent(name: str, correlation: object = None) -> Binding[object]:
    return Binding(
        name,
        (ParameterUpdate(ParameterPath(("filter", "=parent")), name),),
        name if correlation is None else correlation,
    )


@dataclass
class _Leases:
    """Spy on the shared pool: every lease granted, the leases outstanding and their high water."""

    granted: list[tuple[int, int]] = field(default_factory=list)
    high_water: int = 0
    pools: list[_RowBuffer] = field(default_factory=list)
    live: list[object] = field(default_factory=list)

    @property
    def outstanding(self) -> int:
        """Return the rows still held by granted leases (a closed pool zeroes every reservation)."""
        return sum(getattr(reservation, "amount", 0) for reservation in self.live)


@pytest.fixture
def leases(monkeypatch: pytest.MonkeyPatch) -> _Leases:
    spy = _Leases()
    lease = _RowBuffer.lease

    async def tracked_lease(self: _RowBuffer, index: int, grant: Callable[[int], int | None]) -> object:
        reservation = await lease(self, index, grant)
        spy.granted.append((index, reservation.amount))
        spy.live.append(reservation)
        spy.high_water = max(spy.high_water, spy.outstanding)
        if self not in spy.pools:
            spy.pools.append(self)
        return reservation

    monkeypatch.setattr(_RowBuffer, "lease", tracked_lease)
    return spy


async def _drain[T](stream: OperationStream[T]) -> tuple[list[T], OperationReport]:
    async with stream:
        items = [item async for item in stream]
    report = stream.report
    assert report is not None
    return items, report


def _rows_by_parent(outcomes: list[ReferenceOutcome[object]]) -> dict[object, list[int]]:
    rows: dict[object, list[int]] = {}
    for outcome in outcomes:
        if isinstance(outcome, ReferenceItem):
            item = cast("dict[str, int]", outcome.item)
            rows.setdefault(outcome.correlation, []).append(item["id"])
    return rows


def _completes(outcomes: list[ReferenceOutcome[object]]) -> dict[object, ReferenceComplete[object]]:
    return {outcome.correlation: outcome for outcome in outcomes if isinstance(outcome, ReferenceComplete)}


async def _flat_oracle(ids: tuple[int, ...], execution: Execution, rows: int) -> KeysetExecutionReport:
    """Run the flat traversal of one selection under ``max_buffered_rows=rows``; return its evidence."""
    portal = BatchPortal(_flat_answer(ids))
    async with client_for(portal, policy=ExecutionPolicy(max_buffered_rows=rows)) as client:
        stream = client.iter_list_keyset(
            _base(),
            selector=ResultSelector.root(),
            identity=_identity(),
            page_size=PAGE,
            keyset=_keyset(),
            execution=execution,
        )
        delivered, report = await _drain(stream)
    assert [cast("dict[str, int]", row)["id"] for row in delivered] == sorted(ids)
    assert report.keyset_execution is not None
    return report.keyset_execution


def _cost(report: KeysetExecutionReport) -> dict[str, object]:
    """Selection, geometry and cost of one fast run: every evidence field but the shared batch counts."""
    return {
        item.name: getattr(report, item.name)
        for item in dataclasses.fields(report)
        if item.name not in {"batch_requests", "batch_commands", "page_trace_class_counts"}
    }


# A27 / A27-SHARED: completeness and flat-oracle equality across executions, fan-out and density.


@pytest.mark.parametrize("execution", [_range(), _partitioned(), _auto()], ids=["range", "partitioned", "auto"])
@pytest.mark.parametrize("parents", [1, 4, 16])
@pytest.mark.parametrize("distribution", sorted(DISTRIBUTIONS))
@pytest.mark.asyncio
async def test_fast_references_match_the_flat_oracle_at_their_lease(
    execution: Execution,
    parents: int,
    distribution: str,
    leases: _Leases,
) -> None:
    ids = DISTRIBUTIONS[distribution]
    names = [f"p{index}" for index in range(parents)]
    # Every parent holds the same identities: per-binding identity state must keep them apart.
    portal = BatchPortal(_answer(dict.fromkeys(names, ids)))
    # Logical pages are one operation budget: sixteen sparse range sweeps need more than one flat sweep.
    policy = ExecutionPolicy(max_pages=50_000)
    async with client_for(portal, policy=policy) as client:
        outcomes, report = await _drain(
            client.iter_reference_outcomes(
                _base(),
                [_parent(name) for name in names],
                traversal=_traversal(execution),
                dispatch=BatchDispatch(batch_size=50),
            )
        )

    rows = _rows_by_parent(outcomes)
    completes = _completes(outcomes)
    assert set(rows) == set(names) == set(completes)
    assert all(rows[name] == sorted(ids) for name in names)
    assert all(complete.exhausted and complete.row_count == len(ids) for complete in completes.values())
    assert report.state is TerminalState.COMPLETED
    assert leases.outstanding == 0
    assert all(pool.maximum == ExecutionPolicy().max_buffered_rows for pool in leases.pools)
    grants = dict(leases.granted)
    for index, name in enumerate(names):
        evidence = completes[name].keyset_execution
        assert evidence is not None
        assert completes[name].keyset_selection == KeysetSelectionSummary.of(evidence)
        oracle = await _flat_oracle(ids, execution, grants[index])
        assert _cost(evidence) == _cost(oracle)


# A27-N: fail closed on bounds the binding or the server breaks; planning cancel; page stop.


@pytest.mark.asyncio
async def test_a_binding_that_writes_a_fast_owned_bound_fails_closed_before_any_request() -> None:
    portal = BatchPortal(_answer({"good": DISTRIBUTIONS["dense"], "bad": DISTRIBUTIONS["dense"]}))
    bad = Binding(
        "bad",
        (
            ParameterUpdate(ParameterPath(("filter", "=parent")), "bad"),
            ParameterUpdate(ParameterPath(("filter", ">ID")), 5),
        ),
        "bad",
    )
    async with client_for(portal) as client:
        outcomes, _report = await _drain(
            client.iter_reference_outcomes(_base(), [_parent("good"), bad], traversal=_traversal(_auto()))
        )

    terminal = {outcome.correlation: outcome for outcome in outcomes if not isinstance(outcome, ReferenceItem)}
    assert isinstance(terminal["good"], ReferenceComplete)
    assert isinstance(terminal["bad"], ReferenceNotExecuted | ReferenceFailure)
    assert "bad" not in _rows_by_parent(outcomes)
    assert all(query.get("filter[=parent]") != "bad" for query in portal.queries)


@pytest.mark.parametrize("execution", [_range(), _partitioned(), _auto()], ids=["range", "partitioned", "auto"])
@pytest.mark.asyncio
async def test_a_server_ignoring_bounds_fails_its_binding_closed_while_neighbours_complete(
    execution: Execution,
    leases: _Leases,
) -> None:
    portal = BatchPortal(
        _answer({"honest": DISTRIBUTIONS["dense"], "liar": DISTRIBUTIONS["dense"]}, ignore_bounds=frozenset({"liar"}))
    )
    async with client_for(portal) as client:
        outcomes, report = await _drain(
            client.iter_reference_outcomes(
                _base(), [_parent("honest"), _parent("liar")], traversal=_traversal(execution)
            )
        )

    terminal = {outcome.correlation: outcome for outcome in outcomes if not isinstance(outcome, ReferenceItem)}
    assert isinstance(terminal["honest"], ReferenceComplete)
    assert _rows_by_parent(outcomes)["honest"] == sorted(DISTRIBUTIONS["dense"])
    liar = terminal["liar"]
    assert isinstance(liar, ReferenceFailure)
    # Whatever reached the caller is an ordered, duplicate-free prefix; the binding never claims exhaustion.
    delivered = _rows_by_parent(outcomes).get("liar", [])
    assert delivered == sorted(DISTRIBUTIONS["dense"])[: len(delivered)]
    assert liar.partial_rows == len(delivered) < len(DISTRIBUTIONS["dense"])
    assert report.state is TerminalState.COMPLETED_WITH_FAILURES
    assert leases.outstanding == 0


@pytest.mark.asyncio
async def test_closing_during_planning_returns_every_lease_without_a_hang(leases: _Leases) -> None:
    started = asyncio.Event()

    async def blocked(method: str, query: Query) -> Envelope:
        del method, query
        started.set()
        await asyncio.Future[None]()
        raise AssertionError("unreachable")

    portal = BatchPortal(blocked)
    async with client_for(portal) as client:
        stream = client.iter_reference_outcomes(
            _base(), [_parent("p0"), _parent("p1")], traversal=_traversal(_partitioned())
        )
        pull = asyncio.create_task(anext(aiter(stream)))
        await asyncio.wait_for(started.wait(), 5)
        pull.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pull
        await asyncio.wait_for(stream.aclose(), 5)

    assert leases.granted
    assert leases.outstanding == 0
    assert portal.waves  # planning was dispatched, then cancelled after send


@pytest.mark.asyncio
async def test_auto_with_a_page_stop_runs_sequentially_and_says_why() -> None:
    portal = BatchPortal(_answer({"p0": DISTRIBUTIONS["dense"]}))
    stops: list[int] = []

    def on_page(boundary: object) -> ContinuePage:
        stops.append(1)
        del boundary
        return ContinuePage.CONTINUE

    page_stop = type("_Stop", (), {"on_page": staticmethod(on_page)})()
    async with client_for(portal) as client:
        outcomes, _report = await _drain(
            client.iter_reference_outcomes(
                _base(),
                [_parent("p0")],
                traversal=_traversal(_auto()),
                page_stop=page_stop,
            )
        )

    complete = _completes(outcomes)["p0"]
    assert complete.keyset_selection == KeysetSelectionSummary(
        KeysetExecutionKind.AUTO, KeysetExecutionKind.SEQUENTIAL, KeysetSelectionReason.PAGE_STOP
    )
    assert complete.keyset_execution is None
    assert _rows_by_parent(outcomes)["p0"] == sorted(DISTRIBUTIONS["dense"])
    assert stops
    assert all(len(methods) == 1 for methods in portal.methods)


@pytest.mark.parametrize("execution", [_range(), _partitioned()], ids=["range", "partitioned"])
@pytest.mark.asyncio
async def test_explicit_fast_with_a_page_stop_or_direct_dispatch_is_rejected_before_any_request(
    execution: Execution,
) -> None:
    from b24api import DirectDispatch  # noqa: PLC0415 - one focused import

    portal = BatchPortal(_answer({"p0": DISTRIBUTIONS["dense"]}))
    page_stop = type("_Stop", (), {"on_page": staticmethod(lambda _boundary: ContinuePage.CONTINUE)})()
    async with client_for(portal) as client:
        with pytest.raises(CapabilityError, match="page stop requires sequential"):
            client.iter_reference_outcomes(
                _base(),
                [_parent("p0")],
                traversal=_traversal(execution),
                page_stop=page_stop,
            )
        with pytest.raises(CapabilityError, match="requires BatchDispatch"):
            client.iter_reference_outcomes(
                _base(), [_parent("p0")], traversal=_traversal(execution), dispatch=DirectDispatch()
            )
        outcomes, _report = await _drain(
            client.iter_reference_source_outcomes(
                [
                    ReferenceSource(_base(), _traversal(execution), _parent("p0"), page_stop=page_stop),
                    ReferenceSource(_base(), _traversal(execution), _parent("p0", "ok")),
                ]
            )
        )
    assert isinstance(outcomes[0], ReferenceNotExecuted) or isinstance(outcomes[-1], ReferenceNotExecuted)
    assert any(isinstance(outcome, ReferenceComplete) and outcome.correlation == "ok" for outcome in outcomes)


# A27-VIEW: the projection differs only in rows=G; engine and executor get the shared context; one
# shared delta per adjustment; the runtime balance equals the lease holding after every wave.


@pytest.mark.parametrize("execution", [_range(), _auto()], ids=["range", "auto"])
@pytest.mark.asyncio
async def test_the_budget_view_projects_only_the_lease_and_keeps_execution_on_the_shared_context(
    execution: Execution,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    views: list[tuple[BindingBudgetView, int]] = []
    over = BindingBudgetView.over

    def tracked_over(shared: ExecutionContext, lease: RowLease) -> BindingBudgetView:
        view = over(shared, lease)
        views.append((view, lease.capacity))
        return view

    monkeypatch.setattr(BindingBudgetView, "over", staticmethod(tracked_over))
    shared_deltas: list[int] = []
    shared_adjust = ExecutionContext.adjust_buffered_rows

    async def tracked_shared(self: ExecutionContext, delta: int) -> None:
        shared_deltas.append(delta)
        await shared_adjust(self, delta)

    monkeypatch.setattr(ExecutionContext, "adjust_buffered_rows", tracked_shared)
    view_adjust = BindingBudgetView.adjust_buffered_rows
    per_adjust: list[int] = []

    async def tracked_view(self: BindingBudgetView, delta: int) -> None:
        before = len(shared_deltas)
        await view_adjust(self, delta)
        per_adjust.append(len(shared_deltas) - before)

    monkeypatch.setattr(BindingBudgetView, "adjust_buffered_rows", tracked_view)
    engine_contexts: list[object] = []
    engine_execute = wave_engine.SharedWaveEngine.execute_requests

    async def tracked_engine(self: object, requests: tuple[Request, ...], **options: object) -> object:
        engine_contexts.append(options["context"])
        return await engine_execute(self, requests, **options)  # type: ignore[arg-type]

    monkeypatch.setattr(wave_engine.SharedWaveEngine, "execute_requests", tracked_engine)
    balances: list[tuple[int, int]] = []
    execute_wave = KeysetRuntime.execute_wave

    async def tracked_wave(self: KeysetRuntime, plans: tuple[object, ...]) -> object:
        receipts = await execute_wave(self, plans)  # type: ignore[arg-type]
        context = cast("BindingBudgetView", self.context)
        balances.append((self.transactions.buffer_balance, context.lease.held))
        return receipts

    monkeypatch.setattr(KeysetRuntime, "execute_wave", tracked_wave)

    portal = BatchPortal(_answer({"p0": DISTRIBUTIONS["skewed"], "p1": DISTRIBUTIONS["dense"]}))
    async with client_for(portal) as client:
        outcomes, _report = await _drain(
            client.iter_reference_outcomes(_base(), [_parent("p0"), _parent("p1")], traversal=_traversal(execution))
        )

    assert set(_completes(outcomes)) == {"p0", "p1"}
    assert len(views) == 2
    _assert_only_bounded_plans_trim(views, _completes(outcomes))
    for view, _granted in views:
        shared_policy = view.shared.policy
        differing = {
            item.name
            for item in dataclasses.fields(shared_policy)
            if getattr(view.policy, item.name) != getattr(shared_policy, item.name)
        }
        assert differing <= {"max_buffered_rows"}
        assert view.execution_context is view.shared
        assert view.lease.held == 0
    assert per_adjust
    assert set(per_adjust) == {1}
    assert sum(shared_deltas) == 0
    assert engine_contexts
    assert all(type(context) is ExecutionContext for context in engine_contexts)
    assert all(context is views[0][0].shared for context in engine_contexts)
    assert balances
    assert all(balance == held for balance, held in balances)


def _assert_only_bounded_plans_trim(
    views: list[tuple[BindingBudgetView, int]],
    completes: Mapping[object, ReferenceComplete[object]],
) -> None:
    """A plan that bounds the rows it can still hold trims its lease (#33); a lane plan keeps the grant."""
    bounded = sum(
        complete.keyset_execution is not None
        and complete.keyset_execution.selected_kind
        in {KeysetExecutionKind.SEQUENTIAL, KeysetExecutionKind.BOUNDARY_ONLY}
        for complete in completes.values()
    )
    assert sum(view.lease.capacity < granted for view, granted in views) == bounded
    for view, granted in views:
        assert view.policy.max_buffered_rows == view.lease.capacity <= granted
        assert view.lease.capacity in {granted, PAGE}


def test_keyset_context_names_exactly_the_members_fast_keyset_uses() -> None:
    from b24api.traversal import keyset_context  # noqa: PLC0415 - focused protocol import

    members = {name for name in vars(keyset_context.KeysetContext) if not name.startswith("_")}
    assert members == {
        "policy",
        "execution_context",
        "start",
        "snapshot",
        "reserve_page",
        "reserve_pages",
        "commit_page",
        "release_page",
        "adjust_buffered_rows",
    }


# A27-RETAIN / A27-WAVE1: a fixed lease; the wave fit and the charge use the same capacity.


def _shared(rows: int = 2500) -> ExecutionContext:
    from b24api.execution.rate import RateCoordinator  # noqa: PLC0415 - one focused import

    return ExecutionContext(ExecutionPolicy(max_buffered_rows=rows), RateCoordinator(max_concurrency=1))


@pytest.mark.parametrize(("held", "fitted"), [(40, 1), (30, 2)])
@pytest.mark.asyncio
async def test_retained_rows_shrink_the_fitted_wave_and_the_charge_equals_the_fit(held: int, fitted: int) -> None:
    lease = RowLease(50)
    view: KeysetContext = BindingBudgetView.over(_shared(), lease)
    await view.adjust_buffered_rows(held)
    candidates = ("first", "second")
    wave = fit_wave(
        candidates,
        reserves=(10, 10),
        commands=2,
        rows=view.policy.max_buffered_rows - lease.held,
    )
    assert len(wave) == fitted
    await view.adjust_buffered_rows(10 * len(wave))
    assert lease.held == held + 10 * len(wave) <= lease.capacity == 50
    await view.adjust_buffered_rows(-lease.held)
    assert lease.held == 0
    assert lease.capacity == 50


@pytest.mark.asyncio
async def test_one_free_page_runs_two_candidates_as_two_atomic_one_command_waves() -> None:
    lease = RowLease(30)
    view = BindingBudgetView.over(_shared(), lease)
    await view.adjust_buffered_rows(20)  # the planning barrier retains two pages; one page (C) is free
    remaining: tuple[str, ...] = ("a", "b")
    waves: list[tuple[str, ...]] = []
    while remaining:
        wave = fit_wave(remaining, reserves=(10,) * len(remaining), commands=2, rows=30 - lease.held)
        assert wave
        await view.adjust_buffered_rows(10)
        waves.append(wave)
        await view.adjust_buffered_rows(-10)
        remaining = remaining[len(wave) :]
    assert waves == [("a",), ("b",)]
    with pytest.raises(Exception, match="row lease"):
        await view.adjust_buffered_rows(11)
    assert lease.held == 20


@pytest.mark.asyncio
async def test_a_fixed_lease_returns_to_the_pool_and_its_neighbour_exhausts(leases: _Leases) -> None:
    portal = BatchPortal(_answer({"fast": DISTRIBUTIONS["dense"], "plain": tuple(range(1, 31))}))
    policy = ExecutionPolicy(max_buffered_rows=40)
    async with client_for(portal, policy=policy) as client:
        outcomes, _report = await _drain(
            client.iter_reference_source_outcomes(
                [
                    ReferenceSource(_base(), _traversal(_range()), _parent("fast")),
                    ReferenceSource(_base(), _traversal(SequentialKeysetExecution()), _parent("plain")),
                ]
            )
        )
    rows = _rows_by_parent(outcomes)
    assert rows["fast"] == sorted(DISTRIBUTIONS["dense"])
    assert rows["plain"] == list(range(1, 31))
    assert set(_completes(outcomes)) == {"fast", "plain"}
    assert [amount for _index, amount in leases.granted] == [30]
    assert leases.outstanding == 0


# A27-GRANT-LOW / A27-GRANT-OK: AUTO below the floor runs exclusively; a sufficient pool runs both.


@pytest.mark.parametrize("order", [DeliveryOrder.READY, DeliveryOrder.INPUT])
@pytest.mark.asyncio
async def test_auto_below_the_floor_runs_exclusively_and_matches_the_flat_oracle(
    order: DeliveryOrder,
    leases: _Leases,
) -> None:
    ids = DISTRIBUTIONS["dense"]
    portal = BatchPortal(_answer({"p0": ids, "p1": ids}))
    policy = ExecutionPolicy(max_buffered_rows=30)  # C=10, A=4: P=34 > B=30
    async with client_for(portal, policy=policy) as client:
        outcomes, _report = await asyncio.wait_for(
            _drain(
                client.iter_reference_outcomes(
                    _base(),
                    [_parent("p0"), _parent("p1")],
                    traversal=_traversal(_auto()),
                    dispatch=BatchDispatch(output_order=order),
                )
            ),
            10,
        )
    assert leases.granted == [(0, 30), (1, 30)]
    assert leases.high_water == 30
    assert leases.outstanding == 0
    completes = _completes(outcomes)
    assert set(completes) == {"p0", "p1"}
    oracle = await _flat_oracle(ids, _auto(), 30)
    for complete in completes.values():
        assert complete.keyset_execution is not None
        assert _cost(complete.keyset_execution) == _cost(oracle)


@pytest.mark.asyncio
async def test_a_sufficient_pool_runs_two_auto_bindings_to_the_flat_oracle(leases: _Leases) -> None:
    ids = DISTRIBUTIONS["dense"]
    portal = BatchPortal(_answer({"p0": ids, "p1": ids}))
    policy = ExecutionPolicy(max_buffered_rows=200, max_buffered_commands=50)
    async with client_for(portal, policy=policy) as client:
        outcomes, report = await _drain(
            client.iter_reference_outcomes(_base(), [_parent("p0"), _parent("p1")], traversal=_traversal(_auto()))
        )
    rows = _rows_by_parent(outcomes)
    assert sum(len(values) for values in rows.values()) == 400
    completes = _completes(outcomes)
    assert len(completes) == 2
    assert all(complete.exhausted for complete in completes.values())
    grants = dict(leases.granted)
    for index, name in enumerate(("p0", "p1")):
        evidence = completes[name].keyset_execution
        assert evidence is not None
        assert _cost(evidence) == _cost(await _flat_oracle(ids, _auto(), grants[index]))
    assert leases.outstanding == 0
    assert report.buffered_rows_high_water <= 200


# A27-INPUT-TIGHT / A27-INPUT-OK: partitioned lanes beside an ordinary source in INPUT order.


@pytest.mark.asyncio
async def test_partitioned_below_its_floor_fails_finitely_before_any_request() -> None:
    portal = BatchPortal(_answer({"p0": DISTRIBUTIONS["dense"]}))
    policy = ExecutionPolicy(max_buffered_rows=30)  # C=10, lanes 8: P=38 > B=30
    dispatch = BatchDispatch(output_order=DeliveryOrder.INPUT)
    async with client_for(portal, policy=policy) as client:
        with pytest.raises(CapabilityError):
            client.iter_reference_outcomes(
                _base(), [_parent("p0"), _parent("p1")], traversal=_traversal(_partitioned(8)), dispatch=dispatch
            )
        outcomes, _report = await asyncio.wait_for(
            _drain(
                client.iter_reference_source_outcomes(
                    [ReferenceSource(_base(), _traversal(_partitioned(8)), _parent("p0"))], dispatch=dispatch
                )
            ),
            10,
        )
    assert [type(outcome) for outcome in outcomes] == [ReferenceNotExecuted]
    assert portal.waves == []


@pytest.mark.asyncio
async def test_two_partitioned_sources_and_an_ordinary_one_exhaust_in_input_order(leases: _Leases) -> None:
    data = {"a": tuple(range(1, 151)), "b": tuple(range(1001, 1151)), "plain": tuple(range(1, 31))}
    portal = BatchPortal(_answer(data))
    policy = ExecutionPolicy(max_buffered_rows=120)
    async with client_for(portal, policy=policy) as client:
        outcomes, report = await asyncio.wait_for(
            _drain(
                client.iter_reference_source_outcomes(
                    [
                        ReferenceSource(_base(), _traversal(_partitioned(8)), _parent("a")),
                        ReferenceSource(_base(), _traversal(_partitioned(8)), _parent("b")),
                        ReferenceSource(_base(), _traversal(SequentialKeysetExecution()), _parent("plain")),
                    ],
                    dispatch=BatchDispatch(output_order=DeliveryOrder.INPUT),
                )
            ),
            10,
        )
    order = [outcome.correlation for outcome in outcomes]
    rank = {"a": 0, "b": 1, "plain": 2}
    assert order == sorted(order, key=lambda name: rank[cast("str", name)])
    rows = _rows_by_parent(outcomes)
    assert {name: rows[name] for name in data} == {name: sorted(ids) for name, ids in data.items()}
    assert sum(len(values) for values in rows.values()) == 330
    assert set(_completes(outcomes)) == set(data)
    assert report.state is TerminalState.COMPLETED
    assert leases.outstanding == 0
