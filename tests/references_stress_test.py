"""Seeded stress of heterogeneous reference sources on one shared queue and row pool.

Every scenario mixes sequential offset, counted, cursor, sequential keyset and fast/automatic keyset
sources with random sizes, row budgets, batch sizes and delivery orders. Each run must end within a
bounded time, deliver every completed source's rows exactly as its own oracle orders them, return
every fast row lease and leave no buffered row behind. The only tolerated failure is a finite
``BudgetExceededError`` of a fast keyset source whose row pool cannot hold its planning floor.
"""

# ruff: noqa: PLR2004

from __future__ import annotations
import asyncio
import random
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import pytest

from b24api import (
    AutoKeysetExecution,
    BatchDispatch,
    Binding,
    CountedTraversal,
    CursorSpec,
    CursorTraversal,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    KeysetTraversal,
    ParameterPath,
    ParameterUpdate,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    ReplaySafety,
    Request,
    ResultSelector,
    SequentialKeysetExecution,
    SequentialTraversal,
    StableIntegerKeysetContract,
    TerminalState,
)
from b24api.contracts import (
    DeliveryOrder,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ReferenceNotExecuted,
    ReferenceSource,
)
from b24api.errors import BudgetExceededError
from b24api.execution.context import ExecutionContext
from b24api.references.dispatch import _RowBuffer
from tests.scripting import BatchPortal, client_for

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.contracts import ReferenceOutcome, TraversalSpec
    from tests.scripting import Envelope, Query

PAGE = 10
SEEDS = 100
FAST_KINDS = frozenset({"range", "partitioned", "auto"})
KINDS = ("offset", "counted", "cursor", "sequential_keyset", *sorted(FAST_KINDS))
_CONTRACT = StableIntegerKeysetContract()
_UPPER = ParameterPath(("ID",))
_KEY = ("ID",)
_PARENT = ParameterPath(("PARENT",))


@dataclass(frozen=True)
class _Source:
    kind: str
    parent: str
    ids: tuple[int, ...]

    @property
    def expected(self) -> list[int]:
        """Return the rows this source's traversal must deliver, in its delivery order."""
        return sorted(self.ids, reverse=self.kind == "cursor")


def _ids(rng: random.Random) -> tuple[int, ...]:
    size = rng.choice((0, 1, rng.randint(2, 40), rng.randint(41, 250)))
    step = rng.choice((1, 1, 7, 113))
    start = rng.randint(1, 500)
    return tuple(start + index * step for index in range(size))


def _scenario(seed: int) -> tuple[list[_Source], ExecutionPolicy, BatchDispatch]:
    rng = random.Random(seed)
    sources = [_Source(rng.choice(KINDS), f"p{index}", _ids(rng)) for index in range(rng.randint(2, 10))]
    policy = ExecutionPolicy(
        max_buffered_rows=rng.choice((40, 120, 400, 2_500)),
        max_pages=100_000,
        max_requests=100_000,
    )
    dispatch = BatchDispatch(
        batch_size=rng.choice((2, 7, 25, 50)),
        output_order=rng.choice((DeliveryOrder.READY, DeliveryOrder.INPUT)),
    )
    return sources, policy, dispatch


def _page(rows: list[int]) -> list[dict[str, int]]:
    return [{"ID": value} for value in rows]


def _portal(sources: list[_Source]) -> Callable[[str, Query], Envelope]:
    data = {source.parent: source.ids for source in sources}

    def answer(method: str, query: Query) -> Envelope:
        ids = data[query["PARENT"]]
        if method == "keyset.list":
            selected = [value for value in ids if value > int(query.get("filter[>ID]", "-1"))]
            if "filter[<ID]" in query:
                selected = [value for value in selected if value < int(query["filter[<ID]"])]
            selected.sort(reverse=query.get("order[ID]") == "DESC")
            return {"result": _page(selected[: int(query.get("limit", PAGE))])}
        if method == "cursor.list":
            before = query.get("BEFORE")
            selected = sorted((value for value in ids if before is None or value < int(before)), reverse=True)
            return {"result": _page(selected[:PAGE])}
        offset = int(query.get("start", "0"))
        ordered = sorted(ids)
        envelope: Envelope = {"result": _page(ordered[offset : offset + PAGE]), "total": len(ordered)}
        if offset + PAGE < len(ordered):
            envelope["next"] = offset + PAGE
        return envelope

    return answer


def _identity() -> IdentitySpec:
    return IdentitySpec(_KEY, "ID", "ID", IdentityCoercion.EXACT_INTEGER)


def _keyset(execution: object) -> KeysetTraversal:
    return KeysetTraversal(
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=cast("RangeKeysetExecution", execution),
    )


def _cursor() -> CursorTraversal:
    cursor = CursorSpec(ParameterPath(("BEFORE",)), _KEY, IdentityCoercion.EXACT_INTEGER, "descending", "last")
    return CursorTraversal(selector=ResultSelector(), cursor=cursor, page_size=PAGE)


_TRAVERSALS: dict[str, tuple[str, Callable[[], TraversalSpec]]] = {
    "offset": ("offset.list", lambda: SequentialTraversal(identity=_identity(), page_size=PAGE)),
    "counted": ("offset.list", lambda: CountedTraversal(identity=_identity(), page_size=PAGE)),
    "cursor": ("cursor.list", _cursor),
    "sequential_keyset": ("keyset.list", lambda: _keyset(SequentialKeysetExecution())),
    "range": ("keyset.list", lambda: _keyset(RangeKeysetExecution(_CONTRACT))),
    "partitioned": ("keyset.list", lambda: _keyset(PartitionedKeysetExecution(_CONTRACT, target_lanes=4))),
    "auto": ("keyset.list", lambda: _keyset(AutoKeysetExecution(_CONTRACT, target_lanes=4))),
}


def _traversal(kind: str) -> tuple[str, TraversalSpec]:
    method, traversal = _TRAVERSALS[kind]
    return method, traversal()


def _reference(source: _Source) -> ReferenceSource[str]:
    method, traversal = _traversal(source.kind)
    binding = Binding(source.parent, (ParameterUpdate(ParameterPath(("PARENT",)), source.parent),), source.parent)
    return ReferenceSource(Request.bare(method, {}, ReplaySafety.SAFE), traversal, binding)


@dataclass
class _Pool:
    """Spy on every row lease granted and on the shared buffered-row counter."""

    leases: list[object] = field(default_factory=list)
    contexts: list[ExecutionContext] = field(default_factory=list)

    @property
    def outstanding(self) -> int:
        return sum(getattr(lease, "amount", 0) for lease in self.leases)


@pytest.fixture
def pool(monkeypatch: pytest.MonkeyPatch) -> _Pool:
    spy = _Pool()
    lease = _RowBuffer.lease
    start = ExecutionContext.start

    async def tracked_lease(self: _RowBuffer, index: int, grant: Callable[[int], int | None]) -> object:
        reservation = await lease(self, index, grant)
        spy.leases.append(reservation)
        return reservation

    async def tracked_start(self: ExecutionContext) -> None:
        if self not in spy.contexts:
            spy.contexts.append(self)
        await start(self)

    monkeypatch.setattr(_RowBuffer, "lease", tracked_lease)
    monkeypatch.setattr(ExecutionContext, "start", tracked_start)
    return spy


@pytest.mark.parametrize("seed", range(SEEDS))
@pytest.mark.asyncio
async def test_mixed_reference_sources_complete_exactly_and_return_every_row(seed: int, pool: _Pool) -> None:
    sources, policy, dispatch = _scenario(seed)
    portal = BatchPortal(_portal(sources))
    async with client_for(portal, policy=policy) as client:
        stream = client.iter_reference_source_outcomes([_reference(source) for source in sources], dispatch=dispatch)

        async def drain() -> list[ReferenceOutcome[str]]:
            async with stream:
                return [event async for event in stream]

        events = await asyncio.wait_for(drain(), timeout=60)
    report = stream.report
    assert report is not None

    rows: dict[str, list[int]] = {source.parent: [] for source in sources}
    terminals: dict[str, ReferenceOutcome[str]] = {}
    for event in events:
        if isinstance(event, ReferenceItem):
            rows[event.correlation].append(cast("dict[str, int]", event.item)["ID"])
        else:
            assert event.correlation not in terminals, "one terminal event per source"
            terminals[event.correlation] = event

    failed = 0
    for source in sources:
        terminal = terminals[source.parent]
        assert not isinstance(terminal, ReferenceNotExecuted), (source.kind, terminal)
        if isinstance(terminal, ReferenceComplete):
            assert rows[source.parent] == source.expected, source.kind
            assert terminal.row_count == len(source.ids)
            continue
        assert isinstance(terminal, ReferenceFailure), terminal
        # Only a fast keyset source may fail, and only finitely on a row pool below its planning floor.
        assert source.kind in FAST_KINDS, (source.kind, terminal.error)
        assert isinstance(terminal.error, BudgetExceededError), terminal.error
        assert policy.max_buffered_rows < 400
        assert set(rows[source.parent]) <= set(source.ids)
        assert len(set(rows[source.parent])) == len(rows[source.parent])
        failed += 1

    assert report.state is (TerminalState.COMPLETED_WITH_FAILURES if failed else TerminalState.COMPLETED)
    assert pool.outstanding == 0
    assert pool.contexts
    assert [(await context.snapshot()).counters.buffered_rows for context in pool.contexts] == [0] * len(pool.contexts)
    assert all(wave <= dispatch.batch_size for wave in portal.waves)


def test_the_stress_matrix_exercises_every_kind_budget_and_order() -> None:
    scenarios = [_scenario(seed) for seed in range(SEEDS)]
    assert {source.kind for sources, _, _ in scenarios for source in sources} == set(KINDS)
    assert {policy.max_buffered_rows for _, policy, _ in scenarios} == {40, 120, 400, 2_500}
    assert {dispatch.output_order for _, _, dispatch in scenarios} == set(DeliveryOrder)


@pytest.mark.asyncio
async def test_a_short_frontier_page_keeps_its_unused_rows_for_the_frontier() -> None:
    """INPUT order: rows a short frontier page returns stay reserved for the frontier's next page.

    Bindings behind the frontier hold accepted pages until it advances. When the frontier's page
    came back short, its unused rows used to be taken by those bindings, leaving the frontier unable
    to reserve its confirmation page while every holder waited on it.
    """
    sizes = (10, 11, 1, 11, 10)
    sources = [_Source("cursor", f"p{index}", tuple(range(1, size + 1))) for index, size in enumerate(sizes)]
    portal = BatchPortal(_portal(sources))
    policy = ExecutionPolicy(max_buffered_rows=20)
    async with client_for(portal, policy=policy) as client:
        stream = client.iter_references(
            Request.bare("cursor.list", {}, ReplaySafety.SAFE),
            [Binding(source.parent, (ParameterUpdate(_PARENT, source.parent),), source.parent) for source in sources],
            traversal=_cursor(),
            dispatch=BatchDispatch(batch_size=2, output_order=DeliveryOrder.INPUT),
        )

        async def drain() -> list[ReferenceOutcome[str]]:
            async with stream:
                return [event async for event in stream]

        events = await asyncio.wait_for(drain(), timeout=10)
    delivered = [
        (event.correlation, cast("dict[str, int]", event.item)["ID"])
        for event in events
        if isinstance(event, ReferenceItem)
    ]
    assert delivered == [(source.parent, value) for source in sources for value in source.expected]
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED


@pytest.mark.asyncio
async def test_a_binding_waiting_for_its_lease_never_holds_a_wave_back_to_coalesce() -> None:
    """A fast binding parked until a lease frees cannot add a command, so waves do not wait for it.

    The first range binding holds the pool's only floor-sized lease and runs hundreds of
    one-command waves; with a one-second coalescing window, waiting on the parked neighbour would
    take minutes instead of milliseconds.
    """
    sources = [_Source("range", "p0", tuple(range(123, 123 + 113 * 50, 113))), _Source("range", "p1", ())]
    portal = BatchPortal(_portal(sources))
    async with client_for(portal, policy=ExecutionPolicy(max_buffered_rows=40)) as client:
        stream = client.iter_reference_source_outcomes(
            [_reference(source) for source in sources],
            dispatch=BatchDispatch(batch_size=25, coalesce_wait=1.0),
        )

        async def drain() -> list[ReferenceOutcome[str]]:
            async with stream:
                return [event async for event in stream]

        events = await asyncio.wait_for(drain(), timeout=10)
    completes = {event.correlation: event.row_count for event in events if isinstance(event, ReferenceComplete)}
    assert completes == {"p0": 50, "p1": 0}
    assert len(portal.waves) > 100
