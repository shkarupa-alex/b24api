"""White-box budget ledger: accounting a page or a row costs O(1) whatever the number of bindings.

The execution context keeps plain counters and one page entry per open reference binding. The
validated ``BudgetCounters`` value is built only for a snapshot, and a closed binding's entry is
dropped, so neither the number of value constructions nor the ledger grows with the bindings.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

import pytest

from b24api import (
    BatchDispatch,
    Binding,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    SequentialTraversal,
)
from b24api.contracts import ReferenceItem
from b24api.contracts.policy import BudgetCounters
from b24api.errors import BudgetExceededError
from b24api.execution import ExecutionContext, Executor, RateCoordinator
from tests.scripting import BatchPortal, client_for

if TYPE_CHECKING:
    from tests.scripting import Envelope, Query

PAGE = 2
ROWS_PER_BINDING = 3
FEW_BINDINGS = 8
MANY_BINDINGS = 40
SNAPSHOTS_PER_OPERATION = 2


def _answer(method: str, query: Query) -> Envelope:
    del method
    offset = int(query.get("start", "0"))
    rows = [{"ID": index} for index in range(offset + 1, min(offset + PAGE, ROWS_PER_BINDING) + 1)]
    envelope: Envelope = {"result": rows, "total": ROWS_PER_BINDING}
    if offset + PAGE < ROWS_PER_BINDING:
        envelope["next"] = offset + PAGE
    return envelope


async def _stream_references(count: int, monkeypatch: pytest.MonkeyPatch) -> list[ExecutionContext]:
    """Read ``count`` two-page bindings to the end and return every ledger the operation opened."""
    contexts: list[ExecutionContext] = []
    create = Executor.context

    def recorded(executor: Executor, policy: ExecutionPolicy | None = None) -> ExecutionContext:
        contexts.append(create(executor, policy))
        return contexts[-1]

    monkeypatch.setattr(Executor, "context", recorded)
    policy = ExecutionPolicy(max_pages_per_reference=ROWS_PER_BINDING)
    async with client_for(BatchPortal(_answer), policy=policy) as client:
        stream = client.iter_references(
            Request.bare("offset.list", {}, ReplaySafety.SAFE),
            [
                Binding(f"p{index}", (ParameterUpdate(ParameterPath(("PARENT",)), index),), index)
                for index in range(count)
            ],
            traversal=SequentialTraversal(
                identity=IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_INTEGER),
                page_size=PAGE,
            ),
            dispatch=BatchDispatch(batch_size=50, coalesce_wait=0.0),
        )
        async with stream:
            rows = [item async for item in stream if isinstance(item, ReferenceItem)]
    assert len(rows) == count * ROWS_PER_BINDING
    return contexts


@pytest.mark.asyncio
async def test_budget_counters_are_built_per_snapshot_not_per_binding_page_or_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validate = BudgetCounters.__post_init__
    built: list[int] = []

    def counted(counters: BudgetCounters) -> None:
        built.append(len(counters.pages_per_reference))
        validate(counters)

    monkeypatch.setattr(BudgetCounters, "__post_init__", counted)
    await _stream_references(FEW_BINDINGS, monkeypatch)
    few = list(built)
    built.clear()
    contexts = await _stream_references(MANY_BINDINGS, monkeypatch)

    # Reservation, settlement and the row buffer update the ledger in place: only the report's
    # snapshot builds the validated value, so five times the bindings build exactly as many.
    assert built == few
    assert len(built) <= SNAPSHOTS_PER_OPERATION
    # Every binding closed its ledger entry, so neither a snapshot nor the ledger holds one per binding.
    assert not any(built)
    assert [context._reference_pages for context in contexts] == [{}] * len(contexts)  # noqa: SLF001 - open bindings only


@pytest.mark.asyncio
async def test_a_closed_reference_keeps_its_ceiling_until_its_page_in_flight_settles() -> None:
    context = ExecutionContext(ExecutionPolicy(max_pages=4, max_pages_per_reference=2), RateCoordinator())
    context.commit_page(await context.reserve_page(reference="A"))
    in_flight = await context.reserve_page(reference="A")
    assert not context.can_reserve_page(reference="A")

    context.close_reference("A")
    # The page in flight still holds the reference's ceiling and the operation's page budget.
    assert not context.can_reserve_page(reference="A")
    assert (await context.snapshot()).counters.pages_per_reference == (("A", 1),)

    context.commit_page(in_flight)
    snapshot = await context.snapshot()
    assert snapshot.counters.logical_pages == 2  # noqa: PLR2004 - both pages of the closed binding
    assert snapshot.counters.pages_per_reference == ()
    assert context._reference_pages == {}  # noqa: SLF001 - the settled entry is dropped

    released = await context.reserve_page(reference="B")
    context.release_page(released)
    assert context._reference_pages == {}  # noqa: SLF001 - an entry that never committed is dropped
    context.commit_page(await context.reserve_page(reference="B"))
    context.commit_page(await context.reserve_page(reference="B"))
    with pytest.raises(BudgetExceededError, match="logical page budget exhausted"):
        await context.reserve_page(reference="C")
