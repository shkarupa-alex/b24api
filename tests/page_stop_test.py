"""Whole-page durable acknowledgement and bounded-prefix stop via public API."""

from __future__ import annotations
from typing import TYPE_CHECKING

import pytest

from b24api import (
    Bitrix24,
    CursorSpec,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    ParameterPath,
    Request,
    ResultSelector,
    RouteKind,
    SequentialKeysetExecution,
    Settings,
    TerminalState,
    TraversalAssurance,
)
from b24api.contracts import (
    CallerStop,
    ConsistencyPolicy,
    ContinuePage,
    CursorDomain,
    KeysetExecutionKind,
    KeysetSelectionReason,
    KeysetSelectionSummary,
    PageBoundary,
)
from b24api.contracts.completion import PageAcknowledged, PageDelivered, PageScheduled
from b24api.contracts.policy import SnapshotRequirement
from tests.scripting import ResponderTransport, client_for

if TYPE_CHECKING:
    from tests.scripting import ClientFactory


def _five_rows(request: Request) -> object:
    """Three distinct wire families over an independent five-row oracle."""
    parameters = request.copy_parameters()
    if request.method == "offset.list":
        start = parameters.get("start", 0)
        assert isinstance(start, int)
        rows = [{"id": value} for value in (1, 2, 3, 4, 5)[start : start + 2]]
        return {"result": rows, "next": start + 2 if rows else None}
    if request.method == "keyset.list":
        controls = parameters.get("filter", {})
        assert isinstance(controls, dict)
        return {"result": [{"id": value} for value in (1, 2, 3, 4, 5) if value > controls.get(">ID", 0)][:2]}
    control = parameters.get("LAST_ID", 6)
    assert isinstance(control, int)
    return {"result": [{"id": value} for value in (5, 4, 3, 2, 1) if value < control][:2]}


class StopAfterCommit:
    """Persist the whole immutable page before requesting a per-binding stop."""

    def __init__(self) -> None:
        """Keep a keyed application oracle and observed page boundaries."""
        self.rows: dict[int, object] = {}
        self.boundaries: list[PageBoundary] = []

    async def on_page(self, boundary: PageBoundary) -> CallerStop:
        """Commit the page and return a bounded stop decision."""
        self.boundaries.append(boundary)
        for row in boundary.rows:
            assert isinstance(row, dict | tuple) or hasattr(row, "__getitem__")
            self.rows[row["id"]] = row
        return CallerStop("cutoff reached")


def _identity() -> IdentitySpec:
    return IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER)


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["offset", "keyset", "cursor"])
async def test_page_stop_prevents_next_request_and_reports_bounded_prefix(
    family: str, scripted_client: ClientFactory
) -> None:
    transport = ResponderTransport(_five_rows)
    client = scripted_client(transport)
    policy = StopAfterCommit()
    if family == "offset":
        stream = client.iter_list(Request("offset.list", route=RouteKind.BARE), page_size=2, page_stop=policy)
        expected = [1, 2]
    elif family == "keyset":
        stream = client.iter_list_keyset(
            Request("keyset.list", route=RouteKind.BARE),
            selector=ResultSelector.root(),
            identity=_identity(),
            page_size=2,
            keyset=KeysetSpec(),
            execution=SequentialKeysetExecution(),
            page_stop=policy,
        )
        expected = [1, 2]
    else:
        cursor = CursorSpec(
            ParameterPath(("LAST_ID",)),
            ("id",),
            IdentityCoercion.EXACT_INTEGER,
            "descending",
            "last",
            domain=CursorDomain.EXCLUSIVE_POSITIVE_INTEGER,
        )
        stream = client.iter_list_cursor(
            Request("cursor.list", {"LAST_ID": 6}, route=RouteKind.BARE),
            selector=ResultSelector.root(),
            cursor=cursor,
            page_size=2,
            page_stop=policy,
        )
        expected = [5, 4]
    assert [row["id"] async for row in stream] == expected
    assert list(policy.rows) == expected
    assert len(policy.boundaries) == len(transport.requests) == 1
    assert policy.boundaries[0].record.rows_admitted == len(expected)
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.assurance is TraversalAssurance.BOUNDED_PREFIX
    assert not stream.report.exhausted
    assert stream.report.partial
    assert stream.report.terminal_reason == "cutoff reached"


@pytest.mark.asyncio
async def test_incomplete_caller_stop_downgrades_assurance(scripted_client: ClientFactory) -> None:
    transport = ResponderTransport(_five_rows)
    client = scripted_client(transport)
    frozen = ExecutionPolicy(consistency=ConsistencyPolicy(snapshot_requirement=SnapshotRequirement.FROZEN_MANIFEST))
    stream = client.iter_list(
        Request("offset.list", route=RouteKind.BARE),
        page_size=2,
        page_stop=StopAfterCommit(),
        policy=frozen,
    )
    assert [row["id"] async for row in stream] == [1, 2]
    assert len(transport.requests) == 1
    assert stream.report is not None
    assert stream.report.state is TerminalState.INCOMPLETE
    assert not stream.report.exhausted
    assert stream.report.assurance is TraversalAssurance.MECHANICS_ONLY


def test_counted_batch_tail_rejects_page_stop_at_construction() -> None:
    transport = ResponderTransport(_five_rows)
    client = client_for(transport)
    with pytest.raises(ValueError, match="counted physical batch tail"):
        client.iter_list_counted(Request("offset.list", route=RouteKind.BARE), page_stop=StopAfterCommit())
    assert transport.requests == []


class ContinueAfterCommit:
    """Acknowledge pages while letting the source exhaust naturally."""

    def on_page(self, boundary: PageBoundary) -> ContinuePage:
        """Prove page boundaries are observed without stopping."""
        assert boundary.record.rows_admitted == len(boundary.rows)
        return ContinuePage.CONTINUE


@pytest.mark.asyncio
async def test_continue_policy_preserves_natural_exhaustion(scripted_client: ClientFactory) -> None:
    transport = ResponderTransport(_five_rows)
    client = scripted_client(transport)
    stream = client.iter_list(
        Request("offset.list", route=RouteKind.BARE),
        page_size=2,
        page_stop=ContinueAfterCommit(),
    )
    assert [row["id"] async for row in stream] == [1, 2, 3, 4, 5]
    assert stream.report is not None
    assert stream.report.exhausted


@pytest.mark.asyncio
async def test_page_stop_callback_runs_between_delivery_and_acknowledgement(scripted_client: ClientFactory) -> None:
    order: list[str] = []

    class OrderedStop:
        def on_page(self, _boundary: PageBoundary) -> CallerStop:
            order.append("callback")
            return CallerStop("committed")

    client = scripted_client(ResponderTransport(_five_rows))
    stream = client.iter_list(Request("offset.list", route=RouteKind.BARE), page_size=2, page_stop=OrderedStop())
    gate = stream._source.completion_gate  # noqa: SLF001 - observe the actual kernel gate
    original_emit = gate.emit

    def observe(event: object) -> None:
        if isinstance(event, PageDelivered):
            order.append("delivered")
        elif isinstance(event, PageAcknowledged):
            order.append("acknowledged")
        original_emit(event)

    gate.emit = observe
    assert [row["id"] async for row in stream] == [1, 2]
    assert order == ["delivered", "callback", "acknowledged"]


@pytest.mark.asyncio
async def test_page_stop_callback_failure_does_not_acknowledge_or_schedule_next_page(
    scripted_client: ClientFactory,
) -> None:
    class FailingStop:
        def on_page(self, _boundary: PageBoundary) -> ContinuePage:
            raise RuntimeError("commit failed")

    client = scripted_client(ResponderTransport(_five_rows))
    stream = client.iter_list(Request("offset.list", route=RouteKind.BARE), page_size=2, page_stop=FailingStop())
    gate = stream._source.completion_gate  # noqa: SLF001 - observe the actual kernel gate
    events: list[type[object]] = []
    original_emit = gate.emit

    def observe(event: object) -> None:
        events.append(type(event))
        original_emit(event)

    gate.emit = observe
    with pytest.raises(RuntimeError, match="commit failed"):
        _ = [row async for row in stream]
    assert events.count(PageScheduled) == 1
    assert PageDelivered in events
    assert PageAcknowledged not in events


@pytest.mark.asyncio
async def test_default_auto_keyset_uses_sequential_path_for_page_stop(scripted_client: ClientFactory) -> None:
    transport = ResponderTransport(_five_rows)
    client = scripted_client(transport)
    stream = client.iter_list_keyset(
        Request("keyset.list", route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=2,
        page_stop=StopAfterCommit(),
    )
    assert [row["id"] async for row in stream] == [1, 2]
    assert len(transport.requests) == 1
    assert stream.report is not None
    assert not stream.report.exhausted
    # The compact summary says why AUTO ran sequentially; the fast-only report stays absent.
    assert stream.report.keyset_selection == KeysetSelectionSummary(
        KeysetExecutionKind.AUTO,
        KeysetExecutionKind.SEQUENTIAL,
        KeysetSelectionReason.PAGE_STOP,
    )
    assert stream.report.keyset_execution is None


@pytest.mark.asyncio
async def test_explicit_sequential_keyset_reports_its_selection() -> None:
    client = Bitrix24(
        Settings(webhook_url="https://fixture.invalid/rest/1/stop/"), transport=ResponderTransport(_five_rows)
    )
    stream = client.iter_list_keyset(
        Request("keyset.list", route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=2,
        execution=SequentialKeysetExecution(),
    )
    assert [row["id"] async for row in stream] == [1, 2, 3, 4, 5]
    assert stream.report is not None
    assert stream.report.keyset_selection == KeysetSelectionSummary(
        KeysetExecutionKind.SEQUENTIAL,
        KeysetExecutionKind.SEQUENTIAL,
        KeysetSelectionReason.EXPLICIT_SEQUENTIAL,
    )
