"""Whole-page durable acknowledgement and bounded-prefix stop via public API."""

from __future__ import annotations
import json

import pytest

from b24api import (
    Bitrix24,
    CallerStop,
    ContinuePage,
    CursorDomain,
    CursorSpec,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    PageBoundary,
    ParameterPath,
    Request,
    ResultSelector,
    RouteKind,
    SequentialKeysetExecution,
    TerminalState,
    TraversalAssurance,
)
from b24api.contracts.completion import PageAcknowledged, PageDelivered, PageScheduled
from b24api.execution import Executor, WireResponse


class ListTransport:
    """Three distinct wire families over an independent five-row oracle."""

    host = "fixture.invalid"

    def __init__(self) -> None:
        """Track physical requests per run."""
        self.requests: list[Request] = []

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Return the requested page without altering caller stop decisions."""
        assert attempt_timeout > 0
        assert max_response_bytes > 0
        self.requests.append(request)
        parameters = request.copy_parameters()
        if request.method == "offset.list":
            start = parameters.get("start", 0)
            assert isinstance(start, int)
            rows = [{"id": value} for value in (1, 2, 3, 4, 5)[start : start + 2]]
            payload = {"result": rows, "next": start + 2 if rows else None}
        elif request.method == "keyset.list":
            controls = parameters.get("filter", {})
            assert isinstance(controls, dict)
            rows = [{"id": value} for value in (1, 2, 3, 4, 5) if value > controls.get(">ID", 0)][:2]
            payload = {"result": rows}
        else:
            control = parameters.get("LAST_ID", 6)
            assert isinstance(control, int)
            rows = [{"id": value} for value in (5, 4, 3, 2, 1) if value < control][:2]
            payload = {"result": rows}
        return WireResponse(200, (), json.dumps(payload).encode())


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
async def test_page_stop_prevents_next_request_and_reports_bounded_prefix(family: str) -> None:
    transport = ListTransport()
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
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


def test_counted_batch_tail_rejects_page_stop_at_construction() -> None:
    transport = ListTransport()
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
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
async def test_continue_policy_preserves_natural_exhaustion() -> None:
    transport = ListTransport()
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    stream = client.iter_list(
        Request("offset.list", route=RouteKind.BARE),
        page_size=2,
        page_stop=ContinueAfterCommit(),
    )
    assert [row["id"] async for row in stream] == [1, 2, 3, 4, 5]
    assert stream.report is not None
    assert stream.report.exhausted


@pytest.mark.asyncio
async def test_page_stop_callback_runs_between_delivery_and_acknowledgement() -> None:
    order: list[str] = []

    class OrderedStop:
        def on_page(self, _boundary: PageBoundary) -> CallerStop:
            order.append("callback")
            return CallerStop("committed")

    client = Bitrix24._from_executor(Executor(ListTransport()))  # noqa: SLF001 - deterministic facade seam
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
async def test_page_stop_callback_failure_does_not_acknowledge_or_schedule_next_page() -> None:
    class FailingStop:
        def on_page(self, _boundary: PageBoundary) -> ContinuePage:
            raise RuntimeError("commit failed")

    client = Bitrix24._from_executor(Executor(ListTransport()))  # noqa: SLF001 - deterministic facade seam
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
async def test_default_auto_keyset_uses_sequential_path_for_page_stop() -> None:
    transport = ListTransport()
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
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
