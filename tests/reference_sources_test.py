"""Heterogeneous reference sources in one operation (#28): A28, A28-LATE, A28-MATRIX, A28-PENDING, AHOMO."""

from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import pytest

from b24api import (
    BatchDispatch,
    Binding,
    Bitrix24,
    CountedTraversal,
    CursorSpec,
    CursorTraversal,
    DirectDispatch,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    KeysetTraversal,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    ResultSelector,
    SequentialTraversal,
    Settings,
    TerminalState,
)
from b24api.contracts import (
    BoundedIdentityRange,
    CallerStop,
    DeliveryOrder,
    NotExecutedReason,
    PageBoundary,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ReferenceNotExecuted,
    ReferenceSource,
    RequestHeaders,
)
from b24api.contracts.request import RouteKind
from b24api.errors import BudgetExceededError, CapabilityError, InputSourceError, ReferenceFailed
from b24api.transport import TransportCapabilities, WireResponse
from tests.scripting import BatchPortal, Envelope, Query

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from b24api.contracts import OperationReport, OperationStream, ReferenceOutcome, TraversalSpec
    from b24api.transport import WireRequest

ID = IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_INTEGER)
CURSOR = CursorSpec(ParameterPath(("BEFORE",)), ("ID",), IdentityCoercion.EXACT_INTEGER, "descending", "last")
ALPHA = CursorTraversal(selector=ResultSelector(), cursor=CURSOR, page_size=2)
_LIMIT = ParameterPath(("limit",))
BETA = SequentialTraversal(identity=ID, page_size=10)
ALPHA_PAGES = {None: [4, 3], 3: [2, 1], 1: []}


def _client(portal: BatchPortal) -> Bitrix24:
    return Bitrix24(Settings(webhook_url="https://fixture.invalid/rest/1/token/"), transport=portal)


def _report(stream: OperationStream[Any]) -> OperationReport:
    report = stream.report
    assert report is not None
    return report


def _binding(parent: int) -> Binding[int]:
    return Binding("parent", (ParameterUpdate(ParameterPath(("PARENT",)), parent),), parent)


def _answer(method: str, query: Query) -> Envelope:
    if method == "alpha.list":
        before = query.get("BEFORE")
        return {"result": [{"ID": value} for value in ALPHA_PAGES[None if before is None else int(before)]]}
    if method == "beta.list":
        # The same identity 4 as alpha's first row: sources never share identity state.
        return {"result": [{"ID": 4}] if int(query.get("start", "0")) == 0 else []}
    if method == "gamma.list":
        offset = int(query.get("start", "0"))
        rows = [{"ID": row} for row in range(offset, min(offset + 50, 1001))]
        envelope: Envelope = {"result": rows, "total": 1001}
        if offset + 50 < 1001:  # noqa: PLR2004 - the counted fixture's size
            envelope["next"] = offset + 50
        return envelope
    raise AssertionError(method)


def _alpha(parent: int = 17) -> ReferenceSource[int]:
    return ReferenceSource(Request.bare("alpha.list", {}, ReplaySafety.SAFE), ALPHA, _binding(parent))


def _beta(parent: int = 17) -> ReferenceSource[int]:
    return ReferenceSource(Request.bare("beta.list", {}, ReplaySafety.SAFE), BETA, _binding(parent))


def _gamma(parent: int = 1) -> ReferenceSource[int]:
    return ReferenceSource(Request.bare("gamma.list", {}, ReplaySafety.SAFE), CountedTraversal(), _binding(parent))


async def _collect(
    api: Bitrix24,
    sources: object,
    *,
    dispatch: BatchDispatch | DirectDispatch | None = None,
    policy: ExecutionPolicy | None = None,
) -> tuple[list[ReferenceOutcome[int]], OperationReport]:
    stream = api.iter_reference_source_outcomes(
        cast("list[ReferenceSource[int]]", sources),
        dispatch=dispatch or BatchDispatch(),
        policy=policy,
    )
    async with stream:
        events = [event async for event in stream]
    return events, _report(stream)


def _rows(events: list[ReferenceOutcome[int]], index: int) -> list[object]:
    return [
        event.item["ID"] if isinstance(event.item, dict) else None
        for event in events
        if isinstance(event, ReferenceItem) and event.binding_index == index
    ]


# --- A28 ----------------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("order", [DeliveryOrder.READY, DeliveryOrder.INPUT])
async def test_a28_different_templates_share_the_dispatcher_and_keep_separate_state(order: DeliveryOrder) -> None:
    # An async source is read on the event loop; a list is pulled in a worker thread, whose timing
    # would decide whether the second source's head reaches the first wave.
    async def sources() -> AsyncIterator[ReferenceSource[int]]:
        yield _alpha()
        yield _beta()

    portal = BatchPortal(_answer)
    async with _client(portal) as api:
        events, report = await _collect(api, sources(), dispatch=BatchDispatch(output_order=order))

    if order is DeliveryOrder.READY:
        # Pages of both methods coalesce into shared physical batches. Under INPUT, as on the homogeneous
        # facade, a wave forms only from pages ready at the same moment, so sharing one is opportunistic.
        assert any(sorted(wave) == ["alpha.list", "beta.list"] for wave in portal.methods)
        assert portal.waves == [2, 2, 1]
    assert _rows(events, 0) == [4, 3, 2, 1]
    assert _rows(events, 1) == [4]
    completes = [event for event in events if isinstance(event, ReferenceComplete)]
    assert sorted((event.binding_index, event.correlation, event.row_count) for event in completes) == [
        (0, 17, 4),
        (1, 17, 1),
    ]
    assert report.state is TerminalState.COMPLETED
    assert report.exhausted
    assert report.operation == "iter_reference_source_outcomes"
    if order is DeliveryOrder.INPUT:
        indexes = [event.binding_index for event in events]
        assert indexes == sorted(indexes)


@pytest.mark.asyncio
async def test_a28_fail_fast_facade_yields_reference_events() -> None:
    portal = BatchPortal(_answer)
    async with _client(portal) as api:
        stream = api.iter_reference_sources([_alpha(), _beta()])
        async with stream:
            events = [event async for event in stream]
    assert sum(isinstance(event, ReferenceComplete) for event in events) == 2  # noqa: PLR2004
    assert _report(stream).operation == "iter_reference_sources"


# --- A28-LATE -----------------------------------------------------------------------------------

_ACTIVATION_TURNS = 200
_EVENT_TIMEOUT = 5.0


@pytest.mark.asyncio
async def test_a28_late_sources_activate_before_the_counted_source_finishes() -> None:
    # Ordered by events, not by wall time: the late sources arrive once the counted head is
    # answered and the first tail page is in flight, and that batch is held until they have queued
    # their first pages behind it, so the next batch finds both sources' pages waiting.
    tail_started = asyncio.Event()
    late_yielded = asyncio.Event()

    async def answer(method: str, query: Query) -> Envelope:
        tail = method == "gamma.list" and query.get("start", "0") != "0"
        if tail and not tail_started.is_set():
            tail_started.set()
            async with asyncio.timeout(_EVENT_TIMEOUT):
                await late_yielded.wait()
            # Activating a source is in-loop work only, so loop turns, not time, bound it.
            for _turn in range(_ACTIVATION_TURNS):
                await asyncio.sleep(0)
        return _answer(method, query)

    async def sources() -> AsyncIterator[ReferenceSource[int]]:
        yield _gamma()
        async with asyncio.timeout(_EVENT_TIMEOUT):
            await tail_started.wait()
        for parent in (2, 3, 4):
            yield _beta(parent)
        late_yielded.set()

    portal = BatchPortal(answer)
    async with _client(portal) as api:
        events, report = await _collect(api, sources(), dispatch=BatchDispatch(batch_size=10))

    mixed = [wave for wave in portal.methods if {"gamma.list", "beta.list"} <= set(wave)]
    assert mixed, portal.methods
    completes = {event.binding_index: event for event in events if isinstance(event, ReferenceComplete)}
    assert sorted(completes) == [0, 1, 2, 3]
    assert completes[0].row_count == 1001  # noqa: PLR2004
    assert all(event.exhausted for event in completes.values())
    # The late ordinary sources finished while the counted tail was still in flight.
    terminal_order = [event.binding_index for event in events if isinstance(event, ReferenceComplete)]
    assert terminal_order[-1] == 0
    assert report.state is TerminalState.COMPLETED


# --- A28-MATRIX ---------------------------------------------------------------------------------


class _Stop:
    def on_page(self, boundary: PageBoundary) -> CallerStop:
        del boundary
        return CallerStop("enough")


_BOUND = BoundedIdentityRange.capture(
    Request.bare("beta.list", {"filter": {}}, ReplaySafety.SAFE),
    filter_path=ParameterPath(("filter",)),
    upper_id=10,
    lower_exclusive=0,
    fence_path=ParameterPath(("filter", "<=ID")),
    source_version="qualified-test-v1",
)


def _incompatible() -> list[object]:
    request = Request.bare("beta.list", {}, ReplaySafety.SAFE)
    return [
        pytest.param(ReferenceSource(request, CountedTraversal(), _binding(5)), DirectDispatch(), id="counted-direct"),
        pytest.param(
            ReferenceSource(request, CountedTraversal(), _binding(5), page_stop=_Stop()),
            BatchDispatch(),
            id="counted-page-stop",
        ),
        pytest.param(
            ReferenceSource(
                request,
                KeysetTraversal(ResultSelector(), ID, keyset=KeysetSpec(limit_path=_LIMIT, boundary=_BOUND)),
                _binding(5),
            ),
            BatchDispatch(),
            id="keyset-bound",
        ),
        pytest.param(
            ReferenceSource(
                Request.bare("beta.list", {}, ReplaySafety.SAFE, headers=RequestHeaders({"X-Scope": "1"})),
                BETA,
                _binding(5),
            ),
            BatchDispatch(),
            id="batch-scoped-headers",
        ),
        pytest.param(
            ReferenceSource(request, BETA, Binding("seeded", (), 5, start_cursor=3)),
            BatchDispatch(),
            id="start-cursor-outside-cursor",
        ),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(("bad", "dispatch"), _incompatible())
async def test_a28_matrix_an_incompatible_source_is_a_correlated_local_non_execution(
    bad: ReferenceSource[int],
    dispatch: BatchDispatch | DirectDispatch,
) -> None:
    portal = BatchPortal(_answer)
    async with _client(portal) as api:
        events, report = await _collect(api, [_alpha(), bad, _beta(9)], dispatch=dispatch)

    not_executed = [event for event in events if isinstance(event, ReferenceNotExecuted)]
    assert [(event.binding_index, event.correlation, event.reason) for event in not_executed] == [
        (1, 5, NotExecutedReason.LOCAL_VALIDATION_FAILED)
    ]
    assert _rows(events, 0) == [4, 3, 2, 1]
    assert _rows(events, 2) == [4]
    assert all("PARENT" not in query or query["PARENT"] != "5" for query in portal.queries)
    assert report.state is TerminalState.COMPLETED_WITH_FAILURES


@pytest.mark.asyncio
async def test_a28_matrix_the_homogeneous_facade_keeps_its_preflight_rejection() -> None:
    portal = BatchPortal(_answer)
    async with _client(portal) as api:
        with pytest.raises(CapabilityError, match="requires BatchDispatch"):
            api.iter_references(
                Request.bare("gamma.list", {}, ReplaySafety.SAFE),
                [_binding(1)],
                traversal=CountedTraversal(),
                dispatch=DirectDispatch(),
            )
    assert portal.queries == []


@dataclass
class _WirePortal(BatchPortal):
    """A wire transport serving every route; ``json_array_body`` decides native v3 batch support."""

    v3_batch: bool = False
    v3_envelopes: int = 0

    @property
    def capabilities(self) -> TransportCapabilities:
        return TransportCapabilities(routes=frozenset(RouteKind), json_array_body=self.v3_batch)

    async def send_wire(self, request: WireRequest, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        if request.array_body is not None:
            self.v3_envelopes += 1
            return WireResponse(200, (("content-type", "application/json"),), b'{"result":[{"error":"scripted"}]}')
        canonical = Request(request.method, request.copy_parameters(), request.replay_safety, route=request.route)
        return await self.send(canonical, attempt_timeout=attempt_timeout, max_response_bytes=max_response_bytes)


def _v3_beta(parent: int) -> ReferenceSource[int]:
    return ReferenceSource(Request.v3("beta.list", {}, ReplaySafety.SAFE), BETA, _binding(parent))


@pytest.mark.asyncio
async def test_a28_matrix_a_v3_source_without_native_batch_support_is_not_executed() -> None:
    """S4/S6: a v3 source on a transport without JSON-array bodies is rejected before any reservation."""
    portal = _WirePortal(_answer)
    async with _client(portal) as api:
        events, report = await _collect(api, [_v3_beta(5), _beta(9)], dispatch=BatchDispatch())

    not_executed = [event for event in events if isinstance(event, ReferenceNotExecuted)]
    assert [(event.binding_index, event.correlation, event.reason) for event in not_executed] == [
        (0, 5, NotExecutedReason.LOCAL_VALIDATION_FAILED)
    ]
    assert not any(isinstance(event, ReferenceFailure) for event in events)
    assert _rows(events, 1) == [4]
    assert portal.v3_envelopes == 0
    assert all(query["PARENT"] != "5" for query in portal.queries)
    assert report.not_executed == 1
    assert report.failures == 0


@pytest.mark.asyncio
async def test_a28_matrix_the_homogeneous_facade_rejects_v3_without_native_batch_support() -> None:
    portal = _WirePortal(_answer)
    async with _client(portal) as api:
        with pytest.raises(CapabilityError, match="native REST v3 batch"):
            api.iter_references(
                Request.v3("beta.list", {}, ReplaySafety.SAFE), [_binding(5)], traversal=BETA, dispatch=BatchDispatch()
            )
    assert portal.queries == []
    assert portal.v3_envelopes == 0


@pytest.mark.asyncio
async def test_a28_matrix_a_v3_source_with_native_batch_support_is_admitted() -> None:
    portal = _WirePortal(_answer, v3_batch=True)
    async with _client(portal) as api:
        events, report = await _collect(api, [_v3_beta(5), _beta(9)], dispatch=BatchDispatch())

    assert not any(isinstance(event, ReferenceNotExecuted) for event in events)
    assert portal.v3_envelopes >= 1
    assert _rows(events, 1) == [4]
    # A wave mixing the classic and the v3 dialect sends one envelope per dialect, and each one counts.
    assert report.batch_requests == report.physical_requests == portal.v3_envelopes + len(portal.waves)


@pytest.mark.asyncio
async def test_a28_matrix_a_json_route_source_under_batch_is_not_executed() -> None:
    """S6: a source whose request no batch envelope carries is a local non-execution before reservation."""
    portal = _WirePortal(_answer)
    request = Request("beta.list", {}, ReplaySafety.SAFE, route=RouteKind.JSON)
    async with _client(portal) as api:
        events, report = await _collect(api, [ReferenceSource(request, BETA, _binding(5)), _beta(9)])

    not_executed = [event for event in events if isinstance(event, ReferenceNotExecuted)]
    assert [(event.binding_index, event.correlation, event.reason) for event in not_executed] == [
        (0, 5, NotExecutedReason.LOCAL_VALIDATION_FAILED)
    ]
    assert not any(isinstance(event, ReferenceFailure) for event in events)
    assert _rows(events, 1) == [4]
    assert all(query["PARENT"] != "5" for query in portal.queries)
    assert all(record.reference_index != 0 for record in report.page_trace)
    assert report.not_executed == 1
    assert report.failures == 0


@pytest.mark.asyncio
async def test_a28_matrix_fail_fast_rejects_a_json_route_source_before_any_request() -> None:
    portal = _WirePortal(_answer)
    request = Request("beta.list", {}, ReplaySafety.SAFE, route=RouteKind.JSON)
    async with _client(portal) as api:
        stream = api.iter_reference_sources([ReferenceSource(request, BETA, _binding(5)), _beta(9)])
        with pytest.raises(ReferenceFailed) as raised:
            async with stream:
                _ = [event async for event in stream]
    assert raised.value.report is not None
    assert raised.value.report.terminal_reason == "CapabilityError"
    assert portal.queries == []


@pytest.mark.asyncio
async def test_a28_matrix_one_failing_source_leaves_its_neighbours_running() -> None:
    def answer(method: str, query: Query) -> Envelope:
        if method == "beta.list" and query["PARENT"] == "8":
            return {"error": "ACCESS_DENIED", "error_description": "scripted"}
        return _answer(method, query)

    portal = BatchPortal(answer)
    async with _client(portal) as api:
        events, report = await _collect(api, [_alpha(), _beta(8), _beta(9)])

    failures = [event for event in events if isinstance(event, ReferenceFailure)]
    assert [(event.binding_index, event.correlation) for event in failures] == [(1, 8)]
    assert _rows(events, 0) == [4, 3, 2, 1]
    assert _rows(events, 2) == [4]
    assert report.state is TerminalState.COMPLETED_WITH_FAILURES


@pytest.mark.asyncio
@pytest.mark.parametrize("malformed", [_binding(1), "source", None], ids=["binding", "string", "none"])
async def test_a28_matrix_a_malformed_source_value_fails_the_input_source(malformed: object) -> None:
    portal = BatchPortal(_answer)
    async with _client(portal) as api:
        stream = api.iter_reference_source_outcomes(cast("list[ReferenceSource[int]]", [_alpha(), malformed]))
        with pytest.raises(InputSourceError):
            async with stream:
                _ = [event async for event in stream]
    assert _report(stream).state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_a28_matrix_a_raising_source_iterator_fails_the_input_source() -> None:
    async def sources() -> AsyncIterator[ReferenceSource[int]]:
        yield _alpha()
        raise RuntimeError("caller iterator broke")

    portal = BatchPortal(_answer)
    async with _client(portal) as api:
        stream = api.iter_reference_source_outcomes(sources())
        with pytest.raises(InputSourceError):
            async with stream:
                _ = [event async for event in stream]


def test_a28_matrix_reference_source_validates_its_parts() -> None:
    request = Request.bare("beta.list", {}, ReplaySafety.SAFE)
    with pytest.raises(TypeError, match="Request"):
        ReferenceSource("beta.list", BETA, _binding(1))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="TraversalSpec"):
        ReferenceSource(request, object(), _binding(1))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="Binding"):
        ReferenceSource(request, BETA, 1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="page_stop"):
        ReferenceSource(request, BETA, _binding(1), page_stop=object())  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        ReferenceSource(request, BETA, _binding(1), _Stop())  # type: ignore[misc]


@pytest.mark.asyncio
async def test_a28_matrix_page_stop_belongs_to_its_source() -> None:
    portal = BatchPortal(_answer)
    stopped = ReferenceSource(Request.bare("alpha.list", {}, ReplaySafety.SAFE), ALPHA, _binding(17), page_stop=_Stop())
    async with _client(portal) as api:
        events, report = await _collect(api, [stopped, _alpha(18)])

    completes = {event.binding_index: event for event in events if isinstance(event, ReferenceComplete)}
    assert (completes[0].exhausted, completes[0].stop_reason, completes[0].row_count) == (False, "enough", 2)
    assert (completes[1].exhausted, completes[1].row_count) == (True, 4)
    assert report.state is TerminalState.COMPLETED
    assert not report.exhausted


# --- A28-PENDING --------------------------------------------------------------------------------


class _PendingSource:
    """An async source whose second item never arrives; it counts concurrent pulls and closes."""

    def __init__(self) -> None:
        self.in_flight = 0
        self.max_in_flight = 0
        self.pulls = 0
        self.closes = 0
        self._yielded = False

    def __aiter__(self) -> _PendingSource:
        return self

    async def __anext__(self) -> ReferenceSource[int]:
        self.pulls += 1
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            if not self._yielded:
                self._yielded = True
                return _beta()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")
        finally:
            self.in_flight -= 1

    async def aclose(self) -> None:
        self.closes += 1


@pytest.mark.asyncio
async def test_a28_pending_holds_at_most_one_pull_and_closes_the_source_once_at_the_deadline() -> None:
    source = _PendingSource()
    portal = BatchPortal(_answer)
    async with _client(portal) as api:
        stream = api.iter_reference_source_outcomes(source, policy=ExecutionPolicy(max_elapsed=0.2))
        events: list[ReferenceOutcome[int]] = []
        with pytest.raises(BudgetExceededError, match="reference input exceeded operation time budget"):
            async with asyncio.timeout(5), stream:
                events.extend([event async for event in stream])
    assert source.max_in_flight == 1
    assert source.closes == 1
    assert _report(stream).state is not TerminalState.COMPLETED


# --- AHOMO --------------------------------------------------------------------------------------


def _sequential_answer(method: str, query: Query) -> Envelope:
    del method
    parent, offset = int(query["PARENT"]), int(query.get("start", "0"))
    rows = [{"ID": parent * 100 + row} for row in range(offset, min(offset + 2, 5))]
    return {"result": rows}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("traversal", "answer"),
    [
        (SequentialTraversal(identity=ID, page_size=2), _sequential_answer),
        (ALPHA, _answer),
    ],
    ids=["sequential", "cursor"],
)
async def test_ahomo_sources_of_one_template_trace_exactly_like_the_homogeneous_facade(
    traversal: TraversalSpec,
    answer: Callable[[str, Query], Envelope],
) -> None:
    method = "alpha.list" if traversal is ALPHA else "seq.list"
    request = Request.bare(method, {}, ReplaySafety.SAFE)
    parents = (1, 2, 3)

    homogeneous = BatchPortal(answer)
    async with _client(homogeneous) as api:
        stream = api.iter_reference_outcomes(request, [_binding(parent) for parent in parents], traversal=traversal)
        async with stream:
            expected = [event async for event in stream]
    expected_report = _report(stream)

    heterogeneous = BatchPortal(answer)
    async with _client(heterogeneous) as api:
        events, report = await _collect(api, [ReferenceSource(request, traversal, _binding(p)) for p in parents])

    assert events == expected
    assert heterogeneous.waves == homogeneous.waves
    assert heterogeneous.queries == homogeneous.queries
    assert report.page_trace == expected_report.page_trace
    assert (report.physical_requests, report.logical_pages, report.state) == (
        expected_report.physical_requests,
        expected_report.logical_pages,
        expected_report.state,
    )
