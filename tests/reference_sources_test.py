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
from tests.scripting import (
    DEADLINE_BUDGET,
    HANG_BOUND,
    UNREACHABLE_COALESCE_WAIT,
    BatchPortal,
    Envelope,
    Query,
    VirtualClock,
    assert_fired_near_deadline,
    deadline_started,
    on_loop,
)

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


async def _drain(stream: OperationStream[ReferenceOutcome[int]]) -> list[ReferenceOutcome[int]]:
    return [event async for event in stream]


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
    portal = BatchPortal(_answer)
    async with _client(portal) as api:
        # On the event loop: a worker-thread pull's timing would decide whether the second source's
        # head reaches the first wave.
        events, report = await _collect(
            api,
            on_loop((_alpha(), _beta())),
            dispatch=BatchDispatch(output_order=order, coalesce_wait=UNREACHABLE_COALESCE_WAIT),
        )

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


@pytest.mark.asyncio
async def test_a28_late_sources_activate_before_the_counted_source_finishes() -> None:
    # Ordered by events, not by wall time: the late sources arrive once the counted head is
    # answered and the first tail batch is in flight, and that batch is held until they have queued
    # their first pages behind it. Their next pages then leave with tail pages the counted source
    # still has to send, so a batch carries both sources while the counted one is unfinished.
    tail_started = asyncio.Event()
    late_yielded = asyncio.Event()

    async def answer(method: str, query: Query) -> Envelope:
        tail = method == "gamma.list" and query.get("start", "0") != "0"
        if tail and not tail_started.is_set():
            tail_started.set()
            async with asyncio.timeout(HANG_BOUND):
                await late_yielded.wait()
            # Activating a source is in-loop work only, so loop turns, not time, bound it.
            for _turn in range(_ACTIVATION_TURNS):
                await asyncio.sleep(0)
        return _answer(method, query)

    async def sources() -> AsyncIterator[ReferenceSource[int]]:
        yield _gamma()
        async with asyncio.timeout(HANG_BOUND):
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


@pytest.mark.parametrize("turns", [3, 64, 1000])
@pytest.mark.asyncio
async def test_a28_late_pages_wait_for_a_counted_tail_on_its_way_to_the_queue(
    monkeypatch: pytest.MonkeyPatch, turns: int
) -> None:
    """Late pages queued while the counted head is answered leave with the tail its answer starts.

    The counted source stops being runnable as soon as its tail window is full, one or two loop turns
    before those tail pages reach the queue; while they are on the way they still count as potential.
    """
    VirtualClock(turns).install(monkeypatch)
    head_sent = asyncio.Event()
    late_yielded = asyncio.Event()

    async def answer(method: str, query: Query) -> Envelope:
        if method == "gamma.list" and not head_sent.is_set():
            head_sent.set()
            async with asyncio.timeout(HANG_BOUND):
                await late_yielded.wait()
            for _turn in range(_ACTIVATION_TURNS):
                await asyncio.sleep(0)
        return _answer(method, query)

    async def sources() -> AsyncIterator[ReferenceSource[int]]:
        yield _gamma()
        async with asyncio.timeout(HANG_BOUND):
            await head_sent.wait()
        for parent in (2, 3, 4):
            yield _beta(parent)
        late_yielded.set()

    portal = BatchPortal(answer)
    async with _client(portal) as api:
        _events, report = await _collect(api, sources(), dispatch=BatchDispatch(batch_size=10))

    assert report.state is TerminalState.COMPLETED
    # The head travels alone; the late first pages then leave with the first tail pages, not before them.
    assert portal.methods[0] == ("gamma.list",)
    assert sorted(set(portal.methods[1])) == ["beta.list", "gamma.list"], portal.methods
    assert portal.methods[1].count("beta.list") == 3  # noqa: PLR2004


@pytest.mark.asyncio
async def test_a28_a_binding_just_pulled_from_the_source_keeps_its_wave_open(monkeypatch: pytest.MonkeyPatch) -> None:
    """A binding counts as potential from the moment it leaves the source, not only once its task first runs.

    With every activation slot taken, the producer waits for one right after the last pull: the binding
    is no pending pull any more, and its task has not run yet. The first wave still carries every binding.
    """
    clock = VirtualClock().install(monkeypatch)
    portal = BatchPortal(_answer)
    async with _client(portal) as api:
        _events, report = await _collect(
            api,
            on_loop([_beta(parent) for parent in (1, 2, 3)]),
            dispatch=BatchDispatch(batch_size=3),
            policy=ExecutionPolicy(max_active_references=3),
        )

    assert report.state is TerminalState.COMPLETED
    assert portal.waves == [3, 3]
    assert clock.timer_firings == 0


@pytest.mark.asyncio
async def test_a28_a_binding_refused_at_its_pull_never_holds_a_wave_open(monkeypatch: pytest.MonkeyPatch) -> None:
    """A local non-execution is never a producer: the waves of its neighbours leave without waiting for it."""
    clock = VirtualClock().install(monkeypatch)
    portal = BatchPortal(_answer)
    refused = ReferenceSource(
        Request.bare("beta.list", {}, ReplaySafety.SAFE), BETA, Binding("seeded", (), 5, start_cursor=3)
    )
    async with _client(portal) as api:
        events, report = await _collect(
            api,
            on_loop([_beta(1), refused, _beta(3)]),
            dispatch=BatchDispatch(batch_size=3),
            policy=ExecutionPolicy(max_active_references=3),
        )

    assert [event.binding_index for event in events if isinstance(event, ReferenceNotExecuted)] == [1]
    assert report.state is TerminalState.COMPLETED_WITH_FAILURES
    assert portal.waves == [2, 2]
    assert clock.timer_firings == 0


# The counted fixture's tail is 20 pages: one window of 10, then 10 pages its consumer frees one by one.
_TAIL_WAVES = {
    (DeliveryOrder.READY, 1): [1, 10, 10],
    (DeliveryOrder.INPUT, 1): [1, 10, 10],
    (DeliveryOrder.READY, 3): [3, 10, 10, 10, 10, 10, 10],
    # Under INPUT a pull in flight is not waited for, so the first head leaves alone; the next head
    # leaves with the one already pulled. Every tail still leaves in full batches.
    (DeliveryOrder.INPUT, 3): [1, 2, 10, 10, 10, 10, 10, 10],
}


@pytest.mark.parametrize(("order", "sources"), list(_TAIL_WAVES))
@pytest.mark.asyncio
async def test_a28_a_draining_consumer_refills_the_counted_tail_in_full_batches(
    monkeypatch: pytest.MonkeyPatch, order: DeliveryOrder, sources: int
) -> None:
    """Pages freed one per consumed receipt coalesce instead of leaving one per batch.

    The tail's next page waits only for its consumer to move past a receipt it holds, so the
    dispatcher waits for it, as for an ordinary binding awaiting its consumer, with no timer firing.
    """
    clock = VirtualClock().install(monkeypatch)
    portal = BatchPortal(_answer)
    async with _client(portal) as api:
        events, report = await _collect(
            api,
            on_loop([_gamma(parent) for parent in range(1, sources + 1)]),
            dispatch=BatchDispatch(batch_size=10, output_order=order),
        )

    assert report.state is TerminalState.COMPLETED
    assert all(_rows(events, index) == list(range(1001)) for index in range(sources))
    assert [len(wave) for wave in portal.methods] == _TAIL_WAVES[order, sources]
    assert clock.timer_firings == 0


_DELTA_ROWS = 120
_DELTA_PAGE = 2
_DELTA_REFUSED_PAGE = 10


def _delta(_method: str, query: Query) -> Envelope:
    """Answer 120 rows per parent in pages of 2; parent 1 is refused its tenth tail page."""
    parent, offset = int(query["PARENT"]), int(query.get("start", "0"))
    if parent == 1 and offset == _DELTA_REFUSED_PAGE * _DELTA_PAGE:
        return {"error": "ACCESS_DENIED", "error_description": "denied"}
    envelope: Envelope = {
        "result": [{"ID": row} for row in range(offset, min(offset + _DELTA_PAGE, _DELTA_ROWS))],
        "total": _DELTA_ROWS,
    }
    if offset + _DELTA_PAGE < _DELTA_ROWS:
        envelope["next"] = offset + _DELTA_PAGE
    return envelope


@pytest.mark.asyncio
async def test_a28_a_wave_still_leaves_with_the_pages_a_failed_binding_abandoned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A binding refused mid-tail abandons its queued pages while their wave coalesces with other bindings' pages.

    The wave still leaves, without the abandoned pages' ended deadlines, and every other binding completes.
    """
    VirtualClock().install(monkeypatch)
    portal = BatchPortal(_delta)
    async with _client(portal) as api:
        stream = api.iter_reference_outcomes(
            Request.bare("delta.list", {}, ReplaySafety.SAFE),
            on_loop([_binding(parent) for parent in (1, 2, 3)]),
            traversal=CountedTraversal(page_size=_DELTA_PAGE),
            dispatch=BatchDispatch(concurrency=3),
        )
        async with stream:
            # A stranded wave would hang the stream; the bound only turns that into a failure.
            events = await asyncio.wait_for(_drain(stream), timeout=HANG_BOUND)

    terminals = {event.binding_index: type(event) for event in events if not isinstance(event, ReferenceItem)}
    assert terminals == {0: ReferenceFailure, 1: ReferenceComplete, 2: ReferenceComplete}
    assert _rows(events, 1) == _rows(events, 2) == list(range(_DELTA_ROWS))


@pytest.mark.parametrize("order", [DeliveryOrder.READY, DeliveryOrder.INPUT])
@pytest.mark.asyncio
async def test_a28_a_tail_window_filling_the_row_pool_refills_from_the_rows_its_consumer_returns(
    monkeypatch: pytest.MonkeyPatch, order: DeliveryOrder
) -> None:
    """A window that holds the whole pool frees no rows until the consumer moves on; its refill still coalesces."""
    clock = VirtualClock().install(monkeypatch)
    portal = BatchPortal(_answer)
    async with _client(portal) as api:
        events, report = await _collect(
            api,
            on_loop([_gamma()]),
            dispatch=BatchDispatch(batch_size=50, output_order=order),
            policy=ExecutionPolicy(max_buffered_rows=500),
        )

    assert report.state is TerminalState.COMPLETED
    assert _rows(events, 0) == list(range(1001))
    assert [len(wave) for wave in portal.methods] == [1, 10, 10]
    assert clock.timer_firings == 0


@pytest.mark.parametrize("order", [DeliveryOrder.READY, DeliveryOrder.INPUT])
@pytest.mark.asyncio
async def test_a28_a_tail_whose_consumer_waits_for_the_chunk_is_not_waited_for(
    monkeypatch: pytest.MonkeyPatch, order: DeliveryOrder
) -> None:
    """A two-page window filling the row pool: refills pair up, and no chunk waits out its deadline.

    While both window pages wait in a chunk, the consumer waits for them and frees no slot, so that
    chunk leaves at once; each later refill reserves the rows its held receipt returns. With a single
    binding the row gate alone already keeps the first case from waiting, so this pins the outcome.
    """
    clock = VirtualClock().install(monkeypatch)
    portal = BatchPortal(_answer)
    async with _client(portal) as api:
        events, report = await _collect(
            api,
            on_loop([_gamma()]),
            dispatch=BatchDispatch(batch_size=10, output_order=order),
            policy=ExecutionPolicy(max_buffered_rows=100),
        )

    assert report.state is TerminalState.COMPLETED
    assert _rows(events, 0) == list(range(1001))
    assert [len(wave) for wave in portal.methods] == [1] + [2] * 10
    assert clock.timer_firings == 0


@pytest.mark.asyncio
async def test_a28_a_slow_tail_consumer_bounds_each_refill_wait_by_the_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A consumer slower than the coalescing deadline costs at most one deadline per batch, never a stall."""
    clock = VirtualClock(turns=3).install(monkeypatch)
    portal = BatchPortal(_answer)
    events: list[ReferenceOutcome[int]] = []
    async with _client(portal) as api:
        stream = api.iter_reference_source_outcomes(on_loop([_gamma()]), dispatch=BatchDispatch(batch_size=10))
        async with stream:
            async for event in stream:
                events.append(event)
                for _turn in range(10):
                    await asyncio.sleep(0)

    assert _report(stream).state is TerminalState.COMPLETED
    assert _rows(events, 0) == list(range(1001))
    assert 0 < clock.timer_firings <= len(portal.methods)


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
        budget_started = deadline_started()
        stream = api.iter_reference_source_outcomes(source, policy=ExecutionPolicy(max_elapsed=DEADLINE_BUDGET))
        events: list[ReferenceOutcome[int]] = []
        with pytest.raises(BudgetExceededError, match="reference input exceeded operation time budget"):
            async with asyncio.timeout(HANG_BOUND), stream:
                events.extend([event async for event in stream])
        assert_fired_near_deadline(budget_started)
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
    # Both runs' waves close by size or by the end of input, so the comparison never rides on a timer.
    dispatch = BatchDispatch(coalesce_wait=UNREACHABLE_COALESCE_WAIT)

    homogeneous = BatchPortal(answer)
    async with _client(homogeneous) as api:
        stream = api.iter_reference_outcomes(
            request, on_loop([_binding(parent) for parent in parents]), traversal=traversal, dispatch=dispatch
        )
        async with stream:
            expected = [event async for event in stream]
    expected_report = _report(stream)

    heterogeneous = BatchPortal(answer)
    async with _client(heterogeneous) as api:
        events, report = await _collect(
            api, on_loop([ReferenceSource(request, traversal, _binding(p)) for p in parents]), dispatch=dispatch
        )

    assert events == expected
    assert heterogeneous.waves == homogeneous.waves
    assert heterogeneous.queries == homogeneous.queries
    assert report.page_trace == expected_report.page_trace
    assert (report.physical_requests, report.logical_pages, report.state) == (
        expected_report.physical_requests,
        expected_report.logical_pages,
        expected_report.state,
    )
