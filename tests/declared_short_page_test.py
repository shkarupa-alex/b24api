"""Opt-in fixed-step closure on a caller-declared short page, direct and through references."""

from __future__ import annotations
import ast
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlsplit

import pytest

from b24api import (
    BatchDispatch,
    Binding,
    Bitrix24,
    CountedTraversal,
    DirectDispatch,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    OffsetContinuation,
    OffsetSpec,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    SequentialTraversal,
    TerminalState,
    TotalTermination,
    TraversalAssurance,
)
from b24api.completion.closure import DECLARED_SHORT_PAGE_REACHED, qualified_closure
from b24api.completion.gate import CompletionGate
from b24api.contracts import (
    BindingAdmitted,
    BindingClosure,
    BindingTerminal,
    CallerStop,
    CleanupOutcome,
    CleanupState,
    CommandSettlement,
    ConsistencyPolicy,
    ContinuePage,
    OperationReport,
    PageAcknowledged,
    PageBoundary,
    PageCommandOutcome,
    PageDelivered,
    PageIndex,
    PageRejectionCode,
    PageScheduled,
    PageStride,
    PageValidated,
    RawTotalSource,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ShortPageTermination,
    SparseRawBound,
    StreamClosure,
    StreamTerminal,
)
from b24api.contracts.policy import ConfirmationPolicy, DuplicatePolicy, SnapshotRequirement, TotalSemantics
from b24api.encoding import encode_php_query
from b24api.errors import CapabilityError, IncompleteTraversalError, PaginationError
from b24api.traversal.offset_rules import sequential_offset_plan
from b24api.traversal.plans import OffsetSequentialPlan, OffsetTerminalRule
from tests.scripting import ResponderTransport, client_for

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.contracts import OperationStream

    type Envelope = dict[str, object]
    type Page = Callable[[dict[str, str]], Envelope]
    # One test drives the direct and the reference stream through the same assertions.
    type _AnyStream = OperationStream[Any]

WIDTH = 50
FULL_THEN_SHORT = 73
SHORT = 23
STRIDE = PageStride(server_granularity=WIDTH, wire_increment=WIDTH, max_decoded_rows=WIDTH)
DECLARED = OffsetSpec(
    continuation=OffsetContinuation.FIXED_STEP,
    step=WIDTH,
    page_stride=STRIDE,
    short_page_termination=ShortPageTermination.DECLARED_TERMINAL,
)
SELECTOR = ResultSelector(("booking",))
CONTRADICTED = 4


class _Portal:
    """Serve one offset oracle to direct pages and physical batch commands alike.

    ``queries`` holds every logical page as its decoded PHP query, in the order the portal answered it,
    so direct and batched dispatch are compared on the same wire vocabulary.
    """

    def __init__(self, page: Page) -> None:
        self._page = page
        self.queries: list[dict[str, str]] = []
        self.physical = 0

    def _answer(self, query: str) -> Envelope:
        decoded = {key: values[0] for key, values in parse_qs(query, keep_blank_values=True).items()}
        self.queries.append(decoded)
        return self._page(decoded)

    def __call__(self, request: Request) -> Envelope:
        self.physical += 1
        parameters = request.copy_parameters()
        if request.method != "batch":
            query: dict[str | int, object] = dict(parameters.items())
            return self._answer(encode_php_query(query))
        commands = parameters["cmd"]
        assert isinstance(commands, dict)
        envelopes = {key: self._answer(urlsplit(str(command)).query) for key, command in commands.items()}
        return {
            "result": {
                "result": {key: value["result"] for key, value in envelopes.items()},
                "result_error": {},
                "result_total": {key: value["total"] for key, value in envelopes.items() if "total" in value},
                "result_next": {key: value["next"] for key, value in envelopes.items() if "next" in value},
            },
        }

    @property
    def starts(self) -> list[int]:
        return [int(query.get("start", "-1")) for query in self.queries]


def _booking(sizes: dict[int, int], *, next_values: dict[int, int] | None = None, base: int = 0) -> Page:
    """Answer each ``start`` with its declared row count and the endpoint's constant zero total."""

    def page(query: dict[str, str]) -> Envelope:
        start = int(query["start"])
        rows = [{"id": base + start + index + 1} for index in range(sizes[start])]
        envelope: Envelope = {"result": {"booking": rows, "totalCount": 0}, "total": 0}
        if next_values is not None and start in next_values:
            envelope["next"] = next_values[start]
        return envelope

    return page


def _client(portal: _Portal, *, policy: ExecutionPolicy | None = None) -> Bitrix24:
    return client_for(ResponderTransport(portal), policy=policy)


def _request(parameters: dict[str, object] | None = None) -> Request:
    return Request("booking.v1.booking.list", parameters or {}, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)


async def _drain[T](stream: OperationStream[T]) -> tuple[list[T], BaseException | None]:
    items: list[T] = []
    try:
        async with stream:
            async for item in stream:
                items.append(item)  # noqa: PERF401 - keep the prefix delivered before a failure
    except (CapabilityError, IncompleteTraversalError, PaginationError) as error:
        return items, error
    return items, None


async def _refusal[T](build: Callable[[], OperationStream[T]]) -> BaseException | None:
    """Return the error a stream raises at construction or on its first pull."""
    try:
        stream = build()
    except CapabilityError as error:
        return error
    return (await _drain(stream))[1]


def _terminals(monkeypatch: pytest.MonkeyPatch) -> list[BindingTerminal]:
    """Record every binding terminal the completion gates receive, without retaining any row."""
    terminals: list[BindingTerminal] = []
    emit = CompletionGate.emit

    def spy(gate: CompletionGate, event: object) -> None:
        if isinstance(event, BindingTerminal):
            terminals.append(event)
        emit(gate, event)  # type: ignore[arg-type]

    monkeypatch.setattr(CompletionGate, "emit", spy)
    return terminals


# A16.1 and the issue's own configuration: without the opt-in a short page still fails closed.


@pytest.mark.asyncio
async def test_issue_configuration_without_opt_in_still_refuses_a_short_page() -> None:
    portal = _Portal(_booking({0: SHORT}))
    async with _client(portal) as client:
        items, error = await _drain(
            client.iter_list(
                _request(),
                selector=SELECTOR,
                page_size=WIDTH,
                offset=OffsetSpec(continuation=OffsetContinuation.FIXED_STEP, step=WIDTH),
            )
        )

    assert isinstance(error, IncompleteTraversalError)
    assert "cannot prove closure after a short page" in str(error.__cause__)
    assert len(items) == SHORT
    assert portal.starts == [0]


@pytest.mark.asyncio
async def test_page_stride_without_opt_in_still_rejects_an_unexplained_short_page() -> None:
    portal = _Portal(_booking({0: WIDTH, WIDTH: SHORT}))
    async with _client(portal) as client:
        stream = client.iter_list(
            _request(),
            selector=SELECTOR,
            page_size=WIDTH,
            offset=OffsetSpec(continuation=OffsetContinuation.FIXED_STEP, step=WIDTH, page_stride=STRIDE),
        )
        items, error = await _drain(stream)

    assert isinstance(error, IncompleteTraversalError)
    assert "unexplained short page" in str(error.__cause__)
    assert len(items) == WIDTH
    assert stream.report is not None
    assert not stream.report.exhausted


# A16.2 - A16.4: the strict opt-in matrix over full, short and empty pages.


@pytest.mark.parametrize(
    ("sizes", "reason"),
    [
        ({0: WIDTH, WIDTH: SHORT}, DECLARED_SHORT_PAGE_REACHED),
        ({0: SHORT}, DECLARED_SHORT_PAGE_REACHED),
        ({0: WIDTH, WIDTH: 0}, "empty page confirmed terminal"),
        ({0: 0}, "empty page confirmed terminal"),
    ],
)
@pytest.mark.parametrize("identity", [None, IdentitySpec(("id",), "id", "id", IdentityCoercion.EXACT_INTEGER)])
@pytest.mark.asyncio
async def test_declared_short_page_closes_without_a_further_request(
    sizes: dict[int, int],
    reason: str,
    identity: IdentitySpec | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    terminals = _terminals(monkeypatch)
    portal = _Portal(_booking(sizes))
    async with _client(portal) as client:
        stream = client.iter_list(_request(), selector=SELECTOR, identity=identity, page_size=WIDTH, offset=DECLARED)
        items, error = await _drain(stream)

    assert error is None
    assert len(items) == sum(sizes.values())
    # Every window is requested exactly once; nothing follows the closing page.
    assert portal.starts == sorted(sizes)
    report = stream.report
    assert report is not None
    assert report.successful
    assert report.exhausted
    assert report.terminal_reason == reason
    # A declared short page proves the endpoint's stop rule, never identity-exact completeness.
    assert report.assurance is TraversalAssurance.MECHANICS_ONLY
    [terminal] = terminals
    declared = reason == DECLARED_SHORT_PAGE_REACHED
    assert terminal.closure is (BindingClosure.DECLARED_SHORT_PAGE if declared else BindingClosure.SOURCE_EMPTY)
    assert terminal.declared_short_page_width == (WIDTH if declared else None)


# A16.5: a server continuation may agree with a full window; anything else contradicts the protocol.


@pytest.mark.asyncio
async def test_full_page_continuation_equal_to_the_fixed_step_adds_no_request() -> None:
    portal = _Portal(_booking({0: WIDTH, WIDTH: SHORT}, next_values={0: WIDTH}))
    async with _client(portal) as client:
        stream = client.iter_list(_request(), selector=SELECTOR, page_size=WIDTH, offset=DECLARED)
        items, error = await _drain(stream)

    assert error is None
    assert len(items) == FULL_THEN_SHORT
    assert portal.starts == [0, WIDTH]
    assert stream.report is not None
    assert stream.report.exhausted


@pytest.mark.parametrize(
    ("sizes", "next_values", "delivered", "message"),
    [
        ({0: WIDTH}, {0: WIDTH + 1}, 0, "contradicts its fixed step"),
        ({0: WIDTH, WIDTH: SHORT}, {WIDTH: WIDTH * 2}, WIDTH, "after a short or empty page"),
        ({0: WIDTH, WIDTH: 0}, {WIDTH: WIDTH * 2}, WIDTH, "after a short or empty page"),
        ({0: SHORT}, {0: 0}, 0, "after a short or empty page"),
        ({0: WIDTH + 1}, {}, 0, "exceeded the declared page cap"),
    ],
)
@pytest.mark.asyncio
async def test_contradictory_page_is_rejected_without_its_rows(
    sizes: dict[int, int],
    next_values: dict[int, int],
    delivered: int,
    message: str,
) -> None:
    portal = _Portal(_booking(sizes, next_values=next_values))
    async with _client(portal) as client:
        stream = client.iter_list(_request(), selector=SELECTOR, page_size=WIDTH, offset=DECLARED)
        items, error = await _drain(stream)

    assert isinstance(error, IncompleteTraversalError)
    assert isinstance(error.__cause__, PaginationError)
    assert message in str(error.__cause__)
    # Only the previously delivered prefix survives; the rejected page yields nothing.
    assert len(items) == delivered
    report = stream.report
    assert report is not None
    assert not report.exhausted
    assert report.page_trace[-1].rejection_code is PageRejectionCode.RANGE_CONTRADICTION
    assert report.page_trace[-1].rows_admitted == 0


@pytest.mark.asyncio
async def test_without_opt_in_an_empty_fixed_step_page_with_next_still_closes() -> None:
    portal = _Portal(_booking({0: WIDTH, WIDTH: 0}, next_values={WIDTH: WIDTH * 2}))
    async with _client(portal) as client:
        stream = client.iter_list(
            _request(),
            selector=SELECTOR,
            page_size=WIDTH,
            offset=OffsetSpec(continuation=OffsetContinuation.FIXED_STEP, step=WIDTH, page_stride=STRIDE),
        )
        items, error = await _drain(stream)

    assert error is None
    assert len(items) == WIDTH
    assert stream.report is not None
    assert stream.report.terminal_reason == "empty page confirmed terminal"


# A16.6: every contradictory declaration is refused before any request.


@pytest.mark.parametrize(
    ("build", "error"),
    [
        (
            lambda: OffsetSpec(
                continuation=OffsetContinuation.FIXED_STEP,
                step=WIDTH,
                page_stride=STRIDE,
                short_page_termination="declared_terminal",  # type: ignore[arg-type]
            ),
            TypeError,
        ),
        (lambda: OffsetSpec(short_page_termination=ShortPageTermination.DECLARED_TERMINAL), ValueError),
        (
            lambda: OffsetSpec(
                continuation=OffsetContinuation.FIXED_STEP,
                step=WIDTH,
                short_page_termination=ShortPageTermination.DECLARED_TERMINAL,
            ),
            ValueError,
        ),
        (
            lambda: OffsetSpec(
                continuation=OffsetContinuation.FIXED_STEP,
                step=1,
                page_stride=PageStride(1, 1, 1),
                short_page_termination=ShortPageTermination.DECLARED_TERMINAL,
            ),
            ValueError,
        ),
        (
            lambda: OffsetSpec(
                continuation=OffsetContinuation.FIXED_STEP,
                step=WIDTH,
                page_stride=STRIDE,
                total_termination=TotalTermination.EXACT_QUALIFIED,
                short_page_termination=ShortPageTermination.DECLARED_TERMINAL,
            ),
            ValueError,
        ),
        (
            lambda: OffsetSpec(
                continuation=OffsetContinuation.FIXED_STEP,
                step=WIDTH,
                limit_path=ParameterPath(("limit",)),
                page_stride=PageStride(WIDTH, WIDTH, WIDTH, requested_wire_limit=WIDTH * 2),
                short_page_termination=ShortPageTermination.DECLARED_TERMINAL,
            ),
            ValueError,
        ),
        (
            lambda: OffsetSpec(
                continuation=OffsetContinuation.FIXED_STEP,
                step=WIDTH,
                page_stride=STRIDE,
                sparse_raw_bound=SparseRawBound(RawTotalSource.ENVELOPE, STRIDE, 4, "stable raw order"),
                short_page_termination=ShortPageTermination.DECLARED_TERMINAL,
            ),
            ValueError,
        ),
        (
            lambda: OffsetSpec(
                parameter_path=ParameterPath(("page",)),
                page_index=PageIndex(ParameterPath(("page",))),
                short_page_termination=ShortPageTermination.DECLARED_TERMINAL,
            ),
            ValueError,
        ),
        (lambda: SequentialTraversal(page_size=WIDTH - 1, offset=DECLARED), ValueError),
        (lambda: CountedTraversal(offset=DECLARED), ValueError),
    ],
)
def test_contradictory_declarations_are_refused_at_construction(
    build: Callable[[], object],
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        build()


def test_declared_plan_is_one_exact_window_and_other_plans_keep_their_rules() -> None:
    plan = sequential_offset_plan(DECLARED, page_size=WIDTH, duplicate_policy=DuplicatePolicy.REPORT)
    assert plan.terminal == frozenset({OffsetTerminalRule.EMPTY_PAGE, OffsetTerminalRule.DECLARED_SHORT_PAGE})
    assert plan.short_page_width == WIDTH
    ordinary = OffsetSpec(continuation=OffsetContinuation.FIXED_STEP, step=WIDTH, page_stride=STRIDE)
    assert sequential_offset_plan(ordinary, page_size=WIDTH, duplicate_policy=DuplicatePolicy.REPORT).terminal == (
        frozenset({OffsetTerminalRule.EMPTY_PAGE})
    )
    with pytest.raises(ValueError, match="page_size must match"):
        sequential_offset_plan(DECLARED, page_size=WIDTH * 2, duplicate_policy=DuplicatePolicy.REPORT)
    for fields in (
        {"short_page_width": 1},
        {"page_stride": None},
        {"terminal": frozenset({OffsetTerminalRule.DECLARED_SHORT_PAGE, OffsetTerminalRule.QUALIFIED_TOTAL})},
        {"total_semantics": TotalSemantics.ADVISORY},
        {"allow_empty_after_short_window": True},
    ):
        with pytest.raises(ValueError, match=r"declared short-page closure|qualified-total"):
            _declared_plan(**fields)


def _declared_plan(**fields: object) -> OffsetSequentialPlan:
    values: dict[str, object] = {
        "continuation": OffsetContinuation.FIXED_STEP,
        "fixed_step": WIDTH,
        "page_stride": STRIDE,
        "short_page_width": WIDTH,
        "terminal": frozenset({OffsetTerminalRule.EMPTY_PAGE, OffsetTerminalRule.DECLARED_SHORT_PAGE}),
    }
    return OffsetSequentialPlan(**(values | fields))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("parameters", "policy"),
    [
        ({"start": 7}, None),
        ({"start": WIDTH}, None),
        ({}, ExecutionPolicy(consistency=ConsistencyPolicy(total_semantics=TotalSemantics.ADVISORY))),
        ({}, ExecutionPolicy(consistency=ConsistencyPolicy(total_semantics=TotalSemantics.FILTERED_EXACT))),
        (
            {},
            ExecutionPolicy(consistency=ConsistencyPolicy(confirmation_policy=ConfirmationPolicy.EMPTY_AFTER_BOUNDARY)),
        ),
        (
            {},
            ExecutionPolicy(consistency=ConsistencyPolicy(snapshot_requirement=SnapshotRequirement.FROZEN_MANIFEST)),
        ),
    ],
)
@pytest.mark.asyncio
async def test_non_zero_start_or_stronger_policy_is_refused_before_io(
    parameters: dict[str, object],
    policy: ExecutionPolicy | None,
) -> None:
    portal = _Portal(lambda _query: pytest.fail("a refused declaration must not reach the portal"))
    async with _client(portal) as client:
        error = await _refusal(
            lambda: client.iter_list(
                _request(parameters), selector=SELECTOR, page_size=WIDTH, offset=DECLARED, policy=policy
            )
        )
        reference_error = await _refusal(
            lambda: client.iter_reference_outcomes(
                _request(parameters),
                [Binding("one", (), 1)],
                traversal=SequentialTraversal(selector=SELECTOR, page_size=WIDTH, offset=DECLARED),
                dispatch=DirectDispatch(concurrency=1),
                policy=policy,
            )
        )

    assert isinstance(error, CapabilityError)
    assert isinstance(reference_error, CapabilityError)
    assert portal.physical == 0


@pytest.mark.asyncio
async def test_explicit_zero_start_is_the_created_default() -> None:
    portal = _Portal(_booking({0: SHORT}))
    async with _client(portal) as client:
        stream = client.iter_list(_request({"start": 0}), selector=SELECTOR, page_size=WIDTH, offset=DECLARED)
        items, error = await _drain(stream)

    assert error is None
    assert len(items) == SHORT
    assert portal.starts == [0]


# A16.7 and A16.11: per-binding closures, correlation and failure isolation over both dispatch routes.


def _windowed(sizes: dict[int, dict[int, int]], *, next_values: dict[int, dict[int, int]] | None = None) -> Page:
    """Serve an independent booking window per ``filter[within][dateFrom]``."""

    def page(query: dict[str, str]) -> Envelope:
        window = int(query["filter[within][dateFrom]"])
        return _booking(sizes[window], next_values=(next_values or {}).get(window), base=window * 1000)(query)

    return page


def _window(date_from: int) -> Binding[int]:
    return Binding(
        f"window {date_from}",
        (ParameterUpdate(ParameterPath(("filter", "within")), {"dateFrom": date_from, "dateTo": date_from + 1}),),
        date_from,
    )


@pytest.mark.parametrize("dispatch", [DirectDispatch(concurrency=2), BatchDispatch(batch_size=3, concurrency=1)])
@pytest.mark.asyncio
async def test_reference_bindings_close_on_their_own_short_pages(
    dispatch: DirectDispatch | BatchDispatch,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    terminals = _terminals(monkeypatch)
    portal = _Portal(
        _windowed(
            {1: {0: WIDTH, WIDTH: SHORT}, 2: {0: 7}, 3: {0: WIDTH, WIDTH: 0}, 4: {0: SHORT}},
            next_values={CONTRADICTED: {0: WIDTH}},
        )
    )
    async with _client(portal) as client:
        stream = client.iter_reference_outcomes(
            _request({"order": {"id": "ASC"}}),
            [_window(1), _window(2), _window(3), _window(4)],
            traversal=SequentialTraversal(selector=SELECTOR, page_size=WIDTH, offset=DECLARED),
            dispatch=dispatch,
        )
        outcomes, error = await _drain(stream)

    assert error is None
    rows = {
        window: [item.item for item in outcomes if isinstance(item, ReferenceItem) and item.correlation == window]
        for window in (1, 2, 3, 4)
    }
    assert [len(rows[window]) for window in (1, 2, 3)] == [FULL_THEN_SHORT, 7, WIDTH]
    assert rows[4] == []
    completions = {item.correlation: item for item in outcomes if isinstance(item, ReferenceComplete)}
    assert completions[1].closure is BindingClosure.DECLARED_SHORT_PAGE
    assert completions[2].closure is BindingClosure.DECLARED_SHORT_PAGE
    assert completions[3].closure is BindingClosure.SOURCE_EMPTY
    assert all(item.exhausted and item.stop_reason is None for item in completions.values())
    assert [len(rows[window]) for window in (1, 2, 3)] == [completions[window].row_count for window in (1, 2, 3)]
    [failure] = [item for item in outcomes if isinstance(item, ReferenceFailure)]
    assert failure.correlation == CONTRADICTED
    assert failure.partial_rows == 0
    assert isinstance(failure.error, IncompleteTraversalError)
    witnessed = {terminal.closure: terminal.declared_short_page_width for terminal in terminals}
    assert witnessed == {
        BindingClosure.DECLARED_SHORT_PAGE: WIDTH,
        BindingClosure.SOURCE_EMPTY: None,
        BindingClosure.FAILURE: None,
    }
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.COMPLETED_WITH_FAILURES
    assert report.terminal_reason == "reference input exhausted"
    assert report.assurance is TraversalAssurance.MECHANICS_ONLY
    assert sorted(portal.starts) == [0, 0, 0, 0, WIDTH, WIDTH]


def test_reference_complete_closure_is_outside_equality_and_hashing() -> None:
    correlation = object()
    old = ReferenceComplete(0, correlation, 1)
    new = ReferenceComplete(0, correlation, 1, closure=BindingClosure.DECLARED_SHORT_PAGE)

    assert old == new
    assert hash(old) == hash(new)
    assert old.closure is None
    assert new.closure is BindingClosure.DECLARED_SHORT_PAGE


# A16.8: the gate trusts only the acknowledged page and the declared window, never the reason string.


def _gate_with_page(rows: int | None) -> tuple[CompletionGate, int]:
    gate = CompletionGate("run")
    gate.emit(BindingAdmitted(operation_id="run", sequence=0, binding_id=0))
    if rows is None:
        return gate, 1
    events = (
        PageScheduled(operation_id="run", sequence=1, binding_id=0, page_id=0),
        PageCommandOutcome(operation_id="run", sequence=2, binding_id=0, page_id=0, outcome=CommandSettlement.SUCCESS),
        PageValidated(operation_id="run", sequence=3, binding_id=0, page_id=0, row_count=rows),
        PageDelivered(operation_id="run", sequence=4, binding_id=0, page_id=0),
        PageAcknowledged(operation_id="run", sequence=5, binding_id=0, page_id=0),
    )
    for event in events:
        gate.emit(event)
    return gate, 6


def _gate_decision(rows: int | None, closure: BindingClosure, width: object) -> tuple[TerminalState, bool, set[str]]:
    gate, sequence = _gate_with_page(rows)
    gate.emit(
        BindingTerminal(
            operation_id="run",
            sequence=sequence,
            binding_id=0,
            closure=closure,
            declared_short_page_width=width,  # type: ignore[arg-type]
        )
    )
    gate.emit(StreamTerminal(operation_id="run", sequence=sequence + 1, closure=StreamClosure.NATURAL))
    gate.emit(CleanupOutcome(operation_id="run", sequence=sequence + 2, state=CleanupState.SUCCESS))
    decision = gate.decision()
    return decision.state, decision.exhausted, {violation.code for violation in decision.violations}


def test_gate_accepts_a_non_empty_short_last_page_against_its_window() -> None:
    assert _gate_decision(SHORT, BindingClosure.DECLARED_SHORT_PAGE, WIDTH) == (TerminalState.COMPLETED, True, set())


@pytest.mark.parametrize(
    ("rows", "width", "code"),
    [
        (0, WIDTH, "completion_invalid_declared_short_page_witness"),
        (WIDTH, WIDTH, "completion_invalid_declared_short_page_witness"),
        (SHORT, None, "completion_invalid_declared_short_page_witness"),
        (SHORT, "50", "completion_invalid_declared_short_page_witness"),
        (SHORT, float(WIDTH), "completion_invalid_declared_short_page_witness"),
        (1, 1, "completion_invalid_declared_short_page_witness"),
        (1, True, "completion_invalid_declared_short_page_witness"),
        (None, WIDTH, "completion_terminal_lacks_page_witness"),
    ],
)
def test_gate_blocks_an_invalid_declared_short_page_witness(rows: int | None, width: object, code: str) -> None:
    state, exhausted, codes = _gate_decision(rows, BindingClosure.DECLARED_SHORT_PAGE, width)

    assert state is TerminalState.INCOMPLETE
    assert not exhausted
    assert code in codes


@pytest.mark.parametrize("width", [0, False, WIDTH])
@pytest.mark.parametrize(
    ("rows", "closure"),
    [(0, BindingClosure.SOURCE_EMPTY), (SHORT, BindingClosure.CALLER_STOP), (SHORT, BindingClosure.SINGLE_RESPONSE)],
)
def test_gate_blocks_a_declared_width_on_any_other_closure(rows: int, closure: BindingClosure, width: object) -> None:
    state, exhausted, codes = _gate_decision(rows, closure, width)

    assert state is TerminalState.INCOMPLETE
    assert not exhausted
    assert "completion_unexpected_declared_short_page_width" in codes


def test_declared_short_page_reason_maps_to_its_own_closure() -> None:
    assert qualified_closure(DECLARED_SHORT_PAGE_REACHED) is BindingClosure.DECLARED_SHORT_PAGE
    assert qualified_closure("empty page confirmed terminal") is None


# A16.9: caller stops and early closes never strengthen the guarantee.


class _Stops:
    def __init__(self, decision: ContinuePage | CallerStop) -> None:
        self.decision = decision
        self.pages: list[int] = []

    def on_page(self, boundary: PageBoundary) -> ContinuePage | CallerStop:
        self.pages.append(len(boundary.rows))
        return self.decision


@pytest.mark.asyncio
async def test_caller_stop_on_a_continuing_full_page_is_a_bounded_prefix() -> None:
    portal = _Portal(_booking({0: WIDTH, WIDTH: SHORT}))
    stops = _Stops(CallerStop("enough"))
    async with _client(portal) as client:
        stream = client.iter_list(_request(), selector=SELECTOR, page_size=WIDTH, offset=DECLARED, page_stop=stops)
        items, error = await _drain(stream)

    assert error is None
    assert len(items) == WIDTH
    assert portal.starts == [0]
    report = stream.report
    assert report is not None
    assert report.successful
    assert not report.exhausted
    assert report.assurance is TraversalAssurance.BOUNDED_PREFIX


@pytest.mark.parametrize("dispatch", [None, DirectDispatch(concurrency=1), BatchDispatch(batch_size=1)])
@pytest.mark.asyncio
async def test_caller_stop_on_the_terminal_short_page_keeps_the_natural_closure(
    dispatch: DirectDispatch | BatchDispatch | None,
) -> None:
    portal = _Portal(_booking({0: SHORT}))
    stops = _Stops(CallerStop("enough"))
    async with _client(portal) as client:
        if dispatch is None:
            stream: _AnyStream = client.iter_list(
                _request(), selector=SELECTOR, page_size=WIDTH, offset=DECLARED, page_stop=stops
            )
        else:
            stream = client.iter_reference_outcomes(
                _request(),
                [Binding("one", (), 1)],
                traversal=SequentialTraversal(selector=SELECTOR, page_size=WIDTH, offset=DECLARED),
                dispatch=dispatch,
                page_stop=stops,
            )
        items, error = await _drain(stream)

    assert error is None
    assert stops.pages == [SHORT]
    assert portal.starts == [0]
    report = stream.report
    assert report is not None
    assert report.exhausted
    assert report.assurance is TraversalAssurance.MECHANICS_ONLY
    if dispatch is not None:
        [complete] = [item for item in items if isinstance(item, ReferenceComplete)]
        assert complete.closure is BindingClosure.DECLARED_SHORT_PAGE
        assert complete.exhausted


@pytest.mark.parametrize("reference", [False, True])
@pytest.mark.asyncio
async def test_early_close_keeps_the_prior_non_exhausted_outcome(reference: bool) -> None:  # noqa: FBT001
    portal = _Portal(_booking({0: WIDTH, WIDTH: SHORT}))
    async with _client(portal) as client:
        stream: _AnyStream = (
            client.iter_reference_outcomes(
                _request(),
                [Binding("one", (), 1)],
                traversal=SequentialTraversal(selector=SELECTOR, page_size=WIDTH, offset=DECLARED),
                dispatch=DirectDispatch(concurrency=1),
            )
            if reference
            else client.iter_list(_request(), selector=SELECTOR, page_size=WIDTH, offset=DECLARED)
        )
        async with stream:
            await anext(aiter(stream))

    report = stream.report
    assert report is not None
    assert report.state is TerminalState.EARLY_CLOSED
    assert not report.exhausted
    assert report.assurance is TraversalAssurance.MECHANICS_ONLY


# A16.10: the normative booking recipe runs verbatim from the user documentation.

RECIPES = Path(__file__).resolve().parents[1] / "docs" / "recipes.md"
PYTHON_BLOCK = re.compile(r"```python\n(.*?)\n```", re.DOTALL)
DATE_FROM, DATE_TO = 1790000000, 1790086400


def _booking_recipe() -> str:
    [source] = [block for block in PYTHON_BLOCK.findall(RECIPES.read_text("utf-8")) if "booking.v1.booking" in block]
    return str(source)


async def _run_recipe(portal: _Portal) -> dict[str, object]:
    namespace: dict[str, object] = {"date_from": DATE_FROM, "date_to": DATE_TO}
    async with _client(portal) as api:
        namespace["api"] = api
        code = compile(_booking_recipe(), str(RECIPES), "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
        result = eval(code, namespace)  # noqa: S307 - exact trusted repository documentation source
        if result is not None:
            await result
    return namespace


@pytest.mark.parametrize(("sizes", "rows"), [({0: WIDTH, WIDTH: SHORT}, FULL_THEN_SHORT), ({0: SHORT}, SHORT)])
@pytest.mark.asyncio
async def test_booking_recipe_runs_verbatim_with_its_documented_outcome(sizes: dict[int, int], rows: int) -> None:
    portal = _Portal(_booking(sizes))
    namespace = await _run_recipe(portal)

    # The first request creates start=0 and leaves the caller's filter and order untouched.
    assert portal.queries[0] == {
        "filter[within][dateFrom]": str(DATE_FROM),
        "filter[within][dateTo]": str(DATE_TO),
        "order[id]": "ASC",
        "start": "0",
    }
    assert portal.starts == sorted(sizes)
    report = namespace["report"]
    assert isinstance(report, OperationReport)
    assert report.successful
    assert report.exhausted
    assert report.emitted == rows
    assert report.terminal_reason == DECLARED_SHORT_PAGE_REACHED


@pytest.mark.parametrize("dispatch", [DirectDispatch(concurrency=2), BatchDispatch(batch_size=2)])
@pytest.mark.asyncio
async def test_booking_recipe_reference_variant_closes_every_window(dispatch: DirectDispatch | BatchDispatch) -> None:
    namespace = await _run_recipe(_Portal(_booking({0: SHORT})))
    page, offset = namespace["BOOKING_PAGE"], namespace["BOOKING_OFFSET"]
    assert isinstance(page, int)
    assert isinstance(offset, OffsetSpec)
    portal = _Portal(_windowed({1: {0: WIDTH, WIDTH: SHORT}, 2: {0: SHORT}}))
    async with _client(portal) as api:
        stream = api.iter_reference_outcomes(
            _request({"order": {"id": "ASC"}}),
            [_window(1), _window(2)],
            traversal=SequentialTraversal(selector=ResultSelector(("booking",)), page_size=page, offset=offset),
            dispatch=dispatch,
        )
        outcomes, error = await _drain(stream)

    assert error is None
    completions = {item.correlation: item for item in outcomes if isinstance(item, ReferenceComplete)}
    assert {window: item.closure for window, item in completions.items()} == {
        1: BindingClosure.DECLARED_SHORT_PAGE,
        2: BindingClosure.DECLARED_SHORT_PAGE,
    }
    assert {window: item.row_count for window, item in completions.items()} == {1: FULL_THEN_SHORT, 2: SHORT}
    assert stream.report is not None
    assert stream.report.successful
    assert stream.report.exhausted
    assert stream.report.assurance is TraversalAssurance.MECHANICS_ONLY
