"""The page without ``next`` ends an offset traversal; a reported total is checked against the rows read.

The portals below reproduce what live Bitrix24 list methods do past their last row (measured on a test
portal): ``crm.lead.list``, ``crm.deal.list``, ``crm.activity.list`` and ``tasks.task.list`` round
``start`` down to a multiple of 50 and answer a ``start`` at or past the end with their last page again;
``department.get`` ignores ``start`` and answers everything without ``next``; ``user.get`` honors
``start`` and answers an empty page without a total past the end. Every page with more rows carries
``next``; the last page has none and still carries ``total``.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

import pytest

from b24api import (
    Bitrix24,
    CapabilityError,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    IncompleteTraversalError,
    OffsetContinuation,
    OffsetSpec,
    PaginationError,
    ParameterPath,
    ReplaySafety,
    Request,
    ResultSelector,
    TerminalState,
    TotalTermination,
    TraversalAssurance,
)
from b24api.contracts import (
    ConsistencyPolicy,
    ContinuationTraversal,
    PageOutcome,
    PageRejectionCode,
    ResponseCursorRule,
    ResponseFlagTerminal,
)
from b24api.contracts.completion import EMPTY_SOURCE_WITNESS
from b24api.contracts.policy import ConfirmationPolicy, TotalSemantics
from b24api.errors import ApiResponseError, ContinuationError, ContinuationReason, ResultShapeError
from tests.scripting import BatchPortal, client_for

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.contracts import OperationReport, OperationStream
    from b24api.contracts.json import JsonValue
    from tests.scripting import Envelope, Query

PAGE = 50
ID = IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.DECIMAL_STRING_INTEGER)
NO_CONTINUATION = "page without continuation ended the source"
EXACT = OffsetSpec(total_termination=TotalTermination.EXACT_QUALIFIED)


def _rows(count: int) -> list[dict[str, str]]:
    return [{"ID": str(index + 1)} for index in range(count)]


def _rounding(count: int) -> Callable[[str, Query], Envelope]:
    """``crm.lead.list``: ``start`` rounds down to a multiple of 50; past the end the last page comes again."""
    rows = _rows(count)

    def answer(_method: str, query: Query) -> Envelope:
        start = min(int(query.get("start", "0")) // PAGE * PAGE, max(count - 1, 0) // PAGE * PAGE)
        envelope: Envelope = {"result": rows[start : start + PAGE], "total": count}
        if start + PAGE < count:
            envelope["next"] = start + PAGE
        return envelope

    return answer


def _ignores_start(count: int) -> Callable[[str, Query], Envelope]:
    """``department.get``: every ``start`` answers every row, without ``next``."""
    rows = _rows(count)
    return lambda _method, _query: {"result": rows, "total": count}


def _exact_start(count: int) -> Callable[[str, Query], Envelope]:
    """``user.get``: ``start`` is honored; past the end the page is empty and has no total."""
    rows = _rows(count)

    def answer(_method: str, query: Query) -> Envelope:
        start = int(query.get("start", "0"))
        if start >= count:
            return {"result": []}
        envelope: Envelope = {"result": rows[start : start + PAGE], "total": count}
        if start + PAGE < count:
            envelope["next"] = start + PAGE
        return envelope

    return answer


def _request(parameters: dict[str, object] | None = None) -> Request:
    return Request.bare("crm.lead.list", parameters or {}, ReplaySafety.SAFE)


def _client(portal: BatchPortal) -> Bitrix24:
    return client_for(portal)


def _report(stream: OperationStream[JsonValue]) -> OperationReport:
    report = stream.report
    assert report is not None
    return report


async def _drain(stream: OperationStream[JsonValue]) -> tuple[list[JsonValue], BaseException | None]:
    rows: list[JsonValue] = []
    try:
        async with stream:
            async for row in stream:
                rows.append(row)  # noqa: PERF401 - keep the prefix delivered before a failure
    except Exception as error:  # noqa: BLE001 - the test inspects the typed failure
        return rows, error
    return rows, None


def _starts(portal: BatchPortal) -> list[int]:
    return [int(query.get("start", "0")) for query in portal.queries]


# --- the terminal rule ----------------------------------------------------------------------------


@pytest.mark.parametrize("count", [1, 49, 50, 120, 390])
@pytest.mark.asyncio
async def test_a_rounding_endpoint_completes_at_its_page_without_next(count: int) -> None:
    portal = BatchPortal(_rounding(count))
    stream = _client(portal).iter_list(_request(), identity=ID)

    rows, error = await _drain(stream)

    assert error is None
    assert len(rows) == count
    assert _starts(portal) == list(range(0, count, PAGE))
    report = _report(stream)
    assert report.state is TerminalState.COMPLETED
    assert report.exhausted
    assert report.terminal_reason == NO_CONTINUATION
    assert report.assurance is TraversalAssurance.IDENTITY_EXACT


@pytest.mark.parametrize(
    ("answer", "requests"),
    [(_ignores_start(45), [0]), (_exact_start(169), [0, 50, 100, 150])],
    ids=["ignores-start", "exact-start"],
)
@pytest.mark.asyncio
async def test_other_bitrix_list_endpoints_complete_without_a_request_past_the_end(
    answer: Callable[[str, Query], Envelope], requests: list[int]
) -> None:
    portal = BatchPortal(answer)
    stream = _client(portal).iter_list(_request(), identity=ID)

    rows, error = await _drain(stream)

    assert error is None
    assert len(rows) == len({row["ID"] for row in rows if isinstance(row, dict)})
    assert _starts(portal) == requests
    assert _report(stream).terminal_reason == NO_CONTINUATION


@pytest.mark.parametrize(
    ("answer", "count"),
    [(_rounding(390), 390), (_rounding(120), 120), (_ignores_start(45), 45), (_exact_start(169), 169)],
    ids=["rounding-390", "rounding-120", "ignores-start", "exact-start"],
)
@pytest.mark.asyncio
async def test_counted_traversal_never_asks_past_the_end(answer: Callable[[str, Query], Envelope], count: int) -> None:
    portal = BatchPortal(answer)
    stream = _client(portal).iter_list_counted(_request(), identity=ID)

    rows, error = await _drain(stream)

    assert error is None
    assert len(rows) == count
    assert max(_starts(portal)) < count
    assert _report(stream).state is TerminalState.COMPLETED


@pytest.mark.asyncio
async def test_an_exact_total_closes_on_the_page_that_reaches_it() -> None:
    portal = BatchPortal(_rounding(120))
    stream = _client(portal).iter_list(_request(), identity=ID, offset=EXACT)

    rows, error = await _drain(stream)

    assert error is None
    assert len(rows) == 120  # noqa: PLR2004
    assert _starts(portal) == [0, 50, 100]
    assert _report(stream).assurance is TraversalAssurance.IDENTITY_AND_COUNT_MATCHED


# --- the reported total ---------------------------------------------------------------------------


def _short_source(mode: str) -> Callable[[str, Query], Envelope]:
    """Every page reports 120 rows, but the source ends early at offset 50."""
    rows = _rows(120)

    def answer(_method: str, query: Query) -> Envelope:
        start = int(query.get("start", "0"))
        if start == PAGE and mode == "untotalled":
            # The end reports no total: the latest page that reported one still counts the source.
            return {"result": rows[start : start + PAGE]}
        if start == PAGE:
            return {"result": [] if mode == "empty" else rows[start : start + 20], "total": 120}
        return {"result": rows[start : start + PAGE], "total": 120, "next": start + PAGE}

    return answer


@pytest.mark.parametrize("mode", ["empty", "short", "untotalled"])
@pytest.mark.asyncio
async def test_rows_short_of_the_reported_total_never_complete(mode: str) -> None:
    portal = BatchPortal(_short_source(mode))
    stream = _client(portal).iter_list(_request(), identity=ID)

    rows, error = await _drain(stream)

    assert isinstance(error, IncompleteTraversalError)
    cause = error.__cause__
    assert isinstance(cause, PaginationError)
    assert cause.rejection_code is PageRejectionCode.TOTAL_DRIFT
    assert len(rows) == PAGE
    report = _report(stream)
    assert report.state is TerminalState.INCOMPLETE
    assert not report.exhausted
    assert (report.page_trace[-1].outcome, report.page_trace[-1].rejection_code) == (
        PageOutcome.REJECTED,
        PageRejectionCode.TOTAL_DRIFT,
    )


@pytest.mark.asyncio
async def test_rows_beyond_a_reported_total_of_zero_never_complete() -> None:
    rows = _rows(3)
    portal = BatchPortal(lambda _method, _query: {"result": rows, "total": 0})
    stream = _client(portal).iter_list(_request(), identity=ID)

    delivered, error = await _drain(stream)

    assert isinstance(error, IncompleteTraversalError)
    cause = error.__cause__
    assert isinstance(cause, PaginationError)
    assert cause.rejection_code is PageRejectionCode.TOTAL_DRIFT
    assert delivered == []
    assert _report(stream).state is TerminalState.INCOMPLETE


@pytest.mark.asyncio
async def test_an_advisory_total_only_warns_when_the_rows_differ() -> None:
    policy = ExecutionPolicy(consistency=ConsistencyPolicy(total_semantics=TotalSemantics.ADVISORY))
    portal = BatchPortal(_short_source("short"))
    stream = _client(portal).iter_list(_request(), identity=ID, policy=policy)

    rows, error = await _drain(stream)

    assert error is None
    assert len(rows) == PAGE + 20
    report = _report(stream)
    assert report.state is TerminalState.COMPLETED
    assert "advisory_total_mismatch" in {violation.code for violation in report.violations}


@pytest.mark.asyncio
async def test_a_traversal_resumed_past_the_first_page_does_not_compare_the_whole_total() -> None:
    portal = BatchPortal(_rounding(120))
    stream = _client(portal).iter_list(_request({"start": 100}), identity=ID)

    rows, error = await _drain(stream)

    assert error is None
    assert len(rows) == 20  # noqa: PLR2004
    assert _report(stream).terminal_reason == NO_CONTINUATION


@pytest.mark.asyncio
async def test_an_observed_count_endpoint_reads_no_unqualified_total() -> None:
    """Without ``next`` the envelope is no Bitrix list envelope: its total is read only when qualified."""
    rows = _rows(3)
    portal = BatchPortal(lambda _method, query: {"result": rows[int(query["start"]) :][:2], "total": 0})
    offset = OffsetSpec(continuation=OffsetContinuation.OBSERVED_COUNT)
    stream = _client(portal).iter_list(_request(), identity=ID, page_size=2, offset=offset)

    out, error = await _drain(stream)

    assert error is None
    assert len(out) == len(rows)
    assert _starts(portal) == [0, 2, 3]
    assert _report(stream).terminal_reason == "empty page confirmed terminal"


# --- counted and exact-total edges ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_server_next_counted_head_holding_the_whole_range_completes() -> None:
    portal = BatchPortal(_rounding(30))
    offset = OffsetSpec(continuation=OffsetContinuation.SERVER_NEXT, total_termination=TotalTermination.EXACT_QUALIFIED)
    stream = _client(portal).iter_list_counted(_request(), identity=ID, offset=offset)

    rows, error = await _drain(stream)

    assert error is None
    assert len(rows) == 30  # noqa: PLR2004
    assert portal.waves == [1]


@pytest.mark.parametrize("payload", [{"result": []}, {"result": [], "total": -1}])
@pytest.mark.asyncio
async def test_an_exact_total_iter_list_over_an_empty_source_completes(payload: Envelope) -> None:
    portal = BatchPortal(lambda _method, _query: payload)
    stream = _client(portal).iter_list(_request(), identity=ID, offset=EXACT)

    rows, error = await _drain(stream)

    assert error is None
    assert rows == []
    report = _report(stream)
    assert report.state is TerminalState.COMPLETED
    assert report.terminal_reason == EMPTY_SOURCE_WITNESS.terminal_reason


@pytest.mark.asyncio
async def test_an_empty_confirmation_under_server_next_is_refused_before_any_request() -> None:
    policy = ExecutionPolicy(
        consistency=ConsistencyPolicy(confirmation_policy=ConfirmationPolicy.EMPTY_AFTER_BOUNDARY),
    )
    portal = BatchPortal(_rounding(120))
    stream = _client(portal).iter_list(_request(), identity=ID, offset=EXACT, policy=policy)

    rows, error = await _drain(stream)

    assert isinstance(error, CapabilityError)
    assert rows == []
    assert portal.queries == []


# --- an empty page carrying ``next`` --------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_empty_page_with_next_ends_an_offset_traversal_that_does_not_follow_next() -> None:
    rows = _rows(3)

    def answer(_method: str, query: Query) -> Envelope:
        start = int(query["start"])
        return {"result": rows[start:][:2], "next": start + 2}

    portal = BatchPortal(answer)
    offset = OffsetSpec(continuation=OffsetContinuation.OBSERVED_COUNT)
    stream = _client(portal).iter_list(_request(), identity=ID, page_size=2, offset=offset)

    out, error = await _drain(stream)

    assert error is None
    assert len(out) == len(rows)


@pytest.mark.asyncio
async def test_an_empty_page_with_next_is_refused_where_next_is_followed() -> None:
    portal = BatchPortal(lambda _method, query: {"result": [], "next": 50} if query["start"] != "0" else _page_one())
    stream = _client(portal).iter_list(_request(), identity=ID)

    _rows_read, error = await _drain(stream)

    assert isinstance(error, IncompleteTraversalError)
    assert _report(stream).page_trace[-1].rejection_code is PageRejectionCode.RANGE_CONTRADICTION


def _page_one() -> Envelope:
    return {"result": _rows(PAGE), "next": PAGE}


_LAST_ID = ParameterPath(("LAST_ID",))


@pytest.mark.asyncio
async def test_a_continuation_ending_on_an_empty_page_with_next_completes() -> None:
    pages = {"0": [{"ID": "1"}], "1": []}
    portal = BatchPortal(
        lambda _method, query: {
            "result": {"items": pages[query["LAST_ID"]], "more": query["LAST_ID"] == "0", "cursor": 1},
            "next": 50,
        }
    )
    stream = _client(portal).iter_list_continuation(_request({"LAST_ID": 0}), traversal=_continuation())

    rows, error = await _drain(stream)

    assert error is None
    assert rows == [{"ID": "1"}]


# --- a continuation that repeats its control as another spelling -----------------------------------


def _continuation() -> ContinuationTraversal:
    return ContinuationTraversal(
        ResultSelector(("items",)),
        ID,
        (_LAST_ID,),
        ResponseCursorRule(ResultSelector(("cursor",)), _LAST_ID, ResponseFlagTerminal(ResultSelector(("more",)))),
    )


@pytest.mark.asyncio
async def test_a_control_repeated_as_text_is_a_continuation_cycle() -> None:
    """``1`` and ``"1"`` reach the server as the same ``LAST_ID=1``: the second is a repeated request."""
    cursors = {"0": 1, "1": "1"}
    portal = BatchPortal(
        lambda _method, query: {
            "result": {"items": [{"ID": str(len(portal.queries))}], "more": True, "cursor": cursors[query["LAST_ID"]]}
        }
    )
    stream = _client(portal).iter_list_continuation(_request({"LAST_ID": 0}), traversal=_continuation())

    _rows_read, error = await _drain(stream)

    assert isinstance(error, IncompleteTraversalError)
    cause = error.__cause__
    assert isinstance(cause, ContinuationError)
    assert cause.reason is ContinuationReason.CONTINUATION_CYCLE
    assert len(portal.queries) == 2  # noqa: PLR2004


# --- a selector over an empty result ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_nested_selector_over_an_empty_result_is_a_shape_error() -> None:
    portal = BatchPortal(lambda _method, _query: {"result": []})
    stream = _client(portal).iter_list(_request(), selector=ResultSelector(("items",)), identity=ID)

    _rows_read, error = await _drain(stream)

    assert isinstance(error, ResultShapeError)
    assert _report(stream).page_trace[-1].rejection_code is PageRejectionCode.SHAPE_CONTRACT


# --- one failure surface and one rejection code per cause ---------------------------------------------


def _drifting(mutate: Callable[[int, Envelope], object]) -> Callable[[str, Query], Envelope]:
    base = _rounding(150)

    def answer(method: str, query: Query) -> Envelope:
        envelope = base(method, query)
        mutate(int(query.get("start", "0")), envelope)
        return envelope

    return answer


def _second_page(change: Callable[[Envelope], None]) -> Callable[[int, Envelope], None]:
    def mutate(start: int, envelope: Envelope) -> None:
        if start == PAGE:
            change(envelope)

    return mutate


def _duplicate(envelope: Envelope) -> None:
    rows = envelope["result"]
    assert isinstance(rows, list)
    rows[10] = {"ID": "1"}


type _Mutation = Callable[[int, Envelope], object]
_SURFACE: dict[str, tuple[_Mutation, type[Exception], TerminalState, PageRejectionCode | None]] = {
    "api-error": (
        lambda _start, envelope: envelope.update({"error": "ACCESS_DENIED", "error_description": "no"}),
        ApiResponseError,
        TerminalState.FAILED,
        None,
    ),
    "missing-total": (
        lambda _start, envelope: envelope.pop("total"),
        CapabilityError,
        TerminalState.FAILED,
        PageRejectionCode.CAPABILITY_CONTRACT,
    ),
    "shape": (
        _second_page(lambda envelope: envelope.update({"result": {"x": 1}})),
        ResultShapeError,
        TerminalState.FAILED,
        PageRejectionCode.SHAPE_CONTRACT,
    ),
    "duplicate": (
        _second_page(_duplicate),
        PaginationError,
        TerminalState.INCOMPLETE,
        PageRejectionCode.DUPLICATE_IDENTITY,
    ),
    "page-over-cap": (
        _second_page(lambda envelope: envelope.update({"result": [*_rows(PAGE), {"ID": "777"}]})),
        PaginationError,
        TerminalState.INCOMPLETE,
        PageRejectionCode.RANGE_CONTRADICTION,
    ),
    "total-drift": (
        _second_page(lambda envelope: envelope.update({"total": 151})),
        PaginationError,
        TerminalState.INCOMPLETE,
        PageRejectionCode.TOTAL_DRIFT,
    ),
}


@pytest.mark.parametrize("kind", ["iter_list", "iter_list_counted"])
@pytest.mark.parametrize("case", list(_SURFACE))
@pytest.mark.asyncio
async def test_one_cause_fails_iter_list_and_iter_list_counted_alike(case: str, kind: str) -> None:
    """A broken pagination invariant is incomplete; any other failure leaves as raised, on a failed report."""
    mutate, failure, state, code = _SURFACE[case]
    client = _client(BatchPortal(_drifting(mutate)))
    stream = (
        client.iter_list(_request(), identity=ID, offset=EXACT)
        if kind == "iter_list"
        else client.iter_list_counted(_request(), identity=ID)
    )

    _rows_read, error = await _drain(stream)

    incomplete = state is TerminalState.INCOMPLETE
    assert error is not None
    assert isinstance(error, IncompleteTraversalError) is incomplete
    assert isinstance(error.__cause__ if incomplete else error, failure), repr(error)
    report = _report(stream)
    assert report.state is state
    if code is not None:
        assert report.page_trace[-1].rejection_code is code


# --- budgets the exact total already exceeds ---------------------------------------------------------


@pytest.mark.parametrize(
    ("policy", "message"),
    [
        (ExecutionPolicy(max_pages=2), "logical page budget"),
        (ExecutionPolicy(max_requests=1), "physical request budget"),
        (ExecutionPolicy(max_identity_keys=149), "identity evidence budget"),
    ],
    ids=["pages", "requests", "identity-keys"],
)
@pytest.mark.asyncio
async def test_a_counted_range_the_budget_cannot_hold_is_refused_at_its_traced_head(
    policy: ExecutionPolicy, message: str
) -> None:
    portal = BatchPortal(_rounding(150))
    stream = _client(portal).iter_list_counted(_request(), identity=ID, policy=policy)

    rows, error = await _drain(stream)

    assert error is not None
    assert message in str(error)
    assert rows == []
    assert portal.waves == [1]
    report = _report(stream)
    assert report.state is TerminalState.INCOMPLETE
    assert [(record.offset, record.outcome, record.rejection_code) for record in report.page_trace] == [
        (0, PageOutcome.REJECTED, PageRejectionCode.BUDGET_EXHAUSTED)
    ]


@pytest.mark.asyncio
async def test_a_page_beyond_the_identity_budget_is_traced_as_the_budget_end() -> None:
    portal = BatchPortal(_rounding(150))
    stream = _client(portal).iter_list(_request(), identity=ID, policy=ExecutionPolicy(max_identity_keys=149))

    rows, _error = await _drain(stream)

    assert len(rows) == 2 * PAGE
    assert _report(stream).page_trace[-1].rejection_code is PageRejectionCode.BUDGET_EXHAUSTED


class _BrokenLedger:
    def add_if_absent(self, key: str) -> bool:
        del key
        raise OSError("ledger unavailable")


@pytest.mark.asyncio
async def test_a_failed_identity_store_is_traced_as_such() -> None:
    portal = BatchPortal(_rounding(10))
    stream = _client(portal).iter_list(_request(), identity=ID, identity_store=_BrokenLedger())

    _rows_read, error = await _drain(stream)

    assert isinstance(error, IncompleteTraversalError)
    assert _report(stream).page_trace[-1].rejection_code is PageRejectionCode.IDENTITY_STORE
