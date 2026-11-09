"""#29: descending inclusive-timestamp windows with classified paging rows (A29-*)."""

from __future__ import annotations
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from b24api import (
    BatchDispatch,
    Binding,
    CapabilityError,
    DirectDispatch,
    IdentityCoercion,
    IdentitySpec,
    IncompleteTraversalError,
    PaginationError,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    ResultSelector,
    TraversalAssurance,
)
from b24api.contracts import (
    AdaptedPage,
    BindingClosure,
    InclusiveWindowTraversal,
    PageView,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ResponseFlagTerminal,
    WindowRows,
)
from b24api.contracts.json import FrozenMapping, _freeze_json, thaw_json
from b24api.contracts.page_stop import CallerStop, ContinuePage, PageBoundary
from b24api.contracts.report import PageDispatch, PageOutcome, PageRecord
from b24api.errors import ContinuationError, ContinuationReason
from b24api.testing.scripted import ScriptedExchange, ScriptedTransport
from b24api.traversal.inclusive_window import parse_window_timestamp
from tests.scripting import BatchPortal

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.scripting import ClientFactory, Envelope, Query

_FLAG_END = "response flag declared terminal page"
_METHOD = "im.chat.message.window"
_BOUNDARY = ParameterPath(("lastDate",))
_LIMIT = ParameterPath(("limit",))
_T1 = "2026-03-01T10:00:01+00:00"
_T2 = "2026-03-01T10:00:02+00:00"
_T3 = "2026-03-01T10:00:03+00:00"
_T5 = "2026-03-01T10:00:05+00:00"


@dataclass(frozen=True)
class _PinnedClassifier:
    """Pure classifier: pinned rows are supplemental, every other row pages the window."""

    def classify(self, page: PageView, /) -> WindowRows:
        pinned = tuple(index for index, row in enumerate(page.items) if row.get("pinned"))  # type: ignore[union-attr]
        return WindowRows(
            tuple(index for index in range(len(page.items)) if index not in pinned),
            pinned,
        )


@dataclass(frozen=True)
class _FixedClassifier:
    """Return a scripted split (or raise) regardless of the page."""

    decide: Callable[[PageView], object] = field(default=lambda _page: WindowRows((), ()))

    def classify(self, page: PageView, /) -> WindowRows:
        return self.decide(page)  # type: ignore[return-value]


def _window(
    classifier: object = None,
    *,
    requested: int | None = 2,
    maximum: int = 4,
) -> InclusiveWindowTraversal:
    return InclusiveWindowTraversal(
        ResultSelector(("messages",)),
        IdentitySpec(("id",), "id", "id", IdentityCoercion.EXACT_INTEGER),
        ("date",),
        _BOUNDARY,
        ResponseFlagTerminal(ResultSelector(("hasMore",))),
        _PinnedClassifier() if classifier is None else classifier,  # type: ignore[arg-type]
        maximum,
        limit_path=None if requested is None else _LIMIT,
        requested_window_size=requested,
    )


def _request(boundary: str | None = None, *, limit: int | None = 2, **extra: object) -> Request:
    parameters: dict[str, object] = {"chatId": 1, **extra}
    if boundary is not None:
        parameters["lastDate"] = boundary
    if limit is not None:
        parameters["limit"] = limit
    return Request.bare(_METHOD, parameters, ReplaySafety.SAFE)


def _row(row_id: int, date: object, *, pinned: bool = False) -> dict[str, object]:
    return {"id": row_id, "date": date, **({"pinned": True} if pinned else {})}


def _page(*rows: dict[str, object], more: bool) -> dict[str, object]:
    return {"result": {"messages": list(rows), "hasMore": more}}


def _field(row: object, key: str) -> object:
    assert isinstance(row, dict)
    return row[key]


async def _drain(stream: object) -> tuple[list[object], BaseException | None]:
    rows: list[object] = []
    failure: BaseException | None = None
    try:
        async with stream:  # type: ignore[attr-defined]
            async for row in stream:  # type: ignore[attr-defined]
                rows.append(row)  # noqa: PERF401 - keep the prefix delivered before a failure
    except Exception as error:  # noqa: BLE001 - the test inspects the typed failure
        failure = error
    return rows, failure


def _ids(rows: list[object]) -> list[int]:
    return [row["id"] for row in rows]  # type: ignore[index]


def _reason(failure: BaseException | None) -> ContinuationReason:
    assert isinstance(failure, IncompleteTraversalError)
    assert isinstance(failure.error, ContinuationError)
    return failure.error.reason


@pytest.mark.asyncio
async def test_a29_head_overlap_is_delivered_once_and_flag_false_closes(scripted_client: ClientFactory) -> None:
    """A29-HEAD: [20/T3,19/T2] -> raw T2 -> [19/T2,18/T1] false gives [20,19,18] in 2 HTTP."""
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(_request(), _page(_row(20, _T3), _row(19, _T2), more=True)),
            ScriptedExchange.json(_request(_T2), _page(_row(19, _T2), _row(18, _T1), more=False)),
        )
    )
    stream = scripted_client(transport).iter_list_window(_request(limit=None), traversal=_window())
    rows, failure = await _drain(stream)
    assert failure is None
    assert _ids(rows) == [20, 19, 18]
    transport.assert_exhausted()
    report = stream.report
    assert report is not None
    assert report.operation == "iter_list_window"
    assert report.exhausted
    assert report.terminal_reason == _FLAG_END
    assert report.physical_requests == len(transport.calls)
    assert report.assurance is TraversalAssurance.MECHANICS_ONLY


@pytest.mark.asyncio
async def test_a29_head_raw_boundary_string_is_sent_unchanged(scripted_client: ClientFactory) -> None:
    """A29-HEAD/A29-TS: the outgoing boundary keeps its raw offset and fraction, not a normalized form."""
    raw = "2026-03-01T13:00:02.5+03:00"
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(_request(), _page(_row(20, _T3), _row(19, raw), more=True)),
            ScriptedExchange.json(_request(raw), _page(more=False)),
        )
    )
    stream = scripted_client(transport).iter_list_window(_request(), traversal=_window())
    rows, failure = await _drain(stream)
    assert failure is None
    assert _ids(rows) == [20, 19]
    transport.assert_exhausted()
    assert transport.calls[1].copy_parameters()["lastDate"] == raw


@pytest.mark.asyncio
async def test_a29_head_two_references_with_overlapping_ids_close_independently(
    scripted_client: ClientFactory,
) -> None:
    """A29-HEAD: two chats with the same message IDs keep separate seen sets and close on their own flag."""
    exchanges = []
    for chat in (1, 2):
        exchanges.append(ScriptedExchange.json(_request(chatId=chat), _page(_row(20, _T3), _row(19, _T2), more=True)))
        exchanges.append(
            ScriptedExchange.json(_request(_T2, chatId=chat), _page(_row(19, _T2), _row(18, _T1), more=False))
        )
    transport = ScriptedTransport(tuple(exchanges))
    bindings = [Binding(f"chat {chat}", (ParameterUpdate(ParameterPath(("chatId",)), chat),), chat) for chat in (1, 2)]
    stream = scripted_client(transport).iter_reference_outcomes(
        _request(), bindings, traversal=_window(), dispatch=DirectDispatch()
    )
    outcomes, failure = await _drain(stream)
    assert failure is None
    transport.assert_exhausted()
    for chat in (1, 2):
        assert [
            _field(outcome.item, "id")
            for outcome in outcomes
            if isinstance(outcome, ReferenceItem) and outcome.correlation == chat
        ] == [20, 19, 18]
    completes = [outcome for outcome in outcomes if isinstance(outcome, ReferenceComplete)]
    assert len(completes) == len(bindings)
    assert {outcome.closure for outcome in completes} == {BindingClosure.DECLARED_RESPONSE_TERMINAL}
    assert all(outcome.exhausted for outcome in completes)


@pytest.mark.asyncio
async def test_a29_r_pinned_only_continuing_page_fails_only_its_reference(scripted_client: ClientFactory) -> None:
    """A29-R: overlapping IDs; the chat whose continuing page is pinned-only is incomplete, the other complete."""
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(_request(chatId=1), _page(_row(20, _T3), _row(19, _T2), more=False)),
            ScriptedExchange.json(_request(chatId=2), _page(_row(20, _T5, pinned=True), more=True)),
        )
    )
    bindings = [Binding(f"chat {chat}", (ParameterUpdate(ParameterPath(("chatId",)), chat),), chat) for chat in (1, 2)]
    stream = scripted_client(transport).iter_reference_outcomes(
        _request(), bindings, traversal=_window(), dispatch=DirectDispatch()
    )
    outcomes, failure = await _drain(stream)
    assert failure is None
    transport.assert_exhausted()
    completes = [outcome for outcome in outcomes if isinstance(outcome, ReferenceComplete)]
    assert [outcome.correlation for outcome in completes] == [1]
    assert completes[0].closure is BindingClosure.DECLARED_RESPONSE_TERMINAL
    failed = [outcome for outcome in outcomes if isinstance(outcome, ReferenceFailure)]
    assert [outcome.correlation for outcome in failed] == [2]
    assert _reason(failed[0].error) is ContinuationReason.WINDOW_UNQUALIFIED
    assert not [
        outcome
        for outcome in outcomes
        if isinstance(outcome, ReferenceItem) and outcome.correlation == failed[0].correlation
    ]


@pytest.mark.asyncio
async def test_a29_stationary_equal_boundary_fails_without_another_request(scripted_client: ClientFactory) -> None:
    """A29-STATIONARY: incoming T2 and a [19/T2,21/T2] continuing page fail as NONPROGRESS after 1 HTTP."""
    transport = ScriptedTransport(
        (ScriptedExchange.json(_request(_T2), _page(_row(19, _T2), _row(21, _T2), more=True)),)
    )
    stream = scripted_client(transport).iter_list_window(_request(_T2), traversal=_window())
    rows, failure = await _drain(stream)
    assert rows == []
    assert _reason(failure) is ContinuationReason.WINDOW_NONPROGRESS
    transport.assert_exhausted()
    assert stream.report is not None
    assert not stream.report.exhausted
    assert stream.report.physical_requests == 1


@pytest.mark.asyncio
async def test_a29_stationary_head_cohort_fails_on_the_second_page(scripted_client: ClientFactory) -> None:
    """A29-STATIONARY: a head all at T5 sends T5 once; the second all-T5 page fails after 2 HTTP, never nudged."""
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(_request(), _page(_row(30, _T5), _row(29, _T5), more=True)),
            ScriptedExchange.json(_request(_T5), _page(_row(28, _T5), _row(27, _T5), more=True)),
        )
    )
    stream = scripted_client(transport).iter_list_window(_request(), traversal=_window())
    rows, failure = await _drain(stream)
    assert _ids(rows) == [30, 29]
    assert _reason(failure) is ContinuationReason.WINDOW_NONPROGRESS
    transport.assert_exhausted()
    assert stream.report is not None
    assert stream.report.physical_requests == len(transport.calls)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("2026-03-01T10:00:02Z", datetime(2026, 3, 1, 10, 0, 2, tzinfo=UTC), id="zulu"),
        pytest.param(
            "2026-03-01T13:00:02.123456+03:00", datetime(2026, 3, 1, 10, 0, 2, 123456, tzinfo=UTC), id="fraction-offset"
        ),
        pytest.param(
            "2026-03-01T10:00:02.5-00:00", datetime(2026, 3, 1, 10, 0, 2, 500000, tzinfo=UTC), id="minus-zero"
        ),
    ],
)
def test_a29_ts_valid_timestamps_compare_as_utc_instants(value: str, expected: datetime) -> None:
    """A29-TS: Z, six-digit fractions with offsets and -00:00 parse to UTC instants and keep the raw text."""
    boundary = parse_window_timestamp(value)
    assert boundary.instant == expected
    assert boundary.instant.utcoffset() == timedelta(0)
    assert boundary.raw == value


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("2026-03-01 10:00:02Z", id="space"),
        pytest.param("2026-03-01T10:00:02", id="naive"),
        pytest.param("2026-03-01T10:00:02.1234567Z", id="seven-digits"),
        pytest.param("2026-03-01T10:00:02+0300", id="compact-offset"),
        pytest.param("2026-03-01T10:00:60Z", id="second-60"),
        pytest.param("2026-02-30T10:00:00Z", id="calendar"),
        pytest.param("2026-03-01T10:00:00+24:00", id="offset-hours"),
        pytest.param("2026-03-01T10:00:00+03:60", id="offset-minutes"),
        pytest.param("\uff12\uff10\uff12\uff16-03-01T10:00:00Z", id="non-ascii-digits"),
        pytest.param(1772359202, id="number"),
        pytest.param(None, id="null"),
        pytest.param("0001-01-01T00:00:00+01:00", id="underflow"),
        pytest.param("9999-12-31T23:59:59-01:00", id="overflow"),
    ],
)
def test_a29_ts_invalid_timestamps_are_rejected(value: object) -> None:
    """A29-TS: anything outside the exact grammar or calendar fails with WINDOW_TIMESTAMP_INVALID."""
    with pytest.raises(ContinuationError) as caught:
        parse_window_timestamp(value)
    assert caught.value.reason is ContinuationReason.WINDOW_TIMESTAMP_INVALID


@pytest.mark.asyncio
async def test_a29_ts_invalid_row_timestamp_commits_nothing(scripted_client: ClientFactory) -> None:
    """A29-TS: an invalid paging timestamp in a row rejects the page before emission."""
    transport = ScriptedTransport(
        (ScriptedExchange.json(_request(), _page(_row(20, _T3), _row(19, "2026-03-01T10:00:02"), more=True)),)
    )
    stream = scripted_client(transport).iter_list_window(_request(), traversal=_window())
    rows, failure = await _drain(stream)
    assert rows == []
    assert _reason(failure) is ContinuationReason.WINDOW_TIMESTAMP_INVALID
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a29_ts_invalid_caller_boundary_rejects_before_http(scripted_client: ClientFactory) -> None:
    """A29-TS: a caller-supplied boundary outside the grammar is rejected before any request."""
    transport = ScriptedTransport(())
    stream = scripted_client(transport).iter_list_window(_request("yesterday"), traversal=_window())
    rows, failure = await _drain(stream)
    assert rows == []
    assert isinstance(failure, CapabilityError)
    assert transport.calls == ()


@pytest.mark.asyncio
async def test_a29_emptywindow_supplemental_only_terminal_head_succeeds(scripted_client: ClientFactory) -> None:
    """A29-EMPTYWINDOW: a pinned-only head with hasMore=false delivers the pinned rows and completes."""
    transport = ScriptedTransport((ScriptedExchange.json(_request(), _page(_row(40, _T5, pinned=True), more=False)),))
    stream = scripted_client(transport).iter_list_window(_request(), traversal=_window())
    rows, failure = await _drain(stream)
    assert failure is None
    assert _ids(rows) == [40]
    assert stream.report is not None
    assert stream.report.exhausted
    assert stream.report.terminal_reason == _FLAG_END


@pytest.mark.asyncio
async def test_a29_emptywindow_empty_terminal_head_succeeds(scripted_client: ClientFactory) -> None:
    """A29-EMPTYWINDOW: an empty head with hasMore=false is an empty, complete window."""
    transport = ScriptedTransport((ScriptedExchange.json(_request(), _page(more=False)),))
    stream = scripted_client(transport).iter_list_window(_request(), traversal=_window())
    rows, failure = await _drain(stream)
    assert failure is None
    assert rows == []
    assert stream.report is not None
    assert stream.report.exhausted


@pytest.mark.asyncio
async def test_a29_emptywindow_supplemental_only_continuing_page_is_unqualified(
    scripted_client: ClientFactory,
) -> None:
    """A29-EMPTYWINDOW: a pinned-only head with hasMore=true has no boundary and fails as WINDOW_UNQUALIFIED."""
    transport = ScriptedTransport((ScriptedExchange.json(_request(), _page(_row(40, _T5, pinned=True), more=True)),))
    stream = scripted_client(transport).iter_list_window(_request(), traversal=_window())
    rows, failure = await _drain(stream)
    assert rows == []
    assert _reason(failure) is ContinuationReason.WINDOW_UNQUALIFIED
    transport.assert_exhausted()


def _raise(_page: PageView) -> object:
    message = "classifier-secret"
    raise RuntimeError(message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("decide", "reason"),
    [
        pytest.param(
            lambda _page: WindowRows((0, True), ()), ContinuationReason.WINDOW_CLASSIFICATION_INVALID, id="bool"
        ),
        pytest.param(
            lambda _page: WindowRows((0,), (-1,)), ContinuationReason.WINDOW_CLASSIFICATION_INVALID, id="negative"
        ),
        pytest.param(
            lambda _page: WindowRows((0, 1, 2), ()), ContinuationReason.WINDOW_CLASSIFICATION_INVALID, id="range"
        ),
        pytest.param(
            lambda _page: WindowRows((0, 1), (1,)), ContinuationReason.WINDOW_CLASSIFICATION_INVALID, id="repeat"
        ),
        pytest.param(
            lambda _page: WindowRows((0,), ()), ContinuationReason.WINDOW_CLASSIFICATION_INVALID, id="incomplete"
        ),
        pytest.param(lambda _page: (0, 1), ContinuationReason.WINDOW_CLASSIFICATION_INVALID, id="not-window-rows"),
        pytest.param(_raise, ContinuationReason.WINDOW_UNQUALIFIED, id="raised"),
    ],
)
async def test_a29_classify_invalid_partitions_reject_the_page(
    decide: Callable[[PageView], object], reason: ContinuationReason, scripted_client: ClientFactory
) -> None:
    """A29-CLASSIFY: a split that is not an exact partition of int indexes, or a raising classifier, commits nothing."""
    transport = ScriptedTransport((ScriptedExchange.json(_request(), _page(_row(20, _T3), _row(19, _T2), more=True)),))
    stream = scripted_client(transport).iter_list_window(_request(), traversal=_window(_FixedClassifier(decide)))
    rows, failure = await _drain(stream)
    assert rows == []
    assert _reason(failure) is reason
    assert failure is not None
    assert "classifier-secret" not in str(failure)
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a29_classify_ascending_paging_rows_are_invalid(scripted_client: ClientFactory) -> None:
    """A29-CLASSIFY: paging rows must be non-increasing in time."""
    transport = ScriptedTransport((ScriptedExchange.json(_request(), _page(_row(19, _T2), _row(20, _T3), more=True)),))
    stream = scripted_client(transport).iter_list_window(_request(), traversal=_window())
    rows, failure = await _drain(stream)
    assert rows == []
    assert _reason(failure) is ContinuationReason.WINDOW_CLASSIFICATION_INVALID


@pytest.mark.asyncio
async def test_a29_classify_row_above_the_incoming_boundary_is_outside(scripted_client: ClientFactory) -> None:
    """A29-CLASSIFY: a paging row newer than the incoming boundary fails as WINDOW_OUTSIDE_BOUND."""
    transport = ScriptedTransport(
        (ScriptedExchange.json(_request(_T2), _page(_row(20, _T3), _row(19, _T1), more=True)),)
    )
    stream = scripted_client(transport).iter_list_window(_request(_T2), traversal=_window())
    rows, failure = await _drain(stream)
    assert rows == []
    assert _reason(failure) is ContinuationReason.WINDOW_OUTSIDE_BOUND
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a29_cap_sends_requested_size_and_bounds_the_paging_rows(scripted_client: ClientFactory) -> None:
    """A29-CAP: requested 2 / max 4 sends limit=2; 2 paging + 2 pinned pass, 3 paging rows fail."""
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(
                _request(),
                _page(_row(50, _T5, pinned=True), _row(20, _T3), _row(19, _T2), _row(51, _T5, pinned=True), more=True),
            ),
            ScriptedExchange.json(_request(_T2), _page(_row(18, _T1), _row(17, _T1), _row(16, _T1), more=True)),
        )
    )
    stream = scripted_client(transport).iter_list_window(_request(limit=None), traversal=_window())
    rows, failure = await _drain(stream)
    assert _ids(rows) == [50, 20, 19, 51]
    assert _reason(failure) is ContinuationReason.WINDOW_CLASSIFICATION_INVALID
    transport.assert_exhausted()
    assert [call.copy_parameters()["limit"] for call in transport.calls] == [2, 2]


@pytest.mark.asyncio
async def test_a29_cap_decoded_ceiling_rejects_an_oversized_page(scripted_client: ClientFactory) -> None:
    """A29-CAP: more decoded rows than max_decoded_rows fail before classification."""
    calls: list[PageView] = []

    def spy(page: PageView) -> object:
        calls.append(page)
        return WindowRows(tuple(range(len(page.items))), ())

    rows_in = [_row(20 - index, _T3) for index in range(5)]
    transport = ScriptedTransport((ScriptedExchange.json(_request(), _page(*rows_in, more=False)),))
    stream = scripted_client(transport).iter_list_window(
        _request(), traversal=_window(_FixedClassifier(spy), requested=2, maximum=4)
    )
    rows, failure = await _drain(stream)
    assert rows == []
    assert isinstance(failure, IncompleteTraversalError)
    assert isinstance(failure.error, PaginationError)
    assert calls == []


@pytest.mark.asyncio
async def test_a29_role_paging_row_seen_again_as_supplemental_is_delivered_once(scripted_client: ClientFactory) -> None:
    """A29-ROLE: an ID that moves from paging to pinned across pages is delivered on first observation only."""
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(_request(), _page(_row(20, _T3), _row(19, _T2), more=True)),
            ScriptedExchange.json(_request(_T2), _page(_row(20, _T3, pinned=True), _row(18, _T1), more=False)),
        )
    )
    stream = scripted_client(transport).iter_list_window(_request(), traversal=_window())
    rows, failure = await _drain(stream)
    assert failure is None
    assert _ids(rows) == [20, 19, 18]
    assert rows[0] == _row(20, _T3)


@pytest.mark.asyncio
async def test_a29_role_duplicate_identity_within_a_page_is_rejected(scripted_client: ClientFactory) -> None:
    """A29-ROLE: the same identity twice in one page is rejected, even across paging and supplemental roles."""
    transport = ScriptedTransport(
        (ScriptedExchange.json(_request(), _page(_row(20, _T3, pinned=True), _row(20, _T3), more=False)),)
    )
    stream = scripted_client(transport).iter_list_window(_request(), traversal=_window())
    rows, failure = await _drain(stream)
    assert rows == []
    assert isinstance(failure, IncompleteTraversalError)
    assert isinstance(failure.error, PaginationError)
    assert "duplicate identity" in str(failure.error)


@dataclass
class _StopAfterFirst:
    pages: list[int] = field(default_factory=list)

    def on_page(self, boundary: PageBoundary) -> ContinuePage | CallerStop:
        self.pages.append(len(boundary.rows))
        return CallerStop("enough")


@pytest.mark.asyncio
async def test_a29_stop_caller_stop_after_a_continuing_page_sends_nothing_more(scripted_client: ClientFactory) -> None:
    """A29-STOP: CallerStop on the first continuing page sends no further request and reports exhausted=False."""
    transport = ScriptedTransport((ScriptedExchange.json(_request(), _page(_row(20, _T3), _row(19, _T2), more=True)),))
    stop = _StopAfterFirst()
    stream = scripted_client(transport).iter_list_window(_request(), traversal=_window(), page_stop=stop)
    rows, failure = await _drain(stream)
    assert failure is None
    assert _ids(rows) == [20, 19]
    assert stop.pages == [2]
    transport.assert_exhausted()
    assert stream.report is not None
    assert not stream.report.exhausted
    assert stream.report.terminal_reason == "enough"


@pytest.mark.asyncio
async def test_a29_stop_sees_a_continuing_page_with_no_new_rows(scripted_client: ClientFactory) -> None:
    """A29-STOP: the stop policy is consulted after a valid page whose rows were all seen before."""
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(_request(), _page(_row(20, _T3), _row(19, _T2), more=True)),
            ScriptedExchange.json(_request(_T2), _page(_row(19, _T2), _row(18, _T1), more=True)),
            ScriptedExchange.json(_request(_T1), _page(_row(18, _T1), more=False)),
        )
    )

    @dataclass
    class _Count:
        pages: list[int] = field(default_factory=list)

        def on_page(self, boundary: PageBoundary) -> ContinuePage | CallerStop:
            self.pages.append(len(boundary.rows))
            return ContinuePage.CONTINUE

    stop = _Count()
    stream = scripted_client(transport).iter_list_window(_request(), traversal=_window(), page_stop=stop)
    rows, failure = await _drain(stream)
    assert failure is None
    assert _ids(rows) == [20, 19, 18]
    assert stop.pages == [2, 1, 0]
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a29_stop_two_references_stop_independently(scripted_client: ClientFactory) -> None:
    """A29-STOP: each reference stops after its first continuing page with CALLER_STOP and no second request."""
    transport = ScriptedTransport(
        tuple(
            ScriptedExchange.json(_request(chatId=chat), _page(_row(20, _T3), _row(19, _T2), more=True))
            for chat in (1, 2)
        )
    )
    bindings = [Binding(f"chat {chat}", (ParameterUpdate(ParameterPath(("chatId",)), chat),), chat) for chat in (1, 2)]
    stream = scripted_client(transport).iter_reference_outcomes(
        _request(), bindings, traversal=_window(), dispatch=DirectDispatch(), page_stop=_StopAfterFirst()
    )
    outcomes, failure = await _drain(stream)
    assert failure is None
    transport.assert_exhausted()
    completes = [outcome for outcome in outcomes if isinstance(outcome, ReferenceComplete)]
    assert len(completes) == len(bindings)
    assert {outcome.closure for outcome in completes} == {BindingClosure.CALLER_STOP}
    assert not any(outcome.exhausted for outcome in completes)


@pytest.mark.asyncio
async def test_a29_cap_conflicting_caller_limit_rejects_before_http(scripted_client: ClientFactory) -> None:
    """A29-CAP: the limit is SDK-owned; a different caller value is rejected before any request."""
    transport = ScriptedTransport(())
    stream = scripted_client(transport).iter_list_window(_request(limit=99), traversal=_window())
    rows, failure = await _drain(stream)
    assert rows == []
    assert isinstance(failure, CapabilityError)
    assert transport.calls == ()


def test_a29_contract_caps_are_validated_locally() -> None:
    """A29-CAP: requested size must not exceed the decoded ceiling and a sent limit needs a requested size."""
    with pytest.raises(ValueError, match="must not exceed"):
        _window(requested=5, maximum=4)
    with pytest.raises(ValueError, match="requires requested_window_size"):
        InclusiveWindowTraversal(
            ResultSelector(("messages",)),
            IdentitySpec(("id",), "id", "id", IdentityCoercion.EXACT_INTEGER),
            ("date",),
            _BOUNDARY,
            ResponseFlagTerminal(ResultSelector(("hasMore",))),
            _PinnedClassifier(),
            4,
            limit_path=_LIMIT,
        )


@pytest.mark.asyncio
async def test_a_moving_page_of_only_repeated_rows_is_not_a_repeated_page(scripted_client: ClientFactory) -> None:
    """Progress is the boundary: a page whose rows were all seen still moves it, and the flag then closes."""
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(_request(), _page(_row(10, _T5), _row(5, _T1, pinned=True), more=True)),
            ScriptedExchange.json(_request(_T5), _page(_row(10, _T5), _row(5, _T1), more=True)),
            ScriptedExchange.json(_request(_T1), _page(_row(5, _T1), more=False)),
        )
    )
    stream = scripted_client(transport).iter_list_window(_request(limit=None), traversal=_window())
    rows, failure = await _drain(stream)
    assert failure is None
    assert _ids(rows) == [10, 5]
    transport.assert_exhausted()
    assert stream.report is not None
    assert stream.report.exhausted
    assert stream.report.terminal_reason == _FLAG_END


# #34: response-level metadata reaches the page stop on every response, empty and overlap-only pages included.


def _popup_page(*rows: dict[str, object], more: bool, revision: int) -> dict[str, object]:
    return {"result": {"messages": list(rows), "hasMore": more, "popup": {"revision": revision}}}


@dataclass
class _Popups:
    """Merge the response-level popup of every offered page, as a consumer of page metadata does."""

    seen: list[tuple[int, int, int]] = field(default_factory=list)

    def on_page(self, boundary: PageBoundary) -> ContinuePage:
        result = boundary.result
        assert isinstance(result, FrozenMapping)
        popup = result["popup"]
        assert isinstance(popup, FrozenMapping)
        revision = popup["revision"]
        assert isinstance(revision, int)
        self.seen.append((boundary.binding_id, len(boundary.rows), revision))
        return ContinuePage.CONTINUE


@pytest.mark.asyncio
async def test_page_metadata_of_an_empty_terminal_window_page_reaches_the_page_stop(
    scripted_client: ClientFactory,
) -> None:
    """#34: the terminal page has no row but carries the newest popup revision; the page stop receives it."""
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(_request(), _popup_page(_row(20, _T3), more=True, revision=1)),
            ScriptedExchange.json(_request(_T3), _popup_page(more=False, revision=2)),
        )
    )
    popups = _Popups()
    stream = scripted_client(transport).iter_list_window(_request(), traversal=_window(), page_stop=popups)
    rows, failure = await _drain(stream)
    assert failure is None
    assert _ids(rows) == [20]
    assert popups.seen == [(0, 1, 1), (0, 0, 2)]
    transport.assert_exhausted()
    assert stream.report is not None
    assert stream.report.exhausted
    assert stream.report.terminal_reason == _FLAG_END


@pytest.mark.asyncio
async def test_page_metadata_is_the_server_result_beside_the_admitted_and_adapted_rows(
    scripted_client: ClientFactory,
) -> None:
    """#34: an overlap-only page admits no row; its result is still the whole server result, unadapted."""
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(_request(), _popup_page(_row(20, _T3), _row(19, _T2), more=True, revision=1)),
            ScriptedExchange.json(_request(_T2), _popup_page(_row(19, _T2), more=False, revision=2)),
        )
    )
    results: list[tuple[int, object]] = []

    class _Results:
        def on_page(self, boundary: PageBoundary) -> ContinuePage:
            assert boundary.result is not None
            results.append((len(boundary.rows), thaw_json(boundary.result)))
            return ContinuePage.CONTINUE

    class _Tag:
        def adapt(self, page: PageView, /) -> AdaptedPage:
            return AdaptedPage({**row, "tagged": True} for row in page.items)  # type: ignore[dict-item]

    window = replace(_window(), page_adapter=_Tag())
    stream = scripted_client(transport).iter_list_window(_request(), traversal=window, page_stop=_Results())
    rows, failure = await _drain(stream)
    assert failure is None
    assert _ids(rows) == [20, 19]
    assert all(_field(row, "tagged") for row in rows)
    assert results == [
        (2, {"messages": [_row(20, _T3), _row(19, _T2)], "hasMore": True, "popup": {"revision": 1}}),
        (0, {"messages": [_row(19, _T2)], "hasMore": False, "popup": {"revision": 2}}),
    ]


@pytest.mark.asyncio
async def test_page_metadata_reaches_a_reference_page_stop_per_binding(scripted_client: ClientFactory) -> None:
    """#34: every binding's pages, its empty terminal page included, carry their own server result."""
    transport = ScriptedTransport(
        tuple(
            exchange
            for chat in (1, 2)
            for exchange in (
                ScriptedExchange.json(_request(chatId=chat), _popup_page(_row(20, _T3), more=True, revision=chat)),
                ScriptedExchange.json(_request(_T3, chatId=chat), _popup_page(more=False, revision=10 * chat)),
            )
        )
    )
    bindings = [Binding(f"chat {chat}", (ParameterUpdate(ParameterPath(("chatId",)), chat),), chat) for chat in (1, 2)]
    popups = _Popups()
    stream = scripted_client(transport).iter_reference_outcomes(
        _request(), bindings, traversal=_window(), dispatch=DirectDispatch(), page_stop=popups
    )
    outcomes, failure = await _drain(stream)
    assert failure is None
    transport.assert_exhausted()
    assert sorted(popups.seen) == [(0, 0, 10), (0, 1, 1), (1, 0, 20), (1, 1, 2)]
    completes = [outcome for outcome in outcomes if isinstance(outcome, ReferenceComplete)]
    assert len(completes) == len(bindings)
    assert all(outcome.exhausted for outcome in completes)


@pytest.mark.asyncio
async def test_page_metadata_of_sibling_commands_in_one_shared_batch_stays_with_its_binding(
    scripted_client: ClientFactory,
) -> None:
    """#34: bindings whose pages share physical batches each receive their own command's result."""

    def answer(method: str, query: Query) -> Envelope:
        assert method == _METHOD
        chat = int(query["chatId"])
        if "lastDate" not in query:
            return _popup_page(_row(20, _T3), more=True, revision=chat)
        return _popup_page(more=False, revision=10 * chat)

    portal = BatchPortal(answer)
    bindings = [Binding(f"chat {chat}", (ParameterUpdate(ParameterPath(("chatId",)), chat),), chat) for chat in (1, 2)]
    popups = _Popups()
    stream = scripted_client(portal).iter_reference_outcomes(
        _request(), bindings, traversal=_window(), dispatch=BatchDispatch(), page_stop=popups
    )
    outcomes, failure = await _drain(stream)
    assert failure is None
    assert portal.waves == [2, 2]
    assert sorted(popups.seen) == [(0, 0, 10), (0, 1, 1), (1, 0, 20), (1, 1, 2)]
    completes = [outcome for outcome in outcomes if isinstance(outcome, ReferenceComplete)]
    assert len(completes) == len(bindings)
    assert all(outcome.exhausted for outcome in completes)


def test_a_page_boundary_built_by_application_code_has_no_result() -> None:
    """#34: ``result`` is keyword-only and optional, so existing constructions keep working."""
    record = PageRecord(0, None, PageDispatch.DIRECT, None, 0, 0, None, None, PageOutcome.COMMITTED, None)
    result = _freeze_json({"popup": 1})
    assert PageBoundary(0, record, ()).result is None
    assert PageBoundary(0, record, (), result=result).result is result
    with pytest.raises(TypeError):
        PageBoundary(0, record, (), result)  # type: ignore[misc]
    with pytest.raises(TypeError, match="frozen JSON"):
        PageBoundary(0, record, (), result={"popup": 1})  # type: ignore[arg-type]
    assert "popup" not in repr(PageBoundary(0, record, (), result=result))
