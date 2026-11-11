"""Traversal contract hygiene: repeated pages, server-next closure, cursor identity, identity errors, decimal ids."""

from __future__ import annotations
from typing import TYPE_CHECKING, cast

import pytest

from b24api import (
    Binding,
    CursorSpec,
    DirectDispatch,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    OffsetContinuation,
    OffsetSpec,
    ParameterPath,
    ParameterUpdate,
    Request,
    ResultSelector,
    SequentialKeysetExecution,
    SequentialTraversal,
    TotalTermination,
)
from b24api.contracts import AdaptedPage, ReferenceFailure, ReferenceItem
from b24api.contracts.json import thaw_json
from b24api.contracts.report import PageRejectionCode, TraversalAssurance
from b24api.contracts.request import RouteKind
from b24api.errors import (
    IdentityContractError,
    IncompleteTraversalError,
    PageAdaptationError,
    PageAdaptationViolation,
    PaginationError,
)
from tests.scripting import BatchPortal, ResponderTransport, client_for

if TYPE_CHECKING:
    from b24api.contracts import JsonValue, OperationReport, PageAdapter, PageView
    from b24api.contracts.stream import OperationStream
    from tests.scripting import Envelope, Query

_ID = IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_INTEGER)
_DECIMAL_ID = IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.DECIMAL_STRING_INTEGER)
_LIST = Request("crm.deal.list", route=RouteKind.BARE)


def _bitrix_offset(rows: list[dict[str, object]], *, page: int = 2) -> ResponderTransport:
    """Bitrix offset pages: ``next`` only while rows remain, ``total`` on every page."""

    def respond(request: Request) -> object:
        start = int(str(request.copy_parameters().get("start", 0)))
        body: dict[str, object] = {"result": rows[start : start + page], "total": len(rows)}
        if start + page < len(rows):
            body["next"] = start + page
        return body

    return ResponderTransport(respond)


def _report(stream: OperationStream[JsonValue]) -> OperationReport:
    report = stream.report
    assert report is not None
    return report


async def _incomplete_cause(stream: OperationStream[JsonValue]) -> BaseException:
    with pytest.raises(IncompleteTraversalError) as captured:
        _ = [row async for row in stream]
    assert captured.value.__cause__ is not None
    return captured.value.__cause__


# --- repeated pages -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_identityless_repeated_page_fails_closed_and_names_the_remedy() -> None:
    # Two content-equal pages are indistinguishable from a portal that ignored ``start``.
    rows: list[dict[str, object]] = [{"STAGE_ID": stage} for stage in ("NEW", "NEW", "NEW", "NEW", "WON")]
    stream = client_for(_bitrix_offset(rows)).iter_list(_LIST, page_size=2)

    cause = await _incomplete_cause(stream)

    assert isinstance(cause, PaginationError)
    assert "unique identity" in str(cause)
    assert _report(stream).page_trace[-1].rejection_code is PageRejectionCode.REPEATED_FINGERPRINT


@pytest.mark.asyncio
async def test_unique_identity_admits_rows_that_repeat_outside_it() -> None:
    rows: list[dict[str, object]] = [{"ID": index, "STAGE_ID": "NEW"} for index in range(1, 5)]
    stream = client_for(_bitrix_offset(rows)).iter_list(_LIST, page_size=2, identity=_ID)

    assert [row async for row in stream] == rows
    assert _report(stream).assurance is TraversalAssurance.IDENTITY_EXACT


# --- server-next closure --------------------------------------------------------------------------


def test_server_next_without_exact_total_is_refused_at_construction() -> None:
    with pytest.raises(ValueError, match="EXACT_QUALIFIED"):
        OffsetSpec(continuation=OffsetContinuation.SERVER_NEXT)
    with pytest.raises(ValueError, match="EXACT_QUALIFIED"):
        OffsetSpec(continuation=OffsetContinuation.SERVER_NEXT, total_termination=TotalTermination.DISABLED)


@pytest.mark.asyncio
async def test_server_next_with_exact_total_closes_on_the_bitrix_last_page() -> None:
    rows: list[dict[str, object]] = [{"ID": index} for index in range(1, 6)]
    transport = _bitrix_offset(rows)
    offset = OffsetSpec(
        continuation=OffsetContinuation.SERVER_NEXT,
        total_termination=TotalTermination.EXACT_QUALIFIED,
    )
    stream = client_for(transport).iter_list(_LIST, page_size=2, identity=_ID, offset=offset)

    assert [row async for row in stream] == rows
    assert _report(stream).terminal_reason == "qualified total reached"
    assert len(transport.requests) == len(range(0, len(rows), 2))


# --- cursor identity independent of the cursor field ----------------------------------------------

_CURSOR = CursorSpec(
    parameter_path=ParameterPath(("LAST_ID",)),
    item_path=("ID",),
    coercion=IdentityCoercion.EXACT_INTEGER,
    direction="ascending",
    take="last",
)
_XML_ID = IdentitySpec(("XML_ID",), "XML_ID", "XML_ID", IdentityCoercion.EXACT_STRING)
_SELECT_ITEMS = ResultSelector(("items",))


def _cursor_portal(data: list[dict[str, object]]) -> ResponderTransport:
    def respond(request: Request) -> object:
        last = int(str(request.copy_parameters().get("LAST_ID", 0)))
        return {"result": {"items": [row for row in data if int(str(row["ID"])) > last][:2]}}

    return ResponderTransport(respond)


_UNORDERED_XML = [{"ID": index, "XML_ID": xml} for index, xml in zip(range(1, 6), "qbzam", strict=True)]


@pytest.mark.asyncio
async def test_cursor_identity_on_another_field_needs_no_order_of_its_own() -> None:
    stream = client_for(_cursor_portal(_UNORDERED_XML)).iter_list_cursor(
        Request("x.list", route=RouteKind.BARE),
        selector=_SELECT_ITEMS,
        cursor=_CURSOR,
        identity=_XML_ID,
        page_size=2,
    )

    assert [row async for row in stream] == _UNORDERED_XML
    assert _report(stream).assurance is TraversalAssurance.IDENTITY_EXACT


@pytest.mark.asyncio
async def test_cursor_identity_on_another_field_still_rejects_duplicates() -> None:
    data = [{"ID": 1, "XML_ID": "a"}, {"ID": 2, "XML_ID": "b"}, {"ID": 3, "XML_ID": "a"}]
    stream = client_for(_cursor_portal(data)).iter_list_cursor(
        Request("x.list", route=RouteKind.BARE),
        selector=_SELECT_ITEMS,
        cursor=_CURSOR,
        identity=_XML_ID,
        page_size=2,
    )

    cause = await _incomplete_cause(stream)

    assert "duplicate identity" in str(cause)


@pytest.mark.asyncio
async def test_cursor_order_is_still_checked_with_a_foreign_identity() -> None:
    def respond(_request: Request) -> object:
        return {"result": {"items": [{"ID": 2, "XML_ID": "a"}, {"ID": 1, "XML_ID": "b"}]}}

    stream = client_for(ResponderTransport(respond)).iter_list_cursor(
        Request("x.list", route=RouteKind.BARE),
        selector=_SELECT_ITEMS,
        cursor=_CURSOR,
        identity=_XML_ID,
        page_size=2,
    )

    cause = await _incomplete_cause(stream)

    assert "strictly ascending" in str(cause)


@pytest.mark.asyncio
async def test_cursor_identity_on_the_cursor_path_with_another_coercion_has_no_order() -> None:
    # Numeric cursor order 9 < 10 is not string identity order "9" > "10".
    data: list[dict[str, object]] = [{"ID": str(index)} for index in (8, 9, 10, 11)]
    cursor = CursorSpec(
        parameter_path=ParameterPath(("LAST_ID",)),
        item_path=("ID",),
        coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
        direction="ascending",
        take="last",
    )
    stream = client_for(_cursor_portal(data)).iter_list_cursor(
        Request("x.list", route=RouteKind.BARE),
        selector=_SELECT_ITEMS,
        cursor=cursor,
        identity=IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_STRING),
        page_size=2,
    )

    assert [row async for row in stream] == data


@pytest.mark.asyncio
async def test_cursor_references_accept_an_identity_on_another_field() -> None:
    stream = client_for(_cursor_portal(_UNORDERED_XML)).iter_cursors(
        Request("x.list", route=RouteKind.BARE),
        [Binding("parent", (), "parent")],
        selector=_SELECT_ITEMS,
        cursor=_CURSOR,
        identity=_XML_ID,
        page_size=2,
        dispatch=DirectDispatch(),
    )

    events = [event async for event in stream]

    assert [event.item for event in events if isinstance(event, ReferenceItem)] == _UNORDERED_XML


# --- typed identity contract errors ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("row", "observed"),
    [({"TITLE": "no id"}, "missing"), ({"ID": "1"}, "string"), ({"ID": None}, "null")],
)
@pytest.mark.asyncio
async def test_row_breaking_its_identity_raises_identity_contract_error(row: dict[str, object], observed: str) -> None:
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 1}, row]})
    stream = client_for(transport).iter_list(_LIST, identity=_ID)

    cause = await _incomplete_cause(stream)

    assert isinstance(cause, IdentityContractError)
    assert isinstance(cause, PaginationError)
    assert (cause.path, cause.observed_type, cause.row_offset) == (("ID",), observed, 1)
    assert _report(stream).page_trace[-1].rejection_code is PageRejectionCode.IDENTITY_CONTRACT


class _Doubling:
    """Read every row's identity: an adapter must never see a row whose identity is unreadable."""

    def __init__(self) -> None:
        self.pages = 0

    def adapt(self, page: PageView, /) -> AdaptedPage:
        self.pages += 1
        rows = [row for row in (thaw_json(item) for item in page.items) if isinstance(row, dict)]
        return AdaptedPage([{**row, "K": cast("int", row["ID"]) * 2} for row in rows])


class _Renumber:
    """Change a valid identity: an adapter may not touch traversal tokens."""

    def adapt(self, page: PageView, /) -> AdaptedPage:
        rows = [row for row in (thaw_json(item) for item in page.items) if isinstance(row, dict)]
        return AdaptedPage([{**row, "ID": cast("int", row["ID"]) + 100} for row in rows])


class _DropIdentity:
    """Remove a valid identity: an adapter may not touch traversal tokens."""

    def adapt(self, page: PageView, /) -> AdaptedPage:
        rows = [row for row in (thaw_json(item) for item in page.items) if isinstance(row, dict)]
        return AdaptedPage([{key: value for key, value in row.items() if key != "ID"} for row in rows])


@pytest.mark.asyncio
async def test_adapter_never_runs_on_a_row_breaking_its_identity() -> None:
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 1}, {"TITLE": "no id"}]})
    adapter = _Doubling()
    stream = client_for(transport).iter_list(_LIST, identity=_ID, page_adapter=adapter)

    cause = await _incomplete_cause(stream)

    assert isinstance(cause, IdentityContractError)
    assert (cause.observed_type, cause.row_offset) == ("missing", 1)
    assert adapter.pages == 0
    assert _report(stream).page_trace[-1].rejection_code is PageRejectionCode.IDENTITY_CONTRACT


@pytest.mark.asyncio
async def test_adapter_never_runs_on_a_row_breaking_its_cursor() -> None:
    rows: list[dict[str, object]] = [{"ID": 1, "XML_ID": "a"}, {"ID": None, "XML_ID": "b"}]
    adapter = _Doubling()
    transport = ResponderTransport(lambda _request: {"result": {"items": rows}})
    stream = client_for(transport).iter_list_cursor(
        Request("x.list", route=RouteKind.BARE),
        selector=_SELECT_ITEMS,
        cursor=_CURSOR,
        identity=_XML_ID,
        page_size=2,
        page_adapter=adapter,
    )

    cause = await _incomplete_cause(stream)

    assert not isinstance(cause, IdentityContractError)
    assert "cursor path is missing" in str(cause)
    assert adapter.pages == 0


@pytest.mark.parametrize("adapter", [_Renumber(), _DropIdentity()], ids=["renumber", "drop"])
@pytest.mark.asyncio
async def test_adapter_changing_a_valid_identity_is_an_adaptation_violation(adapter: PageAdapter) -> None:
    transport = ResponderTransport(lambda _request: {"result": [{"ID": 1}, {"ID": 2}]})
    stream = client_for(transport).iter_list(_LIST, identity=_ID, page_adapter=adapter)

    with pytest.raises(PageAdaptationError) as captured:
        _ = [row async for row in stream]

    assert captured.value.violation is PageAdaptationViolation.ORDER_OR_IDENTITY_CHANGED
    assert _report(stream).page_trace[-1].rejection_code is PageRejectionCode.PAGE_ADAPTATION


# --- canonical decimal identities -----------------------------------------------------------------


@pytest.mark.parametrize("wire", ["012", "00", " 12", "12 ", "12\n", "1_2", "+12", "-0", "-012", "١٢", "", "-"])
@pytest.mark.asyncio
async def test_decimal_identity_refuses_non_canonical_strings(wire: str) -> None:
    transport = ResponderTransport(lambda _request: {"result": [{"ID": wire}]})
    stream = client_for(transport).iter_list(_LIST, identity=_DECIMAL_ID)

    cause = await _incomplete_cause(stream)

    assert isinstance(cause, IdentityContractError)
    assert cause.observed_type == "string"


@pytest.mark.asyncio
async def test_decimal_identity_admits_canonical_strings_and_integers() -> None:
    rows: list[dict[str, object]] = [{"ID": "0"}, {"ID": "12"}, {"ID": "-7"}, {"ID": 30}]
    transport = ResponderTransport(lambda request: {"result": rows if request.copy_parameters()["start"] == 0 else []})
    stream = client_for(transport).iter_list(_LIST, identity=_DECIMAL_ID)

    assert [row async for row in stream] == rows
    assert _report(stream).unique_rows == len(rows)


_DECIMAL_CURSOR = CursorSpec(
    parameter_path=ParameterPath(("LAST_ID",)),
    item_path=("ID",),
    coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
    direction="ascending",
    take="last",
)


@pytest.mark.asyncio
async def test_decimal_cursor_identity_refuses_a_leading_zero() -> None:
    # Without another identity the cursor is the identity: the typed identity error comes first.
    stream = client_for(_cursor_portal([{"ID": "1"}, {"ID": "02"}])).iter_list_cursor(
        Request("x.list", route=RouteKind.BARE),
        selector=_SELECT_ITEMS,
        cursor=_DECIMAL_CURSOR,
        page_size=2,
    )

    cause = await _incomplete_cause(stream)

    assert isinstance(cause, IdentityContractError)
    assert (cause.path, cause.coercion, cause.row_offset) == (("ID",), IdentityCoercion.DECIMAL_STRING_INTEGER, 1)
    assert _report(stream).page_trace[-1].rejection_code is PageRejectionCode.IDENTITY_CONTRACT


@pytest.mark.asyncio
async def test_cursor_value_breaking_its_coercion_beside_a_valid_identity_stays_a_cursor_error() -> None:
    data: list[dict[str, object]] = [{"ID": "1", "XML_ID": "a"}, {"ID": "02", "XML_ID": "b"}]
    stream = client_for(_cursor_portal(data)).iter_list_cursor(
        Request("x.list", route=RouteKind.BARE),
        selector=_SELECT_ITEMS,
        cursor=_DECIMAL_CURSOR,
        identity=_XML_ID,
        page_size=2,
    )

    cause = await _incomplete_cause(stream)

    assert not isinstance(cause, IdentityContractError)
    assert "cursor_coercion" in str(cause)


# --- the same typed identity error on every traversal ---------------------------------------------


@pytest.mark.asyncio
async def test_sequential_keyset_row_breaking_its_identity_raises_identity_contract_error() -> None:
    def respond(request: Request) -> object:
        bound = request.copy_parameters().get("filter", {})
        assert isinstance(bound, dict)
        return {"result": [{"ID": 1}, {"ID": "2"}] if ">ID" not in bound else []}

    stream = client_for(ResponderTransport(respond)).iter_list_keyset(
        _LIST,
        selector=ResultSelector.root(),
        identity=_ID,
        page_size=2,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
        execution=SequentialKeysetExecution(),
    )

    cause = await _incomplete_cause(stream)

    assert isinstance(cause, IdentityContractError)
    assert (cause.observed_type, cause.row_offset) == ("string", 1)
    assert _report(stream).page_trace[-1].rejection_code is PageRejectionCode.IDENTITY_CONTRACT


@pytest.mark.asyncio
async def test_counted_tail_row_breaking_its_identity_raises_identity_contract_error() -> None:
    rows: list[dict[str, object]] = [{"ID": index} for index in range(1, 7)]
    rows[4] = {"ID": "5"}

    def answer(method: str, query: Query) -> Envelope:
        del method
        start = int(query.get("start", "0"))
        envelope: Envelope = {"result": rows[start : start + 2], "total": len(rows)}
        if start + 2 < len(rows):
            envelope["next"] = start + 2
        return envelope

    stream = client_for(BatchPortal(answer)).iter_list_counted(_LIST, identity=_ID, page_size=2)

    cause = await _incomplete_cause(stream)

    assert isinstance(cause, IdentityContractError)
    assert (cause.observed_type, cause.row_offset) == ("string", 4)


@pytest.mark.asyncio
async def test_reference_row_breaking_its_identity_fails_its_binding_with_identity_contract_error() -> None:
    def answer(method: str, query: Query) -> Envelope:
        del method
        if query.get("start", "0") != "0":
            return {"result": []}
        broken = query.get("filter[=parent]") == "broken"
        return {"result": [{"ID": 1}, {"TITLE": "no id"} if broken else {"ID": 2}]}

    stream = client_for(BatchPortal(answer)).iter_reference_outcomes(
        Request("crm.deal.list", {"filter": {"=parent": "base"}}, route=RouteKind.BARE),
        [_parent("honest"), _parent("broken")],
        traversal=SequentialTraversal(identity=_ID, page_size=2),
    )

    outcomes = [outcome async for outcome in stream]

    failures = [outcome for outcome in outcomes if isinstance(outcome, ReferenceFailure)]
    assert [failure.correlation for failure in failures] == ["broken"]
    cause = failures[0].error.__cause__
    assert isinstance(cause, IdentityContractError)
    assert (cause.observed_type, cause.row_offset) == ("missing", 1)


def _parent(name: str) -> Binding[str]:
    return Binding(name, (ParameterUpdate(ParameterPath(("filter", "=parent")), name),), name)
