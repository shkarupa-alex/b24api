"""Qualified raw bounds traverse empty selected pages without false closure."""

from __future__ import annotations
from typing import TYPE_CHECKING

import pytest

from b24api import (
    Binding,
    DirectDispatch,
    IdentityCoercion,
    IdentitySpec,
    OffsetSpec,
    ParameterPath,
    Request,
    ResultSelector,
    RouteKind,
    SequentialTraversal,
    TerminalState,
    TraversalAssurance,
)
from b24api.contracts import PageStride, RawTotalSource, SparseRawBound
from b24api.contracts.traversal import OffsetContinuation
from b24api.errors import BudgetExceededError, CapabilityError, IncompleteTraversalError, PaginationError
from tests.scripting import ResponderTransport, client_for

if TYPE_CHECKING:
    from tests.scripting import ClientFactory


def _sparse(
    *,
    raw_total: int = 200,
    overrides: dict[int, int | None] | None = None,
    envelope: bool = False,
    rows: dict[int, list[dict[str, int]]] | None = None,
) -> ResponderTransport:
    """Independent raw SQL offset oracle with sparse selected output and one frozen raw extent.

    The extent travels in the result as ``rawTotal`` or, with ``envelope``, as the envelope ``total``.
    """
    selected = {0: [{"id": 1}], 150: [{"id": 2}]} if rows is None else rows

    def respond(request: Request) -> object:
        offset = request.copy_parameters()["start"]
        assert isinstance(offset, int)
        result: dict[str, object] = {"items": selected.get(offset, [])}
        payload: dict[str, object] = {"result": result}
        total = (overrides or {}).get(offset, raw_total)
        if total is not None:
            (payload if envelope else result)["total" if envelope else "rawTotal"] = total
        return payload

    return ResponderTransport(respond)


def _offsets(transport: ResponderTransport) -> list[object]:
    return [parameters["start"] for parameters in transport.parameters]


def _sparse_spec(*, max_pages: int = 4, envelope: bool = False) -> OffsetSpec:
    stride = PageStride(server_granularity=50, wire_increment=50, max_decoded_rows=50)
    return OffsetSpec(
        continuation=OffsetContinuation.FIXED_STEP,
        step=50,
        page_stride=stride,
        sparse_raw_bound=SparseRawBound(
            RawTotalSource.ENVELOPE if envelope else ResultSelector(("rawTotal",)),
            stride,
            max_pages,
            "qualified stable raw ID order",
        ),
    )


@pytest.mark.asyncio
async def test_sparse_offset_crosses_two_empty_selected_pages(scripted_client: ClientFactory) -> None:
    transport = _sparse()
    client = scripted_client(transport)
    stream = client.iter_list(
        Request("example.search", route=RouteKind.BARE),
        selector=ResultSelector(("items",)),
        page_size=50,
        offset=_sparse_spec(),
    )
    rows = [row async for row in stream]
    assert [row["id"] for row in rows] == [1, 2]
    assert _offsets(transport) == [0, 50, 100, 150]
    assert stream.report is not None
    assert stream.report.exhausted
    assert stream.report.assurance is TraversalAssurance.RAW_RANGE_COVERED


@pytest.mark.parametrize(
    ("raw_total", "rows", "offsets"),
    [
        pytest.param(0, {0: [{"id": 1}]}, [0], id="total-below-selected"),
        pytest.param(51, {0: [{"id": 1}], 50: [{"id": 2}, {"id": 3}]}, [0, 50], id="terminal-window-overflow"),
    ],
)
@pytest.mark.asyncio
async def test_sparse_raw_total_contradicted_by_selected_rows_fails_closed(
    raw_total: int, rows: dict[int, list[dict[str, int]]], offsets: list[int], scripted_client: ClientFactory
) -> None:
    transport = _sparse(raw_total=raw_total, rows=rows)
    client = scripted_client(transport)
    stream = client.iter_list(
        Request("example.search", route=RouteKind.BARE),
        selector=ResultSelector(("items",)),
        page_size=50,
        offset=_sparse_spec(),
    )
    with pytest.raises(IncompleteTraversalError) as captured:
        _ = [row async for row in stream]
    assert isinstance(captured.value.error, PaginationError)
    assert "exceed the qualified raw window" in str(captured.value.error)
    assert _offsets(transport) == offsets
    assert stream.report is not None
    assert stream.report.state is TerminalState.INCOMPLETE
    assert not stream.report.exhausted
    # An incomplete report never carries the strength its plan would have proved on completion.
    assert stream.report.assurance is TraversalAssurance.MECHANICS_ONLY


@pytest.mark.asyncio
async def test_sparse_terminal_window_holding_its_remaining_raw_rows_closes(scripted_client: ClientFactory) -> None:
    transport = _sparse(raw_total=51, rows={0: [{"id": 1}], 50: [{"id": 2}]})
    client = scripted_client(transport)
    stream = client.iter_list(
        Request("example.search", route=RouteKind.BARE),
        selector=ResultSelector(("items",)),
        page_size=50,
        offset=_sparse_spec(),
    )
    assert [row["id"] async for row in stream] == [1, 2]
    assert _offsets(transport) == [0, 50]
    assert stream.report is not None
    assert stream.report.exhausted
    assert stream.report.assurance is TraversalAssurance.RAW_RANGE_COVERED


_MISALIGNED = "align with the qualified server page granularity"
_NOT_FROM_ZERO = "complete range from offset zero"
_NO_SPARSE_REFERENCE = "reference traversal does not support a sparse raw bound"


def _resumed(start: int) -> Request:
    return Request("example.search", {"start": start}, route=RouteKind.BARE)


@pytest.mark.parametrize(("start", "message"), [(932, _MISALIGNED), (900, _NOT_FROM_ZERO)])
@pytest.mark.asyncio
async def test_sparse_resume_alignment_before_io(start: int, message: str, scripted_client: ClientFactory) -> None:
    # 932 aliases the server's floor-to-50 window; an aligned 900 is still only part of the raw
    # range, which cannot prove raw-range coverage. Both are refused before the first send.
    transport = _sparse(raw_total=1050)
    client = scripted_client(transport)
    stream = client.iter_list(_resumed(start), selector=ResultSelector(("items",)), page_size=50, offset=_sparse_spec())
    with pytest.raises(CapabilityError, match=message):
        _ = [row async for row in stream]
    assert stream.report is not None
    assert not stream.report.exhausted
    assert _offsets(transport) == []


@pytest.mark.parametrize("start", [0, 900, 932])
def test_sparse_raw_bound_is_refused_for_reference_traversal_before_io(start: int) -> None:
    # An empty raw window is continued past without a delivered page, which reference provenance
    # cannot record, so every origin is refused when the stream is built.
    transport = _sparse()
    client = client_for(transport)
    with pytest.raises(CapabilityError, match=_NO_SPARSE_REFERENCE):
        client.iter_reference_outcomes(
            _resumed(start),
            [Binding("one", (), "one")],
            traversal=SequentialTraversal(page_size=50, selector=ResultSelector(("items",)), offset=_sparse_spec()),
            dispatch=DirectDispatch(concurrency=1),
        )
    assert _offsets(transport) == []


@pytest.mark.asyncio
async def test_non_sparse_fixed_stride_rejects_an_unexplained_short_window(scripted_client: ClientFactory) -> None:
    short_window_extent = 100

    def short_window(request: Request) -> object:
        offset = request.copy_parameters()["start"]
        assert isinstance(offset, int)
        return {
            "result": [{"id": value} for value in range(offset, offset + 25)] if offset < short_window_extent else []
        }

    transport = ResponderTransport(short_window)
    stride = PageStride(server_granularity=50, wire_increment=50, max_decoded_rows=50)
    stream = scripted_client(transport).iter_list(
        Request("example.search", route=RouteKind.BARE),
        identity=IdentitySpec(("id",), "ID", "ID", IdentityCoercion.EXACT_INTEGER),
        page_size=50,
        offset=OffsetSpec(continuation=OffsetContinuation.FIXED_STEP, step=50, page_stride=stride),
    )

    with pytest.raises(IncompleteTraversalError) as captured:
        await anext(stream)

    assert isinstance(captured.value.error, PaginationError)
    assert _offsets(transport) == [0]
    assert stream.report is not None
    assert not stream.report.exhausted


def test_stride_rejects_rounded_alias_and_sparse_budget_is_explicit() -> None:
    with pytest.raises(ValueError, match="align"):
        PageStride(server_granularity=50, wire_increment=25, max_decoded_rows=50)
    with pytest.raises(ValueError, match="page_stride"):
        OffsetSpec(
            sparse_raw_bound=SparseRawBound(
                ResultSelector(("rawTotal",)),
                PageStride(server_granularity=50, wire_increment=50, max_decoded_rows=50),
                4,
                "stable raw order",
            ),
        )
    assert _sparse_spec(max_pages=4).parameter_path == ParameterPath(("start",))


@pytest.mark.asyncio
@pytest.mark.parametrize("overrides", [{50: None}, {50: -1}, {50: 201}])
async def test_sparse_raw_total_missing_invalid_or_changed_never_completes(
    overrides: dict[int, int | None], scripted_client: ClientFactory
) -> None:
    transport = _sparse(overrides=overrides)
    client = scripted_client(transport)
    stream = client.iter_list(
        Request("example.search", route=RouteKind.BARE),
        selector=ResultSelector(("items",)),
        page_size=50,
        offset=_sparse_spec(),
    )
    with pytest.raises(IncompleteTraversalError) as captured:
        _ = [row async for row in stream]
    assert isinstance(captured.value.__cause__, PaginationError)
    assert stream.report is not None
    assert not stream.report.exhausted


@pytest.mark.asyncio
async def test_sparse_raw_total_can_come_from_the_response_envelope(scripted_client: ClientFactory) -> None:
    transport = _sparse(raw_total=150, envelope=True)
    client = scripted_client(transport)
    stream = client.iter_list(
        Request("example.search", route=RouteKind.BARE),
        selector=ResultSelector(("items",)),
        page_size=50,
        offset=_sparse_spec(envelope=True),
    )
    rows = [row async for row in stream]
    assert [row["id"] for row in rows] == [1]
    assert _offsets(transport) == [0, 50, 100]
    assert stream.report is not None
    assert stream.report.exhausted
    assert stream.report.assurance is TraversalAssurance.RAW_RANGE_COVERED


@pytest.mark.asyncio
@pytest.mark.parametrize("overrides", [{50: None}, {50: -1}, {50: 201}])
async def test_sparse_envelope_total_missing_unknown_or_changed_never_completes(
    overrides: dict[int, int | None], scripted_client: ClientFactory
) -> None:
    transport = _sparse(overrides=overrides, envelope=True)
    client = scripted_client(transport)
    stream = client.iter_list(
        Request("example.search", route=RouteKind.BARE),
        selector=ResultSelector(("items",)),
        page_size=50,
        offset=_sparse_spec(envelope=True),
    )
    with pytest.raises(IncompleteTraversalError) as captured:
        _ = [row async for row in stream]
    assert isinstance(captured.value.__cause__, PaginationError)
    assert stream.report is not None
    assert not stream.report.exhausted


def test_sparse_raw_total_rejects_a_root_result_path() -> None:
    stride = PageStride(server_granularity=50, wire_increment=50, max_decoded_rows=50)
    with pytest.raises(ValueError, match="envelope total"):
        SparseRawBound(ResultSelector(()), stride, 2, "qualified stable raw ID order")


@pytest.mark.asyncio
async def test_sparse_raw_page_budget_prevents_unbounded_holes(scripted_client: ClientFactory) -> None:
    transport = _sparse()
    client = scripted_client(transport)
    stream = client.iter_list(
        Request("example.search", route=RouteKind.BARE),
        selector=ResultSelector(("items",)),
        page_size=50,
        offset=_sparse_spec(max_pages=2),
    )
    with pytest.raises(BudgetExceededError):
        _ = [row async for row in stream]
    assert _offsets(transport) == [0, 50]
