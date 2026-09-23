"""Qualified raw bounds traverse empty selected pages without false closure."""

from __future__ import annotations
import json

import pytest

from b24api import (
    Binding,
    Bitrix24,
    DirectDispatch,
    IdentityCoercion,
    IdentitySpec,
    OffsetSpec,
    PageStride,
    ParameterPath,
    RawTotalSource,
    Request,
    ResultSelector,
    RouteKind,
    SequentialTraversal,
    SparseRawBound,
    TerminalState,
    TraversalAssurance,
)
from b24api.contracts.traversal import OffsetContinuation
from b24api.errors import BudgetExceededError, CapabilityError, IncompleteTraversalError, PaginationError
from b24api.execution import Executor, WireResponse


class SparseTransport:
    """Independent raw SQL offset oracle with sparse selected output."""

    host = "fixture.invalid"

    def __init__(
        self,
        *,
        raw_total: int = 200,
        overrides: dict[int, int | None] | None = None,
        envelope: bool = False,
        rows: dict[int, list[dict[str, int]]] | None = None,
    ) -> None:
        """Freeze one raw extent for the synthetic fixture, in the result or the envelope."""
        self.raw_total = raw_total
        self.overrides = overrides or {}
        self.rows = {0: [{"id": 1}], 150: [{"id": 2}]} if rows is None else rows
        self.envelope = envelope
        self.offsets: list[int] = []

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Return selected rows and a separate raw extent."""
        assert attempt_timeout > 0
        assert max_response_bytes > 0
        offset = request.copy_parameters()["start"]
        assert isinstance(offset, int)
        self.offsets.append(offset)
        rows = self.rows.get(offset, [])
        result: dict[str, object] = {"items": rows}
        total = self.overrides.get(offset, self.raw_total)
        payload: dict[str, object] = {"result": result}
        if total is not None:
            (payload if self.envelope else result)["total" if self.envelope else "rawTotal"] = total
        body = json.dumps(payload).encode()
        return WireResponse(200, (), body)


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
async def test_sparse_offset_crosses_two_empty_selected_pages() -> None:
    transport = SparseTransport()
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    stream = client.iter_list(
        Request("example.search", route=RouteKind.BARE),
        selector=ResultSelector(("items",)),
        page_size=50,
        offset=_sparse_spec(),
    )
    rows = [row async for row in stream]
    assert [row["id"] for row in rows] == [1, 2]
    assert transport.offsets == [0, 50, 100, 150]
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
    raw_total: int,
    rows: dict[int, list[dict[str, int]]],
    offsets: list[int],
) -> None:
    transport = SparseTransport(raw_total=raw_total, rows=rows)
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
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
    assert transport.offsets == offsets
    assert stream.report is not None
    assert stream.report.state is TerminalState.INCOMPLETE
    assert not stream.report.exhausted
    # An incomplete report never carries the strength its plan would have proved on completion.
    assert stream.report.assurance is TraversalAssurance.MECHANICS_ONLY


@pytest.mark.asyncio
async def test_sparse_terminal_window_holding_its_remaining_raw_rows_closes() -> None:
    transport = SparseTransport(raw_total=51, rows={0: [{"id": 1}], 50: [{"id": 2}]})
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    stream = client.iter_list(
        Request("example.search", route=RouteKind.BARE),
        selector=ResultSelector(("items",)),
        page_size=50,
        offset=_sparse_spec(),
    )
    assert [row["id"] async for row in stream] == [1, 2]
    assert transport.offsets == [0, 50]
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
async def test_sparse_resume_alignment_before_io(start: int, message: str) -> None:
    # 932 aliases the server's floor-to-50 window; an aligned 900 is still only part of the raw
    # range, which cannot prove raw-range coverage. Both are refused before the first send.
    transport = SparseTransport(raw_total=1050)
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    stream = client.iter_list(_resumed(start), selector=ResultSelector(("items",)), page_size=50, offset=_sparse_spec())
    with pytest.raises(CapabilityError, match=message):
        _ = [row async for row in stream]
    assert stream.report is not None
    assert not stream.report.exhausted
    assert transport.offsets == []


@pytest.mark.parametrize("start", [0, 900, 932])
def test_sparse_raw_bound_is_refused_for_reference_traversal_before_io(start: int) -> None:
    # An empty raw window is continued past without a delivered page, which reference provenance
    # cannot record, so every origin is refused when the stream is built.
    transport = SparseTransport()
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    with pytest.raises(CapabilityError, match=_NO_SPARSE_REFERENCE):
        client.iter_reference_outcomes(
            _resumed(start),
            [Binding("one", (), "one")],
            traversal=SequentialTraversal(page_size=50, selector=ResultSelector(("items",)), offset=_sparse_spec()),
            dispatch=DirectDispatch(concurrency=1),
        )
    assert transport.offsets == []


@pytest.mark.asyncio
async def test_non_sparse_fixed_stride_rejects_an_unexplained_short_window() -> None:
    short_window_extent = 100

    class ShortWindowTransport:
        host = "fixture.invalid"

        def __init__(self) -> None:
            self.offsets: list[int] = []

        async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
            del attempt_timeout, max_response_bytes
            offset = request.copy_parameters()["start"]
            assert isinstance(offset, int)
            self.offsets.append(offset)
            rows = [{"id": value} for value in range(offset, offset + 25)] if offset < short_window_extent else []
            return WireResponse(200, (), json.dumps({"result": rows}).encode())

    transport = ShortWindowTransport()
    stride = PageStride(server_granularity=50, wire_increment=50, max_decoded_rows=50)
    stream = Bitrix24._from_executor(Executor(transport)).iter_list(  # noqa: SLF001
        Request("example.search", route=RouteKind.BARE),
        identity=IdentitySpec(("id",), "ID", "ID", IdentityCoercion.EXACT_INTEGER),
        page_size=50,
        offset=OffsetSpec(continuation=OffsetContinuation.FIXED_STEP, step=50, page_stride=stride),
    )

    with pytest.raises(IncompleteTraversalError) as captured:
        await anext(stream)

    assert isinstance(captured.value.error, PaginationError)
    assert transport.offsets == [0]
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
    overrides: dict[int, int | None],
) -> None:
    transport = SparseTransport(overrides=overrides)
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
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
async def test_sparse_raw_total_can_come_from_the_response_envelope() -> None:
    transport = SparseTransport(raw_total=150, envelope=True)
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    stream = client.iter_list(
        Request("example.search", route=RouteKind.BARE),
        selector=ResultSelector(("items",)),
        page_size=50,
        offset=_sparse_spec(envelope=True),
    )
    rows = [row async for row in stream]
    assert [row["id"] for row in rows] == [1]
    assert transport.offsets == [0, 50, 100]
    assert stream.report is not None
    assert stream.report.exhausted
    assert stream.report.assurance is TraversalAssurance.RAW_RANGE_COVERED


@pytest.mark.asyncio
@pytest.mark.parametrize("overrides", [{50: None}, {50: -1}, {50: 201}])
async def test_sparse_envelope_total_missing_unknown_or_changed_never_completes(
    overrides: dict[int, int | None],
) -> None:
    transport = SparseTransport(overrides=overrides, envelope=True)
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
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
async def test_sparse_raw_page_budget_prevents_unbounded_holes() -> None:
    transport = SparseTransport()
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    stream = client.iter_list(
        Request("example.search", route=RouteKind.BARE),
        selector=ResultSelector(("items",)),
        page_size=50,
        offset=_sparse_spec(max_pages=2),
    )
    with pytest.raises(BudgetExceededError):
        _ = [row async for row in stream]
    assert transport.offsets == [0, 50]
