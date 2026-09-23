"""Qualified raw bounds traverse empty selected pages without false closure."""

from __future__ import annotations
import json

import pytest

from b24api import (
    Bitrix24,
    IdentityCoercion,
    IdentitySpec,
    OffsetSpec,
    PageStride,
    ParameterPath,
    RawTotalSource,
    Request,
    ResultSelector,
    RouteKind,
    SparseRawBound,
    TraversalAssurance,
)
from b24api.contracts.traversal import OffsetContinuation
from b24api.errors import BudgetExceededError, IncompleteTraversalError, PaginationError
from b24api.execution import Executor, WireResponse


class SparseTransport:
    """Independent raw SQL offset oracle with sparse selected output."""

    def __init__(
        self,
        *,
        raw_total: int = 200,
        overrides: dict[int, int | None] | None = None,
        envelope: bool = False,
    ) -> None:
        """Freeze one raw extent for the synthetic fixture, in the result or the envelope."""
        self.raw_total = raw_total
        self.overrides = overrides or {}
        self.envelope = envelope
        self.offsets: list[int] = []

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Return selected rows and a separate raw extent."""
        assert attempt_timeout > 0
        assert max_response_bytes > 0
        offset = request.copy_parameters()["start"]
        assert isinstance(offset, int)
        self.offsets.append(offset)
        rows = {0: [{"id": 1}], 150: [{"id": 2}]}.get(offset, [])
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


@pytest.mark.asyncio
async def test_non_sparse_fixed_stride_rejects_an_unexplained_short_window() -> None:
    short_window_extent = 100

    class ShortWindowTransport:
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
