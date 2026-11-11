"""An early close or a non-``Exception`` exit publishes one report without a false protocol violation."""

from __future__ import annotations
import asyncio
from typing import TYPE_CHECKING, Any

import pytest

from b24api import ReplaySafety, Request, TerminalState
from b24api.contracts import PartialResult
from tests.scripting import BatchPortal, ResponderTransport, client_for

if TYPE_CHECKING:
    from b24api.contracts import OperationReport, OperationStream
    from tests.scripting import Envelope, Query

WIDTH = 50
ROWS = 300
_REQUEST = Request.bare("crm.item.list", {}, ReplaySafety.SAFE)


def _counted(*, slow_from: int | None = None) -> BatchPortal:
    """Serve ``ROWS`` rows with an exact total; pages from ``slow_from`` stay in flight for a while."""

    async def answer(method: str, query: Query) -> Envelope:
        del method
        offset = int(query.get("start", "0"))
        if slow_from is not None and offset >= slow_from:
            await asyncio.sleep(0.05)
        envelope: Envelope = {
            "result": [{"ID": row} for row in range(offset, min(offset + WIDTH, ROWS))],
            "total": ROWS,
        }
        if offset + WIDTH < ROWS:
            envelope["next"] = offset + WIDTH
        return envelope

    return BatchPortal(answer)


def _report(stream: OperationStream[Any]) -> OperationReport:
    report = stream.report
    assert report is not None
    return report


async def _break_after(stream: OperationStream[Any], rows: int) -> None:
    async with stream:
        seen = 0
        async for _ in stream:
            seen += 1
            if seen == rows:
                break


@pytest.mark.parametrize(
    ("rows", "slow_from"),
    [(10, None), (60, None), (60, 2 * WIDTH), (260, None)],
    ids=["head", "tail-page", "tail-page-with-pages-in-flight", "last-page"],
)
@pytest.mark.asyncio
async def test_a_counted_break_on_any_page_publishes_an_early_closed_report(rows: int, slow_from: int | None) -> None:
    before = asyncio.all_tasks()
    async with client_for(_counted(slow_from=slow_from)) as client:
        stream = client.iter_list_counted(_REQUEST, page_size=WIDTH)
        await _break_after(stream, rows)

    report = _report(stream)
    assert report.state is TerminalState.EARLY_CLOSED
    assert report.violations == ()
    assert not report.exhausted
    assert [task for task in asyncio.all_tasks() - before if not task.done()] == []


@pytest.mark.asyncio
async def test_a_counted_collect_with_a_limit_inside_the_tail_publishes_its_report() -> None:
    async with client_for(_counted(slow_from=2 * WIDTH)) as client:
        stream = client.iter_list_counted(_REQUEST, page_size=WIDTH)
        result = await stream.collect(limit=60)

    assert isinstance(result, PartialResult)
    assert len(result.value) == 60  # noqa: PLR2004 - the limit
    assert result.report.state is TerminalState.EARLY_CLOSED
    assert result.report.violations == ()


def _two_rows_per_page(request: Request) -> Envelope:
    start = int(str(request.copy_parameters().get("start", 0)))
    return {"result": [{"id": start}, {"id": start + 1}], "next": start + 2}


@pytest.mark.parametrize("rows", [1, 2], ids=["mid-page", "page-end"])
@pytest.mark.asyncio
async def test_a_sequential_break_with_a_delivered_page_reports_no_protocol_violation(rows: int) -> None:
    async with client_for(ResponderTransport(_two_rows_per_page)) as client:
        stream = client.iter_list(Request.bare("entity.list", replay_safety=ReplaySafety.SAFE))
        await _break_after(stream, rows)

    report = _report(stream)
    assert report.state is TerminalState.EARLY_CLOSED
    assert report.violations == ()


class _Shutdown(BaseException):
    """An application shutdown signal that is not an ``Exception``."""


@pytest.mark.parametrize("family", ["iter_list", "iter_list_counted"])
@pytest.mark.asyncio
async def test_a_base_exception_leaves_a_traversal_as_raised(family: str) -> None:
    def respond(_request: Request) -> Envelope:
        raise _Shutdown

    async with client_for(ResponderTransport(respond)) as client:
        stream = (
            client.iter_list_counted(_REQUEST, page_size=WIDTH)
            if family == "iter_list_counted"
            else client.iter_list(_REQUEST)
        )
        with pytest.raises(_Shutdown):
            async with stream:
                _ = [row async for row in stream]

    assert _report(stream).state is TerminalState.FAILED
