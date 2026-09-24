"""A caller-qualified exact total closes the whole collection, so it cannot start mid-collection (A9)."""

from __future__ import annotations
import json

import pytest

from b24api import (
    Bitrix24,
    CapabilityError,
    OffsetSpec,
    ParameterPath,
    ReplaySafety,
    Request,
    RouteKind,
    Settings,
    TerminalState,
    TotalTermination,
)
from b24api.contracts import OffsetContinuation
from b24api.contracts.policy import TotalSemantics
from b24api.execution import Executor
from b24api.transport import WireResponse
from b24api.traversal import iter_list
from b24api.traversal.plans import OffsetSequentialPlan, OffsetTerminalRule

HOST = "fixture.invalid"
TOTAL = 100
START = 50
PAGE = 50


class _Portal:
    host = HOST

    def __init__(self) -> None:
        self.starts: list[object] = []

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        parameters = request.copy_parameters()
        start = parameters.get("start", parameters.get("page", 0))
        self.starts.append(start)
        assert isinstance(start, int)
        first = start * PAGE - PAGE if "page" in parameters else start
        rows = [{"ID": identity} for identity in range(first + 1, min(first + PAGE, TOTAL) + 1)]
        payload: dict[str, object] = {"result": rows, "total": TOTAL}
        if first + PAGE < TOTAL:
            payload["next"] = first + PAGE
        return WireResponse(200, (("content-type", "application/json"),), json.dumps(payload).encode())

    async def aclose(self) -> None:
        return None


def _client(portal: _Portal) -> Bitrix24:
    return Bitrix24(Settings(webhook_url=f"https://{HOST}/rest/1/preflight/"), transport=portal)


@pytest.mark.asyncio
async def test_exact_qualified_total_refuses_a_nonzero_start_before_io() -> None:
    portal = _Portal()
    async with _client(portal) as client:
        stream = client.iter_list(
            Request("example.list", {"start": START}, ReplaySafety.SAFE, route=RouteKind.BARE),
            offset=OffsetSpec(total_termination=TotalTermination.EXACT_QUALIFIED),
        )
        with pytest.raises(CapabilityError, match="initial offset"):
            await anext(stream)

    assert portal.starts == []
    assert stream.report is not None
    assert stream.report.state is TerminalState.FAILED


@pytest.mark.asyncio
async def test_disabled_total_termination_keeps_the_suffix_traversal() -> None:
    portal = _Portal()
    async with _client(portal) as client:
        stream = client.iter_list(
            Request("example.list", {"start": START}, ReplaySafety.SAFE, route=RouteKind.BARE),
            offset=OffsetSpec(total_termination=TotalTermination.DISABLED),
        )
        rows = [row async for row in stream]

    assert [row["ID"] for row in rows if isinstance(row, dict)] == list(range(START + 1, TOTAL + 1))
    assert portal.starts[0] == START
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED


def _page_index_plan() -> OffsetSequentialPlan:
    # A page-index plan whose initial control is 1, closed by a caller-qualified exact total.
    return OffsetSequentialPlan(
        offset_path=ParameterPath(("page",)),
        continuation=OffsetContinuation.FIXED_STEP,
        fixed_step=1,
        initial_control=1,
        terminal=frozenset({OffsetTerminalRule.EMPTY_PAGE, OffsetTerminalRule.QUALIFIED_TOTAL}),
        total_semantics=TotalSemantics.FILTERED_EXACT,
    )


@pytest.mark.asyncio
async def test_exact_total_compares_the_start_with_the_plan_initial_control() -> None:
    portal = _Portal()
    admitted = iter_list(
        Executor(portal),
        Request("example.list", {"page": 1}, ReplaySafety.SAFE, route=RouteKind.BARE),
        plan=_page_index_plan(),
    )
    rows = [row async for row in admitted]
    assert len(rows) == TOTAL
    assert portal.starts == [1, 2]
    assert admitted.report.completed

    refused = iter_list(
        Executor(portal),
        Request("example.list", {"page": 2}, ReplaySafety.SAFE, route=RouteKind.BARE),
        plan=_page_index_plan(),
    )
    with pytest.raises(CapabilityError, match="initial offset"):
        await anext(refused)
    assert portal.starts == [1, 2]
