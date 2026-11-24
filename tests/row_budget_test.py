"""The row budget bounds what a stream holds after its commands ran: it never loses what the client knows.

A command's decoded result is known only once the command has run, so the row budget cannot refuse to send it.
A result that does not fit what the budget leaves it is dropped and its command ends with ``ResultTooLargeError``,
a failure that keeps the execution facts: it ran and was answered. Waiting for rows a consumer holds is bounded
by the operation's elapsed budget.
"""

from __future__ import annotations
import asyncio
import json
from typing import TYPE_CHECKING, Any, cast

import pytest

from b24api import Bitrix24, Settings
from b24api.contracts import (
    BatchDispatch,
    Command,
    CommandFailure,
    CommandSuccess,
    DeliveryOrder,
    DirectDispatch,
    ExecutionPolicy,
    ReplayDisposition,
    ReplaySafety,
    Request,
    TerminalState,
)
from b24api.contracts.report import PageOutcome
from b24api.contracts.request import RouteKind
from b24api.errors import BatchFailed, BudgetExceededError, IncompleteTraversalError, ResultTooLargeError
from b24api.execution import WireResponse
from b24api.execution.facts import Execution, execution
from b24api.references.dispatch import _RowBuffer
from tests.scripting import (
    DEADLINE_BUDGET,
    DEADLINE_SLACK,
    UNREACHABLE_COALESCE_WAIT,
    BatchPortal,
    ResponderTransport,
    assert_fired_near_deadline,
    client_for,
    deadline_started,
    drain_to_failure,
    on_loop,
    reached,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from b24api.contracts import CommandOutcome
    from b24api.references.dispatch import _Reservation
    from tests.scripting import Envelope, Query

_ADDS = 5
_LISTS = 25
_LIST_ROWS = 120
# The default row budget holds every add, one row of every list and the rest of 20 lists' rows.
_FITTING_LISTS = 20
_DEFAULT_ROWS = ExecutionPolicy().max_buffered_rows


def _batch_answer(request: Request) -> object:
    """Answer each list command of a classic batch with ``_LIST_ROWS`` rows, and every other with its new id."""
    commands = request.copy_parameters()["cmd"]
    assert isinstance(commands, dict)
    result = {
        key: list(range(_LIST_ROWS)) if str(command).startswith("crm.deal.list") else 7
        for key, command in commands.items()
    }
    return {"result": {"result": result, "result_error": []}}


def _deals_and_lists() -> list[Command[str]]:
    adds = [
        Command(
            Request("crm.deal.add", {"fields": {"TITLE": index}}, ReplaySafety.UNSAFE, route=RouteKind.BARE),
            f"a{index}",
        )
        for index in range(_ADDS)
    ]
    lists = [
        Command(Request("crm.deal.list", {"start": index}, ReplaySafety.SAFE, route=RouteKind.BARE), f"l{index}")
        for index in range(_LISTS)
    ]
    return adds + lists


def _assert_dropped(outcome: CommandOutcome[Any], *, rows: int, limit: int) -> None:
    """The command ran and was answered, but its result was dropped: a failure that is not proven not run."""
    assert isinstance(outcome, CommandFailure)
    assert isinstance(outcome.error, ResultTooLargeError)
    assert isinstance(outcome.error, BudgetExceededError)
    assert (outcome.error.rows, outcome.error.limit) == (rows, limit)
    assert execution(outcome.error) is Execution.ANSWERED
    assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE


@pytest.mark.asyncio
async def test_a_window_whose_results_outgrow_the_row_budget_keeps_every_outcome() -> None:
    # One envelope of 5 adds and 25 lists of 120 rows ran; its results hold 3,005 rows, more than the 2,500 the
    # budget allows. Every command keeps its outcome, in input order: the adds and the lists that fit are
    # delivered, and the 5 lists that do not are failures that ran, instead of the whole window being lost.
    transport = ResponderTransport(_batch_answer)
    async with client_for(transport) as client:
        stream = client.batch_outcomes(_deals_and_lists())
        outcomes = [outcome async for outcome in stream]

    assert len(transport.requests) == 1
    assert [outcome.correlation for outcome in outcomes] == [command.correlation for command in _deals_and_lists()]
    delivered, dropped = outcomes[: _ADDS + _FITTING_LISTS], outcomes[_ADDS + _FITTING_LISTS :]
    assert all(isinstance(outcome, CommandSuccess) for outcome in delivered)
    left = _DEFAULT_ROWS - _ADDS - _LISTS - _FITTING_LISTS * (_LIST_ROWS - 1)
    for outcome in dropped:
        _assert_dropped(outcome, rows=_LIST_ROWS, limit=left + 1)
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.COMPLETED_WITH_FAILURES
    assert (report.admitted, report.successes, report.failures) == (
        _ADDS + _LISTS,
        _ADDS + _FITTING_LISTS,
        _LISTS - _FITTING_LISTS,
    )
    assert report.buffered_rows_high_water <= _DEFAULT_ROWS


def _empty_lists_then_three_rows(request: Request) -> object:
    """Answer every command of a classic batch with an empty list, but the last with three rows."""
    commands = request.copy_parameters()["cmd"]
    assert isinstance(commands, dict)
    keys = list(commands)
    return {"result": {"result": {key: [1, 2, 3] if key == keys[-1] else [] for key in keys}, "result_error": []}}


@pytest.mark.asyncio
async def test_an_empty_result_holds_the_one_row_it_was_fitted_with() -> None:
    # Two empty results and a three-row one under a budget of three rows: every outcome holds a row, so the
    # three rows do not fit what the two empty results leave, and the stream holds the three outcomes' rows.
    lists = [Command(Request("crm.deal.list", {"start": index}, route=RouteKind.BARE), index) for index in range(3)]
    async with client_for(
        ResponderTransport(_empty_lists_then_three_rows), policy=ExecutionPolicy(max_buffered_rows=3)
    ) as client:
        stream = client.batch_outcomes(lists)
        outcomes = [outcome async for outcome in stream]

    assert [isinstance(outcome, CommandSuccess) for outcome in outcomes] == [True, True, False]
    _assert_dropped(outcomes[2], rows=3, limit=1)
    report = stream.report
    assert report is not None
    assert report.buffered_rows_high_water == len(lists)


def _three_rows(request: Request) -> object:
    commands = request.copy_parameters()["cmd"]
    assert isinstance(commands, dict)
    return {"result": {"result": {key: [1, 2, 3] for key in commands}, "result_error": []}}


_WRITE = Command(
    Request("crm.item.add", {"fields": {"TITLE": "x"}}, ReplaySafety.UNSAFE, route=RouteKind.BARE), "write"
)


@pytest.mark.asyncio
async def test_a_result_exactly_the_rows_the_budget_leaves_is_delivered() -> None:
    # Three rows under a budget of three: the result fits to the last row, so it is a success.
    transport = ResponderTransport(_three_rows)
    async with client_for(transport, policy=ExecutionPolicy(max_buffered_rows=3)) as client:
        stream = client.batch_outcomes([_WRITE])
        (outcome,) = [outcome async for outcome in stream]

    assert isinstance(outcome, CommandSuccess)
    report = stream.report
    assert report is not None
    assert report.buffered_rows_high_water == 3  # noqa: PLR2004 - the whole budget


@pytest.mark.asyncio
async def test_an_unsafe_write_whose_result_exceeds_the_row_budget_reports_that_it_ran() -> None:
    # The write ran and the client knows it: its three-row result does not fit a budget of two rows, so the
    # outcome is a failure that ran, never lost with the window nor reported as a command never sent.
    transport = ResponderTransport(_three_rows)
    async with client_for(transport, policy=ExecutionPolicy(max_buffered_rows=2)) as client:
        stream = client.batch_outcomes([_WRITE])
        (outcome,) = [outcome async for outcome in stream]

    _assert_dropped(outcome, rows=3, limit=2)
    report = stream.report
    assert report is not None
    assert (report.admitted, report.physical_requests, report.failures) == (1, 1, 1)


@pytest.mark.asyncio
async def test_a_fail_fast_batch_carries_the_dropped_result_in_its_failure() -> None:
    transport = ResponderTransport(_three_rows)
    async with client_for(transport, policy=ExecutionPolicy(max_buffered_rows=2)) as client:
        outcomes, error = await drain_to_failure(client.batch([_WRITE]), BatchFailed)

    assert outcomes == []
    (failed,) = error.outcomes
    _assert_dropped(failed, rows=3, limit=2)
    assert [violation.code for violation in error.report.violations] == ["budget_exhausted"]


_REQUEST_ID = "request-7"


def _three_rows_with_a_request_id(request: Request) -> WireResponse:
    """Answer a direct request, or each command of a classic batch, with three rows, under a request id."""
    commands = request.copy_parameters().get("cmd")
    rows = [1, 2, 3]
    body = (
        {"result": {"result": dict.fromkeys(commands, rows), "result_error": []}}
        if isinstance(commands, dict)
        else {"result": rows}
    )
    headers = (("content-type", "application/json"), ("x-request-id", _REQUEST_ID))
    return WireResponse(200, headers, json.dumps(body).encode())


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["batch", "fan-out-batch", "fan-out-direct"])
async def test_a_dropped_result_keeps_the_evidence_of_the_answer_it_dropped(operation: str) -> None:
    # The command was answered: its failure names the HTTP status and the request id of that answer.
    command = Command(Request("crm.item.list", {}, ReplaySafety.SAFE, route=RouteKind.BARE), 0)
    transport = ResponderTransport(_three_rows_with_a_request_id)
    async with client_for(transport, policy=ExecutionPolicy(max_buffered_rows=2)) as client:
        stream = (
            client.batch_outcomes([command])
            if operation == "batch"
            else client.fan_out_outcomes(
                [command], dispatch=BatchDispatch() if operation == "fan-out-batch" else DirectDispatch()
            )
        )
        (outcome,) = [outcome async for outcome in stream]

    assert isinstance(outcome, CommandFailure)
    assert isinstance(outcome.error, ResultTooLargeError)
    assert (outcome.error.http_status, outcome.error.evidence.request_id) == (200, _REQUEST_ID)


_COUNTED_ROWS = 300
_COUNTED_PAGE = 50
_OVER_CAP_OFFSET = 100


def _counted_with_a_page_over_its_cap(method: str, query: Query) -> Envelope:
    """Serve ``_COUNTED_ROWS`` rows with an exact total; the page at ``_OVER_CAP_OFFSET`` holds one row too many."""
    del method
    offset = int(query.get("start", "0"))
    width = _COUNTED_PAGE + (offset == _OVER_CAP_OFFSET)
    envelope: Envelope = {"result": [{"ID": row} for row in range(offset, offset + width)], "total": _COUNTED_ROWS}
    if offset + _COUNTED_PAGE < _COUNTED_ROWS:
        envelope["next"] = offset + _COUNTED_PAGE
    return envelope


@pytest.mark.asyncio
async def test_a_counted_tail_page_over_its_cap_is_reported_as_answered() -> None:
    # The tail window of two pages fills the row budget; the second page came back a row longer. Both pages
    # ran: the first is delivered, the second's result is dropped as answered, never left out of the report.
    portal = BatchPortal(_counted_with_a_page_over_its_cap)
    async with client_for(portal, policy=ExecutionPolicy(max_buffered_rows=2 * _COUNTED_PAGE)) as client:
        stream = client.iter_list_counted(Request.bare("crm.item.list", {}, ReplaySafety.SAFE), page_size=_COUNTED_PAGE)
        rows, error = await drain_to_failure(stream, IncompleteTraversalError)

    assert portal.waves == [1, 2]
    assert len(rows) == 2 * _COUNTED_PAGE
    cause = error.__cause__
    assert isinstance(cause, ResultTooLargeError)
    assert (cause.rows, cause.limit) == (_COUNTED_PAGE + 1, _COUNTED_PAGE)
    assert execution(cause) is Execution.ANSWERED
    report = stream.report
    assert report is not None
    assert [(record.offset, record.outcome) for record in report.page_trace] == [
        (0, PageOutcome.COMMITTED),
        (_COUNTED_PAGE, PageOutcome.COMMITTED),
        (_OVER_CAP_OFFSET, PageOutcome.REJECTED),
    ]


def _list_answer(count: int) -> Callable[[Request], object]:
    """Answer a direct request, or each command of a classic batch, with a list of ``count`` rows."""

    def respond(request: Request) -> object:
        commands = request.copy_parameters().get("cmd")
        if isinstance(commands, dict):
            return {"result": {"result": {key: list(range(count)) for key in commands}, "result_error": []}}
        return {"result": list(range(count))}

    return respond


def _rows(count: int) -> ResponderTransport:
    """Answer every request with lists of ``count`` rows (``_list_answer``)."""
    return ResponderTransport(_list_answer(count))


# A command's share of the row budget: the budget divided among the results a dispatch has in flight at once, a
# batch's commands times the batches in flight, at most max_active_references and never so many that a full portal
# page (50 rows) no longer fits each. One command a batch, eight batches at once: 312 rows. Two or four batches of
# 50 or 20 commands would leave 25 or 31 rows, less than a page: they keep 50. Ten active references cap four
# batches of five at ten results: 250 rows.
_SINGLE_COMMAND_BATCHES = 8
_PAGE_ROWS = 50
_TEN_REFERENCES = ExecutionPolicy(max_active_references=10)
_SHARES = [
    (BatchDispatch(), None, _PAGE_ROWS),
    (DirectDispatch(), None, _DEFAULT_ROWS // 10),
    (
        BatchDispatch(batch_size=1, concurrency=_SINGLE_COMMAND_BATCHES),
        None,
        _DEFAULT_ROWS // _SINGLE_COMMAND_BATCHES,
    ),
    (BatchDispatch(concurrency=2), None, _PAGE_ROWS),
    (BatchDispatch(batch_size=20, concurrency=4), None, _PAGE_ROWS),
    (BatchDispatch(batch_size=5, concurrency=4), _TEN_REFERENCES, _DEFAULT_ROWS // 10),
]
_SHARE_IDS = [
    "batch",
    "direct",
    "single-command-batches",
    "two-full-batches",
    "four-batches-of-twenty",
    "active-references-cap",
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("dispatch", "policy", "share"), _SHARES, ids=_SHARE_IDS)
async def test_a_fan_out_result_exactly_its_share_of_the_row_budget_is_delivered(
    dispatch: BatchDispatch | DirectDispatch, policy: ExecutionPolicy | None, share: int
) -> None:
    command = Command(Request("crm.item.list", {}, ReplaySafety.SAFE, route=RouteKind.BARE), 0)
    async with client_for(_rows(share), policy=policy) as client:
        stream = client.fan_out_outcomes([command], dispatch=dispatch)
        (outcome,) = [outcome async for outcome in stream]

    assert isinstance(outcome, CommandSuccess)
    assert outcome.response.result == list(range(share))
    report = stream.report
    assert report is not None
    assert report.buffered_rows_high_water == share


@pytest.mark.asyncio
@pytest.mark.parametrize(("dispatch", "policy", "share"), _SHARES, ids=_SHARE_IDS)
async def test_a_fan_out_result_longer_than_its_share_grows_into_the_free_rows(
    dispatch: BatchDispatch | DirectDispatch, policy: ExecutionPolicy | None, share: int
) -> None:
    # The share is what a command reserves before it is sent. Its answer is decoded already: while the pool has
    # the rows, a longer result keeps them, here twice its share.
    command = Command(Request("crm.item.list", {}, ReplaySafety.SAFE, route=RouteKind.BARE), 0)
    async with client_for(_rows(share * 2), policy=policy) as client:
        stream = client.fan_out_outcomes([command], dispatch=dispatch)
        (outcome,) = [outcome async for outcome in stream]

    assert isinstance(outcome, CommandSuccess)
    assert outcome.response.result == list(range(share * 2))
    report = stream.report
    assert report is not None
    assert report.buffered_rows_high_water == share * 2


@pytest.mark.asyncio
@pytest.mark.parametrize(("dispatch", "policy", "share"), _SHARES, ids=_SHARE_IDS)
async def test_a_fan_out_result_longer_than_the_row_budget_reports_that_it_ran(
    dispatch: BatchDispatch | DirectDispatch, policy: ExecutionPolicy | None, share: int
) -> None:
    del share
    command = Command(Request("crm.item.add", {}, ReplaySafety.UNSAFE, route=RouteKind.BARE), 0)
    transport = _rows(_DEFAULT_ROWS + 1)
    async with client_for(transport, policy=policy) as client:
        (outcome,) = [outcome async for outcome in client.fan_out_outcomes([command], dispatch=dispatch)]
        _, error = await drain_to_failure(client.fan_out([command], dispatch=dispatch), BatchFailed)

    _assert_dropped(outcome, rows=_DEFAULT_ROWS + 1, limit=_DEFAULT_ROWS)
    (failed,) = error.outcomes
    _assert_dropped(failed, rows=_DEFAULT_ROWS + 1, limit=_DEFAULT_ROWS)
    assert len(transport.requests) == 2  # noqa: PLR2004 - one send of each operation


_CLIENT_LIMIT = 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dispatch",
    [DirectDispatch(), BatchDispatch(batch_size=1, concurrency=_SINGLE_COMMAND_BATCHES)],
    ids=_SHARE_IDS[1:3],
)
async def test_a_fan_out_share_counts_only_the_sends_the_client_limit_lets_in_flight(
    dispatch: BatchDispatch | DirectDispatch,
) -> None:
    # The client keeps two requests in flight, so two results share the budget, however wide the dispatch: each
    # reserves half of it. Both answer one row more: the first to land waits for a row the other holds, the other
    # finds the rest held by a result that waits as well, and is dropped with the share it held. The first then
    # grows into its rows. Shared among ten, both would have grown.
    share = _DEFAULT_ROWS // _CLIENT_LIMIT
    commands = [
        Command(Request("crm.item.list", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)
        for index in range(_CLIENT_LIMIT)
    ]
    answer = _list_answer(share + 1)
    both = asyncio.Event()

    async def respond(request: Request) -> object:
        # Both commands are in flight before either answers.
        if len(transport.requests) == _CLIENT_LIMIT:
            both.set()
        await both.wait()
        return answer(request)

    transport = ResponderTransport(respond)
    settings = Settings(webhook_url=f"https://{transport.host}/rest/1/token/", max_concurrent_requests=_CLIENT_LIMIT)
    async with Bitrix24(settings, transport=transport) as client:
        outcomes = [outcome async for outcome in client.fan_out_outcomes(on_loop(commands), dispatch=dispatch)]

    (delivered,) = [outcome for outcome in outcomes if isinstance(outcome, CommandSuccess)]
    assert delivered.response.result == list(range(share + 1))
    (dropped,) = [outcome for outcome in outcomes if isinstance(outcome, CommandFailure)]
    _assert_dropped(dropped, rows=share + 1, limit=share)


def _two_answers(share: int, *, longer_first: bool) -> ResponderTransport:
    """Answer two commands in flight together: one with ``share`` rows, the other with a page more, first or last."""
    lists = {0: _list_answer(share), 1: _list_answer(share + _PAGE_ROWS)}
    both = asyncio.Event()

    async def respond(request: Request) -> object:
        if len(transport.requests) == 2:  # noqa: PLR2004 - both commands
            both.set()
        await both.wait()
        command = cast("int", request.copy_parameters()["id"])
        if (command == 1) is not longer_first:
            for _ in range(10):
                await asyncio.sleep(0)
        return lists[command](request)

    transport = ResponderTransport(respond)
    return transport


async def _two_outcomes(transport: ResponderTransport, order: DeliveryOrder) -> tuple[list[CommandOutcome[int]], int]:
    """Fan out two commands in flight together; the consumer stalls after the first outcome until both answered."""
    commands = [
        Command(Request("crm.item.list", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)
        for index in range(2)
    ]
    async with client_for(transport) as client:
        stream = client.fan_out_outcomes(on_loop(commands), dispatch=DirectDispatch(concurrency=2, output_order=order))
        outcomes: list[CommandOutcome[int]] = []
        async with stream:
            async for outcome in stream:
                outcomes.append(outcome)
                for _ in range(30):
                    await asyncio.sleep(0)
    report = stream.report
    assert report is not None
    return outcomes, report.buffered_rows_high_water


@pytest.mark.asyncio
async def test_a_result_behind_the_input_frontier_waits_for_the_rows_the_frontier_frees() -> None:
    # Two commands in flight reserve half the budget each. The second answers first, a page more than its half,
    # while the first, the frontier, is in flight; then the first answers its whole half, which waits for the
    # consumer. Behind the frontier the second waits for the consumer to take the frontier's rows, and grows.
    share = _DEFAULT_ROWS // 2
    outcomes, high_water = await _two_outcomes(_two_answers(share, longer_first=True), DeliveryOrder.INPUT)

    assert [outcome.correlation for outcome in outcomes] == [0, 1]
    assert [_rows_of(outcome) for outcome in outcomes] == [share, share + _PAGE_ROWS]
    assert high_water <= _DEFAULT_ROWS


@pytest.mark.asyncio
async def test_a_ready_result_waits_for_the_rows_the_consumer_holds() -> None:
    # The first command answers its whole half of the budget, and the consumer holds those rows while it stalls;
    # the second answers a page more than its half. The consumer takes rows whatever the second does, so the second
    # waits for them, and grows.
    share = _DEFAULT_ROWS // 2
    outcomes, high_water = await _two_outcomes(_two_answers(share, longer_first=False), DeliveryOrder.READY)

    assert [outcome.correlation for outcome in outcomes] == [0, 1]
    assert [_rows_of(outcome) for outcome in outcomes] == [share, share + _PAGE_ROWS]
    assert high_water <= _DEFAULT_ROWS


_FEW_ROWS = 10
_GROWTH = 1000
# The first three commands, then the rest.
_TWO_BATCHES = 2


@pytest.mark.asyncio
async def test_a_batched_result_waiting_to_grow_frees_its_send_slot_for_the_commands_reserved_before_it() -> None:
    # One batch at a time, three commands a batch, each reserving a third of the budget. The first command answers
    # 1,000 rows more than its share. While it waits to grow it holds no send slot, and no command reserves rows:
    # the rows its batch's other two return are its first, so it grows, and the commands behind it follow in one
    # more batch. Holding the slot, it waited for rows only a command behind it, never sent, could return.
    share = _DEFAULT_ROWS // 3
    rows = {0: share + _GROWTH} | dict.fromkeys(range(1, 6), _FEW_ROWS)

    def respond(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        answers = {key: list(range(rows[int(str(command).rsplit("=", 1)[1])])) for key, command in commands.items()}
        return {"result": {"result": answers, "result_error": []}}

    transport = ResponderTransport(respond)
    commands = [
        Command(Request("crm.item.list", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)
        for index in rows
    ]
    dispatch = BatchDispatch(batch_size=3, concurrency=1, output_order=DeliveryOrder.READY)
    async with client_for(transport, policy=ExecutionPolicy(max_elapsed=DEADLINE_BUDGET * 10)) as client:
        stream = client.fan_out_outcomes(on_loop(commands), dispatch=dispatch)
        outcomes = [outcome async for outcome in stream]

    assert {outcome.correlation: _rows_of(outcome) for outcome in outcomes} == rows
    assert len(transport.requests) == _TWO_BATCHES
    report = stream.report
    assert report is not None
    assert report.buffered_rows_high_water <= _DEFAULT_ROWS


@pytest.mark.asyncio
async def test_a_batched_result_waiting_to_grow_lets_the_command_reserved_behind_it_be_sent() -> None:
    # One batch at a time. The first command travels alone; while it is in flight the second reserves its share and
    # waits for the send slot. The first answers 1,000 rows more than its share, and only the second, once sent and
    # answered, returns enough: a result waiting to grow frees its slot, so the second is sent, lands short, and the
    # first grows into its rows. Holding the slot, the first waited for a command that could never be sent.
    share = _DEFAULT_ROWS // 3
    rows = {0: share + _GROWTH, 1: _FEW_ROWS}
    first_sent = asyncio.Event()

    async def respond(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        if not first_sent.is_set():
            first_sent.set()
            for _ in range(50):
                await asyncio.sleep(0)
        answers = {key: list(range(rows[int(str(command).rsplit("=", 1)[1])])) for key, command in commands.items()}
        return {"result": {"result": answers, "result_error": []}}

    async def commands() -> AsyncIterator[Command[int]]:
        for index in rows:
            yield Command(Request("crm.item.list", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)
            await first_sent.wait()

    transport = ResponderTransport(respond)
    dispatch = BatchDispatch(batch_size=3, concurrency=1, coalesce_wait=0.0, output_order=DeliveryOrder.READY)
    async with client_for(transport, policy=ExecutionPolicy(max_elapsed=DEADLINE_BUDGET * 10)) as client:
        stream = client.fan_out_outcomes(commands(), dispatch=dispatch)
        outcomes = [outcome async for outcome in stream]

    assert {outcome.correlation: _rows_of(outcome) for outcome in outcomes} == rows
    assert len(transport.requests) == _TWO_BATCHES


@pytest.mark.asyncio
@pytest.mark.parametrize("landing", [(2, 1, 0), (2, 0, 1)], ids=["behind-first", "frontier-first"])
async def test_input_results_that_fit_the_budget_together_grow_whichever_lands_first(landing: tuple[int, ...]) -> None:
    # Three commands in flight share the budget. The first two answer more than their share, together less than the
    # budget. Behind the frontier a result waits for the frontier to reach it rather than take rows the frontier may
    # need: whichever lands first, both grow. Taking them, the result behind left the frontier dropped.
    share = _DEFAULT_ROWS // 3
    rows = {0: share + _GROWTH // 2, 1: share + _GROWTH * 4 // 5, 2: _FEW_ROWS}
    all_sent = asyncio.Event()
    landed = {command: asyncio.Event() for command in landing}

    async def respond(request: Request) -> object:
        if len(transport.requests) == len(rows):
            all_sent.set()
        await all_sent.wait()
        command = cast("int", request.copy_parameters()["id"])
        position = landing.index(command)
        if position:
            await landed[landing[position - 1]].wait()
            for _ in range(50):
                await asyncio.sleep(0)
        landed[command].set()
        return {"result": list(range(rows[command]))}

    transport = ResponderTransport(respond)
    commands = [
        Command(Request("crm.item.list", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)
        for index in rows
    ]
    dispatch = DirectDispatch(concurrency=len(rows), output_order=DeliveryOrder.INPUT)
    async with client_for(transport, policy=ExecutionPolicy(max_elapsed=DEADLINE_BUDGET * 10)) as client:
        stream = client.fan_out_outcomes(on_loop(commands), dispatch=dispatch)
        outcomes = [outcome async for outcome in stream]

    assert [(outcome.correlation, _rows_of(outcome)) for outcome in outcomes] == list(rows.items())
    report = stream.report
    assert report is not None
    assert report.buffered_rows_high_water <= _DEFAULT_ROWS


def _rows_of(outcome: CommandOutcome[int]) -> int:
    assert isinstance(outcome, CommandSuccess)
    return len(cast("list[int]", outcome.response.result))


_BATCHES_IN_FLIGHT = 4


@pytest.mark.asyncio
@pytest.mark.parametrize("batch_size", [1, 5])
async def test_a_fan_out_keeps_its_batch_concurrency_in_flight_whatever_its_batch_size(batch_size: int) -> None:
    # Each command's row share used to be sized for one batch: under batch_size=1 one command took the whole row
    # budget, so one batch was in flight whatever the concurrency, and batch_size=5 fit five commands in all. The
    # share now divides the budget among every batch in flight, so the dispatch, not the row pool, sets the peak.
    commands_in_flight = _BATCHES_IN_FLIGHT * batch_size
    release = asyncio.Event()
    full = asyncio.Event()

    async def respond(request: Request) -> object:
        if len(transport.requests) == _BATCHES_IN_FLIGHT:
            full.set()
        await release.wait()
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {"result": {"result": dict.fromkeys(commands, 1), "result_error": []}}

    transport = ResponderTransport(respond)
    commands = [
        Command(Request("crm.item.get", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)
        for index in range(3 * commands_in_flight)
    ]
    dispatch = BatchDispatch(
        batch_size=batch_size, concurrency=_BATCHES_IN_FLIGHT, coalesce_wait=UNREACHABLE_COALESCE_WAIT
    )
    async with client_for(transport) as client:
        stream = client.fan_out_outcomes(on_loop(commands), dispatch=dispatch)

        async def drain() -> int:
            async with stream:
                return len([outcome async for outcome in stream])

        drained = asyncio.create_task(drain())
        await reached(full)
        # Every send blocks until released and the input is fed on the loop: no batch beyond the peak can start
        # unless a limit admits more, and these turns of the loop give it the room to.
        for _ in range(50):
            await asyncio.sleep(0)
        in_flight = [len(cast("dict[str, object]", request.copy_parameters()["cmd"])) for request in transport.requests]
        release.set()
        assert await drained == len(commands)

    assert in_flight == [batch_size] * _BATCHES_IN_FLIGHT
    report = stream.report
    assert report is not None
    assert report.buffered_rows_high_water <= _DEFAULT_ROWS


@pytest.mark.asyncio
@pytest.mark.parametrize("order", [DeliveryOrder.READY, DeliveryOrder.INPUT])
async def test_a_command_waiting_for_rows_a_stalled_consumer_holds_ends_with_the_elapsed_budget(
    monkeypatch: pytest.MonkeyPatch, order: DeliveryOrder
) -> None:
    # Each result takes the whole row budget, so the second command waits for the rows of the first, which the
    # consumer holds while it stalls. The wait ends with the operation's elapsed budget, while the consumer still
    # stalls, rather than when the consumer next reads: the second command fails unsent, and the operation ends
    # with its time budget. Under INPUT the second command reads ahead of the consumer and waits the same way.
    reserve = _RowBuffer.reserve
    wait_ended = asyncio.Event()

    async def observed_reserve(buffer: _RowBuffer, index: int, amount: int) -> _Reservation:
        try:
            return await reserve(buffer, index, amount)
        finally:
            if index:
                wait_ended.set()

    monkeypatch.setattr(_RowBuffer, "reserve", observed_reserve)
    commands = [
        Command(Request("crm.item.get", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE), index)
        for index in range(2)
    ]
    transport = _rows(2)
    policy = ExecutionPolicy(max_elapsed=DEADLINE_BUDGET, max_buffered_rows=2)
    async with client_for(transport, policy=policy) as client:
        started = deadline_started()
        stream = client.fan_out_outcomes(on_loop(commands), dispatch=DirectDispatch(concurrency=1, output_order=order))
        first = await anext(stream)
        # The consumer stalls with the first result in hand until the second command's wait has ended.
        await reached(wait_ended, DEADLINE_BUDGET + DEADLINE_SLACK)
        assert_fired_near_deadline(started)
        rest, error = await drain_to_failure(stream, BudgetExceededError)

    assert isinstance(first, CommandSuccess)
    (waited,) = rest
    assert isinstance(waited, CommandFailure)
    assert str(waited.error) == "row buffer wait exceeded operation time budget"
    assert execution(waited.error) is Execution.NOT_SENT
    assert len(transport.requests) == 1
    assert isinstance(error, BudgetExceededError)
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.INCOMPLETE
    assert "budget_exhausted" in {violation.code for violation in report.violations}


# The rows each of two commands answers, and the limit the second is dropped with: the rows it could hold.
_DEADLINE_DROPS: dict[str, tuple[tuple[int, int], int]] = {
    "its_share": ((2, 3), 2),
    # The first command's unused row is free: the second could hold it as well.
    "its_share_and_the_free_rows": ((1, 4), 3),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("case", list(_DEADLINE_DROPS))
@pytest.mark.parametrize("order", [DeliveryOrder.READY, DeliveryOrder.INPUT])
async def test_a_result_waiting_to_grow_when_the_elapsed_budget_runs_out_ends_as_a_command_that_ran(
    monkeypatch: pytest.MonkeyPatch, order: DeliveryOrder, case: str
) -> None:
    # Two commands in flight share 4 rows, 2 each. The consumer holds the first command's rows while it stalls; the
    # second answers more than its share and the free rows and waits to grow into rows the consumer's may return. The
    # elapsed budget runs out first: the second command ran, so it ends dropped as answered, never as unsent, with the
    # rows it could hold as its limit, and the operation ends with its time budget.
    answers, limit = _DEADLINE_DROPS[case]
    grow = _RowBuffer.grow
    wait_ended = asyncio.Event()

    async def observed_grow(buffer: _RowBuffer, reservation: _Reservation, total: int) -> int | None:
        try:
            return await grow(buffer, reservation, total)
        finally:
            wait_ended.set()

    monkeypatch.setattr(_RowBuffer, "grow", observed_grow)
    both_sent = asyncio.Event()

    async def respond(request: Request) -> object:
        if len(transport.requests) == len(commands):
            both_sent.set()
        await both_sent.wait()
        index = cast("int", request.copy_parameters()["id"])
        for _turn in range(10 * index):  # the second answer lands once the first is the consumer's
            await asyncio.sleep(0)
        return {"result": list(range(answers[index]))}

    transport = ResponderTransport(respond)
    commands = [
        Command(Request("crm.item.add", {"id": index}, ReplaySafety.UNSAFE, route=RouteKind.BARE), index)
        for index in range(2)
    ]
    policy = ExecutionPolicy(max_elapsed=DEADLINE_BUDGET, max_buffered_rows=4)
    async with client_for(transport, policy=policy) as client:
        stream = client.fan_out_outcomes(on_loop(commands), dispatch=DirectDispatch(concurrency=2, output_order=order))
        first = await anext(stream)
        # The consumer stalls with the first result in hand until the second command's growth has ended.
        await reached(wait_ended, DEADLINE_BUDGET + DEADLINE_SLACK)
        rest, error = await drain_to_failure(stream, BudgetExceededError)

    assert isinstance(first, CommandSuccess)
    (dropped,) = rest
    assert isinstance(dropped, CommandFailure)
    assert isinstance(dropped.error, ResultTooLargeError)
    assert (dropped.error.rows, dropped.error.limit) == (answers[1], limit)
    assert execution(dropped.error) is Execution.ANSWERED
    assert dropped.replay_disposition is ReplayDisposition.NOT_ELIGIBLE
    assert len(transport.requests) == len(commands)
    assert isinstance(error, BudgetExceededError)
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.INCOMPLETE
    assert "budget_exhausted" in {violation.code for violation in report.violations}
