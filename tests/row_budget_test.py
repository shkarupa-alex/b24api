"""The row budget bounds what a stream holds after its commands ran: it never loses what the client knows.

A command's decoded result is known only once the command has run, so the row budget cannot refuse to send it.
A result that does not fit what the budget leaves it is dropped and its command ends with ``ResultTooLargeError``,
a failure that keeps the execution facts: it ran and was answered. Waiting for rows a consumer holds is bounded
by the operation's elapsed budget.
"""

from __future__ import annotations
import asyncio
from typing import TYPE_CHECKING, Any

import pytest

from b24api.contracts import (
    BatchDispatch,
    Command,
    CommandFailure,
    CommandSuccess,
    DirectDispatch,
    ExecutionPolicy,
    ReplayDisposition,
    ReplaySafety,
    Request,
    TerminalState,
)
from b24api.contracts.request import RouteKind
from b24api.errors import BatchFailed, BudgetExceededError, ResultTooLargeError
from b24api.execution.facts import Execution, execution
from b24api.references.dispatch import _RowBuffer
from tests.scripting import (
    DEADLINE_BUDGET,
    DEADLINE_SLACK,
    ResponderTransport,
    assert_fired_near_deadline,
    client_for,
    deadline_started,
    drain_to_failure,
    on_loop,
    reached,
)

if TYPE_CHECKING:
    from b24api.contracts import CommandOutcome
    from b24api.references.dispatch import _Reservation

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


def _three_rows(request: Request) -> object:
    commands = request.copy_parameters()["cmd"]
    assert isinstance(commands, dict)
    return {"result": {"result": {key: [1, 2, 3] for key in commands}, "result_error": []}}


_WRITE = Command(
    Request("crm.item.add", {"fields": {"TITLE": "x"}}, ReplaySafety.UNSAFE, route=RouteKind.BARE), "write"
)


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


def _rows(count: int) -> ResponderTransport:
    """Answer a direct request, or each command of a classic batch, with a list of ``count`` rows."""

    def respond(request: Request) -> object:
        commands = request.copy_parameters().get("cmd")
        if isinstance(commands, dict):
            return {"result": {"result": {key: list(range(count)) for key in commands}, "result_error": []}}
        return {"result": list(range(count))}

    return ResponderTransport(respond)


# A command's share of the default row budget: the budget divided among the results a dispatch holds at once.
_SHARES = [(BatchDispatch(), _DEFAULT_ROWS // 50), (DirectDispatch(), _DEFAULT_ROWS // 10)]


@pytest.mark.asyncio
@pytest.mark.parametrize(("dispatch", "share"), _SHARES, ids=["batch", "direct"])
async def test_a_fan_out_result_longer_than_its_share_of_the_row_budget_reports_that_it_ran(
    dispatch: BatchDispatch | DirectDispatch, share: int
) -> None:
    command = Command(Request("crm.item.add", {}, ReplaySafety.UNSAFE, route=RouteKind.BARE), 0)
    transport = _rows(share + 1)
    async with client_for(transport) as client:
        (outcome,) = [outcome async for outcome in client.fan_out_outcomes([command], dispatch=dispatch)]
        _, error = await drain_to_failure(client.fan_out([command], dispatch=dispatch), BatchFailed)

    _assert_dropped(outcome, rows=share + 1, limit=share)
    (failed,) = error.outcomes
    _assert_dropped(failed, rows=share + 1, limit=share)
    assert len(transport.requests) == 2  # noqa: PLR2004 - one send of each operation


@pytest.mark.asyncio
async def test_a_command_waiting_for_rows_a_stalled_consumer_holds_ends_with_the_elapsed_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Each result takes the whole row budget, so the second command waits for the rows of the first, which the
    # consumer holds while it stalls. The wait ends with the operation's elapsed budget, while the consumer still
    # stalls, rather than when the consumer next reads: the second command fails unsent, and the operation ends
    # with its time budget.
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
        stream = client.fan_out_outcomes(on_loop(commands), dispatch=DirectDispatch(concurrency=1))
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
