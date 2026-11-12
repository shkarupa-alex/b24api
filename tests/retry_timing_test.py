"""Retry timing: the backoff against a throttle hint, where the retry window opens, and when a retry is sent.

Time is virtual throughout: the executor and its coordinator read an injected clock, and the retry sleep advances
it, so no test waits on the wall clock or asserts one.
"""

from __future__ import annotations
import asyncio
import email.utils
import json
import os
import random
from collections.abc import Mapping
from time import gmtime, strftime, tzset
from typing import TYPE_CHECKING

import pytest

from b24api import ReplaySafety, Request
from b24api.batch.correlation import _Command
from b24api.batch.engine import BatchExecutor
from b24api.batch.outcome import BatchFailure, BatchOutcome, BatchSuccess
from b24api.batch.stream import batch_outcome_stream
from b24api.contracts import ResultErrorSpec, ResultSelector
from b24api.contracts.policy import ExecutionPolicy, ReplayDisposition, RetryPolicy
from b24api.contracts.request import RouteKind
from b24api.errors import (
    AmbiguousExecutionError,
    ApiResponseError,
    BatchCommandError,
    BudgetExceededError,
    HTTPGatewayError,
)
from b24api.execution import Executor, RateCoordinator, WireResponse, WorkClass
from b24api.execution.facts import Execution, execution
from b24api.testing import ScriptedExchange, ScriptedTransport
from tests.scripting import ResponderTransport, always, replies

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from b24api import Transport
    from b24api.execution.rate import DeadlineBudget, _Permit

_JSON = (("content-type", "application/json"),)
# The response's own clock, years behind the local one: a hint measured locally would have passed long ago.
_SERVER_NOW = 1_100_000_000.0
_SERVER_DATE = ("date", email.utils.formatdate(_SERVER_NOW, usegmt=True))
_OK = WireResponse(200, _JSON, b'{"result":{"ok":1}}')
_QUERY_LIMIT = json.dumps({"error": "QUERY_LIMIT_EXCEEDED", "error_description": "Too many requests"}).encode()


class _Time:
    """A virtual clock; the retry sleep advances it by ``overrun`` times the requested delay."""

    def __init__(self, *, overrun: float = 1.0) -> None:
        self.now = 0.0
        self.overrun = overrun
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds * self.overrun
        await asyncio.sleep(0)


def _executor(transport: Transport, time: _Time, *, max_concurrency: int = 10) -> Executor:
    # A random source of one half makes the jitter factor exactly one.
    coordinator = RateCoordinator(max_concurrency=max_concurrency, clock=time.clock)
    return Executor(transport, coordinator=coordinator, clock=time.clock, sleep=time.sleep, random_source=lambda: 0.5)


def _request(safety: ReplaySafety, method: str = "crm.item.add") -> Request:
    return Request(method, {"fields": {"title": "x"}}, safety, route=RouteKind.BARE)


def _refused(headers: tuple[tuple[str, str], ...] = (), *, status: int = 503) -> WireResponse:
    """A structured quota refusal: it proves the request did not run, so any request may be sent again."""
    return WireResponse(status, (*_JSON, *headers), _QUERY_LIMIT)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "hint",
    [
        ("retry-after", "0"),
        ("retry-after", email.utils.formatdate(_SERVER_NOW - 60, usegmt=True)),
        ("x-bitrix-ratelimit-reset", str(int(_SERVER_NOW) - 1)),
    ],
    ids=["retry-after-zero", "retry-after-past-date", "reset-just-before-now"],
)
async def test_a_throttle_hint_shorter_than_the_backoff_keeps_the_backoff(hint: tuple[str, str]) -> None:
    time = _Time()
    transport = ResponderTransport(always(WireResponse(429, (_SERVER_DATE, hint), b"")))
    executor = _executor(transport, time)

    with pytest.raises(BudgetExceededError, match="attempt budget"):
        await executor.execute(_request(ReplaySafety.SAFE, "crm.item.get"), policy=ExecutionPolicy())
    await executor.coordinator.close()

    # Five sends, each retry after the default exponential backoff: the hint never erased it.
    assert len(transport.requests) == 5  # noqa: PLR2004
    assert time.sleeps == [0.5, 1.0, 2.0, 4.0]
    assert time.now == 7.5  # noqa: PLR2004


@pytest.mark.asyncio
async def test_a_throttle_hint_shorter_than_a_nonzero_backoff_keeps_the_backoff() -> None:
    # A 0.1-second hint against a constant 0.3-second backoff: each retry waits the backoff, not the hint.
    time = _Time()
    sent_at: list[float] = []

    def respond(_request: Request) -> WireResponse:
        sent_at.append(time.now)
        return WireResponse(429, (("retry-after", "0.1"),), b"") if len(sent_at) < 3 else _OK  # noqa: PLR2004

    transport = ResponderTransport(respond)
    executor = _executor(transport, time)
    # A short window makes a wait on the hint alone, which never ends on this clock, fail fast instead of hang.
    policy = ExecutionPolicy(
        max_retry_elapsed_per_request=2.0, retry=RetryPolicy(initial_delay=0.3, backoff=1.0, jitter=0)
    )

    response = await executor.execute(_request(ReplaySafety.SAFE, "crm.item.get"), policy=policy)
    await executor.coordinator.close()

    assert response.result == {"ok": 1}
    assert time.sleeps == pytest.approx([0.3, 0.3])
    assert sent_at == pytest.approx([0.0, 0.3, 0.6])


def _imf_date(moment: float) -> str:
    return email.utils.formatdate(moment, usegmt=True)


def _asctime_date(moment: float) -> str:
    return strftime("%a %b %d %H:%M:%S %Y", gmtime(moment))


def _zoneless_date(moment: float) -> str:
    return email.utils.formatdate(moment)  # the ``-0000`` form


_DATE_FORMS = {"imf": _imf_date, "asctime": _asctime_date, "zoneless": _zoneless_date}


@pytest.fixture
def east_of_utc() -> Iterator[None]:
    """Run in UTC+3, where a zoneless HTTP date read as local time would be three hours early."""
    previous = os.environ.get("TZ")
    os.environ["TZ"] = "Etc/GMT-3"
    tzset()
    try:
        yield
    finally:
        if previous is None:
            del os.environ["TZ"]
        else:
            os.environ["TZ"] = previous
        tzset()


@pytest.mark.asyncio
@pytest.mark.usefixtures("east_of_utc")
@pytest.mark.parametrize("date_form", sorted(_DATE_FORMS))
@pytest.mark.parametrize(
    "hint",
    [
        *(("retry-after", form(_SERVER_NOW + 10)) for form in _DATE_FORMS.values()),
        ("x-bitrix-ratelimit-reset", str(int(_SERVER_NOW) + 10)),
    ],
    ids=[*(f"retry-after-{name}" for name in _DATE_FORMS), "reset-epoch"],
)
async def test_a_dated_hint_is_measured_against_the_response_date(hint: tuple[str, str], date_form: str) -> None:
    time = _Time()
    date = ("date", _DATE_FORMS[date_form](_SERVER_NOW))
    transport = ResponderTransport(always(WireResponse(429, (date, hint), b"")))
    executor = _executor(transport, time)

    with pytest.raises(BudgetExceededError, match="attempt budget"):
        await executor.execute(
            _request(ReplaySafety.SAFE, "crm.item.get"), policy=ExecutionPolicy(max_attempts_per_request=1)
        )
    snapshot = await executor.coordinator.snapshot()
    await executor.coordinator.close()

    # Ten seconds after the server's own Date, however far the local clock runs ahead of it, and whichever HTTP
    # date form either header uses: a date without a zone is GMT, not local time.
    assert snapshot.cooldown_until == 10.0  # noqa: PLR2004


@pytest.mark.asyncio
@pytest.mark.parametrize(("max_elapsed", "succeeded"), [(900.0, 60), (2.5, 30)], ids=["window", "operation-bound"])
async def test_the_retry_window_opens_when_the_request_gets_its_permit(max_elapsed: float, succeeded: int) -> None:
    # 60 requests of one virtual second each share the coordinator's 10 permits under a 3-second retry window.
    time = _Time()
    in_flight: list[asyncio.Future[None]] = []

    async def one_second(_request: Request) -> WireResponse:
        answered = asyncio.get_running_loop().create_future()
        in_flight.append(answered)
        await answered
        return _OK

    transport = ResponderTransport(one_second)
    executor = _executor(transport, time)
    policy = ExecutionPolicy(max_elapsed=max_elapsed, max_retry_elapsed_per_request=3.0)
    calls = [asyncio.ensure_future(executor.execute(_request(ReplaySafety.UNKNOWN), policy=policy)) for _ in range(60)]
    while not all(call.done() for call in calls):
        await _settle(lambda: len(in_flight) == min(10, sum(not call.done() for call in calls)))
        time.now += 1.0
        for answered in in_flight:
            answered.set_result(None)
        in_flight.clear()
    await executor.coordinator.close()

    results = [call.exception() or call.result() for call in calls]
    # Each request sent got the whole window, however long it queued for its permit; the queue itself is bounded
    # by the operation's elapsed budget, and a request it stops was never sent.
    assert sum(not isinstance(result, BaseException) for result in results) == succeeded
    assert all(isinstance(result, BudgetExceededError) for result in results if isinstance(result, BaseException))
    assert transport.attempt_timeouts == [
        min(3.0, max_elapsed - wave) for wave in range(succeeded // 10) for _ in range(10)
    ]


async def _settle(ready: Callable[[], bool]) -> None:
    """Run the loop until ``ready`` holds; counted in loop turns, never in time."""
    for _turn in range(10_000):
        if ready():
            return
        await asyncio.sleep(0)
    raise AssertionError("the requests never reached the transport")


def _assert_refused_without_a_send_into_the_rest(error: BudgetExceededError, sends: int) -> None:
    """The earlier refusal stays conclusive: the request ends proven not run, never unknown."""
    assert sends == 1
    assert isinstance(error.__cause__, ApiResponseError)
    assert error.__cause__.normalized_code == "query_limit_exceeded"
    assert execution(error) is Execution.ANSWERED


@pytest.mark.asyncio
async def test_an_unsafe_retry_is_not_sent_into_the_sliver_a_throttle_hint_leaves() -> None:
    time = _Time()
    transport = ResponderTransport(replies(_refused((("retry-after", "0.95"),)), _OK))
    executor = _executor(transport, time)

    # The 0.95-second hint would leave the retry 0.05 seconds: it is refused when planned, before any wait.
    with pytest.raises(BudgetExceededError, match="retry delay") as raised:
        await executor.execute(_request(ReplaySafety.UNSAFE), policy=ExecutionPolicy(max_retry_elapsed_per_request=1.0))
    await executor.coordinator.close()

    _assert_refused_without_a_send_into_the_rest(raised.value, len(transport.requests))
    assert time.sleeps == []


@pytest.mark.asyncio
@pytest.mark.parametrize("safety", [ReplaySafety.SAFE, ReplaySafety.UNSAFE, ReplaySafety.UNKNOWN])
async def test_only_a_safe_retry_is_sent_with_less_window_than_its_wait(safety: ReplaySafety) -> None:
    # A 0.6-second backoff in a 1-second window leaves the retry 0.4 seconds.
    time = _Time()
    transport = ResponderTransport(replies(_refused(), _OK))
    executor = _executor(transport, time)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=1.0, retry=RetryPolicy(initial_delay=0.6, jitter=0))

    if safety is ReplaySafety.SAFE:
        response = await executor.execute(_request(safety), policy=policy)
        assert response.result == {"ok": 1}
        assert transport.attempt_timeouts == pytest.approx([1.0, 0.4])
    else:
        with pytest.raises(BudgetExceededError) as raised:
            await executor.execute(_request(safety), policy=policy)
        _assert_refused_without_a_send_into_the_rest(raised.value, len(transport.requests))
    await executor.coordinator.close()


@pytest.mark.asyncio
async def test_an_unsafe_retry_whose_wait_overran_is_stopped_before_its_send() -> None:
    # The plan fits (a 1-second wait, then 2 seconds of a 3-second window), but the wait overruns to 2.5 seconds,
    # as a late wake or a long permit queue would: the 0.5 seconds left are less than the wait, so nothing is sent.
    time = _Time(overrun=2.5)
    transport = ResponderTransport(replies(_refused(), _OK))
    executor = _executor(transport, time)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=3.0, retry=RetryPolicy(initial_delay=1.0, jitter=0))

    with pytest.raises(BudgetExceededError, match="shorter than the wait") as raised:
        await executor.execute(_request(ReplaySafety.UNSAFE), policy=policy)
    await executor.coordinator.close()

    _assert_refused_without_a_send_into_the_rest(raised.value, len(transport.requests))
    assert time.sleeps == [1.0]


@pytest.mark.asyncio
async def test_an_unsafe_retry_is_measured_against_the_wait_it_got_not_the_wait_planned() -> None:
    # A 0.1-second wait in a 1-second window is planned, but the send gets its turn only after 0.85 seconds (a
    # late wake or a long permit queue): the 0.15 seconds left are more than the plan but less than the wait.
    time = _Time(overrun=8.5)
    transport = ResponderTransport(replies(_refused(), _OK))
    executor = _executor(transport, time)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=1.0, retry=RetryPolicy(initial_delay=0.1, jitter=0))

    with pytest.raises(BudgetExceededError, match="shorter than the wait") as raised:
        await executor.execute(_request(ReplaySafety.UNSAFE), policy=policy)
    await executor.coordinator.close()

    _assert_refused_without_a_send_into_the_rest(raised.value, len(transport.requests))
    assert time.now == pytest.approx(0.85)


@pytest.mark.asyncio
async def test_an_unsafe_retry_whose_wait_is_exactly_half_the_window_is_refused_when_planned() -> None:
    # A 0.5-second wait in a 1-second window would leave exactly the wait, and any time between the plan and the
    # send would make the send refuse after the whole wait: the plan refuses it at once instead.
    time = _Time()
    transport = ResponderTransport(replies(_refused(), _OK))
    executor = _executor(transport, time)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=1.0, retry=RetryPolicy(initial_delay=0.5, jitter=0))

    with pytest.raises(BudgetExceededError, match="retry delay") as raised:
        await executor.execute(_request(ReplaySafety.UNSAFE), policy=policy)
    await executor.coordinator.close()

    _assert_refused_without_a_send_into_the_rest(raised.value, len(transport.requests))
    assert time.sleeps == []


@pytest.mark.asyncio
async def test_an_unsafe_retry_is_refused_on_its_backoff_when_the_hint_is_shorter() -> None:
    # A 0.1-second hint against a 0.6-second backoff: the wait is the backoff, which a 1-second window cannot
    # carry twice, so the retry is refused when planned, before any wait.
    time = _Time()
    transport = ResponderTransport(replies(_refused((("retry-after", "0.1"),)), _OK))
    executor = _executor(transport, time)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=1.0, retry=RetryPolicy(initial_delay=0.6, jitter=0))

    with pytest.raises(BudgetExceededError, match="retry delay") as raised:
        await executor.execute(_request(ReplaySafety.UNSAFE), policy=policy)
    await executor.coordinator.close()

    _assert_refused_without_a_send_into_the_rest(raised.value, len(transport.requests))
    assert time.sleeps == []


def _sent_keys(request: Request) -> list[str]:
    commands = request.copy_parameters()["cmd"]
    assert isinstance(commands, Mapping)
    return sorted(str(key) for key in commands)


@pytest.mark.asyncio
@pytest.mark.parametrize("batch", ["unsafe-alone", "safe-and-unsafe"])
async def test_a_classic_batch_replay_sends_only_its_safe_commands_into_a_short_window(batch: str) -> None:
    # Both commands are refused by quota, so both may run again; a 0.6-second backoff leaves 0.4 of a 1-second
    # window, too little for the UNSAFE command but not for the SAFE one, which is replayed alone.
    answered: list[bool] = []

    def respond(request: Request) -> object:
        keys = _sent_keys(request)
        if not answered:
            answered.append(True)
            return {"result": {"result": [], "result_error": {key: {"error": "QUERY_LIMIT_EXCEEDED"} for key in keys}}}
        return {"result": {"result": {key: {"ok": 1} for key in keys}, "result_error": []}}

    time = _Time()
    transport = ResponderTransport(respond)
    executor = _executor(transport, time)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=1.0, retry=RetryPolicy(initial_delay=0.6, jitter=0))
    mixed = batch == "safe-and-unsafe"
    unsafe_request = _request(ReplaySafety.UNSAFE)
    requests = (_request(ReplaySafety.SAFE, "crm.item.get"), unsafe_request) if mixed else (unsafe_request,)

    outcomes = await BatchExecutor(executor).execute_requests(requests, context=executor.context(policy))
    await executor.coordinator.close()

    *safe, unsafe = outcomes
    sends = [_sent_keys(request) for request in transport.requests]
    assert isinstance(unsafe, BatchFailure)
    assert isinstance(unsafe.error, BatchCommandError)
    assert unsafe.replay_disposition is ReplayDisposition.ELIGIBLE
    if mixed:
        assert len(sends) == 2  # noqa: PLR2004
        assert len(sends[1]) == 1
        assert sends[1][0] in sends[0]
        assert [type(outcome) for outcome in safe] == [BatchSuccess]
        assert time.sleeps == [0.6]
    else:
        # Nothing is waited for a round that cannot be sent.
        assert len(sends) == 1
        assert time.sleeps == []


_ENVELOPE_REFUSALS = {"status-429": WireResponse(429, (), b""), "query-limit": _refused()}


def _refused_entries(keys: list[str]) -> object:
    return {"result": {"result": [], "result_error": {key: {"error": "QUERY_LIMIT_EXCEEDED"} for key in keys}}}


def _mixed_batch(first: Callable[[list[str]], object]) -> tuple[ResponderTransport, tuple[Request, Request]]:
    """A classic batch of a SAFE command and an UNSAFE one: ``first`` answers its first send, success the rest."""

    def respond(request: Request) -> object:
        keys = _sent_keys(request)
        if len(transport.requests) == 1:
            return first(keys)
        return {"result": {"result": {key: {"ok": 1} for key in keys}, "result_error": []}}

    transport = ResponderTransport(respond)
    return transport, (_request(ReplaySafety.SAFE, "crm.item.get"), _request(ReplaySafety.UNSAFE))


def _assert_unsafe_ends_refused(outcome: object, refusal: type[Exception], match: str) -> None:
    """The UNSAFE command ends with the budget that stopped its replay, caused by the refusal: proven not run."""
    assert isinstance(outcome, BatchFailure)
    assert isinstance(outcome.error, BudgetExceededError)
    assert match in str(outcome.error)
    assert isinstance(outcome.error.__cause__, refusal)
    assert execution(outcome.error) is Execution.ANSWERED
    # As for a lone request or an UNSAFE-only envelope the budget stops: the budget itself is not retried.
    assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", sorted(_ENVELOPE_REFUSALS))
async def test_a_refused_mixed_classic_envelope_replays_its_safe_commands_into_a_short_window(refusal: str) -> None:
    # The whole envelope is refused, and a 0.6-second backoff leaves 0.4 of a 1-second window: enough for a SAFE
    # replay but not an UNSAFE one, so the SAFE command alone is sent after the same wait.
    time = _Time()
    transport, requests = _mixed_batch(lambda _keys: _ENVELOPE_REFUSALS[refusal])
    executor = _executor(transport, time)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=1.0, retry=RetryPolicy(initial_delay=0.6, jitter=0))

    safe, unsafe = await BatchExecutor(executor).execute_requests(requests, context=executor.context(policy))
    await executor.coordinator.close()

    sends = [_sent_keys(request) for request in transport.requests]
    assert [len(keys) for keys in sends] == [2, 1]
    assert time.sleeps == [0.6]
    assert isinstance(safe, BatchSuccess)
    _assert_unsafe_ends_refused(
        unsafe, HTTPGatewayError if refusal == "status-429" else ApiResponseError, "retry delay would exceed"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("refused", ["entries", "envelope"])
async def test_a_mixed_classic_round_refused_at_its_send_still_carries_its_safe_commands(refused: str) -> None:
    # The plan fits (a 0.3-second wait, then 0.7 seconds of a 1-second window), but the wait overruns to 0.9
    # seconds: the 0.1 seconds left refuse the UNSAFE command at the send, and the SAFE one goes at once.
    time = _Time(overrun=3.0)
    transport, requests = _mixed_batch(_refused_entries if refused == "entries" else lambda _keys: _refused())
    executor = _executor(transport, time)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=1.0, retry=RetryPolicy(initial_delay=0.3, jitter=0))

    context = executor.context(policy)
    safe, unsafe = await BatchExecutor(executor).execute_requests(requests, context=context)
    await executor.coordinator.close()

    sends = [_sent_keys(request) for request in transport.requests]
    assert [len(keys) for keys in sends] == [2, 1]
    assert time.sleeps == [0.3]
    assert time.now == pytest.approx(0.9)
    assert (await context.snapshot()).retries == 1  # one retry, whose round the send carried on
    assert isinstance(safe, BatchSuccess)
    if refused == "entries":
        # The command's own entry answered it with a refusal, and nothing after proved more.
        assert isinstance(unsafe, BatchFailure)
        assert isinstance(unsafe.error, BatchCommandError)
        assert unsafe.replay_disposition is ReplayDisposition.ELIGIBLE
    else:
        _assert_unsafe_ends_refused(unsafe, ApiResponseError, "shorter than the wait")


@pytest.mark.asyncio
async def test_a_mixed_classic_round_with_no_window_for_any_replay_waits_for_nothing() -> None:
    # A 1-second backoff fills the 1-second window: no command may be sent again, so nothing is waited.
    time = _Time()
    transport, requests = _mixed_batch(lambda _keys: _refused())
    executor = _executor(transport, time)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=1.0, retry=RetryPolicy(initial_delay=1.0, jitter=0))

    safe, unsafe = await BatchExecutor(executor).execute_requests(requests, context=executor.context(policy))
    await executor.coordinator.close()

    assert len(transport.requests) == 1
    assert time.sleeps == []
    for outcome in (safe, unsafe):
        assert isinstance(outcome, BatchFailure)
        assert isinstance(outcome.error, BudgetExceededError)
        assert isinstance(outcome.error.__cause__, ApiResponseError)


@pytest.mark.asyncio
async def test_a_fail_fast_classic_batch_is_never_split_by_its_replay_window() -> None:
    # The window carries a SAFE replay but not an UNSAFE one; a fail-fast batch is sent again whole or not at all.
    time = _Time()
    transport, requests = _mixed_batch(lambda _keys: _refused())
    executor = _executor(transport, time)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=1.0, retry=RetryPolicy(initial_delay=0.6, jitter=0))
    commands = tuple(_Command(index, f"c{index:012d}", request, None) for index, request in enumerate(requests))

    outcomes = await BatchExecutor(executor).execute_chunk(commands, context=executor.context(policy), halt=True)
    await executor.coordinator.close()

    assert len(transport.requests) == 1
    assert time.sleeps == []
    for outcome in outcomes:
        assert isinstance(outcome, BatchFailure)
        assert isinstance(outcome.error, BudgetExceededError)
        assert isinstance(outcome.error.__cause__, ApiResponseError)


def _time_limited_second(keys: list[str]) -> object:
    """The first command's entry is refused by quota; the second's hits its method's time limit."""
    first, second = keys
    errors = {first: {"error": "QUERY_LIMIT_EXCEEDED"}, second: {"error": "OPERATION_TIME_LIMIT"}}
    return {"result": {"result": [], "result_error": errors}}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("safety", "method"), [(ReplaySafety.UNSAFE, "crm.item.add"), (ReplaySafety.SAFE, "crm.item.list")]
)
async def test_a_held_method_never_stops_the_replay_of_another_command(safety: ReplaySafety, method: str) -> None:
    # The time limit holds the second command's method for 120 seconds, beyond the 30-second window; the first
    # command's own method is not held, so it is replayed alone after its backoff, as a lone request would be.
    time = _Time()
    transport, (safe, _unsafe) = _mixed_batch(_time_limited_second)
    executor = _executor(transport, time)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=30.0, retry=RetryPolicy(initial_delay=0.2, jitter=0))
    requests = (safe, _request(safety, method))

    first, second = await BatchExecutor(executor).execute_requests(requests, context=executor.context(policy))
    await executor.coordinator.close()

    sends = [_sent_keys(request) for request in transport.requests]
    assert [len(keys) for keys in sends] == [2, 1]
    assert sends[1] == sends[0][:1]
    assert time.sleeps == [0.2]
    assert isinstance(first, BatchSuccess)
    assert isinstance(second, BatchFailure)
    assert isinstance(second.error, BatchCommandError)
    assert second.error.normalized_code == "operation_time_limit"


def _time_limited_first(keys: list[str]) -> object:
    """The first command's entry hits its method's time limit; the second's is refused by quota."""
    first, second = keys
    errors = {first: {"error": "OPERATION_TIME_LIMIT"}, second: {"error": "QUERY_LIMIT_EXCEEDED"}}
    return {"result": {"result": [], "result_error": errors}}


def _time_limited_batch(
    time: _Time, *, time_limit: float, unrepeatable: ReplaySafety, refusals: int = 1
) -> tuple[ResponderTransport, Executor, tuple[Request, Request]]:
    """A SAFE ``get`` whose method's time limit lasts ``time_limit`` next to an ``unrepeatable`` command.

    The first send answers the ``get`` with its time limit and refuses the other by quota, as do the next
    ``refusals - 1`` sends of the other command; every later send succeeds.
    """

    def respond(request: Request) -> object:
        keys = _sent_keys(request)
        if len(transport.requests) == 1:
            return _time_limited_first(keys)
        if len(transport.requests) <= refusals:
            return _refused_entries(keys)
        return {"result": {"result": {key: {"ok": 1} for key in keys}, "result_error": []}}

    transport = ResponderTransport(respond)
    coordinator = RateCoordinator(clock=time.clock, operation_time_limit_delay=time_limit)
    executor = Executor(
        transport, coordinator=coordinator, clock=time.clock, sleep=time.sleep, random_source=lambda: 0.5
    )
    method = "crm.item.add" if unrepeatable is ReplaySafety.UNSAFE else "crm.item.update"
    return transport, executor, (_request(ReplaySafety.SAFE, "crm.item.get"), _request(unrepeatable, method))


@pytest.mark.asyncio
@pytest.mark.parametrize("unrepeatable", [ReplaySafety.UNSAFE, ReplaySafety.UNKNOWN])
async def test_a_held_safe_command_is_carried_to_a_later_round_instead_of_taking_the_replay_away(
    unrepeatable: ReplaySafety,
) -> None:
    # The get's method is held 0.6 seconds of a 1-second window: a wait an UNSAFE or UNKNOWN command could not
    # afford twice. The other command goes after its own 0.05-second backoff; the get follows in the next round,
    # after its hold (a 0.6-second backoff there outlasts the 0.55 seconds left of it).
    time = _Time()
    transport, executor, requests = _time_limited_batch(time, time_limit=0.6, unrepeatable=unrepeatable)
    policy = ExecutionPolicy(
        max_retry_elapsed_per_request=1.0, retry=RetryPolicy(initial_delay=0.05, backoff=12.0, jitter=0)
    )

    safe, other = await BatchExecutor(executor).execute_requests(requests, context=executor.context(policy))
    await executor.coordinator.close()

    sends = [_sent_keys(request) for request in transport.requests]
    get, add = sends[0]
    assert sends == [[get, add], [add], [get]]
    assert time.sleeps == pytest.approx([0.05, 0.6])
    assert isinstance(safe, BatchSuccess)
    assert isinstance(other, BatchSuccess)


@pytest.mark.asyncio
async def test_a_safe_command_held_beyond_the_window_never_holds_back_the_others() -> None:
    time = _Time()
    transport, executor, requests = _time_limited_batch(time, time_limit=120.0, unrepeatable=ReplaySafety.UNSAFE)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=30.0, retry=RetryPolicy(initial_delay=0.2, jitter=0))

    safe, unsafe = await BatchExecutor(executor).execute_requests(requests, context=executor.context(policy))
    await executor.coordinator.close()

    sends = [_sent_keys(request) for request in transport.requests]
    assert [len(keys) for keys in sends] == [2, 1]
    assert sends[1] == sends[0][1:]
    assert time.sleeps == [0.2]
    assert isinstance(unsafe, BatchSuccess)
    # The get keeps the answer its entry gave it.
    assert isinstance(safe, BatchFailure)
    assert isinstance(safe.error, BatchCommandError)
    assert safe.error.normalized_code == "operation_time_limit"


@pytest.mark.asyncio
async def test_a_carried_command_is_bounded_by_the_attempt_budget() -> None:
    # The get's method is held 20 seconds of a 30-second window, more than an UNSAFE command can afford, and the
    # UNSAFE command is refused again each time: each round sends it and carries the get, spending one attempt,
    # until the third attempt ends both with the answers their entries gave them.
    time = _Time()
    transport, executor, requests = _time_limited_batch(
        time, time_limit=20.0, unrepeatable=ReplaySafety.UNSAFE, refusals=3
    )
    policy = ExecutionPolicy(
        max_attempts_per_request=3,
        max_retry_elapsed_per_request=30.0,
        retry=RetryPolicy(initial_delay=0.1, backoff=1.0, jitter=0),
    )

    safe, unsafe = await BatchExecutor(executor).execute_requests(requests, context=executor.context(policy))
    await executor.coordinator.close()

    sends = [_sent_keys(request) for request in transport.requests]
    assert [len(keys) for keys in sends] == [2, 1, 1]
    assert time.sleeps == pytest.approx([0.1, 0.1])
    for outcome, code in ((safe, "operation_time_limit"), (unsafe, "query_limit_exceeded")):
        assert isinstance(outcome, BatchFailure)
        assert isinstance(outcome.error, BatchCommandError)
        assert outcome.error.normalized_code == code


@pytest.mark.asyncio
async def test_a_permit_failure_after_a_round_refused_at_its_send_ends_the_carried_commands() -> None:
    # The refused envelope's replay is planned, the wait overruns, and the UNSAFE command is refused at its send;
    # the SAFE command's round then fails to get its permit. That failure is its outcome: nothing is planned again.
    time = _Time(overrun=3.0)
    transport, requests = _mixed_batch(lambda _keys: WireResponse(429, (), b""))
    executor = _executor(transport, time)
    acquire = executor.coordinator.acquire
    permits = 0

    async def acquire_once_more(
        work_class: WorkClass, *, methods: frozenset[str], budget: DeadlineBudget | None = None
    ) -> _Permit:
        nonlocal permits
        permits += 1
        if permits == 3:  # noqa: PLR2004 - the first send, the refused round, then the carried round
            raise TimeoutError
        return await acquire(work_class, methods=methods, budget=budget)

    executor.coordinator.acquire = acquire_once_more  # type: ignore[method-assign]
    policy = ExecutionPolicy(max_retry_elapsed_per_request=1.0, retry=RetryPolicy(initial_delay=0.3, jitter=0))

    safe, unsafe = await BatchExecutor(executor).execute_requests(requests, context=executor.context(policy))
    await executor.coordinator.close()

    assert len(transport.requests) == 1
    assert permits == 3  # noqa: PLR2004
    assert isinstance(safe, BatchFailure)
    assert isinstance(safe.error, BudgetExceededError)
    assert "permit wait" in str(safe.error)
    assert isinstance(safe.error.__cause__, HTTPGatewayError)
    _assert_unsafe_ends_refused(unsafe, HTTPGatewayError, "shorter than the wait")


def _holds_pass_in_virtual_time(executor: Executor, time: _Time) -> None:
    """A permit for a held method waits the hold out on the virtual clock, as the coordinator does on the real one."""
    acquire = executor.coordinator.acquire

    async def acquire_after_hold(
        work_class: WorkClass, *, methods: frozenset[str], budget: DeadlineBudget | None = None
    ) -> _Permit:
        time.now += executor.coordinator.admission_delay(methods)
        return await acquire(work_class, methods=methods, budget=budget)

    executor.coordinator.acquire = acquire_after_hold  # type: ignore[method-assign]


@pytest.mark.asyncio
async def test_a_held_safe_command_carried_past_the_last_attempt_keeps_its_answer() -> None:
    # As when the get is carried, but the second round spends the last attempt: no round sends the get later, so
    # the attempt budget stops it, and it keeps the answer its entry gave it.
    time = _Time()
    transport, executor, requests = _time_limited_batch(time, time_limit=0.6, unrepeatable=ReplaySafety.UNSAFE)
    policy = ExecutionPolicy(
        max_attempts_per_request=2,
        max_retry_elapsed_per_request=1.0,
        retry=RetryPolicy(initial_delay=0.05, jitter=0),
    )

    safe, unsafe = await BatchExecutor(executor).execute_requests(requests, context=executor.context(policy))
    await executor.coordinator.close()

    sends = [_sent_keys(request) for request in transport.requests]
    get, add = sends[0]
    assert sends == [[get, add], [add]]
    assert isinstance(unsafe, BatchSuccess)
    assert isinstance(safe, BatchFailure)
    assert isinstance(safe.error, BatchCommandError)
    assert safe.error.normalized_code == "operation_time_limit"


def _carried_past_an_unknown_round(
    time: _Time, third: WireResponse | None
) -> tuple[ResponderTransport, Executor, tuple[Request, Request, Request]]:
    """A list, a get and an add: the first send is refused whole while the get's method is held 0.6 seconds.

    The get is carried past the second send, of the list and the add, which ends unknown (a bare 500); ``third``
    answers the third send.
    """

    async def respond(_request: Request) -> object:
        sent = len(transport.requests)
        if sent == 1:
            await executor.coordinator.observe_operation_time_limit("crm.item.get", delay=0.6)
            return WireResponse(429, (), b"")
        return WireResponse(500, (), b"oops") if sent == 2 or third is None else third  # noqa: PLR2004

    transport = ResponderTransport(respond)
    executor = _executor(transport, time)
    _holds_pass_in_virtual_time(executor, time)
    requests = (
        _request(ReplaySafety.SAFE, "crm.item.list"),
        _request(ReplaySafety.SAFE, "crm.item.get"),
        _request(ReplaySafety.UNSAFE),
    )
    return transport, executor, requests


@pytest.mark.asyncio
async def test_a_held_safe_command_carried_past_the_last_attempt_ends_with_its_own_refusal() -> None:
    # The second round spends the last attempt and the get is carried past it: the attempt budget stops it, caused
    # by the refusal of the one send it was in, though the second send, which it was not in, may have run.
    time = _Time()
    transport, executor, requests = _carried_past_an_unknown_round(time, None)
    policy = ExecutionPolicy(
        max_attempts_per_request=2,
        max_retry_elapsed_per_request=1.0,
        retry=RetryPolicy(initial_delay=0.05, jitter=0),
    )

    listed, get, add = await BatchExecutor(executor).execute_requests(requests, context=executor.context(policy))
    await executor.coordinator.close()

    assert [len(_sent_keys(request)) for request in transport.requests] == [3, 2]
    assert isinstance(get, BatchFailure)
    assert isinstance(get.error, BudgetExceededError)
    assert "attempt budget exhausted" in str(get.error)
    assert isinstance(get.error.__cause__, HTTPGatewayError)
    assert get.error.__cause__.http_status == 429  # noqa: PLR2004
    assert execution(get.error) is Execution.ANSWERED
    for unknown in (listed, add):
        assert isinstance(unknown, BatchFailure)
        assert isinstance(unknown.error, AmbiguousExecutionError)


@pytest.mark.asyncio
@pytest.mark.parametrize(("stop", "status"), [("hold", 504), ("permit", 408)])
async def test_a_carried_command_stopped_at_its_permit_reports_the_failure_it_follows(stop: str, status: int) -> None:
    # The get is carried past the second round, which sends the add alone and ends unknown. The get's own round is
    # then stopped at its permit: by a 5-second hold another request learns during its wait, or by a permit wait
    # that runs out. The get ends with that stop caused by the refusal it follows, not by the failure of the
    # round it was not in, which is the latest failure of the batch.
    time = _Time()
    coordinator = RateCoordinator(clock=time.clock)

    async def sleep(seconds: float) -> None:
        await time.sleep(seconds)
        if stop == "hold" and len(time.sleeps) == 2:  # noqa: PLR2004 - the wait before the get's round
            await coordinator.observe_operation_time_limit("crm.item.get", delay=5.0)

    async def respond(_request: Request) -> object:
        if len(transport.requests) == 1:
            await coordinator.observe_operation_time_limit("crm.item.get", delay=0.6)
            return WireResponse(429, (), b"")
        return WireResponse(status, (), b"lost")

    transport = ResponderTransport(respond)
    executor = Executor(transport, coordinator=coordinator, clock=time.clock, sleep=sleep, random_source=lambda: 0.5)
    acquire = coordinator.acquire

    async def acquire_twice(
        work_class: WorkClass, *, methods: frozenset[str], budget: DeadlineBudget | None = None
    ) -> _Permit:
        if stop == "permit" and len(transport.requests) == 2:  # noqa: PLR2004 - the get's round
            raise TimeoutError
        return await acquire(work_class, methods=methods, budget=budget)

    coordinator.acquire = acquire_twice  # type: ignore[method-assign]
    policy = ExecutionPolicy(
        max_retry_elapsed_per_request=1.0, retry=RetryPolicy(initial_delay=0.3, backoff=2.0, jitter=0)
    )
    requests = (_request(ReplaySafety.SAFE, "crm.item.get"), _request(ReplaySafety.UNSAFE))

    get, add = await BatchExecutor(executor).execute_requests(requests, context=executor.context(policy))
    await coordinator.close()

    sends = [_sent_keys(request) for request in transport.requests]
    assert [len(keys) for keys in sends] == [2, 1]
    assert time.sleeps == pytest.approx([0.3, 0.6])
    assert isinstance(add, BatchFailure)
    assert isinstance(add.error, AmbiguousExecutionError)
    assert isinstance(get, BatchFailure)
    assert isinstance(get.error, BudgetExceededError)
    assert ("coordinator hold" if stop == "hold" else "permit wait") in str(get.error)
    assert isinstance(get.error.__cause__, HTTPGatewayError)
    assert get.error.__cause__.http_status == 429  # noqa: PLR2004
    assert execution(get.error) is Execution.ANSWERED


@pytest.mark.asyncio
async def test_a_command_carried_past_an_unknown_round_and_then_stopped_ends_as_proven_not_run() -> None:
    # The get was sent once, in the refused first send. The third round's 1.2-second backoff exceeds the window,
    # so the budget stops the list and the get: the list may have run in the second send, the get never did.
    time = _Time()
    transport, executor, requests = _carried_past_an_unknown_round(time, None)
    policy = ExecutionPolicy(
        max_attempts_per_request=3,
        max_retry_elapsed_per_request=1.0,
        retry=RetryPolicy(initial_delay=0.05, backoff=24.0, jitter=0),
    )

    listed, get, add = await BatchExecutor(executor).execute_requests(requests, context=executor.context(policy))
    await executor.coordinator.close()

    sends = [_sent_keys(request) for request in transport.requests]
    assert [len(keys) for keys in sends] == [3, 2]
    assert isinstance(get, BatchFailure)
    assert isinstance(get.error, BudgetExceededError)
    assert "retry delay" in str(get.error)
    assert isinstance(get.error.__cause__, HTTPGatewayError)
    assert get.error.__cause__.http_status == 429  # noqa: PLR2004
    assert execution(get.error) is Execution.ANSWERED
    for unknown in (listed, add):
        assert isinstance(unknown, BatchFailure)
        assert isinstance(unknown.error, AmbiguousExecutionError)
        assert execution(unknown.error) is Execution.POSSIBLY_RUN


@pytest.mark.asyncio
async def test_a_round_failure_shared_with_a_carried_command_reports_what_each_command_sent() -> None:
    # The get rejoins the list in the third round, after its hold, and a conclusive 400 ends both. The list may
    # have run in the second send, which the get was carried past: only the list ends unknown.
    time = _Time()
    rejected = WireResponse(400, (), b"bad")
    transport, executor, requests = _carried_past_an_unknown_round(time, rejected)
    policy = ExecutionPolicy(
        max_attempts_per_request=3,
        max_retry_elapsed_per_request=1.0,
        retry=RetryPolicy(initial_delay=0.05, backoff=1.0, jitter=0),
    )

    listed, get, add = await BatchExecutor(executor).execute_requests(requests, context=executor.context(policy))
    await executor.coordinator.close()

    sends = [_sent_keys(request) for request in transport.requests]
    assert [len(keys) for keys in sends] == [3, 2, 2]
    assert sends[2] == [sends[0][0], sends[0][1]]
    assert time.now == pytest.approx(0.6)
    assert isinstance(get, BatchFailure)
    assert isinstance(get.error, HTTPGatewayError)
    assert get.error.http_status == 400  # noqa: PLR2004
    assert execution(get.error) is Execution.ANSWERED
    assert isinstance(listed, BatchFailure)
    assert isinstance(listed.error, AmbiguousExecutionError)
    assert execution(listed.error) is Execution.POSSIBLY_RUN
    assert isinstance(listed.error.__cause__, HTTPGatewayError)
    assert listed.error.__cause__.http_status == 500  # noqa: PLR2004
    assert isinstance(add, BatchFailure)
    assert execution(add.error) is Execution.POSSIBLY_RUN


@pytest.mark.asyncio
async def test_a_fail_fast_round_waits_for_the_longest_hold_of_any_of_its_commands() -> None:
    # The envelope is refused while the second command's method learns a 0.6-second hold, which the UNSAFE
    # command cannot afford twice in a 1-second window: the whole round is refused when planned, before any wait.
    time = _Time()
    executor_box: list[Executor] = []

    async def respond(_request: Request) -> object:
        await executor_box[0].coordinator.observe_operation_time_limit("crm.item.add", delay=0.6)
        return WireResponse(429, (), b"")

    transport = ResponderTransport(respond)
    executor = _executor(transport, time)
    executor_box.append(executor)
    _holds_pass_in_virtual_time(executor, time)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=1.0, retry=RetryPolicy(initial_delay=0.05, jitter=0))
    requests = (_request(ReplaySafety.SAFE, "crm.item.get"), _request(ReplaySafety.UNSAFE))
    commands = tuple(_Command(index, f"c{index:012d}", request, None) for index, request in enumerate(requests))

    outcomes = await BatchExecutor(executor).execute_chunk(commands, context=executor.context(policy), halt=True)
    await executor.coordinator.close()

    assert len(transport.requests) == 1
    assert time.sleeps == []
    assert time.now == 0.0
    for outcome in outcomes:
        assert isinstance(outcome, BatchFailure)
        assert isinstance(outcome.error, BudgetExceededError)
        assert "retry delay" in str(outcome.error)
        assert isinstance(outcome.error.__cause__, HTTPGatewayError)


@pytest.mark.asyncio
async def test_an_unrepeatable_command_the_window_refuses_never_holds_back_a_held_safe_one() -> None:
    # Both entries are refused by quota and the get's method learns a 0.6-second hold. A 0.55-second backoff is more
    # than the UNSAFE command can afford twice in a 1-second window, so it is refused and does not limit the round:
    # the get goes alone after its hold, as a lone SAFE request would.
    time = _Time()
    executor_box: list[Executor] = []

    async def respond(request: Request) -> object:
        keys = _sent_keys(request)
        if len(transport.requests) == 1:
            await executor_box[0].coordinator.observe_operation_time_limit("crm.item.get", delay=0.6)
            return _refused_entries(keys)
        return {"result": {"result": {key: {"ok": 1} for key in keys}, "result_error": []}}

    transport = ResponderTransport(respond)
    executor = _executor(transport, time)
    executor_box.append(executor)
    _holds_pass_in_virtual_time(executor, time)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=1.0, retry=RetryPolicy(initial_delay=0.55, jitter=0))
    requests = (_request(ReplaySafety.SAFE, "crm.item.get"), _request(ReplaySafety.UNSAFE))

    safe, unsafe = await BatchExecutor(executor).execute_requests(requests, context=executor.context(policy))
    await executor.coordinator.close()

    sends = [_sent_keys(request) for request in transport.requests]
    assert [len(keys) for keys in sends] == [2, 1]
    assert sends[1] == sends[0][:1]
    # The hold outlasts the backoff: the permit waits it out, and the request does not sleep.
    assert time.sleeps == []
    assert time.now == pytest.approx(0.6)
    assert isinstance(safe, BatchSuccess)
    assert isinstance(unsafe, BatchFailure)
    assert isinstance(unsafe.error, BatchCommandError)
    assert unsafe.replay_disposition is ReplayDisposition.ELIGIBLE


_ERROR_SPEC = ResultErrorSpec(ResultSelector(("error",)), ("code",), ("message",))
_V3_REFUSAL = {"error": {"code": "QUERY_LIMIT_EXCEEDED", "message": "slow down"}}


def _v3(identifier: int, safety: ReplaySafety) -> Request:
    method = "x.item.get" if safety is ReplaySafety.SAFE else "x.item.add"
    return Request(method, {"id": identifier}, safety, route=RouteKind.API_V3, result_error=_ERROR_SPEC)


@pytest.mark.asyncio
async def test_a_rest_v3_batch_replays_its_safe_envelope_but_not_an_unsafe_one_into_a_short_window() -> None:
    # REST v3 envelopes carry one safety class, so the SAFE command and the UNSAFE one are refused in separate
    # envelopes, each with its own window: the SAFE one is replayed, the UNSAFE one keeps its refusal unwaited.
    safe, unsafe = _v3(1, ReplaySafety.SAFE), _v3(2, ReplaySafety.UNSAFE)
    transport = ScriptedTransport(
        (
            ScriptedExchange.v3_batch((safe,), (_V3_REFUSAL,)),
            ScriptedExchange.v3_batch((safe,), ({"item": {"id": 1}},)),
            ScriptedExchange.v3_batch((unsafe,), (_V3_REFUSAL,)),
        )
    )
    time = _Time()
    executor = _executor(transport, time)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=1.0, retry=RetryPolicy(initial_delay=0.6, jitter=0))

    first, second = await BatchExecutor(executor).execute_requests((safe, unsafe), context=executor.context(policy))
    await executor.coordinator.close()

    transport.assert_exhausted()
    assert isinstance(first, BatchSuccess)
    assert isinstance(second, BatchFailure)
    assert isinstance(second.error, BatchCommandError)
    assert second.replay_disposition is ReplayDisposition.ELIGIBLE
    assert time.sleeps == [0.6]


@pytest.mark.asyncio
@pytest.mark.parametrize(("overrun", "match"), [(1.0, "retry delay"), (3.0, "shorter than the wait")])
async def test_a_refused_rest_v3_envelope_ends_as_a_classic_batch_when_the_window_refuses_its_replay(
    overrun: float, match: str
) -> None:
    # The UNSAFE envelope is refused with a 429; its replay does not fit the 1-second window, either when planned
    # (a 0.6-second backoff) or at its send (a 0.3-second backoff that overruns to 0.9 seconds).
    unsafe = _v3(2, ReplaySafety.UNSAFE)
    transport = ScriptedTransport(
        (
            ScriptedExchange.v3_batch((unsafe,), payload={}, status=429),
            ScriptedExchange.v3_batch((unsafe,), ({"item": {"id": 2}},)),
        )
    )
    executor = _executor(transport, _Time(overrun=overrun))
    backoff = 0.6 if overrun == 1.0 else 0.3
    policy = ExecutionPolicy(max_retry_elapsed_per_request=1.0, retry=RetryPolicy(initial_delay=backoff, jitter=0))

    (outcome,) = await BatchExecutor(executor).execute_requests((unsafe,), context=executor.context(policy))
    await executor.coordinator.close()

    assert len(transport.wire_calls) == 1
    _assert_unsafe_ends_refused(outcome, HTTPGatewayError, match)


@pytest.mark.asyncio
async def test_a_refused_rest_v3_envelope_stopped_at_its_permit_ends_as_a_classic_batch() -> None:
    # While the replay of the refused envelope waits, the host is held for two minutes: the permit is refused, and
    # the command ends with that stop, caused by the refusal, as a classic batch's and a lone request's do.
    unsafe = _v3(2, ReplaySafety.UNSAFE)
    transport = ScriptedTransport((ScriptedExchange.v3_batch((unsafe,), payload={}, status=429),))
    time = _Time()
    coordinator = RateCoordinator(clock=time.clock)

    async def sleep(seconds: float) -> None:
        await coordinator.observe_throttle(120.0, reason="http_429")
        await time.sleep(seconds)

    executor = Executor(transport, coordinator=coordinator, clock=time.clock, sleep=sleep, random_source=lambda: 0.5)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=10.0, retry=RetryPolicy(initial_delay=0.3, jitter=0))

    (outcome,) = await BatchExecutor(executor).execute_requests((unsafe,), context=executor.context(policy))
    await coordinator.close()

    transport.assert_exhausted()
    _assert_unsafe_ends_refused(outcome, HTTPGatewayError, "coordinator hold")


@pytest.mark.asyncio
async def test_a_rest_v3_envelope_opens_its_window_when_it_gets_its_permit() -> None:
    request = _v3(1, ReplaySafety.SAFE)
    transport = ScriptedTransport((ScriptedExchange.v3_batch((request,), ({"item": {"id": 1}},)),))
    time = _Time()
    executor = _executor(transport, time, max_concurrency=1)
    coordinator = executor.coordinator
    held = await coordinator.acquire(WorkClass.INTERACTIVE_DIRECT, methods=frozenset({"other.method"}))
    context = executor.context(ExecutionPolicy(max_retry_elapsed_per_request=3.0))
    run = asyncio.ensure_future(BatchExecutor(executor).execute_requests((request,), context=context))
    for _turn in range(10_000):
        if any(count for _class, count in (await coordinator.snapshot()).queued):
            break
        await asyncio.sleep(0)
    # The envelope queued five seconds, longer than its 3-second window, before it got the permit.
    time.now += 5.0
    await held.release()
    (outcome,) = await run
    await coordinator.close()

    transport.assert_exhausted()
    assert isinstance(outcome, BatchSuccess)


@pytest.mark.asyncio
async def test_a_known_hold_longer_than_the_operation_budget_fails_without_waiting() -> None:
    transport = ResponderTransport(always(_OK))
    executor = _executor(transport, _Time())
    await executor.coordinator.observe_operation_time_limit("crm.item.get", delay=120.0)

    with pytest.raises(BudgetExceededError, match="coordinator hold"):
        await executor.execute(_request(ReplaySafety.SAFE, "crm.item.get"), policy=ExecutionPolicy(max_elapsed=1.0))
    await executor.coordinator.close()

    assert transport.requests == []


@pytest.mark.asyncio
async def test_a_hold_that_appears_during_the_retry_wait_fails_with_the_failure_that_asked_for_the_retry() -> None:
    time = _Time()
    transport = ResponderTransport(replies(_refused(), _OK))
    coordinator = RateCoordinator(clock=time.clock)

    async def sleep(seconds: float) -> None:
        # While the retry waits, another request learns of a two-minute hold on the host.
        await coordinator.observe_throttle(120.0, reason="http_429")
        await time.sleep(seconds)

    executor = Executor(transport, coordinator=coordinator, clock=time.clock, sleep=sleep)

    with pytest.raises(BudgetExceededError, match="coordinator hold") as raised:
        await executor.execute(_request(ReplaySafety.SAFE, "crm.item.get"), policy=ExecutionPolicy(max_elapsed=10.0))
    await executor.coordinator.close()

    assert len(transport.requests) == 1
    assert isinstance(raised.value.__cause__, ApiResponseError)
    assert raised.value.__cause__.normalized_code == "query_limit_exceeded"


@pytest.mark.asyncio
async def test_the_admission_delay_of_no_method_is_the_host_cooldown() -> None:
    coordinator = RateCoordinator(clock=lambda: 0.0)
    assert coordinator.admission_delay(frozenset()) == 0.0
    await coordinator.observe_throttle(5.0, reason="http_429")
    assert coordinator.admission_delay(frozenset()) == 5.0  # noqa: PLR2004
    await coordinator.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [408, 504])
async def test_a_request_timeout_or_gateway_timeout_is_retried_for_safe_work(status: int) -> None:
    time = _Time()
    transport = ResponderTransport(replies(WireResponse(status, (), b"timeout"), _OK))
    executor = _executor(transport, time)

    response = await executor.execute(_request(ReplaySafety.SAFE, "crm.item.get"), policy=ExecutionPolicy())
    await executor.coordinator.close()

    assert response.result == {"ok": 1}
    assert len(transport.requests) == 2  # noqa: PLR2004


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [408, 504])
@pytest.mark.parametrize("safety", [ReplaySafety.UNSAFE, ReplaySafety.UNKNOWN])
async def test_a_request_timeout_or_gateway_timeout_is_never_replayed_for_other_work(
    status: int, safety: ReplaySafety
) -> None:
    transport = ResponderTransport(replies(WireResponse(status, (), b"timeout"), _OK))
    executor = _executor(transport, _Time())

    with pytest.raises(AmbiguousExecutionError) as raised:
        await executor.execute(_request(safety), policy=ExecutionPolicy())
    await executor.coordinator.close()

    assert len(transport.requests) == 1
    assert isinstance(raised.value.__cause__, HTTPGatewayError)


@pytest.mark.asyncio
async def test_insufficient_storage_is_not_retried() -> None:
    transport = ResponderTransport(replies(WireResponse(507, (), b"insufficient storage"), _OK))
    executor = _executor(transport, _Time())

    with pytest.raises(HTTPGatewayError) as raised:
        await executor.execute(_request(ReplaySafety.SAFE, "crm.item.get"), policy=ExecutionPolicy())
    await executor.coordinator.close()

    assert raised.value.http_status == 507  # noqa: PLR2004
    assert len(transport.requests) == 1


def test_the_default_retry_statuses_are_the_refusals_and_the_timeouts_and_overloads() -> None:
    assert RetryPolicy().transient_http_statuses == {408, 423, 425, 429, 500, 502, 503, 504}


@pytest.mark.asyncio
async def test_a_time_limit_on_a_classic_batch_envelope_pauses_its_methods() -> None:
    time = _Time()
    limited = WireResponse(200, _JSON, b'{"error":"OPERATION_TIME_LIMIT","error_description":"wait"}')
    transport = ResponderTransport(replies(limited, _OK))
    executor = _executor(transport, time)
    policy = ExecutionPolicy(max_retry_elapsed_per_request=30.0)

    (outcome,) = await BatchExecutor(executor).execute_requests(
        (_request(ReplaySafety.SAFE, "crm.item.list"),), context=executor.context(policy)
    )
    held = executor.coordinator.admission_delay(frozenset({"crm.item.list"}))
    await executor.coordinator.close()

    # The 120-second method cooldown holds the command's own method, so its retry cannot fit the window.
    assert held == 120.0  # noqa: PLR2004
    assert len(transport.requests) == 1
    assert isinstance(outcome, BatchFailure)
    assert isinstance(outcome.error, BudgetExceededError)
    assert isinstance(outcome.error.__cause__, ApiResponseError)


def _sent_methods(request: Request) -> list[str]:
    """Return the methods a classic batch envelope carries, in its command order."""
    commands = request.copy_parameters()["cmd"]
    assert isinstance(commands, Mapping)
    return [str(command).partition("?")[0] for command in commands.values()]


def _answers_with_a_time_limit_once(method: str) -> Callable[[Request], object]:
    """Answer ``method``'s command in the first envelope with OPERATION_TIME_LIMIT, and every other with success."""
    sent: list[Request] = []

    def respond(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, Mapping)
        sent.append(request)
        errors = {
            key: {"error": "OPERATION_TIME_LIMIT", "error_description": "wait"}
            for key, command in commands.items()
            if len(sent) == 1 and str(command).partition("?")[0] == method
        }
        result = {key: {"ok": 1} for key in commands if key not in errors}
        return {"result": {"result": result, "result_error": errors or []}}

    return respond


def _assert_refused_by_its_hold(outcome: object) -> None:
    """The command was never sent: it ends with the hold's refusal, proven not run and never replayed."""
    assert isinstance(outcome, BatchFailure)
    assert isinstance(outcome.error, BudgetExceededError)
    assert str(outcome.error) == "coordinator hold exceeds execution time budget"
    assert outcome.error.__cause__ is None
    assert execution(outcome.error) is Execution.NOT_SENT
    assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE


def _neighbouring_chunk() -> tuple[_Command, ...]:
    """A chunk after the one whose ``crm.item.get`` hit its time limit: two other methods around that one."""
    requests = (
        _request(ReplaySafety.SAFE, "crm.item.list"),
        _request(ReplaySafety.SAFE, "crm.item.get"),
        _request(ReplaySafety.UNSAFE, "crm.item.add"),
    )
    return tuple(_Command(index, f"c{index:012d}", request, None) for index, request in enumerate(requests, 1))


@pytest.mark.asyncio
@pytest.mark.parametrize("max_elapsed", [900.0, 100.0], ids=["default-budget", "budget-shorter-than-the-hold"])
async def test_a_method_held_beyond_the_window_is_split_off_the_first_send_of_a_neighbouring_chunk(
    max_elapsed: float,
) -> None:
    # The first chunk's get hits its time limit: its method is held for 120 seconds, beyond the 30-second window.
    # The next chunk sends its other commands at once; its get is refused unsent. Neither waits for the hold nor
    # fails the commands of other methods, whether or not the operation's own budget outlasts the hold.
    time = _Time()
    transport = ResponderTransport(_answers_with_a_time_limit_once("crm.item.get"))
    executor = _executor(transport, time)
    _holds_pass_in_virtual_time(executor, time)
    engine = BatchExecutor(executor)
    context = executor.context(ExecutionPolicy(max_elapsed=max_elapsed, max_retry_elapsed_per_request=30.0))
    first = (_Command(0, "c000000000000", _request(ReplaySafety.SAFE, "crm.item.get"), None),)
    await engine.execute_chunk(first, context=context, halt=False, split_held=True)

    listed, held, added = await engine.execute_chunk(
        _neighbouring_chunk(), context=context, halt=False, split_held=True
    )
    await executor.coordinator.close()

    assert [_sent_methods(request) for request in transport.requests] == [
        ["crm.item.get"],
        ["crm.item.list", "crm.item.add"],
    ]
    assert time.now == 0.0
    assert isinstance(listed, BatchSuccess)
    assert isinstance(added, BatchSuccess)
    _assert_refused_by_its_hold(held)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("max_elapsed", "waited"), [(900.0, 120.0), (100.0, None)], ids=["default-budget", "budget-shorter-than-the-hold"]
)
async def test_a_fail_fast_first_send_with_a_method_held_beyond_the_window_goes_whole_to_its_permit(
    max_elapsed: float, waited: float | None
) -> None:
    # A fail-fast chunk is never split: leaving the held get out would let nothing go, since its failure fails the
    # batch. The chunk goes whole to its permit, as a lone call does: the permit waits out the 120-second hold
    # within the operation's budget and sends every command, or refuses the chunk at once, unsent, when the hold
    # outlasts what the operation has left.
    time = _Time()
    transport = ResponderTransport(_answers_with_a_time_limit_once("crm.item.get"))
    executor = _executor(transport, time)
    _holds_pass_in_virtual_time(executor, time)
    engine = BatchExecutor(executor)
    context = executor.context(ExecutionPolicy(max_elapsed=max_elapsed, max_retry_elapsed_per_request=30.0))
    first = (_Command(0, "c000000000000", _request(ReplaySafety.SAFE, "crm.item.get"), None),)
    await engine.execute_chunk(first, context=context, halt=True, split_held=True)

    outcomes = await engine.execute_chunk(_neighbouring_chunk(), context=context, halt=True, split_held=True)
    await executor.coordinator.close()

    if waited is None:
        assert len(transport.requests) == 1
        assert time.now == 0.0
        for outcome in outcomes:
            _assert_refused_by_its_hold(outcome)
        return
    assert [_sent_methods(request) for request in transport.requests][1:] == [
        ["crm.item.list", "crm.item.get", "crm.item.add"]
    ]
    assert time.now == waited
    assert all(isinstance(outcome, BatchSuccess) for outcome in outcomes)


@pytest.mark.asyncio
async def test_a_first_send_weighs_a_method_hold_by_what_it_adds_to_the_host_cooldown() -> None:
    # The host is held 60 seconds, which every command waits anyway. The list's method is held 70 seconds, 10 more
    # than that: it is sent with the add once its hold ends. The get's method is held 120 seconds, 60 more than the
    # host, beyond the 30-second window: it is refused unsent, so the others do not wait for it.
    time = _Time()
    answer = {"result": {"result": {"c000000000001": 1, "c000000000003": 3}, "result_error": []}}
    transport = ResponderTransport(always(answer))
    sent_at: list[float] = []
    send = transport.send

    async def record(request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        sent_at.append(time.now)
        return await send(request, attempt_timeout=attempt_timeout, max_response_bytes=max_response_bytes)

    transport.send = record  # type: ignore[method-assign]
    executor = _executor(transport, time)
    _holds_pass_in_virtual_time(executor, time)
    coordinator = executor.coordinator
    await coordinator.observe_throttle(60.0, reason="http_429")
    await coordinator.observe_operation_time_limit("crm.item.list", delay=70.0)
    await coordinator.observe_operation_time_limit("crm.item.get", delay=120.0)
    context = executor.context(ExecutionPolicy(max_retry_elapsed_per_request=30.0))

    listed, held, added = await BatchExecutor(executor).execute_chunk(
        _neighbouring_chunk(), context=context, halt=False, split_held=True
    )
    await coordinator.close()

    assert [_sent_methods(request) for request in transport.requests] == [["crm.item.list", "crm.item.add"]]
    assert sent_at == [70.0]
    assert isinstance(listed, BatchSuccess)
    assert isinstance(added, BatchSuccess)
    _assert_refused_by_its_hold(held)


@pytest.mark.asyncio
async def test_a_rest_v3_first_send_splits_off_a_method_held_beyond_the_window() -> None:
    held = Request("x.item.get", {"id": 1}, ReplaySafety.SAFE, route=RouteKind.API_V3, result_error=_ERROR_SPEC)
    free = Request("x.item.list", {}, ReplaySafety.SAFE, route=RouteKind.API_V3, result_error=_ERROR_SPEC)
    transport = ScriptedTransport((ScriptedExchange.v3_batch((free,), ({"items": []},)),))
    time = _Time()
    executor = _executor(transport, time)
    _holds_pass_in_virtual_time(executor, time)
    await executor.coordinator.observe_operation_time_limit("x.item.get")
    context = executor.context(ExecutionPolicy(max_retry_elapsed_per_request=30.0))

    commands = tuple(_Command(index, f"c{index:012d}", request, None) for index, request in enumerate((held, free)))
    refused, listed = await BatchExecutor(executor).execute_chunk(
        commands, context=context, halt=False, split_held=True
    )
    await executor.coordinator.close()

    transport.assert_exhausted()
    assert time.now == 0.0
    assert isinstance(listed, BatchSuccess)
    _assert_refused_by_its_hold(refused)


async def _first_send(
    commands: tuple[tuple[str, ReplaySafety], ...],
    *,
    host: float,
    held: Mapping[str, float],
    policy: ExecutionPolicy,
    halt: bool = False,
) -> tuple[list[tuple[float, list[str]]], tuple[BatchOutcome, ...]]:
    """Send one user batch chunk of ``commands`` (method, safety) while the host and ``held`` methods are held.

    Returns each envelope sent, with when it was sent on the virtual clock and the methods it carried, and the
    chunk's outcomes. Every command it carries succeeds.
    """
    time = _Time()
    sends: list[tuple[float, list[str]]] = []

    def respond(request: Request) -> object:
        sends.append((time.now, _sent_methods(request)))
        keys = request.copy_parameters()["cmd"]
        assert isinstance(keys, Mapping)
        return {"result": {"result": {key: {"ok": 1} for key in keys}, "result_error": []}}

    executor = _executor(ResponderTransport(respond), time)
    _holds_pass_in_virtual_time(executor, time)
    coordinator = executor.coordinator
    if host:
        await coordinator.observe_throttle(host, reason="http_429")
    for method, delay in held.items():
        await coordinator.observe_operation_time_limit(method, delay=delay)
    context = executor.context(policy)
    await context.start()
    chunk = tuple(
        _Command(index, f"c{index:012d}", _request(safety, method), None)
        for index, (method, safety) in enumerate(commands)
    )
    outcomes = await BatchExecutor(executor).execute_chunk(chunk, context=context, halt=halt, split_held=True)
    await coordinator.close()
    return sends, outcomes


_LIST, _GET, _ADD = "crm.item.list", "crm.item.get", "crm.item.add"


@pytest.mark.asyncio
async def test_a_method_whose_hold_after_the_host_outlasts_the_operation_is_split_off() -> None:
    # The host is held 50 seconds and the get's method 110, 60 more: within the 120-second window, but the operation
    # has only 100 seconds, so the get could never be admitted. It is refused, and the list goes once the host lets it.
    sends, (listed, held) = await _first_send(
        ((_LIST, ReplaySafety.SAFE), (_GET, ReplaySafety.SAFE)),
        host=50.0,
        held={_GET: 110.0},
        policy=ExecutionPolicy(max_elapsed=100.0, max_retry_elapsed_per_request=120.0),
    )

    assert sends == [(50.0, [_LIST])]
    assert isinstance(listed, BatchSuccess)
    _assert_refused_by_its_hold(held)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("hold", "sends"),
    [(20.0, [(20.0, [_LIST, _ADD])]), (30.0, [(0.0, [_LIST])])],
    ids=["unsafe-waits-more-than-half-the-window", "a-hold-as-long-as-the-window-is-refused"],
)
async def test_a_first_send_waits_for_a_hold_shorter_than_the_window_whatever_its_safety(
    hold: float, sends: list[tuple[float, list[str]]]
) -> None:
    # A first send repeats nothing and its window opens after the hold, so an UNSAFE command waits for any hold
    # shorter than the 30-second window, more than half of it included; a hold of the whole window is refused.
    sent, (listed, added) = await _first_send(
        ((_LIST, ReplaySafety.SAFE), (_ADD, ReplaySafety.UNSAFE)),
        host=0.0,
        held={_ADD: hold},
        policy=ExecutionPolicy(max_retry_elapsed_per_request=30.0),
    )

    assert sent == sends
    assert isinstance(listed, BatchSuccess)
    if hold < 30.0:  # noqa: PLR2004
        assert isinstance(added, BatchSuccess)
    else:
        _assert_refused_by_its_hold(added)


@pytest.mark.asyncio
@pytest.mark.parametrize("halt", [False, True], ids=["outcomes", "fail-fast"])
async def test_a_user_batch_whose_every_command_is_held_waits_at_its_permit(halt: bool) -> None:  # noqa: FBT001
    # Leaving a command out is worth it only to let others go. Every command here is held 120 seconds, beyond the
    # 30-second window, so the chunk goes whole to its permit and waits within the operation's budget, as a lone
    # request does, instead of being refused unsent and pulling the next chunk at once.
    sends, outcomes = await _first_send(
        ((_GET, ReplaySafety.SAFE), (_GET, ReplaySafety.SAFE)),
        host=0.0,
        held={_GET: 120.0},
        policy=ExecutionPolicy(max_retry_elapsed_per_request=30.0),
        halt=halt,
    )

    assert sends == [(120.0, [_GET, _GET])]
    assert all(isinstance(outcome, BatchSuccess) for outcome in outcomes)


@pytest.mark.asyncio
async def test_the_private_stream_of_counted_tails_waits_for_a_held_method_instead_of_splitting_it_off() -> None:
    # The stream that sends counted tails' pages is never split: a page left out for its method's hold would fail
    # its traversal. Its chunk waits whole at its permit for the get's 120-second hold, beyond the 30-second window,
    # and every command goes in one envelope once the hold ends.
    time = _Time()
    sends: list[tuple[float, list[str]]] = []

    def respond(request: Request) -> object:
        sends.append((time.now, _sent_methods(request)))
        keys = request.copy_parameters()["cmd"]
        assert isinstance(keys, Mapping)
        return {"result": {"result": {key: {"ok": 1} for key in keys}, "result_error": []}}

    executor = _executor(ResponderTransport(respond), time)
    _holds_pass_in_virtual_time(executor, time)
    await executor.coordinator.observe_operation_time_limit(_GET, delay=120.0)
    stream = batch_outcome_stream(
        BatchExecutor(executor),
        [_request(ReplaySafety.SAFE, method) for method in (_LIST, _GET, _ADD)],
        batch_size=3,
        policy=ExecutionPolicy(max_retry_elapsed_per_request=30.0),
    )

    outcomes = [outcome async for outcome in stream]
    await executor.coordinator.close()

    assert sends == [(120.0, [_LIST, _GET, _ADD])]
    assert all(isinstance(outcome, BatchSuccess) for outcome in outcomes)


def _cannot_wait(hold: float, host: float, policy: ExecutionPolicy) -> bool:
    """Whether a first send may not wait for a command's method hold.

    It may not when the hold, beyond the host's, outlasts the window, or with the host's outlasts the operation.
    """
    window = min(policy.max_retry_elapsed_per_request, policy.max_elapsed)
    total = max(hold, host)
    return total - host >= window or total >= policy.max_elapsed


@pytest.mark.asyncio
async def test_a_first_send_leaves_out_exactly_the_commands_that_cannot_wait() -> None:
    # Seeded scenarios of host and method holds: a command is refused exactly when it cannot wait and another of
    # its chunk can; otherwise, and always for a fail-fast chunk, which is never split, the permit admits the chunk
    # or refuses it whole when its hold outlasts the operation. Nothing is sent before the holds of what it carries
    # end.
    methods = (_LIST, _GET, _ADD)
    for seed in range(300):
        rnd = random.Random(seed)
        commands = tuple((rnd.choice(methods), rnd.choice(tuple(ReplaySafety))) for _ in range(rnd.randint(1, 6)))
        host = rnd.choice((0.0, 0.0, 10.0, 50.0, 90.0))
        held = {method: rnd.choice((0.0, 0.0, 20.0, 35.0, 70.0, 110.0, 130.0)) for method in methods}
        policy = ExecutionPolicy(
            max_elapsed=rnd.choice((40.0, 100.0, 900.0)), max_retry_elapsed_per_request=rnd.choice((30.0, 120.0))
        )
        halt = rnd.random() < 0.3  # noqa: PLR2004
        if host >= policy.max_elapsed:
            continue
        sends, outcomes = await _first_send(
            commands,
            host=host,
            held={method: delay for method, delay in held.items() if delay},
            policy=policy,
            halt=halt,
        )
        cannot = {index for index, (method, _) in enumerate(commands) if _cannot_wait(held[method], host, policy)}
        if halt or len(cannot) in {0, len(commands)}:
            whole_hold = max(max(held[method], host) for method, _ in commands)
            expected = set(range(len(commands))) if whole_hold >= policy.max_elapsed else set()
        else:
            expected = cannot
        refused = {outcome.command_index for outcome in outcomes if isinstance(outcome, BatchFailure)}
        assert refused == expected, f"seed {seed}"
        for at, carried in sends:
            assert at >= max(max(held[method], host) for method in carried), f"seed {seed}"


@pytest.mark.asyncio
async def test_a_spent_request_budget_refuses_a_send_before_it_queues_for_a_cooldown() -> None:
    # The budget's one request is spent and the host is then held a minute: the next send is refused at once,
    # instead of waiting the minute out at its permit only to be refused there.
    time = _Time()
    transport = ResponderTransport(always(_OK))
    executor = _executor(transport, time)
    _holds_pass_in_virtual_time(executor, time)
    context = executor.context(ExecutionPolicy(max_requests=1))
    await executor.execute(_request(ReplaySafety.SAFE, "crm.item.get"), context=context)
    await executor.coordinator.observe_throttle(60.0, reason="http_429")

    with pytest.raises(BudgetExceededError, match="physical request budget exhausted"):
        await executor.execute(_request(ReplaySafety.SAFE, "crm.item.get"), context=context)
    await executor.coordinator.close()

    assert len(transport.requests) == 1
    assert time.now == 0.0
    assert context.requests_refused
