"""Load limits: what a server's refusal holds (the host, one method, nothing), for how long, and who waits for it.

Time is virtual: the executor and its coordinator read an injected clock, the retry sleep and the coordinator's wait
for a hold advance it, so no test waits on the wall clock.
"""

from __future__ import annotations
import asyncio
import email.utils
import json
import time as wall
from typing import TYPE_CHECKING

import pytest

from b24api import ReplaySafety, Request, RouteKind
from b24api.batch.correlation import _Command
from b24api.batch.engine import BatchExecutor
from b24api.batch.outcome import BatchFailure, BatchSuccess
from b24api.contracts import ResultErrorSpec, ResultSelector
from b24api.contracts.policy import ExecutionPolicy, RetryPolicy
from b24api.errors import ApiResponseError, B24ApiError, BudgetExceededError, HTTPGatewayError
from b24api.execution import Executor, RateCoordinator, WireResponse
from b24api.execution.executor import _Pause, _plan_round, _SendPlan
from b24api.execution.facts import Execution, execution
from b24api.testing import ScriptedTransport
from tests.scripting import ResponderTransport, replies

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.transport import WireRequest

_JSON = (("content-type", "application/json"),)
_POLICY = ExecutionPolicy(retry=RetryPolicy(jitter=0))


class _Time:
    """A virtual clock: the retry sleep (recorded) and the coordinator's wait for a hold (not) advance it."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds
        await asyncio.sleep(0)

    async def hold(self, seconds: float) -> None:
        self.now += seconds
        await asyncio.sleep(0)


def _executor(transport: ResponderTransport | ScriptedTransport, time: _Time, *, max_concurrency: int = 10) -> Executor:
    coordinator = RateCoordinator(max_concurrency=max_concurrency, clock=time.clock, sleep=time.hold)
    return Executor(transport, coordinator=coordinator, clock=time.clock, sleep=time.sleep, random_source=lambda: 0.5)


def _refusal(code: str, *, status: int = 503, headers: tuple[tuple[str, str], ...] = ()) -> WireResponse:
    body = json.dumps({"error": code, "error_description": "limit"}).encode()
    return WireResponse(status, (*_JSON, *headers), body)


def _timed(sent_at: list[float], time: _Time, respond: Callable[[Request], object]) -> Callable[[Request], object]:
    """Record when each send left, then answer it."""

    def answer(request: Request) -> object:
        sent_at.append(time.now)
        return respond(request)

    return answer


def _in_flight(respond: Callable[[Request], object]) -> Callable[[Request], object]:
    """Answer a send only after the loop turns, so the sends that leave together are all in flight at once."""

    async def answer(request: Request) -> object:
        for _ in range(8):
            await asyncio.sleep(0)
        return respond(request)

    return answer


def _get(method: str = "crm.item.get") -> Request:
    return Request.bare(method, {"id": 1}, ReplaySafety.SAFE)


@pytest.mark.asyncio
async def test_a_declared_embedded_limit_refusal_is_retried_and_slows_the_host() -> None:
    # R5: the request's own contract finds the quota refusal inside a 200 answer. It is classified within the
    # attempt, as an HTTP refusal is: retried after the host's pause, an UNSAFE request included (it proves
    # nothing ran), instead of ending the call at once.
    time = _Time()
    spec = ResultErrorSpec(ResultSelector(("error",)), ("code",))
    request = Request.bare("entity.add", {}, ReplaySafety.UNSAFE, result_error=spec)
    transport = ResponderTransport(replies({"result": {"error": {"code": "QUERY_LIMIT_EXCEEDED"}}}, {"result": 42}))
    executor = _executor(transport, time)
    context = executor.context(_POLICY)

    response = await executor.execute(request, context=context)
    snapshot = await context.snapshot()
    await executor.coordinator.close()

    assert response.result == 42  # noqa: PLR2004
    assert len(transport.requests) == 2  # noqa: PLR2004
    assert snapshot.retries == 1
    assert snapshot.cooldown_seconds == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_a_quota_refusal_without_a_hint_holds_the_host_for_every_request() -> None:
    # T-2: the portal refuses by quota without Retry-After. The refusal pauses the host, so the next request,
    # of another method, waits out the pause instead of hitting the limit at once.
    time = _Time()
    sent_at: list[float] = []
    transport = ResponderTransport(_timed(sent_at, time, replies(_refusal("QUERY_LIMIT_EXCEEDED"), {"result": 1})))
    executor = _executor(transport, time)
    one_attempt = ExecutionPolicy(max_attempts_per_request=1, retry=RetryPolicy(jitter=0))

    with pytest.raises(BudgetExceededError, match="attempt budget"):
        await executor.execute(_get(), policy=one_attempt)
    await executor.execute(_get("crm.deal.list"), policy=one_attempt)
    await executor.coordinator.close()

    assert sent_at == [0.0, 0.5]


@pytest.mark.asyncio
async def test_a_burst_of_quota_refusals_pauses_the_host_once_and_a_refusal_after_the_pause_doubles_it() -> None:
    # Three sends leave together and are all refused: one burst, one pause of the initial delay, counted once in
    # the cooldown. Refused again after that pause, the next pause doubles; an answer ends the streak.
    time = _Time()
    sent_at: list[float] = []
    answers = iter([_refusal("QUERY_LIMIT_EXCEEDED")] * 4 + [{"result": 1}] * 3)
    transport = ResponderTransport(_timed(sent_at, time, _in_flight(lambda _request: next(answers))))
    executor = _executor(transport, time, max_concurrency=3)
    context = executor.context(_POLICY)

    await asyncio.gather(*(executor.execute(_get(f"crm.item.get{index}"), context=context) for index in range(3)))
    snapshot = await context.snapshot()
    await executor.coordinator.close()

    # The fourth send, the first retry after the burst's pause, is refused again: the host waits twice as long.
    assert sent_at == [0.0, 0.0, 0.0, 0.5, 0.5, 0.5, 1.5]
    assert snapshot.cooldown_seconds == pytest.approx(1.5)
    assert executor.coordinator.limit_streak(None, sent_at=2.0) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "code"), [(401, "expired_token"), (404, "ERROR_NOT_FOUND")])
async def test_a_hint_on_a_failure_that_is_no_load_limit_never_holds_the_host(status: int, code: str) -> None:
    # T-5: a Retry-After of an hour on an expired token or a missing entity says nothing about the load.
    time = _Time()
    sent_at: list[float] = []
    refusal = _refusal(code, status=status, headers=(("retry-after", "3600"),))
    transport = ResponderTransport(_timed(sent_at, time, replies(refusal, {"result": 1})))
    executor = _executor(transport, time)

    with pytest.raises(ApiResponseError):
        await executor.execute(_get(), policy=_POLICY)
    await executor.execute(_get("crm.deal.list"), policy=_POLICY)
    assert executor.coordinator.admission_delay(frozenset({"crm.item.get"})) == 0.0
    await executor.coordinator.close()

    assert sent_at == [0.0, 0.0]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 503])
async def test_a_hint_on_a_load_status_holds_the_host(status: int) -> None:
    time = _Time()
    sent_at: list[float] = []
    refusal = WireResponse(status, (("retry-after", "2"),), b"busy")
    transport = ResponderTransport(_timed(sent_at, time, replies(refusal, {"result": 1}, {"result": 1})))
    executor = _executor(transport, time)

    await executor.execute(_get(), policy=_POLICY)
    await executor.execute(_get("crm.deal.list"), policy=_POLICY)
    await executor.coordinator.close()

    assert sent_at == [0.0, 2.0, 2.0]


@pytest.mark.asyncio
async def test_concurrent_hints_add_to_the_cooldown_only_the_time_the_host_was_held() -> None:
    # T-12: ten sends refused at once with a 0.2-second hint hold the host 0.2 seconds, not 2.
    time = _Time()
    answers = iter([WireResponse(429, (("retry-after", "0.2"),), b"")] * 10 + [{"result": 1}] * 10)
    transport = ResponderTransport(_in_flight(lambda _request: next(answers)))
    executor = _executor(transport, time)
    context = executor.context(ExecutionPolicy(retry=RetryPolicy(initial_delay=0.1, jitter=0)))

    await asyncio.gather(*(executor.execute(_get(f"crm.item.get{index}"), context=context) for index in range(10)))
    snapshot = await context.snapshot()
    await executor.coordinator.close()

    assert snapshot.cooldown_seconds == pytest.approx(0.2)


def _now_date() -> tuple[str, str]:
    return ("date", email.utils.formatdate(wall.time(), usegmt=True))


@pytest.mark.asyncio
async def test_a_time_limit_holds_its_method_until_its_operating_time_resets_and_ends_with_its_code() -> None:
    # T-7: the method's last answer said its operating time resets in 300 seconds. Its time limit holds that
    # method, and only it, until then: longer than the 120-second window, so the call ends with the server's
    # refusal itself, its code readable, instead of a budget error hiding it in its cause.
    time = _Time()
    sent_at: list[float] = []
    answered = {"result": 1, "time": {"operating_reset_at": wall.time() + 300, "operating": 470}}
    transport = ResponderTransport(
        _timed(
            sent_at,
            time,
            replies(
                WireResponse(200, (*_JSON, _now_date()), json.dumps(answered).encode()),
                _refusal("OPERATION_TIME_LIMIT"),
                {"result": 2},
                {"result": 3},
            ),
        )
    )
    executor = _executor(transport, time)

    await executor.execute(_get(), policy=_POLICY)
    with pytest.raises(ApiResponseError) as refused:
        await executor.execute(_get(), policy=_POLICY)
    held = executor.coordinator.admission_delay(frozenset({"crm.item.get"}))
    other = await executor.execute(_get("crm.deal.list"), policy=_POLICY)
    same = await executor.execute(_get(), policy=_POLICY)
    await executor.coordinator.close()

    assert refused.value.normalized_code == "operation_time_limit"
    assert held == pytest.approx(300, abs=2)
    assert (other.result, same.result) == (2, 3)
    assert sent_at[:3] == [0.0, 0.0, 0.0]
    assert sent_at[3] == pytest.approx(300, abs=2)


@pytest.mark.asyncio
async def test_a_methods_reset_is_measured_against_the_answers_date() -> None:
    # The server's clock runs 30 seconds ahead of the local one: its answer says the method's operating time resets
    # 300 seconds after its own ``Date``, which the local clock would read as 330.
    time = _Time()
    ahead = wall.time() + 30
    answered = {"result": 1, "time": {"operating_reset_at": ahead + 300, "operating": 470}}
    date = ("date", email.utils.formatdate(ahead, usegmt=True))
    transport = ResponderTransport(replies(WireResponse(200, (*_JSON, date), json.dumps(answered).encode())))
    executor = _executor(transport, time)

    await executor.execute(_get(), policy=_POLICY)
    reset = executor.coordinator.method_reset("crm.item.get")
    await executor.coordinator.close()

    assert reset == pytest.approx(300, abs=2)


@pytest.mark.asyncio
async def test_a_time_limit_of_a_method_without_a_known_reset_holds_it_for_the_retry_backoff() -> None:
    time = _Time()
    sent_at: list[float] = []
    transport = ResponderTransport(_timed(sent_at, time, replies(_refusal("OPERATION_TIME_LIMIT"), {"result": 1})))
    executor = _executor(transport, time)

    await executor.execute(_get(), policy=_POLICY)
    await executor.coordinator.close()

    assert sent_at == [0.0, 0.5]


@pytest.mark.asyncio
async def test_a_classic_batch_command_keeps_its_result_time_and_its_method_reset() -> None:
    # П-6: each command's ``result_time`` is its answer's timing, as a direct answer's ``time`` is, and says when its
    # method's operating time resets, which a later time limit of that method holds it until.
    timing = {
        "start": 1.0,
        "finish": 1.25,
        "duration": 0.25,
        "processing": 0.0,
        "date_start": "",
        "date_finish": "",
        "operating_reset_at": wall.time() + 300,
    }
    transport = ResponderTransport(
        lambda request: {
            "result": {
                "result": dict.fromkeys(_keys(request), 1),
                "result_error": [],
                "result_time": dict.fromkeys(_keys(request), timing),
            }
        }
    )
    executor = _executor(transport, _Time())

    (outcome,) = await BatchExecutor(executor).execute_requests((_get(),), context=executor.context(_POLICY))
    reset = executor.coordinator.method_reset("crm.item.get")
    await executor.coordinator.close()

    assert isinstance(outcome, BatchSuccess)
    assert outcome.response is not None
    assert outcome.response.time is not None
    assert outcome.response.time.duration == 0.25  # noqa: PLR2004
    assert reset == pytest.approx(300, abs=2)


@pytest.mark.asyncio
@pytest.mark.parametrize("timing", [None, "late", [1]])
async def test_a_classic_batch_answer_whose_timing_cannot_be_read_keeps_its_answers(timing: object) -> None:
    # The commands' timing is diagnostic: a ``result_time`` member that is not a map of it is left out, and each
    # command keeps the answer its entry gave.
    def respond(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {"result": {"result": dict.fromkeys(commands, 1), "result_error": [], "result_time": timing}}

    executor = Executor(ResponderTransport(respond), coordinator=RateCoordinator())
    outcomes = await BatchExecutor(executor).execute_requests(
        (Request.bare("crm.item.add", {"i": 0}, ReplaySafety.UNSAFE), _get()), context=executor.context(_POLICY)
    )
    await executor.coordinator.close()

    assert all(isinstance(outcome, BatchSuccess) for outcome in outcomes)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entry",
    ["late", {"duration": -1.0, "start": 1.0, "finish": 0.0}, {"start": 10**400}, {"operating_reset_at": "soon"}],
    ids=["not-a-map", "negative", "too-large", "bad-reset"],
)
async def test_a_classic_command_whose_own_timing_cannot_be_read_keeps_its_answer(entry: object) -> None:
    # One command's own ``result_time`` entry cannot be read (whatever its decoder raises): it is left out, and both
    # commands keep the answers their entries gave.
    def respond(request: Request) -> object:
        first, second = _keys(request)
        timing = {first: entry, second: {"start": 1.0, "finish": 2.0, "duration": 1.0}}
        return {"result": {"result": {first: 1, second: 1}, "result_error": [], "result_time": timing}}

    executor = Executor(ResponderTransport(respond), coordinator=RateCoordinator())
    outcomes = await BatchExecutor(executor).execute_requests(
        (Request.bare("crm.item.add", {"i": 0}, ReplaySafety.UNSAFE), _get()), context=executor.context(_POLICY)
    )
    await executor.coordinator.close()

    assert all(isinstance(outcome, BatchSuccess) for outcome in outcomes)


def _keys(request: Request) -> list[str]:
    """The keys of the commands a classic batch envelope carries."""
    commands = request.copy_parameters()["cmd"]
    assert isinstance(commands, dict)
    return list(commands)


def _commands(*methods: str) -> tuple[_Command, ...]:
    requests = (Request.bare(method, {"id": 1}, ReplaySafety.UNSAFE) for method in methods)
    return tuple(_Command(index, f"c{index:012d}", request, None) for index, request in enumerate(requests))


def _sent_methods(request: Request) -> list[str]:
    commands = request.copy_parameters()["cmd"]
    assert isinstance(commands, dict)
    return [str(command).partition("?")[0] for command in commands.values()]


@pytest.mark.asyncio
async def test_commands_of_a_held_method_go_in_a_later_envelope_and_the_others_go_now() -> None:
    # П-1: the update's method is held 120 seconds, beyond the 30-second window but within the operation's 900.
    # The get goes now; the updates follow in a later envelope once the hold ends, instead of being refused.
    time = _Time()
    sent_at: list[float] = []
    transport = ResponderTransport(
        _timed(
            sent_at,
            time,
            lambda request: {"result": {"result": dict.fromkeys(_keys(request), 1), "result_error": []}},
        )
    )
    executor = _executor(transport, time)
    executor.coordinator.hold_method("crm.deal.update", 120.0)
    context = executor.context(ExecutionPolicy(max_retry_elapsed_per_request=30.0))

    outcomes = await BatchExecutor(executor).execute_chunk(
        _commands("crm.deal.update", "crm.deal.get", "crm.deal.update"), context=context, halt=False, split_held=True
    )
    await executor.coordinator.close()

    assert [_sent_methods(request) for request in transport.requests] == [
        ["crm.deal.get"],
        ["crm.deal.update", "crm.deal.update"],
    ]
    assert sent_at == [0.0, 120.0]
    assert all(isinstance(outcome, BatchSuccess) for outcome in outcomes)


class _Ticking(_Time):
    """A virtual clock that moves on by a nanosecond at every reading, as a real one does between two readings."""

    def clock(self) -> float:
        self.now += 1e-9
        return self.now


def _safe_commands(*methods: str) -> tuple[_Command, ...]:
    requests = (Request.bare(method, {"id": 1}, ReplaySafety.SAFE) for method in methods)
    return tuple(_Command(index, f"c{index:012d}", request, None) for index, request in enumerate(requests))


@pytest.mark.asyncio
async def test_commands_held_until_the_same_instant_go_in_one_envelope_whatever_the_clock_reads_between() -> None:
    # Every command of the held method waits for the same deadline: none is held longer because the clock moved on
    # between their readings, so they all go in one later envelope.
    time = _Ticking()
    transport = ResponderTransport(
        lambda request: {"result": {"result": dict.fromkeys(_keys(request), 1), "result_error": []}}
    )
    executor = _executor(transport, time)
    executor.coordinator.hold_method("crm.item.get", 1.0)
    context = executor.context(_POLICY)

    outcomes = await BatchExecutor(executor).execute_chunk(
        _safe_commands("crm.deal.get", *(["crm.item.get"] * 4)), context=context, halt=False, split_held=True
    )
    await executor.coordinator.close()

    assert [_sent_methods(request) for request in transport.requests] == [["crm.deal.get"], ["crm.item.get"] * 4]
    assert all(isinstance(outcome, BatchSuccess) for outcome in outcomes)


@pytest.mark.asyncio
async def test_a_replay_round_of_one_held_method_goes_whole_whatever_the_clock_reads_between() -> None:
    # The envelope is refused as a whole and its method held: its four commands wait for one deadline, so the
    # replay round sends them together, within their attempts, instead of carrying all but one.
    time = _Ticking()
    refused = _refusal("OPERATION_TIME_LIMIT", headers=(("retry-after", "1"),))
    answers = iter([refused])
    transport = ResponderTransport(
        lambda request: (
            next(answers, None) or {"result": {"result": dict.fromkeys(_keys(request), 1), "result_error": []}}
        )
    )
    executor = _executor(transport, time)
    context = executor.context(ExecutionPolicy(max_attempts_per_request=2, retry=RetryPolicy(jitter=0)))

    outcomes = await BatchExecutor(executor).execute_chunk(
        _safe_commands(*(["crm.item.get"] * 4)), context=context, halt=False, split_held=True
    )
    await executor.coordinator.close()

    assert [len(_keys(request)) for request in transport.requests] == [4, 4]
    assert all(isinstance(outcome, BatchSuccess) for outcome in outcomes)


@pytest.mark.asyncio
async def test_a_command_held_beyond_what_the_operation_has_left_is_refused_unsent() -> None:
    time = _Time()
    transport = ResponderTransport(
        lambda request: {"result": {"result": dict.fromkeys(_keys(request), 1), "result_error": []}}
    )
    executor = _executor(transport, time)
    executor.coordinator.hold_method("crm.deal.update", 120.0)
    context = executor.context(ExecutionPolicy(max_elapsed=100.0, max_retry_elapsed_per_request=30.0))

    update, get = await BatchExecutor(executor).execute_chunk(
        _commands("crm.deal.update", "crm.deal.get"), context=context, halt=False, split_held=True
    )
    await executor.coordinator.close()

    assert [_sent_methods(request) for request in transport.requests] == [["crm.deal.get"]]
    assert isinstance(get, BatchSuccess)
    assert isinstance(update, BatchFailure)
    assert isinstance(update.error, BudgetExceededError)


@pytest.mark.asyncio
async def test_a_chunk_whose_every_command_its_first_send_refuses_counts_no_envelope() -> None:
    # The operation has no time left: the first send's plan refuses each command before any permit, so no envelope
    # starts and none counts against the request budget.
    time = _Time()
    transport = ResponderTransport(lambda _request: {"result": 1})
    executor = _executor(transport, time)
    context = executor.context(ExecutionPolicy(max_elapsed=1.0))
    await context.start()
    time.now += 5.0
    envelopes = 0

    def counted() -> None:
        nonlocal envelopes
        envelopes += 1

    outcomes = await BatchExecutor(executor).execute_chunk(
        _commands("crm.deal.get", "crm.deal.list"), context=context, halt=False, split_held=True, on_envelope=counted
    )
    await executor.coordinator.close()

    assert envelopes == 0
    assert transport.requests == []
    assert all(isinstance(outcome, BatchFailure) for outcome in outcomes)


class _Portal(ScriptedTransport):
    """Answer classic and REST 3.0 batch envelopes and lone calls; record when each send left and what it carried."""

    def __init__(self, time: _Time, answer: Callable[[WireRequest], WireResponse]) -> None:
        super().__init__(())
        self.time = time
        self.answer = answer
        self.sends: list[tuple[float, str, tuple[str, ...]]] = []

    async def send_wire(self, wire: WireRequest, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        if wire.array_body is not None:
            self.sends.append((self.time.now, "v3", tuple(command.method for command in wire.array_body.commands)))
            payload: object = {"result": [{"item": 1} for _ in wire.array_body.commands]}
            return WireResponse(200, _JSON, json.dumps(payload).encode())
        if wire.method == "batch":
            commands = wire.parameters["cmd"]
            assert isinstance(commands, dict)
            self.sends.append(
                (self.time.now, "batch", tuple(str(each).partition("?")[0] for each in commands.values()))
            )
            # The envelope's own timing is the batch call's: no command's method resets with it.
            payload = {
                "result": {"result": dict.fromkeys(commands, 1), "result_error": []},
                "time": {"operating_reset_at": wall.time() + 300, "operating": 1},
            }
            return WireResponse(200, _JSON, json.dumps(payload).encode())
        self.sends.append((self.time.now, wire.method, ()))
        return self.answer(wire)


def _answered(_wire: WireRequest) -> WireResponse:
    return WireResponse(200, _JSON, b'{"result":1}')


def _portal_executor(transport: _Portal, time: _Time) -> Executor:
    coordinator = RateCoordinator(clock=time.clock, sleep=time.hold)
    return Executor(transport, coordinator=coordinator, clock=time.clock, sleep=time.sleep, random_source=lambda: 0.5)


@pytest.mark.asyncio
async def test_an_envelope_held_longer_goes_after_the_other_envelopes_of_its_chunk() -> None:
    # The update's method is held two minutes: it goes in a later envelope, after the chunk's other envelopes, which
    # go at once, the REST 3.0 one included.
    time = _Time()
    transport = _Portal(time, _answered)
    executor = _portal_executor(transport, time)
    executor.coordinator.hold_method("crm.deal.update", 120.0)
    requests = (
        Request.bare("crm.deal.update", {"id": 1}, ReplaySafety.UNSAFE),
        Request.bare("crm.deal.get", {"id": 1}, ReplaySafety.UNSAFE),
        Request("x.item.get", {"id": 1}, ReplaySafety.SAFE, route=RouteKind.API_V3),
    )
    chunk = tuple(_Command(index, f"c{index:012d}", request, None) for index, request in enumerate(requests))
    context = executor.context(ExecutionPolicy(max_retry_elapsed_per_request=30.0))

    outcomes = await BatchExecutor(executor).execute_chunk(chunk, context=context, halt=False, split_held=True)
    await executor.coordinator.close()

    assert all(isinstance(outcome, BatchSuccess) for outcome in outcomes)
    assert transport.sends == [
        (0.0, "batch", ("crm.deal.get",)),
        (0.0, "v3", ("x.item.get",)),
        (120.0, "batch", ("crm.deal.update",)),
    ]


@pytest.mark.asyncio
async def test_a_classic_envelope_timing_resets_none_of_its_commands_methods() -> None:
    # The envelope's own ``time`` says when the batch call's operating time resets, not its commands'. A later time
    # limit of a command's method holds it for the retry backoff, its reset unknown.
    time = _Time()
    refused = WireResponse(503, _JSON, b'{"error":"OPERATION_TIME_LIMIT","error_description":"x"}')
    transport = _Portal(time, lambda _wire: refused)
    executor = _portal_executor(transport, time)
    chunk = (_Command(0, "c000000000000", _get("crm.deal.get"), None),)
    await BatchExecutor(executor).execute_chunk(chunk, context=executor.context(_POLICY), halt=False, split_held=True)
    once = ExecutionPolicy(max_attempts_per_request=1, retry=RetryPolicy(jitter=0))

    with pytest.raises(B24ApiError):
        await executor.execute(_get("crm.deal.get"), policy=once)
    held = executor.coordinator.admission_delay(frozenset({"crm.deal.get"}))
    await executor.coordinator.close()

    assert held == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_a_429_without_a_hint_holds_the_portal_for_the_retry_backoff() -> None:
    time = _Time()
    answers = iter([WireResponse(429, (("content-type", "text/plain"),), b"Too Many Requests")])
    transport = _Portal(time, lambda wire: next(answers, None) or _answered(wire))
    executor = _portal_executor(transport, time)
    once = ExecutionPolicy(max_attempts_per_request=1, retry=RetryPolicy(jitter=0))

    with pytest.raises(B24ApiError):
        await executor.execute(_get(), policy=once)
    await executor.execute(_get("crm.deal.list"), policy=once)
    await executor.coordinator.close()

    assert [(at, method) for at, method, _ in transport.sends] == [(0.0, "crm.item.get"), (0.5, "crm.deal.list")]


class _EntryCodes(ScriptedTransport):
    """Answer each classic envelope with every command's entry failing with the next code of ``codes``."""

    def __init__(self, codes: list[str]) -> None:
        super().__init__(())
        self.codes = codes

    async def send_wire(self, wire: WireRequest, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        commands = wire.parameters["cmd"]
        assert isinstance(commands, dict)
        code = self.codes.pop(0)
        errors = {key: {"error": code, "error_description": "x"} for key in commands}
        return WireResponse(200, _JSON, json.dumps({"result": {"result": {}, "result_error": errors}}).encode())


@pytest.mark.asyncio
async def test_an_entry_that_fails_with_no_load_limit_ends_its_methods_streak() -> None:
    # A time limit, then an ordinary failure of the same method, then a time limit again: the failure is an answer
    # and no refusal, so the second limit starts a new streak and holds for the first backoff, not the second.
    time = _Time()
    executor = _executor(_EntryCodes(["OPERATION_TIME_LIMIT", "NOT_FOUND", "OPERATION_TIME_LIMIT"]), time)
    policy = ExecutionPolicy(
        max_attempts_per_request=1, retry=RetryPolicy(initial_delay=0.05, maximum_delay=5.0, jitter=0)
    )
    for index in range(3):
        time.now += 1.0
        chunk = (_Command(index, f"c{index:012d}", _get(), None),)
        await BatchExecutor(executor).execute_chunk(chunk, context=executor.context(policy), halt=False)
    held = executor.coordinator.admission_delay(frozenset({"crm.item.get"}))
    await executor.coordinator.close()

    assert held == pytest.approx(0.05)


class _EntryTimeLimits(ScriptedTransport):
    """Answer every command of each envelope, classic or REST 3.0, with its own entry's time limit; note each send."""

    def __init__(self, time: _Time) -> None:
        super().__init__(())
        self.time = time
        self.sent: list[float] = []

    async def send_wire(self, wire: WireRequest, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        self.sent.append(self.time.now)
        if wire.array_body is not None:
            elements = [{"error": {"code": "OPERATION_TIME_LIMIT"}} for _ in wire.array_body.commands]
            return WireResponse(200, _JSON, json.dumps({"result": elements}).encode())
        commands = wire.parameters["cmd"]
        assert isinstance(commands, dict)
        errors = {key: {"error": "OPERATION_TIME_LIMIT", "error_description": "x"} for key in commands}
        return WireResponse(200, _JSON, json.dumps({"result": {"result": [], "result_error": errors}}).encode())


@pytest.mark.asyncio
@pytest.mark.parametrize("route", [RouteKind.BARE, RouteKind.API_V3], ids=["classic-entry", "v3-element"])
async def test_an_entry_refused_again_after_its_methods_pause_began_doubles_the_hold(route: RouteKind) -> None:
    # A batch command's own entry, classic or REST 3.0, refuses it with a time limit, no reset known; its retry
    # leaves after the pause began and is refused again: the method's streak grows, so its hold doubles.
    time = _Time()
    transport = _EntryTimeLimits(time)
    executor = _executor(transport, time)
    policy = ExecutionPolicy(max_attempts_per_request=2, retry=RetryPolicy(jitter=0))
    spec = ResultErrorSpec(ResultSelector(("error",)), ("code",)) if route is RouteKind.API_V3 else None
    request = Request("crm.item.get", {"id": 1}, ReplaySafety.SAFE, route=route, result_error=spec)
    chunk = (_Command(0, "c000000000000", request, None),)

    (outcome,) = await BatchExecutor(executor).execute_chunk(chunk, context=executor.context(policy), halt=False)
    held = executor.coordinator.admission_delay(frozenset({"crm.item.get"}))
    await executor.coordinator.close()

    assert isinstance(outcome, BatchFailure)
    assert transport.sent == [0.0, 0.5]
    assert held == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_a_first_send_refuses_a_command_whose_hold_outlasts_what_the_hosts_hold_leaves() -> None:
    # The host is held 0.3 s and the operation has 1 s: a command's window opens after the host's hold, so 0.7 s
    # are left for it. One whose own method is held 0.8 s longer is refused at once, rather than carried to a later
    # envelope that its permit would then refuse; the other command goes now.
    time = _Time()
    executor = _portal_executor(_Portal(time, _answered), time)
    executor.coordinator.hold_method("b.x", 1.1)
    await executor.coordinator.observe_throttle(0.3, reason="test")
    context = executor.context(ExecutionPolicy(max_elapsed=1.0, retry=RetryPolicy(jitter=0)))
    await context.start()

    plan = executor.plan_first_send(context, methods=(frozenset({"a.x"}), frozenset({"b.x"})))
    await executor.coordinator.close()

    assert plan is not None
    assert plan.plans[0] is _SendPlan.SEND
    assert plan.plans[1].refused


def _staggered(time: _Time, respond: Callable[[Request], object]) -> Callable[[Request], object]:
    """Let each send leave 0.1 seconds after the one before it, and answer it once the sends are all in flight."""

    async def answer(request: Request) -> object:
        time.now += 0.1
        for _ in range(8):
            await asyncio.sleep(0)
        return respond(request)

    return answer


@pytest.mark.asyncio
async def test_a_burst_whose_sends_left_at_different_moments_before_the_pause_pauses_the_host_once() -> None:
    # The sends of a burst leave one after another, all before the first refusal begins the pause: every one of
    # them belongs to the burst that pause answers, so the host pauses once, for the initial delay.
    time = _Time()
    answers = iter([_refusal("QUERY_LIMIT_EXCEEDED")] * 3 + [{"result": 1}] * 3)
    transport = ResponderTransport(_staggered(time, lambda _request: next(answers)))
    executor = _executor(transport, time, max_concurrency=3)
    context = executor.context(_POLICY)

    await asyncio.gather(*(executor.execute(_get(f"crm.item.get{index}"), context=context) for index in range(3)))
    snapshot = await context.snapshot()
    await executor.coordinator.close()

    assert snapshot.cooldown_seconds == pytest.approx(0.5)


_BURST = 3
# How far the clock moves while each refusal of a burst comes back: the answers arrive one after another.
_ANSWER_LAG = 0.01
_V3_RATE_LIMIT = WireResponse(
    429, _JSON, json.dumps({"error": {"code": "BITRIX_REST_V3_EXCEPTION_RATELIMITEXCEPTION", "message": "x"}}).encode()
)


class _BurstThenAnswers(ScriptedTransport):
    """Refuse a burst of sends, all in flight at once, one answer after another as the clock moves; then answer."""

    def __init__(self, time: _Time, dialect: str) -> None:
        super().__init__(())
        self.time = time
        self.dialect = dialect
        self.sent: list[float] = []
        # Each refusal's turn: the first comes once the whole burst is in flight, each next after the one before.
        self.turns = [asyncio.Event() for _ in range(_BURST)]

    async def send_wire(self, wire: WireRequest, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        self.sent.append(self.time.now)
        number = len(self.sent)
        if number > _BURST:
            return self._answer(wire)
        if number == _BURST:
            self.turns[0].set()
        await self.turns[number - 1].wait()
        self.time.now += _ANSWER_LAG
        if number < _BURST:
            self.turns[number].set()
        return self._refusal(wire)

    def _refusal(self, wire: WireRequest) -> WireResponse:
        if self.dialect in {"call", "classic"}:
            return _refusal("QUERY_LIMIT_EXCEEDED")
        if self.dialect == "v3":
            return _V3_RATE_LIMIT
        if wire.array_body is not None:
            elements = [{"error": {"code": "QUERY_LIMIT_EXCEEDED"}} for _ in wire.array_body.commands]
            return WireResponse(200, _JSON, json.dumps({"result": elements}).encode())
        commands = wire.parameters["cmd"]
        assert isinstance(commands, dict)
        errors = {key: {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "x"} for key in commands}
        return WireResponse(200, _JSON, json.dumps({"result": {"result": [], "result_error": errors}}).encode())

    @staticmethod
    def _answer(wire: WireRequest) -> WireResponse:
        if wire.array_body is not None:
            return WireResponse(
                200, _JSON, json.dumps({"result": [{"item": 1} for _ in wire.array_body.commands]}).encode()
            )
        if wire.method == "batch":
            commands = wire.parameters["cmd"]
            assert isinstance(commands, dict)
            body = {"result": {"result": dict.fromkeys(commands, 1), "result_error": []}}
            return WireResponse(200, _JSON, json.dumps(body).encode())
        return WireResponse(200, _JSON, b'{"result":1}')


@pytest.mark.asyncio
@pytest.mark.parametrize("dialect", ["call", "classic", "v3", "classic_entry", "v3_entry"])
async def test_a_burst_refused_while_the_clock_moves_pauses_the_host_once(dialect: str) -> None:
    # Three sends leave together and are refused one after another as the clock moves: each refusal is read after the
    # first began the pause, but every send left before it. The streak counts when a send left, not when its refusal
    # came back, so the burst never grows it, on every route a refusal comes by: each refusal holds the host for the
    # initial delay from when it came back, and the pause ends that long after the last.
    time = _Time()
    transport = _BurstThenAnswers(time, dialect)
    executor = _executor(transport, time, max_concurrency=_BURST)
    context = executor.context(ExecutionPolicy(retry=RetryPolicy(initial_delay=0.1, maximum_delay=1.0, jitter=0)))
    route = RouteKind.API_V3 if dialect.startswith("v3") else RouteKind.BARE
    spec = ResultErrorSpec(ResultSelector(("error",)), ("code",)) if dialect == "v3_entry" else None

    async def one(index: int) -> None:
        request = Request(f"crm.item.get{index}", {"id": 1}, ReplaySafety.SAFE, route=route, result_error=spec)
        if dialect == "call":
            await executor.execute(request, context=context)
            return
        chunk = (_Command(0, "c000000000000", request, None),)
        (outcome,) = await BatchExecutor(executor).execute_chunk(chunk, context=context, halt=False)
        assert isinstance(outcome, BatchSuccess)

    await asyncio.gather(*(one(index) for index in range(_BURST)))
    snapshot = await context.snapshot()
    await executor.coordinator.close()

    assert transport.sent[:_BURST] == [0.0] * _BURST
    assert snapshot.cooldown_seconds == pytest.approx(0.1 + (_BURST - 1) * _ANSWER_LAG)


@pytest.mark.asyncio
@pytest.mark.parametrize("dialect", ["call", "classic", "v3", "classic_entry", "v3_entry"])
async def test_a_request_queued_for_a_permit_waits_for_the_hold_the_refusal_that_freed_it_set(dialect: str) -> None:
    # Every permit is taken by the burst, and one more request waits for a permit. The first refusal frees its permit
    # before its answer is read; the hold that answer sets holds the queued request too: it leaves once the pause
    # ends, never inside it, and the burst pauses the host once.
    time = _Time()
    transport = _BurstThenAnswers(time, dialect)
    executor = _executor(transport, time, max_concurrency=_BURST)
    context = executor.context(ExecutionPolicy(retry=RetryPolicy(initial_delay=0.1, maximum_delay=1.0, jitter=0)))
    route = RouteKind.API_V3 if dialect.startswith("v3") else RouteKind.BARE
    spec = ResultErrorSpec(ResultSelector(("error",)), ("code",)) if dialect == "v3_entry" else None

    async def one(index: int) -> None:
        request = Request(f"crm.item.get{index}", {"id": 1}, ReplaySafety.SAFE, route=route, result_error=spec)
        if dialect == "call":
            await executor.execute(request, context=context)
            return
        chunk = (_Command(0, "c000000000000", request, None),)
        (outcome,) = await BatchExecutor(executor).execute_chunk(chunk, context=context, halt=False)
        assert isinstance(outcome, BatchSuccess)

    await asyncio.gather(*(one(index) for index in range(_BURST + 1)))
    snapshot = await context.snapshot()
    await executor.coordinator.close()

    pause_end = 0.1 + _BURST * _ANSWER_LAG
    assert transport.sent[:_BURST] == [0.0] * _BURST
    assert transport.sent[_BURST] == pytest.approx(pause_end)
    assert snapshot.cooldown_seconds == pytest.approx(0.1 + (_BURST - 1) * _ANSWER_LAG)


@pytest.mark.asyncio
async def test_an_answer_to_a_send_that_left_before_the_pause_keeps_the_streak() -> None:
    # A and B leave together; A is refused, then B is answered. B left before the pause began, so its answer says
    # nothing of the limit after it: C, refused after the pause, doubles it.
    time = _Time()
    sent_at: dict[str, list[float]] = {}
    refused = asyncio.Event()

    async def respond(request: Request) -> object:
        sent_at.setdefault(request.method, []).append(time.now)
        if request.method == "a.x":
            for _ in range(4):
                await asyncio.sleep(0)
            refused.set()
            return _refusal("QUERY_LIMIT_EXCEEDED")
        if request.method == "b.y":
            await refused.wait()
            for _ in range(4):
                await asyncio.sleep(0)
            return {"result": 1}
        return _refusal("QUERY_LIMIT_EXCEEDED") if len(sent_at["c.z"]) == 1 else {"result": 1}

    executor = _executor(ResponderTransport(respond), time)
    one = ExecutionPolicy(max_attempts_per_request=1, retry=RetryPolicy(jitter=0))
    await asyncio.gather(
        executor.execute(_get("a.x"), policy=one), executor.execute(_get("b.y"), policy=one), return_exceptions=True
    )
    await executor.execute(_get("c.z"), policy=_POLICY)
    await executor.coordinator.close()

    assert sent_at["c.z"] == [0.5, 1.5]


_SECOND = 2


@pytest.mark.asyncio
async def test_a_time_limit_after_its_methods_reset_has_passed_holds_it_for_the_retry_backoff() -> None:
    # The reset the method's answer said has passed: it is unknown again, and the time limit holds the method for
    # the backoff of its streak, as with no reset known.
    time = _Time()
    sent_at: list[float] = []
    answered = {"result": 1, "time": {"operating_reset_at": wall.time() + 1, "operating": 470}}
    transport = ResponderTransport(
        _timed(
            sent_at,
            time,
            replies(
                WireResponse(200, (*_JSON, _now_date()), json.dumps(answered).encode()),
                _refusal("OPERATION_TIME_LIMIT"),
                {"result": _SECOND},
            ),
        )
    )
    executor = _executor(transport, time)

    await executor.execute(_get(), policy=_POLICY)
    time.now += 5.0
    retried = await executor.execute(_get(), policy=_POLICY)
    await executor.coordinator.close()

    assert retried.result == _SECOND
    assert sent_at == [0.0, 5.0, 5.5]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("hint", "resent_at"),
    [
        ("2", 2.0),
        # A zero hint holds nothing: the retry waits its backoff alone, not until the method's reset.
        ("0", RetryPolicy().initial_delay),
    ],
)
async def test_a_time_limit_with_a_hint_holds_its_method_for_the_hint_not_until_its_reset(
    hint: str, resent_at: float
) -> None:
    time = _Time()
    sent_at: list[float] = []
    answered = {"result": 1, "time": {"operating_reset_at": wall.time() + 100, "operating": 470}}
    transport = ResponderTransport(
        _timed(
            sent_at,
            time,
            replies(
                WireResponse(200, (*_JSON, _now_date()), json.dumps(answered).encode()),
                _refusal("OPERATION_TIME_LIMIT", headers=(("retry-after", hint),)),
                {"result": 2},
            ),
        )
    )
    executor = _executor(transport, time)

    await executor.execute(_get(), policy=_POLICY)
    await executor.execute(_get(), policy=_POLICY)
    await executor.coordinator.close()

    assert sent_at == [0.0, 0.0, resent_at]


@pytest.mark.asyncio
async def test_a_methods_time_limit_streak_is_its_own_not_the_hosts() -> None:
    # The host was refused by quota and still counts that streak; the method's first time limit is the first of its
    # own streak, held for the initial delay.
    time = _Time()
    sent_at: list[float] = []
    answers = replies(_refusal("QUERY_LIMIT_EXCEEDED"), _refusal("OPERATION_TIME_LIMIT"), {"result": 1})
    transport = ResponderTransport(_timed(sent_at, time, answers))
    executor = _executor(transport, time)
    one = ExecutionPolicy(max_attempts_per_request=1, retry=RetryPolicy(jitter=0))

    with pytest.raises(BudgetExceededError, match="attempt budget"):
        await executor.execute(_get("crm.deal.list"), policy=one)
    await executor.execute(_get(), policy=_POLICY)
    await executor.coordinator.close()

    assert sent_at == [0.0, 0.5, 1.0]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 503])
async def test_a_rest_v3_rate_limit_holds_the_host_by_its_code(status: int) -> None:
    # REST 3.0 codes keep their exact case; the rate limit is the same whatever the status that carries it.
    time = _Time()
    sent_at: list[float] = []
    body = json.dumps({"error": {"code": "BITRIX_REST_V3_EXCEPTION_RATELIMITEXCEPTION", "message": "limit"}})
    refusal = WireResponse(status, _JSON, body.encode())
    transport = ResponderTransport(_timed(sent_at, time, replies(refusal, {"result": 1})))
    executor = _executor(transport, time)
    one = ExecutionPolicy(max_attempts_per_request=1, retry=RetryPolicy(jitter=0))

    with pytest.raises(BudgetExceededError, match="attempt budget") as refused:
        await executor.execute(_get(), policy=one)
    await executor.execute(_get("crm.deal.list"), policy=one)
    await executor.coordinator.close()

    assert isinstance(refused.value.__cause__, ApiResponseError)
    assert refused.value.__cause__.normalized_code == "BITRIX_REST_V3_EXCEPTION_RATELIMITEXCEPTION"
    assert sent_at == [0.0, 0.5]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "code", "held"),
    [
        (400, "ERROR_ARGUMENT", 0.5),
        (403, "ACCESS_DENIED", 0.5),
        (500, "INTERNAL_SERVER_ERROR", 0.5),
        (503, "BUSY", 1.0),
    ],
)
async def test_a_direct_answer_that_fails_about_anything_but_the_load_ends_the_hosts_streak(
    status: int, code: str, held: float
) -> None:
    # A quota refusal, then a request sent after its pause answered with an ordinary failure, then a quota refusal
    # again: the failure answered the request, as a command's entry does, so the second refusal starts a new streak
    # and holds the host for the first backoff. A 503 is about the load: it ends nothing, and the streak doubles.
    time = _Time()
    executor = _executor(
        ResponderTransport(
            replies(_refusal("QUERY_LIMIT_EXCEEDED"), _refusal(code, status=status), _refusal("QUERY_LIMIT_EXCEEDED"))
        ),
        time,
    )
    one = ExecutionPolicy(max_attempts_per_request=1, retry=RetryPolicy(jitter=0))
    for method in ("a.x", "b.y", "c.z"):
        with pytest.raises(B24ApiError):
            await executor.execute(_get(method), policy=one)
    hold = executor.coordinator.admission_delay(frozenset({"d.w"}))
    await executor.coordinator.close()

    assert hold == pytest.approx(held)


@pytest.mark.asyncio
async def test_a_direct_answer_that_fails_with_no_load_limit_ends_its_methods_streak() -> None:
    # As a command's entry does: a time limit, an ordinary failure of the same method, a time limit again.
    time = _Time()
    answers = replies(
        _refusal("OPERATION_TIME_LIMIT"), _refusal("NOT_FOUND", status=400), _refusal("OPERATION_TIME_LIMIT")
    )
    executor = _executor(ResponderTransport(answers), time)
    policy = ExecutionPolicy(
        max_attempts_per_request=1, retry=RetryPolicy(initial_delay=0.05, maximum_delay=5.0, jitter=0)
    )
    for _ in range(3):
        time.now += 1.0
        with pytest.raises(B24ApiError):
            await executor.execute(_get(), policy=policy)
    held = executor.coordinator.admission_delay(frozenset({"crm.item.get"}))
    await executor.coordinator.close()

    assert held == pytest.approx(0.05)


_BACKOFF = 5.0


class _FirstElementFails(ScriptedTransport):
    """Answer a batch envelope with its first command's element carrying ``code`` and every other a plain item."""

    def __init__(self, code: str) -> None:
        super().__init__(())
        self.code = code

    async def send_wire(self, wire: WireRequest, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        failed: dict[str, object] = {"error": {"code": self.code}}
        if wire.array_body is not None:
            elements = [failed, *({"item": 1} for _ in wire.array_body.commands[1:])]
            return WireResponse(200, _JSON, json.dumps({"result": elements}).encode())
        commands = wire.parameters["cmd"]
        assert isinstance(commands, dict)
        entries = {key: failed if index == 0 else {"item": 1} for index, key in enumerate(commands)}
        return WireResponse(200, _JSON, json.dumps({"result": {"result": entries, "result_error": []}}).encode())


@pytest.mark.asyncio
@pytest.mark.parametrize("route", [RouteKind.API_V3, RouteKind.BARE])
@pytest.mark.parametrize(
    ("code", "method_held", "host_held"),
    [("OPERATION_TIME_LIMIT", _BACKOFF, 0.0), ("QUERY_LIMIT_EXCEEDED", _BACKOFF, _BACKOFF)],
)
async def test_a_limit_an_element_declares_holds_its_method_or_the_host_as_a_direct_answer_does(
    route: RouteKind, code: str, method_held: float, host_held: float
) -> None:
    # The command's own error contract finds the limit code in its element of the envelope's answer, a REST 3.0
    # array's element as a classic entry: a time limit holds its method, a quota refusal the host, for the backoff.
    time = _Time()
    executor = _executor(_FirstElementFails(code), time)
    spec = ResultErrorSpec(ResultSelector(("error",)), ("code",))
    chunk = tuple(
        _Command(
            index,
            f"c{index:012d}",
            Request("m.x", {"i": index}, ReplaySafety.UNSAFE, route=route, result_error=spec),
            None,
        )
        for index in range(2)
    )
    policy = ExecutionPolicy(max_attempts_per_request=1, retry=RetryPolicy(initial_delay=_BACKOFF, jitter=0))

    await BatchExecutor(executor).execute_chunk(chunk, context=executor.context(policy), halt=False)
    held = executor.coordinator.admission_delays((frozenset({"m.x"}), frozenset({"other.y"})))
    await executor.coordinator.close()

    assert held == (method_held, host_held)


_HOLDS = (10.0, 20.0, 30.0, 40.0, 50.0)
_SHORT_WINDOW = 60.0


def _held_then_answered(
    time: _Time, sends: list[tuple[float, list[str]]], holds: dict[str, float], executor: list[Executor]
) -> Callable[[Request], object]:
    """Refuse every entry of the first envelope by quota and hold each method ``holds`` says; answer the rest."""

    def respond(request: Request) -> object:
        methods = _sent_methods(request)
        sends.append((time.now, methods))
        keys = _keys(request)
        if len(sends) > 1:
            return {"result": {"result": dict.fromkeys(keys, 1), "result_error": []}}
        for method, held in holds.items():
            executor[0].coordinator.hold_method(method, held)
        errors = {key: {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "x"} for key in keys}
        return {"result": {"result": [], "result_error": errors}}

    return respond


async def _replayed(
    holds: dict[str, float], safeties: tuple[ReplaySafety, ...], policy: ExecutionPolicy
) -> tuple[list[tuple[float, list[str]]], tuple[object, ...]]:
    time = _Time()
    sends: list[tuple[float, list[str]]] = []
    built: list[Executor] = []
    executor = _executor(ResponderTransport(_held_then_answered(time, sends, holds, built)), time)
    built.append(executor)
    chunk = tuple(
        _Command(index, f"c{index:012d}", Request.bare(method, {"i": index}, safety), None)
        for index, (method, safety) in enumerate(zip(holds, safeties, strict=True))
    )
    outcomes = await BatchExecutor(executor).execute_chunk(chunk, context=executor.context(policy), halt=False)
    await executor.coordinator.close()
    return sends, tuple(outcomes)


@pytest.mark.asyncio
async def test_a_replay_round_waits_for_every_safe_command_it_can_so_carrying_spends_no_attempt() -> None:
    # The rounds of a batch share its attempts: a round carrying the more held commands would spend one attempt per
    # hold. Five commands held 10 to 50 s go together once the longest hold ends, within two attempts.
    holds = {f"m{index}.get": held for index, held in enumerate(_HOLDS)}
    policy = ExecutionPolicy(max_attempts_per_request=2, retry=RetryPolicy(initial_delay=0.01, jitter=0))

    sends, outcomes = await _replayed(holds, tuple(ReplaySafety.SAFE for _ in holds), policy)

    assert [methods for _, methods in sends] == [list(holds), list(holds)]
    assert sends[1][0] == pytest.approx(max(_HOLDS), abs=0.1)
    assert all(isinstance(outcome, BatchSuccess) for outcome in outcomes)


@pytest.mark.asyncio
async def test_a_replay_round_sends_an_unsafe_command_with_a_safe_one_held_longer_it_can_wait_for() -> None:
    holds = {"m.u": 1.0, "m.s": 3.0}
    policy = ExecutionPolicy(retry=RetryPolicy(initial_delay=0.01, jitter=0))

    sends, outcomes = await _replayed(holds, (ReplaySafety.UNSAFE, ReplaySafety.SAFE), policy)

    assert [methods for _, methods in sends] == [["m.u", "m.s"], ["m.u", "m.s"]]
    assert all(isinstance(outcome, BatchSuccess) for outcome in outcomes)


_U, _S = ReplaySafety.UNSAFE, ReplaySafety.SAFE
_SEND, _CARRY, _HELD_OUT = _SendPlan.SEND, _SendPlan.CARRY, _SendPlan.HELD_OUT


_WINDOW = 120.0


@pytest.mark.parametrize(
    ("sends", "window", "round_kind", "held", "plans"),
    [
        # A replay round waits for every send that can wait as long: carrying would spend an attempt per hold.
        (tuple((each, _S) for each in _HOLDS), _WINDOW, "replay", max(_HOLDS), (_SEND,) * len(_HOLDS)),
        (((1.0, _U), (3.0, _S)), _WINDOW, "replay", 3.0, (_SEND, _SEND)),
        # The UNSAFE send cannot wait 40 s in a 60 s window: it goes when it needs, the SAFE one in a later round.
        (((1.0, _U), (40.0, _S)), _SHORT_WINDOW, "replay", 1.0, (_SEND, _CARRY)),
        # It cannot wait 100 s in a 120 s window either, but it can wait 3 s: the SAFE send held 3 s goes with it.
        (((1.0, _U), (3.0, _S), (100.0, _S)), _WINDOW, "replay", 3.0, (_SEND, _SEND, _CARRY)),
        # An UNSAFE send its window refuses is not sent: its hold does not limit the round's wait for the others.
        (((70.0, _U), (80.0, _S), (0.0, _S)), _WINDOW, "replay", 80.0, (_HELD_OUT, _SEND, _SEND)),
        # A first send's later envelopes have attempts of their own: the least held go now.
        (
            tuple((each, _S) for each in _HOLDS),
            _WINDOW,
            "first send",
            min(_HOLDS),
            (_SEND, *(_CARRY,) * (len(_HOLDS) - 1)),
        ),
    ],
)
def test_a_round_plan_waits_for_its_sends_together_only_where_they_share_attempts(
    sends: tuple[tuple[float, ReplaySafety], ...],
    window: float,
    round_kind: str,
    held: float,
    plans: tuple[_SendPlan, ...],
) -> None:
    own = tuple(_Pause(0.0, hold, window, 0.0) for hold, _safety in sends)
    safeties = tuple(safety for _hold, safety in sends)

    plan = _plan_round(own, safeties, whole=False, together=round_kind == "replay")

    assert plan.pause.held == held
    assert plan.plans == plans


_FIRST_BACKOFF = 0.05
_SECOND_BACKOFF = 0.1


class _InTurn(ScriptedTransport):
    """Answer each send, a lone call or an envelope of either dialect, with the next of ``answers``."""

    def __init__(self, *answers: WireResponse) -> None:
        super().__init__(())
        self.answers = list(answers)

    async def send_wire(self, wire: WireRequest, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del wire, attempt_timeout, max_response_bytes
        return self.answers.pop(0)


_ENVELOPE_REFUSALS = {
    RouteKind.BARE: _refusal("INVALID_REQUEST", status=400),
    RouteKind.API_V3: WireResponse(
        403,
        _JSON,
        json.dumps({"error": {"code": "BITRIX_REST_V3_EXCEPTION_ACCESSDENIEDEXCEPTION", "message": "no"}}).encode(),
    ),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("route", list(_ENVELOPE_REFUSALS))
@pytest.mark.parametrize(
    ("code", "scope", "held"),
    [
        # The envelope's own failure is an answer from the host: it ends the host's streak, so the next quota
        # refusal holds for the first backoff again.
        ("QUERY_LIMIT_EXCEEDED", frozenset[str](), _FIRST_BACKOFF),
        # It answers about no command's method: the method's time-limit streak goes on to the second backoff.
        ("OPERATION_TIME_LIMIT", frozenset({"crm.item.get"}), _SECOND_BACKOFF),
    ],
)
async def test_an_envelope_refused_whole_for_no_load_limit_ends_the_hosts_streak_and_no_methods(
    code: str, scope: frozenset[str], held: float, route: RouteKind
) -> None:
    time = _Time()
    transport = _InTurn(_refusal(code), _ENVELOPE_REFUSALS[route], _refusal(code))
    executor = _executor(transport, time)
    policy = ExecutionPolicy(
        max_attempts_per_request=1,
        retry=RetryPolicy(initial_delay=_FIRST_BACKOFF, maximum_delay=5.0, jitter=0),
    )
    time.now += 1.0
    with pytest.raises(B24ApiError):
        await executor.execute(_get(), policy=policy)
    time.now += 1.0
    command = Request("crm.item.get", {"id": 1}, ReplaySafety.SAFE, route=route)
    (outcome,) = await BatchExecutor(executor).execute_chunk(
        (_Command(0, "c000000000000", command, None),), context=executor.context(policy), halt=False
    )
    assert isinstance(outcome, BatchFailure)
    time.now += 1.0
    with pytest.raises(B24ApiError):
        await executor.execute(_get(), policy=policy)
    delay = executor.coordinator.admission_delay(scope)
    await executor.coordinator.close()

    assert delay == pytest.approx(held)


@pytest.mark.asyncio
@pytest.mark.parametrize("route", [RouteKind.BARE, RouteKind.API_V3])
async def test_an_envelope_refused_whole_with_a_load_status_keeps_the_hosts_streak(route: RouteKind) -> None:
    # A 503 that names no limit is still an answer about the load, for an envelope as for a lone request: it ends
    # no streak, so the quota refusal after it holds the host for the second backoff.
    time = _Time()
    transport = _InTurn(
        _refusal("QUERY_LIMIT_EXCEEDED"), _refusal("BUSY", status=503), _refusal("QUERY_LIMIT_EXCEEDED")
    )
    executor = _executor(transport, time)
    policy = ExecutionPolicy(
        max_attempts_per_request=1,
        retry=RetryPolicy(initial_delay=_FIRST_BACKOFF, maximum_delay=5.0, jitter=0),
    )
    time.now += 1.0
    with pytest.raises(B24ApiError):
        await executor.execute(_get(), policy=policy)
    time.now += 1.0
    command = Request("crm.item.get", {"id": 1}, ReplaySafety.SAFE, route=route)
    (outcome,) = await BatchExecutor(executor).execute_chunk(
        (_Command(0, "c000000000000", command, None),), context=executor.context(policy), halt=False
    )
    assert isinstance(outcome, BatchFailure)
    time.now += 1.0
    with pytest.raises(B24ApiError):
        await executor.execute(_get(), policy=policy)
    delay = executor.coordinator.admission_delay(frozenset())
    await executor.coordinator.close()

    assert delay == pytest.approx(_SECOND_BACKOFF)


class _GatewayWhileHeld(ScriptedTransport):
    """Answer every send with a gateway's 502, no answer about the load, as a neighbour holds its method 200 s."""

    def __init__(self, coordinator: RateCoordinator) -> None:
        super().__init__(())
        self.coordinator = coordinator

    async def send_wire(self, wire: WireRequest, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del wire, attempt_timeout, max_response_bytes
        self.coordinator.hold_method("crm.item.get", 200.0)
        return WireResponse(502, (("content-type", "text/html"),), b"<html>bad gateway</html>")


@pytest.mark.asyncio
@pytest.mark.parametrize("sent_as", ["call", "batch"])
async def test_a_send_held_beyond_its_window_after_a_failure_about_no_load_ends_with_the_budgets_refusal(
    sent_as: str,
) -> None:
    # Only the server's answer about its load ends a send held out with itself: after a 502 the budget refuses it,
    # caused by the 502, for a lone request as for a classic batch command.
    coordinator = RateCoordinator()
    executor = Executor(_GatewayWhileHeld(coordinator), coordinator=coordinator, random_source=lambda: 0.5)
    policy = ExecutionPolicy(retry=RetryPolicy(initial_delay=0.01, jitter=0))
    if sent_as == "call":
        with pytest.raises(B24ApiError) as raised:
            await executor.execute(_get(), policy=policy)
        refusal: BaseException = raised.value
    else:
        chunk = tuple(_Command(index, f"c{index:012d}", _get(), None) for index in range(2))
        first, _second = await BatchExecutor(executor).execute_chunk(
            chunk, context=executor.context(policy), halt=False
        )
        assert isinstance(first, BatchFailure)
        # The 502 left the command's fate unknown; what ended it is the budget's refusal.
        assert first.error.__cause__ is not None
        refusal = first.error.__cause__
    await coordinator.close()

    assert isinstance(refusal, BudgetExceededError)
    assert isinstance(refusal.__cause__, HTTPGatewayError)


@pytest.mark.asyncio
async def test_a_command_held_out_after_its_load_answer_keeps_what_its_earlier_send_left_open() -> None:
    # The command's first envelope got a gateway's 502, so it may have run; the replay's 503 holds the host beyond
    # the window. The command is held out, yet still possibly run: it ends with the failure that left it open.
    answers = [
        WireResponse(502, (("content-type", "text/html"),), b"<html>bad gateway</html>"),
        _refusal("QUERY_LIMIT_EXCEEDED", headers=(("retry-after", "200"),)),
    ]
    executor = _executor(ResponderTransport(replies(*answers)), _Time())
    chunk = (_Command(0, "c000000000000", _get(), None),)

    (outcome,) = await BatchExecutor(executor).execute_chunk(chunk, context=executor.context(_POLICY), halt=False)
    await executor.coordinator.close()

    assert isinstance(outcome, BatchFailure)
    assert execution(outcome.error) is Execution.POSSIBLY_RUN
    assert isinstance(outcome.error.__cause__, HTTPGatewayError)
