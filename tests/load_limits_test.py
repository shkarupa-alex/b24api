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
from b24api.errors import ApiResponseError, B24ApiError, BudgetExceededError
from b24api.execution import Executor, RateCoordinator, WireResponse
from b24api.execution.executor import _SendPlan
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
async def test_a_time_limit_with_a_hint_holds_its_method_for_the_hint_not_until_its_reset() -> None:
    time = _Time()
    sent_at: list[float] = []
    answered = {"result": 1, "time": {"operating_reset_at": wall.time() + 100, "operating": 470}}
    transport = ResponderTransport(
        _timed(
            sent_at,
            time,
            replies(
                WireResponse(200, (*_JSON, _now_date()), json.dumps(answered).encode()),
                _refusal("OPERATION_TIME_LIMIT", headers=(("retry-after", "2"),)),
                {"result": 2},
            ),
        )
    )
    executor = _executor(transport, time)

    await executor.execute(_get(), policy=_POLICY)
    await executor.execute(_get(), policy=_POLICY)
    await executor.coordinator.close()

    assert sent_at == [0.0, 0.0, 2.0]


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
