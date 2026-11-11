"""One contract of execution facts: whether a sent command may have run decides its public variant.

The exception type names why a command failed; it never stands in for proof that the command did not run.
Classic and REST v3 batch, direct and batch fan-out, and direct and batch references report the same
evidence the same way.
"""

from __future__ import annotations
import asyncio
import contextlib
import json
import time
from typing import TYPE_CHECKING, cast

import pytest

from b24api import BatchDispatch, BatchFailed, DirectDispatch, ExecutionPolicy, ReplaySafety, Request, RouteKind
from b24api.completion import CompletionGate
from b24api.contracts import (
    Binding,
    Command,
    CommandFailure,
    CommandNotExecuted,
    CommandOutcomeUnknown,
    CommandSettlement,
    CommandSuccess,
    NotExecutedReason,
    PageOutcome,
    PageRejectionCode,
    ParameterPath,
    ParameterUpdate,
    ReferenceFailure,
    ReferenceOutcomeUnknown,
    ReplayDisposition,
    ResultErrorShape,
    ResultErrorSpec,
    ResultSelector,
    RetryPolicy,
    SequentialTraversal,
)
from b24api.contracts.completion import PageCommandOutcome
from b24api.contracts.policy import AmbiguityReason
from b24api.errors import (
    AmbiguousExecutionError,
    ApiResponseError,
    B24ApiError,
    BatchCommandError,
    EnvelopeContractError,
    FailurePhase,
    HTTPGatewayError,
    ProtocolError,
    TransportError,
)
from b24api.testing import ScriptedExchange, ScriptedTransport
from b24api.transport import WireRequest, WireResponse
from tests.scripting import ResponderTransport, client_for, on_loop

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator

    from b24api import Bitrix24
    from b24api.contracts import CommandOutcome, ReferenceOutcome
    from b24api.contracts.report import OperationReport
    from b24api.contracts.stream import OperationStream

_JSON = (("content-type", "application/json"),)
_NO_DELAY = RetryPolicy(initial_delay=0, maximum_delay=0, jitter=0)
_UNSAFE = Request.bare("entity.add", replay_safety=ReplaySafety.UNSAFE)
_HTTP_OK = 200
_HTTP_SERVER_ERROR = 500
_HTTP_BAD_GATEWAY = 502
_LATE_WINDOW = 0.05
_LATE_ANSWER = 0.08
_ANSWER = 42
_HTTP_FORBIDDEN = 403
_RETRIED = 2
_PATHS = 2
_PATHS_WITH_V3 = 3
_SHAPE = ResultErrorSpec(ResultSelector(("error",)), ("code",), shape=ResultErrorShape.MAPPING)
_MALFORMED_ERROR = {"error": ["not", "a", "map"]}
_DENIED = WireResponse(_HTTP_FORBIDDEN, _JSON, b'{"error":"ACCESS_DENIED","error_description":"denied"}')
_V3_DENIED = WireResponse(_HTTP_FORBIDDEN, _JSON, b'{"error":{"code":"ACCESS_DENIED","message":"denied"}}')
_HTTP_BAD_REQUEST = 400
# A budget no admission wait reaches: only the test moves the clock past it.
_UNREACHABLE_SECONDS = 3600.0
_MALFORMED_ERROR_BODIES = [b'{"error":null,"error_description":"x"}', b'{"error":{"code":5}}']
_UNDECODABLE = {"empty object": b"{}", "truncated JSON": b'{"result":{"result":{"c0'}
_REFERENCE = Request.bare("item.list", replay_safety=ReplaySafety.SAFE)
_BINDINGS = (Binding("b1", (ParameterUpdate(ParameterPath(("filter", "ID")), 1),), "x"),)
_COMMAND_PATHS = ["classic batch", "direct fan-out"]
_FAN_OUT_PATHS = ["direct fan-out", "batch fan-out"]
_REFERENCE_PATHS = ["reference direct", "reference batch"]


def _keys(request: Request) -> list[str]:
    commands = request.copy_parameters()["cmd"]
    assert isinstance(commands, dict)
    return list(commands)


async def _drain[T](stream: AsyncIterator[T]) -> list[T]:
    return [outcome async for outcome in stream]


def _ambiguity(outcome: object) -> AmbiguousExecutionError:
    """Return the ambiguity of an outcome reported as possibly executed, which is never replayed."""
    assert isinstance(outcome, CommandOutcomeUnknown | ReferenceOutcomeUnknown)
    assert isinstance(outcome.error, AmbiguousExecutionError)
    assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE
    return outcome.error


@pytest.mark.asyncio
async def test_a_key_both_a_result_and_a_refusal_is_unknown_and_never_replayed() -> None:
    # The answer contradicts itself, so the quota refusal in it proves nothing: the creation may have run.
    def contradictory(request: Request) -> object:
        (key,) = _keys(request)
        return {"result": {"result": {key: 1}, "result_error": {key: {"error": "QUERY_LIMIT_EXCEEDED"}}}}

    transport = ResponderTransport(contradictory)
    async with client_for(transport, policy=ExecutionPolicy(retry=_NO_DELAY)) as client:
        stream = client.batch_outcomes([Command(_UNSAFE, "x")])
        (outcome,) = await _drain(stream)

    assert len(transport.requests) == 1
    assert isinstance(_ambiguity(outcome).__cause__, ProtocolError)
    assert stream.report is not None
    assert stream.report.unknown == 1


@pytest.mark.asyncio
async def test_a_batch_command_without_an_entry_is_unknown_unless_the_server_halted() -> None:
    def partial(request: Request) -> object:
        first, _second = _keys(request)
        return {"result": {"result": {first: 1}, "result_error": []}}

    async with client_for(ResponderTransport(partial)) as client:
        first, second = await _drain(client.batch_outcomes([Command(_UNSAFE, "a"), Command(_UNSAFE, "b")]))

    assert isinstance(first, CommandSuccess)
    assert isinstance(_ambiguity(second).__cause__, ProtocolError)


def _outcomes(
    client: Bitrix24, mode: str, request: Request = _UNSAFE
) -> OperationStream[CommandOutcome[str]] | OperationStream[ReferenceOutcome[str]]:
    """Send one command on ``mode``'s path; a reference traverses the list request for one binding."""
    if mode in _REFERENCE_PATHS:
        dispatch = DirectDispatch() if mode == "reference direct" else BatchDispatch()
        bindings = on_loop(_BINDINGS)
        return client.iter_reference_outcomes(_REFERENCE, bindings, traversal=SequentialTraversal(), dispatch=dispatch)
    commands = on_loop([Command(request, "x")])
    if mode == "classic batch":
        return client.batch_outcomes(commands)
    return client.fan_out_outcomes(commands, dispatch=BatchDispatch() if mode == "batch fan-out" else DirectDispatch())


class _MovableClockLoop(asyncio.SelectorEventLoop):
    """An event loop whose clock the test moves forward, so a deadline passes exactly when the test says."""

    def __init__(self) -> None:
        super().__init__()
        self.moved = 0.0

    def time(self) -> float:
        return super().time() + self.moved


@pytest.mark.parametrize("mode", ["classic batch", *_FAN_OUT_PATHS, *_REFERENCE_PATHS])
def test_a_send_cut_by_the_operation_deadline_is_unknown_with_its_cut_send_as_cause(mode: str) -> None:
    # The deadline bounds admission, not the send: the executor ends the send and records that it may have run.
    # The operation deadline passes only once the send is in flight: the transport moves the loop's clock past it,
    # so no admission wait, however slow, can reach it first.
    policy = ExecutionPolicy(
        max_elapsed=_UNREACHABLE_SECONDS,
        max_retry_elapsed_per_request=2 * _UNREACHABLE_SECONDS,
        max_attempts_per_request=1,
    )

    async def passing_the_deadline(_request: Request) -> object:
        loop = asyncio.get_running_loop()
        assert isinstance(loop, _MovableClockLoop)
        loop.moved += policy.max_elapsed
        return await asyncio.Future[object]()

    async def scenario() -> tuple[object, OperationReport | None, int]:
        transport = ResponderTransport(passing_the_deadline)
        async with client_for(transport, policy=policy) as client:
            stream = _outcomes(client, mode)
            outcomes = await _drain(stream)
        (outcome,) = outcomes
        return outcome, stream.report, len(transport.requests)

    with asyncio.Runner(loop_factory=_MovableClockLoop) as runner:
        outcome, report, sends = runner.run(scenario())

    assert sends == 1
    ambiguity = _ambiguity(outcome)
    assert ambiguity.reason is AmbiguityReason.DEADLINE_AFTER_DISPATCH
    causes = list(_causes(ambiguity))
    assert any(isinstance(cause, TransportError) for cause in causes)
    assert any(isinstance(cause, TimeoutError) for cause in causes)
    assert not any("admission" in str(cause) for cause in causes)
    assert report is not None
    assert report.unknown == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("body", _MALFORMED_ERROR_BODIES, ids=["null code", "malformed v3 error object"])
@pytest.mark.parametrize("mode", [*_COMMAND_PATHS, "batch fan-out", *_REFERENCE_PATHS])
async def test_a_malformed_error_body_on_a_refusing_status_is_a_conclusive_failure(mode: str, body: bytes) -> None:
    # The status answers the send whatever its body; a body that cannot be decoded changes nothing about it.
    transport = ResponderTransport(lambda _request: WireResponse(_HTTP_BAD_REQUEST, _JSON, body))
    async with client_for(transport, policy=ExecutionPolicy(retry=_NO_DELAY)) as client:
        stream = _outcomes(client, mode)
        (outcome,) = await _drain(stream)
        with pytest.raises(ProtocolError):
            await client.call(_UNSAFE)

    assert isinstance(outcome, CommandFailure | ReferenceFailure)
    assert any(isinstance(cause, ProtocolError) for cause in _causes(outcome.error))
    assert stream.report is not None
    assert stream.report.unknown == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("body", list(_UNDECODABLE.values()), ids=list(_UNDECODABLE))
@pytest.mark.parametrize("mode", _COMMAND_PATHS + _REFERENCE_PATHS)
async def test_an_undecodable_success_after_dispatch_is_unknown_with_its_protocol_cause(mode: str, body: bytes) -> None:
    transport = ResponderTransport(lambda _request: WireResponse(_HTTP_OK, _JSON, body))
    async with client_for(transport, policy=ExecutionPolicy(retry=_NO_DELAY)) as client:
        stream = _outcomes(client, mode)
        (outcome,) = await _drain(stream)
        # A direct call keeps raising the failure itself.
        with pytest.raises(ProtocolError) as raised:
            await client.call(_UNSAFE)

    ambiguity = _ambiguity(outcome)
    assert isinstance(ambiguity.__cause__, ProtocolError)
    assert ambiguity.reason is AmbiguityReason.HTTP_STATUS_AFTER_DISPATCH
    assert stream.report is not None
    assert stream.report.unknown == 1
    assert not isinstance(raised.value, AmbiguousExecutionError)

    # Bitrix's own structured error, by contrast, answers the same command conclusively.
    structured = json.dumps({"error": "INTERNAL_SERVER_ERROR", "error_description": "x"}).encode()
    transport = ResponderTransport(lambda _request: WireResponse(_HTTP_SERVER_ERROR, _JSON, structured))
    async with client_for(transport, policy=ExecutionPolicy(retry=_NO_DELAY)) as client:
        (answered,) = await _drain(_outcomes(client, mode))
    assert isinstance(answered, CommandFailure | ReferenceFailure)
    assert any(isinstance(cause, ApiResponseError) for cause in _causes(answered.error))


def _lost(request: Request) -> TransportError:
    return TransportError("connection lost", phase=FailurePhase.DISPATCH_STARTED, request_summary=request.summary)


def _causes(error: BaseException) -> Iterator[BaseException]:
    current: BaseException | None = error
    while current is not None:
        yield current
        current = current.__cause__


@pytest.mark.asyncio
@pytest.mark.parametrize("answer", ["malformed metadata", "malformed embedded error"])
async def test_one_undecodable_command_answer_is_unknown_on_every_path(answer: str) -> None:
    # The command was sent and answered, but an answer the client cannot decode proves nothing about what it did.
    embedded = answer == "malformed embedded error"
    request = Request.bare("entity.add", replay_safety=ReplaySafety.UNSAFE, result_error=_SHAPE if embedded else None)
    result: object = _MALFORMED_ERROR if embedded else _ANSWER
    total = None if embedded else "invalid"

    def classic(batch: Request) -> object:
        (key,) = _keys(batch)
        entry: dict[str, object] = {"result": {key: result}, "result_error": {}}
        if total is not None:
            entry["result_total"] = {key: total}
        return {"result": entry}

    def direct(_request: Request) -> object:
        return {"result": result} if total is None else {"result": result, "total": total}

    outcomes: list[CommandOutcome[str]] = []
    for respond, fan_out in ((classic, False), (direct, True)):
        async with client_for(ResponderTransport(respond)) as client:
            commands = [Command(request, "x")]
            stream = (
                client.fan_out_outcomes(commands, dispatch=DirectDispatch())
                if fan_out
                else client.batch_outcomes(commands)
            )
            outcomes.extend(await _drain(stream))
    if embedded:  # REST v3 has no paging metadata, only the request's own embedded-error contract
        native = Request("entity.add", {"id": 1}, ReplaySafety.UNSAFE, route=RouteKind.API_V3, result_error=_SHAPE)
        transport = ScriptedTransport((ScriptedExchange.v3_batch((native,), (_MALFORMED_ERROR,)),))
        async with client_for(transport) as client:
            outcomes.extend(await _drain(client.batch_outcomes([Command(native, "x")])))
        transport.assert_exhausted()

    assert len(outcomes) == (_PATHS_WITH_V3 if embedded else _PATHS)
    for outcome in outcomes:
        ambiguity = _ambiguity(outcome)
        assert ambiguity.reason is AmbiguityReason.HTTP_STATUS_AFTER_DISPATCH
        assert isinstance(ambiguity.__cause__, ProtocolError)


class _LostEnvelopes(ScriptedTransport):
    """Lose the first ``lost`` REST v3 batch envelopes after dispatch; answer every later send from the script."""

    def __init__(self, exchanges: tuple[ScriptedExchange, ...], *, lost: int) -> None:
        super().__init__(exchanges)
        self._lost = lost

    async def send_wire(self, request: WireRequest, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        if request.array_body is not None and self._lost:
            self._lost -= 1
            raise TransportError("connection lost", phase=FailurePhase.DISPATCH_STARTED)
        return await super().send_wire(request, attempt_timeout=attempt_timeout, max_response_bytes=max_response_bytes)


def _refused_command(batch: Request) -> object:
    """Answer the batch envelope, refusing each of its commands with Bitrix's own error."""
    return {"result": {"result": [], "result_error": {key: {"error": "ACCESS_DENIED"} for key in _keys(batch)}}}


async def _lost_then_refused_v3(mode: str) -> CommandOutcome[str]:
    policy = ExecutionPolicy(retry=_NO_DELAY)
    if mode == "REST v3 isolated send":
        # Both envelope sends are lost; the command's own isolated send is then refused.
        native = Request("entity.get", {"id": 1}, ReplaySafety.SAFE, route=RouteKind.API_V3)
        transport = _LostEnvelopes((ScriptedExchange(native, _V3_DENIED),), lost=_RETRIED)
    else:
        # The retried envelope is answered, and its element refuses the command by the request's contract.
        native = Request("entity.get", {"id": 1}, ReplaySafety.SAFE, route=RouteKind.API_V3, result_error=_SHAPE)
        refusal = {"error": {"code": "ACCESS_DENIED", "message": "denied"}}
        transport = _LostEnvelopes((ScriptedExchange.v3_batch((native,), (refusal,)),), lost=1)
    async with client_for(transport, policy=policy) as client:
        (outcome,) = await _drain(client.batch_outcomes([Command(native, "x")]))
    transport.assert_exhausted()
    return outcome


async def _lost_then_refused(mode: str) -> object:
    """Send one SAFE command whose first send loses its answer and whose retry is refused conclusively."""
    if mode.startswith("REST v3"):
        return await _lost_then_refused_v3(mode)
    sends = 0

    def respond(request: Request) -> object:
        nonlocal sends
        sends += 1
        if sends == 1:
            return _lost(request)
        return _refused_command(request) if mode == "classic batch command refusal" else _DENIED

    safe = Request.bare("entity.get", replay_safety=ReplaySafety.SAFE)
    path = "classic batch" if mode.startswith("classic batch") else mode
    async with client_for(ResponderTransport(respond), policy=ExecutionPolicy(retry=_NO_DELAY)) as client:
        (outcome,) = await _drain(_outcomes(client, path, safe))
    assert sends == _RETRIED
    return outcome


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode",
    [
        "classic batch envelope refusal",
        "classic batch command refusal",
        "direct fan-out",
        "REST v3 isolated send",
        "REST v3 element refusal",
        *_REFERENCE_PATHS,
    ],
)
async def test_a_lost_send_explains_the_unknown_outcome_whatever_answered_its_retry(mode: str) -> None:
    # The first send may have run; the conclusive refusal of a later one answers only that send.
    ambiguity = _ambiguity(await _lost_then_refused(mode))

    assert ambiguity.reason is AmbiguityReason.CONNECTION_LOST_AFTER_DISPATCH
    assert any(isinstance(cause, TransportError) for cause in _causes(ambiguity))


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", ["envelope", "command"])
async def test_a_command_replayed_after_a_lost_round_stays_explained_by_it(refusal: str) -> None:
    # The lost round may have run both commands. Only the SAFE one is replayed, and whatever refuses the replay
    # answers that send alone: both stay unknown, explained by the lost round.
    rounds: list[list[str]] = []

    def respond(batch: Request) -> object:
        rounds.append(_keys(batch))
        if len(rounds) == 1:
            return _lost(batch)
        return _DENIED if refusal == "envelope" else _refused_command(batch)

    safe = Request.bare("entity.get", replay_safety=ReplaySafety.SAFE)
    async with client_for(ResponderTransport(respond), policy=ExecutionPolicy(retry=_NO_DELAY)) as client:
        stream = client.batch_outcomes([Command(_UNSAFE, "unsafe"), Command(safe, "safe")])
        outcomes = await _drain(stream)

    assert [len(keys) for keys in rounds] == [2, 1]
    assert len(outcomes) == len(rounds[0])
    for outcome in outcomes:
        ambiguity = _ambiguity(outcome)
        assert ambiguity.reason is AmbiguityReason.CONNECTION_LOST_AFTER_DISPATCH
        assert any(isinstance(cause, TransportError) for cause in _causes(ambiguity))
    assert stream.report is not None
    assert stream.report.unknown == len(outcomes)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["classic batch", "batch fan-out"])
async def test_a_replay_round_after_an_answered_retry_stays_explained_by_the_lost_send(mode: str) -> None:
    # The lost envelope may have run both commands; its retry answers them, and a quota refusal makes one replay
    # in a round of its own. Whatever refuses that replay, the lost send leaves it unknown.
    rounds: list[list[str]] = []

    def respond(batch: Request) -> object:
        keys = _keys(batch)
        rounds.append(keys)
        if len(rounds) == 1:
            return _lost(batch)
        if len(rounds) == _RETRIED:
            refused, answered = keys
            return {"result": {"result": {answered: 1}, "result_error": {refused: {"error": "QUERY_LIMIT_EXCEEDED"}}}}
        return _refused_command(batch)

    safe = [Request.bare("entity.get", {"id": index}, replay_safety=ReplaySafety.SAFE) for index in (1, 2)]
    commands = [Command(safe[0], "refused"), Command(safe[1], "answered")]
    async with client_for(ResponderTransport(respond), policy=ExecutionPolicy(retry=_NO_DELAY)) as client:
        stream = (
            client.batch_outcomes(commands)
            if mode == "classic batch"
            else client.fan_out_outcomes(commands, dispatch=BatchDispatch())
        )
        refused, answered = await _drain(stream)

    assert [len(keys) for keys in rounds] == [2, 2, 1]
    assert isinstance(answered, CommandSuccess)
    ambiguity = _ambiguity(refused)
    assert ambiguity.reason is AmbiguityReason.CONNECTION_LOST_AFTER_DISPATCH
    assert any(isinstance(cause, TransportError) for cause in _causes(ambiguity))


_FIRST_PAGE = {"result": [{"ID": index} for index in range(1, 51)], "total": 120, "next": 50}


_LIST_PATHS = ["sequential", "counted", "counted batch tail"]


def _paged(client: Bitrix24, path: str) -> OperationStream[object]:
    """Fetch pages on ``path``: a fan-out sends one command, a reference or a list traverses its pages."""
    if path in _FAN_OUT_PATHS or path in _REFERENCE_PATHS:
        return cast("OperationStream[object]", _outcomes(client, path))
    request = Request.bare("item.list")
    stream = client.iter_list(request) if path == "sequential" else client.iter_list_counted(request)
    return cast("OperationStream[object]", stream)


@pytest.mark.asyncio
@pytest.mark.parametrize("path", [*_FAN_OUT_PATHS, *_REFERENCE_PATHS, *_LIST_PATHS])
async def test_a_failed_page_is_traced_and_settled_by_whether_it_may_have_run(
    path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A refusal answers the page conclusively; a lost answer leaves open whether it ran. Its trace says the same.
    settled: list[CommandSettlement] = []
    emit = CompletionGate.emit

    def spy(gate: CompletionGate, event: object) -> None:
        if isinstance(event, PageCommandOutcome):
            settled.append(event.outcome)
        emit(gate, event)  # type: ignore[arg-type]

    monkeypatch.setattr(CompletionGate, "emit", spy)
    policy = ExecutionPolicy(retry=_NO_DELAY, max_attempts_per_request=1)
    failed: list[tuple[CommandSettlement, PageOutcome, PageRejectionCode | None]] = []
    for fail in (lambda _request: _DENIED, _lost):
        settled.clear()

        def respond(page: Request, fail: Callable[[Request], object] = fail) -> object:
            # A counted traversal fetches its tail in a batch once the first page reports the total.
            tail = path == "counted batch tail"
            return _FIRST_PAGE if tail and page.method != "batch" else fail(page)

        async with client_for(ResponderTransport(respond), policy=policy) as client:
            stream = _paged(client, path)
            with contextlib.suppress(B24ApiError):
                async with stream:
                    _ = [item async for item in stream]
        assert stream.report is not None
        (page,) = (record for record in stream.report.page_trace if record.outcome is not PageOutcome.COMMITTED)
        failure = next(state for state in settled if state is not CommandSettlement.SUCCESS)
        failed.append((failure, page.outcome, page.rejection_code))

    assert failed == [
        (CommandSettlement.FAILURE, PageOutcome.REJECTED, PageRejectionCode.COMMAND_FAILURE),
        (CommandSettlement.UNKNOWN, PageOutcome.UNKNOWN, PageRejectionCode.AMBIGUOUS_EXECUTION),
    ]


@pytest.mark.asyncio
async def test_halted_comes_only_from_the_servers_own_halt() -> None:
    # The first command's error is found in its result by its own contract, not by the server, so the server did
    # not halt and the second command, whose result came back, is no HALTED command: its undecodable metadata
    # leaves it unknown.
    def contract_error(request: Request) -> object:
        first, second = _keys(request)
        return {
            "result": {
                "result": {first: {"error": {"code": "ACCESS_DENIED"}}, second: 17},
                "result_error": {},
                "result_total": {second: "invalid"},
            }
        }

    def server_halt(request: Request) -> object:
        first, _second = _keys(request)
        return {"result": {"result": [], "result_error": {first: {"error": "ACCESS_DENIED"}}}}

    spec = ResultErrorSpec(ResultSelector(("error",)), ("code",), shape=ResultErrorShape.MAPPING)
    commands = [
        Command(Request.bare("w.a", replay_safety=ReplaySafety.UNSAFE, result_error=spec), "a"),
        Command(Request.bare("w.b", replay_safety=ReplaySafety.UNSAFE), "b"),
    ]
    windows = []
    for respond in (contract_error, server_halt):
        async with client_for(ResponderTransport(respond)) as client:
            with pytest.raises(BatchFailed) as captured:
                _ = [outcome async for outcome in client.batch(commands)]
        windows.append(captured.value.outcomes)

    (first, second), (refused, halted) = windows
    assert isinstance(first, CommandFailure)
    assert isinstance(first.error, BatchCommandError)
    assert isinstance(_ambiguity(second).__cause__, ProtocolError)
    assert isinstance(refused, CommandFailure)
    assert isinstance(halted, CommandNotExecuted)
    assert halted.reason is NotExecutedReason.HALTED

    # The server's halt proves only that its own send skipped the command: an earlier lost send may have run it.
    sends = 0

    def lost_then_halt(request: Request) -> object:
        nonlocal sends
        sends += 1
        return _lost(request) if sends == 1 else server_halt(request)

    safe = [Command(Request.bare(method, replay_safety=ReplaySafety.SAFE), method) for method in ("r.a", "r.b")]
    async with client_for(ResponderTransport(lost_then_halt), policy=ExecutionPolicy(retry=_NO_DELAY)) as client:
        with pytest.raises(BatchFailed) as captured:
            _ = [outcome async for outcome in client.batch(safe)]
    assert sends == _RETRIED
    for outcome in captured.value.outcomes:
        assert _ambiguity(outcome).reason is AmbiguityReason.CONNECTION_LOST_AFTER_DISPATCH


@pytest.mark.asyncio
async def test_batch_fan_out_keeps_the_replay_disposition_of_logical_batch() -> None:
    def refused(request: Request) -> object:
        errors = {key: {"error": "QUERY_LIMIT_EXCEEDED"} for key in _keys(request)}
        return {"result": {"result": [], "result_error": errors}}

    outcomes = []
    async with client_for(ResponderTransport(refused), policy=ExecutionPolicy(max_attempts_per_request=1)) as client:
        for fan_out in (False, True):
            commands = [Command(_UNSAFE, "x")]
            stream = (
                client.fan_out_outcomes(commands, dispatch=BatchDispatch())
                if fan_out
                else client.batch_outcomes(commands)
            )
            outcomes.extend(await _drain(stream))

    # The same evidence gives the same outcome: a quota refusal proves the command did not run, so it may be sent again.
    for outcome in outcomes:
        assert isinstance(outcome, CommandFailure)
        assert isinstance(outcome.error, BatchCommandError)
        assert outcome.replay_disposition is ReplayDisposition.ELIGIBLE


def _late(reply: object) -> Callable[[Request], object]:
    """Answer only after the retry window has passed, without yielding, so no timeout can cut the send."""

    def respond(request: Request) -> object:
        time.sleep(_LATE_ANSWER)
        if request.method == "batch":
            (key,) = _keys(request)
            return {"result": {"result": {key: reply}, "result_error": []}}
        return {"result": reply}

    return respond


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["call", "classic batch", "direct fan-out"])
async def test_a_success_answered_after_the_time_budget_is_returned(mode: str) -> None:
    policy = ExecutionPolicy(retry=_NO_DELAY, max_retry_elapsed_per_request=_LATE_WINDOW)
    transport = ResponderTransport(_late(_ANSWER))
    async with client_for(transport, policy=policy) as client:
        if mode == "call":
            assert await client.call(_UNSAFE) == _ANSWER
            return
        commands = [Command(_UNSAFE, "x")]
        stream = client.batch_outcomes(commands) if mode == "classic batch" else client.fan_out_outcomes(commands)
        (outcome,) = await _drain(stream)

    assert isinstance(outcome, CommandSuccess)
    assert outcome.response.result == _ANSWER
    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_a_failure_answered_after_the_time_budget_meets_the_ambiguity_rules() -> None:
    def late_gateway(_request: Request) -> WireResponse:
        time.sleep(_LATE_ANSWER)
        return WireResponse(_HTTP_BAD_GATEWAY, (), b"bad gateway")

    policy = ExecutionPolicy(retry=_NO_DELAY, max_retry_elapsed_per_request=_LATE_WINDOW)
    async with client_for(ResponderTransport(late_gateway), policy=policy) as client:
        with pytest.raises(AmbiguousExecutionError) as raised:
            await client.call(_UNSAFE)

    assert raised.value.reason is AmbiguityReason.HTTP_STATUS_AFTER_DISPATCH
    assert isinstance(raised.value.__cause__, HTTPGatewayError)
    assert not isinstance(raised.value.__cause__, EnvelopeContractError)
