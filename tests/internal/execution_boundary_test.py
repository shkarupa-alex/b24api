"""Executor boundary: replay decision matrix, foreign transport failures and the response ceiling."""

from __future__ import annotations
import asyncio
import json
from collections.abc import Mapping
from typing import TYPE_CHECKING

import httpx
import pytest

from b24api import BatchFailed, Bitrix24, ReplaySafety, Request, Settings, TerminalState
from b24api._error_types import FailurePhase
from b24api.batch.engine import BatchExecutor
from b24api.batch.outcome import BatchFailure
from b24api.contracts import Command, CommandFailure, CommandOutcomeUnknown, CommandSuccess, ReplayDisposition
from b24api.contracts.policy import AmbiguityPolicy, AmbiguityReason, ExecutionPolicy, RetryPolicy
from b24api.contracts.request import RouteKind
from b24api.errors import (
    AmbiguousExecutionError,
    ApiResponseError,
    BatchCommandError,
    BudgetExceededError,
    CapabilityError,
    EnvelopeContractError,
    HTTPGatewayError,
    ResponseTooLargeError,
    TransportError,
)
from b24api.execution import Executor, HttpxTransport, WireResponse
from b24api.execution import executor as executor_module
from b24api.transport.base import TransportCapabilities, WireRequest
from b24api.transport.protocol import ProtocolCodec

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from b24api.contracts.command import CommandOutcome
    from b24api.contracts.stream import OperationStream

HOST = "test.invalid"
HTTP_OK = 200
CEILING = 64
SEVERAL = 3
_SAFETIES = (ReplaySafety.SAFE, ReplaySafety.UNSAFE, ReplaySafety.UNKNOWN)


class _Script:
    """Record every physical send and answer from a per-attempt behavior list."""

    host = HOST

    def __init__(self, behaviors: list[Callable[[Request], WireResponse]]) -> None:
        self.behaviors = behaviors
        self.sent: list[Request] = []
        self.entered = 0

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        behavior = self.behaviors[min(self.entered, len(self.behaviors) - 1)]
        self.entered += 1
        return behavior(request)


def _envelope(request: Request) -> WireResponse:
    if request.method == "batch":
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        payload: object = {"result": {"result": dict.fromkeys(commands, True), "result_error": []}}
    else:
        payload = {"result": True}
    return WireResponse(HTTP_OK, (("content-type", "application/json"),), json.dumps(payload).encode())


def _policy(**overrides: object) -> ExecutionPolicy:
    values: dict[str, object] = {
        "max_attempts_per_request": 3,
        "max_elapsed": 10.0,
        "max_retry_elapsed_per_request": 10.0,
        "retry": RetryPolicy(initial_delay=0, maximum_delay=0, jitter=0),
    }
    values.update(overrides)
    return ExecutionPolicy(**values)  # type: ignore[arg-type]


def _client(transport: object) -> Bitrix24:
    settings = Settings(webhook_url=f"https://{HOST}/rest/1/token/")
    return Bitrix24(settings, transport=transport)  # type: ignore[arg-type]


def _request(safety: ReplaySafety, name: str = "sample.get") -> Request:
    return Request(name, replay_safety=safety, route=RouteKind.BARE)


async def _drain(stream: OperationStream[CommandOutcome[int]]) -> list[CommandOutcome[int]]:
    return [outcome async for outcome in stream]


# --- A3 --------------------------------------------------------------------------------------


class _SilentAuth(httpx.Auth):
    """An injected auth flow that never yields the owned request (a permanent transport refusal)."""

    def __init__(self) -> None:
        self.flows = 0

    async def async_auth_flow(self, request: httpx.Request) -> AsyncGenerator[httpx.Request, httpx.Response]:
        self.flows += 1
        if request.method == "NEVER":
            yield request


@pytest.mark.asyncio
async def test_permanent_transport_refusal_is_raised_once_instead_of_exhausting_the_budget() -> None:
    dispatched: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        dispatched.append(request)
        return httpx.Response(HTTP_OK, json={"result": True}, request=request)

    auth = _SilentAuth()
    client = httpx.AsyncClient(transport=httpx.MockTransport(respond), auth=auth)
    transport = HttpxTransport(f"https://{HOST}/rest/1/token/", client=client)
    try:
        with pytest.raises(TransportError, match="replaced the owned request") as captured:
            await Executor(transport).execute(_request(ReplaySafety.SAFE, "profile"))
    finally:
        await transport.aclose()
        await client.aclose()

    # Before A3 the default policy made five attempts and raised BudgetExceededError.
    assert captured.value.retryable is False
    assert not isinstance(captured.value, BudgetExceededError)
    assert auth.flows == 1
    assert dispatched == []


# --- D05 matrix ------------------------------------------------------------------------------


def _raise(phase: FailurePhase, *, retryable: bool, sink: list[TransportError]) -> Callable[[Request], WireResponse]:
    def behavior(request: Request) -> WireResponse:
        error = TransportError("synthetic failure", phase=phase, retryable=retryable, request_summary=request.summary)
        sink.append(error)
        raise error

    return behavior


def _counting(transport: _Script) -> Callable[[Request], WireResponse]:
    def behavior(request: Request) -> WireResponse:
        transport.sent.append(request)
        return _envelope(request)

    return behavior


def _matrix_transport(phase: FailurePhase, *, retryable: bool) -> tuple[_Script, list[TransportError]]:
    raised: list[TransportError] = []
    transport = _Script([])
    failing = _raise(phase, retryable=retryable, sink=raised)

    def failing_send(request: Request) -> WireResponse:
        transport.sent.append(request)
        return failing(request)

    transport.behaviors = [failing_send, _counting(transport)]
    return transport, raised


@pytest.mark.asyncio
@pytest.mark.parametrize("retryable", [False, True], ids=["permanent", "retryable"])
@pytest.mark.parametrize("safety", _SAFETIES)
@pytest.mark.parametrize("phase", list(FailurePhase))
async def test_direct_replay_matrix(phase: FailurePhase, safety: ReplaySafety, retryable: bool) -> None:  # noqa: FBT001
    transport, raised = _matrix_transport(phase, retryable=retryable)
    possible = phase not in {FailurePhase.NOT_DISPATCHED, FailurePhase.CONNECTION_ESTABLISHED}

    call = _client(transport).call(_request(safety), policy=_policy())
    if possible and safety is not ReplaySafety.SAFE:
        with pytest.raises(AmbiguousExecutionError) as ambiguous:
            await call
        assert ambiguous.value.__cause__ is raised[0]
        assert len(transport.sent) == 1
    elif not retryable:
        with pytest.raises(TransportError) as permanent:
            await call
        assert permanent.value is raised[0]
        assert len(transport.sent) == 1
    else:
        assert await call is True
        assert len(transport.sent) == 2  # noqa: PLR2004 - one retry within the budget


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, SEVERAL], ids=["one", "several"])
@pytest.mark.parametrize("retryable", [False, True], ids=["permanent", "retryable"])
@pytest.mark.parametrize("safety", _SAFETIES)
@pytest.mark.parametrize("phase", list(FailurePhase))
async def test_physical_batch_replay_matrix(
    phase: FailurePhase,
    safety: ReplaySafety,
    retryable: bool,  # noqa: FBT001
    count: int,
) -> None:
    transport, raised = _matrix_transport(phase, retryable=retryable)
    possible = phase not in {FailurePhase.NOT_DISPATCHED, FailurePhase.CONNECTION_ESTABLISHED}
    stream = _client(transport).batch_outcomes(
        [Command(_request(safety), index) for index in range(count)],
        policy=_policy(),
    )

    outcomes = await _drain(stream)

    assert len(outcomes) == count
    assert all(not isinstance(outcome.error, BudgetExceededError) for outcome in outcomes if hasattr(outcome, "error"))
    assert stream.report is not None
    if retryable and (not possible or safety is ReplaySafety.SAFE):
        # Owner decision (2026-09-25): SAFE work is replayed within the budget; other work only when nothing
        # was dispatched.
        assert len(transport.sent) == 2  # noqa: PLR2004 - one replay within the budget
        assert all(isinstance(outcome, CommandSuccess) for outcome in outcomes)
        assert stream.report.state is TerminalState.COMPLETED
    elif possible:
        # A batch that may have been accepted and may not be replayed: every admitted command is unknown.
        assert len(transport.sent) == 1
        assert all(isinstance(outcome, CommandOutcomeUnknown) for outcome in outcomes)
        for outcome in outcomes:
            assert isinstance(outcome, CommandOutcomeUnknown)
            assert isinstance(outcome.error, AmbiguousExecutionError)
            assert outcome.error.__cause__ is raised[0]
            assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE
        assert stream.report.unknown == count
        assert stream.report.state is TerminalState.COMPLETED_WITH_FAILURES
    else:
        assert len(transport.sent) == 1
        for outcome in outcomes:
            assert isinstance(outcome, CommandFailure)
            assert outcome.error is raised[0]
            assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE
        assert stream.report.failures == count
    assert stream.report.physical_requests == len(transport.sent)
    assert stream.report.batch_requests == 1


HTTP_BAD_GATEWAY = 502
HTTP_TOO_MANY_REQUESTS = 429


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, SEVERAL], ids=["one", "several"])
@pytest.mark.parametrize("safety", _SAFETIES)
@pytest.mark.parametrize("status", [HTTP_BAD_GATEWAY, HTTP_TOO_MANY_REQUESTS])
async def test_physical_batch_after_an_unstructured_transient_status(
    status: int,
    safety: ReplaySafety,
    count: int,
) -> None:
    # Owner decision (2026-09-25): a physical batch is replayed as a direct request of its combined safety would
    # be. SAFE is replayed after any transient status; UNSAFE and UNKNOWN only after one that means the batch was
    # not accepted (429). After a status that may follow execution (502) every admitted command becomes unknown.
    replayed = safety is ReplaySafety.SAFE or status == HTTP_TOO_MANY_REQUESTS
    transport = _Script([])

    def transient(request: Request) -> WireResponse:
        transport.sent.append(request)
        return WireResponse(status, (("content-type", "text/html"),), b"<html>transient</html>")

    transport.behaviors = [transient, _counting(transport)]
    stream = _client(transport).batch_outcomes(
        [Command(_request(safety), index) for index in range(count)],
        policy=_policy(),
    )

    outcomes = await _drain(stream)

    assert stream.report is not None
    assert len(outcomes) == count
    if replayed:
        assert len(transport.sent) == 2  # noqa: PLR2004 - one replay within the budget
        assert stream.report.retries == 1
        assert all(isinstance(outcome, CommandSuccess) for outcome in outcomes)
    else:
        assert len(transport.sent) == 1
        assert stream.report.retries == 0
        for outcome in outcomes:
            assert isinstance(outcome, CommandOutcomeUnknown)
            assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE
            assert isinstance(outcome.error, AmbiguousExecutionError)
            assert outcome.error.reason is AmbiguityReason.HTTP_STATUS_AFTER_DISPATCH
            assert isinstance(outcome.error.__cause__, HTTPGatewayError)
        assert stream.report.unknown == count
    assert stream.report.physical_requests == len(transport.sent)


@pytest.mark.asyncio
async def test_structured_refusal_of_a_safe_batch_and_a_transient_status_on_a_direct_call_keep_their_retry() -> None:
    # Controls for the rule above: a Bitrix envelope refusing the whole batch proves it did not run, so the batch
    # is sent again; a SAFE direct request retries after an unstructured 502, an UNSAFE one after a 429 only.
    batch_transport = _Script([])

    def refused(request: Request) -> WireResponse:
        batch_transport.sent.append(request)
        body = {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "Too many requests"}
        return WireResponse(503, (("content-type", "application/json"),), json.dumps(body).encode())

    batch_transport.behaviors = [refused, _counting(batch_transport)]
    outcomes = await _drain(
        _client(batch_transport).batch_outcomes(
            [Command(_request(ReplaySafety.SAFE), index) for index in range(SEVERAL)], policy=_policy()
        )
    )
    assert len(batch_transport.sent) == 2  # noqa: PLR2004 - one retry after the structured refusal
    assert all(isinstance(outcome, CommandSuccess) for outcome in outcomes)

    direct_transport = _Script([])

    def transient(request: Request) -> WireResponse:
        direct_transport.sent.append(request)
        return WireResponse(HTTP_BAD_GATEWAY, (("content-type", "text/html"),), b"<html>transient</html>")

    direct_transport.behaviors = [transient, _counting(direct_transport)]
    await _client(direct_transport).call(_request(ReplaySafety.SAFE), policy=_policy())
    assert len(direct_transport.sent) == 2  # noqa: PLR2004 - one retry after the transient status

    for status, sends in ((HTTP_TOO_MANY_REQUESTS, 2), (HTTP_BAD_GATEWAY, 1)):
        unsafe_transport = _Script([])

        def answered(request: Request, *, status: int = status, sent: _Script = unsafe_transport) -> WireResponse:
            sent.sent.append(request)
            return WireResponse(status, (("content-type", "text/html"),), b"<html>transient</html>")

        unsafe_transport.behaviors = [answered, _counting(unsafe_transport)]
        call = _client(unsafe_transport).call(_request(ReplaySafety.UNSAFE), policy=_policy())
        if status == HTTP_BAD_GATEWAY:
            with pytest.raises(AmbiguousExecutionError):
                await call
        else:
            await call
        assert len(unsafe_transport.sent) == sends


def _batch_methods(request: Request) -> list[str]:
    commands = request.copy_parameters()["cmd"]
    assert isinstance(commands, dict)
    return [str(query).split("?", 1)[0] for query in commands.values()]


def _batch_answer(results: Mapping[str, object], errors: Mapping[str, object]) -> WireResponse:
    body: dict[str, object] = {"result": {"result": dict(results), "result_error": dict(errors) or []}}
    return WireResponse(HTTP_OK, (("content-type", "application/json"),), json.dumps(body).encode())


_QUERY_LIMIT = {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "Too many requests"}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["status", "connection"])
async def test_a_mixed_batch_that_may_have_run_sends_only_its_safe_commands_again(failure: str) -> None:
    # Owner decision (2026-09-25): profile reads and one task creation share a batch that may have run. The reads
    # are sent again in a smaller batch; only the creation, which a replay could duplicate, is reported unknown.
    transport = _Script([])

    def broken(request: Request) -> WireResponse:
        transport.sent.append(request)
        if failure == "status":
            return WireResponse(HTTP_BAD_GATEWAY, (("content-type", "text/html"),), b"<html>bad gateway</html>")
        raise TransportError("connection lost", phase=FailurePhase.DISPATCH_STARTED, retryable=True)

    transport.behaviors = [broken, _counting(transport)]
    commands = [Command(_request(ReplaySafety.SAFE, "user.get"), index) for index in range(SEVERAL)]
    commands.insert(1, Command(_request(ReplaySafety.UNSAFE, "tasks.task.add"), SEVERAL))
    stream = _client(transport).batch_outcomes(commands, policy=_policy())

    outcomes = await _drain(stream)

    assert [outcome.correlation for outcome in outcomes] == [command.correlation for command in commands]
    assert len(transport.sent) == 2  # noqa: PLR2004 - the first batch and the smaller replay
    assert _batch_methods(transport.sent[1]) == ["user.get"] * SEVERAL
    for command, outcome in zip(commands, outcomes, strict=True):
        if command.request.method == "tasks.task.add":
            assert isinstance(outcome, CommandOutcomeUnknown)
            assert isinstance(outcome.error, AmbiguousExecutionError)
        else:
            assert isinstance(outcome, CommandSuccess)
    assert stream.report is not None
    assert stream.report.retries == 1
    assert stream.report.unknown == 1


@pytest.mark.asyncio
async def test_a_mixed_batch_that_was_not_accepted_is_sent_again_whole() -> None:
    # An unstructured 429 means nothing ran, so the task creation is sent again together with the reads.
    transport = _Script([])

    def throttled(request: Request) -> WireResponse:
        transport.sent.append(request)
        return WireResponse(HTTP_TOO_MANY_REQUESTS, (("content-type", "text/html"),), b"<html>slow down</html>")

    transport.behaviors = [throttled, _counting(transport)]
    commands = [
        Command(_request(ReplaySafety.SAFE, "user.get"), 0),
        Command(_request(ReplaySafety.UNSAFE, "tasks.task.add"), 1),
    ]

    outcomes = await _drain(_client(transport).batch_outcomes(commands, policy=_policy()))

    assert [_batch_methods(request) for request in transport.sent] == [["user.get", "tasks.task.add"]] * 2
    assert all(isinstance(outcome, CommandSuccess) for outcome in outcomes)


@pytest.mark.asyncio
async def test_commands_refused_by_their_own_transient_error_are_sent_again_alone() -> None:
    # A per-command QUERY_LIMIT_EXCEEDED answers before that command runs, so the SAFE read and the UNSAFE creation
    # it hit are both sent again; a permanent per-command error is kept, and nothing that succeeded is repeated.
    transport = _Script([])

    def partial(request: Request) -> WireResponse:
        transport.sent.append(request)
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        keys = list(commands)
        denied = {"error": "ACCESS_DENIED", "error_description": "no"}
        return _batch_answer({keys[0]: True}, {keys[1]: _QUERY_LIMIT, keys[2]: _QUERY_LIMIT, keys[3]: denied})

    transport.behaviors = [partial, _counting(transport)]
    commands = [
        Command(_request(ReplaySafety.SAFE, "user.get"), 0),
        Command(_request(ReplaySafety.SAFE, "user.get"), 1),
        Command(_request(ReplaySafety.UNSAFE, "tasks.task.add"), 2),
        Command(_request(ReplaySafety.UNSAFE, "tasks.task.update"), 3),
    ]
    stream = _client(transport).batch_outcomes(commands, policy=_policy())

    outcomes = await _drain(stream)

    assert len(transport.sent) == 2  # noqa: PLR2004 - the first batch and the smaller replay
    assert _batch_methods(transport.sent[1]) == ["user.get", "tasks.task.add"]
    assert [type(outcome) for outcome in outcomes] == [CommandSuccess, CommandSuccess, CommandSuccess, CommandFailure]
    denied_outcome = outcomes[3]
    assert isinstance(denied_outcome, CommandFailure)
    assert isinstance(denied_outcome.error, BatchCommandError)
    assert denied_outcome.error.normalized_code == "access_denied"
    assert stream.report is not None
    assert stream.report.retries == 1


@pytest.mark.asyncio
async def test_a_command_refused_on_every_round_keeps_its_error_once_the_attempt_budget_is_spent() -> None:
    transport = _Script([])

    def refused(request: Request) -> WireResponse:
        transport.sent.append(request)
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return _batch_answer({}, dict.fromkeys(commands, _QUERY_LIMIT))

    transport.behaviors = [refused]
    stream = _client(transport).batch_outcomes([Command(_request(ReplaySafety.UNSAFE), 0)], policy=_policy())

    outcomes = await _drain(stream)

    assert len(transport.sent) == 3  # noqa: PLR2004 - max_attempts_per_request rounds
    assert isinstance(outcomes[0], CommandFailure)
    assert isinstance(outcomes[0].error, BatchCommandError)
    assert outcomes[0].error.normalized_code == "query_limit_exceeded"


@pytest.mark.asyncio
async def test_a_fail_fast_batch_is_never_split_for_a_replay() -> None:
    transport = _Script([])

    def partial(request: Request) -> WireResponse:
        transport.sent.append(request)
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        keys = list(commands)
        return _batch_answer({keys[0]: True}, {keys[1]: _QUERY_LIMIT})

    transport.behaviors = [partial, _counting(transport)]
    stream = _client(transport).batch(
        [Command(_request(ReplaySafety.SAFE), index) for index in range(2)], policy=_policy()
    )
    with pytest.raises(BatchFailed):
        [outcome async for outcome in stream]
    assert len(transport.sent) == 1


_AFTER_WRITE = {"error": "AFTER_WRITE", "error_description": "failed after the write"}
HTTP_BAD_REQUEST = 400
HTTP_CONFLICT = 409


def _command_errors_then(
    transport: _Script, error: Mapping[str, object], *later: Callable[[Request], WireResponse]
) -> None:
    """Answer the first batch with ``error`` for every command, then follow ``later``."""

    def refused(request: Request) -> WireResponse:
        transport.sent.append(request)
        return _batch_answer({}, dict.fromkeys(_batch_keys(request), error))

    transport.behaviors = [refused, *later]


def _batch_keys(request: Request) -> list[str]:
    commands = request.copy_parameters()["cmd"]
    assert isinstance(commands, dict)
    return list(commands)


def _direct_answers(transport: _Script, first: WireResponse) -> None:
    def answered(request: Request) -> WireResponse:
        transport.sent.append(request)
        return first

    transport.behaviors = [answered, _counting(transport)]


@pytest.mark.asyncio
@pytest.mark.parametrize("safety", _SAFETIES)
async def test_a_configured_retry_code_or_status_does_not_prove_an_unsafe_request_never_ran(
    safety: ReplaySafety,
) -> None:
    # A code or status added to the retry policy says a failure is worth retrying, which is enough for SAFE work;
    # only the ambiguity policy's refusals prove that UNSAFE or UNKNOWN work did not run (owner decision).
    retry = RetryPolicy(
        transient_http_statuses=frozenset({HTTP_CONFLICT}),
        transient_api_codes=frozenset({"after_write"}),
        initial_delay=0,
        maximum_delay=0,
        jitter=0,
    )
    structured = WireResponse(
        HTTP_BAD_REQUEST, (("content-type", "application/json"),), json.dumps(_AFTER_WRITE).encode()
    )
    unstructured = WireResponse(HTTP_CONFLICT, (("content-type", "text/html"),), b"<html>conflict</html>")
    expected = 2 if safety is ReplaySafety.SAFE else 1
    for first, error_type in ((structured, ApiResponseError), (unstructured, HTTPGatewayError)):
        direct = _Script([])
        _direct_answers(direct, first)
        call = _client(direct).call(_request(safety, "tasks.task.add"), policy=_policy(retry=retry))
        if safety is ReplaySafety.SAFE:
            await call
        else:
            with pytest.raises(error_type):
                await call
        assert len(direct.sent) == expected

    batch = _Script([])
    _command_errors_then(batch, _AFTER_WRITE, _counting(batch))
    command = Command(_request(safety, "tasks.task.add"), 0)
    (outcome,) = await _drain(_client(batch).batch_outcomes([command], policy=_policy(retry=retry)))
    assert len(batch.sent) == expected
    if safety is ReplaySafety.SAFE:
        assert isinstance(outcome, CommandSuccess)
    else:
        assert isinstance(outcome, CommandFailure)
        assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE

    # Declaring the code a refusal is what lets the creation run again, directly and inside a batch.
    refusal = AmbiguityPolicy(refusal_api_codes=frozenset({"after_write"}))
    declared = _Script([])
    _direct_answers(declared, structured)
    await _client(declared).call(_request(safety, "tasks.task.add"), policy=_policy(retry=retry, ambiguity=refusal))
    assert len(declared.sent) == 2  # noqa: PLR2004 - the refusal and its replay
    declared_batch = _Script([])
    _command_errors_then(declared_batch, _AFTER_WRITE, _counting(declared_batch))
    (replayed,) = await _drain(
        _client(declared_batch).batch_outcomes([command], policy=_policy(retry=retry, ambiguity=refusal))
    )
    assert len(declared_batch.sent) == 2  # noqa: PLR2004 - the refused batch and its replay
    assert isinstance(replayed, CommandSuccess)


def _refused_then(transport: _Script, *later: Callable[[Request], WireResponse]) -> None:
    _command_errors_then(transport, _QUERY_LIMIT, *later)


def _status(transport: _Script, status: int) -> Callable[[Request], WireResponse]:
    def answered(request: Request) -> WireResponse:
        transport.sent.append(request)
        return WireResponse(status, (("content-type", "text/html"),), b"<html>transient</html>")

    return answered


@pytest.mark.asyncio
async def test_a_replay_round_that_was_sent_keeps_its_own_outcome_when_the_budget_stops_the_next_attempt() -> None:
    # The replay reached Bitrix and got a 502, and the request budget stopped the retry after it: the command may
    # have run, so its earlier "refused before running" outcome must not stand.
    transport = _Script([])
    _refused_then(transport, _status(transport, HTTP_BAD_GATEWAY))
    stream = _client(transport).batch_outcomes(
        [Command(_request(ReplaySafety.SAFE, "user.get"), 0)], policy=_policy(max_requests=2)
    )

    (outcome,) = await _drain(stream)

    assert len(transport.sent) == 2  # noqa: PLR2004 - the refused batch and the replay the budget ends
    assert isinstance(outcome, CommandOutcomeUnknown)
    assert isinstance(outcome.error, AmbiguousExecutionError)
    assert outcome.error.reason is AmbiguityReason.HTTP_STATUS_AFTER_DISPATCH
    budget = outcome.error.__cause__
    assert isinstance(budget, BudgetExceededError)
    assert isinstance(budget.__cause__, HTTPGatewayError)
    assert budget.__cause__.http_status == HTTP_BAD_GATEWAY

    # A replay refused with a listed status ran nothing, so the stop after it keeps the refusal.
    refused = _Script([])
    _refused_then(refused, _status(refused, HTTP_TOO_MANY_REQUESTS))
    (still_refused,) = await _drain(
        _client(refused).batch_outcomes(
            [Command(_request(ReplaySafety.UNSAFE, "tasks.task.add"), 0)], policy=_policy(max_requests=2)
        )
    )
    assert len(refused.sent) == 2  # noqa: PLR2004 - the refused batch and the refused replay
    assert isinstance(still_refused, CommandFailure)
    assert still_refused.replay_disposition is ReplayDisposition.ELIGIBLE

    # Control: a budget that stops the replay before it is sent leaves the refusal the command received.
    stopped = _Script([])
    _refused_then(stopped, _counting(stopped))
    (kept,) = await _drain(
        _client(stopped).batch_outcomes(
            [Command(_request(ReplaySafety.UNSAFE, "tasks.task.add"), 0)], policy=_policy(max_requests=1)
        )
    )
    assert len(stopped.sent) == 1
    assert isinstance(kept, CommandFailure)
    assert isinstance(kept.error, BatchCommandError)
    assert kept.replay_disposition is ReplayDisposition.ELIGIBLE


@pytest.mark.asyncio
@pytest.mark.parametrize("retryable", ["retryable", "permanent"])
async def test_a_batch_retried_after_a_send_that_may_have_run_stays_unknown_whatever_ends_the_retries(
    retryable: str,
) -> None:
    # The first send got a 502 and may have run; the retry never left the client. The round ends on that last
    # failure (or on the attempt budget after it), yet the command may already have run.
    transport = _Script([])

    def not_dispatched(request: Request) -> WireResponse:
        transport.sent.append(request)
        raise TransportError("refused locally", phase=FailurePhase.NOT_DISPATCHED, retryable=retryable == "retryable")

    transport.behaviors = [_status(transport, HTTP_BAD_GATEWAY), not_dispatched]
    stream = _client(transport).batch_outcomes(
        [Command(_request(ReplaySafety.SAFE, "user.get"), 0)], policy=_policy(max_attempts_per_request=2)
    )

    (outcome,) = await _drain(stream)

    assert len(transport.sent) == 2  # noqa: PLR2004 - the 502 and the retry that was not dispatched
    assert isinstance(outcome, CommandOutcomeUnknown)
    assert isinstance(outcome.error, AmbiguousExecutionError)


@pytest.mark.asyncio
@pytest.mark.parametrize("replay", ["success", "status"])
@pytest.mark.parametrize("rounds", [1, 2], ids=["first-round", "replay-round"])
async def test_a_batch_answered_after_the_time_budget_reports_its_unsafe_command_unknown(
    replay: str, rounds: int
) -> None:
    # Bitrix answered, but the time budget ran out before the answer was classified: an UNSAFE creation in that
    # batch may have run, whether it was the first batch or a replay after a quota refusal.
    now = [0.0]
    transport = _Script([])

    def late(request: Request) -> WireResponse:
        now[0] = 100.0
        if replay == "success":
            return _counting(transport)(request)
        return _status(transport, HTTP_BAD_GATEWAY)(request)

    if rounds == 1:
        transport.behaviors = [late]
    else:
        _refused_then(transport, late)
    executor = Executor(transport, clock=lambda: now[0])
    request = _request(ReplaySafety.UNSAFE, "tasks.task.add")

    (outcome,) = await BatchExecutor(executor).execute_requests((request,), context=executor.context(_policy()))

    assert len(transport.sent) == rounds
    assert isinstance(outcome, BatchFailure)
    assert isinstance(outcome.error, AmbiguousExecutionError)
    assert isinstance(outcome.error.__cause__, BudgetExceededError)
    assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE
    assert outcome.error.reason is (
        AmbiguityReason.DEADLINE_AFTER_DISPATCH if replay == "success" else AmbiguityReason.HTTP_STATUS_AFTER_DISPATCH
    )


@pytest.mark.asyncio
async def test_replay_rounds_and_the_retries_inside_them_share_one_attempt_budget() -> None:
    # max_attempts_per_request bounds every send of a command: the refused batch, the replay and its retries.
    transport = _Script([])
    _refused_then(transport, _status(transport, HTTP_TOO_MANY_REQUESTS), _status(transport, HTTP_TOO_MANY_REQUESTS))
    transport.behaviors.append(_counting(transport))
    stream = _client(transport).batch_outcomes([Command(_request(ReplaySafety.SAFE, "user.get"), 0)], policy=_policy())

    (outcome,) = await _drain(stream)

    assert len(transport.sent) == 3  # noqa: PLR2004 - max_attempts_per_request
    assert not isinstance(outcome, CommandSuccess)
    assert stream.report is not None
    assert stream.report.physical_requests == 3  # noqa: PLR2004 - no fourth send reaches the prepared success

    # The SAFE reads replayed from a mixed batch count the batch that carried them.
    mixed = _Script([])
    mixed.behaviors = [_status(mixed, HTTP_BAD_GATEWAY)]
    commands = [
        Command(_request(ReplaySafety.SAFE, "user.get"), 0),
        Command(_request(ReplaySafety.UNSAFE, "tasks.task.add"), 1),
    ]
    outcomes = await _drain(_client(mixed).batch_outcomes(commands, policy=_policy()))
    assert len(mixed.sent) == 3  # noqa: PLR2004 - the mixed batch and two sends of the reads alone
    for outcome in outcomes:
        assert isinstance(outcome, CommandOutcomeUnknown)
        assert isinstance(outcome.error, AmbiguousExecutionError)
        assert outcome.error.reason is AmbiguityReason.HTTP_STATUS_AFTER_DISPATCH

    # One attempt allows no replay round at all.
    single = _Script([])
    _refused_then(single, _counting(single))
    (refused,) = await _drain(
        _client(single).batch_outcomes(
            [Command(_request(ReplaySafety.SAFE, "user.get"), 0)], policy=_policy(max_attempts_per_request=1)
        )
    )
    assert len(single.sent) == 1
    assert isinstance(refused, CommandFailure)


@pytest.mark.asyncio
async def test_replay_rounds_share_the_retry_time_budget_measured_from_the_first_send() -> None:
    now = [0.0]
    transport = _Script([])

    def slow(behavior: Callable[[Request], WireResponse]) -> Callable[[Request], WireResponse]:
        def answered(request: Request) -> WireResponse:
            now[0] += 6.0
            return behavior(request)

        return answered

    _refused_then(transport, slow(_status(transport, HTTP_TOO_MANY_REQUESTS)), _counting(transport))
    transport.behaviors[0] = slow(transport.behaviors[0])
    executor = Executor(transport, clock=lambda: now[0])
    context = executor.context(_policy(max_elapsed=100.0, max_retry_elapsed_per_request=10.0))
    request = _request(ReplaySafety.SAFE, "user.get")

    (outcome,) = await BatchExecutor(executor).execute_requests((request,), context=context)

    # Each send takes 6 s: the replay's own 429 retry would start at 12 s, past the 10 s window of the first send.
    assert len(transport.sent) == 2  # noqa: PLR2004 - the refusal and one replay inside the window
    assert isinstance(outcome, BatchFailure)


# --- A13 -------------------------------------------------------------------------------------


class _ForeignTransportError(Exception):
    """An arbitrary exception from a user transport."""


def _foreign(*, after_send: bool, sent: list[Request]) -> Callable[[Request], WireResponse]:
    def behavior(request: Request) -> WireResponse:
        if after_send:
            sent.append(request)
        raise _ForeignTransportError("user transport exploded with secret-token")

    return behavior


@pytest.mark.asyncio
@pytest.mark.parametrize("after_send", [False, True], ids=["at-entry", "after-send"])
async def test_unknown_transport_exception_is_a_non_retryable_dispatch_started_failure(after_send: bool) -> None:  # noqa: FBT001
    transport = _Script([])
    transport.behaviors = [_foreign(after_send=after_send, sent=transport.sent)]

    with pytest.raises(TransportError) as captured:
        await _client(transport).call(_request(ReplaySafety.SAFE), policy=_policy())

    error = captured.value
    assert transport.entered == 1
    assert error.phase is FailurePhase.DISPATCH_STARTED
    assert error.possible_acceptance
    assert error.retryable is False
    assert isinstance(error.__cause__, _ForeignTransportError)
    assert "secret-token" not in str(error)
    assert "secret-token" not in repr(error.to_safe_dict())


@pytest.mark.asyncio
@pytest.mark.parametrize("safety", [ReplaySafety.UNSAFE, ReplaySafety.UNKNOWN])
@pytest.mark.parametrize("after_send", [False, True], ids=["at-entry", "after-send"])
async def test_unknown_transport_exception_is_ambiguous_for_unproven_direct_requests(
    after_send: bool,  # noqa: FBT001
    safety: ReplaySafety,
) -> None:
    transport = _Script([])
    transport.behaviors = [_foreign(after_send=after_send, sent=transport.sent)]

    with pytest.raises(AmbiguousExecutionError) as captured:
        await _client(transport).call(_request(safety), policy=_policy())

    assert transport.entered == 1
    assert captured.value.reason is AmbiguityReason.CONNECTION_LOST_AFTER_DISPATCH
    cause = captured.value.__cause__
    assert isinstance(cause, TransportError)
    assert cause.phase is FailurePhase.DISPATCH_STARTED
    assert isinstance(cause.__cause__, _ForeignTransportError)


@pytest.mark.asyncio
async def test_failure_before_the_transport_is_called_keeps_its_own_classification() -> None:
    class _WireOnly(_Script):
        capabilities = "not capabilities"

        async def send_wire(
            self,
            request: WireRequest,
            *,
            attempt_timeout: float,
            max_response_bytes: int,
        ) -> WireResponse:
            del request, attempt_timeout, max_response_bytes
            raise AssertionError("transport must not be entered")

    transport = _WireOnly([_envelope])

    with pytest.raises(CapabilityError, match="malformed capabilities"):
        await _client(transport).call(_request(ReplaySafety.UNSAFE), policy=_policy())

    assert transport.entered == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("after_send", [False, True], ids=["at-entry", "after-send"])
async def test_batch_outcomes_are_total_after_an_unknown_transport_exception(after_send: bool) -> None:  # noqa: FBT001
    transport = _Script([])
    transport.behaviors = [_foreign(after_send=after_send, sent=transport.sent)]
    rejected = Request("sample.v3", replay_safety=ReplaySafety.SAFE, route=RouteKind.API_V3)
    commands = [
        Command(_request(ReplaySafety.SAFE), 0),
        Command(rejected, 1),
        Command(_request(ReplaySafety.UNSAFE, "sample.add"), 2),
        Command(_request(ReplaySafety.UNKNOWN), 3),
    ]
    stream = _client(transport).batch_outcomes(commands, policy=_policy())

    outcomes = await _drain(stream)

    assert transport.entered == 1
    assert [outcome.correlation for outcome in outcomes] == [0, 1, 2, 3]
    preflight = outcomes[1]
    assert isinstance(preflight, CommandFailure)
    assert isinstance(preflight.error, CapabilityError)
    admitted = [outcome for index, outcome in enumerate(outcomes) if index != 1]
    for outcome in admitted:
        assert isinstance(outcome, CommandOutcomeUnknown)
        assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE
        assert isinstance(outcome.error, AmbiguousExecutionError)
        cause = outcome.error.__cause__
        assert isinstance(cause, TransportError)
        assert isinstance(cause.__cause__, _ForeignTransportError)
    assert stream.report is not None
    assert stream.report.physical_requests == 1
    assert stream.report.unknown == len(admitted)
    assert stream.report.failures == 1
    assert stream.report.state is TerminalState.COMPLETED_WITH_FAILURES


@pytest.mark.asyncio
async def test_fail_fast_batch_raises_batch_failed_with_unknown_outcomes() -> None:
    transport = _Script([])
    transport.behaviors = [_foreign(after_send=True, sent=transport.sent)]
    stream = _client(transport).batch(
        [Command(_request(ReplaySafety.SAFE), index) for index in range(SEVERAL)],
        policy=_policy(),
    )

    with pytest.raises(BatchFailed) as captured:
        await anext(stream)

    assert transport.entered == 1
    assert len(captured.value.outcomes) == SEVERAL
    assert all(isinstance(outcome, CommandOutcomeUnknown) for outcome in captured.value.outcomes)
    assert not any(isinstance(outcome, CommandFailure) for outcome in captured.value.outcomes)
    assert captured.value.report.physical_requests == 1


class _ForeignBaseException(BaseException):
    """A non-Exception signal that must never be converted into an outcome."""


@pytest.mark.asyncio
@pytest.mark.parametrize("signal", [asyncio.CancelledError, _ForeignBaseException])
async def test_cancellation_and_base_exceptions_are_never_converted(signal: type[BaseException]) -> None:
    def behavior(_request: Request) -> WireResponse:
        raise signal("stop")

    transport = _Script([behavior])
    with pytest.raises(signal):
        await Executor(transport).execute(_request(ReplaySafety.UNSAFE), policy=_policy())

    batch_transport = _Script([behavior])
    stream = _client(batch_transport).batch_outcomes([Command(_request(ReplaySafety.SAFE), 0)], policy=_policy())
    with pytest.raises(signal):
        await anext(stream)
    assert batch_transport.entered == 1


# --- B29 -------------------------------------------------------------------------------------


def _sized(size: int) -> Callable[[Request], WireResponse]:
    def behavior(request: Request) -> WireResponse:
        body = json.dumps(_payload(request)).encode()
        assert len(body) <= size
        return WireResponse(HTTP_OK, (("content-type", "application/json"),), body + b" " * (size - len(body)))

    return behavior


def _payload(request: Request) -> object:
    if request.method == "batch":
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {"result": {"result": dict.fromkeys(commands, 1), "result_error": []}}
    return {"result": 1}


class _DecoderSpy:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.calls = 0
        original_decode = executor_module._decode_success  # noqa: SLF001 - decoder spy
        original_error = ProtocolCodec.error_from_http

        def decode(*args: object, **kwargs: object) -> object:
            self.calls += 1
            return original_decode(*args, **kwargs)  # type: ignore[arg-type]

        def error_from_http(codec: ProtocolCodec, **kwargs: object) -> object:
            self.calls += 1
            return original_error(codec, **kwargs)  # type: ignore[arg-type]

        original_parse = executor_module._parse_success_body  # noqa: SLF001 - decoder spy

        def parse(*args: object, **kwargs: object) -> object:
            self.calls += 1
            return original_parse(*args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(executor_module, "_decode_success", decode)
        monkeypatch.setattr(executor_module, "_parse_success_body", parse)
        monkeypatch.setattr(ProtocolCodec, "error_from_http", error_from_http)


@pytest.mark.asyncio
async def test_oversized_injected_response_is_rejected_before_decoding_for_safe_direct(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spy = _DecoderSpy(monkeypatch)
    transport = _Script([_sized(CEILING + 1)])

    with pytest.raises(ResponseTooLargeError, match="byte ceiling") as captured:
        await _client(transport).call(_request(ReplaySafety.SAFE), policy=_policy(max_response_bytes=CEILING))

    assert transport.entered == 1
    assert spy.calls == 0
    assert captured.value.request_summary is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("safety", [ReplaySafety.UNSAFE, ReplaySafety.UNKNOWN])
async def test_oversized_injected_response_is_ambiguous_for_unproven_direct(
    monkeypatch: pytest.MonkeyPatch,
    safety: ReplaySafety,
) -> None:
    spy = _DecoderSpy(monkeypatch)
    transport = _Script([_sized(CEILING + 1)])

    with pytest.raises(AmbiguousExecutionError) as captured:
        await _client(transport).call(_request(safety), policy=_policy(max_response_bytes=CEILING))

    assert transport.entered == 1
    assert spy.calls == 0
    assert captured.value.reason is AmbiguityReason.RESPONSE_LIMIT_AFTER_DISPATCH
    assert captured.value.declared_unsafe is (safety is ReplaySafety.UNSAFE)
    assert isinstance(captured.value.__cause__, ResponseTooLargeError)


@pytest.mark.asyncio
@pytest.mark.parametrize("safety", _SAFETIES)
async def test_oversized_injected_batch_response_makes_every_command_unknown(
    monkeypatch: pytest.MonkeyPatch,
    safety: ReplaySafety,
) -> None:
    transport = _Script([_sized(4 * CEILING)])
    spy = _DecoderSpy(monkeypatch)
    stream = _client(transport).batch_outcomes(
        [Command(_request(safety), index) for index in range(SEVERAL)],
        policy=_policy(max_response_bytes=4 * CEILING - 1),
    )

    outcomes = await _drain(stream)

    assert transport.entered == 1
    assert spy.calls == 0
    for outcome in outcomes:
        assert isinstance(outcome, CommandOutcomeUnknown)
        assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE
        assert isinstance(outcome.error, AmbiguousExecutionError)
        assert outcome.error.reason is AmbiguityReason.RESPONSE_LIMIT_AFTER_DISPATCH
        assert isinstance(outcome.error.__cause__, ResponseTooLargeError)
    assert stream.report is not None
    assert stream.report.unknown == SEVERAL
    assert stream.report.physical_requests == 1


@pytest.mark.asyncio
async def test_oversized_batch_response_from_the_default_transport_makes_every_command_unknown() -> None:
    # The bundled HttpxTransport enforces the same ceiling: a SAFE physical batch whose response is refused
    # may have run, so every command is unknown rather than a shared CommandFailure (2.3 behavior).
    sends: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        sends.append(request)
        return httpx.Response(HTTP_OK, headers={"content-type": "application/json"}, content=b" " * 4 * CEILING)

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    client = _client(HttpxTransport(f"https://{HOST}/rest/1/token/", client=http_client))
    policy = _policy(max_response_bytes=4 * CEILING - 1)
    try:
        outcomes = await _drain(
            client.batch_outcomes(
                [Command(_request(ReplaySafety.SAFE), index) for index in range(SEVERAL)], policy=policy
            )
        )
        assert len(sends) == 1
        for outcome in outcomes:
            assert isinstance(outcome, CommandOutcomeUnknown)
            assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE
            assert isinstance(outcome.error.__cause__, ResponseTooLargeError)

        # A direct SAFE request through the same transport still raises the refusal itself.
        with pytest.raises(ResponseTooLargeError):
            await client.call(_request(ReplaySafety.SAFE), policy=policy)
        assert len(sends) == 2  # noqa: PLR2004 - one batch send, one direct send
    finally:
        await http_client.aclose()


@pytest.mark.asyncio
async def test_response_of_exactly_the_ceiling_is_accepted() -> None:
    transport = _Script([_sized(CEILING)])

    result = await _client(transport).call(_request(ReplaySafety.UNSAFE), policy=_policy(max_response_bytes=CEILING))

    assert result == 1
    assert transport.entered == 1


@pytest.mark.asyncio
async def test_wire_transport_response_is_bound_by_the_same_ceiling() -> None:
    class _Wire(_Script):
        capabilities = TransportCapabilities()

        async def send_wire(
            self,
            request: WireRequest,
            *,
            attempt_timeout: float,
            max_response_bytes: int,
        ) -> WireResponse:
            del request, attempt_timeout, max_response_bytes
            self.entered += 1
            return WireResponse(HTTP_OK, (("content-type", "application/json"),), b" " * (CEILING + 1))

    transport = _Wire([])

    with pytest.raises(ResponseTooLargeError):
        await _client(transport).call(_request(ReplaySafety.SAFE), policy=_policy(max_response_bytes=CEILING))

    assert transport.entered == 1


# --- B8 --------------------------------------------------------------------------------------


class _ParseCounter:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.strict = 0
        self.codec = 0
        original_strict = executor_module._parse_success_body  # noqa: SLF001 - parse spy
        original_codec = ProtocolCodec._parse_body  # noqa: SLF001 - parse spy

        def strict(*args: object, **kwargs: object) -> object:
            self.strict += 1
            return original_strict(*args, **kwargs)  # type: ignore[arg-type]

        def codec(body: object) -> object:
            # Only a text body is parsed; a mapping handed over from the strict parse is not parsed again.
            if not isinstance(body, Mapping):
                self.codec += 1
            return original_codec(body)  # type: ignore[arg-type]

        monkeypatch.setattr(executor_module, "_parse_success_body", strict)
        monkeypatch.setattr(ProtocolCodec, "_parse_body", staticmethod(codec))


def _json_body(payload: object) -> Callable[[Request], WireResponse]:
    def behavior(_request: Request) -> WireResponse:
        return WireResponse(HTTP_OK, (("content-type", "application/json"),), json.dumps(payload).encode())

    return behavior


@pytest.mark.asyncio
async def test_json_success_body_is_parsed_exactly_once(monkeypatch: pytest.MonkeyPatch) -> None:
    counter = _ParseCounter(monkeypatch)
    transport = _Script([_json_body({"result": {"ID": "1"}, "total": 1})])

    assert await _client(transport).call(_request(ReplaySafety.SAFE)) == {"ID": "1"}
    assert (counter.strict, counter.codec) == (1, 0)


@pytest.mark.asyncio
async def test_batch_success_body_is_parsed_exactly_once(monkeypatch: pytest.MonkeyPatch) -> None:
    counter = _ParseCounter(monkeypatch)
    transport = _Script([_envelope])

    outcomes = await _drain(_client(transport).batch_outcomes([Command(_request(ReplaySafety.SAFE), 0)]))

    assert [type(outcome) for outcome in outcomes] == [CommandSuccess]
    assert (counter.strict, counter.codec) == (1, 0)


@pytest.mark.asyncio
async def test_structured_error_in_success_status_keeps_the_codec_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counter = _ParseCounter(monkeypatch)
    transport = _Script([_json_body({"error": "ACCESS_DENIED", "error_description": "denied"})])

    with pytest.raises(ApiResponseError) as captured:
        await _client(transport).call(_request(ReplaySafety.SAFE))

    assert captured.value.normalized_code == "access_denied"
    assert captured.value.evidence.body_preview is not None
    assert "denied" in captured.value.evidence.body_preview
    assert (counter.strict, counter.codec) == (1, 0)
    assert transport.entered == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [b'{"error":"ACCESS_DENIED","junk":NaN}', b'{"error":"ACCESS_DENIED","description":"\xff"}'],
    ids=["non-finite-number", "invalid-utf8"],
)
async def test_strict_only_defect_in_a_success_error_body_is_an_envelope_contract_error(
    monkeypatch: pytest.MonkeyPatch,
    body: bytes,
) -> None:
    # A lenient second parse would accept these bytes and report the embedded error; B8 keeps the one
    # strict parse authoritative, so the response is malformed rather than a structured API error.
    counter = _ParseCounter(monkeypatch)
    transport = _Script([lambda _request: WireResponse(HTTP_OK, (("content-type", "application/json"),), body)])

    with pytest.raises(EnvelopeContractError):
        await _client(transport).call(_request(ReplaySafety.SAFE))

    assert (counter.strict, counter.codec) == (1, 0)
    assert transport.entered == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("closed", "refusal"),
    [("transport", "transport is closed"), ("injected client", "HTTP client is closed")],
)
async def test_closed_default_transport_refuses_before_dispatch_without_implying_acceptance(
    closed: str, refusal: str
) -> None:
    # A closed HttpxTransport, or an open one over an injected client closed before the call, refuses before
    # any byte leaves the process: NOT_DISPATCHED, never ambiguous, never retried. Only a foreign transport's
    # arbitrary exception keeps the conservative classification.
    sends: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        sends.append(request)
        return httpx.Response(HTTP_OK, headers={"content-type": "application/json"}, content=b'{"result": 1}')

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    transport = HttpxTransport(f"https://{HOST}/rest/1/token/", client=http_client)
    client = _client(transport)
    try:
        # Control: the same pair sends while both are open.
        assert await client.call(_request(ReplaySafety.UNSAFE), policy=_policy()) == 1
        assert len(sends) == 1
        sends.clear()
        await (transport.aclose() if closed == "transport" else http_client.aclose())
        for safety in (ReplaySafety.UNSAFE, ReplaySafety.SAFE):
            with pytest.raises(TransportError, match=refusal) as refused:
                await client.call(_request(safety), policy=_policy())
            assert refused.value.phase is FailurePhase.NOT_DISPATCHED
            assert refused.value.retryable is False
        outcomes = await _drain(
            client.batch_outcomes([Command(_request(ReplaySafety.UNSAFE), index) for index in range(2)])
        )
        # A refused chunk is a correlated failure of every command, as for any NOT_DISPATCHED transport error.
        assert len(outcomes) == 2  # noqa: PLR2004
        for outcome in outcomes:
            assert isinstance(outcome, CommandFailure)
            assert isinstance(outcome.error, TransportError)
            assert outcome.error.phase is FailurePhase.NOT_DISPATCHED
            assert outcome.error.retryable is False
        assert sends == []
    finally:
        await transport.aclose()
        await http_client.aclose()
