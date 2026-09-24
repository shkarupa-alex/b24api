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
from b24api.contracts import Command, CommandFailure, CommandOutcomeUnknown, CommandSuccess, ReplayDisposition
from b24api.contracts.policy import AmbiguityReason, ExecutionPolicy, RetryPolicy
from b24api.contracts.request import RouteKind
from b24api.errors import (
    AmbiguousExecutionError,
    ApiResponseError,
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
    if possible:
        # A physical batch that may have been accepted is never replayed, whatever its safety or flag.
        assert len(transport.sent) == 1
        assert all(isinstance(outcome, CommandOutcomeUnknown) for outcome in outcomes)
        for outcome in outcomes:
            assert isinstance(outcome, CommandOutcomeUnknown)
            assert isinstance(outcome.error, AmbiguousExecutionError)
            assert outcome.error.__cause__ is raised[0]
            assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE
        assert stream.report.unknown == count
        assert stream.report.state is TerminalState.COMPLETED_WITH_FAILURES
    elif not retryable:
        assert len(transport.sent) == 1
        for outcome in outcomes:
            assert isinstance(outcome, CommandFailure)
            assert outcome.error is raised[0]
            assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE
        assert stream.report.failures == count
    else:
        assert len(transport.sent) == 2  # noqa: PLR2004 - nothing was dispatched before the retry
        assert all(isinstance(outcome, CommandSuccess) for outcome in outcomes)
        assert stream.report.state is TerminalState.COMPLETED
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
    # §3.4: a physical batch is never replayed as a whole, whatever the safety flag. Every admitted command
    # becomes unknown after a status that may follow execution (502) and fails after one that means the
    # batch was not accepted (429); the scripted success after the status stays unused.
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
    assert len(transport.sent) == 1
    assert len(outcomes) == count
    assert stream.report.retries == 0
    for outcome in outcomes:
        if status == HTTP_BAD_GATEWAY:
            assert isinstance(outcome, CommandOutcomeUnknown)
            assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE
            assert isinstance(outcome.error, AmbiguousExecutionError)
            assert outcome.error.reason is AmbiguityReason.HTTP_STATUS_AFTER_DISPATCH
            assert isinstance(outcome.error.__cause__, HTTPGatewayError)
        else:
            assert isinstance(outcome, CommandFailure)
            assert outcome.replay_disposition is ReplayDisposition.NOT_ELIGIBLE
            assert isinstance(outcome.error, HTTPGatewayError)
            assert outcome.error.http_status == HTTP_TOO_MANY_REQUESTS
    if status == HTTP_BAD_GATEWAY:
        assert stream.report.unknown == count
    assert stream.report.physical_requests == len(transport.sent)


@pytest.mark.asyncio
async def test_structured_refusal_of_a_safe_batch_and_a_transient_status_on_a_direct_call_keep_their_retry() -> None:
    # Positive controls for the rule above: a Bitrix envelope refusing the whole batch proves it did not run,
    # so the batch is sent again; a SAFE direct request still retries after an unstructured 502.
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
async def test_closed_default_transport_refuses_before_dispatch_without_implying_acceptance() -> None:
    # A closed HttpxTransport refuses before any byte leaves the process: NOT_DISPATCHED, never ambiguous,
    # never retried. Only a foreign transport's arbitrary exception keeps the conservative classification.
    sends: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        sends.append(request)
        return httpx.Response(HTTP_OK, headers={"content-type": "application/json"}, content=b'{"result": 1}')

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    transport = HttpxTransport(f"https://{HOST}/rest/1/token/", client=http_client)
    client = _client(transport)
    await transport.aclose()
    try:
        for safety in (ReplaySafety.UNSAFE, ReplaySafety.SAFE):
            with pytest.raises(TransportError, match="transport is closed") as refused:
                await client.call(_request(safety), policy=_policy())
            assert refused.value.phase is FailurePhase.NOT_DISPATCHED
            assert refused.value.retryable is False
        outcomes = await _drain(
            client.batch_outcomes([Command(_request(ReplaySafety.UNSAFE), index) for index in range(2)])
        )
        assert outcomes
        assert not any(isinstance(outcome, CommandOutcomeUnknown) for outcome in outcomes)
        assert sends == []
    finally:
        await http_client.aclose()
