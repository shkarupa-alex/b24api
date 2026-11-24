"""What one send's answer proves about whether its request ran: one verdict, read from the whole checked answer.

Every path (``call()``, direct and batch fan-out, classic and REST v3 batch) reads a send's failure through the same
rule, so the same answer gives the same outcome wherever it arrives. A command that may have run is never reported
as a definite failure and never sent again unless it is ``SAFE``; only a listed refusal proves it did not run.
"""

from __future__ import annotations
import json
from dataclasses import asdict, replace
from typing import TYPE_CHECKING

import pytest

from b24api import (
    BatchDispatch,
    BatchFailed,
    DirectDispatch,
    ExecutionPolicy,
    ReplaySafety,
    Request,
    RetryPolicy,
    RouteKind,
)
from b24api.cli import _safe_error
from b24api.completion.reference_recorder import ReferenceCompletionRecorder
from b24api.contracts import (
    Command,
    CommandFailure,
    CommandOutcomeUnknown,
    CommandSettlement,
    CommandSuccess,
    Response,
    ResultErrorSpec,
    ResultSelector,
)
from b24api.contracts.json import MAX_JSON_DEPTH
from b24api.contracts.response import ResponseTime
from b24api.errors import (
    AmbiguousExecutionError,
    ApiResponseError,
    BudgetExceededError,
    EnvelopeContractError,
    FailurePhase,
    HTTPGatewayError,
    ProtocolError,
    TransportError,
)
from b24api.execution.facts import Execution, execution, record_dispatch, settlement
from b24api.execution.failure import outcome_error
from b24api.testing import ScriptedExchange, ScriptedTransport
from b24api.transport import WireResponse
from b24api.transport.protocol import decode_json
from tests.scripting import ResponderTransport, client_for

if TYPE_CHECKING:
    from collections.abc import AsyncIterable

    from b24api.contracts import CommandOutcome, OperationReport

SAFE = ReplaySafety.SAFE
UNSAFE = ReplaySafety.UNSAFE
JSON = (("content-type", "application/json"),)
POLICY = ExecutionPolicy(max_attempts_per_request=2, retry=RetryPolicy(initial_delay=0, maximum_delay=0, jitter=0))
CREATE = Request.bare("entity.add", {"fields": {"TITLE": "x"}}, UNSAFE)
READ = Request.bare("entity.get", {"id": 1}, SAFE)
# A body nested deeper than the JSON decoder can follow, though far below the response size ceiling.
DEEP = b'{"result":' + b"[" * 20000 + b"0" + b"]" * 20000 + b"}"
DEEP_ERROR = b'{"error":"QUERY_LIMIT_EXCEEDED","detail":' + b"[" * 20000 + b"0" + b"]" * 20000 + b"}"
TWO = 2
CREATED = 7
EMBEDDED = ResultErrorSpec(ResultSelector(("error",)), ("code",), ("message",))
# A repeat that would hide an embedded error behind its last, empty, value: read as such, a failure is a success.
HIDDEN_ERRORS = (
    b'{"result":{"error":{"code":"ERR","code":""}}}',
    b'{"result":{"error":{"code":"ERR","code":null}}}',
    b'{"result":{"error":{"code":"ERR","code":0}}}',
    b'{"result":{"error":{"code":"ERR"},"error":null}}',
    b'{"result":{"error":{"code":"ERR"},"error":{}}}',
    b'{"result":{"error":{"code":"A"},"error":{"code":"B"}}}',
)
HIDDEN_IDS = ("empty-code", "null-code", "zero-code", "null-error", "empty-error", "repeated-error")


def _raw(body: bytes, status: int = 200) -> WireResponse:
    return WireResponse(status, JSON, body)


def _json(payload: object, status: int = 200) -> WireResponse:
    return _raw(json.dumps(payload).encode(), status)


def _key(request: Request) -> str:
    commands = request.copy_parameters()["cmd"]
    assert isinstance(commands, dict)
    return next(iter(commands))


def _batch_ok(request: Request) -> dict[str, object]:
    commands = request.copy_parameters()["cmd"]
    assert isinstance(commands, dict)
    return {"result": {"result": dict.fromkeys(commands, 42), "result_error": []}}


async def _drain[T](stream: AsyncIterable[T]) -> tuple[list[T], OperationReport]:
    async with stream as opened:  # type: ignore[attr-defined]
        items = [item async for item in opened]
    report = opened.report
    assert report is not None
    return items, report


async def _batch_outcomes(
    transport: ResponderTransport, commands: list[Command[str]]
) -> tuple[list[CommandOutcome[str]], OperationReport]:
    async with client_for(transport, policy=POLICY) as api:
        return await _drain(api.batch_outcomes(commands))


async def _fan_out(
    transport: ResponderTransport, command: Command[str], *, dispatch: DirectDispatch | BatchDispatch
) -> CommandOutcome[str]:
    async with client_for(transport, policy=POLICY) as api:
        outcomes, _report = await _drain(api.fan_out_outcomes([command], dispatch=dispatch))
    (outcome,) = outcomes
    return outcome


@pytest.mark.asyncio
async def test_repeated_member_of_a_v3_error_object_never_proves_a_refusal() -> None:
    """A REST 3.0 ``error`` object that repeats ``code`` is not read as its last code: the creation stays unknown."""
    body = b'{"error":{"code":"INTERNAL_SERVER_ERROR","code":"QUERY_LIMIT_EXCEEDED","message":"m"}}'
    create = Request("x.item.add", {"fields": {"title": "x"}}, UNSAFE, route=RouteKind.API_V3)
    transport = ScriptedTransport((replace(ScriptedExchange.json(create, {}), response=_raw(body, 503)),))
    async with client_for(transport, policy=POLICY) as api:
        with pytest.raises(AmbiguousExecutionError):
            await api.call(create)

    assert len(transport.calls) == 1


@pytest.mark.asyncio
async def test_repeated_member_of_a_classic_envelope_never_proves_a_refusal() -> None:
    """An envelope that repeats ``result_error`` keeps two answers where one is read: the command is unknown."""

    def answer(request: Request) -> object:
        if len(transport.requests) > 1:
            return _batch_ok(request)
        key = _key(request)
        raw = (
            f'{{"result":{{"result_error":{{"{key}":{{"error":"INTERNAL_SERVER_ERROR"}}}},"result":[],'
            f'"result_error":{{"{key}":{{"error":"QUERY_LIMIT_EXCEEDED"}}}}}}}}'
        )
        return _raw(raw.encode())

    transport = ResponderTransport(answer)
    outcomes, report = await _batch_outcomes(transport, [Command(CREATE, "x")])

    assert len(transport.requests) == 1
    assert isinstance(outcomes[0], CommandOutcomeUnknown)
    assert isinstance(outcomes[0].error.__cause__, ProtocolError)
    assert report.unknown == 1


@pytest.mark.asyncio
async def test_repeated_member_of_a_command_error_entry_never_proves_a_refusal() -> None:
    """A command's error entry that repeats ``error`` names no one error: that command is unknown, never resent."""

    def answer(request: Request) -> object:
        if len(transport.requests) > 1:
            return _batch_ok(request)
        key = _key(request)
        raw = (
            f'{{"result":{{"result":[],"result_error":{{"{key}":'
            '{"error":"INTERNAL_SERVER_ERROR","error":"QUERY_LIMIT_EXCEEDED"}}}}'
        )
        return _raw(raw.encode())

    transport = ResponderTransport(answer)
    outcomes, report = await _batch_outcomes(transport, [Command(CREATE, "x")])

    assert len(transport.requests) == 1
    assert isinstance(outcomes[0], CommandOutcomeUnknown)
    assert isinstance(outcomes[0].error.__cause__, ProtocolError)
    assert report.unknown == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        b'{"result":{"result":{"a":1,"a":2}}}',
        b'{"result":{"result_total":1,"result_total":2}}',
        b'{"result":{"result_error":{"a":{"error":"X","error":"Y"}}}}',
    ],
    ids=["result", "result_total", "result_error"],
)
async def test_a_direct_result_shaped_like_an_envelope_keeps_its_repeated_names(body: bytes) -> None:
    """Only a batch envelope's members decide its commands' answers: a direct result's own data is data."""
    transport = ResponderTransport(lambda _request: _raw(body))
    async with client_for(transport, policy=POLICY) as api:
        result = await api.call(READ)

    assert result == json.loads(body)["result"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [423, 425, 429])
@pytest.mark.parametrize(
    "body",
    [
        b'{"error":"QUERY_LIMIT_EXCEEDED","error":"INTERNAL_SERVER_ERROR"}',
        b'{"error":"QUERY_LIMIT_EXCEEDED","error_description":"a","error_description":"b"}',
        b'{"message":"a","message":"b"}',
        b'{"result":1,"result":2}',
    ],
    ids=["error", "error-description", "message-without-error", "result-without-error"],
)
async def test_repeated_member_at_a_refusal_status_is_not_a_refusal(status: int, body: bytes) -> None:
    """A body the decoder refuses for a repeat is no gateway's page, ``error`` or not: it proves no refusal."""
    transport = ResponderTransport(
        lambda _request: _raw(body, status) if len(transport.requests) == 1 else {"result": 1}
    )
    async with client_for(transport, policy=POLICY) as api:
        with pytest.raises(ProtocolError):
            await api.call(CREATE)

    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_repeated_v3_error_member_at_a_refusal_status_leaves_the_envelope_unknown() -> None:
    """A REST 3.0 envelope's repeated error at 429 is the server's unreadable answer: unknown, not sent again."""
    requests = tuple(Request("x.item.add", {"id": index}, UNSAFE, route=RouteKind.API_V3) for index in (1, 2))
    body = b'{"error":{"code":"BITRIX_REST_V3_EXCEPTION_RATELIMITEXCEPTION","code":"INTERNAL","message":"m"}}'
    exchange = ScriptedExchange.v3_batch(requests, payload={}, status=429)
    transport = ScriptedTransport((replace(exchange, response=WireResponse(429, JSON, body)),))
    async with client_for(transport, policy=POLICY) as api:
        outcomes, report = await _drain(api.batch_outcomes([Command(request, "x") for request in requests]))

    assert all(isinstance(outcome, CommandOutcomeUnknown) for outcome in outcomes)
    assert report.physical_requests == 1
    transport.assert_exhausted()


@pytest.mark.asyncio
@pytest.mark.parametrize("member", ["result_total", "result_next", "result_time"])
async def test_repeated_command_key_in_any_envelope_map_leaves_the_command_unknown(member: str) -> None:
    """Every map of command keys decides its commands' answers: a key repeated in any of them makes them unknown."""

    def answer(request: Request) -> object:
        if len(transport.requests) > 1:
            return _batch_ok(request)
        key = _key(request)
        raw = f'{{"result":{{"result":{{"{key}":{{"ID":1}}}},"result_error":[],"{member}":{{"{key}":1,"{key}":2}}}}}}'
        return _raw(raw.encode())

    transport = ResponderTransport(answer)
    outcomes, report = await _batch_outcomes(transport, [Command(CREATE, "x")])

    assert len(transport.requests) == 1
    assert isinstance(outcomes[0], CommandOutcomeUnknown)
    assert report.unknown == 1


# An embedded result error (ResultErrorSpec) that cannot be read ---------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        b'{"result":{"error":{"code":[],"message":"m"}}}',
        b'{"result":{"error":{"code":"INTERNAL_SERVER_ERROR","code":"QUERY_LIMIT_EXCEEDED","message":"m"}}}',
        b'{"result":{"error":{"code":"ERR","message":"a","message":"b"}}}',
        *HIDDEN_ERRORS,
    ],
    ids=["malformed-code", "repeated-code", "repeated-message", *HIDDEN_IDS],
)
async def test_call_of_a_creation_whose_embedded_error_is_unreadable_raises_ambiguity(body: bytes) -> None:
    """An embedded error the request's contract cannot read names no failure: ``call()`` reports the unknown."""
    create = Request.bare("entity.add", {"fields": {"TITLE": "x"}}, UNSAFE, result_error=EMBEDDED)
    transport = ResponderTransport(lambda _request: _raw(body) if len(transport.requests) == 1 else {"result": 1})
    async with client_for(transport, policy=POLICY) as api:
        with pytest.raises(AmbiguousExecutionError) as raised:
            await api.call(create)

    assert len(transport.requests) == 1
    assert isinstance(raised.value.__cause__, ProtocolError)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("spec", "error"),
    [
        (EMBEDDED, '{"code":"ERR","message":"m","extra":1,"extra":2}'),
        (ResultErrorSpec(ResultSelector(("error",)), ("code",)), '{"code":"ERR","message":"a","message":"b"}'),
    ],
    ids=["other-member", "message-outside-the-contract"],
)
async def test_any_repeated_member_of_a_found_embedded_error_leaves_its_command_unknown(
    spec: ResultErrorSpec, error: str
) -> None:
    """A found error that repeats any member, read by the contract or not, is unreadable: no failure is concluded."""
    create = Request.bare("entity.add", {"fields": {"TITLE": "x"}}, UNSAFE, result_error=spec)
    direct = ResponderTransport(lambda _request: _raw(f'{{"result":{{"error":{error}}}}}'.encode()))
    async with client_for(direct, policy=POLICY) as api:
        with pytest.raises(AmbiguousExecutionError) as raised:
            await api.call(create)
    assert isinstance(raised.value.__cause__, ProtocolError)

    def answer(request: Request) -> object:
        raw = f'{{"result":{{"result":{{"{_key(request)}":{{"error":{error}}}}},"result_error":[]}}}}'
        return _raw(raw.encode())

    classic = ResponderTransport(answer)
    outcomes, _report = await _batch_outcomes(classic, [Command(create, "x")])
    assert isinstance(outcomes[0], CommandOutcomeUnknown)
    assert (len(direct.requests), len(classic.requests)) == (1, 1)


@pytest.mark.asyncio
async def test_a_repeat_on_the_path_to_an_embedded_error_description_raises_ambiguity() -> None:
    """A description reached through an object that repeats its name is unreadable, as a repeated code is."""
    spec = ResultErrorSpec(ResultSelector(("error",)), ("code",), ("message", "text"))
    create = Request.bare("entity.add", {"fields": {"TITLE": "x"}}, UNSAFE, result_error=spec)
    body = b'{"result":{"error":{"code":"ERR","message":{"text":"a","text":"b"}}}}'
    transport = ResponderTransport(lambda _request: _raw(body) if len(transport.requests) == 1 else {"result": 1})
    async with client_for(transport, policy=POLICY) as api:
        with pytest.raises(AmbiguousExecutionError) as raised:
            await api.call(create)

    assert len(transport.requests) == 1
    assert isinstance(raised.value.__cause__, ProtocolError)


@pytest.mark.asyncio
async def test_a_repeat_hiding_an_embedded_error_behind_a_list_index_raises_ambiguity() -> None:
    """A selector through a list index is checked as any other: the repeated ``error`` there hides no failure."""
    spec = ResultErrorSpec(ResultSelector(("items", 0, "error")), ("code",))
    create = Request.bare("entity.add", {"fields": {"TITLE": "x"}}, UNSAFE, result_error=spec)
    body = b'{"result":{"items":[{"error":{"code":"ERR"},"error":null}]}}'
    transport = ResponderTransport(lambda _request: _raw(body) if len(transport.requests) == 1 else {"result": 1})
    async with client_for(transport, policy=POLICY) as api:
        with pytest.raises(AmbiguousExecutionError):
            await api.call(create)

    assert len(transport.requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("body", HIDDEN_ERRORS, ids=HIDDEN_IDS)
async def test_a_repeat_hiding_an_embedded_error_leaves_a_batch_command_unknown(body: bytes) -> None:
    """In a classic and a REST 3.0 batch alike, a repeat on the way to an embedded error makes the command unknown."""
    create = Request.bare("entity.add", {"fields": {"TITLE": "x"}}, UNSAFE, result_error=EMBEDDED)
    result = body.decode()[len('{"result":') : -1]

    def answer(request: Request) -> object:
        key = _key(request)
        return _raw(f'{{"result":{{"result":{{"{key}":{result}}},"result_error":[]}}}}'.encode())

    classic = ResponderTransport(answer)
    outcomes, _report = await _batch_outcomes(classic, [Command(create, "x")])
    assert isinstance(outcomes[0], CommandOutcomeUnknown)
    assert len(classic.requests) == 1

    requests = (Request("x.item.add", {"id": 1}, UNSAFE, route=RouteKind.API_V3, result_error=EMBEDDED),)
    exchange = ScriptedExchange.v3_batch(requests, payload={})
    v3 = ScriptedTransport((replace(exchange, response=WireResponse(200, JSON, f'{{"result":[{result}]}}'.encode())),))
    async with client_for(v3, policy=POLICY) as api:
        v3_outcomes, _report = await _drain(api.batch_outcomes([Command(requests[0], "x")]))
    assert isinstance(v3_outcomes[0], CommandOutcomeUnknown)
    v3.assert_exhausted()


@pytest.mark.asyncio
async def test_repeated_code_of_an_embedded_error_in_a_classic_batch_leaves_the_command_unknown() -> None:
    """A classic batch command whose embedded error repeats ``code`` is unknown: no refusal is read, nothing resent."""
    create = Request.bare("entity.add", {"fields": {"TITLE": "x"}}, UNSAFE, result_error=EMBEDDED)

    def answer(request: Request) -> object:
        if len(transport.requests) > 1:
            return _batch_ok(request)
        key = _key(request)
        raw = (
            f'{{"result":{{"result":{{"{key}":'
            '{"error":{"code":"INTERNAL_SERVER_ERROR","code":"QUERY_LIMIT_EXCEEDED"}}},"result_error":[]}}'
        )
        return _raw(raw.encode())

    transport = ResponderTransport(answer)
    outcomes, report = await _batch_outcomes(transport, [Command(create, "x")])

    assert len(transport.requests) == 1
    assert isinstance(outcomes[0], CommandOutcomeUnknown)
    assert report.unknown == 1


@pytest.mark.asyncio
async def test_listed_refusal_code_in_an_embedded_error_of_a_classic_batch_command_replays_it() -> None:
    """A refusal code the command's own contract finds in its result proves it did not run: it is replayed."""
    create = Request.bare("entity.add", {"fields": {"TITLE": "x"}}, UNSAFE, result_error=EMBEDDED)

    def answer(request: Request) -> object:
        if len(transport.requests) > 1:
            return _batch_ok(request)
        refused = {"error": {"code": "QUERY_LIMIT_EXCEEDED", "message": "Too many requests"}}
        return {"result": {"result": {_key(request): refused}, "result_error": []}}

    transport = ResponderTransport(answer)
    outcomes, report = await _batch_outcomes(transport, [Command(create, "x")])

    assert len(transport.requests) == TWO
    assert isinstance(outcomes[0], CommandSuccess)
    assert (report.failures, report.unknown) == (0, 0)


@pytest.mark.asyncio
async def test_repeated_code_of_an_embedded_error_in_a_v3_batch_leaves_only_that_command_unknown() -> None:
    """A REST 3.0 element whose error repeats ``code`` is unknown and not isolated; its neighbour still succeeds."""
    requests = tuple(
        Request("x.item.add", {"id": index}, UNSAFE, route=RouteKind.API_V3, result_error=EMBEDDED) for index in (1, 2)
    )
    body = b'{"result":[{"error":{"code":"INTERNAL","code":"QUERY_LIMIT_EXCEEDED","message":"m"}},{"item":{"id":2}}]}'
    exchange = ScriptedExchange.v3_batch(requests, payload={})
    transport = ScriptedTransport((replace(exchange, response=WireResponse(200, JSON, body)),))
    async with client_for(transport, policy=POLICY) as api:
        outcomes, report = await _drain(api.batch_outcomes([Command(request, "x") for request in requests]))

    assert isinstance(outcomes[0], CommandOutcomeUnknown)
    assert isinstance(outcomes[1], CommandSuccess)
    assert (report.physical_requests, report.unknown) == (1, 1)
    transport.assert_exhausted()


# A REST 3.0 envelope refused with a body that cannot be read (B1) ----------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "body"),
    [
        (403, b"<html>403</html>"),
        (403, b'{"error":{"code":[],"message":"m"}}'),
        (400, b'{"result":[{"item":{"id":1}}],"error":{"code":"BAD","message":"m"}}'),
        (400, b'{"error":{"code":"BAD"'),
        (429, b'{"error":{"code":[],"message":"m"}}'),
    ],
    ids=["html", "malformed-error", "result-and-error", "truncated", "malformed-error-at-a-refusal-status"],
)
async def test_unreadable_refusal_of_a_v3_envelope_leaves_its_creations_unknown(status: int, body: bytes) -> None:
    """A REST 3.0 envelope may abort after its first commands ran: only a 401 answers them all, readable or not.

    A body the server wrote proves no refusal even at a refusal status: only a gateway's page there does.
    """
    requests = tuple(Request("x.item.add", {"id": index}, UNSAFE, route=RouteKind.API_V3) for index in (1, 2))
    exchange = ScriptedExchange.v3_batch(requests, payload={}, status=status)
    transport = ScriptedTransport((replace(exchange, response=WireResponse(status, JSON, body)),))
    async with client_for(transport, policy=POLICY) as api:
        outcomes, report = await _drain(api.batch_outcomes([Command(request, "x") for request in requests]))

    assert all(isinstance(outcome, CommandOutcomeUnknown) for outcome in outcomes)
    assert (report.physical_requests, report.unknown) == (1, TWO)
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_unreadable_credential_refusal_of_a_v3_envelope_fails_each_command_without_isolation() -> None:
    """A 401 answers every command of a REST 3.0 envelope whatever its body: no command could pass it."""
    requests = tuple(Request("x.item.get", {"id": index}, SAFE, route=RouteKind.API_V3) for index in (1, 2))
    exchange = ScriptedExchange.v3_batch(requests, payload={}, status=401)
    transport = ScriptedTransport((replace(exchange, response=WireResponse(401, JSON, b"<html>401</html>")),))
    async with client_for(transport, policy=POLICY) as api:
        outcomes, report = await _drain(api.batch_outcomes([Command(request, "x") for request in requests]))

    assert all(isinstance(outcome, CommandFailure) for outcome in outcomes)
    assert (report.physical_requests, report.batch_isolation_requests) == (1, 0)
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a_credential_refusal_of_a_v3_envelope_is_not_resent_whatever_its_code() -> None:
    """A REST 3.0 envelope's 401 answers it, even naming a refusal code: its creations are not sent again."""
    requests = tuple(Request("x.item.add", {"id": index}, UNSAFE, route=RouteKind.API_V3) for index in (1, 2))
    exchange = ScriptedExchange.v3_batch(requests, payload={"error": "QUERY_LIMIT_EXCEEDED"}, status=401)
    transport = ScriptedTransport((exchange,))
    async with client_for(transport, policy=POLICY) as api:
        outcomes, report = await _drain(api.batch_outcomes([Command(request, "x") for request in requests]))

    assert all(isinstance(outcome, CommandFailure) for outcome in outcomes)
    assert report.physical_requests == 1
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_contradictory_body_at_a_refusal_status_is_not_a_refusal() -> None:
    """A rate-limit status whose body also carries a result is not the gateway's bare refusal: nothing is resent."""
    body = b'{"result":42,"error":"QUERY_LIMIT_EXCEEDED"}'
    transport = ResponderTransport(lambda _request: _raw(body, 429) if len(transport.requests) == 1 else {"result": 1})
    async with client_for(transport, policy=POLICY) as api:
        with pytest.raises(ProtocolError):
            await api.call(CREATE)

    assert len(transport.requests) == 1


# Contradictory or repeated control members (R1, R2) -----------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("dispatch", [DirectDispatch(), BatchDispatch()], ids=["direct", "batch"])
async def test_result_beside_a_refusal_code_proves_nothing_and_is_never_replayed(
    dispatch: DirectDispatch | BatchDispatch,
) -> None:
    """A body that is both a result and a quota refusal answers nothing: the creation is unknown, sent once."""
    contradiction = {"result": 42, "error": "QUERY_LIMIT_EXCEEDED"}

    def answer(request: Request) -> object:
        if len(transport.requests) > 1:
            return {"result": 43} if request.method != "batch" else _batch_ok(request)
        if request.method == "batch":
            return {"result": {"result": {_key(request): 42}, "result_error": []}, "error": "QUERY_LIMIT_EXCEEDED"}
        return contradiction

    transport = ResponderTransport(answer)
    outcome = await _fan_out(transport, Command(CREATE, "x"), dispatch=dispatch)

    assert len(transport.requests) == 1
    assert isinstance(outcome, CommandOutcomeUnknown)
    assert isinstance(outcome.error.__cause__, ProtocolError)


@pytest.mark.asyncio
async def test_call_raises_ambiguity_for_a_result_beside_an_error() -> None:
    """``call()`` reports the same contradiction as the outcomes do: the request may have run."""
    transport = ResponderTransport(lambda _request: {"result": 42, "error": "QUERY_LIMIT_EXCEEDED"})
    async with client_for(transport, policy=POLICY) as api:
        with pytest.raises(AmbiguousExecutionError) as raised:
            await api.call(CREATE)

    assert len(transport.requests) == 1
    assert isinstance(raised.value.__cause__, ProtocolError)


@pytest.mark.asyncio
async def test_repeated_command_key_in_result_error_never_proves_a_refusal() -> None:
    """A repeated command key keeps two answers where one is read: the command is unknown, never replayed."""

    def answer(request: Request) -> object:
        if len(transport.requests) > 1:
            return _batch_ok(request)
        key = _key(request)
        raw = (
            '{"result":{"result":{},"result_error":{'
            f'"{key}":{{"error":"INTERNAL_SERVER_ERROR"}},"{key}":{{"error":"QUERY_LIMIT_EXCEEDED"}}'
            "}}}"
        )
        return _raw(raw.encode())

    transport = ResponderTransport(answer)
    outcomes, report = await _batch_outcomes(transport, [Command(CREATE, "x")])

    assert len(transport.requests) == 1
    assert isinstance(outcomes[0], CommandOutcomeUnknown)
    assert isinstance(outcomes[0].error.__cause__, ProtocolError)
    assert report.unknown == 1


@pytest.mark.asyncio
async def test_repeated_top_level_error_never_proves_a_refusal() -> None:
    """An error body that repeats ``error`` is not read as its last code: a 503 that may have run stays unknown."""
    body = b'{"error":"INTERNAL_SERVER_ERROR","error":"QUERY_LIMIT_EXCEEDED"}'
    transport = ResponderTransport(lambda _request: _raw(body, 503) if len(transport.requests) == 1 else {"result": 1})
    async with client_for(transport, policy=POLICY) as api:
        with pytest.raises(AmbiguousExecutionError):
            await api.call(CREATE)

    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_repeated_member_in_result_data_is_still_tolerated() -> None:
    """Only control members must be unique: a result's own data keeps the last value of a repeated name."""
    transport = ResponderTransport(lambda _request: _raw(b'{"result":{"TITLE":"a","TITLE":"b"}}'))
    async with client_for(transport, policy=POLICY) as api:
        assert await api.call(READ) == {"TITLE": "b"}


# A malformed result_error entry (R3) ----------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", [None, [], {}, {"error": []}], ids=["null", "list", "empty", "list-code"])
async def test_malformed_error_entry_is_unknown_not_a_server_failure(entry: object) -> None:
    """An entry that names no error is not the server's answer: the sent creation is unknown."""
    transport = ResponderTransport(lambda request: {"result": {"result": {}, "result_error": {_key(request): entry}}})
    outcomes, report = await _batch_outcomes(transport, [Command(CREATE, "x")])

    assert isinstance(outcomes[0], CommandOutcomeUnknown)
    assert isinstance(outcomes[0].error.__cause__, ProtocolError)
    assert (report.failures, report.unknown) == (0, 1)


@pytest.mark.asyncio
async def test_malformed_error_entry_does_not_prove_the_fail_fast_halt() -> None:
    """Under fail-fast the server halts after an error it answered; an entry naming none proves no halt."""

    def answer(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        first = next(iter(commands))
        return {"result": {"result": {}, "result_error": {first: {}}}}

    transport = ResponderTransport(answer)
    async with client_for(transport, policy=POLICY) as api:
        stream = api.batch([Command(CREATE, "a"), Command(CREATE, "b")])
        with pytest.raises(BatchFailed) as raised:
            await _drain(stream)

    outcomes = raised.value.outcomes
    assert [type(outcome) for outcome in outcomes] == [CommandOutcomeUnknown, CommandOutcomeUnknown]


# A body nested deeper than the decoder follows (R4, T-3) ------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("dispatch", [DirectDispatch(), BatchDispatch()], ids=["direct", "batch"])
async def test_too_deep_answer_after_dispatch_is_unknown(dispatch: DirectDispatch | BatchDispatch) -> None:
    """The decoder's depth limit is a malformed answer: a sent creation is unknown, never a failure."""
    transport = ResponderTransport(lambda _request: _raw(DEEP))
    outcome = await _fan_out(transport, Command(CREATE, "x"), dispatch=dispatch)

    assert len(transport.requests) == 1
    assert isinstance(outcome, CommandOutcomeUnknown)


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [DEEP, DEEP_ERROR], ids=["result", "error"])
@pytest.mark.parametrize(
    ("status", "raised"),
    [(429, None), (500, AmbiguousExecutionError), (400, HTTPGatewayError)],
    ids=["refusal", "ambiguous", "answered"],
)
async def test_too_deep_answer_outside_2xx_is_read_by_its_status(
    body: bytes, status: int, raised: type[Exception] | None
) -> None:
    """A too-deep body is malformed at any status, its preview included: a gateway's page the status decides."""
    transport = ResponderTransport(
        lambda _request: _raw(body, status) if len(transport.requests) == 1 else {"result": CREATED}
    )
    async with client_for(transport, policy=POLICY) as api:
        if raised is None:
            assert await api.call(CREATE) == CREATED
        else:
            with pytest.raises(raised):
                await api.call(CREATE)

    assert len(transport.requests) == (TWO if raised is None else 1)


def _nested(kind: str, levels: int) -> str:
    """A JSON value whose scalar sits ``levels`` levels below it, through arrays or objects."""
    return ("[" * levels + "0" + "]" * levels) if kind == "array" else ('{"a":' * levels + "0" + "}" * levels)


@pytest.mark.parametrize("kind", ["array", "object"])
def test_the_decoder_refuses_a_member_deeper_than_it_holds_once_frozen_on_any_interpreter(kind: str) -> None:
    """The limit is the decoder's own, not the parser's, and sits where a frozen member's does, nothing earlier."""
    # Shallow siblings lift the body's openers past the limit, so the depth is walked, not skipped by the count.
    padding = ",".join(["{}"] * MAX_JSON_DEPTH)
    deepest = decode_json(f'{{"result":{_nested(kind, MAX_JSON_DEPTH)},"padding":[{padding}]}}')
    assert isinstance(deepest, dict)
    assert Response(deepest["result"]).result is not None
    with pytest.raises(ValueError, match="supported depth"):
        decode_json(f'{{"result":{_nested(kind, MAX_JSON_DEPTH + 1)}}}')


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["array", "object"])
async def test_a_body_deeper_than_the_supported_depth_is_a_malformed_answer(kind: str) -> None:
    """Past the limit a 2xx body is malformed, as any other: a read raises the decoder's failure."""
    body = f'{{"result":{_nested(kind, MAX_JSON_DEPTH + 1)}}}'.encode()
    transport = ResponderTransport(lambda _request: _raw(body))
    async with client_for(transport, policy=POLICY) as api:
        with pytest.raises(EnvelopeContractError, match="Malformed"):
            await api.call(READ)


@pytest.mark.asyncio
async def test_a_refusal_code_in_a_body_too_deep_to_read_proves_nothing() -> None:
    """A refusal code beside a member too deep to read is not read: at 503 the creation may have run, sent once."""
    body = f'{{"error":"QUERY_LIMIT_EXCEEDED","detail":{_nested("object", MAX_JSON_DEPTH + 1)}}}'.encode()
    transport = ResponderTransport(lambda _request: _raw(body, 503) if len(transport.requests) == 1 else {"result": 1})
    async with client_for(transport, policy=POLICY) as api:
        with pytest.raises(AmbiguousExecutionError):
            await api.call(CREATE)

    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_too_deep_server_error_ends_the_commands_of_a_tolerant_batch_not_the_batch() -> None:
    """A too-deep 500 on a classic envelope leaves its commands unknown; the stream itself does not fail."""
    transport = ResponderTransport(lambda _request: _raw(DEEP_ERROR, 500))
    outcomes, report = await _batch_outcomes(transport, [Command(CREATE, "a"), Command(CREATE, "b")])

    assert all(isinstance(outcome, CommandOutcomeUnknown) for outcome in outcomes)
    assert (report.unknown, report.physical_requests) == (TWO, 1)


@pytest.mark.asyncio
async def test_too_deep_answer_ends_one_command_of_a_tolerant_batch_not_the_batch() -> None:
    """A tolerant batch reports the commands of a too-deep envelope unknown and goes on."""
    transport = ResponderTransport(lambda _request: _raw(DEEP))
    outcomes, report = await _batch_outcomes(transport, [Command(CREATE, "a"), Command(READ, "b")])

    assert all(isinstance(outcome, CommandOutcomeUnknown) for outcome in outcomes)
    assert report.unknown == TWO


@pytest.mark.asyncio
@pytest.mark.parametrize("dispatch", [DirectDispatch(), BatchDispatch()], ids=["direct", "batch"])
async def test_a_leading_byte_order_mark_is_ignored(dispatch: DirectDispatch | BatchDispatch) -> None:
    """RFC 8259 lets a parser ignore a byte order mark: an answer led by one is read, not malformed (T-14)."""

    def answer(request: Request) -> WireResponse:
        if isinstance(dispatch, DirectDispatch):
            return _raw("\ufeff".encode() + json.dumps({"result": CREATED}).encode())
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        body = {"result": {"result": dict.fromkeys(commands, CREATED), "result_error": []}}
        return _raw("\ufeff".encode() + json.dumps(body).encode())

    transport = ResponderTransport(answer)
    outcome = await _fan_out(transport, Command(CREATE, "x"), dispatch=dispatch)

    assert isinstance(outcome, CommandSuccess)
    assert outcome.response.result == CREATED


@pytest.mark.asyncio
async def test_only_a_leading_byte_order_mark_is_ignored() -> None:
    """A byte order mark inside the answer's data is data: it is kept as the server sent it."""
    transport = ResponderTransport(lambda _request: _raw('{"result":"a\ufeffb"}'.encode()))
    async with client_for(transport, policy=POLICY) as api:
        assert await api.call(READ) == "a\ufeffb"


def test_a_failure_carried_in_another_keeps_its_execution_facts() -> None:
    """Wrapping a foreign failure for an outcome keeps what was recorded about whether its command ran."""
    foreign = RuntimeError("caller code failed after the send")
    record_dispatch(foreign, possibly_executed=True)

    carried = outcome_error(foreign, "fan-out command failed")

    assert carried.__cause__ is foreign
    assert execution(carried) is Execution.POSSIBLY_RUN
    assert settlement(carried) is CommandSettlement.UNKNOWN


# One verdict for direct calls (T-4, T-9, K-5) -----------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [b'{"result":', b"{}", b'{"result":1,"total":"7"}', b'{"result":1,"next":true}'],
    ids=["truncated", "no-result", "total-string", "next-bool"],
)
async def test_call_of_a_creation_answered_unreadably_raises_ambiguity(body: bytes) -> None:
    """A 2xx answer ``call()`` cannot decode proves nothing: a creation raises ``AmbiguousExecutionError``."""
    transport = ResponderTransport(lambda _request: _raw(body))
    async with client_for(transport, policy=POLICY) as api:
        with pytest.raises(AmbiguousExecutionError) as raised:
            await api.call(CREATE)

    assert isinstance(raised.value.__cause__, ProtocolError)
    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_call_of_a_read_answered_unreadably_raises_the_decoder_failure() -> None:
    """A ``SAFE`` read is never ambiguous to its caller: it raises what made the answer unreadable."""
    transport = ResponderTransport(lambda _request: _raw(b"{}"))
    async with client_for(transport, policy=POLICY) as api:
        with pytest.raises(EnvelopeContractError):
            await api.call(READ)


@pytest.mark.asyncio
async def test_readable_server_timing_reaches_the_response() -> None:
    """A readable ``time`` block is kept whole, the method's operating time and its reset included."""
    timing = ResponseTime(
        start=1.0,
        finish=1.5,
        duration=0.5,
        processing=0.25,
        date_start="2026-11-18T10:00:00+03:00",
        date_finish="2026-11-18T10:00:01+03:00",
        operating_reset_at=1763450400.0,
        operating=2.5,
    )
    transport = ResponderTransport(lambda _request: {"result": 42, "time": asdict(timing)})
    async with client_for(transport, policy=POLICY) as api:
        response = await api.call_response(CREATE)

    assert response.time == timing


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "timing",
    [
        {"start": 2.0, "finish": 1.0, "duration": -1.0, "processing": 0.0},
        [],
        "x",
        {"start": "1.0", "finish": "1.5", "duration": "0.5", "processing": "0.25"},
        {"start": 10**400, "finish": 1.5, "duration": 0.5, "processing": 0.25},
    ],
    ids=["negative-duration", "list", "string", "numbers-as-strings", "integer-beyond-float"],
)
async def test_unreadable_server_timing_keeps_the_answer(timing: object) -> None:
    """The server's timing is diagnostic: a block that cannot be read is dropped, never the result with it."""
    transport = ResponderTransport(lambda _request: {"result": 42, "time": timing})
    async with client_for(transport, policy=POLICY) as api:
        response = await api.call_response(CREATE)

    assert (response.result, response.time) == (42, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["INTERNAL_SERVER_ERROR", "BITRIX_REST_V3_EXCEPTION_LIMITER_QUERYTIMELIMITEXCEPTION"])
async def test_structured_server_error_status_leaves_a_creation_unknown(code: str) -> None:
    """A 5xx proves nothing about whether the method ran, structured or not, unless its code is a listed refusal."""
    transport = ResponderTransport(lambda _request: _json({"error": code}, 503))
    async with client_for(transport, policy=POLICY) as api:
        with pytest.raises(AmbiguousExecutionError) as raised:
            await api.call(CREATE)

    assert isinstance(raised.value.__cause__, ApiResponseError)
    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_listed_refusal_code_on_a_server_error_status_is_still_retried() -> None:
    """The portal's rate limit (HTTP 503 ``QUERY_LIMIT_EXCEEDED``) proves the creation did not run: it is sent again."""
    transport = ResponderTransport(
        lambda _request: (
            _json({"error": "QUERY_LIMIT_EXCEEDED"}, 503) if len(transport.requests) == 1 else {"result": CREATED}
        )
    )
    async with client_for(transport, policy=POLICY) as api:
        assert await api.call(CREATE) == CREATED

    assert len(transport.requests) == TWO


@pytest.mark.asyncio
async def test_request_level_refusal_stays_a_failure() -> None:
    """A structured 4xx answers the request: it is a failure, raised as the server's error."""
    transport = ResponderTransport(lambda _request: _json({"error": "expired_token"}, 401))
    async with client_for(transport, policy=POLICY) as api:
        with pytest.raises(ApiResponseError):
            await api.call(CREATE)


# One verdict for batch envelopes (P-2, P-4) --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_structured_server_error_on_a_classic_envelope_leaves_its_commands_unknown() -> None:
    """A 500 on a classic envelope may come after some commands ran: none is a failure, none is replayed."""
    transport = ResponderTransport(lambda _request: _json({"error": "INTERNAL_SERVER_ERROR"}, 500))
    outcomes, report = await _batch_outcomes(transport, [Command(CREATE, "a"), Command(CREATE, "b")])

    assert all(isinstance(outcome, CommandOutcomeUnknown) for outcome in outcomes)
    assert (report.failures, report.unknown, report.physical_requests) == (0, TWO, 1)


@pytest.mark.asyncio
async def test_rate_limit_on_a_classic_envelope_is_a_refusal_replayed_whole() -> None:
    """The classic envelope is limited as one request: its 503 ``QUERY_LIMIT_EXCEEDED`` is replayed whatever safety."""
    transport = ResponderTransport(
        lambda request: (
            _json({"error": "QUERY_LIMIT_EXCEEDED"}, 503) if len(transport.requests) == 1 else _batch_ok(request)
        )
    )
    outcomes, report = await _batch_outcomes(transport, [Command(CREATE, "a"), Command(CREATE, "b")])

    assert all(isinstance(outcome, CommandSuccess) for outcome in outcomes)
    assert report.physical_requests == TWO


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "code"), [(401, "expired_token"), (401, "invalid_token")])
async def test_request_level_refusal_of_a_v3_envelope_fails_each_command_without_isolation(
    status: int, code: str
) -> None:
    """A REST v3 envelope refused for its credentials is answered whole, as a classic one is: no isolation.

    Any other structured error may name one command that aborted the envelope after others ran, so it isolates.
    """
    requests = tuple(Request("x.item.get", {"id": index}, SAFE, route=RouteKind.API_V3) for index in (1, 2, 3))
    transport = ScriptedTransport((ScriptedExchange.v3_batch(requests, payload={"error": code}, status=status),))
    async with client_for(transport, policy=POLICY) as api:
        outcomes, report = await _drain(api.batch_outcomes([Command(request, "x") for request in requests]))

    assert all(isinstance(outcome, CommandFailure) for outcome in outcomes)
    assert all(isinstance(outcome.error, ApiResponseError) for outcome in outcomes)  # type: ignore[union-attr]
    assert (report.physical_requests, report.batch_isolation_requests, report.ambiguous_physical_attempts) == (1, 0, 0)
    transport.assert_exhausted()


# A send its transport refused for good before it left (T-14) -------------------------------------------------


@pytest.mark.asyncio
async def test_request_the_transport_could_not_build_is_not_sent() -> None:
    """A non-retryable failure of phase ``not_dispatched`` never left: no physical request, settled not executed."""
    unbuildable = TransportError("HTTP request could not be built", phase=FailurePhase.NOT_DISPATCHED, retryable=False)
    transport = ResponderTransport(lambda _request: unbuildable)
    async with client_for(transport, policy=POLICY) as api:
        outcomes, report = await _drain(api.fan_out_outcomes([Command(CREATE, "x")], dispatch=DirectDispatch()))
        with pytest.raises(TransportError) as raised:
            await api.call(CREATE)

    assert isinstance(outcomes[0], CommandFailure)
    assert report.physical_requests == 0
    assert execution(raised.value) is Execution.NOT_SENT
    assert settlement(raised.value) is CommandSettlement.NOT_EXECUTED


@pytest.mark.asyncio
async def test_refused_connection_is_still_a_send_retried() -> None:
    """A retryable failure before dispatch (a refused connection) is a send, retried whatever the safety."""
    refused = TransportError("connection refused", phase=FailurePhase.NOT_DISPATCHED)
    transport = ResponderTransport(lambda _request: refused if len(transport.requests) == 1 else {"result": CREATED})
    async with client_for(transport, policy=POLICY) as api:
        assert await api.call(CREATE) == CREATED

    assert len(transport.requests) == TWO


# A spent retry budget names what it retried (K-10) -------------------------------------------------------------


@pytest.mark.asyncio
async def test_spent_retry_budget_names_its_request_and_its_cause() -> None:
    """The budget that ends a retried request names the request, and the CLI rendering shows the cause."""
    transport = ResponderTransport(lambda _request: _json({"error": "QUERY_LIMIT_EXCEEDED"}, 503))
    async with client_for(transport, policy=POLICY) as api:
        with pytest.raises(BudgetExceededError) as raised:
            await api.call(READ)

    assert raised.value.request_summary == READ.summary
    rendered = _safe_error(raised.value)
    assert rendered["request"] == READ.summary.to_dict()
    cause = rendered["cause"]
    assert isinstance(cause, dict)
    assert (cause["type"], cause["code"], cause["evidence"]["http_status"]) == (
        "ApiResponseError",
        "query_limit_exceeded",
        503,
    )


# A binding's unknown mark (P-11) -------------------------------------------------------------------------------


def test_binding_unknown_mark_survives_a_later_conclusive_failure() -> None:
    """A binding with an unknown page stays unknown when a later page fails conclusively."""
    binding = ReferenceCompletionRecorder().admit(0)
    binding.scheduled()
    binding.settled(CommandSettlement.UNKNOWN)
    binding.scheduled()
    binding.settled(CommandSettlement.FAILURE)

    assert binding.unknown
