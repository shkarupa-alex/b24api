"""Native REST v3 batch (#21): wire, fixtures, attribution, abort recovery, isolation and fail-fast (A21-*)."""

from __future__ import annotations
import asyncio
import inspect
import json
import time
from typing import TYPE_CHECKING

import httpx
import pytest

from b24api import BatchFailed, ExecutionPolicy, ReplaySafety, Request, RetryPolicy, RouteKind
from b24api.batch.engine import BatchExecutor
from b24api.batch.outcome import BatchFailure, BatchSuccess
from b24api.contracts import (
    Command,
    CommandFailure,
    CommandNotExecuted,
    CommandOutcomeUnknown,
    CommandSuccess,
    NotExecutedReason,
    ResultErrorSpec,
    ResultSelector,
)
from b24api.contracts.report import OperationReport, TerminalState
from b24api.errors import (
    AmbiguousExecutionError,
    ApiResponseError,
    BatchCommandError,
    CapabilityError,
    ProtocolError,
    ResultShapeError,
)
from b24api.execution import Executor, RateCoordinator
from b24api.testing import ScriptedCall, ScriptedExchange, ScriptedTransport, ScriptedV3Command
from b24api.transport import HttpxTransport, TransportCapabilities, WireRequest, WireResponse
from tests.scripting import ResponderTransport, client_for

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from b24api.contracts import CommandOutcome, OperationStream

SAFE = ReplaySafety.SAFE
UNSAFE = ReplaySafety.UNSAFE
UNKNOWN = ReplaySafety.UNKNOWN
NO_DELAY = RetryPolicy(initial_delay=0, maximum_delay=0, jitter=0)
JSON_HEADERS = (("content-type", "application/json"),)
ERROR_SPEC = ResultErrorSpec(ResultSelector(("error",)), ("code",), ("message",))
TWO = 2
THREE = 3
FOUR = 4
BAD_REQUEST = 400
COOLDOWN_FLOOR = 0.04


def _v3(
    identifier: int,
    *,
    method: str = "x.item.get",
    safety: ReplaySafety = SAFE,
    result_error: ResultErrorSpec | None = None,
) -> Request:
    return Request(method, {"id": identifier}, safety, route=RouteKind.API_V3, result_error=result_error)


def _item(identifier: int) -> dict[str, object]:
    return {"item": {"id": identifier}}


def _abort(code: str, *, message: str = "synthetic rejection") -> dict[str, object]:
    return {"error": {"code": code, "message": message}}


def _policy(**overrides: object) -> ExecutionPolicy:
    retry = overrides.pop("retry", NO_DELAY)
    return ExecutionPolicy(retry=retry, **overrides)  # type: ignore[arg-type]


def _direct(request: Request, payload: object, *, status: int = 200) -> ScriptedExchange:
    body = json.dumps(payload, separators=(",", ":")).encode()
    return ScriptedExchange(request, WireResponse(status, JSON_HEADERS, body))


async def _run(
    transport: ScriptedTransport | ResponderTransport,
    requests: Sequence[Request],
    *,
    policy: ExecutionPolicy | None = None,
    batch_size: int | None = None,
) -> tuple[list[CommandOutcome[int]], OperationReport]:
    async with client_for(transport, policy=policy or _policy()) as api:
        stream = api.batch_outcomes(
            [Command(request, index) for index, request in enumerate(requests)], batch_size=batch_size
        )
        async with stream:
            outcomes = [outcome async for outcome in stream]
    report = stream.report
    assert report is not None
    return outcomes, report


async def _drain(
    stream: OperationStream[CommandSuccess[int]],
) -> tuple[list[CommandSuccess[int]], BatchFailed[int] | None]:
    delivered: list[CommandSuccess[int]] = []
    try:
        async with stream:
            async for outcome in stream:
                delivered.append(outcome)  # noqa: PERF401 - keep the prefix delivered before the failure
    except BatchFailed as error:
        return delivered, error
    return delivered, None


def _v3_calls(transport: ScriptedTransport) -> list[ScriptedCall]:
    return [call for call in transport.wire_calls if call.v3_wire is not None]


def _wire(*requests: Request) -> tuple[ScriptedV3Command, ...]:
    return tuple(ScriptedV3Command(request.method, request.copy_parameters()) for request in requests)


# A21-WIRE --------------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a21_wire_four_safe_v3_reads_travel_in_one_envelope_in_input_order() -> None:
    """A21-WIRE: four SAFE v3 commands are one native envelope, one physical request, input-ordered results."""
    requests = [_v3(identifier) for identifier in range(1, 5)]
    transport = ScriptedTransport(
        (ScriptedExchange.v3_batch(tuple(requests), tuple(_item(i) for i in range(1, 5))),),
    )

    outcomes, report = await _run(transport, requests)

    assert [type(outcome) for outcome in outcomes] == [CommandSuccess] * FOUR
    assert [outcome.result for outcome in outcomes if isinstance(outcome, CommandSuccess)] == [
        _item(i) for i in range(1, 5)
    ]
    assert [outcome.correlation for outcome in outcomes] == [0, 1, 2, 3]
    assert report.state is TerminalState.COMPLETED
    assert report.physical_requests == 1
    assert report.batch_requests == 1
    assert report.batch_commands == FOUR
    assert report.ambiguous_physical_attempts == 0
    assert report.batch_isolation_requests == 0
    # The envelope has no Request form: ``calls`` stays classic-only, ``wire_calls`` records the array.
    assert transport.calls == ()
    assert transport.wire_calls == (ScriptedCall(None, _wire(*requests), SAFE),)
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a21_wire_all_success_unsafe_and_unknown_share_one_non_safe_envelope() -> None:
    """A21-WIRE: UNSAFE and UNKNOWN are compatible; the envelope is UNSAFE and succeeds as one request."""
    requests = [_v3(1, method="x.item.add", safety=UNSAFE), _v3(2, method="x.item.get", safety=UNKNOWN)]
    exchange = ScriptedExchange.v3_batch(tuple(requests), (_item(1), _item(2)))
    assert exchange.request.replay_safety is UNSAFE
    transport = ScriptedTransport((exchange,))

    outcomes, report = await _run(transport, requests)

    assert all(isinstance(outcome, CommandSuccess) for outcome in outcomes)
    assert report.physical_requests == 1
    assert [call.replay_safety for call in transport.wire_calls] == [UNSAFE]
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a21_wire_httpx_posts_a_root_array_to_the_v3_batch_route_once_per_group() -> None:
    """A21-WIRE: httpx sends a root JSON array (no ``cmd``) to ``/rest/api/<user>/<token>/batch`` per group."""
    seen: list[tuple[str, str | None, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append((request.url.path, request.headers.get("content-type"), body))
        assert isinstance(body, list)
        results = [{"item": {"id": entry["query"]["id"]}} for entry in body]
        return httpx.Response(200, json={"result": results}, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = HttpxTransport("https://fixture.invalid/rest/1/token/", client=client)
    requests = [_v3(1), _v3(2), _v3(3, method="x.item.update", safety=UNSAFE)]
    try:
        outcomes, report = await _run(transport, requests)  # type: ignore[arg-type]
    finally:
        await transport.aclose()
        await client.aclose()

    assert all(isinstance(outcome, CommandSuccess) for outcome in outcomes)
    assert report.physical_requests == TWO
    assert seen == [
        (
            "/rest/api/1/token/batch",
            "application/json",
            [{"method": "x.item.get", "query": {"id": 1}}, {"method": "x.item.get", "query": {"id": 2}}],
        ),
        ("/rest/api/1/token/batch", "application/json", [{"method": "x.item.update", "query": {"id": 3}}]),
    ]


def test_a21_wire_envelope_evidence_is_value_free() -> None:
    """A21-WIRE: the envelope summary and repr never carry inner methods, keys or values."""
    exchange = ScriptedExchange.v3_batch((_v3(77, method="x.secret.get"),), (_item(77),))
    text = repr(exchange) + repr(exchange.v3_wire)
    assert "77" not in text
    assert "x.secret.get" in repr(exchange.v3_wire)  # the wire-visible method is fixture data, the query is not
    assert TransportCapabilities().json_array_body is False
    assert HttpxTransport.capabilities.json_array_body is True
    assert ScriptedTransport.capabilities.json_array_body is True
    with pytest.raises(TypeError, match="json_array_body"):
        TransportCapabilities(json_array_body=1)  # type: ignore[arg-type]


# A21-SCRIPTED ----------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a21_scripted_result_error_is_not_wire_visible_and_does_not_affect_matching() -> None:
    """A21-SCRIPTED: equal method/query with different result_error contracts match one fixture."""
    plain = _v3(5)
    contracted = _v3(5, result_error=ERROR_SPEC)
    transport = ScriptedTransport((ScriptedExchange.v3_batch((plain,), (_item(5),)),))

    outcomes, _report = await _run(transport, [contracted])

    assert isinstance(outcomes[0], CommandSuccess)
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a21_scripted_safe_and_unsafe_equal_commands_are_two_envelopes_matched_by_safety() -> None:
    """A21-SCRIPTED: equal method/query in SAFE and UNSAFE groups are matched by each envelope's safety."""
    safe = _v3(9)
    unsafe = _v3(9, safety=UNSAFE)
    transport = ScriptedTransport(
        (
            # Listed in reverse order: only the envelope safety tells the two fixtures apart.
            ScriptedExchange.v3_batch((unsafe,), ({"item": {"id": 9, "from": "unsafe"}},)),
            ScriptedExchange.v3_batch((safe,), ({"item": {"id": 9, "from": "safe"}},)),
        ),
    )

    outcomes, report = await _run(transport, [safe, unsafe])

    results = [outcome.result for outcome in outcomes if isinstance(outcome, CommandSuccess)]
    assert results == [{"item": {"id": 9, "from": "safe"}}, {"item": {"id": 9, "from": "unsafe"}}]
    assert report.physical_requests == TWO
    assert [call.replay_safety for call in transport.wire_calls] == [SAFE, UNSAFE]
    assert [call.v3_wire for call in transport.wire_calls] == [_wire(safe), _wire(unsafe)]
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a21_scripted_classic_and_array_never_match_each_other() -> None:
    """A21-SCRIPTED: a classic call never consumes a v3 fixture, and an array never consumes a classic one."""
    command = _v3(1)
    v3_fixture = ScriptedExchange.v3_batch((command,), (_item(1),))
    transport = ScriptedTransport((v3_fixture,))
    with pytest.raises(AssertionError, match="unexpected scripted request"):
        await transport.send(v3_fixture.request, attempt_timeout=1, max_response_bytes=1024)

    classic = ScriptedTransport((ScriptedExchange.json(v3_fixture.request, {"result": [_item(1)]}),))
    envelope = await _envelope_of(command)
    with pytest.raises(AssertionError, match="unexpected scripted request"):
        await classic.send_wire(envelope, attempt_timeout=1, max_response_bytes=1024)
    assert classic.calls == ()
    assert classic.wire_calls == (ScriptedCall(None, _wire(command), SAFE),)


async def _envelope_of(*requests: Request) -> WireRequest:
    """Build a real v3 envelope through the batch engine's public chunk seam, capturing the sent wire."""
    captured: list[WireRequest] = []

    class _Capture(ScriptedTransport):
        async def send_wire(
            self, request: WireRequest, *, attempt_timeout: float, max_response_bytes: int
        ) -> WireResponse:
            captured.append(request)
            return await super().send_wire(
                request, attempt_timeout=attempt_timeout, max_response_bytes=max_response_bytes
            )

    transport = _Capture((ScriptedExchange.v3_batch(requests, tuple(_item(1) for _ in requests)),))
    engine = BatchExecutor(Executor(transport))
    await engine.execute_requests(requests, context=engine.executor.context(_policy()))
    return captured[0]


def test_a21_scripted_fixture_invariants_fail_closed() -> None:
    """A21-SCRIPTED: descriptor, cardinality, results/payload exclusivity and command representation."""
    command = _v3(1)
    with pytest.raises(ValueError, match="exactly one of results or payload"):
        ScriptedExchange.v3_batch((command,))
    with pytest.raises(ValueError, match="exactly one of results or payload"):
        ScriptedExchange.v3_batch((command,), (_item(1),), payload={"result": []})
    with pytest.raises(ValueError, match="results must match"):
        ScriptedExchange.v3_batch((command, _v3(2)), (_item(1),))
    with pytest.raises(ValueError, match="API_V3 JSON commands"):
        ScriptedExchange.v3_batch((Request("x.item.get", route=RouteKind.BARE),), (_item(1),))
    with pytest.raises(ValueError, match=r"1\.\.50 commands"):
        ScriptedExchange.v3_batch((), ())
    response = WireResponse(200, JSON_HEADERS, b"{}")
    with pytest.raises(ValueError, match="envelope descriptor"):
        ScriptedExchange(command, response, v3_wire=_wire(command))
    with pytest.raises(ValueError, match="ScriptedV3Command"):
        ScriptedExchange(ScriptedExchange.v3_batch((command,), (_item(1),)).request, response, v3_wire=())
    # The query is a frozen canonical copy: later mutation of the source mapping changes nothing.
    source: dict[str, object] = {"id": [1, 2]}
    frozen = ScriptedV3Command("x.item.get", source)  # type: ignore[arg-type]
    source["id"] = [3]
    assert frozen == ScriptedV3Command("x.item.get", {"id": [1, 2]})
    abort = ScriptedExchange.v3_batch((command,), payload=_abort("X"), status=400)
    assert abort.response.status_code == BAD_REQUEST


# A21-PREFLIGHT ---------------------------------------------------------------------------------------------


class _NoArrayScriptedTransport(ScriptedTransport):
    capabilities = TransportCapabilities(positional_json=True, routes=frozenset(RouteKind))


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["send_only", "no_array_body"])
async def test_a21_preflight_unsupported_transport_rejects_v3_commands_before_reservation(kind: str) -> None:
    """A21-PREFLIGHT: send-only or a WireTransport without json_array_body: CapabilityError, zero sends."""
    requests = [_v3(1), _v3(2, safety=UNSAFE)]
    transport: ScriptedTransport | ResponderTransport
    if kind == "send_only":
        transport = ResponderTransport(lambda _request: pytest.fail("a v3 command must not be sent"))
    else:
        transport = _NoArrayScriptedTransport(())

    outcomes, report = await _run(transport, requests)

    assert all(isinstance(outcome, CommandFailure) for outcome in outcomes)
    assert all(
        isinstance(outcome.error, CapabilityError) and "v3 batch" in str(outcome.error)
        for outcome in outcomes
        if isinstance(outcome, CommandFailure)
    )
    assert report.physical_requests == 0
    assert report.batch_requests == 0
    if isinstance(transport, ResponderTransport):
        assert transport.requests == []
    else:
        assert transport.wire_calls == ()


# A21-EMBEDDED ----------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a21_embedded_safe_errors_are_attributed_by_index_without_resending_neighbours() -> None:
    """A21-EMBEDDED: ok + embedded API error + malformed embedded error in one SAFE complete array."""
    requests = [
        _v3(1, result_error=ERROR_SPEC),
        _v3(2, result_error=ERROR_SPEC),
        _v3(3, result_error=ERROR_SPEC),
    ]
    transport = ScriptedTransport(
        (
            ScriptedExchange.v3_batch(
                tuple(requests),
                (_item(1), {"error": {"code": "ITEM_LOCKED", "message": "locked"}}, {"error": ["not", "a", "map"]}),
            ),
        ),
    )

    outcomes, report = await _run(transport, requests)

    assert isinstance(outcomes[0], CommandSuccess)
    assert isinstance(outcomes[1], CommandFailure)
    # The embedded API error keeps the classic batch type and its code.
    assert type(outcomes[1].error) is BatchCommandError
    assert outcomes[1].error.normalized_code == "item_locked"
    assert isinstance(outcomes[2], CommandFailure)
    assert type(outcomes[2].error) is ProtocolError
    assert not isinstance(outcomes[2].error, ResultShapeError)
    assert report.physical_requests == 1
    assert report.ambiguous_physical_attempts == 0
    assert report.batch_isolation_requests == 0
    assert report.state is TerminalState.COMPLETED_WITH_FAILURES
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a21_embedded_unsafe_non_refusal_error_is_not_replayed() -> None:
    """A21-EMBEDDED: an UNSAFE embedded non-refusal failure settles once; its neighbour succeeds."""
    requests = [
        _v3(1, method="x.item.add", safety=UNSAFE, result_error=ERROR_SPEC),
        _v3(2, method="x.item.add", safety=UNSAFE, result_error=ERROR_SPEC),
    ]
    transport = ScriptedTransport(
        (
            ScriptedExchange.v3_batch(
                tuple(requests), (_item(1), {"error": {"code": "QUOTA_FULL", "message": "no space"}})
            ),
        ),
    )

    outcomes, report = await _run(transport, requests)

    assert isinstance(outcomes[0], CommandSuccess)
    assert isinstance(outcomes[1], CommandFailure)
    assert isinstance(outcomes[1].error, BatchCommandError)
    assert report.physical_requests == 1
    assert report.retries == 0
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a21_embedded_safe_transient_is_retried_once_alone_with_inherited_attempts() -> None:
    """A21-EMBEDDED: a SAFE transient embedded error is resent alone in a new envelope, same lineage."""
    first, second = _v3(1, result_error=ERROR_SPEC), _v3(2, result_error=ERROR_SPEC)
    transient = {"error": {"code": "QUERY_LIMIT_EXCEEDED", "message": "slow down"}}
    transport = ScriptedTransport(
        (
            ScriptedExchange.v3_batch((first, second), (_item(1), transient)),
            ScriptedExchange.v3_batch((second,), (_item(2),)),
        ),
    )

    outcomes, report = await _run(transport, [first, second])

    assert all(isinstance(outcome, CommandSuccess) for outcome in outcomes)
    assert [call.v3_wire for call in transport.wire_calls] == [_wire(first, second), _wire(second)]
    assert report.physical_requests == TWO
    assert report.retries == 1
    assert report.ambiguous_physical_attempts == 0
    assert report.batch_isolation_requests == 0
    transport.assert_exhausted()

    # Inherited lineage: with two attempts per request the replay is the last send; no fresh budget follows.
    exhausted = ScriptedTransport(
        (
            ScriptedExchange.v3_batch((first, second), (_item(1), transient)),
            ScriptedExchange.v3_batch((second,), (transient,)),
        ),
    )
    outcomes, report = await _run(exhausted, [first, second], policy=_policy(max_attempts_per_request=TWO))
    assert isinstance(outcomes[1], CommandFailure)
    assert isinstance(outcomes[1].error, BatchCommandError)
    assert report.physical_requests == TWO
    exhausted.assert_exhausted()


# A21-MIXED -------------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a21_mixed_safety_groups_keep_input_order_and_a_non_safe_abort_is_unknown() -> None:
    """A21-MIXED: SAFE / UNSAFE+UNKNOWN / SAFE groups in order; the aborted non-SAFE group is not replayed."""
    safe_head = _v3(1)
    unsafe = _v3(2, method="x.item.add", safety=UNSAFE)
    unknown = _v3(3, method="x.item.touch", safety=UNKNOWN)
    safe_tail = _v3(4)
    transport = ScriptedTransport(
        (
            ScriptedExchange.v3_batch((safe_head,), (_item(1),)),
            ScriptedExchange.v3_batch((unsafe, unknown), payload=_abort("SYNTHETIC_VALIDATION"), status=400),
            ScriptedExchange.v3_batch((safe_tail,), (_item(4),)),
        ),
    )

    outcomes, report = await _run(transport, [safe_head, unsafe, unknown, safe_tail])

    assert [type(outcome) for outcome in outcomes] == [
        CommandSuccess,
        CommandOutcomeUnknown,
        CommandOutcomeUnknown,
        CommandSuccess,
    ]
    assert [call.v3_wire for call in transport.wire_calls] == [
        _wire(safe_head),
        _wire(unsafe, unknown),
        _wire(safe_tail),
    ]
    assert [call.replay_safety for call in transport.wire_calls] == [SAFE, UNSAFE, SAFE]
    assert report.physical_requests == THREE
    assert report.unknown == TWO
    assert report.ambiguous_physical_attempts == 1
    assert report.batch_isolation_requests == 0
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a21_mixed_bare_and_v3_never_share_an_envelope() -> None:
    """A21-MIXED: BARE and API_V3 commands of one chunk go to their own envelopes."""
    bare = Request("x.item.get", {"id": 1}, SAFE, route=RouteKind.BARE)
    v3 = _v3(2)
    transport = ScriptedTransport(
        (
            ScriptedExchange.batch((bare,), (_item(1),), total=1),
            ScriptedExchange.v3_batch((v3,), (_item(2),)),
        ),
    )

    outcomes, report = await _run(transport, [bare, v3])

    assert all(isinstance(outcome, CommandSuccess) for outcome in outcomes)
    assert [call.request is not None for call in transport.wire_calls] == [True, False]
    assert report.physical_requests == TWO
    transport.assert_exhausted()


# A21-PRESPONSE ---------------------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("label", "response"),
    [
        ("structured_api_error", WireResponse(500, JSON_HEADERS, json.dumps(_abort("INTERNAL")).encode())),
        ("invalid_json", WireResponse(200, JSON_HEADERS, b'{"result": [')),
        ("short_array", WireResponse(200, JSON_HEADERS, json.dumps({"result": [_item(1)]}).encode())),
        ("non_object_element", WireResponse(200, JSON_HEADERS, json.dumps({"result": [_item(1), 7]}).encode())),
        ("unstructured_5xx", WireResponse(503, (), b"gateway")),
    ],
)
async def test_a21_presponse_non_safe_outcomes_after_dispatch_are_unknown_without_resend(
    label: str, response: WireResponse
) -> None:
    """A21-PRESPONSE: classification precedes retry; non-SAFE commands become unknown, nothing is resent."""
    requests = (_v3(1, method="x.item.add", safety=UNSAFE), _v3(2, method="x.item.add", safety=UNSAFE))
    fixture = ScriptedExchange.v3_batch(requests, (_item(1), _item(2)))
    transport = ScriptedTransport((ScriptedExchange(fixture.request, response, v3_wire=fixture.v3_wire),))

    outcomes, report = await _run(transport, requests)

    transport.assert_exhausted()
    assert all(isinstance(outcome, CommandOutcomeUnknown) for outcome in outcomes), label
    assert all(
        isinstance(outcome.error, AmbiguousExecutionError) and outcome.error.declared_unsafe
        for outcome in outcomes
        if isinstance(outcome, CommandOutcomeUnknown)
    )
    assert report.physical_requests == 1
    assert report.ambiguous_physical_attempts == 1
    assert report.unknown == TWO


@pytest.mark.asyncio
async def test_a21_presponse_size_overflow_after_dispatch_is_unknown_without_resend() -> None:
    """A21-PRESPONSE: a response over the byte ceiling (httpx bounded reader) leaves non-SAFE work unknown."""
    sends = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal sends
        sends += 1
        return httpx.Response(200, json={"result": [{"item": {"pad": "x" * 512}}]}, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = HttpxTransport("https://fixture.invalid/rest/1/token/", client=client)
    try:
        outcomes, report = await _run(
            transport,  # type: ignore[arg-type]
            [_v3(1, method="x.item.add", safety=UNKNOWN)],
            policy=_policy(max_response_bytes=64),
        )
    finally:
        await transport.aclose()
        await client.aclose()

    assert isinstance(outcomes[0], CommandOutcomeUnknown)
    assert sends == 1
    assert report.ambiguous_physical_attempts == 1


@pytest.mark.asyncio
async def test_a21_presponse_cancellation_before_response_propagates_without_resend() -> None:
    """A21-PRESPONSE: cancellation after dispatch and before the Response replays nothing and propagates."""
    started = asyncio.Event()
    sends = 0

    class _Blocking(ScriptedTransport):
        async def send_wire(
            self, request: WireRequest, *, attempt_timeout: float, max_response_bytes: int
        ) -> WireResponse:
            del request, attempt_timeout, max_response_bytes
            nonlocal sends
            sends += 1
            started.set()
            await asyncio.Future[None]()
            raise AssertionError("unreachable")

    engine = BatchExecutor(Executor(_Blocking(())))
    context = engine.executor.context(_policy())
    task = asyncio.create_task(
        engine.execute_requests((_v3(1, method="x.item.add", safety=UNSAFE), _v3(2)), context=context)
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    snapshot = await context.snapshot()
    assert sends == 1
    assert snapshot.counters.physical_requests == 1


# A21-REFUSAL -----------------------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 503])
async def test_a21_refusal_structured_quota_after_possible_prefix_is_unknown_for_non_safe(status: int) -> None:
    """A21-REFUSAL: a structured quota code proves nothing about a possibly executed v3 prefix."""
    requests = (_v3(1, method="x.item.add", safety=UNSAFE), _v3(2, method="x.item.add", safety=UNKNOWN))
    transport = ScriptedTransport(
        (ScriptedExchange.v3_batch(requests, payload={"error": "QUERY_LIMIT_EXCEEDED"}, status=status),),
    )

    outcomes, report = await _run(transport, requests)

    assert all(isinstance(outcome, CommandOutcomeUnknown) for outcome in outcomes)
    assert report.physical_requests == 1
    assert report.retries == 0
    assert report.unknown == TWO
    assert report.not_executed == 0
    transport.assert_exhausted()


# A21-THROTTLE ----------------------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("safety", [SAFE, UNSAFE])
async def test_a21_throttle_unstructured_429_is_an_envelope_refusal_retried_without_direct_sends(
    safety: ReplaySafety,
) -> None:
    """A21-THROTTLE: unstructured 429 then success: two envelope sends and no individual requests."""
    requests = tuple(_v3(identifier, safety=safety) for identifier in (1, 2, 3))
    fixture = ScriptedExchange.v3_batch(requests, tuple(_item(i) for i in (1, 2, 3)))
    refused = ScriptedExchange(fixture.request, WireResponse(429, (), b"slow down"), v3_wire=fixture.v3_wire)
    transport = ScriptedTransport((refused, fixture))

    outcomes, report = await _run(transport, requests)

    assert all(isinstance(outcome, CommandSuccess) for outcome in outcomes)
    assert report.physical_requests == TWO
    assert transport.calls == ()
    assert report.ambiguous_physical_attempts == 0
    assert report.batch_isolation_requests == 0
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a21_throttle_structured_quota_safe_abort_is_retried_once_as_an_envelope() -> None:
    """A21-THROTTLE: a structured SAFE quota abort is retried as one envelope, not N direct requests."""
    requests = tuple(_v3(identifier) for identifier in (1, 2, 3))
    fixture = ScriptedExchange.v3_batch(requests, tuple(_item(i) for i in (1, 2, 3)))
    quota = ScriptedExchange.v3_batch(requests, payload={"error": "QUERY_LIMIT_EXCEEDED"}, status=503)
    transport = ScriptedTransport((quota, fixture))

    outcomes, report = await _run(transport, requests)

    assert all(isinstance(outcome, CommandSuccess) for outcome in outcomes)
    assert report.physical_requests == TWO
    assert transport.calls == ()
    assert report.ambiguous_physical_attempts == 1
    assert report.batch_isolation_requests == 0
    transport.assert_exhausted()


class _ObservingCoordinator(RateCoordinator):
    def __init__(self) -> None:
        super().__init__(operation_time_limit_delay=0.05)
        self.observed: list[tuple[str, str]] = []

    async def observe_api_throttle(self, method: str, code: str, *, delay: float | None = None) -> float:
        self.observed.append((method, code))
        return await super().observe_api_throttle(method, code, delay=delay)


@pytest.mark.asyncio
async def test_a21_throttle_operation_time_limit_abort_observes_each_distinct_method_and_waits() -> None:
    """A21-THROTTLE: OTL abort on chunk a,b,a observes exactly {a,b}; the retry waits for the cooldown."""
    requests = (_v3(1, method="x.alpha.get"), _v3(2, method="x.beta.get"), _v3(3, method="x.alpha.get"))
    fixture = ScriptedExchange.v3_batch(requests, tuple(_item(i) for i in (1, 2, 3)))
    limited = ScriptedExchange.v3_batch(requests, payload={"error": "OPERATION_TIME_LIMIT"}, status=503)
    sent_at: list[float] = []

    class _Timed(ScriptedTransport):
        async def send_wire(
            self, request: WireRequest, *, attempt_timeout: float, max_response_bytes: int
        ) -> WireResponse:
            sent_at.append(time.monotonic())
            return await super().send_wire(
                request, attempt_timeout=attempt_timeout, max_response_bytes=max_response_bytes
            )

    transport = _Timed((limited, fixture))
    coordinator = _ObservingCoordinator()
    engine = BatchExecutor(Executor(transport, coordinator=coordinator))
    context = engine.executor.context(_policy())
    try:
        outcomes = await engine.execute_requests(requests, context=context)
    finally:
        await coordinator.close()

    assert all(isinstance(outcome, BatchSuccess) for outcome in outcomes)
    assert coordinator.observed == [("x.alpha.get", "operation_time_limit"), ("x.beta.get", "operation_time_limit")]
    assert len(sent_at) == TWO
    # The 0.05 s method cooldown, less timer slack.
    assert sent_at[1] - sent_at[0] >= COOLDOWN_FLOOR
    snapshot = await context.snapshot()
    assert snapshot.ambiguous_physical_attempts == 1
    assert snapshot.batch_isolation_requests == 0
    transport.assert_exhausted()


# A21-POISON2 -----------------------------------------------------------------------------------------------


def _poisoned(requests: tuple[Request, ...]) -> ScriptedExchange:
    return ScriptedExchange.v3_batch(requests, payload=_abort("SYNTHETIC_NOT_FOUND"), status=400)


@pytest.mark.asyncio
async def test_a21_poison2_safe_abort_isolates_each_command_and_attributes_the_poisoned_one() -> None:
    """A21-POISON2: 3 SAFE, the middle one permanently failing, one abort: S/F/S in 1+3 sends."""
    requests = (_v3(1), _v3(2), _v3(3))
    transport = ScriptedTransport(
        (
            _poisoned(requests),
            _direct(requests[0], {"result": _item(1)}),
            _direct(requests[1], _abort("SYNTHETIC_NOT_FOUND"), status=400),
            _direct(requests[2], {"result": _item(3)}),
        ),
    )

    outcomes, report = await _run(transport, requests)

    assert [type(outcome) for outcome in outcomes] == [CommandSuccess, CommandFailure, CommandSuccess]
    assert isinstance(outcomes[1], CommandFailure)
    assert isinstance(outcomes[1].error, ApiResponseError)
    assert [outcome.result for outcome in outcomes if isinstance(outcome, CommandSuccess)] == [_item(1), _item(3)]
    assert report.physical_requests == 1 + THREE
    assert report.unknown == 0
    assert report.ambiguous_physical_attempts == 1
    assert report.batch_isolation_requests == THREE
    # Isolated requests are ordinary API_V3 calls; they are never packed into a batch again.
    assert sorted(call.method for call in transport.calls) == ["x.item.get"] * THREE
    assert all(call.route is RouteKind.API_V3 for call in transport.calls)
    assert len(_v3_calls(transport)) == 1
    transport.assert_exhausted()


# A21-LINEAGE -----------------------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("label", "policy"),
    [
        ("single_attempt", _policy(max_attempts_per_request=1)),
        ("isolation_disabled", _policy(retry=RetryPolicy(initial_delay=0, jitter=0, isolate_safe_batch_abort=False))),
    ],
)
async def test_a21_lineage_forbids_isolation_without_budget_and_reports_unknown(
    label: str, policy: ExecutionPolicy
) -> None:
    """A21-LINEAGE: attempts=1 or isolation=False: no further send; commands are unknown, not not-executed."""
    requests = (_v3(1), _v3(2), _v3(3))
    transport = ScriptedTransport((_poisoned(requests),))

    outcomes, report = await _run(transport, requests, policy=policy)

    assert all(isinstance(outcome, CommandOutcomeUnknown) for outcome in outcomes), label
    assert report.physical_requests == 1
    assert report.unknown == THREE
    assert report.not_executed == 0
    assert report.batch_isolation_requests == 0
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a21_lineage_low_request_budget_leaves_unsent_isolation_unknown() -> None:
    """A21-LINEAGE: a request budget for only one isolated call: the others stay unknown, never not-executed."""
    requests = (_v3(1), _v3(2), _v3(3))
    transport = ScriptedTransport(
        (_poisoned(requests), *(_direct(request, {"result": _item(1)}) for request in requests)),
    )

    outcomes, report = await _run(transport, requests, policy=_policy(max_requests=TWO))

    assert report.physical_requests == TWO
    assert report.batch_isolation_requests == 1
    assert sum(isinstance(outcome, CommandSuccess) for outcome in outcomes) == 1
    assert sum(isinstance(outcome, CommandOutcomeUnknown) for outcome in outcomes) == TWO
    assert report.not_executed == 0


@pytest.mark.asyncio
async def test_a21_lineage_expired_retry_time_forbids_isolation() -> None:
    """A21-LINEAGE: the retry clock started at the envelope; once it has elapsed nothing more is sent."""
    now = [1000.0]
    requests = (_v3(1), _v3(2))

    class _SlowAbort(ScriptedTransport):
        async def send_wire(
            self, request: WireRequest, *, attempt_timeout: float, max_response_bytes: int
        ) -> WireResponse:
            now[0] += 30.0
            return await super().send_wire(
                request, attempt_timeout=attempt_timeout, max_response_bytes=max_response_bytes
            )

    transport = _SlowAbort((_poisoned(requests),))
    executor = Executor(transport, clock=lambda: now[0])
    engine = BatchExecutor(executor)
    context = executor.context(_policy(max_retry_elapsed_per_request=10.0))
    outcomes = await engine.execute_requests(requests, context=context)

    assert all(isinstance(outcome, BatchFailure) for outcome in outcomes)
    assert all(
        isinstance(outcome.error, AmbiguousExecutionError) for outcome in outcomes if isinstance(outcome, BatchFailure)
    )
    snapshot = await context.snapshot()
    assert snapshot.counters.physical_requests == 1
    assert snapshot.batch_isolation_requests == 0
    assert snapshot.ambiguous_physical_attempts == 1


@pytest.mark.asyncio
async def test_a21_lineage_proven_nonexecution_later_does_not_erase_earlier_possible_execution() -> None:
    """A21-LINEAGE: after a possibly executed SAFE envelope, a later refused send keeps the commands unknown."""
    requests = (_v3(1), _v3(2))
    fixture = ScriptedExchange.v3_batch(requests, (_item(1), _item(2)))
    abort = ScriptedExchange.v3_batch(requests, payload={"error": "QUERY_LIMIT_EXCEEDED"}, status=503)
    refused = ScriptedExchange(fixture.request, WireResponse(429, (), b""), v3_wire=fixture.v3_wire)
    transport = ScriptedTransport((abort, refused))

    outcomes, report = await _run(
        transport,
        requests,
        policy=_policy(max_attempts_per_request=TWO, retry=RetryPolicy(initial_delay=0, jitter=0)),
    )

    assert all(isinstance(outcome, CommandOutcomeUnknown) for outcome in outcomes)
    assert report.physical_requests == TWO
    assert report.not_executed == 0
    transport.assert_exhausted()


def test_a21_lineage_isolation_switch_is_keyword_only_and_strict_bool() -> None:
    """``RetryPolicy.isolate_safe_batch_abort`` defaults to True, is keyword-only and refuses non-bool values."""
    assert RetryPolicy().isolate_safe_batch_abort is True
    assert RetryPolicy(isolate_safe_batch_abort=False).isolate_safe_batch_abort is False
    with pytest.raises(TypeError, match="isolate_safe_batch_abort"):
        RetryPolicy(isolate_safe_batch_abort=1)  # type: ignore[arg-type]
    parameter = inspect.signature(RetryPolicy).parameters["isolate_safe_batch_abort"]
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY


def test_report_counters_are_keyword_only_non_negative_and_default_to_zero() -> None:
    """The two new OperationReport counters default to zero and are validated like the others."""
    report = OperationReport(TerminalState.COMPLETED, "batch", "completed")
    assert (report.ambiguous_physical_attempts, report.batch_isolation_requests) == (0, 0)
    with pytest.raises(ValueError, match="non-negative"):
        OperationReport(TerminalState.COMPLETED, "batch", "completed", ambiguous_physical_attempts=-1)
    with pytest.raises(ValueError, match="non-negative"):
        OperationReport(TerminalState.COMPLETED, "batch", "completed", batch_isolation_requests=True)


# A21-HALT --------------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a21_halt_v3_fail_fast_resolves_the_accepted_chunk_and_never_reads_the_next() -> None:
    """A21-HALT: a failing v3 chunk raises BatchFailed; the lazy source is not read past that chunk."""
    pulled: list[int] = []
    requests = [_v3(identifier, result_error=ERROR_SPEC) for identifier in range(1, 5)]

    async def commands() -> AsyncIterator[Command[int]]:
        for index, request in enumerate(requests):
            pulled.append(index)
            yield Command(request, index)

    transport = ScriptedTransport(
        (
            ScriptedExchange.v3_batch(
                tuple(requests[:2]), (_item(1), {"error": {"code": "ITEM_LOCKED", "message": "locked"}})
            ),
        ),
    )
    async with client_for(transport, policy=_policy()) as api:
        delivered, error = await _drain(api.batch(commands(), batch_size=TWO))

    assert [outcome.index for outcome in delivered] == [0]
    assert isinstance(error, BatchFailed)
    failed = error.outcomes
    assert len(failed) == 1
    assert isinstance(failed[0], CommandFailure)
    assert isinstance(failed[0].error, BatchCommandError)
    assert pulled == [0, 1]
    assert len(transport.wire_calls) == 1
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a21_halt_later_envelope_of_the_accepted_chunk_is_not_sent_after_a_failure() -> None:
    """A21-HALT: in fail-fast, the envelope after a failed one is reported halted, never sent."""
    safe_head = _v3(1)
    unsafe = _v3(2, method="x.item.add", safety=UNSAFE)
    safe_tail = _v3(3)
    transport = ScriptedTransport(
        (
            ScriptedExchange.v3_batch((safe_head,), (_item(1),)),
            ScriptedExchange.v3_batch((unsafe,), payload=_abort("SYNTHETIC_VALIDATION"), status=400),
        ),
    )
    async with client_for(transport, policy=_policy()) as api:
        stream = api.batch([Command(request, index) for index, request in enumerate((safe_head, unsafe, safe_tail))])
        with pytest.raises(BatchFailed) as raised:
            async with stream:
                _ = [outcome async for outcome in stream]

    failed = raised.value.outcomes
    assert [type(outcome) for outcome in failed] == [CommandOutcomeUnknown, CommandNotExecuted]
    assert isinstance(failed[1], CommandNotExecuted)
    assert failed[1].reason is NotExecutedReason.HALTED
    assert len(transport.wire_calls) == TWO
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a21_halt_classic_bare_fail_fast_envelope_is_unchanged_on_a_v3_capable_transport() -> None:
    """A21-HALT: a BARE fail-fast chunk still sends one classic ``halt=1`` envelope; no v3 path is involved."""
    seen: list[Request] = []

    def respond(request: Request) -> object:
        seen.append(request)
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        return {"result": {"result": {key: {"ok": True} for key in commands}, "result_error": []}}

    transport = ResponderTransport(respond)
    requests = [Request("x.item.get", {"id": index}, SAFE, route=RouteKind.BARE) for index in range(3)]
    async with client_for(transport, policy=_policy()) as api:
        stream = api.batch([Command(request, index) for index, request in enumerate(requests)])
        async with stream:
            delivered = [outcome async for outcome in stream]

    assert len(delivered) == THREE
    assert len(seen) == 1
    assert seen[0].route is RouteKind.BARE
    assert seen[0].copy_parameters()["halt"] == 1
    assert stream.report is not None
    assert stream.report.ambiguous_physical_attempts == 0


@pytest.mark.asyncio
async def test_a21_kernel_outcomes_keep_total_correlation_for_v3_chunks() -> None:
    """The scheduler-facing seam returns one correlated outcome per request, in input order."""
    requests = (_v3(1), _v3(2, method="x.item.add", safety=UNSAFE))
    transport = ScriptedTransport(
        (
            ScriptedExchange.v3_batch((requests[0],), (_item(1),)),
            ScriptedExchange.v3_batch((requests[1],), (_item(2),)),
        ),
    )
    engine = BatchExecutor(Executor(transport))
    outcomes = await engine.execute_requests(requests, context=engine.executor.context(_policy()))

    assert [outcome.command_index for outcome in outcomes] == [0, 1]
    assert all(isinstance(outcome, BatchSuccess) for outcome in outcomes)
    successes = [outcome for outcome in outcomes if isinstance(outcome, BatchSuccess)]
    assert [outcome.response.total if outcome.response else None for outcome in successes] == [None, None]
    assert [outcome.response.next if outcome.response else None for outcome in successes] == [None, None]
