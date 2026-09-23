"""Regression coverage for the third v2 client-findings increment."""

from __future__ import annotations
import asyncio
import json
import subprocess
import sys
import threading
import time
from typing import TYPE_CHECKING

import pytest

from b24api import (
    AmbiguityPolicy,
    AmbiguityReason,
    AmbiguousExecutionError,
    BatchDispatch,
    BinaryEvidence,
    BinaryResponse,
    Binding,
    Bitrix24,
    BodyEncoding,
    Command,
    CommandFailure,
    CommandSuccess,
    CompositeIdentitySpec,
    CountedTraversal,
    CursorSpec,
    CursorTraversal,
    DirectDispatch,
    EnvelopeContractError,
    ExecutionPolicy,
    IdentityCoercion,
    IdentityComponent,
    IdentitySpec,
    KeysetSpec,
    KeysetTraversal,
    OffsetContinuation,
    OffsetSpec,
    PageDispatch,
    PageIndex,
    PageOutcome,
    PageRejectionCode,
    PageStride,
    ParameterPath,
    ReferenceFailure,
    ReferenceItem,
    ReplayDisposition,
    ReplaySafety,
    Request,
    RequestHeaders,
    RequestSummary,
    ResultCollectionShape,
    ResultErrorShape,
    ResultErrorSpec,
    ResultSelector,
    SequentialKeysetExecution,
    SequentialTraversal,
    SplitOrderSpec,
    TotalTermination,
    TransportCapabilities,
    UnknownRequestCollector,
    WireRequest,
    WireResponse,
    traversal_control_paths,
)
from b24api.contracts.request import RouteKind
from b24api.encoding import encode_php_query
from b24api.errors import (
    ApiResponseError,
    CapabilityError,
    IncompleteTraversalError,
    PaginationError,
    ProtocolError,
    ReferenceFailed,
    ResultShapeError,
)
from b24api.execution import Executor
from b24api.testing import ConformanceCase, run_transport_conformance
from b24api.testing import transport as testing_transport
from b24api.transport import HttpxTransport
from b24api.traversal import iter_list
from b24api.traversal.plans import CountedOffsetPlan

PAGE_SIZE = 50
OBSERVED_REQUESTS = 2
REFERENCE_OUTCOMES = 2
FANOUT_COMMANDS = 3
DISTINCT_REQUESTS = 2

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator


class _Transport:
    host = "test.invalid"

    def __init__(self, handler: Callable[[Request], WireResponse]) -> None:
        self.handler = handler
        self.requests: list[Request] = []

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        self.requests.append(request)
        return self.handler(request)


class _WireTransport(_Transport):
    capabilities = TransportCapabilities(
        frozenset({BodyEncoding.JSON, BodyEncoding.FORM_URLENCODED}),
        scoped_headers=True,
    )

    def __init__(self, handler: Callable[[Request], WireResponse]) -> None:
        super().__init__(handler)
        self.wire_requests: list[WireRequest] = []

    async def send_wire(
        self,
        request: WireRequest,
        *,
        attempt_timeout: float,
        max_response_bytes: int,
    ) -> WireResponse:
        del attempt_timeout, max_response_bytes
        self.wire_requests.append(request)
        return self.handler(
            Request(
                request.method,
                parameters=request.copy_parameters(),
                replay_safety=request.replay_safety,
                encoding=request.encoding,
                headers=request.headers,
                result_error=request.result_error,
                route=RouteKind.BARE,
            ),
        )


def _response(result: object, *, total: int | None = None, next_value: int | None = None) -> WireResponse:
    payload: dict[str, object] = {"result": result}
    if total is not None:
        payload["total"] = total
    if next_value is not None:
        payload["next"] = next_value
    return WireResponse(200, (("Content-Type", "application/json"),), json.dumps(payload).encode())


def _client(transport: _Transport) -> Bitrix24:
    return Bitrix24._from_executor(Executor(transport))  # noqa: SLF001


def test_request_derivation_preserves_all_non_parameter_contracts() -> None:
    error_spec = ResultErrorSpec(ResultSelector.root(), ("code",))
    request = Request(
        "example.action",
        {"old": 1},
        ReplaySafety.UNSAFE,
        encoding=BodyEncoding.FORM_URLENCODED,
        headers=RequestHeaders({"X-Domain-Ack": "opaque"}),
        result_error=error_spec,
        route=RouteKind.BARE,
    )

    derived = request.with_parameters({"new": 2})

    assert derived.copy_parameters() == {"new": 2}
    assert derived.replay_safety is ReplaySafety.UNSAFE
    assert derived.encoding is BodyEncoding.FORM_URLENCODED
    assert derived.headers == request.headers
    assert derived.result_error is error_spec
    assert "opaque" not in repr(derived)


def test_request_hash_is_structural_and_independent_of_mapping_order() -> None:
    first = Request("example.action", {"outer": {"a": 1, "b": [2, 3]}}, route=RouteKind.BARE)
    second = Request("example.action", {"outer": {"b": [2, 3], "a": 1}}, route=RouteKind.BARE)

    assert first == second
    assert hash(first) == hash(second)


@pytest.mark.parametrize(
    ("first_value", "second_value"),
    [(True, 1), (False, 0), (1, 1.0), (-0.0, 0.0)],
)
def test_request_equality_preserves_wire_significant_json_scalar_types(
    first_value: object,
    second_value: object,
) -> None:
    first = Request("example.action", {"outer": [first_value]}, route=RouteKind.BARE)
    second = Request("example.action", {"outer": [second_value]}, route=RouteKind.BARE)

    assert first != second
    assert len({first, second}) == DISTINCT_REQUESTS


def test_request_rejects_excessive_json_depth_before_hashing() -> None:
    nested: object = 1
    for _index in range(300):
        nested = [nested]

    with pytest.raises(ValueError, match="nesting exceeds"):
        Request("example.action", {"nested": nested}, route=RouteKind.BARE)


def test_headers_are_normalized_and_reserved_families_are_rejected() -> None:
    assert RequestHeaders({"X-Zeta": "z", "X-Alpha": "a"}).names == ("x-alpha", "x-zeta")
    with pytest.raises(ValueError, match="reserved"):
        RequestHeaders({"Authorization": "value"})
    with pytest.raises(ValueError, match="duplicate"):
        RequestHeaders((("X-Test", "one"), ("x-test", "two")))


def test_public_request_summary_normalizes_and_redacts_header_names() -> None:
    sensitive = "ABCDEFGHIJKLMNOP"
    summary = RequestSummary(
        method="example.action",
        header_names=("X-Safe", f"https://secret.invalid/rest/1/{sensitive}/"),
    )

    assert "x-safe" in summary.header_names
    assert sensitive.casefold() not in repr(summary.header_names)


@pytest.mark.asyncio
async def test_advanced_request_requires_advertised_transport_before_io() -> None:
    transport = _Transport(lambda _request: _response(result=True))
    request = Request("example.action", encoding=BodyEncoding.FORM_URLENCODED, route=RouteKind.BARE)

    with pytest.raises(CapabilityError):
        await Executor(transport).execute(request)

    assert transport.requests == []


@pytest.mark.asyncio
async def test_binary_call_returns_every_success_byte_without_json_sniffing() -> None:
    transport = _Transport(lambda _request: WireResponse(200, (("Content-Type", "text/csv"),), b"a,b\n1,2\n"))

    response = await _client(transport).call_bytes(
        Request("example.download", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    )

    assert response.body == b"a,b\n1,2\n"
    assert response.content_type == "text/csv"
    assert response.evidence.sha256 is None
    assert "a,b" not in repr(response)


def test_binary_evidence_rejects_non_digest_sha256() -> None:
    with pytest.raises(ValueError, match="64 lower-case hexadecimal"):
        BinaryEvidence(200, "application/octet-stream", 0, "not-a-digest")


def test_binary_response_rejects_digest_that_does_not_match_body() -> None:
    evidence = BinaryEvidence(200, "application/octet-stream", 3, "0" * 64)

    with pytest.raises(ValueError, match="digest does not match body"):
        BinaryResponse(b"abc", content_type="application/octet-stream", evidence=evidence)


def test_wire_response_repr_drops_unbounded_invalid_media_type() -> None:
    response = WireResponse(200, (("content-type", "x" * 500),), b"secret")

    assert "x" * 20 not in repr(response)
    assert "secret" not in repr(response)


@pytest.mark.asyncio
async def test_binary_content_type_evidence_drops_parameters_and_rejects_unsafe_media_types() -> None:
    sensitive_value = "token-secret-value-123"
    transport = _Transport(
        lambda _request: WireResponse(
            200,
            (("Content-Type", f"application/octet-stream; token={sensitive_value}"),),
            b"x",
        ),
    )

    response = await _client(transport).call_bytes(
        Request("example.download", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    )

    assert response.content_type == "application/octet-stream"
    assert sensitive_value not in repr(response)
    assert sensitive_value not in repr(response.evidence)
    unsafe = BinaryEvidence(200, f"invalid {sensitive_value}", 1)
    assert unsafe.content_type is None
    assert BinaryResponse(b"x", content_type=f"invalid {sensitive_value}", evidence=unsafe).content_type is None
    assert BinaryEvidence(200, "application/x.foo~bar", 1).content_type == "application/x.foo~bar"
    with pytest.raises(TypeError, match="content type"):
        BinaryEvidence(200, 42, 1)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_malformed_wire_capabilities_are_rejected_before_attempt_accounting() -> None:
    transport = _WireTransport(lambda _request: _response({"ok": 1}))
    transport.capabilities = "malformed"  # type: ignore[assignment]
    context = Executor(transport).context()

    with pytest.raises(CapabilityError, match="malformed capabilities"):
        await Executor(transport).execute(Request("example.list", route=RouteKind.BARE), context=context)

    assert transport.requests == []
    assert transport.wire_requests == []
    assert (await context.snapshot()).counters.physical_requests == 0

    traversal_transport = _WireTransport(lambda _request: _response([]))
    traversal_transport.capabilities = "malformed"  # type: ignore[assignment]
    stream = _client(traversal_transport).iter_list(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    )
    with pytest.raises(CapabilityError, match="malformed capabilities"):
        await anext(stream)
    assert stream.report is not None
    assert stream.report.page_trace == ()
    assert traversal_transport.wire_requests == []


@pytest.mark.asyncio
async def test_embedded_error_contract_is_opt_in_and_preserves_wire_code() -> None:
    spec = ResultErrorSpec(
        ResultSelector(("error",)),
        ("code",),
        ("message",),
        ResultErrorShape.MAPPING,
    )
    transport = _Transport(lambda _request: _response({"error": {"code": "ACCESS_DENIED", "message": "Denied"}}))

    with pytest.raises(ApiResponseError) as captured:
        await Executor(transport).execute(Request("example.list", result_error=spec, route=RouteKind.BARE))

    assert captured.value.wire_code == "ACCESS_DENIED"
    assert captured.value.normalized_code == "access_denied"
    assert "[ACCESS_DENIED] (normalized: access_denied)" in str(captured.value)


@pytest.mark.asyncio
async def test_unstructured_post_dispatch_status_is_ambiguous_for_unknown() -> None:
    transport = _Transport(lambda _request: WireResponse(503, (), b"unstructured"))

    with pytest.raises(AmbiguousExecutionError) as captured:
        await Executor(transport).execute(Request("example.write", route=RouteKind.BARE))

    assert captured.value.reason is AmbiguityReason.HTTP_STATUS_AFTER_DISPATCH
    assert captured.value.declared_unsafe is False


@pytest.mark.asyncio
@pytest.mark.parametrize("safety", tuple(ReplaySafety))
async def test_malformed_nonempty_2xx_remains_protocol_error_under_broad_ambiguity_policy(
    safety: ReplaySafety,
) -> None:
    transport = _Transport(lambda _request: WireResponse(200, (), b"not-json"))
    policy = ExecutionPolicy(ambiguity=AmbiguityPolicy(frozenset(range(100, 600))))

    with pytest.raises(ProtocolError) as captured:
        await Executor(transport).execute(
            Request("example.list", replay_safety=safety, route=RouteKind.BARE),
            policy=policy,
        )

    assert type(captured.value).__name__ == "ProtocolError"


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [b"", b"[]", b"{}"])
async def test_success_envelope_defects_remain_envelope_contract_errors(body: bytes) -> None:
    transport = _Transport(lambda _request: WireResponse(200, (), body))

    with pytest.raises(EnvelopeContractError):
        await Executor(transport).execute(
            Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        )


@pytest.mark.asyncio
async def test_repository_httpx_transport_passes_complete_public_conformance_suite() -> None:
    report = await run_transport_conformance(HttpxTransport)

    assert report.passed
    assert len(report.outcomes) == len(ConformanceCase)
    routes = {outcome.case: outcome for outcome in report.outcomes if outcome.case.value.startswith("route_")}
    assert set(routes) == {ConformanceCase.ROUTE_JSON_SUFFIX, ConformanceCase.ROUTE_API_V3_REBASE}
    assert all(outcome.passed and not outcome.skipped for outcome in routes.values())


@pytest.mark.asyncio
async def test_conformance_runner_times_out_transport_that_never_reaches_responder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(testing_transport, "_CASE_TIMEOUT_SECONDS", 0.05)
    started = time.monotonic()
    report = await run_transport_conformance(
        lambda _url: _Transport(lambda _request: _response(result=True)),
        cases=frozenset({ConformanceCase.LEGACY_JSON_BODY}),
    )

    assert time.monotonic() - started < 1
    assert report.outcomes == (
        testing_transport.ConformanceOutcome(
            ConformanceCase.LEGACY_JSON_BODY,
            passed=False,
            detail="CaseDeadlineExceeded",
        ),
    )


@pytest.mark.asyncio
async def test_conformance_runner_bounds_blocking_factory_and_hanging_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(testing_transport, "_CASE_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(testing_transport, "_CLOSE_TIMEOUT_SECONDS", 0.05)

    def blocking_factory(_url: str) -> _Transport:
        time.sleep(0.2)
        return _Transport(lambda _request: _response(result=True))

    started = time.monotonic()
    factory_report = await run_transport_conformance(
        blocking_factory,
        cases=frozenset({ConformanceCase.LEGACY_JSON_BODY}),
    )
    factory_wall_ceiling = 0.15
    assert time.monotonic() - started < factory_wall_ceiling
    assert factory_report.failures[0].detail == "CaseDeadlineExceeded"

    release_cleanup = asyncio.Event()

    class HangingCloseTransport(_Transport):
        async def aclose(self) -> None:
            while not release_cleanup.is_set():
                try:
                    await release_cleanup.wait()
                except asyncio.CancelledError:
                    continue

    started = time.monotonic()
    cleanup_report = await run_transport_conformance(
        lambda _url: HangingCloseTransport(lambda _request: _response(result=True)),
        cases=frozenset({ConformanceCase.HOST_PROPERTY_CREDENTIAL_FREE}),
    )
    cleanup_wall_ceiling = 0.5
    assert time.monotonic() - started < cleanup_wall_ceiling
    assert cleanup_report.failures[0].detail == "CleanupDeadlineExceeded"
    release_cleanup.set()
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_conformance_runner_distinguishes_transport_timeout_from_harness_deadline() -> None:
    def raises_timeout(_request: Request) -> WireResponse:
        raise TimeoutError

    report = await run_transport_conformance(
        lambda _url: _Transport(raises_timeout),
        cases=frozenset({ConformanceCase.LEGACY_JSON_BODY}),
    )

    assert report.failures[0].detail == "TimeoutError"


@pytest.mark.asyncio
async def test_isolated_base_exceptions_become_outcomes_without_cancelling_caller() -> None:
    class CancelledSend(_Transport):
        async def send(
            self,
            request: Request,
            *,
            attempt_timeout: float,
            max_response_bytes: int,
        ) -> WireResponse:
            del request, attempt_timeout, max_response_bytes
            raise asyncio.CancelledError

    send_report = await run_transport_conformance(
        lambda _url: CancelledSend(lambda _request: _response(result=True)),
        cases=frozenset({ConformanceCase.LEGACY_JSON_BODY}),
    )

    class CancelledClose(_Transport):
        async def aclose(self) -> None:
            raise asyncio.CancelledError

    close_report = await run_transport_conformance(
        lambda _url: CancelledClose(lambda _request: _response(result=True)),
        cases=frozenset({ConformanceCase.HOST_PROPERTY_CREDENTIAL_FREE}),
    )

    assert send_report.failures[0].detail == "CancelledError"
    assert close_report.failures[0].detail == "CancelledError"
    current = asyncio.current_task()
    assert current is not None
    assert current.cancelling() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("abort", [KeyboardInterrupt, SystemExit])
async def test_isolated_process_aborts_become_value_free_outcomes(
    abort: type[BaseException],
) -> None:
    class AbortingSend(_Transport):
        async def send(
            self,
            request: Request,
            *,
            attempt_timeout: float,
            max_response_bytes: int,
        ) -> WireResponse:
            del request, attempt_timeout, max_response_bytes
            raise abort

    report = await run_transport_conformance(
        lambda _url: AbortingSend(lambda _request: _response(result=True)),
        cases=frozenset({ConformanceCase.LEGACY_JSON_BODY}),
    )

    assert report.failures[0].detail == abort.__name__


@pytest.mark.asyncio
async def test_case_failure_deterministically_precedes_resistant_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(testing_transport, "_CLOSE_TIMEOUT_SECONDS", 0.02)
    releases: list[threading.Event] = []

    class TimeoutThenResistantClose(_Transport):
        async def send(
            self,
            request: Request,
            *,
            attempt_timeout: float,
            max_response_bytes: int,
        ) -> WireResponse:
            del request, attempt_timeout, max_response_bytes
            raise TimeoutError

        async def aclose(self) -> None:
            release = threading.Event()
            releases.append(release)
            while not release.is_set():
                try:
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    continue

    details = []
    for _index in range(10):
        report = await run_transport_conformance(
            lambda _url: TimeoutThenResistantClose(lambda _request: _response(result=True)),
            cases=frozenset({ConformanceCase.LEGACY_JSON_BODY}),
        )
        details.append(report.failures[0].detail)

    assert details == ["TimeoutError"] * 10
    for release in releases:
        release.set()
    await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_conformance_runner_preserves_loop_affinity_and_isolates_case_responders() -> None:
    loop_was_running: list[bool] = []
    base_urls: list[str] = []

    def factory(base_url: str) -> HttpxTransport:
        loop_was_running.append(asyncio.get_running_loop().is_running())
        base_urls.append(base_url)
        return HttpxTransport(base_url)

    report = await run_transport_conformance(
        factory,
        cases=frozenset({ConformanceCase.LEGACY_JSON_BODY, ConformanceCase.JSON_CONTENT_TYPE}),
    )

    assert report.passed
    assert len(loop_was_running) == DISTINCT_REQUESTS
    assert all(loop_was_running)
    assert len(set(base_urls)) == DISTINCT_REQUESTS


def test_cancellation_resistant_cleanup_cannot_block_process_shutdown() -> None:
    script = """
import asyncio
from b24api.testing import ConformanceCase, run_transport_conformance
from b24api.testing import transport as module

module._CLOSE_TIMEOUT_SECONDS = 0.02

class Resistant:
    host = "test.invalid"
    async def send(self, request, *, attempt_timeout, max_response_bytes):
        raise AssertionError
    async def aclose(self):
        while True:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                continue

async def main():
    report = await run_transport_conformance(
        lambda _url: Resistant(),
        cases=frozenset({ConformanceCase.HOST_PROPERTY_CREDENTIAL_FREE}),
    )
    assert report.failures[0].detail == "CleanupDeadlineExceeded"

asyncio.run(main())
"""
    result = subprocess.run(  # noqa: S603 - fixed interpreter and in-repository regression script
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=1,
    )

    assert result.returncode == 0, (result.stdout, result.stderr)


@pytest.mark.asyncio
async def test_fixed_step_ignores_relative_next_and_exact_total_can_terminate() -> None:
    step = 2

    def handler(request: Request) -> WireResponse:
        start = request.copy_parameters().get("start", 0)
        if start == 0:
            return _response([{"ID": 1}, {"ID": 2}], total=3, next_value=2)
        if start == step:
            return _response([{"ID": 3}], total=3, next_value=1)
        raise AssertionError("fixed-step traversal used an undeclared offset")

    transport = _Transport(handler)
    stream = _client(transport).iter_list(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        identity=IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_INTEGER),
        page_size=step,
        offset=OffsetSpec(
            continuation=OffsetContinuation.FIXED_STEP,
            step=step,
            total_termination=TotalTermination.EXACT_QUALIFIED,
        ),
    )

    assert [row async for row in stream] == [{"ID": 1}, {"ID": 2}, {"ID": 3}]
    assert [request.copy_parameters()["start"] for request in transport.requests] == [0, step]
    assert stream.report is not None
    assert stream.report.assurance.value == "identity_and_count_matched"


@pytest.mark.asyncio
@pytest.mark.parametrize("second_page", [[{"ID": 2}], []])
async def test_fixed_step_rejects_any_closure_after_a_short_unqualified_window(
    second_page: list[dict[str, int]],
) -> None:
    step = 2

    def handler(request: Request) -> WireResponse:
        start = request.copy_parameters().get("start", 0)
        if start == 0:
            return _response([{"ID": 1}])
        if start == step:
            return _response(second_page)
        raise AssertionError("fixed-step traversal used an undeclared offset")

    transport = _Transport(handler)
    stream = _client(transport).iter_list(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        page_size=step,
        offset=OffsetSpec(continuation=OffsetContinuation.FIXED_STEP, step=step),
    )

    assert await anext(stream) == {"ID": 1}
    with pytest.raises(IncompleteTraversalError) as captured:
        await anext(stream)

    assert isinstance(captured.value.__cause__, PaginationError)
    assert "cannot prove closure after a short page" in str(captured.value.__cause__)
    assert [request.copy_parameters()["start"] for request in transport.requests] == [0, step]


@pytest.mark.asyncio
async def test_reference_fixed_step_rejects_empty_closure_after_a_short_window() -> None:
    step = 2

    def handler(request: Request) -> WireResponse:
        start = request.copy_parameters().get("start", 0)
        return _response([{"ID": 1}]) if start == 0 else _response([])

    stream = _client(_Transport(handler)).iter_reference_outcomes(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        [Binding("one", (), object())],
        traversal=SequentialTraversal(
            page_size=step,
            offset=OffsetSpec(continuation=OffsetContinuation.FIXED_STEP, step=step),
        ),
        dispatch=DirectDispatch(concurrency=1),
    )

    outcomes = [outcome async for outcome in stream]
    failure = outcomes[-1]
    assert isinstance(failure, ReferenceFailure)
    assert isinstance(failure.error, IncompleteTraversalError)
    assert isinstance(failure.error.__cause__, PaginationError)
    assert "cannot prove closure after a short page" in str(failure.error.__cause__)
    assert not stream.report.exhausted


@pytest.mark.asyncio
async def test_reference_sequential_preserves_page_index_controls() -> None:
    initial_page = 3
    next_page = 5
    page_size = 2
    observed: list[tuple[int, int]] = []

    def handler(request: Request) -> WireResponse:
        parameters = request.copy_parameters()
        page = parameters["page"]
        limit = parameters["limit"]
        assert isinstance(page, int)
        assert isinstance(limit, int)
        observed.append((page, limit))
        return _response([{"ID": 1}, {"ID": 2}]) if page == initial_page else _response([])

    stream = _client(_Transport(handler)).iter_references(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        [Binding("one", (), object())],
        traversal=SequentialTraversal(
            page_size=page_size,
            offset=OffsetSpec(
                parameter_path=ParameterPath(("page",)),
                limit_path=ParameterPath(("limit",)),
                page_index=PageIndex(
                    ParameterPath(("page",)),
                    initial=initial_page,
                    increment=page_size,
                    max_rows=page_size,
                ),
            ),
        ),
        dispatch=DirectDispatch(concurrency=1),
    )

    outcomes = [outcome async for outcome in stream]
    assert len(outcomes) == page_size + 1
    assert observed == [(initial_page, page_size), (next_page, page_size)]


@pytest.mark.asyncio
async def test_page_index_rejects_rows_after_a_short_window_in_direct_and_reference_paths() -> None:
    page_size = 2
    page_path = ParameterPath(("page",))

    def handler(request: Request) -> WireResponse:
        page = request.copy_parameters()["page"]
        assert isinstance(page, int)
        pages = {1: [{"ID": 1}, {"ID": 2}], 2: [{"ID": 3}], 3: [{"ID": 4}, {"ID": 5}]}
        return _response(pages[page])

    traversal = SequentialTraversal(
        page_size=page_size,
        offset=OffsetSpec(parameter_path=page_path, page_index=PageIndex(page_path, max_rows=page_size)),
    )
    direct = _client(_Transport(handler)).iter_list(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        page_size=traversal.page_size,
        offset=traversal.offset,
    )
    with pytest.raises(IncompleteTraversalError) as direct_failure:
        _ = [item async for item in direct]
    assert isinstance(direct_failure.value.__cause__, PaginationError)
    assert not direct.report.exhausted

    reference = _client(_Transport(handler)).iter_reference_outcomes(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        [Binding("one", (), object())],
        traversal=traversal,
        dispatch=DirectDispatch(concurrency=1),
    )
    outcomes = [outcome async for outcome in reference]
    failure = outcomes[-1]
    assert isinstance(failure, ReferenceFailure)
    assert isinstance(failure.error, IncompleteTraversalError)
    assert isinstance(failure.error.__cause__, PaginationError)
    assert not reference.report.exhausted


def test_counted_rejects_inconsistent_or_wire_limited_page_stride_before_io() -> None:
    transport = _Transport(lambda _request: pytest.fail("invalid counted stride must reject before I/O"))
    client = _client(transport)
    with pytest.raises(ValueError, match="decoded row cap must equal"):
        OffsetSpec(
            limit_path=ParameterPath(("limit",)),
            continuation=OffsetContinuation.FIXED_STEP,
            step=PAGE_SIZE,
            total_termination=TotalTermination.EXACT_QUALIFIED,
            page_stride=PageStride(PAGE_SIZE, PAGE_SIZE, PAGE_SIZE // 2),
        )
    wire_limited = OffsetSpec(
        limit_path=ParameterPath(("limit",)),
        continuation=OffsetContinuation.FIXED_STEP,
        step=PAGE_SIZE,
        total_termination=TotalTermination.EXACT_QUALIFIED,
        page_stride=PageStride(PAGE_SIZE, PAGE_SIZE, PAGE_SIZE, requested_wire_limit=PAGE_SIZE * 2),
    )

    with pytest.raises(ValueError, match="requested_wire_limit"):
        client.iter_list_counted(
            Request("example.list", route=RouteKind.BARE),
            page_size=PAGE_SIZE,
            offset=wire_limited,
        )
    with pytest.raises(ValueError, match="requested_wire_limit"):
        CountedTraversal(page_size=PAGE_SIZE, offset=wire_limited)
    assert transport.requests == []


@pytest.mark.parametrize("requested_wire_limit", [None, PAGE_SIZE])
def test_ordinary_sequential_stride_rejects_a_decoded_subwindow(
    requested_wire_limit: int | None,
) -> None:
    with pytest.raises(ValueError, match="decoded row cap must equal the wire increment"):
        OffsetSpec(
            limit_path=ParameterPath(("limit",)),
            continuation=OffsetContinuation.FIXED_STEP,
            step=PAGE_SIZE,
            page_stride=PageStride(
                PAGE_SIZE,
                PAGE_SIZE,
                PAGE_SIZE // 2,
                requested_wire_limit=requested_wire_limit,
            ),
        )


def test_page_stride_rejects_an_explicit_subwindow_wire_limit() -> None:
    with pytest.raises(ValueError, match="must cover the wire increment"):
        PageStride(
            server_granularity=PAGE_SIZE,
            wire_increment=PAGE_SIZE * 2,
            max_decoded_rows=PAGE_SIZE,
            requested_wire_limit=PAGE_SIZE,
        )


def test_ordinary_page_stride_rejects_overlapping_decoded_windows() -> None:
    with pytest.raises(ValueError, match="decoded row cap must equal the wire increment"):
        OffsetSpec(
            limit_path=ParameterPath(("limit",)),
            continuation=OffsetContinuation.FIXED_STEP,
            step=PAGE_SIZE,
            total_termination=TotalTermination.EXACT_QUALIFIED,
            page_stride=PageStride(
                server_granularity=PAGE_SIZE,
                wire_increment=PAGE_SIZE,
                max_decoded_rows=PAGE_SIZE * 2,
                requested_wire_limit=PAGE_SIZE * 2,
            ),
        )


@pytest.mark.asyncio
async def test_reference_counted_fixed_step_ignores_relative_next() -> None:
    starts: list[int] = []

    def handler(request: Request) -> WireResponse:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        key, command = next(iter(commands.items()))
        assert isinstance(command, str)
        start = PAGE_SIZE if f"start={PAGE_SIZE}" in command else 0
        starts.append(start)
        if start == 0:
            result, next_value = [{"ID": 1}, {"ID": 2}], 2
        if start == PAGE_SIZE:
            result, next_value = [{"ID": 3}], 1
        return _response(
            {
                "result": {key: result},
                "result_error": {},
                "result_total": {key: 3},
                "result_next": {key: next_value},
            },
        )

    transport = _Transport(handler)
    stream = _client(transport).iter_references(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        [Binding("one", (), object())],
        traversal=CountedTraversal(
            identity=None,
            offset=OffsetSpec(
                continuation=OffsetContinuation.FIXED_STEP,
                step=PAGE_SIZE,
                total_termination=TotalTermination.EXACT_QUALIFIED,
            ),
        ),
        dispatch=BatchDispatch(batch_size=1, concurrency=1),
    )

    outcomes = [item async for item in stream]
    assert [item.item for item in outcomes if isinstance(item, ReferenceItem)] == [
        {"ID": 1},
        {"ID": 2},
        {"ID": 3},
    ]
    assert starts == [0, PAGE_SIZE]


@pytest.mark.asyncio
async def test_counted_fixed_step_accepts_zero_total_terminal_next_zero() -> None:
    stream = _client(_Transport(lambda _request: _response([], total=0, next_value=0))).iter_list_counted(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        identity=None,
        offset=OffsetSpec(
            continuation=OffsetContinuation.FIXED_STEP,
            step=PAGE_SIZE,
            total_termination=TotalTermination.EXACT_QUALIFIED,
        ),
    )

    assert [row async for row in stream] == []
    assert stream.report is not None
    assert stream.report.unique_rows == 0


def test_counted_rejects_unqualified_total_and_non_batchable_request_before_io() -> None:
    transport = _WireTransport(lambda _request: _response([], total=0))
    client = _client(transport)

    with pytest.raises(ValueError, match="exact-qualified"):
        client.iter_list_counted(Request("example.list", route=RouteKind.BARE), offset=OffsetSpec())
    with pytest.raises(CapabilityError, match="JSON requests without scoped headers"):
        client.iter_list_counted(
            Request("example.list", encoding=BodyEncoding.FORM_URLENCODED, route=RouteKind.BARE),
            offset=OffsetSpec(total_termination=TotalTermination.EXACT_QUALIFIED),
        )
    with pytest.raises(CapabilityError, match="JSON requests without scoped headers"):
        client.iter_list_counted(
            Request("example.list", headers=RequestHeaders({"X-Test": "present"}), route=RouteKind.BARE),
            offset=OffsetSpec(total_termination=TotalTermination.EXACT_QUALIFIED),
        )
    for route in (RouteKind.JSON, RouteKind.API_V3):
        with pytest.raises(CapabilityError, match="JSON requests without scoped headers"):
            client.iter_list_counted(
                Request("example.list", route=route),
                offset=OffsetSpec(total_termination=TotalTermination.EXACT_QUALIFIED),
            )

    assert transport.requests == []
    assert transport.wire_requests == []


@pytest.mark.asyncio
async def test_filtered_exact_rejects_minus_one_as_absent_on_nonterminal_page() -> None:
    stream = _client(_Transport(lambda _request: _response([{"ID": 1}], total=-1, next_value=1))).iter_list(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        identity=None,
        offset=OffsetSpec(total_termination=TotalTermination.EXACT_QUALIFIED),
    )

    with pytest.raises(CapabilityError, match="non-negative total"):
        await anext(stream)


@pytest.mark.asyncio
async def test_composite_identity_detects_duplicate_tuple() -> None:
    identity = CompositeIdentitySpec(
        (
            IdentityComponent(("type",), IdentityCoercion.EXACT_INTEGER),
            IdentityComponent(("id",), IdentityCoercion.EXACT_INTEGER),
        ),
    )
    transport = _Transport(lambda _request: _response([{"type": 1, "id": 7}, {"type": 1, "id": 7}]))
    stream = _client(transport).iter_list(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        identity=identity,
    )

    with pytest.raises(IncompleteTraversalError) as captured:
        await anext(stream)

    assert stream.report is not None
    assert stream.report.page_trace[0].outcome is PageOutcome.REJECTED
    assert stream.report.page_trace[0].rows_admitted == 0
    assert isinstance(captured.value.error, PaginationError)
    assert captured.value.to_safe_dict()["cause"] is not None
    assert [violation.code for violation in stream.report.violations if violation.severity.value == "blocking"] == [
        "pagination_invariant",
    ]
    violation = next(item for item in stream.report.violations if item.severity.value == "blocking")
    assert violation.error is captured.value.error
    assert "[pagination_invariant]" in str(captured.value)


@pytest.mark.asyncio
async def test_keyset_page_trace_never_publishes_identity_as_offset() -> None:
    calls = 0

    def handler(_request: Request) -> WireResponse:
        nonlocal calls
        calls += 1
        return _response([{"ID": 900001}]) if calls == 1 else _response([])

    stream = _client(_Transport(handler)).iter_list_keyset(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_INTEGER),
        execution=SequentialKeysetExecution(),
    )

    assert [row async for row in stream] == [{"ID": 900001}]
    assert stream.report is not None
    assert all(record.offset is None for record in stream.report.page_trace)


@pytest.mark.asyncio
async def test_shape_rejection_is_retained_as_zero_admission_page_evidence() -> None:
    stream = _client(_Transport(lambda _request: _response("not-a-mapping"))).iter_list(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        collection_shape=ResultCollectionShape.MAPPING_VALUES,
    )

    with pytest.raises(ResultShapeError) as captured:
        await anext(stream)

    assert captured.value.request_summary is not None
    assert captured.value.request_summary.method == "example.list"
    assert captured.value.page_offset == 0

    assert stream.report is not None
    assert stream.report.page_trace[0].outcome is PageOutcome.REJECTED
    assert stream.report.page_trace[0].rejection_code is PageRejectionCode.SHAPE_CONTRACT
    assert stream.report.page_trace[0].rows_admitted == 0


@pytest.mark.asyncio
async def test_sequence_shape_rejection_has_selector_type_and_page_context() -> None:
    stream = _client(_Transport(lambda _request: _response({"items": {"one": 1}}))).iter_list(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        selector=ResultSelector(("items",)),
    )

    with pytest.raises(ResultShapeError) as captured:
        await anext(stream)

    assert captured.value.selector == ResultSelector(("items",))
    assert captured.value.expected_shape is ResultCollectionShape.SEQUENCE
    assert captured.value.observed_type == "object"
    assert captured.value.page_offset == 0


@pytest.mark.asyncio
async def test_batch_isolates_embedded_shape_failure_and_normalizes_terminal_next() -> None:
    malformed = ResultErrorSpec(ResultSelector.root(), ("code",), shape=ResultErrorShape.MAPPING)

    def handler(request: Request) -> WireResponse:
        keys = tuple(request.copy_parameters()["cmd"])
        return _response(
            {
                "result": {keys[0]: "bad-shape", keys[1]: {"ok": True}},
                "result_error": {},
                "result_next": {keys[1]: -1},
            },
        )

    stream = _client(_Transport(handler)).batch_outcomes(
        [
            Command(
                Request("example.bad", replay_safety=ReplaySafety.SAFE, result_error=malformed, route=RouteKind.BARE),
                None,
            ),
            Command(Request("example.good", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE), None),
        ],
    )
    outcomes = [outcome async for outcome in stream]

    assert isinstance(outcomes[0], CommandFailure)
    assert isinstance(outcomes[0].error, ProtocolError)
    assert outcomes[0].replay_disposition is ReplayDisposition.NOT_ELIGIBLE
    assert isinstance(outcomes[1], CommandSuccess)
    assert outcomes[1].response.next is None


@pytest.mark.asyncio
async def test_tolerant_batch_rejects_unsupported_representation_per_command() -> None:
    def handler(request: Request) -> WireResponse:
        keys = tuple(request.copy_parameters()["cmd"])
        assert len(keys) == 1
        return _response({"result": {keys[0]: {"ok": True}}, "result_error": {}})

    stream = _client(_Transport(handler)).batch_outcomes(
        [
            Command(
                Request(
                    "example.form",
                    replay_safety=ReplaySafety.SAFE,
                    encoding=BodyEncoding.FORM_URLENCODED,
                    route=RouteKind.BARE,
                ),
                "form",
            ),
            Command(Request("example.good", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE), "json"),
        ],
    )
    outcomes = [outcome async for outcome in stream]

    assert isinstance(outcomes[0], CommandFailure)
    assert isinstance(outcomes[0].error, CapabilityError)
    assert outcomes[0].replay_disposition is ReplayDisposition.NOT_ELIGIBLE
    assert isinstance(outcomes[1], CommandSuccess)


@pytest.mark.asyncio
async def test_reference_report_aggregates_page_trace_with_global_sequences() -> None:
    def handler(request: Request) -> WireResponse:
        start = request.copy_parameters().get("start", 0)
        return _response([{"ID": 1}], next_value=1) if start == 0 else _response([])

    stream = _client(_Transport(handler)).iter_reference_outcomes(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        [Binding("one", (), object())],
        traversal=SequentialTraversal(),
        dispatch=DirectDispatch(concurrency=1),
    )

    assert len([outcome async for outcome in stream]) == REFERENCE_OUTCOMES
    assert stream.report is not None
    assert [record.sequence for record in stream.report.page_trace] == [0, 1]
    assert [record.offset for record in stream.report.page_trace] == [0, 1]
    assert [record.reference_index for record in stream.report.page_trace] == [0, 0]


def test_reference_base_control_preflight_does_not_consume_bindings() -> None:
    consumed = False

    def bindings() -> Iterator[Binding[object]]:
        nonlocal consumed
        consumed = True
        yield Binding("one", (), object())

    with pytest.raises(CapabilityError, match="traversal controls"):
        _client(_Transport(lambda _request: _response([]))).iter_references(
            Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
            bindings(),
            traversal=SequentialTraversal(
                offset=OffsetSpec(
                    parameter_path=ParameterPath(("paging", "start")),
                    allow_create_controls=False,
                ),
            ),
            dispatch=DirectDispatch(concurrency=1),
        )

    assert consumed is False


@pytest.mark.parametrize(
    "candidate_request",
    [
        Request("example.list", encoding=BodyEncoding.FORM_URLENCODED, route=RouteKind.BARE),
        Request("example.list", headers=RequestHeaders({"X-Test": "present"}), route=RouteKind.BARE),
    ],
)
def test_reference_transport_preflight_does_not_consume_bindings(candidate_request: Request) -> None:
    consumed = False

    def bindings() -> Iterator[Binding[object]]:
        nonlocal consumed
        consumed = True
        yield Binding("one", (), object())

    transport = _Transport(lambda _request: _response([]))
    with pytest.raises(CapabilityError, match="advanced request delivery"):
        _client(transport).iter_references(
            candidate_request,
            bindings(),
            traversal=SequentialTraversal(),
            dispatch=DirectDispatch(concurrency=1),
        )

    assert consumed is False
    assert transport.requests == []


def test_reference_malformed_capabilities_do_not_consume_async_bindings() -> None:
    consumed = False

    async def bindings() -> AsyncIterator[Binding[object]]:
        nonlocal consumed
        consumed = True
        yield Binding("one", (), object())

    transport = _WireTransport(lambda _request: _response([]))
    transport.capabilities = "malformed"  # type: ignore[assignment]
    with pytest.raises(CapabilityError, match="malformed capabilities"):
        _client(transport).iter_references(
            Request("example.list", route=RouteKind.BARE),
            bindings(),
            traversal=SequentialTraversal(),
            dispatch=DirectDispatch(concurrency=1),
        )

    assert consumed is False
    assert transport.wire_requests == []


@pytest.mark.asyncio
async def test_zero_total_counted_page_completes_in_kernel_and_reference_paths() -> None:
    kernel = iter_list(
        Executor(_Transport(lambda _request: _response([], total=0))),
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        plan=CountedOffsetPlan(),
    )
    assert [row async for row in kernel] == []
    assert kernel.report.completed

    def reference_handler(request: Request) -> WireResponse:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        keys = tuple(commands)
        return _response(
            {
                "result": {key: [] for key in keys},
                "result_error": {},
                "result_total": dict.fromkeys(keys, 0),
            },
        )

    reference = _client(_Transport(reference_handler)).iter_references(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        [Binding("empty", (), object())],
        traversal=CountedTraversal(),
        dispatch=BatchDispatch(batch_size=1),
    )
    outcomes = [outcome async for outcome in reference]
    assert len(outcomes) == 1
    assert reference.report is not None
    assert reference.report.successful
    assert len(reference.report.page_trace) == 1
    assert reference.report.page_trace[0].dispatch is PageDispatch.BATCH
    assert reference.report.page_trace[0].batch_index == 0
    assert reference.report.page_trace[0].reference_index == 0


@pytest.mark.asyncio
async def test_reference_fail_fast_classifies_underlying_pagination_failure() -> None:
    stream = _client(_Transport(lambda _request: _response([{"ID": 7}, {"ID": 7}]))).iter_references(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        [Binding("one", (), object())],
        traversal=SequentialTraversal(identity=IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_INTEGER)),
        dispatch=DirectDispatch(concurrency=1),
    )

    with pytest.raises(ReferenceFailed) as captured:
        await anext(stream)

    assert captured.value.report is not None
    assert any(item.code == "pagination_invariant" for item in captured.value.report.violations)
    assert captured.value.report.terminal_reason == "PaginationError"
    assert "_ReferenceWindowError" not in repr(captured.value.report)
    assert "_PageRejectionError" not in repr(captured.value.report)


@pytest.mark.asyncio
async def test_reference_fail_fast_retains_retryable_replay_disposition_in_report() -> None:
    def handler(request: Request) -> WireResponse:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        key = next(iter(commands))
        return _response(
            {
                "result": {},
                "result_error": {key: {"error": "OPERATION_TIME_LIMIT", "error_description": "wait"}},
            },
        )

    stream = _client(_Transport(handler)).iter_references(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        [Binding("one", (), object())],
        traversal=CountedTraversal(),
        dispatch=BatchDispatch(batch_size=1),
    )

    with pytest.raises(ReferenceFailed) as captured:
        await anext(stream)

    outcome = captured.value.outcomes[0]
    assert isinstance(outcome, ReferenceFailure)
    assert outcome.replay_disposition is ReplayDisposition.ELIGIBLE
    blocking = next(item for item in captured.value.report.violations if item.severity.value == "blocking")
    assert blocking.replay_disposition is ReplayDisposition.ELIGIBLE


@pytest.mark.asyncio
async def test_split_keyset_controls_and_tolerant_mapping_terminal() -> None:
    seen: list[dict[str, object]] = []

    def handler(request: Request) -> WireResponse:
        parameters = request.copy_parameters()
        seen.append(parameters)
        if len(seen) == 1:
            return _response({"one": {"CONFIG_ID": 4}})
        return _response([])

    transport = _Transport(handler)
    stream = _client(transport).iter_list_keyset(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        selector=ResultSelector.root(),
        identity=IdentitySpec(("CONFIG_ID",), "CONFIG_ID", "CONFIG_ID", IdentityCoercion.EXACT_INTEGER),
        collection_shape=ResultCollectionShape.MAPPING_VALUES_OR_EMPTY,
        keyset=KeysetSpec(
            order_path=None,
            split_order=SplitOrderSpec(ParameterPath(("SORT",)), ParameterPath(("ORDER",))),
        ),
        execution=SequentialKeysetExecution(),
    )

    assert [row async for row in stream] == [{"CONFIG_ID": 4}]
    assert seen[0]["SORT"] == "CONFIG_ID"
    assert seen[0]["ORDER"] == "ASC"
    assert stream.report is not None
    assert any(violation.code == "collection_shape_degraded" for violation in stream.report.violations)


def test_unknown_request_collector_is_bounded_but_counts_saturation() -> None:
    collector = UnknownRequestCollector(limit=1)
    collector(Request("one", route=RouteKind.BARE).summary)
    collector(Request("two", route=RouteKind.BARE).summary)
    assert collector.observed == OBSERVED_REQUESTS
    assert [summary.method for summary in collector.summaries] == ["one"]


@pytest.mark.asyncio
async def test_unknown_audit_failure_is_value_free_and_retained_in_stream_report() -> None:
    sensitive_value = "audit-secret-value-123"

    def broken_audit(_summary: object) -> None:
        raise RuntimeError(sensitive_value)

    client = Bitrix24._from_executor(  # noqa: SLF001
        Executor(_Transport(lambda _request: _response([]))),
        unknown_request_audit=broken_audit,
    )
    with pytest.warns(RuntimeWarning, match="audit hook raised RuntimeError"):
        stream = client.iter_list(Request("unknown.list", route=RouteKind.BARE))
    assert [row async for row in stream] == []
    assert stream.report is not None
    violation = next(item for item in stream.report.violations if item.code == "audit_hook_failed")
    assert sensitive_value not in violation.message


def test_php_encoding_supports_percent_bearing_container_keys() -> None:
    assert encode_php_query({"filter": {"%NAME": ["a", "b"]}}) == (
        "filter%5B%25NAME%5D%5B0%5D=a&filter%5B%25NAME%5D%5B1%5D=b"
    )


def test_traversal_control_path_contract_covers_every_public_variant() -> None:
    identity = IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_INTEGER)
    limit = ParameterPath(("limit",))
    cursor_path = ParameterPath(("cursor",))
    cases = (
        (SequentialTraversal(offset=OffsetSpec(limit_path=limit)), {("start",), ("limit",)}),
        (
            CountedTraversal(offset=OffsetSpec(limit_path=limit, total_termination=TotalTermination.EXACT_QUALIFIED)),
            {("start",), ("limit",)},
        ),
        (KeysetTraversal(ResultSelector.root(), identity), {("filter",), ("order",), ("start",)}),
        (
            CursorTraversal(
                ResultSelector.root(),
                CursorSpec(cursor_path, ("ID",), IdentityCoercion.EXACT_INTEGER, "ascending", "last", limit),
            ),
            {("cursor",), ("limit",)},
        ),
    )

    for traversal, expected in cases:
        assert {path.path for path in traversal_control_paths(traversal)} == expected


@pytest.mark.asyncio
async def test_split_order_control_paths_equal_controls_injected_by_deterministic_run() -> None:
    seen: list[dict[str, object]] = []

    def handler(request: Request) -> WireResponse:
        parameters = request.copy_parameters()
        seen.append(parameters)
        return _response([{"ID": 1}]) if len(seen) == 1 else _response([])

    split = SplitOrderSpec(ParameterPath(("SORT",)), ParameterPath(("ORDER",)))
    keyset = KeysetSpec(order_path=None, split_order=split)
    traversal = KeysetTraversal(
        ResultSelector.root(),
        IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_INTEGER),
        keyset=keyset,
    )
    stream = _client(_Transport(handler)).iter_list_keyset(
        Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        selector=traversal.selector,
        identity=traversal.identity,
        keyset=keyset,
        execution=SequentialKeysetExecution(),
    )

    assert [row async for row in stream] == [{"ID": 1}]
    injected_roots = {ParameterPath((name,)) for parameters in seen for name in parameters}
    assert injected_roots == set(traversal_control_paths(traversal))


@pytest.mark.asyncio
async def test_conformance_detects_transport_that_skips_reserved_header_revalidation() -> None:
    class UnsafeWireTransport(_WireTransport):
        async def send_wire(
            self,
            request: WireRequest,
            *,
            attempt_timeout: float,
            max_response_bytes: int,
        ) -> WireResponse:
            del request, attempt_timeout, max_response_bytes
            return WireResponse(200, (), b"{}")

    report = await run_transport_conformance(
        lambda _url: UnsafeWireTransport(lambda _request: _response({"ok": 1})),
        cases=frozenset({ConformanceCase.FORBIDDEN_HEADER_REVALIDATED}),
    )

    assert report.outcomes[0].passed is False


@pytest.mark.asyncio
async def test_unknown_audit_runs_at_each_lazy_fanout_admission_only() -> None:
    collector = UnknownRequestCollector()
    transport = _Transport(lambda _request: _response({"ok": 1}))
    client = Bitrix24._from_executor(Executor(transport), unknown_request_audit=collector)  # noqa: SLF001
    commands = (
        Command(Request("unknown.one", route=RouteKind.BARE), 1),
        Command(Request("safe.one", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE), 2),
        Command(Request("unknown.two", route=RouteKind.BARE), 3),
    )

    stream = client.fan_out_outcomes(commands)
    assert collector.observed == 0
    assert len([outcome async for outcome in stream]) == FANOUT_COMMANDS
    assert collector.observed == OBSERVED_REQUESTS
    assert [summary.method for summary in collector.summaries] == ["unknown.one", "unknown.two"]


@pytest.mark.asyncio
async def test_lazy_audit_failures_are_retained_in_batch_report() -> None:
    def broken_audit(_summary: object) -> None:
        raise RuntimeError("opaque failure")

    client = Bitrix24._from_executor(  # noqa: SLF001
        Executor(_Transport(lambda _request: _response({"ok": 1}))),
        unknown_request_audit=broken_audit,
    )
    stream = client.fan_out_outcomes((Command(Request("unknown.one", route=RouteKind.BARE), 1),))

    with pytest.warns(RuntimeWarning, match="audit hook raised RuntimeError"):
        assert len([outcome async for outcome in stream]) == 1
    assert stream.report is not None
    assert any(item.code == "audit_hook_failed" for item in stream.report.violations)


_ROUNDED_STRIDE = OffsetSpec(
    continuation=OffsetContinuation.FIXED_STEP,
    step=PAGE_SIZE,
    page_stride=PageStride(PAGE_SIZE, PAGE_SIZE, PAGE_SIZE),
)


@pytest.mark.asyncio
async def test_page_stride_rejects_misaligned_initial_offset_before_io() -> None:
    transport = _Transport(lambda _request: pytest.fail("a misaligned stride offset must reject before I/O"))
    direct = _client(transport).iter_list(
        Request("example.list", {"start": 932}, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        page_size=PAGE_SIZE,
        offset=_ROUNDED_STRIDE,
    )
    with pytest.raises(CapabilityError, match="align with the qualified server page granularity"):
        _ = [item async for item in direct]
    assert transport.requests == []
    assert not direct.report.exhausted

    with pytest.raises(CapabilityError, match="align with the qualified server page granularity"):
        _client(transport).iter_reference_outcomes(
            Request("example.list", {"start": 932}, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
            [Binding("one", (), object())],
            traversal=SequentialTraversal(page_size=PAGE_SIZE, offset=_ROUNDED_STRIDE),
            dispatch=DirectDispatch(concurrency=1),
        )
    assert transport.requests == []


@pytest.mark.asyncio
async def test_page_stride_accepts_an_aligned_initial_offset() -> None:
    starts: list[object] = []

    def handler(request: Request) -> WireResponse:
        start = request.copy_parameters()["start"]
        starts.append(start)
        rows = [{"ID": index} for index in range(PAGE_SIZE)] if start == 900 else []  # noqa: PLR2004
        return _response(rows)

    stream = _client(_Transport(handler)).iter_list(
        Request("example.list", {"start": 900}, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        page_size=PAGE_SIZE,
        offset=_ROUNDED_STRIDE,
    )
    rows = [item async for item in stream]
    assert len(rows) == PAGE_SIZE
    assert starts == [900, 900 + PAGE_SIZE]
