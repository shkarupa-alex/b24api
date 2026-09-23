"""Dependency-free conformance runner for public transport contracts."""

from __future__ import annotations
import asyncio
import contextlib
import inspect
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from functools import partial
from types import MappingProxyType

from b24api.contracts.request import Request, RouteKind
from b24api.contracts.wire import BodyEncoding, RequestHeaders
from b24api.errors import ResponseTooLargeError
from b24api.testing._isolation import (
    IsolationAbortError,
    IsolationController,
    IsolationDeadlineError,
    run_isolated,
)
from b24api.transport.base import Transport, TransportCapabilities, WireRequest, WireResponse, WireTransport

type TransportFactory = Callable[[str], Transport | Awaitable[Transport]]
_HTTP_OK = 200
_CASE_TIMEOUT_SECONDS = 6.0
_CLOSE_TIMEOUT_SECONDS = 1.0


class ConformanceCase(StrEnum):
    """Permanent transport-boundary conformance cases."""

    LEGACY_JSON_BODY = "legacy_json_body"
    JSON_CONTENT_TYPE = "json_content_type"
    FORM_ENCODING_EXACT = "form_encoding_exact"
    FORM_NESTED_BRACKETS = "form_nested_brackets"
    SCOPED_HEADERS_FORWARDED = "scoped_headers_forwarded"
    FORBIDDEN_HEADER_REVALIDATED = "forbidden_header_revalidated"
    RESPONSE_BYTE_CEILING = "response_byte_ceiling"
    RESPONSE_STATUS_RANGE = "response_status_range"
    WIRE_RESPONSE_IMMUTABLE = "wire_response_immutable"
    REPR_REDACTION = "repr_redaction"
    HOST_PROPERTY_CREDENTIAL_FREE = "host_property_credential_free"
    CANCELLATION_PROPAGATES = "cancellation_propagates"
    ROUTE_JSON_SUFFIX = "route_json_suffix"
    ROUTE_API_V3_REBASE = "route_api_v3_rebase"


# A declared route must reach its own endpoint on the conformance webhook ``/rest/1/token/``.
_CASE_ROUTES = MappingProxyType(
    {
        ConformanceCase.ROUTE_JSON_SUFFIX: RouteKind.JSON,
        ConformanceCase.ROUTE_API_V3_REBASE: RouteKind.API_V3,
    }
)
_ROUTE_PATHS = MappingProxyType(
    {
        RouteKind.JSON: "/rest/1/token/conformance.test.json",
        RouteKind.API_V3: "/rest/api/1/token/conformance.test",
    }
)


@dataclass(frozen=True, slots=True)
class ConformanceOutcome:
    """Value-free outcome for one conformance case."""

    case: ConformanceCase
    passed: bool
    skipped: bool = False
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class ConformanceReport:
    """Immutable aggregate conformance result."""

    outcomes: tuple[ConformanceOutcome, ...]

    @property
    def failures(self) -> tuple[ConformanceOutcome, ...]:
        """Return non-skipped failures."""
        return tuple(outcome for outcome in self.outcomes if not outcome.passed and not outcome.skipped)

    @property
    def passed(self) -> bool:
        """Return whether every executed case passed."""
        return not self.failures


@dataclass(slots=True)
class _Capture:
    request_line: str
    headers: dict[str, str]
    body: bytes


async def run_transport_conformance(
    factory: TransportFactory,
    *,
    cases: frozenset[ConformanceCase] | None = None,
) -> ConformanceReport:
    """Exercise a transport against a temporary loopback HTTP/1.1 responder."""
    selected = frozenset(ConformanceCase) if cases is None else frozenset(cases)
    if any(not isinstance(case, ConformanceCase) for case in selected):
        raise TypeError("cases must contain ConformanceCase values")
    outcomes: list[ConformanceOutcome] = []
    for case in ConformanceCase:
        if case not in selected:
            continue
        try:
            outcomes.append(
                await run_isolated(
                    partial(_run_case_environment, case, factory),
                    case_seconds=_CASE_TIMEOUT_SECONDS,
                ),
            )
        except (IsolationAbortError, IsolationDeadlineError) as error:
            outcomes.append(ConformanceOutcome(case, passed=False, detail=error.detail))
        except Exception as error:  # noqa: BLE001 - report type only, never exception values
            outcomes.append(ConformanceOutcome(case, passed=False, detail=type(error).__name__))
    return ConformanceReport(tuple(outcomes))


async def _run_case_environment(
    case: ConformanceCase,
    factory: TransportFactory,
    controller: IsolationController,
) -> ConformanceOutcome:
    """Own one case's event loop, responder, captures, and transport."""
    captures: asyncio.Queue[_Capture] = asyncio.Queue()

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            with contextlib.suppress(asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
                head = await reader.readuntil(b"\r\n\r\n")
                lines = head.decode("latin-1").split("\r\n")
                request_line = lines[0]
                headers = {
                    name.strip().casefold(): value.strip()
                    for line in lines[1:]
                    if ":" in line
                    for name, value in (line.split(":", 1),)
                }
                length = int(headers.get("content-length", "0"))
                body = await reader.readexactly(length) if length else b""
                await captures.put(_Capture(request_line, headers, body))
                if "cancel.test" in request_line:
                    await reader.read()
                    return
                response_body = b'{"result":true}'
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                    + str(len(response_body)).encode()
                    + b"\r\nConnection: close\r\n\r\n"
                    + response_body,
                )
                await writer.drain()
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        return await _run_owned_case(case, factory, f"http://127.0.0.1:{port}/rest/1/token/", captures, controller)
    finally:
        server.close()
        await server.wait_closed()


async def _run_owned_case(  # noqa: C901, PLR0912 - preserve the first lifecycle failure
    case: ConformanceCase,
    factory: TransportFactory,
    base_url: str,
    captures: asyncio.Queue[_Capture],
    controller: IsolationController,
) -> ConformanceOutcome:
    """Create and exercise one transport, then close it under a separate deadline."""
    deadline = asyncio.get_running_loop().time() + _CASE_TIMEOUT_SECONDS
    transport_value = factory(base_url)
    transport = (
        await _await_bounded(
            transport_value,
            seconds=max(0.0, deadline - asyncio.get_running_loop().time()),
            detail="CaseDeadlineExceeded",
        )
        if inspect.isawaitable(transport_value)
        else transport_value
    )
    outcome: ConformanceOutcome | None = None
    failure: BaseException | None = None
    try:
        outcome = await _await_bounded(
            _run_case(case, transport, captures),
            seconds=max(0.0, deadline - asyncio.get_running_loop().time()),
            detail="CaseDeadlineExceeded",
        )
    except asyncio.CancelledError as error:
        failure = error
    except Exception as error:  # noqa: BLE001 - report type only, never exception values
        failure = error
    except BaseException as error:  # noqa: BLE001 - isolate untrusted aborts
        failure = IsolationAbortError(type(error).__name__)
    finally:
        controller.enter_cleanup(_CLOSE_TIMEOUT_SECONDS, preceding_detail=_failure_detail(failure))
        close = getattr(transport, "aclose", None)
        if close is not None:
            try:
                close_value = close()
                if inspect.isawaitable(close_value):
                    await _await_bounded(
                        close_value,
                        seconds=_CLOSE_TIMEOUT_SECONDS,
                        detail="CleanupDeadlineExceeded",
                    )
            except asyncio.CancelledError as error:
                if failure is None:
                    failure = error
            except Exception as error:  # noqa: BLE001 - report type only, never exception values
                if failure is None:
                    failure = error
            except BaseException as error:  # noqa: BLE001 - isolate untrusted aborts
                if failure is None:
                    failure = IsolationAbortError(type(error).__name__)
    if failure is not None:
        raise failure
    if outcome is None:  # pragma: no cover - defensive invariant
        raise RuntimeError("conformance case produced no outcome")
    return outcome


def _failure_detail(error: BaseException | None) -> str | None:
    """Return the value-free detail that cleanup must not mask."""
    if isinstance(error, IsolationAbortError | IsolationDeadlineError):
        return error.detail
    return type(error).__name__ if error is not None else None


async def _await_bounded[T](awaitable: Awaitable[T], *, seconds: float, detail: str) -> T:
    """Await extension code without waiting indefinitely for cancellation cooperation."""
    task = asyncio.ensure_future(awaitable)
    try:
        done, _ = await asyncio.wait({task}, timeout=seconds)
    except asyncio.CancelledError:
        task.cancel()
        task.add_done_callback(_consume_detached_task)
        raise
    if task in done:
        return task.result()
    task.cancel()
    task.add_done_callback(_consume_detached_task)
    raise IsolationDeadlineError(detail)


def _consume_detached_task[T](task: asyncio.Future[T]) -> None:
    """Retrieve any eventual exception from cancellation-resistant extension code."""
    if not task.done():
        return
    with contextlib.suppress(BaseException):
        task.result()


async def _run_case(  # noqa: C901, PLR0911, PLR0912, PLR0915
    case: ConformanceCase,
    transport: Transport,
    captures: asyncio.Queue[_Capture],
) -> ConformanceOutcome:
    capabilities = transport.capabilities if isinstance(transport, WireTransport) else TransportCapabilities()
    if (
        case
        in {
            ConformanceCase.FORM_ENCODING_EXACT,
            ConformanceCase.FORM_NESTED_BRACKETS,
        }
        and BodyEncoding.FORM_URLENCODED not in capabilities.encodings
    ):
        return ConformanceOutcome(case, passed=True, skipped=True)
    if case is ConformanceCase.SCOPED_HEADERS_FORWARDED and not capabilities.scoped_headers:
        return ConformanceOutcome(case, passed=True, skipped=True)
    route = _CASE_ROUTES.get(case, RouteKind.BARE)
    if route not in capabilities.routes:
        return ConformanceOutcome(case, passed=True, skipped=True)
    if case is ConformanceCase.RESPONSE_STATUS_RANGE:
        try:
            WireResponse(99, (), b"")
        except ValueError:
            return ConformanceOutcome(case, passed=True)
        return ConformanceOutcome(case, passed=False)
    if case is ConformanceCase.WIRE_RESPONSE_IMMUTABLE:
        body = bytearray(b"ok")
        response = WireResponse(200, (), bytes(body))
        body[:] = b"no"
        return ConformanceOutcome(case, passed=response.body == b"ok")
    if case is ConformanceCase.REPR_REDACTION:
        sensitive_value = "conformance-sensitive-value"
        response = WireResponse(200, (), sensitive_value.encode())
        safe_wire_request = WireRequest(
            Request("repr.test", parameters={"value": sensitive_value}, route=RouteKind.BARE),
        )
        return ConformanceOutcome(
            case,
            passed=sensitive_value not in repr(response) and sensitive_value not in repr(safe_wire_request),
        )
    if case is ConformanceCase.HOST_PROPERTY_CREDENTIAL_FREE:
        return ConformanceOutcome(case, passed="@" not in transport.host and "/" not in transport.host)
    if case is ConformanceCase.FORBIDDEN_HEADER_REVALIDATED:
        if not isinstance(transport, WireTransport):
            return ConformanceOutcome(case, passed=True, skipped=True)
        forged_headers = RequestHeaders()
        object.__setattr__(forged_headers, "items", (("authorization", "secret"),))
        forged = WireRequest(Request("header.test", route=RouteKind.BARE))
        object.__setattr__(forged, "headers", forged_headers)
        try:
            await transport.send_wire(forged, attempt_timeout=5, max_response_bytes=1024)
        except ValueError:
            _discard_capture(captures)
            return ConformanceOutcome(case, passed=True)
        _discard_capture(captures)
        return ConformanceOutcome(case, passed=False)
    if case is ConformanceCase.CANCELLATION_PROPAGATES:
        task = asyncio.create_task(
            transport.send(Request("cancel.test", route=RouteKind.BARE), attempt_timeout=5, max_response_bytes=1024),
        )
        await captures.get()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return ConformanceOutcome(case, passed=True)
        return ConformanceOutcome(case, passed=False)
    if case is ConformanceCase.RESPONSE_BYTE_CEILING:
        try:
            await transport.send(Request("limit.test", route=RouteKind.BARE), attempt_timeout=5, max_response_bytes=1)
        except ResponseTooLargeError:
            await captures.get()
            return ConformanceOutcome(case, passed=True)
        _discard_capture(captures)
        return ConformanceOutcome(case, passed=False)

    request = Request("conformance.test", parameters={"plain": "a b"}, route=route)
    expected_body: bytes | None = None
    expected_header: tuple[str, str] | None = None
    if case is ConformanceCase.FORM_ENCODING_EXACT:
        request = Request(
            "conformance.test",
            parameters={"plain": "a b", "flag": False},
            encoding=BodyEncoding.FORM_URLENCODED,
            route=RouteKind.BARE,
        )
        expected_body = b"plain=a+b&flag=0"
    elif case is ConformanceCase.FORM_NESTED_BRACKETS:
        request = Request(
            "conformance.test",
            parameters={"outer": {"items": ["x", None]}},
            encoding=BodyEncoding.FORM_URLENCODED,
            route=RouteKind.BARE,
        )
        expected_body = b"outer%5Bitems%5D%5B0%5D=x"
    elif case is ConformanceCase.SCOPED_HEADERS_FORWARDED:
        request = Request(
            "conformance.test",
            headers=RequestHeaders({"X-Conformance": "present"}),
            route=RouteKind.BARE,
        )
        expected_header = ("x-conformance", "present")
    response = (
        await transport.send(request, attempt_timeout=5, max_response_bytes=1024)
        if case is ConformanceCase.LEGACY_JSON_BODY or not isinstance(transport, WireTransport)
        else await transport.send_wire(WireRequest(request), attempt_timeout=5, max_response_bytes=1024)
    )
    capture = await captures.get()
    passed = response.status_code == _HTTP_OK
    if case is ConformanceCase.LEGACY_JSON_BODY:
        try:
            passed = passed and json.loads(capture.body) == {"plain": "a b"}
        except (json.JSONDecodeError, UnicodeDecodeError):
            passed = False
    elif case is ConformanceCase.JSON_CONTENT_TYPE:
        passed = passed and capture.headers.get("content-type") == "application/json"
    elif expected_body is not None:
        passed = passed and capture.body == expected_body
    elif expected_header is not None:
        passed = passed and capture.headers.get(expected_header[0]) == expected_header[1]
    elif case in _CASE_ROUTES:
        passed = passed and capture.request_line.split(" ")[1] == _ROUTE_PATHS[route]
    return ConformanceOutcome(case, passed=passed)


def _discard_capture(captures: asyncio.Queue[_Capture]) -> None:
    """Discard one unexpected capture without blocking later cases."""
    with contextlib.suppress(asyncio.QueueEmpty):
        captures.get_nowait()


__all__ = [
    "ConformanceCase",
    "ConformanceOutcome",
    "ConformanceReport",
    "TransportFactory",
    "run_transport_conformance",
]
