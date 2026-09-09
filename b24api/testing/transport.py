"""Dependency-free conformance runner for public transport contracts."""

from __future__ import annotations
import asyncio
import contextlib
import inspect
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum

from b24api.contracts.request import Request
from b24api.contracts.wire import BodyEncoding, RequestHeaders
from b24api.errors import ResponseTooLargeError
from b24api.transport.base import Transport, TransportCapabilities, WireRequest, WireResponse, WireTransport

type TransportFactory = Callable[[str], Transport | Awaitable[Transport]]
_HTTP_OK = 200


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
    captures: asyncio.Queue[_Capture] = asyncio.Queue()

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
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
            await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    base_url = f"http://127.0.0.1:{port}/"
    outcomes: list[ConformanceOutcome] = []
    try:
        for case in ConformanceCase:
            if case not in selected:
                continue
            transport_value = factory(base_url)
            transport = await transport_value if inspect.isawaitable(transport_value) else transport_value
            try:
                outcomes.append(await _run_case(case, transport, captures))
            except Exception as error:  # noqa: BLE001 - report type only, never exception values
                outcomes.append(ConformanceOutcome(case, passed=False, detail=type(error).__name__))
            finally:
                close = getattr(transport, "aclose", None)
                if close is not None:
                    await close()
    finally:
        server.close()
        await server.wait_closed()
    return ConformanceReport(tuple(outcomes))


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
        safe_wire_request = WireRequest(Request("repr.test", parameters={"value": sensitive_value}))
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
        forged = WireRequest(Request("header.test"))
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
            transport.send(Request("cancel.test"), attempt_timeout=5, max_response_bytes=1024),
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
            await transport.send(Request("limit.test"), attempt_timeout=5, max_response_bytes=1)
        except ResponseTooLargeError:
            await captures.get()
            return ConformanceOutcome(case, passed=True)
        _discard_capture(captures)
        return ConformanceOutcome(case, passed=False)

    request = Request("conformance.test", parameters={"plain": "a b"})
    expected_body: bytes | None = None
    expected_header: tuple[str, str] | None = None
    if case is ConformanceCase.FORM_ENCODING_EXACT:
        request = Request(
            "conformance.test",
            parameters={"plain": "a b", "flag": False},
            encoding=BodyEncoding.FORM_URLENCODED,
        )
        expected_body = b"plain=a+b&flag=0"
    elif case is ConformanceCase.FORM_NESTED_BRACKETS:
        request = Request(
            "conformance.test",
            parameters={"outer": {"items": ["x", None]}},
            encoding=BodyEncoding.FORM_URLENCODED,
        )
        expected_body = b"outer%5Bitems%5D%5B0%5D=x"
    elif case is ConformanceCase.SCOPED_HEADERS_FORWARDED:
        request = Request("conformance.test", headers=RequestHeaders({"X-Conformance": "present"}))
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
