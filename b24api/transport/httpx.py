"""Transport lifecycle, replay-aware retries, and shared rate coordination."""

from __future__ import annotations
import asyncio
import contextlib
import json
import weakref
from typing import TYPE_CHECKING, cast

import httpx

from b24api._error_types import FailurePhase
from b24api._webhook import TokenVault, Webhook, WebhookRefusal, parse_webhook
from b24api.contracts.evidence import ResponseEvidence
from b24api.contracts.request_summary import RequestSummary, RouteKind
from b24api.contracts.wire import BodyEncoding, _validate_headers
from b24api.encoding import encode_php_query
from b24api.errors import (
    ClientClosedError,
    ProtocolError,
    ResponseTooLargeError,
    TransportError,
)
from b24api.transport.base import (
    _HTTP_STATUS_MAXIMUM,
    _HTTP_STATUS_MINIMUM,
    TransportCapabilities,
    WireRequest,
    WireResponse,
)
from b24api.transport.decoding import BOUNDED_ACCEPT_ENCODING, BodyReadOutcome, read_bounded_body
from b24api.transport.logging_shield import HTTPX_LOG_SHIELD, OwnedRequestReplacedError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from b24api.contracts.json import JsonValue
    from b24api.contracts.request import Request


_TOKENS = TokenVault()


def _token_for(handle: str) -> str:
    token = _TOKENS.fetch(handle)
    if token is None:
        # Refused before any I/O: nothing reached the portal, so the request is not possibly accepted.
        raise TransportError("transport credential is unavailable", phase=FailurePhase.NOT_DISPATCHED, retryable=False)
    return token


class _PhaseTracker:
    """Translate httpcore trace events into the last conclusive lifecycle phase."""

    def __init__(self) -> None:
        self.phase = FailurePhase.NOT_DISPATCHED

    async def __call__(self, event_name: str, _info: Mapping[str, object]) -> None:
        if event_name.endswith(("connect_tcp.complete", "start_tls.complete")):
            self.phase = FailurePhase.CONNECTION_ESTABLISHED
        elif ".send_request_" in event_name and event_name.endswith(".started"):
            self.phase = FailurePhase.DISPATCH_STARTED
        elif event_name.endswith("receive_response_headers.complete"):
            self.phase = FailurePhase.HEADERS_RECEIVED
        elif event_name.endswith("receive_response_body.started"):
            self.phase = FailurePhase.BODY_PARTIALLY_RECEIVED


_REPLACED_OWNED_REQUEST = "Injected client auth replaced the owned request; only in-place auth is supported"


def _method_url(webhook: Webhook, request: WireRequest) -> str:
    """Resolve an explicit route only at dispatch, keeping credential out of public values."""
    base = webhook.api_v3_url if request.route is RouteKind.API_V3 else webhook.url
    suffix = ".json" if request.route is RouteKind.JSON else ""
    return f"{base}{request.method}{suffix}"


def _closed_refusal(*, transport_closed: bool) -> ClientClosedError:
    """A closed transport or injected client refuses before any I/O; the request never left the process."""
    return ClientClosedError("transport is closed" if transport_closed else "HTTP client is closed")


class HttpxTransport:
    """HTTPX transport with conservative failure-phase classification."""

    capabilities = TransportCapabilities(
        encodings=frozenset({BodyEncoding.JSON, BodyEncoding.FORM_URLENCODED}),
        scoped_headers=True,
        positional_json=True,
        routes=frozenset(RouteKind),
        json_array_body=True,
    )

    def __init__(self, webhook_url: str, *, client: httpx.AsyncClient | None = None) -> None:
        """Initialize instance state.

        Every refusal is raised after this frame's copies of the webhook are cleared, so a traceback rendered
        with its frames' locals shows no part of the URL.
        """
        if not isinstance(webhook_url, str):
            webhook_url = ""
            raise TypeError("webhook URL must be a string")
        # The one parse Settings uses too; the webhook's ``repr`` leaves the token out.
        parsed = parse_webhook(webhook_url)
        webhook_url = ""
        match parsed:
            case WebhookRefusal():
                raise ValueError(parsed.value)
            case Webhook():
                webhook = parsed
        # Both log filters are installed before a client exists or an injected one is registered.
        HTTPX_LOG_SHIELD.register_transport()
        shield_finalizer = weakref.finalize(self, HTTPX_LOG_SHIELD.release_transport)
        resolved_client = client
        if resolved_client is None:
            # Environment and proxy failures may quote the webhook; the refusal below carries none of it.
            with contextlib.suppress(Exception):
                resolved_client = httpx.AsyncClient(http2=True)
        if resolved_client is None:
            shield_finalizer()
            raise RuntimeError("HTTP client initialization failed")
        # Only the token is a secret: the vault holds it, and the rest of the webhook is a plain attribute.
        self._webhook_handle = _TOKENS.store(webhook.token)
        self._webhook_finalizer = weakref.finalize(self, _TOKENS.drop, self._webhook_handle)
        self._client = resolved_client
        self._owns_client = client is None
        self._closed = False
        self._address = webhook.address
        self._shield_finalizer = shield_finalizer
        # Keyed by the client, not this transport: an injected client's connections outlive the transport.
        # A client that cannot be weakly registered gets its HTTP/2 sends refused by ``admit_send``.
        HTTPX_LOG_SHIELD.bind_client(self._client, credentials=(webhook.token,))

    @property
    def host(self) -> str:
        """Return the normalized portal host without credentials."""
        return self._address.host

    async def send(
        self,
        request: Request,
        *,
        attempt_timeout: float,
        max_response_bytes: int,
    ) -> WireResponse:
        """Send one transport request attempt."""
        return await self.send_wire(
            WireRequest(request),
            attempt_timeout=attempt_timeout,
            max_response_bytes=max_response_bytes,
        )

    async def send_wire(
        self,
        request: WireRequest,
        *,
        attempt_timeout: float,
        max_response_bytes: int,
    ) -> WireResponse:
        """Send one owned request attempt under the HTTPX log shield.

        The scoped headers are validated here, once: a forged wire request is refused with
        ``ValueError`` before the credential is even read.
        """
        if self._closed or self._client.is_closed:
            raise _closed_refusal(transport_closed=self._closed)
        if isinstance(max_response_bytes, bool) or max_response_bytes < 1:
            raise ValueError("max_response_bytes must be a positive integer")
        HTTPX_LOG_SHIELD.admit_send(self._client)
        headers = _validate_headers(request.headers.items)
        return await self._send_wire_impl(
            request,
            headers=headers,
            webhook=Webhook(self._address, _token_for(self._webhook_handle)),
            attempt_timeout=attempt_timeout,
            max_response_bytes=max_response_bytes,
        )

    def _with_bounded_accept_encoding(self, headers: dict[str, str]) -> dict[str, str]:
        """Advertise only what the bounded decoder can inflate.

        A caller's header, or one an injected client was configured with, is kept; a coding it admits
        is then refused on arrival.
        """
        if "accept-encoding" not in headers and (
            self._owns_client or self._client.headers.get("accept-encoding") == _httpx_default_accept_encoding()
        ):
            headers["accept-encoding"] = BOUNDED_ACCEPT_ENCODING
        return headers

    def _build_request(
        self, request: WireRequest, headers: tuple[tuple[str, str], ...], method_url: str
    ) -> httpx.Request:
        """Build the owned HTTPX request from validated headers; it holds the credential-bearing URL."""
        request_headers = self._with_bounded_accept_encoding(dict(headers))
        if request.encoding is BodyEncoding.JSON:
            request_headers["content-type"] = "application/json"
            if (json_body := _json_array_content(request)) is not None:
                content = json.dumps(json_body, ensure_ascii=False, separators=(",", ":")).encode()
                return self._client.build_request("POST", method_url, headers=request_headers, content=content)
            return self._client.build_request(
                "POST", method_url, headers=request_headers, json=request.copy_parameters()
            )
        if request.encoding is BodyEncoding.FORM_URLENCODED:
            request_headers["content-type"] = "application/x-www-form-urlencoded"
            content = encode_php_query(cast("Mapping[str | int, object]", request.copy_parameters())).encode()
            return self._client.build_request("POST", method_url, headers=request_headers, content=content)
        raise TypeError("unsupported body encoding")  # pragma: no cover - guarded by typed contracts/capabilities

    async def _send_wire_impl(
        self,
        request: WireRequest,
        *,
        headers: tuple[tuple[str, str], ...],
        webhook: Webhook,
        attempt_timeout: float,
        max_response_bytes: int,
    ) -> WireResponse:
        """Build, send and read one request; no exception raised inside the HTTP client leaves this frame.

        Each failure is classified by :func:`_client_failure` and raised outside its handler, so neither
        that exception, its request, nor their credential-bearing URL appears in any rendering of what
        the caller sees: the error, its chain, its traceback or the ``repr`` of this frame's locals.
        """
        tracker = _PhaseTracker()
        http_request: httpx.Request | None = None
        outcome: WireResponse | BaseException
        with HTTPX_LOG_SHIELD.request(credentials=(webhook.token,)) as ownership:
            try:
                # HTTPX refuses some values only while building, a header it cannot encode for example. The
                # method URL carries the token, so no local of this frame holds it.
                http_request = self._build_request(request, headers, _method_url(webhook, request))
                ownership.claim(http_request)
                http_request.extensions["trace"] = tracker
                timeouts = dict.fromkeys(("connect", "read", "write", "pool"), attempt_timeout)
                http_request.extensions["timeout"] = timeouts
                response = await self._client.send(http_request, stream=True, auth=ownership.guard(self._client.auth))
            except asyncio.CancelledError as error:
                outcome = asyncio.CancelledError(*error.args)
            except Exception as error:  # noqa: BLE001 - every escape is classified, never chained
                outcome = _client_failure(error, tracker.phase, request.summary, built=http_request is not None)
            else:
                outcome = await _read_response(response, tracker, max_response_bytes, request.summary)
                del response
        http_request = None
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def aclose(self) -> None:
        """Close owned asynchronous resources."""
        if self._closed:
            return
        self._closed = True
        try:
            if self._owns_client:
                await self._client.aclose()
        finally:
            try:
                self._webhook_finalizer()
            finally:
                self._shield_finalizer()


def _json_array_content(request: WireRequest) -> list[JsonValue] | None:
    """Return a JSON body sent as a top-level array: a v3 batch envelope or exact positional slots."""
    if request.array_body is not None:
        # Native REST v3 batch: a top-level JSON array of {method, query}, in input order.
        return request.array_body.to_wire()
    if request.positional is not None:
        return request.positional.to_wire_slots()
    return None


# HTTPX request failures by the latest step they may follow; whether the request may have been sent.
_HTTPX_SEND_FAILURES: tuple[tuple[tuple[type[httpx.RequestError], ...], str, bool], ...] = (
    ((httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout), "Transport failed before dispatch", False),
    ((httpx.WriteError, httpx.WriteTimeout), "Transport failed during or after possible request dispatch", True),
    (
        (httpx.ReadError, httpx.ReadTimeout, httpx.RemoteProtocolError),
        "Transport failed after possible request dispatch",
        True,
    ),
    ((httpx.TransportError,), "Unclassified transport failure after possible dispatch", True),
    ((httpx.RequestError,), "HTTP client request failed after possible dispatch", True),
)


def _client_failure(error: Exception, phase: FailurePhase, summary: RequestSummary, *, built: bool) -> BaseException:
    """Classify an exception raised inside the HTTP client into what the transport raises instead.

    One rule for every stage, building, sending and reading. Only the exception's class and a
    received status are read, never its text, request or headers. A request that could not be built
    was never sent, and building it again would fail again. HTTPX's request errors describe I/O and
    are retried by the phase they may follow. Anything else (an event hook's ``raise_for_status()``,
    any exception of a hook, an auth flow, a custom transport or response stream, a ``B24ApiError``
    raised inside the client) is code that would fail the same way again, so it is not retried; it
    may follow dispatch. A builtin ``TimeoutError`` keeps the executor's deadline classification, as
    the executor boundary keeps it for any transport.
    """
    if not built:
        message = f"HTTP request could not be built ({type(error).__name__}); nothing was sent"
        return TransportError(message, phase=phase, request_summary=summary, retryable=False)
    if isinstance(error, TimeoutError):
        return TimeoutError()
    if isinstance(error, OwnedRequestReplacedError):
        # Only in-place auth flows keep the owned request's lineage provable; a substitute is never sent.
        replaced_phase = _at_least_dispatch_started(phase) if error.after_response else phase
        return TransportError(_REPLACED_OWNED_REQUEST, phase=replaced_phase, request_summary=summary, retryable=False)
    for classes, message, possibly_sent in _HTTPX_SEND_FAILURES:
        if isinstance(error, classes):
            failed_phase = _at_least_dispatch_started(phase) if possibly_sent else phase
            return TransportError(message, phase=failed_phase, request_summary=summary)
    # Read from the instance only, so no property of a foreign exception runs here.
    response = vars(error).get("response")
    status = response.status_code if isinstance(response, httpx.Response) else None
    received = "" if status is None else f" with HTTP status {status}"
    return TransportError(
        f"HTTP client raised {type(error).__name__}{received} after possible dispatch",
        phase=_at_least_dispatch_started(phase),
        request_summary=summary,
        evidence=ResponseEvidence(http_status=status),
        retryable=False,
    )


async def _read_response(
    response: httpx.Response, tracker: _PhaseTracker, maximum: int, summary: RequestSummary
) -> WireResponse | BaseException:
    """Read status, headers and the bounded body, then release the connection; return what to raise instead."""
    outcome: WireResponse | BaseException
    try:
        status_code = response.status_code
        if not _HTTP_STATUS_MINIMUM <= status_code <= _HTTP_STATUS_MAXIMUM:
            outcome = ProtocolError("HTTP response status is outside the valid range")
        else:
            headers = tuple(response.headers.multi_items())
            tracker.phase = FailurePhase.BODY_PARTIALLY_RECEIVED
            outcome = _body_outcome(await read_bounded_body(response, maximum), status_code, headers, tracker, summary)
    except Exception as error:  # noqa: BLE001 - classified like a send failure, never chained
        outcome = _client_failure(error, tracker.phase, summary, built=True)
    finally:
        cancellation_args = await _release(response)
        del response
    if cancellation_args is not None and not isinstance(outcome, asyncio.CancelledError):
        return asyncio.CancelledError(*cancellation_args)
    return outcome


def _body_outcome(
    body: BodyReadOutcome,
    status_code: int,
    headers: tuple[tuple[str, str], ...],
    tracker: _PhaseTracker,
    summary: RequestSummary,
) -> WireResponse | BaseException:
    """Turn a bounded body read into the response, or into what reading it must raise."""
    if body.cancellation_args is not None:
        return asyncio.CancelledError(*body.cancellation_args)
    if body.transport_failure is not None:
        return TransportError(body.transport_failure, phase=tracker.phase, request_summary=summary)
    if body.too_large:
        return ResponseTooLargeError(
            "Decompressed response body exceeded the configured byte ceiling", request_summary=summary
        )
    tracker.phase = FailurePhase.RESPONSE_COMPLETE
    return WireResponse(status_code=status_code, headers=headers, body=cast("bytes", body.body))


async def _release(response: httpx.Response) -> tuple[object, ...] | None:
    """Close the response, returning the arguments of a cancellation meanwhile.

    It is the only close, since the body is read from the stream directly, and it runs once the outcome
    is decided, a complete body included. Its failure may carry the webhook URL and changes no outcome,
    so it is dropped; a cancellation is replayed by the caller without the HTTPX frames it was raised in.
    """
    try:
        with contextlib.suppress(Exception):
            await response.aclose()
    except asyncio.CancelledError as error:
        return error.args
    return None


def _httpx_default_accept_encoding() -> str:
    """Return the value HTTPX puts on a client given none; it grows ``br``/``zstd`` once those codecs are installed."""
    # Read from the pinned HTTPX, like the shield's ``_send_handling_auth`` attribution.
    return httpx._client.ACCEPT_ENCODING  # noqa: SLF001 - tell HTTPX's default from a configured value


def _at_least_dispatch_started(phase: FailurePhase) -> FailurePhase:
    if phase in {FailurePhase.NOT_DISPATCHED, FailurePhase.CONNECTION_ESTABLISHED}:
        return FailurePhase.DISPATCH_STARTED
    return phase
