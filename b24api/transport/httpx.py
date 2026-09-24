"""Transport lifecycle, replay-aware retries, and shared rate coordination."""

from __future__ import annotations
import asyncio
import json
import secrets
import uuid
import weakref
from typing import TYPE_CHECKING, cast

import httpx

from b24api._error_types import FailurePhase
from b24api.contracts.request_summary import RouteKind
from b24api.contracts.wire import BodyEncoding, _validate_headers
from b24api.encoding import encode_php_query
from b24api.errors import (
    B24ApiError,
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
from b24api.transport.logging_shield import HTTPX_LOG_SHIELD, OwnedRequestReplacedError, webhook_credentials

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from b24api.contracts.request import Request
    from b24api.transport.logging_shield import LogOwnership


def _webhook_vault() -> tuple[Callable[[str], str], Callable[[str], str], Callable[[str], None]]:
    entries: dict[str, tuple[bytes, bytes]] = {}

    def store(webhook_url: str) -> str:
        handle = uuid.uuid4().hex
        plaintext = webhook_url.encode()
        key = secrets.token_bytes(len(plaintext))
        entries[handle] = (bytes(left ^ right for left, right in zip(plaintext, key, strict=True)), key)
        return handle

    def fetch(handle: str) -> str:
        try:
            ciphertext, key = entries[handle]
        except KeyError as error:
            # Refused before any I/O: nothing reached the portal, so the request is not possibly accepted.
            raise TransportError(
                "transport credential is unavailable", phase=FailurePhase.NOT_DISPATCHED, retryable=False
            ) from error
        return bytes(left ^ right for left, right in zip(ciphertext, key, strict=True)).decode()

    def drop(handle: str) -> None:
        entries.pop(handle, None)

    return store, fetch, drop


_store_webhook, _webhook_for, _drop_webhook = _webhook_vault()
_CLASSIC_WEBHOOK_PARTS = 3


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


def _normalized_webhook_host(webhook_url: str) -> str:
    parsed = httpx.URL(webhook_url)
    if parsed.host is None:
        raise ValueError("webhook URL must contain a host")
    parts = parsed.path.strip("/").split("/")
    if (
        len(parts) != _CLASSIC_WEBHOOK_PARTS
        or parts[0] != "rest"
        or not all(parts[1:])
        or not parsed.path.endswith("/")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("webhook URL must be a classic /rest/user/token/ base")
    return parsed.host


_REPLACED_OWNED_REQUEST = "Injected client auth replaced the owned request; only in-place auth is supported"


def _method_url(webhook_url: str, request: WireRequest) -> str:
    """Resolve an explicit route only at dispatch, keeping credential out of public values."""
    parsed = httpx.URL(webhook_url)
    path = parsed.path
    if request.route is RouteKind.API_V3:
        parts = path.strip("/").split("/")
        if len(parts) != _CLASSIC_WEBHOOK_PARTS or parts[0] != "rest" or not all(parts[1:]):
            raise ValueError("API_V3 requires a classic /rest/user/token/ webhook base")
        path = f"/rest/api/{parts[1]}/{parts[2]}/"
    suffix = ".json" if request.route is RouteKind.JSON else ""
    return str(parsed.copy_with(path=f"{path}{request.method}{suffix}"))


def _closed_refusal() -> TransportError:
    """A closed transport refuses before any I/O; the request never left the process."""
    return TransportError("transport is closed", phase=FailurePhase.NOT_DISPATCHED, retryable=False)


class HttpxTransport:
    """HTTPX transport with conservative failure-phase classification."""

    capabilities = TransportCapabilities(
        encodings=frozenset({BodyEncoding.JSON, BodyEncoding.FORM_URLENCODED}),
        scoped_headers=True,
        positional_json=True,
        routes=frozenset(RouteKind),
    )

    def __init__(self, webhook_url: str, *, client: httpx.AsyncClient | None = None) -> None:
        """Initialize instance state."""
        if not webhook_url.endswith("/"):
            webhook_url += "/"
        normalized_host = _normalized_webhook_host(webhook_url)
        # Both log filters are installed before a client exists or an injected one is registered.
        HTTPX_LOG_SHIELD.register_transport()
        shield_finalizer = weakref.finalize(self, HTTPX_LOG_SHIELD.release_transport)
        resolved_client = client
        client_initialization_failed = False
        if resolved_client is None:
            try:
                resolved_client = httpx.AsyncClient(http2=True)
            except Exception:  # noqa: BLE001 - sanitize environment/proxy constructor failures
                client_initialization_failed = True
        normalized_webhook = webhook_url
        webhook_url = ""
        if client_initialization_failed:
            normalized_webhook = ""
            shield_finalizer()
            raise RuntimeError("HTTP client initialization failed")
        self._webhook_handle = _store_webhook(normalized_webhook)
        self._webhook_finalizer = weakref.finalize(self, _drop_webhook, self._webhook_handle)
        self._client = cast("httpx.AsyncClient", resolved_client)
        self._owns_client = client is None
        self._closed = False
        self._host = normalized_host
        self._shield_finalizer = shield_finalizer
        # Keyed by the client, not this transport: an injected client's connections outlive the transport.
        # A client that cannot be weakly registered gets its HTTP/2 sends refused by ``admit_send``.
        HTTPX_LOG_SHIELD.bind_client(self._client, credentials=webhook_credentials(normalized_webhook))
        normalized_webhook = ""

    @property
    def host(self) -> str:
        """Return the normalized portal host without credentials."""
        return self._host

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
        """Protect the emitting HTTPX logger for one owned request."""
        if self._closed:
            raise _closed_refusal()
        HTTPX_LOG_SHIELD.admit_send(self._client)
        method_url = _method_url(_webhook_for(self._webhook_handle), request)
        try:
            with HTTPX_LOG_SHIELD.request(method_url) as ownership:
                return await self._send_wire_impl(
                    request,
                    method_url=method_url,
                    ownership=ownership,
                    attempt_timeout=attempt_timeout,
                    max_response_bytes=max_response_bytes,
                )
        finally:
            method_url = ""

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

    async def _send_wire_impl(  # noqa: C901, PLR0912, PLR0915
        self,
        request: WireRequest,
        *,
        method_url: str,
        ownership: LogOwnership,
        attempt_timeout: float,
        max_response_bytes: int,
    ) -> WireResponse:
        """Send one explicitly represented transport request attempt."""
        if self._closed:
            raise _closed_refusal()
        if isinstance(max_response_bytes, bool) or max_response_bytes < 1:
            raise ValueError("max_response_bytes must be a positive integer")
        tracker = _PhaseTracker()
        failure: tuple[str, FailurePhase] | None = None
        cancellation_args: tuple[object, ...] | None = None
        http_request: httpx.Request | None = None
        try:
            request_headers = self._with_bounded_accept_encoding(dict(_validate_headers(request.headers.items)))
            if request.encoding is BodyEncoding.JSON:
                request_headers["content-type"] = "application/json"
                if request.positional is not None:
                    content = json.dumps(
                        request.positional.to_wire_slots(),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ).encode()
                    http_request = self._client.build_request(
                        "POST",
                        method_url,
                        headers=request_headers,
                        content=content,
                    )
                else:
                    http_request = self._client.build_request(
                        "POST",
                        method_url,
                        headers=request_headers,
                        json=request.copy_parameters(),
                    )
            elif request.encoding is BodyEncoding.FORM_URLENCODED:
                request_headers["content-type"] = "application/x-www-form-urlencoded"
                content = encode_php_query(cast("Mapping[str | int, object]", request.copy_parameters())).encode()
                http_request = self._client.build_request(
                    "POST",
                    method_url,
                    headers=request_headers,
                    content=content,
                )
            else:  # pragma: no cover - guarded by typed contracts/capabilities
                raise TypeError("unsupported body encoding")
            method_url = ""
            ownership.claim(http_request)
            http_request.extensions["trace"] = tracker
            http_request.extensions["timeout"] = {
                "connect": attempt_timeout,
                "read": attempt_timeout,
                "write": attempt_timeout,
                "pool": attempt_timeout,
            }
            response = await self._client.send(http_request, stream=True, auth=ownership.guard(self._client.auth))
        except asyncio.CancelledError as error:
            cancellation_args = error.args
            http_request = None
        except OwnedRequestReplacedError as error:
            # Only in-place auth flows keep the owned request's lineage provable; a substitute is never sent.
            answered = error.after_response
            failure = (
                _REPLACED_OWNED_REQUEST,
                _at_least_dispatch_started(tracker.phase) if answered else tracker.phase,
            )
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
            failure = ("Transport failed before dispatch", tracker.phase)
        except (httpx.WriteError, httpx.WriteTimeout):
            failure = (
                "Transport failed during or after possible request dispatch",
                _at_least_dispatch_started(tracker.phase),
            )
        except (httpx.ReadError, httpx.ReadTimeout, httpx.RemoteProtocolError):
            failure = (
                "Transport failed after possible request dispatch",
                _at_least_dispatch_started(tracker.phase),
            )
        except httpx.TransportError:
            failure = (
                "Unclassified transport failure after possible dispatch",
                _at_least_dispatch_started(tracker.phase),
            )
        except httpx.RequestError:
            failure = (
                "HTTP client request failed after possible dispatch",
                _at_least_dispatch_started(tracker.phase),
            )
        if cancellation_args is not None:
            # Raise outside the handler so neither HTTPX traceback frames nor
            # their credential-bearing request locals remain reachable.
            raise asyncio.CancelledError(*cancellation_args)
        if failure is not None:
            # Do not retain httpx's credential-bearing request on an exception
            # chain or in the outgoing traceback frame's local variables.
            http_request = None
            message, phase = failure
            raise TransportError(
                message,
                phase=phase,
                request_summary=request.summary,
                retryable=message != _REPLACED_OWNED_REQUEST,
            )
        pending_error: B24ApiError | None = None
        body_outcome = BodyReadOutcome()
        try:
            status_code = response.status_code
            if not _HTTP_STATUS_MINIMUM <= status_code <= _HTTP_STATUS_MAXIMUM:
                pending_error = ProtocolError("HTTP response status is outside the valid range")
                response_headers: tuple[tuple[str, str], ...] = ()
            else:
                response_headers = tuple(response.headers.multi_items())
                tracker.phase = FailurePhase.BODY_PARTIALLY_RECEIVED
                body_outcome = await read_bounded_body(response, max_response_bytes)
        finally:
            await response.aclose()
            del response
            http_request = None
        if body_outcome.cancellation_args is not None:
            raise asyncio.CancelledError(*body_outcome.cancellation_args)
        if body_outcome.transport_failure is not None:
            raise TransportError(
                body_outcome.transport_failure,
                phase=tracker.phase,
                request_summary=request.summary,
            )
        if body_outcome.too_large:
            raise ResponseTooLargeError(
                "Decompressed response body exceeded the configured byte ceiling",
                request_summary=request.summary,
            )
        if pending_error is not None:
            raise pending_error
        tracker.phase = FailurePhase.RESPONSE_COMPLETE
        return WireResponse(status_code=status_code, headers=response_headers, body=cast("bytes", body_outcome.body))

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


def _httpx_default_accept_encoding() -> str:
    """Return the value HTTPX puts on a client given none; it grows ``br``/``zstd`` once those codecs are installed."""
    # Read from the pinned HTTPX, like the shield's ``_send_handling_auth`` attribution.
    return httpx._client.ACCEPT_ENCODING  # noqa: SLF001 - tell HTTPX's default from a configured value


def _at_least_dispatch_started(phase: FailurePhase) -> FailurePhase:
    if phase in {FailurePhase.NOT_DISPATCHED, FailurePhase.CONNECTION_ESTABLISHED}:
        return FailurePhase.DISPATCH_STARTED
    return phase
