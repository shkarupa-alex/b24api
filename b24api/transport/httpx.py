"""Transport lifecycle, replay-aware retries, and shared rate coordination."""

from __future__ import annotations
import asyncio
import secrets
import uuid
import weakref
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import httpx

from b24api.errors import (
    B24ApiError,
    FailurePhase,
    ProtocolError,
    ResponseTooLargeError,
    TransportError,
)
from b24api.transport.base import _HTTP_STATUS_MAXIMUM, _HTTP_STATUS_MINIMUM, WireResponse

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from b24api.contracts.request import Request


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
            raise RuntimeError("transport credential is unavailable") from error
        return bytes(left ^ right for left, right in zip(ciphertext, key, strict=True)).decode()

    def drop(handle: str) -> None:
        entries.pop(handle, None)

    return store, fetch, drop


_store_webhook, _webhook_for, _drop_webhook = _webhook_vault()


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


@dataclass(frozen=True, slots=True)
class _BodyReadOutcome:
    body: bytes | None = None
    cancellation_args: tuple[object, ...] | None = None
    transport_failure: str | None = None
    too_large: bool = False


async def _read_bounded_body(response: httpx.Response, maximum: int) -> _BodyReadOutcome:
    """Consume decoded bytes without propagating credential-bearing HTTPX exceptions."""
    body = bytearray()
    try:
        async for chunk in response.aiter_bytes():
            if len(body) + len(chunk) > maximum:
                return _BodyReadOutcome(too_large=True)
            body.extend(chunk)
    except asyncio.CancelledError as error:
        return _BodyReadOutcome(cancellation_args=error.args)
    except (httpx.ReadError, httpx.ReadTimeout, httpx.RemoteProtocolError, httpx.DecodingError):
        return _BodyReadOutcome(transport_failure="Transport failed while reading the response body")
    except httpx.TransportError:
        return _BodyReadOutcome(transport_failure="Unclassified transport failure while reading the response body")
    except httpx.RequestError:
        return _BodyReadOutcome(transport_failure="HTTP client failed while reading the response body")
    return _BodyReadOutcome(body=bytes(body))


def _normalized_webhook_host(webhook_url: str) -> str:
    parsed = httpx.URL(webhook_url)
    if parsed.host is None:
        raise ValueError("webhook URL must contain a host")
    return parsed.host


class HttpxTransport:
    """HTTPX transport with conservative failure-phase classification."""

    def __init__(self, webhook_url: str, *, client: httpx.AsyncClient | None = None) -> None:
        """Initialize instance state."""
        if not webhook_url.endswith("/"):
            webhook_url += "/"
        normalized_host = _normalized_webhook_host(webhook_url)
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
            raise RuntimeError("HTTP client initialization failed")
        self._webhook_handle = _store_webhook(normalized_webhook)
        self._webhook_finalizer = weakref.finalize(self, _drop_webhook, self._webhook_handle)
        self._client = cast("httpx.AsyncClient", resolved_client)
        self._owns_client = client is None
        self._closed = False
        self._host = normalized_host

    @property
    def host(self) -> str:
        """Return the normalized portal host without credentials."""
        return self._host

    async def send(  # noqa: C901, PLR0912, PLR0915
        self,
        request: Request,
        *,
        attempt_timeout: float,
        max_response_bytes: int,
    ) -> WireResponse:
        """Send one transport request attempt."""
        if self._closed:
            raise RuntimeError("transport is closed")
        if isinstance(max_response_bytes, bool) or max_response_bytes < 1:
            raise ValueError("max_response_bytes must be a positive integer")
        tracker = _PhaseTracker()
        failure: tuple[str, FailurePhase] | None = None
        cancellation_args: tuple[object, ...] | None = None
        http_request: httpx.Request | None = None
        try:
            http_request = self._client.build_request(
                "POST",
                f"{_webhook_for(self._webhook_handle)}{request.method}",
                headers={"Content-Type": "application/json"},
                json=request.to_wire_parameters(),
            )
            http_request.extensions["trace"] = tracker
            http_request.extensions["timeout"] = {
                "connect": attempt_timeout,
                "read": attempt_timeout,
                "write": attempt_timeout,
                "pool": attempt_timeout,
            }
            response = await self._client.send(http_request, stream=True)
        except asyncio.CancelledError as error:
            cancellation_args = error.args
            http_request = None
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
            raise TransportError(message, phase=phase, request_summary=request.summary)
        pending_error: B24ApiError | None = None
        body_outcome = _BodyReadOutcome()
        try:
            status_code = response.status_code
            if not _HTTP_STATUS_MINIMUM <= status_code <= _HTTP_STATUS_MAXIMUM:
                pending_error = ProtocolError("HTTP response status is outside the valid range")
                headers: tuple[tuple[str, str], ...] = ()
            else:
                headers = tuple(response.headers.multi_items())
                tracker.phase = FailurePhase.BODY_PARTIALLY_RECEIVED
                body_outcome = await _read_bounded_body(response, max_response_bytes)
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
        return WireResponse(status_code=status_code, headers=headers, body=cast("bytes", body_outcome.body))

    async def aclose(self) -> None:
        """Close owned asynchronous resources."""
        if self._closed:
            return
        self._closed = True
        try:
            if self._owns_client:
                await self._client.aclose()
        finally:
            self._webhook_finalizer()


def _at_least_dispatch_started(phase: FailurePhase) -> FailurePhase:
    if phase in {FailurePhase.NOT_DISPATCHED, FailurePhase.CONNECTION_ESTABLISHED}:
        return FailurePhase.DISPATCH_STARTED
    return phase
