"""Keep b24api webhook URLs out of HTTPX INFO and hpack DEBUG records before handler formatting.

A b24api request can emit several records, including redirect hops whose URLs b24api never built,
so the filter scrubs any credential-shaped Bitrix URL segment and sensitive query value, not only the
registered webhook token. Neither task context nor client identity proves ownership: a caller's
response hook or auth flow may send an unrelated request in the same task, even through the same
injected client. A record is therefore attributed to the root of the redirect chain that emits it:
the request the innermost HTTPX ``_send_handling_auth`` is dispatching, which HTTPX never rebinds to a
redirect hop and which a nested send replaces with its own frame. The root is owned only when it is
the very Request object b24api built for this dispatch; its URL, headers and extensions are mutable
by caller auth and hooks, so none of them can prove ownership. An auth flow could otherwise make
lineage undecidable by yielding a fresh request, either a substitute for the owned call or an
unrelated one with the same address, so the owned send runs the client's auth through a guard that
admits only that object itself, mutated in place, and refuses any other request before HTTPX
dispatches it. Records of other roots
keep every byte except the registered webhook secret, which is never logged. If no emitting frame is
found (a changed HTTPX internal), the record is scrubbed conservatively.

HPACK cannot be attributed that way: ``hpack.hpack`` and ``hpack.table`` log Huffman-coded header
blocks, and a connection's table may evict an owned ``:path`` later, while it encodes someone else's
request. While any library HTTPX client is alive, every record of those two loggers is suppressed. An
injected client stays registered, weakly and once, for as long as it is open, because its connections
outlive the short transport that used them; a closed or collected client drops out of the table, and
foreign hpack records are visible again. An HTTP/2-capable send is refused before I/O whenever that
suppression cannot be guaranteed.
"""

from __future__ import annotations
import logging
import re
import sys
import threading
import weakref
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import PurePath
from typing import TYPE_CHECKING

import httpx

from b24api.errors import CapabilityError
from b24api.transport.log_records import redact_owned_value, redact_registered_value, rewrite_record

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterator
    from types import FrameType

_LOGGER = logging.getLogger("httpx")
#: Every hpack logger that can write a header; the shield test pins this list against the installed hpack.
HPACK_LOGGER_NAMES = ("hpack.hpack", "hpack.table")
_HPACK_LOGGERS = tuple(logging.getLogger(name) for name in HPACK_LOGGER_NAMES)
_ACTIVE_OWNERSHIP: ContextVar[LogOwnership | None] = ContextVar(
    "b24api_httpx_ownership",
    default=None,
)
_EMITTING_METHOD = "_send_handling_auth"
_EMITTING_MODULE = ("httpx", "_client.py")
_OWNER_EXTENSION = "b24api_log_owner"
_UNATTRIBUTED = object()
_CREDENTIAL_PATH = re.compile(r"/rest/(?:api/)?[^/]+/([^/]+)/", re.IGNORECASE)
_UNREGISTERED_CLIENT = "HTTP/2 send refused: the HTTPX client cannot be weakly registered for hpack log suppression"
_MISSING_HPACK_FILTER = "HTTP/2 send refused: the hpack log filter was removed from its logger"

type _Client = httpx.AsyncClient | httpx.Client


def webhook_credentials(url: str) -> tuple[str, ...]:
    """Extract the credential segment shared by classic webhook bases and classic and V3 method URLs."""
    match = _CREDENTIAL_PATH.search(url)
    if match is None or not match.group(1):
        raise ValueError("HTTPX log shield requires a credentialed Bitrix method URL")
    return (match.group(1),)


def _emitting_root() -> object:
    """Return the auth-dispatched request whose redirect chain is emitting the current record."""
    frame: FrameType | None = sys._getframe(2)  # noqa: SLF001 - attribution must inspect the synchronous emitting stack
    while frame is not None:
        code = frame.f_code
        if code.co_name == _EMITTING_METHOD and PurePath(code.co_filename).parts[-2:] == _EMITTING_MODULE:
            return frame.f_locals.get("request", _UNATTRIBUTED)
        frame = frame.f_back
    return _UNATTRIBUTED


def _may_negotiate_http2(client: _Client) -> bool:
    """Report whether any of a client's transports could speak HTTP/2; unknown transports might."""
    try:
        transports = (client._transport, *client._mounts.values())  # noqa: SLF001 - pinned HTTPX client layout
        for transport in transports:
            if transport is None or isinstance(transport, httpx.MockTransport):
                continue
            if not isinstance(transport, httpx.AsyncHTTPTransport | httpx.HTTPTransport):
                return True
            if getattr(transport._pool, "_http2", True):  # noqa: SLF001 - pinned httpcore pool layout
                return True
    except AttributeError:
        return True
    return False


class LogOwnership:
    """Recognize the redirect chains of one owned dispatch by the identity of its root request."""

    __slots__ = ("_request", "credentials")

    def __init__(self, url: str) -> None:
        """Bind the owned webhook credential segment."""
        self.credentials = webhook_credentials(url)
        self._request: weakref.ref[httpx.Request] | None = None

    def claim(self, request: httpx.Request) -> None:
        """Bind the exact owned request before it is sent; the extension only labels it for observers."""
        # Weak, so a context copied into a caller task cannot keep the credential-bearing request alive.
        self._request = weakref.ref(request)
        request.extensions[_OWNER_EXTENSION] = self

    def owns(self, request: object) -> bool:
        """Report whether a chain root is the claimed request object, however it was mutated."""
        return self._request is not None and self._request() is request

    def guard(self, auth: httpx.Auth | None) -> httpx.Auth:
        """Wrap the client's auth so the owned send can dispatch nothing but the claimed request."""
        return _OwnedRequestAuth(auth)


class OwnedRequestReplacedError(RuntimeError):
    """An injected client's auth flow yielded a request other than the owned one; it was not sent."""

    def __init__(self, message: str, *, after_response: bool) -> None:
        """Record whether the owned request had already been answered, so dispatch stays accounted."""
        super().__init__(message)
        self.after_response = after_response


class _OwnedRequestAuth(httpx.Auth):
    """Run the client's auth on the owned request, refusing any request it would substitute."""

    def __init__(self, inner: httpx.Auth | None) -> None:
        self._inner = inner

    async def async_auth_flow(self, request: httpx.Request) -> AsyncGenerator[httpx.Request, httpx.Response]:
        if self._inner is None:
            yield request
            return
        flow = self._inner.async_auth_flow(request)
        try:
            try:
                yielded = await flow.__anext__()
            except StopAsyncIteration:
                raise OwnedRequestReplacedError(
                    "injected client auth yielded no request", after_response=False
                ) from None
            answered = False
            while True:
                if yielded is not request:
                    raise OwnedRequestReplacedError(
                        "injected client auth replaced the owned request", after_response=answered
                    )
                response = yield yielded
                answered = True
                try:
                    yielded = await flow.asend(response)
                except StopAsyncIteration:
                    return
        finally:
            await flow.aclose()


class _OwnedRequestFilter(logging.Filter):
    """Rewrite records of an in-flight owned dispatch and its redirect hops; keep the secret out of all others."""

    def __init__(self, registered: Callable[[], tuple[str, ...]]) -> None:
        super().__init__()
        self._registered = registered

    def filter(self, record: logging.LogRecord) -> bool:
        owner = _ACTIVE_OWNERSHIP.get()
        registered = self._registered()
        if owner is None:
            if registered:
                rewrite_record(record, lambda value: redact_registered_value(value, registered), protected=False)
            return True
        root = _emitting_root()
        credentials = tuple(dict.fromkeys((*owner.credentials, *registered)))
        if root is _UNATTRIBUTED or owner.owns(root):
            rewrite_record(record, lambda value: redact_owned_value(value, credentials), protected=True)
        else:
            rewrite_record(record, lambda value: redact_registered_value(value, credentials), protected=False)
        return True


class _HpackRecordFilter(logging.Filter):
    """Drop every hpack record while a library HTTPX client is alive, whatever the record carries."""

    def __init__(self, active: Callable[[], bool]) -> None:
        super().__init__()
        self._active = active

    def filter(self, record: logging.LogRecord) -> bool:
        del record  # the decision never reads the record
        return not self._active()


class _ClientRegistration:
    """One live client's registered secrets; it never refers back to the client it is keyed by."""

    __slots__ = ("credentials", "finalizer")

    def __init__(self) -> None:
        self.credentials: set[str] = set()
        self.finalizer: weakref.finalize[[_ClientRegistration], _Client] | None = None


class HttpxLogShield:
    """Reference-count the process logger filters across transports, clients and in-flight sends."""

    def __init__(self) -> None:
        """Own both filters independently of any transport instance."""
        self._lock = threading.RLock()
        self._filter = _OwnedRequestFilter(self.registered_secrets)
        self._hpack_filter = _HpackRecordFilter(self.suppresses_hpack)
        self._transports = 0
        self._in_flight = 0
        self._clients: weakref.WeakKeyDictionary[_Client, _ClientRegistration] = weakref.WeakKeyDictionary()
        self._secrets: dict[str, int] = {}

    def register_transport(self, *, credentials: tuple[str, ...] = (), client: _Client | None = None) -> bool:
        """Install both filters while a transport can issue requests, before it creates or uses a client.

        Args:
            credentials: Webhook secrets registered for the lifetime of ``client``.
            client: A client that already exists; a transport creating its own binds it afterwards.

        Returns:
            Whether ``client``, when given, could be weakly registered.
        """
        with self._lock:
            self._transports += 1
            self._ensure_installed()
        return True if client is None else self.bind_client(client, credentials=credentials)

    def bind_client(self, client: _Client, *, credentials: tuple[str, ...]) -> bool:
        """Tie hpack suppression and registered secrets to the client's own life, once per client and secret.

        Returns:
            False when the client cannot be weakly referenced or hashed; its HTTP/2 sends are then refused.
        """
        with self._lock:
            try:
                registration = self._clients.get(client)
                if registration is None:
                    registration = _ClientRegistration()
                    self._clients[client] = registration
                    registration.finalizer = weakref.finalize(client, self._forget, registration)
            except TypeError:
                return False
            for credential in credentials:
                if credential not in registration.credentials:
                    registration.credentials.add(credential)
                    self._secrets[credential] = self._secrets.get(credential, 0) + 1
            self._ensure_installed()
            return True

    def release_transport(self) -> None:
        """Remove the filters only after the last transport, request and open client finish."""
        with self._lock:
            if self._transports < 1:
                raise RuntimeError("HTTPX log shield transport underflow")
            self._transports -= 1
            self._remove_if_idle()

    def admit_send(self, client: _Client) -> None:
        """Refuse, before any I/O, an HTTP/2-capable send whose hpack records could escape suppression.

        Raises:
            CapabilityError: The client is not weakly registered, or external code removed the hpack filter.
        """
        if client.is_closed or not _may_negotiate_http2(client):
            # HTTPX itself refuses a closed client before I/O; an HTTP/1.1-only client never runs HPACK.
            return
        with self._lock:
            self._prune_closed()
            try:
                registered = client in self._clients
            except TypeError:
                registered = False
            installed = all(self._hpack_filter in logger.filters for logger in _HPACK_LOGGERS)
        if not registered:
            raise CapabilityError(_UNREGISTERED_CLIENT)
        if not installed:
            raise CapabilityError(_MISSING_HPACK_FILTER)

    def registered_secrets(self) -> tuple[str, ...]:
        """Return each registered webhook secret once."""
        with self._lock:
            return tuple(self._secrets)

    def suppresses_hpack(self) -> bool:
        """Report whether a library HTTPX client is alive, dropping clients that were closed meanwhile."""
        with self._lock:
            self._prune_closed()
            return bool(self._transports or self._in_flight or len(self._clients))

    @contextmanager
    def request(self, url: str) -> Iterator[LogOwnership]:
        """Protect one dispatch; the caller claims its HTTPX request with the yielded marker."""
        ownership = LogOwnership(url)
        with self._lock:
            if self._transports < 1:
                raise RuntimeError("HTTPX log shield has no registered transport")
            self._in_flight += 1
            if self._filter not in _LOGGER.filters:
                _LOGGER.addFilter(self._filter)
        token = _ACTIVE_OWNERSHIP.set(ownership)
        try:
            yield ownership
        finally:
            _ACTIVE_OWNERSHIP.reset(token)
            with self._lock:
                self._in_flight -= 1
                self._remove_if_idle()

    def _forget(self, registration: _ClientRegistration) -> None:
        # Runs on explicit pruning or when the client is collected; it never touches logger filter lists,
        # because collection can interrupt a logger that is iterating them.
        with self._lock:
            for credential in registration.credentials:
                remaining = self._secrets[credential] - 1
                if remaining:
                    self._secrets[credential] = remaining
                else:
                    del self._secrets[credential]
            registration.credentials.clear()

    def _prune_closed(self) -> None:
        for client, registration in list(self._clients.items()):
            if client.is_closed:
                del self._clients[client]
                if registration.finalizer is not None:
                    registration.finalizer()

    def _ensure_installed(self) -> None:
        # The hpack filter is (re)installed only at registration: a filter removed later refuses HTTP/2 sends.
        if self._filter not in _LOGGER.filters:
            _LOGGER.addFilter(self._filter)
        for logger in _HPACK_LOGGERS:
            if self._hpack_filter not in logger.filters:
                logger.addFilter(self._hpack_filter)

    def _remove_if_idle(self) -> None:
        if self._transports or self._in_flight:
            return
        _LOGGER.removeFilter(self._filter)
        self._prune_closed()
        if not self._clients:
            for logger in _HPACK_LOGGERS:
                logger.removeFilter(self._hpack_filter)


HTTPX_LOG_SHIELD = HttpxLogShield()
