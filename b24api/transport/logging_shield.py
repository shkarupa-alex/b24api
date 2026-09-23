"""Keep b24api webhook URLs out of HTTPX INFO records before handler formatting.

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

from b24api.redaction import Redactor

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator
    from types import FrameType

_LOGGER = logging.getLogger("httpx")
_ACTIVE_OWNERSHIP: ContextVar[LogOwnership | None] = ContextVar(
    "b24api_httpx_ownership",
    default=None,
)
_EMITTING_METHOD = "_send_handling_auth"
_EMITTING_MODULE = ("httpx", "_client.py")
_OWNER_EXTENSION = "b24api_log_owner"
_UNATTRIBUTED = object()
# Fields logging itself sets; everything else on a record arrived through ``extra``.
_STANDARD_RECORD_FIELDS = frozenset(
    (*logging.LogRecord("", logging.INFO, "", 0, "", None, None).__dict__, "message", "asctime"),
)
_REPLACEMENT = "[REDACTED]"
_CREDENTIAL_PATH = re.compile(r"/rest/(?:api/)?[^/]+/([^/]+)/", re.IGNORECASE)
# Full-length scrubbing: truncating a record's format string would break its %-interpolation.
_RECORD_REDACTOR = Redactor(max_string=1 << 20)
_HOP_CREDENTIAL = re.compile(
    r"(?P<prefix>/rest/(?:api/)?[^/\s?#\"']+/)(?P<token>[^/\s?#\"']+)(?=[/?#\s\"']|$)",
    re.IGNORECASE,
)


def _credentials(url: str) -> tuple[str, ...]:
    """Extract the credential segment shared by classic and V3 method URLs."""
    match = _CREDENTIAL_PATH.search(url)
    if match is None or not match.group(1):
        raise ValueError("HTTPX log shield requires a credentialed Bitrix method URL")
    return (match.group(1),)


def _redact_registered_value(value: object, credentials: tuple[str, ...]) -> object:
    """Remove only the registered secret from a foreign record field, leaving it otherwise untouched."""
    rendered = str(value)
    if not any(credential in rendered for credential in credentials):
        return value
    for credential in credentials:
        rendered = rendered.replace(credential, _REPLACEMENT)
    return rendered


def _redact_owned_value(value: object, credentials: tuple[str, ...]) -> object:
    """Scrub one field of an owned record, keeping its original type when nothing is secret."""
    rendered = str(value)
    scrubbed = rendered
    for credential in credentials:
        scrubbed = scrubbed.replace(credential, _REPLACEMENT)
    scrubbed = _HOP_CREDENTIAL.sub(lambda match: match.group("prefix") + _REPLACEMENT, scrubbed)
    scrubbed = _RECORD_REDACTOR.redact_text(scrubbed)
    return value if scrubbed == rendered else scrubbed


def _emitting_root() -> object:
    """Return the auth-dispatched request whose redirect chain is emitting the current record."""
    frame: FrameType | None = sys._getframe(2)  # noqa: SLF001 - attribution must inspect the synchronous emitting stack
    while frame is not None:
        code = frame.f_code
        if code.co_name == _EMITTING_METHOD and PurePath(code.co_filename).parts[-2:] == _EMITTING_MODULE:
            return frame.f_locals.get("request", _UNATTRIBUTED)
        frame = frame.f_back
    return _UNATTRIBUTED


class LogOwnership:
    """Recognize the redirect chains of one owned dispatch by the identity of its root request."""

    __slots__ = ("_request", "credentials")

    def __init__(self, url: str) -> None:
        """Bind the owned webhook credential segment."""
        self.credentials = _credentials(url)
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

    def filter(self, record: logging.LogRecord) -> bool:
        owner = _ACTIVE_OWNERSHIP.get()
        if owner is None:
            return True
        root = _emitting_root()
        redact = _redact_owned_value if root is _UNATTRIBUTED or owner.owns(root) else _redact_registered_value
        credentials = owner.credentials
        args = record.args
        record.msg = redact(record.msg, credentials)
        if isinstance(args, dict):
            record.args = {key: redact(value, credentials) for key, value in args.items()}
        elif isinstance(args, tuple):
            record.args = tuple(redact(value, credentials) for value in args)
        # Any caller-supplied extra (a hook's ``hop_url``, instrumentation fields) can carry a URL.
        for name, value in tuple(record.__dict__.items()):
            if name not in _STANDARD_RECORD_FIELDS:
                record.__dict__[name] = redact(value, credentials)
        return True


class HttpxLogShield:
    """Reference-count one process logger filter across transports and in-flight sends."""

    def __init__(self) -> None:
        """Own one filter independently of any transport instance."""
        self._lock = threading.RLock()
        self._filter = _OwnedRequestFilter()
        self._transports = 0
        self._in_flight = 0

    def register_transport(self) -> None:
        """Keep the filter installed while a transport can issue requests."""
        with self._lock:
            self._transports += 1
            self._ensure_installed()

    def release_transport(self) -> None:
        """Remove the filter only after the last transport and request finish."""
        with self._lock:
            if self._transports < 1:
                raise RuntimeError("HTTPX log shield transport underflow")
            self._transports -= 1
            self._remove_if_idle()

    @contextmanager
    def request(self, url: str) -> Iterator[LogOwnership]:
        """Protect one dispatch; the caller claims its HTTPX request with the yielded marker."""
        ownership = LogOwnership(url)
        with self._lock:
            if self._transports < 1:
                raise RuntimeError("HTTPX log shield has no registered transport")
            self._in_flight += 1
            self._ensure_installed()
        token = _ACTIVE_OWNERSHIP.set(ownership)
        try:
            yield ownership
        finally:
            _ACTIVE_OWNERSHIP.reset(token)
            with self._lock:
                self._in_flight -= 1
                self._remove_if_idle()

    def _ensure_installed(self) -> None:
        if self._filter not in _LOGGER.filters:
            _LOGGER.addFilter(self._filter)

    def _remove_if_idle(self) -> None:
        if self._transports == 0 and self._in_flight == 0:
            _LOGGER.removeFilter(self._filter)


HTTPX_LOG_SHIELD = HttpxLogShield()
