"""Keep b24api webhook URLs out of HTTPX INFO records before handler formatting.

A b24api request can emit several records, including redirect hops whose URLs b24api never built,
so the filter scrubs any credential-shaped Bitrix URL segment and sensitive query value, not only the
registered webhook token. Neither task context nor client identity proves ownership: a caller's
response hook or auth flow may send an unrelated request in the same task, even through the same
injected client, and an auth flow may yield unrelated requests of its own. A record is therefore
attributed to the root of the redirect chain that emits it: the request the innermost HTTPX
``_send_handling_auth`` is dispatching. HTTPX rebinds that root to each request the auth flow yields
but never to a redirect hop, and a nested send opens its own frame. The root is owned when it
carries this dispatch's marker, holds the owned webhook credential, or retries the owned portal
operation (same host and method path, whatever user and token segments it carries), which covers
an auth-flow substitute of the owned request even under a rotated credential; roots to any other
address stay untouched. If no such frame is found (a changed HTTPX internal), the record is
scrubbed conservatively.
"""

from __future__ import annotations
import logging
import re
import sys
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import PurePath
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from b24api.redaction import Redactor

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import FrameType

    import httpx

_LOGGER = logging.getLogger("httpx")
_ACTIVE_OWNERSHIP: ContextVar[LogOwnership | None] = ContextVar(
    "b24api_httpx_ownership",
    default=None,
)
_EMITTING_METHOD = "_send_handling_auth"
_EMITTING_MODULE = ("httpx", "_client.py")
_OWNER_EXTENSION = "b24api_log_owner"
_UNATTRIBUTED = object()
_REPLACEMENT = "[REDACTED]"
_CREDENTIAL_PATH = re.compile(r"/rest/(?:api/)?[^/]+/([^/]+)/", re.IGNORECASE)
_CREDENTIAL_SEGMENTS = re.compile(r"(?P<prefix>/rest/(?:api/)?)[^/]+/[^/]+/", re.IGNORECASE)
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


def _operation(url: str) -> tuple[str, int | None, str]:
    """Identify a portal operation independently of the user and token segments of its URL."""
    parts = urlsplit(url)
    path = _CREDENTIAL_SEGMENTS.sub(r"\g<prefix>*/*/", parts.path, count=1)
    return ((parts.hostname or "").lower(), parts.port, path.lower())


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
    """Recognize the redirect chains of one owned dispatch, including auth-flow substitutes."""

    __slots__ = ("credentials", "operation")

    def __init__(self, url: str) -> None:
        """Bind the owned webhook credential segment and portal operation."""
        self.credentials = _credentials(url)
        self.operation = _operation(url)

    def claim(self, request: httpx.Request) -> None:
        """Attach this ownership marker before the request is sent."""
        request.extensions[_OWNER_EXTENSION] = self

    def owns(self, request: object) -> bool:
        """Report whether a chain root is marked, holds the owned credential or retries the owned operation."""
        extensions = getattr(request, "extensions", None)
        if isinstance(extensions, dict) and extensions.get(_OWNER_EXTENSION) is self:
            return True
        url = str(getattr(request, "url", ""))
        match = _CREDENTIAL_PATH.search(url)
        if match is not None and match.group(1) in self.credentials:
            return True
        return match is not None and _operation(url) == self.operation


class _OwnedRequestFilter(logging.Filter):
    """Rewrite records of an in-flight owned dispatch, across auth substitutes and redirect hops."""

    def filter(self, record: logging.LogRecord) -> bool:
        owner = _ACTIVE_OWNERSHIP.get()
        if owner is None:
            return True
        root = _emitting_root()
        if root is not _UNATTRIBUTED and not owner.owns(root):
            return True
        credentials = owner.credentials
        args = record.args
        record.msg = _redact_owned_value(record.msg, credentials)
        if isinstance(args, dict):
            record.args = {key: _redact_owned_value(value, credentials) for key, value in args.items()}
        elif isinstance(args, tuple):
            record.args = tuple(_redact_owned_value(value, credentials) for value in args)
        for name in ("url", "request_url"):
            if name in record.__dict__:
                record.__dict__[name] = _redact_owned_value(record.__dict__[name], credentials)
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
