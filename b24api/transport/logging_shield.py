"""Keep b24api webhook URLs out of HTTPX INFO records before handler formatting.

A b24api request can emit several records, including redirect hops whose URLs b24api never built,
so the filter scrubs any credential-shaped Bitrix URL segment and sensitive query value, not only the
registered webhook token. Neither task context nor client identity proves ownership: a caller's
response hook or auth flow may send an unrelated request in the same task, even through the same
injected client. Nor does the request that reaches the wire: an auth flow may replace it, and a
redirect builds a new one. Each owned request therefore carries a unique marker, and a record is
attributed to the request passed to the innermost HTTPX ``send`` on the emitting stack. Auth
replacements and redirect hops run inside that call; an unrelated nested request opens its own. If
no such frame is found (a changed HTTPX internal), the record is scrubbed conservatively.
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

from b24api.redaction import Redactor

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import FrameType

    import httpx

_LOGGER = logging.getLogger("httpx")
_ACTIVE_CREDENTIALS: ContextVar[tuple[tuple[str, ...], LogOwnership] | None] = ContextVar(
    "b24api_httpx_credentials",
    default=None,
)
_EMITTING_METHOD = "send"
_EMITTING_MODULE = ("httpx", "_client.py")
_OWNER_EXTENSION = "b24api_log_owner"
_UNATTRIBUTED = object()
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


def _redact_owned_value(value: object, credentials: tuple[str, ...]) -> object:
    """Scrub one field of an owned record, keeping its original type when nothing is secret."""
    rendered = str(value)
    scrubbed = rendered
    for credential in credentials:
        scrubbed = scrubbed.replace(credential, _REPLACEMENT)
    scrubbed = _HOP_CREDENTIAL.sub(lambda match: match.group("prefix") + _REPLACEMENT, scrubbed)
    scrubbed = _RECORD_REDACTOR.redact_text(scrubbed)
    return value if scrubbed == rendered else scrubbed


def _emitting_request() -> object:
    """Return the request passed to the innermost HTTPX client send that is emitting the current record."""
    frame: FrameType | None = sys._getframe(2)  # noqa: SLF001 - attribution must inspect the synchronous emitting stack
    while frame is not None:
        code = frame.f_code
        if code.co_name == _EMITTING_METHOD and PurePath(code.co_filename).parts[-2:] == _EMITTING_MODULE:
            return frame.f_locals.get("request", _UNATTRIBUTED)
        frame = frame.f_back
    return _UNATTRIBUTED


class LogOwnership:
    """Mark one owned HTTPX request so records emitted anywhere inside its send are recognized."""

    __slots__ = ()

    def claim(self, request: httpx.Request) -> None:
        """Attach this ownership marker before the request is sent."""
        request.extensions[_OWNER_EXTENSION] = self

    def owns(self, request: object) -> bool:
        """Report whether a sent request carries this exact marker."""
        extensions = getattr(request, "extensions", None)
        return isinstance(extensions, dict) and extensions.get(_OWNER_EXTENSION) is self


class _OwnedRequestFilter(logging.Filter):
    """Rewrite records emitted for an in-flight owned request, across all redirect hops."""

    def filter(self, record: logging.LogRecord) -> bool:
        active = _ACTIVE_CREDENTIALS.get()
        if active is None:
            return True
        credentials, owner = active
        emitted = _emitting_request()
        if emitted is not _UNATTRIBUTED and not owner.owns(emitted):
            return True
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
        with self._lock:
            if self._transports < 1:
                raise RuntimeError("HTTPX log shield has no registered transport")
            self._in_flight += 1
            self._ensure_installed()
        ownership = LogOwnership()
        token = _ACTIVE_CREDENTIALS.set((_credentials(url), ownership))
        try:
            yield ownership
        finally:
            _ACTIVE_CREDENTIALS.reset(token)
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
