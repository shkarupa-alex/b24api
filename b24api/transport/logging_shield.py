"""Keep b24api webhook URLs out of HTTPX INFO records before handler formatting.

While a b24api request is in flight, every record HTTPX emits in that task belongs to it, including
records for redirect hops whose URLs b24api never built. The filter therefore scrubs any
credential-shaped Bitrix URL segment and sensitive query value in those records, not only the
registered webhook token, and leaves records emitted outside an owned request untouched.
"""

from __future__ import annotations
import logging
import re
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING

from b24api.redaction import Redactor

if TYPE_CHECKING:
    from collections.abc import Iterator

_LOGGER = logging.getLogger("httpx")
_ACTIVE_CREDENTIALS: ContextVar[tuple[str, ...] | None] = ContextVar("b24api_httpx_credentials", default=None)
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


class _OwnedRequestFilter(logging.Filter):
    """Rewrite every record emitted while this task sends an owned request, across all redirect hops."""

    def filter(self, record: logging.LogRecord) -> bool:
        credentials = _ACTIVE_CREDENTIALS.get()
        if credentials is None:
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
    def request(self, url: str) -> Iterator[None]:
        """Protect one dispatch without changing unrelated HTTPX logger traffic."""
        with self._lock:
            if self._transports < 1:
                raise RuntimeError("HTTPX log shield has no registered transport")
            self._in_flight += 1
            self._ensure_installed()
        token = _ACTIVE_CREDENTIALS.set(_credentials(url))
        try:
            yield
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
