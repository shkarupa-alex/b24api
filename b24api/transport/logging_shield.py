"""Keep b24api webhook URLs out of HTTPX INFO records before handler formatting."""

from __future__ import annotations
import logging
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING

from b24api.redaction import DEFAULT_REDACTOR

if TYPE_CHECKING:
    from collections.abc import Iterator

_LOGGER = logging.getLogger("httpx")
_ACTIVE_URL: ContextVar[str | None] = ContextVar("b24api_httpx_url", default=None)
_REPLACEMENT = "[REDACTED]"


def _redact_owned_url(value: object, url: str) -> object:
    rendered = str(value)
    if isinstance(value, str) or url in rendered:
        return DEFAULT_REDACTOR.redact_text(rendered.replace(url, _REPLACEMENT))
    return value


class _OwnedRequestFilter(logging.Filter):
    """Rewrite only a record emitted while this task sends its registered URL."""

    def filter(self, record: logging.LogRecord) -> bool:
        url = _ACTIVE_URL.get()
        if url is None:
            return True
        args = record.args
        values = args.values() if isinstance(args, dict) else args if isinstance(args, tuple) else ()
        if url not in str(record.msg) and not any(url in str(value) for value in values):
            return True
        record.msg = _redact_owned_url(record.msg, url)
        if isinstance(args, dict):
            record.args = {key: _redact_owned_url(value, url) for key, value in args.items()}
        elif isinstance(args, tuple):
            record.args = tuple(_redact_owned_url(value, url) for value in args)
        for name in ("url", "request_url"):
            if name in record.__dict__:
                record.__dict__[name] = _redact_owned_url(record.__dict__[name], url)
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
        token = _ACTIVE_URL.set(url)
        try:
            yield
        finally:
            _ACTIVE_URL.reset(token)
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
