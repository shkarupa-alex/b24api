"""httpcore's DEBUG trace records of an owned send are scrubbed as HTTPX's own records are."""

from __future__ import annotations
import asyncio
import logging
import re
from importlib import util
from pathlib import Path

import pytest

from b24api import Request, RouteKind
from b24api.transport import HttpxTransport
from b24api.transport.logging_shield import HTTPCORE_LOGGER_NAMES

_TOKEN = "s3cr3tT0kenValue9"  # noqa: S105 - a fixture webhook token the shield must hide
_QUERY_SECRET = "z" * 10
_REDIRECT = 302


def test_the_shield_covers_every_logger_the_installed_httpcore_writes_to() -> None:
    spec = util.find_spec("httpcore")
    assert spec is not None
    assert spec.origin is not None
    sources = Path(spec.origin).parent.rglob("*.py")
    names = {
        name
        for source in sources
        for name in re.findall(r"""logging\.getLogger\(["'](httpcore[.\w]*)["']\)""", source.read_text())
    }

    assert names == set(HTTPCORE_LOGGER_NAMES)


class _Records(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


@pytest.mark.asyncio
async def test_a_redirect_location_naming_the_webhook_token_is_scrubbed_from_httpcore_debug_records() -> None:
    async def redirect(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")
        location = f"/rest/1/{_TOKEN}/other.method?auth={_QUERY_SECRET}".encode()
        writer.write(b"HTTP/1.1 302 Found\r\nlocation: " + location + b"\r\ncontent-length: 0\r\n\r\n")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(redirect, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    loggers = [logging.getLogger(name) for name in HTTPCORE_LOGGER_NAMES]
    levels = [logger.level for logger in loggers]
    handler = _Records()
    for logger in loggers:
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    transport = HttpxTransport(f"http://127.0.0.1:{port}/rest/1/{_TOKEN}/")
    try:
        response = await transport.send(
            Request("crm.deal.list", route=RouteKind.BARE), attempt_timeout=5, max_response_bytes=1024
        )
    finally:
        await transport.aclose()
        server.close()
        await server.wait_closed()
        for logger, level in zip(loggers, levels, strict=True):
            logger.removeHandler(handler)
            logger.setLevel(level)

    assert response.status_code == _REDIRECT
    headers = [message for message in handler.messages if "receive_response_headers.complete" in message]
    assert headers
    assert all(b"location" in message.encode() for message in headers)
    assert not any(_TOKEN in message for message in handler.messages)
    assert not any(_QUERY_SECRET in message for message in handler.messages)
