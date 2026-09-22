"""Positive controls for credential-safe HTTPX INFO logging."""

from __future__ import annotations
import asyncio
import io
import logging

import httpx
import pytest

from b24api import BodyEncoding, Request, RouteKind
from b24api.errors import TransportError
from b24api.transport import HttpxTransport
from b24api.transport.logging_shield import HTTPX_LOG_SHIELD

_OWNED_MARKER = "synthetic-owned-secret-123456"
_FOREIGN_MARKER = "synthetic-foreign-secret-123456"
_SUCCESS_STATUS = 200
_RESPONSE_STATUS = 403
_OWNED_RECORDS = 2
_TOTAL_RECORDS = 3


class _CollectingHandler(logging.StreamHandler[io.StringIO]):
    """Retain the original LogRecord as well as the formatted handler output."""

    def __init__(self) -> None:
        self.output = io.StringIO()
        self.records: list[logging.LogRecord] = []
        super().__init__(self.output)

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
        super().emit(record)


@pytest.mark.asyncio
async def test_httpx_info_record_is_emitted_and_rewritten_before_handler_formatting() -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    def respond(request: httpx.Request) -> httpx.Response:
        status = _SUCCESS_STATUS if request.headers.get("content-type") == "application/json" else _RESPONSE_STATUS
        return httpx.Response(status, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    first = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    second = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    try:
        responses = await asyncio.gather(
            *(
                first.send(
                    Request("profile", route=RouteKind.JSON, encoding=encoding),
                    attempt_timeout=1,
                    max_response_bytes=1024,
                )
                for encoding in (BodyEncoding.JSON, BodyEncoding.FORM_URLENCODED)
            )
        )
        assert {response.status_code for response in responses} == {_SUCCESS_STATUS, _RESPONSE_STATUS}
        assert len(handler.records) == _OWNED_RECORDS
        for record in handler.records:
            assert record.name == "httpx"
            assert record.levelno == logging.INFO
            assert record.pathname.endswith("httpx/_client.py")
            raw = f"{record.msg!r} {record.args!r}"
            assert _OWNED_MARKER not in raw
            assert _OWNED_MARKER not in record.getMessage()
            assert "[REDACTED]" in record.getMessage()
        assert _OWNED_MARKER not in handler.output.getvalue()

        await first.aclose()
        assert HTTPX_LOG_SHIELD._filter in logger.filters  # noqa: SLF001 - lifecycle control
        logger.removeFilter(HTTPX_LOG_SHIELD._filter)  # noqa: SLF001 - simulate logger drift
        await second.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024)
        assert len(handler.records) == _TOTAL_RECORDS
        assert _OWNED_MARKER not in handler.records[-1].getMessage()
        assert HTTPX_LOG_SHIELD._filter in logger.filters  # noqa: SLF001 - reinstallation control

        await client.post(f"https://portal.invalid/rest/1/{_FOREIGN_MARKER}/profile")
        assert _FOREIGN_MARKER in handler.records[-1].getMessage()
    finally:
        await first.aclose()
        await second.aclose()
        await client.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - final cleanup control


@pytest.mark.asyncio
async def test_httpx_timeout_uses_redacted_exception_after_inflight_cleanup() -> None:
    async def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("synthetic timeout", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(timeout))
    transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    try:
        with pytest.raises(TransportError) as captured:
            await transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024)
        assert _OWNED_MARKER not in repr(captured.value)
    finally:
        await transport.aclose()
        await client.aclose()


@pytest.mark.asyncio
async def test_closing_transport_keeps_filter_until_inflight_httpx_record_is_emitted() -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def respond(request: httpx.Request) -> httpx.Response:
        entered.set()
        await release.wait()
        return httpx.Response(_SUCCESS_STATUS, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    task = asyncio.create_task(
        transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024),
    )
    try:
        await entered.wait()
        await transport.aclose()
        assert HTTPX_LOG_SHIELD._filter in logger.filters  # noqa: SLF001 - in-flight lifecycle control
        release.set()
        await task
        assert len(handler.records) == 1
        assert _OWNED_MARKER not in handler.records[0].getMessage()
    finally:
        release.set()
        await task
        await transport.aclose()
        await client.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - in-flight cleanup control
