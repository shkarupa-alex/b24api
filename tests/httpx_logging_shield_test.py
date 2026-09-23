"""Positive controls for credential-safe HTTPX INFO logging."""

from __future__ import annotations
import asyncio
import io
import logging
import tomllib
from pathlib import Path

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
_REDIRECT_RECORDS = 2


def test_httpx_dependency_range_matches_the_positive_controlled_minor() -> None:
    project = tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text())

    assert "httpx[http2]>=0.28.1,<0.29" in project["project"]["dependencies"]


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


@pytest.mark.asyncio
@pytest.mark.parametrize("route", [RouteKind.BARE, RouteKind.JSON, RouteKind.API_V3])
async def test_redirect_hops_keep_the_owned_webhook_credential_out_of_httpx_info(
    route: RouteKind,
) -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.host == "portal.invalid":
            redirected = request.url.copy_with(scheme="https", host="redirect.invalid")
            return httpx.Response(301, headers={"location": str(redirected)}, request=request)
        return httpx.Response(_SUCCESS_STATUS, json={"result": True}, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond), follow_redirects=True)
    transport = HttpxTransport(f"http://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    try:
        response = await transport.send(
            Request("profile", route=route),
            attempt_timeout=1,
            max_response_bytes=1024,
        )
        assert response.status_code == _SUCCESS_STATUS
        assert len(handler.records) == _REDIRECT_RECORDS
        for record in handler.records:
            assert _OWNED_MARKER not in f"{record.msg!r} {record.args!r}"
            assert _OWNED_MARKER not in record.getMessage()
        assert _OWNED_MARKER not in handler.output.getvalue()
    finally:
        await transport.aclose()
        await client.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)


_SECOND_MARKER = "synthetic-second-secret-654321"
_REDIRECT_TARGETS = (
    f"/rest/1/{_SECOND_MARKER}/profile",
    f"/rest/api/1/{_SECOND_MARKER}/profile",
    f"/profile?auth={_SECOND_MARKER}",
)


@pytest.mark.asyncio
@pytest.mark.parametrize("target", _REDIRECT_TARGETS, ids=["classic", "api-v3", "auth-query"])
@pytest.mark.parametrize("route", [RouteKind.BARE, RouteKind.JSON, RouteKind.API_V3])
async def test_redirect_replaces_webhook_token_without_logging_either_secret(route: RouteKind, target: str) -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.host == "portal.invalid":
            return httpx.Response(301, headers={"location": f"https://redirect.invalid{target}"}, request=request)
        logger.info("hop observed", extra={"url": str(request.url), "request_url": str(request.url)})
        return httpx.Response(_SUCCESS_STATUS, json={"result": True}, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond), follow_redirects=True)
    transport = HttpxTransport(f"http://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    try:
        response = await transport.send(Request("profile", route=route), attempt_timeout=1, max_response_bytes=1024)
        assert response.status_code == _SUCCESS_STATUS
        assert len(handler.records) == _REDIRECT_RECORDS + 1
        for record in handler.records:
            extras = (record.__dict__.get("url"), record.__dict__.get("request_url"))
            raw = f"{record.msg!r} {record.args!r} {extras!r}"
            for marker in (_OWNED_MARKER, _SECOND_MARKER):
                assert marker not in raw
                assert marker not in record.getMessage()
        assert _OWNED_MARKER not in handler.output.getvalue()
        assert _SECOND_MARKER not in handler.output.getvalue()

        await client.get(f"https://redirect.invalid/rest/1/{_FOREIGN_MARKER}/profile")
        assert _FOREIGN_MARKER in handler.records[-1].getMessage()
    finally:
        await transport.aclose()
        await client.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - final cleanup control


_THIRD_MARKER = "synthetic-third-secret-777777"


@pytest.mark.asyncio
async def test_unrelated_httpx_request_inside_owned_response_hook_is_unchanged() -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    foreign_url = f"https://other.invalid/rest/1/{_FOREIGN_MARKER}/profile"
    foreign = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(_SUCCESS_STATUS, request=request)),
    )
    hook_calls: list[str] = []

    async def hook(response: httpx.Response) -> None:
        if not hook_calls:
            hook_calls.append(str(response.url))
            await foreign.get(foreign_url)

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.host == "portal.invalid":
            location = f"https://redirect.invalid/rest/1/{_THIRD_MARKER}/profile"
            return httpx.Response(301, headers={"location": location}, request=request)
        return httpx.Response(_SUCCESS_STATUS, json={"result": True}, request=request)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(respond),
        follow_redirects=True,
        event_hooks={"response": [hook]},
    )
    transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    try:
        response = await transport.send(
            Request("profile", route=RouteKind.BARE),
            attempt_timeout=1,
            max_response_bytes=1024,
        )
        assert response.status_code == _SUCCESS_STATUS
        messages = [record.getMessage() for record in handler.records]
        assert len(messages) == _TOTAL_RECORDS
        foreign_records = [record for record in handler.records if "other.invalid" in record.getMessage()]
        assert len(foreign_records) == 1
        assert _FOREIGN_MARKER in f"{foreign_records[0].args!r}"
        assert foreign_url in foreign_records[0].getMessage()
        assert foreign_url in handler.output.getvalue()
        for record in handler.records:
            if record in foreign_records:
                continue
            for marker in (_OWNED_MARKER, _THIRD_MARKER):
                assert marker not in f"{record.msg!r} {record.args!r}"
                assert marker not in record.getMessage()
        for marker in (_OWNED_MARKER, _THIRD_MARKER):
            assert marker not in handler.output.getvalue()
    finally:
        await transport.aclose()
        await client.aclose()
        await foreign.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - final cleanup control


def test_record_without_an_emitting_httpx_client_is_scrubbed_conservatively() -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    HTTPX_LOG_SHIELD.register_transport()
    try:
        with HTTPX_LOG_SHIELD.request(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/profile", client=object()):
            logger.info("hop %s", f"https://redirect.invalid/rest/1/{_SECOND_MARKER}/profile")
        assert len(handler.records) == 1
        assert _SECOND_MARKER not in handler.records[0].getMessage()
    finally:
        HTTPX_LOG_SHIELD.release_transport()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - final cleanup control
