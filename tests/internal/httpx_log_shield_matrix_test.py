"""Verification matrix for the shared HTTPX log shield on HTTP/2, hpack and tracebacks (A20, section 3.8).

Every cell drives real HPACK: ``tests.h2c_server`` speaks cleartext HTTP/2 with prior knowledge on loopback,
so the client's encoder, its decoder and table eviction log exactly what they would against a portal.
"""

from __future__ import annotations
import asyncio
import gc
import importlib
import io
import logging
import pkgutil
import sys
from contextlib import contextmanager
from typing import TYPE_CHECKING

import h2
import hpack
import httpx
import pytest

from b24api.contracts import Request, RouteKind
from b24api.errors import CapabilityError
from b24api.transport import HttpxTransport
from b24api.transport.logging_shield import HPACK_LOGGER_NAMES, HTTPX_LOG_SHIELD
from tests.h2c_server import H2cServer

if TYPE_CHECKING:
    from collections.abc import Iterator

_OWNED = "synthetic-owned-h2-secret-424242"
_HOP = "synthetic-hop-h2-secret-515151"
_FOREIGN = "synthetic-foreign-h2-secret-636363"
_OK = 200
_MOVED = 301
_SEQUENTIAL_CLIENTS = 12
_REAL_ASYNC_CLIENT = httpx.AsyncClient
_FORMATTER = logging.Formatter("%(name)s %(message)s")


class _Capture(logging.Handler):
    """Keep every record and its formatted text, reading raw fields directly rather than only via a Formatter."""

    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []
        # ``exc_text`` as the filters left it; a Formatter caches its own rendering on the record afterwards.
        self.filtered_exc_text: dict[int, str | None] = {}
        self.output = io.StringIO()

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
        self.filtered_exc_text[id(record)] = record.exc_text
        self.output.write(_FORMATTER.format(record) + "\n")

    def named(self, *names: str) -> list[logging.LogRecord]:
        return [record for record in self.records if record.name in names]


@contextmanager
def _capture() -> Iterator[_Capture]:
    handler = _Capture()
    loggers = [logging.getLogger(name) for name in ("httpx", *HPACK_LOGGER_NAMES)]
    levels = [logger.level for logger in loggers]
    for logger in loggers:
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        for logger, level in zip(loggers, levels, strict=True):
            logger.removeHandler(handler)
            logger.setLevel(level)


def _assert_secret_free(handler: _Capture, *secrets: str) -> None:
    for record in handler.records:
        raw = f"{record.msg!r} {record.args!r} {record.exc_text!r} {record.stack_info!r} {record.__dict__!r}"
        for secret in secrets:
            assert secret not in raw
            assert secret not in record.getMessage()
    for secret in secrets:
        assert secret not in handler.output.getvalue()


def _h2c_client(**kwargs: object) -> httpx.AsyncClient:
    return _REAL_ASYNC_CLIENT(**{**kwargs, "http1": False, "http2": True})  # type: ignore[arg-type]


def _echo(path: str) -> tuple[int, tuple[tuple[str, str], ...], bytes]:
    # Echo the path in a response header, so the client's HPACK decoder handles the token too.
    return _OK, (("x-echo", path), ("content-type", "application/json")), b'{"result":true}'


async def _foreign_h2_exchange(origin: str) -> None:
    async with _h2c_client() as foreign:
        await foreign.post(f"{origin}/rest/1/{_FOREIGN}/profile")


def _assert_shield_idle() -> None:
    gc.collect()
    assert HTTPX_LOG_SHIELD.suppresses_hpack() is False


@pytest.mark.asyncio
async def test_owned_http2_encode_and_decode_records_are_suppressed_until_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _assert_shield_idle()
    # The transport's own client, forced to cleartext HTTP/2 so a loopback server can answer it.
    monkeypatch.setattr(httpx, "AsyncClient", _h2c_client)
    async with H2cServer(_echo) as server:
        with _capture() as handler:
            transport = HttpxTransport(f"{server.origin}/rest/1/{_OWNED}/")
            try:
                response = await transport.send(
                    Request("profile", route=RouteKind.BARE), attempt_timeout=5, max_response_bytes=1024
                )
                assert response.status_code == _OK
                assert server.paths == [f"/rest/1/{_OWNED}/profile"]
                assert handler.named(*HPACK_LOGGER_NAMES) == []
                assert len(handler.named("httpx")) == 1
                assert "HTTP/2 200" in handler.named("httpx")[0].getMessage()
            finally:
                await transport.aclose()
            _assert_secret_free(handler, _OWNED)
            # Positive control: once the owned client is closed, the same exchange logs hpack records again.
            await _foreign_h2_exchange(server.origin)
            foreign = handler.named(*HPACK_LOGGER_NAMES)
            assert foreign
            assert any(_FOREIGN in record.getMessage() for record in foreign)


@pytest.mark.asyncio
async def test_table_eviction_during_a_foreign_request_is_suppressed_while_the_client_is_open() -> None:
    _assert_shield_idle()
    async with H2cServer(_echo) as server:
        with _capture() as handler:
            client = _h2c_client()
            transport = HttpxTransport(f"{server.origin}/rest/1/{_OWNED}/", client=client)
            await transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=5, max_response_bytes=1024)
            # The short transport is gone, but its connection's table still holds the owned :path.
            await transport.aclose()
            await client.post(f"{server.origin}/foreign", headers={"x-pad": "a" * 4000})
            assert handler.named(*HPACK_LOGGER_NAMES) == []
            await client.aclose()
            _assert_secret_free(handler, _OWNED)

            # Positive control: the same eviction on an unregistered client is logged, and visible again.
            async with _h2c_client() as foreign:
                await foreign.post(f"{server.origin}/rest/1/{_FOREIGN}/profile")
                await foreign.post(f"{server.origin}/foreign", headers={"x-pad": "a" * 4000})
            evictions = [record for record in handler.named("hpack.table") if "Evicting" in record.getMessage()]
            assert any(_FOREIGN in record.getMessage() for record in evictions)


@pytest.mark.asyncio
async def test_http2_redirect_to_another_token_keeps_both_secrets_out_of_every_record() -> None:
    _assert_shield_idle()

    def respond(path: str) -> tuple[int, tuple[tuple[str, str], ...], bytes]:
        if _OWNED in path:
            return _MOVED, (("location", f"/rest/1/{_HOP}/profile"),), b""
        return _echo(path)

    async with H2cServer(respond) as server:
        with _capture() as handler:
            client = _h2c_client(follow_redirects=True)
            transport = HttpxTransport(f"{server.origin}/rest/1/{_OWNED}/", client=client)
            try:
                response = await transport.send(
                    Request("profile", route=RouteKind.BARE), attempt_timeout=5, max_response_bytes=1024
                )
                assert response.status_code == _OK
                assert server.paths == [f"/rest/1/{_OWNED}/profile", f"/rest/1/{_HOP}/profile"]
                assert len(handler.named("httpx")) == len(server.paths)
                assert handler.named(*HPACK_LOGGER_NAMES) == []
            finally:
                await transport.aclose()
                await client.aclose()
            _assert_secret_free(handler, _OWNED, _HOP)


@pytest.mark.asyncio
async def test_parallel_http2_clients_keep_suppression_until_the_last_one_closes() -> None:
    _assert_shield_idle()
    hpack_logger = logging.getLogger("hpack.hpack")
    async with H2cServer(_echo) as server:
        with _capture() as handler:
            clients = [_h2c_client(), _h2c_client()]
            transports = [
                HttpxTransport(f"{server.origin}/rest/{index}/{_OWNED}{index}/", client=client)
                for index, client in enumerate(clients)
            ]
            await asyncio.gather(
                *(
                    transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=5, max_response_bytes=1024)
                    for transport in transports
                )
            )
            for transport in transports:
                await transport.aclose()
            await clients[0].aclose()
            hpack_logger.debug("foreign hpack record while one client is open")
            assert handler.named(*HPACK_LOGGER_NAMES) == []
            await clients[1].aclose()
            hpack_logger.debug("foreign hpack record after the last client closed")
            assert [record.getMessage() for record in handler.named(*HPACK_LOGGER_NAMES)] == [
                "foreign hpack record after the last client closed"
            ]
            _assert_secret_free(handler, f"{_OWNED}0", f"{_OWNED}1")


@pytest.mark.asyncio
async def test_sequential_injected_clients_return_both_tables_to_their_initial_size() -> None:
    _assert_shield_idle()
    initial_clients = len(HTTPX_LOG_SHIELD._clients)  # noqa: SLF001 - table-size control
    initial_secrets = len(HTTPX_LOG_SHIELD.registered_secrets())
    hpack_logger = logging.getLogger("hpack.hpack")

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(_OK, json={"result": True}, request=request)

    with _capture() as handler:
        for index in range(_SEQUENTIAL_CLIENTS):
            client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
            # Two transports on one client and one secret stay one registration and one registry entry.
            transports = [
                HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED}{index}/", client=client) for _ in range(2)
            ]
            for transport in transports:
                await transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=64)
            assert len(HTTPX_LOG_SHIELD._clients) == initial_clients + 1  # noqa: SLF001 - dedup control
            assert len(HTTPX_LOG_SHIELD.registered_secrets()) == initial_secrets + 1
            for transport in transports:
                await transport.aclose()
            if index % 2:
                await client.aclose()
            del client, transports, transport
            gc.collect()
            hpack_logger.debug("foreign hpack record %d", index)
            assert len(HTTPX_LOG_SHIELD._clients) == initial_clients  # noqa: SLF001 - closed or collected
            assert len(HTTPX_LOG_SHIELD.registered_secrets()) == initial_secrets
        visible = [record.getMessage() for record in handler.named("hpack.hpack")]
    assert visible == [f"foreign hpack record {index}" for index in range(_SEQUENTIAL_CLIENTS)]


class _NoIoHttp2Transport(httpx.AsyncHTTPTransport):
    """An HTTP/2-capable transport that answers locally and counts every attempted dispatch."""

    def __init__(self) -> None:
        super().__init__(http2=True)
        self.calls = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        return httpx.Response(_OK, json={"result": True}, request=request)


@pytest.mark.asyncio
@pytest.mark.parametrize("logger_name", HPACK_LOGGER_NAMES)
async def test_removed_hpack_filter_refuses_http2_send_before_io(logger_name: str) -> None:
    _assert_shield_idle()
    raw = _NoIoHttp2Transport()
    client = httpx.AsyncClient(transport=raw)
    transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED}/", client=client)
    http1 = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(_OK, request=request)))
    http1_transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED}/", client=http1)
    logger = logging.getLogger(logger_name)
    try:
        await transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=64)
        assert raw.calls == 1
        logger.removeFilter(HTTPX_LOG_SHIELD._hpack_filter)  # noqa: SLF001 - simulate external logging drift
        with pytest.raises(CapabilityError, match="hpack log filter was removed") as caught:
            await transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=64)
        assert raw.calls == 1
        assert _OWNED not in str(caught.value)
        assert _OWNED not in repr(caught.value.__dict__)
        # HTTP/1.1 never runs HPACK, so an HTTP/1.1-only client keeps sending.
        await http1_transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=64)
        # A new registration installs the filter again before its first request.
        again = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED}/", client=client)
        await again.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=64)
        assert raw.calls == 2  # noqa: PLR2004 - one refused send in between
        await again.aclose()
    finally:
        await transport.aclose()
        await http1_transport.aclose()
        await client.aclose()
        await http1.aclose()


class _UnhashableClient(httpx.AsyncClient):
    """An injected client that cannot be a weak mapping key."""

    __hash__ = None  # type: ignore[assignment]


@pytest.mark.asyncio
async def test_client_that_cannot_be_weakly_registered_refuses_http2_but_not_http1() -> None:
    _assert_shield_idle()
    raw = _NoIoHttp2Transport()
    http2_client = _UnhashableClient(transport=raw)
    http1_client = _UnhashableClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(_OK, request=request)),
    )
    http2_transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED}/", client=http2_client)
    http1_transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED}/", client=http1_client)
    try:
        with pytest.raises(CapabilityError, match="cannot be weakly registered") as caught:
            await http2_transport.send(
                Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=64
            )
        assert raw.calls == 0
        assert _OWNED not in str(caught.value)
        response = await http1_transport.send(
            Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=64
        )
        assert response.status_code == _OK
    finally:
        await http2_transport.aclose()
        await http1_transport.aclose()
        await http2_client.aclose()
        await http1_client.aclose()


def test_every_header_capable_hpack_logger_is_filtered() -> None:
    # A new emitter in a wider hpack or h2 range must not escape the fail-closed filter unnoticed.
    loggers: set[str] = set()
    for package in (hpack, h2):
        for module_info in pkgutil.walk_packages(package.__path__, f"{package.__name__}."):
            importlib.import_module(module_info.name)
        loggers.update(
            value.name
            for name, module in tuple(sys.modules.items())
            if name == package.__name__ or name.startswith(f"{package.__name__}.")
            for value in vars(module).values()
            if isinstance(value, logging.Logger)
        )
    assert loggers == set(HPACK_LOGGER_NAMES)


def _fail(message: str) -> None:
    raise RuntimeError(message)


def _raise_with(message: str) -> BaseException:
    try:
        _fail(message)
    except RuntimeError as error:
        return error
    raise AssertionError("unreachable")


@pytest.mark.asyncio
async def test_owned_record_traceback_is_rendered_scrubbed_and_detached() -> None:
    _assert_shield_idle()
    logger = logging.getLogger("httpx")

    async def hook(response: httpx.Response) -> None:
        error = _raise_with(f"hook failed for {response.request.url}")
        logger.error("owned hook", exc_info=(type(error), error, error.__traceback__), stack_info=True)
        plain = _raise_with("no secret in this failure")
        logger.error("owned plain", exc_info=(type(plain), plain, plain.__traceback__))

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(_OK, json={"result": 1}, request=request)),
        event_hooks={"response": [hook]},
    )
    transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED}/", client=client)
    with _capture() as handler:
        try:
            await transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=64)
        finally:
            await transport.aclose()
            await client.aclose()
    owned = next(record for record in handler.records if record.msg == "owned hook")
    assert owned.exc_info is None
    assert owned.exc_text is not None
    assert "RuntimeError: hook failed for" in owned.exc_text
    assert "[REDACTED]" in owned.exc_text
    assert owned.stack_info is not None
    # An owned record is detached even when nothing in its traceback matched a secret.
    plain = next(record for record in handler.records if record.msg == "owned plain")
    assert plain.exc_info is None
    assert handler.filtered_exc_text[id(plain)] is not None
    assert "no secret in this failure" in str(plain.exc_text)
    _assert_secret_free(handler, _OWNED)


def test_foreign_record_with_an_exception_but_no_secret_keeps_every_field_object() -> None:
    logger = logging.getLogger("httpx")
    foreign_url = httpx.URL(f"https://other.invalid/rest/1/{_FOREIGN}/profile")
    error = _raise_with(f"foreign failure for {foreign_url}")
    exc_info = (type(error), error, error.__traceback__)
    HTTPX_LOG_SHIELD.register_transport()
    try:
        # With and without an owned dispatch in flight: neither may touch a record without a secret.
        for owned_url in (None, f"https://portal.invalid/rest/1/{_OWNED}/profile"):
            with _capture() as handler:
                if owned_url is None:
                    logger.error("foreign %s", foreign_url, exc_info=exc_info)
                    logger.info("plain foreign %s", foreign_url)
                else:
                    with HTTPX_LOG_SHIELD.request(owned_url) as ownership:
                        ownership.claim(httpx.Request("POST", owned_url))
                        _foreign_client_log(logger, foreign_url, exc_info)
            failed, plain = (record for record in handler.records if record.msg != 'HTTP Request: %s %s "%s %d %s"')
            assert failed.exc_info is exc_info
            assert handler.filtered_exc_text[id(failed)] is None
            assert isinstance(failed.args, tuple)
            assert failed.args[0] is foreign_url
            assert plain.exc_info is None
            assert isinstance(plain.args, tuple)
            assert plain.args[0] is foreign_url
            assert _FOREIGN in handler.output.getvalue()
    finally:
        HTTPX_LOG_SHIELD.release_transport()


def _foreign_client_log(
    logger: logging.Logger,
    foreign_url: httpx.URL,
    exc_info: tuple[type[BaseException], BaseException, object],
) -> None:
    """Emit foreign records from inside a sync HTTPX send whose root is not the owned request."""
    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(_OK, request=request)),
        event_hooks={
            "request": [
                lambda _request: logger.error("foreign %s", foreign_url, exc_info=exc_info),  # type: ignore[arg-type]
                lambda _request: logger.info("plain foreign %s", foreign_url),
            ]
        },
    )
    try:
        client.post(foreign_url)
    finally:
        client.close()


@pytest.mark.asyncio
async def test_foreign_record_carrying_a_registered_secret_is_scrubbed_with_its_traceback() -> None:
    _assert_shield_idle()
    logger = logging.getLogger("httpx")
    owned_url = f"https://portal.invalid/rest/1/{_OWNED}/profile"
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(_OK, request=request)))
    transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED}/", client=client)
    error = _raise_with(f"caller failure for {owned_url}")
    try:
        with _capture() as handler:
            # No owned dispatch is in flight: the caller logs the secret on its own.
            logger.error("caller %s", owned_url, exc_info=(type(error), error, error.__traceback__))
            logger.info("caller without the secret %s", "https://other.invalid/")
    finally:
        await transport.aclose()
        await client.aclose()
    scrubbed, untouched = handler.records
    assert scrubbed.exc_info is None
    assert scrubbed.exc_text is not None
    assert "caller failure for" in scrubbed.exc_text
    assert untouched.args == ("https://other.invalid/",)
    _assert_secret_free(handler, _OWNED)


@pytest.mark.asyncio
async def test_secret_registry_is_deduplicated_by_value_and_tied_to_client_lifetime() -> None:
    _assert_shield_idle()
    initial = set(HTTPX_LOG_SHIELD.registered_secrets())
    clients = [
        httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(_OK, request=request)))
        for _ in range(2)
    ]
    transports = [HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED}/", client=client) for client in clients]
    for transport in transports:
        await transport.aclose()
    # Both injected clients are still open: the secret stays registered, once.
    assert set(HTTPX_LOG_SHIELD.registered_secrets()) == {*initial, _OWNED}
    await clients[0].aclose()
    assert HTTPX_LOG_SHIELD.suppresses_hpack() is True
    assert set(HTTPX_LOG_SHIELD.registered_secrets()) == {*initial, _OWNED}
    await clients[1].aclose()
    assert HTTPX_LOG_SHIELD.suppresses_hpack() is False
    assert set(HTTPX_LOG_SHIELD.registered_secrets()) == initial


@pytest.mark.asyncio
async def test_registered_secret_stays_scrubbed_while_the_injected_client_outlives_its_transport() -> None:
    _assert_shield_idle()
    logger = logging.getLogger("httpx")
    owned_url = f"https://portal.invalid/rest/1/{_OWNED}/profile"
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(_OK, request=request)))
    await HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED}/", client=client).aclose()
    try:
        with _capture() as handler:
            # The transport is gone but its injected client, and so the registered secret, is still live.
            await client.get(owned_url)
            error = _raise_with(f"caller failure for {owned_url}")
            logger.error("caller %s", owned_url, exc_info=(type(error), error, error.__traceback__))
            logger.info("caller without the secret %s", "https://other.invalid/")
    finally:
        await client.aclose()
    _assert_secret_free(handler, _OWNED)
    assert handler.records[-1].args == ("https://other.invalid/",)

    # Closing the client drops its secret at once; the next shield lifecycle event removes both filters.
    assert _OWNED not in HTTPX_LOG_SHIELD.registered_secrets()
    await HttpxTransport("https://portal.invalid/rest/1/synthetic-probe-secret-000000/").aclose()
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - final cleanup control
    for name in HPACK_LOGGER_NAMES:
        assert HTTPX_LOG_SHIELD._hpack_filter not in logging.getLogger(name).filters  # noqa: SLF001
    _assert_shield_idle()
