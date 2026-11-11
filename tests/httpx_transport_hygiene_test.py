"""HTTPX transport hygiene: no webhook on an error chain, no userinfo, nothing unsendable reported as sent."""

from __future__ import annotations
import asyncio
import contextlib
import gc
import logging
import re
import traceback
import types
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from pydantic import HttpUrl, ValidationError

from b24api import (
    AmbiguousExecutionError,
    B24ApiError,
    Bitrix24,
    ExecutionPolicy,
    ProtocolError,
    ReplaySafety,
    Request,
    Settings,
    TransportError,
)
from b24api._error_types import FailurePhase
from b24api._webhook import TokenVault, Webhook, WebhookRefusal, parse_webhook
from b24api.contracts import RequestHeaders
from b24api.contracts.request import RouteKind
from b24api.errors import ResponseTooLargeError
from b24api.transport import HttpxTransport
from b24api.transport.logging_shield import HTTPX_LOG_SHIELD, LogOwnership
from tests.scripting import client_for

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Callable, Iterator

FRAGMENT = "hygiene-webhook-credential-7c1e"
WEBHOOK = f"https://fixture.invalid/rest/1/{FRAGMENT}/"
_ONE_ATTEMPT = ExecutionPolicy(max_attempts_per_request=1)
_SERVER_ERROR = 500


def _chain(error: BaseException) -> Iterator[BaseException]:
    seen: set[int] = set()
    pending: list[BaseException | None] = [error]
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        yield current
        pending += [current.__cause__, current.__context__]


def _local_texts(value: object, *, depth: int = 8) -> Iterator[str]:
    """Render a frame local as traceback tooling does: its ``repr``, recursing only into builtin containers."""
    yield repr(value)
    if depth == 0:
        return
    items: tuple[object, ...] = ()
    if isinstance(value, dict):
        items = (*value.keys(), *value.values())
    elif isinstance(value, list | tuple | set | frozenset):
        items = tuple(value)
    for item in items:
        yield from _local_texts(item, depth=depth - 1)


def _rendered_texts(error: BaseException) -> Iterator[str]:
    """Every text an error tracker or traceback formatter renders from ``error``, frame locals included."""
    yield "".join(traceback.format_exception(error))
    yield "".join(traceback.TracebackException.from_exception(error, capture_locals=True).format())
    for link in _chain(error):
        yield from (str(link), repr(link), *map(repr, link.args), *map(str, getattr(link, "__notes__", ())))
        frames = link.__traceback__
        while frames is not None:
            for value in frames.tb_frame.f_locals.values():
                yield from _local_texts(value)
            frames = frames.tb_next


def _assert_token_unreachable(error: BaseException, log: str = "") -> None:
    """Assert the token is in no rendering of the error, its chain and frame locals, nor in the log."""
    for link in _chain(error):
        if isinstance(link, httpx.HTTPError):
            with pytest.raises(RuntimeError):
                _ = link.request  # an HTTPX error on the chain would carry the credential-bearing request
    assert [text for text in _rendered_texts(error) if FRAGMENT in text] == []
    assert FRAGMENT not in log


class _Opaque(types.SimpleNamespace):
    def __repr__(self) -> str:
        return "_Opaque()"


def test_the_renderer_reaches_a_token_held_in_a_container_of_a_frame_local() -> None:
    def fail(holder: object) -> None:
        assert holder is not None
        raise RuntimeError("no text of its own")

    with pytest.raises(RuntimeError) as captured:
        fail([{"nested": (WEBHOOK,)}])
    assert any(FRAGMENT in text for text in _rendered_texts(captured.value))

    # Like traceback tooling, the renderer shows an object by its ``repr`` and does not open its attributes.
    with pytest.raises(RuntimeError) as captured:
        fail([_Opaque(held=WEBHOOK)])
    assert not any(FRAGMENT in text for text in _rendered_texts(captured.value))


async def _raise_for_status(response: httpx.Response) -> None:
    response.raise_for_status()


async def _request_hook_quoting_the_url(request: httpx.Request) -> None:
    raise RuntimeError(f"refused {request.url}")


async def _hook_raising_a_b24api_error(request: httpx.Request) -> None:
    raise TransportError(f"nested failure for {request.url}")


async def _hook_timing_out(request: httpx.Request) -> None:
    raise TimeoutError(f"hook timed out for {request.url}")


class _AuthQuotingTheUrl(httpx.Auth):
    async def async_auth_flow(self, request: httpx.Request) -> AsyncGenerator[httpx.Request, httpx.Response]:
        response = yield request
        raise PermissionError(f"refused {request.url} after {response.status_code}")


def _ok(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"result": 1, "time": {}}, request=request)


def _server_error(request: httpx.Request) -> httpx.Response:
    return httpx.Response(_SERVER_ERROR, json={"error": "x"}, request=request)


def _protocol_refusal(request: httpx.Request) -> httpx.Response:
    raise httpx.LocalProtocolError(f"cannot send {request.url}", request=request)


def _foreign_failure(request: httpx.Request) -> httpx.Response:
    raise ValueError(f"foreign transport failed for {request.url}")


class _Body(httpx.AsyncByteStream):
    """A response stream that fails while streaming, or only when it is released after the last chunk."""

    def __init__(self, url: str, failure: Exception | None, *, on_release: bool) -> None:
        self._url = url
        self._failure = failure
        self._on_release = on_release

    async def __aiter__(self) -> AsyncIterator[bytes]:
        if self._failure is not None and not self._on_release:
            yield b'{"res'
            raise self._failure
        yield b'{"result": 1, "time": {}}'

    async def aclose(self) -> None:
        if self._failure is not None and self._on_release:
            raise self._failure


def _streaming(failure: Callable[[str], Exception], *, on_release: bool) -> Callable[[httpx.Request], httpx.Response]:
    def respond(request: httpx.Request) -> httpx.Response:
        stream = _Body(str(request.url), failure(str(request.url)), on_release=on_release)
        return httpx.Response(200, stream=stream, request=request)

    return respond


def _foreign_stream_failure(url: str) -> Exception:
    return RuntimeError(f"stream failed for {url}")


def _httpx_read_failure(url: str) -> Exception:
    return httpx.ReadError(f"connection lost for {url}")


def _release_failure(url: str) -> Exception:
    return httpx.StreamError(f"release failed for {url}")


def _client(
    handler: Callable[[httpx.Request], httpx.Response], hooks: _Hooks | None = None, auth: httpx.Auth | None = None
) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), event_hooks=hooks, auth=auth)


async def _call(http: httpx.AsyncClient, safety: ReplaySafety, *, policy: ExecutionPolicy = _ONE_ATTEMPT) -> object:
    """Call through a client configured with the same webhook, as ``from_webhook`` does: its redactor holds it."""
    transport = HttpxTransport(WEBHOOK, client=http)
    try:
        async with Bitrix24(Settings(webhook_url=WEBHOOK), transport=transport, policy=policy) as client:
            return await client.call(Request.bare("user.current", replay_safety=safety))
    finally:
        await transport.aclose()


type _Hooks = dict[str, list[Callable[..., object]]]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("handler", "hooks", "auth", "retryable"),
    [
        pytest.param(
            _server_error, {"response": [_raise_for_status]}, None, False, id="response-hook-raise-for-status"
        ),
        pytest.param(_ok, {"request": [_request_hook_quoting_the_url]}, None, False, id="request-hook-quoting-the-url"),
        pytest.param(_ok, {"request": [_hook_raising_a_b24api_error]}, None, False, id="hook-raising-a-b24api-error"),
        pytest.param(_ok, {}, _AuthQuotingTheUrl(), False, id="auth-flow-quoting-the-url"),
        pytest.param(_protocol_refusal, {}, None, True, id="httpx-transport-error"),
        pytest.param(_foreign_failure, {}, None, False, id="foreign-transport-exception"),
        pytest.param(_streaming(_foreign_stream_failure, on_release=False), {}, None, False, id="foreign-stream"),
        pytest.param(_streaming(_httpx_read_failure, on_release=False), {}, None, True, id="httpx-stream-error"),
    ],
)
async def test_no_rendering_of_a_raised_error_or_the_debug_log_carries_the_webhook(
    handler: Callable[[httpx.Request], httpx.Response],
    hooks: _Hooks,
    auth: httpx.Auth | None,
    caplog: pytest.LogCaptureFixture,
    *,
    retryable: bool,
) -> None:
    caplog.set_level(logging.DEBUG)
    async with _client(handler, hooks, auth) as http:
        with pytest.raises(B24ApiError) as captured:
            await _call(http, ReplaySafety.SAFE)

    # A retryable failure arrives as the cause of the exhausted attempt budget; the chain is checked whole.
    _assert_token_unreachable(captured.value, caplog.text)
    failure = next(link for link in _chain(captured.value) if isinstance(link, TransportError))
    # None of them proves the request stayed in the process, so it counts as possibly sent.
    assert failure.possible_acceptance is True
    # One rule in every stage: HTTPX's request errors are I/O and retried; anything else would fail again.
    assert failure.retryable is retryable


@pytest.mark.asyncio
async def test_a_hook_failure_keeps_the_received_status_and_exception_class() -> None:
    async with _client(_server_error, {"response": [_raise_for_status]}) as http:
        with pytest.raises(TransportError) as captured:
            await _call(http, ReplaySafety.SAFE)

    assert captured.value.http_status == _SERVER_ERROR
    assert captured.value.to_safe_dict()["evidence"] == {**captured.value.evidence.to_dict(), "http_status": 500}
    assert "HTTPStatusError" in str(captured.value)
    assert "500" in str(captured.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("safety", [ReplaySafety.SAFE, ReplaySafety.UNSAFE])
async def test_a_builtin_timeout_inside_the_client_keeps_the_deadline_classification(safety: ReplaySafety) -> None:
    async with _client(_ok, {"request": [_hook_timing_out]}) as http:
        with pytest.raises(B24ApiError) as captured:
            await _call(http, safety)

    _assert_token_unreachable(captured.value)
    timeout = next(link for link in _chain(captured.value) if isinstance(link, TimeoutError))
    assert timeout.args == ()
    if safety is ReplaySafety.UNSAFE:
        assert isinstance(captured.value, AmbiguousExecutionError)
        assert captured.value.reason.value == "deadline_after_dispatch"


@pytest.mark.asyncio
@pytest.mark.parametrize("safety", [ReplaySafety.SAFE, ReplaySafety.UNSAFE])
@pytest.mark.parametrize("failure", [_release_failure, _foreign_stream_failure], ids=["httpx", "foreign"])
async def test_a_release_failure_after_the_last_chunk_keeps_the_received_response(
    safety: ReplaySafety, failure: Callable[[str], Exception]
) -> None:
    # The stream is released after its last chunk; the whole body had arrived by then.
    async with _client(_streaming(failure, on_release=True)) as http:
        assert await _call(http, safety) == 1


class _UnreleasableBody(httpx.AsyncByteStream):
    """Streams its chunks, optionally loses the connection after them, and always fails to close."""

    def __init__(self, url: str, chunks: tuple[bytes, ...], *, lost: bool) -> None:
        self._url = url
        self._chunks = chunks
        self._lost = lost

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk
        if self._lost:
            raise httpx.ReadError(f"connection lost for {self._url}")

    async def aclose(self) -> None:
        raise RuntimeError(f"release failed for {self._url}")


_JSON = {"content-type": "application/json"}
_SMALL_BODY_LIMIT = 1024


def _unreleasable(
    status: int, chunks: tuple[bytes, ...], *, lost: bool = False, headers: dict[str, str] = _JSON
) -> Callable[[httpx.Request], httpx.Response]:
    def respond(request: httpx.Request) -> httpx.Response:
        stream = _UnreleasableBody(str(request.url), chunks, lost=lost)
        return httpx.Response(status, headers=headers, stream=stream, request=request)

    return respond


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("respond", "expected"),
    [
        pytest.param(_unreleasable(200, (b'{"res',), lost=True), TransportError, id="lost-midway"),
        pytest.param(_unreleasable(200, (b"x" * 5000,)), ResponseTooLargeError, id="too-large"),
        pytest.param(_unreleasable(600, (b"{}",)), ProtocolError, id="status-out-of-range"),
        pytest.param(
            _unreleasable(200, (b"garbage",), headers={**_JSON, "content-encoding": "gzip"}),
            TransportError,
            id="bad-gzip",
        ),
    ],
)
async def test_a_failing_release_after_a_voided_response_stays_off_the_chain(
    respond: Callable[[httpx.Request], httpx.Response],
    expected: type[B24ApiError],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    policy = ExecutionPolicy(max_attempts_per_request=1, max_response_bytes=_SMALL_BODY_LIMIT)
    async with _client(respond) as http:
        with pytest.raises(B24ApiError) as captured:
            await _call(http, ReplaySafety.SAFE, policy=policy)

    # The release runs after the outcome is decided; its failure, which quotes the URL, is dropped.
    assert any(isinstance(link, expected) for link in _chain(captured.value))
    _assert_token_unreachable(captured.value, caplog.text)


async def _close_unread(response: httpx.Response) -> None:
    await response.aclose()


@pytest.mark.asyncio
async def test_a_response_a_hook_closed_unread_is_not_read_as_a_body() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=_Body(str(request.url), None, on_release=False), request=request)

    async with _client(respond, {"response": [_close_unread]}) as http:
        with pytest.raises(TransportError) as captured:
            await _call(http, ReplaySafety.SAFE)

    assert "StreamClosed" in str(captured.value)
    assert captured.value.retryable is False
    _assert_token_unreachable(captured.value)


class _CancelledRelease(httpx.AsyncByteStream):
    """Streams its chunks, optionally loses the connection after them, and is cancelled while closing."""

    def __init__(self, *, lost: bool) -> None:
        self._lost = lost

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b'{"result": 1, "time": {}}'
        if self._lost:
            raise httpx.ReadError("connection lost")

    async def aclose(self) -> None:
        raise asyncio.CancelledError(_CANCEL_MESSAGE)


_CANCEL_MESSAGE = "cancelled by the caller"


@pytest.mark.asyncio
@pytest.mark.parametrize("lost", [False, True], ids=["complete-body", "lost-midway"])
async def test_a_cancellation_while_releasing_is_replayed_without_the_request(*, lost: bool) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=_CancelledRelease(lost=lost), request=request)

    async with _client(respond) as http:
        transport = HttpxTransport(WEBHOOK, client=http)
        try:
            # Even a complete response yields to the cancellation, re-raised outside the HTTPX frames.
            with pytest.raises(asyncio.CancelledError) as captured:
                await transport.send(Request.bare("user.current"), attempt_timeout=1, max_response_bytes=1024)
        finally:
            await transport.aclose()

    assert captured.value.args == (_CANCEL_MESSAGE,)
    _assert_token_unreachable(captured.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("maximum", [0, -1, True])
async def test_a_non_positive_response_ceiling_is_refused_before_anything_is_sent(maximum: int) -> None:
    sent: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return _ok(request)

    async with _client(record) as http:
        transport = HttpxTransport(WEBHOOK, client=http)
        try:
            with pytest.raises(ValueError, match="max_response_bytes must be a positive integer"):
                await transport.send(Request.bare("user.current"), attempt_timeout=1, max_response_bytes=maximum)
        finally:
            await transport.aclose()
    assert sent == []


def _refuse_to_build(method: str, url: httpx.URL | str, **_kwargs: object) -> httpx.Request:
    """Refuse like HTTPX does for a value it cannot encode, quoting the credential-bearing URL."""
    raise httpx.InvalidURL(f"cannot build {method} {url}")


@pytest.mark.asyncio
@pytest.mark.parametrize("safety", [ReplaySafety.UNSAFE, ReplaySafety.UNKNOWN, ReplaySafety.SAFE])
async def test_a_request_the_client_cannot_build_was_never_sent(
    safety: ReplaySafety, monkeypatch: pytest.MonkeyPatch
) -> None:
    sent: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return _ok(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(record)) as http:
        monkeypatch.setattr(http, "build_request", _refuse_to_build)
        transport = HttpxTransport(WEBHOOK, client=http)
        try:
            async with client_for(transport) as client:
                with pytest.raises(TransportError) as captured:
                    await client.call(Request.bare("crm.item.add", replay_safety=safety))
        finally:
            await transport.aclose()

    error = captured.value
    assert not isinstance(error, AmbiguousExecutionError)
    assert error.phase is FailurePhase.NOT_DISPATCHED
    assert error.possible_acceptance is False
    assert error.retryable is False
    assert "InvalidURL" in str(error)
    assert sent == []
    _assert_token_unreachable(error)


@pytest.mark.parametrize("method", ["..", ".", ".profile", "profile.", "crm..item", "crm.item..add"])
def test_a_method_with_an_empty_name_segment_is_refused_when_the_request_is_built(method: str) -> None:
    # An empty segment is a URL dot segment: ``..`` would address the portal above the webhook token.
    with pytest.raises(ValueError, match="dot-separated names"):
        Request.bare(method)


@pytest.mark.parametrize("method", ["profile", "crm.item.add", "user_get", "tasks.task.result.list", "a1.B_2"])
def test_dot_separated_method_names_are_accepted(method: str) -> None:
    assert Request.bare(method).method == method


@pytest.mark.parametrize(
    "value",
    ["привет", "café", " leading", "trailing ", "\tleading-tab", "a\x7fb", "a\x01b", "a\rb", "a\x00b"],
)
def test_a_header_value_outside_the_rfc_9110_grammar_is_refused_when_the_headers_are_built(value: str) -> None:
    with pytest.raises(ValueError, match="visible ASCII"):
        RequestHeaders({"x-note": value})


_HEADER_VALUE_MAXIMUM = 8192


def test_an_over_long_header_value_is_refused_for_its_length() -> None:
    assert RequestHeaders({"x-note": "v" * _HEADER_VALUE_MAXIMUM}).items == (("x-note", "v" * _HEADER_VALUE_MAXIMUM),)
    with pytest.raises(ValueError, match="cannot be longer than 8192 characters"):
        RequestHeaders({"x-note": "v" * (_HEADER_VALUE_MAXIMUM + 1)})


@pytest.mark.parametrize("value", ["", "v", "a b", "a\tb", "!~ printable ASCII ~!"])
def test_visible_ascii_header_values_are_accepted(value: str) -> None:
    assert RequestHeaders({"x-note": value}).items == (("x-note", value),)


@pytest.mark.asyncio
async def test_an_accepted_header_value_reaches_the_portal_unchanged() -> None:
    seen: list[str | None] = []

    def record(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("x-note"))
        return _ok(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(record)) as http:
        transport = HttpxTransport(WEBHOOK, client=http)
        try:
            async with client_for(transport) as client:
                headers = RequestHeaders({"x-note": "a\tb c"})
                await client.call(Request.bare("profile", replay_safety=ReplaySafety.SAFE, headers=headers))
        finally:
            await transport.aclose()
    assert seen == ["a\tb c"]


_USERINFO_URLS = [
    "https://alice:PassW0rdXYZ@fixture.invalid/rest/1/tok/",
    "https://alice@fixture.invalid/rest/1/tok/",
    "https://:PassW0rdXYZ@fixture.invalid/rest/1/tok/",
]


@pytest.mark.parametrize("url", _USERINFO_URLS)
def test_settings_refuse_a_webhook_with_userinfo_without_quoting_it(url: str) -> None:
    with pytest.raises(ValidationError) as captured:
        Settings(webhook_url=url)

    (issue,) = captured.value.errors()
    assert issue["type"] == "webhook_url_userinfo"
    assert issue["loc"] == ("webhook_url",)
    assert "user name or password" in issue["msg"]
    for secret in ("PassW0rdXYZ", "alice"):
        assert secret not in str(captured.value)
        assert secret not in repr(captured.value)


@pytest.mark.parametrize("url", _USERINFO_URLS)
def test_from_webhook_refuses_userinfo(url: str) -> None:
    with pytest.raises(ValidationError, match="user name or password"):
        Bitrix24.from_webhook(url)


@pytest.mark.parametrize("url", _USERINFO_URLS)
def test_httpx_transport_refuses_userinfo_before_creating_a_client(url: str) -> None:
    with pytest.raises(ValueError, match="user name or password") as captured:
        HttpxTransport(url)
    assert "PassW0rdXYZ" not in str(captured.value)


def _without_this_files_frames(error: BaseException) -> BaseException:
    """Drop this file's frames: they hold the URL as the test's own input, not as anything the library kept."""
    kept: list[types.TracebackType] = []
    frames = error.__traceback__
    while frames is not None:
        if frames.tb_frame.f_code.co_filename != __file__:
            kept.append(frames)
        frames = frames.tb_next
    rebuilt: types.TracebackType | None = None
    for frame in reversed(kept):
        rebuilt = types.TracebackType(rebuilt, frame.tb_frame, frame.tb_lasti, frame.tb_lineno)
    return error.with_traceback(rebuilt)


_USERINFO_MARK = "PassW0rdXYZ"
_REFUSED_WEBHOOKS = {
    "userinfo": f"https://alice:{_USERINFO_MARK}@fixture.invalid/rest/1/{FRAGMENT}/",
    "not-classic": f"https://fixture.invalid/rest/1/{FRAGMENT}/extra/",
    "query": f"https://fixture.invalid/rest/1/{FRAGMENT}/?{_USERINFO_MARK}",
    "no-host": f"https:///rest/1/{FRAGMENT}/",
    "unparsable": f"https://fixture.invalid:{_USERINFO_MARK}/rest/1/{FRAGMENT}/",
    # Punycode labels HTTPX cannot decode to build a request.
    "undecodable-emoji-host": f"https://xn--ls8h.la/rest/1/{FRAGMENT}/",
    "undecodable-symbol-host": f"https://xn--i-7iq.ws/rest/1/{FRAGMENT}/",
    "invalid-punycode-host": f"https://xn--a.example/rest/1/{FRAGMENT}/",
    # Percent-encoded segments are refused, not decoded: the path sent must be the classic base itself.
    "percent-encoded-rest": f"https://fixture.invalid/%72est/1/{FRAGMENT}/",
    "encoded-slash": f"https://fixture.invalid/rest%2F1/{FRAGMENT}/",
    "percent-encoded-token": f"https://fixture.invalid/rest/1/{FRAGMENT}%2D/",
    "encoded-dot-segment": f"https://fixture.invalid/rest/%2E/1/{FRAGMENT}/",
    "ipv6-zone": f"https://[fe80::1%25eth0]/rest/1/{FRAGMENT}/",
    "port-zero": f"https://fixture.invalid:0/rest/1/{FRAGMENT}/",
    "websocket-scheme": f"wss://fixture.invalid/rest/1/{FRAGMENT}/",
}


def _failing_client(*_args: object, **_kwargs: object) -> httpx.AsyncClient:
    raise RuntimeError(f"proxy refused {WEBHOOK}")


def _client_from_settings(url: str | bytes) -> Bitrix24:
    return Bitrix24(Settings(webhook_url=url))


_ENTRYPOINTS: dict[str, Callable[[Any], object]] = {
    "transport": HttpxTransport,
    "from_webhook": Bitrix24.from_webhook,
    "client": _client_from_settings,
}
_CONSTRUCTION_CASES = [
    *(
        pytest.param(entry, url, id=f"{name}-{entry}")
        for name, url in _REFUSED_WEBHOOKS.items()
        for entry in _ENTRYPOINTS
    ),
    *(pytest.param(entry, WEBHOOK, id=f"client-init-{entry}") for entry in _ENTRYPOINTS),
    # ``Settings`` reads bytes as UTF-8, as pydantic did; the transport and ``from_webhook`` take only a string.
    *(pytest.param(entry, WEBHOOK.encode(), id=f"bytes-{entry}") for entry in ("transport", "from_webhook")),
]


@pytest.mark.parametrize(("entrypoint", "url"), _CONSTRUCTION_CASES)
def test_no_construction_error_renders_the_webhook_with_frame_locals(
    entrypoint: str,
    url: str | bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if url == WEBHOOK:
        monkeypatch.setattr(httpx, "AsyncClient", _failing_client)
    construct = _ENTRYPOINTS[entrypoint]
    with pytest.raises((ValueError, TypeError, RuntimeError)) as captured:
        construct(url)

    error = _without_this_files_frames(captured.value)
    assert error.__traceback__ is not None  # the library's own frames are still rendered
    leaked = [text for text in _rendered_texts(error) if FRAGMENT in text or _USERINFO_MARK in text]
    assert leaked == []


@pytest.mark.parametrize("host", ["xn--ls8h.la", "xn--i-7iq.ws", "XN--I-7IQ.ws"])
def test_a_host_httpx_cannot_decode_is_refused_when_the_transport_is_built(host: str) -> None:
    with pytest.raises(ValueError, match="webhook host cannot be addressed by HTTPX"):
        HttpxTransport(f"https://{host}/rest/1/{FRAGMENT}/")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("url", "host"),
    [
        (f"https://faß.de/rest/1/{FRAGMENT}/", "xn--fa-hia.de"),
        (f"https://XN--FA-HIA.de/rest/1/{FRAGMENT}/", "xn--fa-hia.de"),
        (f"https://bücher.example/rest/1/{FRAGMENT}/", "xn--bcher-kva.example"),
    ],
)
async def test_an_idna_host_is_sent_in_the_ascii_form_settings_uses(url: str, host: str) -> None:
    sent: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        sent.append(request.headers["host"])
        return _ok(request)

    async with _client(respond) as http:
        transport = HttpxTransport(url, client=http)
        try:
            async with Bitrix24(Settings(webhook_url=url), transport=transport, policy=_ONE_ATTEMPT) as client:
                assert client.host == transport.host == host
                assert await client.call(Request.bare("user.current")) == 1
        finally:
            await transport.aclose()
    assert sent == [host]


@pytest.mark.asyncio
@pytest.mark.parametrize("url", ["https://@fixture.invalid/rest/1/tok/", "https://:@fixture.invalid/rest/1/tok/"])
async def test_an_empty_userinfo_is_no_userinfo_as_in_settings(url: str) -> None:
    transport = HttpxTransport(url)
    try:
        assert transport.host == Settings(webhook_url=url).webhook_url.host == "fixture.invalid"
    finally:
        await transport.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        f"https://fixture.invalid/rest/./1/{FRAGMENT}/",
        f"https://fixture.invalid/rest/1/./{FRAGMENT}/",
        f"https://fixture.invalid/other/../rest/1/{FRAGMENT}/",
        f"HTTPS://Fixture.Invalid:443/rest/1/{FRAGMENT}",
    ],
    ids=["dot-before-user", "dot-before-token", "dot-dot", "case-default-port-no-slash"],
)
async def test_the_credential_registered_with_the_log_shield_is_the_token_as_sent(url: str) -> None:
    owned: list[tuple[str, ...]] = []

    async def record_ownership(request: httpx.Request) -> None:
        owned.append(request.extensions["b24api_log_owner"].credentials)

    sent: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        sent.append(str(request.url))
        return _ok(request)

    async with _client(respond, {"request": [record_ownership]}) as http:
        transport = HttpxTransport(url, client=http)
        try:
            # Literal dot segments are resolved, as HTTPX and Settings resolve them, before anything is derived.
            assert FRAGMENT in HTTPX_LOG_SHIELD.registered_secrets()
            async with Bitrix24(Settings(webhook_url=url), transport=transport, policy=_ONE_ATTEMPT) as client:
                assert await client.call(Request.bare("user.current")) == 1
        finally:
            await transport.aclose()
    assert sent == [f"https://fixture.invalid/rest/1/{FRAGMENT}/user.current"]
    assert owned == [(FRAGMENT,)]


# Authorities URL parsers repair silently; the rule refuses them instead.
_ODD_AUTHORITIES = [
    "https://portal.example:+443/rest/1/tok/",
    "https://portal.example:\u0668\u0660\u0668\u0660/rest/1/tok/",  # Arabic-Indic digits
    "https://portal.example:/rest/1/tok/",
    "https://portal\u3002example/rest/1/tok/",  # ideographic full stop
]
# "portal" in full-width letters, which HTTPX refuses to IDNA-encode.
_FULL_WIDTH_HOST_URL = "https://" + "\uff50\uff4f\uff52\uff54\uff41\uff4c" + ".example/rest/1/tok/"
_AGREEMENT_URLS = [
    "https://portal.example/rest/1/tok/",
    "https://portal.example/rest/1/tok",
    "http://portal.example:8080/rest/12/tok/",
    "https://Portal.Example./rest/1/tok/",
    "https://127.0.0.1/rest/1/tok/",
    "https://a_b.example/rest/1/tok/",
    "https://-portal.example/rest/1/tok/",
    "https://portal-.example/rest/1/tok/",
    "https://bitrix_web/rest/1/tok/",
    "http://web_1:8080/rest/1/tok/",
    "https://-crm.company.ru/rest/1/tok/",
    "https://b\u00fccher_x.example/rest/1/tok/",
    "https://-b\u00fccher.example/rest/1/tok/",
    "https://[::1]/rest/1/tok/",
    "https://[0:0::1]:8443/rest/1/tok/",
    "https://faß.de/rest/1/tok/",
    "https://bücher.example/rest/1/tok/",
    "https://@portal.example/rest/1/tok/",
    "https://:@portal.example/rest/1/tok/",
    "https://alice@portal.example/rest/1/tok/",
    "https://xn--ls8h.la/rest/1/tok/",
    "https://xn--a.example/rest/1/tok/",
    "https://1.2.3/rest/1/tok/",
    "https://1.2.3.4./rest/1/tok/",
    "https://01.2.3.4/rest/1/tok/",
    "https://999.1.1.1/rest/1/tok/",
    "https://portal%20example/rest/1/tok/",
    "https://portal.example:99999/rest/1/tok/",
    "https://portal.example:0/rest/1/tok/",
    "https://[fe80::1%25eth0]/rest/1/tok/",
    "ws://portal.example/rest/1/tok/",
    "portal.example/rest/1/tok/",
    " https://portal.example/rest/1/tok/",
    "https://portal.example/rest/1/tok/\n",
    "https://portal.example/rest/1/tok/extra/",
    "https://portal.example/rest/api/1/tok/",
    "https://portal.example/rest/user/tok/",
    "https://portal.example//rest/1/tok/",
    "https://portal.example/rest/1/tok;x/",
    "https://portal.example/rest/1/tok/?",
    "https://portal.example/rest/1/tok/#x",
    "https://portal.example/rest/1/tok/#",
    " \thttps://portal.example/rest/1/tok/\r\n",
    "https://portal.ex\tample/rest/1/tok/",
    "https://portal.xn--ls8h.la/rest/1/tok/",
    "https://xn---p1ai.example/rest/1/tok/",
    "https://portal.xn--p1.ai/rest/1/tok/",
    _FULL_WIDTH_HOST_URL,
    "https://0x7f.0.0.1/rest/1/tok/",
    "https://2130706433/rest/1/tok/",
    "https:portal.example/rest/1/tok/",
    "https://portal.example\\rest\\1\\tok\\",
    "https://portal.example/rest/1/" + "t" * 2100 + "/",
    "HTTPS://portal.example:443/rest/1/tok/",
    "Http://portal.example:80/rest/1/tok/",
    "HTTPS://portal.example:8443/rest/1/tok/",
    "https://[::ffff:1.2.3.4]/rest/1/tok/",
    "https://[::ffff:102:304]:8443/rest/1/tok/",
    "https://[::1.2.3.4]/rest/1/tok/",
    *_ODD_AUTHORITIES,
]


def _settings_host(url: str) -> str | None:
    """The host ``Bitrix24`` reads from what ``Settings`` stores, or ``None`` when ``Settings`` refuses."""
    with contextlib.suppress(ValidationError):
        webhook = parse_webhook(str(Settings(webhook_url=url).webhook_url))
        return webhook.host if isinstance(webhook, Webhook) else f"stored form refused: {webhook}"
    return None


async def _transport_host(url: str) -> str | None:
    with contextlib.suppress(ValueError):
        transport = HttpxTransport(url)
        await transport.aclose()
        return transport.host
    return None


@pytest.mark.asyncio
@pytest.mark.parametrize("url", _AGREEMENT_URLS)
async def test_settings_and_the_transport_accept_the_same_webhooks_with_the_same_host(url: str) -> None:
    transport_host = await _transport_host(url)
    assert _settings_host(url) == transport_host
    if transport_host is not None:
        _assert_canonical(url)


def _assert_canonical(url: str) -> None:
    """A parsed webhook is canonical: pydantic keeps its base, and the base parses back to the same webhook."""
    webhook = parse_webhook(url)
    assert isinstance(webhook, Webhook)
    assert parse_webhook(webhook.url) == webhook
    assert str(HttpUrl(webhook.url)) == webhook.url
    assert str(Settings(webhook_url=url).webhook_url) == webhook.url


@pytest.mark.parametrize("edge", [chr(code) for code in range(0x21)])
def test_every_control_character_and_space_at_the_edges_is_dropped(edge: str) -> None:
    webhook = parse_webhook(f"{edge}{edge}https://portal.example/rest/1/tok/{edge}")
    assert isinstance(webhook, Webhook)
    assert webhook.url == "https://portal.example/rest/1/tok/"


_SETTINGS_ERROR_TYPES = {
    WebhookRefusal.INVALID: "webhook_url",
    WebhookRefusal.USERINFO: "webhook_url_userinfo",
    WebhookRefusal.HOST: "webhook_url_host",
    WebhookRefusal.SHAPE: "webhook_url_shape",
}
_EXPECTED_REFUSALS = [
    pytest.param("https://portal.example/rest/user/tok/", WebhookRefusal.SHAPE, id="non-numeric-user"),
    pytest.param("https://portal.example/rest/1/tok/extra/", WebhookRefusal.SHAPE, id="extra-segment"),
    pytest.param("https://portal.example/rest/1/tok/?", WebhookRefusal.SHAPE, id="empty-query"),
    pytest.param("https://portal.example/rest/1/tok/#", WebhookRefusal.SHAPE, id="empty-fragment"),
    pytest.param("https://portal.example/rest/1/tok/#x", WebhookRefusal.SHAPE, id="fragment"),
    pytest.param("https://portal.example/%72est/1/tok/", WebhookRefusal.SHAPE, id="percent-encoded"),
    pytest.param("https://alice@portal.example/rest/1/tok/", WebhookRefusal.USERINFO, id="user-name"),
    pytest.param("https://xn--ls8h.la/rest/1/tok/", WebhookRefusal.HOST, id="undecodable-first-label"),
    pytest.param("https://xn---p1ai.example/rest/1/tok/", WebhookRefusal.HOST, id="pydantic-refused-label"),
    pytest.param("https://portal.xn--p1.ai/rest/1/tok/", WebhookRefusal.HOST, id="pydantic-refused-tld"),
    pytest.param(_FULL_WIDTH_HOST_URL, WebhookRefusal.INVALID, id="full-width-host"),
    pytest.param("https://1.2.3/rest/1/tok/", WebhookRefusal.INVALID, id="short-ipv4"),
    pytest.param("https://a*b.example/rest/1/tok/", WebhookRefusal.INVALID, id="non-dns-host"),
    pytest.param("https://a%41b.example/rest/1/tok/", WebhookRefusal.INVALID, id="percent-encoded-host"),
    pytest.param("https://[fe80::1%25eth0]/rest/1/tok/", WebhookRefusal.INVALID, id="ipv6-zone"),
    pytest.param("https://portal.example:0/rest/1/tok/", WebhookRefusal.INVALID, id="port-zero"),
    pytest.param("ws://portal.example/rest/1/tok/", WebhookRefusal.INVALID, id="websocket"),
    pytest.param("https://portal.ex\tample/rest/1/tok/", WebhookRefusal.INVALID, id="inner-tab"),
    pytest.param("https://portal.example/rest/1/" + "t" * 2100 + "/", WebhookRefusal.INVALID, id="too-long"),
    *(
        pytest.param(url, WebhookRefusal.INVALID, id=f"odd-authority-{index}")
        for index, url in enumerate(_ODD_AUTHORITIES)
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("url", "refusal"), _EXPECTED_REFUSALS)
async def test_settings_and_the_transport_refuse_with_the_same_reason(url: str, refusal: WebhookRefusal) -> None:
    assert parse_webhook(url) is refusal
    with pytest.raises(ValidationError) as captured:
        Settings(webhook_url=url)
    (issue,) = captured.value.errors()
    assert (issue["type"], issue["msg"]) == (_SETTINGS_ERROR_TYPES[refusal], refusal.value)
    with pytest.raises(ValueError, match=re.escape(refusal.value)):
        HttpxTransport(url)


_CANONICAL_BASES = [
    pytest.param("HTTPS://Portal.Example:443/rest/./1/tok", "https://portal.example/rest/1/tok/", id="normalized"),
    pytest.param(" https://portal.example/rest/1/tok/\n", "https://portal.example/rest/1/tok/", id="surrounding-space"),
    pytest.param("https://:@faß.de/rest/1/tok/", "https://xn--fa-hia.de/rest/1/tok/", id="idna-empty-userinfo"),
    pytest.param("http://[0:0::1]:8080/rest/12/tok/", "http://[::1]:8080/rest/12/tok/", id="ipv6-port"),
    # HTTPX decodes only a host that starts with a punycode label, so a later undecodable label is sent.
    pytest.param("https://portal.xn--ls8h.la/rest/1/tok/", "https://portal.xn--ls8h.la/rest/1/tok/", id="later-label"),
    # Container service names: URL parsers keep underscores and edge hyphens in ASCII labels.
    pytest.param("https://bitrix_web/rest/1/tok/", "https://bitrix_web/rest/1/tok/", id="underscore"),
    pytest.param("http://WEB_1:8080/rest/1/tok/", "http://web_1:8080/rest/1/tok/", id="underscore-port"),
    pytest.param("https://-crm.company.ru/rest/1/tok/", "https://-crm.company.ru/rest/1/tok/", id="edge-hyphen"),
    # HTTPX drops the default port only for a lower-case scheme.
    pytest.param("HTTP://portal.example:80/rest/1/tok/", "http://portal.example/rest/1/tok/", id="upper-scheme-port"),
    # Python writes an IPv4-mapped address dotted, URL parsers in hexadecimal.
    pytest.param("https://[::ffff:1.2.3.4]/rest/1/tok/", "https://[::ffff:102:304]/rest/1/tok/", id="ipv4-mapped"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("url", "canonical"), _CANONICAL_BASES)
async def test_settings_stores_and_the_transport_sends_the_canonical_base(url: str, canonical: str) -> None:
    assert parse_webhook(url) == parse_webhook(canonical)
    _assert_canonical(url)
    for value in (url, url.encode(), bytearray(url.encode()), HttpUrl(canonical)):
        assert str(Settings(webhook_url=value).webhook_url) == canonical
    sent: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        sent.append(str(request.url))
        return _ok(request)

    async with _client(respond) as http:
        transport = HttpxTransport(url, client=http)
        try:
            await transport.send(Request.bare("user.current"), attempt_timeout=1, max_response_bytes=1024)
            await transport.send(
                Request("tasks.task.get", route=RouteKind.API_V3), attempt_timeout=1, max_response_bytes=1024
            )
        finally:
            await transport.aclose()
    assert sent == [f"{canonical}user.current", canonical.replace("/rest/", "/rest/api/") + "tasks.task.get"]


def test_the_token_vault_keeps_tokens_masked_until_dropped() -> None:
    vault = TokenVault()
    token = "vault" + "-held-token"
    handle = vault.store(token)
    # Everything the vault holds, as the garbage collector reaches it, shows the token only masked.
    assert token not in repr(gc.get_referents(vault))
    assert vault.fetch(handle) == token
    vault.drop(handle)
    vault.drop(handle)
    assert vault.fetch(handle) is None


@pytest.mark.asyncio
async def test_a_refused_shield_entry_renders_no_webhook_with_frame_locals(monkeypatch: pytest.MonkeyPatch) -> None:
    async with _client(_ok) as http:
        transport = HttpxTransport(WEBHOOK, client=http)
        try:
            # The shield refuses a dispatch it has no registration for, before the request is built.
            monkeypatch.setattr(HTTPX_LOG_SHIELD, "_transports", 0)
            with pytest.raises(RuntimeError, match="no registered transport") as refused:
                await transport.send(Request.bare("user.current"), attempt_timeout=1, max_response_bytes=1024)
            monkeypatch.undo()
        finally:
            await transport.aclose()
    for text in _rendered_texts(refused.value):
        assert FRAGMENT not in text


def test_a_refused_log_ownership_renders_no_credential_with_frame_locals() -> None:
    with pytest.raises(ValueError, match="requires the webhook credential") as refused:
        LogOwnership((FRAGMENT, ""))
    for text in _rendered_texts(refused.value):
        assert FRAGMENT not in text
