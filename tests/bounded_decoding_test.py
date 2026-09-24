"""Decompression is bounded inside zlib, before a block past the response ceiling is ever allocated (A2)."""

from __future__ import annotations
import gzip
import tracemalloc
import zlib
from typing import TYPE_CHECKING

import httpx
import pytest

from b24api.contracts import Request, RequestHeaders, RouteKind
from b24api.errors import ResponseTooLargeError, TransportError
from b24api.transport import HttpxTransport
from b24api.transport.decoding import BOUNDED_ACCEPT_ENCODING, _BoundedDecoder, _DecodeRefusedError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

_OK = 200
_LIMIT = 1 << 20
_BOMB_SIZE = 64 << 20
_PAYLOAD = b'{"result":{"value":"' + b"x" * 4096 + b'"}}'
_BODY_FAILURE = "Transport failed while reading the response body"
_MEGABYTE = 1 << 20


class _RawStream(httpx.AsyncByteStream):
    """Serve raw transfer bytes in fixed chunks and record whether any were read."""

    def __init__(self, raw: bytes, *, chunk_size: int = 65_536) -> None:
        self.raw = raw
        self.chunk_size = chunk_size
        self.served = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for index in range(0, len(self.raw), self.chunk_size):
            chunk = self.raw[index : index + self.chunk_size]
            self.served += len(chunk)
            yield chunk


def _transport(respond: Callable[[httpx.Request], httpx.Response]) -> tuple[HttpxTransport, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    return HttpxTransport("https://portal.invalid/rest/1/token/", client=client), client


async def _send(transport: HttpxTransport, *, limit: int = _LIMIT) -> bytes:
    response = await transport.send(
        Request("profile", route=RouteKind.BARE), attempt_timeout=5, max_response_bytes=limit
    )
    return response.body


def _cascade(payload: bytes) -> bytes:
    return gzip.compress(gzip.compress(payload))


def _deflate(payload: bytes, *, raw: bool) -> bytes:
    compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS if raw else zlib.MAX_WBITS)
    return compressor.compress(payload) + compressor.flush()


@pytest.mark.asyncio
async def test_single_gzip_bomb_is_refused_inside_the_decompressor() -> None:
    bomb = gzip.compress(bytes(_BOMB_SIZE))
    transport, client = _transport(
        lambda request: httpx.Response(
            _OK, headers={"content-encoding": "gzip"}, stream=_RawStream(bomb), request=request
        )
    )
    tracemalloc.start()
    try:
        with pytest.raises(ResponseTooLargeError, match="byte ceiling"):
            await _send(transport)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        await transport.aclose()
        await client.aclose()
    assert peak < 2 * _LIMIT + _MEGABYTE


@pytest.mark.asyncio
async def test_stacked_gzip_cascade_is_refused_before_decompression() -> None:
    cascade = _cascade(bytes(_BOMB_SIZE))
    stream = _RawStream(cascade)
    transport, client = _transport(
        lambda request: httpx.Response(_OK, headers={"content-encoding": "gzip, gzip"}, stream=stream, request=request)
    )
    tracemalloc.start()
    try:
        with pytest.raises(TransportError, match=_BODY_FAILURE) as caught:
            await _send(transport)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        await transport.aclose()
        await client.aclose()
    assert stream.served == 0
    assert caught.value.possible_acceptance
    assert peak < 2 * _LIMIT + _MEGABYTE


@pytest.mark.asyncio
@pytest.mark.parametrize("coding", ["br", "zstd", "compress", "x-gzip", "gzip, deflate", "deflate, identity, gzip"])
async def test_unbounded_or_stacked_codings_are_refused_before_reading(coding: str) -> None:
    stream = _RawStream(gzip.compress(_PAYLOAD))
    transport, client = _transport(
        lambda request: httpx.Response(_OK, headers={"content-encoding": coding}, stream=stream, request=request)
    )
    try:
        with pytest.raises(TransportError, match=_BODY_FAILURE):
            await _send(transport)
    finally:
        await transport.aclose()
        await client.aclose()
    assert stream.served == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("coding", "raw"),
    [
        ("gzip", gzip.compress(_PAYLOAD)),
        ("GZip", gzip.compress(_PAYLOAD)),
        ("deflate", _deflate(_PAYLOAD, raw=False)),
        ("deflate", _deflate(_PAYLOAD, raw=True)),
        ("identity", _PAYLOAD),
        ("identity, gzip", gzip.compress(_PAYLOAD)),
        ("", _PAYLOAD),
    ],
    ids=["gzip", "gzip-case", "deflate-zlib", "deflate-raw", "identity", "identity-then-gzip", "empty"],
)
async def test_supported_codings_decode_when_fed_one_byte_at_a_time(coding: str, raw: bytes) -> None:
    transport, client = _transport(
        lambda request: httpx.Response(
            _OK, headers={"content-encoding": coding}, stream=_RawStream(raw, chunk_size=1), request=request
        )
    )
    try:
        assert await _send(transport) == _PAYLOAD
    finally:
        await transport.aclose()
        await client.aclose()


@pytest.mark.parametrize("raw", [False, True], ids=["zlib", "raw"])
@pytest.mark.parametrize("payload", [b"", b"x", _PAYLOAD], ids=["empty", "one-byte", "payload"])
def test_deflate_chooses_its_framing_once_from_a_complete_header(raw: bool, payload: bytes) -> None:  # noqa: FBT001
    encoded = _deflate(payload, raw=raw)
    decoder = _BoundedDecoder.for_encoding("deflate", limit=len(payload))
    decoded = b"".join(decoder.feed(encoded[index : index + 1]) for index in range(len(encoded)))

    assert decoded + decoder.finish() == payload


def test_raw_deflate_shorter_than_a_zlib_header_is_decided_at_finish() -> None:
    encoded = _deflate(b"", raw=True)
    decoder = _BoundedDecoder.for_encoding("deflate", limit=1)
    assert decoder.feed(encoded[:1]) == b""

    assert decoder.feed(encoded[1:]) + decoder.finish() == b""


def test_decoder_never_asks_zlib_for_more_than_one_byte_past_the_ceiling() -> None:
    decoder = _BoundedDecoder.for_encoding("gzip", limit=len(_PAYLOAD))
    assert decoder.feed(gzip.compress(_PAYLOAD)) + decoder.finish() == _PAYLOAD

    over = _BoundedDecoder.for_encoding("gzip", limit=len(_PAYLOAD) - 1)
    with pytest.raises(Exception, match="byte ceiling") as caught:
        over.feed(gzip.compress(_PAYLOAD))
    assert type(caught.value).__name__ == "_DecodedBodyTooLargeError"


def test_trailing_bytes_after_the_gzip_stream_are_dropped_without_retention() -> None:
    decoder = _BoundedDecoder.for_encoding("gzip", limit=len(_PAYLOAD))
    decoded = decoder.feed(gzip.compress(_PAYLOAD) + b"trailing")
    for _ in range(1_000):
        decoded += decoder.feed(b"x" * 65_536)

    assert decoded + decoder.finish() == _PAYLOAD


@pytest.mark.parametrize("coding", ["br", "zstd", "gzip, gzip", "unknown"])
def test_decoder_refuses_codings_it_cannot_bound(coding: str) -> None:
    with pytest.raises(_DecodeRefusedError):
        _BoundedDecoder.for_encoding(coding, limit=_LIMIT)


@pytest.mark.asyncio
async def test_invalid_gzip_stream_is_the_same_body_read_failure_as_before() -> None:
    transport, client = _transport(
        lambda request: httpx.Response(
            _OK, headers={"content-encoding": "gzip"}, stream=_RawStream(b"not-a-gzip-stream"), request=request
        )
    )
    try:
        with pytest.raises(TransportError, match=_BODY_FAILURE) as caught:
            await _send(transport)
    finally:
        await transport.aclose()
        await client.aclose()
    assert caught.value.retryable


@pytest.mark.asyncio
async def test_exact_ceiling_passes_and_one_more_byte_is_too_large() -> None:
    raw = gzip.compress(_PAYLOAD)

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(_OK, headers={"content-encoding": "gzip"}, stream=_RawStream(raw), request=request)

    transport, client = _transport(respond)
    try:
        assert await _send(transport, limit=len(_PAYLOAD)) == _PAYLOAD
        with pytest.raises(ResponseTooLargeError):
            await _send(transport, limit=len(_PAYLOAD) - 1)
    finally:
        await transport.aclose()
        await client.aclose()


def _observe_accept_encoding(seen: list[str | None]) -> Callable[[httpx.Request], httpx.Response]:
    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("accept-encoding"))
        return httpx.Response(_OK, json={"result": True}, request=request)

    return respond


@pytest.mark.asyncio
async def test_owned_client_advertises_only_bounded_codings(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str | None] = []
    real_client = httpx.AsyncClient
    # Stand in for brotli/zstandard being installed: HTTPX would then advertise them on its own.
    monkeypatch.setattr(httpx._client, "ACCEPT_ENCODING", "gzip, deflate, br, zstd")  # noqa: SLF001
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: real_client(
            transport=httpx.MockTransport(_observe_accept_encoding(seen)),
            headers={"accept-encoding": "gzip, deflate, br, zstd"},
            **kwargs,
        ),
    )
    transport = HttpxTransport("https://portal.invalid/rest/1/token/")
    try:
        await _send(transport)
    finally:
        await transport.aclose()
    assert seen == [BOUNDED_ACCEPT_ENCODING]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("client_header", "caller_header", "expected"),
    [
        (None, None, BOUNDED_ACCEPT_ENCODING),
        ("br", None, "br"),
        (None, "identity", "identity"),
        ("br", "gzip", "gzip"),
    ],
    ids=["httpx-default", "injected-choice", "caller-choice", "caller-over-injected"],
)
async def test_injected_client_and_caller_accept_encoding_are_not_rewritten(
    monkeypatch: pytest.MonkeyPatch,
    client_header: str | None,
    caller_header: str | None,
    expected: str,
) -> None:
    monkeypatch.setattr(httpx._client, "ACCEPT_ENCODING", "gzip, deflate, br, zstd")  # noqa: SLF001
    seen: list[str | None] = []
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_observe_accept_encoding(seen)),
        headers={"accept-encoding": client_header or "gzip, deflate, br, zstd"},
    )
    transport = HttpxTransport("https://portal.invalid/rest/1/token/", client=client)
    headers = RequestHeaders({"Accept-Encoding": caller_header}) if caller_header else RequestHeaders()
    try:
        await transport.send(
            Request("profile", route=RouteKind.BARE, headers=headers),
            attempt_timeout=1,
            max_response_bytes=_LIMIT,
        )
    finally:
        await transport.aclose()
        await client.aclose()
    assert seen == [expected]


@pytest.mark.asyncio
async def test_a_coding_the_injected_client_admitted_is_refused_on_arrival() -> None:
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                _OK, headers={"content-encoding": "br"}, stream=_RawStream(b"\x0b\x00\x80"), request=request
            )
        ),
        headers={"accept-encoding": "br"},
    )
    transport = HttpxTransport("https://portal.invalid/rest/1/token/", client=client)
    try:
        with pytest.raises(TransportError, match=_BODY_FAILURE):
            await _send(transport)
    finally:
        await transport.aclose()
        await client.aclose()
