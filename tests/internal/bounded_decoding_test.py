"""Decompression is bounded inside zlib, before a block past the response ceiling is ever allocated (A2)."""

from __future__ import annotations
import gzip
import tracemalloc
import zlib
from typing import TYPE_CHECKING

import httpx
import pytest

from b24api import Bitrix24, ExecutionPolicy, ReplaySafety, Settings
from b24api.contracts import Request, RequestHeaders, RouteKind
from b24api.errors import BudgetExceededError, FailurePhase, ResponseTooLargeError, TransportError
from b24api.transport import HttpxTransport
from b24api.transport.decoding import (
    BOUNDED_ACCEPT_ENCODING,
    _BoundedDecoder,
    _DecodedBodyTooLargeError,
    _DecodeRefusedError,
)

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


def _feed(decoder: _BoundedDecoder, raw: bytes, chunk: int) -> bytes:
    """Feed ``raw`` in ``chunk``-byte pieces, or whole when ``chunk`` is 0."""
    size = chunk or max(len(raw), 1)
    return b"".join(decoder.feed(raw[index : index + size]) for index in range(0, len(raw), size))


def _decode(coding: str, raw: bytes, chunk: int, *, limit: int = _LIMIT) -> bytes:
    """Decode a whole body fed in ``chunk``-byte pieces, checking the end of its stream."""
    decoder = _BoundedDecoder.for_encoding(coding, limit=limit)
    decoded = _feed(decoder, raw, chunk)
    decoder.finish()
    return decoded


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
    decoder.finish()

    assert decoded == payload


# A non-final stored block with a set padding bit: 0x081d passes the zlib header check, yet it is raw deflate.
_STORED_JSON = b'{"result":"abcdefghijklmnop"}'
_RAW_DEFLATE_PASSING_THE_ZLIB_CHECK = b"\x08\x1d\x00\xe2\xff" + _STORED_JSON + b"\x01\x00\x00\xff\xff"


@pytest.mark.asyncio
@pytest.mark.parametrize("chunk", [1, len(_RAW_DEFLATE_PASSING_THE_ZLIB_CHECK)], ids=["one-byte", "whole"])
async def test_raw_deflate_that_passes_the_zlib_header_check_is_replayed_as_raw(chunk: int) -> None:
    encoded = _RAW_DEFLATE_PASSING_THE_ZLIB_CHECK
    assert zlib.decompress(encoded, -zlib.MAX_WBITS) == _STORED_JSON
    decoder = _BoundedDecoder.for_encoding("deflate", limit=len(_STORED_JSON))
    decoded = b"".join(decoder.feed(encoded[index : index + chunk]) for index in range(0, len(encoded), chunk))
    decoder.finish()
    assert decoded == _STORED_JSON

    transport, client = _transport(
        lambda request: httpx.Response(
            _OK, headers={"content-encoding": "deflate"}, stream=_RawStream(encoded, chunk_size=chunk), request=request
        )
    )
    try:
        assert await _send(transport, limit=len(_STORED_JSON)) == _STORED_JSON
        with pytest.raises(ResponseTooLargeError, match="byte ceiling"):
            await _send(transport, limit=len(_STORED_JSON) - 1)
    finally:
        await transport.aclose()
        await client.aclose()


def test_deflate_framing_guess_is_final_once_it_decoded_output_or_both_framings_fail() -> None:
    corrupt = zlib.compress(_PAYLOAD)[:2] + b"\xff" * 16
    with pytest.raises(zlib.error):
        _BoundedDecoder.for_encoding("deflate", limit=_LIMIT).feed(corrupt)

    wrapped = zlib.compress(_PAYLOAD)
    decoder = _BoundedDecoder.for_encoding("deflate", limit=_LIMIT)
    assert decoder.feed(wrapped[:-4]) == _PAYLOAD
    with pytest.raises(zlib.error):
        decoder.feed(b"\x00" * 4 + b"\xff" * 8)


def test_raw_deflate_split_inside_its_two_bytes_is_decided_from_both() -> None:
    encoded = _deflate(b"", raw=True)
    decoder = _BoundedDecoder.for_encoding("deflate", limit=1)
    assert decoder.feed(encoded[:1]) == b""

    assert decoder.feed(encoded[1:]) == b""
    decoder.finish()


def test_decoder_never_asks_zlib_for_more_than_one_byte_past_the_ceiling() -> None:
    decoder = _BoundedDecoder.for_encoding("gzip", limit=len(_PAYLOAD))
    assert decoder.feed(gzip.compress(_PAYLOAD)) == _PAYLOAD
    decoder.finish()

    over = _BoundedDecoder.for_encoding("gzip", limit=len(_PAYLOAD) - 1)
    with pytest.raises(Exception, match="byte ceiling") as caught:
        over.feed(gzip.compress(_PAYLOAD))
    assert type(caught.value).__name__ == "_DecodedBodyTooLargeError"


_GZIP = gzip.compress(_PAYLOAD)
_ZLIB = _deflate(_PAYLOAD, raw=False)
_RAW = _deflate(_PAYLOAD, raw=True)
_GZIP_TRAILER = 8  # CRC-32 and ISIZE
_ZLIB_TRAILER = 4  # Adler-32
_TRUNCATED_STREAMS = [
    pytest.param("gzip", _GZIP[: len(_GZIP) // 2], id="gzip-middle"),
    pytest.param("gzip", _GZIP[:-_GZIP_TRAILER], id="gzip-without-crc-and-size"),
    pytest.param("gzip", _GZIP[:-1], id="gzip-without-last-byte"),
    pytest.param("gzip", _GZIP[:1], id="gzip-first-byte"),
    pytest.param("gzip", _GZIP[:2], id="gzip-first-two-bytes"),
    pytest.param("deflate", _ZLIB[: len(_ZLIB) // 2], id="zlib-middle"),
    pytest.param("deflate", _ZLIB[:-_ZLIB_TRAILER], id="zlib-without-adler32"),
    pytest.param("deflate", _ZLIB[:1], id="zlib-first-byte"),
    pytest.param("deflate", _ZLIB[:2], id="zlib-header-only"),
    pytest.param("deflate", _RAW[: len(_RAW) // 2], id="raw-middle"),
    pytest.param("deflate", _RAW[:-1], id="raw-without-last-byte"),
    pytest.param("deflate", _RAW[:1], id="raw-first-byte"),
    pytest.param("deflate", _RAW[:2], id="raw-first-two-bytes"),
    pytest.param("deflate", _RAW_DEFLATE_PASSING_THE_ZLIB_CHECK[:4], id="raw-under-the-zlib-guess"),
    pytest.param("deflate", _RAW_DEFLATE_PASSING_THE_ZLIB_CHECK[:-5], id="raw-replayed-after-the-zlib-guess"),
]
_DEFLATE_STREAMS = [pytest.param(_ZLIB, id="zlib"), pytest.param(_RAW, id="raw")]
_FILE = bytes(range(256)) * 128


@pytest.mark.parametrize("chunk", [1, 0], ids=["one-byte", "whole"])
@pytest.mark.parametrize(("coding", "raw"), _TRUNCATED_STREAMS)
def test_a_compressed_stream_that_ends_before_its_end_marker_is_refused(coding: str, raw: bytes, chunk: int) -> None:
    decoder = _BoundedDecoder.for_encoding(coding, limit=_LIMIT)
    _feed(decoder, raw, chunk)

    with pytest.raises(zlib.error, match="ended before the end of its stream"):
        decoder.finish()


@pytest.mark.asyncio
@pytest.mark.parametrize(("coding", "raw"), _TRUNCATED_STREAMS)
async def test_a_truncated_compressed_body_is_the_body_read_failure(coding: str, raw: bytes) -> None:
    transport, client = _transport(
        lambda request: httpx.Response(
            _OK,
            headers={"content-encoding": coding, "content-length": str(len(raw))},
            stream=_RawStream(raw),
            request=request,
        )
    )
    try:
        with pytest.raises(TransportError, match=_BODY_FAILURE) as caught:
            await _send(transport)
    finally:
        await transport.aclose()
        await client.aclose()
    assert caught.value.phase is FailurePhase.BODY_PARTIALLY_RECEIVED
    assert caught.value.retryable


@pytest.mark.asyncio
async def test_call_bytes_refuses_a_truncated_gzip_body_instead_of_returning_its_prefix() -> None:
    encoded = gzip.compress(_FILE)
    raw = encoded[: len(encoded) // 2]
    with pytest.raises(EOFError):
        gzip.decompress(raw)

    def respond(request: httpx.Request) -> httpx.Response:
        headers = {"content-encoding": "gzip", "content-length": str(len(raw))}
        return httpx.Response(_OK, headers=headers, stream=_RawStream(raw), request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    webhook = "https://portal.invalid/rest/1/token/"
    policy = ExecutionPolicy(max_attempts_per_request=1, binary_digest=True)
    try:
        async with Bitrix24(Settings(webhook_url=webhook), transport=HttpxTransport(webhook, client=client)) as api:
            with pytest.raises(BudgetExceededError) as caught:
                await api.call_bytes(
                    Request("crm.item.getfile", {"fileId": 7}, ReplaySafety.SAFE, route=RouteKind.BARE), policy=policy
                )
    finally:
        await client.aclose()
    cause = caught.value.__cause__
    assert isinstance(cause, TransportError)
    assert str(cause) == _BODY_FAILURE


@pytest.mark.parametrize("chunks", [(), (b"",)], ids=["no-chunk", "empty-chunk"])
@pytest.mark.parametrize("coding", ["gzip", "deflate"])
def test_no_raw_byte_under_a_compression_coding_is_an_empty_body(coding: str, chunks: tuple[bytes, ...]) -> None:
    decoder = _BoundedDecoder.for_encoding(coding, limit=_LIMIT)

    assert b"".join(decoder.feed(chunk) for chunk in chunks) == b""
    decoder.finish()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [_OK, 204])
@pytest.mark.parametrize("coding", ["gzip", "deflate"])
async def test_an_empty_body_with_a_compression_coding_stays_empty(coding: str, status: int) -> None:
    transport, client = _transport(
        lambda request: httpx.Response(
            status, headers={"content-encoding": coding}, stream=_RawStream(b""), request=request
        )
    )
    try:
        assert await _send(transport) == b""
    finally:
        await transport.aclose()
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("chunk", [1, 65_536], ids=["one-byte", "whole"])
@pytest.mark.parametrize("raw", _DEFLATE_STREAMS)
async def test_bytes_after_a_complete_deflate_stream_are_still_dropped(raw: bytes, chunk: int) -> None:
    transport, client = _transport(
        lambda request: httpx.Response(
            _OK,
            headers={"content-encoding": "deflate"},
            stream=_RawStream(raw + b"trailing garbage", chunk_size=chunk),
            request=request,
        )
    )
    try:
        assert await _send(transport) == _PAYLOAD
    finally:
        await transport.aclose()
        await client.aclose()


@pytest.mark.parametrize(
    ("raw", "filler"),
    [(_GZIP, b"\x00"), (_ZLIB, b"x"), (_RAW, b"x")],
    ids=["gzip-zero-padding", "zlib-trailing-bytes", "raw-trailing-bytes"],
)
def test_bytes_skipped_after_a_complete_stream_are_not_retained(raw: bytes, filler: bytes) -> None:
    decoder = _BoundedDecoder.for_encoding("gzip" if raw is _GZIP else "deflate", limit=len(_PAYLOAD))
    decoded = decoder.feed(raw + filler * 16)
    for _ in range(1_000):
        decoded += decoder.feed(filler * 65_536)
    decoder.finish()

    assert decoded == _PAYLOAD


_MEMBERS = (b"a" * 100, _FILE, b"", b"z" * 5)


@pytest.mark.parametrize("chunk", [1, 7, 0], ids=["one-byte", "seven-bytes", "whole"])
@pytest.mark.parametrize("padding", [b"", b"\x00" * 3], ids=["adjacent", "zero-padded"])
@pytest.mark.parametrize("count", [2, 3, 4], ids=["two", "three-with-an-empty-one", "four"])
def test_every_gzip_member_is_decoded_as_gzip_decompress_decodes_it(count: int, padding: bytes, chunk: int) -> None:
    raw = padding.join(gzip.compress(member) for member in _MEMBERS[:count]) + padding
    expected = b"".join(_MEMBERS[:count])
    assert gzip.decompress(raw) == expected

    assert _decode("gzip", raw, chunk, limit=len(expected)) == expected


_SECOND_MEMBER = gzip.compress(_FILE)
_AFTER_A_MEMBER_REFUSED = [
    pytest.param(_GZIP + _SECOND_MEMBER[: len(_SECOND_MEMBER) // 2], id="second-member-cut"),
    pytest.param(_GZIP + _SECOND_MEMBER[:-_GZIP_TRAILER], id="second-member-without-crc-and-size"),
    pytest.param(_GZIP + _SECOND_MEMBER[:1], id="second-member-first-byte"),
    pytest.param(_GZIP + b"\x00\x00" + _SECOND_MEMBER[:-1], id="padded-member-cut"),
    pytest.param(_GZIP + b"trailing garbage", id="garbage"),
    pytest.param(_GZIP + b"\x00\x00trailing garbage", id="garbage-after-padding"),
]


@pytest.mark.parametrize("chunk", [1, 0], ids=["one-byte", "whole"])
@pytest.mark.parametrize("raw", _AFTER_A_MEMBER_REFUSED)
def test_bytes_after_a_gzip_member_that_are_no_whole_member_are_refused(raw: bytes, chunk: int) -> None:
    with pytest.raises((EOFError, gzip.BadGzipFile)):
        gzip.decompress(raw)
    with pytest.raises(zlib.error):
        _decode("gzip", raw, chunk)


@pytest.mark.asyncio
@pytest.mark.parametrize("chunk", [1, 65_536], ids=["one-byte", "whole"])
async def test_a_multi_member_gzip_body_is_returned_whole(chunk: int) -> None:
    raw = gzip.compress(_PAYLOAD) + gzip.compress(_FILE)
    transport, client = _transport(
        lambda request: httpx.Response(
            _OK, headers={"content-encoding": "gzip"}, stream=_RawStream(raw, chunk_size=chunk), request=request
        )
    )
    try:
        assert await _send(transport) == _PAYLOAD + _FILE
    finally:
        await transport.aclose()
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", _AFTER_A_MEMBER_REFUSED)
async def test_bytes_after_a_gzip_member_that_are_no_whole_member_are_the_body_read_failure(raw: bytes) -> None:
    transport, client = _transport(
        lambda request: httpx.Response(
            _OK, headers={"content-encoding": "gzip"}, stream=_RawStream(raw), request=request
        )
    )
    try:
        with pytest.raises(TransportError, match=_BODY_FAILURE):
            await _send(transport)
    finally:
        await transport.aclose()
        await client.aclose()


def test_the_ceiling_spans_every_gzip_member() -> None:
    raw = gzip.compress(_PAYLOAD) * 2
    decoder = _BoundedDecoder.for_encoding("gzip", limit=2 * len(_PAYLOAD))
    assert decoder.feed(raw) == _PAYLOAD * 2
    decoder.finish()

    over = _BoundedDecoder.for_encoding("gzip", limit=2 * len(_PAYLOAD) - 1)
    with pytest.raises(_DecodedBodyTooLargeError):
        over.feed(raw)


@pytest.mark.asyncio
async def test_a_bomb_spread_over_gzip_members_is_refused_inside_the_decompressor() -> None:
    bomb = gzip.compress(bytes(_MEGABYTE)) * (_BOMB_SIZE // _MEGABYTE)
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
@pytest.mark.parametrize("reader", ["transport", "hook"])
@pytest.mark.parametrize(
    "raw",
    [gzip.compress(_FILE)[:200], gzip.compress(_FILE) + gzip.compress(_PAYLOAD)],
    ids=["truncated", "two-members"],
)
async def test_a_body_read_inside_the_injected_client_is_taken_as_httpx_decoded_it(raw: bytes, reader: str) -> None:
    """Pin the documented limitation: HTTPX decoded the body, and its raw bytes are out of public reach."""

    def respond(request: httpx.Request) -> httpx.Response:
        if reader == "transport":
            return httpx.Response(_OK, headers={"content-encoding": "gzip"}, content=raw, request=request)
        return httpx.Response(_OK, headers={"content-encoding": "gzip"}, stream=_RawStream(raw), request=request)

    async def read_body(response: httpx.Response) -> None:
        await response.aread()

    hooks = {"response": [read_body]} if reader == "hook" else {}
    client = httpx.AsyncClient(transport=httpx.MockTransport(respond), event_hooks=hooks)
    transport = HttpxTransport("https://portal.invalid/rest/1/token/", client=client)
    try:
        body = await _send(transport)
    finally:
        await transport.aclose()
        await client.aclose()
    assert body
    assert _FILE.startswith(body)
    assert len(body) <= len(_FILE)


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
