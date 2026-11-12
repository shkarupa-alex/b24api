"""Bound decompressed response bodies inside the decompressor, before any block is allocated.

HTTPX's own ``aiter_bytes`` inflates a whole received chunk before a caller can look at its size, so a
32 KiB gzip body can allocate tens of megabytes and a ``gzip, gzip`` cascade over a gigabyte before a
byte ceiling is checked. The decoder here reads raw transfer bytes instead and never asks zlib for more
than one byte past the remaining budget. Only one ``identity``, ``gzip`` or ``deflate`` coding can be
bounded this way; a stacked, unknown, ``br`` or ``zstd`` coding is refused before decompression. A compressed
stream that ends before its end marker is refused too, where HTTPX would return the decoded prefix, and every
member of a multi-member gzip body is decoded, as ``gzip.decompress`` decodes them, where HTTPX keeps the first.
"""

from __future__ import annotations
import asyncio
import zlib
from dataclasses import dataclass
from typing import Self, cast

import httpx

#: The codings b24api advertises on the requests it owns; exactly the ones this decoder can bound.
BOUNDED_ACCEPT_ENCODING = "gzip, deflate"
_IDENTITY = "identity"
#: The failure every undecodable body shares, whether HTTPX, zlib or the coding check refused it.
BODY_READ_FAILURE = "Transport failed while reading the response body"
_GZIP_WBITS = zlib.MAX_WBITS | 16
_RAW_DEFLATE_WBITS = -zlib.MAX_WBITS
_ZLIB_HEADER_BYTES = 2
_ZLIB_HEADER_CHECK = 31
_ZLIB_DEFLATE_METHOD = 8
_ZLIB_MAX_WINDOW_INFO = 7
_GZIP_PADDING = b"\x00"


class _DecodeRefusedError(Exception):
    """The response declares a content coding the bounded decoder cannot verify; nothing was inflated."""


class _DecodedBodyTooLargeError(Exception):
    """The decoded body would exceed the byte ceiling; the excess block was never produced."""


def _zlib_wrapped(header: bytes) -> bool:
    """Decide from the first two bytes whether ``deflate`` arrived zlib-wrapped (RFC 1950) or raw (RFC 1951)."""
    method_and_window, flags = header[0], header[1]
    return (
        method_and_window & 0x0F == _ZLIB_DEFLATE_METHOD
        and method_and_window >> 4 <= _ZLIB_MAX_WINDOW_INFO
        and (method_and_window << 8 | flags) % _ZLIB_HEADER_CHECK == 0
    )


class _BoundedDecoder:
    """Inflate one response body without ever holding more than ``limit + 1`` decoded bytes per call."""

    __slots__ = ("_coding", "_decompressor", "_limit", "_prefix", "_produced", "_replay")

    def __init__(self, coding: str, *, limit: int) -> None:
        """Bind one supported coding; use :meth:`for_encoding` to parse a response header."""
        self._coding = coding
        self._limit = limit
        self._produced = 0
        self._prefix = b""
        # Raw deflate input kept while a zlib-framing guess has decoded nothing, so the guess can be undone.
        self._replay: bytearray | None = None
        self._decompressor: zlib._Decompress | None = None

    @classmethod
    def for_encoding(cls, content_encoding: str | None, *, limit: int) -> Self:
        """Accept identity or exactly one gzip/deflate coding; refuse stacked and unknown codings."""
        codings = [
            coding
            for coding in (part.strip().casefold() for part in (content_encoding or "").split(","))
            if coding and coding != _IDENTITY
        ]
        if not codings:
            return cls(_IDENTITY, limit=limit)
        if len(codings) > 1 or codings[0] not in {"gzip", "deflate"}:
            raise _DecodeRefusedError("response content coding cannot be decoded within the byte ceiling")
        return cls(codings[0], limit=limit)

    def feed(self, raw: bytes) -> bytes:
        """Return the decoded bytes of one raw chunk, raising before the ceiling would be crossed.

        Raises:
            _DecodedBodyTooLargeError: The decoded body would exceed the ceiling.
            zlib.error: The raw bytes are not a valid stream of the declared coding.
        """
        if self._coding == _IDENTITY:
            return self._count(raw)
        if self._decompressor is None:
            # The decompressor waits for two raw bytes, so a body without any stays empty. Deflate is sent
            # zlib-wrapped by most servers and raw by some; decide once, from a complete header, never from a
            # first fragment that may be a single byte.
            self._prefix += raw
            if len(self._prefix) < _ZLIB_HEADER_BYTES:
                return b""
            raw, self._prefix = self._prefix, b""
            self._decompressor = self._open(raw)
        if self._replay is not None:
            return self._inflate_framing_guess(raw, self._replay)
        return self._inflate(raw)

    def _open(self, header: bytes) -> zlib._Decompress:
        """Choose the decompressor from the first raw bytes: gzip, or deflate zlib-wrapped or raw."""
        if self._coding == "gzip":
            return zlib.decompressobj(_GZIP_WBITS)
        wrapped = _zlib_wrapped(header)
        self._replay = bytearray() if wrapped else None
        return zlib.decompressobj(zlib.MAX_WBITS if wrapped else _RAW_DEFLATE_WBITS)

    def _inflate_framing_guess(self, raw: bytes, replay: bytearray) -> bytes:
        """Inflate under the zlib-framing guess while it has decoded nothing.

        A raw stream whose first two bytes happen to pass the zlib header check fails under that guess before
        any output; its input is then replayed as raw deflate, as HTTPX falls back.
        """
        replay += raw
        try:
            decoded = self._inflate(raw)
        except zlib.error:
            if self._produced:
                raise
            self._replay = None
            self._decompressor = zlib.decompressobj(_RAW_DEFLATE_WBITS)
            return self._inflate(bytes(replay))
        if decoded or len(replay) > self._limit:
            # Decoded output proves the framing; the kept input never outgrows the decoded ceiling.
            self._replay = None
        return decoded

    def finish(self) -> None:
        """Check, after the last raw chunk, that a compressed body reached the end of its stream.

        ``feed`` has already returned every decoded byte: ``_inflate`` hands zlib all input with room for one
        byte past the ceiling, so a stream that stopped within the ceiling has no output pending. An unbounded
        ``flush()`` is never needed.

        Raises:
            zlib.error: The raw bytes ended before the end of the compressed stream.
        """
        if self._coding == _IDENTITY or (self._decompressor is None and not self._prefix):
            # No raw byte at all under a compression coding is an empty body, as HTTPX reads it.
            return
        if self._decompressor is None or not self._decompressor.eof:
            # A truncated stream, or gzip member, is refused, as ``gzip.decompress`` and ``zlib.decompress``
            # refuse it, where HTTPX returns the decoded prefix. One byte is truncated too: no framing fits in it.
            raise zlib.error("compressed response body ended before the end of its stream")

    def _inflate(self, data: bytes) -> bytes:
        decompressor = self._decompressor
        if decompressor is None:  # pragma: no cover - feed always chooses a decompressor first
            raise RuntimeError("bounded decoder has no decompressor")
        pieces: list[bytes] = []
        while data:
            if decompressor.eof:
                if self._coding != "gzip":
                    # Bytes after a zlib or raw deflate stream are dropped, never handed back to zlib, whose
                    # ``unused_data`` would otherwise accumulate them without bound.
                    break
                # After a gzip member, zero padding is skipped and anything else must start the next member,
                # as ``gzip.decompress`` reads them; the ceiling spans every member.
                data = data.lstrip(_GZIP_PADDING)
                if not data:
                    break
                decompressor = self._decompressor = zlib.decompressobj(_GZIP_WBITS)
            pieces.append(self._count(decompressor.decompress(data, self._limit - self._produced + 1)))
            data = decompressor.unused_data if decompressor.eof else decompressor.unconsumed_tail
        return b"".join(pieces)

    def _count(self, decoded: bytes) -> bytes:
        self._produced += len(decoded)
        if self._produced > self._limit:
            raise _DecodedBodyTooLargeError("decoded response body exceeded the byte ceiling")
        return decoded


@dataclass(frozen=True, slots=True)
class BodyReadOutcome:
    """What reading one response body produced, carrying no credential-bearing HTTPX object."""

    body: bytes | None = None
    cancellation_args: tuple[object, ...] | None = None
    transport_failure: str | None = None
    too_large: bool = False


async def read_bounded_body(response: httpx.Response, maximum: int) -> BodyReadOutcome:
    """Decode raw bytes under the ceiling; HTTPX request errors and decoding failures become an outcome."""
    body = bytearray()
    try:
        # A refused coding fails here, before any body byte is read.
        decoder = _BoundedDecoder.for_encoding(response.headers.get("content-encoding"), limit=maximum)
        if response.is_stream_consumed:
            # A body read inside the injected client, by its transport (``httpx.Response(content=...)`` in a
            # ``MockTransport``) or by a response hook (``await response.aread()``), arrives decoded by HTTPX,
            # which checks no end of stream and keeps only the first gzip member. Its raw bytes may still sit
            # behind the client's stream wrapper, but the public API cannot tell an in-memory stream from a
            # released network one, whose replay would read a connection back in the pool. Only the ceiling
            # remains to enforce.
            body.extend(_BoundedDecoder(_IDENTITY, limit=maximum).feed(response.content))
        else:
            await _read_raw(response, decoder, body)
            decoder.finish()
    except _DecodedBodyTooLargeError:
        return BodyReadOutcome(too_large=True)
    except asyncio.CancelledError as error:
        return BodyReadOutcome(cancellation_args=error.args)
    except (
        _DecodeRefusedError,
        httpx.ReadError,
        httpx.ReadTimeout,
        httpx.RemoteProtocolError,
        httpx.DecodingError,
        zlib.error,
    ):
        return BodyReadOutcome(transport_failure=BODY_READ_FAILURE)
    except httpx.TransportError:
        return BodyReadOutcome(transport_failure="Unclassified transport failure while reading the response body")
    except httpx.RequestError:
        return BodyReadOutcome(transport_failure="HTTP client failed while reading the response body")
    return BodyReadOutcome(body=bytes(body))


async def _read_raw(response: httpx.Response, decoder: _BoundedDecoder, body: bytearray) -> None:
    """Feed every raw transfer chunk to the decoder; ``aiter_bytes`` would inflate a chunk before its size is known.

    The stream is read directly, not through ``aiter_raw()``, which also closes it after the last chunk:
    the transport's own release is then the only close, and its failure cannot void a complete body.
    """
    if response.is_closed:
        raise httpx.StreamClosed  # a hook closed the response unread; ``aiter_raw()`` refuses it alike
    # ``AsyncClient`` wraps every response stream in an asynchronous one before any hook sees it.
    async for chunk in cast("httpx.AsyncByteStream", response.stream):
        body.extend(decoder.feed(chunk))
