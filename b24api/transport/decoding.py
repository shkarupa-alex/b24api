"""Bound decompressed response bodies inside the decompressor, before any block is allocated.

HTTPX's own ``aiter_bytes`` inflates a whole received chunk before a caller can look at its size, so a
32 KiB gzip body can allocate tens of megabytes and a ``gzip, gzip`` cascade over a gigabyte before a
byte ceiling is checked. The decoder here reads raw transfer bytes instead and never asks zlib for more
than one byte past the remaining budget. Only one ``identity``, ``gzip`` or ``deflate`` coding can be
bounded this way; a stacked, unknown, ``br`` or ``zstd`` coding is refused before decompression.
"""

from __future__ import annotations
import asyncio
import zlib
from dataclasses import dataclass
from typing import Self

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
        self._decompressor: zlib._Decompress | None = zlib.decompressobj(_GZIP_WBITS) if coding == "gzip" else None

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
            # Deflate is sent zlib-wrapped by most servers and raw by some; decide once, from a complete
            # header, never from a first fragment that may be a single byte.
            self._prefix += raw
            if len(self._prefix) < _ZLIB_HEADER_BYTES:
                return b""
            raw, self._prefix = self._prefix, b""
            wrapped = _zlib_wrapped(raw)
            self._decompressor = zlib.decompressobj(zlib.MAX_WBITS if wrapped else _RAW_DEFLATE_WBITS)
            self._replay = bytearray() if wrapped else None
        if self._replay is not None:
            return self._inflate_framing_guess(raw, self._replay)
        return self._inflate(raw)

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

    def finish(self) -> bytes:
        """Return what remains after the last raw chunk.

        Raises:
            _DecodedBodyTooLargeError: The decoded body would exceed the ceiling.
            zlib.error: The raw bytes are not a valid stream of the declared coding.
        """
        if self._coding == _IDENTITY:
            return b""
        if self._decompressor is None:
            # Fewer than two bytes cannot carry a zlib header, so the body is raw deflate.
            self._decompressor = zlib.decompressobj(_RAW_DEFLATE_WBITS)
            remainder = self._inflate(self._prefix)
            self._prefix = b""
            return remainder + self._flush()
        return self._flush()

    def _inflate(self, data: bytes) -> bytes:
        decompressor = self._decompressor
        if decompressor is None:  # pragma: no cover - feed/finish always choose a decompressor first
            raise RuntimeError("bounded decoder has no decompressor")
        pieces: list[bytes] = []
        # Bytes after the end of the stream are dropped, as HTTPX drops them, and never handed back to zlib,
        # whose ``unused_data`` would otherwise accumulate them without bound.
        while data and not decompressor.eof:
            pieces.append(self._count(decompressor.decompress(data, self._limit - self._produced + 1)))
            data = decompressor.unconsumed_tail
        return b"".join(pieces)

    def _flush(self) -> bytes:
        decompressor = self._decompressor
        if decompressor is None or decompressor.eof:
            return b""
        # ``_inflate`` drained every unconsumed tail, so no pending input remains for ``flush`` to inflate
        # without a length bound; a truncated stream is tolerated here exactly as HTTPX tolerates it.
        return self._count(decompressor.flush())

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
    """Decode raw bytes under the ceiling without propagating credential-bearing HTTPX exceptions."""
    body = bytearray()
    try:
        # A refused coding fails here, before any body byte is read.
        decoder = _BoundedDecoder.for_encoding(response.headers.get("content-encoding"), limit=maximum)
        if response.is_stream_consumed:
            # An in-memory response (a mock or custom transport) arrives already read and decoded; its
            # allocation happened inside that transport, so only the ceiling remains to enforce.
            body.extend(_BoundedDecoder(_IDENTITY, limit=maximum).feed(response.content))
        else:
            # Raw transfer bytes only: ``aiter_bytes`` would inflate a whole chunk before its size is known.
            async for chunk in response.aiter_raw():
                body.extend(decoder.feed(chunk))
            body.extend(decoder.finish())
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
