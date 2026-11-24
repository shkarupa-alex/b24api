"""Immutable request-side wire representation contracts."""

from __future__ import annotations
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum

_HEADER_TOKEN = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]{1,128}$")
# RFC 9110 field-value, checked before dispatch: visible ASCII with inner spaces and tabs, nothing around it.
# The grammar's obsolete non-ASCII obs-text is refused too, since HTTPX encodes header values as ASCII.
_HEADER_VALUE = re.compile(r"(?:[\x21-\x7e](?:[\t\x20-\x7e]*[\x21-\x7e])?)?")
_FORBIDDEN_HEADERS = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "host",
        "content-length",
        "content-type",
        "transfer-encoding",
        "connection",
        "upgrade",
        "te",
        "trailer",
        "keep-alive",
        "expect",
        "x-method-override",
        "x-http-method-override",
    },
)
_FORBIDDEN_PREFIXES = ("proxy-", "forwarded", "x-forwarded-", "x-http-method-")
_HEADER_COUNT_MAXIMUM = 32
_HEADER_VALUE_MAXIMUM = 8192
_HEADER_PAIR_LENGTH = 2
_CONTENT_TYPE_MAXIMUM = 256
_TCHARS = r"[!#$%&'*+.^_`|~0-9A-Za-z-]+"
_QUOTED_STRING = r'"(?:[\t \x21\x23-\x5b\x5d-\x7e]|\\[\t \x21-\x7e])*"'
# RFC 9110 media-type: type "/" subtype *( OWS ";" OWS [ parameter ] ), parameter = token "=" ( token / quoted-string ).
_MEDIA_TYPE = re.compile(
    rf"{_TCHARS}/{_TCHARS}(?:[\t ]*;[\t ]*(?:{_TCHARS}=(?:{_TCHARS}|{_QUOTED_STRING}))?)*",
)


class BodyEncoding(StrEnum):
    """Where and how a request's named parameters travel.

    ``JSON`` and ``FORM_URLENCODED`` send them as a POST body. ``QUERY_STRING`` sends them in the URL's query,
    in the PHP bracket encoding of form bodies: a GET without a body, or a POST of the request's raw
    ``content`` when it has one, for the actions that read only a query or a raw body.
    """

    JSON = "json"
    FORM_URLENCODED = "form_urlencoded"
    QUERY_STRING = "query_string"


@dataclass(frozen=True, slots=True, init=False)
class RequestHeaders:
    """Validated method-scoped headers without client-owned protocol fields."""

    items: tuple[tuple[str, str], ...]

    def __init__(self, items: Mapping[str, str] | Iterable[tuple[str, str]] = ()) -> None:
        """Validate, normalize and freeze header pairs."""
        source = items.items() if isinstance(items, Mapping) else items
        object.__setattr__(self, "items", _validate_headers(tuple(source)))

    @property
    def names(self) -> tuple[str, ...]:
        """Return normalized header names without values."""
        return tuple(name for name, _ in self.items)


@dataclass(frozen=True, slots=True, init=False)
class RawContent:
    """A raw request body sent byte for byte, with the content type its caller declares.

    The representation names the content type and length only, never the bytes.
    """

    data: bytes = field(repr=False)
    content_type: str

    def __init__(self, data: bytes, content_type: str) -> None:
        """Validate the content type and keep the bytes."""
        _validate_raw_content(data, content_type)
        object.__setattr__(self, "data", data)
        object.__setattr__(self, "content_type", content_type)

    @property
    def byte_length(self) -> int:
        """Return the body's length in bytes."""
        return len(self.data)

    def __repr__(self) -> str:
        """Return a representation without the bytes."""
        return f"RawContent(content_type={self.content_type!r}, byte_length={self.byte_length!r})"


def _validate_raw_content(data: object, content_type: object) -> None:
    """Refuse raw content whose bytes are no bytes or whose content type is no media type (RFC 9110, 8.3.1).

    A transport calls it again before it sends, as it does ``_validate_headers``: a forged value never leaves.
    """
    if not isinstance(data, bytes):
        raise TypeError("raw content data must be bytes")
    if not isinstance(content_type, str):
        raise TypeError("raw content type must be a string")
    if (
        len(content_type) > _CONTENT_TYPE_MAXIMUM
        or not _HEADER_VALUE.fullmatch(content_type)
        or not _MEDIA_TYPE.fullmatch(content_type)
    ):
        raise ValueError("raw content type must be a media type such as application/octet-stream")


def _validate_headers(items: tuple[tuple[str, str], ...]) -> tuple[tuple[str, str], ...]:
    if len(items) > _HEADER_COUNT_MAXIMUM:
        raise ValueError("request headers cannot contain more than 32 entries")
    normalized: list[tuple[str, str]] = []
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, tuple) or len(item) != _HEADER_PAIR_LENGTH:
            raise TypeError("request headers must be name/value pairs")
        name, value = item
        if not isinstance(name, str) or not isinstance(value, str):
            raise TypeError("request header names and values must be strings")
        lowered = name.casefold()
        if not _HEADER_TOKEN.fullmatch(name) or any(character in name for character in "\r\n\0"):
            raise ValueError("request header name is invalid")
        if len(value) > _HEADER_VALUE_MAXIMUM:
            raise ValueError(f"request header value cannot be longer than {_HEADER_VALUE_MAXIMUM} characters")
        if not _HEADER_VALUE.fullmatch(value):
            raise ValueError("request header value must be visible ASCII, with spaces or tabs only between characters")
        if lowered in seen:
            raise ValueError("duplicate request header name")
        if lowered in _FORBIDDEN_HEADERS or any(lowered.startswith(prefix) for prefix in _FORBIDDEN_PREFIXES):
            raise ValueError(f"request header is reserved: {lowered}")
        seen.add(lowered)
        normalized.append((lowered, value))
    return tuple(sorted(normalized))


__all__ = ["BodyEncoding", "RawContent", "RequestHeaders"]
