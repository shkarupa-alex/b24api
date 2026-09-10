"""Immutable request-side wire representation contracts."""

from __future__ import annotations
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum

_HEADER_TOKEN = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]{1,128}$")
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


class BodyEncoding(StrEnum):
    """Supported request body representations."""

    JSON = "json"
    FORM_URLENCODED = "form_urlencoded"


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
        if len(value) > _HEADER_VALUE_MAXIMUM or any(character in value for character in "\r\n\0"):
            raise ValueError("request header value is invalid")
        if lowered in seen:
            raise ValueError("duplicate request header name")
        if lowered in _FORBIDDEN_HEADERS or any(lowered.startswith(prefix) for prefix in _FORBIDDEN_PREFIXES):
            raise ValueError(f"request header is reserved: {lowered}")
        seen.add(lowered)
        normalized.append((lowered, value))
    return tuple(sorted(normalized))


__all__ = ["BodyEncoding", "RequestHeaders"]
