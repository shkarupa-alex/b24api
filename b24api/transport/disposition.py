"""Pure Content-Disposition filename reading over raw response headers (RFC 6266, RFC 8187).

The decoded name is metadata, not a path: it is never normalized, truncated or reduced to a basename,
and no rejection reason here carries the header value, so nothing of the name reaches diagnostics.
"""

from __future__ import annotations
import re
import unicodedata

from b24api.contracts.response import FilenameStatus, _usable_filename

_HEADER_NAME = "content-disposition"
_HEADER_MAXIMUM = 8192
_TOKEN = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+")
_EXT_VALUE = re.compile(r"(?P<charset>[!#$%&+^_`{}~0-9A-Za-z-]+)'(?P<language>[0-9A-Za-z-]*)'(?P<chars>.*)", re.DOTALL)
_EXT_CHARS = re.compile(r"(?:%[0-9A-Fa-f]{2}|[!#$&+.^_`|~0-9A-Za-z-])*")
_EXT_CHARSETS = frozenset({"utf-8", "iso-8859-1"})
_PLAIN = "filename"
_EXTENDED = "filename*"


class _MalformedError(ValueError):
    """A structural defect that makes the whole header unreadable."""


def disposition_filename(headers: tuple[tuple[str, str], ...]) -> tuple[str | None, FilenameStatus]:
    """Return the decoded Content-Disposition filename and its status from raw response headers.

    No header is ``ABSENT``; a repeated header, an overlong value, an unclosed quoted-string, a control
    character, a repeated ``filename``/``filename*`` parameter, or filename candidates none of which is
    usable is ``MALFORMED``. A valid ``filename*`` (UTF-8 or ISO-8859-1) takes precedence over a valid plain
    ``filename``. Only ``PARSED`` carries a name.
    """
    values = [value for name, value in headers if name.isascii() and name.lower() == _HEADER_NAME]
    if not values:
        return None, FilenameStatus.ABSENT
    try:
        parameters = _parameters(values)
    except _MalformedError:
        return None, FilenameStatus.MALFORMED
    if not parameters:
        return None, FilenameStatus.ABSENT
    # A usable name is never empty, so ``or`` falls back exactly when filename* is absent or unusable.
    filename = _extended_filename(parameters.get(_EXTENDED)) or _plain_filename(parameters.get(_PLAIN))
    return (filename, FilenameStatus.PARSED) if filename is not None else (None, FilenameStatus.MALFORMED)


def _parameters(values: list[str]) -> dict[str, str]:
    """Split the single bounded header into its type and raw parameter values, keeping only the filename ones."""
    if len(values) > 1:
        raise _MalformedError
    value = values[0]
    if len(value) > _HEADER_MAXIMUM or len(value.encode("utf-8", errors="surrogatepass")) > _HEADER_MAXIMUM:
        raise _MalformedError
    # A control code point anywhere in the raw header (C0, DEL or C1; HTAB is whitespace) makes it malformed.
    if any(char != "\t" and unicodedata.category(char) == "Cc" for char in value):
        raise _MalformedError
    disposition_type, *segments = _split_outside_quotes(value)
    if not _TOKEN.fullmatch(disposition_type.strip(" \t")):
        raise _MalformedError
    parameters: dict[str, str] = {}
    for segment in segments:
        stripped = segment.strip(" \t")
        if not stripped:
            continue
        name, separator, raw = stripped.partition("=")
        name = name.rstrip(" \t")
        if not separator or not _TOKEN.fullmatch(name):
            raise _MalformedError
        name = name.lower()  # an ASCII token: Unicode case folding cannot turn another name into filename
        if name in {_PLAIN, _EXTENDED}:
            if name in parameters:
                raise _MalformedError
            parameters[name] = raw.lstrip(" \t")
    return parameters


def _split_outside_quotes(value: str) -> list[str]:
    """Split on semicolons outside quoted-strings; an unclosed quoted-string is malformed."""
    segments: list[str] = []
    current: list[str] = []
    quoted = escaped = False
    for char in value:
        if escaped:
            escaped = False
        elif quoted and char == "\\":
            escaped = True
        elif char == '"':
            quoted = not quoted
        elif char == ";" and not quoted:
            segments.append("".join(current))
            current = []
            continue
        current.append(char)
    if quoted:
        raise _MalformedError
    segments.append("".join(current))
    return segments


def _plain_filename(raw: str | None) -> str | None:
    """Decode a token or a complete quoted-string; anything else is not a usable candidate."""
    decoded: str | None = None
    if raw is not None and _TOKEN.fullmatch(raw):
        decoded = raw
    elif raw is not None and len(raw) > 1 and raw[0] == raw[-1] == '"':
        decoded = _unquote(raw[1:-1])
    return decoded if _usable_filename(decoded) else None


def _unquote(inner: str) -> str | None:
    """Resolve quoted-pairs; a bare quote inside means the value was not one quoted-string."""
    result: list[str] = []
    escaped = False
    for char in inner:
        if escaped:
            result.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == '"':
            return None
        else:
            result.append(char)
    return None if escaped else "".join(result)


def _extended_filename(raw: str | None) -> str | None:
    """Strictly decode an RFC 8187 ext-value: known charset, valid percent triplets, no plus-as-space."""
    match = None if raw is None else _EXT_VALUE.fullmatch(raw)
    if match is None or match["charset"].casefold() not in _EXT_CHARSETS or not _EXT_CHARS.fullmatch(match["chars"]):
        return None
    chars = match["chars"]
    encoded = bytearray()
    index = 0
    while index < len(chars):
        if chars[index] == "%":
            encoded.append(int(chars[index + 1 : index + 3], 16))
            index += 3
        else:
            encoded.append(ord(chars[index]))
            index += 1
    try:
        decoded = bytes(encoded).decode(match["charset"].casefold(), errors="strict")
    except UnicodeDecodeError:
        return None
    return decoded if _usable_filename(decoded) else None
