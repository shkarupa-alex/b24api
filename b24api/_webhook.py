"""The one parse of a Bitrix webhook URL, shared by ``Settings`` and the HTTPX transport.

A webhook is accepted only in its classic form, ``http[s]://<host>[:<port>]/rest/<user>/<token>/``,
read the way HTTPX reads it to send and representable as pydantic's ``HttpUrl``, which ``Settings``
stores. Everything derived from it (the host, the method URLs and the secret registered for
redaction) comes from the one ``Webhook`` value, so no two readers disagree.
"""

from __future__ import annotations
import contextlib
import ipaddress
import re
import secrets
import uuid
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType

import httpx
from pydantic import HttpUrl, TypeAdapter, ValidationError

_DEFAULT_PORTS = MappingProxyType({"http": 80, "https": 443})
# Percent-encoded characters are refused rather than decoded, so the path HTTPX sends is the classic base
# itself; HTTPX resolves literal ``.`` and ``..`` segments while parsing.
_CLASSIC_PATH = re.compile(r"/rest/(?P<user>[0-9]+)/(?P<token>[A-Za-z0-9_-]+)/?")
# A label as written: ASCII letters, digits, ``_`` and ``-`` (as in container service names such as
# ``bitrix_web``, which URL parsers keep unchanged), or non-ASCII characters, which IDNA then encodes or
# refuses, other than the full stops IDNA would read as dots.
_LABEL = r"(?:[A-Za-z0-9_-]|[^\x00-\x7f\u3002\uff0e\uff61])+"
# The authority as written, since URL parsers silently repair other forms (``:+443``, ``portal。example``,
# a backslash, a missing ``//``): any userinfo (refused separately), then a bracketed IPv6 address or
# labels with an optional root dot, then an optional port of ASCII digits.
_AUTHORITY = re.compile(
    rf"[A-Za-z][A-Za-z0-9+.-]*://(?:[^/?#\\]*@)?"
    rf"(?:\[[0-9A-Fa-f:.]+\]|(?:{_LABEL}\.)*{_LABEL}\.?)"
    r"(?::[0-9]+)?(?=[/?#]|$)"
)
# A host whose last label is a number is an IPv4 address to URL parsers; only a dotted quad is accepted.
_NUMERIC_LABEL = re.compile(r"(?:[0-9]+|0x[0-9a-f]*)")
_PORT_MAXIMUM = 65535
# Leading and trailing C0 controls and spaces are dropped, as URL parsers drop them; any other is refused.
_SURROUNDING = "".join(map(chr, range(0x21)))
_HTTP_URL: TypeAdapter[HttpUrl] = TypeAdapter(HttpUrl)


class WebhookRefusal(Enum):
    """Why a webhook URL is refused; no message carries any part of the URL."""

    INVALID = "webhook URL is not a valid http or https URL"
    USERINFO = "webhook URL must not contain a user name or password"
    HOST = "webhook host cannot be addressed by HTTPX"
    SHAPE = "webhook URL must be a classic /rest/<user>/<token>/ base"


@dataclass(frozen=True, slots=True)
class WebhookAddress:
    """Everything about a webhook but its token, so it can be kept in plain attributes."""

    scheme: str
    #: ASCII and lower case, IDNA-encoded; an IPv6 address without brackets.
    host: str
    #: ``None`` for the scheme's default port.
    port: int | None
    user: str

    def url(self, token: str) -> str:
        """The classic base exactly as it is sent, method names appended."""
        return f"{self._origin}/rest/{self.user}/{token}/"

    def api_v3_url(self, token: str) -> str:
        """The REST 3.0 base of the same webhook."""
        return f"{self._origin}/rest/api/{self.user}/{token}/"

    @property
    def _origin(self) -> str:
        host = f"[{self.host}]" if ":" in self.host else self.host
        port = "" if self.port is None else f":{self.port}"
        return f"{self.scheme}://{host}{port}"


@dataclass(frozen=True, slots=True)
class Webhook:
    """A parsed classic webhook; its ``repr`` leaves the token out."""

    address: WebhookAddress
    token: str = field(repr=False)

    @property
    def host(self) -> str:
        """The ASCII host requests are sent to."""
        return self.address.host

    @property
    def url(self) -> str:
        """The classic base exactly as it is sent."""
        return self.address.url(self.token)

    @property
    def api_v3_url(self) -> str:
        """The REST 3.0 base of the same webhook."""
        return self.address.api_v3_url(self.token)


def parse_webhook(url: object) -> Webhook | WebhookRefusal:
    """Parse a webhook URL, never raising: a raising frame would keep the URL in its locals.

    HTTPX raises some failures only when a property is read; any of them is a refusal. A parsed webhook
    is canonical: ``parse_webhook(webhook.url) == webhook``, and pydantic's ``HttpUrl`` keeps
    ``webhook.url`` as it is.
    """
    with contextlib.suppress(Exception):
        return _parse(url)
    return WebhookRefusal.INVALID


def _parse(url: object) -> Webhook | WebhookRefusal:
    if not isinstance(url, str):
        return WebhookRefusal.INVALID
    url = url.strip(_SURROUNDING)
    parsed = httpx.URL(url)
    checked = _checked(url, parsed)
    if isinstance(checked, WebhookRefusal):
        return checked
    host, port, path = checked
    webhook = Webhook(WebhookAddress(parsed.scheme, host, port, path["user"]), path["token"])
    return _unrepresentable(webhook) or webhook


def _checked(url: str, parsed: httpx.URL) -> tuple[str, int | None, re.Match[str]] | WebhookRefusal:
    """Apply the rule in order, the first unmet condition naming the refusal; return the canonical parts."""
    default_port = _DEFAULT_PORTS.get(parsed.scheme)
    if default_port is None:
        return WebhookRefusal.INVALID
    # An empty user name and password are no userinfo. The path token is the credential; HTTPX would not
    # send userinfo as Basic auth, only log it.
    if parsed.username or parsed.password:
        return WebhookRefusal.USERINFO
    host = _ascii_host(parsed) if _AUTHORITY.match(url) else None
    # HTTPX drops the default port only for a lower-case scheme, so the port is compared here.
    port = None if parsed.port == default_port else parsed.port
    if host is None or not (port is None or 1 <= port <= _PORT_MAXIMUM):
        return WebhookRefusal.INVALID
    if not _decodable_host(parsed):
        return WebhookRefusal.HOST
    path = _CLASSIC_PATH.fullmatch(parsed.raw_path.decode("ascii"))
    # HTTPX drops an empty fragment, so the URL as written is checked for one.
    if path is None or "#" in url:
        return WebhookRefusal.SHAPE
    return host, port, path


def _ascii_host(parsed: httpx.URL) -> str | None:
    """Return the host as sent, in the one form both HTTPX and ``Settings`` read alike, or ``None``."""
    host = parsed.raw_host.decode("ascii")
    if ":" in host:
        address = ipaddress.IPv6Address(host)
        # Python writes an IPv4-mapped address with its IPv4 part dotted, URL parsers in hexadecimal.
        if address.ipv4_mapped is None:
            return address.compressed
        mapped = int(address.ipv4_mapped)
        return f"::ffff:{mapped >> 16:x}:{mapped & 0xFFFF:x}"
    if _NUMERIC_LABEL.fullmatch(host.rstrip(".").rsplit(".", 1)[-1]):
        return host if str(ipaddress.IPv4Address(host)) == host else None
    return host


def _decodable_host(parsed: httpx.URL) -> bool:
    """Whether HTTPX can decode the host, as it does to build every request.

    HTTPX decodes a host that starts with a punycode label, and IDNA 2008 refuses some such labels that
    URL parsers otherwise accept (``xn--ls8h.la``); every call to such a host would fail before dispatch.
    """
    with contextlib.suppress(Exception):
        return bool(parsed.host)
    return False


def _unrepresentable(webhook: Webhook) -> WebhookRefusal | None:
    """Refuse what ``Settings`` could not store as pydantic's ``HttpUrl``: too long, or a host it refuses."""
    try:
        _HTTP_URL.validate_python(webhook.url)
    except ValidationError as error:
        too_long = any(issue["type"] == "url_too_long" for issue in error.errors(include_input=False))
        return WebhookRefusal.INVALID if too_long else WebhookRefusal.HOST
    return None


class TokenVault:
    """Webhook tokens under opaque handles, masked, so no rendering of their holder shows one."""

    __slots__ = ("_entries",)

    def __init__(self) -> None:
        """Start empty."""
        self._entries: dict[str, tuple[bytes, bytes]] = {}

    def store(self, token: str) -> str:
        """Keep a token and return its handle."""
        handle = uuid.uuid4().hex
        plaintext = token.encode()
        key = secrets.token_bytes(len(plaintext))
        self._entries[handle] = (bytes(left ^ right for left, right in zip(plaintext, key, strict=True)), key)
        return handle

    def fetch(self, handle: str) -> str | None:
        """Return the token of a handle, or ``None`` once it is dropped."""
        entry = self._entries.get(handle)
        if entry is None:
            return None
        ciphertext, key = entry
        return bytes(left ^ right for left, right in zip(ciphertext, key, strict=True)).decode()

    def drop(self, handle: str) -> None:
        """Forget a token; dropping twice is harmless."""
        self._entries.pop(handle, None)
