"""Transport lifecycle, replay-aware retries, and shared rate coordination."""

from __future__ import annotations
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol, cast, runtime_checkable

from b24api.contracts.json import FrozenMapping, JsonValue, _thaw_json
from b24api.contracts.request import ReplaySafety, Request, RequestSummary, ResultErrorSpec, RouteKind
from b24api.contracts.response import _safe_media_type
from b24api.contracts.wire import BodyEncoding, RequestHeaders
from b24api.errors import CapabilityError

if TYPE_CHECKING:
    from b24api.contracts.positional import PositionalArguments

_HTTP_STATUS_MINIMUM = 100
_HTTP_STATUS_MAXIMUM = 599


@dataclass(frozen=True, slots=True)
class WireResponse:
    """Complete bounded transport response consumed by the protocol layer."""

    status_code: int
    headers: tuple[tuple[str, str], ...]
    body: bytes

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        if not _HTTP_STATUS_MINIMUM <= self.status_code <= _HTTP_STATUS_MAXIMUM:
            raise ValueError("HTTP status must be between 100 and 599")
        object.__setattr__(self, "headers", tuple(self.headers))
        object.__setattr__(self, "body", bytes(self.body))

    @property
    def header_map(self) -> dict[str, str]:
        """Return the header map."""
        return {name.casefold(): value for name, value in self.headers}

    @property
    def content_type(self) -> str | None:
        """Return the declared content type, when present."""
        return self.header_map.get("content-type")

    @property
    def byte_length(self) -> int:
        """Return the exact bounded body length."""
        return len(self.body)

    def __repr__(self) -> str:
        """Return value-free wire evidence."""
        content_type = _safe_media_type(self.content_type)
        return (
            f"WireResponse(status_code={self.status_code!r}, content_type={content_type!r}, "
            f"byte_length={self.byte_length!r})"
        )


@dataclass(frozen=True, slots=True)
class TransportCapabilities:
    """Advanced request features explicitly honored by a transport."""

    encodings: frozenset[BodyEncoding] = frozenset({BodyEncoding.JSON})
    scoped_headers: bool = False
    positional_json: bool = False
    # A route is served only when declared: a transport that ignores it would answer a V3 or .json
    # request from the classic handler, which silently reaches another endpoint.
    routes: frozenset[RouteKind] = frozenset({RouteKind.BARE})

    def __post_init__(self) -> None:
        """Validate and freeze advertised capabilities."""
        object.__setattr__(self, "encodings", frozenset(self.encodings))
        object.__setattr__(self, "routes", frozenset(self.routes))
        if not self.routes or any(not isinstance(route, RouteKind) for route in self.routes):
            raise TypeError("transport routes must be a non-empty set of RouteKind values")
        if not self.encodings or BodyEncoding.JSON not in self.encodings:
            raise ValueError("transport encodings must include JSON")
        if any(not isinstance(encoding, BodyEncoding) for encoding in self.encodings):
            raise TypeError("transport encodings must be BodyEncoding values")
        if not isinstance(self.scoped_headers, bool):
            raise TypeError("scoped_headers must be a bool")
        if not isinstance(self.positional_json, bool):
            raise TypeError("positional_json must be a bool")


@dataclass(frozen=True, slots=True, init=False)
class WireRequest:
    """Transport-facing request without a caller-controlled destination."""

    method: str
    route: RouteKind
    replay_safety: ReplaySafety
    encoding: BodyEncoding
    headers: RequestHeaders
    result_error: ResultErrorSpec | None
    _parameters: FrozenMapping = field(repr=False)
    _positional: PositionalArguments | None = field(repr=False)

    def __init__(self, request: Request) -> None:
        """Build from an already-canonical request."""
        if not isinstance(request, Request):
            raise TypeError("wire request requires a canonical Request")
        object.__setattr__(self, "method", request.method)
        object.__setattr__(self, "route", request.route)
        object.__setattr__(self, "replay_safety", request.replay_safety)
        object.__setattr__(self, "encoding", request.encoding)
        object.__setattr__(self, "headers", request.headers)
        object.__setattr__(self, "result_error", request.result_error)
        object.__setattr__(self, "_parameters", request._parameters)  # noqa: SLF001 - canonical immutable handoff
        object.__setattr__(self, "_positional", request.positional)

    @property
    def positional(self) -> PositionalArguments | None:
        """Return exact positional arguments, if present."""
        return self._positional

    @property
    def parameters(self) -> MappingProxyType[str, JsonValue]:
        """Return detached read-only parameters."""
        return MappingProxyType(self.copy_parameters())

    def copy_parameters(self) -> dict[str, JsonValue]:
        """Return a detached mutable parameter tree."""
        if self._positional is not None:
            raise ValueError("positional arguments have no named parameter mapping")
        return cast("dict[str, JsonValue]", _thaw_json(self._parameters))

    @property
    def summary(self) -> RequestSummary:
        """Return bounded value-free request evidence."""
        return RequestSummary(
            method=self.method,
            parameter_keys=(f"@{self._positional.layout_id}",) if self._positional else tuple(sorted(self._parameters)),
            encoding=self.encoding,
            header_names=self.headers.names,
            route=self.route,
        )

    def __repr__(self) -> str:
        """Return a representation that excludes parameters and header values."""
        return f"WireRequest(summary={self.summary!r}, encoding={self.encoding!r})"


class Transport(Protocol):
    """One cancellable attempt that honors ``attempt_timeout`` and classifies failures.

    Implementations must not suppress cancellation indefinitely. The executor's
    operation deadline is only a hard public bound when the injected transport
    cooperates with cancellation or returns within ``attempt_timeout``. A send-only
    transport cannot declare routes, so the executor gives it only ``RouteKind.BARE``
    requests; a ``WireTransport`` declares the routes it builds in its capabilities.
    """

    @property
    def host(self) -> str:
        """Return the normalized portal host without credentials."""
        ...

    async def send(
        self,
        request: Request,
        *,
        attempt_timeout: float,
        max_response_bytes: int,
    ) -> WireResponse:
        """Send one transport request attempt."""
        ...


@runtime_checkable
class WireTransport(Transport, Protocol):
    """Transport capable of explicitly advertised request representations."""

    capabilities: TransportCapabilities

    async def send_wire(
        self,
        request: WireRequest,
        *,
        attempt_timeout: float,
        max_response_bytes: int,
    ) -> WireResponse:
        """Send one advanced wire request attempt."""
        ...


def preflight_transport(transport: WireTransport | None, request: Request) -> None:
    """Reject unsupported request representation before budget reservation or I/O."""
    if request.positional is not None and (
        transport is None
        or not isinstance(transport.capabilities, TransportCapabilities)
        or not transport.capabilities.positional_json
    ):
        raise CapabilityError("transport does not support positional JSON arguments", request_summary=request.summary)
    advanced = request.encoding.value != "json" or bool(request.headers.items)
    if transport is None:
        # A send-only transport cannot declare routes, so it may serve only the classic bare route.
        if request.route is not RouteKind.BARE:
            raise CapabilityError(f"transport does not build the {request.route.value} route")
        if advanced:
            raise CapabilityError("transport does not support advanced request delivery")
        return
    capabilities = transport.capabilities
    if not isinstance(capabilities, TransportCapabilities):
        raise CapabilityError("transport exposes malformed capabilities")
    if request.route not in capabilities.routes:
        raise CapabilityError(f"transport does not build the {request.route.value} route")
    if request.encoding not in capabilities.encodings:
        raise CapabilityError(f"transport does not support {request.encoding.value} request bodies")
    if request.headers.items and not capabilities.scoped_headers:
        raise CapabilityError("transport does not support scoped request headers")
