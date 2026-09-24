"""Immutable public values shared by execution and evidence layers."""

from __future__ import annotations
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import TypedDict, cast

from b24api._diagnostics import DiagnosticContext
from b24api.contracts.json import FrozenMapping, JsonValue, _freeze_json, _thaw_json
from b24api.contracts.policy import IdentityCoercion
from b24api.contracts.positional import PositionalArguments
from b24api.contracts.request_summary import RequestSummary, RouteKind
from b24api.contracts.wire import BodyEncoding, RequestHeaders
from b24api.redaction import DEFAULT_REDACTOR, Redactor

_METHOD_RE = re.compile(r"^[A-Za-z0-9_.]+$")
type PathPart = str | int
_COMPONENT_LABEL_MAXIMUM = 80
_COMPOSITE_COMPONENT_MINIMUM = 2
_COMPOSITE_COMPONENT_MAXIMUM = 8


def summarize_request(  # noqa: PLR0913
    method: object,
    parameters: object = None,
    *,
    encoding: BodyEncoding = BodyEncoding.JSON,
    route: RouteKind = RouteKind.BARE,
    header_names: tuple[str, ...] = (),
    redactor: Redactor = DEFAULT_REDACTOR,
) -> RequestSummary:
    """Build a safe summary from an explicit method and parameter mapping."""
    safe_method = redactor.redact_text(str(method))
    keys: tuple[str, ...] = ()
    if isinstance(parameters, Mapping):
        keys = tuple(sorted(redactor.redact_text(str(key)) for key in parameters)[: redactor.max_items])
    return RequestSummary(
        method=safe_method,
        parameter_keys=keys,
        encoding=encoding,
        header_names=header_names,
        route=route,
    )


class ReplaySafety(StrEnum):
    """Whether repeating a request is proven safe."""

    SAFE = "safe"
    UNSAFE = "unsafe"
    UNKNOWN = "unknown"


class _OptionalRequestSpec(TypedDict, total=False):
    parameters: Mapping[str, object] | PositionalArguments
    replay_safety: ReplaySafety
    encoding: BodyEncoding
    headers: RequestHeaders
    result_error: ResultErrorSpec


class RequestSpec(_OptionalRequestSpec):
    """Closed mapping form accepted at public request boundaries."""

    method: str
    route: RouteKind


@dataclass(frozen=True, slots=True)
class ParameterPath:
    """Exact case-sensitive path to a wire control."""

    path: tuple[PathPart, ...]

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        object.__setattr__(self, "path", tuple(self.path))
        _validate_path(self.path, allow_empty=False)


@dataclass(frozen=True, slots=True)
class ResultSelector:
    """Exact case-sensitive path to a result value."""

    path: tuple[PathPart, ...] = ()

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        object.__setattr__(self, "path", tuple(self.path))
        _validate_path(self.path, allow_empty=True)

    @classmethod
    def root(cls) -> ResultSelector:
        """Return a selector for the response root."""
        return cls(())


@dataclass(frozen=True, slots=True)
class IdentitySpec:
    """Independent item, filter, and order identity roles."""

    item_path: tuple[PathPart, ...]
    filter_key: str
    order_key: str
    coercion: IdentityCoercion = IdentityCoercion.EXACT_STRING

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        object.__setattr__(self, "item_path", tuple(self.item_path))
        _validate_path(self.item_path, allow_empty=False)
        if not self.filter_key or not self.order_key:
            raise ValueError("identity filter_key and order_key must be non-empty")
        if not isinstance(self.coercion, IdentityCoercion):
            raise TypeError("coercion must be an IdentityCoercion")


class ResultErrorShape(StrEnum):
    """Shape of an endpoint-declared error embedded inside ``result``."""

    MAPPING = "mapping"
    SEQUENCE_ITEMS = "sequence_items"


@dataclass(frozen=True, slots=True)
class ResultErrorSpec:
    """Declarative location of a method-level error inside ``result``."""

    selector: ResultSelector
    code_path: tuple[PathPart, ...]
    description_path: tuple[PathPart, ...] | None = None
    shape: ResultErrorShape = ResultErrorShape.MAPPING

    def __post_init__(self) -> None:
        """Validate and freeze paths."""
        object.__setattr__(self, "code_path", tuple(self.code_path))
        _validate_path(self.code_path, allow_empty=False)
        if self.description_path is not None:
            object.__setattr__(self, "description_path", tuple(self.description_path))
            _validate_path(self.description_path, allow_empty=False)
        if not isinstance(self.selector, ResultSelector) or not isinstance(self.shape, ResultErrorShape):
            raise TypeError("result error fields must use their declared contract types")


@dataclass(frozen=True, slots=True)
class IdentityComponent:
    """One component of a composite traversal identity."""

    item_path: tuple[PathPart, ...]
    coercion: IdentityCoercion = IdentityCoercion.EXACT_STRING
    label: str | None = None

    def __post_init__(self) -> None:
        """Validate and freeze the component."""
        object.__setattr__(self, "item_path", tuple(self.item_path))
        _validate_path(self.item_path, allow_empty=False)
        if not isinstance(self.coercion, IdentityCoercion):
            raise TypeError("coercion must be an IdentityCoercion")
        if self.label is not None and (
            not isinstance(self.label, str) or not self.label or len(self.label) > _COMPONENT_LABEL_MAXIMUM
        ):
            raise ValueError("component label must be a non-empty string of at most 80 characters")


@dataclass(frozen=True, slots=True)
class CompositeIdentitySpec:
    """Unordered tuple identity for sequential and counted traversal."""

    components: tuple[IdentityComponent, ...]

    def __post_init__(self) -> None:
        """Validate component cardinality and uniqueness."""
        object.__setattr__(self, "components", tuple(self.components))
        if not _COMPOSITE_COMPONENT_MINIMUM <= len(self.components) <= _COMPOSITE_COMPONENT_MAXIMUM:
            raise ValueError("composite identity must contain 2..8 components")
        if any(not isinstance(component, IdentityComponent) for component in self.components):
            raise TypeError("components must be IdentityComponent values")
        paths = [component.item_path for component in self.components]
        labels = [component.label for component in self.components if component.label is not None]
        if len(set(paths)) != len(paths) or len(set(labels)) != len(labels):
            raise ValueError("composite identity paths and labels must be unique")


type TraversalIdentity = IdentitySpec | CompositeIdentitySpec


def _validate_path(path: tuple[PathPart, ...], *, allow_empty: bool) -> None:
    if not path and not allow_empty:
        raise ValueError("path must not be empty")
    for part in path:
        if isinstance(part, str) and not part:
            raise ValueError("string path parts must not be empty")
        if isinstance(part, int) and (isinstance(part, bool) or part < 0):
            raise ValueError("integer path parts must be non-negative")
        if not isinstance(part, str | int):
            raise TypeError("path parts must be strings or integers")


@dataclass(frozen=True, slots=True, init=False)
class Request:
    """Deeply immutable canonical request with detached accessors."""

    method: str
    route: RouteKind
    replay_safety: ReplaySafety
    encoding: BodyEncoding
    headers: RequestHeaders
    result_error: ResultErrorSpec | None
    _parameters: FrozenMapping = field(repr=False)
    _positional: PositionalArguments | None = field(repr=False)

    def __init__(  # noqa: C901, PLR0913 - canonical request boundary validates each declared contract
        self,
        method: str,
        parameters: Mapping[str, object] | PositionalArguments | None = None,
        replay_safety: ReplaySafety = ReplaySafety.UNKNOWN,
        *,
        encoding: BodyEncoding = BodyEncoding.JSON,
        headers: RequestHeaders = RequestHeaders(),  # noqa: B008 - immutable value singleton
        result_error: ResultErrorSpec | None = None,
        route: RouteKind,
    ) -> None:
        """Initialize instance state."""
        if not _METHOD_RE.fullmatch(method):
            raise ValueError("method must contain only letters, digits, dots, and underscores")
        if method.endswith(".json"):
            raise ValueError("request methods are logical names and cannot include a .json route suffix")
        if not isinstance(route, RouteKind):
            raise TypeError("route must be a RouteKind")
        if route is RouteKind.API_V3 and encoding is not BodyEncoding.JSON:
            raise ValueError("API_V3 requires JSON body encoding")
        if not isinstance(replay_safety, ReplaySafety):
            raise TypeError("replay_safety must be a ReplaySafety")
        if not isinstance(encoding, BodyEncoding) or not isinstance(headers, RequestHeaders):
            raise TypeError("encoding and headers must use their declared contract types")
        if result_error is not None and not isinstance(result_error, ResultErrorSpec):
            raise TypeError("result_error must be a ResultErrorSpec")
        positional = parameters if isinstance(parameters, PositionalArguments) else None
        if positional is not None and encoding is not BodyEncoding.JSON:
            raise ValueError("positional arguments require JSON body encoding")
        if positional is not None and route is RouteKind.API_V3:
            raise ValueError("PHP positional arguments cannot use the API_V3 route")
        frozen = _freeze_json({} if positional is not None else parameters or {})
        if not isinstance(frozen, FrozenMapping):
            raise TypeError("request parameters must be a mapping")
        object.__setattr__(self, "method", method)
        object.__setattr__(self, "route", route)
        object.__setattr__(self, "replay_safety", replay_safety)
        object.__setattr__(self, "encoding", encoding)
        object.__setattr__(self, "headers", headers)
        object.__setattr__(self, "result_error", result_error)
        object.__setattr__(self, "_parameters", frozen)
        object.__setattr__(self, "_positional", positional)

    @property
    def positional(self) -> PositionalArguments | None:
        """Return immutable positional arguments, if this request uses them."""
        return self._positional

    @property
    def parameters(self) -> Mapping[str, JsonValue]:
        """Return the parameters."""
        return MappingProxyType(self.copy_parameters())

    def copy_parameters(self) -> dict[str, JsonValue]:
        """Return a mutable copy of the immutable request parameters."""
        if self._positional is not None:
            raise ValueError("positional arguments have no named parameter mapping")
        return cast("dict[str, JsonValue]", _thaw_json(self._parameters))

    def to_wire_parameters(self) -> dict[str, JsonValue]:
        """Return the to wire parameters representation."""
        return self.copy_parameters()

    def with_parameters(self, parameters: Mapping[str, object]) -> Request:
        """Replace parameters while preserving every other request contract."""
        if self._positional is not None:
            raise ValueError("with_parameters cannot replace positional arguments")
        return Request(
            self.method,
            parameters,
            self.replay_safety,
            encoding=self.encoding,
            headers=self.headers,
            result_error=self.result_error,
            route=self.route,
        )

    @property
    def summary(self) -> RequestSummary:
        """Return the summary."""
        return summarize_request(
            self.method,
            {f"@{self._positional.layout_id}": None} if self._positional else self._parameters,
            encoding=self.encoding,
            header_names=self.headers.names,
            route=self.route,
        )

    def __repr__(self) -> str:
        """Return a safe representation."""
        return f"Request(summary={self.summary!r}, replay_safety={self.replay_safety!r})"


def diagnostic_context(request: Request) -> DiagnosticContext:
    """Derive the private diagnostic context bound to this one canonical request.

    The context is a pure function of the frozen request, resolved lazily on the error path, so it
    adds no per-request cost and is never stored on the request, a shared context or a report.
    Positional PHP arguments carry no named field positions and get an empty alias map.
    """
    parameters = request.copy_parameters() if request.positional is None else None
    return DiagnosticContext(parameters, headers=request.headers.items)


type RequestLike = Request | RequestSpec


def canonical_request(raw: RequestLike) -> Request:
    """Canonicalize the closed public request mapping at the API boundary."""
    if isinstance(raw, Request):
        return raw
    if not isinstance(raw, Mapping):
        raise TypeError("request must be a Request or closed request mapping")
    unknown = set(raw) - {"method", "parameters", "replay_safety", "encoding", "headers", "result_error", "route"}
    if unknown:
        raise ValueError(f"unknown request fields: {sorted(unknown)}")
    method = raw.get("method")
    parameters = raw.get("parameters")
    safety = raw.get("replay_safety", ReplaySafety.UNKNOWN)
    encoding = raw.get("encoding", BodyEncoding.JSON)
    headers = raw.get("headers", RequestHeaders())
    result_error = raw.get("result_error")
    route = raw.get("route")
    if not isinstance(method, str):
        raise TypeError("request mapping requires a string method")
    if parameters is not None and not isinstance(parameters, Mapping | PositionalArguments):
        raise TypeError("request mapping parameters must be a mapping or PositionalArguments")
    if not isinstance(safety, ReplaySafety):
        raise TypeError("request replay_safety must be a ReplaySafety")
    if not isinstance(encoding, BodyEncoding) or not isinstance(headers, RequestHeaders):
        raise TypeError("request encoding and headers must use their declared contract types")
    if result_error is not None and not isinstance(result_error, ResultErrorSpec):
        raise TypeError("request result_error must be a ResultErrorSpec")
    if not isinstance(route, RouteKind):
        raise TypeError("request route must be a RouteKind")
    return Request(
        method,
        parameters,
        replay_safety=safety,
        encoding=encoding,
        headers=headers,
        result_error=result_error,
        route=route,
    )
