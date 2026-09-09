"""Immutable public values shared by execution and evidence layers."""

from __future__ import annotations
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import TypedDict, cast

from b24api.contracts.json import FrozenMapping, JsonValue, _freeze_json, _thaw_json
from b24api.contracts.policy import IdentityCoercion
from b24api.contracts.wire import BodyEncoding, RequestHeaders
from b24api.redaction import DEFAULT_REDACTOR, Redactor

_METHOD_RE = re.compile(r"^[A-Za-z0-9_.]+$")
type PathPart = str | int
_COMPONENT_LABEL_MAXIMUM = 80
_COMPOSITE_COMPONENT_MINIMUM = 2
_COMPOSITE_COMPONENT_MAXIMUM = 8


@dataclass(frozen=True, slots=True)
class RequestSummary:
    """Bounded request identity that intentionally excludes parameter values."""

    method: str
    parameter_keys: tuple[str, ...] = ()
    encoding: BodyEncoding = BodyEncoding.JSON
    header_names: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        object.__setattr__(self, "method", DEFAULT_REDACTOR.redact_text(self.method))
        object.__setattr__(
            self,
            "parameter_keys",
            tuple(DEFAULT_REDACTOR.redact_text(str(key)) for key in self.parameter_keys[: DEFAULT_REDACTOR.max_items]),
        )
        if not isinstance(self.encoding, BodyEncoding):
            raise TypeError("encoding must be a BodyEncoding")
        object.__setattr__(
            self,
            "header_names",
            tuple(
                sorted(
                    DEFAULT_REDACTOR.redact_text(str(name).casefold())
                    for name in self.header_names[: DEFAULT_REDACTOR.max_items]
                ),
            ),
        )

    def to_dict(self) -> dict[str, object]:
        """Return the to dict representation."""
        return {
            "method": self.method,
            "parameter_keys": list(self.parameter_keys),
            "encoding": self.encoding.value,
            "header_names": list(self.header_names),
        }


def summarize_request(
    method: object,
    parameters: object = None,
    *,
    encoding: BodyEncoding = BodyEncoding.JSON,
    header_names: tuple[str, ...] = (),
    redactor: Redactor = DEFAULT_REDACTOR,
) -> RequestSummary:
    """Build a safe summary from an explicit method and parameter mapping."""
    safe_method = redactor.redact_text(str(method))
    keys: tuple[str, ...] = ()
    if isinstance(parameters, Mapping):
        keys = tuple(sorted(redactor.redact_text(str(key)) for key in parameters)[: redactor.max_items])
    return RequestSummary(method=safe_method, parameter_keys=keys, encoding=encoding, header_names=header_names)


class ReplaySafety(StrEnum):
    """Whether repeating a request is proven safe."""

    SAFE = "safe"
    UNSAFE = "unsafe"
    UNKNOWN = "unknown"


class _OptionalRequestSpec(TypedDict, total=False):
    parameters: Mapping[str, object]
    replay_safety: ReplaySafety
    encoding: BodyEncoding
    headers: RequestHeaders
    result_error: ResultErrorSpec


class RequestSpec(_OptionalRequestSpec):
    """Closed mapping form accepted at public request boundaries."""

    method: str


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
    replay_safety: ReplaySafety
    encoding: BodyEncoding
    headers: RequestHeaders
    result_error: ResultErrorSpec | None
    _parameters: FrozenMapping = field(repr=False)

    def __init__(  # noqa: PLR0913
        self,
        method: str,
        parameters: Mapping[str, object] | None = None,
        replay_safety: ReplaySafety = ReplaySafety.UNKNOWN,
        *,
        encoding: BodyEncoding = BodyEncoding.JSON,
        headers: RequestHeaders = RequestHeaders(),  # noqa: B008 - immutable value singleton
        result_error: ResultErrorSpec | None = None,
    ) -> None:
        """Initialize instance state."""
        if not _METHOD_RE.fullmatch(method):
            raise ValueError("method must contain only letters, digits, dots, and underscores")
        if not isinstance(replay_safety, ReplaySafety):
            raise TypeError("replay_safety must be a ReplaySafety")
        if not isinstance(encoding, BodyEncoding) or not isinstance(headers, RequestHeaders):
            raise TypeError("encoding and headers must use their declared contract types")
        if result_error is not None and not isinstance(result_error, ResultErrorSpec):
            raise TypeError("result_error must be a ResultErrorSpec")
        frozen = _freeze_json(parameters or {})
        if not isinstance(frozen, FrozenMapping):
            raise TypeError("request parameters must be a mapping")
        object.__setattr__(self, "method", method)
        object.__setattr__(self, "replay_safety", replay_safety)
        object.__setattr__(self, "encoding", encoding)
        object.__setattr__(self, "headers", headers)
        object.__setattr__(self, "result_error", result_error)
        object.__setattr__(self, "_parameters", frozen)

    @property
    def parameters(self) -> Mapping[str, JsonValue]:
        """Return the parameters."""
        return MappingProxyType(self.copy_parameters())

    def copy_parameters(self) -> dict[str, JsonValue]:
        """Return a mutable copy of the immutable request parameters."""
        return cast("dict[str, JsonValue]", _thaw_json(self._parameters))

    def to_wire_parameters(self) -> dict[str, JsonValue]:
        """Return the to wire parameters representation."""
        return self.copy_parameters()

    def with_parameters(self, parameters: Mapping[str, object]) -> Request:
        """Replace parameters while preserving every other request contract."""
        return Request(
            self.method,
            parameters,
            self.replay_safety,
            encoding=self.encoding,
            headers=self.headers,
            result_error=self.result_error,
        )

    @property
    def summary(self) -> RequestSummary:
        """Return the summary."""
        return summarize_request(
            self.method,
            self._parameters,
            encoding=self.encoding,
            header_names=self.headers.names,
        )

    def __repr__(self) -> str:
        """Return a safe representation."""
        return f"Request(summary={self.summary!r}, replay_safety={self.replay_safety!r})"


type RequestLike = Request | RequestSpec


def canonical_request(raw: RequestLike) -> Request:
    """Canonicalize the closed public request mapping at the API boundary."""
    if isinstance(raw, Request):
        return raw
    if not isinstance(raw, Mapping):
        raise TypeError("request must be a Request or closed request mapping")
    unknown = set(raw) - {"method", "parameters", "replay_safety", "encoding", "headers", "result_error"}
    if unknown:
        raise ValueError(f"unknown request fields: {sorted(unknown)}")
    method = raw.get("method")
    parameters = raw.get("parameters")
    safety = raw.get("replay_safety", ReplaySafety.UNKNOWN)
    encoding = raw.get("encoding", BodyEncoding.JSON)
    headers = raw.get("headers", RequestHeaders())
    result_error = raw.get("result_error")
    if not isinstance(method, str):
        raise TypeError("request mapping requires a string method")
    if parameters is not None and not isinstance(parameters, Mapping):
        raise TypeError("request mapping parameters must be a mapping")
    if not isinstance(safety, ReplaySafety):
        raise TypeError("request replay_safety must be a ReplaySafety")
    if not isinstance(encoding, BodyEncoding) or not isinstance(headers, RequestHeaders):
        raise TypeError("request encoding and headers must use their declared contract types")
    if result_error is not None and not isinstance(result_error, ResultErrorSpec):
        raise TypeError("request result_error must be a ResultErrorSpec")
    return Request(
        method,
        parameters,
        replay_safety=safety,
        encoding=encoding,
        headers=headers,
        result_error=result_error,
    )
