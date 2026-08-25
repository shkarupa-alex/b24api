# ruff: noqa: ANN401
"""Immutable public values shared by execution and evidence layers."""

from __future__ import annotations
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any, cast

from b24api.contracts.json import FrozenMapping, JsonValue, _freeze_json, _thaw_json
from b24api.contracts.policy import IdentityCoercion
from b24api.query import build_query
from b24api.redaction import DEFAULT_REDACTOR, Redactor

_METHOD_RE = re.compile(r"^[A-Za-z0-9_.]+$")
type PathPart = str | int
type RequestSpec = Mapping[str, object]
type RequestLike = Request | RequestSpec


@dataclass(frozen=True, slots=True)
class RequestSummary:
    """Bounded request identity that intentionally excludes parameter values."""

    method: str
    parameter_keys: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        object.__setattr__(self, "method", DEFAULT_REDACTOR.redact_text(self.method))
        object.__setattr__(
            self,
            "parameter_keys",
            tuple(DEFAULT_REDACTOR.redact_text(str(key)) for key in self.parameter_keys[: DEFAULT_REDACTOR.max_items]),
        )

    def to_dict(self) -> dict[str, object]:
        """Return the to dict representation."""
        return {"method": self.method, "parameter_keys": list(self.parameter_keys)}


def summarize_request(
    method: object,
    parameters: object = None,
    *,
    redactor: Redactor = DEFAULT_REDACTOR,
) -> RequestSummary:
    """Build a safe summary from canonical or legacy request-like values."""
    safe_method = redactor.redact_text(str(method))
    keys: tuple[str, ...] = ()
    if isinstance(parameters, Mapping):
        keys = tuple(sorted(redactor.redact_text(str(key)) for key in parameters)[: redactor.max_items])
    return RequestSummary(method=safe_method, parameter_keys=keys)


def summarize_request_like(request: Any, *, redactor: Redactor = DEFAULT_REDACTOR) -> RequestSummary | None:
    """Summarize an optional duck-typed request without importing legacy models."""
    if request is None:
        return None
    return summarize_request(
        getattr(request, "method", type(request).__name__),
        getattr(request, "parameters", None),
        redactor=redactor,
    )


class ReplaySafety(StrEnum):
    """Whether repeating a request is proven safe."""

    SAFE = "safe"
    UNSAFE = "unsafe"
    UNKNOWN = "unknown"


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
    _parameters: FrozenMapping = field(repr=False)

    def __init__(
        self,
        method: str,
        parameters: Mapping[str, object] | None = None,
        replay_safety: ReplaySafety = ReplaySafety.UNKNOWN,
    ) -> None:
        """Initialize instance state."""
        if not _METHOD_RE.fullmatch(method):
            raise ValueError("method must contain only letters, digits, dots, and underscores")
        if not isinstance(replay_safety, ReplaySafety):
            raise TypeError("replay_safety must be a ReplaySafety")
        frozen = _freeze_json(parameters or {})
        if not isinstance(frozen, FrozenMapping):
            raise TypeError("request parameters must be a mapping")
        object.__setattr__(self, "method", method)
        object.__setattr__(self, "replay_safety", replay_safety)
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

    @property
    def query(self) -> str:
        """Return the committed PHP-style method/query representation."""
        parameters = self.to_wire_parameters()
        query = build_query(cast("dict[Any, Any]", parameters))
        return self.method if not query else f"{self.method}?{query}"

    @property
    def summary(self) -> RequestSummary:
        """Return the summary."""
        return summarize_request(self.method, self._parameters)

    def __repr__(self) -> str:
        """Return a safe representation."""
        return f"Request(summary={self.summary!r}, replay_safety={self.replay_safety!r})"


def canonical_request(raw: RequestLike) -> Request:
    """Canonicalize the closed public request mapping at the API boundary."""
    if isinstance(raw, Request):
        return raw
    if not isinstance(raw, Mapping):
        raise TypeError("request must be a Request or closed request mapping")
    unknown = set(raw) - {"method", "parameters", "replay_safety"}
    if unknown:
        raise ValueError(f"unknown request fields: {sorted(unknown)}")
    method = raw.get("method")
    parameters = raw.get("parameters")
    safety = raw.get("replay_safety", ReplaySafety.UNKNOWN)
    if not isinstance(method, str):
        raise TypeError("request mapping requires a string method")
    if parameters is not None and not isinstance(parameters, Mapping):
        raise TypeError("request mapping parameters must be a mapping")
    if isinstance(safety, str):
        try:
            safety = ReplaySafety(safety)
        except ValueError as error:
            raise ValueError("request replay_safety is invalid") from error
    if not isinstance(safety, ReplaySafety):
        raise TypeError("request replay_safety must be a ReplaySafety")
    return Request(method, parameters, replay_safety=safety)
