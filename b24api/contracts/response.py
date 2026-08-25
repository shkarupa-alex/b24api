"""Immutable public values shared by execution and evidence layers."""

from __future__ import annotations
import math
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, cast

from b24api.contracts.json import FrozenJson, FrozenMapping, JsonValue, _freeze_json, _is_plain_int, _thaw_json
from b24api.contracts.request import PathPart, ResultSelector
from b24api.redaction import DEFAULT_REDACTOR

if TYPE_CHECKING:
    from collections.abc import Mapping

    from b24api.contracts.request import ParameterPath


class ResultCollectionShape(StrEnum):
    """Closed interpretation of a selected result collection."""

    SEQUENCE = "sequence"
    MAPPING_VALUES = "mapping_values"


@dataclass(frozen=True, slots=True)
class ResponseEvidence:
    """Bounded redacted HTTP evidence safe for default serialization."""

    http_status: int | None = None
    request_id: str | None = None
    headers: tuple[tuple[str, str], ...] = ()
    body_preview: str | None = None

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        redacted_headers = DEFAULT_REDACTOR.redact(dict(self.headers))
        object.__setattr__(
            self,
            "headers",
            tuple(sorted((str(key), str(value)) for key, value in redacted_headers.items()))[:50],
        )
        if self.request_id is not None:
            object.__setattr__(self, "request_id", DEFAULT_REDACTOR.redact_text(self.request_id))
        if self.body_preview is not None:
            object.__setattr__(self, "body_preview", DEFAULT_REDACTOR.redact_text(self.body_preview))

    def to_dict(self) -> dict[str, object]:
        """Return the to dict representation."""
        return {
            "http_status": self.http_status,
            "request_id": self.request_id,
            "headers": dict(self.headers),
            "body_preview": self.body_preview,
        }


@dataclass(frozen=True, slots=True)
class ResponseTime:
    """Immutable server timing values."""

    start: float
    finish: float
    duration: float
    processing: float
    date_start: str
    date_finish: str
    operating_reset_at: float | None = None
    operating: float | None = None

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        for value in (self.start, self.finish, self.duration, self.processing, self.operating_reset_at, self.operating):
            if value is not None and not math.isfinite(value):
                raise ValueError("response time values must be finite")
        if self.duration < 0 or self.processing < 0 or (self.operating is not None and self.operating < 0):
            raise ValueError("response durations must be non-negative")


@dataclass(frozen=True, slots=True, init=False)
class Response:
    """Deeply immutable response with explicit result selection."""

    time: ResponseTime | None
    total: int | None
    next: int | None
    evidence: ResponseEvidence
    _result: FrozenJson = field(repr=False)

    def __init__(
        self,
        result: object,
        *,
        time: ResponseTime | None = None,
        total: int | None = None,
        next: int | None = None,  # noqa: A002
        evidence: ResponseEvidence | None = None,
    ) -> None:
        """Initialize instance state."""
        if total is not None and (not _is_plain_int(total) or total < -1):
            raise ValueError("total must be -1 or non-negative")
        if next is not None and (not _is_plain_int(next) or next < 0):
            raise ValueError("next must be non-negative")
        if evidence is not None and not isinstance(evidence, ResponseEvidence):
            raise TypeError("evidence must be ResponseEvidence")
        object.__setattr__(self, "_result", _freeze_json(result))
        object.__setattr__(self, "time", time)
        object.__setattr__(self, "total", total)
        object.__setattr__(self, "next", next)
        object.__setattr__(self, "evidence", evidence or ResponseEvidence())

    @property
    def result(self) -> JsonValue:
        """Return the result."""
        return _thaw_json(self._result)

    def list_items(self, selector: ResultSelector | None = None) -> list[JsonValue]:
        """Return list items selected from the response result."""
        selector = selector or ResultSelector.root()
        selected: JsonValue = self.result
        for part in selector.path:
            if isinstance(part, str):
                if not isinstance(selected, dict) or part not in selected:
                    raise KeyError(f"result selector path not found: {selector.path!r}")
                selected = selected[part]
                continue
            if not isinstance(selected, list) or part >= len(selected):
                raise KeyError(f"result selector path not found: {selector.path!r}")
            selected = selected[part]
        if not isinstance(selected, list):
            raise TypeError("selected result must be a list")
        return selected

    def __repr__(self) -> str:
        """Return a safe representation."""
        result = self.result
        size = len(result) if isinstance(result, list | dict) else None
        return (
            "Response("
            f"result_type={type(result).__name__!r}, result_size={size!r}, total={self.total!r}, "
            f"next={self.next!r}, evidence={self.evidence!r})"
        )


def inject_controls(
    parameters: Mapping[str, object],
    updates: Mapping[ParameterPath, object],
    *,
    allow_create: bool,
) -> dict[str, JsonValue]:
    """Inject plan controls into a detached tree or reject ambiguity/conflicts."""
    frozen = _freeze_json(parameters)
    if not isinstance(frozen, FrozenMapping):
        raise TypeError("parameters must be a mapping")
    result = cast("dict[str, JsonValue]", _thaw_json(frozen))
    normalized_paths: set[tuple[PathPart, ...]] = set()
    for path, value in updates.items():
        normalized = tuple(part.casefold() if isinstance(part, str) else part for part in path.path)
        if normalized in normalized_paths:
            raise ValueError("two injected controls address the same case-insensitive path")
        normalized_paths.add(normalized)
        _inject_one(result, path.path, _thaw_json(_freeze_json(value)), allow_create=allow_create)
    return result


def _inject_one(  # noqa: C901, PLR0912
    root: dict[str, JsonValue],
    path: tuple[PathPart, ...],
    value: JsonValue,
    *,
    allow_create: bool,
) -> None:
    current: JsonValue = root
    for part in path[:-1]:
        if isinstance(part, str):
            if not isinstance(current, dict):
                raise TypeError("control path traverses a non-mapping value")
            actual = _case_insensitive_key(current, part)
            if actual is None:
                if not allow_create:
                    raise KeyError(f"missing control container: {part}")
                current[part] = {}
                actual = part
            current = current[actual]
        else:
            if not isinstance(current, list) or part >= len(current):
                raise KeyError(f"missing control list index: {part}")
            current = current[part]
    final = path[-1]
    if isinstance(final, str):
        if not isinstance(current, dict):
            raise TypeError("control path terminates in a non-mapping value")
        actual = _case_insensitive_key(current, final)
        if actual is None:
            current[final] = value
        elif current[actual] != value:
            raise ValueError(f"caller control conflicts with injected value at {path!r}")
    else:
        if not isinstance(current, list) or final >= len(current):
            raise KeyError(f"missing control list index: {final}")
        if current[final] != value:
            raise ValueError(f"caller control conflicts with injected value at {path!r}")


def _case_insensitive_key(mapping: Mapping[str, object], requested: str) -> str | None:
    matches = [key for key in mapping if key.casefold() == requested.casefold()]
    if len(matches) > 1:
        raise ValueError(f"ambiguous case-insensitive control key: {requested}")
    return matches[0] if matches else None
