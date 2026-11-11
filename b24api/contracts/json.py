"""Immutable public values shared by execution and evidence layers."""

from __future__ import annotations
import math
from collections.abc import Iterator, Mapping, Sequence
from typing import cast

type JsonScalar = None | bool | int | float | str
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type FrozenJson = JsonScalar | tuple[FrozenJson, ...] | FrozenMapping
_MAX_JSON_DEPTH = 256


class FrozenMapping(Mapping[str, FrozenJson]):
    """Immutable mapping used for canonical JSON storage and page views."""

    __slots__ = ("_key", "_values")

    def __init__(self, values: Mapping[str, FrozenJson]) -> None:
        """Copy values into canonical immutable storage."""
        self._values = dict(values)
        self._key: object | None = None

    def __getitem__(self, key: str) -> FrozenJson:
        """Return one frozen value."""
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        """Iterate canonical keys."""
        return iter(self._values)

    def __len__(self) -> int:
        """Return the number of canonical keys."""
        return len(self._values)

    def __eq__(self, other: object) -> bool:
        """Compare canonical mappings structurally."""
        return isinstance(other, FrozenMapping) and self._comparison_key() == other._comparison_key()

    def __hash__(self) -> int:
        """Hash immutable JSON with wire-significant scalar types preserved."""
        return hash(self._comparison_key())

    def _comparison_key(self) -> object:
        """Return the cached type-sensitive structural key."""
        if self._key is None:
            self._key = (
                "object",
                tuple(sorted((key, _frozen_json_key(item)) for key, item in self._values.items())),
            )
        return self._key


def _frozen_json_key(value: FrozenJson) -> object:  # noqa: PLR0911 - closed JSON scalar/container variants
    """Return a recursively type-tagged structural comparison key."""
    if value is None:
        return ("null",)
    if isinstance(value, bool):
        return ("boolean", value)
    if isinstance(value, int):
        return ("integer", value)
    if isinstance(value, float):
        return ("number", value.hex())
    if isinstance(value, str):
        return ("string", value)
    if isinstance(value, tuple):
        return ("array", tuple(_frozen_json_key(item) for item in value))
    return value._comparison_key()  # noqa: SLF001 - closed recursive value implementation


def _plain_scalar(value: JsonScalar) -> JsonScalar:
    """Return a JSON scalar as its exact built-in type.

    A subclass, such as a ``(str, Enum)`` member, is stored as its plain value, so every encoding sends
    the same JSON: a PHP query would otherwise render it through its own ``__str__``.
    """
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("JSON numbers must be finite")
        return value
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value if type(value) is int else int.__int__(value)
    if isinstance(value, str):
        return value if type(value) is str else str.__str__(value)
    if not math.isfinite(value):
        raise ValueError("JSON numbers must be finite")
    return value if type(value) is float else float.__float__(value)


def _freeze_json(
    value: object,
    *,
    active: set[int] | None = None,
    depth: int = 0,
) -> FrozenJson:
    if depth > _MAX_JSON_DEPTH:
        raise ValueError("JSON nesting exceeds the supported depth")
    active = active if active is not None else set()
    kind = type(value)
    if value is None or kind is str or kind is int or kind is bool:
        return cast("JsonScalar", value)
    if isinstance(value, bool | int | str | float):
        return _plain_scalar(value)
    if isinstance(value, Mapping):
        identity = id(value)
        if identity in active:
            raise ValueError("cyclic JSON mappings are not supported")
        active.add(identity)
        try:
            frozen: dict[str, FrozenJson] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError("JSON object keys must be strings")
                frozen[key if type(key) is str else str.__str__(key)] = _freeze_json(
                    item, active=active, depth=depth + 1
                )
            return FrozenMapping(frozen)
        finally:
            active.remove(identity)
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray | str):
        identity = id(value)
        if identity in active:
            raise ValueError("cyclic JSON arrays are not supported")
        active.add(identity)
        try:
            return tuple(_freeze_json(item, active=active, depth=depth + 1) for item in value)
        finally:
            active.remove(identity)
    raise TypeError(f"unsupported JSON value type: {type(value).__name__}")


def _has_empty_container(value: FrozenJson) -> bool:
    """Return whether a frozen value holds an empty array or object below its own top level."""
    pending: list[FrozenJson] = list(value.values()) if isinstance(value, FrozenMapping) else [value]
    while pending:
        item = pending.pop()
        if isinstance(item, FrozenMapping | tuple):
            if not item:
                return True
            pending.extend(item.values() if isinstance(item, FrozenMapping) else item)
    return False


def thaw_json(value: FrozenJson) -> JsonValue:
    """Return a plain mutable JSON copy of a frozen value: dicts for mappings, lists for arrays."""
    if isinstance(value, FrozenMapping):
        return {key: thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_json(item) for item in value]
    return value


def _is_plain_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _json_type_name(value: object) -> str:  # noqa: PLR0911
    """Return a value-free JSON type name for diagnostics."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, str):
        return "string"
    if _is_plain_int(value):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, Mapping):
        return "object"
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray | str):
        return "array"
    return type(value).__name__
