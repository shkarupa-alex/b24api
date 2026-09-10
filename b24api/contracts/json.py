"""Immutable public values shared by execution and evidence layers."""

from __future__ import annotations
import math
from collections.abc import Iterator, Mapping, Sequence

type JsonScalar = None | bool | int | float | str
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type FrozenJson = JsonScalar | tuple[FrozenJson, ...] | FrozenMapping
_MAX_JSON_DEPTH = 256


class FrozenMapping(Mapping[str, FrozenJson]):
    """Private immutable mapping used for canonical JSON storage."""

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


def _freeze_json(  # noqa: C901 - closed JSON scalar/container variants
    value: object,
    *,
    active: set[int] | None = None,
    depth: int = 0,
) -> FrozenJson:
    if depth > _MAX_JSON_DEPTH:
        raise ValueError("JSON nesting exceeds the supported depth")
    active = active if active is not None else set()
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON numbers must be finite")
        return value
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
                frozen[key] = _freeze_json(item, active=active, depth=depth + 1)
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


def _thaw_json(value: FrozenJson) -> JsonValue:
    if isinstance(value, FrozenMapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
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
