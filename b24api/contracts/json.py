"""Immutable public values shared by execution and evidence layers."""

from __future__ import annotations
import math
from collections.abc import Iterator, Mapping, Sequence

type JsonScalar = None | bool | int | float | str
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type FrozenJson = JsonScalar | tuple[FrozenJson, ...] | FrozenMapping


class FrozenMapping(Mapping[str, FrozenJson]):
    """Private immutable mapping used for canonical JSON storage."""

    __slots__ = ("_values",)

    def __init__(self, values: Mapping[str, FrozenJson]) -> None:
        """Copy values into canonical immutable storage."""
        self._values = dict(values)

    def __getitem__(self, key: str) -> FrozenJson:
        """Return one frozen value."""
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        """Iterate canonical keys."""
        return iter(self._values)

    def __len__(self) -> int:
        """Return the number of canonical keys."""
        return len(self._values)


def _freeze_json(value: object, *, active: set[int] | None = None) -> FrozenJson:
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
                frozen[key] = _freeze_json(item, active=active)
            return FrozenMapping(frozen)
        finally:
            active.remove(identity)
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray | str):
        identity = id(value)
        if identity in active:
            raise ValueError("cyclic JSON arrays are not supported")
        active.add(identity)
        try:
            return tuple(_freeze_json(item, active=active) for item in value)
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
