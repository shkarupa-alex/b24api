"""Immutable public values shared by execution and evidence layers."""

from __future__ import annotations
import math
import sys
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any

type JsonScalar = None | bool | int | float | str
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type FrozenJson = JsonScalar | tuple[FrozenJson, ...] | FrozenMapping
_MAX_JSON_DEPTH = 256
_PLAIN_SCALARS = frozenset({type(None), str, int, bool})


class FrozenMapping(Mapping[str, FrozenJson]):
    """Immutable mapping used for canonical JSON storage and page views."""

    __slots__ = ("_key", "_values")

    def __init__(self, values: Mapping[str, object]) -> None:
        """Freeze a JSON object deeply: nested mappings and sequences become frozen values too."""
        if not isinstance(values, Mapping):
            raise TypeError("FrozenMapping requires a mapping")
        # Members ``_freeze_json`` has just frozen are kept as they are rather than walked again.
        self._values: dict[str, FrozenJson] = (
            values if type(values) is _FrozenMembers else _frozen_members(values, wire=False, active=set(), depth=0)
        )
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


def _wire_scalar(value: JsonScalar) -> JsonScalar:
    """Return a scalar that JSON and bracket encoding can both write, or raise ``ValueError``.

    A string must encode as UTF-8 (a lone surrogate, as ``os.fsdecode`` returns for a non-UTF-8 file name,
    does not), and an integer must have a decimal form within the interpreter's digit limit.
    """
    if isinstance(value, str):
        if not value.isascii():
            try:
                value.encode("utf-8")
            except UnicodeEncodeError:
                raise ValueError("JSON strings must be valid Unicode without lone surrogates") from None
    elif isinstance(value, int) and not isinstance(value, bool):
        limit = sys.get_int_max_str_digits()
        # A decimal digit takes more than three bits, so only a longer integer can exceed the limit.
        if limit and value.bit_length() > 3 * limit:
            try:
                int.__repr__(value)
            except ValueError:
                raise ValueError("JSON integers must have at most the interpreter's decimal digit limit") from None
    return value


def _freeze_json(
    value: object,
    *,
    wire: bool = False,
    active: set[int] | None = None,
    depth: int = 0,
) -> FrozenJson:
    """Freeze a JSON value; ``wire`` also refuses a scalar no request encoding can write."""
    if depth > _MAX_JSON_DEPTH:
        raise ValueError("JSON nesting exceeds the supported depth")
    active = active if active is not None else set()
    if value is None or isinstance(value, bool | int | str | float):
        plain = value if type(value) in _PLAIN_SCALARS else _plain_scalar(value)
        return _wire_scalar(plain) if wire else plain
    if isinstance(value, Mapping):
        return FrozenMapping(_frozen_members(value, wire=wire, active=active, depth=depth))
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray | str):
        identity = id(value)
        if identity in active:
            raise ValueError("cyclic JSON arrays are not supported")
        active.add(identity)
        try:
            return tuple(_freeze_json(item, wire=wire, active=active, depth=depth + 1) for item in value)
        finally:
            active.remove(identity)
    raise TypeError(f"unsupported JSON value type: {type(value).__name__}")


class _FrozenMembers(dict[str, FrozenJson]):
    """The members of one JSON object, every key a plain string and every value already frozen."""

    __slots__ = ()


def _frozen_members(value: Mapping[Any, object], *, wire: bool, active: set[int], depth: int) -> _FrozenMembers:
    """Freeze the members of one JSON object, its keys as plain strings."""
    identity = id(value)
    if identity in active:
        raise ValueError("cyclic JSON mappings are not supported")
    active.add(identity)
    try:
        frozen = _FrozenMembers()
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("JSON object keys must be strings")
            plain_key = key if type(key) is str else str.__str__(key)
            if wire:
                _wire_scalar(plain_key)
            frozen[plain_key] = _freeze_json(item, wire=wire, active=active, depth=depth + 1)
        return frozen
    finally:
        active.remove(identity)


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


def _string_free_tuple[T](values: Iterable[T], *, label: str) -> tuple[T, ...]:
    """Return ``values`` as a tuple, refusing a bare string, which would otherwise split into characters."""
    if isinstance(values, str | bytes | bytearray):
        raise TypeError(f"{label} must be a sequence of parts, not a single string")
    return tuple(values)


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
