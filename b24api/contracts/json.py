"""Immutable public values shared by execution and evidence layers."""

from __future__ import annotations
import math
from collections.abc import Iterator, Mapping, Sequence
from typing import cast

type JsonScalar = None | bool | int | float | str
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type FrozenJson = JsonScalar | tuple[FrozenJson, ...] | FrozenMapping
MAX_JSON_DEPTH = 256


class DecodedJsonObject(dict[str, JsonValue]):
    """A decoded JSON object whose member names repeat: the last member wins, and the repeated names are kept."""

    __slots__ = ("repeated_names",)

    def __init__(self, pairs: list[tuple[str, JsonValue]]) -> None:
        """Keep the last value of each name, and each name seen again."""
        super().__init__(pairs)
        seen: set[str] = set()
        repeated: list[str] = []
        for name, _ in pairs:
            if name in seen:
                repeated.append(name)
            seen.add(name)
        self.repeated_names = tuple(repeated)

    @classmethod
    def holding(cls, values: dict[str, JsonValue], repeated: tuple[str, ...]) -> DecodedJsonObject:
        """Return an object of ``values`` that records ``repeated``, as a thawed copy of a frozen one does."""
        decoded = cls([])
        decoded.update(values)
        decoded.repeated_names = repeated
        return decoded


def repeated_names(value: object) -> tuple[str, ...]:
    """Return the member names a decoded JSON object repeated, decoded or frozen; empty for any other value."""
    return value.repeated_names if isinstance(value, DecodedJsonObject | FrozenMapping) else ()


def nests_deeper_than_supported(body: object) -> bool:
    """Return whether a decoded body holds a value more than ``MAX_JSON_DEPTH`` levels below a top-level member.

    A response keeps each top-level member (its ``result`` above all) as a frozen JSON value of its own, which holds
    ``MAX_JSON_DEPTH`` levels below itself, so a member is measured from depth 0.
    """
    pending: list[tuple[object, int]] = [(body, -1)]
    while pending:
        item, depth = pending.pop()
        children = item.values() if isinstance(item, dict) else item if isinstance(item, list) else ()
        for child in children:
            if depth + 1 > MAX_JSON_DEPTH:
                return True
            if isinstance(child, dict | list):
                pending.append((child, depth + 1))
    return False


class FrozenMapping(Mapping[str, FrozenJson]):
    """Immutable mapping used for canonical JSON storage and page views."""

    __slots__ = ("_key", "_repeated", "_values")

    def __init__(self, values: Mapping[str, FrozenJson], *, repeated: tuple[str, ...] = ()) -> None:
        """Copy values into canonical immutable storage, with the member names a decoded object repeated."""
        self._values = dict(values)
        self._key: object | None = None
        self._repeated = repeated

    @property
    def repeated_names(self) -> tuple[str, ...]:
        """Member names the decoded JSON object this was frozen from repeated (each kept its last value).

        Empty for a mapping built in code. It takes no part in equality: it records how the answer was written.
        """
        return self._repeated

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
    if depth > MAX_JSON_DEPTH:
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
            return FrozenMapping(frozen, repeated=repeated_names(value))
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
        thawed: dict[str, JsonValue] = {key: thaw_json(item) for key, item in value.items()}
        # A repeated member name stays visible to whoever reads the answer's control members from the copy.
        return DecodedJsonObject.holding(thawed, value.repeated_names) if value.repeated_names else thawed
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
