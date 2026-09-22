"""Exact top-level positional JSON arguments for qualified PHP method layouts."""

from __future__ import annotations
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import cast

from b24api.contracts.json import FrozenJson, JsonValue, _freeze_json, _thaw_json

type SlotPathPart = str | int
_LAYOUT_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")


class SlotShape(StrEnum):
    """Top-level JSON shape admitted by a PHP positional slot."""

    ANY = "any"
    SCALAR = "scalar"
    OBJECT = "object"
    ARRAY = "array"


@dataclass(frozen=True, slots=True)
class SlotContract:
    """Name, shape, and ownership of one exact PHP argument position."""

    name: str
    shape: SlotShape
    fixed: bool = False
    nullable: bool = False

    def __post_init__(self) -> None:
        """Reject ambiguous or mistyped slot declarations."""
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("slot name must be a non-empty string")
        if not isinstance(self.shape, SlotShape):
            raise TypeError("slot shape must be a SlotShape")


@dataclass(frozen=True, slots=True)
class PositionalLayout:
    """Versioned method-profile contract for exact positional controls."""

    layout_id: str
    slots: tuple[SlotContract, ...]
    control_paths: frozenset[tuple[SlotPathPart, ...]] = frozenset()

    def __post_init__(self) -> None:
        """Validate exact arity and case-sensitive writable paths."""
        if not isinstance(self.layout_id, str) or not _LAYOUT_ID_RE.fullmatch(self.layout_id):
            raise ValueError("layout_id must be a bounded safe identifier")
        object.__setattr__(self, "slots", tuple(self.slots))
        object.__setattr__(self, "control_paths", frozenset(tuple(path) for path in self.control_paths))
        if not self.slots or any(not isinstance(slot, SlotContract) for slot in self.slots):
            raise ValueError("layout must declare at least one typed slot")
        if len({slot.name for slot in self.slots}) != len(self.slots):
            raise ValueError("slot names must be unique")
        for path in self.control_paths:
            if not path or type(path[0]) is not int or not 0 <= path[0] < len(self.slots):
                raise ValueError("control path must begin with a valid slot index")
            if self.slots[path[0]].fixed:
                raise ValueError("fixed slots cannot own writable controls")
            if any(type(part) not in {str, int} or part == "" or (type(part) is int and part < 0) for part in path[1:]):
                raise ValueError("control path has an invalid segment")


@dataclass(frozen=True, slots=True, init=False)
class Present:
    """A frozen JSON value occupying exactly one PHP argument slot."""

    _value: FrozenJson = field(repr=False)

    def __init__(self, value: object) -> None:
        """Freeze the argument at the public boundary."""
        object.__setattr__(self, "_value", _freeze_json(value))

    @property
    def value(self) -> JsonValue:
        """Return a detached JSON value."""
        return _thaw_json(self._value)

    def __repr__(self) -> str:
        """Exclude caller data from diagnostics."""
        return "Present(<value>)"


@dataclass(frozen=True, slots=True)
class EmptyObject:
    """An explicit empty JSON object placeholder."""


@dataclass(frozen=True, slots=True)
class EmptyArray:
    """An explicit empty JSON array placeholder."""


@dataclass(frozen=True, slots=True)
class Null:
    """An explicit JSON null placeholder."""


@dataclass(frozen=True, slots=True)
class Omitted:
    """An unused trailing position omitted from the wire array."""


type Slot = Present | EmptyObject | EmptyArray | Null | Omitted


@dataclass(frozen=True, slots=True, init=False)
class PositionalArguments:
    """Exact immutable slots plus their versioned method-profile layout."""

    slots: tuple[Slot, ...] = field(repr=False)
    layout_id: str
    layout: PositionalLayout = field(repr=False)

    def __init__(self, slots: tuple[Slot, ...], layout_id: str, *, layout: PositionalLayout) -> None:
        """Validate arity, placeholders, and slot shape without wire coercion."""
        if not isinstance(layout, PositionalLayout) or layout_id != layout.layout_id:
            raise ValueError("layout_id must identify the supplied positional layout")
        canonical = tuple(slots)
        if len(canonical) != len(layout.slots):
            raise ValueError("positional slot count differs from layout arity")
        omitted = False
        for slot, contract in zip(canonical, layout.slots, strict=True):
            if not isinstance(slot, Present | EmptyObject | EmptyArray | Null | Omitted):
                raise TypeError("positional slots must use explicit Slot values")
            if isinstance(slot, Omitted):
                if contract.fixed:
                    raise ValueError("a fixed positional slot cannot be omitted")
                omitted = True
                continue
            if omitted:
                raise ValueError("Omitted is allowed only in a trailing suffix")
            _validate_slot_shape(slot, contract)
        object.__setattr__(self, "slots", canonical)
        object.__setattr__(self, "layout_id", layout_id)
        object.__setattr__(self, "layout", layout)

    def to_wire_slots(self) -> list[JsonValue]:
        """Return detached top-level JSON array with every internal placeholder."""
        values: list[JsonValue] = []
        for slot in self.slots:
            if isinstance(slot, Omitted):
                break
            if isinstance(slot, Present):
                values.append(slot.value)
            elif isinstance(slot, EmptyObject):
                values.append({})
            elif isinstance(slot, EmptyArray):
                values.append([])
            else:
                values.append(None)
        return values

    def write_control(self, path: tuple[SlotPathPart, ...], value: object) -> PositionalArguments:
        """Return a new value when its declared parent path already exists."""
        path = tuple(path)
        if path not in self.layout.control_paths:
            raise ValueError("control path is not declared by this positional layout")
        slot_index = cast("int", path[0])
        if len(path) == 1:
            replacement = Present(value)
        else:
            original = self.slots[slot_index]
            if not isinstance(original, Present):
                raise ValueError("nested control requires a present slot")
            root = original.value
            parent: JsonValue = root
            for part in path[1:-1]:
                parent = _existing_child(parent, part)
            last = path[-1]
            _set_existing_child(parent, last, _thaw_json(_freeze_json(value)))
            replacement = Present(root)
        revised = list(self.slots)
        revised[slot_index] = replacement
        return PositionalArguments(tuple(revised), self.layout_id, layout=self.layout)

    def __repr__(self) -> str:
        """Exclude all argument values from diagnostics."""
        return f"PositionalArguments(layout_id={self.layout_id!r}, slot_count={len(self.slots)})"


def _validate_slot_shape(slot: Slot, contract: SlotContract) -> None:
    if isinstance(slot, Null):
        if contract.nullable or contract.shape is SlotShape.ANY:
            return
        raise ValueError("null is not allowed in this positional slot")
    value = slot.value if isinstance(slot, Present) else {} if isinstance(slot, EmptyObject) else []
    if value is None and not (contract.nullable or contract.shape is SlotShape.ANY):
        raise ValueError("null is not allowed in this positional slot")
    shape = contract.shape
    if shape is SlotShape.OBJECT and not isinstance(value, Mapping):
        raise ValueError("positional slot requires an object")
    if shape is SlotShape.ARRAY and not isinstance(value, list):
        raise ValueError("positional slot requires an array")
    if shape is SlotShape.SCALAR and isinstance(value, Mapping | list):
        raise ValueError("positional slot requires a scalar")


def _existing_child(parent: JsonValue, part: SlotPathPart) -> JsonValue:
    if type(part) is str and isinstance(parent, dict) and part in parent:
        return parent[part]
    if type(part) is int and isinstance(parent, list) and 0 <= part < len(parent):
        return parent[part]
    raise ValueError("positional control path does not exist")


def _set_existing_child(parent: JsonValue, part: SlotPathPart, value: JsonValue) -> None:
    if type(part) is str and isinstance(parent, dict):
        parent[part] = value
        return
    if type(part) is int and isinstance(parent, list) and 0 <= part < len(parent):
        parent[part] = value
        return
    raise ValueError("positional control path does not exist")
