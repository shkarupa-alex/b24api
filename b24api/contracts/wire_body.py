"""Internal immutable body of a native REST v3 batch envelope; not part of the public contract."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from b24api.contracts.dispatch import PORTAL_BATCH_CAP
from b24api.contracts.json import FrozenMapping, thaw_json
from b24api.contracts.request import ReplaySafety

if TYPE_CHECKING:
    from collections.abc import Iterable

    from b24api.contracts.json import JsonValue

V3_BATCH_METHOD = "batch"


@dataclass(frozen=True, slots=True)
class V3BatchCommand:
    """One wire-visible command of a v3 batch: the target method and its canonical query object."""

    method: str
    query: FrozenMapping = field(repr=False)

    def __post_init__(self) -> None:
        """Require a method name and an already canonical query."""
        if not isinstance(self.method, str) or not self.method:
            raise ValueError("v3 batch command method must be a non-empty string")
        if not isinstance(self.query, FrozenMapping):
            raise TypeError("v3 batch command query must be a canonical FrozenMapping")

    def to_wire(self) -> dict[str, JsonValue]:
        """Return the detached ``{"method": ..., "query": ...}`` array element."""
        return {"method": self.method, "query": thaw_json(self.query)}


@dataclass(frozen=True, slots=True)
class V3BatchBody:
    """Ordered commands of one v3 batch envelope; the server answers in this input order."""

    commands: tuple[V3BatchCommand, ...] = field(repr=False)

    def __post_init__(self) -> None:
        """Bound the envelope by the portal command cap."""
        object.__setattr__(self, "commands", tuple(self.commands))
        if not 1 <= len(self.commands) <= PORTAL_BATCH_CAP:
            raise ValueError(f"v3 batch body requires 1..{PORTAL_BATCH_CAP} commands")
        if any(not isinstance(command, V3BatchCommand) for command in self.commands):
            raise TypeError("v3 batch body must contain V3BatchCommand values")

    def to_wire(self) -> list[JsonValue]:
        """Return the detached top-level JSON array; no aliases and no ``$ref`` substitutions."""
        return [command.to_wire() for command in self.commands]

    def __repr__(self) -> str:
        """Return a value-free representation."""
        return f"V3BatchBody(commands={len(self.commands)})"


def envelope_safety(safeties: Iterable[ReplaySafety]) -> ReplaySafety:
    """Return the replay safety of one physical envelope from the safeties of the commands it carries.

    An all-SAFE envelope is SAFE; any UNSAFE command makes it UNSAFE; otherwise it is UNKNOWN.
    """
    values = frozenset(safeties)
    if values == {ReplaySafety.SAFE}:
        return ReplaySafety.SAFE
    if ReplaySafety.UNSAFE in values:
        return ReplaySafety.UNSAFE
    return ReplaySafety.UNKNOWN


__all__: list[str] = []
