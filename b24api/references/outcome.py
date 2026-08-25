"""Immutable public values shared by execution and evidence layers."""

from __future__ import annotations
from dataclasses import dataclass, field

from b24api.contracts.json import FrozenJson, JsonValue, _freeze_json, _is_plain_int, _thaw_json
from b24api.contracts.policy import ReplayDisposition
from b24api.contracts.request import ReplaySafety, Request

STABLE_KEY_MAXIMUM = 100


@dataclass(frozen=True, slots=True)
class ReferenceRequest:
    """Immutable request correlated to a reference key."""

    request: Request = field(repr=False)
    reference_key: str = field(repr=False)
    correlation: object = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        if not isinstance(self.request, Request):
            raise TypeError("reference request must contain a canonical Request")
        if not self.reference_key or len(self.reference_key) > STABLE_KEY_MAXIMUM:
            raise ValueError("reference_key must be 1..100 characters")


@dataclass(frozen=True, slots=True, init=False)
class ReferenceItem:
    """Successful reference item; raw item and correlation are hidden from repr."""

    reference_key: str = field(repr=False)
    _item: FrozenJson = field(repr=False)
    correlation: object = field(default=None, repr=False)

    def __init__(self, reference_key: str, item: object, correlation: object = None) -> None:
        """Initialize instance state."""
        if not reference_key or len(reference_key) > STABLE_KEY_MAXIMUM:
            raise ValueError("reference_key must be 1..100 characters")
        object.__setattr__(self, "reference_key", reference_key)
        object.__setattr__(self, "_item", _freeze_json(item))
        object.__setattr__(self, "correlation", correlation)

    @property
    def item(self) -> JsonValue:
        """Return the item."""
        return _thaw_json(self._item)


@dataclass(frozen=True, slots=True, init=False)
class ReferenceFailure:
    """Failed reference state with raw correlation excluded from repr."""

    reference_key: str = field(repr=False)
    request: Request = field(repr=False)
    error: object = field(repr=False)
    _cursor: FrozenJson = field(repr=False)
    page_state: int = 0
    partial_rows: int = 0
    replay_safety: ReplaySafety = ReplaySafety.UNKNOWN
    replay_disposition: ReplayDisposition = ReplayDisposition.NOT_ELIGIBLE
    correlation: object = field(default=None, repr=False)

    def __init__(  # noqa: PLR0913
        self,
        reference_key: str,
        request: Request,
        error: object,
        cursor: object = None,
        page_state: int = 0,
        partial_rows: int = 0,
        replay_safety: ReplaySafety = ReplaySafety.UNKNOWN,
        replay_disposition: ReplayDisposition = ReplayDisposition.NOT_ELIGIBLE,
        correlation: object = None,
    ) -> None:
        """Initialize instance state."""
        if not reference_key or len(reference_key) > STABLE_KEY_MAXIMUM:
            raise ValueError("reference_key must be 1..100 characters")
        if not isinstance(request, Request):
            raise TypeError("reference failure request must be canonical Request")
        if not _is_plain_int(page_state) or page_state < 0:
            raise ValueError("page_state cannot be negative")
        if not _is_plain_int(partial_rows) or partial_rows < 0:
            raise ValueError("partial_rows cannot be negative")
        if not isinstance(replay_safety, ReplaySafety):
            raise TypeError("replay_safety must be ReplaySafety")
        if not isinstance(replay_disposition, ReplayDisposition):
            raise TypeError("replay_disposition must be ReplayDisposition")
        object.__setattr__(self, "reference_key", reference_key)
        object.__setattr__(self, "request", request)
        object.__setattr__(self, "error", error)
        object.__setattr__(self, "_cursor", _freeze_json(cursor))
        object.__setattr__(self, "page_state", page_state)
        object.__setattr__(self, "partial_rows", partial_rows)
        object.__setattr__(self, "replay_safety", replay_safety)
        object.__setattr__(self, "replay_disposition", replay_disposition)
        object.__setattr__(self, "correlation", correlation)

    @property
    def cursor(self) -> JsonValue:
        """Return the cursor."""
        return _thaw_json(self._cursor)


type ReferenceOutcome = ReferenceItem | ReferenceFailure
