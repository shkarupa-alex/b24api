"""Immutable public values shared by execution and evidence layers."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Self

from b24api.contracts.json import FrozenJson, JsonValue, _freeze_json, _is_plain_int, _thaw_json
from b24api.contracts.policy import ReplayDisposition
from b24api.contracts.request import ReplaySafety, Request
from b24api.contracts.response import Response

STABLE_KEY_MAXIMUM = 100


@dataclass(frozen=True, slots=True)
class BatchCommandEvidence:
    """Safe correlation facts for one batch command."""

    command_index: int
    stable_key: str
    original_code: str | int | None = None
    normalized_code: str | None = None

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        if not _is_plain_int(self.command_index) or self.command_index < 0:
            raise ValueError("command_index cannot be negative")
        if not self.stable_key or len(self.stable_key) > STABLE_KEY_MAXIMUM:
            raise ValueError("stable_key must be 1..100 characters")

    def to_dict(self) -> dict[str, object]:
        """Return the to dict representation."""
        return {
            "command_index": self.command_index,
            "stable_key": self.stable_key,
            "original_code": self.original_code,
            "normalized_code": self.normalized_code,
        }


@dataclass(frozen=True, slots=True, init=False)
class BatchSuccess:
    """One successful command with raw values excluded from repr."""

    command_index: int
    stable_key: str
    request: Request = field(repr=False)
    _result: FrozenJson = field(repr=False)
    _decoded_rows: int = field(repr=False)
    payload: object = field(default=None, repr=False)
    evidence: BatchCommandEvidence | None = None
    replay_disposition: ReplayDisposition | None = None
    response: Response | None = field(default=None, repr=False)

    def __init__(  # noqa: PLR0913
        self,
        command_index: int,
        stable_key: str,
        request: Request,
        result: object,
        payload: object = None,
        evidence: BatchCommandEvidence | None = None,
        replay_disposition: ReplayDisposition | None = None,
        response: Response | None = None,
    ) -> None:
        """Initialize instance state."""
        _validate_batch_correlation(command_index, stable_key, request, evidence)
        if replay_disposition is not None and not isinstance(replay_disposition, ReplayDisposition):
            raise TypeError("replay_disposition must be a ReplayDisposition or None")
        if response is not None and not isinstance(response, Response):
            raise TypeError("response must be a Response or None")
        frozen_result = _freeze_json(result)
        if response is not None and response.result != _thaw_json(frozen_result):
            raise ValueError("batch response result must match the correlated command result")
        object.__setattr__(self, "command_index", command_index)
        object.__setattr__(self, "stable_key", stable_key)
        object.__setattr__(self, "request", request)
        object.__setattr__(self, "_result", frozen_result)
        object.__setattr__(self, "_decoded_rows", len(frozen_result) if isinstance(frozen_result, tuple) else 1)
        object.__setattr__(self, "payload", payload)
        object.__setattr__(self, "evidence", evidence)
        object.__setattr__(self, "replay_disposition", replay_disposition)
        object.__setattr__(self, "response", response)

    @classmethod
    def _from_response(  # noqa: PLR0913 - private zero-copy correlated construction
        cls,
        command_index: int,
        stable_key: str,
        request: Request,
        response: Response,
        payload: object = None,
        evidence: BatchCommandEvidence | None = None,
        replay_disposition: ReplayDisposition | None = None,
    ) -> Self:
        """Share an already frozen correlated response inside the trusted decoder."""
        _validate_batch_correlation(command_index, stable_key, request, evidence)
        if not isinstance(response, Response):
            raise TypeError("response must be a Response")
        if replay_disposition is not None and not isinstance(replay_disposition, ReplayDisposition):
            raise TypeError("replay_disposition must be a ReplayDisposition or None")
        instance = cls.__new__(cls)
        object.__setattr__(instance, "command_index", command_index)
        object.__setattr__(instance, "stable_key", stable_key)
        object.__setattr__(instance, "request", request)
        object.__setattr__(instance, "_result", response._result)  # noqa: SLF001 - same immutable model
        object.__setattr__(
            instance,
            "_decoded_rows",
            len(response._result) if isinstance(response._result, tuple) else 1,  # noqa: SLF001
        )
        object.__setattr__(instance, "payload", payload)
        object.__setattr__(instance, "evidence", evidence)
        object.__setattr__(instance, "replay_disposition", replay_disposition)
        object.__setattr__(instance, "response", response)
        return instance

    @property
    def result(self) -> JsonValue:
        """Return the result."""
        return _thaw_json(self._result)

    @property
    def decoded_rows(self) -> int:
        """Top-level decoded row weight retained by this command outcome."""
        return self._decoded_rows


@dataclass(frozen=True, slots=True)
class BatchFailure:
    """One failed command with total correlation and safe repr."""

    command_index: int
    stable_key: str
    request: Request = field(repr=False)
    error: object = field(repr=False)
    replay_safety: ReplaySafety = ReplaySafety.UNKNOWN
    replay_disposition: ReplayDisposition = ReplayDisposition.NOT_ELIGIBLE
    payload: object = field(default=None, repr=False)
    evidence: BatchCommandEvidence | None = None

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        _validate_batch_correlation(self.command_index, self.stable_key, self.request, self.evidence)
        if not isinstance(self.replay_safety, ReplaySafety):
            raise TypeError("replay_safety must be a ReplaySafety")
        if not isinstance(self.replay_disposition, ReplayDisposition):
            raise TypeError("replay_disposition must be a ReplayDisposition")


def _validate_batch_correlation(
    command_index: int,
    stable_key: str,
    request: Request,
    evidence: BatchCommandEvidence | None,
) -> None:
    if not _is_plain_int(command_index) or command_index < 0:
        raise ValueError("command_index cannot be negative")
    if not stable_key or len(stable_key) > STABLE_KEY_MAXIMUM:
        raise ValueError("stable_key must be 1..100 characters")
    if not isinstance(request, Request):
        raise TypeError("request must be canonical Request")
    if evidence is not None and not isinstance(evidence, BatchCommandEvidence):
        raise TypeError("evidence must be BatchCommandEvidence or None")


type BatchOutcome = BatchSuccess | BatchFailure
