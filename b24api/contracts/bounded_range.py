"""Caller-qualified exact upper boundary for one keyset filter."""

from __future__ import annotations
import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Self, cast

from b24api.contracts.request import ParameterPath, Request

_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{64}$")
_VERSION_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")


def filter_fingerprint(request: Request, path: ParameterPath) -> str:
    """Hash the detached exact-case base filter before traversal-owned controls."""
    current: object = request.copy_parameters()
    for part in path.path:
        if type(part) is str and isinstance(current, Mapping) and part in current:
            current = cast("Mapping[str, object]", current)[part]
        elif type(part) is int and isinstance(current, list) and 0 <= part < len(current):
            current = cast("list[object]", current)[part]
        else:
            current = {}
            break
    if not isinstance(current, Mapping):
        raise TypeError("bounded keyset base filter must be an object")
    encoded = json.dumps(current, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class BoundedIdentityRange:
    """Exact admitted upper ID and enforced fence for one versioned filter."""

    filter_fingerprint: str
    upper_id: int
    lower_exclusive: int | None
    order: str
    fence_path: ParameterPath
    source_version: str

    def __post_init__(self) -> None:
        """Reject ambiguous bounds or unversioned caller assertions."""
        if not _FINGERPRINT_RE.fullmatch(self.filter_fingerprint):
            raise ValueError("filter_fingerprint must be a SHA-256 hex digest")
        if type(self.upper_id) is not int or self.upper_id < 1:
            raise ValueError("upper_id must be a positive integer")
        if self.lower_exclusive is not None and (
            type(self.lower_exclusive) is not int or self.lower_exclusive < 0 or self.lower_exclusive >= self.upper_id
        ):
            raise ValueError("lower_exclusive must be below the upper_id")
        if self.order != "ascending":
            raise ValueError("bounded keyset currently requires ascending order")
        if not isinstance(self.fence_path, ParameterPath):
            raise TypeError("fence_path must be a ParameterPath")
        if not isinstance(self.source_version, str) or not _VERSION_RE.fullmatch(self.source_version):
            raise ValueError("source_version must be a bounded profile identifier")

    @classmethod
    def capture(  # noqa: PLR0913
        cls,
        request: Request,
        *,
        filter_path: ParameterPath,
        upper_id: int,
        lower_exclusive: int | None,
        fence_path: ParameterPath,
        source_version: str,
    ) -> Self:
        """Bind a qualified boundary to the exact caller base filter."""
        return cls(
            filter_fingerprint(request, filter_path), upper_id, lower_exclusive,
            "ascending", fence_path, source_version,
        )
