"""Immutable public values shared by execution and evidence layers."""

from __future__ import annotations
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from b24api.redaction import DEFAULT_REDACTOR, Redactor


@dataclass(frozen=True, slots=True)
class RequestSummary:
    """Bounded request identity that intentionally excludes parameter values."""

    method: str
    parameter_keys: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {"method": self.method, "parameter_keys": list(self.parameter_keys)}


@dataclass(frozen=True, slots=True)
class ResponseEvidence:
    """Bounded redacted HTTP evidence safe for default serialization."""

    http_status: int | None = None
    request_id: str | None = None
    headers: tuple[tuple[str, str], ...] = ()
    body_preview: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "http_status": self.http_status,
            "request_id": self.request_id,
            "headers": dict(self.headers),
            "body_preview": self.body_preview,
        }


@dataclass(frozen=True, slots=True)
class BatchCommandEvidence:
    """Safe correlation facts for one batch command."""

    command_index: int
    stable_key: str
    original_code: str | int | None = None
    normalized_code: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "command_index": self.command_index,
            "stable_key": self.stable_key,
            "original_code": self.original_code,
            "normalized_code": self.normalized_code,
        }


def summarize_request(
    method: object,
    parameters: object = None,
    *,
    redactor: Redactor = DEFAULT_REDACTOR,
) -> RequestSummary:
    """Build a safe summary from canonical or legacy request-like values."""
    safe_method = redactor.redact_text(str(method))
    keys: tuple[str, ...] = ()
    if isinstance(parameters, Mapping):
        keys = tuple(sorted(redactor.redact_text(str(key)) for key in parameters)[: redactor.max_items])
    return RequestSummary(method=safe_method, parameter_keys=keys)


def summarize_request_like(request: Any, *, redactor: Redactor = DEFAULT_REDACTOR) -> RequestSummary | None:  # noqa: ANN401
    """Summarize an optional duck-typed request without importing legacy models."""
    if request is None:
        return None
    return summarize_request(
        getattr(request, "method", type(request).__name__),
        getattr(request, "parameters", None),
        redactor=redactor,
    )
