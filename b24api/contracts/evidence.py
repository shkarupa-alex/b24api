"""Redacted HTTP evidence: a contracts leaf that the error base and responses both carry."""

from __future__ import annotations
from dataclasses import dataclass

from b24api.redaction import DEFAULT_REDACTOR


@dataclass(frozen=True, slots=True)
class ResponseEvidence:
    """Bounded redacted HTTP evidence safe for default serialization."""

    http_status: int | None = None
    request_id: str | None = None
    headers: tuple[tuple[str, str], ...] = ()
    body_preview: str | None = None

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        redacted_headers = DEFAULT_REDACTOR.redact(dict(self.headers))
        object.__setattr__(
            self,
            "headers",
            tuple(sorted((str(key), str(value)) for key, value in redacted_headers.items()))[:50],
        )
        if self.request_id is not None:
            object.__setattr__(self, "request_id", DEFAULT_REDACTOR.redact_text(self.request_id))
        if self.body_preview is not None:
            object.__setattr__(self, "body_preview", DEFAULT_REDACTOR.redact_text(self.body_preview))

    def to_dict(self) -> dict[str, object]:
        """Return the to dict representation."""
        return {
            "http_status": self.http_status,
            "request_id": self.request_id,
            "headers": dict(self.headers),
            "body_preview": self.body_preview,
        }


__all__ = ["ResponseEvidence"]
