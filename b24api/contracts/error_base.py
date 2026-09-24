"""Base of the public error hierarchy, owned by the contracts layer so contracts never import ``errors``.

``b24api.errors`` re-exports both classes as the same objects and remains their public address.
"""

from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar

from b24api._error_types import ErrorOrigin
from b24api.contracts.evidence import ResponseEvidence
from b24api.redaction import DEFAULT_REDACTOR, Redactor

if TYPE_CHECKING:
    from b24api._diagnostics import DiagnosticContext
    from b24api.contracts.request import RequestSummary


class B24ApiError(Exception):
    """Base error whose default text and serialization contain safe evidence only."""

    default_origin: ClassVar[ErrorOrigin | None] = None

    def __init__(  # noqa: PLR0913
        self,
        message: str,
        *,
        origin: ErrorOrigin | None = None,
        description: str | None = None,
        request_summary: RequestSummary | None = None,
        evidence: ResponseEvidence | None = None,
        retryable: bool = False,
        redactor: Redactor = DEFAULT_REDACTOR,
        diagnostics: DiagnosticContext | None = None,
    ) -> None:
        """Render every text field once, through the request's diagnostic context when one is given."""
        resolved_origin = origin or self.default_origin
        if resolved_origin is None:
            raise TypeError("origin is required for B24ApiError")
        self.origin = resolved_origin
        self.description = redactor.render_text(description, context=diagnostics) if description is not None else None
        self.request_summary = request_summary
        self.request = request_summary
        self.evidence = evidence or ResponseEvidence()
        self.retryable = retryable
        super().__init__(redactor.render_text(message, context=diagnostics))

    @property
    def http_status(self) -> int | None:
        """Return the http status."""
        return self.evidence.http_status

    def to_safe_dict(self) -> dict[str, object]:
        """Serialize only bounded redacted fields."""
        return {
            "type": type(self).__name__,
            "origin": self.origin.value,
            "message": str(self),
            "description": self.description,
            "request": self.request_summary.to_dict() if self.request_summary else None,
            "evidence": self.evidence.to_dict(),
            "retryable": self.retryable,
        }

    def __repr__(self) -> str:
        """Return a safe representation."""
        return f"{type(self).__name__}({self.to_safe_dict()!r})"


class BudgetExceededError(B24ApiError):
    """Execution would exceed an explicit operational budget."""

    default_origin = ErrorOrigin.BUDGET


__all__ = ["B24ApiError", "BudgetExceededError"]
