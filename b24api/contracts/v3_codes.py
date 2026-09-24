"""The closed set of REST 3.0 error codes b24api recognizes, and their one safe rendering.

The set is the single owner of V3 code knowledge: protocol classification reads it to decide
retryability and the error renderer reads it to decide which codes may be shown verbatim. Everything
outside it, including any other ``SCREAMING_SNAKE`` or ``BITRIX_REST_V3_*`` string, keeps the
conservative free-text redaction, because a caller credential may have the same shape. A new code is
one literal here plus one test.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.redaction import DEFAULT_REDACTOR, REDACTED, Redactor

if TYPE_CHECKING:
    from b24api._diagnostics import DiagnosticContext

VALIDATION_ERROR_CODE = "BITRIX_REST_V3_EXCEPTION_VALIDATION_REQUESTVALIDATIONEXCEPTION"
METHOD_NOT_FOUND_ERROR_CODE = "BITRIX_REST_V3_EXCEPTION_METHODNOTFOUNDEXCEPTION"
# Every known code is a deterministic request fault; a retryable code would join through its own subset.
NON_RETRYABLE_V3_ERROR_CODES = frozenset({VALIDATION_ERROR_CODE, METHOD_NOT_FOUND_ERROR_CODE})
KNOWN_V3_ERROR_CODES = NON_RETRYABLE_V3_ERROR_CODES
_KNOWN_FOLDED = frozenset(code.casefold() for code in KNOWN_V3_ERROR_CODES)


def is_known_v3_code(code: str) -> bool:
    """Report whether a wire code is one of the fixed known V3 literals, ignoring case only."""
    return code.casefold() in _KNOWN_FOLDED


def render_code(
    code: str | int,
    *,
    redactor: Redactor = DEFAULT_REDACTOR,
    context: DiagnosticContext | None = None,
) -> str:
    """Render one structured error code: exact secret, then known V3 literal, then free-text redaction.

    Recognition never overrides an exact secret. Case folding is the only normalization, so the
    lower-cased public ``code`` attribute of a known literal stays diagnosable; because recognition
    ignores case, a secret equal up to case is redacted too. A longer or shorter string that merely
    contains a known code is free text.
    """
    text = str(code)
    if redactor.is_known_secret(text, ignore_case=True) or (
        context is not None and context.is_known_secret(text, ignore_case=True)
    ):
        return REDACTED
    if is_known_v3_code(text):
        return text
    return redactor.render_text(text, context=context)


__all__ = [
    "KNOWN_V3_ERROR_CODES",
    "METHOD_NOT_FOUND_ERROR_CODE",
    "NON_RETRYABLE_V3_ERROR_CODES",
    "VALIDATION_ERROR_CODE",
    "is_known_v3_code",
    "render_code",
]
