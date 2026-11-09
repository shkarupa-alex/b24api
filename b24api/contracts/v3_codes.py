"""REST 3.0 error codes: how a structured code is recognized, and the few codes b24api acts on.

Recognition and retry are separate decisions.

Recognition decides only whether a code may be shown verbatim. A REST 3.0 server names its refusal in
``error.code``, and that set is open: by default the code is the upper-cased PHP class name of the
thrown exception (``BITRIX_REST_V3_EXCEPTION_ACCESSDENIEDEXCEPTION``), but any module may subclass the
server exception and override the code (``NOTE_EMPTY_UPDATE``, ``FEATURE_NOT_AVAILABLE_ON_CURRENT_PLAN``,
lower-case ``invalid_select_field``). A closed list would drift, so the structured ``error.code`` of a
REST 3.0 envelope is recognized by shape: an ASCII snake-case identifier of at least two words, upper
case with each word starting with a letter, or lower case of letters only. That shape is applied to
nothing else: free text, a classic ``{"error": ...}`` code and every other channel keep the conservative
bare-credential redaction, which hides any 16+ character run of letters and digits. Recognition never
overrides a registered exact secret, the request's own secrets or its field names: a code holding a
secret in any letter case is redacted whole, and one holding a request field name is aliased, or
redacted when only its letter case differs from the request's spelling.

Retry is decided by the policy, not by recognition. ``RetryPolicy.transient_api_codes`` and
``AmbiguityPolicy.refusal_api_codes`` include the server's load limiter codes by default; every other
code, recognized or not, is a deterministic refusal unless the caller lists it. A REST 3.0 envelope
with one of the two codes in ``NON_RETRYABLE_V3_ERROR_CODES`` is never retried, even when listed.
"""

from __future__ import annotations
import re
from typing import TYPE_CHECKING

from b24api.redaction import DEFAULT_REDACTOR, REDACTED, Redactor

if TYPE_CHECKING:
    from b24api._diagnostics import DiagnosticContext

VALIDATION_ERROR_CODE = "BITRIX_REST_V3_EXCEPTION_VALIDATION_REQUESTVALIDATIONEXCEPTION"
METHOD_NOT_FOUND_ERROR_CODE = "BITRIX_REST_V3_EXCEPTION_METHODNOTFOUNDEXCEPTION"
# 429, thrown by the load limiter's check before the method starts: a refusal that proves nothing ran.
RATE_LIMIT_ERROR_CODE = "BITRIX_REST_V3_EXCEPTION_RATELIMITEXCEPTION"
# 503, thrown when the database killed a query of the running method on its time limit: it may have run.
QUERY_TIME_LIMIT_ERROR_CODE = "BITRIX_REST_V3_EXCEPTION_LIMITER_QUERYTIMELIMITEXCEPTION"

# Deterministic request faults: never retried, even when a caller lists them as transient.
NON_RETRYABLE_V3_ERROR_CODES = frozenset({VALIDATION_ERROR_CODE, METHOD_NOT_FOUND_ERROR_CODE})
# Load limits worth another attempt after the server's ``Retry-After``; the retry policy lists them by default.
TRANSIENT_V3_ERROR_CODES = frozenset({RATE_LIMIT_ERROR_CODE, QUERY_TIME_LIMIT_ERROR_CODE})
# The subset that proves the request did not run; the ambiguity policy lists it by default.
REFUSAL_V3_ERROR_CODES = frozenset({RATE_LIMIT_ERROR_CODE})
# The codes b24api names itself. They render verbatim in every channel; any other code only by shape.
KNOWN_V3_ERROR_CODES = NON_RETRYABLE_V3_ERROR_CODES | TRANSIENT_V3_ERROR_CODES
_KNOWN_FOLDED = frozenset(code.casefold() for code in KNOWN_V3_ERROR_CODES)
# Upper-case codes carry digits only in version words (``V2``, ``V3``, ``B2E``); every lower-case code the
# server sends is letters only, which keeps lower-case credentials such as ``sk_live_a1b2c3...`` out.
_UPPER_CODE_SHAPE = re.compile(r"[A-Z][A-Z0-9]*(?:_[A-Z][A-Z0-9]*)+")
_LOWER_CODE_SHAPE = re.compile(r"[a-z]+(?:_[a-z]+)+")


def is_known_v3_code(code: str) -> bool:
    """Report whether a wire code is one of the fixed known V3 literals, ignoring case only."""
    return code.casefold() in _KNOWN_FOLDED


def is_v3_code_shape(code: str) -> bool:
    """Report whether a string has the shape of a REST 3.0 ``error.code``, in upper or lower case.

    The shape is ASCII snake case of at least two words: upper case with each word starting with a
    letter (``BITRIX_REST_V3_EXCEPTION_RATELIMITEXCEPTION``, ``NOTE_EMPTY_UPDATE``), or lower case of
    letters only (``invalid_select_field``). Mixed case, a word starting with a digit
    (``UF_CRM_1700000000123``), a lower-case digit or any other character is not.
    """
    return _UPPER_CODE_SHAPE.fullmatch(code) is not None or _LOWER_CODE_SHAPE.fullmatch(code) is not None


def render_code(
    code: str | int,
    *,
    redactor: Redactor = DEFAULT_REDACTOR,
    context: DiagnosticContext | None = None,
    v3_envelope: bool = False,
) -> str:
    """Render one structured error code: exact secret, then known literal, then V3 shape, then free text.

    Recognition never overrides an exact secret: a code that equals or contains a registered or request
    secret, in any letter case, is redacted whole. A known literal is recognized up to letter case, so the
    lower-cased public ``code`` attribute stays diagnosable. With ``v3_envelope``, the code came from
    ``error.code`` of a REST 3.0 envelope, and a code of the V3 shape is shown unless it holds a request
    field name in any letter case, which is aliased, or redacted when only its case differs. Anything
    else is free text.
    """
    text = str(code)
    # Free-text replacement is case-sensitive, and a code is also shown lower-cased: a code that holds a
    # secret in any letter case is hidden whole.
    if redactor.contains_known_secret(text) or (context is not None and context.contains_known_secret(text)):
        return REDACTED
    if is_known_v3_code(text):
        return text
    if v3_envelope and is_v3_code_shape(text):
        if context is None or not context.aliases_text(text):
            return text
        # A request field name, possibly in another letter case than the request spelled it, is never shown.
        rendered = redactor.render_text(text, context=context)
        return rendered if rendered != text else REDACTED
    return redactor.render_text(text, context=context)


__all__ = [
    "KNOWN_V3_ERROR_CODES",
    "METHOD_NOT_FOUND_ERROR_CODE",
    "NON_RETRYABLE_V3_ERROR_CODES",
    "QUERY_TIME_LIMIT_ERROR_CODE",
    "RATE_LIMIT_ERROR_CODE",
    "REFUSAL_V3_ERROR_CODES",
    "TRANSIENT_V3_ERROR_CODES",
    "VALIDATION_ERROR_CODE",
    "is_known_v3_code",
    "is_v3_code_shape",
    "render_code",
]
