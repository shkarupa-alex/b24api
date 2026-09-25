"""Rewrite one HTTPX log record in place without changing what carries no secret.

Both shield paths share :func:`rewrite_record`: an owned dispatch scrubs every credential-shaped segment,
while any other record loses only the registered webhook secrets. A field in which nothing was found keeps
its original object and type, so a foreign record without a secret is left exactly as it was emitted.
A traceback is part of the record too: once a record is protected, its exception and stack are rendered,
scrubbed and stored as text, and the raw ``exc_info`` is dropped, so no later handler can render the
original exception, with its credential-bearing message or frames, again.
"""

from __future__ import annotations
import logging
import re
from typing import TYPE_CHECKING

from b24api.redaction import Redactor

if TYPE_CHECKING:
    from collections.abc import Callable

_REPLACEMENT = "[REDACTED]"
# Fields logging itself sets; everything else on a record arrived through ``extra``.
_STANDARD_RECORD_FIELDS = frozenset(
    (*logging.LogRecord("", logging.INFO, "", 0, "", None, None).__dict__, "message", "asctime"),
)
# Full-length scrubbing: truncating a record's format string would break its %-interpolation.
_RECORD_REDACTOR = Redactor(max_string=1 << 20)
_HOP_CREDENTIAL = re.compile(
    r"(?P<prefix>/rest/(?:api/)?[^/\s?#\"']+/)(?P<token>[^/\s?#\"']+)(?=[/?#\s\"']|$)",
    re.IGNORECASE,
)
_TRACEBACK_FORMATTER = logging.Formatter()
_UNRENDERABLE_TRACEBACK = "[traceback unavailable]"


def redact_registered_value(value: object, credentials: tuple[str, ...]) -> object:
    """Remove only the registered secret from a foreign record field, leaving it otherwise untouched."""
    rendered = str(value)
    if not any(credential in rendered for credential in credentials):
        return value
    for credential in credentials:
        rendered = rendered.replace(credential, _REPLACEMENT)
    return rendered


def redact_owned_value(value: object, credentials: tuple[str, ...]) -> object:
    """Scrub one field of an owned record, keeping its original type when nothing is secret."""
    rendered = str(value)
    scrubbed = rendered
    for credential in credentials:
        scrubbed = scrubbed.replace(credential, _REPLACEMENT)
    scrubbed = _HOP_CREDENTIAL.sub(lambda match: match.group("prefix") + _REPLACEMENT, scrubbed)
    scrubbed = _RECORD_REDACTOR.redact_text(scrubbed)
    return value if scrubbed == rendered else scrubbed


def _rendered_exception(record: logging.LogRecord) -> str | None:
    if record.exc_text:
        return record.exc_text
    if not record.exc_info:
        return None
    try:
        return _TRACEBACK_FORMATTER.formatException(record.exc_info)
    except Exception:  # noqa: BLE001 - a hostile exception must not break logging or escape unscrubbed
        return _UNRENDERABLE_TRACEBACK


def _rewrite_fields(record: logging.LogRecord, redact: Callable[[object], object]) -> bool:
    changed = False
    message = redact(record.msg)
    if message is not record.msg:
        record.msg, changed = message, True
    args = record.args
    if isinstance(args, dict):
        rewritten = {key: redact(value) for key, value in args.items()}
        if any(rewritten[key] is not value for key, value in args.items()):
            record.args, changed = rewritten, True
    elif isinstance(args, tuple):
        values = tuple(redact(value) for value in args)
        if any(new is not old for new, old in zip(values, args, strict=True)):
            record.args, changed = values, True
    # Any caller-supplied extra (a hook's ``hop_url``, instrumentation fields) can carry a URL.
    for name, value in tuple(record.__dict__.items()):
        if name not in _STANDARD_RECORD_FIELDS:
            scrubbed = redact(value)
            if scrubbed is not value:
                record.__dict__[name], changed = scrubbed, True
    return changed


def rewrite_record(record: logging.LogRecord, redact: Callable[[object], object], *, protected: bool) -> None:
    """Scrub a record's message, arguments, extras and traceback, reassigning only fields that changed.

    Args:
        record: The record, rewritten in place before any handler formats it.
        redact: Returns its argument itself when nothing secret was found, otherwise the scrubbed text.
        protected: The record belongs to an owned dispatch, so its traceback is materialized even when
            nothing in it matched; otherwise that happens only once a secret was found in the record.
    """
    changed = _rewrite_fields(record, redact)
    traceback = _rendered_exception(record)
    stack = record.stack_info
    if traceback is None and stack is None:
        return
    scrubbed_traceback = None if traceback is None else redact(traceback)
    scrubbed_stack = None if stack is None else redact(stack)
    if protected or changed or scrubbed_traceback is not traceback or scrubbed_stack is not stack:
        record.exc_text = None if scrubbed_traceback is None else str(scrubbed_traceback)
        record.stack_info = None if scrubbed_stack is None else str(scrubbed_stack)
        # Dropped last, so no later handler can render the original exception and its frames again.
        record.exc_info = None
