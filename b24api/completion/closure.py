"""Closure witnesses named once, shared by the drivers that set them and the gate evidence that reads them.

A driver records why a traversal ended as a terminal reason string, which also appears in public
reports. The completion recorders translate the qualified reasons into a ``BindingClosure``; keeping
the strings and that translation here means a reworded reason cannot silently fall back to
``SOURCE_EMPTY`` in one reader while another still recognises it.
"""

from __future__ import annotations

from b24api.contracts.completion import BindingClosure

SINGLE_RESPONSE_COMPLETE = "single response complete"
QUALIFIED_TOTAL_REACHED = "qualified total reached"
SPARSE_RAW_RANGE_COVERED = "qualified sparse raw range covered"
ADMITTED_UPPER_BOUNDARY_REACHED = "exact admitted upper boundary reached"
DECLARED_SHORT_PAGE_REACHED = "declared short page reached"
RESPONSE_FLAG_DECLARED_TERMINAL = "response flag declared terminal page"
CONTINUATION_RULE_DECLARED_END = "continuation rule declared end of source"


def qualified_closure(terminal_reason: str | None) -> BindingClosure | None:
    """Return the qualified closure a completed traversal's reason proves, or ``None`` for plain exhaustion."""
    closure: BindingClosure | None = None
    match terminal_reason:
        case "single response complete":
            closure = BindingClosure.SINGLE_RESPONSE
        case "qualified total reached":
            closure = BindingClosure.QUALIFIED_TOTAL
        case "qualified sparse raw range covered":
            closure = BindingClosure.RAW_RANGE_COVERED
        case "exact admitted upper boundary reached":
            closure = BindingClosure.BOUNDARY_SEEN
        case "declared short page reached":
            closure = BindingClosure.DECLARED_SHORT_PAGE
        case "response flag declared terminal page":
            closure = BindingClosure.DECLARED_RESPONSE_TERMINAL
        case "continuation rule declared end of source":
            closure = BindingClosure.RULE_DECLARED_END
    return closure
