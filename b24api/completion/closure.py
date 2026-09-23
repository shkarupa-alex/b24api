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


def qualified_closure(terminal_reason: str | None) -> BindingClosure | None:
    """Return the qualified closure a completed traversal's reason proves, or ``None`` for plain exhaustion."""
    match terminal_reason:
        case "single response complete":
            return BindingClosure.SINGLE_RESPONSE
        case "qualified total reached":
            return BindingClosure.QUALIFIED_TOTAL
        case "qualified sparse raw range covered":
            return BindingClosure.RAW_RANGE_COVERED
        case "exact admitted upper boundary reached":
            return BindingClosure.BOUNDARY_SEEN
        case _:
            return None
