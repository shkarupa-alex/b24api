"""Explicit offset closure-rule selection independent of wire stride."""

from __future__ import annotations

from b24api.contracts.traversal import OffsetSpec, TotalTermination
from b24api.traversal.plans import OffsetTerminalRule


def offset_terminal_rules(offset: OffsetSpec) -> frozenset[OffsetTerminalRule]:
    """Keep sparse raw closure separate from selected-row emptiness."""
    if offset.sparse_raw_bound is not None:
        return frozenset({OffsetTerminalRule.SPARSE_RAW_BOUND})
    if offset.total_termination is TotalTermination.DISABLED:
        return frozenset({OffsetTerminalRule.EMPTY_PAGE})
    return frozenset({OffsetTerminalRule.EMPTY_PAGE, OffsetTerminalRule.QUALIFIED_TOTAL})
