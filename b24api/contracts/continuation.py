"""Pure caller-owned continuation rules and inclusive-window row classification.

A continuation rule and a window classifier see one immutable page and return a value; the SDK owns
every request, all state, validation, cycle detection and the terminal witness. Nothing here
performs I/O or keeps state between pages.
"""

from __future__ import annotations
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from b24api.contracts.reference import ParameterUpdate
from b24api.contracts.request import ParameterPath, ResultSelector

if TYPE_CHECKING:
    from b24api.contracts.json import FrozenJson
    from b24api.contracts.page import PageView

_MISSING = object()


class ContinuationReason(StrEnum):
    """Closed reasons a continuation or inclusive-window traversal cannot continue truthfully."""

    RULE_FAILED = "rule_failed"
    INVALID_UPDATES = "invalid_updates"
    MISSING_CONTINUATION = "missing_continuation"
    CONTINUATION_CYCLE = "continuation_cycle"
    WINDOW_UNQUALIFIED = "window_unqualified"
    WINDOW_CLASSIFICATION_INVALID = "window_classification_invalid"
    WINDOW_OUTSIDE_BOUND = "window_outside_bound"
    WINDOW_NONPROGRESS = "window_nonprogress"
    WINDOW_TIMESTAMP_INVALID = "window_timestamp_invalid"


class _ContinuationContractError(Exception):
    """A built-in rule found the page unable to continue; the SDK raises the typed public error."""

    def __init__(self, reason: ContinuationReason, message: str) -> None:
        self.reason = reason
        super().__init__(message)


def _lookup(value: FrozenJson, path: tuple[str | int, ...]) -> FrozenJson | object:
    """Return the value at an exact result path, or the missing marker."""
    current: FrozenJson | object = value
    for part in path:
        if isinstance(part, str):
            if not isinstance(current, Mapping) or part not in current:
                return _MISSING
            current = current[part]
        else:
            if not isinstance(current, tuple) or part >= len(current):
                return _MISSING
            current = current[part]
    return current


@dataclass(frozen=True, slots=True)
class ResponseFlagTerminal:
    """A required boolean ``result`` flag: ``True`` means more pages, ``False`` closes the source.

    ``selector`` is relative to ``result``. A ``False`` flag closes even a non-empty page, so no
    empty confirmation request is sent; a missing or non-boolean flag fails the traversal closed.
    """

    selector: ResultSelector

    def __post_init__(self) -> None:
        """Require a non-root result path; a root flag cannot share a result with the rows."""
        if not isinstance(self.selector, ResultSelector):
            raise TypeError("terminal selector must be a ResultSelector")
        if not self.selector.path:
            raise ValueError("terminal flag selector must name a non-root result path")


def _response_more(result: FrozenJson, terminal: ResponseFlagTerminal) -> bool:
    """Return the declared more-flag of one page or raise the closed missing-continuation reason."""
    flag = _lookup(result, terminal.selector.path)
    if flag is _MISSING:
        raise _ContinuationContractError(ContinuationReason.MISSING_CONTINUATION, "response flag is missing")
    if not isinstance(flag, bool):
        raise _ContinuationContractError(ContinuationReason.MISSING_CONTINUATION, "response flag is not a boolean")
    return flag


@dataclass(frozen=True, slots=True)
class ContinueWith:
    """Continue with exactly these control values; the SDK validates them against the declared controls."""

    updates: tuple[ParameterUpdate, ...]

    def __post_init__(self) -> None:
        """Freeze the update sequence; the SDK checks paths, types and size."""
        object.__setattr__(self, "updates", tuple(self.updates))


@dataclass(frozen=True, slots=True)
class EndOfSource:
    """The rule declares the source ended; the current page is still validated and delivered."""


class ContinuationRule(Protocol):
    """Pure synchronous decision from one immutable page to the next control vector or the end."""

    def evaluate(self, page: PageView, /) -> ContinueWith | EndOfSource:
        """Return the complete next control vector, or the end of the source."""
        ...


@dataclass(frozen=True, slots=True)
class ResponseCursorRule:
    """Carry one response-owned JSON cursor, whole, into one control while a boolean flag says more.

    With ``True`` the cursor must be present and non-null; it is copied unchanged (an object cursor
    keeps every field and its full precision). With ``False`` the rule ends the source.
    """

    cursor_selector: ResultSelector
    parameter_path: ParameterPath
    terminal: ResponseFlagTerminal

    def __post_init__(self) -> None:
        """Validate the rule's typed locations."""
        if not isinstance(self.cursor_selector, ResultSelector) or not self.cursor_selector.path:
            raise ValueError("cursor_selector must be a non-root ResultSelector")
        if not isinstance(self.parameter_path, ParameterPath):
            raise TypeError("parameter_path must be a ParameterPath")
        if not isinstance(self.terminal, ResponseFlagTerminal):
            raise TypeError("terminal must be a ResponseFlagTerminal")

    def evaluate(self, page: PageView, /) -> ContinueWith | EndOfSource:
        """Return the whole response cursor while the flag says more, else the end of the source."""
        if not _response_more(page.result, self.terminal):
            return EndOfSource()
        cursor = _lookup(page.result, self.cursor_selector.path)
        if cursor is _MISSING or cursor is None:
            raise _ContinuationContractError(
                ContinuationReason.MISSING_CONTINUATION,
                "response cursor is missing while the flag declares more pages",
            )
        return ContinueWith((ParameterUpdate(self.parameter_path, cursor),))


@dataclass(frozen=True, slots=True)
class WindowRows:
    """A complete disjoint split of one page's row indexes into paging and supplemental rows.

    Paging rows belong to the inclusive timestamp window and carry the next boundary; supplemental
    rows (for example pinned or fixed entries) are delivered but never move the boundary.
    """

    paging_indexes: tuple[int, ...]
    supplemental_indexes: tuple[int, ...]

    def __post_init__(self) -> None:
        """Freeze both index sequences; the SDK checks they partition the page exactly."""
        object.__setattr__(self, "paging_indexes", tuple(self.paging_indexes))
        object.__setattr__(self, "supplemental_indexes", tuple(self.supplemental_indexes))


class WindowClassifier(Protocol):
    """Pure synchronous split of one immutable page; raising declares the page unqualified."""

    def classify(self, page: PageView, /) -> WindowRows:
        """Return every row index exactly once as paging or supplemental."""
        ...


__all__ = [
    "ContinuationReason",
    "ContinuationRule",
    "ContinueWith",
    "EndOfSource",
    "ResponseCursorRule",
    "ResponseFlagTerminal",
    "WindowClassifier",
    "WindowRows",
]
