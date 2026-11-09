"""Public whole-page callback and bounded-prefix stop decision."""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from b24api.contracts.json import FrozenMapping
from b24api.contracts.report import PageRecord
from b24api.redaction import DEFAULT_REDACTOR

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from b24api.contracts.json import FrozenJson

_MAX_REASON_LENGTH = 128


@dataclass(frozen=True, slots=True)
class PageBoundary:
    """One validated whole page offered after all its rows were consumed.

    ``result`` is the page's immutable server result, the same snapshot a page adapter, a window
    classifier and a continuation rule see as ``PageView.result``: response-level fields beside the
    rows, such as a flag or a popup object, reach the application on every offered page, including
    a page that admits no row. It is ``None`` only for a boundary built by application code. Like
    ``rows``, it is checked at its top level only: a mutable mapping or list there is rejected, and
    nested values are the frozen values the library builds or the caller's responsibility.
    """

    binding_id: int
    record: PageRecord
    rows: tuple[FrozenJson, ...]
    result: FrozenJson = field(default=None, kw_only=True, repr=False)

    def __post_init__(self) -> None:
        """Keep correlation and page content immutable and exact."""
        if type(self.binding_id) is not int or self.binding_id < 0:
            raise ValueError("binding_id must be a non-negative integer")
        if not isinstance(self.record, PageRecord):
            raise TypeError("record must be a PageRecord")
        object.__setattr__(self, "rows", tuple(self.rows))
        if len(self.rows) != self.record.rows_admitted:
            raise ValueError("page boundary must contain all admitted rows")
        if self.result is not None and not isinstance(self.result, str | int | float | tuple | FrozenMapping):
            raise TypeError("result must be frozen JSON, not a mutable mapping or list")


class ContinuePage(StrEnum):
    """Continue scheduling after acknowledgement."""

    CONTINUE = "continue"


@dataclass(frozen=True, slots=True)
class CallerStop:
    """Stop one binding after its acknowledged page."""

    reason: str

    def __post_init__(self) -> None:
        """Keep the public reason bounded and redacted."""
        if not isinstance(self.reason, str):
            raise TypeError("stop reason must be a string")
        safe = DEFAULT_REDACTOR.redact_text(self.reason)
        if not safe or len(safe) > _MAX_REASON_LENGTH:
            raise ValueError("stop reason must contain 1..128 safe characters")
        object.__setattr__(self, "reason", safe)


type PageStopResult = ContinuePage | CallerStop


class PageStopPolicy(Protocol):
    """Application-owned durable whole-page acknowledgement and stop choice."""

    def on_page(self, boundary: PageBoundary) -> PageStopResult | Awaitable[PageStopResult]:
        """Commit the page and decide whether this binding continues."""
        ...
