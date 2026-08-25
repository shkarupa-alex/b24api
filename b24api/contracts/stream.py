"""Public asynchronous stream protocol and bounded partial helper value."""

from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, Self

if TYPE_CHECKING:
    from b24api.contracts.report import OperationReport


@dataclass(frozen=True, slots=True)
class PartialResult[T]:
    """A bounded value paired with its frozen non-complete report."""

    value: T
    report: OperationReport


class OperationStream[T](Protocol):
    """Single-use asynchronous operation with explicit lifecycle ownership."""

    @property
    def report(self) -> OperationReport | None:
        """Return the frozen report after termination, otherwise none."""
        ...

    def __aiter__(self) -> Self:
        """Return this single-use asynchronous iterator."""
        ...

    async def __anext__(self) -> T:
        """Return the next item or finalize terminal evidence."""
        ...

    async def __aenter__(self) -> Self:
        """Enter without prefetching."""
        ...

    async def __aexit__(self, *_exc: object) -> None:
        """Close owned work on context exit."""
        ...

    async def aclose(self) -> None:
        """Close idempotently, including an in-flight pull."""
        ...

    async def first(self) -> PartialResult[tuple[T, ...]]:
        """Consume at most one item and close."""
        ...

    async def collect(self, *, limit: int) -> PartialResult[list[T]]:
        """Consume at most a positive bounded number of items and close."""
        ...


__all__ = ["OperationStream", "PartialResult"]
