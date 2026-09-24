"""Lazy admission-time observation for logical command sources."""

from __future__ import annotations
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable, Iterator
from typing import TYPE_CHECKING, Protocol, Self, runtime_checkable

from b24api.contracts.command import Command
from b24api.contracts.violation import retain_violations

if TYPE_CHECKING:
    from b24api.contracts.report import Violation
    from b24api.contracts.request import Request


@runtime_checkable
class _SyncClosable(Protocol):
    def close(self) -> None: ...


@runtime_checkable
class _AsyncClosable(Protocol):
    async def aclose(self) -> None: ...


class _SyncAuditSource[C](Iterator[Command[C]]):
    def __init__(self, source: Iterable[Command[C]], audit: Callable[[Request], Violation | None]) -> None:
        self._iterator = iter(source)
        self._audit = audit
        self.violations: list[Violation] = []

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Command[C]:
        command = next(self._iterator)
        if isinstance(command, Command):
            violation = self._audit(command.request)
            if violation is not None:
                # Bounded like the report: a long source must not retain one violation per command.
                self.violations = list(retain_violations((*self.violations, violation)))
        return command

    def close(self) -> None:
        if isinstance(self._iterator, _SyncClosable):
            self._iterator.close()


class _AsyncAuditSource[C](AsyncIterator[Command[C]]):
    def __init__(self, source: AsyncIterable[Command[C]], audit: Callable[[Request], Violation | None]) -> None:
        self._iterator = aiter(source)
        self._audit = audit
        self.violations: list[Violation] = []

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Command[C]:
        command = await anext(self._iterator)
        if isinstance(command, Command):
            violation = self._audit(command.request)
            if violation is not None:
                # Bounded like the report: a long source must not retain one violation per command.
                self.violations = list(retain_violations((*self.violations, violation)))
        return command

    async def aclose(self) -> None:
        if isinstance(self._iterator, _AsyncClosable):
            await self._iterator.aclose()


def audit_command_source[C](
    source: Iterable[Command[C]] | AsyncIterable[Command[C]],
    audit: Callable[[Request], Violation | None],
) -> Iterable[Command[C]] | AsyncIterable[Command[C]]:
    """Observe each command exactly when its lazy source admits it."""
    if isinstance(source, AsyncIterable):
        return _AsyncAuditSource(source, audit)
    return _SyncAuditSource(source, audit)


__all__ = ["audit_command_source"]
