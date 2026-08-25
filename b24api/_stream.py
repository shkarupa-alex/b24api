"""Internal lifecycle adapter from proven subsystem streams to the v2 contract."""

from __future__ import annotations
import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import replace
from typing import Protocol, Self, cast

from b24api.contracts.report import OperationReport, TerminalState, TraversalAssurance
from b24api.contracts.stream import PartialResult
from b24api.error import IncompleteTraversalError
from b24api.models import OperationReport as KernelReport
from b24api.models import TerminalState as KernelTerminalState

type Mapper[S, T] = Callable[[S], T | Awaitable[T]]


class _ClosableIterator[T](AsyncIterator[T], Protocol):
    report: KernelReport

    async def aclose(self) -> None:
        """Close the underlying subsystem stream."""
        ...


def _terminal_state(state: KernelTerminalState, *, failures: int) -> TerminalState:
    if state is KernelTerminalState.COMPLETED:
        return TerminalState.COMPLETED_WITH_FAILURES if failures else TerminalState.COMPLETED
    if state is KernelTerminalState.INCOMPLETE:
        return TerminalState.INCOMPLETE
    if state is KernelTerminalState.CANCELLED:
        return TerminalState.CANCELLED
    return TerminalState.FAILED


def _public_report(  # noqa: PLR0913
    report: KernelReport,
    *,
    operation: str,
    assurance: TraversalAssurance | None,
    admitted: int,
    emitted: int,
    successes: int,
    failures: int,
    not_executed: int,
    unknown: int,
    early_closed: bool = False,
) -> OperationReport:
    state = TerminalState.EARLY_CLOSED if early_closed else _terminal_state(report.state, failures=failures)
    return OperationReport(
        state=state,
        operation=operation,
        terminal_reason=report.terminal_reason or state.value,
        assurance=assurance,
        admitted=admitted,
        emitted=emitted,
        successes=successes,
        failures=failures,
        not_executed=not_executed,
        unknown=unknown,
        unique_rows=report.unique_rows,
        physical_requests=report.physical_requests,
        logical_pages=report.logical_pages,
        batch_requests=report.batch_requests,
        batch_commands=report.batch_commands,
        retries=report.retries,
        cooldown_seconds=report.cooldown_seconds,
        buffered_rows_high_water=report.buffered_rows_high_water,
        violations=report.violations,
    )


def _attach_report(error: BaseException, report: OperationReport) -> None:
    with contextlib.suppress(AttributeError, TypeError):
        error.report = report  # type: ignore[attr-defined]


async def _resolve[T](value: T | Awaitable[T]) -> T:
    if isinstance(value, Awaitable):
        return await value
    return value


async def _pull_next[T](source: AsyncIterator[T]) -> T:
    return await anext(source)


class MappedOperationStream[S, T]:
    """Map an owned kernel stream while publishing one v2 terminal report."""

    def __init__(  # noqa: PLR0913
        self,
        source: _ClosableIterator[S],
        mapper: Mapper[S, T],
        *,
        operation: str,
        assurance: TraversalAssurance | None = None,
        classify: Callable[[T], str] | None = None,
        deregister: Callable[[MappedOperationStream[S, T]], None] | None = None,
    ) -> None:
        """Initialize without starting or prefetching the source."""
        self._source = source
        self._mapper = mapper
        self._operation = operation
        self._assurance = assurance
        self._classify = classify
        self._deregister = deregister
        self._report: OperationReport | None = None
        self._pull: asyncio.Task[S] | None = None
        self._terminated = False
        self._early_closed = False
        self._admitted = 0
        self._emitted = 0
        self._successes = 0
        self._failures = 0
        self._not_executed = 0
        self._unknown = 0

    @property
    def report(self) -> OperationReport | None:
        """Return the same frozen terminal report after cleanup."""
        return self._report

    def __aiter__(self) -> Self:
        """Return this single-use stream."""
        return self

    async def __aenter__(self) -> Self:
        """Enter without prefetching."""
        if self._terminated:
            raise RuntimeError("stream is already terminated")
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Close owned work on context exit."""
        await self.aclose()

    async def __anext__(self) -> T:
        """Pull exactly one source item and map it."""
        if self._terminated:
            if self._early_closed:
                raise RuntimeError("stream was closed before exhaustion")
            raise StopAsyncIteration
        if self._pull is not None:
            raise RuntimeError("concurrent stream pulls are not allowed")
        self._pull = asyncio.create_task(_pull_next(self._source))
        try:
            source_item = await self._pull
        except StopAsyncIteration:
            self._finalize()
            raise
        except asyncio.CancelledError as error:
            await self._close_source()
            self._finalize(forced_state=TerminalState.CANCELLED)
            _attach_report(error, cast("OperationReport", self._report))
            raise
        except BaseException as error:
            await self._close_source()
            forced = TerminalState.INCOMPLETE if isinstance(error, IncompleteTraversalError) else TerminalState.FAILED
            self._finalize(forced_state=forced)
            _attach_report(error, cast("OperationReport", self._report))
            raise
        finally:
            self._pull = None
        item = await _resolve(self._mapper(source_item))
        self._admitted += 1
        self._emitted += 1
        self._record_variant(item)
        return item

    async def aclose(self) -> None:
        """Close idempotently, including while a pull is in flight."""
        if self._terminated:
            return
        self._early_closed = True
        pull = self._pull
        if pull is not None and not pull.done():
            pull.cancel()
            with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
                await pull
        await self._close_source()
        self._finalize(forced_state=TerminalState.EARLY_CLOSED)

    async def first(self) -> PartialResult[tuple[T, ...]]:
        """Consume zero or one item without an extra proof pull."""
        values: tuple[T, ...]
        try:
            values = (await anext(self),)
        except StopAsyncIteration:
            values = ()
        if not self._terminated:
            await self.aclose()
        return PartialResult(values, cast("OperationReport", self._report))

    async def collect(self, *, limit: int) -> PartialResult[list[T]]:
        """Consume up to a positive caller bound without an extra proof pull."""
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("limit must be a positive integer")
        values: list[T] = []
        while len(values) < limit:
            try:
                values.append(await anext(self))
            except StopAsyncIteration:
                break
        if not self._terminated:
            await self.aclose()
        return PartialResult(values, cast("OperationReport", self._report))

    async def _close_source(self) -> None:
        await self._source.aclose()

    def _record_variant(self, item: T) -> None:
        variant = self._classify(item) if self._classify is not None else "success"
        if variant == "success":
            self._successes += 1
        elif variant == "failure":
            self._failures += 1
        elif variant == "not_executed":
            self._not_executed += 1
        elif variant == "unknown":
            self._unknown += 1
        else:
            raise RuntimeError("stream outcome classifier returned an unknown variant")

    def _finalize(self, *, forced_state: TerminalState | None = None) -> None:
        if self._report is not None:
            return
        report = _public_report(
            self._source.report,
            operation=self._operation,
            assurance=self._assurance,
            admitted=self._admitted,
            emitted=self._emitted,
            successes=self._successes,
            failures=self._failures,
            not_executed=self._not_executed,
            unknown=self._unknown,
            early_closed=self._early_closed,
        )
        if forced_state is not None and report.state is not forced_state:
            report = replace(report, state=forced_state, terminal_reason=forced_state.value)
        self._report = report
        self._terminated = True
        if self._deregister is not None:
            self._deregister(self)


__all__ = ["MappedOperationStream"]
