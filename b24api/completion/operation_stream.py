"""Internal lifecycle adapter from subsystem streams to the v2 contract."""

from __future__ import annotations
import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from typing import TYPE_CHECKING, Protocol, Self, cast

from b24api.completion.gate import CompletionGate, CompletionReportFacts
from b24api.contracts.policy import KernelState
from b24api.contracts.report import (
    KeysetSelectionSummary,
    OperationReport,
    TerminalState,
    TraversalAssurance,
    Violation,
)
from b24api.contracts.stream import PartialResult
from b24api.errors import IncompleteTraversalError
from b24api.execution.context import await_cleanup_resistant, rearm_cancellation
from b24api.execution.failure import attach_report as _attach_report
from b24api.execution.failure import finalize_failure

if TYPE_CHECKING:
    from b24api.execution.snapshot import KernelReport

type Mapper[S, T] = Callable[[S], T | Awaitable[T]]


class _ClosableIterator[T](AsyncIterator[T], Protocol):
    report: KernelReport

    async def aclose(self) -> None:
        """Close the underlying subsystem stream."""
        ...


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
        error_mapper: Callable[[BaseException, OperationReport], BaseException] | None = None,
        error_items: Callable[[BaseException], Iterable[T]] | None = None,
        count_admitted: Callable[[T], bool] | None = None,
        source_admitted: Callable[[], int] | None = None,
        source_buffered_commands: Callable[[], int] | None = None,
        source_active_references: Callable[[], int] | None = None,
        initial_violations: tuple[Violation, ...] = (),
        source_violations: Callable[[], tuple[Violation, ...]] | None = None,
        deregister: Callable[[MappedOperationStream[S, T]], None] | None = None,
        keyset_selection: KeysetSelectionSummary | None = None,
    ) -> None:
        """Initialize without starting or prefetching the source."""
        self._source = source
        self._mapper = mapper
        self._operation = operation
        self._assurance = assurance
        self._classify = classify
        self._error_mapper = error_mapper
        self._error_items = error_items
        self._count_admitted = count_admitted
        self._source_admitted = source_admitted
        self._source_buffered_commands = source_buffered_commands
        self._source_active_references = source_active_references
        self._initial_violations = initial_violations
        self._keyset_selection = keyset_selection
        self._source_violations = source_violations
        self._deregister = deregister
        self._report: OperationReport | None = None
        self._terminal_error: BaseException | None = None
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

    async def __anext__(self) -> T:  # noqa: C901, PLR0912 - lifecycle and outcome finalization are one transition
        """Pull exactly one source item and map it."""
        if self._terminated:
            if self._early_closed:
                raise RuntimeError("stream was closed before exhaustion")
            if self._terminal_error is not None:
                raise self._terminal_error
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
            await self._close_then_finalize(TerminalState.CANCELLED)
            _attach_report(error, cast("OperationReport", self._report))
            self._terminal_error = error
            raise
        except BaseException as error:
            cleanup = await await_cleanup_resistant(self._close_source())
            if cleanup.error is not None:
                # The primary failure stays the raised one; the source already recorded its cleanup violation.
                error.add_note(f"stream cleanup also failed ({type(cleanup.error).__name__})")
            rearm_cancellation(cleanup.cancellation)
            if self._error_items is not None:
                for item in self._error_items(error):
                    if self._count_admitted is None or self._count_admitted(item):
                        self._admitted += 1
                    self._record_variant(item)
            forced = TerminalState.INCOMPLETE if isinstance(error, IncompleteTraversalError) else TerminalState.FAILED
            self._finalize(forced_state=forced)
            report = cast("OperationReport", self._report)
            report, _ = finalize_failure(
                error,
                report,
                operation=self._operation,
                terminal_reason=report.terminal_reason,
            )
            self._report = report
            propagated = self._error_mapper(error, report) if self._error_mapper is not None else error
            _attach_report(propagated, report)
            self._terminal_error = propagated
            if propagated is error:
                raise
            raise propagated from error
        finally:
            self._pull = None
        item = await _resolve(self._mapper(source_item))
        if self._count_admitted is None or self._count_admitted(item):
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
        await self._close_then_finalize(TerminalState.EARLY_CLOSED)

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

    async def _close_then_finalize(self, forced_state: TerminalState) -> None:
        """Close the source and publish the report even when closing fails; the close failure carries it."""
        try:
            await self._close_source()
        except BaseException as error:
            self._finalize(forced_state=forced_state)
            _attach_report(error, cast("OperationReport", self._report))
            raise
        self._finalize(forced_state=forced_state)

    def _record_variant(self, item: T) -> None:
        variant = self._classify(item) if self._classify is not None else "success"
        if variant == "item":
            return
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
        gate = getattr(self._source, "completion_gate", None)
        extra_violations = (*self._initial_violations, *(_read_violations(self._source_violations)))
        admitted = max(self._admitted, _read_source_counter(self._source_admitted))
        buffered_commands = _read_source_counter(self._source_buffered_commands)
        active_references = _read_source_counter(self._source_active_references)
        if not isinstance(gate, CompletionGate):
            raise TypeError("public operation source lacks completion evidence")
        if self._source.report.state is KernelState.NOT_STARTED:
            gate.abort_unstarted()
        gate.attach_report(
            CompletionReportFacts(
                source=self._source.report,
                operation=self._operation,
                assurance=self._assurance,
                admitted=admitted,
                emitted=self._emitted,
                successes=self._successes,
                failures=self._failures,
                not_executed=self._not_executed,
                unknown=self._unknown,
                buffered_commands_high_water=buffered_commands,
                active_references_high_water=active_references,
                early_closed=self._early_closed,
                forced_state=forced_state,
                extra_violations=extra_violations,
                keyset_selection=self._keyset_selection,
            )
        )
        report = gate.finish()
        self._report = report
        self._terminated = True
        if self._deregister is not None:
            self._deregister(self)


def _read_source_counter(reader: Callable[[], int] | None) -> int:
    """Read one explicitly wired non-negative source metric."""
    if reader is None:
        return 0
    value = reader()
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise RuntimeError("operation source exposed an invalid report counter")
    return value


def _read_violations(reader: Callable[[], tuple[Violation, ...]] | None) -> tuple[Violation, ...]:
    if reader is None:
        return ()
    values = tuple(reader())
    if any(not isinstance(value, Violation) for value in values):
        raise RuntimeError("operation source exposed invalid violations")
    return values


__all__ = ["MappedOperationStream"]
