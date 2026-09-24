"""Internal lifecycle adapter from subsystem streams to the v2 contract."""

from __future__ import annotations
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import replace
from typing import TYPE_CHECKING, Protocol, Self

from b24api.completion.gate import CompletionGate, CompletionReportFacts
from b24api.contracts.policy import KernelState
from b24api.contracts.report import (
    KeysetSelectionSummary,
    OperationReport,
    TerminalState,
    TraversalAssurance,
    Violation,
    ViolationSeverity,
)
from b24api.errors import IncompleteTraversalError
from b24api.execution.failure import finalize_failure
from b24api.execution.lifecycle import (
    CleanupAttempt,
    LifecycleHooks,
    OperationRunner,
    TerminalCause,
    with_cleanup_attempt,
    with_fallback_cleanup,
)
from b24api.execution.snapshot import KernelReport

if TYPE_CHECKING:
    from types import TracebackType

    from b24api.contracts.stream import PartialResult

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
        self._early_closed = False
        self._cleanup: tuple[TerminalCause, CleanupAttempt] | None = None
        self._admitted = 0
        self._emitted = 0
        self._successes = 0
        self._failures = 0
        self._not_executed = 0
        self._unknown = 0
        self._runner: OperationRunner[T, OperationReport] = OperationRunner(
            self._items(),
            LifecycleHooks(
                finalize=self._finalize,
                failure_report=self._failure_report,
                cleanup=self._source.aclose,
                propagate=self._propagate,
            ),
        )

    @property
    def report(self) -> OperationReport | None:
        """Return the same frozen terminal report after cleanup."""
        return self._report

    def __aiter__(self) -> Self:
        """Return this single-use stream."""
        return self

    async def __aenter__(self) -> Self:
        """Enter without prefetching."""
        if self._runner.terminated:
            raise RuntimeError("stream is already terminated")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close owned work on context exit; a close failure never replaces the body's own exception."""
        await self._runner.__aexit__(exc_type, exc, traceback)

    async def __anext__(self) -> T:
        """Pull exactly one source item and map it; after termination no work starts."""
        return await anext(self._runner)

    async def aclose(self) -> None:
        """Close idempotently, including while a pull is in flight."""
        await self._runner.aclose()

    async def first(self) -> PartialResult[tuple[T, ...]]:
        """Consume zero or one item without an extra proof pull."""
        return await self._runner.first()

    async def collect(self, *, limit: int) -> PartialResult[list[T]]:
        """Consume up to a positive caller bound without an extra proof pull."""
        return await self._runner.collect(limit=limit)

    async def _items(self) -> AsyncIterator[T]:
        while True:
            try:
                source_item = await anext(self._source)
            except StopAsyncIteration:
                return
            item = await _resolve(self._mapper(source_item))
            if self._count_admitted is None or self._count_admitted(item):
                self._admitted += 1
            self._emitted += 1
            self._record_variant(item)
            yield item

    def _finalize(self, cause: TerminalCause, _reason: str | None, attempt: CleanupAttempt) -> OperationReport:
        self._cleanup = (cause, attempt)
        error = self._runner.primary_error
        if cause is TerminalCause.EARLY_CLOSED:
            self._early_closed = True
            return self._publish(forced_state=TerminalState.EARLY_CLOSED)
        if cause is TerminalCause.CANCELLED:
            return self._publish(forced_state=TerminalState.CANCELLED)
        if cause is TerminalCause.EXHAUSTED or error is None:
            return self._publish()
        if self._error_items is not None:
            for item in self._error_items(error):
                if self._count_admitted is None or self._count_admitted(item):
                    self._admitted += 1
                self._record_variant(item)
        forced = TerminalState.INCOMPLETE if isinstance(error, IncompleteTraversalError) else TerminalState.FAILED
        report = self._publish(forced_state=forced)
        report, _ = finalize_failure(error, report, operation=self._operation, terminal_reason=report.terminal_reason)
        self._report = report
        return report

    def _failure_report(self, cause: TerminalCause, reason: str, attempt: CleanupAttempt) -> OperationReport:
        self._cleanup = (cause, attempt)
        if self._report is not None:
            return self._report
        failed = with_fallback_cleanup(
            KernelReport(state=KernelState.FAILED, terminal_reason=reason), cause, attempt, subject="stream"
        )
        facts = self._facts(failed, forced_state=TerminalState.FAILED)
        facts = replace(
            facts,
            extra_violations=(
                *facts.extra_violations,
                Violation(ViolationSeverity.BLOCKING, "report_finalize_failure", "stream report finalization failed"),
            ),
        )
        self._report = self._gate().publish_failure(facts)
        self._terminated()
        return self._report

    def _propagate(self, error: BaseException, report: OperationReport) -> BaseException:
        return self._error_mapper(error, report) if self._error_mapper is not None else error

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

    def _gate(self) -> CompletionGate:
        gate = getattr(self._source, "completion_gate", None)
        if not isinstance(gate, CompletionGate):
            raise TypeError("public operation source lacks completion evidence")
        return gate

    def _facts(self, source: KernelReport, *, forced_state: TerminalState | None) -> CompletionReportFacts:
        return CompletionReportFacts(
            source=source,
            operation=self._operation,
            assurance=self._assurance,
            admitted=max(self._admitted, _read_source_counter(self._source_admitted)),
            emitted=self._emitted,
            successes=self._successes,
            failures=self._failures,
            not_executed=self._not_executed,
            unknown=self._unknown,
            buffered_commands_high_water=_read_source_counter(self._source_buffered_commands),
            active_references_high_water=_read_source_counter(self._source_active_references),
            early_closed=self._early_closed,
            forced_state=forced_state,
            extra_violations=(*self._initial_violations, *(_read_violations(self._source_violations))),
            keyset_selection=self._keyset_selection,
        )

    def _publish(self, *, forced_state: TerminalState | None = None) -> OperationReport:
        gate = self._gate()
        if self._source.report.state is KernelState.NOT_STARTED:
            gate.abort_unstarted()
        gate.attach_report(self._facts(self._source_report(), forced_state=forced_state))
        self._report = gate.finish()
        self._terminated()
        return self._report

    def _source_report(self) -> KernelReport:
        """Return the source's report with this stream's own cleanup result when the source lacks one."""
        report = self._source.report
        if self._cleanup is None or any(violation.code == "cleanup_failure" for violation in report.violations):
            return report
        cause, attempt = self._cleanup
        return with_cleanup_attempt(report, cause, attempt, subject="stream")

    def _terminated(self) -> None:
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
