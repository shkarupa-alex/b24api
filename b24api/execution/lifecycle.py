"""One owner of stream termination: classify, clean up, finalize once, publish, re-raise (spec §3.1).

A stream family supplies an algorithm body that only yields items and raises failures, plus hooks
that close its resources and build its report. The runner decides the terminal cause, runs cleanup
under cancellation protection, finalizes exactly once after cleanup, attaches the published report
to every raised exception and replays a cancellation that cleanup had to defer.
"""

from __future__ import annotations
import asyncio
import inspect
from dataclasses import dataclass
from enum import Enum, StrEnum
from typing import TYPE_CHECKING, NoReturn, Self, cast

from b24api.contracts.policy import KernelState
from b24api.contracts.report import Violation, ViolationSeverity
from b24api.contracts.stream import PartialResult
from b24api.execution.context import await_cleanup_resistant, rearm_cancellation
from b24api.execution.failure import EARLY_CLOSE_REASON, attach_report, report_reason, with_cleanup_failure
from b24api.execution.snapshot import KernelReport

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable
    from types import TracebackType

    from b24api.contracts.report import OperationReport


class TerminalCause(StrEnum):
    """Why a stream stopped, before its family maps it to a public state."""

    EXHAUSTED = "exhausted"
    EARLY_CLOSED = "early_closed"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class CleanupAttempt:
    """What cleanup reported: its own failure and a cancellation that arrived while it ran."""

    error: BaseException | None = None
    cancellation: asyncio.CancelledError | None = None


@dataclass(frozen=True, slots=True)
class LifecycleHooks[R]:
    """Family callbacks: resource cleanup, the one report, its fallback and error attachment."""

    # Builds the report once, after cleanup; the reason is ``report_reason`` of a FAILED primary.
    finalize: Callable[[TerminalCause, str | None, CleanupAttempt], R | Awaitable[R]]
    # Publishes one minimal FAILED report when ``finalize`` itself raised; never calls it again.
    failure_report: Callable[[TerminalCause, str, CleanupAttempt], R]
    # Closes the source, child work and dispatchers; may raise, never swallowed by the family.
    cleanup: Callable[[], Awaitable[None]]
    attach: Callable[[BaseException, R], None] = attach_report
    # Maps a FAILED primary to the exception the caller receives (for example ``BatchFailed``).
    propagate: Callable[[BaseException, R], BaseException] | None = None


def kernel_terminal(cause: TerminalCause, reason: str | None) -> tuple[KernelState, str]:
    """Return the kernel state and reason a family's report starts from for this cause."""
    if cause is TerminalCause.EXHAUSTED:
        return KernelState.COMPLETED, "input exhausted"
    if cause is TerminalCause.EARLY_CLOSED:
        return KernelState.CANCELLED, EARLY_CLOSE_REASON
    if cause is TerminalCause.CANCELLED:
        return KernelState.CANCELLED, "iteration cancelled"
    return KernelState.FAILED, reason or "stream failed"


def with_cleanup_attempt(
    report: KernelReport,
    cause: TerminalCause,
    attempt: CleanupAttempt,
    *,
    subject: str = "batch",
) -> KernelReport:
    """Record a cleanup result on a kernel report by the §3.1 table.

    Without a primary failure a cleanup failure turns the report FAILED ("stream cleanup failed"); a
    cancellation during that cleanup is raised but not recorded. Behind a FAILED primary both the
    cleanup failure and a cleanup cancellation are secondary violations.
    """
    if cause is not TerminalCause.FAILED:
        if attempt.error is None:
            return report
        return with_cleanup_failure(report, attempt.error, terminal=True, subject=subject)
    for failure in (attempt.error, attempt.cancellation):
        if failure is not None:
            report = with_cleanup_failure(report, failure, terminal=False, subject=subject)
    return report


def cleanup_failed(cause: TerminalCause, attempt: CleanupAttempt) -> bool:
    """Whether the gate must record ``CleanupState.FAILURE`` for this attempt."""
    return attempt.error is not None or (cause is TerminalCause.FAILED and attempt.cancellation is not None)


def with_fallback_cleanup(
    report: KernelReport,
    cause: TerminalCause,
    attempt: CleanupAttempt,
    *,
    subject: str = "batch",
) -> KernelReport:
    """Keep a cleanup result on a fallback report; the finalization failure stays its terminal reason."""
    cancellation = attempt.cancellation if cause is TerminalCause.FAILED else None
    for failure in (attempt.error, cancellation):
        if failure is not None:
            report = with_cleanup_failure(report, failure, terminal=False, subject=subject)
    return report


def failed_kernel_report(
    cause: TerminalCause,
    reason: str,
    attempt: CleanupAttempt,
    *,
    subject: str = "batch",
) -> KernelReport:
    """Return the minimal FAILED kernel report published when a family's finalizer raised."""
    report = KernelReport(
        state=KernelState.FAILED,
        terminal_reason=reason,
        violations=(
            Violation(ViolationSeverity.BLOCKING, "report_finalize_failure", "stream report finalization failed"),
        ),
    )
    return with_fallback_cleanup(report, cause, attempt, subject=subject)


class _Phase(Enum):
    NEW = "new"
    PULLING = "pulling"
    CLOSING = "closing"
    CLOSED = "closed"


async def _pull_next[T](body: AsyncIterator[T]) -> T:
    return await anext(body)


async def _settle(pull: asyncio.Future[object]) -> None:
    # The cancelled read reports its own outcome to its caller; the closer only waits for it.
    await asyncio.wait((pull,))


class OperationRunner[T, R]:
    """Single-use async iterator that owns one body's termination and publishes one report.

    ``isolated_pulls`` runs each read in a child task so ``aclose()`` from another task can cancel
    only that read; a kernel runner driven by one outer runner reads inline instead.
    """

    def __init__(self, body: AsyncIterator[T], hooks: LifecycleHooks[R], *, isolated_pulls: bool = True) -> None:
        """Initialize without starting the body."""
        self._body = body
        self._hooks = hooks
        self._isolated = isolated_pulls
        self._phase = _Phase.NEW
        self._pull: asyncio.Future[T] | None = None
        self._published = asyncio.Event()
        self._report: R | None = None
        self._primary: BaseException | None = None
        self._finalize_error: BaseException | None = None

    @property
    def report(self) -> R | None:
        """Return the report published after cleanup, or ``None`` before termination."""
        return self._report

    @property
    def primary_error(self) -> BaseException | None:
        """Return the body failure or cancellation being finalized, for the family's report hook."""
        return self._primary

    @property
    def terminated(self) -> bool:
        """Whether the report has been published."""
        return self._phase is _Phase.CLOSED

    def __aiter__(self) -> Self:
        """Return this single-use iterator."""
        return self

    async def __aenter__(self) -> Self:
        """Enter without reading an item."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close on exit; a close failure or cancellation never replaces the body's primary exception."""
        del exc_type, traceback
        try:
            await self.aclose()
        except asyncio.CancelledError as cancellation:
            if exc is None or isinstance(exc, asyncio.CancelledError):
                raise
            # §3.1 X + C: the body's exception stays primary and the cancellation lands on the next await.
            rearm_cancellation(cancellation)
        except Exception as close_error:
            if exc is None:
                raise
            exc.add_note(f"stream cleanup also failed ({type(close_error).__name__})")

    async def __anext__(self) -> T:
        """Read one item; the read that ends the body publishes the report and raises by §3.1."""
        if self._phase in {_Phase.CLOSING, _Phase.CLOSED}:
            raise StopAsyncIteration
        if self._phase is _Phase.PULLING:
            raise RuntimeError("concurrent stream pull")
        self._phase = _Phase.PULLING
        try:
            if self._isolated:
                self._pull = asyncio.ensure_future(_pull_next(self._body))
                item = await self._pull
            else:
                item = await anext(self._body)
        except BaseException as error:
            self._pull = None
            if self._phase is _Phase.CLOSING:
                # A concurrent aclose() owns termination; this read reports the published report.
                await self._published.wait()
                if self._report is not None:
                    self._hooks.attach(error, self._report)
                raise
            self._phase = _Phase.CLOSING
            cause = (
                TerminalCause.EXHAUSTED
                if isinstance(error, StopAsyncIteration)
                else TerminalCause.CANCELLED
                if isinstance(error, asyncio.CancelledError)
                else TerminalCause.FAILED
            )
            await self._terminate(cause, None if cause is TerminalCause.EXHAUSTED else error)
            raise
        self._pull = None
        if self._phase is _Phase.PULLING:
            self._phase = _Phase.NEW
        return item

    async def aclose(self) -> None:
        """Close idempotently; an active read is cancelled and awaited before one termination."""
        if self._phase is _Phase.CLOSED:
            return
        if self._phase is _Phase.CLOSING:
            await self._published.wait()
            return
        pull = self._pull
        if self._phase is _Phase.PULLING and pull is None:
            raise RuntimeError("an inline stream read cannot be closed from another task")
        self._phase = _Phase.CLOSING
        pending: asyncio.CancelledError | None = None
        if pull is not None and not pull.done():
            pull.cancel()
            pending = (await await_cleanup_resistant(_settle(cast("asyncio.Future[object]", pull)))).cancellation
        await self._terminate(TerminalCause.EARLY_CLOSED, None, pending=pending)

    async def first(self) -> PartialResult[tuple[T, ...]]:
        """Consume zero or one item, then close without an extra proof read."""
        values: tuple[T, ...] = ()
        try:
            values = (await anext(self),)
        except StopAsyncIteration:
            values = ()
        finally:
            await self.aclose()
        return PartialResult(values, cast("OperationReport", self._report))

    async def collect(self, *, limit: int) -> PartialResult[list[T]]:
        """Consume up to a positive bound, then close without an extra proof read."""
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("limit must be a positive integer")
        values: list[T] = []
        try:
            while len(values) < limit:
                try:
                    values.append(await anext(self))
                except StopAsyncIteration:
                    break
        finally:
            await self.aclose()
        return PartialResult(values, cast("OperationReport", self._report))

    async def _clean_up(self) -> None:
        try:
            close = getattr(self._body, "aclose", None)
            if close is not None:
                await close()
        finally:
            await self._hooks.cleanup()

    async def _publish(self, cause: TerminalCause, reason: str | None, attempt: CleanupAttempt) -> tuple[R, bool]:
        """Finalize once under cancellation protection; return the report and whether finalize failed."""
        published: list[R] = []

        async def finalize() -> None:
            value = self._hooks.finalize(cause, reason, attempt)
            published.append(cast("R", await value) if inspect.isawaitable(value) else value)

        outcome = await await_cleanup_resistant(finalize())
        failed = outcome.error is not None and not published
        try:
            report = published[0] if published else self._hooks.failure_report(cause, reason or cause.value, attempt)
        except BaseException:
            # Even a failing fallback ends publication, so no later close waits for a report that never comes.
            self._phase = _Phase.CLOSED
            self._published.set()
            raise
        self._report = report
        self._phase = _Phase.CLOSED
        self._published.set()
        if outcome.error is not None:
            self._hooks.attach(outcome.error, report)
            self._finalize_error = outcome.error
        rearm_cancellation(outcome.cancellation)
        return report, failed

    async def _terminate(
        self,
        cause: TerminalCause,
        primary: BaseException | None,
        *,
        pending: asyncio.CancelledError | None = None,
    ) -> None:
        self._primary = primary
        cleanup = await await_cleanup_resistant(self._clean_up())
        attempt = CleanupAttempt(cleanup.error, cleanup.cancellation or pending)
        reason = report_reason(primary) if cause is TerminalCause.FAILED and primary is not None else None
        report, finalize_failed = await self._publish(cause, reason, attempt)
        if cause is TerminalCause.FAILED and primary is not None:
            self._raise_failed(primary, report, attempt, finalize_failed=finalize_failed)
        if finalize_failed and self._finalize_error is not None and primary is None:
            raise self._finalize_error
        self._raise_terminal(cause, primary, report, attempt)

    def _raise_failed(
        self,
        primary: BaseException,
        report: R,
        attempt: CleanupAttempt,
        *,
        finalize_failed: bool,
    ) -> NoReturn:
        """Raise a FAILED primary; cleanup and finalization failures only annotate it."""
        if attempt.error is not None:
            primary.add_note(f"stream cleanup also failed ({type(attempt.error).__name__})")
        if finalize_failed and self._finalize_error is not None:
            primary.add_note(f"stream report finalization failed ({type(self._finalize_error).__name__})")
        propagated = self._hooks.propagate(primary, report) if self._hooks.propagate is not None else primary
        self._hooks.attach(primary, report)
        self._hooks.attach(propagated, report)
        rearm_cancellation(attempt.cancellation)
        if propagated is primary:
            raise primary
        raise propagated from primary

    def _raise_terminal(
        self,
        cause: TerminalCause,
        primary: BaseException | None,
        report: R,
        attempt: CleanupAttempt,
    ) -> None:
        """Raise by the exhausted, early-close and cancelled rows; an early close returns quietly."""
        raised: BaseException | None = None
        if attempt.error is not None:
            raised = attempt.error
            primary_cancellation = primary if isinstance(primary, asyncio.CancelledError) else None
            rearm_cancellation(attempt.cancellation or primary_cancellation)
        elif attempt.cancellation is not None:
            raised = attempt.cancellation
        elif primary is not None:
            raised = primary
        if raised is None:
            if cause is TerminalCause.EXHAUSTED:
                raise StopAsyncIteration
            return
        self._hooks.attach(raised, report)
        if attempt.error is None and attempt.cancellation is not None and primary is not None:
            raise raised from primary
        raise raised


__all__ = [
    "CleanupAttempt",
    "LifecycleHooks",
    "OperationRunner",
    "TerminalCause",
    "cleanup_failed",
    "failed_kernel_report",
    "kernel_terminal",
    "with_cleanup_attempt",
]
