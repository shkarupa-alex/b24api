"""One owner of stream termination: classify, clean up, finalize once, publish, re-raise (spec §3.1).

A stream family supplies an algorithm body that only yields items and raises failures, plus hooks
that close its resources and build its report. The runner decides the terminal cause, runs cleanup
under cancellation protection, finalizes exactly once after cleanup, attaches the published report
to every raised exception and replays a cancellation that cleanup had to defer.
"""

from __future__ import annotations
import asyncio
import inspect
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum, StrEnum
from typing import TYPE_CHECKING, NoReturn, Self, cast

from b24api.contracts.policy import KernelState
from b24api.contracts.report import Violation, ViolationSeverity
from b24api.contracts.stream import PartialResult
from b24api.errors import CleanupOverrunError
from b24api.execution.context import CancellationMark, await_cleanup_resistant, rearm_cancellation
from b24api.execution.failure import (
    EARLY_CLOSE_REASON,
    attach_report,
    public_cause,
    report_reason,
    with_cleanup_failure,
)
from b24api.execution.snapshot import KernelReport

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable
    from types import TracebackType

    from b24api.contracts.report import OperationReport


def _carried_cause(public: BaseException, error: BaseException) -> BaseException | None:
    """Return what ``public``, raised for ``error``, chains from: what a private carrier carries, never the carrier.

    The carried failure itself, when it is what is raised, keeps its own cause (the source's exception, say).
    """
    cause = public_cause(error)
    return public.__cause__ if public is cause else cause


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
    # False when a deadline stopped the wait: cleanup goes on in the background and the body never
    # recorded its terminal, so the family reports from what it holds instead of the body's report.
    finished: bool = True


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
    # Seconds the operation's cleanup window has left (``ExecutionContext.cleanup_window``); bounds how long a
    # close and its cleanup defer a caller's cancellation. ``None`` defers it until cleanup ends, for a runner
    # driven by an outer one.
    cleanup_budget: Callable[[], float] | None = None


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
    cleanup failure and a cleanup cancellation are secondary violations, as is a cleanup that overran
    its window behind a caller's cancellation (``cancellation_outranks``).
    """
    if cancellation_outranks(cause, attempt):
        return with_cleanup_failure(report, cast("BaseException", attempt.error), terminal=False, subject=subject)
    if cause is not TerminalCause.FAILED:
        if attempt.error is None:
            return report
        return with_cleanup_failure(report, attempt.error, terminal=True, subject=subject)
    for failure in (attempt.error, attempt.cancellation):
        if failure is not None:
            report = with_cleanup_failure(report, failure, terminal=False, subject=subject)
    return report


def cancellation_outranks(cause: TerminalCause, attempt: CleanupAttempt) -> bool:
    """Whether a caller's cancellation, not the cleanup failure, ends the stream: the cleanup overran its window.

    The window ended the wait the cancellation was deferred by, so the overrun is that bound, not a failure of
    what was closed: the cancellation is raised and the stream is cancelled, the overrun a secondary violation.
    """
    return cause is TerminalCause.CANCELLED and isinstance(attempt.error, CleanupOverrunError)


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


# The work tokens of the runners whose isolated pull or cleanup owns the current task. Each runs in its
# own task, and every task started while it runs copies its context, so the marker reaches all work done
# on its behalf: a family's workers and the caller code they run (a page-stop callback, a source, a
# transport). A token, not the runner, so a task that outlives the stream never keeps it alive.
_WORK_OWNERS: ContextVar[tuple[object, ...]] = ContextVar("b24api_work_owners", default=())

_SELF_CLOSE = (
    "a stream cannot be closed from inside its own pull or cleanup (a page_stop callback, a command, "
    "binding or reference source, a transport, or a task one of them started): the close would wait for itself"
)


async def _settle(pull: asyncio.Future[object]) -> None:
    # The cancelled read reports its own outcome to its caller; the closer only waits for it.
    await asyncio.wait((pull,))


class OperationRunner[T, R]:
    """Single-use async iterator that owns one body's termination and publishes one report.

    ``isolated_pulls`` runs each read in a child task so ``aclose()`` from another task can cancel
    only that read; a kernel runner driven by one outer runner reads inline instead. ``works_between_reads``
    declares a body whose own tasks keep running caller code while no read is in progress.
    """

    def __init__(
        self,
        body: AsyncIterator[T],
        hooks: LifecycleHooks[R],
        *,
        isolated_pulls: bool = True,
        works_between_reads: bool = False,
    ) -> None:
        """Initialize without starting the body."""
        self._body = body
        self._hooks = hooks
        self._isolated = isolated_pulls
        self._works_between_reads = works_between_reads
        self._phase = _Phase.NEW
        self._pull: asyncio.Future[T] | None = None
        self._published = asyncio.Event()
        self._report: R | None = None
        self._primary: BaseException | None = None
        self._finalize_error: BaseException | None = None
        self._work_token = object()
        # The one cleanup, kept even when a deadline stopped waiting for it, so it lives until it ends.
        self._cleanup_task: asyncio.Future[None] | None = None

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

    @property
    def inside_own_work(self) -> bool:
        """Whether a close requested from the current task could wait for that task itself.

        The task carries this runner's marker when it is the runner's pull or cleanup or was started
        while one ran. A close waits for such work while the pull or cleanup task is running, and for a
        body that works between reads as long as it is open; otherwise it waits for none.
        """
        if not any(token is self._work_token for token in _WORK_OWNERS.get()):
            return False
        running = self._pull if self._phase is _Phase.PULLING else self._cleanup_task
        if running is not None and not running.done():
            return True
        return self._phase is _Phase.NEW and self._works_between_reads

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
            exc.add_note(f"stream cleanup also failed ({report_reason(close_error)})")

    async def __anext__(self) -> T:
        """Read one item; the read that ends the body publishes the report and raises by §3.1."""
        if self._phase in {_Phase.CLOSING, _Phase.CLOSED}:
            raise StopAsyncIteration
        if self._phase is _Phase.PULLING:
            raise RuntimeError("concurrent stream pull")
        # Taken before any state changes, so a failing budget hook cannot leave the read half-started.
        deadline = self._cleanup_deadline()
        self._phase = _Phase.PULLING
        reader = CancellationMark.now()
        try:
            if self._isolated:
                self._pull = asyncio.ensure_future(self._pull_next())
                item = await self._pull
            else:
                item = await anext(self._body)
        except BaseException as error:
            self._pull = None
            if self._phase is _Phase.CLOSING:
                # A concurrent aclose() owns termination; this read reports the published report.
                await self._published.wait()
                closed = self._closed_read_error(error)
                if closed is error:
                    raise
                raise closed from _carried_cause(closed, error)
            self._phase = _Phase.CLOSING
            cause = (
                TerminalCause.EXHAUSTED
                if isinstance(error, StopAsyncIteration)
                else TerminalCause.CANCELLED
                if isinstance(error, asyncio.CancelledError)
                else TerminalCause.FAILED
            )
            # A pull task that ends in its own failure absorbs the reader's cancellation it was handed.
            absorbed = None
            if reader.requested() and not isinstance(error, asyncio.CancelledError):
                absorbed = asyncio.CancelledError()
            await self._terminate(
                cause,
                None if cause is TerminalCause.EXHAUSTED else error,
                pending=absorbed,
                deadline=deadline,
            )
            raise
        self._pull = None
        if self._phase is _Phase.PULLING:
            self._phase = _Phase.NEW
        return item

    def _closed_read_error(self, error: BaseException) -> BaseException:
        """Return what a read a concurrent close ended raises: its own end, a failure mapped as the stream's are."""
        report = self._report
        if report is None:
            return error
        return self._public_error(error, report)

    def _public_error(self, error: BaseException, report: R) -> BaseException:
        """Return the failure the caller sees for ``error``, which is not the stream's primary, with the report.

        A failure that does not end the stream itself (a read a concurrent close ended, or its cleanup's) leaves
        through ``propagate`` as one that ends it does, so a private carrier (a source's failure) never reaches the
        caller.
        """
        self._hooks.attach(error, report)
        propagate = self._hooks.propagate
        if propagate is None or not isinstance(error, Exception) or isinstance(error, StopAsyncIteration):
            return error
        propagated = propagate(error, report)
        self._hooks.attach(propagated, report)
        return propagated

    async def aclose(self) -> None:
        """Close idempotently; an active read is cancelled and awaited before one termination.

        A close requested from inside this runner's own work is refused rather than left waiting for itself.
        """
        if self._phase is _Phase.CLOSED:
            return
        if self.inside_own_work:
            raise RuntimeError(_SELF_CLOSE)
        if self._phase is _Phase.CLOSING:
            await self._published.wait()
            return
        pull = self._pull
        if self._phase is _Phase.PULLING and pull is None:
            raise RuntimeError("an inline stream read cannot be closed from another task")
        # Taken before the phase changes, so a failing budget hook cannot wedge the stream in CLOSING.
        deadline = self._cleanup_deadline()
        self._phase = _Phase.CLOSING
        if pull is not None and not pull.done():
            pull.cancel()
        await self._terminate(TerminalCause.EARLY_CLOSED, None, read=pull, deadline=deadline)

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

    def _cleanup_deadline(self) -> float | None:
        budget = self._hooks.cleanup_budget
        return None if budget is None else asyncio.get_running_loop().time() + max(0.0, budget())

    def _mark_own_work(self) -> None:
        # Called first in the pull or cleanup task, whose context is its own copy, so it needs no reset.
        _WORK_OWNERS.set((*_WORK_OWNERS.get(), self._work_token))

    async def _pull_next(self) -> T:
        self._mark_own_work()
        return await anext(self._body)

    async def _clean_up(self, read: asyncio.Future[T] | None) -> None:
        # Cleanup runs caller code too (a source's ``finally``, a transport), which must not close this runner.
        self._mark_own_work()
        if read is not None:
            # A cancelled read ends before the body closes under it, even after a deadline stopped the wait.
            await _settle(cast("asyncio.Future[object]", read))
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
        read: asyncio.Future[T] | None = None,
        deadline: float | None = None,
    ) -> None:
        """Settle a cancelled ``read``, clean up, publish once and raise by the §3.1 rows.

        A caller's cancellation, pending or arriving during cleanup, is deferred only until ``deadline``;
        cleanup still running then finishes in the background and is reported as failed.
        """
        self._primary = primary
        cancelled = pending is not None or isinstance(primary, asyncio.CancelledError)
        self._cleanup_task = asyncio.ensure_future(self._clean_up(read))
        cleanup = await await_cleanup_resistant(self._cleanup_task, deadline=deadline, cancelled=cancelled)
        attempt = CleanupAttempt(cleanup.error, cleanup.cancellation or pending, finished=cleanup.unfinished is None)
        reason = report_reason(primary) if cause is TerminalCause.FAILED and primary is not None else None
        report, finalize_failed = await self._publish(cause, reason, attempt)
        if cause is TerminalCause.FAILED and primary is not None:
            self._raise_failed(primary, report, attempt, finalize_failed=finalize_failed)
        if finalize_failed and self._finalize_error is not None and primary is None:
            if attempt.error is not None:
                self._finalize_error.add_note(f"stream cleanup also failed ({report_reason(attempt.error)})")
            # The finalization failure leaves in place of a cancellation cleanup absorbed, so it is replayed.
            rearm_cancellation(attempt.cancellation)
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
            primary.add_note(f"stream cleanup also failed ({report_reason(attempt.error)})")
        if finalize_failed and self._finalize_error is not None:
            primary.add_note(f"stream report finalization failed ({type(self._finalize_error).__name__})")
        propagated = self._hooks.propagate(primary, report) if self._hooks.propagate is not None else primary
        self._hooks.attach(primary, report)
        self._hooks.attach(propagated, report)
        rearm_cancellation(attempt.cancellation)
        if propagated is primary:
            raise primary
        raise propagated from _carried_cause(propagated, primary)

    def _raise_terminal(
        self,
        cause: TerminalCause,
        primary: BaseException | None,
        report: R,
        attempt: CleanupAttempt,
    ) -> None:
        """Raise by the exhausted, early-close and cancelled rows; an early close returns quietly."""
        raised: BaseException | None = None
        if attempt.error is not None and not cancellation_outranks(cause, attempt):
            raised = self._public_error(attempt.error, report)
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
        if primary is not None and attempt.cancellation is not None and raised is attempt.cancellation:
            raise attempt.cancellation from primary
        if attempt.error is not None and raised is not attempt.error and raised is not attempt.cancellation:
            raise raised from _carried_cause(raised, attempt.error)
        raise raised


__all__ = [
    "CleanupAttempt",
    "LifecycleHooks",
    "OperationRunner",
    "TerminalCause",
    "cancellation_outranks",
    "cleanup_failed",
    "failed_kernel_report",
    "kernel_terminal",
    "with_cleanup_attempt",
]
