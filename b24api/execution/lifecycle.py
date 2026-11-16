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
from typing import TYPE_CHECKING, NoReturn, Protocol, Self, cast

from b24api.contracts.policy import KernelState
from b24api.contracts.report import Violation, ViolationSeverity
from b24api.contracts.stream import PartialResult
from b24api.execution.context import (
    OPERATION_LATCH,
    WORK_OWNERS,
    CancellationMark,
    OperationLatch,
    Settled,
    await_cleanup_resistant,
    is_escape,
    is_interrupt,
    rearm_cancellation,
    spawn_settled,
)
from b24api.execution.failure import (
    CANCELLED_REASON,
    EARLY_CLOSE_REASON,
    INTERRUPTED_REASON,
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
    # An interrupt (``is_interrupt``) ended it; the operation raises it and reports it (``with_interrupt``).
    INTERRUPTED = "interrupted"


# The causes behind which a failed cleanup is secondary: something failed first.
_FAILING = frozenset({TerminalCause.FAILED, TerminalCause.INTERRUPTED})


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
    # Counts an item once its reader has it. The body counts what it reads; a reader cancelled as a read hands its
    # item over never gets that item, so only this counts what the stream delivered.
    delivered: Callable[[], None] | None = None
    # Returns the report of an operation an interrupt ended in its one shape (``with_interrupt``), applied to the
    # report published either way, finalized or fallback. ``None`` for a runner whose operation reports elsewhere.
    interrupted: Callable[[R, BaseException], R] | None = None


def kernel_terminal(cause: TerminalCause, reason: str | None) -> tuple[KernelState, str]:
    """Return the kernel state and reason a family's report starts from for this cause."""
    if cause is TerminalCause.EXHAUSTED:
        return KernelState.COMPLETED, "input exhausted"
    if cause is TerminalCause.EARLY_CLOSED:
        return KernelState.CANCELLED, EARLY_CLOSE_REASON
    if cause is TerminalCause.CANCELLED:
        return KernelState.CANCELLED, CANCELLED_REASON
    if cause is TerminalCause.INTERRUPTED:
        return KernelState.FAILED, INTERRUPTED_REASON
    return KernelState.FAILED, reason or "stream failed"


def with_cleanup_attempt(report: KernelReport, cause: TerminalCause, attempt: CleanupAttempt) -> KernelReport:
    """Record a cleanup result on a kernel report by the §3.1 table.

    Without a primary failure a cleanup failure turns the report FAILED ("stream cleanup failed"); a
    cancellation during that cleanup is raised but not recorded. Behind a FAILED primary or an interrupt both the
    cleanup failure and a cleanup cancellation are secondary violations, as is a cleanup failure behind a caller's
    cancellation (``cancellation_outranks``). An escape is never a cleanup failure: the runner latches it.
    """
    if cancellation_outranks(cause, attempt):
        return with_cleanup_failure(report, cast("BaseException", attempt.error), terminal=False)
    if cause not in _FAILING:
        if attempt.error is None:
            return report
        return with_cleanup_failure(report, attempt.error, terminal=True)
    for failure in (attempt.error, attempt.cancellation):
        if failure is not None:
            report = with_cleanup_failure(report, failure, terminal=False)
    return report


def cancellation_outranks(cause: TerminalCause, attempt: CleanupAttempt) -> bool:
    """Whether a caller's cancellation, not the cleanup failure, ends the stream: the cancellation came first.

    The reader's cancellation ended the stream before its close failed or overran its window, so the cancellation
    is raised and the stream is cancelled, the close failure a secondary violation ("stream cleanup also failed").
    """
    return cause is TerminalCause.CANCELLED and attempt.error is not None


def closed_behind_its_end(cause: TerminalCause, attempt: CleanupAttempt) -> bool:
    """Whether the stream's close failed behind what ended it first: its reader's cancellation, or an interrupt.

    A source the stream closes records a failed close as what failed it, not knowing what came first; behind either
    end the close is "stream cleanup also failed", however the end reached the close (in a read or between reads).
    """
    return cancellation_outranks(cause, attempt) or (cause is TerminalCause.INTERRUPTED and attempt.error is not None)


def cleanup_failed(cause: TerminalCause, attempt: CleanupAttempt) -> bool:
    """Whether the gate must record ``CleanupState.FAILURE`` for this attempt."""
    return attempt.error is not None or (cause in _FAILING and attempt.cancellation is not None)


def with_fallback_cleanup(report: KernelReport, cause: TerminalCause, attempt: CleanupAttempt) -> KernelReport:
    """Keep a cleanup result on a fallback report; the finalization failure stays its terminal reason."""
    cancellation = attempt.cancellation if cause in _FAILING else None
    for failure in (attempt.error, cancellation):
        if failure is not None:
            report = with_cleanup_failure(report, failure, terminal=False)
    return report


def failed_kernel_report(cause: TerminalCause, reason: str, attempt: CleanupAttempt) -> KernelReport:
    """Return the minimal FAILED kernel report published when a family's finalizer raised."""
    report = KernelReport(
        state=KernelState.FAILED,
        terminal_reason=reason,
        violations=(
            Violation(ViolationSeverity.BLOCKING, "report_finalize_failure", "stream report finalization failed"),
        ),
    )
    return with_fallback_cleanup(report, cause, attempt)


class _Phase(Enum):
    NEW = "new"
    PULLING = "pulling"
    # An interrupt ended the operation between reads and is out already; nobody has begun its end (cleanup and
    # publication) yet. Whoever comes first runs it: the task the interrupt started, a read or a close.
    INTERRUPTED = "interrupted"
    # Somebody (a read, a close, the task an interrupt started) has claimed the end; its cleanup has not begun.
    CLOSING = "closing"
    # The end's cleanup has begun closing the source. An end is published once its cleanup began, or once its claimant
    # gave up waiting for it at its deadline; a cleanup cancelled before it began (a loop's shutdown cancels every task
    # it finds) closed nothing, so the claimant starts it again (``_end``).
    CLEANING = "cleaning"
    CLOSED = "closed"


# An end some caller runs: a read or close that finds it waits for its publication.
_ENDING = frozenset({_Phase.CLOSING, _Phase.CLEANING})


_SELF_CLOSE = (
    "a stream cannot be closed from inside its own pull or cleanup (a page_stop callback, a command, "
    "binding or reference source, a transport, or a task one of them started): the close would wait for itself"
)


class _Read:
    """One isolated read; ``closed`` once the runner's close cancelled it."""

    __slots__ = ("closed",)

    def __init__(self) -> None:
        self.closed = False


# The isolated read whose task runs the current code. A kernel runner reads inline in its outer runner's read, so
# a cancellation that the outer runner's close delivers ends the kernel by that close, not by a caller's cancel.
# Only such an inline runner consults it: an isolated runner's own close is its CLOSING phase, and a stream of the
# caller's that code run by the read reads (a transport, say, in a task the read started) is not this read's.
_CURRENT_READ: ContextVar[_Read | None] = ContextVar("b24api_current_read", default=None)


def _closed_by_owner(error: BaseException) -> bool:
    read = _CURRENT_READ.get()
    return isinstance(error, asyncio.CancelledError) and read is not None and read.closed


async def _settle(pull: asyncio.Future[Settled[object]], read: _Read) -> BaseException | None:
    """Wait for the read the close ended; return the failure a read the close cancelled ended in instead.

    The read reports its own outcome to its caller as well. Its failure is the close's: it ended while the close
    cancelled it, in its cleanup (a kernel read inline in it ends by that close, failing when its cleanup does).
    """
    await asyncio.wait((pull,))
    if not read.closed or pull.cancelled():
        return None
    error = pull.result().error
    return None if isinstance(error, StopAsyncIteration) else error


class _Report(Protocol):
    """A stream's published report: its violations record what failed, a failed close among them."""

    @property
    def violations(self) -> tuple[Violation, ...]:
        """Return the report's violations."""
        ...


class OperationRunner[T, R: _Report]:
    """Single-use async iterator that owns one body's termination and publishes one report.

    ``isolated_pulls`` runs each read in a child task so ``aclose()`` from another task can cancel
    only that read; a kernel runner driven by one outer runner reads inline instead. ``works_between_reads``
    declares a body whose own tasks keep running caller code while no read is in progress.

    An isolated runner is its operation's: it owns the operation's ``OperationLatch``, set in its read and cleanup
    tasks for all the work they start. An interrupt latched while a read is in flight ends that read, which raises
    it, as does every end of the stream, once the report is published. One latched while no read is in flight goes
    to the event loop at once, and the operation's end follows (``_interrupted``): no reader is there to raise it.
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
        self._pull: asyncio.Future[Settled[T]] | None = None
        self._read = _Read()
        # Set as the phase turns CLOSED: what a read or close waits on for an end another caller is running.
        self._published = asyncio.Event()
        self._report: R | None = None
        self._primary: BaseException | None = None
        self._finalize_error: BaseException | None = None
        self._cleanup_failure: BaseException | None = None
        self._own_latch = OperationLatch(self._interrupted)
        self._work_token = object()
        # The one cleanup, kept even when a deadline stopped waiting for it, so it lives until it ends.
        self._cleanup_task: asyncio.Future[Settled[None]] | None = None
        # The task that runs the end of an operation an interrupt ended between reads. The event loop keeps only a weak
        # reference to a task, so this one keeps it from being collected before it ends: nothing else refers to it.
        self._ending: asyncio.Future[Settled[None]] | None = None

    @property
    def report(self) -> R | None:
        """Return the report published after cleanup, or ``None`` before termination."""
        return self._report

    @property
    def primary_error(self) -> BaseException | None:
        """Return the body failure or cancellation being finalized, for the family's report hook."""
        return self._primary

    @property
    def interrupt(self) -> BaseException | None:
        """Return the interrupt that ends this stream's operation (``OperationLatch``), ``None`` without one."""
        return self._latch().first

    @property
    def cleanup_failure(self) -> BaseException | None:
        """Return the cleanup failure this stream raised as its end, ``None`` when anything else ended it.

        A runner driven by an outer one raises it from the read or close that ended it; the outer runner reads it as
        that close's failure, not as a failure of the work it read, so every stream reports a failed close alike.
        """
        return self._cleanup_failure

    @property
    def terminated(self) -> bool:
        """Whether the report has been published."""
        return self._phase is _Phase.CLOSED

    @property
    def inside_own_work(self) -> bool:
        """Whether a close requested from the current task could wait for that task itself.

        The task carries this runner's marker when it is the runner's pull or cleanup or was started
        while one ran. A close waits for such work while the pull or cleanup task is running, for a
        body that works between reads as long as it is open, and for all of it from the moment an interrupt
        ended the operation between reads until its end begins; otherwise it waits for none.
        """
        if not any(token is self._work_token for token in WORK_OWNERS.get()):
            return False
        running = self._pull if self._phase is _Phase.PULLING else self._cleanup_task
        if running is not None and not running.done():
            return True
        return self._phase is _Phase.INTERRUPTED or (self._phase is _Phase.NEW and self._works_between_reads)

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
        """Close on exit; a close failure or cancellation never replaces the body's primary exception.

        The body's own exception is noted with the report's record of the failed close, word for word.
        """
        del exc_type, traceback
        try:
            await self.aclose()
        except asyncio.CancelledError as cancellation:
            if exc is None or isinstance(exc, asyncio.CancelledError):
                raise
            # §3.1 X + C: the body's exception stays primary and the cancellation lands on the next await.
            rearm_cancellation(cancellation)
        except Exception:
            if exc is None:
                raise
            if (note := self._failed_close_record()) is not None:
                exc.add_note(note)

    def _failed_close_record(self) -> str | None:
        """Return the published report's record of the failed close."""
        violations = self._report.violations if self._report is not None else ()
        return next((violation.message for violation in violations if violation.code == "cleanup_failure"), None)

    async def __anext__(self) -> T:
        """Read one item; the read that ends the body publishes the report and raises by §3.1."""
        await self._stop_if_ended()
        if self._phase is _Phase.PULLING:
            raise RuntimeError("concurrent stream pull")
        # Taken before any state changes, so a failing budget hook cannot leave the read half-started.
        deadline = self._cleanup_deadline()
        self._phase = _Phase.PULLING
        reader = CancellationMark.now()
        try:
            if self._isolated:
                self._read = _Read()
                self._pull = spawn_settled(self._pull_next(self._read))
                item = (await self._pull).unwrap()
            else:
                item = await anext(self._body)
        except BaseException as error:
            # Where an inline kernel first catches an escape its read raised (its send runs in the read): its end
            # raises what the kernel ends with, a failed cleanup say, so the latch is what keeps it.
            self._latch().record(error)
            self._pull = None
            if self._phase in _ENDING:
                # A concurrent aclose() owns termination; this read reports the published report.
                await self._published.wait()
                self._raise_latched()
                if is_escape(error):
                    # The close took it (``_raise_latched``): this read ends as the close's cancellation of it ends it.
                    raise asyncio.CancelledError from None
                closed = self._closed_read_error(error)
                if closed is error:
                    raise
                raise closed from _carried_cause(closed, error)
            self._phase = _Phase.CLOSING
            cause = self._cause_of(error)
            # A pull task that ends in its own failure absorbs the reader's cancellation it was handed.
            absorbed = None
            if reader.requested() and not isinstance(error, asyncio.CancelledError):
                absorbed = asyncio.CancelledError()
            await self._terminate(
                cause,
                error if cause in {TerminalCause.CANCELLED, TerminalCause.FAILED} else None,
                pending=absorbed,
                deadline=deadline,
            )
            raise
        self._pull = None
        if self._phase is _Phase.PULLING:
            self._phase = _Phase.NEW
            await self._end_if_interrupted(deadline)
        if self._hooks.delivered is not None:
            self._hooks.delivered()
        return item

    async def _stop_if_ended(self) -> None:
        """Raise ``StopAsyncIteration`` for a read of a stream that ended.

        A read that finds the operation ending sees its end over first: it runs the end an interrupt left between
        reads that nobody began (``_end_between_reads``), or waits for the one another caller runs. Only a read from
        the stream's own work cannot, since that end waits for it.
        """
        if not self.inside_own_work:
            await self._see_end()
        if self._phase in {_Phase.INTERRUPTED, *_ENDING, _Phase.CLOSED}:
            raise StopAsyncIteration

    async def _see_end(self) -> None:
        """See an end in progress over: run the one nobody began, or wait for the one another caller runs."""
        if self._phase is _Phase.INTERRUPTED:
            await self._end_between_reads()
        elif self._phase in _ENDING:
            await self._published.wait()

    async def _end_if_interrupted(self, deadline: float | None) -> None:
        """End the stream with this read when an interrupt was latched while it ran, though the read returned an item.

        The read the interrupt woke had its item already (the wake's cancellation came too late), or the read's own
        task latched it and went on: the read raises the interrupt instead of delivering the item.
        """
        if self._own_latch.first is not None:
            self._phase = _Phase.CLOSING
            await self._terminate(TerminalCause.INTERRUPTED, None, deadline=deadline)

    def _cause_of(self, error: BaseException) -> TerminalCause:
        """Classify the end of the read that ended the body."""
        if isinstance(error, StopAsyncIteration):
            return TerminalCause.EXHAUSTED
        if is_interrupt(error):
            return TerminalCause.INTERRUPTED
        if not self._isolated and _closed_by_owner(error):
            return TerminalCause.EARLY_CLOSED
        if isinstance(error, asyncio.CancelledError):
            return TerminalCause.CANCELLED
        return TerminalCause.FAILED

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
        if self._phase in {_Phase.INTERRUPTED, *_ENDING}:
            await self._see_end()
            return
        pull = self._pull
        if self._phase is _Phase.PULLING and pull is None:
            raise RuntimeError("an inline stream read cannot be closed from another task")
        # Taken before the phase changes, so a failing budget hook cannot wedge the stream in CLOSING.
        deadline = self._cleanup_deadline()
        self._phase = _Phase.CLOSING
        if pull is not None and not pull.done():
            self._read.closed = True
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
        WORK_OWNERS.set((*WORK_OWNERS.get(), self._work_token))
        if self._isolated:
            OPERATION_LATCH.set(self._own_latch)

    def _latch(self) -> OperationLatch:
        """Return the operation's latch: this runner's own, or its outer runner's when it reads inline in it."""
        if self._isolated:
            return self._own_latch
        return OPERATION_LATCH.get() or self._own_latch

    def _interrupted(self) -> None:
        """React to the operation's first interrupt: the read in flight raises it, or, without one, the event loop.

        A read in flight is cancelled, unless the interrupt is ending it already, and raises it once the operation has
        ended. Without one, between reads, the interrupt goes to the event loop now, once (``OperationLatch.hand_on``):
        nothing waits on any later step to deliver it. The end (cleanup and publication) follows in a task of its own,
        unless a read or close begins it first (``_end_between_reads``). That task is started after the interrupt is
        handed on, so a loop the interrupt stops does so before that task's first step. Under ``asyncio.run`` the
        shutdown then cancels the task, and the end is run by a close the shutdown runs as it unwinds a task it
        cancelled (``async with stream``, the client's ``async with``): that cancellation has landed already, so the
        cleanup runs to its end and closes the source. A read that resumed ahead of the loop's raise and claimed the end
        sees its cleanup, which the shutdown cancels before it begins, started again (``_end``), past the shutdown's
        cancellations. A stream nothing closes then gets no end and no report.
        """
        pull = self._pull
        if self._phase is _Phase.PULLING and pull is not None and pull is not asyncio.current_task():
            pull.cancel()
        elif self._phase is _Phase.NEW:
            self._phase = _Phase.INTERRUPTED
            if (interrupt := self._own_latch.close()) is not None:
                self._own_latch.hand_on(interrupt)
            self._ending = spawn_settled(self._end_between_reads())

    async def _end_between_reads(self) -> None:
        """Run the end of an operation an interrupt ended between reads, unless somebody began it already.

        Its interrupt is out (``_interrupted``): the end raises none, whoever runs it. It cleans up and publishes the
        report in the interrupt's shape, attached to the interrupt as ``.report``; a cancellation that arrived while
        cleanup ran is raised, to a read or close that ran the end, and is recorded as cleanup's.
        """
        if self._phase is not _Phase.INTERRUPTED:
            return
        self._phase = _Phase.CLOSING
        report, attempt, _ = await self._end(TerminalCause.INTERRUPTED, None)
        if (interrupt := self._own_latch.first) is not None:
            self._hooks.attach(interrupt, report)
        if attempt.cancellation is not None:
            raise attempt.cancellation

    def _raise_latched(self) -> None:
        """The one raise point of the operation's escapes: close its latch and raise what it returns, with the report.

        Every end of the stream raises the interrupt, ahead of whatever else would leave: the read or close that ended
        it and a read a concurrent close ended. Without one, the first end raises the first undelivered escape, as
        from a ``finally``. Only the latch's owner closes it: a runner reading inline in its operation's read ends as
        it would otherwise, so the operation's report keeps what ended it first, and its owner raises the interrupt;
        it raises the undelivered escape, which its owner's end then raises as it propagates there. One handed to the
        event loop between reads never gets here: no read or close ends that operation (``_end_between_reads``). Once
        closed, the latch keeps nothing: an escape caught later goes to the event loop (``OperationLatch.record``).
        """
        latch = self._latch()
        escape = latch.close() if latch is self._own_latch else latch.pending()
        if escape is not None:
            if self._report is not None:
                self._hooks.attach(escape, self._report)
            raise escape

    async def _pull_next(self, read: _Read) -> T:
        # Run as a settled task: the reader raises the failure it ends in. The reader replays the cancellation it
        # handed over itself (``CancellationMark``), and a close that cancelled the read settles it.
        self._mark_own_work()
        _CURRENT_READ.set(read)
        return await anext(self._body)

    async def _clean_up(self, read: asyncio.Future[Settled[T]] | None) -> None:
        # Cleanup runs caller code too (a source's ``finally``, a transport), which must not close this runner.
        self._mark_own_work()
        read_failure = None
        if read is not None:
            # A cancelled read ends before the body closes under it, even after a deadline stopped the wait.
            read_failure = await _settle(cast("asyncio.Future[Settled[object]]", read), self._read)
        if self._phase is _Phase.CLOSING:
            # Unless its claimant gave up at its deadline and published the end before this cleanup got here.
            self._phase = _Phase.CLEANING
        try:
            close = getattr(self._body, "aclose", None)
            if close is not None:
                await close()
        finally:
            await self._hooks.cleanup()
        if read_failure is not None:
            raise read_failure

    async def _publish(self, cause: TerminalCause, reason: str | None, attempt: CleanupAttempt) -> tuple[R, bool]:
        """Finalize once under cancellation protection; return the report and whether finalize failed."""
        published: list[R] = []

        async def finalize() -> None:
            value = self._hooks.finalize(cause, reason, attempt)
            published.append(await value if inspect.isawaitable(value) else value)

        outcome = await await_cleanup_resistant(finalize())
        failed = outcome.error is not None and not published
        try:
            report = published[0] if published else self._hooks.failure_report(cause, reason or cause.value, attempt)
            if (interrupt := self.interrupt) is not None and self._hooks.interrupted is not None:
                # Whatever ended the stream, an interrupt is what leaves: the report has its one shape either way.
                report = self._hooks.interrupted(report, interrupt)
            self._report = self._own_latch.report = report
        finally:
            # Publication ends here on every path, a failing fallback's included: nobody waits for a report that
            # never comes.
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
        read: asyncio.Future[Settled[T]] | None = None,
        deadline: float | None = None,
    ) -> None:
        """End the operation (``_end``) and raise by the §3.1 rows."""
        report, attempt, finalize_failed = await self._end(
            cause, primary, pending=pending, read=read, deadline=deadline
        )
        # The operation's end: its escape is raised now, ahead of anything else.
        self._raise_latched()
        if cause is TerminalCause.FAILED and primary is not None:
            self._raise_failed(primary, report, attempt, finalize_failed=finalize_failed)
        if finalize_failed and self._finalize_error is not None and primary is None:
            # A cleanup failure behind it is in the fallback report it carries, not a note on it. The finalization
            # failure leaves in place of a cancellation cleanup absorbed, so it is replayed.
            rearm_cancellation(attempt.cancellation)
            raise self._finalize_error
        self._raise_terminal(cause, primary, report, attempt)

    async def _end(
        self,
        cause: TerminalCause,
        primary: BaseException | None,
        *,
        pending: asyncio.CancelledError | None = None,
        read: asyncio.Future[Settled[T]] | None = None,
        deadline: float | None = None,
    ) -> tuple[R, CleanupAttempt, bool]:
        """Settle a cancelled ``read``, clean up and publish once; return the report, cleanup and finalization outcome.

        A caller's cancellation, pending or arriving during cleanup, is deferred only until ``deadline``;
        cleanup still running then finishes in the background and is reported as failed. Only a cleanup that began
        settles the end (``_Phase.CLEANING``): one cancelled before it began, as a loop's shutdown cancels every task
        it finds, closed nothing, so it is started again. The returned cleanup outcome is what the end raises: the
        report records only a cancellation that arrived while cleanup ran, never the caller's own cancellation that
        came before (``pending``: absorbed by its read, or delivered before cleanup began), which the end raises as
        one cleanup deferred.
        """
        self._primary = primary
        # The operation is ending: an interrupt latched from now on wakes nothing, so it cancels no read, not the one
        # an inline kernel's cleanup runs in.
        self._latch().end()
        while True:
            cancelled = pending is not None or isinstance(primary, asyncio.CancelledError)
            self._cleanup_task = spawn_settled(self._clean_up(read))
            cleanup = await await_cleanup_resistant(self._cleanup_task, deadline=deadline, cancelled=cancelled)
            if self._phase is not _Phase.CLOSING or cleanup.unfinished is not None:
                break
            pending = pending or cleanup.cancellation
        # An escape is never a cleanup failure: its task latched it (``settle``), and it leaves as the end.
        error = None if is_escape(cleanup.error) else cleanup.error
        attempt = CleanupAttempt(error, cleanup.cancellation, finished=cleanup.unfinished is None)
        reason = report_reason(primary) if cause is TerminalCause.FAILED and primary is not None else None
        report, finalize_failed = await self._publish(cause, reason, attempt)
        if pending is not None and attempt.cancellation is None:
            attempt = CleanupAttempt(attempt.error, pending, finished=attempt.finished)
        return report, attempt, finalize_failed

    def _raise_failed(
        self,
        primary: BaseException,
        report: R,
        attempt: CleanupAttempt,
        *,
        finalize_failed: bool,
    ) -> NoReturn:
        """Raise a FAILED primary; a finalization failure annotates it.

        A cleanup failure behind it is no note: the report the raised error carries records it, in every family, as
        "stream cleanup also failed (X)".
        """
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
            raised = self._cleanup_failure = self._public_error(attempt.error, report)
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
    "closed_behind_its_end",
    "failed_kernel_report",
    "kernel_terminal",
    "with_cleanup_attempt",
]
