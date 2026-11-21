"""One owner of stream termination: classify, clean up, finalize once, publish, re-raise (spec §3.1).

A stream family supplies an algorithm body that only yields items and raises failures, plus hooks
that close its resources and build its report. The runner decides the terminal cause, runs cleanup
under cancellation protection, finalizes exactly once after cleanup, attaches the published report
to every raised exception and replays a cancellation that cleanup had to defer.
"""

from __future__ import annotations
import asyncio
import contextlib
import functools
import inspect
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum, StrEnum
from typing import TYPE_CHECKING, Any, NoReturn, Protocol, Self, cast

from b24api.contracts.policy import KernelState
from b24api.contracts.report import Violation, ViolationSeverity
from b24api.contracts.stream import PartialResult
from b24api.errors import StreamClosedError
from b24api.execution.context import (
    CANCEL_GRACE_SECONDS,
    OPERATION_LATCH,
    WORK_OWNERS,
    CancellationMark,
    OperationLatch,
    Settled,
    await_cleanup_resistant,
    grace_left,
    is_escape,
    is_interrupt,
    own_cancellation,
    rearm_cancellation,
    share_grace,
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
    # False when the claimant stopped waiting after the cancellation grace: cleanup goes on in the background and the
    # body never recorded its terminal, so the family reports from what it holds instead of the body's report.
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
    # The cleanup failure the body's source raised as its end (``OperationRunner.cleanup_failure`` of a kernel read
    # inline in the body): a read a close cancelled that ends in it failed by that close. A read that ends in any other
    # failure failed on its own, before the close reached it, and that failure ends the stream.
    source_cleanup_failure: Callable[[], BaseException | None] | None = None
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

    The reader's cancellation ended the stream before its close failed or overran its window. A close that overran
    the cancellation grace (``finished`` is false) overran it because a cancellation stopped the wait for it, so that
    cancellation came first too, whatever closed the stream, unless a failure or an interrupt ended it. Either way the
    cancellation is raised, the close failure a secondary violation ("stream cleanup also failed").
    """
    if attempt.error is None:
        return False
    return cause is TerminalCause.CANCELLED or (not attempt.finished and cause not in _FAILING)


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
    # Open, between reads.
    NEW = "new"
    # A read is in progress: from its start until its reader resumes. That includes the handoff, the loop turns between
    # the pull's end and the reader's resumption: an item is the reader's only once the reader resumes with it, so a
    # close that comes in the handoff ends the read as one that comes while the pull runs (``StreamClosedError``).
    PULLING = "pulling"
    # An interrupt ended the operation between reads and is out already; nobody has begun its end (cleanup and
    # publication) yet. Whoever comes first runs it: the task the interrupt started, a read or a close.
    INTERRUPTED = "interrupted"
    # Somebody (a read, a close, the task an interrupt started) has claimed the end; its cleanup has not begun.
    CLOSING = "closing"
    # The end's cleanup has begun closing the source. An end is published once its cleanup began, or once its claimant
    # gave up waiting for it after the cancellation grace; a cleanup cancelled before it began (a loop's shutdown
    # cancels every task it finds) closed nothing, so the claimant starts it again (``_end``). Whoever claimed an end
    # publishes it: a claimant whose coroutine is closed (``GeneratorExit``: it can await nothing more) hands the rest
    # of the end to a task of the runner's own (``_hand_over_end``), so every end that begins is published.
    CLEANING = "cleaning"
    CLOSED = "closed"


# An end some caller runs: a read or close that finds it waits for its publication.
_ENDING = frozenset({_Phase.CLOSING, _Phase.CLEANING})


_SELF_CLOSE = (
    "a stream cannot be closed from inside its own pull or cleanup (a page_stop callback, a command, "
    "binding or reference source, a transport, or a task one of them started): the close would wait for itself"
)


class _Read:
    """One isolated read; ``closed`` once the runner's close cancelled it; ``waiter`` is what its reader waits on.

    ``cancellation`` is the reader's own, once it caught one while it waited for its pull: only its task's cancellation
    cancels what the reader waits on, so it is the reader's whatever the task's cancellation count says (a replayed
    cancellation leaves the count as it was). A pull that then fails absorbs it, and the read's end defers it for what
    is left of its grace.
    """

    __slots__ = ("cancellation", "closed", "waiter")

    def __init__(self, waiter: _PullWaiter | None = None) -> None:
        self.closed = False
        self.waiter = waiter
        self.cancellation: asyncio.CancelledError | None = None


def _cancelled_reader(reader: CancellationMark, read: _Read | None) -> bool:
    """Return whether the reader of ``read`` was cancelled itself: after its mark, or while it waited for its pull."""
    return reader.requested() or (read is not None and read.cancellation is not None)


def _caught_by(read: _Read | None) -> asyncio.CancelledError | None:
    """Return the cancellation the reader of ``read`` caught while it waited for its pull."""
    return None if read is None else read.cancellation


class _PullWaiter(asyncio.Future[None]):
    """What a read waits on: done when its pull ends; cancelling it cancels the pull in that same step.

    The reader's cancellation, pending when the read starts or arriving while it waits, reaches its pull at once, as
    it reaches a task the reader awaits directly: a pull not yet started never starts. Unlike awaiting the pull
    itself, the reader is woken at once, so how long it then waits for the pull is its own (the grace). The pull marks
    its own end (``pull_ended``), so its reader resumes in the turn after it, as one awaiting the pull would; its
    done callback is the fallback for a pull that never ran.
    """

    def __init__(self) -> None:
        super().__init__()
        self._pull: asyncio.Future[Any] | None = None
        self.handed = False

    def watch(self, pull: asyncio.Future[Any]) -> None:
        """Wait on ``pull``: cancelling this cancels it, and its end ends this wait."""
        self._pull = pull
        pull.add_done_callback(self._pull_done)

    def pull_ended(self) -> None:
        """End the wait: the pull is returning, or has returned."""
        if not self.done():
            self.set_result(None)

    def _pull_done(self, _pull: asyncio.Future[Any]) -> None:
        self.pull_ended()

    def cancel(self, msg: object | None = None) -> bool:
        """Cancel the pull, then this wait; ``handed`` tells whether the pull, still running, took the cancellation."""
        if self._pull is not None and self._pull.cancel(msg):
            self.handed = True
        return super().cancel(msg)


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
        # The reads a close ended that wait for its end's publication (``_see_close_end``): an interrupt that end
        # raises is theirs to raise, even when the end was handed over (``_end_handed_over``).
        self._closed_reads = 0
        self._report: R | None = None
        self._primary: BaseException | None = None
        self._finalize_error: BaseException | None = None
        self._cleanup_failure: BaseException | None = None
        self._own_latch = OperationLatch(self._interrupted)
        self._work_token = object()
        # The one cleanup, kept even when its claimant stopped waiting for it, so it lives until it ends.
        self._cleanup_task: asyncio.Future[Settled[None]] | None = None
        # The one finalization, kept so a claimant that hands its end over mid-publication never runs it twice.
        self._finalize_task: asyncio.Future[Settled[R]] | None = None
        # The failure a read a close cancelled ended in on its own, before the close reached it: it ends the stream.
        self._read_failure: BaseException | None = None
        # The task that runs an end nobody awaits: an operation an interrupt ended between reads, or one whose
        # claimant's coroutine was closed. The event loop keeps only a weak reference to a task, so this one keeps it
        # from being collected before it ends: nothing else refers to it.
        self._ending: asyncio.Future[Settled[object]] | None = None
        # The event loop the stream runs in, from its first read or end: an end handed over runs there alone.
        self._loop: asyncio.AbstractEventLoop | None = None

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
    def background_cleanup(self) -> asyncio.Future[Settled[None]] | None:
        """Return the cleanup still running after a cancelled end stopped waiting for it at the grace, if any.

        Read once the end publishes its report, as its last step does, or after.
        """
        task = self._cleanup_task
        return task if task is not None and not task.done() else None

    async def _await_background_cleanup(self, cancelled: asyncio.CancelledError | None) -> None:
        """Wait for the cleanup a cancelled end left running (``background_cleanup``); the report records its failure.

        The cleanup's own work closing the stream again waits for nothing: that would be waiting for itself. A
        cancellation, held already (``cancelled``) or arriving, is deferred for what is left of its grace, as any
        close's is; one that arrives is then raised.
        """
        task = self.background_cleanup
        if task is None or self.inside_own_work:
            return
        cleanup = await await_cleanup_resistant(task, grace=self._cancel_grace(), cancelled=cancelled)
        if cleanup.cancellation is not None:
            raise cleanup.cancellation

    @property
    def inside_own_work(self) -> bool:
        """Whether a close requested from the current task could wait for that task itself.

        The task carries this runner's marker when it is the runner's pull or cleanup or was started (by caller code
        too) while one ran. Such work is the stream's while the stream works: a body that works between reads works as
        long as the stream is open; any other from a read's start until its reader resumes (``_Phase.PULLING``, the
        handoff included) and while its end runs. The rule reads the phase alone, never whether a task happens to be
        running at that moment, so a close from the stream's own work is refused whatever turn of the loop it comes in.
        Once the stream is closed, its work is the cleanup a cancelled end left running (``background_cleanup``), until
        that cleanup ends.
        """
        if not any(token is self._work_token for token in WORK_OWNERS.get()):
            return False
        if self._phase is _Phase.CLOSED:
            return self.background_cleanup is not None
        return self._works_between_reads or self._phase is not _Phase.NEW

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
        # A body its task's cancellation ended (Ctrl-C under ``asyncio.run``) exits through a cancelled close.
        cancelled = own_cancellation(exc)
        await close_on_exit(
            functools.partial(self._close, cancelled), exc, note=lambda _failure: self._failed_close_record()
        )

    def _failed_close_record(self) -> str | None:
        """Return the published report's record of the failed close."""
        violations = self._report.violations if self._report is not None else ()
        return next((violation.message for violation in violations if violation.code == "cleanup_failure"), None)

    async def __anext__(self) -> T:
        """Read one item; the read that ends the body publishes the report and raises by §3.1."""
        self._bind_loop()
        await self._stop_if_ended()
        if self._phase is _Phase.PULLING:
            raise RuntimeError("concurrent stream pull")
        self._phase = _Phase.PULLING
        reader = CancellationMark.now()
        pull = None
        read = None
        try:
            if self._isolated:
                waiter = _PullWaiter()
                read = self._read = _Read(waiter)
                pull = self._pull = spawn_settled(self._pull_next(read))
                waiter.watch(pull)
                item = await self._await_pull(pull, read, waiter)
            else:
                item = await anext(self._body)
        except GeneratorExit:
            # The reader's coroutine is being closed (by hand, or collected with its task): it can await nothing more.
            self._abandon_read()
            raise
        except BaseException as error:
            # Where an inline kernel first catches an escape its read raised (its send runs in the read): its end
            # raises what the kernel ends with, a failed cleanup say, so the latch is what keeps it.
            self._latch().record(error)
            self._pull = None
            # A pull its cancelled reader stopped waiting for: the end settles it before it closes the body.
            running = pull if pull is not None and not pull.done() else None
            if self._phase in _ENDING:
                # A concurrent aclose() owns termination; this read reports the published report.
                await self._raise_closed_read(error, reader, read)
            self._phase = _Phase.CLOSING
            cause = self._cause_of(error)
            # A pull task that ends in its own failure absorbs the reader's cancellation it was handed: the one the
            # reader caught keeps its grace (``_await_pull``).
            absorbed = None
            if _cancelled_reader(reader, read) and not isinstance(error, asyncio.CancelledError):
                caught = _caught_by(read)
                absorbed = asyncio.CancelledError() if caught is None else caught
            await self._terminate(
                cause,
                error if cause in {TerminalCause.CANCELLED, TerminalCause.FAILED} else None,
                pending=absorbed,
                read=running,
                # A pull that outlived the cancellation grace spent it: its end does not wait for it a second time.
                grace=None if running is None else 0.0,
            )
            raise
        self._pull = None
        if self._phase is not _Phase.PULLING:
            # A close claimed the end while this read was in progress, in its handoff (its item made, its reader not
            # resumed yet): the item is not the reader's, and the read ends as the close's cancellation would end it,
            # or as its reader's own cancellation does, if it caught one (``_await_pull``), within what is left of its
            # grace.
            arrived = await self._see_close_end(_caught_by(read))
            if arrived is not None:
                raise arrived
            raise self._stream_closed()
        self._phase = _Phase.NEW
        await self._end_if_interrupted()
        if self._hooks.delivered is not None:
            self._hooks.delivered()
        return item

    async def _await_pull(self, pull: asyncio.Future[Settled[T]], read: _Read, waiter: _PullWaiter) -> T:
        """Return the read's item, or raise how its pull ended; a cancelled reader waits for it only the grace.

        The pull is cancelled with its reader. One that ends within the grace ends the read as it ended (its failure,
        a cancellation, or even its item, as awaiting it would). One still running then (a synchronous source blocked
        in its worker thread, say) is left to the end, which settles it before it closes the body. A cancellation that
        comes once the pull has ended (in the handoff, its reader not resumed yet) reaches no pull: the read raises it,
        as awaiting the ended pull would.
        """
        waiting = waiter
        try:
            await waiting
            if not pull.done():
                # The pull marked its end, then waited a turn more (``settle``): its done callback ends this wait.
                waiting = _PullWaiter()
                waiting.watch(pull)
                await waiting
        except asyncio.CancelledError as error:
            read.cancellation = error
            if not waiting.handed:
                raise
            # A second cancellation that arrives meanwhile ends with this one's grace, and is raised unless the read
            # raises a cancellation or a failure already.
            again: asyncio.CancelledError | None = None
            while not pull.done() and (left := grace_left(error, CANCEL_GRACE_SECONDS)) > 0:
                try:
                    # The grace is the cancellation's, shared with every wait it unwinds after this one.
                    await asyncio.wait((pull,), timeout=left)
                except asyncio.CancelledError as arrived:
                    share_grace(error, arrived)
                    again = arrived
            if not pull.done():
                raise
            try:
                item = pull.result().unwrap()
            except asyncio.CancelledError as ended:
                # The pull ended in the reader's cancellation: what it raises keeps that cancellation's grace.
                share_grace(error, ended)
                raise
            if again is not None:
                raise again from None
            return item
        # A cancelled pull raises the cancellation it ended in, with what it carries (a failed close as its cause).
        return pull.result().unwrap()

    async def _stop_if_ended(self) -> None:
        """Raise ``StopAsyncIteration`` for a read of a stream that ended.

        A read that finds the operation ending sees its end over first: it runs the end an interrupt left between
        reads that nobody began (``_end_between_reads``), or waits for the one another caller runs. Only a read from
        the stream's own work cannot, since that end waits for it.
        """
        if not self.inside_own_work and (arrived := await self._see_end(None)) is not None:
            raise arrived
        if self._phase in {_Phase.INTERRUPTED, *_ENDING, _Phase.CLOSED}:
            raise StopAsyncIteration

    async def _see_end(self, cancelled: asyncio.CancelledError | None) -> asyncio.CancelledError | None:
        """See an end in progress over: run the one nobody began, or wait for the one another caller runs.

        Either is a wait for cleanup, as a close's own is: a cancellation, held already (``cancelled``) or arriving,
        waits for it only what is left of its grace, and is returned for the caller to raise. Without one, the end is
        seen to its publication.
        """
        if self._phase is _Phase.INTERRUPTED:
            await self._end_between_reads(cancelled)
            return None
        if self._phase in _ENDING:
            published = await await_cleanup_resistant(
                self._published.wait(), grace=self._cancel_grace(), cancelled=cancelled
            )
            return published.cancellation or cancelled
        return cancelled

    async def _see_close_end(self, cancelled: asyncio.CancelledError | None) -> asyncio.CancelledError | None:
        """See the end a close runs over (``_see_end``) as a read it ended does, then raise its interrupt, if any.

        Once the end is published, the read raises the interrupt (``_raise_latched``), which every end raises, in place
        of what it would raise otherwise; a reader whose grace ran out before that leaves without it. It raises no
        other escape: that is the close's to raise, or the event loop's when the close can raise nothing
        (``_end_handed_over``). While it waits it is counted, so an end handed over leaves the interrupt to it.
        """
        self._closed_reads += 1
        try:
            arrived = await self._see_end(cancelled)
        finally:
            self._closed_reads -= 1
        if self._published.is_set() and self._own_latch.first is not None:
            self._raise_latched()
        return arrived

    async def _end_if_interrupted(self) -> None:
        """End the stream with this read when an interrupt was latched while it ran, though the read returned an item.

        The read the interrupt woke had its item already (the wake's cancellation came too late), or the read's own
        task latched it and went on: the read raises the interrupt instead of delivering the item.
        """
        if self._own_latch.first is not None:
            self._phase = _Phase.CLOSING
            await self._terminate(TerminalCause.INTERRUPTED, None)

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

    async def _raise_closed_read(self, error: BaseException, reader: CancellationMark, read: _Read | None) -> NoReturn:
        """Raise what a read a concurrent close ended raises (``_closed_read_error``), once the end is published.

        A reader whose own cancellation ended the read waits for that end only what is left of its grace, as a cancelled
        close does; so does one cancelled while it waits, which then raises that cancellation. A reader whose
        cancellation its pull absorbed by failing (``_await_pull``) waits the same, and the cancellation is replayed
        behind the failure the read raises, as a read no close ended replays it. One its pull absorbed by escaping (an
        interrupt included) waits the same too, and the read raises that cancellation itself, once: an escape is the
        close's to raise, even when the close's cleanup latched it before the end was published, or the event loop's
        when the close can raise nothing (``_end_handed_over``). An interrupt, which every end raises
        (``OperationLatch.close``), a read raises once the end is published, in place of whatever else it would raise,
        its reader's cancellation included (``_see_close_end``): a cancelled reader whose grace runs out before that
        leaves with its cancellation, and the close raises the interrupt.
        """
        if isinstance(error, asyncio.CancelledError):
            held = error if _cancelled_reader(reader, read) else None
        else:
            held = _caught_by(read)
        arrived = await self._see_close_end(held)
        if arrived is not None and arrived is not held:
            raise arrived
        closed = self._closed_read_error(error, reader, read)
        if held is not None and closed is not held:
            rearm_cancellation(held)
        if closed is error:
            raise error
        if isinstance(closed, StreamClosedError | asyncio.CancelledError):
            raise closed from None
        raise closed from _carried_cause(closed, error)

    def _closed_read_error(self, error: BaseException, reader: CancellationMark, read: _Read | None) -> BaseException:
        """Return what a read a concurrent close ended raises.

        The close's cancellation of the read ended it: ``StreamClosedError``, or the reader's own cancellation when its
        task was cancelled too. An escape its pull ended in (the close's to raise; an interrupt the published end's,
        which ``_see_close_end`` raises before this) ends it the same: as the cancellation its reader caught while
        it waited for the pull, if any. A failure the read ended in (its own, which ended the
        stream, or the close's) leaves as the stream's failures do (``_public_error``).
        """
        if isinstance(error, asyncio.CancelledError):
            return error if _cancelled_reader(reader, read) else self._stream_closed()
        if is_escape(error):
            return _caught_by(read) or self._stream_closed()
        report = self._report
        if report is None:
            return error
        return self._public_error(error, report)

    def _stream_closed(self) -> StreamClosedError:
        """Return the error of a read a concurrent close ended, carrying the published report."""
        error = StreamClosedError()
        if self._report is not None:
            self._hooks.attach(error, self._report)
        return error

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
        await self._close(None)

    async def _close(self, cancelled: asyncio.CancelledError | None) -> None:
        """Close as ``aclose`` does; ``cancelled``: the cancellation the close's caller holds already.

        A close whose caller holds a cancellation (an ``async with`` body that cancellation ended) waits for cleanup
        only the cancellation grace, as one cancelled while it waits does. That cancellation is its caller's, which
        raises it (``close_on_exit``): the close neither raises it nor replays it behind a failure, so it is delivered
        once.
        """
        if self._phase is _Phase.CLOSED:
            await self._await_background_cleanup(cancelled)
            return
        if self.inside_own_work:
            raise RuntimeError(_SELF_CLOSE)
        if self._phase in {_Phase.INTERRUPTED, *_ENDING}:
            # A close that joins the end waits for it as for its own: the cleanup left running included.
            if (arrived := await self._see_end(cancelled)) is not None:
                raise arrived
            await self._await_background_cleanup(None)
            return
        pull = self._pull
        if self._phase is _Phase.PULLING and pull is None:
            raise RuntimeError("an inline stream read cannot be closed from another task")
        self._phase = _Phase.CLOSING
        if pull is not None:
            # The read in progress is the close's to end, in its handoff too: its reader never gets its item.
            self._read.closed = True
            if not pull.done():
                pull.cancel()
        cause, primary, report, attempt, finalize_failed = await self._end(
            TerminalCause.EARLY_CLOSED, None, pending=cancelled, read=pull
        )
        if cancelled is not None and attempt.cancellation is cancelled:
            # The caller's own cancellation: the caller raises it.
            attempt = CleanupAttempt(attempt.error, None, finished=attempt.finished)
        self._raise_close_end(cause, primary, report, attempt, finalize_failed=finalize_failed)

    def _raise_close_end(
        self,
        cause: TerminalCause,
        primary: BaseException | None,
        report: R,
        attempt: CleanupAttempt,
        *,
        finalize_failed: bool,
    ) -> None:
        """Raise what the end a close ran raises; a read that failed on its own before the close leaves it quiet."""
        if cause is TerminalCause.FAILED:
            # The read failed on its own before the close ended it: that failure ends the stream and its reader
            # raises it, so the close returns quietly.
            self._raise_latched()
            if attempt.cancellation is not None:
                raise attempt.cancellation
            return
        self._raise_end(cause, primary, report, attempt, finalize_failed=finalize_failed)

    def _abandon_read(self) -> None:
        """End the stream for a reader whose coroutine is closed in its read, as a close from another task would.

        Nothing of that coroutine can await, so the end runs in a task of the runner's own (``_hand_over_end``).
        A close that claimed the end first runs it already.
        """
        pull, self._pull = self._pull, None
        if self._phase is not _Phase.PULLING:
            return
        self._phase = _Phase.CLOSING
        if pull is not None:
            self._read.closed = True
            if not pull.done():
                pull.cancel()
        self._hand_over_end(TerminalCause.EARLY_CLOSED, None, pull)

    async def first(self) -> PartialResult[tuple[T, ...]]:
        """Consume zero or one item, then close without an extra proof read."""
        values: list[T] = []
        await self._consume(values, limit=1)
        return PartialResult(tuple(values), cast("OperationReport", self._report))

    async def collect(self, *, limit: int) -> PartialResult[list[T]]:
        """Consume up to a positive bound, then close without an extra proof read."""
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("limit must be a positive integer")
        values: list[T] = []
        await self._consume(values, limit=limit)
        return PartialResult(values, cast("OperationReport", self._report))

    async def _consume(self, values: list[T], *, limit: int) -> None:
        """Read up to ``limit`` items into ``values``, then close.

        What the stream delivered before a read or the close raised is not lost with the return value: the error
        carries it as ``partial_values``. The close is the ``async with`` exit's, so a cancellation that ends the reads
        waits for cleanup only its grace.
        """
        try:
            async with self:
                with contextlib.suppress(StopAsyncIteration):
                    while len(values) < limit:
                        values.append(await anext(self))
        except BaseException as error:
            attach_partial_values(error, tuple(values))
            raise

    def _cancel_grace(self) -> float | None:
        """How long the end defers a caller's cancellation: the grace; to its end for a runner an outer one drives."""
        return CANCEL_GRACE_SECONDS if self._isolated else None

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
        ended. Without one, between reads, the interrupt goes to the event loop now, once
        (``OperationLatch.close_to_loop``): nothing waits on any later step to deliver it. The end (cleanup and
        publication) follows in a task of its own, unless a read or close begins it first (``_end_between_reads``). That
        task is started after the interrupt is handed on, so a loop the interrupt stops does so before that task's first
        step. Under ``asyncio.run`` the shutdown then cancels the task, and the end is run by a close the shutdown runs
        as it unwinds a task it cancelled (``async with stream``, the client's ``async with``): that cancellation has
        landed already, so the cleanup runs to its end and closes the source. A read that resumed ahead of the loop's
        raise and claimed the end sees its cleanup, which the shutdown cancels before it begins, started again
        (``_end``), past the shutdown's cancellations. A stream nothing closes then gets no end and no report.
        """
        pull = self._pull
        if self._phase is _Phase.PULLING and pull is not None and pull is not asyncio.current_task():
            pull.cancel()
        elif self._phase is _Phase.NEW:
            self._phase = _Phase.INTERRUPTED
            self._own_latch.close_to_loop()
            self._ending = spawn_settled(self._end_between_reads(None))

    async def _end_between_reads(self, cancelled: asyncio.CancelledError | None) -> None:
        """Run the end of an operation an interrupt ended between reads, unless somebody began it already.

        Its interrupt is out (``_interrupted``): the end raises none, whoever runs it. It cleans up and publishes the
        report in the interrupt's shape, attached to the interrupt as ``.report``; a cancellation that arrived while
        cleanup ran is raised, to a read or close that ran the end, and is recorded as cleanup's; so is one the close
        that runs it holds already (``cancelled``), whose grace runs now.
        """
        if self._phase is not _Phase.INTERRUPTED:
            return
        self._phase = _Phase.CLOSING
        _, _, report, attempt, _ = await self._end(TerminalCause.INTERRUPTED, None, pending=cancelled)
        if (interrupt := self._own_latch.first) is not None:
            self._hooks.attach(interrupt, report)
        if attempt.cancellation is not None:
            raise attempt.cancellation

    def _raise_latched(self) -> None:
        """The one raise point of the operation's escapes: close its latch and raise what it returns, with the report.

        Every end of the stream raises the interrupt, ahead of whatever else would leave: the read or close that ended
        it and a read a concurrent close ended. Without one, the first end raises the first undelivered escape, as
        from a ``finally``. Only the latch's owner raises: a runner reading inline in its operation's read ends as it
        would otherwise, so the operation's report keeps what ended it first, and its owner raises the escape. One
        handed to the event loop between reads never gets here: no read or close ends that operation
        (``_end_between_reads``), nor does one of an end whose claimant can raise nothing (``_end_handed_over``),
        unless a read the close ended raises its interrupt. Once closed, the latch keeps nothing: an escape caught
        later goes to the event loop (``OperationLatch.record``).
        """
        latch = self._latch()
        if latch is self._own_latch and (escape := latch.close()) is not None:
            if self._report is not None:
                self._hooks.attach(escape, self._report)
            raise escape

    async def _pull_next(self, read: _Read) -> T:
        # Run as a settled task: the reader raises the failure it ends in. The reader replays the cancellation it
        # handed over itself (``CancellationMark``), and a close that cancelled the read settles it.
        self._mark_own_work()
        _CURRENT_READ.set(read)
        try:
            return await anext(self._body)
        finally:
            if read.waiter is not None:
                read.waiter.pull_ended()

    async def _clean_up(self, read: asyncio.Future[Settled[T]] | None) -> None:
        # Cleanup runs caller code too (a source's ``finally``, a transport), which must not close this runner.
        self._mark_own_work()
        close_failure = None
        if read is not None:
            # A cancelled read ends before the body closes under it, even after a claimant stopped waiting.
            read_failure = await _settle(cast("asyncio.Future[Settled[object]]", read), self._read)
            if read_failure is not None and (is_escape(read_failure) or self._failed_by_close(read_failure)):
                close_failure = read_failure
            else:
                self._read_failure = read_failure
        if self._phase is _Phase.CLOSING:
            # Unless its claimant gave up after the grace and published the end before this cleanup got here.
            self._phase = _Phase.CLEANING
        try:
            close = getattr(self._body, "aclose", None)
            if close is not None:
                await close()
        finally:
            await self._hooks.cleanup()
        if close_failure is not None:
            raise close_failure

    def _failed_by_close(self, failure: BaseException) -> bool:
        """Whether a read the close cancelled ended in that close's failure: the failed close of a kernel read inline.

        Any other failure the read ended in came before the close reached it: the read failed on its own.
        """
        source_failure = self._hooks.source_cleanup_failure
        return source_failure is not None and failure is source_failure()

    async def _finalized(self, cause: TerminalCause, reason: str | None, attempt: CleanupAttempt) -> R:
        value = self._hooks.finalize(cause, reason, attempt)
        return await value if inspect.isawaitable(value) else value

    async def _publish(self, cause: TerminalCause, reason: str | None, attempt: CleanupAttempt) -> tuple[R, bool]:
        """Finalize once under cancellation protection; return the report and whether finalize failed."""
        if self._finalize_task is None:
            self._finalize_task = spawn_settled(self._finalized(cause, reason, attempt))
        outcome = await await_cleanup_resistant(self._finalize_task)
        failed = outcome.error is not None
        try:
            report = (
                self._hooks.failure_report(cause, reason or cause.value, attempt)
                if failed
                else cast("R", self._finalize_task.result().value)
            )
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
        # Finalization waited with no grace: the cancellation it absorbed has none to carry.
        rearm_cancellation(outcome.cancellation)
        return report, failed

    async def _terminate(
        self,
        cause: TerminalCause,
        primary: BaseException | None,
        *,
        pending: asyncio.CancelledError | None = None,
        read: asyncio.Future[Settled[T]] | None = None,
        grace: float | None = None,
    ) -> None:
        """End the operation (``_end``) and raise by the §3.1 rows (``_raise_end``)."""
        cause, primary, report, attempt, finalize_failed = await self._end(
            cause, primary, pending=pending, read=read, grace=grace
        )
        self._raise_end(cause, primary, report, attempt, finalize_failed=finalize_failed)

    def _raise_end(
        self,
        cause: TerminalCause,
        primary: BaseException | None,
        report: R,
        attempt: CleanupAttempt,
        *,
        finalize_failed: bool,
    ) -> None:
        """Raise what the operation's end raises by the §3.1 rows."""
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
        grace: float | None = None,
    ) -> tuple[TerminalCause, BaseException | None, R, CleanupAttempt, bool]:
        """Settle a cancelled ``read``, clean up and publish once; return the end's cause, primary, report and outcomes.

        A caller's cancellation, pending or arriving during cleanup, is deferred only for the cancellation grace
        (``grace`` overrides it: none is left when the read spent it already); cleanup still running then finishes
        in the background and is reported as failed. Only a cleanup that began
        settles the end (``_Phase.CLEANING``): one cancelled before it began, as a loop's shutdown cancels every task
        it finds, closed nothing, so it is started again. The returned cleanup outcome is what the end raises: the
        report records only a cancellation that arrived while cleanup ran, never the caller's own cancellation that
        came before (``pending``: absorbed by its read, or delivered before cleanup began), which the end raises as
        one cleanup deferred. A read a close cancelled that had failed on its own turns the close into that failure's
        end (``FAILED``). A claimant whose coroutine is closed meanwhile hands the rest over (``_hand_over_end``).
        """
        self._bind_loop()
        self._primary = primary
        # The operation is ending: an interrupt latched from now on wakes nothing, so it cancels no read, not the one
        # an inline kernel's cleanup runs in.
        self._latch().end()
        try:
            while True:
                cancelled = pending or (primary if isinstance(primary, asyncio.CancelledError) else None)
                if self._cleanup_task is None or (self._cleanup_task.done() and self._phase is _Phase.CLOSING):
                    self._cleanup_task = spawn_settled(self._clean_up(read))
                cleanup = await await_cleanup_resistant(
                    self._cleanup_task, grace=self._cancel_grace() if grace is None else grace, cancelled=cancelled
                )
                if self._phase is not _Phase.CLOSING or cleanup.unfinished is not None:
                    break
                pending = pending or cleanup.cancellation
            # An escape is never a cleanup failure: its task latched it (``settle``), and it leaves as the end.
            error = None if is_escape(cleanup.error) else cleanup.error
            attempt = CleanupAttempt(error, cleanup.cancellation, finished=cleanup.unfinished is None)
            if cause is TerminalCause.EARLY_CLOSED and self._read_failure is not None:
                cause, primary = TerminalCause.FAILED, self._read_failure
                self._primary = primary
            reason = report_reason(primary) if cause is TerminalCause.FAILED and primary is not None else None
            report, finalize_failed = await self._publish(cause, reason, self._recorded(attempt))
        except GeneratorExit:
            self._hand_over_end(cause, primary, read)
            raise
        if pending is not None and attempt.cancellation is None:
            attempt = CleanupAttempt(attempt.error, pending, finished=attempt.finished)
        return cause, primary, report, attempt, finalize_failed

    def _recorded(self, attempt: CleanupAttempt) -> CleanupAttempt:
        """Return the attempt the report records: an inline end records no cancellation that is its owner's close.

        A runner an outer one drives waits for its cleanup to the end, so the close of the outer stream, which cancels
        the read this end runs in (``_closed_by_owner``), cut nothing here: the cleanup it waited for did not fail. A
        cancellation of the outer stream's caller is recorded as any cancellation that arrived while cleanup ran. The
        attempt itself still carries the cancellation, and the end raises it.
        """
        cancellation = attempt.cancellation
        if self._isolated or cancellation is None or not attempt.finished or not _closed_by_owner(cancellation):
            return attempt
        return CleanupAttempt(attempt.error, None, finished=True)

    def _hand_over_end(
        self, cause: TerminalCause, primary: BaseException | None, read: asyncio.Future[Settled[T]] | None
    ) -> None:
        """Run an end whose claimant can await no more (its coroutine is closed) to its publication in a task.

        The task joins the cleanup and the finalization already running, never starting a second of either. Only the
        event loop that ran the stream runs it. Without it running nothing can: the loop is gone, and a stream it left
        that is collected inside another loop gets no task there, which that loop's shutdown would wait on for work
        that is not its own.
        """
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            return
        if running is self._loop:
            self._ending = spawn_settled(self._end_handed_over(cause, primary, read))

    async def _end_handed_over(
        self, cause: TerminalCause, primary: BaseException | None, read: asyncio.Future[Settled[T]] | None
    ) -> None:
        """Run an end handed over (``_hand_over_end``) to its publication; what it raises leaves the event loop.

        Its claimant can raise nothing, so its escape goes where an end with no read or close to raise to sends it
        (``OperationLatch.close_to_loop``): the interrupt is raised out of the loop, unless a read the close ended
        waits for the end and raises it (``_see_close_end``); any other escape goes to the loop's exception handler.
        Only the latch's owner delivers, as only it raises (``_raise_latched``).
        """
        await self._end(cause, primary, read=read)
        if self._latch() is self._own_latch:
            self._own_latch.close_to_loop(raised_by_read=self._closed_reads > 0)

    def _bind_loop(self) -> None:
        """Record the event loop the stream runs in, as its first read or end begins."""
        if self._loop is None:
            self._loop = asyncio.get_running_loop()

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
            # A primary cancellation would have outranked the failure (``cancellation_outranks``): only one the
            # cleanup absorbed is replayed.
            rearm_cancellation(attempt.cancellation)
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


async def close_on_exit(
    close: Callable[[], Awaitable[None]],
    primary: BaseException | None,
    *,
    note: Callable[[Exception], str | None],
) -> None:
    """Close on the exit of an ``async with`` by §3.1: the body's own exception stays what leaves.

    The one rule for every owner closed on exit, a stream or the client. A cancellation the close absorbed lands on
    the next await (``rearm_cancellation``) behind the body's exception, which a structured owner of the cancellation
    (``asyncio.timeout``, ``TaskGroup``) then sees as it would without the close. The replay is a new delivery: an
    outer exit it reaches defers it for a grace of its own. A failed close is noted on the body's
    exception (``note``) instead of replacing it. Anything else the close raises (an escape) replaces it, as from a
    ``finally``.
    """
    try:
        await close()
    except asyncio.CancelledError as cancellation:
        if primary is None or isinstance(primary, asyncio.CancelledError):
            raise
        rearm_cancellation(cancellation)
    except Exception as failure:
        if primary is None:
            raise
        if (text := note(failure)) is not None:
            primary.add_note(text)


def attach_partial_values(error: BaseException, values: tuple[object, ...]) -> None:
    """Attach what a bounded consumer (``first``, ``collect``) read before ``error`` ended it, as ``partial_values``."""
    with contextlib.suppress(AttributeError, TypeError):
        error.partial_values = values  # type: ignore[attr-defined]


__all__ = [
    "CleanupAttempt",
    "LifecycleHooks",
    "OperationRunner",
    "TerminalCause",
    "attach_partial_values",
    "cancellation_outranks",
    "cleanup_failed",
    "close_on_exit",
    "closed_behind_its_end",
    "failed_kernel_report",
    "kernel_terminal",
    "with_cleanup_attempt",
]
