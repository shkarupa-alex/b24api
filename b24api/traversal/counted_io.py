"""Counted head/tail I/O seams shared by the flat counted stream and the reference queue.

The counted head transaction and the dense tail validation live in ``counted_batch.py`` and are
identical for both callers. Only the physical dispatch, the decoded-row accounting and the
completion recorder differ: the flat stream sends its head directly and its tail through a private
``_BatchOutcomeStream`` and owns the whole operation row counter, while a reference binding sends
both through the shared batch dispatcher and charges its rows to the shared row pool.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from b24api.batch.engine import BatchExecutor
from b24api.batch.outcome import BatchFailure, BatchSuccess
from b24api.batch.stream import _BatchOutcomeStream
from b24api.contracts.completion import CommandSettlement
from b24api.contracts.report import PageDispatch
from b24api.errors import B24ApiError, CapabilityError, IncompleteTraversalError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from b24api.batch.engine import _BatchInput
    from b24api.batch.outcome import BatchOutcome
    from b24api.completion.recorder import CountedCompletionRecorder
    from b24api.contracts.policy import ExecutionPolicy
    from b24api.contracts.request import Request
    from b24api.contracts.response import Response
    from b24api.execution import ExecutionContext
    from b24api.execution.snapshot import KernelReport
    from b24api.traversal.strategy_context import StrategyContext


@dataclass(frozen=True, slots=True)
class HeadAttempt:
    """One dispatched counted head with the dispatch facts its settlement must use."""

    response: Response | None = field(repr=False)
    error: BaseException | None = field(repr=False)
    dispatch: PageDispatch
    batch_index: int | None
    dispatch_started: bool
    settlement: CommandSettlement


@dataclass(frozen=True, slots=True)
class TailReceipt:
    """One dense tail command outcome, yielded in planned offset order."""

    ordinal: int
    outcome: BatchOutcome = field(repr=False)
    batch_index: int | None


class CountedRowAccount(Protocol):
    """Decoded head rows held between selection and delivery."""

    async def hold(self, rows: int) -> None:
        """Charge the selected head rows before the head decision."""
        ...

    async def release_held(self) -> None:
        """Release the held head rows exactly once, after delivery or rejection."""
        ...


class CountedTailStream(Protocol):
    """Ordered dense tail receipts and their cleanup."""

    @property
    def report(self) -> KernelReport | None:
        """Return the private tail kernel report, when the tail has one."""
        ...

    def __aiter__(self) -> AsyncIterator[TailReceipt]:
        """Iterate receipts in planned offset order."""
        ...

    def settle(self, receipt: TailReceipt) -> None:
        """Record the receipt's settlement and focus the completion recorder on its page."""
        ...

    def failure(self, receipt: TailReceipt, outcome: BatchFailure) -> BaseException:
        """Return the error a failed tail command raises."""
        ...

    async def aclose(self) -> None:
        """Release unsent and undelivered tail pages exactly once."""
        ...


class CountedWaveIO(Protocol):
    """Physical seams of one counted traversal."""

    @property
    def head_dispatch(self) -> PageDispatch:
        """Return the planned head dispatch provenance."""
        ...

    @property
    def rows(self) -> CountedRowAccount:
        """Return the decoded head row account."""
        ...

    @property
    def allows_sequential(self) -> bool:
        """Return whether a non-dense qualified head may continue sequentially."""
        ...

    @property
    def completed_reason(self) -> str:
        """Return the terminal reason a dense traversal records when it reaches its total."""
        ...

    async def execute_head(self, request: Request, *, context: ExecutionContext) -> HeadAttempt:
        """Open the head page in the completion recorder and dispatch it; a failure is raised."""
        ...

    def head_failure(self, error: BaseException) -> HeadAttempt:
        """Return the dispatch facts of a head whose dispatch raised ``error``."""
        ...

    async def tail_batch_size(
        self,
        ctx: StrategyContext,
        *,
        total: int,
        stride: int,
        head_rows: int,
        batch_size: int,
    ) -> int:
        """Refuse a tail the budgets cannot hold and return its physical batch size."""
        ...

    def open_tail(
        self,
        requests: Iterator[_BatchInput],
        *,
        batch_size: int,
        policy: ExecutionPolicy,
        context: ExecutionContext,
    ) -> CountedTailStream:
        """Start the dense tail over lazily planned page requests."""
        ...


class FlatCountedRowAccount:
    """The flat stream's whole-operation row counter, set and cleared as before."""

    def __init__(self, context: ExecutionContext) -> None:
        """Hold the operation context whose counter this stream owns alone."""
        self._context = context

    async def hold(self, rows: int) -> None:
        """Record the selected head rows as the operation's buffered rows."""
        await self._context.set_buffered_rows(rows)

    async def release_held(self) -> None:
        """Clear the operation's buffered rows."""
        await self._context.set_buffered_rows(0)


class _FlatTailStream:
    """Adapt the private batch outcome stream to ordered tail receipts."""

    def __init__(self, outcomes: _BatchOutcomeStream, recorder: CountedCompletionRecorder | None) -> None:
        self._outcomes = outcomes
        self._recorder = recorder

    @property
    def report(self) -> KernelReport:
        return self._outcomes.report

    async def __aiter__(self) -> AsyncIterator[TailReceipt]:
        async for outcome in self._outcomes:
            if not isinstance(outcome, BatchFailure) and (
                not isinstance(outcome, BatchSuccess) or outcome.response is None
            ):
                raise CapabilityError("parallel counted batch outcome lacks correlated response evidence")
            yield TailReceipt(outcome.command_index, outcome, outcome.command_index)

    def settle(self, receipt: TailReceipt) -> None:
        if self._recorder is not None:
            self._recorder.activate(receipt.ordinal + 1)
            self._recorder.settled(
                CommandSettlement.SUCCESS if isinstance(receipt.outcome, BatchSuccess) else CommandSettlement.FAILURE
            )

    def failure(self, receipt: TailReceipt, outcome: BatchFailure) -> BaseException:
        del receipt
        error = outcome.error
        if isinstance(error, B24ApiError):
            failure = IncompleteTraversalError(
                report=self._outcomes.report,
                error=error,
                replay_disposition=outcome.replay_disposition,
            )
            failure.__cause__ = error
            failure.__suppress_context__ = True
            return failure
        return error if isinstance(error, BaseException) else CapabilityError("parallel counted batch command failed")

    async def aclose(self) -> None:
        await self._outcomes.aclose()


class DirectCountedWaveIO:
    """Flat counted I/O: a direct head and a privately batched tail (unchanged wire and counters)."""

    head_dispatch = PageDispatch.DIRECT
    allows_sequential = False
    completed_reason = "parallel counted traversal completed"

    def __init__(self, ctx: StrategyContext, recorder: CountedCompletionRecorder | None) -> None:
        """Bind the traversal context and its flat counted recorder."""
        self._ctx = ctx
        self._recorder = recorder
        self.rows = FlatCountedRowAccount(ctx.context)

    async def execute_head(self, request: Request, *, context: ExecutionContext) -> HeadAttempt:
        """Open the head page in the flat recorder and send it directly."""
        recorder = self._recorder
        if recorder is not None:
            recorder.activate(recorder.reserve())
        head_reservation = None
        try:
            head_reservation = await context.reserve_page()
            head = await self._ctx.executor.execute(request, context=context)
        except BaseException:
            if head_reservation is not None:
                context.release_page(head_reservation)
            raise
        context.commit_page(head_reservation)
        return HeadAttempt(
            head,
            None,
            PageDispatch.DIRECT,
            None,
            dispatch_started=True,
            settlement=CommandSettlement.SUCCESS,
        )

    def head_failure(self, error: BaseException) -> HeadAttempt:
        """Classify a failed direct head: possibly executed once dispatch started, else not executed."""
        dispatched = bool(getattr(error, "_b24api_dispatch_started", False))
        return HeadAttempt(
            None,
            error,
            PageDispatch.DIRECT,
            None,
            dispatch_started=dispatched,
            settlement=CommandSettlement.UNKNOWN if dispatched else CommandSettlement.NOT_EXECUTED,
        )

    async def tail_batch_size(
        self,
        ctx: StrategyContext,
        *,
        total: int,
        stride: int,
        head_rows: int,
        batch_size: int,
    ) -> int:
        """Refuse a tail the operation budgets cannot hold; return the physical batch size that fits."""
        policy = ctx.context.policy
        tail_pages = (total - 1) // stride if total > head_rows else 0
        budget = await ctx.context.snapshot()
        if budget.counters.logical_pages + tail_pages > policy.max_pages:
            raise CapabilityError("parallel counted traversal exceeds the logical page budget")
        if tail_pages and stride > policy.max_buffered_rows:
            raise CapabilityError("parallel counted page exceeds the decoded row buffer budget")
        fitted = min(batch_size, max(1, policy.max_buffered_rows // stride))
        minimum_tail_requests = (tail_pages + fitted - 1) // fitted
        if budget.counters.physical_requests + minimum_tail_requests > policy.max_requests:
            raise CapabilityError("parallel counted traversal exceeds the physical request budget")
        return fitted

    def open_tail(
        self,
        requests: Iterator[_BatchInput],
        *,
        batch_size: int,
        policy: ExecutionPolicy,
        context: ExecutionContext,
    ) -> _FlatTailStream:
        """Stream the tail through a private bounded batch outcome stream."""
        outcomes = _BatchOutcomeStream(
            BatchExecutor(self._ctx.executor),
            requests,
            batch_size=batch_size,
            policy=policy,
            context=context,
            logical_page_per_command=True,
        )
        return _FlatTailStream(outcomes, self._recorder)


__all__ = [
    "CountedRowAccount",
    "CountedTailStream",
    "CountedWaveIO",
    "DirectCountedWaveIO",
    "FlatCountedRowAccount",
    "HeadAttempt",
    "TailReceipt",
]
