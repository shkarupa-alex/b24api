"""Counted physical-batch traversal strategy."""

from __future__ import annotations
import asyncio
from typing import TYPE_CHECKING

from b24api.completion.recorder import CountedCompletionRecorder
from b24api.contracts.completion import CommandSettlement
from b24api.contracts.policy import ConfirmationPolicy, KernelState
from b24api.contracts.report import PageDispatch
from b24api.contracts.traversal import OffsetContinuation
from b24api.errors import B24ApiError, CapabilityError, IncompleteTraversalError
from b24api.execution import (
    await_cleanup_resistant,
    rearm_cancellation,
)
from b24api.traversal.counted_rules import (
    CountedContradiction,
    CountedPageFacts,
    expected_counted_next,
    judge_counted_page,
)
from b24api.traversal.identity import _Page, _request_with_controls
from b24api.traversal.plans import (
    CountedOffsetMode,
    CountedOffsetPlan,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator

    from b24api.batch.engine import _BatchInput
    from b24api.batch.outcome import BatchFailure
    from b24api.contracts.json import FrozenJson
    from b24api.contracts.request import ParameterPath, Request
    from b24api.contracts.response import Response
    from b24api.execution.context import CleanupResult
    from b24api.execution.snapshot import KernelReport
    from b24api.traversal.strategy_context import StrategyContext


def _counted_head_range(  # noqa: C901 - one closed set of head range contradictions
    plan: CountedOffsetPlan,
    head: Response,
    head_items: tuple[FrozenJson, ...],
    page_size: int,
) -> tuple[int, int]:
    """Return the exact total and stride a counted head qualifies, or reject the head."""
    total = head.total
    if total is None or total < 0:
        raise CapabilityError("parallel counted traversal requires a non-negative total")
    if total < len(head_items):
        raise CapabilityError("parallel counted traversal observed total below the head page")
    stride: int | None
    if total == 0 and not head_items:
        stride = page_size
    elif plan.mode is CountedOffsetMode.PARALLEL_FIXED_STRIDE:
        stride = plan.fixed_stride
    elif plan.continuation is OffsetContinuation.SERVER_NEXT:
        stride = head.next
    elif plan.continuation is OffsetContinuation.OBSERVED_COUNT:
        stride = len(head_items)
    else:
        stride = head.next if head.next is not None and head.next > 0 else len(head_items)
    if not isinstance(stride, int) or isinstance(stride, bool) or stride < 1:
        raise CapabilityError("parallel counted traversal requires a positive in-band stride")
    if len(head_items) != min(stride, total):
        raise CapabilityError("parallel counted head length contradicts the planned exact range")
    if total > len(head_items) and head.next is None and plan.continuation is OffsetContinuation.SERVER_NEXT:
        raise CapabilityError("parallel counted traversal has no in-band tail stride")
    if (
        plan.continuation in {OffsetContinuation.SERVER_NEXT, OffsetContinuation.SERVER_NEXT_OR_OBSERVED_COUNT}
        and head.next is not None
        and head.next > 0
        and head.next != stride
    ):
        raise CapabilityError("parallel counted head continuation contradicts its row count")
    verdict = judge_counted_page(
        CountedPageFacts(0, len(head_items), 0, total, head.next, plan.continuation),
    )
    if verdict.contradiction is CountedContradiction.CONTINUATION_AFTER_TOTAL:
        raise CapabilityError("parallel counted traversal completed while continuation remained")
    return total, stride


def _counted_parts(ctx: StrategyContext) -> tuple[CountedOffsetPlan, CountedCompletionRecorder | None]:
    """Return the counted plan and its page-reserving recorder, or reject any other traversal."""
    plan, recorder = ctx.plan, ctx.completion_recorder
    if not isinstance(plan, CountedOffsetPlan):
        raise TypeError("counted batch traversal requires CountedOffsetPlan")
    if recorder is not None and not isinstance(recorder, CountedCompletionRecorder):
        raise TypeError("counted batch traversal requires a counted completion recorder")
    return plan, recorder


def empty_source_head_eligible(
    ctx: StrategyContext,
    response: Response,
    source: tuple[FrozenJson, ...],
    adapted: tuple[FrozenJson, ...],
) -> bool:
    """Return, without side effects, whether an unvalidated head may witness an empty source.

    Only the first counted page at offset zero qualifies, with empty source and adapted rows, no
    usable total, no continuation, no fixed step, no earlier exact total and no caller-declared
    qualified-total confirmation. ``-1`` is the unknown-total sentinel and never counts as zero.
    """
    return (
        isinstance(ctx.plan, CountedOffsetPlan)
        and ctx.plan.continuation is not OffsetContinuation.FIXED_STEP
        and ctx.confirmation_policy is not ConfirmationPolicy.QUALIFIED_TOTAL
        and ctx.page_offset == 0
        and ctx.page_trace_count == 0
        and ctx.validated_rows == 0
        and ctx.expected_total is None
        and not source
        and not adapted
        and response.total in {None, -1}
        and response.next is None
    )


class CountedBatchStrategy:
    """The committed direct head and batched tail of a counted traversal (§3.6 paged form)."""

    def __init__(self, *, batch_size: int, page_size: int) -> None:
        """Hold the physical batch size and the requested page size."""
        self._batch_size = batch_size
        self._page_size = page_size
        self._plan: CountedOffsetPlan | None = None
        self._recorder: CountedCompletionRecorder | None = None
        self._total = 0
        self._stride = 1

    @property
    def plan(self) -> CountedOffsetPlan:
        """Return the counted plan of the running traversal."""
        if self._plan is None:
            raise RuntimeError("counted batch traversal has not started")
        return self._plan

    async def pages(self, ctx: StrategyContext) -> AsyncGenerator[_Page]:
        """Execute the committed direct-head/batched-tail counted traversal."""
        self._plan, self._recorder = _counted_parts(ctx)
        ctx.begin_external_validation()
        try:
            head = await self._fetch_head(ctx)
            head_items, empty_source = await self._qualify_head(ctx, head)
            if empty_source:
                # One empty head without a total or continuation closes the source; no tail is scheduled.
                ctx.validate_external_page(head_items, head, terminal=True, empty_source=True)
                yield _Page((), head, ())
                await ctx.context.set_buffered_rows(0)
                return
            batch_size = await self._tail_batch_size(ctx, len(head_items))
            closes = self._total == len(head_items)
            ctx.validate_external_page(head_items, head, terminal=closes)
            yield _Page(tuple(head_items), head, (1,) * len(head_items), not closes)
            await ctx.context.set_buffered_rows(0)
            if not closes:
                async for page in self._tail(ctx, batch_size):
                    yield page
                if ctx.validated_rows != self._total:
                    raise CapabilityError("parallel counted traversal did not emit its exact total")
            ctx.terminal_reason = "parallel counted traversal completed"
        finally:
            ctx.close_external_validation()

    def _page_request(self, ctx: StrategyContext, offset: int) -> Request:
        plan = self.plan
        controls: dict[ParameterPath, object] = {plan.offset_path: offset}
        if plan.limit_path is not None:
            controls[plan.limit_path] = self._page_size
        return _request_with_controls(ctx.request, controls, allow_create=plan.allow_create_controls)

    async def _fetch_head(self, ctx: StrategyContext) -> Response:
        """Fetch the direct head page, settling its recorder page and page budget reservation."""
        recorder = self._recorder
        ctx.schedule_page(offset=0, dispatch=PageDispatch.DIRECT)
        if recorder is not None:
            recorder.activate(recorder.reserve())
        head_reservation = None
        try:
            head_reservation = await ctx.context.reserve_page()
            head = await ctx.executor.execute(self._page_request(ctx, 0), context=ctx.context)
        except BaseException as error:
            dispatched = bool(getattr(error, "_b24api_dispatch_started", False))
            if recorder is not None:
                recorder.settled(CommandSettlement.UNKNOWN if dispatched else CommandSettlement.NOT_EXECUTED)
            if head_reservation is not None:
                ctx.context.release_page(head_reservation)
            if dispatched:
                ctx.record_unknown_page(dispatch=PageDispatch.DIRECT, batch_index=None, error=error)
            raise
        ctx.context.commit_page(head_reservation)
        if recorder is not None:
            recorder.settled(CommandSettlement.SUCCESS)
        return head

    async def _qualify_head(self, ctx: StrategyContext, head: Response) -> tuple[tuple[FrozenJson, ...], bool]:
        """Select the head and fix the exact total and stride, or record the head's rejection."""
        trace_count = ctx.page_trace_count
        head_items: tuple[FrozenJson, ...] = ()
        try:
            head_items = ctx.select_page(head)
            await ctx.context.set_buffered_rows(len(head_items))
            empty_source = ctx.empty_source_head_eligible(head, ctx.source_page.current(head_items), head_items)
            if not empty_source:
                self._total, self._stride = _counted_head_range(self.plan, head, head_items, self._page_size)
        except BaseException as error:
            if ctx.page_trace_count == trace_count:
                ctx.reject_external_page(head_items, head, error)
            raise
        return head_items, empty_source

    async def _tail_batch_size(self, ctx: StrategyContext, head_rows: int) -> int:
        """Refuse a tail the operation budgets cannot hold; return the physical batch size that fits."""
        total, stride, policy = self._total, self._stride, ctx.context.policy
        tail_pages = (total - 1) // stride if total > head_rows else 0
        budget = await ctx.context.snapshot()
        if budget.counters.logical_pages + tail_pages > policy.max_pages:
            raise CapabilityError("parallel counted traversal exceeds the logical page budget")
        if tail_pages and stride > policy.max_buffered_rows:
            raise CapabilityError("parallel counted page exceeds the decoded row buffer budget")
        batch_size = min(self._batch_size, max(1, policy.max_buffered_rows // stride))
        minimum_tail_requests = (tail_pages + batch_size - 1) // batch_size
        if budget.counters.physical_requests + minimum_tail_requests > policy.max_requests:
            raise CapabilityError("parallel counted traversal exceeds the physical request budget")
        return batch_size

    def _tail_requests(self, ctx: StrategyContext) -> Iterator[_BatchInput]:
        from b24api.batch.engine import _BatchInput  # noqa: PLC0415

        for start in range(self._stride, self._total, self._stride):
            if self._recorder is not None:
                self._recorder.reserve()
            yield _BatchInput(self._page_request(ctx, start))

    async def _tail(self, ctx: StrategyContext, batch_size: int) -> AsyncGenerator[_Page]:
        """Stream the batched tail and close it, keeping the primary failure over a cleanup failure."""
        from b24api.batch.engine import BatchExecutor  # noqa: PLC0415
        from b24api.batch.stream import _BatchOutcomeStream  # noqa: PLC0415

        outcomes = _BatchOutcomeStream(
            BatchExecutor(ctx.executor),
            self._tail_requests(ctx),
            batch_size=batch_size,
            policy=ctx.context.policy,
            context=ctx.context,
            logical_page_per_command=True,
        )
        primary_error: BaseException | None = None
        try:
            async for outcome in outcomes:
                response, items, continuing = self._validated_outcome(ctx, outcome, outcomes.report)
                yield _Page(tuple(items), response, (1,) * len(items), continuing)
        except BaseException as error:
            primary_error = error
            raise
        finally:
            cleanup = await await_cleanup_resistant(outcomes.aclose())
            ctx.batch_report = outcomes.report
            _settle_tail_cleanup(cleanup, primary_error)
        if outcomes.report.state is not KernelState.COMPLETED:
            raise IncompleteTraversalError(report=outcomes.report)

    def _tail_failure(self, ctx: StrategyContext, outcome: BatchFailure, report: KernelReport) -> BaseException:
        """Record a failed tail command as an unknown page and return the failure to raise."""
        if self._recorder is not None:
            self._recorder.activate(outcome.command_index + 1)
            self._recorder.settled(CommandSettlement.FAILURE)
        error = outcome.error
        recorded = (
            error if isinstance(error, BaseException) else CapabilityError("parallel counted batch command failed")
        )
        ctx.schedule_page(
            offset=self._stride * (outcome.command_index + 1),
            dispatch=PageDispatch.BATCH,
            batch_index=outcome.command_index,
        )
        ctx.record_unknown_page(dispatch=PageDispatch.BATCH, batch_index=outcome.command_index, error=recorded)
        if isinstance(error, B24ApiError):
            failure = IncompleteTraversalError(
                report=report, error=error, replay_disposition=outcome.replay_disposition
            )
            failure.__cause__ = error
            failure.__suppress_context__ = True
            return failure
        return recorded

    def _validated_outcome(
        self,
        ctx: StrategyContext,
        outcome: object,
        report: KernelReport,
    ) -> tuple[Response, tuple[FrozenJson, ...], bool]:
        """Validate one tail page against the planned exact range, or record its rejection."""
        from b24api.batch.outcome import BatchFailure, BatchSuccess  # noqa: PLC0415

        if isinstance(outcome, BatchFailure):
            raise self._tail_failure(ctx, outcome, report)
        if not isinstance(outcome, BatchSuccess) or outcome.response is None:
            raise CapabilityError("parallel counted batch outcome lacks correlated response evidence")
        response, total, stride = outcome.response, self._total, self._stride
        if self._recorder is not None:
            self._recorder.activate(outcome.command_index + 1)
            self._recorder.settled(CommandSettlement.SUCCESS)
        start = stride * (outcome.command_index + 1)
        ctx.schedule_page(offset=start, dispatch=PageDispatch.BATCH, batch_index=outcome.command_index)
        trace_count = ctx.page_trace_count
        items: tuple[FrozenJson, ...] = ()
        try:
            items = ctx.select_page(response)
            _check_tail_page(self.plan, response, len(items), start=start, stride=stride, total=total)
            ctx.validate_external_page(items, response, terminal=start + stride >= total)
        except BaseException as error:
            if ctx.page_trace_count == trace_count:
                ctx.reject_external_page(items, response, error)
            raise
        return response, items, start + stride < total


def _check_tail_page(  # noqa: PLR0913 - the planned range of one tail page
    plan: CountedOffsetPlan,
    response: Response,
    rows: int,
    *,
    start: int,
    stride: int,
    total: int,
) -> None:
    """Reject a tail page whose rows, total or continuation contradict the planned exact range."""
    if rows != min(stride, total - start):
        raise CapabilityError("parallel counted page length contradicts the planned exact range")
    if response.total not in {None, -1} and response.total != total:
        raise CapabilityError("parallel counted page total contradicts the head total")
    expected_next = expected_counted_next(start, stride, total)
    if plan.continuation.value == "server_next" and response.next != expected_next:
        raise CapabilityError("parallel counted continuation contradicts the planned exact range")
    if (
        plan.continuation.value == "server_next_or_observed_count"
        and response.next is not None
        and response.next != expected_next
    ):
        raise CapabilityError("parallel counted continuation contradicts the planned exact range")


def _settle_tail_cleanup(cleanup: CleanupResult, primary_error: BaseException | None) -> None:
    """Raise a tail cleanup failure unless a primary failure outranks it; replay a deferred cancellation."""
    preserve_primary = primary_error is not None and not isinstance(
        primary_error, asyncio.CancelledError | GeneratorExit
    )
    pending_cancellation: asyncio.CancelledError | None = None
    if cleanup.error is not None:
        if not preserve_primary:
            rearm_cancellation(cleanup.cancellation)
            raise cleanup.error
        if isinstance(cleanup.error, asyncio.CancelledError):
            pending_cancellation = cleanup.error
        if cleanup.cancellation is not None:
            pending_cancellation = cleanup.cancellation
    elif cleanup.cancellation is not None and not preserve_primary:
        raise cleanup.cancellation
    elif preserve_primary:
        pending_cancellation = cleanup.cancellation
    rearm_cancellation(pending_cancellation)


__all__ = ["CountedBatchStrategy", "empty_source_head_eligible"]
