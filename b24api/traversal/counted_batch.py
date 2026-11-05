"""Counted head transaction and dense tail shared by the flat stream and the reference queue.

One head transaction serves both callers: the head is dispatched through the caller's I/O seam,
selected and adapted once, decided by the pure counted rules (``EMPTY``, ``DENSE``,
``SEQUENTIAL`` or ``REJECT``), validated, committed and yielded once. A dense head opens a lazily
planned tail whose receipts are committed strictly in offset order; a reference head that does not
prove its width continues on the sequential counted strategy, seeded with the committed head
instead of fetching it again. The flat stream never continues sequentially: its unqualified head
keeps today's rejection.
"""

from __future__ import annotations
import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from b24api.batch.engine import _BatchInput
from b24api.completion.recorder import CountedCompletionRecorder
from b24api.contracts.policy import ConfirmationPolicy, KernelState
from b24api.contracts.report import PageDispatch
from b24api.contracts.traversal import OffsetContinuation
from b24api.errors import CapabilityError, IncompleteTraversalError
from b24api.execution import (
    await_cleanup_resistant,
    rearm_cancellation,
)
from b24api.traversal.counted_io import DirectCountedWaveIO
from b24api.traversal.counted_rules import (
    CountedHeadFacts,
    CountedHeadPlan,
    EmptyHeadFacts,
    check_tail_page,
    counted_head_range,
    dense_width_proven,
)
from b24api.traversal.counted_rules import (
    empty_source_head_eligible as _empty_source_head_eligible,
)
from b24api.traversal.identity import _Page, _request_with_controls
from b24api.traversal.plans import (
    CountedOffsetPlan,
)
from b24api.traversal.sequential import CountedStrategy

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator

    from b24api.contracts.json import FrozenJson
    from b24api.contracts.request import ParameterPath, Request
    from b24api.contracts.response import Response
    from b24api.execution.context import CleanupResult
    from b24api.traversal.counted_io import CountedTailStream, CountedWaveIO, TailReceipt
    from b24api.traversal.strategy_context import PageVerdict, StrategyContext


@dataclass(frozen=True, slots=True)
class _SequentialSeed:
    """A selected reference head the sequential counted strategy judged and continues from."""

    strategy: CountedStrategy
    head: Response = field(repr=False)
    items: tuple[FrozenJson, ...] = field(repr=False)
    verdict: PageVerdict


def _counted_plan(ctx: StrategyContext) -> CountedOffsetPlan:
    plan = ctx.plan
    if not isinstance(plan, CountedOffsetPlan):
        raise TypeError("counted batch traversal requires CountedOffsetPlan")
    return plan


def _head_plan(plan: CountedOffsetPlan) -> CountedHeadPlan:
    return CountedHeadPlan(plan.continuation, plan.fixed_stride)


def empty_source_head_eligible(
    ctx: StrategyContext,
    response: Response,
    source: tuple[FrozenJson, ...],
    adapted: tuple[FrozenJson, ...],
) -> bool:
    """Return, without side effects, whether an unvalidated head may witness an empty source."""
    plan = ctx.plan
    return isinstance(plan, CountedOffsetPlan) and _empty_source_head_eligible(
        EmptyHeadFacts(
            first_page=ctx.page_trace_count == 0,
            offset=ctx.page_offset,
            validated_rows=ctx.validated_rows,
            expected_total=ctx.expected_total,
            fixed_step=plan.continuation is OffsetContinuation.FIXED_STEP,
            qualified_total_required=ctx.confirmation_policy is ConfirmationPolicy.QUALIFIED_TOTAL,
            source_rows=len(source),
            adapted_rows=len(adapted),
            total=response.total,
            next=response.next,
        ),
    )


class CountedBatchStrategy:
    """The committed counted head and its dense tail or sequential continuation (§3.6 paged form)."""

    def __init__(self, *, batch_size: int, page_size: int, io: CountedWaveIO | None = None) -> None:
        """Hold the physical batch size, the declared page size and the caller's I/O seam."""
        self._batch_size = batch_size
        self._page_size = page_size
        self._io = io
        self._plan: CountedOffsetPlan | None = None
        self._total = 0
        self._stride = 1

    @property
    def plan(self) -> CountedOffsetPlan:
        """Return the counted plan of the running traversal."""
        if self._plan is None:
            raise RuntimeError("counted batch traversal has not started")
        return self._plan

    def _seam(self, ctx: StrategyContext) -> CountedWaveIO:
        if self._io is not None:
            return self._io
        recorder = ctx.completion_recorder
        if recorder is not None and not isinstance(recorder, CountedCompletionRecorder):
            raise TypeError("counted batch traversal requires a counted completion recorder")
        return DirectCountedWaveIO(ctx, recorder)

    async def pages(self, ctx: StrategyContext) -> AsyncGenerator[_Page]:
        """Run the head transaction, then the dense tail or the seeded sequential continuation."""
        self._plan = _counted_plan(ctx)
        io = self._seam(ctx)
        ctx.begin_external_validation()
        try:
            head = await self._head(ctx, io)
            head_items, facts, verdict, sequential = await self._decide(ctx, io, head)
            if facts.decision == "EMPTY":
                # One empty head without a total or continuation closes the source; no tail is scheduled.
                ctx.validate_external_page(head_items, head, terminal=True, empty_source=True)
                yield _Page((), head, ())
                await io.rows.release_held()
                return
            if sequential is not None and verdict is not None:
                async for page in self._sequential(ctx, io, _SequentialSeed(sequential, head, head_items, verdict)):
                    yield page
                return
            batch_size = await io.tail_batch_size(
                ctx,
                total=self._total,
                stride=self._stride,
                head_rows=len(head_items),
                batch_size=self._batch_size,
            )
            closes = self._total == len(head_items)
            ctx.validate_external_page(head_items, head, terminal=closes)
            yield _Page(tuple(head_items), head, (1,) * len(head_items), not closes)
            await io.rows.release_held()
            if not closes:
                async for page in self._tail(ctx, io, batch_size):
                    yield page
                if ctx.validated_rows != self._total:
                    raise CapabilityError("parallel counted traversal did not emit its exact total")
            ctx.terminal_reason = io.completed_reason
        finally:
            ctx.close_external_validation()

    def _page_request(self, ctx: StrategyContext, offset: int) -> Request:
        plan = self.plan
        controls: dict[ParameterPath, object] = {plan.offset_path: offset}
        if plan.limit_path is not None:
            controls[plan.limit_path] = self._page_size
        return _request_with_controls(ctx.request, controls, allow_create=plan.allow_create_controls)

    async def _head(self, ctx: StrategyContext, io: CountedWaveIO) -> Response:
        """Dispatch the head and settle it with the dispatch facts the seam observed."""
        ctx.schedule_page(offset=0, dispatch=io.head_dispatch)
        recorder = ctx.completion_recorder
        try:
            attempt = await io.execute_head(self._page_request(ctx, 0), context=ctx.context)
        except BaseException as error:
            failed = io.head_failure(error)
            if recorder is not None:
                recorder.settled(failed.settlement)
            if failed.dispatch_started:
                report_error = failed.error if failed.error is not None else error
                ctx.set_page_dispatch(dispatch=failed.dispatch, batch_index=failed.batch_index)
                ctx.record_unknown_page(dispatch=failed.dispatch, batch_index=failed.batch_index, error=report_error)
            raise
        if recorder is not None:
            recorder.settled(attempt.settlement)
        if attempt.response is None:
            raise RuntimeError("counted head lacks its response")
        ctx.set_page_dispatch(dispatch=attempt.dispatch, batch_index=attempt.batch_index)
        return attempt.response

    async def _decide(
        self,
        ctx: StrategyContext,
        io: CountedWaveIO,
        head: Response,
    ) -> tuple[tuple[FrozenJson, ...], CountedHeadFacts, PageVerdict | None, CountedStrategy | None]:
        """Select the head once, hold its rows and decide it; a rejection is recorded once."""
        trace_count = ctx.page_trace_count
        head_items: tuple[FrozenJson, ...] = ()
        try:
            head_items = ctx.select_page(head)
            await io.rows.hold(len(head_items))
            if ctx.empty_source_head_eligible(head, ctx.source_page.current(head_items), head_items):
                return head_items, CountedHeadFacts("EMPTY", None, None, 0), None, None
            try:
                self._total, self._stride = counted_head_range(
                    _head_plan(self.plan),
                    total=head.total,
                    next_offset=head.next,
                    head_rows=len(head_items),
                    page_size=self._page_size,
                )
                dense = not io.allows_sequential or dense_width_proven(
                    _head_plan(self.plan),
                    total=self._total,
                    stride=self._stride,
                    head_rows=len(head_items),
                    page_size=self._page_size,
                )
            except CapabilityError:
                if not io.allows_sequential:
                    raise
                dense = False
            if dense:
                return head_items, CountedHeadFacts("DENSE", self._total, self._stride, len(head_items)), None, None
            # A reference head that does not prove a dense width keeps the sequential counted rules.
            sequential = CountedStrategy(self.plan)
            sequential.seed_head(ctx)
            verdict = sequential.judge(ctx, head, head_items)
        except BaseException as error:
            if ctx.page_trace_count == trace_count:
                ctx.reject_external_page(head_items, head, error)
            await io.rows.release_held()
            raise
        return head_items, CountedHeadFacts("SEQUENTIAL", head.total, None, len(head_items)), verdict, sequential

    async def _sequential(
        self, ctx: StrategyContext, io: CountedWaveIO, seed: _SequentialSeed
    ) -> AsyncGenerator[_Page]:
        """Commit the already selected head, then continue on the sequential counted strategy."""
        try:
            identities = ctx.validate_page(
                seed.items,
                response=seed.head,
                terminal=seed.verdict.terminal,
                identities=seed.verdict.identities,
            )
        except BaseException:
            await io.rows.release_held()
            raise
        if seed.items:
            yield _Page(tuple(seed.items), seed.head, (1,) * len(seed.items), not seed.verdict.terminal)
        await io.rows.release_held()
        async for page in ctx.continue_sequential(seed.strategy, identities):
            yield page

    def _tail_requests(self, ctx: StrategyContext) -> Iterator[_BatchInput]:
        recorder = ctx.completion_recorder
        for start in range(self._stride, self._total, self._stride):
            if isinstance(recorder, CountedCompletionRecorder):
                recorder.reserve()
            yield _BatchInput(self._page_request(ctx, start))

    async def _tail(self, ctx: StrategyContext, io: CountedWaveIO, batch_size: int) -> AsyncGenerator[_Page]:
        """Stream the dense tail and close it, keeping the primary failure over a cleanup failure."""
        tail = io.open_tail(
            self._tail_requests(ctx),
            batch_size=batch_size,
            policy=ctx.context.policy,
            context=ctx.context,
        )
        primary_error: BaseException | None = None
        try:
            async for receipt in tail:
                response, items, continuing = self._validated_receipt(ctx, tail, receipt)
                yield _Page(tuple(items), response, (1,) * len(items), continuing)
        except BaseException as error:
            primary_error = error
            raise
        finally:
            cleanup = await await_cleanup_resistant(tail.aclose())
            if tail.report is not None:
                ctx.batch_report = tail.report
            _settle_tail_cleanup(cleanup, primary_error)
        if tail.report is not None and tail.report.state is not KernelState.COMPLETED:
            raise IncompleteTraversalError(report=tail.report)

    def _validated_receipt(
        self,
        ctx: StrategyContext,
        tail: CountedTailStream,
        receipt: TailReceipt,
    ) -> tuple[Response, tuple[FrozenJson, ...], bool]:
        """Validate one tail page against the planned exact range, or record its rejection."""
        failure = receipt.failure
        tail.settle(receipt)
        start = self._stride * (receipt.ordinal + 1)
        ctx.schedule_page(offset=start, dispatch=PageDispatch.BATCH, batch_index=receipt.batch_index)
        if failure is not None:
            error = failure.error
            recorded = (
                error if isinstance(error, BaseException) else CapabilityError("parallel counted batch command failed")
            )
            ctx.record_unknown_page(dispatch=PageDispatch.BATCH, batch_index=receipt.batch_index, error=recorded)
            raise tail.failure(receipt, failure)
        response, total, stride = receipt.response, self._total, self._stride
        if response is None:
            raise CapabilityError("parallel counted batch outcome lacks correlated response evidence")
        trace_count = ctx.page_trace_count
        items: tuple[FrozenJson, ...] = ()
        try:
            items = ctx.select_page(response)
            check_tail_page(
                self.plan.continuation,
                rows=len(items),
                total_reported=response.total,
                next_reported=response.next,
                start=start,
                stride=stride,
                total=total,
            )
            ctx.validate_external_page(items, response, terminal=start + stride >= total)
        except BaseException as error:
            if ctx.page_trace_count == trace_count:
                ctx.reject_external_page(items, response, error)
            raise
        return response, items, start + stride < total


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
