"""Counted physical-batch traversal strategy."""

from __future__ import annotations
import asyncio
from typing import TYPE_CHECKING, Any

from b24api.contracts.policy import KernelState
from b24api.errors import CapabilityError, IncompleteTraversalError
from b24api.execution import (
    await_cleanup_resistant,
    rearm_cancellation,
)
from b24api.plans import (
    CountedOffsetMode,
    CountedOffsetPlan,
)
from b24api.traversal.identity import _Page, _request_with_controls, _response_items

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from b24api.contracts.json import JsonValue
    from b24api.contracts.request import ParameterPath
    from b24api.contracts.response import Response
    from b24api.execution.snapshot import KernelReport


class _CountedBatchMixin:
    terminal_reason: str | None
    batch_report: KernelReport | None

    async def counted_batch_pages(  # noqa: C901, PLR0912, PLR0915
        self: Any,
        *,
        batch_size: int,
        page_size: int,
    ) -> AsyncGenerator[_Page]:
        """Execute the committed direct-head/batched-tail counted traversal."""
        from b24api.batch.engine import BatchExecutor  # noqa: PLC0415
        from b24api.batch.outcome import BatchFailure, BatchSuccess  # noqa: PLC0415
        from b24api.batch.stream import _BatchOutcomeStream  # noqa: PLC0415

        if not isinstance(self.plan, CountedOffsetPlan):
            raise TypeError("counted batch traversal requires CountedOffsetPlan")
        if self.plan.mode is not CountedOffsetMode.SEQUENTIAL_NEXT:
            raise CapabilityError("compatibility counted batch traversal requires the canonical counted plan")
        self.begin_external_validation()
        outcomes: _BatchOutcomeStream | None = None
        try:
            head_controls: dict[ParameterPath, object] = {self.plan.offset_path: 0}
            if self.plan.limit_path is not None:
                head_controls[self.plan.limit_path] = page_size
            head_reservation = await self.context.reserve_page()
            try:
                head = await self.executor.execute(
                    _request_with_controls(
                        self.request,
                        head_controls,
                        allow_create=self.plan.allow_create_controls,
                    ),
                    context=self.context,
                )
            except BaseException:
                self.context.release_page(head_reservation)
                raise
            self.context.commit_page(head_reservation)
            head_items = _response_items(head, self.selector)
            await self.context.set_buffered_rows(len(head_items))
            total = head.total
            if total is None or total < 0:
                raise CapabilityError("parallel counted traversal requires a non-negative total")
            if total < len(head_items):
                raise CapabilityError("parallel counted traversal observed total below the head page")
            stride = head.next if head.next is not None else page_size
            if isinstance(stride, bool) or stride < 1:
                raise CapabilityError("parallel counted traversal requires a positive in-band stride")
            if total > len(head_items) and head.next is None:
                raise CapabilityError("parallel counted traversal has no in-band tail stride")
            if head.next is not None and head.next != len(head_items):
                raise CapabilityError("parallel counted head continuation contradicts its row count")
            if total == len(head_items) and head.next is not None:
                raise CapabilityError("parallel counted traversal completed while continuation remained")
            tail_pages = (total - 1) // stride if total > len(head_items) else 0
            budget = await self.context.snapshot()
            if budget.counters.logical_pages + tail_pages > self.context.policy.max_pages:
                raise CapabilityError("parallel counted traversal exceeds the logical page budget")
            if tail_pages and stride > self.context.policy.max_buffered_rows:
                raise CapabilityError("parallel counted page exceeds the decoded row buffer budget")
            effective_batch_size = min(
                batch_size,
                max(1, self.context.policy.max_buffered_rows // stride),
            )
            minimum_tail_requests = (tail_pages + effective_batch_size - 1) // effective_batch_size
            if budget.counters.physical_requests + minimum_tail_requests > self.context.policy.max_requests:
                raise CapabilityError("parallel counted traversal exceeds the physical request budget")
            self.validate_external_page(head_items, head)
            yield _Page(tuple(head_items), head, (1,) * len(head_items))
            await self.context.set_buffered_rows(0)
            if total == len(head_items):
                self.finish_external_validation()
                self.terminal_reason = "parallel counted traversal completed"
                return
            requests = (
                _request_with_controls(
                    self.request,
                    {
                        self.plan.offset_path: start,
                        **({self.plan.limit_path: page_size} if self.plan.limit_path is not None else {}),
                    },
                    allow_create=self.plan.allow_create_controls,
                )
                for start in range(stride, total, stride)
            )
            outcomes = _BatchOutcomeStream(
                BatchExecutor(self.executor),
                requests,
                batch_size=effective_batch_size,
                policy=self.context.policy,
                context=self.context,
                logical_page_per_command=True,
            )

            def validated_outcome(outcome: object) -> tuple[Response, list[JsonValue]]:
                if isinstance(outcome, BatchFailure):
                    if isinstance(outcome.error, BaseException):
                        raise outcome.error
                    raise CapabilityError("parallel counted batch command failed")
                if not isinstance(outcome, BatchSuccess) or outcome.response is None:
                    raise CapabilityError("parallel counted batch outcome lacks correlated response evidence")
                response = outcome.response
                start = stride * (outcome.command_index + 1)
                items = _response_items(response, self.selector)
                expected_rows = min(stride, total - start)
                if len(items) != expected_rows:
                    raise CapabilityError("parallel counted page length contradicts the planned exact range")
                if response.total is not None and response.total != total:
                    raise CapabilityError("parallel counted page total contradicts the head total")
                expected_next = start + stride if start + stride < total else None
                if response.next != expected_next:
                    raise CapabilityError("parallel counted continuation contradicts the planned exact range")
                self.validate_external_page(items, response)
                return response, items

            primary_error: BaseException | None = None
            pending_cancellation: asyncio.CancelledError | None = None
            try:
                async for outcome in outcomes:
                    response, items = validated_outcome(outcome)
                    yield _Page(tuple(items), response, (1,) * len(items))
            except BaseException as error:
                primary_error = error
                raise
            finally:
                preserve_primary = primary_error is not None and not isinstance(
                    primary_error,
                    asyncio.CancelledError | GeneratorExit,
                )
                cleanup = await await_cleanup_resistant(outcomes.aclose())
                self.batch_report = outcomes.report
                if cleanup.error is not None:
                    cleanup_error = cleanup.error
                    if not preserve_primary:
                        rearm_cancellation(cleanup.cancellation)
                        raise cleanup_error
                    if isinstance(cleanup_error, asyncio.CancelledError):
                        pending_cancellation = cleanup_error
                    if cleanup.cancellation is not None:
                        pending_cancellation = cleanup.cancellation
                elif cleanup.cancellation is not None and not preserve_primary:
                    raise cleanup.cancellation
                elif preserve_primary:
                    pending_cancellation = cleanup.cancellation
                rearm_cancellation(pending_cancellation)
            if outcomes.report.state is not KernelState.COMPLETED:
                raise IncompleteTraversalError(report=outcomes.report)
            if self.validated_rows != total:
                raise CapabilityError("parallel counted traversal did not emit its exact total")
            self.finish_external_validation()
            self.terminal_reason = "parallel counted traversal completed"
        finally:
            self.close_external_validation()
