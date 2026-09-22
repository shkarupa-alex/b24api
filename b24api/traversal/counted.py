"""Lazy exact direct-head plus physically batched counted traversal stream."""

from __future__ import annotations
import asyncio
from typing import TYPE_CHECKING, Self

from b24api.completion.recorder import CountedCompletionRecorder
from b24api.contracts.completion import BindingClosure, CleanupState, StreamClosure
from b24api.contracts.json import _thaw_json
from b24api.contracts.policy import (
    CompletionAssurance,
    ExecutionPolicy,
    KernelState,
    SnapshotRequirement,
    SnapshotState,
)
from b24api.contracts.report import retain_page_trace
from b24api.errors import CapabilityError, IncompleteTraversalError
from b24api.execution.failure import attach_report as _attach_report
from b24api.execution.snapshot import KernelReport
from b24api.traversal.driver import PaginationDriver

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from b24api.contracts.json import JsonValue
    from b24api.contracts.page import PageAdapter
    from b24api.contracts.request import Request, ResultSelector, TraversalIdentity
    from b24api.contracts.response import ResponseEvidence
    from b24api.execution import Executor
    from b24api.traversal.plans import CountedOffsetPlan


class CountedItemStream:
    """Single-use counted item stream backed by the proven pagination driver."""

    def __init__(  # noqa: PLR0913
        self,
        executor: Executor,
        request: Request,
        *,
        plan: CountedOffsetPlan,
        selector: ResultSelector,
        identity: TraversalIdentity | None,
        page_size: int,
        batch_size: int,
        policy: ExecutionPolicy,
        page_adapter: PageAdapter,
    ) -> None:
        """Initialize without scheduling work."""
        self._context = executor.context(policy)
        self._completion = CountedCompletionRecorder()
        self._completion_cleanup_done = False
        self._driver = PaginationDriver(
            executor,
            request,
            plan,
            selector=selector,
            identity=identity,
            context=self._context,
            page_cap_hint=page_size,
            page_adapter=page_adapter,
            completion_recorder=self._completion,
        )
        self._page_size = page_size
        self._batch_size = batch_size
        self._runner: AsyncGenerator[JsonValue] | None = None
        self._closed = False
        self._emitted = 0
        self._unique = 0
        self._evidence: list[ResponseEvidence] = []
        self.report = KernelReport()

    def __aiter__(self) -> Self:
        """Return this asynchronous iterator."""
        return self

    @property
    def completion_gate(self) -> object:
        """Expose live counted completion evidence to the operation wrapper."""
        return self._completion.gate

    def _finish_completion_cleanup(self) -> None:
        if self._completion_cleanup_done or self.report.state is KernelState.NOT_STARTED:
            return
        self._completion.cleanup(CleanupState.SUCCESS)
        self._completion_cleanup_done = True

    async def __anext__(self) -> JsonValue:
        """Return the next validated item."""
        if self._closed:
            raise StopAsyncIteration
        if self._runner is None:
            self._runner = self._run()
        return await anext(self._runner)

    async def aclose(self) -> None:
        """Close the counted traversal idempotently."""
        if self._closed:
            return
        self._closed = True
        if self._runner is not None:
            await self._runner.aclose()
        if self.report.state is KernelState.NOT_STARTED:
            await self._finalize(KernelState.CANCELLED, "stream closed before exhaustion")
        self._finish_completion_cleanup()

    async def _run(self) -> AsyncGenerator[JsonValue]:
        primary: BaseException | None = None
        try:
            async for page in self._driver.counted_batch_pages(
                batch_size=self._batch_size,
                page_size=self._page_size,
            ):
                self._evidence.append(page.response.evidence)
                for item, is_unique in zip(page.items, self._driver.last_page_unique_mask, strict=True):
                    self._emitted += 1
                    self._unique += int(is_unique)
                    yield _thaw_json(item)
                if page.items:
                    self._completion.delivered()
                    self._completion.acknowledged()
            await self._finalize(KernelState.COMPLETED, "counted traversal completed exactly")
        except asyncio.CancelledError as error:
            primary = error
            await self._finalize(KernelState.CANCELLED, "iteration cancelled")
            _attach_report(error, self.report)
            raise
        except GeneratorExit as error:
            primary = error
            await self._finalize(KernelState.CANCELLED, "stream closed before exhaustion")
            raise
        except BaseException as error:
            primary = error
            snapshot = await self._context.snapshot()
            if isinstance(error, CapabilityError) and snapshot.counters.physical_requests == 0:
                await self._finalize(KernelState.FAILED, type(error).__name__)
                _attach_report(error, self.report)
                raise
            await self._finalize(KernelState.INCOMPLETE, type(error).__name__)
            incomplete = IncompleteTraversalError(report=self.report)
            incomplete.__cause__ = error
            raise incomplete from error
        finally:
            self._closed = True
            if primary is not None and self.report.state is KernelState.NOT_STARTED:
                await self._finalize(KernelState.FAILED, type(primary).__name__)
            self._finish_completion_cleanup()

    async def _finalize(self, state: KernelState, reason: str) -> None:
        if self.report.state is not KernelState.NOT_STARTED:
            return
        snapshot = await self._context.snapshot()
        batch = self._driver.batch_report
        snapshot_state = (
            SnapshotState.NOT_REQUESTED
            if self._context.policy.consistency.snapshot_requirement is SnapshotRequirement.TRAVERSAL_ONLY
            else SnapshotState.UNVERIFIED
        )
        if state is KernelState.COMPLETED and snapshot_state is SnapshotState.UNVERIFIED:
            state = KernelState.INCOMPLETE
            reason = "required snapshot was not verified"
        page_trace, page_trace_truncated = retain_page_trace(
            tuple(self._driver.page_trace),
            self._context.policy.page_trace_limit,
        )
        page_trace_truncated = page_trace_truncated or self._driver.page_trace_truncated
        self.report = KernelReport(
            state=state,
            assurance=CompletionAssurance.CALLER_ASSERTED,
            snapshot=snapshot_state,
            plan_id="iter_list_counted",
            dispatch_id="batch",
            emitted_rows=self._emitted,
            unique_rows=self._unique,
            physical_requests=snapshot.counters.physical_requests,
            logical_pages=snapshot.counters.logical_pages,
            batch_requests=batch.batch_requests if batch is not None else 0,
            batch_commands=batch.batch_commands if batch is not None else 0,
            retries=snapshot.retries,
            cooldown_seconds=snapshot.cooldown_seconds,
            buffered_rows_high_water=snapshot.counters.buffered_rows_high_water,
            violations=tuple(self._driver.violations),
            terminal_reason=reason,
            evidence=tuple(self._evidence),
            page_trace=page_trace,
            page_trace_truncated=page_trace_truncated,
        )
        closure = BindingClosure.QUALIFIED_TOTAL if state is KernelState.COMPLETED else BindingClosure.FAILURE
        stream = (
            StreamClosure.NATURAL if state is KernelState.COMPLETED
            else StreamClosure.CANCELLED if state is KernelState.CANCELLED
            else StreamClosure.EARLY_CLOSE
        )
        self._completion.terminal(closure, stream)


__all__ = ["CountedItemStream"]
