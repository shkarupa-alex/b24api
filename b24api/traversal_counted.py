"""Lazy exact direct-head plus physically batched counted traversal stream."""

from __future__ import annotations
import asyncio
import contextlib
from typing import TYPE_CHECKING, Self

from b24api.error import CapabilityError, IncompleteTraversalError
from b24api.models import (
    CompletionAssurance,
    ExecutionPolicy,
    IdentitySpec,
    JsonValue,
    OperationReport,
    Request,
    ResponseEvidence,
    ResultSelector,
    SnapshotRequirement,
    SnapshotState,
    TerminalState,
)
from b24api.pagination import PaginationDriver

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from b24api.execution import Executor
    from b24api.plans import CountedOffsetPlan


def _attach_report(error: BaseException, report: OperationReport) -> None:
    with contextlib.suppress(AttributeError, TypeError):
        error.report = report  # type: ignore[attr-defined]


class CountedItemStream:
    """Single-use counted item stream backed by the proven pagination driver."""

    def __init__(  # noqa: PLR0913
        self,
        executor: Executor,
        request: Request,
        *,
        plan: CountedOffsetPlan,
        selector: ResultSelector,
        identity: IdentitySpec,
        page_size: int,
        batch_size: int,
        policy: ExecutionPolicy,
    ) -> None:
        """Initialize without scheduling work."""
        self._context = executor.context(policy)
        self._driver = PaginationDriver(
            executor,
            request,
            plan,
            selector=selector,
            identity=identity,
            context=self._context,
            page_cap_hint=page_size,
        )
        self._page_size = page_size
        self._batch_size = batch_size
        self._runner: AsyncGenerator[JsonValue] | None = None
        self._closed = False
        self._emitted = 0
        self._unique = 0
        self._evidence: list[ResponseEvidence] = []
        self.report = OperationReport()

    def __aiter__(self) -> Self:
        """Return this asynchronous iterator."""
        return self

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
        if self.report.state is TerminalState.NOT_STARTED:
            await self._finalize(TerminalState.CANCELLED, "stream closed before exhaustion")

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
                    yield item
            await self._finalize(TerminalState.COMPLETED, "counted traversal completed exactly")
        except asyncio.CancelledError as error:
            primary = error
            await self._finalize(TerminalState.CANCELLED, "iteration cancelled")
            _attach_report(error, self.report)
            raise
        except GeneratorExit as error:
            primary = error
            await self._finalize(TerminalState.CANCELLED, "stream closed before exhaustion")
            raise
        except BaseException as error:
            primary = error
            snapshot = await self._context.snapshot()
            if isinstance(error, CapabilityError) and snapshot.counters.physical_requests == 0:
                await self._finalize(TerminalState.FAILED, type(error).__name__)
                _attach_report(error, self.report)
                raise
            await self._finalize(TerminalState.INCOMPLETE, type(error).__name__)
            incomplete = IncompleteTraversalError(report=self.report)
            incomplete.__cause__ = error
            raise incomplete from error
        finally:
            self._closed = True
            if primary is not None and self.report.state is TerminalState.NOT_STARTED:
                await self._finalize(TerminalState.FAILED, type(primary).__name__)

    async def _finalize(self, state: TerminalState, reason: str) -> None:
        if self.report.state is not TerminalState.NOT_STARTED:
            return
        snapshot = await self._context.snapshot()
        batch = self._driver.batch_report
        snapshot_state = (
            SnapshotState.NOT_REQUESTED
            if self._context.policy.consistency.snapshot_requirement is SnapshotRequirement.TRAVERSAL_ONLY
            else SnapshotState.UNVERIFIED
        )
        if state is TerminalState.COMPLETED and snapshot_state is SnapshotState.UNVERIFIED:
            state = TerminalState.INCOMPLETE
            reason = "required snapshot was not verified"
        self.report = OperationReport(
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
        )


__all__ = ["CountedItemStream"]
