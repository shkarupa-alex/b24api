"""The wave engine of a flat fast keyset traversal.

A flat traversal owns its physical requests: a wave travels as one private physical batch, and a
sequential finish page, which no other command could share, travels as one direct request.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.batch.engine import BatchExecutor, BatchProgress
from b24api.batch.outcome import BatchSuccess
from b24api.contracts.report import PageDispatch
from b24api.execution.facts import settlement

if TYPE_CHECKING:
    from collections.abc import Mapping

    from b24api.batch.outcome import BatchOutcome
    from b24api.completion.fast_recorder import FastPageLedger
    from b24api.contracts.request import Request
    from b24api.execution import ExecutionContext, Executor


class FlatWaveEngine:
    """``WaveEngine`` of a flat traversal: private batches for waves, a direct request per finish page."""

    def __init__(self, executor: Executor, ledger: FastPageLedger) -> None:
        """Bind the operation executor, its private batch executor and the traversal's page ledger."""
        self._executor = executor
        self._batches = BatchExecutor(executor)
        self._ledger = ledger

    @property
    def portal_command_cap(self) -> int:
        """Return the private batch executor's command cap."""
        return self._batches.portal_command_cap

    @property
    def page_dispatch(self) -> PageDispatch:
        """Return ``DIRECT``: a flat finish page has no command to share a batch with."""
        return PageDispatch.DIRECT

    async def execute_requests(
        self,
        pages: Mapping[str, Request],
        *,
        context: ExecutionContext,
        advisory_totals: bool = False,
        strict_envelope: bool = False,
        strict_json_members: bool = False,
    ) -> tuple[BatchOutcome, ...]:
        """Execute one wave of scheduled pages, by command id, as one private physical batch.

        A wave the traversal's end interrupts, its cancellation included, sends nothing more: each of its pages
        settles from what its own command is known to have done, as the shared engine settles a binding's wave.
        """
        progress = BatchProgress()
        try:
            return await self._batches.execute_requests(
                tuple(pages.values()),
                context=context,
                advisory_totals=advisory_totals,
                strict_envelope=strict_envelope,
                strict_json_members=strict_json_members,
                progress=progress,
            )
        except BaseException:
            for index, command_id in enumerate(pages):
                self._ledger.settle(command_id, progress.settlement(index))
            raise

    async def execute_page(self, request: Request, *, command_id: str, context: ExecutionContext) -> BatchOutcome:
        """Execute one finish page as a direct request; its failure, raised by the executor, settles the page."""
        try:
            response = await self._executor.execute(request, context=context)
        except BaseException as error:
            self._ledger.settle(command_id, settlement(error))
            raise
        return BatchSuccess(0, "finish", request, response.result, response=response)


__all__ = ["FlatWaveEngine"]
