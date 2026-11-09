"""The wave engine of a flat fast keyset traversal.

A flat traversal owns its physical requests: a wave travels as one private physical batch, and a
sequential finish page, which no other command could share, travels as one direct request.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.batch.engine import BatchExecutor
from b24api.batch.outcome import BatchSuccess
from b24api.contracts.report import PageDispatch

if TYPE_CHECKING:
    from b24api.batch.outcome import BatchOutcome
    from b24api.contracts.request import Request
    from b24api.execution import ExecutionContext, Executor


class FlatWaveEngine:
    """``WaveEngine`` of a flat traversal: private batches for waves, a direct request per finish page."""

    def __init__(self, executor: Executor) -> None:
        """Bind the operation executor and its private batch executor."""
        self._executor = executor
        self._batches = BatchExecutor(executor)

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
        requests: tuple[Request, ...],
        *,
        context: ExecutionContext,
        advisory_totals: bool = False,
        strict_envelope: bool = False,
        strict_json_members: bool = False,
    ) -> tuple[BatchOutcome, ...]:
        """Execute one wave as one private physical batch."""
        return await self._batches.execute_requests(
            requests,
            context=context,
            advisory_totals=advisory_totals,
            strict_envelope=strict_envelope,
            strict_json_members=strict_json_members,
        )

    async def execute_page(self, request: Request, *, context: ExecutionContext) -> BatchOutcome:
        """Execute one finish page as a direct request; its failure is raised by the executor."""
        response = await self._executor.execute(request, context=context)
        return BatchSuccess(0, "finish", request, response.result, response=response)


__all__ = ["FlatWaveEngine"]
