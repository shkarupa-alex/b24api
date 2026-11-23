"""A fast keyset reference binding's wave engine: waves and finish pages through the shared dispatcher.

The binding's runtime fits each wave into its own row lease before calling the engine and reserves
the wave's logical pages itself, so the engine only queues the commands in the shared dispatcher
(which coalesces them with other bindings' commands of the same decoding options) and returns one
correlated outcome per command once every command has settled. A sequential finish page is a
one-command wave: it shares a physical batch with the other bindings' commands instead of costing
a direct request of its own.
"""

from __future__ import annotations
import asyncio
from typing import TYPE_CHECKING, Protocol

from b24api.batch.outcome import settlement_of
from b24api.contracts.completion import CommandSettlement
from b24api.contracts.report import PageDispatch
from b24api.errors import CapabilityError
from b24api.execution.context import Settled, spawn_settled
from b24api.references.dispatch import _DecodeOptions, _failed_page, _Producing
from b24api.traversal.keyset_context import FailedWavePage

if TYPE_CHECKING:
    from collections.abc import Mapping

    from b24api.batch.outcome import BatchOutcome
    from b24api.completion.reference_recorder import ReferenceFastLedger
    from b24api.contracts.request import Request
    from b24api.execution import ExecutionContext
    from b24api.references.dispatch import _BatchPageDispatcher, _DispatchedPage, _ProducerState, _Transit
    from b24api.references.run import _ReferenceRun


class WaveHost(Protocol):
    """The scheduler members a shared fast wave travels through."""

    @property
    def context(self) -> ExecutionContext:
        """Return the shared operation context."""
        ...

    @property
    def producer_state(self) -> _ProducerState:
        """Return the advisory producer availability the dispatcher coalesces on."""
        ...

    @property
    def batch_dispatcher(self) -> _BatchPageDispatcher:
        """Return the shared batch dispatcher."""
        ...

    @property
    def wave_command_cap(self) -> int:
        """Return the most commands one shared physical batch carries."""
        ...


class SharedWaveEngine:
    """``WaveEngine`` of one reference binding over the operation's shared batch dispatcher."""

    def __init__(self, host: WaveHost, run: _ReferenceRun, ledger: ReferenceFastLedger) -> None:
        """Bind the shared dispatcher, the operation context and the binding's run and ledger."""
        self._dispatcher = host.batch_dispatcher
        self._state = host.producer_state
        self._run = run
        self._ledger = ledger
        self._context = host.context
        self._cap = host.wave_command_cap
        self._failed: tuple[BaseException, tuple[FailedWavePage, ...]] | None = None

    @property
    def portal_command_cap(self) -> int:
        """Return the shared dispatcher's command cap: one wave fits one shared physical batch."""
        return self._cap

    @property
    def page_dispatch(self) -> PageDispatch:
        """Return ``BATCH``: a finish page travels as one command of a shared physical batch."""
        return PageDispatch.BATCH

    async def execute_page(self, request: Request, *, command_id: str, context: ExecutionContext) -> BatchOutcome:
        """Queue one finish page as a one-command wave, decoded as strictly as every wave command."""
        (outcome,) = await self.execute_requests(
            {command_id: request},
            context=context,
            strict_envelope=True,
        )
        return outcome

    async def execute_requests(
        self,
        pages: Mapping[str, Request],
        *,
        context: ExecutionContext,
        advisory_totals: bool = False,
        strict_envelope: bool = False,
    ) -> tuple[BatchOutcome, ...]:
        """Queue one atomic wave of scheduled pages, by command id, and return their outcomes in that order."""
        if context is not self._context:
            raise RuntimeError("a fast keyset wave must be charged to the shared operation context")
        if not pages or len(pages) > self._cap:
            raise ValueError("fast keyset wave must contain 1..portal_command_cap requests")
        options = _DecodeOptions(advisory_totals, strict_envelope)
        errors: dict[int, BaseException] = {}
        key = self._run.producer_key
        self._state.move(key, _Producing.WAITING)
        tasks = self._start(tuple(pages.values()), options, errors)
        try:
            await asyncio.wait(tasks)
            # Each command's error is already recorded in ``errors``; retrieve it so it is not logged as lost.
            await asyncio.gather(*tasks, return_exceptions=True)
            outcomes = self._correlate(tasks, errors)
        except BaseException as error:
            # A wave that ends early, or whose envelope failed as a whole, settles each of its pages first.
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._settle(tuple(pages), tasks, errors, error)
            raise
        # Only a wave the runtime receives may lead to another: a whole-envelope failure ends the binding.
        self._state.move(key, _Producing.RUNNABLE)
        return outcomes

    def _correlate(
        self, tasks: tuple[asyncio.Task[Settled[_DispatchedPage]], ...], errors: dict[int, BaseException]
    ) -> tuple[BatchOutcome, ...]:
        """Return each command's outcome in order; raise a whole-envelope failure, as the flat engine raises it."""
        outcomes: list[BatchOutcome] = []
        failure: BaseException | None = None
        for index, task in enumerate(tasks):
            error = errors.get(index)
            if error is None:
                outcome = task.result().unwrap().outcome
                if outcome is None:
                    error = errors[index] = CapabilityError("batch page response metadata is unavailable")
                else:
                    outcomes.append(outcome)
                    self._run.page_state += 1
                    continue
            failed = _failed_page(error)
            if failed.failure is not None:  # the command's own failure is its outcome
                outcomes.append(failed.failure)
                continue
            failure = failure or failed.error
        if failure is not None:
            raise failure
        return tuple(outcomes)

    def _settle(
        self,
        command_ids: tuple[str, ...],
        tasks: tuple[asyncio.Task[Settled[_DispatchedPage]], ...],
        errors: dict[int, BaseException],
        raised: BaseException,
    ) -> None:
        """Settle every page of a wave whose call raises, by id, from what its own command's dispatch proved."""
        failed = tuple(_failed_wave_page(task, errors.get(index)) for index, task in enumerate(tasks))
        for command_id, page in zip(command_ids, failed, strict=True):
            self._ledger.settle(command_id, page.settlement)
        self._failed = (raised, failed)

    def failed_pages(self, error: BaseException) -> tuple[FailedWavePage, ...] | None:
        """Return the pages of the latest call that ``error`` failed, as the engine settled them."""
        failed = self._failed
        return failed[1] if failed is not None and failed[0] is error else None

    def _start(
        self,
        requests: tuple[Request, ...],
        options: _DecodeOptions,
        errors: dict[int, BaseException],
    ) -> tuple[asyncio.Task[Settled[_DispatchedPage]], ...]:
        """Start one task per command, each counted in transit until its command reaches the queue.

        A command's task returns how its fetch ended (``Settled``): the fetch runs the caller's transport.
        """
        tasks: list[asyncio.Task[Settled[_DispatchedPage]]] = []
        for index, request in enumerate(requests):
            transit = self._state.depart(self._run.producer_key, page_reserved=True)
            task = spawn_settled(self._fetch(index, request, options, errors, transit))
            transit.carried_by(task)
            tasks.append(task)
        return tuple(tasks)

    async def _fetch(
        self,
        index: int,
        request: Request,
        options: _DecodeOptions,
        errors: dict[int, BaseException],
        transit: _Transit,
    ) -> _DispatchedPage:
        try:
            dispatched = await self._dispatcher.fetch(
                request,
                self._run.producer_key,
                options=options,
                page_reserved=True,
                transit=transit,
            )
        except BaseException as error:
            errors[index] = error
            raise
        # The wave's rows were charged to the binding's lease before dispatch, so the sender's
        # admission and settlement waits never depend on this binding's ordered consumer.
        for future in (dispatched.admission, dispatched.settlement):
            if future is not None and not future.done():
                future.set_result(None)
        return dispatched


def _failed_wave_page(task: asyncio.Task[Settled[_DispatchedPage]], error: BaseException | None) -> FailedWavePage:
    """Settle one wave command from what its dispatch proved: its failure, or its outcome (``settlement_of``).

    A task cancelled before it ran never handed its command to the dispatcher: it was not executed, in no batch.
    """
    if error is not None:
        failed = _failed_page(error)
        return FailedWavePage(failed.settlement, failed.batch_index)
    outcome = task.result().unwrap().outcome if task.done() and not task.cancelled() else None
    if outcome is None:
        return FailedWavePage(CommandSettlement.NOT_EXECUTED, None)
    return FailedWavePage(settlement_of(outcome), outcome.command_index)


__all__ = ["SharedWaveEngine", "WaveHost"]
