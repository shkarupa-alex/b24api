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

from b24api.contracts.completion import CommandSettlement
from b24api.contracts.report import PageDispatch
from b24api.errors import CapabilityError
from b24api.references.dispatch import _BatchPageError, _DecodeOptions, _failed_settlement

if TYPE_CHECKING:
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

    @property
    def portal_command_cap(self) -> int:
        """Return the shared dispatcher's command cap: one wave fits one shared physical batch."""
        return self._cap

    @property
    def page_dispatch(self) -> PageDispatch:
        """Return ``BATCH``: a finish page travels as one command of a shared physical batch."""
        return PageDispatch.BATCH

    async def execute_page(self, request: Request, *, context: ExecutionContext) -> BatchOutcome:
        """Queue one finish page as a one-command wave, decoded as strictly as every wave command."""
        (outcome,) = await self.execute_requests(
            (request,),
            context=context,
            strict_envelope=True,
            strict_json_members=True,
        )
        return outcome

    async def execute_requests(
        self,
        requests: tuple[Request, ...],
        *,
        context: ExecutionContext,
        advisory_totals: bool = False,
        strict_envelope: bool = False,
        strict_json_members: bool = False,
    ) -> tuple[BatchOutcome, ...]:
        """Queue one atomic wave and return its correlated outcomes in request order."""
        if context is not self._context:
            raise RuntimeError("a fast keyset wave must be charged to the shared operation context")
        if not requests or len(requests) > self._cap:
            raise ValueError("fast keyset wave must contain 1..portal_command_cap requests")
        options = _DecodeOptions(advisory_totals, strict_envelope, strict_json_members)
        errors: dict[int, BaseException] = {}
        key = self._run.producer_key
        self._state.runnable.discard(key)
        self._state.touch()
        tasks = self._start(requests, options, errors)
        try:
            await asyncio.wait(tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._ledger.settle_wave(
                tuple(_settlement(task, errors.get(index)) for index, task in enumerate(tasks)),
            )
            raise
        # Each command's error is already recorded in ``errors``; retrieve it so it is not logged as lost.
        await asyncio.gather(*tasks, return_exceptions=True)
        self._state.runnable.add(key)
        self._state.touch()
        outcomes: list[BatchOutcome] = []
        failure: BaseException | None = None
        for index, task in enumerate(tasks):
            error = errors.get(index)
            if error is None:
                outcome = task.result().outcome
                if outcome is None:
                    error = errors[index] = CapabilityError("batch page response metadata is unavailable")
                else:
                    outcomes.append(outcome)
                    self._run.page_state += 1
                    continue
            if isinstance(error, _BatchPageError):
                outcomes.append(error.failure)
                continue
            failure = failure or error
        if failure is not None:
            # A whole-envelope failure is raised, as the flat engine raises it; every command settles first.
            self._ledger.settle_wave(
                tuple(_settlement(task, errors.get(index)) for index, task in enumerate(tasks)),
            )
            raise failure
        return tuple(outcomes)

    def _start(
        self,
        requests: tuple[Request, ...],
        options: _DecodeOptions,
        errors: dict[int, BaseException],
    ) -> tuple[asyncio.Task[_DispatchedPage], ...]:
        """Start one task per command, each counted in transit until its command reaches the queue."""
        tasks: list[asyncio.Task[_DispatchedPage]] = []
        for index, request in enumerate(requests):
            transit = self._state.depart(self._run.producer_key)
            task = asyncio.create_task(self._fetch(index, request, options, errors, transit))
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


def _settlement(task: asyncio.Task[_DispatchedPage], error: BaseException | None) -> CommandSettlement:
    """Classify one wave command from what its dispatch proved."""
    if error is not None:
        return _failed_settlement(error)
    if task.done() and not task.cancelled():
        return CommandSettlement.SUCCESS
    return CommandSettlement.NOT_EXECUTED


__all__ = ["SharedWaveEngine", "WaveHost"]
