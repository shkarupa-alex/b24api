"""Counted references on the shared queue: the head and the dense tail ride the shared dispatcher.

The head transaction and the ordered tail validation are the flat ones (``counted_batch.py``). This
module supplies only the reference seams: the head is one shared-queue page of the binding, its
selected rows are charged to the binding's own row reservation in the shared pool (never to the
whole operation counter), and the dense tail plans its pages lazily into the shared dispatcher, each
page reserving its rows before dispatch and committing strictly in offset order.
"""

from __future__ import annotations
import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from b24api.completion.closure import QUALIFIED_TOTAL_REACHED
from b24api.contracts.completion import CommandSettlement
from b24api.contracts.report import PageDispatch
from b24api.errors import CapabilityError
from b24api.references.dispatch import _BatchPageError, _failed_dispatch, _failed_settlement
from b24api.traversal.counted_io import HeadAttempt, TailReceipt

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from b24api.batch.engine import _BatchInput
    from b24api.batch.outcome import BatchFailure
    from b24api.contracts.policy import ExecutionPolicy
    from b24api.contracts.request import Request
    from b24api.execution import ExecutionContext
    from b24api.execution.snapshot import KernelReport
    from b24api.references.dispatch import _DispatchedPage, _ProducerState, _Reservation, _RowBuffer, _Transit
    from b24api.references.run import _ReferenceRun
    from b24api.traversal.strategy_context import StrategyContext


class _PageFetcher(Protocol):
    async def fetch(
        self, request: Request, reference_id: str, *, transit: _Transit | None = None
    ) -> _DispatchedPage: ...


class CountedHost(Protocol):
    """The scheduler members the shared counted seams use."""

    @property
    def buffer(self) -> _RowBuffer:
        """Return the shared row pool."""
        ...

    @property
    def dispatcher(self) -> _PageFetcher:
        """Return the shared page dispatcher."""
        ...

    @property
    def producer_state(self) -> _ProducerState:
        """Return the advisory producer availability the dispatcher coalesces on."""
        ...

    @property
    def page_dispatch(self) -> PageDispatch:
        """Return the physical dispatch every page travels by."""
        ...

    def take_page_sequence(self) -> int:
        """Return the next operation-wide page sequence."""
        ...

    async def dispatch_page(self, run: _ReferenceRun, request: Request) -> _DispatchedPage:
        """Schedule, reserve and dispatch one binding page."""
        ...

    async def abandon_page(self, run: _ReferenceRun) -> None:
        """Return the row capacity of a page whose dispatch raised."""
        ...


class LeaseCountedRowAccount:
    """Charge a counted head's selected rows to its own page reservation in the shared row pool."""

    def __init__(self, host: CountedHost, run: _ReferenceRun) -> None:
        """Bind the shared pool and the binding whose head reservation is charged."""
        self._host = host
        self._run = run

    async def hold(self, rows: int) -> None:
        """Accept the selected head rows into the head's reservation (one shared counter delta)."""
        reservation = self._run.reservation
        if reservation is None:
            raise RuntimeError("counted head lacks its row reservation")
        await self._host.buffer.accept(reservation, rows)

    async def release_held(self) -> None:
        """Return held head rows the consumer did not take; a delivered head holds none."""
        reservation = self._run.reservation
        if reservation is not None and reservation.amount:
            await self._host.buffer.abort(reservation)


class SharedCountedWaveIO:
    """A reference binding's counted seams: shared dispatcher, shared row pool, binding recorder."""

    head_dispatch = PageDispatch.BATCH
    allows_sequential = True
    completed_reason = QUALIFIED_TOTAL_REACHED

    def __init__(self, host: CountedHost, run: _ReferenceRun) -> None:
        """Bind the scheduler that owns the shared queue and the binding's run state."""
        self._host = host
        self._run = run
        self.rows = LeaseCountedRowAccount(host, run)

    async def execute_head(self, request: Request, *, context: ExecutionContext) -> HeadAttempt:
        """Schedule the head as one shared-queue page of the binding; a failure is raised."""
        del context  # the shared dispatcher already runs on the operation context
        try:
            dispatched = await self._host.dispatch_page(self._run, request)
        except BaseException:
            await self._host.abandon_page(self._run)
            raise
        return HeadAttempt(
            dispatched.response,
            None,
            dispatched.dispatch,
            dispatched.batch_index,
            dispatch_started=True,
            settlement=CommandSettlement.SUCCESS,
        )

    def head_failure(self, error: BaseException) -> HeadAttempt:
        """Return the actual batch provenance of a failed head and its settlement."""
        failed = _failed_dispatch(self._host.page_dispatch, error)
        return HeadAttempt(
            None,
            failed.error or error,
            failed.dispatch,
            failed.batch_index,
            dispatch_started=isinstance(error, _BatchPageError)
            or bool(getattr(error, "_b24api_dispatch_started", False)),
            settlement=_failed_settlement(error),
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
        """Refuse a tail the logical page budget cannot hold; return the bounded in-flight tail window."""
        del stride
        policy = ctx.context.policy
        cap = self._run.program.page_cap
        tail_pages = (total - 1) // cap if total > head_rows else 0
        budget = await ctx.context.snapshot()
        if budget.counters.logical_pages + tail_pages > policy.max_pages:
            raise CapabilityError("parallel counted traversal exceeds the logical page budget")
        return max(1, min(batch_size, policy.max_buffered_rows // cap))

    def open_tail(
        self,
        requests: Iterator[_BatchInput],
        *,
        batch_size: int,
        policy: ExecutionPolicy,
        context: ExecutionContext,
    ) -> ReferenceTailStream:
        """Stream the dense tail through the shared dispatcher with ``batch_size`` pages in flight."""
        del policy, context
        return ReferenceTailStream(self._host, self._run, requests, window=batch_size)


@dataclass(slots=True, eq=False)
class _TailSlot:
    ordinal: int
    page_id: int
    sequence: int
    reservation: _Reservation | None = None
    task: asyncio.Task[_DispatchedPage] | None = None
    error: BaseException | None = field(default=None, repr=False)
    consumed: bool = False


class ReferenceTailStream:
    """Lazily planned dense tail pages in the shared queue, yielded in planned offset order.

    A background submitter reserves each page's rows, schedules its completion page and hands it to
    the shared dispatcher, at most ``window`` pages ahead of the consumer. Receipts are yielded in
    offset order; ``aclose`` settles every page the binding will not commit exactly once: never sent
    as not executed, sent without an observed outcome as unknown, settled but unconsumed as aborted.
    """

    report: KernelReport | None = None

    def __init__(
        self,
        host: CountedHost,
        run: _ReferenceRun,
        requests: Iterator[_BatchInput],
        *,
        window: int,
    ) -> None:
        """Hold the planned requests; nothing is reserved or sent before iteration starts."""
        self._host = host
        self._run = run
        self._requests = requests
        self._window = asyncio.Semaphore(max(1, window))
        self._slots: dict[int, _TailSlot] = {}
        self._changed = asyncio.Event()
        self._submitter: asyncio.Task[None] | None = None
        self._closed = False

    async def __aiter__(self) -> AsyncIterator[TailReceipt]:
        """Yield each planned page's receipt in offset order."""
        self._run.tail_active = True
        self._submitter = asyncio.create_task(self._submit())
        ordinal = 0
        while True:
            slot = await self._slot(ordinal)
            if slot is None:
                return
            ordinal += 1
            yield await self._receipt(slot)
            self._window.release()

    async def _submit(self) -> None:
        """Plan, reserve and dispatch tail pages ahead of the consumer, bounded by the window."""
        host, run = self._host, self._run
        state, key = host.producer_state, run.producer_key
        try:
            for ordinal, item in enumerate(self._requests):
                if self._window.locked():
                    state.runnable.discard(key)
                    state.touch()
                await self._window.acquire()
                state.runnable.add(key)
                state.touch()
                slot = _TailSlot(ordinal, run.completion.reserve(), host.take_page_sequence())
                self._slots[ordinal] = slot
                slot.reservation = await host.buffer.reserve(run.work.index, run.program.page_cap)
                transit = state.depart(key)
                slot.task = asyncio.create_task(self._fetch(slot, item.request, transit))
                transit.carried_by(slot.task)
                self._changed.set()
        finally:
            state.runnable.discard(key)
            state.touch()
            self._changed.set()

    async def _fetch(self, slot: _TailSlot, request: Request, transit: _Transit) -> _DispatchedPage:
        try:
            dispatched = await self._host.dispatcher.fetch(request, self._run.producer_key, transit=transit)
        except BaseException as error:
            slot.error = error
            raise
        # The page's rows were reserved before dispatch, so the sender's admission and settlement
        # waits never depend on this binding's ordered consumer.
        for future in (dispatched.admission, dispatched.settlement):
            if future is not None and not future.done():
                future.set_result(None)
        return dispatched

    async def _slot(self, ordinal: int) -> _TailSlot | None:
        """Wait until the page at ``ordinal`` is dispatched, or the submitter ends without it."""
        submitter = self._submitter
        if submitter is None:
            raise RuntimeError("counted tail iteration has not started")
        while True:
            self._changed.clear()
            slot = self._slots.get(ordinal)
            if slot is not None and slot.task is not None:
                return slot
            if submitter.done():
                error = None if submitter.cancelled() else submitter.exception()
                if error is not None:
                    raise error
                if slot is None:
                    return None
                raise RuntimeError("counted tail page lacks its dispatch")
            await self._changed.wait()

    async def _receipt(self, slot: _TailSlot) -> TailReceipt:
        """Take ownership of one dispatched page: its reservation becomes the binding's page reservation."""
        task = slot.task
        if task is None:
            raise RuntimeError("counted tail page lacks its dispatch")
        run = self._run
        try:
            dispatched = await task
        except _BatchPageError as error:
            self._take(slot)
            return TailReceipt(slot.ordinal, None, error.failure, error.failure.command_index)
        self._take(slot)
        run.page_state += 1
        return TailReceipt(slot.ordinal, dispatched.response, None, dispatched.batch_index)

    def _take(self, slot: _TailSlot) -> None:
        run = self._run
        slot.consumed = True
        run.reservation, slot.reservation = slot.reservation, None
        run.scheduled_sequences.append(slot.sequence)
        run.completion.activate(slot.page_id)

    def settle(self, receipt: TailReceipt) -> None:
        """Record the activated page's physical command outcome."""
        failure = receipt.failure
        self._run.completion.settled(
            CommandSettlement.SUCCESS if failure is None else _failed_settlement(_BatchPageError(failure))
        )

    def failure(self, receipt: TailReceipt, outcome: BatchFailure) -> BaseException:
        """Return the binding failure carrying the command's replay disposition."""
        del receipt
        return _BatchPageError(outcome)

    async def aclose(self) -> None:
        """Stop planning, then settle and release every page the binding will not commit, once."""
        if self._closed:
            return
        self._closed = True
        submitter = self._submitter
        if submitter is not None and not submitter.done():
            submitter.cancel()
        if submitter is not None:
            await asyncio.gather(submitter, return_exceptions=True)
        pending = [slot for slot in self._slots.values() if not slot.consumed]
        for slot in pending:
            if slot.task is not None and not slot.task.done():
                slot.task.cancel()
        await asyncio.gather(*(slot.task for slot in pending if slot.task is not None), return_exceptions=True)
        for slot in sorted(pending, key=lambda item: item.ordinal):
            self._run.completion.abandon_reserved(slot.page_id, _abandoned_settlement(slot))
            if slot.reservation is not None:
                await self._host.buffer.abort(slot.reservation)
                slot.reservation = None
        self._slots.clear()


def _abandoned_settlement(slot: _TailSlot) -> CommandSettlement:
    """Classify an uncommitted tail page from what its dispatch proved."""
    task = slot.task
    if task is None:
        return CommandSettlement.NOT_EXECUTED
    if not task.cancelled() and task.exception() is None:
        return CommandSettlement.SUCCESS
    error = slot.error
    return CommandSettlement.NOT_EXECUTED if error is None else _failed_settlement(error)


__all__ = ["LeaseCountedRowAccount", "ReferenceTailStream", "SharedCountedWaveIO"]
