"""Bounded fair scheduling for independent and paginated references."""

from __future__ import annotations
import asyncio
import contextlib
from collections.abc import AsyncIterable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from b24api.batch.engine import BatchExecutor
from b24api.batch.outcome import BatchFailure
from b24api.contracts.policy import ReplayDisposition
from b24api.errors import BudgetExceededError, CapabilityError
from b24api.execution import (
    ExecutionContext,
    Executor,
    WorkClass,
)
from b24api.references.outcome import (
    ReferenceFailure,
    ReferenceItem,
    ReferenceRequest,
)

if TYPE_CHECKING:
    from b24api.contracts.json import JsonValue
    from b24api.contracts.report import Violation
    from b24api.contracts.request import Request
    from b24api.contracts.response import Response
    from b24api.plans import (
        BatchDispatch,
        DirectDispatch,
    )

type ReferenceSource = Iterable[ReferenceRequest] | AsyncIterable[ReferenceRequest]
_MISSING = object()
_SYNC_EXHAUSTED = object()
_BATCH_COALESCE_IDLE_TURNS = 16


@dataclass(frozen=True, slots=True)
class _Work:
    index: int
    reference: ReferenceRequest


@dataclass(slots=True)
class _Reservation:
    index: int
    amount: int
    accepted: bool = False


@dataclass(slots=True)
class _PageEvent:
    work: _Work
    items: tuple[JsonValue, ...]
    response: Response
    item_weights: tuple[int, ...]
    unique_mask: tuple[bool, ...]
    violations: tuple[Violation, ...]
    reservation: _Reservation
    acknowledged: asyncio.Future[None]


@dataclass(frozen=True, slots=True)
class _DoneEvent:
    work: _Work
    row_count: int
    violations: tuple[Violation, ...]


@dataclass(frozen=True, slots=True)
class _FailureEvent:
    work: _Work
    error: BaseException
    cursor: JsonValue
    page_state: int
    partial_rows: int
    violations: tuple[Violation, ...]
    replay_disposition: ReplayDisposition = ReplayDisposition.NOT_ELIGIBLE


type _Event = _PageEvent | _DoneEvent | _FailureEvent


@dataclass(frozen=True, slots=True)
class _KernelReferenceComplete:
    work_index: int
    reference: ReferenceRequest
    row_count: int


@dataclass(frozen=True, slots=True)
class _KernelFanOutSuccess:
    work_index: int
    reference: ReferenceRequest
    response: Response


type ReferenceStreamItem = ReferenceItem | ReferenceFailure | _KernelReferenceComplete | _KernelFanOutSuccess


class _ReferenceWindowError(Exception):
    def __init__(self, failure: ReferenceFailure) -> None:
        self.failure = failure
        super().__init__("reference traversal window failed")


@dataclass(slots=True)
class _PendingBatch:
    request: Request
    reference_id: str
    future: asyncio.Future[Response]


@dataclass(slots=True)
class _AdmissionState:
    tasks: dict[int, asyncio.Task[None]]
    queues: dict[int, asyncio.Queue[_Event]]
    ready: asyncio.Queue[_Event]
    slots: asyncio.Semaphore
    changed: asyncio.Event


class _BatchPageError(Exception):
    def __init__(self, failure: BatchFailure) -> None:
        self.failure = failure
        super().__init__("batch page command failed")


class _RowBuffer:
    """Reserve page capacity before I/O and account decoded retained rows exactly."""

    def __init__(self, maximum: int, context: ExecutionContext, *, head_reserve: int = 0) -> None:
        self.maximum = maximum
        self.context = context
        self._available = maximum
        self._accounted = 0
        self._head_index = 0
        self._head_reserve = head_reserve
        self._closed = False
        self._condition = asyncio.Condition()
        self._reservations: list[_Reservation] = []

    async def reserve(self, index: int, amount: int) -> _Reservation:
        if amount > self.maximum:
            raise BudgetExceededError("one decoded page exceeds the global row buffer")
        async with self._condition:
            await self._condition.wait_for(lambda: self._closed or self._can_reserve(index, amount))
            if self._closed:
                raise asyncio.CancelledError
            self._available -= amount
            reservation = _Reservation(index, amount)
            self._reservations.append(reservation)
            return reservation

    def _can_reserve(self, index: int, amount: int) -> bool:
        if self._available < amount:
            return False
        if self._head_reserve == 0 or index == self._head_index:
            return True
        head_has_capacity = any(
            reservation.index == self._head_index and reservation.amount > 0 for reservation in self._reservations
        )
        required_head_reserve = 0 if head_has_capacity else self._head_reserve
        return self._available - amount >= required_head_reserve

    async def accept(self, reservation: _Reservation, actual: int) -> None:
        if actual > reservation.amount:
            await self.abort(reservation)
            raise BudgetExceededError("decoded page exceeded its reserved row capacity")
        async with self._condition:
            if self._closed:
                raise asyncio.CancelledError
            unused = reservation.amount - actual
            reservation.amount = actual
            reservation.accepted = True
            self._available += unused
            self._accounted += actual
            if actual == 0:
                self._reservations.remove(reservation)
            self._condition.notify_all()
        if actual:
            await self.context.adjust_buffered_rows(actual)

    async def release(self, reservation: _Reservation, count: int) -> None:
        if count < 0 or count > reservation.amount:
            raise ValueError("invalid row-buffer release")
        if not count:
            return
        async with self._condition:
            if reservation.amount == 0:
                return
            reservation.amount -= count
            self._available += count
            self._accounted -= count
            if reservation.amount == 0:
                self._reservations.remove(reservation)
            self._condition.notify_all()
        await self.context.adjust_buffered_rows(-count)

    async def abort(self, reservation: _Reservation) -> None:
        amount = reservation.amount
        if not amount:
            return
        async with self._condition:
            amount = reservation.amount
            if not amount:
                return
            reservation.amount = 0
            self._available += amount
            if reservation.accepted:
                self._accounted -= amount
            self._reservations.remove(reservation)
            self._condition.notify_all()
        if reservation.accepted:
            await self.context.adjust_buffered_rows(-amount)

    async def advance_head(self, index: int, reserve: int) -> None:
        async with self._condition:
            self._head_index = index
            self._head_reserve = reserve
            self._condition.notify_all()

    async def close(self) -> None:
        async with self._condition:
            if self._closed:
                return
            self._closed = True
            self._accounted = 0
            self._available = self.maximum
            for reservation in self._reservations:
                reservation.amount = 0
            self._reservations.clear()
            self._condition.notify_all()
        await self.context.set_buffered_rows(0)


class _DirectPageDispatcher:
    def __init__(
        self,
        executor: Executor,
        context: ExecutionContext,
        plan: DirectDispatch,
    ) -> None:
        self.executor = executor
        self.context = context
        concurrency = min(plan.concurrency, context.policy.max_direct_concurrency)
        self._semaphore = asyncio.Semaphore(concurrency)
        self.batch_requests = 0
        self.batch_commands = 0

    async def fetch(self, request: Request, reference_id: str) -> Response:
        reservation = await self.context.reserve_page(reference=reference_id)
        remaining = self.context.policy.max_elapsed - self.context.elapsed
        if remaining <= 0:
            self.context.release_page(reservation)
            raise BudgetExceededError("operation elapsed budget exhausted before direct admission")
        try:
            async with asyncio.timeout(remaining):
                async with self._semaphore:
                    response = await self.executor.execute(
                        request,
                        context=self.context,
                        work_class=WorkClass.TRAVERSAL_DIRECT,
                    )
                    self.context.commit_page(reservation)
        except TimeoutError as error:
            self.context.release_page(reservation)
            raise BudgetExceededError("direct scheduler admission exceeded operation time budget") from error
        except BaseException:
            self.context.release_page(reservation)
            raise
        return response

    async def aclose(self) -> None:
        return


class _BatchPageDispatcher:
    def __init__(
        self,
        executor: Executor,
        context: ExecutionContext,
        plan: BatchDispatch,
    ) -> None:
        self.context = context
        self.plan = plan
        self._executor = BatchExecutor(executor)
        self._queue: asyncio.Queue[_PendingBatch | None] = asyncio.Queue(
            maxsize=context.policy.max_active_references,
        )
        self._worker: asyncio.Task[None] | None = None
        self._workers: tuple[asyncio.Task[None], ...] = ()
        self._closed = False
        self.batch_requests = 0
        self.batch_commands = 0

    async def fetch(self, request: Request, reference_id: str) -> Response:
        if self._closed:
            raise RuntimeError("batch page dispatcher is closed")
        if self._worker is None:
            concurrency = min(self.plan.concurrency, self.context.policy.max_active_references)
            self._workers = tuple(asyncio.create_task(self._run()) for _index in range(concurrency))
            self._worker = self._workers[0]
        reservation = await self.context.reserve_page(reference=reference_id)
        future: asyncio.Future[Response] = asyncio.get_running_loop().create_future()
        pending = _PendingBatch(request, reference_id, future)
        remaining = self.context.policy.max_elapsed - self.context.elapsed
        if remaining <= 0:
            self.context.release_page(reservation)
            raise BudgetExceededError("operation elapsed budget exhausted before batch admission")
        try:
            async with asyncio.timeout(remaining):
                await self._queue.put(pending)
                response = await future
                self.context.commit_page(reservation)
                return response
        except asyncio.CancelledError:
            future.cancel()
            self.context.release_page(reservation)
            raise
        except TimeoutError as error:
            future.cancel()
            self.context.release_page(reservation)
            raise BudgetExceededError("batch scheduler admission exceeded operation time budget") from error
        except BaseException:
            self.context.release_page(reservation)
            raise

    async def _run(self) -> None:  # noqa: C901, PLR0912
        while not self._closed:
            first = await self._queue.get()
            if first is None:
                return
            if first.future.done():
                continue
            chunk = [first]
            idle_turns = 0
            stop_requested = False
            while len(chunk) < self.plan.batch_size and idle_turns < _BATCH_COALESCE_IDLE_TURNS:
                await asyncio.sleep(0)
                added = False
                while len(chunk) < self.plan.batch_size:
                    try:
                        pending = self._queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if pending is None:
                        stop_requested = True
                        break
                    if pending.future.done():
                        continue
                    chunk.append(pending)
                    added = True
                if stop_requested:
                    break
                idle_turns = 0 if added else idle_turns + 1
            self.batch_requests += 1
            self.batch_commands += len(chunk)
            try:
                outcomes = await self._executor.execute_requests(
                    tuple(item.request for item in chunk),
                    context=self.context,
                )
            except asyncio.CancelledError:
                for item in chunk:
                    item.future.cancel()
                raise
            except Exception as error:  # noqa: BLE001 - total chunk correlation boundary
                for item in chunk:
                    if not item.future.done():
                        item.future.set_exception(error)
                continue
            for item, outcome in zip(chunk, outcomes, strict=True):
                if item.future.done():
                    continue
                if isinstance(outcome, BatchFailure):
                    item.future.set_exception(_BatchPageError(outcome))
                    continue
                success = outcome
                if success.response is None:
                    item.future.set_exception(CapabilityError("batch page response metadata is unavailable"))
                    continue
                item.future.set_result(success.response)
            if stop_requested:
                return

    async def aclose(self) -> None:  # noqa: C901 - closes every independently owned batch worker
        if self._closed:
            return
        self._closed = True
        if self._worker is None:
            return
        for worker in self._workers:
            if not worker.done():
                worker.cancel()
        try:
            await asyncio.sleep(0)
            active = tuple(worker for worker in self._workers if not worker.done())
            if active:
                remaining = max(0.0, self.context.policy.max_elapsed - self.context.elapsed)
                _, pending_workers = await asyncio.wait(active, timeout=remaining)
                if pending_workers:
                    raise BudgetExceededError("batch dispatcher cleanup exceeded operation time budget")
            for worker in self._workers:
                with contextlib.suppress(asyncio.CancelledError):
                    await worker
        finally:
            while True:
                try:
                    pending = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if pending is not None and not pending.future.done():
                    pending.future.cancel()
