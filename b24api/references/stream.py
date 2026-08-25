"""Bounded fair scheduling for independent and paginated references."""

from __future__ import annotations
import asyncio
import contextlib
from collections.abc import AsyncGenerator, AsyncIterator
from dataclasses import replace
from typing import TYPE_CHECKING, Self, cast

from b24api.contracts.policy import (
    CompletionAssurance,
    ExecutionPolicy,
    KernelState,
    SnapshotRequirement,
    SnapshotState,
)
from b24api.contracts.report import Violation, ViolationSeverity
from b24api.execution import (
    Executor,
    await_cancellation_resistant,
    await_cleanup_resistant,
    rearm_cancellation,
)
from b24api.execution.snapshot import KernelReport
from b24api.plans import (
    BatchDispatch,
    DirectDispatch,
    DispatchPlan,
    ListPlan,
    ReferenceOutputOrder,
    SingleResponsePlan,
)
from b24api.references.dispatch import (
    _MISSING,
    ReferenceSource,
    ReferenceStreamItem,
)
from b24api.references.outcome import ReferenceItem
from b24api.references.scheduler import ReferenceScheduler
from b24api.references.support import _attach_report
from b24api.traversal import PaginationDriver

if TYPE_CHECKING:
    from b24api.contracts.request import IdentitySpec, ResultSelector


class ReferenceStream(AsyncIterator[ReferenceStreamItem]):
    """Lazy reference stream with one frozen report and deterministic cleanup."""

    def __init__(
        self,
        scheduler: ReferenceScheduler,
        source: ReferenceSource,
        *,
        assurance: CompletionAssurance = CompletionAssurance.CALLER_ASSERTED,
    ) -> None:
        """Initialize instance state."""
        self._scheduler = scheduler
        self._source = source
        self._runner: AsyncGenerator[ReferenceStreamItem] | None = None
        self._prefetched: ReferenceStreamItem | object = _MISSING
        self._closed = False
        self._emitted = 0
        self._unique_emitted = 0
        self._assurance = assurance
        self.report = KernelReport(assurance=assurance)

    def __aiter__(self) -> Self:
        """Return this asynchronous iterator."""
        return self

    @property
    def active_references_high_water(self) -> int:
        """Return the bounded scheduler admission high-water mark."""
        return self._scheduler.active_references_high_water

    async def __anext__(self) -> ReferenceStreamItem:
        """Return the next asynchronous item."""
        if self._closed:
            raise StopAsyncIteration
        if self._prefetched is not _MISSING:
            item = cast("ReferenceStreamItem", self._prefetched)
            self._prefetched = _MISSING
            self._record_delivery(item)
            return item
        if self._runner is None:
            self._runner = self._run()
        item = await anext(self._runner)
        self._record_delivery(item)
        return item

    def _record_delivery(self, item: ReferenceStreamItem) -> None:
        if isinstance(item, ReferenceItem):
            self._emitted += 1
            if self._scheduler.record_delivery(item):
                self._unique_emitted += 1

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        if self._closed:
            raise RuntimeError("stream is closed")
        if self._runner is None:
            self._runner = self._run()
            with contextlib.suppress(StopAsyncIteration):
                self._prefetched = await anext(self._runner)
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Exit the asynchronous context."""
        await self.aclose()

    async def aclose(self) -> None:
        """Close owned asynchronous resources."""
        if self._closed:
            await self._observe_source_cleanup()
            return
        self._closed = True
        try:
            if self._runner is not None:
                await self._runner.aclose()
        except BaseException as error:
            if self.report.state is KernelState.NOT_STARTED:
                cancellation = await await_cancellation_resistant(
                    self._finalize(KernelState.CANCELLED, "stream cleanup failed"),
                )
                if cancellation is not None:
                    _attach_report(cancellation, self.report)
                    raise cancellation from error
            _attach_report(error, self.report)
            raise
        finally:
            self._prefetched = _MISSING
        await self._observe_source_cleanup()
        if self.report.state is KernelState.NOT_STARTED and self._runner is not None:
            await self._finalize(KernelState.CANCELLED, "stream closed before exhaustion")

    async def _observe_source_cleanup(self) -> None:
        try:
            await self._scheduler.observe_source_cleanup()
        except BaseException as error:
            if self.report.state is KernelState.NOT_STARTED:
                cancellation = await await_cancellation_resistant(
                    self._finalize(KernelState.CANCELLED, "stream cleanup failed"),
                )
                if cancellation is not None:
                    _attach_report(cancellation, self.report)
                    raise cancellation from error
            _attach_report(error, self.report)
            raise

    async def _run(self) -> AsyncGenerator[ReferenceStreamItem]:  # noqa: C901, PLR0912, PLR0915
        outcomes = self._scheduler.outcomes(self._source)
        naturally_exhausted = False
        primary_error: BaseException | None = None
        pending_cancellation: asyncio.CancelledError | None = None
        try:
            async for outcome in outcomes:
                yield outcome
            naturally_exhausted = True
            await self._finalize(KernelState.COMPLETED, "reference input exhausted")
        except asyncio.CancelledError as error:
            primary_error = error
            repeated = await await_cancellation_resistant(
                self._finalize(KernelState.CANCELLED, "iteration cancelled"),
            )
            if repeated is not None:
                primary_error = repeated
                _attach_report(repeated, self.report)
                raise repeated from error
            _attach_report(error, self.report)
            raise
        except GeneratorExit as error:
            primary_error = error
            cancellation = await await_cancellation_resistant(
                self._finalize(KernelState.CANCELLED, "stream closed before exhaustion"),
            )
            if cancellation is not None:
                primary_error = cancellation
                _attach_report(cancellation, self.report)
                raise cancellation from error
            _attach_report(error, self.report)
            raise
        except BaseException as error:
            primary_error = error
            cancellation = await await_cancellation_resistant(
                self._finalize(KernelState.FAILED, type(error).__name__),
            )
            if cancellation is not None:
                _attach_report(cancellation, self.report)
                pending_cancellation = cancellation
            _attach_report(error, self.report)
            raise
        finally:
            preserve_primary = primary_error is not None and not isinstance(
                primary_error,
                asyncio.CancelledError | GeneratorExit,
            )
            cleanup = await await_cleanup_resistant(outcomes.aclose())
            if cleanup.error is not None:
                cleanup_error = cleanup.error
                if preserve_primary:
                    _attach_report(cleanup_error, self.report)
                    if isinstance(cleanup_error, asyncio.CancelledError):
                        pending_cancellation = cleanup_error
                    if cleanup.cancellation is not None:
                        pending_cancellation = cleanup.cancellation
                else:
                    await self._record_terminal_cleanup_failure(cleanup_error)
                    rearm_cancellation(cleanup.cancellation)
                    raise cleanup_error
            if cleanup.cancellation is not None and not preserve_primary and cleanup.error is None:
                _attach_report(cleanup.cancellation, self.report)
                raise cleanup.cancellation
            if cleanup.cancellation is not None and preserve_primary:
                pending_cancellation = cleanup.cancellation
            if not naturally_exhausted and self.report.state is KernelState.NOT_STARTED:
                await self._finalize(KernelState.CANCELLED, "stream abandoned")
            if self.report.state is not KernelState.NOT_STARTED:
                self._closed = True
            if preserve_primary:
                rearm_cancellation(pending_cancellation)

    async def _record_terminal_cleanup_failure(self, error: BaseException) -> None:
        if self.report.state is KernelState.NOT_STARTED:
            await self._finalize(KernelState.FAILED, "stream cleanup failed")
        violations = self.report.violations
        if not any(violation.code == "cleanup_failure" for violation in violations):
            violations = (
                *violations,
                Violation(
                    severity=ViolationSeverity.BLOCKING,
                    code="cleanup_failure",
                    message=f"reference cleanup failed ({type(error).__name__})",
                ),
            )
        self.report = replace(
            self.report,
            state=KernelState.FAILED,
            terminal_reason="stream cleanup failed",
            violations=violations,
        )
        _attach_report(error, self.report)

    async def _finalize(self, state: KernelState, reason: str) -> None:
        if self.report.state is not KernelState.NOT_STARTED:
            return
        snapshot = await self._scheduler.context.snapshot()
        consistency = self._scheduler.context.policy.consistency
        snapshot_state = (
            SnapshotState.NOT_REQUESTED
            if consistency.snapshot_requirement is SnapshotRequirement.TRAVERSAL_ONLY
            else SnapshotState.UNVERIFIED
        )
        violations = tuple(self._scheduler.violations)
        if state is KernelState.COMPLETED and snapshot_state is SnapshotState.UNVERIFIED:
            state = KernelState.INCOMPLETE
            reason = "required snapshot was not verified"
            violations = (
                *violations,
                Violation(
                    severity=ViolationSeverity.BLOCKING,
                    code="snapshot_unverified",
                    message="the requested stable snapshot was not verified",
                ),
            )
        self.report = KernelReport(
            state=state,
            assurance=self._assurance,
            snapshot=snapshot_state,
            plan_id=type(self._scheduler.plan).__name__,
            dispatch_id=type(self._scheduler.dispatch).__name__,
            emitted_rows=self._emitted,
            unique_rows=self._unique_emitted,
            physical_requests=snapshot.counters.physical_requests,
            logical_pages=snapshot.counters.logical_pages,
            batch_requests=self._scheduler.dispatcher.batch_requests,
            batch_commands=self._scheduler.dispatcher.batch_commands,
            retries=snapshot.retries,
            cooldown_seconds=snapshot.cooldown_seconds,
            buffered_rows_high_water=snapshot.counters.buffered_rows_high_water,
            violations=violations,
            terminal_reason=reason,
        )


def fan_out(  # noqa: PLR0913
    executor: Executor,
    requests: ReferenceSource,
    *,
    dispatch: DispatchPlan,
    output_order: ReferenceOutputOrder = ReferenceOutputOrder.READY,
    tolerant: bool = False,
    policy: ExecutionPolicy | None = None,
) -> ReferenceStream:
    """Schedule independent requests as single-response reference traversals."""
    return iter_references(
        executor,
        requests,
        plan=SingleResponsePlan(),
        dispatch=dispatch,
        output_order=output_order,
        tolerant=tolerant,
        policy=policy,
        _whole_result=True,
    )


def iter_references(  # noqa: PLR0913
    executor: Executor,
    requests: ReferenceSource,
    *,
    plan: ListPlan,
    dispatch: DispatchPlan,
    selector: ResultSelector | None = None,
    identity: IdentitySpec | None = None,
    output_order: ReferenceOutputOrder = ReferenceOutputOrder.READY,
    tolerant: bool = False,
    policy: ExecutionPolicy | None = None,
    _whole_result: bool = False,
    _emit_complete: bool = False,
    _emit_response: bool = False,
    _capture_fail_fast: bool = False,
    _page_cap_hint: int | None = None,
    _assurance: CompletionAssurance = CompletionAssurance.CALLER_ASSERTED,
) -> ReferenceStream:
    """Construct a lazy bounded reference traversal stream without I/O."""
    PaginationDriver.validate_plan(plan)
    if not isinstance(dispatch, BatchDispatch | DirectDispatch):
        raise TypeError("dispatch must be a canonical DispatchPlan")
    if not isinstance(output_order, ReferenceOutputOrder):
        raise TypeError("output_order must be ReferenceOutputOrder")
    if dispatch.output_order is not output_order:
        raise ValueError("dispatch and stream output order must agree")
    if _page_cap_hint is not None and (
        not isinstance(_page_cap_hint, int) or isinstance(_page_cap_hint, bool) or _page_cap_hint < 1
    ):
        raise ValueError("page cap hint must be a positive integer")
    scheduler = ReferenceScheduler(
        executor,
        plan=plan,
        dispatch=dispatch,
        selector=selector,
        identity=identity,
        output_order=output_order,
        tolerant=tolerant,
        policy=policy or ExecutionPolicy(),
        whole_result=_whole_result,
        emit_complete=_emit_complete,
        emit_response=_emit_response,
        capture_fail_fast=_capture_fail_fast,
        page_cap_hint=_page_cap_hint,
    )
    return ReferenceStream(
        scheduler,
        requests,
        assurance=_assurance,
    )
