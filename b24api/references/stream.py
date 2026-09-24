"""Bounded fair scheduling for independent and paginated references."""

from __future__ import annotations
from collections.abc import AsyncGenerator, AsyncIterator
from typing import TYPE_CHECKING, Self, cast

from b24api._sources import OwnedSource
from b24api.contracts.completion import CleanupState, StreamClosure
from b24api.contracts.page import IdentityPageAdapter, PageAdapter
from b24api.contracts.policy import (
    CompletionAssurance,
    ExecutionPolicy,
    KernelState,
    SnapshotRequirement,
    SnapshotState,
)
from b24api.contracts.report import Violation, ViolationSeverity, retain_page_trace
from b24api.contracts.violation import retain_violations
from b24api.execution.lifecycle import (
    CleanupAttempt,
    LifecycleHooks,
    OperationRunner,
    TerminalCause,
    cleanup_failed,
    failed_kernel_report,
    kernel_terminal,
    with_cleanup_attempt,
)
from b24api.execution.snapshot import KernelReport
from b24api.references.dispatch import (
    ReferenceSource,
    ReferenceStreamItem,
)
from b24api.references.outcome import ReferenceItem, ReferenceRequest
from b24api.references.scheduler import ReferenceScheduler
from b24api.traversal import PaginationDriver
from b24api.traversal.plans import (
    BatchDispatch,
    DirectDispatch,
    DispatchPlan,
    ListPlan,
    ReferenceOutputOrder,
    SingleResponsePlan,
)

_IDENTITY_PAGE_ADAPTER = IdentityPageAdapter()

if TYPE_CHECKING:
    from types import TracebackType

    from b24api.contracts.page_stop import PageStopPolicy
    from b24api.contracts.request import ResultSelector, TraversalIdentity
    from b24api.execution import Executor


class ReferenceStream(AsyncIterator[ReferenceStreamItem]):
    """Lazy reference stream with one frozen report and deterministic cleanup."""

    def __init__(
        self,
        scheduler: ReferenceScheduler,
        source: ReferenceSource | OwnedSource[ReferenceRequest],
        *,
        assurance: CompletionAssurance = CompletionAssurance.CALLER_ASSERTED,
    ) -> None:
        """Initialize instance state."""
        self._scheduler = scheduler
        self._source = own_reference_source(source)
        self._outcomes: AsyncGenerator[ReferenceStreamItem] | None = None
        self._emitted = 0
        self._unique_emitted = 0
        self._assurance = assurance
        self.report = KernelReport(assurance=assurance)
        self._runner = OperationRunner(
            self._run(),
            LifecycleHooks(finalize=self._finalize, failure_report=_failure_report, cleanup=self._cleanup),
            isolated_pulls=False,
        )

    def __aiter__(self) -> Self:
        """Return this asynchronous iterator."""
        return self

    @property
    def completion_gate(self) -> object:
        """Expose the reference scheduler's correlated completion gate."""
        return self._scheduler.completion.gate

    @property
    def active_references_high_water(self) -> int:
        """Return the bounded scheduler admission high-water mark."""
        return self._scheduler.active_references_high_water

    async def __anext__(self) -> ReferenceStreamItem:
        """Return the next asynchronous item."""
        return await anext(self._runner)

    async def __aenter__(self) -> Self:
        """Enter without reading an item."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close on exit without replacing the body's primary exception."""
        await self._runner.__aexit__(exc_type, exc, traceback)

    async def aclose(self) -> None:
        """Close owned resources once; the report is published after cleanup."""
        await self._runner.aclose()

    async def _run(self) -> AsyncGenerator[ReferenceStreamItem]:
        outcomes = self._outcomes = self._scheduler.outcomes(self._source)
        async for outcome in outcomes:
            if isinstance(outcome, ReferenceItem):
                self._emitted += 1
                if self._scheduler.record_delivery(outcome):
                    self._unique_emitted += 1
            yield outcome

    async def _cleanup(self) -> None:
        # Closing the scheduler cancels and awaits in-flight reference work and closes the source,
        # all before the terminal event is emitted.
        if self._outcomes is not None:
            await self._outcomes.aclose()

    async def _finalize(self, cause: TerminalCause, failure: str | None, attempt: CleanupAttempt) -> KernelReport:
        if self._outcomes is None:
            return self.report
        state, reason = kernel_terminal(cause, failure)
        if cause is TerminalCause.EXHAUSTED:
            reason = "reference input exhausted"
        snapshot = await self._scheduler.context.snapshot()
        consistency = self._scheduler.context.policy.consistency
        snapshot_state = (
            SnapshotState.NOT_REQUESTED
            if consistency.snapshot_requirement is SnapshotRequirement.TRAVERSAL_ONLY
            else SnapshotState.UNVERIFIED
        )
        violations = retain_violations((*self._scheduler.violations, *self._source.violations))
        if state is KernelState.COMPLETED and snapshot_state is SnapshotState.UNVERIFIED:
            state = KernelState.INCOMPLETE
            reason = "required snapshot was not verified"
            violations = retain_violations(
                (
                    *violations,
                    Violation(
                        severity=ViolationSeverity.BLOCKING,
                        code="snapshot_unverified",
                        message="the requested stable snapshot was not verified",
                    ),
                )
            )
        page_trace, page_trace_truncated = retain_page_trace(
            tuple(sorted(self._scheduler.page_trace, key=lambda record: record.sequence)),
            self._scheduler.context.policy.page_trace_limit,
        )
        page_trace_truncated = page_trace_truncated or self._scheduler.page_trace_truncated
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
            caller_stopped=bool(self._scheduler.stopped_bindings),
            page_trace=page_trace,
            page_trace_truncated=page_trace_truncated,
        )
        self._scheduler.completion.stream_terminal(
            StreamClosure.NATURAL
            if state is KernelState.COMPLETED
            else StreamClosure.CANCELLED
            if state is KernelState.CANCELLED
            else StreamClosure.EARLY_CLOSE,
        )
        self.report = with_cleanup_attempt(self.report, cause, attempt, subject="reference")
        failed = cleanup_failed(cause, attempt)
        self._scheduler.completion.cleanup(CleanupState.FAILURE if failed else CleanupState.SUCCESS)
        return self.report


def _failure_report(_cause: TerminalCause, reason: str, _attempt: CleanupAttempt) -> KernelReport:
    return failed_kernel_report(reason)


def _accept_reference(item: object, _index: int) -> ReferenceRequest:
    if not isinstance(item, ReferenceRequest):
        raise TypeError("reference source must yield ReferenceRequest values")
    return item


def own_reference_source(source: ReferenceSource | OwnedSource[ReferenceRequest]) -> OwnedSource[ReferenceRequest]:
    """Adopt a raw reference source; a family that already owns its source passes it through."""
    if isinstance(source, OwnedSource):
        return cast("OwnedSource[ReferenceRequest]", source)
    return OwnedSource.adapt(source, accept=_accept_reference, inline_sequences=True)


def fan_out(  # noqa: PLR0913
    executor: Executor,
    requests: ReferenceSource | OwnedSource[ReferenceRequest],
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
    requests: ReferenceSource | OwnedSource[ReferenceRequest],
    *,
    plan: ListPlan,
    dispatch: DispatchPlan,
    selector: ResultSelector | None = None,
    identity: TraversalIdentity | None = None,
    output_order: ReferenceOutputOrder = ReferenceOutputOrder.READY,
    tolerant: bool = False,
    policy: ExecutionPolicy | None = None,
    _whole_result: bool = False,
    _emit_complete: bool = False,
    _emit_response: bool = False,
    _capture_fail_fast: bool = False,
    _page_cap_hint: int | None = None,
    _assurance: CompletionAssurance = CompletionAssurance.CALLER_ASSERTED,
    _page_adapter: PageAdapter = _IDENTITY_PAGE_ADAPTER,
    _page_stop: PageStopPolicy | None = None,
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
        page_adapter=_page_adapter,
        page_stop=_page_stop,
    )
    return ReferenceStream(
        scheduler,
        requests,
        assurance=_assurance,
    )
