"""Lazy correctness-first sequential traversal streams and state machines."""

from __future__ import annotations
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator
from inspect import isawaitable
from typing import TYPE_CHECKING, Self, cast

from b24api.completion.recorder import CompletionRecorder
from b24api.contracts.completion import CleanupState
from b24api.contracts.json import JsonValue, _thaw_json
from b24api.contracts.page import IdentityPageAdapter, PageAdapter
from b24api.contracts.page_stop import CallerStop, ContinuePage, PageBoundary, PageStopPolicy
from b24api.contracts.policy import (
    CompletionAssurance,
    ExecutionPolicy,
    KernelState,
    ReplayDisposition,
    SnapshotRequirement,
    SnapshotState,
)
from b24api.contracts.report import Violation, ViolationSeverity, retain_page_trace
from b24api.errors import IncompleteTraversalError, PaginationError
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
from b24api.traversal.driver import PaginationDriver
from b24api.traversal.plans import ItemCursorPlan, KeysetPlan, OffsetSequentialPlan

if TYPE_CHECKING:
    from types import TracebackType

    from b24api.contracts.identity_store import IdentityStore
    from b24api.contracts.request import Request, ResultSelector, TraversalIdentity
    from b24api.execution import Executor
    from b24api.traversal.identity import _Page
    from b24api.traversal.plans import (
        ListPlan,
    )

_IDENTITY_PAGE_ADAPTER = IdentityPageAdapter()


class ItemStream(AsyncIterator[JsonValue]):
    """Lazy item traversal stream with deterministic cleanup and final report."""

    def __init__(  # noqa: PLR0913
        self,
        executor: Executor,
        request: Request,
        plan: ListPlan,
        *,
        selector: ResultSelector | None = None,
        identity: TraversalIdentity | None = None,
        policy: ExecutionPolicy | None = None,
        page_cap_hint: int | None = None,
        assurance: CompletionAssurance = CompletionAssurance.CALLER_ASSERTED,
        page_adapter: PageAdapter = _IDENTITY_PAGE_ADAPTER,
        page_stop: PageStopPolicy | None = None,
        identity_store: IdentityStore | None = None,
    ) -> None:
        """Initialize instance state."""
        PaginationDriver.validate_plan(plan)
        self._context = executor.context(policy)
        self._completion = (
            CompletionRecorder()
            if isinstance(
                plan,
                OffsetSequentialPlan | KeysetPlan | ItemCursorPlan,
            )
            else None
        )
        self._driver = PaginationDriver(
            executor,
            request,
            plan,
            selector=selector,
            identity=identity,
            context=self._context,
            page_cap_hint=page_cap_hint,
            page_adapter=page_adapter,
            completion_recorder=self._completion,
            identity_store=identity_store,
        )
        self._assurance = assurance
        if page_stop is not None and not callable(getattr(page_stop, "on_page", None)):
            raise TypeError("page_stop must implement on_page")
        self._page_stop = page_stop
        self._caller_stopped = False
        self._stop_reason: str | None = None
        self._pages: AsyncGenerator[_Page] | None = None
        self._emitted = 0
        self._unique_emitted = 0
        self.report = KernelReport(assurance=assurance)
        self._runner = OperationRunner(
            self._run(),
            LifecycleHooks(
                finalize=self._finalize,
                failure_report=self._failure_report,
                cleanup=self._cleanup,
                propagate=self._propagate,
            ),
            isolated_pulls=False,
        )

    def __aiter__(self) -> Self:
        """Return this asynchronous iterator."""
        return self

    @property
    def completion_gate(self) -> object:
        """Return live bounded evidence for the sequential traversal family."""
        return None if self._completion is None else self._completion.gate

    async def __anext__(self) -> JsonValue:
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

    async def _run(self) -> AsyncGenerator[JsonValue]:
        pages = self._pages = self._driver.pages()
        async for page in pages:
            buffered = deque(zip(page.items, self._driver.last_page_unique_mask, strict=True))
            await self._context.set_buffered_rows(len(buffered))
            while buffered:
                item, is_unique = buffered.popleft()
                await self._context.set_buffered_rows(len(buffered) + 1)
                self._emitted += 1
                self._unique_emitted += is_unique
                yield _thaw_json(item)
                await self._context.set_buffered_rows(len(buffered))
            if page.items and self._completion is not None:
                self._completion.delivered()
            if self._page_stop is not None and await self._stops_after(page):
                break
            if self._page_stop is None and page.items and self._completion is not None:
                self._completion.acknowledged()

    async def _stops_after(self, page: _Page) -> bool:
        """Ask the page-stop policy about one delivered page; a stop ends a continuing traversal."""
        record = self._driver.last_page_record
        if record is None:
            raise RuntimeError("validated page lacks completion provenance")
        decision = cast("PageStopPolicy", self._page_stop).on_page(PageBoundary(0, record, tuple(page.items)))
        if isawaitable(decision):
            decision = await decision
        if not isinstance(decision, ContinuePage | CallerStop):
            raise TypeError("page stop policy returned an invalid decision")
        if page.items and self._completion is not None:
            self._completion.acknowledged()
        if isinstance(decision, CallerStop) and page.continuing:
            self._caller_stopped = True
            self._stop_reason = decision.reason
            return True
        return False

    async def _cleanup(self) -> None:
        if self._pages is None:
            return
        await self._pages.aclose()
        await self._context.set_buffered_rows(0)

    def _propagate(self, error: BaseException, report: KernelReport) -> BaseException:
        if not isinstance(error, PaginationError):
            return error
        return IncompleteTraversalError(
            report=report,
            error=error,
            replay_disposition=getattr(error, "replay_disposition", ReplayDisposition.NOT_ELIGIBLE),
        )

    async def _finalize(self, cause: TerminalCause, failure: str | None, attempt: CleanupAttempt) -> KernelReport:
        if self._pages is None:
            return self.report
        state, reason = kernel_terminal(cause, failure)
        if cause is TerminalCause.EXHAUSTED:
            reason = self._stop_reason or self._driver.terminal_reason or "terminal confirmed"
        elif isinstance(self._runner.primary_error, PaginationError):
            state = KernelState.INCOMPLETE
        snapshot = await self._context.snapshot()
        consistency = self._context.policy.consistency
        snapshot_state = (
            SnapshotState.NOT_REQUESTED
            if consistency.snapshot_requirement is SnapshotRequirement.TRAVERSAL_ONLY
            else SnapshotState.UNVERIFIED
        )
        violations = tuple(self._driver.violations)
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
        page_trace, page_trace_truncated = retain_page_trace(
            tuple(self._driver.page_trace),
            self._context.policy.page_trace_limit,
        )
        page_trace_truncated = page_trace_truncated or self._driver.page_trace_truncated
        self.report = KernelReport(
            state=state,
            assurance=self._assurance,
            snapshot=snapshot_state,
            emitted_rows=self._emitted,
            unique_rows=self._unique_emitted,
            duplicate_identities=self._driver.duplicate_identities,
            physical_requests=snapshot.counters.physical_requests,
            logical_pages=snapshot.counters.logical_pages,
            retries=snapshot.retries,
            cooldown_seconds=snapshot.cooldown_seconds,
            buffered_rows_high_water=snapshot.counters.buffered_rows_high_water,
            violations=violations,
            terminal_reason=reason,
            caller_stopped=self._caller_stopped,
            page_trace=page_trace,
            page_trace_truncated=page_trace_truncated,
        )
        if self._completion is not None:
            plan = self._driver.plan
            self._completion.terminal_from_kernel(
                state,
                boundary=isinstance(plan, KeysetPlan) and plan.boundary is not None,
                caller_stopped=self._caller_stopped,
                terminal_reason=self._driver.terminal_reason,
                qualified_total=self._driver.expected_total,
            )
        self.report = with_cleanup_attempt(self.report, cause, attempt, subject="pagination")
        if self._completion is not None:
            failed = cleanup_failed(cause, attempt)
            self._completion.cleanup(CleanupState.FAILURE if failed else CleanupState.SUCCESS)
        return self.report

    def _failure_report(self, cause: TerminalCause, reason: str, attempt: CleanupAttempt) -> KernelReport:
        # The finalizer raised: publish the fallback as this kernel's report and close its completion gate,
        # so a public adapter over it can still publish its one FAILED report.
        self.report = failed_kernel_report(cause, reason, attempt, subject="pagination")
        if self._completion is not None:
            self._completion.gate.close_after_failure(
                CleanupState.FAILURE if cleanup_failed(cause, attempt) else CleanupState.SUCCESS
            )
        return self.report


def iter_list(  # noqa: PLR0913
    executor: Executor,
    request: Request,
    *,
    plan: ListPlan,
    selector: ResultSelector | None = None,
    identity: TraversalIdentity | None = None,
    policy: ExecutionPolicy | None = None,
    _page_cap_hint: int | None = None,
    _assurance: CompletionAssurance = CompletionAssurance.CALLER_ASSERTED,
    _page_adapter: PageAdapter = _IDENTITY_PAGE_ADAPTER,
    _page_stop: PageStopPolicy | None = None,
    _identity_store: IdentityStore | None = None,
) -> ItemStream:
    """Construct a lazy canonical item stream without performing I/O."""
    return ItemStream(
        executor,
        request,
        plan,
        selector=selector,
        identity=identity,
        policy=policy,
        page_cap_hint=_page_cap_hint,
        assurance=_assurance,
        page_adapter=_page_adapter,
        page_stop=_page_stop,
        identity_store=_identity_store,
    )
