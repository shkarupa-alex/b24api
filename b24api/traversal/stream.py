"""Lazy correctness-first sequential traversal streams and state machines."""

# ruff: noqa: TRY301 - source error provenance is recorded at the lifecycle boundary

from __future__ import annotations
import asyncio
import contextlib
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator
from dataclasses import replace
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
from b24api.execution import (
    Executor,
    await_cancellation_resistant,
    await_cleanup_resistant,
    rearm_cancellation,
)
from b24api.execution.failure import attach_report as _attach_report
from b24api.execution.snapshot import KernelReport
from b24api.traversal.driver import PaginationDriver
from b24api.traversal.identity import _MISSING, _Page
from b24api.traversal.plans import ItemCursorPlan, KeysetPlan, OffsetSequentialPlan

if TYPE_CHECKING:
    from b24api.contracts.identity_store import IdentityStore
    from b24api.contracts.request import Request, ResultSelector, TraversalIdentity
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
        self._completion_cleanup_done = False
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
        self._runner: AsyncGenerator[tuple[JsonValue, bool]] | None = None
        self._prefetched: tuple[JsonValue, bool] | object = _MISSING
        self._closed = False
        self._emitted = 0
        self._unique_emitted = 0
        self.report = KernelReport(assurance=assurance)

    def __aiter__(self) -> Self:
        """Return this asynchronous iterator."""
        return self

    @property
    def completion_gate(self) -> object:
        """Return live bounded evidence for the sequential traversal family."""
        return None if self._completion is None else self._completion.gate

    async def __anext__(self) -> JsonValue:
        """Return the next asynchronous item."""
        if self._closed:
            raise StopAsyncIteration
        if self._prefetched is not _MISSING:
            item, is_unique = cast("tuple[JsonValue, bool]", self._prefetched)
            self._prefetched = _MISSING
            self._record_delivery(is_unique=is_unique)
            return item
        if self._runner is None:
            self._runner = self._run()
        try:
            item, is_unique = await anext(self._runner)
        except BaseException:
            self._finish_completion_cleanup()
            raise
        self._record_delivery(is_unique=is_unique)
        return item

    def _finish_completion_cleanup(self) -> None:
        recorder = self._completion
        if recorder is None or self._completion_cleanup_done or self.report.state is KernelState.NOT_STARTED:
            return
        failed = any(item.code == "cleanup_failure" for item in self.report.violations)
        recorder.cleanup(CleanupState.FAILURE if failed else CleanupState.SUCCESS)
        self._completion_cleanup_done = True

    def _record_delivery(self, *, is_unique: bool) -> None:
        self._emitted += 1
        if is_unique:
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
            self._finish_completion_cleanup()
        if self.report.state is KernelState.NOT_STARTED and self._runner is not None:
            await self._finalize(KernelState.CANCELLED, "stream closed before exhaustion")
            self._finish_completion_cleanup()

    async def _run(self) -> AsyncGenerator[tuple[JsonValue, bool]]:  # noqa: C901, PLR0912, PLR0915
        pages = self._driver.pages()
        naturally_exhausted = False
        primary_error: BaseException | None = None
        pending_cancellation: asyncio.CancelledError | None = None
        try:
            async for page in pages:
                buffered = deque(zip(page.items, self._driver.last_page_unique_mask, strict=True))
                await self._context.set_buffered_rows(len(buffered))
                while buffered:
                    item, is_unique = buffered.popleft()
                    await self._context.set_buffered_rows(len(buffered) + 1)
                    yield _thaw_json(item), is_unique
                    await self._context.set_buffered_rows(len(buffered))
                if page.items and self._completion is not None:
                    self._completion.delivered()
                if self._page_stop is not None:
                    record = self._driver.last_page_record
                    if record is None:
                        raise RuntimeError("validated page lacks completion provenance")
                    decision = self._page_stop.on_page(PageBoundary(0, record, tuple(page.items)))
                    if isawaitable(decision):
                        decision = await decision
                    if not isinstance(decision, ContinuePage | CallerStop):
                        raise TypeError("page stop policy returned an invalid decision")
                    if page.items and self._completion is not None:
                        self._completion.acknowledged()
                    if isinstance(decision, CallerStop) and page.continuing:
                        self._caller_stopped = True
                        self._stop_reason = decision.reason
                        break
                elif page.items and self._completion is not None:
                    self._completion.acknowledged()
            naturally_exhausted = True
            await self._finalize(
                KernelState.COMPLETED,
                self._stop_reason or self._driver.terminal_reason or "terminal confirmed",
            )
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
        except PaginationError as error:
            reason = str(getattr(error, "report_name", type(error).__name__))
            cancellation = await await_cancellation_resistant(
                self._finalize(KernelState.INCOMPLETE, reason),
            )
            incomplete = IncompleteTraversalError(
                report=self.report,
                error=error,
                replay_disposition=getattr(error, "replay_disposition", ReplayDisposition.NOT_ELIGIBLE),
            )
            primary_error = incomplete
            if cancellation is not None:
                _attach_report(cancellation, self.report)
                pending_cancellation = cancellation
            _attach_report(incomplete, self.report)
            raise incomplete from error
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
            cleanup = await await_cleanup_resistant(self._cleanup_pages(pages))
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
                    message=f"pagination cleanup failed ({type(error).__name__})",
                ),
            )
        self.report = replace(
            self.report,
            state=KernelState.FAILED,
            terminal_reason="stream cleanup failed",
            violations=violations,
        )
        _attach_report(error, self.report)

    async def _cleanup_pages(self, pages: AsyncGenerator[_Page]) -> None:
        await pages.aclose()
        await self._context.set_buffered_rows(0)

    async def _finalize(self, state: KernelState, reason: str) -> None:
        if self.report.state is not KernelState.NOT_STARTED:
            return
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
            plan_id=type(self._driver.plan).__name__,
            dispatch_id="sequential_direct",
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
            self._completion.terminal_from_plan(
                self._driver.plan,
                state,
                caller_stopped=self._caller_stopped,
                terminal_reason=self._driver.terminal_reason,
                qualified_total=self._driver._expected_total,  # noqa: SLF001 - driver qualified-total witness
            )


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
