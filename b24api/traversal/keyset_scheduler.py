"""One ordered scheduler for range, occupied-anchor, and automatic keysets."""

# ruff: noqa: FBT003, PLR0915
from __future__ import annotations
from collections import deque
from dataclasses import replace
from typing import TYPE_CHECKING

from b24api.contracts.keyset_execution import (
    AutoKeysetExecution,
    ClosureWitness,
    KeysetExecutionKind,
    KeysetPageCompletion,
    KeysetPhase,
    KeysetSelectionReason,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    TotalHintMode,
)
from b24api.contracts.report import KeysetExecutionReport, PageDispatch, PageOutcome, PageRejectionCode, Violation
from b24api.errors import BudgetExceededError, PaginationError
from b24api.traversal import keyset_step
from b24api.traversal.keyset_auto import (
    AnchorFacts,
    FinalSelection,
    Preselected,
    Preselection,
    SelectorInputs,
    TotalHintState,
    constrain_anchor_preselection,
    finalize,
    preselect,
)
from b24api.traversal.keyset_capability import (
    analyze_boundary,
    anchor_capable_batch_capacity,
    compact_anchor_receipts,
    normalize_anchor_receipts,
    selected_lane_geometry,
)
from b24api.traversal.keyset_costs import selected_range_geometry
from b24api.traversal.keyset_fast_plan import (
    FastKeysetPlan,
    LaneBounds,
    LaneKind,
    LaneSpec,
    LaneState,
    LaneStatus,
    LazyRangePlan,
    PlanOutcome,
    fit_wave,
)
from b24api.traversal.keyset_observation import (
    abort_staged_observations,
    close_scheduler,
    flush_staged_observations,
    raise_boundary_cap_contradiction,
    record_scheduler_observation,
    reject_boundary_observations,
    validate_canary_observations,
)
from b24api.traversal.keyset_reporting import build_scheduler_report, initial_report_selection
from b24api.traversal.keyset_transaction_contract import (
    KeysetTransactionState,
    build_controlled_request,
    build_lane_plan,
)
from b24api.traversal.keyset_transactions import (
    build_anchor_plans,
    build_canary_plans,
    execute_body_wave,
    execute_finish_page,
    execute_wave,
)
from b24api.traversal.ordered_admission import FastCounters, OrderedAdmissionState, drain_complete_lanes
from b24api.traversal.page_validation import (
    LaneCommandPlan,
    LaneReceipt,
    normalize_tail_receipt,
    validate_boundary_direction,
)

if TYPE_CHECKING:
    from b24api.batch.engine import BatchExecutor
    from b24api.contracts.json import JsonValue
    from b24api.contracts.request import IdentitySpec, Request, ResultSelector
    from b24api.contracts.response import Response, ResultCollectionShape
    from b24api.contracts.traversal import KeysetSpec
    from b24api.execution import ExecutionContext, Executor
    from b24api.traversal.keyset_fast_stream import FastTraceRecorder


class KeysetFastScheduler:
    """Own fast planning, wave transactions, ordered admission, and finish."""

    def __init__(  # noqa: PLR0913
        self,
        *,
        executor: Executor,
        request: Request,
        identity: IdentitySpec,
        keyset: KeysetSpec,
        selector: ResultSelector,
        collection_shape: ResultCollectionShape,
        page_size: int,
        effective_page_cap: int,
        execution: RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution,
        context: ExecutionContext,
        engine: BatchExecutor,
        trace: FastTraceRecorder,
    ) -> None:
        """Initialize isolated planning, admission, and reporting state."""
        self.executor = executor
        self.request = request
        self.identity = identity
        self.keyset = keyset
        self.selector, self.collection_shape = selector, collection_shape
        self.page_size, self.effective_page_cap = page_size, effective_page_cap
        self.execution, self.context, self.engine, self.trace = execution, context, engine, trace
        self.completion = execution.contract.page_completion
        requested_batch = getattr(execution, "batch_size", None) or 50
        self.batch_capacity = min(
            requested_batch,
            engine.portal_command_cap,
            context.policy.max_buffered_commands,
            context.policy.max_buffered_rows // effective_page_cap,
        )
        self.admission = OrderedAdmissionState(
            direction="asc" if keyset.direction == "ascending" else "desc",
            identity=identity,
        )
        self.violations: list[Violation] = []
        self.batch_requests, self.batch_commands = 0, 0
        self.transactions = KeysetTransactionState()
        self._observation_ordinal = self._command_ordinal = 0
        self._planned, self._closed = False, False
        self._plan_outcome: PlanOutcome | None = None
        self._preselection: Preselection | None = None
        self._final: FinalSelection | None = None
        self._total_hint = TotalHintState(False, None, False, False)
        self._tail: LaneReceipt | None = None
        self._head_admitted = False
        self._selected, self._reason = initial_report_selection(execution)
        self._assured = False
        self._anchor_count, self._anchor_probe_commands, self._empty_anchor_probes = 0, 0, 0
        self._target_lanes: int | None = None
        self._window_width: int | None = None
        self._window_count: int | None = None
        self._range_geometry: LazyRangePlan | None = None
        self._offered_rows = 0
        self._head_rows, self._tail_rows = 0, 0
        self._interior_span: int | None = None
        self._density_num: int | None = None
        self._density_den: int | None = None
        self._interior_estimate: int | None = None
        self._total_estimate: int | None = None
        self._selected_estimate: int | None = None
        self._frozen_report: KeysetExecutionReport | None = None

    @property
    def counters(self) -> FastCounters:
        """Return a detached snapshot of row-provenance counters."""
        return self.admission.snapshot_counters()

    @property
    def terminal(self) -> bool:
        """Return whether traversal has reached a terminal state."""
        return self.transactions.terminal

    def mark_emitted(self, count: int) -> None:
        """Record rows delivered to the caller."""
        self.admission.mark_emitted(count)

    def next_command_id(self, phase: KeysetPhase) -> str:
        """Allocate a unique command identifier for a transaction phase."""
        value = f"{phase.value}-{self._command_ordinal}"
        self._command_ordinal += 1
        return value

    async def adjust_buffer(self, delta: int) -> None:
        """Apply a row-buffer accounting delta for a transaction."""
        if not delta:
            return
        await self.context.adjust_buffered_rows(delta)
        self.transactions.buffer_balance += delta

    def record_page(  # noqa: PLR0913
        self,
        plan: LaneCommandPlan,
        *,
        index: int | None,
        selected: int,
        admitted: int,
        outcome: PageOutcome = PageOutcome.COMMITTED,
        rejection: PageRejectionCode | None = None,
        violation: Violation | None = None,
        response: Response | None = None,
        witness: ClosureWitness | None = None,
        dispatch: PageDispatch = PageDispatch.BATCH,
    ) -> None:
        """Record one transaction page observation."""
        record_scheduler_observation(
            self,
            plan,
            index=index,
            selected=selected,
            admitted=admitted,
            outcome=outcome,
            rejection=rejection,
            violation=violation,
            response=response,
            witness=witness,
            dispatch=dispatch,
        )

    async def execute_wave(self, plans: tuple[LaneCommandPlan, ...]) -> tuple[LaneReceipt, ...]:
        """Execute one correlated transaction wave."""
        return await execute_wave(self, plans)

    def _validate_canaries(self, receipts: tuple[LaneReceipt, ...], expected: dict[str, tuple[int, ...]]) -> None:
        validate_canary_observations(
            receipts,
            expected,
            self.transactions.staged_observations,
            self.record_page,
            self.admission,
            self.violations,
        )
        self._assured = True

    async def _chunked_waves(
        self,
        plans: tuple[LaneCommandPlan, ...],
        *,
        compact_anchors: bool = False,
    ) -> tuple[LaneReceipt, ...]:
        receipts: list[LaneReceipt] = []
        remaining = plans
        while remaining:
            wave = fit_wave(
                remaining,
                reserves=tuple(plan.reserved_rows for plan in remaining),
                commands=self.batch_capacity,
                rows=self.context.policy.max_buffered_rows - self.transactions.buffer_balance,
            )
            if not wave:
                raise BudgetExceededError("fast keyset planning wave has no available row capacity")
            wave_receipts = await self.execute_wave(wave)
            if compact_anchors:
                wave_receipts, discarded, retained = compact_anchor_receipts(wave_receipts)
                self.admission.record_raw(discarded + retained)
                self.admission.record_discarded(discarded)
                await self.adjust_buffer(retained)
            receipts.extend(wave_receipts)
            remaining = remaining[len(wave) :]
        return tuple(receipts)

    async def _canaries(self, asc: LaneReceipt, desc: LaneReceipt) -> None:
        try:
            plans, expected = build_canary_plans(self, asc, desc)
            self._validate_canaries(await self._chunked_waves(plans), expected)
        except BaseException:
            abort_staged_observations(
                self.transactions.staged_observations,
                self.record_page,
                self.admission,
                self.violations,
            )
            raise

    def _consume_anchors(self, receipts: tuple[LaneReceipt, ...], lo: int, hi: int, charged: int) -> tuple[int, ...]:
        result = normalize_anchor_receipts(receipts, lo=lo, upper_exclusive=hi)
        self.transactions.anchor_rows = result.rows
        self.transactions.anchor_commands = result.commands
        self._empty_anchor_probes = result.empty_probes
        self._anchor_count = len(result.anchors)
        self.admission.record_raw(result.raw_rows - charged)
        self.admission.record_discarded(result.discarded_rows)
        return result.anchors

    async def _partition_planning(self, asc: LaneReceipt, desc: LaneReceipt, target: int) -> tuple[int, ...]:
        precharged = 0
        canaries_validated = False
        try:
            canaries, expected = build_canary_plans(self, asc, desc)
            anchors = build_anchor_plans(self, asc, desc, target)
            self._anchor_probe_commands = len(anchors)
            planning = (*canaries, *anchors)
            co_scheduled = fit_wave(
                planning,
                reserves=tuple(plan.reserved_rows for plan in planning),
                commands=self.batch_capacity,
                rows=self.context.policy.max_buffered_rows - self.transactions.buffer_balance,
            )
            if len(co_scheduled) == len(planning):
                receipts = await self.execute_wave(co_scheduled)
                canary_receipts, anchor_receipts = receipts[: len(canaries)], receipts[len(canaries) :]
                self._validate_canaries(canary_receipts, expected)
                canaries_validated = True
            else:
                canary_receipts = await self._chunked_waves(canaries)
                self._validate_canaries(canary_receipts, expected)
                canaries_validated = True
                anchor_receipts = await self._chunked_waves(anchors, compact_anchors=True) if anchors else ()
                precharged = sum(bool(receipt.rows) for receipt in anchor_receipts)
        except BaseException:
            if canaries_validated:
                self.admission.record_discarded(len(asc.rows) + len(desc.rows))
            abort_staged_observations(
                self.transactions.staged_observations,
                self.record_page,
                self.admission,
                self.violations,
            )
            raise
        result = self._consume_anchors(anchor_receipts, max(asc.identities), min(desc.identities), precharged)
        await self.adjust_buffer(self._anchor_count - precharged)
        return result

    def drain_admission_frontier(self) -> None:
        """Admit all complete lanes at the ordered frontier."""
        self.transactions.lane_index = drain_complete_lanes(
            self.transactions.lane_index,
            self.transactions.lanes,
            self.transactions.lane_rows,
            self.transactions.lane_identities,
            self.transactions.lane_commands,
            self._admit_receipt,
            self.trace.admit,
            self._range_geometry,
        )

    async def plan_barrier(self) -> PlanOutcome:  # noqa: C901, PLR0912
        """Resolve and freeze the fast-keyset execution plan."""
        if self._planned:
            if self._plan_outcome is None:
                raise RuntimeError("planned scheduler has no outcome")
            return self._plan_outcome
        self.transactions.planning_bounds.clear()
        self.transactions.planning_descending.clear()
        advisory = (
            isinstance(self.execution, AutoKeysetExecution)
            and self.execution.total_hint is TotalHintMode.REQUEST_ADVISORY
        )
        boundary_plans = []
        for ordinal, direction in enumerate(("ASC", "DESC")):
            request = build_controlled_request(
                self,
                direction=direction,
                limit=self.effective_page_cap,
                advisory_start=advisory,
            )
            lane = LaneState(
                LaneSpec(
                    ordinal,
                    LaneKind.HEAD if ordinal == 0 else LaneKind.TAIL,
                    LaneBounds(None, None),
                    ordinal == 1,
                    True,
                    None,
                ),
                None,
                LaneStatus.OPEN,
                None,
                0,
                self.effective_page_cap,
                deque(),
            )
            boundary_plans.append(build_lane_plan(self, lane, phase=KeysetPhase.BOUNDARY, request=request))
        asc, desc = await self.execute_wave(tuple(boundary_plans))
        contradiction = validate_boundary_direction(ascending=asc, descending=desc)
        if contradiction is not None:
            self.violations.append(contradiction.violation)
            reject_boundary_observations(
                self.transactions.staged_observations,
                self.record_page,
                self.admission,
                rows=len(asc.rows) + len(desc.rows),
                violation=contradiction.violation,
                raw=True,
            )
            raise PaginationError(contradiction.detail)
        self.admission.record_raw(len(asc.rows) + len(desc.rows))
        await self.adjust_buffer(len(asc.rows) + len(desc.rows))
        available_rows = self.context.policy.max_buffered_rows - self.transactions.buffer_balance
        self.batch_capacity = min(self.batch_capacity, available_rows // self.effective_page_cap)
        head, tail = (asc, desc) if self.keyset.direction == "ascending" else (desc, asc)
        self._tail = tail
        self._head_rows, self._tail_rows = len(head.rows), len(tail.rows)
        asc_total, desc_total = (self.transactions.boundary_totals.get(receipt.command_id) for receipt in (asc, desc))
        analysis = analyze_boundary(
            asc,
            desc,
            ascending_total=asc_total,
            descending_total=desc_total,
            advisory=advisory,
        )
        self.transactions.boundary_totals.clear()
        self._total_hint = analysis.total_hint
        self._interior_span = analysis.interior_span
        self._density_num, self._density_den = analysis.density_numerator, analysis.density_denominator
        selected: KeysetExecutionKind
        if isinstance(self.execution, AutoKeysetExecution):
            inputs = SelectorInputs(
                analysis.facts,
                self.effective_page_cap,
                self.batch_capacity,
                self.execution.target_lanes,
                self.execution.max_range_waves,
                self.execution.range_window_width,
                self.completion,
                self._total_hint.observed if self._total_hint.plausible else None,
                self.keyset.direction == "descending",
            )
            self._preselection = constrain_anchor_preselection(
                preselect(inputs),
                anchor_capacity=anchor_capable_batch_capacity(
                    current=self.batch_capacity,
                    available_rows=available_rows,
                    page_cap=self.effective_page_cap,
                    target_lanes=self.execution.target_lanes,
                ),
            )
            self._reason = self._preselection.reason
            if self._total_hint.plausible:
                baseline = preselect(replace(inputs, advisory_total=None))
                used = self._preselection.interior_rows_estimate > baseline.interior_rows_estimate
                self._total_hint = replace(self._total_hint, used=used)
            self._interior_estimate = self._preselection.interior_rows_estimate
            self._total_estimate = self._preselection.total_rows_estimate
            bounded = self._preselection.plan in {Preselected.RANGE, Preselected.PROBE_ANCHORS}
            if (
                bounded
                and self.completion is KeysetPageCompletion.SHORT_PAGE_EXHAUSTS
                and (len(asc.rows) != self.effective_page_cap or len(desc.rows) != self.effective_page_cap)
            ):
                raise_boundary_cap_contradiction(
                    self.transactions.staged_observations,
                    self.record_page,
                    self.admission,
                    self.violations,
                    len(asc.rows) + len(desc.rows),
                )
            if self._preselection.plan is Preselected.BOUNDARY_ONLY:
                selected = KeysetExecutionKind.BOUNDARY_ONLY
            elif self._preselection.plan is Preselected.SEQUENTIAL:
                selected = KeysetExecutionKind.SEQUENTIAL
            elif self._preselection.plan is Preselected.RANGE:
                await self._canaries(asc, desc)
                selected = KeysetExecutionKind.RANGE
            else:
                anchors = await self._partition_planning(asc, desc, self.execution.target_lanes)
                facts = AnchorFacts(tuple(anchors), self._anchor_probe_commands, self._empty_anchor_probes)
                self._final = finalize(inputs, self._preselection, facts)
                selected = self._final.kind
                if selected is not KeysetExecutionKind.PARTITIONED:
                    self.admission.record_discarded(self._anchor_count)
                    await self.adjust_buffer(-self._anchor_count)
                    self.transactions.anchor_rows.clear()
                    self.transactions.anchor_commands.clear()
            self._selected_estimate = (
                self._final.estimate.requests
                if self._final is not None
                else self._preselection.sequential_estimate.requests
                if selected is KeysetExecutionKind.SEQUENTIAL
                else self._preselection.range_estimate.requests
                if self._preselection.range_estimate is not None and selected is KeysetExecutionKind.RANGE
                else int(bool(asc.rows))
            )
            self._target_lanes = self.execution.target_lanes
        elif not asc.rows or analysis.facts.overlapping or analysis.facts.adjacent:
            selected = KeysetExecutionKind.BOUNDARY_ONLY
            self._reason = (
                KeysetSelectionReason.EXPLICIT_PARTITIONED
                if isinstance(self.execution, PartitionedKeysetExecution)
                else KeysetSelectionReason.EXPLICIT_RANGE
            )
        else:
            if self.completion is KeysetPageCompletion.SHORT_PAGE_EXHAUSTS and (
                len(asc.rows) != self.effective_page_cap or len(desc.rows) != self.effective_page_cap
            ):
                raise_boundary_cap_contradiction(
                    self.transactions.staged_observations,
                    self.record_page,
                    self.admission,
                    self.violations,
                    len(asc.rows) + len(desc.rows),
                )
            if isinstance(self.execution, RangeKeysetExecution):
                await self._canaries(asc, desc)
                selected = KeysetExecutionKind.RANGE
                self._reason = KeysetSelectionReason.EXPLICIT_RANGE
            elif isinstance(self.execution, PartitionedKeysetExecution):
                selected = KeysetExecutionKind.PARTITIONED
                anchors = await self._partition_planning(asc, desc, self.execution.target_lanes)
                self._reason = (
                    KeysetSelectionReason.EXPLICIT_PARTITIONED
                    if anchors
                    else KeysetSelectionReason.DEGENERATE_SINGLE_LANE
                )
                self._target_lanes = self.execution.target_lanes
            else:
                raise TypeError("unknown fast keyset execution")
        flush_staged_observations(self.transactions.staged_observations, self.record_page)
        self._selected = selected
        if not asc.rows or analysis.facts.overlapping or analysis.facts.adjacent:
            self._density_num = self._density_den = None
        await self._configure_selected(asc, desc)
        plan = FastKeysetPlan(
            selected,
            tuple(lane.spec for lane in self.transactions.lanes),
            self.page_size,
            self.effective_page_cap,
            self.batch_capacity,
            self.completion,
            self.identity,
            self.keyset,
            self.collection_shape,
            self.transactions.finish_cursor,
        )
        self._plan_outcome = PlanOutcome(plan, self._preselection, self._final)
        self._planned = True
        return self._plan_outcome

    async def _configure_selected(self, asc: LaneReceipt, desc: LaneReceipt) -> None:
        head = asc if self.keyset.direction == "ascending" else desc
        self._admit_receipt(head)
        self._head_admitted = bool(head.rows)
        if self._selected is KeysetExecutionKind.SEQUENTIAL:
            discarded = len(desc.rows) if head is asc else len(asc.rows)
            self.admission.record_probe_discarded(discarded)
            await self.adjust_buffer(-discarded)
            self._tail = None
            self.transactions.finish_cursor = head.identities[-1] if head.identities else None
            self.transactions.finishing = bool(head.rows)
            if not head.rows:
                self.transactions.terminal = True
            return
        if self._selected is KeysetExecutionKind.BOUNDARY_ONLY:
            tail = desc if head is asc else asc
            await self._admit_tail(tail)
            self._tail = None
            self.transactions.finish_cursor = tail.identities[0] if tail.identities else None
            self.transactions.finishing = bool(tail.rows)
            if not head.rows:
                self.transactions.terminal = True
            return
        if self._selected is KeysetExecutionKind.RANGE:
            if not isinstance(self.execution, RangeKeysetExecution | AutoKeysetExecution):
                raise TypeError("range selection requires range or auto execution")
            width, count = selected_range_geometry(
                execution=self.execution,
                completion=self.completion,
                page_cap=self.effective_page_cap,
                ascending=asc.identities,
                descending=desc.identities,
            )
            self._window_width, self._window_count = width, count
            self._range_geometry = LazyRangePlan(
                max(asc.identities),
                min(desc.identities),
                width,
                count,
                self.keyset.direction == "descending",
            )
            self._range_geometry.fill(
                self.batch_capacity,
                self.transactions.lanes,
                self.transactions.lane_rows,
                self.transactions.lane_identities,
                self.transactions.lane_commands,
            )
            return
        geometry = selected_lane_geometry(
            selected=self._selected,
            execution=self.execution,
            keyset=self.keyset,
            completion=self.completion,
            page_cap=self.effective_page_cap,
            ascending=asc.identities,
            descending=desc.identities,
            anchors=tuple(sorted(self.transactions.anchor_rows)),
        )
        specs = geometry.specs
        self.transactions.lanes = [
            LaneState(
                spec,
                spec.bounds.upper_exclusive if spec.descending else spec.bounds.lower_exclusive,
                LaneStatus.OPEN,
                None,
                0,
                0,
                deque(),
            )
            for spec in specs
        ]
        self.transactions.lane_rows = {lane.spec.ordinal: [] for lane in self.transactions.lanes}
        self.transactions.lane_identities = {lane.spec.ordinal: [] for lane in self.transactions.lanes}
        self.transactions.lane_commands = {lane.spec.ordinal: [] for lane in self.transactions.lanes}

    def _admit_receipt(self, receipt: LaneReceipt) -> None:
        commit = self.admission.validate_and_commit(receipt)
        self.trace.admit(receipt.command_id, len(commit.rows))
        if commit.rows:
            self.transactions.pending.append(commit.rows)

    async def _admit_tail(self, tail: LaneReceipt) -> None:
        normalized, overlap = normalize_tail_receipt(tail, already_seen=self.admission.has_seen)
        self.admission.record_boundary_overlap(overlap)
        await self.adjust_buffer(-overlap)
        self._admit_receipt(normalized)

    async def next_rows(self) -> tuple[JsonValue, ...]:
        """Return the next ordered row group, or an empty tuple at completion."""
        if self._closed or self.transactions.terminal:
            return ()
        if self._offered_rows:
            await self.adjust_buffer(-self._offered_rows)
            self._offered_rows = 0
        if not self._planned:
            await self.plan_barrier()
        while not self.transactions.pending and not self.transactions.terminal:
            if self.transactions.lanes and self.transactions.lane_index < len(self.transactions.lanes):
                await self._body_wave()
                continue
            if self._selected in {KeysetExecutionKind.RANGE, KeysetExecutionKind.PARTITIONED} and not (
                self.transactions.finishing
            ):
                if self._tail is None:
                    raise RuntimeError("bounded plan lost its tail")
                await self._admit_tail(self._tail)
                self.transactions.finish_cursor = (
                    max(self._tail.identities) if self.keyset.direction == "ascending" else min(self._tail.identities)
                )
                self.transactions.finishing = True
                self._tail = None
                continue
            if self.transactions.finishing:
                await self._finish_page()
                continue
            self.transactions.terminal = True
        if not self.transactions.pending:
            return ()
        rows = self.transactions.pending.popleft()
        self._offered_rows = len(rows)
        return rows

    async def _body_wave(self) -> None:
        await execute_body_wave(self)

    async def _finish_page(self) -> None:
        finish_plan = keyset_step.sequential_keyset_plan(self.keyset, self.page_size)
        request = keyset_step.keyset_page_request(
            self.request,
            plan=finish_plan,
            identity=self.identity,
            cursor=self.transactions.finish_cursor,
        )
        await execute_finish_page(self, finish_plan, request)

    async def aclose(self) -> None:
        """Release retained scheduler state exactly once."""
        await close_scheduler(self)

    def report_fragment(self) -> KeysetExecutionReport:
        """Return the current or frozen redacted execution report."""
        if self._frozen_report is not None:
            return self._frozen_report
        return build_scheduler_report(self)


__all__ = ["KeysetFastScheduler", "PlanOutcome"]
