"""One ordered scheduler for range, occupied-anchor, and automatic keysets."""
# ruff: noqa: D102, D107, FBT003, I001, PERF401, PLC0415, PLR0915, SLF001, TRY301
from __future__ import annotations
from collections import Counter, deque
from dataclasses import replace
from typing import TYPE_CHECKING

from b24api.batch.outcome import BatchSuccess
from b24api.contracts.keyset_execution import (
    AutoKeysetExecution, ClosureWitness, KeysetExecutionKind, KeysetPageCompletion, KeysetPhase,
    KeysetSelectionReason, PartitionedKeysetExecution, RangeKeysetExecution, TotalHintMode,
)
from b24api.contracts.report import (
    KeysetExecutionReport, PageDispatch, PageOutcome, PageRejectionCode, Violation, ViolationSeverity,
)
from b24api.errors import PaginationError
from b24api.traversal import keyset_step
from b24api.traversal.keyset_auto import (
    AnchorFacts, FinalSelection, Preselected, Preselection, SelectorInputs, TotalHintState, finalize, preselect,
)
from b24api.traversal.keyset_capability import (
    CapabilityCommand, analyze_boundary, anchor_capable_batch_capacity, anchor_commands, boundary_totals,
    canary_commands, compact_anchor_receipts, descending_closure_witness,
    lane_for_command, normalize_anchor_receipts, page_observation, selected_lane_geometry,
)
from b24api.traversal.keyset_fast_plan import (
    FastKeysetPlan, LaneBounds, LaneKind, LaneSpec, LaneState, LaneStatus, PlanOutcome, fit_wave,
)
from b24api.traversal.ordered_admission import FastCounters, OrderedAdmissionState
from b24api.traversal.page_validation import (
    LaneCommandPlan, LaneReceipt, ReceiptRejection, classify_rejection, validate_boundary_direction,
    validate_lane_receipt,
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
        self.executor = executor
        self.request = request
        self.identity = identity
        self.keyset = keyset
        self.selector, self.collection_shape = selector, collection_shape
        self.page_size, self.effective_page_cap = page_size, effective_page_cap
        self.execution = execution
        self.context = context
        self.engine = engine
        self.trace = trace
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
        self._planning_requests = Counter[KeysetPhase]()
        self._planning_physical_requests = 0
        self._closures = Counter[ClosureWitness]()
        self._continuations = 0
        self._observation_ordinal = 0
        self._command_ordinal = 0
        self._planned, self._closed, self._terminal = False, False, False
        self._plan_outcome: PlanOutcome | None = None
        self._preselection: Preselection | None = None
        self._final: FinalSelection | None = None
        self._total_hint = TotalHintState(False, None, False, False)
        self._tail: LaneReceipt | None = None
        self._head_admitted = False
        self._selected = KeysetExecutionKind.RANGE
        self._reason = KeysetSelectionReason.EXPLICIT_RANGE
        self._lanes: list[LaneState] = []
        self._lane_rows: dict[int, list[JsonValue]] = {}
        self._lane_identities: dict[int, list[int]] = {}
        self._lane_commands: dict[int, list[tuple[str, int]]] = {}
        self._lane_index = 0
        self._anchor_rows: dict[int, JsonValue] = {}
        self._anchor_commands: dict[int, str] = {}
        self._anchor_count, self._anchor_probe_commands, self._empty_anchor_probes = 0, 0, 0
        self._target_lanes: int | None = None
        self._window_width: int | None = None
        self._window_count: int | None = None
        self._pending: deque[tuple[JsonValue, ...]] = deque()
        self._offered_rows, self._buffer_balance = 0, 0
        self._finish_cursor: int | None = None
        self._finishing = False
        self._head_rows, self._tail_rows = 0, 0
        self._interior_span: int | None = None
        self._density_num: int | None = None
        self._density_den: int | None = None
        self._interior_estimate: int | None = None
        self._total_estimate: int | None = None
        self._selected_estimate: int | None = None
        self._frozen_report: KeysetExecutionReport | None = None
        self._planning_bounds: dict[str, LaneBounds] = {}
        self._planning_descending: dict[str, bool] = {}
        self._boundary_totals: dict[str, int | None] = {}
    @property
    def counters(self) -> FastCounters:
        return self.admission.snapshot_counters()
    @property
    def terminal(self) -> bool:
        return self._terminal
    def mark_emitted(self, count: int) -> None:
        self.admission.mark_emitted(count)
    def _next_command_id(self, phase: KeysetPhase) -> str:
        value = f"{phase.value}-{self._command_ordinal}"
        self._command_ordinal += 1
        return value
    async def _adjust_buffer(self, delta: int) -> None:
        if not delta:
            return
        await self.context.adjust_buffered_rows(delta)
        self._buffer_balance += delta
    def _lane_plan(self, lane: LaneState, *, phase: KeysetPhase, request: Request,
                   reserve: int | None = None, single: bool = False) -> LaneCommandPlan:
        return LaneCommandPlan(
            lane.spec.ordinal, self._next_command_id(phase), phase, request,
            reserve or self.effective_page_cap, single,
        )
    def _controls(self, *, direction: str, lower: int | None = None, upper: int | None = None,
                  limit: int | None = None, advisory_start: bool = False) -> Request:
        return keyset_step.bounded_keyset_request(
            self.request, keyset=self.keyset, identity=self.identity, direction=direction,
            lower=lower, upper=upper, limit=limit, advisory_start=advisory_start,
        )
    def _record(  # noqa: PLR0913
        self, plan: LaneCommandPlan, *, index: int | None, selected: int, admitted: int,
        outcome: PageOutcome = PageOutcome.COMMITTED,
        rejection: PageRejectionCode | None = None,
        violation: Violation | None = None,
        response: Response | None = None,
        witness: ClosureWitness | None = None,
        dispatch: PageDispatch = PageDispatch.BATCH,
    ) -> None:
        self.trace.record(page_observation(
            self._observation_ordinal, plan, index=index, selected=selected, admitted=admitted,
            effective_page_cap=self.effective_page_cap, outcome=outcome, rejection=rejection,
            violation=violation, response=response, witness=witness, dispatch=dispatch,
        ))
        self._observation_ordinal += 1
    async def _wave(self, plans: tuple[LaneCommandPlan, ...]) -> tuple[LaneReceipt, ...]:  # noqa: C901, PLR0912
        if not plans or len(plans) > self.engine.portal_command_cap:
            raise ValueError("fast keyset wave must contain 1..50 commands")
        reservations = []
        reserved_rows = sum(plan.reserved_rows for plan in plans)
        if reserved_rows > self.context.policy.max_buffered_rows - self._buffer_balance:
            raise RuntimeError("fast keyset wave exceeds currently available row capacity")
        try:
            for _ in plans:
                reservations.append(await self.context.reserve_page())
            await self._adjust_buffer(reserved_rows)
            advisory = (all(plan.phase is KeysetPhase.BOUNDARY for plan in plans)
                        and getattr(getattr(self.execution, "total_hint", None), "value", None) == "request_advisory")
            outcomes = await self.engine.execute_requests(
                tuple(plan.request for plan in plans), context=self.context, advisory_totals=advisory,
                strict_envelope=True, strict_json_members=True,
            )
            self._boundary_totals = boundary_totals(plans, outcomes)
            self.batch_requests += 1
            self.batch_commands += len(plans)
            planning = {KeysetPhase.BOUNDARY, KeysetPhase.CANARY, KeysetPhase.ANCHOR_PROBE}
            phases = {plan.phase for plan in plans}
            self._planning_physical_requests += int(bool(phases & planning))
            for phase in phases & planning:
                self._planning_requests[phase] += 1
            receipts: list[LaneReceipt] = []
            failed = False
            for index, (plan, outcome, reservation) in enumerate(zip(plans, outcomes, reservations, strict=True)):
                commit = self.context.commit_page if isinstance(outcome, BatchSuccess) else self.context.release_page
                commit(reservation)
                lane = lane_for_command(
                    plan, planning_bounds=self._planning_bounds, planning_descending=self._planning_descending,
                    finish_lane=getattr(self, "_finish_lane", None), lanes=self._lanes)
                receipt = validate_lane_receipt(
                    plan=plan, lane=lane,
                    outcome=outcome, identity=self.identity,
                    collection_shape=self.collection_shape,
                    effective_page_cap=plan.reserved_rows if plan.expects_single_row else self.effective_page_cap,
                    completion=self.completion, selector=self.selector,
                )
                if isinstance(receipt, ReceiptRejection):
                    failed = True
                    self.violations.append(receipt.violation)
                    page_outcome, rejection_code = classify_rejection(outcome)
                    self._record(
                        plan, index=index, selected=0, admitted=0, outcome=page_outcome,
                        rejection=rejection_code, violation=receipt.violation,
                    )
                else:
                    receipts.append(receipt)
            if failed:
                successful = {receipt.command_id: receipt for receipt in receipts}
                for index, plan in enumerate(plans):
                    if plan.command_id in successful:
                        self._record(
                            plan, index=index, selected=len(successful[plan.command_id].rows), admitted=0,
                            outcome=PageOutcome.REJECTED, rejection=PageRejectionCode.TRANSACTION_ABORTED,
                        )
                raise PaginationError("fast keyset wave validation failed")
            for index, (plan, receipt) in enumerate(zip(plans, receipts, strict=True)):
                outcome = outcomes[index]
                self._record(
                    plan, index=index, selected=len(receipt.rows), admitted=0,
                    response=outcome.response if isinstance(outcome, BatchSuccess) else None,
                    witness=receipt.witness,
                )
            return tuple(receipts)
        finally:
            for reservation in reservations:
                self.context.release_page(reservation)
            await self._adjust_buffer(-min(reserved_rows, self._buffer_balance))
    def _capability_plans(self, commands: tuple[CapabilityCommand, ...],
                          phase: KeysetPhase) -> tuple[LaneCommandPlan, ...]:
        plans = []
        for command in commands:
            bounds = command.bounds
            request = self._controls(direction="DESC" if command.descending else "ASC",
                                     lower=bounds.lower_exclusive, upper=bounds.upper_exclusive,
                                     limit=1 if command.expects_single_row else self.effective_page_cap)
            lane = LaneState(
                LaneSpec(command.ordinal, LaneKind.LANE, bounds, command.descending, False, None),
                bounds.upper_exclusive if command.descending else bounds.lower_exclusive,
                LaneStatus.OPEN, None, 0, command.reserve, deque(),
            )
            plan = self._lane_plan(lane, phase=phase, request=request, reserve=command.reserve,
                                   single=command.expects_single_row)
            self._planning_bounds[plan.command_id] = bounds
            self._planning_descending[plan.command_id] = command.descending
            plans.append(plan)
        return tuple(plans)
    def _canary_plans(self, asc: LaneReceipt,
                      desc: LaneReceipt) -> tuple[tuple[LaneCommandPlan, ...], dict[str, tuple[int, ...]]]:
        commands = canary_commands(asc.identities, desc.identities, self.effective_page_cap)
        plans = self._capability_plans(commands, KeysetPhase.CANARY)
        expected = {
            plan.command_id: command.expected
            for plan, command in zip(plans, commands, strict=True)
            if command.expected is not None
        }
        return plans, expected
    def _anchor_plans(self, asc: LaneReceipt, desc: LaneReceipt, target: int) -> tuple[LaneCommandPlan, ...]:
        commands = anchor_commands(
            lo=max(asc.identities), upper_exclusive=min(desc.identities), target_lanes=target,
            page_cap=self.effective_page_cap, writable_limit=self.keyset.limit_path is not None,
        )
        return self._capability_plans(commands, KeysetPhase.ANCHOR_PROBE)
    def _validate_canaries(self, receipts: tuple[LaneReceipt, ...],
                           expected: dict[str, tuple[int, ...]]) -> None:
        by_id = {receipt.command_id: receipt for receipt in receipts}
        if any(by_id[command_id].identities != values for command_id, values in expected.items()):
            violation = Violation(
                ViolationSeverity.BLOCKING, "canary_contradiction", "bounded keyset capability canary failed",
            )
            self.violations.append(violation)
            raise PaginationError("bounded keyset capability canary failed")
        self.admission.record_raw(sum(len(receipt.rows) for receipt in receipts), discarded=True)
    async def _chunked_waves(
        self, plans: tuple[LaneCommandPlan, ...], *, compact_anchors: bool = False,
    ) -> tuple[LaneReceipt, ...]:
        receipts: list[LaneReceipt] = []
        remaining = plans
        while remaining:
            wave = fit_wave(
                remaining, reserves=tuple(plan.reserved_rows for plan in remaining), commands=self.batch_capacity,
                rows=self.context.policy.max_buffered_rows - self._buffer_balance,
            )
            if not wave:
                raise RuntimeError("fast keyset planning wave has no available row capacity")
            wave_receipts = await self._wave(wave)
            if compact_anchors:
                wave_receipts, discarded, retained = compact_anchor_receipts(wave_receipts)
                self.admission.record_raw(discarded)
                self.admission.record_discarded(discarded)
                await self._adjust_buffer(retained)
            receipts.extend(wave_receipts)
            remaining = remaining[len(wave) :]
        return tuple(receipts)
    async def _canaries(self, asc: LaneReceipt, desc: LaneReceipt) -> None:
        plans, expected = self._canary_plans(asc, desc)
        self._validate_canaries(await self._chunked_waves(plans), expected)
    def _consume_anchors(self, receipts: tuple[LaneReceipt, ...], *, lo: int, hi: int) -> tuple[int, ...]:
        result = normalize_anchor_receipts(receipts, lo=lo, upper_exclusive=hi)
        self._anchor_rows = result.rows
        self._anchor_commands = result.commands
        self._empty_anchor_probes = result.empty_probes
        self._anchor_count = len(result.anchors)
        self.admission.record_raw(result.raw_rows)
        self.admission.record_discarded(result.discarded_rows)
        return result.anchors
    async def _partition_planning(self, asc: LaneReceipt, desc: LaneReceipt, target: int) -> tuple[int, ...]:
        canaries, expected = self._canary_plans(asc, desc)
        anchors = self._anchor_plans(asc, desc, target)
        self._anchor_probe_commands = len(anchors)
        planning = (*canaries, *anchors)
        co_scheduled = fit_wave(
            planning, reserves=tuple(plan.reserved_rows for plan in planning), commands=self.batch_capacity,
            rows=self.context.policy.max_buffered_rows - self._buffer_balance,
        )
        precharged = 0
        if len(co_scheduled) == len(planning):
            receipts = await self._wave(co_scheduled)
            canary_receipts, anchor_receipts = receipts[: len(canaries)], receipts[len(canaries) :]
        else:
            canary_receipts = await self._chunked_waves(canaries)
            anchor_receipts = await self._chunked_waves(anchors, compact_anchors=True) if anchors else ()
            precharged = sum(bool(receipt.rows) for receipt in anchor_receipts)
        self._validate_canaries(canary_receipts, expected)
        result = self._consume_anchors(anchor_receipts, lo=max(asc.identities), hi=min(desc.identities))
        await self._adjust_buffer(self._anchor_count - precharged)
        return result
    def _drain_admission_frontier(self) -> None:
        while self._lane_index < len(self._lanes):
            lane = self._lanes[self._lane_index]
            identities = self._lane_identities[lane.spec.ordinal]
            rows = self._lane_rows[lane.spec.ordinal]
            commands = self._lane_commands[lane.spec.ordinal]
            if rows or commands:
                receipt = LaneReceipt(
                    lane.spec.ordinal, f"body-admit-{lane.spec.ordinal}", tuple(rows), tuple(identities), False,
                    identities[-1] if identities else None, lane.witness, (),
                )
                self._admit_receipt(receipt)
                anchor = lane.spec.retained_upper_anchor if lane.status is LaneStatus.CLOSED else None
                for index, (command_id, count) in enumerate(commands):
                    self.trace.admit(command_id, count + int(anchor is not None and index == len(commands) - 1))
                rows.clear()
                identities.clear()
                commands.clear()
            if lane.status is LaneStatus.OPEN:
                return
            self._lane_index += 1
    async def plan_barrier(self) -> PlanOutcome:  # noqa: C901, PLR0912
        if self._planned:
            if self._plan_outcome is None:
                raise RuntimeError("planned scheduler has no outcome")
            return self._plan_outcome
        self._planning_bounds.clear()
        self._planning_descending.clear()
        advisory = (
            isinstance(self.execution, AutoKeysetExecution)
            and self.execution.total_hint is TotalHintMode.REQUEST_ADVISORY
        )
        boundary_plans = []
        for ordinal, direction in enumerate(("ASC", "DESC")):
            request = self._controls(direction=direction, limit=self.effective_page_cap, advisory_start=advisory)
            lane = LaneState(
                LaneSpec(ordinal, LaneKind.HEAD if ordinal == 0 else LaneKind.TAIL, LaneBounds(None, None),
                         ordinal == 1, True, None),
                None, LaneStatus.OPEN, None, 0, self.effective_page_cap, deque(),
            )
            boundary_plans.append(self._lane_plan(lane, phase=KeysetPhase.BOUNDARY, request=request))
        asc, desc = await self._wave(tuple(boundary_plans))
        contradiction = validate_boundary_direction(ascending=asc, descending=desc)
        if contradiction is not None:
            self.violations.append(contradiction.violation)
            raise PaginationError(contradiction.detail)
        self.admission.record_raw(len(asc.rows) + len(desc.rows))
        await self._adjust_buffer(len(asc.rows) + len(desc.rows))
        available_rows = self.context.policy.max_buffered_rows - self._buffer_balance
        self.batch_capacity = min(self.batch_capacity, available_rows // self.effective_page_cap)
        head, tail = (asc, desc) if self.keyset.direction == "ascending" else (desc, asc)
        self._tail = tail
        self._head_rows, self._tail_rows = len(head.rows), len(tail.rows)
        asc_total, desc_total = (self._boundary_totals.get(receipt.command_id) for receipt in (asc, desc))
        analysis = analyze_boundary(
            asc, desc,
            ascending_total=asc_total,
            descending_total=desc_total,
            advisory=advisory,
        )
        self._boundary_totals.clear()
        self._total_hint = analysis.total_hint
        self._interior_span = analysis.interior_span
        self._density_num, self._density_den = analysis.density_numerator, analysis.density_denominator
        selected: KeysetExecutionKind
        if isinstance(self.execution, AutoKeysetExecution):
            selector_capacity = anchor_capable_batch_capacity(
                current=self.batch_capacity, available_rows=available_rows,
                page_cap=self.effective_page_cap, target_lanes=self.execution.target_lanes,
            )
            inputs = SelectorInputs(
                analysis.facts, self.effective_page_cap, selector_capacity, self.execution.target_lanes,
                self.execution.max_range_waves, self.execution.range_window_width, self.completion,
                self._total_hint.observed if self._total_hint.plausible else None,
            )
            self._preselection = preselect(inputs)
            if self._total_hint.plausible:
                baseline = preselect(replace(inputs, advisory_total=None))
                used = self._preselection.interior_rows_estimate > baseline.interior_rows_estimate
                self._total_hint = replace(self._total_hint, used=used)
            self._interior_estimate = self._preselection.interior_rows_estimate
            self._total_estimate = self._preselection.total_rows_estimate
            bounded = self._preselection.plan in {Preselected.RANGE, Preselected.PROBE_ANCHORS}
            if (bounded and self.completion is KeysetPageCompletion.SHORT_PAGE_EXHAUSTS
                    and (len(asc.rows) != self.effective_page_cap or len(desc.rows) != self.effective_page_cap)):
                raise PaginationError("boundary pages did not establish page-cap agreement")
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
                    await self._adjust_buffer(-self._anchor_count)
                    self._anchor_rows.clear()
                    self._anchor_commands.clear()
            self._reason = self._preselection.reason
            self._selected_estimate = (
                self._final.estimate.requests
                if self._final is not None
                else self._preselection.sequential_estimate.requests
                if selected is KeysetExecutionKind.SEQUENTIAL
                else self._preselection.range_estimate.requests
                if self._preselection.range_estimate is not None and selected is KeysetExecutionKind.RANGE
                else 1 + int(bool(asc.rows))
            )
            self._target_lanes = self.execution.target_lanes
        elif not asc.rows or analysis.facts.overlapping or analysis.facts.adjacent:
            selected = KeysetExecutionKind.BOUNDARY_ONLY
            self._reason = (KeysetSelectionReason.EXPLICIT_PARTITIONED
                            if isinstance(self.execution, PartitionedKeysetExecution)
                            else KeysetSelectionReason.EXPLICIT_RANGE)
        else:
            if (self.completion is KeysetPageCompletion.SHORT_PAGE_EXHAUSTS
                    and (len(asc.rows) != self.effective_page_cap or len(desc.rows) != self.effective_page_cap)):
                raise PaginationError("boundary pages did not establish page-cap agreement")
            if isinstance(self.execution, RangeKeysetExecution):
                await self._canaries(asc, desc)
                selected = KeysetExecutionKind.RANGE
                self._reason = KeysetSelectionReason.EXPLICIT_RANGE
            elif isinstance(self.execution, PartitionedKeysetExecution):
                selected = KeysetExecutionKind.PARTITIONED
                self._reason = KeysetSelectionReason.EXPLICIT_PARTITIONED
                await self._partition_planning(asc, desc, self.execution.target_lanes)
                self._target_lanes = self.execution.target_lanes
            else:
                raise TypeError("unknown fast keyset execution")
        self._selected = selected
        await self._configure_selected(asc, desc)
        plan = FastKeysetPlan(selected, tuple(lane.spec for lane in self._lanes), self.page_size,
                              self.effective_page_cap, self.batch_capacity, self.completion, self.identity,
                              self.keyset, self.collection_shape, self._finish_cursor)
        self._plan_outcome = PlanOutcome(plan, self._preselection, self._final)
        self._planned = True
        return self._plan_outcome
    async def _configure_selected(self, asc: LaneReceipt, desc: LaneReceipt) -> None:
        head = asc if self.keyset.direction == "ascending" else desc
        self._admit_receipt(head)
        self._head_admitted = bool(head.rows)
        if self._selected is KeysetExecutionKind.SEQUENTIAL:
            discarded = len(desc.rows) if head is asc else len(asc.rows)
            self.admission._counters.probe_rows_discarded += discarded
            await self._adjust_buffer(-discarded)
            self._tail = None
            self._finish_cursor = head.identities[-1] if head.identities else None
            self._finishing = bool(head.rows)
            if not head.rows:
                self._terminal = True
            return
        if self._selected is KeysetExecutionKind.BOUNDARY_ONLY:
            tail = desc if head is asc else asc
            self._admit_tail(tail)
            self._tail = None
            self._finish_cursor = tail.identities[0] if tail.identities else None
            self._finishing = bool(tail.rows)
            if not head.rows:
                self._terminal = True
            return
        geometry = selected_lane_geometry(
            selected=self._selected, execution=self.execution, keyset=self.keyset, completion=self.completion,
            page_cap=self.effective_page_cap, ascending=asc.identities, descending=desc.identities,
            anchors=tuple(sorted(self._anchor_rows)),
        )
        specs = geometry.specs
        self._window_width, self._window_count = geometry.window_width, geometry.window_count
        self._lanes = [
            LaneState(
                spec, spec.bounds.upper_exclusive if spec.descending else spec.bounds.lower_exclusive,
                LaneStatus.OPEN, None, 0, 0, deque(),
            )
            for spec in specs
        ]
        self._lane_rows = {lane.spec.ordinal: [] for lane in self._lanes}
        self._lane_identities = {lane.spec.ordinal: [] for lane in self._lanes}
        self._lane_commands = {lane.spec.ordinal: [] for lane in self._lanes}
    def _admit_receipt(self, receipt: LaneReceipt) -> None:
        commit = self.admission.validate_and_commit(receipt)
        self.trace.admit(receipt.command_id, len(commit.rows))
        if commit.rows:
            self._pending.append(commit.rows)
    def _admit_tail(self, tail: LaneReceipt) -> None:
        rows_ids = list(zip(tail.rows, tail.identities, strict=True))
        rows_ids.reverse()
        filtered = [(row, identity) for row, identity in rows_ids if not self.admission.has_seen(identity)]
        overlap = len(rows_ids) - len(filtered)
        self.admission.record_boundary_overlap(overlap)
        normalized = replace(tail, rows=tuple(row for row, _ in filtered),
                             identities=tuple(identity for _, identity in filtered),
                             last_identity=filtered[-1][1] if filtered else None)
        self._admit_receipt(normalized)
    async def next_rows(self) -> tuple[JsonValue, ...]:
        if self._closed or self._terminal:
            return ()
        if self._offered_rows:
            await self._adjust_buffer(-self._offered_rows)
            self._offered_rows = 0
        if not self._planned:
            await self.plan_barrier()
        while not self._pending and not self._terminal:
            if self._lanes and self._lane_index < len(self._lanes):
                await self._body_wave()
                continue
            if self._selected in {KeysetExecutionKind.RANGE, KeysetExecutionKind.PARTITIONED} and not self._finishing:
                if self._tail is None:
                    raise RuntimeError("bounded plan lost its tail")
                self._admit_tail(self._tail)
                self._finish_cursor = (
                    max(self._tail.identities) if self.keyset.direction == "ascending" else min(self._tail.identities)
                )
                self._finishing = True
                self._tail = None
                continue
            if self._finishing:
                await self._finish_page()
                continue
            self._terminal = True
        if not self._pending:
            return ()
        rows = self._pending.popleft()
        self._offered_rows = len(rows)
        return rows
    async def _body_wave(self) -> None:  # noqa: C901
        open_lanes = [lane for lane in self._lanes[self._lane_index :] if lane.status is LaneStatus.OPEN]
        if not open_lanes:
            self._drain_admission_frontier()
            return
        candidates = []
        for lane in open_lanes[: self.batch_capacity]:
            lower = lane.spec.bounds.lower_exclusive
            upper = lane.spec.bounds.upper_exclusive
            if lane.spec.descending:
                upper = lane.cursor
            else:
                lower = lane.cursor
            request = self._controls(direction="DESC" if lane.spec.descending else "ASC",
                                     lower=lower, upper=upper, limit=self.effective_page_cap)
            candidates.append(self._lane_plan(lane, phase=KeysetPhase.BODY, request=request))
        plan_candidates = tuple(candidates)
        plans = fit_wave(plan_candidates, reserves=tuple(plan.reserved_rows for plan in plan_candidates),
                         commands=self.batch_capacity,
                         rows=self.context.policy.max_buffered_rows - self._buffer_balance)
        if not plans:
            raise RuntimeError("fast keyset body wave has no available row capacity")
        receipts = await self._wave(plans)
        retained = sum(len(receipt.rows) for receipt in receipts)
        await self._adjust_buffer(retained)
        self.admission.record_raw(retained)
        by_ordinal = {receipt.lane_ordinal: receipt for receipt in receipts}
        for lane in open_lanes[: len(plans)]:
            receipt = by_ordinal[lane.spec.ordinal]
            lane.rounds += 1
            self._lane_rows[lane.spec.ordinal].extend(receipt.rows)
            self._lane_identities[lane.spec.ordinal].extend(receipt.identities)
            self._lane_commands[lane.spec.ordinal].append((receipt.command_id, len(receipt.rows)))
            witness = receipt.witness
            if lane.spec.descending:
                witness = descending_closure_witness(
                    lane, receipt.identities, completion=self.completion, page_cap=self.effective_page_cap)
            if receipt.identities:
                lane.cursor = receipt.identities[-1]
            if witness is None:
                self._continuations += 1
                continue
            lane.status = LaneStatus.CLOSED
            lane.witness = witness
            anchor = lane.spec.retained_upper_anchor
            if anchor is not None:
                row = self._anchor_rows.get(anchor)
                if row is None:
                    raise PaginationError("partition lane lost its retained anchor")
                self._lane_rows[lane.spec.ordinal].append(row)
                self._lane_identities[lane.spec.ordinal].append(anchor)
                lane.witness = ClosureWitness.ANCHOR_FENCE
                self._closures[ClosureWitness.ANCHOR_FENCE] += 1
            else:
                self._closures[witness] += 1
        self._drain_admission_frontier()
    async def _finish_page(self) -> None:
        if self._finish_cursor is None:
            self._terminal = True
            return
        finish_plan = keyset_step.sequential_keyset_plan(self.keyset, self.page_size)
        request = keyset_step.keyset_page_request(
            self.request, plan=finish_plan, identity=self.identity, cursor=self._finish_cursor)
        direction = "ASC" if finish_plan.direction == "asc" else "DESC"
        bounds = LaneBounds(self._finish_cursor if direction == "ASC" else None,
                            self._finish_cursor if direction == "DESC" else None)
        spec = LaneSpec(0, LaneKind.FINISH, bounds, direction == "DESC", True, None)
        self._finish_lane = LaneState(spec, self._finish_cursor, LaneStatus.OPEN, None, 0, 0, deque())
        plan = self._lane_plan(self._finish_lane, phase=KeysetPhase.FINISH, request=request)
        reservation = await self.context.reserve_page()
        await self._adjust_buffer(self.effective_page_cap)
        try:
            response = await self.executor.execute(request, context=self.context)
            self.context.commit_page(reservation)
            outcome = BatchSuccess(0, "finish", request, response.result, response=response)
            receipt = validate_lane_receipt(
                plan=plan, lane=self._finish_lane, outcome=outcome, identity=self.identity,
                collection_shape=self.collection_shape, effective_page_cap=self.effective_page_cap,
                completion=KeysetPageCompletion.EMPTY_CONFIRMATION, selector=self.selector,
            )
            if isinstance(receipt, ReceiptRejection):
                self.violations.append(receipt.violation)
                self._record(
                    plan, index=None, selected=0, admitted=0, outcome=PageOutcome.REJECTED,
                    rejection=PageRejectionCode.RANGE_CONTRADICTION, violation=receipt.violation,
                    dispatch=PageDispatch.DIRECT,
                )
                raise PaginationError(receipt.detail)
            terminal = keyset_step.keyset_page_terminal(finish_plan, len(receipt.rows))
            await self._adjust_buffer(-self.effective_page_cap + len(receipt.rows))
            self.admission.record_raw(len(receipt.rows))
            self._record(
                plan, index=None, selected=len(receipt.rows), admitted=len(receipt.rows), response=response,
                witness=receipt.witness, dispatch=PageDispatch.DIRECT,
            )
            if terminal is not None:
                self._terminal = True
                self._finishing = False
                return
            self._finish_cursor = keyset_step.next_keyset_cursor(self._finish_cursor, receipt.identities)
            self._admit_receipt(receipt)
        except BaseException:
            self.context.release_page(reservation)
            raise
        finally:
            if self._buffer_balance > self.context.policy.max_buffered_rows:
                raise RuntimeError("fast scheduler buffer accounting escaped policy")
    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._frozen_report = self.report_fragment()
        if self._buffer_balance:
            await self._adjust_buffer(-self._buffer_balance)
        if self._buffer_balance != 0:
            raise RuntimeError("fast scheduler buffer balance survived cleanup")
        for retained in (self._pending, self._lane_rows, self._lane_identities, self._lane_commands, self._lanes):
            retained.clear()
        self._tail = None
        self._anchor_rows.clear()
        self._anchor_commands.clear()
        self._boundary_totals.clear()
        self._planning_bounds.clear()
        self._planning_descending.clear()
        self._plan_outcome = None
        self.admission.assert_clean()
        self.admission.close()
    def report_fragment(self) -> KeysetExecutionReport:
        if self._frozen_report is not None:
            return self._frozen_report
        from b24api.traversal.keyset_reporting import build_scheduler_report
        return build_scheduler_report(self)
__all__ = ["KeysetFastScheduler", "PlanOutcome"]
