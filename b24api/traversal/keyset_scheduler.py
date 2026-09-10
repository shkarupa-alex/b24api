"""One ordered scheduler for range, occupied-anchor, and automatic keysets."""

# ruff: noqa: D101, D102, D107, FBT003, PLC0415, PLR0915, TRY301

from __future__ import annotations
from collections import Counter, deque
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from b24api.batch.outcome import BatchSuccess
from b24api.contracts.keyset_execution import (
    AutoKeysetExecution,
    ClosureWitness,
    KeysetExecutionKind,
    KeysetPageCompletion,
    KeysetPhase,
    KeysetSelectionReason,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    StableIntegerKeysetContract,
    TotalHintMode,
)
from b24api.contracts.report import (
    KeysetExecutionReport,
    PageDispatch,
    PageOutcome,
    PageRejectionCode,
    Violation,
)
from b24api.errors import PaginationError
from b24api.traversal.keyset_auto import (
    AnchorFacts,
    BoundaryFacts,
    FinalSelection,
    Preselected,
    Preselection,
    SelectorInputs,
    TotalHintState,
    finalize,
    normalize_total_hint,
    preselect,
)
from b24api.traversal.keyset_fast_plan import (
    FastKeysetPlan,
    LaneBounds,
    LaneKind,
    LaneSpec,
    LaneState,
    LaneStatus,
    plan_lanes_from_anchors,
    plan_windows,
)
from b24api.traversal.keyset_range import range_window_width
from b24api.traversal.keyset_scheduler_support import KeysetSchedulerSupport
from b24api.traversal.ordered_admission import FastCounters, OrderedAdmissionState
from b24api.traversal.page_validation import (
    LaneCommandPlan,
    LaneReceipt,
    ReceiptRejection,
    validate_boundary_direction,
    validate_lane_receipt,
)

if TYPE_CHECKING:
    from b24api.batch.engine import BatchExecutor
    from b24api.contracts.json import JsonValue
    from b24api.contracts.request import IdentitySpec, Request, ResultSelector
    from b24api.contracts.response import ResultCollectionShape
    from b24api.contracts.traversal import KeysetSpec
    from b24api.execution import ExecutionContext, Executor
    from b24api.traversal.keyset_fast_stream import FastTraceRecorder


@dataclass(frozen=True, slots=True)
class PlanOutcome:
    plan: FastKeysetPlan
    preselection: Preselection | None
    final: FinalSelection | None


class KeysetFastScheduler(KeysetSchedulerSupport):
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
        self.selector = selector
        self.collection_shape = collection_shape
        self.page_size = page_size
        self.effective_page_cap = effective_page_cap
        self.execution = execution
        self.context = context
        self.engine = engine
        self.trace = trace
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
        self.batch_requests = 0
        self.batch_commands = 0
        self._planning_requests = Counter[KeysetPhase]()
        self._planning_physical_requests = 0
        self._closures = Counter[ClosureWitness]()
        self._continuations = 0
        self._observation_ordinal = 0
        self._command_ordinal = 0
        self._planned = False
        self._closed = False
        self._terminal = False
        self._plan_outcome: PlanOutcome | None = None
        self._preselection: Preselection | None = None
        self._final: FinalSelection | None = None
        self._total_hint = TotalHintState(False, None, False, False)
        self._head: LaneReceipt | None = None
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
        self._anchor_count = 0
        self._target_lanes: int | None = None
        self._window_width: int | None = None
        self._window_count: int | None = None
        self._pending: deque[tuple[JsonValue, ...]] = deque()
        self._offered_rows = 0
        self._buffer_balance = 0
        self._finish_cursor: int | None = None
        self._finishing = False
        self._head_rows = 0
        self._tail_rows = 0
        self._interior_span: int | None = None
        self._density_num: int | None = None
        self._density_den: int | None = None
        self._interior_estimate: int | None = None
        self._total_estimate: int | None = None
        self._selected_estimate: int | None = None

    @property
    def counters(self) -> FastCounters:
        """Return current row counters."""
        return self.admission.snapshot_counters()

    @property
    def terminal(self) -> bool:
        """Whether traversal exhaustion was confirmed."""
        return self._terminal

    def mark_emitted(self, count: int) -> None:
        """Record public delivery while scheduler retains offered-row accounting."""
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

    def _lane_plan(
        self,
        lane: LaneState,
        *,
        phase: KeysetPhase,
        request: Request,
        reserve: int | None = None,
        single: bool = False,
    ) -> LaneCommandPlan:
        return LaneCommandPlan(
            lane.spec.ordinal,
            self._next_command_id(phase),
            phase,
            request,
            reserve or self.effective_page_cap,
            single,
        )

    @property
    def _contract(self) -> StableIntegerKeysetContract:
        return self.execution.contract

    @property
    def _completion(self) -> KeysetPageCompletion:
        return self._contract.page_completion

    async def plan_barrier(self) -> PlanOutcome:  # noqa: C901, PLR0912
        """Cross boundary, optional canary/anchor, and final-selection barriers."""
        if self._planned:
            if self._plan_outcome is None:
                raise RuntimeError("planned scheduler has no outcome")
            return self._plan_outcome
        self._planning_bounds: dict[str, LaneBounds] = {}
        self._planning_descending: dict[str, bool] = {}
        advisory = (
            isinstance(self.execution, AutoKeysetExecution)
            and self.execution.total_hint is TotalHintMode.REQUEST_ADVISORY
        )
        boundary_plans = []
        for ordinal, direction in enumerate(("ASC", "DESC")):
            request = self._controls(
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
            boundary_plans.append(self._lane_plan(lane, phase=KeysetPhase.BOUNDARY, request=request))
        asc, desc = await self._wave(tuple(boundary_plans))
        contradiction = validate_boundary_direction(ascending=asc, descending=desc)
        if contradiction is not None:
            self.violations.append(contradiction.violation)
            raise PaginationError(contradiction.detail)
        self.admission.record_raw(len(asc.rows) + len(desc.rows))
        await self._adjust_buffer(len(asc.rows) + len(desc.rows))
        head, tail = (asc, desc) if self.keyset.direction == "ascending" else (desc, asc)
        self._head, self._tail = head, tail
        self._head_rows, self._tail_rows = len(head.rows), len(tail.rows)
        all_boundary = (*asc.identities, *desc.identities)
        maximum_hint = len(asc.rows) + len(desc.rows)
        if asc.identities and desc.identities:
            maximum_hint += max(0, min(desc.identities) - max(asc.identities) - 1)
        asc_response = self._response_for(boundary_plans[0], asc)
        desc_response = self._response_for(boundary_plans[1], desc)
        self._total_hint = normalize_total_hint(
            requested=advisory,
            head=asc_response.total if asc_response is not None else None,
            tail=desc_response.total if desc_response is not None else None,
            maximum=maximum_hint,
        )
        overlap = bool(asc.identities and desc.identities and max(asc.identities) >= min(desc.identities))
        adjacent = bool(asc.identities and desc.identities and min(desc.identities) - max(asc.identities) == 1)
        facts = BoundaryFacts(
            len(asc.rows),
            len(desc.rows),
            min(asc.identities) if asc.identities else None,
            max(asc.identities) if asc.identities else None,
            min(desc.identities) if desc.identities else None,
            max(desc.identities) if desc.identities else None,
            overlap,
            adjacent,
        )
        if asc.identities and desc.identities:
            self._interior_span = max(0, min(desc.identities) - max(asc.identities) - 1)
            self._density_num = len(asc.rows) + len(desc.rows)
            self._density_den = max(
                1,
                max(asc.identities) - min(asc.identities) + 1 + max(desc.identities) - min(desc.identities) + 1,
            )
        selected: KeysetExecutionKind
        if isinstance(self.execution, AutoKeysetExecution):
            inputs = SelectorInputs(
                facts,
                self.effective_page_cap,
                self.batch_capacity,
                self.execution.target_lanes,
                self.execution.max_range_waves,
                self.execution.range_window_width,
                self._completion,
                self._total_hint.observed if self._total_hint.plausible else None,
            )
            self._preselection = preselect(inputs)
            if self._total_hint.plausible:
                baseline = preselect(replace(inputs, advisory_total=None))
                self._total_hint = replace(
                    self._total_hint,
                    used=self._preselection.interior_rows_estimate > baseline.interior_rows_estimate,
                )
            self._interior_estimate = self._preselection.interior_rows_estimate
            self._total_estimate = self._preselection.total_rows_estimate
            bounded = self._preselection.plan in {Preselected.RANGE, Preselected.PROBE_ANCHORS}
            if (
                bounded
                and self._completion is KeysetPageCompletion.SHORT_PAGE_EXHAUSTS
                and (len(asc.rows) != self.effective_page_cap or len(desc.rows) != self.effective_page_cap)
            ):
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
                self._final = finalize(
                    inputs,
                    self._preselection,
                    AnchorFacts(tuple(anchors), self._anchor_probe_commands, self._empty_anchor_probes),
                )
                selected = self._final.kind
                if selected is not KeysetExecutionKind.PARTITIONED:
                    self.admission.record_discarded(self._anchor_count)
            self._reason = self._preselection.reason
            self._selected_estimate = (
                self._final.estimate.requests
                if self._final is not None
                else self._preselection.sequential_estimate.requests
                if selected is KeysetExecutionKind.SEQUENTIAL
                else self._preselection.range_estimate.requests
                if self._preselection.range_estimate is not None and selected is KeysetExecutionKind.RANGE
                else 0
            )
            self._target_lanes = self.execution.target_lanes
        elif not asc.rows or overlap or adjacent:
            selected = KeysetExecutionKind.BOUNDARY_ONLY
        else:
            if self._completion is KeysetPageCompletion.SHORT_PAGE_EXHAUSTS and (
                len(asc.rows) != self.effective_page_cap or len(desc.rows) != self.effective_page_cap
            ):
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
        plan = FastKeysetPlan(
            selected,
            tuple(lane.spec for lane in self._lanes),
            self.page_size,
            self.effective_page_cap,
            self.batch_capacity,
            self._completion,
            self.identity,
            self.keyset,
            self.collection_shape,
            self._finish_cursor,
        )
        self._plan_outcome = PlanOutcome(plan, self._preselection, self._final)
        self._planned = True
        del all_boundary
        return self._plan_outcome

    async def _configure_selected(self, asc: LaneReceipt, desc: LaneReceipt) -> None:
        head = asc if self.keyset.direction == "ascending" else desc
        self._admit_receipt(head)
        self._head_admitted = bool(head.rows)
        if self._selected is KeysetExecutionKind.SEQUENTIAL:
            discarded = len(desc.rows) if head is asc else len(asc.rows)
            self.admission.record_raw(0, discarded=True)
            self.admission._counters.probe_rows_discarded += discarded  # noqa: SLF001 - scheduler is counter owner
            await self._adjust_buffer(-discarded)
            self._finish_cursor = head.identities[-1] if head.identities else None
            self._finishing = bool(head.rows)
            if not head.rows:
                self._terminal = True
            return
        if self._selected is KeysetExecutionKind.BOUNDARY_ONLY:
            tail = desc if head is asc else asc
            self._admit_tail(tail)
            self._finish_cursor = tail.identities[0] if tail.identities else None
            self._finishing = bool(tail.rows)
            if not head.rows:
                self._terminal = True
            return
        lo = max(asc.identities)
        hi = min(desc.identities)
        if self._selected is KeysetExecutionKind.RANGE:
            explicit = (
                self.execution.window_width
                if isinstance(self.execution, RangeKeysetExecution)
                else self.execution.range_window_width
                if isinstance(self.execution, AutoKeysetExecution)
                else None
            )
            width = range_window_width(
                completion=self._completion,
                page_cap=self.effective_page_cap,
                span=max(0, hi - lo - 1),
                density_numerator=len(asc.rows) + len(desc.rows),
                density_denominator=max(
                    1,
                    max(asc.identities) - min(asc.identities) + 1 + max(desc.identities) - min(desc.identities) + 1,
                ),
                explicit=explicit,
            )
            specs = plan_windows(lo=lo, upper_exclusive=hi, width=width)
            self._window_width = width
            self._window_count = len(specs)
        else:
            anchors = tuple(sorted(self._anchor_rows))
            specs = plan_lanes_from_anchors(lo=lo, upper_exclusive=hi, anchors=anchors)
        if self.keyset.direction == "descending":
            reversed_specs = []
            for ordinal, spec in enumerate(reversed(specs)):
                retained = (
                    spec.bounds.lower_exclusive
                    if self._selected is KeysetExecutionKind.PARTITIONED and spec.bounds.lower_exclusive != lo
                    else None
                )
                reversed_specs.append(replace(spec, ordinal=ordinal, descending=True, retained_upper_anchor=retained))
            specs = tuple(reversed_specs)
        self._lanes = [
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
        self._lane_rows = {lane.spec.ordinal: [] for lane in self._lanes}
        self._lane_identities = {lane.spec.ordinal: [] for lane in self._lanes}
        self._lane_commands = {lane.spec.ordinal: [] for lane in self._lanes}
        if self._selected is KeysetExecutionKind.PARTITIONED and self._anchor_count:
            await self._adjust_buffer(self._anchor_count)

    def _admit_receipt(self, receipt: LaneReceipt) -> None:
        commit = self.admission.validate_and_commit(receipt)
        self.trace.admit(receipt.command_id, len(commit.rows))
        if commit.rows:
            self._pending.append(commit.rows)

    def _admit_tail(self, tail: LaneReceipt) -> None:
        rows_ids = list(zip(tail.rows, tail.identities, strict=True))
        if self.keyset.direction == "ascending":
            rows_ids.reverse()
        else:
            rows_ids.reverse()
        seen = self.admission.seen_identities
        filtered = [(row, identity) for row, identity in rows_ids if identity not in seen]
        overlap = len(rows_ids) - len(filtered)
        self.admission.record_boundary_overlap(overlap)
        if overlap:
            # The duplicate tail rows remain pinned only until normalization.
            pass
        normalized = replace(
            tail,
            rows=tuple(row for row, _ in filtered),
            identities=tuple(identity for _, identity in filtered),
            last_identity=filtered[-1][1] if filtered else None,
        )
        self._admit_receipt(normalized)

    async def next_rows(self) -> tuple[JsonValue, ...]:
        """Return the next ordered admitted chunk, or empty on confirmed exhaustion."""
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

    async def _body_wave(self) -> None:
        open_lanes = [lane for lane in self._lanes[self._lane_index :] if lane.status is LaneStatus.OPEN]
        if not open_lanes:
            self._drain_closed_lanes()
            return
        plans = []
        for lane in open_lanes[: max(1, self.batch_capacity)]:
            lower = lane.spec.bounds.lower_exclusive
            upper = lane.spec.bounds.upper_exclusive
            if lane.spec.descending:
                upper = lane.cursor
            else:
                lower = lane.cursor
            request = self._controls(
                direction="DESC" if lane.spec.descending else "ASC",
                lower=lower,
                upper=upper,
                limit=self.effective_page_cap,
            )
            plans.append(self._lane_plan(lane, phase=KeysetPhase.BODY, request=request))
        receipts = await self._wave(tuple(plans))
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
            if receipt.identities:
                lane.cursor = receipt.identities[-1]
            witness = receipt.witness
            if lane.spec.descending:
                witness = self._descending_witness(lane, receipt)
            if witness is None:
                self._continuations += 1
                continue
            lane.status = LaneStatus.CLOSED
            lane.witness = witness
            self._closures[witness] += 1
            anchor = lane.spec.retained_upper_anchor
            if anchor is not None:
                row = self._anchor_rows.get(anchor)
                if row is None:
                    raise PaginationError("partition lane lost its retained anchor")
                self._lane_rows[lane.spec.ordinal].append(row)
                self._lane_identities[lane.spec.ordinal].append(anchor)
                lane.witness = ClosureWitness.ANCHOR_FENCE
                self._closures[ClosureWitness.ANCHOR_FENCE] += 1
        self._drain_closed_lanes()

    def _descending_witness(self, lane: LaneState, receipt: LaneReceipt) -> ClosureWitness | None:
        if not receipt.identities:
            return ClosureWitness.EMPTY
        lower = lane.spec.bounds.lower_exclusive
        if lower is not None and receipt.identities[-1] == lower + 1:
            return ClosureWitness.TOP
        if (
            self._completion is KeysetPageCompletion.SHORT_PAGE_EXHAUSTS
            and len(receipt.identities) < self.effective_page_cap
        ):
            return ClosureWitness.SHORT_PAGE
        return None

    async def _finish_page(self) -> None:
        if self._finish_cursor is None:
            self._terminal = True
            return
        direction = "ASC" if self.keyset.direction == "ascending" else "DESC"
        request = self._controls(
            direction=direction,
            lower=self._finish_cursor if direction == "ASC" else None,
            upper=self._finish_cursor if direction == "DESC" else None,
            limit=self.effective_page_cap,
        )
        spec = LaneSpec(
            0,
            LaneKind.FINISH,
            LaneBounds(
                self._finish_cursor if direction == "ASC" else None,
                self._finish_cursor if direction == "DESC" else None,
            ),
            direction == "DESC",
            True,
            None,
        )
        self._finish_lane = LaneState(spec, self._finish_cursor, LaneStatus.OPEN, None, 0, 0, deque())
        plan = self._lane_plan(self._finish_lane, phase=KeysetPhase.FINISH, request=request)
        reservation = await self.context.reserve_page()
        await self._adjust_buffer(self.effective_page_cap)
        try:
            response = await self.executor.execute(request, context=self.context)
            self.context.commit_page(reservation)
            outcome = BatchSuccess(0, "finish", request, response.result, response=response)
            receipt = validate_lane_receipt(
                plan=plan,
                lane=self._finish_lane,
                outcome=outcome,
                identity=self.identity,
                collection_shape=self.collection_shape,
                effective_page_cap=self.effective_page_cap,
                completion=KeysetPageCompletion.EMPTY_CONFIRMATION,
                selector=self.selector,
            )
            if isinstance(receipt, ReceiptRejection):
                self.violations.append(receipt.violation)
                self._record(
                    plan,
                    index=None,
                    selected=0,
                    admitted=0,
                    outcome=PageOutcome.REJECTED,
                    rejection=PageRejectionCode.RANGE_CONTRADICTION,
                    violation=receipt.violation,
                    dispatch=PageDispatch.DIRECT,
                )
                raise PaginationError(receipt.detail)
            await self._adjust_buffer(-self.effective_page_cap + len(receipt.rows))
            self.admission.record_raw(len(receipt.rows))
            self._record(
                plan,
                index=None,
                selected=len(receipt.rows),
                admitted=len(receipt.rows),
                response=response,
                witness=receipt.witness,
                dispatch=PageDispatch.DIRECT,
            )
            if not receipt.rows:
                self._terminal = True
                self._finishing = False
                return
            self._finish_cursor = receipt.identities[-1]
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
        if self._buffer_balance:
            await self._adjust_buffer(-self._buffer_balance)
        if self._buffer_balance != 0:
            raise RuntimeError("fast scheduler buffer balance survived cleanup")
        self._pending.clear()
        self._lane_rows.clear()
        self._lane_identities.clear()
        self._lane_commands.clear()
        self.admission.assert_clean()

    def report_fragment(self) -> KeysetExecutionReport:
        """Build immutable aggregate execution evidence."""
        from b24api.traversal.keyset_reporting import build_scheduler_report

        return build_scheduler_report(self)


__all__ = ["KeysetFastScheduler", "PlanOutcome"]
