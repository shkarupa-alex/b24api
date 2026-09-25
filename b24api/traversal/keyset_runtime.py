"""One ordered runtime for range, occupied-anchor, and automatic keysets."""

from __future__ import annotations
from collections import deque
from typing import TYPE_CHECKING

from b24api.completion.fast_recorder import FastCompletionRecorder
from b24api.contracts.completion import BindingClosure
from b24api.contracts.dispatch import PORTAL_BATCH_CAP
from b24api.contracts.keyset_execution import (
    AutoKeysetExecution,
    ClosureWitness,
    KeysetExecutionKind,
    KeysetPhase,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
)
from b24api.contracts.report import KeysetExecutionReport, PageDispatch, PageOutcome, PageRejectionCode, Violation
from b24api.traversal import keyset_step
from b24api.traversal.keyset_fast_plan import LaneState, LaneStatus, LazyRangePlan
from b24api.traversal.keyset_observation import page_observation
from b24api.traversal.keyset_ordered_admission import FastCounters, OrderedAdmissionState, drain_complete_lanes
from b24api.traversal.keyset_page_validation import LaneCommandPlan, LaneReceipt, normalize_tail_receipt
from b24api.traversal.keyset_plan import FastKeysetPlan, KeysetPlanner
from b24api.traversal.keyset_reporting import build_keyset_report
from b24api.traversal.keyset_transaction_contract import KeysetTransactionState
from b24api.traversal.keyset_transactions import execute_body_wave, execute_finish_page, execute_wave

if TYPE_CHECKING:
    from b24api.batch.engine import BatchExecutor
    from b24api.contracts.json import FrozenJson
    from b24api.contracts.page import PageAdapter
    from b24api.contracts.request import IdentitySpec, Request, ResultSelector
    from b24api.contracts.response import Response, ResultCollectionShape
    from b24api.contracts.traversal import KeysetSpec
    from b24api.execution import ExecutionContext, Executor
    from b24api.traversal.keyset_observation import FastTraceRecorder


class KeysetRuntime:
    """Own the transaction host, ordered admission, body and finish waves, observation and reporting."""

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
        page_adapter: PageAdapter,
    ) -> None:
        """Initialize isolated planning, admission, and reporting state."""
        self.executor = executor
        self.request = request
        self.identity = identity
        self.keyset = keyset
        self.selector, self.collection_shape = selector, collection_shape
        self.page_size, self.effective_page_cap = page_size, effective_page_cap
        self.execution, self.context, self.engine, self.trace = execution, context, engine, trace
        self.page_adapter = page_adapter
        self.completion_recorder = FastCompletionRecorder()
        self.completion = execution.contract.page_completion
        requested_batch = getattr(execution, "batch_size", None) or PORTAL_BATCH_CAP
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
        self.planner = KeysetPlanner(
            execution=execution,
            keyset=keyset,
            completion=self.completion,
            effective_page_cap=effective_page_cap,
        )
        self.plan: FastKeysetPlan | None = None
        self._observation_ordinal = self._command_ordinal = 0
        self._planned, self._closed = False, False
        self._tail: LaneReceipt | None = None
        self._head_admitted = False
        self._window_width: int | None = None
        self._window_count: int | None = None
        self._range_geometry: LazyRangePlan | None = None
        self._offered_rows = 0
        self._pending_owners: deque[tuple[tuple[str, int], ...]] = deque()
        self._offered_owners: deque[tuple[str, int]] = deque()
        self._completion_witnesses = 0
        self._frozen_report: KeysetExecutionReport | None = None

    @property
    def counters(self) -> FastCounters:
        """Return a detached snapshot of row-provenance counters."""
        return self.admission.snapshot_counters()

    @property
    def closed(self) -> bool:
        """Return whether the runtime has released its lanes and stopped planning."""
        return self._closed

    @property
    def terminal(self) -> bool:
        """Return whether traversal has reached a terminal state."""
        return self.transactions.terminal

    def completion_closure(self) -> tuple[BindingClosure | None, int]:
        """Return runtime-qualified evidence for one naturally completed plan."""
        if not self.transactions.terminal:
            return None, 0
        if self.planner.facts.head_rows == 0:
            return BindingClosure.SOURCE_EMPTY, 0
        witnesses = sum(self.transactions.closures.values()) + self._completion_witnesses
        if witnesses < 1:
            return None, 0
        return BindingClosure.KEYSET_PLAN_COVERED, witnesses

    def mark_emitted(self, count: int) -> None:
        """Record rows delivered to the caller."""
        self.admission.mark_emitted(count)
        for _ in range(count):
            if not self._offered_owners:
                raise RuntimeError("fast emitted row has no page owner")
            command_id, remaining = self._offered_owners.popleft()
            self.completion_recorder.emitted(command_id)
            if remaining > 1:
                self._offered_owners.appendleft((command_id, remaining - 1))

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
        """Record one transaction page observation under the runtime's sole observation ordinal."""
        self.trace.record(
            page_observation(
                self._observation_ordinal,
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
            ),
        )
        self._observation_ordinal += 1
        self.completion_recorder.recorded(plan.command_id, outcome)
        if outcome is PageOutcome.COMMITTED and selected == 0:
            self.completion_recorder.admit(plan.command_id, 0)

    async def execute_wave(self, plans: tuple[LaneCommandPlan, ...]) -> tuple[LaneReceipt, ...]:
        """Execute one correlated transaction wave."""
        return await execute_wave(self, plans)

    def add_pending_owner(self, owners: tuple[tuple[str, int], ...]) -> None:
        """Queue the command owners of the admitted row group appended last."""
        self._pending_owners.append(owners)

    def record_completion_witness(self) -> None:
        """Count one terminal closure witness that no lane recorded."""
        self._completion_witnesses += 1

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

    async def plan_barrier(self) -> FastKeysetPlan:
        """Plan once through the planner, then configure lanes and admit the boundary pages."""
        if self._planned:
            if self.plan is None:
                raise RuntimeError("planned runtime has no plan")
            return self.plan
        planned = await self.planner.plan(self)
        head, tail = (
            (planned.ascending, planned.descending)
            if self.keyset.direction == "ascending"
            else (planned.descending, planned.ascending)
        )
        self._tail = tail
        await self._configure(planned.plan, head, tail)
        self.plan = planned.plan
        self._planned = True
        return planned.plan

    async def _configure(self, plan: FastKeysetPlan, head: LaneReceipt, tail: LaneReceipt) -> None:
        self._admit_receipt(head)
        self._head_admitted = bool(head.rows)
        if plan.kind is KeysetExecutionKind.SEQUENTIAL:
            self.admission.record_probe_discarded(len(tail.rows))
            await self.adjust_buffer(-len(tail.rows))
            self._tail = None
            self.transactions.finish_cursor = head.identities[-1] if head.identities else None
            self.transactions.finishing = bool(head.rows)
            if not head.rows:
                self.transactions.terminal = True
            return
        if plan.kind is KeysetExecutionKind.BOUNDARY_ONLY:
            await self._admit_tail(tail)
            self._tail = None
            self.transactions.finish_cursor = tail.identities[0] if tail.identities else None
            self.transactions.finishing = bool(tail.rows)
            if not head.rows:
                self.transactions.terminal = True
            return
        bounds = plan.range_bounds
        if bounds is not None:
            self._window_width, self._window_count = bounds.width, bounds.count
            self._range_geometry = LazyRangePlan(
                bounds.lo,
                bounds.upper_exclusive,
                bounds.width,
                bounds.count,
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
        self.transactions.lanes = [
            LaneState(
                spec,
                spec.bounds.upper_exclusive if spec.descending else spec.bounds.lower_exclusive,
                LaneStatus.OPEN,
                None,
                0,
                0,
            )
            for spec in plan.partition_lanes
        ]
        self.transactions.lane_rows = {lane.spec.ordinal: [] for lane in self.transactions.lanes}
        self.transactions.lane_identities = {lane.spec.ordinal: [] for lane in self.transactions.lanes}
        self.transactions.lane_commands = {lane.spec.ordinal: [] for lane in self.transactions.lanes}

    def _admit_receipt(self, receipt: LaneReceipt) -> None:
        commit = self.admission.validate_and_commit(receipt)
        self.trace.admit(receipt.command_id, len(commit.rows))
        if receipt.command_id.startswith("body-admit-"):
            owners = tuple(self.transactions.lane_commands[receipt.lane_ordinal])
        else:
            owners = ((receipt.command_id, len(commit.rows)),)
        if sum(count for _, count in owners) != len(commit.rows):
            raise RuntimeError("fast admission lost page provenance")
        for command_id, count in owners:
            self.completion_recorder.admit(command_id, count)
        if commit.rows:
            self.transactions.pending.append(commit.rows)
            self._pending_owners.append(tuple((command_id, count) for command_id, count in owners if count))

    async def _admit_tail(self, tail: LaneReceipt) -> None:
        normalized, overlap = normalize_tail_receipt(tail, already_seen=self.admission.has_seen)
        self.admission.record_boundary_overlap(overlap)
        await self.adjust_buffer(-overlap)
        self._admit_receipt(normalized)

    async def next_rows(self) -> tuple[FrozenJson, ...]:
        """Return the next ordered row group, or an empty tuple at completion."""
        if self._closed or self.transactions.terminal:
            return ()
        if self._offered_rows:
            await self.adjust_buffer(-self._offered_rows)
            self._offered_rows = 0
        plan = await self.plan_barrier()
        while not self.transactions.pending and not self.transactions.terminal:
            if self.transactions.lanes and self.transactions.lane_index < len(self.transactions.lanes):
                await self.body_wave()
                continue
            if plan.kind in {KeysetExecutionKind.RANGE, KeysetExecutionKind.PARTITIONED} and not (
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
        return self._offer_pending()

    def _offer_pending(self) -> tuple[FrozenJson, ...]:
        """Transfer one admitted group and its command owners to the iterator."""
        rows = self.transactions.pending.popleft()
        if self._offered_owners:
            raise RuntimeError("fast row group was replaced before delivery")
        self._offered_owners = deque(self._pending_owners.popleft())
        self._offered_rows = len(rows)
        return rows

    async def body_wave(self) -> None:
        """Advance one bounded body wave of the selected lanes."""
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
        """Release all state retained by the runtime exactly once."""
        if self._closed:
            return
        self.admission.discard_unadmitted_raw()
        self._frozen_report = self.report_fragment()
        if self.transactions.buffer_balance:
            await self.adjust_buffer(-self.transactions.buffer_balance)
        if self.transactions.buffer_balance != 0:
            raise RuntimeError("fast runtime buffer balance survived cleanup")
        transactions = self.transactions
        for retained in (
            transactions.pending,
            self._pending_owners,
            self._offered_owners,
            transactions.lane_rows,
            transactions.lane_identities,
            transactions.lane_commands,
            transactions.lanes,
            transactions.anchor_rows,
            transactions.anchor_commands,
            transactions.planning_bounds,
            transactions.planning_descending,
            transactions.boundary_totals,
            transactions.staged_observations,
        ):
            retained.clear()
        self._tail = None
        self.plan = None
        self._range_geometry = None
        self.admission.assert_clean()
        self.admission.close()
        self._closed = True

    def report_fragment(self) -> KeysetExecutionReport:
        """Return the current or frozen redacted execution report."""
        if self._frozen_report is not None:
            return self._frozen_report
        return build_keyset_report(
            execution=self.execution,
            facts=self.planner.facts,
            transactions=self.transactions,
            counters=self.counters,
            trace=self.trace,
            batch_capacity=self.batch_capacity,
            head_admitted=self._head_admitted,
            window_width=self._window_width,
            window_count=self._window_count,
        )


__all__ = ["KeysetRuntime"]
