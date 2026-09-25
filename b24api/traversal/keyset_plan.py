"""Fast-keyset planning: boundary analysis, automatic selection, anchor probing and the frozen plan."""

from __future__ import annotations
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from b24api.contracts.keyset_execution import (
    AutoKeysetExecution,
    KeysetExecutionKind,
    KeysetPageCompletion,
    KeysetPhase,
    KeysetSelectionReason,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    TotalHintMode,
)
from b24api.errors import BudgetExceededError, PaginationError
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
    BoundaryAnalysis,
    analyze_boundary,
    anchor_capable_batch_capacity,
    compact_anchor_receipts,
    normalize_anchor_receipts,
)
from b24api.traversal.keyset_fast_plan import LaneBounds, LaneKind, LaneSpec, LaneState, LaneStatus, fit_wave
from b24api.traversal.keyset_geometry import partition_lane_specs, selected_range_geometry
from b24api.traversal.keyset_observation import (
    abort_staged_observations,
    finalize_boundary_observations,
    raise_boundary_cap_contradiction,
    reject_boundary_observations,
)
from b24api.traversal.keyset_page_validation import validate_boundary_direction
from b24api.traversal.keyset_transaction_contract import build_controlled_request, build_lane_plan
from b24api.traversal.keyset_transactions import build_anchor_plans

if TYPE_CHECKING:
    from b24api.contracts.traversal import KeysetSpec
    from b24api.traversal.keyset_page_validation import LaneCommandPlan, LaneReceipt
    from b24api.traversal.keyset_transaction_contract import KeysetTransactionHost


def initial_report_selection(
    execution: RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution,
) -> tuple[KeysetExecutionKind, KeysetSelectionReason]:
    """Represent incomplete planning inside the existing closed report enums."""
    reason = (
        KeysetSelectionReason.EXPLICIT_PARTITIONED
        if isinstance(execution, PartitionedKeysetExecution)
        else KeysetSelectionReason.EXPLICIT_RANGE
        if isinstance(execution, RangeKeysetExecution)
        else KeysetSelectionReason.INSUFFICIENT_PREDICTED_GAIN
    )
    selected = (
        KeysetExecutionKind.PARTITIONED
        if isinstance(execution, PartitionedKeysetExecution)
        else KeysetExecutionKind.RANGE
        if isinstance(execution, RangeKeysetExecution)
        else KeysetExecutionKind.AUTO
    )
    return selected, reason


@dataclass(frozen=True, slots=True)
class RangeBounds:
    """Numeric-window geometry of a selected range plan."""

    lo: int
    upper_exclusive: int
    width: int
    count: int


@dataclass(frozen=True, slots=True)
class FastKeysetPlan:
    """Selected fast-keyset plan: kind, reason and bounds, without HTTP or runtime counters."""

    kind: KeysetExecutionKind
    reason: KeysetSelectionReason
    range_bounds: RangeBounds | None
    partition_lanes: tuple[LaneSpec, ...]
    batch_capacity: int


@dataclass(slots=True)
class PlanningFacts:
    """Planner evidence the execution report carries, also when planning stops early."""

    selected: KeysetExecutionKind
    reason: KeysetSelectionReason
    preselection: Preselection | None = None
    final: FinalSelection | None = None
    total_hint: TotalHintState = field(default_factory=lambda: TotalHintState(False, None, False, False))  # noqa: FBT003
    head_rows: int = 0
    tail_rows: int = 0
    interior_span: int | None = None
    density_numerator: int | None = None
    density_denominator: int | None = None
    interior_estimate: int | None = None
    total_estimate: int | None = None
    selected_estimate: int | None = None
    target_lanes: int | None = None
    anchor_count: int = 0
    anchor_probe_commands: int = 0
    empty_anchor_probes: int = 0


@dataclass(frozen=True, slots=True)
class PlannedBoundaries:
    """The frozen plan together with the boundary pages the runtime admits."""

    plan: FastKeysetPlan
    ascending: LaneReceipt
    descending: LaneReceipt


class KeysetPlanner:
    """Own boundary analysis, automatic selection, eligibility of bounded plans and anchor probing."""

    def __init__(
        self,
        *,
        execution: RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution,
        keyset: KeysetSpec,
        completion: KeysetPageCompletion,
        effective_page_cap: int,
    ) -> None:
        """Start from the incomplete-planning report selection."""
        self.execution = execution
        self.keyset = keyset
        self.completion = completion
        self.effective_page_cap = effective_page_cap
        self.facts = PlanningFacts(*initial_report_selection(execution))

    async def plan(self, host: KeysetTransactionHost) -> PlannedBoundaries:
        """Run the planning waves through the host once and freeze the selected plan."""
        host.transactions.planning_bounds.clear()
        host.transactions.planning_descending.clear()
        advisory = (
            isinstance(self.execution, AutoKeysetExecution)
            and self.execution.total_hint is TotalHintMode.REQUEST_ADVISORY
        )
        asc, desc = await self._boundary_wave(host, advisory=advisory)
        analysis, available_rows = await self._accept_boundaries(host, asc, desc, advisory=advisory)
        facts = analysis.facts
        if isinstance(self.execution, AutoKeysetExecution):
            selected = await self._select_auto(host, asc, desc, analysis, available_rows)
        elif not asc.rows or facts.overlapping or facts.adjacent:
            selected = KeysetExecutionKind.BOUNDARY_ONLY
            self.facts.reason = (
                KeysetSelectionReason.EXPLICIT_PARTITIONED
                if isinstance(self.execution, PartitionedKeysetExecution)
                else KeysetSelectionReason.EXPLICIT_RANGE
            )
        else:
            selected = await self._select_explicit(host, asc, desc)
        finalize_boundary_observations(host.transactions.staged_observations, host.record_page)
        self.facts.selected = selected
        if not asc.rows or facts.overlapping or facts.adjacent:
            self.facts.density_numerator = self.facts.density_denominator = None
        plan = FastKeysetPlan(
            selected,
            self.facts.reason,
            self._range_bounds(selected, analysis, asc, desc),
            self._partition_lanes(selected, host, asc, desc),
            host.batch_capacity,
        )
        return PlannedBoundaries(plan, asc, desc)

    async def _boundary_wave(self, host: KeysetTransactionHost, *, advisory: bool) -> tuple[LaneReceipt, LaneReceipt]:
        boundary_plans = []
        for ordinal, direction in enumerate(("ASC", "DESC")):
            request = build_controlled_request(
                host,
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
                    None,
                ),
                None,
                LaneStatus.OPEN,
                None,
                0,
                self.effective_page_cap,
            )
            boundary_plans.append(build_lane_plan(host, lane, phase=KeysetPhase.BOUNDARY, request=request))
        asc, desc = await host.execute_wave(tuple(boundary_plans))
        contradiction = validate_boundary_direction(ascending=asc, descending=desc)
        if contradiction is not None:
            host.violations.append(contradiction.violation)
            reject_boundary_observations(
                host.transactions.staged_observations,
                host.record_page,
                host.admission,
                rows=len(asc.rows) + len(desc.rows),
                violation=contradiction.violation,
                raw=True,
            )
            raise PaginationError(contradiction.detail)
        return asc, desc

    async def _accept_boundaries(
        self,
        host: KeysetTransactionHost,
        asc: LaneReceipt,
        desc: LaneReceipt,
        *,
        advisory: bool,
    ) -> tuple[BoundaryAnalysis, int]:
        host.admission.record_raw(len(asc.rows) + len(desc.rows))
        await host.adjust_buffer(len(asc.rows) + len(desc.rows))
        available_rows = host.context.policy.max_buffered_rows - host.transactions.buffer_balance
        host.batch_capacity = min(host.batch_capacity, available_rows // self.effective_page_cap)
        head, tail = (asc, desc) if self.keyset.direction == "ascending" else (desc, asc)
        self.facts.head_rows, self.facts.tail_rows = len(head.rows), len(tail.rows)
        asc_total, desc_total = (host.transactions.boundary_totals.get(receipt.command_id) for receipt in (asc, desc))
        analysis = analyze_boundary(
            asc,
            desc,
            ascending_total=asc_total,
            descending_total=desc_total,
            advisory=advisory,
        )
        host.transactions.boundary_totals.clear()
        self.facts.total_hint = analysis.total_hint
        density = analysis.density
        if density is not None:
            self.facts.interior_span = density.span
            self.facts.density_numerator, self.facts.density_denominator = density.numerator, density.denominator
        return analysis, available_rows

    async def _select_auto(
        self,
        host: KeysetTransactionHost,
        asc: LaneReceipt,
        desc: LaneReceipt,
        analysis: BoundaryAnalysis,
        available_rows: int,
    ) -> KeysetExecutionKind:
        execution = self.execution
        if not isinstance(execution, AutoKeysetExecution):
            raise TypeError("automatic selection requires auto execution")
        facts = self.facts
        inputs = SelectorInputs(
            analysis.facts,
            self.effective_page_cap,
            host.batch_capacity,
            execution.target_lanes,
            execution.max_range_waves,
            execution.range_window_width,
            self.completion,
            facts.total_hint.observed if facts.total_hint.plausible else None,
            self.keyset.direction == "descending",
        )
        preselection = constrain_anchor_preselection(
            preselect(inputs),
            anchor_capacity=anchor_capable_batch_capacity(
                current=host.batch_capacity,
                available_rows=available_rows,
                page_cap=self.effective_page_cap,
                target_lanes=execution.target_lanes,
            ),
        )
        facts.preselection, facts.reason = preselection, preselection.reason
        if facts.total_hint.plausible:
            baseline = preselect(replace(inputs, advisory_total=None))
            used = preselection.interior_rows_estimate > baseline.interior_rows_estimate
            facts.total_hint = replace(facts.total_hint, used=used)
        facts.interior_estimate = preselection.interior_rows_estimate
        facts.total_estimate = preselection.total_rows_estimate
        if preselection.plan in {Preselected.RANGE, Preselected.PROBE_ANCHORS}:
            self._require_full_boundaries(host, asc, desc)
        if preselection.plan is Preselected.BOUNDARY_ONLY:
            selected = KeysetExecutionKind.BOUNDARY_ONLY
        elif preselection.plan is Preselected.SEQUENTIAL:
            selected = KeysetExecutionKind.SEQUENTIAL
        elif preselection.plan is Preselected.RANGE:
            selected = KeysetExecutionKind.RANGE
        else:
            anchors = await self._partition_planning(host, asc, desc, execution.target_lanes)
            facts.final = finalize(inputs, preselection, AnchorFacts(tuple(anchors), facts.empty_anchor_probes))
            selected = facts.final.kind
            if selected is not KeysetExecutionKind.PARTITIONED:
                host.admission.record_discarded(facts.anchor_count)
                await host.adjust_buffer(-facts.anchor_count)
                host.transactions.anchor_rows.clear()
                host.transactions.anchor_commands.clear()
        facts.selected_estimate = (
            facts.final.estimate.requests
            if facts.final is not None
            else preselection.sequential_estimate.requests
            if selected is KeysetExecutionKind.SEQUENTIAL
            else preselection.range_estimate.requests
            if preselection.range_estimate is not None and selected is KeysetExecutionKind.RANGE
            else int(bool(asc.rows))
        )
        facts.target_lanes = execution.target_lanes
        return selected

    async def _select_explicit(
        self,
        host: KeysetTransactionHost,
        asc: LaneReceipt,
        desc: LaneReceipt,
    ) -> KeysetExecutionKind:
        self._require_full_boundaries(host, asc, desc)
        if isinstance(self.execution, RangeKeysetExecution):
            self.facts.reason = KeysetSelectionReason.EXPLICIT_RANGE
            return KeysetExecutionKind.RANGE
        if isinstance(self.execution, PartitionedKeysetExecution):
            anchors = await self._partition_planning(host, asc, desc, self.execution.target_lanes)
            self.facts.reason = (
                KeysetSelectionReason.EXPLICIT_PARTITIONED if anchors else KeysetSelectionReason.DEGENERATE_SINGLE_LANE
            )
            self.facts.target_lanes = self.execution.target_lanes
            return KeysetExecutionKind.PARTITIONED
        raise TypeError("unknown fast keyset execution")

    def _require_full_boundaries(self, host: KeysetTransactionHost, asc: LaneReceipt, desc: LaneReceipt) -> None:
        """Bounded plans under short-page completion need both boundary pages at the page cap."""
        if self.completion is KeysetPageCompletion.SHORT_PAGE_EXHAUSTS and (
            len(asc.rows) != self.effective_page_cap or len(desc.rows) != self.effective_page_cap
        ):
            raise_boundary_cap_contradiction(
                host.transactions.staged_observations,
                host.record_page,
                host.admission,
                host.violations,
                len(asc.rows) + len(desc.rows),
            )

    async def _partition_planning(
        self,
        host: KeysetTransactionHost,
        asc: LaneReceipt,
        desc: LaneReceipt,
        target: int,
    ) -> tuple[int, ...]:
        precharged = 0
        try:
            anchors = build_anchor_plans(host, asc, desc, target)
            self.facts.anchor_probe_commands = len(anchors)
            anchor_receipts = await self._anchor_waves(host, anchors) if anchors else ()
            precharged = sum(bool(receipt.rows) for receipt in anchor_receipts)
        except BaseException:
            abort_staged_observations(
                host.transactions.staged_observations,
                host.record_page,
                host.admission,
                host.violations,
            )
            raise
        result = normalize_anchor_receipts(
            anchor_receipts, lo=max(asc.identities), upper_exclusive=min(desc.identities)
        )
        host.transactions.anchor_rows = result.rows
        host.transactions.anchor_commands = result.commands
        self.facts.empty_anchor_probes = result.empty_probes
        self.facts.anchor_count = len(result.anchors)
        host.admission.record_raw(result.raw_rows - precharged)
        host.admission.record_discarded(result.discarded_rows)
        await host.adjust_buffer(self.facts.anchor_count - precharged)
        return result.anchors

    async def _anchor_waves(
        self,
        host: KeysetTransactionHost,
        plans: tuple[LaneCommandPlan, ...],
    ) -> tuple[LaneReceipt, ...]:
        """Run anchor probes in capacity-fitted waves, retaining only each probe's first row."""
        receipts: list[LaneReceipt] = []
        remaining = plans
        while remaining:
            wave = fit_wave(
                remaining,
                reserves=tuple(plan.reserved_rows for plan in remaining),
                commands=host.batch_capacity,
                rows=host.context.policy.max_buffered_rows - host.transactions.buffer_balance,
            )
            if not wave:
                raise BudgetExceededError("fast keyset planning wave has no available row capacity")
            wave_receipts, discarded, retained = compact_anchor_receipts(await host.execute_wave(wave))
            host.admission.record_raw(discarded + retained)
            host.admission.record_discarded(discarded)
            await host.adjust_buffer(retained)
            receipts.extend(wave_receipts)
            remaining = remaining[len(wave) :]
        return tuple(receipts)

    def _range_bounds(
        self,
        selected: KeysetExecutionKind,
        analysis: BoundaryAnalysis,
        asc: LaneReceipt,
        desc: LaneReceipt,
    ) -> RangeBounds | None:
        if selected is not KeysetExecutionKind.RANGE:
            return None
        if not isinstance(self.execution, RangeKeysetExecution | AutoKeysetExecution):
            raise TypeError("range selection requires range or auto execution")
        if analysis.density is None:
            raise RuntimeError("range selection lacks boundary density")
        width, count = selected_range_geometry(
            execution=self.execution,
            completion=self.completion,
            page_cap=self.effective_page_cap,
            density=analysis.density,
        )
        return RangeBounds(max(asc.identities), min(desc.identities), width, count)

    def _partition_lanes(
        self,
        selected: KeysetExecutionKind,
        host: KeysetTransactionHost,
        asc: LaneReceipt,
        desc: LaneReceipt,
    ) -> tuple[LaneSpec, ...]:
        if selected is not KeysetExecutionKind.PARTITIONED:
            return ()
        return partition_lane_specs(
            lo=max(asc.identities),
            upper_exclusive=min(desc.identities),
            anchors=tuple(sorted(host.transactions.anchor_rows)),
            descending=self.keyset.direction == "descending",
        )


__all__ = [
    "FastKeysetPlan",
    "KeysetPlanner",
    "PlannedBoundaries",
    "PlanningFacts",
    "RangeBounds",
    "initial_report_selection",
]
