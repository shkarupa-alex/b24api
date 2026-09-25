"""Explicit state, host protocol and construction boundary for fast-keyset transactions."""

from __future__ import annotations
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from b24api.batch.outcome import BatchSuccess
from b24api.contracts.keyset_execution import KeysetPhase
from b24api.traversal import keyset_step
from b24api.traversal.keyset_page_validation import LaneCommandPlan

if TYPE_CHECKING:
    from b24api.batch.engine import BatchExecutor
    from b24api.batch.outcome import BatchOutcome
    from b24api.completion.fast_recorder import FastCompletionRecorder
    from b24api.contracts.json import FrozenJson
    from b24api.contracts.keyset_execution import (
        AutoKeysetExecution,
        ClosureWitness,
        KeysetPageCompletion,
        PartitionedKeysetExecution,
        RangeKeysetExecution,
    )
    from b24api.contracts.page import PageAdapter
    from b24api.contracts.report import PageDispatch, PageOutcome, PageRejectionCode, Violation
    from b24api.contracts.request import IdentitySpec, Request, ResultSelector
    from b24api.contracts.response import Response, ResultCollectionShape
    from b24api.contracts.traversal import KeysetSpec
    from b24api.execution import ExecutionContext, Executor
    from b24api.traversal.keyset_fast_plan import LaneBounds, LaneState
    from b24api.traversal.keyset_observation import FastTraceRecorder
    from b24api.traversal.keyset_ordered_admission import OrderedAdmissionState
    from b24api.traversal.keyset_page_validation import LaneReceipt


@dataclass(slots=True)
class KeysetTransactionState:
    """Mutable state intentionally shared with stateless transaction helpers."""

    planning_requests: Counter[KeysetPhase] = field(default_factory=Counter)
    planning_physical_requests: int = 0
    continuations: int = 0
    boundary_totals: dict[str, int | None] = field(default_factory=dict)
    closures: Counter[ClosureWitness] = field(default_factory=Counter)
    terminal: bool = False
    finishing: bool = False
    lanes: list[LaneState] = field(default_factory=list)
    lane_rows: dict[int, list[FrozenJson]] = field(default_factory=dict)
    lane_identities: dict[int, list[int]] = field(default_factory=dict)
    lane_commands: dict[int, list[tuple[str, int]]] = field(default_factory=dict)
    lane_index: int = 0
    anchor_rows: dict[int, FrozenJson] = field(default_factory=dict)
    anchor_commands: dict[int, str] = field(default_factory=dict)
    pending: deque[tuple[FrozenJson, ...]] = field(default_factory=deque)
    buffer_balance: int = 0
    finish_cursor: int | None = None
    finish_lane: LaneState | None = None
    planning_bounds: dict[str, LaneBounds] = field(default_factory=dict)
    planning_descending: dict[str, bool] = field(default_factory=dict)
    staged_observations: list[tuple[LaneCommandPlan, int, int, Response | None, ClosureWitness | None]] = field(
        default_factory=list,
    )


class KeysetTransactionHost(Protocol):
    """Everything the planner and the stateless transactions need from the runtime that owns them."""

    executor: Executor
    request: Request
    identity: IdentitySpec
    keyset: KeysetSpec
    selector: ResultSelector
    collection_shape: ResultCollectionShape
    page_size: int
    effective_page_cap: int
    execution: RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution
    context: ExecutionContext
    engine: BatchExecutor
    trace: FastTraceRecorder
    page_adapter: PageAdapter
    completion_recorder: FastCompletionRecorder
    completion: KeysetPageCompletion
    batch_capacity: int
    admission: OrderedAdmissionState
    violations: list[Violation]
    batch_requests: int
    batch_commands: int
    transactions: KeysetTransactionState

    def next_command_id(self, phase: KeysetPhase) -> str:
        """Allocate a unique command identifier for a transaction phase."""
        ...

    async def adjust_buffer(self, delta: int) -> None:
        """Apply a row-buffer accounting delta for a transaction."""
        ...

    def record_page(  # noqa: PLR0913 - mirrors the one page observation record
        self,
        plan: LaneCommandPlan,
        *,
        index: int | None,
        selected: int,
        admitted: int,
        outcome: PageOutcome = ...,
        rejection: PageRejectionCode | None = None,
        violation: Violation | None = None,
        response: Response | None = None,
        witness: ClosureWitness | None = None,
        dispatch: PageDispatch = ...,
    ) -> None:
        """Record one transaction page observation."""
        ...

    async def execute_wave(self, plans: tuple[LaneCommandPlan, ...]) -> tuple[LaneReceipt, ...]:
        """Execute one correlated transaction wave."""
        ...

    def drain_admission_frontier(self) -> None:
        """Admit all complete lanes at the ordered frontier."""
        ...

    def add_pending_owner(self, owners: tuple[tuple[str, int], ...]) -> None:
        """Queue the command owners of the admitted row group appended last."""
        ...

    def record_completion_witness(self) -> None:
        """Count one terminal closure witness that no lane recorded."""
        ...


def boundary_totals(
    plans: tuple[LaneCommandPlan, ...],
    outcomes: tuple[BatchOutcome, ...],
) -> dict[str, int | None]:
    """Retain boundary scalar totals without retaining response row payloads."""
    if not all(plan.phase is KeysetPhase.BOUNDARY for plan in plans):
        return {}
    return {
        plan.command_id: outcome.response.total
        for plan, outcome in zip(plans, outcomes, strict=True)
        if isinstance(outcome, BatchSuccess) and outcome.response is not None
    }


def build_lane_plan(  # noqa: PLR0913
    host: KeysetTransactionHost,
    lane: LaneState,
    *,
    phase: KeysetPhase,
    request: Request,
    reserve: int | None = None,
    single: bool = False,
) -> LaneCommandPlan:
    """Build one correlated lane-command reservation."""
    return LaneCommandPlan(
        lane.spec.ordinal,
        host.next_command_id(phase),
        phase,
        request,
        reserve or host.effective_page_cap,
        single,
    )


def build_controlled_request(  # noqa: PLR0913
    host: KeysetTransactionHost,
    *,
    direction: str,
    lower: int | None = None,
    upper: int | None = None,
    limit: int | None = None,
    advisory_start: bool = False,
) -> Request:
    """Build one keyset request from transaction bounds."""
    return keyset_step.bounded_keyset_request(
        host.request,
        keyset=host.keyset,
        identity=host.identity,
        direction=direction,
        lower=lower,
        upper=upper,
        limit=limit,
        advisory_start=advisory_start,
    )


__all__ = [
    "KeysetTransactionHost",
    "KeysetTransactionState",
    "boundary_totals",
    "build_controlled_request",
    "build_lane_plan",
]
