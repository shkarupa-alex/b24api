"""Explicit state and construction boundary for fast-keyset transactions."""

from __future__ import annotations
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from b24api.batch.outcome import BatchSuccess
from b24api.contracts.keyset_execution import KeysetPhase
from b24api.traversal import keyset_step
from b24api.traversal.keyset_page_validation import LaneCommandPlan

if TYPE_CHECKING:
    from b24api.batch.outcome import BatchOutcome
    from b24api.contracts.json import FrozenJson
    from b24api.contracts.keyset_execution import ClosureWitness
    from b24api.contracts.request import Request
    from b24api.contracts.response import Response
    from b24api.traversal.keyset_fast_plan import LaneBounds, LaneState
    from b24api.traversal.keyset_scheduler import KeysetFastScheduler


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
    scheduler: KeysetFastScheduler,
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
        scheduler.next_command_id(phase),
        phase,
        request,
        reserve or scheduler.effective_page_cap,
        single,
    )


def build_controlled_request(  # noqa: PLR0913
    scheduler: KeysetFastScheduler,
    *,
    direction: str,
    lower: int | None = None,
    upper: int | None = None,
    limit: int | None = None,
    advisory_start: bool = False,
) -> Request:
    """Build one keyset request from transaction bounds."""
    return keyset_step.bounded_keyset_request(
        scheduler.request,
        keyset=scheduler.keyset,
        identity=scheduler.identity,
        direction=direction,
        lower=lower,
        upper=upper,
        limit=limit,
        advisory_start=advisory_start,
    )


__all__ = ["KeysetTransactionState", "boundary_totals", "build_controlled_request", "build_lane_plan"]
