"""Stateless I/O transactions used by the single fast keyset scheduler."""

# ruff: noqa: C901, FBT003, PLR0915, SLF001, TRY301

from __future__ import annotations
from collections import deque
from typing import TYPE_CHECKING

from b24api.batch.outcome import BatchSuccess
from b24api.contracts.keyset_execution import ClosureWitness, KeysetPageCompletion, KeysetPhase
from b24api.contracts.report import PageDispatch, PageOutcome, PageRejectionCode
from b24api.errors import BudgetExceededError, PaginationError
from b24api.traversal import keyset_step
from b24api.traversal.keyset_capability import (
    anchor_commands,
    boundary_totals,
    canary_commands,
    lane_for_command,
)
from b24api.traversal.keyset_fast_plan import (
    LaneBounds,
    LaneKind,
    LaneSpec,
    LaneState,
    LaneStatus,
    build_capability_plans,
    fit_wave,
)
from b24api.traversal.keyset_observation import stage_or_record_observation
from b24api.traversal.keyset_range import descending_closure_witness
from b24api.traversal.page_validation import ReceiptRejection, classify_rejection, validate_lane_receipt

if TYPE_CHECKING:
    from b24api.contracts.request import Request
    from b24api.traversal.keyset_scheduler import KeysetFastScheduler
    from b24api.traversal.page_validation import LaneCommandPlan, LaneReceipt
    from b24api.traversal.plans import KeysetPlan


def build_canary_plans(
    scheduler: KeysetFastScheduler,
    asc: LaneReceipt,
    desc: LaneReceipt,
) -> tuple[tuple[LaneCommandPlan, ...], dict[str, tuple[int, ...]]]:
    """Build canary requests and their expected identity sequences."""
    commands = canary_commands(asc.identities, desc.identities, scheduler.effective_page_cap)
    plans = build_capability_plans(
        commands,
        KeysetPhase.CANARY,
        scheduler._controls,
        scheduler._lane_plan,
        scheduler.effective_page_cap,
        scheduler._planning_bounds,
        scheduler._planning_descending,
    )
    expected = {
        plan.command_id: command.expected
        for plan, command in zip(plans, commands, strict=True)
        if command.expected is not None
    }
    return plans, expected


def build_anchor_plans(
    scheduler: KeysetFastScheduler,
    asc: LaneReceipt,
    desc: LaneReceipt,
    target: int,
) -> tuple[LaneCommandPlan, ...]:
    """Build occupied-anchor capability requests."""
    commands = anchor_commands(
        lo=max(asc.identities),
        upper_exclusive=min(desc.identities),
        target_lanes=target,
        page_cap=scheduler.effective_page_cap,
        writable_limit=scheduler.keyset.limit_path is not None,
    )
    return build_capability_plans(
        commands,
        KeysetPhase.ANCHOR_PROBE,
        scheduler._controls,
        scheduler._lane_plan,
        scheduler.effective_page_cap,
        scheduler._planning_bounds,
        scheduler._planning_descending,
    )


async def execute_wave(
    scheduler: KeysetFastScheduler,
    plans: tuple[LaneCommandPlan, ...],
) -> tuple[LaneReceipt, ...]:
    """Execute and validate one atomic correlated batch wave."""
    if not plans or len(plans) > scheduler.engine.portal_command_cap:
        raise ValueError("fast keyset wave must contain 1..50 commands")
    reserved_rows = sum(plan.reserved_rows for plan in plans)
    charged_rows = 0
    if reserved_rows > scheduler.context.policy.max_buffered_rows - scheduler._buffer_balance:
        raise BudgetExceededError("fast keyset wave exceeds currently available row capacity")
    reservations = await scheduler.context.reserve_pages(len(plans))
    try:
        await scheduler._adjust_buffer(reserved_rows)
        charged_rows = reserved_rows
        advisory = (
            all(plan.phase is KeysetPhase.BOUNDARY for plan in plans)
            and getattr(getattr(scheduler.execution, "total_hint", None), "value", None) == "request_advisory"
        )
        outcomes = await scheduler.engine.execute_requests(
            tuple(plan.request for plan in plans),
            context=scheduler.context,
            advisory_totals=advisory,
            strict_envelope=True,
            strict_json_members=True,
        )
        scheduler._boundary_totals = boundary_totals(plans, outcomes)
        scheduler.batch_requests += 1
        scheduler.batch_commands += len(plans)
        planning = {KeysetPhase.BOUNDARY, KeysetPhase.CANARY, KeysetPhase.ANCHOR_PROBE}
        phases = {plan.phase for plan in plans}
        scheduler._planning_physical_requests += int(bool(phases & planning))
        for phase in phases & planning:
            scheduler._planning_requests[phase] += 1
        receipts: list[LaneReceipt] = []
        failed, selected_rows = False, 0
        for index, (plan, outcome, reservation) in enumerate(zip(plans, outcomes, reservations, strict=True)):
            commit = (
                scheduler.context.commit_page if isinstance(outcome, BatchSuccess) else scheduler.context.release_page
            )
            commit(reservation)
            lane = lane_for_command(
                plan,
                planning_bounds=scheduler._planning_bounds,
                planning_descending=scheduler._planning_descending,
                finish_lane=getattr(scheduler, "_finish_lane", None),
                lanes=scheduler._lanes,
            )
            receipt = validate_lane_receipt(
                plan=plan,
                lane=lane,
                outcome=outcome,
                identity=scheduler.identity,
                collection_shape=scheduler.collection_shape,
                effective_page_cap=plan.reserved_rows if plan.expects_single_row else scheduler.effective_page_cap,
                completion=scheduler.completion,
                selector=scheduler.selector,
            )
            selected_rows += receipt.selected_rows if isinstance(receipt, ReceiptRejection) else len(receipt.rows)
            if isinstance(receipt, ReceiptRejection):
                failed = True
                scheduler.violations.append(receipt.violation)
                page_outcome, rejection_code = classify_rejection(outcome)
                scheduler._record(
                    plan,
                    index=index,
                    selected=receipt.selected_rows,
                    admitted=0,
                    outcome=page_outcome,
                    rejection=rejection_code,
                    violation=receipt.violation,
                )
            else:
                receipts.append(receipt)
        if failed:
            successful = {receipt.command_id: receipt for receipt in receipts}
            for index, plan in enumerate(plans):
                if plan.command_id in successful:
                    scheduler._record(
                        plan,
                        index=index,
                        selected=len(successful[plan.command_id].rows),
                        admitted=0,
                        outcome=PageOutcome.REJECTED,
                        rejection=PageRejectionCode.TRANSACTION_ABORTED,
                    )
            scheduler.admission.record_raw(selected_rows, discarded=True)
            raise PaginationError("fast keyset wave validation failed")
        stage_semantics = bool(phases & {KeysetPhase.BOUNDARY, KeysetPhase.CANARY})
        for index, (plan, receipt) in enumerate(zip(plans, receipts, strict=True)):
            outcome = outcomes[index]
            response = outcome.response if isinstance(outcome, BatchSuccess) else None
            stage_or_record_observation(
                scheduler._staged_observations,
                scheduler._record,
                stage=stage_semantics,
                plan=plan,
                index=index,
                rows=len(receipt.rows),
                response=response,
                witness=receipt.witness,
            )
        return tuple(receipts)
    finally:
        for reservation in reservations:
            scheduler.context.release_page(reservation)
        await scheduler._adjust_buffer(-min(charged_rows, scheduler._buffer_balance))


async def execute_body_wave(scheduler: KeysetFastScheduler) -> None:
    """Advance one bounded group without letting a later lane outrun the frontier."""
    open_lanes = [lane for lane in scheduler._lanes[scheduler._lane_index :] if lane.status is LaneStatus.OPEN]
    if not open_lanes:
        scheduler._drain_admission_frontier()
        return
    frontier = open_lanes[0]
    candidates_lanes = [
        lane
        for lane in open_lanes
        if lane is frontier or not scheduler._lane_rows[lane.spec.ordinal] or lane.rounds <= frontier.rounds
    ][: scheduler.batch_capacity]
    candidates = []
    for lane in candidates_lanes:
        lower = lane.spec.bounds.lower_exclusive
        upper = lane.spec.bounds.upper_exclusive
        if lane.spec.descending:
            upper = lane.cursor
        else:
            lower = lane.cursor
        request = scheduler._controls(
            direction="DESC" if lane.spec.descending else "ASC",
            lower=lower,
            upper=upper,
            limit=scheduler.effective_page_cap,
        )
        candidates.append(scheduler._lane_plan(lane, phase=KeysetPhase.BODY, request=request))
    plan_candidates = tuple(candidates)
    plans = fit_wave(
        plan_candidates,
        reserves=tuple(plan.reserved_rows for plan in plan_candidates),
        commands=scheduler.batch_capacity,
        rows=scheduler.context.policy.max_buffered_rows - scheduler._buffer_balance,
    )
    if not plans:
        raise BudgetExceededError("fast keyset body wave has no available row capacity")
    receipts = await scheduler._wave(plans)
    retained = sum(len(receipt.rows) for receipt in receipts)
    await scheduler._adjust_buffer(retained)
    scheduler.admission.record_raw(retained)
    by_ordinal = {receipt.lane_ordinal: receipt for receipt in receipts}
    for lane in candidates_lanes[: len(plans)]:
        receipt = by_ordinal[lane.spec.ordinal]
        lane.rounds += 1
        scheduler._lane_rows[lane.spec.ordinal].extend(receipt.rows)
        scheduler._lane_identities[lane.spec.ordinal].extend(receipt.identities)
        scheduler._lane_commands[lane.spec.ordinal].append((receipt.command_id, len(receipt.rows)))
        witness = receipt.witness
        if lane.spec.descending:
            witness = descending_closure_witness(
                lane,
                receipt.identities,
                completion=scheduler.completion,
                page_cap=scheduler.effective_page_cap,
            )
        if receipt.identities:
            lane.cursor = receipt.identities[-1]
        if witness is None:
            scheduler._continuations += 1
            continue
        lane.status = LaneStatus.CLOSED
        lane.witness = witness
        anchor = lane.spec.retained_upper_anchor
        if anchor is not None:
            row = scheduler._anchor_rows.pop(anchor, None)
            if row is None:
                raise PaginationError("partition lane lost its retained anchor")
            scheduler._anchor_commands.pop(anchor, None)
            scheduler._lane_rows[lane.spec.ordinal].append(row)
            scheduler._lane_identities[lane.spec.ordinal].append(anchor)
            lane.witness = ClosureWitness.ANCHOR_FENCE
            scheduler._closures[ClosureWitness.ANCHOR_FENCE] += 1
        else:
            scheduler._closures[witness] += 1
    scheduler._drain_admission_frontier()


async def execute_finish_page(
    scheduler: KeysetFastScheduler,
    finish_plan: KeysetPlan,
    request: Request,
) -> None:
    """Execute one direct sequential finish page and record its final admission outcome."""
    if scheduler._finish_cursor is None:
        scheduler._terminal = True
        return
    direction = "ASC" if finish_plan.direction == "asc" else "DESC"
    bounds = LaneBounds(
        scheduler._finish_cursor if direction == "ASC" else None,
        scheduler._finish_cursor if direction == "DESC" else None,
    )
    spec = LaneSpec(0, LaneKind.FINISH, bounds, direction == "DESC", True, None)
    finish_lane = LaneState(
        spec,
        scheduler._finish_cursor,
        LaneStatus.OPEN,
        None,
        0,
        0,
        deque(),
    )
    scheduler._finish_lane = finish_lane
    plan = scheduler._lane_plan(finish_lane, phase=KeysetPhase.FINISH, request=request)
    reservation = await scheduler.context.reserve_page()
    try:
        await scheduler._adjust_buffer(scheduler.effective_page_cap)
        response = await scheduler.executor.execute(request, context=scheduler.context)
        scheduler.context.commit_page(reservation)
        outcome = BatchSuccess(0, "finish", request, response.result, response=response)
        receipt = validate_lane_receipt(
            plan=plan,
            lane=finish_lane,
            outcome=outcome,
            identity=scheduler.identity,
            collection_shape=scheduler.collection_shape,
            effective_page_cap=scheduler.effective_page_cap,
            completion=KeysetPageCompletion.EMPTY_CONFIRMATION,
            selector=scheduler.selector,
        )
        if isinstance(receipt, ReceiptRejection):
            scheduler.violations.append(receipt.violation)
            scheduler._record(
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
        terminal = keyset_step.keyset_page_terminal(finish_plan, len(receipt.rows))
        await scheduler._adjust_buffer(-scheduler.effective_page_cap + len(receipt.rows))
        scheduler.admission.record_raw(len(receipt.rows))
        commit = scheduler.admission.validate_and_commit(receipt)
        scheduler._record(
            plan,
            index=None,
            selected=len(receipt.rows),
            admitted=len(commit.rows),
            response=response,
            witness=receipt.witness,
            dispatch=PageDispatch.DIRECT,
        )
        scheduler.trace.admit(receipt.command_id, len(commit.rows))
        if commit.rows:
            scheduler._pending.append(commit.rows)
        if terminal is not None:
            scheduler._terminal = True
            scheduler._finishing = False
            return
        scheduler._finish_cursor = keyset_step.next_keyset_cursor(
            scheduler._finish_cursor,
            receipt.identities,
        )
    except BaseException:
        scheduler.context.release_page(reservation)
        raise
    if scheduler._buffer_balance > scheduler.context.policy.max_buffered_rows:
        raise RuntimeError("fast scheduler buffer accounting escaped policy")


async def close_scheduler(scheduler: KeysetFastScheduler) -> None:
    """Release all scheduler-owned retained state exactly once."""
    if scheduler._closed:
        return
    scheduler._frozen_report = scheduler.report_fragment()
    if scheduler._buffer_balance:
        await scheduler._adjust_buffer(-scheduler._buffer_balance)
    if scheduler._buffer_balance != 0:
        raise RuntimeError("fast scheduler buffer balance survived cleanup")
    for retained in (
        scheduler._pending,
        scheduler._lane_rows,
        scheduler._lane_identities,
        scheduler._lane_commands,
        scheduler._lanes,
    ):
        retained.clear()
    scheduler._tail = None
    scheduler._anchor_rows.clear()
    scheduler._anchor_commands.clear()
    scheduler._planning_bounds.clear()
    scheduler._planning_descending.clear()
    scheduler._boundary_totals.clear()
    scheduler._staged_observations.clear()
    scheduler._plan_outcome = None
    scheduler._range_geometry = None
    scheduler.admission.assert_clean()
    scheduler.admission.close()
    scheduler._closed = True


__all__ = [
    "build_anchor_plans",
    "build_canary_plans",
    "close_scheduler",
    "execute_body_wave",
    "execute_finish_page",
    "execute_wave",
]
