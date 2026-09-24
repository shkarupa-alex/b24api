"""Stateless I/O transactions used by the fast keyset scheduler."""

# ruff: noqa: C901, PLR0915, TRY301

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.batch.outcome import BatchSuccess
from b24api.contracts.completion import CommandSettlement
from b24api.contracts.keyset_execution import ClosureWitness, KeysetPageCompletion, KeysetPhase
from b24api.contracts.report import PageDispatch, PageOutcome, PageRejectionCode
from b24api.errors import (
    B24ApiError,
    BudgetExceededError,
    IncompleteTraversalError,
    PageAdaptationError,
    PaginationError,
)
from b24api.traversal import keyset_step
from b24api.traversal.keyset_capability import (
    anchor_commands,
    build_capability_plans,
    lane_for_command,
)
from b24api.traversal.keyset_fast_plan import (
    LaneBounds,
    LaneKind,
    LaneSpec,
    LaneState,
    LaneStatus,
    fit_wave,
)
from b24api.traversal.keyset_observation import stage_or_record_observation
from b24api.traversal.keyset_range import descending_closure_witness
from b24api.traversal.keyset_transaction_contract import boundary_totals, build_controlled_request, build_lane_plan
from b24api.traversal.page_validation import (
    LaneCommandPlan,
    ReceiptRejection,
    classify_rejection,
    validate_lane_receipt,
)

if TYPE_CHECKING:
    from b24api.contracts.request import Request
    from b24api.traversal.keyset_scheduler import KeysetFastScheduler
    from b24api.traversal.page_validation import LaneReceipt
    from b24api.traversal.plans import KeysetPlan


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
        lambda **kwargs: build_controlled_request(scheduler, **kwargs),
        lambda lane, **kwargs: build_lane_plan(scheduler, lane, **kwargs),
        scheduler.effective_page_cap,
        scheduler.transactions.planning_bounds,
        scheduler.transactions.planning_descending,
    )


async def execute_wave(  # noqa: PLR0912 - one atomic correlated validation transaction
    scheduler: KeysetFastScheduler,
    plans: tuple[LaneCommandPlan, ...],
) -> tuple[LaneReceipt, ...]:
    """Execute and validate one atomic correlated batch wave."""
    if not plans or len(plans) > scheduler.engine.portal_command_cap:
        raise ValueError("fast keyset wave must contain 1..50 commands")
    reserved_rows = sum(plan.reserved_rows for plan in plans)
    charged_rows = 0
    if reserved_rows > scheduler.context.policy.max_buffered_rows - scheduler.transactions.buffer_balance:
        raise BudgetExceededError("fast keyset wave exceeds currently available row capacity")
    reservations = await scheduler.context.reserve_pages(len(plans))
    try:
        await scheduler.adjust_buffer(reserved_rows)
        charged_rows = reserved_rows
        for plan in plans:
            scheduler.completion_recorder.schedule(plan.command_id)
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
        for plan, outcome in zip(plans, outcomes, strict=True):
            scheduler.completion_recorder.settle(
                plan.command_id,
                CommandSettlement.SUCCESS if isinstance(outcome, BatchSuccess) else CommandSettlement.FAILURE,
            )
        scheduler.transactions.boundary_totals = boundary_totals(plans, outcomes)
        scheduler.batch_requests += 1
        scheduler.batch_commands += len(plans)
        planning = {KeysetPhase.BOUNDARY, KeysetPhase.CANARY, KeysetPhase.ANCHOR_PROBE}
        phases = {plan.phase for plan in plans}
        scheduler.transactions.planning_physical_requests += int(bool(phases & planning))
        for phase in phases & planning:
            scheduler.transactions.planning_requests[phase] += 1
        receipts: list[LaneReceipt] = []
        rejections: list[ReceiptRejection] = []
        failed, selected_rows = False, 0
        for index, (plan, outcome, reservation) in enumerate(zip(plans, outcomes, reservations, strict=True)):
            commit = (
                scheduler.context.commit_page if isinstance(outcome, BatchSuccess) else scheduler.context.release_page
            )
            commit(reservation)
            lane = lane_for_command(
                plan,
                planning_bounds=scheduler.transactions.planning_bounds,
                planning_descending=scheduler.transactions.planning_descending,
                finish_lane=scheduler.transactions.finish_lane,
                lanes=scheduler.transactions.lanes,
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
                page_adapter=scheduler.page_adapter,
            )
            selected_rows += receipt.selected_rows if isinstance(receipt, ReceiptRejection) else len(receipt.rows)
            if isinstance(receipt, ReceiptRejection):
                rejections.append(receipt)
                failed = True
                scheduler.violations.append(receipt.violation)
                page_outcome, rejection_code = classify_rejection(outcome)
                if isinstance(receipt.error, PageAdaptationError):
                    rejection_code = PageRejectionCode.PAGE_ADAPTATION
                scheduler.record_page(
                    plan,
                    index=index,
                    selected=receipt.selected_rows,
                    admitted=0,
                    outcome=page_outcome,
                    rejection=rejection_code,
                    violation=receipt.violation,
                )
            else:
                scheduler.completion_recorder.validated(plan.command_id, receipt.identities)
                receipts.append(receipt)
        if failed:
            successful = {receipt.command_id: receipt for receipt in receipts}
            for index, plan in enumerate(plans):
                if plan.command_id in successful:
                    scheduler.record_page(
                        plan,
                        index=index,
                        selected=len(successful[plan.command_id].rows),
                        admitted=0,
                        outcome=PageOutcome.REJECTED,
                        rejection=PageRejectionCode.TRANSACTION_ABORTED,
                    )
            scheduler.admission.record_raw(selected_rows, discarded=True)
            cause = next(
                (receipt.error for receipt in rejections if receipt.error is not None),
                None,
            )
            if cause is not None:
                rejection = next(receipt for receipt in rejections if receipt.error is cause)
                if isinstance(cause, B24ApiError) and rejection.violation.code == "command_failure":
                    raise IncompleteTraversalError(
                        report=None,
                        error=cause,
                        replay_disposition=rejection.replay_disposition,
                    ) from cause
                raise cause
            raise PaginationError("fast keyset wave validation failed")
        stage_semantics = bool(phases & {KeysetPhase.BOUNDARY, KeysetPhase.CANARY})
        for index, (plan, receipt) in enumerate(zip(plans, receipts, strict=True)):
            outcome = outcomes[index]
            response = outcome.response if isinstance(outcome, BatchSuccess) else None
            stage_or_record_observation(
                scheduler.transactions.staged_observations,
                scheduler.record_page,
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
        await scheduler.adjust_buffer(-min(charged_rows, scheduler.transactions.buffer_balance))


async def execute_body_wave(scheduler: KeysetFastScheduler) -> None:  # noqa: PLR0912
    """Advance one bounded group without letting a later lane outrun the frontier."""
    open_lanes = [
        lane
        for lane in scheduler.transactions.lanes[scheduler.transactions.lane_index :]
        if lane.status is LaneStatus.OPEN
    ]
    if not open_lanes:
        scheduler.drain_admission_frontier()
        return
    frontier = open_lanes[0]
    candidates_lanes = [
        lane
        for lane in open_lanes
        if lane is frontier or not scheduler.transactions.lane_rows[lane.spec.ordinal] or lane.rounds <= frontier.rounds
    ][: scheduler.batch_capacity]
    candidates = []
    for lane in candidates_lanes:
        lower = lane.spec.bounds.lower_exclusive
        upper = lane.spec.bounds.upper_exclusive
        if lane.spec.descending:
            upper = lane.cursor
        else:
            lower = lane.cursor
        request = build_controlled_request(
            scheduler,
            direction="DESC" if lane.spec.descending else "ASC",
            lower=lower,
            upper=upper,
            limit=scheduler.effective_page_cap,
        )
        candidates.append(build_lane_plan(scheduler, lane, phase=KeysetPhase.BODY, request=request))
    plan_candidates = tuple(candidates)
    plans = fit_wave(
        plan_candidates,
        reserves=tuple(plan.reserved_rows for plan in plan_candidates),
        commands=scheduler.batch_capacity,
        rows=scheduler.context.policy.max_buffered_rows - scheduler.transactions.buffer_balance,
    )
    if not plans:
        raise BudgetExceededError("fast keyset body wave has no available row capacity")
    receipts = await scheduler.execute_wave(plans)
    retained = sum(len(receipt.rows) for receipt in receipts)
    await scheduler.adjust_buffer(retained)
    scheduler.admission.record_raw(retained)
    by_ordinal = {receipt.lane_ordinal: receipt for receipt in receipts}
    for lane in candidates_lanes[: len(plans)]:
        receipt = by_ordinal[lane.spec.ordinal]
        lane.rounds += 1
        scheduler.transactions.lane_rows[lane.spec.ordinal].extend(receipt.rows)
        scheduler.transactions.lane_identities[lane.spec.ordinal].extend(receipt.identities)
        scheduler.transactions.lane_commands[lane.spec.ordinal].append((receipt.command_id, len(receipt.rows)))
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
            scheduler.transactions.continuations += 1
            continue
        lane.status = LaneStatus.CLOSED
        lane.witness = witness
        anchor = lane.spec.retained_upper_anchor
        if anchor is not None:
            row = scheduler.transactions.anchor_rows.pop(anchor, None)
            if row is None:
                raise PaginationError("partition lane lost its retained anchor")
            anchor_command = scheduler.transactions.anchor_commands.pop(anchor, None)
            if anchor_command is None:
                raise PaginationError("partition lane lost its retained anchor command")
            scheduler.transactions.lane_rows[lane.spec.ordinal].append(row)
            scheduler.transactions.lane_identities[lane.spec.ordinal].append(anchor)
            scheduler.transactions.lane_commands[lane.spec.ordinal].append((anchor_command, 1))
            lane.witness = ClosureWitness.ANCHOR_FENCE
            scheduler.transactions.closures[ClosureWitness.ANCHOR_FENCE] += 1
        else:
            scheduler.transactions.closures[witness] += 1
    scheduler.drain_admission_frontier()


async def execute_finish_page(
    scheduler: KeysetFastScheduler,
    finish_plan: KeysetPlan,
    request: Request,
) -> None:
    """Execute one direct sequential finish page and record its final admission outcome."""
    if scheduler.transactions.finish_cursor is None:
        scheduler.transactions.terminal = True
        return
    direction = "ASC" if finish_plan.direction == "asc" else "DESC"
    bounds = LaneBounds(
        scheduler.transactions.finish_cursor if direction == "ASC" else None,
        scheduler.transactions.finish_cursor if direction == "DESC" else None,
    )
    spec = LaneSpec(0, LaneKind.FINISH, bounds, direction == "DESC", None)
    finish_lane = LaneState(
        spec,
        scheduler.transactions.finish_cursor,
        LaneStatus.OPEN,
        None,
        0,
        0,
    )
    scheduler.transactions.finish_lane = finish_lane
    plan = build_lane_plan(scheduler, finish_lane, phase=KeysetPhase.FINISH, request=request)
    reservation = await scheduler.context.reserve_page()
    try:
        await scheduler.adjust_buffer(scheduler.effective_page_cap)
        scheduler.completion_recorder.schedule(plan.command_id)
        response = await scheduler.executor.execute(request, context=scheduler.context)
        scheduler.completion_recorder.settle(plan.command_id, CommandSettlement.SUCCESS)
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
            page_adapter=scheduler.page_adapter,
        )
        if isinstance(receipt, ReceiptRejection):
            scheduler.violations.append(receipt.violation)
            scheduler.admission.record_raw(receipt.selected_rows, discarded=True)
            scheduler.record_page(
                plan,
                index=None,
                selected=receipt.selected_rows,
                admitted=0,
                outcome=PageOutcome.REJECTED,
                rejection=(
                    PageRejectionCode.PAGE_ADAPTATION
                    if isinstance(receipt.error, PageAdaptationError)
                    else PageRejectionCode.RANGE_CONTRADICTION
                ),
                violation=receipt.violation,
                dispatch=PageDispatch.DIRECT,
            )
            if receipt.error is not None:
                raise receipt.error
            raise PaginationError(receipt.detail)
        scheduler.completion_recorder.validated(plan.command_id, receipt.identities)
        terminal = keyset_step.keyset_page_terminal(finish_plan, len(receipt.rows))
        await scheduler.adjust_buffer(-scheduler.effective_page_cap + len(receipt.rows))
        scheduler.admission.record_raw(len(receipt.rows))
        try:
            commit = scheduler.admission.validate_and_commit(receipt)
        except PaginationError:
            scheduler.admission.record_discarded(len(receipt.rows))
            scheduler.record_page(
                plan,
                index=None,
                selected=len(receipt.rows),
                admitted=0,
                outcome=PageOutcome.REJECTED,
                rejection=PageRejectionCode.RANGE_CONTRADICTION,
                response=response,
                witness=receipt.witness,
                dispatch=PageDispatch.DIRECT,
            )
            raise
        scheduler.record_page(
            plan,
            index=None,
            selected=len(receipt.rows),
            admitted=len(commit.rows),
            response=response,
            witness=receipt.witness,
            dispatch=PageDispatch.DIRECT,
        )
        scheduler.trace.admit(receipt.command_id, len(commit.rows))
        scheduler.completion_recorder.admit(plan.command_id, len(commit.rows))
        if commit.rows:
            scheduler.transactions.pending.append(commit.rows)
            scheduler._pending_owners.append(((plan.command_id, len(commit.rows)),))  # noqa: SLF001
        if terminal is not None:
            if receipt.witness is None:
                raise RuntimeError("terminal fast finish page lacked a closure witness")
            scheduler._completion_witnesses += 1  # noqa: SLF001 - transaction closes its owning scheduler
            scheduler.transactions.terminal = True
            scheduler.transactions.finishing = False
            return
        scheduler.transactions.finish_cursor = keyset_step.next_keyset_cursor(
            scheduler.transactions.finish_cursor,
            receipt.identities,
        )
    except BaseException:
        scheduler.context.release_page(reservation)
        raise
    if scheduler.transactions.buffer_balance > scheduler.context.policy.max_buffered_rows:
        raise RuntimeError("fast scheduler buffer accounting escaped policy")


__all__ = [
    "build_anchor_plans",
    "execute_body_wave",
    "execute_finish_page",
    "execute_wave",
]
