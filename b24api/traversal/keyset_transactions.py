"""Stateless I/O transactions used by the fast keyset host."""

from __future__ import annotations
from typing import TYPE_CHECKING, NoReturn

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
from b24api.traversal.keyset_geometry import descending_closure_witness
from b24api.traversal.keyset_observation import stage_or_record_observation
from b24api.traversal.keyset_page_validation import (
    LaneCommandPlan,
    ReceiptRejection,
    classify_rejection,
    validate_lane_receipt,
)
from b24api.traversal.keyset_transaction_contract import (
    KeysetTransactionHost,
    boundary_totals,
    build_controlled_request,
    build_lane_plan,
)

if TYPE_CHECKING:
    from b24api.batch.outcome import BatchOutcome
    from b24api.contracts.request import Request
    from b24api.execution.context import PageReservation
    from b24api.traversal.keyset_page_validation import LaneReceipt
    from b24api.traversal.plans import KeysetPlan


def build_anchor_plans(
    host: KeysetTransactionHost,
    asc: LaneReceipt,
    desc: LaneReceipt,
    target: int,
) -> tuple[LaneCommandPlan, ...]:
    """Build occupied-anchor capability requests."""
    commands = anchor_commands(
        lo=max(asc.identities),
        upper_exclusive=min(desc.identities),
        target_lanes=target,
        page_cap=host.effective_page_cap,
        writable_limit=host.keyset.limit_path is not None,
    )
    return build_capability_plans(
        commands,
        KeysetPhase.ANCHOR_PROBE,
        lambda **kwargs: build_controlled_request(host, **kwargs),
        lambda lane, **kwargs: build_lane_plan(host, lane, **kwargs),
        host.effective_page_cap,
        host.transactions.planning_bounds,
        host.transactions.planning_descending,
    )


async def execute_wave(
    host: KeysetTransactionHost,
    plans: tuple[LaneCommandPlan, ...],
) -> tuple[LaneReceipt, ...]:
    """Execute and validate one atomic correlated batch wave."""
    if not plans or len(plans) > host.engine.portal_command_cap:
        raise ValueError("fast keyset wave must contain 1..50 commands")
    reserved_rows = sum(plan.reserved_rows for plan in plans)
    charged_rows = 0
    if reserved_rows > host.context.policy.max_buffered_rows - host.transactions.buffer_balance:
        raise BudgetExceededError("fast keyset wave exceeds currently available row capacity")
    reservations = await host.context.reserve_pages(len(plans))
    try:
        await host.adjust_buffer(reserved_rows)
        charged_rows = reserved_rows
        for plan in plans:
            host.completion_recorder.schedule(plan.command_id)
        advisory = (
            all(plan.phase is KeysetPhase.BOUNDARY for plan in plans)
            and getattr(getattr(host.execution, "total_hint", None), "value", None) == "request_advisory"
        )
        outcomes = await host.engine.execute_requests(
            tuple(plan.request for plan in plans),
            context=host.context,
            advisory_totals=advisory,
            strict_envelope=True,
            strict_json_members=True,
        )
        _record_wave_dispatch(host, plans, outcomes)
        receipts, rejections, selected_rows = _validate_wave(host, plans, outcomes, reservations)
        if rejections:
            _abort_wave(host, plans, receipts, rejections, selected_rows)
        stage_semantics = any(plan.phase is KeysetPhase.BOUNDARY for plan in plans)
        for index, (plan, receipt, outcome) in enumerate(zip(plans, receipts, outcomes, strict=True)):
            stage_or_record_observation(
                host.transactions.staged_observations,
                host.record_page,
                stage=stage_semantics,
                plan=plan,
                index=index,
                rows=len(receipt.rows),
                response=outcome.response if isinstance(outcome, BatchSuccess) else None,
                witness=receipt.witness,
            )
        return tuple(receipts)
    finally:
        for reservation in reservations:
            host.context.release_page(reservation)
        await host.adjust_buffer(-min(charged_rows, host.transactions.buffer_balance))


def _record_wave_dispatch(
    host: KeysetTransactionHost,
    plans: tuple[LaneCommandPlan, ...],
    outcomes: tuple[BatchOutcome, ...],
) -> None:
    """Settle every command and count the physical request before any receipt is validated."""
    for plan, outcome in zip(plans, outcomes, strict=True):
        host.completion_recorder.settle(
            plan.command_id,
            CommandSettlement.SUCCESS if isinstance(outcome, BatchSuccess) else CommandSettlement.FAILURE,
        )
    host.transactions.boundary_totals = boundary_totals(plans, outcomes)
    host.batch_requests += 1
    host.batch_commands += len(plans)
    planning = {KeysetPhase.BOUNDARY, KeysetPhase.ANCHOR_PROBE}
    phases = {plan.phase for plan in plans}
    host.transactions.planning_physical_requests += int(bool(phases & planning))
    for phase in phases & planning:
        host.transactions.planning_requests[phase] += 1


def _validate_wave(
    host: KeysetTransactionHost,
    plans: tuple[LaneCommandPlan, ...],
    outcomes: tuple[BatchOutcome, ...],
    reservations: tuple[PageReservation, ...],
) -> tuple[list[LaneReceipt], list[ReceiptRejection], int]:
    """Charge each page, validate its receipt, and record every rejected command's page."""
    receipts: list[LaneReceipt] = []
    rejections: list[ReceiptRejection] = []
    selected_rows = 0
    for index, (plan, outcome, reservation) in enumerate(zip(plans, outcomes, reservations, strict=True)):
        commit = host.context.commit_page if isinstance(outcome, BatchSuccess) else host.context.release_page
        commit(reservation)
        lane = lane_for_command(
            plan,
            planning_bounds=host.transactions.planning_bounds,
            planning_descending=host.transactions.planning_descending,
            finish_lane=host.transactions.finish_lane,
            lanes=host.transactions.lanes,
        )
        receipt = validate_lane_receipt(
            plan=plan,
            lane=lane,
            outcome=outcome,
            identity=host.identity,
            collection_shape=host.collection_shape,
            effective_page_cap=plan.reserved_rows if plan.expects_single_row else host.effective_page_cap,
            completion=host.completion,
            selector=host.selector,
            page_adapter=host.page_adapter,
        )
        if not isinstance(receipt, ReceiptRejection):
            selected_rows += len(receipt.rows)
            host.completion_recorder.validated(plan.command_id)
            receipts.append(receipt)
            continue
        selected_rows += receipt.selected_rows
        rejections.append(receipt)
        host.violations.append(receipt.violation)
        page_outcome, rejection_code = classify_rejection(outcome)
        if isinstance(receipt.error, PageAdaptationError):
            rejection_code = PageRejectionCode.PAGE_ADAPTATION
        host.record_page(
            plan,
            index=index,
            selected=receipt.selected_rows,
            admitted=0,
            outcome=page_outcome,
            rejection=rejection_code,
            violation=receipt.violation,
        )
    return receipts, rejections, selected_rows


def _abort_wave(
    host: KeysetTransactionHost,
    plans: tuple[LaneCommandPlan, ...],
    receipts: list[LaneReceipt],
    rejections: list[ReceiptRejection],
    selected_rows: int,
) -> NoReturn:
    """Reject the valid pages of a failed wave too, discard its rows, and raise the first cause."""
    successful = {receipt.command_id: receipt for receipt in receipts}
    for index, plan in enumerate(plans):
        if plan.command_id in successful:
            host.record_page(
                plan,
                index=index,
                selected=len(successful[plan.command_id].rows),
                admitted=0,
                outcome=PageOutcome.REJECTED,
                rejection=PageRejectionCode.TRANSACTION_ABORTED,
            )
    host.admission.record_raw(selected_rows, discarded=True)
    rejection = next((receipt for receipt in rejections if receipt.error is not None), None)
    if rejection is None or rejection.error is None:
        raise PaginationError("fast keyset wave validation failed")
    cause = rejection.error
    if isinstance(cause, B24ApiError) and rejection.violation.code == "command_failure":
        raise IncompleteTraversalError(
            report=None,
            error=cause,
            replay_disposition=rejection.replay_disposition,
        ) from cause
    raise cause


async def execute_body_wave(host: KeysetTransactionHost) -> None:
    """Advance one bounded group without letting a later lane outrun the frontier."""
    open_lanes = [
        lane for lane in host.transactions.lanes[host.transactions.lane_index :] if lane.status is LaneStatus.OPEN
    ]
    if not open_lanes:
        host.drain_admission_frontier()
        return
    frontier = open_lanes[0]
    candidates_lanes = [
        lane
        for lane in open_lanes
        if lane is frontier or not host.transactions.lane_rows[lane.spec.ordinal] or lane.rounds <= frontier.rounds
    ][: host.batch_capacity]
    plan_candidates = tuple(_body_plan(host, lane) for lane in candidates_lanes)
    plans = fit_wave(
        plan_candidates,
        reserves=tuple(plan.reserved_rows for plan in plan_candidates),
        commands=host.batch_capacity,
        rows=host.context.policy.max_buffered_rows - host.transactions.buffer_balance,
    )
    if not plans:
        raise BudgetExceededError("fast keyset body wave has no available row capacity")
    receipts = await host.execute_wave(plans)
    retained = sum(len(receipt.rows) for receipt in receipts)
    await host.adjust_buffer(retained)
    host.admission.record_raw(retained)
    by_ordinal = {receipt.lane_ordinal: receipt for receipt in receipts}
    for lane in candidates_lanes[: len(plans)]:
        receipt = by_ordinal[lane.spec.ordinal]
        lane.rounds += 1
        host.transactions.lane_rows[lane.spec.ordinal].extend(receipt.rows)
        host.transactions.lane_identities[lane.spec.ordinal].extend(receipt.identities)
        host.transactions.lane_commands[lane.spec.ordinal].append((receipt.command_id, len(receipt.rows)))
        witness = receipt.witness
        if lane.spec.descending:
            witness = descending_closure_witness(
                lane,
                receipt.identities,
                completion=host.completion,
                page_cap=host.effective_page_cap,
            )
        if receipt.identities:
            lane.cursor = receipt.identities[-1]
        if witness is None:
            host.transactions.continuations += 1
        else:
            _close_lane(host, lane, witness)
    host.drain_admission_frontier()


def _body_plan(host: KeysetTransactionHost, lane: LaneState) -> LaneCommandPlan:
    """Build the next body page of one lane, fenced by its cursor on the side it advances."""
    lower = lane.spec.bounds.lower_exclusive
    upper = lane.spec.bounds.upper_exclusive
    if lane.spec.descending:
        upper = lane.cursor
    else:
        lower = lane.cursor
    request = build_controlled_request(
        host,
        direction="DESC" if lane.spec.descending else "ASC",
        lower=lower,
        upper=upper,
        limit=host.effective_page_cap,
    )
    return build_lane_plan(host, lane, phase=KeysetPhase.BODY, request=request)


def _close_lane(host: KeysetTransactionHost, lane: LaneState, witness: ClosureWitness) -> None:
    """Close a lane on its witness; a partition lane first takes back its retained upper anchor row."""
    lane.status = LaneStatus.CLOSED
    lane.witness = witness
    anchor = lane.spec.retained_upper_anchor
    if anchor is None:
        host.transactions.closures[witness] += 1
        return
    row = host.transactions.anchor_rows.pop(anchor, None)
    if row is None:
        raise PaginationError("partition lane lost its retained anchor")
    anchor_command = host.transactions.anchor_commands.pop(anchor, None)
    if anchor_command is None:
        raise PaginationError("partition lane lost its retained anchor command")
    host.transactions.lane_rows[lane.spec.ordinal].append(row)
    host.transactions.lane_identities[lane.spec.ordinal].append(anchor)
    host.transactions.lane_commands[lane.spec.ordinal].append((anchor_command, 1))
    lane.witness = ClosureWitness.ANCHOR_FENCE
    host.transactions.closures[ClosureWitness.ANCHOR_FENCE] += 1


async def execute_finish_page(
    host: KeysetTransactionHost,
    finish_plan: KeysetPlan,
    request: Request,
) -> None:
    """Execute one direct sequential finish page and record its final admission outcome."""
    if host.transactions.finish_cursor is None:
        host.transactions.terminal = True
        return
    finish_lane = host.transactions.finish_lane = _finish_lane(host.transactions.finish_cursor, finish_plan)
    plan = build_lane_plan(host, finish_lane, phase=KeysetPhase.FINISH, request=request)
    reservation = await host.context.reserve_page()
    try:
        await host.adjust_buffer(host.effective_page_cap)
        host.completion_recorder.schedule(plan.command_id)
        response = await host.executor.execute(request, context=host.context)
        host.completion_recorder.settle(plan.command_id, CommandSettlement.SUCCESS)
        host.context.commit_page(reservation)
        outcome = BatchSuccess(0, "finish", request, response.result, response=response)
        receipt = validate_lane_receipt(
            plan=plan,
            lane=finish_lane,
            outcome=outcome,
            identity=host.identity,
            collection_shape=host.collection_shape,
            effective_page_cap=host.effective_page_cap,
            completion=KeysetPageCompletion.EMPTY_CONFIRMATION,
            selector=host.selector,
            page_adapter=host.page_adapter,
        )
        if isinstance(receipt, ReceiptRejection):
            _reject_finish_receipt(host, plan, receipt)
        host.completion_recorder.validated(plan.command_id)
        terminal = keyset_step.keyset_page_terminal(finish_plan, len(receipt.rows))
        await host.adjust_buffer(-host.effective_page_cap + len(receipt.rows))
        host.admission.record_raw(len(receipt.rows))
        try:
            commit = host.admission.validate_and_commit(receipt)
        except PaginationError:
            host.admission.record_discarded(len(receipt.rows))
            host.record_page(
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
        host.record_page(
            plan,
            index=None,
            selected=len(receipt.rows),
            admitted=len(commit.rows),
            response=response,
            witness=receipt.witness,
            dispatch=PageDispatch.DIRECT,
        )
        host.trace.admit(receipt.command_id, len(commit.rows))
        host.completion_recorder.admit(plan.command_id, len(commit.rows))
        if commit.rows:
            host.transactions.pending.append(commit.rows)
            host.add_pending_owner(((plan.command_id, len(commit.rows)),))
        if terminal is not None:
            if receipt.witness is None:
                raise RuntimeError("terminal fast finish page lacked a closure witness")  # noqa: TRY301 - invariant
            host.record_completion_witness()
            host.transactions.terminal = True
            host.transactions.finishing = False
            return
        host.transactions.finish_cursor = keyset_step.next_keyset_cursor(
            host.transactions.finish_cursor,
            receipt.identities,
        )
    except BaseException:
        host.context.release_page(reservation)
        raise
    if host.transactions.buffer_balance > host.context.policy.max_buffered_rows:
        raise RuntimeError("fast host buffer accounting escaped policy")


def _finish_lane(cursor: int, finish_plan: KeysetPlan) -> LaneState:
    """Open the single sequential finish lane that continues from the fast phase's cursor."""
    descending = finish_plan.direction != "asc"
    bounds = LaneBounds(None if descending else cursor, cursor if descending else None)
    return LaneState(LaneSpec(0, LaneKind.FINISH, bounds, descending, None), cursor, LaneStatus.OPEN, None, 0, 0)


def _reject_finish_receipt(host: KeysetTransactionHost, plan: LaneCommandPlan, receipt: ReceiptRejection) -> NoReturn:
    """Record a rejected finish page, discard its rows, and raise its cause."""
    host.violations.append(receipt.violation)
    host.admission.record_raw(receipt.selected_rows, discarded=True)
    host.record_page(
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


__all__ = [
    "build_anchor_plans",
    "execute_body_wave",
    "execute_finish_page",
    "execute_wave",
]
