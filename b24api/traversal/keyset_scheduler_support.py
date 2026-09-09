"""Request construction and wave transactions used by the sole keyset scheduler."""

# ruff: noqa: C901, FBT003, PERF401, PLC0415, PLR0912, PLR0913, PLR2004

from __future__ import annotations
from collections import deque
from typing import TYPE_CHECKING, Any, cast

from b24api.batch.outcome import BatchFailure, BatchSuccess
from b24api.contracts.keyset_execution import ClosureWitness, KeysetPhase
from b24api.contracts.report import PageDispatch, PageOutcome, PageRejectionCode, Violation, ViolationSeverity
from b24api.errors import PaginationError
from b24api.traversal.identity import _child_path, _request_with_controls
from b24api.traversal.keyset_fast_plan import LaneBounds, LaneKind, LaneSpec, LaneState, LaneStatus
from b24api.traversal.keyset_partition import anchor_guesses, normalize_anchors
from b24api.traversal.page_validation import (
    LaneCommandPlan,
    LaneReceipt,
    ReceiptRejection,
    validate_lane_receipt,
)

if TYPE_CHECKING:
    from b24api.contracts.request import ParameterPath, Request
    from b24api.contracts.response import Response


class KeysetSchedulerSupport:
    """Methods sharing scheduler-owned state without owning state themselves."""

    def _controls(
        self: Any,
        *,
        direction: str,
        lower: int | None = None,
        upper: int | None = None,
        limit: int | None = None,
        advisory_start: bool = False,
    ) -> Request:
        updates: dict[ParameterPath, object] = {}
        if self.keyset.split_order is None:
            if self.keyset.order_path is None:
                raise RuntimeError("keyset ordering contract is missing")
            updates[_child_path(self.keyset.order_path, self.identity.order_key)] = direction
        else:
            updates[self.keyset.split_order.field_path] = self.keyset.split_order.field_value or self.identity.order_key
            updates[self.keyset.split_order.direction_path] = (
                self.keyset.split_order.ascending if direction == "ASC" else self.keyset.split_order.descending
            )
        if lower is not None:
            updates[_child_path(self.keyset.filter_path, f">{self.identity.filter_key}")] = lower
        if upper is not None:
            updates[_child_path(self.keyset.filter_path, f"<{self.identity.filter_key}")] = upper
        if self.keyset.start_suppression_path is not None:
            updates[self.keyset.start_suppression_path] = 0 if advisory_start else -1
        if self.keyset.limit_path is not None and limit is not None:
            updates[self.keyset.limit_path] = limit
        return _request_with_controls(self.request, updates, allow_create=self.keyset.allow_create_controls)

    async def _wave(self: Any, plans: tuple[LaneCommandPlan, ...]) -> tuple[LaneReceipt, ...]:
        if not plans or len(plans) > self.engine.portal_command_cap:
            raise ValueError("fast keyset wave must contain 1..50 commands")
        reservations = []
        reserved_rows = sum(plan.reserved_rows for plan in plans)
        try:
            for _ in plans:
                reservations.append(await self.context.reserve_page())
            await self._adjust_buffer(reserved_rows)
            outcomes = await self.engine.execute_requests(tuple(plan.request for plan in plans), context=self.context)
            self._last_wave_responses = {
                plan.command_id: outcome.response
                for plan, outcome in zip(plans, outcomes, strict=True)
                if isinstance(outcome, BatchSuccess) and outcome.response is not None
            }
            self.batch_requests += 1
            self.batch_commands += len(plans)
            planning = {KeysetPhase.BOUNDARY, KeysetPhase.CANARY, KeysetPhase.ANCHOR_PROBE}
            phases = {plan.phase for plan in plans}
            if phases & planning:
                self._planning_physical_requests += 1
            for phase in phases & planning:
                self._planning_requests[phase] += 1
            receipts: list[LaneReceipt] = []
            failed = False
            for index, (plan, outcome, reservation) in enumerate(zip(plans, outcomes, reservations, strict=True)):
                if isinstance(outcome, BatchSuccess):
                    self.context.commit_page(reservation)
                else:
                    self.context.release_page(reservation)
                receipt = validate_lane_receipt(
                    plan=plan,
                    lane=self._lane_for_plan(plan),
                    outcome=outcome,
                    identity=self.identity,
                    collection_shape=self.collection_shape,
                    effective_page_cap=(plan.reserved_rows if plan.expects_single_row else self.effective_page_cap),
                    completion=self._completion,
                    selector=self.selector,
                )
                if isinstance(receipt, ReceiptRejection):
                    failed = True
                    self.violations.append(receipt.violation)
                    self._record(
                        plan,
                        index=index,
                        selected=0,
                        admitted=0,
                        outcome=PageOutcome.REJECTED,
                        rejection=(
                            PageRejectionCode.COMMAND_FAILURE
                            if isinstance(outcome, BatchFailure)
                            else PageRejectionCode.RANGE_CONTRADICTION
                        ),
                        violation=receipt.violation,
                    )
                else:
                    receipts.append(receipt)
            if failed:
                successful = {receipt.command_id: receipt for receipt in receipts}
                for index, plan in enumerate(plans):
                    if plan.command_id in successful:
                        self._record(
                            plan,
                            index=index,
                            selected=len(successful[plan.command_id].rows),
                            admitted=0,
                            outcome=PageOutcome.REJECTED,
                            rejection=PageRejectionCode.TRANSACTION_ABORTED,
                        )
                raise PaginationError("fast keyset wave validation failed")
            for index, (plan, receipt) in enumerate(zip(plans, receipts, strict=True)):
                outcome = outcomes[index]
                self._record(
                    plan,
                    index=index,
                    selected=len(receipt.rows),
                    admitted=0,
                    response=outcome.response if isinstance(outcome, BatchSuccess) else None,
                    witness=receipt.witness,
                )
            return tuple(receipts)
        finally:
            for reservation in reservations:
                self.context.release_page(reservation)
            release = min(reserved_rows, self._buffer_balance)
            await self._adjust_buffer(-release)

    def _lane_for_plan(self: Any, plan: LaneCommandPlan) -> LaneState:
        if plan.phase is KeysetPhase.BOUNDARY:
            return LaneState(
                LaneSpec(plan.lane_ordinal, LaneKind.HEAD, LaneBounds(None, None), plan.lane_ordinal == 1, True, None),
                None,
                LaneStatus.OPEN,
                None,
                0,
                plan.reserved_rows,
                deque(),
            )
        if plan.phase in {KeysetPhase.CANARY, KeysetPhase.ANCHOR_PROBE}:
            descending = self._planning_descending.get(plan.command_id, False)
            bounds = self._planning_bounds[plan.command_id]
            return LaneState(
                LaneSpec(plan.lane_ordinal, LaneKind.LANE, bounds, descending, False, None),
                bounds.upper_exclusive if descending else bounds.lower_exclusive,
                LaneStatus.OPEN,
                None,
                0,
                plan.reserved_rows,
                deque(),
            )
        if plan.phase is KeysetPhase.FINISH:
            return cast("LaneState", self._finish_lane)
        return cast("LaneState", next(lane for lane in self._lanes if lane.spec.ordinal == plan.lane_ordinal))

    def _record(
        self: Any,
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
        from b24api.contracts.keyset_execution import TraceClass
        from b24api.traversal.keyset_fast_stream import PageObservation

        self.trace.record(
            PageObservation(
                self._observation_ordinal,
                plan.phase,
                plan.lane_ordinal,
                plan.command_id,
                dispatch,
                index,
                selected,
                admitted,
                response.total if response is not None else None,
                response.next if response is not None else None,
                selected == self.effective_page_cap,
                witness,
                outcome,
                rejection,
                violation,
                TraceClass.BODY,
            ),
        )
        self._observation_ordinal += 1

    def _response_for(self: Any, plan: LaneCommandPlan, receipt: LaneReceipt) -> Response | None:
        del plan
        return cast("Response | None", self._last_wave_responses.get(receipt.command_id))

    def _make_canary_plans(
        self: Any,
        asc: LaneReceipt,
        desc: LaneReceipt,
    ) -> tuple[tuple[LaneCommandPlan, ...], dict[str, tuple[int, ...]]]:
        prefixes = (asc.identities, tuple(reversed(desc.identities)))
        pair = next(
            ((values[index], values[index + 1]) for values in prefixes for index in range(len(values) - 1)),
            None,
        )
        if pair is None or self.effective_page_cap < 2:
            raise PaginationError("boundary facts cannot construct five capability canaries")
        p, q = pair
        definitions: tuple[tuple[int, int, tuple[int, ...]], ...] = (
            (p, p + 1, ()),
            (p - 1, p, ()),
            (p - 1, p + 1, (p,)),
            (p - 1, q + 1, (p, q)),
            (p - 1, q + 1, (q, p)),
        )
        plans: list[LaneCommandPlan] = []
        expected: dict[str, tuple[int, ...]] = {}
        for ordinal in (2, 0, 4, 1, 3):
            lower, upper, values = definitions[ordinal]
            descending = ordinal == 4
            request = self._controls(
                direction="DESC" if descending else "ASC",
                lower=lower,
                upper=upper,
                limit=self.effective_page_cap,
            )
            lane = LaneState(
                LaneSpec(ordinal, LaneKind.LANE, LaneBounds(lower, upper), descending, False, None),
                upper if descending else lower,
                LaneStatus.OPEN,
                None,
                0,
                self.effective_page_cap,
                deque(),
            )
            plan = self._lane_plan(lane, phase=KeysetPhase.CANARY, request=request)
            self._planning_bounds[plan.command_id] = lane.spec.bounds
            self._planning_descending[plan.command_id] = descending
            plans.append(plan)
            expected[plan.command_id] = values
        return tuple(plans), expected

    def _validate_canaries(self: Any, receipts: tuple[LaneReceipt, ...], expected: dict[str, tuple[int, ...]]) -> None:
        by_id = {receipt.command_id: receipt for receipt in receipts}
        if any(by_id[command_id].identities != values for command_id, values in expected.items()):
            violation = Violation(
                ViolationSeverity.BLOCKING,
                "canary_contradiction",
                "bounded keyset capability canary failed",
            )
            self.violations.append(violation)
            raise PaginationError("bounded keyset capability canary failed")
        count = sum(len(receipt.rows) for receipt in receipts)
        self.admission.record_raw(count, discarded=True)

    async def _canaries(self: Any, asc: LaneReceipt, desc: LaneReceipt) -> None:
        plans, expected = self._make_canary_plans(asc, desc)
        self._validate_canaries(await self._chunked_waves(plans), expected)

    def _make_anchor_plans(self: Any, asc: LaneReceipt, desc: LaneReceipt, target: int) -> tuple[LaneCommandPlan, ...]:
        lo, hi = max(asc.identities), min(desc.identities)
        plans = []
        for ordinal, guess in enumerate(anchor_guesses(lo=lo, upper_exclusive=hi, target_lanes=target)):
            reserve = 1 if self.keyset.limit_path is not None else self.effective_page_cap
            request = self._controls(
                direction="ASC",
                lower=guess,
                upper=hi,
                limit=1 if self.keyset.limit_path else None,
            )
            lane = LaneState(
                LaneSpec(ordinal, LaneKind.LANE, LaneBounds(guess, hi), False, False, None),
                guess,
                LaneStatus.OPEN,
                None,
                0,
                reserve,
                deque(),
            )
            plan = self._lane_plan(
                lane,
                phase=KeysetPhase.ANCHOR_PROBE,
                request=request,
                reserve=reserve,
                single=self.keyset.limit_path is not None,
            )
            self._planning_bounds[plan.command_id] = lane.spec.bounds
            self._planning_descending[plan.command_id] = False
            plans.append(plan)
        return tuple(plans)

    def _consume_anchors(self: Any, receipts: tuple[LaneReceipt, ...], *, lo: int, hi: int) -> tuple[int, ...]:
        anchors = []
        self._empty_anchor_probes = 0
        discarded = 0
        for receipt in receipts:
            if not receipt.rows:
                self._empty_anchor_probes += 1
                continue
            anchor = receipt.identities[0]
            anchors.append(anchor)
            self._anchor_rows.setdefault(anchor, receipt.rows[0])
            discarded += max(0, len(receipt.rows) - 1)
        normalized = normalize_anchors(lo=lo, upper_exclusive=hi, anchors=tuple(anchors))
        discarded += len(anchors) - len(normalized)
        self._anchor_count = len(normalized)
        self.admission.record_raw(sum(len(receipt.rows) for receipt in receipts))
        self.admission.record_discarded(discarded)
        return normalized

    async def _probe_anchors(self: Any, asc: LaneReceipt, desc: LaneReceipt, target: int) -> tuple[int, ...]:
        plans = self._make_anchor_plans(asc, desc, target)
        self._anchor_probe_commands = len(plans)
        receipts = await self._chunked_waves(plans) if plans else ()
        return cast("tuple[int, ...]", self._consume_anchors(receipts, lo=max(asc.identities), hi=min(desc.identities)))

    async def _partition_planning(self: Any, asc: LaneReceipt, desc: LaneReceipt, target: int) -> tuple[int, ...]:
        canaries, expected = self._make_canary_plans(asc, desc)
        anchors = self._make_anchor_plans(asc, desc, target)
        self._anchor_probe_commands = len(anchors)
        if len(canaries) + len(anchors) <= self.batch_capacity:
            receipts = await self._wave((*canaries, *anchors))
            canary_receipts = receipts[: len(canaries)]
            anchor_receipts = receipts[len(canaries) :]
            self._validate_canaries(canary_receipts, expected)
        else:
            canary_receipts = await self._chunked_waves(canaries)
            self._validate_canaries(canary_receipts, expected)
            anchor_receipts = await self._chunked_waves(anchors) if anchors else ()
        return cast(
            "tuple[int, ...]",
            self._consume_anchors(anchor_receipts, lo=max(asc.identities), hi=min(desc.identities)),
        )

    async def _chunked_waves(self: Any, plans: tuple[LaneCommandPlan, ...]) -> tuple[LaneReceipt, ...]:
        receipts: list[LaneReceipt] = []
        size = max(1, self.batch_capacity)
        for offset in range(0, len(plans), size):
            receipts.extend(await self._wave(plans[offset : offset + size]))
        return tuple(receipts)


__all__ = ["KeysetSchedulerSupport"]
