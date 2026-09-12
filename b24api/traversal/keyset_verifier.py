"""Explicit, fail-closed verification of strict integer keyset bounds."""

from __future__ import annotations
import itertools
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from b24api.batch.engine import BatchExecutor
from b24api.batch.outcome import BatchFailure, BatchSuccess
from b24api.contracts.keyset_capability import (
    KeysetCapabilityCheckName,
    KeysetCapabilityCheckOutcome,
    KeysetCapabilityCheckResult,
    KeysetCapabilityReport,
    KeysetCapabilityVerdict,
    KeysetInconclusiveReason,
    MembershipRecheck,
)
from b24api.contracts.keyset_execution import KeysetPhase
from b24api.contracts.policy import IdentityCoercion
from b24api.contracts.report import (
    PageDispatch,
    PageOutcome,
    PageRecord,
    Violation,
    ViolationSeverity,
    retain_page_trace,
)
from b24api.contracts.wire import BodyEncoding
from b24api.errors import CapabilityError, KeysetCapabilityError, PaginationError
from b24api.traversal import keyset_step
from b24api.traversal.facade import _collection_selector
from b24api.traversal.identity import _child_path, _request_with_controls
from b24api.traversal.keyset_capability import canary_commands
from b24api.traversal.keyset_eligibility import _reject_owned_controls
from b24api.traversal.values import _coerce_identity, _extract_path, _response_items, _validate_order

if TYPE_CHECKING:
    from b24api.contracts.policy import ExecutionPolicy
    from b24api.contracts.request import IdentitySpec, Request, ResultSelector
    from b24api.contracts.response import Response, ResultCollectionShape
    from b24api.contracts.traversal import KeysetSpec
    from b24api.execution import Executor

_PROOF = frozenset(
    {
        KeysetCapabilityCheckOutcome.OUT_OF_INTERVAL_ROWS,
        KeysetCapabilityCheckOutcome.ORDER_INVALID,
        KeysetCapabilityCheckOutcome.CAP_EXCEEDED,
        KeysetCapabilityCheckOutcome.SHAPE_INVALID,
    },
)
_RECHECK_LIMIT = 8
_MINIMUM_PAIR_ROWS = 2


@dataclass(frozen=True, slots=True)
class _CheckPlan:
    name: KeysetCapabilityCheckName
    lower: int
    upper: int
    descending: bool
    expected: tuple[int, ...]


class _Verifier:
    def __init__(  # noqa: PLR0913
        self,
        executor: Executor,
        request: Request,
        *,
        selector: ResultSelector,
        identity: IdentitySpec,
        collection_shape: ResultCollectionShape,
        page_size: int,
        keyset: KeysetSpec,
        policy: ExecutionPolicy,
    ) -> None:
        self.executor, self.request = executor, request
        self.selector = _collection_selector(selector, collection_shape)
        self.identity, self.page_size, self.keyset = identity, page_size, keyset
        self.context = executor.context(policy)
        self.engine = BatchExecutor(executor)
        self.capacity = min(50, policy.max_buffered_commands, max(1, policy.max_buffered_rows // page_size))
        self.batch_waves = 0
        self.logical_commands = 0
        self.page_trace: tuple[PageRecord, ...] = ()
        self.page_trace_truncated = False
        self._trace_sequence = 0

    async def run(self) -> KeysetCapabilityReport:
        await self.context.start()
        boundaries = (
            self._request("ASC", None, None),
            self._request("DESC", None, None),
        )
        boundary_responses = await self._waves(boundaries, KeysetPhase.BOUNDARY)
        asc = self._identities(boundary_responses[0], direction="asc")
        desc = self._identities(boundary_responses[1], direction="desc")
        reason = self._early_reason(asc, desc)
        if reason is not None:
            return await self._report(
                KeysetCapabilityVerdict.INCONCLUSIVE,
                self._not_executed(),
                reason=reason,
                cross_digit=False,
            )
        pair = self._pair(asc, desc)
        if pair is None:
            return await self._report(
                KeysetCapabilityVerdict.INCONCLUSIVE,
                self._not_executed(),
                reason=KeysetInconclusiveReason.NO_USABLE_IDENTITY_PAIR,
                cross_digit=False,
            )
        snapshot = tuple(sorted({*asc, *desc}))
        plans = self._plans(asc, desc, snapshot)
        responses = await self._waves(
            tuple(self._request("DESC" if plan.descending else "ASC", plan.lower, plan.upper) for plan in plans),
            KeysetPhase.CANARY,
        )
        checks = tuple(self._check(plan, response) for plan, response in zip(plans, responses, strict=True))
        if any(check.outcome in _PROOF for check in checks):
            return await self._report(
                KeysetCapabilityVerdict.UNSUPPORTED,
                checks,
                reason=None,
                cross_digit=self._cross_digit(pair),
            )
        checks = self._mark_contradictions(plans, responses, checks)
        drifting = tuple(
            check
            for check in checks
            if check.outcome
            in {
                KeysetCapabilityCheckOutcome.IN_INTERVAL_DRIFT,
                KeysetCapabilityCheckOutcome.CROSS_CANARY_CONTRADICTION,
            }
        )
        if not drifting:
            return await self._report(
                KeysetCapabilityVerdict.VERIFIED,
                checks,
                reason=None,
                cross_digit=self._cross_digit(pair),
            )
        checks, reason = await self._recheck(checks)
        return await self._report(
            KeysetCapabilityVerdict.INCONCLUSIVE,
            checks,
            reason=reason,
            cross_digit=self._cross_digit(pair),
        )

    def _request(self, direction: str, lower: int | None, upper: int | None) -> Request:
        return keyset_step.bounded_keyset_request(
            self.request,
            keyset=self.keyset,
            identity=self.identity,
            direction=direction,
            lower=lower,
            upper=upper,
            limit=self.page_size,
        )

    async def _waves(self, requests: tuple[Request, ...], phase: KeysetPhase) -> tuple[Response, ...]:
        responses: list[Response] = []
        for offset in range(0, len(requests), self.capacity):
            wave = requests[offset : offset + self.capacity]
            reservations = await self.context.reserve_pages(len(wave))
            self.batch_waves += 1
            self.logical_commands += len(wave)
            try:
                outcomes = await self.engine.execute_requests(
                    wave,
                    context=self.context,
                    strict_envelope=True,
                    strict_json_members=True,
                )
                for reservation, outcome in zip(reservations, outcomes, strict=True):
                    if isinstance(outcome, BatchFailure):
                        self.context.release_page(reservation)
                        if isinstance(outcome.error, BaseException):
                            raise outcome.error
                        raise CapabilityError("keyset verifier batch command failed")
                    if not isinstance(outcome, BatchSuccess) or outcome.response is None:
                        self.context.release_page(reservation)
                        raise CapabilityError("keyset verifier lacks correlated response evidence")
                    self.context.commit_page(reservation)
                    responses.append(outcome.response)
                    self._record_response(outcome.response, outcome.command_index, phase)
            finally:
                for reservation in reservations:
                    self.context.release_page(reservation)
        return tuple(responses)

    def _record_response(self, response: Response, batch_index: int, phase: KeysetPhase) -> None:
        try:
            rows_selected = len(_response_items(response, self.selector))
        except (CapabilityError, PaginationError):
            rows_selected = 0
        total = response.total if isinstance(response.total, int) and response.total >= 0 else None
        next_value = response.next if isinstance(response.next, int) and response.next >= 0 else None
        record = PageRecord(
            sequence=self._trace_sequence,
            offset=None,
            dispatch=PageDispatch.BATCH,
            batch_index=batch_index,
            rows_selected=rows_selected,
            rows_admitted=0,
            reported_total=total,
            reported_next=next_value,
            outcome=PageOutcome.COMMITTED,
            rejection_code=None,
            phase=phase,
        )
        self._trace_sequence += 1
        retained, truncated = retain_page_trace(
            (*self.page_trace, record),
            self.context.policy.page_trace_limit,
        )
        self.page_trace = retained
        self.page_trace_truncated = self.page_trace_truncated or truncated

    def _identities(
        self,
        response: Response,
        *,
        direction: str | None = None,
        enforce_cap: bool = True,
    ) -> tuple[int, ...]:
        rows = _response_items(response, self.selector)
        if enforce_cap and self.keyset.limit_path is not None and len(rows) > self.page_size:
            raise PaginationError("keyset verifier response exceeded its declared page cap")
        values = tuple(
            _coerce_identity(_extract_path(row, self.identity.item_path), self.identity.coercion) for row in rows
        )
        if any(not isinstance(value, int) for value in values):
            raise CapabilityError("keyset verifier requires integer-coercible identity values")
        identities = cast("tuple[int, ...]", values)
        if direction is not None:
            _validate_order(identities, direction)
        return identities

    def _early_reason(self, asc: tuple[int, ...], desc: tuple[int, ...]) -> KeysetInconclusiveReason | None:
        if self.page_size < _MINIMUM_PAIR_ROWS:
            return KeysetInconclusiveReason.PAGE_CAP_TOO_SMALL
        if len(asc) == len(desc) == 1 and asc[0] != desc[0]:
            return KeysetInconclusiveReason.PAGE_CAP_TOO_SMALL
        if len({*asc, *desc}) < _MINIMUM_PAIR_ROWS:
            return KeysetInconclusiveReason.INSUFFICIENT_ROWS
        return None

    @staticmethod
    def _pair(asc: tuple[int, ...], desc: tuple[int, ...]) -> tuple[int, int] | None:
        ascending_desc = tuple(reversed(desc))
        candidates = tuple(itertools.pairwise(asc)) + tuple(itertools.pairwise(ascending_desc))
        increasing = tuple((left, right) for left, right in candidates if left < right)
        fallback = increasing[0] if increasing else None
        return next((_pair for _pair in increasing if _Verifier._cross_digit(_pair)), fallback)

    @staticmethod
    def _cross_digit(pair: tuple[int, int]) -> bool:
        return len(str(abs(pair[0]))) != len(str(abs(pair[1])))

    @staticmethod
    def _plans(
        ascending: tuple[int, ...],
        descending: tuple[int, ...],
        snapshot: tuple[int, ...],
    ) -> tuple[_CheckPlan, ...]:
        commands = {command.ordinal: command for command in canary_commands(ascending, descending, 2)}
        command_ordinals = {
            KeysetCapabilityCheckName.LOWER_EMPTY: 1,
            KeysetCapabilityCheckName.UPPER_EMPTY: 0,
            KeysetCapabilityCheckName.SINGLETON: 2,
            KeysetCapabilityCheckName.TWO_ROW_ASC: 3,
            KeysetCapabilityCheckName.TWO_ROW_DESC: 4,
        }
        plans: list[_CheckPlan] = []
        for name in KeysetCapabilityCheckName:
            command = commands[command_ordinals[name]]
            lower, upper = command.bounds.lower_exclusive, command.bounds.upper_exclusive
            if lower is None or upper is None:
                raise RuntimeError("verifier canary geometry must have two strict bounds")
            expected = tuple(value for value in snapshot if lower < value < upper)
            plans.append(_CheckPlan(name, lower, upper, command.descending, expected))
        return tuple(plans)

    def _check(self, plan: _CheckPlan, response: Response) -> KeysetCapabilityCheckResult:
        try:
            identities = self._identities(response, enforce_cap=False)
        except (CapabilityError, PaginationError):
            return KeysetCapabilityCheckResult(plan.name, KeysetCapabilityCheckOutcome.SHAPE_INVALID)
        if self.keyset.limit_path is not None and len(identities) > self.page_size:
            return KeysetCapabilityCheckResult(plan.name, KeysetCapabilityCheckOutcome.CAP_EXCEEDED, len(identities))
        outside = tuple(value for value in identities if not plan.lower < value < plan.upper)
        if outside:
            return KeysetCapabilityCheckResult(
                plan.name,
                KeysetCapabilityCheckOutcome.OUT_OF_INTERVAL_ROWS,
                len(identities),
                out_of_interval_identities=outside,
            )
        direction = "desc" if plan.descending else "asc"
        try:
            _validate_order(identities, direction)
        except PaginationError:
            return KeysetCapabilityCheckResult(plan.name, KeysetCapabilityCheckOutcome.ORDER_INVALID, len(identities))
        observed = set(identities)
        expected = set(plan.expected)
        missing, extra = tuple(sorted(expected - observed)), tuple(sorted(observed - expected))
        outcome = (
            KeysetCapabilityCheckOutcome.PASSED
            if not missing and not extra
            else KeysetCapabilityCheckOutcome.IN_INTERVAL_DRIFT
        )
        return KeysetCapabilityCheckResult(
            plan.name,
            outcome,
            len(identities),
            missing_in_interval_identities=missing,
            extra_in_interval_identities=extra,
        )

    def _mark_contradictions(
        self,
        plans: tuple[_CheckPlan, ...],
        responses: tuple[Response, ...],
        checks: tuple[KeysetCapabilityCheckResult, ...],
    ) -> tuple[KeysetCapabilityCheckResult, ...]:
        observations = tuple(set(self._identities(response, enforce_cap=False)) for response in responses)
        contradictory: list[set[int]] = [set() for _ in plans]
        for left in range(len(plans)):
            for right in range(left + 1, len(plans)):
                changed = observations[left] ^ observations[right]
                for value in changed:
                    if not (
                        plans[left].lower < value < plans[left].upper
                        and plans[right].lower < value < plans[right].upper
                    ):
                        continue
                    contradictory[left].add(value)
                    contradictory[right].add(value)
        return tuple(
            KeysetCapabilityCheckResult(
                check.name,
                (
                    KeysetCapabilityCheckOutcome.CROSS_CANARY_CONTRADICTION
                    if values and check.outcome not in _PROOF
                    else check.outcome
                ),
                check.rows_selected,
                check.out_of_interval_identities,
                check.missing_in_interval_identities,
                check.extra_in_interval_identities,
                tuple(sorted(values)),
                check.recheck,
            )
            for check, values in zip(checks, contradictory, strict=True)
        )

    async def _recheck(
        self,
        checks: tuple[KeysetCapabilityCheckResult, ...],
    ) -> tuple[tuple[KeysetCapabilityCheckResult, ...], KeysetInconclusiveReason]:
        contradictory = sorted({cast("int", value) for check in checks for value in check.contradictory_identities})
        missing = sorted({cast("int", value) for check in checks for value in check.missing_in_interval_identities})
        extras = sorted({cast("int", value) for check in checks for value in check.extra_in_interval_identities})
        ordered: list[int] = []
        for group in (contradictory, missing, extras):
            ordered.extend(value for value in group if value not in ordered)
        selected, truncated = tuple(ordered[:_RECHECK_LIMIT]), len(ordered) > _RECHECK_LIMIT
        responses = await self._waves(tuple(self._exact_request(value) for value in selected), KeysetPhase.CANARY)
        still = tuple(
            value for value, response in zip(selected, responses, strict=True) if value in self._identities(response)
        )
        gone = tuple(value for value in selected if value not in still)
        recheck = MembershipRecheck(
            selected,
            still,
            gone,
            tuple(value for value in contradictory if value in selected),
            truncated=truncated,
        )
        updated = tuple(
            KeysetCapabilityCheckResult(
                check.name,
                check.outcome,
                check.rows_selected,
                check.out_of_interval_identities,
                check.missing_in_interval_identities,
                check.extra_in_interval_identities,
                check.contradictory_identities,
                recheck,
            )
            if check.outcome is not KeysetCapabilityCheckOutcome.PASSED
            else check
            for check in checks
        )
        if contradictory:
            reason = KeysetInconclusiveReason.UNSTABLE_BOUNDARY
        elif set(missing) & set(still):
            reason = KeysetInconclusiveReason.BOUND_OVER_RESTRICTIVE_SUSPECTED
        elif extras:
            reason = KeysetInconclusiveReason.IN_RANGE_DRIFT
        else:
            reason = KeysetInconclusiveReason.CONCURRENT_MUTATION
        return updated, reason

    def _exact_request(self, value: int) -> Request:
        request = self._request("ASC", None, None)
        return _request_with_controls(
            request,
            {_child_path(self.keyset.filter_path, self.identity.filter_key): value},
            allow_create=self.keyset.allow_create_controls,
        )

    @staticmethod
    def _not_executed() -> tuple[KeysetCapabilityCheckResult, ...]:
        return tuple(
            KeysetCapabilityCheckResult(name, KeysetCapabilityCheckOutcome.NOT_EXECUTED)
            for name in KeysetCapabilityCheckName
        )

    async def _report(
        self,
        verdict: KeysetCapabilityVerdict,
        checks: tuple[KeysetCapabilityCheckResult, ...],
        *,
        reason: KeysetInconclusiveReason | None,
        cross_digit: bool,
    ) -> KeysetCapabilityReport:
        snapshot = await self.context.snapshot()
        violations = tuple(
            Violation(
                ViolationSeverity.BLOCKING if check.outcome in _PROOF else ViolationSeverity.WARNING,
                f"keyset_capability_{check.outcome.value}",
                f"keyset capability check {check.name.value} observed {check.outcome.value}",
            )
            for check in checks
            if check.outcome
            not in {
                KeysetCapabilityCheckOutcome.PASSED,
                KeysetCapabilityCheckOutcome.NOT_EXECUTED,
            }
        )
        return KeysetCapabilityReport(
            verdict=verdict,
            checks=checks,
            physical_requests=snapshot.counters.physical_requests,
            batch_waves=self.batch_waves,
            logical_commands=self.logical_commands,
            cross_digit_pair_exercised=cross_digit,
            inconclusive_reason=reason,
            inconclusive_detail=None if reason is None else reason.value,
            violations=violations,
            page_trace=self.page_trace,
            page_trace_truncated=self.page_trace_truncated,
        )


async def verify_keyset_capability(  # noqa: PLR0913
    executor: Executor,
    request: Request,
    *,
    selector: ResultSelector,
    identity: IdentitySpec,
    collection_shape: ResultCollectionShape,
    page_size: int,
    keyset: KeysetSpec,
    policy: ExecutionPolicy,
) -> KeysetCapabilityReport:
    """Return a verified report or raise a typed fail-closed verdict."""
    if not isinstance(page_size, int) or isinstance(page_size, bool) or page_size < 1:
        raise ValueError("page_size must be a positive integer")
    if request.encoding is not BodyEncoding.JSON or request.headers.items:
        raise CapabilityError("keyset verifier supports JSON requests without scoped headers")
    if identity.coercion not in {IdentityCoercion.EXACT_INTEGER, IdentityCoercion.DECIMAL_STRING_INTEGER}:
        raise CapabilityError("keyset verifier requires integer identity coercion")
    _reject_owned_controls(request, identity, keyset)
    verifier = _Verifier(
        executor,
        request,
        selector=selector,
        identity=identity,
        collection_shape=collection_shape,
        page_size=page_size,
        keyset=keyset,
        policy=policy,
    )
    report = await verifier.run()
    if report.verdict is not KeysetCapabilityVerdict.VERIFIED:
        raise KeysetCapabilityError(report=report)
    return report


__all__: list[str] = []
