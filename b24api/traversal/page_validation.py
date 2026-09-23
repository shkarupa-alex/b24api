"""Transactional fast-keyset receipt selection and lane-local validation."""

# ruff: noqa: C901, PLR0912, PLR2004, TRY301

from __future__ import annotations
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from b24api.batch.outcome import BatchFailure, BatchSuccess
from b24api.contracts.keyset_execution import ClosureWitness, KeysetPageCompletion, KeysetPhase
from b24api.contracts.page import IdentityPageAdapter, PageAdapter
from b24api.contracts.policy import ReplayDisposition
from b24api.contracts.report import PageOutcome, PageRejectionCode, Violation, ViolationSeverity
from b24api.contracts.request import ResultSelector
from b24api.errors import (
    AmbiguousExecutionError,
    B24ApiError,
    CapabilityError,
    EnvelopeContractError,
    PageAdaptationError,
    PaginationError,
    ProtocolError,
)
from b24api.traversal.keyset_range import closure_witness
from b24api.traversal.page_adaptation import adapt_page
from b24api.traversal.values import _coerce_identity, _extract_path, _response_items, _validate_order

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.batch.outcome import BatchOutcome
    from b24api.contracts.json import FrozenJson
    from b24api.contracts.request import IdentitySpec, Request
    from b24api.contracts.response import ResultCollectionShape
    from b24api.traversal.keyset_fast_plan import LaneState

_IDENTITY_PAGE_ADAPTER = IdentityPageAdapter()


@dataclass(frozen=True, slots=True)
class LaneCommandPlan:
    """One correlated command and its reservation contract."""

    lane_ordinal: int
    command_id: str
    phase: KeysetPhase
    request: Request
    reserved_rows: int
    expects_single_row: bool


@dataclass(frozen=True, slots=True)
class LaneReceipt:
    """Fully decoded and lane-validated response."""

    lane_ordinal: int
    command_id: str
    rows: tuple[FrozenJson, ...]
    identities: tuple[int, ...]
    page_full: bool
    last_identity: int | None
    witness: ClosureWitness | None
    warnings: tuple[Violation, ...]


@dataclass(frozen=True, slots=True)
class ReceiptRejection:
    """Value-free receipt rejection."""

    lane_ordinal: int
    command_id: str
    violation: Violation
    detail: str
    selected_rows: int = 0
    error: BaseException | None = None
    replay_disposition: ReplayDisposition = ReplayDisposition.NOT_ELIGIBLE


def _rejection(  # noqa: PLR0913
    plan: LaneCommandPlan,
    message: str,
    *,
    code: str = "keyset_receipt",
    selected_rows: int = 0,
    error: BaseException | None = None,
    replay_disposition: ReplayDisposition = ReplayDisposition.NOT_ELIGIBLE,
) -> ReceiptRejection:
    return ReceiptRejection(
        plan.lane_ordinal,
        plan.command_id,
        Violation(
            ViolationSeverity.BLOCKING,
            code,
            message,
            error=error if isinstance(error, B24ApiError) else None,
            replay_disposition=replay_disposition,
        ),
        message,
        selected_rows,
        error,
        replay_disposition,
    )


def select_rows(*, outcome: BatchSuccess, selector: ResultSelector) -> tuple[FrozenJson, ...]:
    """Select rows from one correlated successful command response."""
    if outcome.response is None:
        raise PaginationError("batch command lacks correlated response evidence")
    return tuple(_response_items(outcome.response, selector))


def classify_rejection(outcome: BatchOutcome) -> tuple[PageOutcome, PageRejectionCode]:
    """Classify one correlated failure without inspecting unsafe values."""
    if not isinstance(outcome, BatchFailure):
        return PageOutcome.REJECTED, PageRejectionCode.RANGE_CONTRADICTION
    error = outcome.error
    if isinstance(error, AmbiguousExecutionError):
        return PageOutcome.UNKNOWN, PageRejectionCode.AMBIGUOUS_EXECUTION
    if isinstance(error, ProtocolError | EnvelopeContractError):
        summary = error.request_summary
        if summary is not None and summary.method == "batch":
            return PageOutcome.REJECTED, PageRejectionCode.BATCH_ENVELOPE
        return PageOutcome.REJECTED, PageRejectionCode.COMMAND_FAILURE
    return PageOutcome.REJECTED, PageRejectionCode.COMMAND_FAILURE


def validate_lane_receipt(  # noqa: PLR0913
    *,
    plan: LaneCommandPlan,
    lane: LaneState,
    outcome: BatchOutcome,
    identity: IdentitySpec,
    collection_shape: ResultCollectionShape,
    effective_page_cap: int,
    completion: KeysetPageCompletion,
    selector: ResultSelector | None = None,
    page_adapter: PageAdapter = _IDENTITY_PAGE_ADAPTER,
) -> LaneReceipt | ReceiptRejection:
    """Validate one outcome without mutating global or lane state."""
    del collection_shape  # selection shape is represented by the prepared selector
    if isinstance(outcome, BatchFailure):
        return _rejection(
            plan,
            "keyset batch command failed",
            code="command_failure",
            error=outcome.error if isinstance(outcome.error, BaseException) else None,
            replay_disposition=outcome.replay_disposition,
        )
    if not isinstance(outcome, BatchSuccess) or outcome.response is None:
        return _rejection(plan, "keyset batch outcome is not a correlated success", code="command_failure")
    rows: tuple[FrozenJson, ...] = ()
    try:
        rows = _response_items(outcome.response, selector or ResultSelector.root())
        if plan.phase not in {KeysetPhase.ANCHOR_PROBE, KeysetPhase.CANARY}:
            rows = adapt_page(
                outcome.response,
                rows,
                adapter=page_adapter,
                identities=((identity.item_path, identity.coercion),),
                request_summary=plan.request.summary,
                page_offset=lane.rounds,
            )
        if len(rows) > effective_page_cap or len(rows) > plan.reserved_rows:
            raise PaginationError("response exceeded the declared keyset page cap")
        if plan.expects_single_row and len(rows) > 1:
            raise PaginationError("anchor probe returned more than one row")
        coerced = tuple(_coerce_identity(_extract_path(row, identity.item_path), identity.coercion) for row in rows)
        if any(not isinstance(value, int) for value in coerced):
            raise PaginationError("fast keyset identity must coerce to an integer")
        identities = tuple(value for value in coerced if isinstance(value, int))
        direction = "desc" if lane.spec.descending else "asc"
        _validate_order(list(identities), direction)
        lower = lane.spec.bounds.lower_exclusive
        upper = lane.spec.bounds.upper_exclusive
        if any(lower is not None and value <= lower for value in identities):
            raise PaginationError("keyset page ignored its strict lower bound")
        if any(upper is not None and value >= upper for value in identities):
            raise PaginationError("keyset page ignored its strict upper bound")
        if lane.cursor is not None and identities:
            if not lane.spec.descending and identities[0] <= lane.cursor:
                raise PaginationError("keyset lane cursor did not advance")
            if lane.spec.descending and identities[0] >= lane.cursor:
                raise PaginationError("descending keyset lane cursor did not advance")
        witness = None
        if plan.phase in {KeysetPhase.BODY, KeysetPhase.FINISH} and not lane.spec.descending:
            witness = closure_witness(
                bounds=lane.spec.bounds,
                cursor=lane.cursor if lane.cursor is not None else (lower if lower is not None else -1),
                identities=identities,
                page_cap=effective_page_cap,
                completion=completion,
            )
        elif not rows:
            witness = ClosureWitness.EMPTY
        return LaneReceipt(
            plan.lane_ordinal,
            plan.command_id,
            rows,
            identities,
            len(rows) == effective_page_cap,
            identities[-1] if identities else None,
            witness,
            (),
        )
    except PageAdaptationError as error:
        return _rejection(plan, str(error), code="page_adaptation", selected_rows=len(rows), error=error)
    except (CapabilityError, PaginationError) as error:
        return _rejection(plan, str(error), code="range_contradiction", selected_rows=len(rows), error=error)


def validate_boundary_direction(
    *,
    ascending: LaneReceipt,
    descending: LaneReceipt,
) -> ReceiptRejection | None:
    """Reject an ignored descending direction before boundary admission."""
    if bool(ascending.identities) != bool(descending.identities):
        return ReceiptRejection(
            descending.lane_ordinal,
            descending.command_id,
            Violation(
                ViolationSeverity.BLOCKING,
                "direction_contradiction",
                "boundary directions disagree on whether the selection is empty",
            ),
            "boundary directions disagree on whether the selection is empty",
        )
    if len(descending.identities) >= 2 and any(
        current >= previous for previous, current in zip(descending.identities, descending.identities[1:], strict=False)
    ):
        return ReceiptRejection(
            descending.lane_ordinal,
            descending.command_id,
            Violation(ViolationSeverity.BLOCKING, "direction_contradiction", "descending boundary order was ignored"),
            "descending boundary order was ignored",
        )
    if ascending.identities and descending.identities and max(descending.identities) < max(ascending.identities):
        return ReceiptRejection(
            descending.lane_ordinal,
            descending.command_id,
            Violation(ViolationSeverity.BLOCKING, "direction_contradiction", "descending boundary precedes head"),
            "descending boundary precedes head",
        )
    return None


def normalize_tail_receipt(
    tail: LaneReceipt,
    *,
    already_seen: Callable[[int], bool],
) -> tuple[LaneReceipt, int]:
    """Remove boundary overlap while preserving traversal-order tail ownership."""
    rows_ids = tuple(reversed(tuple(zip(tail.rows, tail.identities, strict=True))))
    filtered = tuple((row, identity) for row, identity in rows_ids if not already_seen(identity))
    overlap = len(rows_ids) - len(filtered)
    return replace(
        tail,
        rows=tuple(row for row, _ in filtered),
        identities=tuple(identity for _, identity in filtered),
        last_identity=filtered[-1][1] if filtered else None,
    ), overlap


__all__ = [
    "LaneCommandPlan",
    "LaneReceipt",
    "ReceiptRejection",
    "classify_rejection",
    "select_rows",
    "validate_boundary_direction",
    "validate_lane_receipt",
]
