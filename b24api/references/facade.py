"""Canonical bound-reference composition over the proven bounded scheduler."""

# ruff: noqa: PLR0913 - bounded orchestration adapter
from __future__ import annotations
from collections.abc import AsyncIterator, Callable
from typing import TYPE_CHECKING, Literal, Protocol, cast

from b24api.completion.operation_stream import MappedOperationStream
from b24api.contracts.dispatch import DirectDispatch, DispatchSpec
from b24api.contracts.keyset_execution import SequentialKeysetExecution
from b24api.contracts.policy import (
    DuplicatePolicy,
    ExecutionPolicy,
    IdentityRequirement,
    OrderSemantics,
    TotalSemantics,
)
from b24api.contracts.reference import (
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ReferenceNotExecuted,
    ReferenceOutcome,
    ReferenceOutcomeUnknown,
)
from b24api.contracts.request import (
    IdentitySpec,
    Request,
    RequestLike,
    ResultSelector,
    TraversalIdentity,
    canonical_request,
)
from b24api.contracts.traversal import (
    CountedTraversal,
    KeysetTraversal,
    SequentialTraversal,
    TraversalSpec,
)
from b24api.errors import (
    AmbiguousExecutionError,
    B24ApiError,
    CapabilityError,
    IncompleteTraversalError,
    PageAdaptationError,
    PaginationError,
    ReferenceFailed,
)
from b24api.references.binding import BindingSource, _BindingContext, _BindingSourceError, binding_source
from b24api.references.dispatch import (
    _KernelReferenceComplete,
    _ReferenceWindowError,
)
from b24api.references.dispatch_plan import kernel_dispatch
from b24api.references.outcome import ReferenceFailure as KernelFailure
from b24api.references.outcome import ReferenceItem as KernelItem
from b24api.references.stream import iter_references as _iter_references
from b24api.traversal.driver import PaginationDriver
from b24api.traversal.offset_rules import sequential_offset_plan
from b24api.traversal.plans import (
    CountedOffsetMode,
    CountedOffsetPlan,
    CursorTerminalRule,
    ItemCursorPlan,
    KeysetPlan,
    KeysetTerminalRule,
    ListPlan,
    OffsetContinuation,
)

if TYPE_CHECKING:
    from b24api.contracts.page_stop import PageStopPolicy
    from b24api.contracts.report import OperationReport, Violation
    from b24api.contracts.stream import OperationStream
    from b24api.execution.snapshot import KernelReport

type KernelReferenceEvent = KernelItem | KernelFailure | _KernelReferenceComplete
type Deregister = Callable[[object], None]


class ReferenceKernelStream(AsyncIterator[KernelReferenceEvent], Protocol):
    """Narrow structural view of the reference scheduler output."""

    report: KernelReport
    active_references_high_water: int

    async def aclose(self) -> None:
        """Close scheduler-owned resources."""
        ...


def _direction(value: str) -> Literal["asc", "desc"]:
    return "asc" if value == "ascending" else "desc"


def _kernel_plan(traversal: TraversalSpec) -> tuple[ListPlan, ResultSelector, TraversalIdentity | None]:
    if isinstance(traversal, SequentialTraversal):
        if traversal.offset.sparse_raw_bound is not None:
            # Reference pages carry one provenance record per delivered page; an empty raw window the
            # driver continues past has no delivery, so the binding could never close truthfully.
            raise CapabilityError("reference traversal does not support a sparse raw bound; traverse it directly")
        return (
            sequential_offset_plan(
                traversal.offset,
                page_size=traversal.page_size,
                duplicate_policy=DuplicatePolicy.ERROR,
            ),
            traversal.selector,
            traversal.identity,
        )
    if isinstance(traversal, CountedTraversal):
        counted_mechanics = traversal.offset
        return (
            CountedOffsetPlan(
                offset_path=counted_mechanics.parameter_path,
                limit_path=counted_mechanics.limit_path,
                requested_page_size=(traversal.page_size if counted_mechanics.limit_path is not None else None),
                allow_create_controls=counted_mechanics.allow_create_controls,
                identity_requirement=(
                    IdentityRequirement.OPTIONAL if traversal.identity is None else IdentityRequirement.REQUIRED
                ),
                duplicate_policy=DuplicatePolicy.ERROR,
                total_semantics=TotalSemantics.FILTERED_EXACT,
                continuation=counted_mechanics.continuation,
                mode=(
                    CountedOffsetMode.PARALLEL_FIXED_STRIDE
                    if counted_mechanics.continuation is OffsetContinuation.FIXED_STEP
                    else CountedOffsetMode.SEQUENTIAL_NEXT
                ),
                fixed_stride=counted_mechanics.step,
            ),
            traversal.selector,
            traversal.identity,
        )
    if isinstance(traversal, KeysetTraversal):
        if not isinstance(traversal.execution, SequentialKeysetExecution):
            raise CapabilityError("reference keyset traversal supports sequential execution only")
        keyset_mechanics = traversal.keyset
        if keyset_mechanics.boundary is not None:
            # A bounded range is fingerprinted to one request filter; bindings rewrite parameters,
            # so it cannot be shared across references and must not be silently dropped.
            raise CapabilityError("reference keyset traversal does not support a bounded identity range")
        direction = _direction(keyset_mechanics.direction)
        return (
            KeysetPlan(
                direction=direction,
                filter_path=keyset_mechanics.filter_path,
                order_path=keyset_mechanics.order_path,
                split_order=keyset_mechanics.split_order,
                start_suppression_path=keyset_mechanics.start_suppression_path,
                limit_path=keyset_mechanics.limit_path,
                requested_page_size=(traversal.page_size if keyset_mechanics.limit_path is not None else None),
                terminal=KeysetTerminalRule.EMPTY_CONFIRMATION,
                allow_create_controls=keyset_mechanics.allow_create_controls,
                identity_requirement=IdentityRequirement.REQUIRED,
                order_semantics=(OrderSemantics.ASCENDING if direction == "asc" else OrderSemantics.DESCENDING),
                duplicate_policy=DuplicatePolicy.ERROR,
                total_semantics=TotalSemantics.IGNORE,
            ),
            traversal.selector,
            traversal.identity,
        )
    cursor_mechanics = traversal.cursor
    direction = _direction(cursor_mechanics.direction)
    identity = traversal.identity
    if identity is None:
        identity = IdentitySpec(
            item_path=cursor_mechanics.item_path,
            filter_key="cursor",
            order_key="cursor",
            coercion=cursor_mechanics.coercion,
        )
    return (
        ItemCursorPlan(
            cursor_request_path=cursor_mechanics.parameter_path,
            cursor_item_path=cursor_mechanics.item_path,
            cursor_coercion=cursor_mechanics.coercion,
            direction=direction,
            cursor_take=cursor_mechanics.take,
            limit_path=cursor_mechanics.limit_path,
            requested_page_size=traversal.page_size if cursor_mechanics.limit_path is not None else None,
            terminal=CursorTerminalRule.EMPTY_CONFIRMATION,
            allow_create_controls=cursor_mechanics.allow_create_controls,
            domain=cursor_mechanics.domain,
            identity_requirement=IdentityRequirement.REQUIRED,
            order_semantics=OrderSemantics.ASCENDING if direction == "asc" else OrderSemantics.DESCENDING,
            duplicate_policy=DuplicatePolicy.ERROR,
        ),
        traversal.selector,
        identity,
    )


class _ReferenceEventMapper:
    """Stateful bounded converter assigning per-binding item indexes."""

    def __init__(self) -> None:
        """Initialize bounded per-active-binding row positions."""
        self._item_indexes: dict[int, int] = {}

    def __call__(self, event: KernelReferenceEvent) -> ReferenceOutcome[object]:
        """Convert one kernel event without interpreting correlation."""
        if isinstance(event, KernelItem):
            context = cast("_BindingContext", event.correlation)
            item_index = self._item_indexes.get(context.index, 0)
            self._item_indexes[context.index] = item_index + 1
            return ReferenceItem(context.index, context.correlation, item_index, event.item)
        if isinstance(event, _KernelReferenceComplete):
            context = cast("_BindingContext", event.reference.correlation)
            self._item_indexes.pop(context.index, None)
            return ReferenceComplete(
                context.index,
                context.correlation,
                event.row_count,
                exhausted=event.stopped_reason is None,
                stop_reason=event.stopped_reason,
            )
        context = cast("_BindingContext", event.correlation)
        self._item_indexes.pop(context.index, None)
        if event.not_executed_reason is not None:
            return ReferenceNotExecuted(context.index, context.correlation, event.not_executed_reason)
        error = event.error if isinstance(event.error, B24ApiError) else CapabilityError("reference failed")
        application_failure = isinstance(error, PageAdaptationError) and event.partial_rows == 0
        if isinstance(error, PaginationError) or (
            isinstance(error, CapabilityError) and event.page_state > 0 and not application_failure
        ):
            incomplete_cause = error
            # Only the whole stream has an operation report; the binding's evidence is this failure.
            error = IncompleteTraversalError(
                report=None,
                error=incomplete_cause,
                replay_disposition=event.replay_disposition,
            )
            error.__cause__ = incomplete_cause
        if isinstance(error, AmbiguousExecutionError):
            return ReferenceOutcomeUnknown(
                context.index,
                context.correlation,
                error,
                event.partial_rows,
                event.replay_disposition,
            )
        return ReferenceFailure(
            context.index,
            context.correlation,
            error,
            event.partial_rows,
            event.replay_disposition,
        )


def _reference_variant(outcome: ReferenceOutcome[object]) -> str:
    if isinstance(outcome, ReferenceItem):
        return "item"
    if isinstance(outcome, ReferenceComplete):
        return "success"
    if isinstance(outcome, ReferenceFailure):
        return "failure"
    if isinstance(outcome, ReferenceNotExecuted):
        return "not_executed"
    if isinstance(outcome, ReferenceOutcomeUnknown):
        return "unknown"
    raise TypeError("reference stream emitted an unknown outcome")


def _reference_terminal(outcome: ReferenceOutcome[object]) -> bool:
    return not isinstance(outcome, ReferenceItem)


def _reference_error_items(
    error: BaseException,
    mapper: _ReferenceEventMapper,
) -> tuple[ReferenceOutcome[object], ...]:
    if isinstance(error, _ReferenceWindowError):
        return (mapper(error.failure),)
    return ()


def _reference_error(error: BaseException, report: OperationReport, mapper: _ReferenceEventMapper) -> BaseException:
    if isinstance(error, _ReferenceWindowError):
        return ReferenceFailed(_reference_error_items(error, mapper), report=report)
    if isinstance(error, _BindingSourceError):
        return error.report_cause
    return error


def kernel_reference_stream[C](
    executor: object,
    base: Request,
    bindings: BindingSource[C],
    *,
    traversal: TraversalSpec,
    dispatch: DispatchSpec,
    policy: ExecutionPolicy,
    tolerant: bool,
    audit: Callable[[Request], Violation | None] | None = None,
    page_stop: PageStopPolicy | None = None,
) -> ReferenceKernelStream:
    """Build the internal owned stream after all base controls are validated."""
    from b24api.execution import Executor  # noqa: PLC0415 - narrow internal composition import

    if not isinstance(executor, Executor):
        raise TypeError("executor must be an Executor")
    if isinstance(traversal, CountedTraversal) and isinstance(dispatch, DirectDispatch):
        raise CapabilityError("counted reference traversal requires BatchDispatch")
    if isinstance(traversal, CountedTraversal) and page_stop is not None:
        raise CapabilityError("counted physical batch tail does not support page stop")
    plan, selector, identity = _kernel_plan(traversal)
    preflight = PaginationDriver(
        executor,
        base,
        plan,
        selector=selector,
        identity=identity,
        context=executor.context(policy),
        page_adapter=traversal.page_adapter,
    )
    preflight._validate_capabilities()  # noqa: SLF001 - reject base controls before consuming caller input
    executor._preflight_request(base)  # noqa: SLF001 - reject transport representation before caller input
    dispatch_plan = kernel_dispatch(dispatch, policy)
    stream = _iter_references(
        executor,
        binding_source(base, bindings, traversal, audit),
        plan=plan,
        dispatch=dispatch_plan,
        selector=selector,
        identity=identity,
        output_order=dispatch_plan.output_order,
        tolerant=tolerant,
        policy=policy,
        _emit_complete=True,
        _capture_fail_fast=not tolerant,
        _page_cap_hint=traversal.page_size,
        _page_adapter=traversal.page_adapter,
        _page_stop=page_stop,
    )
    return cast("ReferenceKernelStream", stream)


def reference_stream[C](
    executor: object,
    request: RequestLike,
    bindings: BindingSource[C],
    *,
    traversal: TraversalSpec,
    dispatch: DispatchSpec,
    policy: ExecutionPolicy,
    tolerant: bool,
    audit: Callable[[Request], Violation | None] | None = None,
    page_stop: PageStopPolicy | None = None,
    deregister: Deregister,
) -> OperationStream[ReferenceOutcome[C]]:
    """Compose the public bound-reference stream over the scheduler kernel."""
    base = canonical_request(request)
    if not isinstance(dispatch, DirectDispatch) and (base.encoding.value != "json" or base.headers.items):
        raise CapabilityError("physical batch supports JSON requests without scoped headers; use direct dispatch")
    source = kernel_reference_stream(
        executor,
        base,
        bindings,
        traversal=traversal,
        dispatch=dispatch,
        policy=policy,
        tolerant=tolerant,
        audit=audit,
        page_stop=page_stop,
    )
    mapper = _ReferenceEventMapper()
    stream = MappedOperationStream(
        source,
        mapper,
        operation="iter_reference_outcomes" if tolerant else "iter_references",
        classify=_reference_variant,
        error_mapper=lambda error, report: _reference_error(error, report, mapper),
        error_items=lambda error: _reference_error_items(error, mapper),
        count_admitted=_reference_terminal,
        source_active_references=lambda: source.active_references_high_water,
        deregister=deregister,
    )
    return cast("OperationStream[ReferenceOutcome[C]]", stream)


__all__ = [
    "BindingSource",
    "KernelReferenceEvent",
    "ReferenceKernelStream",
    "kernel_reference_stream",
    "reference_stream",
]
