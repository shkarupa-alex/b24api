"""Public list-strategy composition over internal traversal plans."""

from __future__ import annotations
from collections.abc import Callable
from typing import TYPE_CHECKING, cast

from b24api._stream import MappedOperationStream, _ClosableIterator
from b24api.batch.engine import BatchExecutor
from b24api.batch.facade import resolve_batch_size
from b24api.contracts.keyset_execution import (
    AutoKeysetExecution,
    KeysetExecution,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    SequentialKeysetExecution,
)
from b24api.contracts.policy import (
    DuplicatePolicy,
    ExecutionPolicy,
    IdentityRequirement,
    OrderSemantics,
    TotalSemantics,
)
from b24api.contracts.report import TraversalAssurance, Violation
from b24api.contracts.request import (
    IdentitySpec,
    RequestLike,
    ResultSelector,
    RouteKind,
    TraversalIdentity,
    canonical_request,
)
from b24api.contracts.traversal import OffsetContinuation, TotalTermination
from b24api.contracts.wire import BodyEncoding
from b24api.errors import CapabilityError
from b24api.traversal.counted import CountedItemStream
from b24api.traversal.facade_support import _checked_identity_store, _collection_selector, _direction
from b24api.traversal.keyset_eligibility import validate_fast_keyset
from b24api.traversal.keyset_fast_stream import FastTraceRecorder, KeysetFastStream
from b24api.traversal.keyset_scheduler import KeysetFastScheduler
from b24api.traversal.keyset_step import sequential_keyset_plan
from b24api.traversal.offset_rules import sequential_offset_plan
from b24api.traversal.plans import (
    CountedOffsetMode,
    CountedOffsetPlan,
    CursorTerminalRule,
    ItemCursorPlan,
    KeysetPlan,
    OffsetSequentialPlan,
)
from b24api.traversal.stream import iter_list as _iter_list

if TYPE_CHECKING:
    from b24api.contracts.identity_store import IdentityStore
    from b24api.contracts.json import JsonValue
    from b24api.contracts.page import PageAdapter
    from b24api.contracts.page_stop import PageStopPolicy
    from b24api.contracts.response import ResultCollectionShape
    from b24api.contracts.stream import OperationStream
    from b24api.contracts.traversal import CursorSpec, KeysetSpec, OffsetSpec
    from b24api.execution.executor import Executor

type Deregister = Callable[[object], None]


def _mapped_stream(
    source: _ClosableIterator[JsonValue],
    *,
    operation: str,
    assurance: TraversalAssurance,
    deregister: Deregister,
    audit_violations: tuple[Violation, ...] = (),
) -> OperationStream[JsonValue]:
    stream: MappedOperationStream[JsonValue, JsonValue] = MappedOperationStream(
        source,
        lambda item: item,
        operation=operation,
        assurance=assurance,
        deregister=deregister,
        initial_violations=audit_violations,
    )
    return cast("OperationStream[JsonValue]", stream)


def sequential_stream(  # noqa: PLR0913
    executor: Executor,
    request: RequestLike,
    *,
    selector: ResultSelector,
    identity: TraversalIdentity | None,
    collection_shape: ResultCollectionShape,
    page_size: int,
    offset: OffsetSpec,
    page_adapter: PageAdapter,
    page_stop: PageStopPolicy | None,
    policy: ExecutionPolicy,
    deregister: Deregister,
    audit_violations: tuple[Violation, ...] = (),
    identity_store: IdentityStore | None = None,
) -> OperationStream[JsonValue]:
    """Compose conservative sequential offset/server-next traversal."""
    ledger = _checked_identity_store(identity_store, identity)
    plan = sequential_offset_plan(offset, page_size=page_size, duplicate_policy=DuplicatePolicy.REPORT)
    if offset.total_termination is TotalTermination.EXACT_QUALIFIED:
        assurance = (
            TraversalAssurance.IDENTITY_AND_COUNT_MATCHED if identity is not None else TraversalAssurance.COUNT_MATCHED
        )
    else:
        assurance = (
            TraversalAssurance.RAW_RANGE_COVERED
            if offset.sparse_raw_bound is not None
            else TraversalAssurance.IDENTITY_EXACT
            if identity is not None
            else TraversalAssurance.MECHANICS_ONLY
        )
    return _plan_stream(
        executor,
        request,
        plan=plan,
        selector=selector,
        identity=identity,
        collection_shape=collection_shape,
        page_size=page_size,
        page_adapter=page_adapter,
        page_stop=page_stop,
        policy=policy,
        operation="iter_list",
        assurance=assurance,
        deregister=deregister,
        audit_violations=audit_violations,
        identity_store=ledger,
    )


def keyset_stream(  # noqa: PLR0913
    executor: Executor,
    request: RequestLike,
    *,
    selector: ResultSelector,
    identity: IdentitySpec,
    collection_shape: ResultCollectionShape,
    page_size: int,
    keyset: KeysetSpec,
    execution: KeysetExecution,
    page_adapter: PageAdapter,
    page_stop: PageStopPolicy | None,
    policy: ExecutionPolicy,
    deregister: Deregister,
    audit_violations: tuple[Violation, ...] = (),
) -> OperationStream[JsonValue]:
    """Compose sequential or automatic/explicit fast no-count traversal."""
    if not isinstance(
        execution,
        SequentialKeysetExecution | RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution,
    ):
        raise TypeError("execution must be a supported KeysetExecution")
    if page_stop is not None and isinstance(execution, AutoKeysetExecution):
        execution = SequentialKeysetExecution()
    if page_stop is not None and not isinstance(execution, SequentialKeysetExecution):
        raise CapabilityError("page stop requires sequential keyset execution")
    if not isinstance(execution, SequentialKeysetExecution):
        canonical = canonical_request(request)
        effective_cap = validate_fast_keyset(
            executor,
            canonical,
            identity=identity,
            keyset=keyset,
            page_size=page_size,
            execution=execution,
            policy=policy,
        )
        context = executor.context(policy)
        trace = FastTraceRecorder(policy.page_trace_limit)
        scheduler = KeysetFastScheduler(
            executor=executor,
            request=canonical,
            identity=identity,
            keyset=keyset,
            selector=_collection_selector(selector, collection_shape),
            collection_shape=collection_shape,
            page_size=page_size,
            effective_page_cap=effective_cap,
            execution=execution,
            context=context,
            engine=BatchExecutor(executor),
            trace=trace,
            page_adapter=page_adapter,
        )
        return _mapped_stream(
            KeysetFastStream(scheduler),
            operation="iter_list_keyset",
            assurance=TraversalAssurance.IDENTITY_EXACT,
            deregister=deregister,
            audit_violations=audit_violations,
        )
    plan = sequential_keyset_plan(keyset, page_size)
    return _plan_stream(
        executor,
        request,
        plan=plan,
        selector=selector,
        identity=identity,
        collection_shape=collection_shape,
        page_size=page_size,
        page_adapter=page_adapter,
        page_stop=page_stop,
        policy=policy,
        operation="iter_list_keyset",
        assurance=(TraversalAssurance.BOUNDED_RANGE_OBSERVED if keyset.boundary else TraversalAssurance.IDENTITY_EXACT),
        deregister=deregister,
        audit_violations=audit_violations,
    )


def counted_stream(  # noqa: PLR0913
    executor: Executor,
    request: RequestLike,
    *,
    identity: TraversalIdentity | None,
    selector: ResultSelector,
    collection_shape: ResultCollectionShape,
    page_size: int,
    batch_size: int | None,
    offset: OffsetSpec,
    page_adapter: PageAdapter,
    policy: ExecutionPolicy,
    deregister: Deregister,
    audit_violations: tuple[Violation, ...] = (),
    identity_store: IdentityStore | None = None,
) -> OperationStream[JsonValue]:
    """Compose exact direct-head plus physically batched counted traversal."""
    ledger = _checked_identity_store(identity_store, identity)
    if not isinstance(page_size, int) or isinstance(page_size, bool) or page_size < 1:
        raise ValueError("page_size must be a positive integer")
    if offset.page_index is not None:
        raise ValueError("counted physical batch does not support page_index")
    if offset.total_termination is not TotalTermination.EXACT_QUALIFIED:
        raise ValueError("counted traversal requires exact-qualified total termination")
    if offset.continuation is OffsetContinuation.FIXED_STEP and offset.step != page_size:
        raise ValueError("fixed-step traversal requires page_size equal to step")
    stride = offset.page_stride
    if stride is not None and stride.max_decoded_rows != page_size:
        raise ValueError("counted page_size must match page_stride max_decoded_rows")
    if stride is not None and stride.requested_wire_limit is not None:
        raise ValueError("counted traversal does not support page_stride requested_wire_limit")
    canonical = canonical_request(request)
    if (
        canonical.route is not RouteKind.BARE
        or canonical.encoding is not BodyEncoding.JSON
        or canonical.headers.items
        or canonical.positional is not None
    ):
        raise CapabilityError("counted traversal supports JSON requests without scoped headers")
    executor._preflight_request(canonical)  # noqa: SLF001 - operation-wide preflight before stream construction
    plan = CountedOffsetPlan(
        offset_path=offset.parameter_path,
        limit_path=offset.limit_path,
        requested_page_size=page_size if offset.limit_path is not None else None,
        allow_create_controls=offset.allow_create_controls,
        identity_requirement=IdentityRequirement.OPTIONAL if identity is None else IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.UNORDERED,
        duplicate_policy=DuplicatePolicy.ERROR,
        total_semantics=TotalSemantics.FILTERED_EXACT,
        continuation=offset.continuation,
        mode=(
            CountedOffsetMode.PARALLEL_FIXED_STRIDE
            if offset.continuation is OffsetContinuation.FIXED_STEP
            else CountedOffsetMode.SEQUENTIAL_NEXT
        ),
        fixed_stride=offset.step,
    )
    source = CountedItemStream(
        executor,
        canonical,
        plan=plan,
        selector=_collection_selector(selector, collection_shape),
        identity=identity,
        page_size=page_size,
        batch_size=resolve_batch_size(batch_size, policy),
        policy=policy,
        page_adapter=page_adapter,
        identity_store=ledger,
    )
    return _mapped_stream(
        source,
        operation="iter_list_counted",
        assurance=(
            TraversalAssurance.IDENTITY_AND_COUNT_MATCHED if identity is not None else TraversalAssurance.COUNT_MATCHED
        ),
        deregister=deregister,
        audit_violations=audit_violations,
    )


def cursor_stream(  # noqa: PLR0913
    executor: Executor,
    request: RequestLike,
    *,
    selector: ResultSelector,
    cursor: CursorSpec,
    identity: IdentitySpec | None,
    collection_shape: ResultCollectionShape,
    page_size: int,
    page_adapter: PageAdapter,
    page_stop: PageStopPolicy | None,
    policy: ExecutionPolicy,
    deregister: Deregister,
    audit_violations: tuple[Violation, ...] = (),
) -> OperationStream[JsonValue]:
    """Compose strict dependent cursor traversal with empty confirmation."""
    cursor_identity = identity or IdentitySpec(
        item_path=cursor.item_path,
        filter_key="cursor",
        order_key="cursor",
        coercion=cursor.coercion,
    )
    direction = _direction(cursor.direction)
    plan = ItemCursorPlan(
        cursor_request_path=cursor.parameter_path,
        cursor_item_path=cursor.item_path,
        cursor_coercion=cursor.coercion,
        direction=direction,
        cursor_take=cursor.take,
        limit_path=cursor.limit_path,
        requested_page_size=page_size if cursor.limit_path is not None else None,
        terminal=CursorTerminalRule.EMPTY_CONFIRMATION,
        allow_create_controls=cursor.allow_create_controls,
        domain=cursor.domain,
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING if direction == "asc" else OrderSemantics.DESCENDING,
        duplicate_policy=DuplicatePolicy.ERROR,
    )
    return _plan_stream(
        executor,
        request,
        plan=plan,
        selector=selector,
        identity=cursor_identity,
        collection_shape=collection_shape,
        page_size=page_size,
        page_adapter=page_adapter,
        page_stop=page_stop,
        policy=policy,
        operation="iter_list_cursor",
        assurance=TraversalAssurance.IDENTITY_EXACT,
        deregister=deregister,
        audit_violations=audit_violations,
    )


def _plan_stream(  # noqa: PLR0913
    executor: Executor,
    request: RequestLike,
    *,
    plan: OffsetSequentialPlan | KeysetPlan | ItemCursorPlan,
    selector: ResultSelector,
    identity: TraversalIdentity | None,
    collection_shape: ResultCollectionShape,
    page_size: int,
    page_adapter: PageAdapter,
    page_stop: PageStopPolicy | None,
    policy: ExecutionPolicy,
    operation: str,
    assurance: TraversalAssurance,
    deregister: Deregister,
    audit_violations: tuple[Violation, ...] = (),
    identity_store: IdentityStore | None = None,
) -> OperationStream[JsonValue]:
    source = _iter_list(
        executor,
        canonical_request(request),
        plan=plan,
        selector=_collection_selector(selector, collection_shape),
        identity=identity,
        policy=policy,
        _page_cap_hint=page_size,
        _page_adapter=page_adapter,
        _page_stop=page_stop,
        _identity_store=identity_store,
    )
    return _mapped_stream(
        source,
        operation=operation,
        assurance=assurance,
        deregister=deregister,
        audit_violations=audit_violations,
    )


__all__ = ["counted_stream", "cursor_stream", "keyset_stream", "sequential_stream"]
