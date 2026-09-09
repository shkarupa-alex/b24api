"""Public list-strategy composition over internal traversal plans."""

from __future__ import annotations
from collections.abc import Callable
from typing import TYPE_CHECKING, Literal, cast

from b24api._stream import MappedOperationStream, _ClosableIterator
from b24api.batch.facade import resolve_batch_size
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
    TraversalIdentity,
    canonical_request,
)
from b24api.contracts.response import ResultCollectionShape
from b24api.contracts.traversal import OffsetContinuation, TotalTermination
from b24api.contracts.wire import BodyEncoding
from b24api.errors import CapabilityError
from b24api.traversal.counted import CountedItemStream
from b24api.traversal.plans import (
    CountedOffsetMode,
    CountedOffsetPlan,
    CursorTerminalRule,
    ItemCursorPlan,
    KeysetPlan,
    KeysetTerminalRule,
    OffsetSequentialPlan,
    OffsetTerminalRule,
)
from b24api.traversal.stream import iter_list as _iter_list
from b24api.traversal.values import _MappingValuesResultSelector, _TolerantMappingValuesResultSelector

if TYPE_CHECKING:
    from b24api.contracts.json import JsonValue
    from b24api.contracts.stream import OperationStream
    from b24api.contracts.traversal import CursorSpec, KeysetSpec, OffsetSpec
    from b24api.execution.executor import Executor

type Deregister = Callable[[object], None]


def _direction(value: str) -> Literal["asc", "desc"]:
    return "asc" if value == "ascending" else "desc"


def _collection_selector(selector: ResultSelector, shape: ResultCollectionShape) -> ResultSelector:
    if not isinstance(shape, ResultCollectionShape):
        raise TypeError("collection_shape must be a ResultCollectionShape")
    if shape is ResultCollectionShape.SEQUENCE:
        return selector
    if shape is ResultCollectionShape.MAPPING_VALUES_OR_EMPTY:
        return _TolerantMappingValuesResultSelector(selector.path)
    return _MappingValuesResultSelector(selector.path)


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
    policy: ExecutionPolicy,
    deregister: Deregister,
    audit_violations: tuple[Violation, ...] = (),
) -> OperationStream[JsonValue]:
    """Compose conservative sequential offset/server-next traversal."""
    if offset.continuation is OffsetContinuation.FIXED_STEP and offset.step != page_size:
        raise ValueError("fixed-step traversal requires page_size equal to step")
    plan = OffsetSequentialPlan(
        offset_path=offset.parameter_path,
        limit_path=offset.limit_path,
        requested_page_size=page_size if offset.limit_path is not None else None,
        continuation=offset.continuation,
        fixed_step=offset.step,
        terminal=(
            frozenset({OffsetTerminalRule.EMPTY_PAGE})
            if offset.total_termination is TotalTermination.DISABLED
            else frozenset({OffsetTerminalRule.EMPTY_PAGE, OffsetTerminalRule.QUALIFIED_TOTAL})
        ),
        allow_create_controls=offset.allow_create_controls,
        identity_requirement=IdentityRequirement.OPTIONAL,
        duplicate_policy=DuplicatePolicy.ERROR,
        total_semantics=(
            TotalSemantics.IGNORE
            if offset.total_termination is TotalTermination.DISABLED
            else TotalSemantics.FILTERED_EXACT
        ),
    )
    if offset.total_termination is TotalTermination.EXACT_QUALIFIED:
        assurance = (
            TraversalAssurance.IDENTITY_AND_COUNT_MATCHED if identity is not None else TraversalAssurance.COUNT_MATCHED
        )
    else:
        assurance = TraversalAssurance.IDENTITY_EXACT if identity is not None else TraversalAssurance.MECHANICS_ONLY
    return _plan_stream(
        executor,
        request,
        plan=plan,
        selector=selector,
        identity=identity,
        collection_shape=collection_shape,
        page_size=page_size,
        policy=policy,
        operation="iter_list",
        assurance=assurance,
        deregister=deregister,
        audit_violations=audit_violations,
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
    policy: ExecutionPolicy,
    deregister: Deregister,
    audit_violations: tuple[Violation, ...] = (),
) -> OperationStream[JsonValue]:
    """Compose exact sequential no-count keyset traversal."""
    direction = _direction(keyset.direction)
    plan = KeysetPlan(
        direction=direction,
        filter_path=keyset.filter_path,
        order_path=keyset.order_path,
        split_order=keyset.split_order,
        start_suppression_path=keyset.start_suppression_path,
        limit_path=keyset.limit_path,
        requested_page_size=page_size if keyset.limit_path is not None else None,
        terminal=KeysetTerminalRule.EMPTY_CONFIRMATION,
        allow_create_controls=keyset.allow_create_controls,
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING if direction == "asc" else OrderSemantics.DESCENDING,
        duplicate_policy=DuplicatePolicy.ERROR,
        total_semantics=TotalSemantics.IGNORE,
    )
    return _plan_stream(
        executor,
        request,
        plan=plan,
        selector=selector,
        identity=identity,
        collection_shape=collection_shape,
        page_size=page_size,
        policy=policy,
        operation="iter_list_keyset",
        assurance=TraversalAssurance.IDENTITY_EXACT,
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
    policy: ExecutionPolicy,
    deregister: Deregister,
    audit_violations: tuple[Violation, ...] = (),
) -> OperationStream[JsonValue]:
    """Compose exact direct-head plus physically batched counted traversal."""
    if not isinstance(page_size, int) or isinstance(page_size, bool) or page_size < 1:
        raise ValueError("page_size must be a positive integer")
    if offset.total_termination is not TotalTermination.EXACT_QUALIFIED:
        raise ValueError("counted traversal requires exact-qualified total termination")
    if offset.continuation is OffsetContinuation.FIXED_STEP and offset.step != page_size:
        raise ValueError("fixed-step traversal requires page_size equal to step")
    canonical = canonical_request(request)
    if canonical.encoding is not BodyEncoding.JSON or canonical.headers.items:
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
    policy: ExecutionPolicy,
    operation: str,
    assurance: TraversalAssurance,
    deregister: Deregister,
    audit_violations: tuple[Violation, ...] = (),
) -> OperationStream[JsonValue]:
    source = _iter_list(
        executor,
        canonical_request(request),
        plan=plan,
        selector=_collection_selector(selector, collection_shape),
        identity=identity,
        policy=policy,
        _page_cap_hint=page_size,
    )
    return _mapped_stream(
        source,
        operation=operation,
        assurance=assurance,
        deregister=deregister,
        audit_violations=audit_violations,
    )


__all__ = ["counted_stream", "cursor_stream", "keyset_stream", "sequential_stream"]
