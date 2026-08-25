"""Canonical bound-reference composition over the proven bounded scheduler."""

# ruff: noqa: PLR0913, TRY301 - bounded orchestration adapter

from __future__ import annotations
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol, Self, cast, runtime_checkable

from b24api.contracts.dispatch import DeliveryOrder, DirectDispatch, DispatchSpec
from b24api.contracts.policy import (
    DuplicatePolicy,
    ExecutionPolicy,
    IdentityRequirement,
    OrderSemantics,
    TotalSemantics,
)
from b24api.contracts.reference import (
    Binding,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ReferenceNotExecuted,
    ReferenceOutcome,
    ReferenceOutcomeUnknown,
)
from b24api.contracts.request import (
    IdentitySpec,
    ParameterPath,
    Request,
    ResultSelector,
)
from b24api.contracts.traversal import (
    CountedTraversal,
    KeysetTraversal,
    SequentialTraversal,
    TraversalSpec,
)
from b24api.errors import AmbiguousExecutionError, B24ApiError, CapabilityError, InputSourceError, ReferenceFailed
from b24api.plans import (
    BatchDispatch as KernelBatchDispatch,
)
from b24api.plans import (
    CountedOffsetPlan,
    CursorTerminalRule,
    DispatchPlan,
    ItemCursorPlan,
    KeysetPlan,
    KeysetTerminalRule,
    ListPlan,
    OffsetContinuation,
    OffsetSequentialPlan,
    OffsetTerminalRule,
    ReferenceOutputOrder,
)
from b24api.plans import (
    DirectDispatch as KernelDirectDispatch,
)
from b24api.references.dispatch import (
    _KernelReferenceComplete,
    _ReferenceWindowError,
)
from b24api.references.outcome import ReferenceFailure as KernelFailure
from b24api.references.outcome import ReferenceItem as KernelItem
from b24api.references.outcome import ReferenceRequest
from b24api.references.stream import (
    iter_references as _iter_references,
)

if TYPE_CHECKING:
    from b24api.contracts.json import JsonValue
    from b24api.execution.snapshot import KernelReport

type BindingSource[C] = Iterable[Binding[C]] | AsyncIterable[Binding[C]]
type KernelReferenceEvent = KernelItem | KernelFailure | _KernelReferenceComplete


class ReferenceKernelStream(AsyncIterator[KernelReferenceEvent], Protocol):
    """Narrow structural view of the reference scheduler output."""

    report: KernelReport
    active_references_high_water: int

    async def aclose(self) -> None:
        """Close scheduler-owned resources."""
        ...


@runtime_checkable
class _SyncClosable(Protocol):
    def close(self) -> None:
        """Close a synchronous iterator."""
        ...


@runtime_checkable
class _AsyncClosable(Protocol):
    async def aclose(self) -> None:
        """Close an asynchronous iterator."""
        ...


@dataclass(frozen=True, slots=True)
class _BindingContext:
    index: int
    correlation: object


class _BindingSourceError(Exception):
    pass


def _normalized(path: ParameterPath) -> tuple[str | int, ...]:
    return tuple(part.casefold() if isinstance(part, str) else part for part in path.path)


def _overlaps(left: tuple[str | int, ...], right: tuple[str | int, ...]) -> bool:
    shared = min(len(left), len(right))
    return left[:shared] == right[:shared]


def _control_paths(traversal: TraversalSpec) -> tuple[ParameterPath, ...]:
    if isinstance(traversal, SequentialTraversal | CountedTraversal):
        return tuple(
            path for path in (traversal.offset.parameter_path, traversal.offset.limit_path) if path is not None
        )
    if isinstance(traversal, KeysetTraversal):
        keyset_mechanics = traversal.keyset
        return tuple(
            path
            for path in (
                keyset_mechanics.filter_path,
                keyset_mechanics.order_path,
                keyset_mechanics.start_suppression_path,
                keyset_mechanics.limit_path,
            )
            if path is not None
        )
    cursor_mechanics = traversal.cursor
    return tuple(path for path in (cursor_mechanics.parameter_path, cursor_mechanics.limit_path) if path is not None)


def _validate_binding_controls(binding: Binding[object], traversal: TraversalSpec) -> None:
    controls = tuple(_normalized(path) for path in _control_paths(traversal))
    for update in binding.updates:
        update_path = _normalized(update.path)
        if any(_overlaps(update_path, control) for control in controls):
            raise ValueError("binding update collides with a traversal control path")


def _matching_key(mapping: dict[str, JsonValue], requested: str) -> str | None:
    matches = [key for key in mapping if key.casefold() == requested.casefold()]
    if len(matches) > 1:
        raise ValueError(f"ambiguous case-insensitive binding key: {requested}")
    return matches[0] if matches else None


def _replace_path(root: dict[str, JsonValue], path: tuple[str | int, ...], value: JsonValue) -> None:
    current: JsonValue = root
    for part in path[:-1]:
        if isinstance(part, str):
            if not isinstance(current, dict):
                raise TypeError("binding path traverses a non-mapping value")
            actual = _matching_key(current, part)
            if actual is None:
                current[part] = {}
                actual = part
            current = current[actual]
            continue
        if not isinstance(current, list) or part >= len(current):
            raise KeyError(f"missing binding list index: {part}")
        current = current[part]
    final = path[-1]
    if isinstance(final, str):
        if not isinstance(current, dict):
            raise TypeError("binding path terminates in a non-mapping value")
        actual = _matching_key(current, final)
        current[final if actual is None else actual] = value
        return
    if not isinstance(current, list) or final >= len(current):
        raise KeyError(f"missing binding list index: {final}")
    current[final] = value


def _bind_request(base: Request, binding: Binding[object], index: int, traversal: TraversalSpec) -> ReferenceRequest:
    _validate_binding_controls(binding, traversal)
    parameters = base.copy_parameters()
    for update in binding.updates:
        _replace_path(parameters, update.path.path, update.value)
    return ReferenceRequest(
        Request(base.method, parameters, base.replay_safety),
        f"r{index:012d}",
        _BindingContext(index, binding.correlation),
    )


class _SyncBindingAdapter[C](Iterator[ReferenceRequest]):
    def __init__(self, base: Request, source: Iterable[Binding[C]], traversal: TraversalSpec) -> None:
        self._base = base
        self._iterator = iter(source)
        self._traversal = traversal
        self._index = 0

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> ReferenceRequest:
        try:
            binding = next(self._iterator)
            if not isinstance(binding, Binding):
                raise TypeError("reference source must yield Binding values")
            request = _bind_request(self._base, cast("Binding[object]", binding), self._index, self._traversal)
        except StopIteration:
            raise
        except Exception as error:
            raise _BindingSourceError from error
        self._index += 1
        return request

    def close(self) -> None:
        if isinstance(self._iterator, _SyncClosable):
            self._iterator.close()


class _AsyncBindingAdapter[C](AsyncIterator[ReferenceRequest]):
    def __init__(self, base: Request, source: AsyncIterable[Binding[C]], traversal: TraversalSpec) -> None:
        self._base = base
        self._iterator = aiter(source)
        self._traversal = traversal
        self._index = 0

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> ReferenceRequest:
        try:
            binding = await anext(self._iterator)
            if not isinstance(binding, Binding):
                raise TypeError("reference source must yield Binding values")
            request = _bind_request(self._base, cast("Binding[object]", binding), self._index, self._traversal)
        except StopAsyncIteration:
            raise
        except Exception as error:
            raise _BindingSourceError from error
        self._index += 1
        return request

    async def aclose(self) -> None:
        if isinstance(self._iterator, _AsyncClosable):
            await self._iterator.aclose()


def _binding_source[C](
    base: Request,
    source: BindingSource[C],
    traversal: TraversalSpec,
) -> Iterable[ReferenceRequest] | AsyncIterable[ReferenceRequest]:
    if isinstance(source, AsyncIterable):
        return _AsyncBindingAdapter(base, source, traversal)
    return _SyncBindingAdapter(base, source, traversal)


def _direction(value: str) -> Literal["asc", "desc"]:
    return "asc" if value == "ascending" else "desc"


def _kernel_plan(traversal: TraversalSpec) -> tuple[ListPlan, ResultSelector, IdentitySpec | None]:
    if isinstance(traversal, SequentialTraversal):
        offset_mechanics = traversal.offset
        return (
            OffsetSequentialPlan(
                offset_path=offset_mechanics.parameter_path,
                limit_path=offset_mechanics.limit_path,
                requested_page_size=traversal.page_size if offset_mechanics.limit_path is not None else None,
                continuation=OffsetContinuation.SERVER_NEXT_OR_OBSERVED_COUNT,
                terminal=frozenset({OffsetTerminalRule.EMPTY_PAGE}),
                allow_create_controls=offset_mechanics.allow_create_controls,
                identity_requirement=IdentityRequirement.OPTIONAL,
                duplicate_policy=DuplicatePolicy.ERROR,
                total_semantics=TotalSemantics.IGNORE,
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
                identity_requirement=IdentityRequirement.REQUIRED,
                duplicate_policy=DuplicatePolicy.ERROR,
                total_semantics=TotalSemantics.FILTERED_EXACT,
            ),
            traversal.selector,
            traversal.identity,
        )
    if isinstance(traversal, KeysetTraversal):
        keyset_mechanics = traversal.keyset
        direction = _direction(keyset_mechanics.direction)
        return (
            KeysetPlan(
                direction=direction,
                filter_path=keyset_mechanics.filter_path,
                order_path=keyset_mechanics.order_path,
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
            identity_requirement=IdentityRequirement.REQUIRED,
            order_semantics=OrderSemantics.ASCENDING if direction == "asc" else OrderSemantics.DESCENDING,
            duplicate_policy=DuplicatePolicy.ERROR,
        ),
        traversal.selector,
        identity,
    )


def _output_order(value: DeliveryOrder) -> ReferenceOutputOrder:
    return ReferenceOutputOrder.READY if value is DeliveryOrder.READY else ReferenceOutputOrder.INPUT


def _kernel_dispatch(dispatch: DispatchSpec, policy: ExecutionPolicy) -> DispatchPlan:
    if isinstance(dispatch, DirectDispatch):
        return KernelDirectDispatch(
            concurrency=min(dispatch.concurrency, policy.max_direct_concurrency, policy.max_active_references),
            output_order=_output_order(dispatch.output_order),
        )
    return KernelBatchDispatch(
        batch_size=min(dispatch.batch_size, policy.max_buffered_commands),
        concurrency=min(dispatch.concurrency, policy.max_active_references),
        output_order=_output_order(dispatch.output_order),
    )


class _ReferenceEventMapper:
    """Stateful bounded converter assigning per-binding item indexes."""

    def __init__(self) -> None:
        """Initialize bounded per-active-binding row positions."""
        self._item_indexes: dict[int, int] = {}

    def __call__(self, event: KernelReferenceEvent) -> ReferenceOutcome[object]:
        """Convert one kernel event without interpreting correlation."""
        if isinstance(event, KernelItem):
            context = cast("_BindingContext", event.payload)
            item_index = self._item_indexes.get(context.index, 0)
            self._item_indexes[context.index] = item_index + 1
            return ReferenceItem(context.index, context.correlation, item_index, event.item)
        if isinstance(event, _KernelReferenceComplete):
            context = cast("_BindingContext", event.reference.payload)
            self._item_indexes.pop(context.index, None)
            return ReferenceComplete(context.index, context.correlation, event.row_count)
        context = cast("_BindingContext", event.payload)
        self._item_indexes.pop(context.index, None)
        error = event.error if isinstance(event.error, B24ApiError) else CapabilityError("reference failed")
        if isinstance(error, AmbiguousExecutionError):
            return ReferenceOutcomeUnknown(context.index, context.correlation, error, event.partial_rows)
        return ReferenceFailure(context.index, context.correlation, error, event.partial_rows)


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


def _reference_error(error: BaseException, report: object, mapper: _ReferenceEventMapper) -> BaseException:
    if isinstance(error, _ReferenceWindowError):
        return ReferenceFailed(_reference_error_items(error, mapper), report=report)
    if isinstance(error, _BindingSourceError):
        return InputSourceError("Reference input source failed")
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
) -> ReferenceKernelStream:
    """Build the internal owned stream after all base controls are validated."""
    from b24api.execution import Executor  # noqa: PLC0415 - narrow internal composition import

    if not isinstance(executor, Executor):
        raise TypeError("executor must be an Executor")
    plan, selector, identity = _kernel_plan(traversal)
    kernel_dispatch = _kernel_dispatch(dispatch, policy)
    stream = _iter_references(
        executor,
        _binding_source(base, bindings, traversal),
        plan=plan,
        dispatch=kernel_dispatch,
        selector=selector,
        identity=identity,
        output_order=kernel_dispatch.output_order,
        tolerant=tolerant,
        policy=policy,
        _emit_complete=True,
        _capture_fail_fast=not tolerant,
    )
    return cast("ReferenceKernelStream", stream)


__all__ = [
    "BindingSource",
    "KernelReferenceEvent",
    "ReferenceKernelStream",
    "_ReferenceEventMapper",
    "_reference_error",
    "_reference_error_items",
    "_reference_terminal",
    "_reference_variant",
    "kernel_reference_stream",
]
