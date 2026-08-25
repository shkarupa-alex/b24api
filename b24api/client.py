"""Thin v2 Bitrix24 composition facade."""

# ruff: noqa: SLF001 - class-owned alternate construction and kernel composition

from __future__ import annotations
import weakref
from collections.abc import AsyncIterable, Iterable, Mapping
from typing import Literal, Self, cast

from b24api._stream import MappedOperationStream
from b24api.batch.logical import LogicalBatchKernelStream, _BatchWindowError
from b24api.contracts import (
    BatchDispatch,
    Command,
    CommandFailure,
    CommandNotExecuted,
    CommandOutcome,
    CommandOutcomeUnknown,
    CommandSuccess,
    CursorSpec,
    DirectDispatch,
    DispatchSpec,
    ExecutionPolicy,
    IdentitySpec,
    JsonValue,
    KeysetSpec,
    OffsetSpec,
    OperationStream,
    ReferenceEvent,
    ReferenceOutcome,
    ReplaySafety,
    Request,
    Response,
    ResultCollectionShape,
    ResultSelector,
    TraversalAssurance,
    TraversalSpec,
)
from b24api.contracts.policy import DuplicatePolicy, IdentityRequirement, OrderSemantics, TotalSemantics
from b24api.error import BatchFailed, InputSourceError
from b24api.execution import Executor, HttpxTransport, Transport
from b24api.fanout_v2 import (
    CommandSource as FanOutCommandSource,
)
from b24api.fanout_v2 import (
    _fanout_error,
    _fanout_error_items,
    _fanout_variant,
    _FanOutMapper,
    kernel_fanout_stream,
)
from b24api.plans import (
    CountedOffsetPlan,
    CursorTerminalRule,
    ItemCursorPlan,
    KeysetPlan,
    KeysetTerminalRule,
    OffsetContinuation,
    OffsetSequentialPlan,
    OffsetTerminalRule,
)
from b24api.reference_v2 import (
    BindingSource,
    _reference_error,
    _reference_error_items,
    _reference_terminal,
    _reference_variant,
    _ReferenceEventMapper,
    kernel_reference_stream,
)
from b24api.settings import Settings, api_settings
from b24api.traversal import CountedItemStream, _MappingValuesResultSelector
from b24api.traversal import iter_list as _iter_list

type RequestSpec = Mapping[str, object]
type RequestLike = Request | RequestSpec
type _PublicStream = MappedOperationStream[object, object]
_ROOT_SELECTOR = ResultSelector.root()
_DEFAULT_OFFSET = OffsetSpec()
_DEFAULT_KEYSET = KeysetSpec()
_DEFAULT_REFERENCE_DISPATCH = BatchDispatch()
_DEFAULT_FANOUT_DISPATCH = DirectDispatch()


def _canonical_request(raw: RequestLike) -> Request:
    if isinstance(raw, Request):
        return raw
    if not isinstance(raw, Mapping):
        raise TypeError("request must be a Request or closed request mapping")
    unknown = set(raw) - {"method", "parameters", "replay_safety"}
    if unknown:
        raise ValueError(f"unknown request fields: {sorted(unknown)}")
    method = raw.get("method")
    parameters = raw.get("parameters")
    safety = raw.get("replay_safety", ReplaySafety.UNKNOWN)
    if not isinstance(method, str):
        raise TypeError("request mapping requires a string method")
    if parameters is not None and not isinstance(parameters, Mapping):
        raise TypeError("request mapping parameters must be a mapping")
    if isinstance(safety, str):
        try:
            safety = ReplaySafety(safety)
        except ValueError as error:
            raise ValueError("request replay_safety is invalid") from error
    if not isinstance(safety, ReplaySafety):
        raise TypeError("request replay_safety must be a ReplaySafety")
    return Request(method, parameters, replay_safety=safety)


def _normalized_host(host: str) -> str:
    return host.strip().rstrip(".").casefold()


def _direction(value: str) -> Literal["asc", "desc"]:
    return "asc" if value == "ascending" else "desc"


def _collection_selector(selector: ResultSelector, shape: ResultCollectionShape) -> ResultSelector:
    if shape is ResultCollectionShape.SEQUENCE:
        return selector
    return _MappingValuesResultSelector(selector.path)


def _batch_outcome_variant(outcome: CommandOutcome[object]) -> str:
    if isinstance(outcome, CommandSuccess):
        return "success"
    if isinstance(outcome, CommandFailure):
        return "failure"
    if isinstance(outcome, CommandNotExecuted):
        return "not_executed"
    if isinstance(outcome, CommandOutcomeUnknown):
        return "unknown"
    raise TypeError("batch stream emitted an unknown command outcome")


def _require_command_success(outcome: CommandOutcome[object]) -> CommandSuccess[object]:
    if not isinstance(outcome, CommandSuccess):
        raise TypeError("fail-fast batch kernel emitted a negative outcome")
    return outcome


def _batch_error_items(error: BaseException) -> tuple[CommandOutcome[object], ...]:
    if isinstance(error, _BatchWindowError):
        return error.outcomes
    return ()


def _fail_fast_batch_error(error: BaseException, report: object) -> BaseException:
    if isinstance(error, _BatchWindowError):
        return BatchFailed(error.outcomes, report=report)
    if isinstance(error, InputSourceError):
        return BatchFailed((), report=report)
    return error


class Bitrix24:
    """Async method-agnostic client over one correctness and execution kernel."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        policy: ExecutionPolicy | None = None,
        transport: Transport | None = None,
    ) -> None:
        """Compose settings, transport ownership and default policy."""
        resolved = settings if settings is not None else api_settings()
        if not isinstance(resolved, Settings):
            raise TypeError("settings must be a Settings value")
        if policy is not None and not isinstance(policy, ExecutionPolicy):
            raise TypeError("policy must be an ExecutionPolicy")
        host = resolved.webhook_url.host
        if host is None:
            raise ValueError("webhook URL must contain a host")
        if transport is None:
            selected_transport: Transport = HttpxTransport(str(resolved.webhook_url))
            owned_transport: HttpxTransport | None = cast("HttpxTransport", selected_transport)
        else:
            if _normalized_host(transport.host) != _normalized_host(host):
                raise ValueError("injected transport host does not match Settings")
            selected_transport = transport
            owned_transport = None
        self._settings: Settings | None = resolved
        self._transport = selected_transport
        self._owned_transport = owned_transport
        self._executor = Executor(selected_transport)
        self._default_policy = policy or ExecutionPolicy(
            max_retry_elapsed_per_request=float(resolved.http_timeout),
        )
        self._host = host
        self._closed = False
        self._streams: weakref.WeakSet[_PublicStream] = weakref.WeakSet()

    @classmethod
    def _from_executor(
        cls,
        executor: Executor,
        *,
        policy: ExecutionPolicy | None = None,
        host: str = "test.invalid",
    ) -> Bitrix24:
        """Construct over an injected deterministic executor for tests."""
        instance = cls.__new__(cls)
        instance._settings = None
        instance._transport = executor.transport
        instance._owned_transport = None
        instance._executor = executor
        instance._default_policy = policy or ExecutionPolicy()
        instance._host = host
        instance._closed = False
        instance._streams = weakref.WeakSet()
        return instance

    @property
    def host(self) -> str:
        """Return the configured portal host without credentials."""
        return self._host

    async def __aenter__(self) -> Self:
        """Enter this long-lived client."""
        self._require_open()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Close streams before the owned transport."""
        await self.aclose()

    async def aclose(self) -> None:
        """Close active streams and then the owned transport idempotently."""
        if self._closed:
            return
        self._closed = True
        for stream in tuple(self._streams):
            await stream.aclose()
        if self._owned_transport is not None:
            await self._owned_transport.aclose()

    async def call(self, request: RequestLike, *, policy: ExecutionPolicy | None = None) -> JsonValue:
        """Execute one request and return detached decoded JSON."""
        return (await self.call_response(request, policy=policy)).result

    async def call_response(self, request: RequestLike, *, policy: ExecutionPolicy | None = None) -> Response:
        """Execute one request and return its immutable response envelope."""
        self._require_open()
        return await self._executor.execute(_canonical_request(request), policy=policy or self._default_policy)

    def batch[C](
        self,
        commands: Iterable[Command[C]] | AsyncIterable[Command[C]],
        *,
        batch_size: int | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[CommandSuccess[C]]:
        """Execute an arbitrary logical batch with bounded fail-fast windows."""
        effective_policy, effective_batch_size = self._batch_parameters(batch_size, policy)
        source = LogicalBatchKernelStream(
            self._executor,
            commands,
            batch_size=effective_batch_size,
            fail_fast=True,
            policy=effective_policy,
        )
        stream = MappedOperationStream(
            source,
            _require_command_success,
            operation="batch",
            classify=_batch_outcome_variant,
            error_mapper=_fail_fast_batch_error,
            error_items=_batch_error_items,
            deregister=self._discard_stream,
        )
        self._streams.add(cast("_PublicStream", stream))
        return cast("OperationStream[CommandSuccess[C]]", stream)

    def batch_outcomes[C](
        self,
        commands: Iterable[Command[C]] | AsyncIterable[Command[C]],
        *,
        batch_size: int | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[CommandOutcome[C]]:
        """Execute an arbitrary logical batch and retain every correlated state."""
        effective_policy, effective_batch_size = self._batch_parameters(batch_size, policy)
        source = LogicalBatchKernelStream(
            self._executor,
            commands,
            batch_size=effective_batch_size,
            fail_fast=False,
            policy=effective_policy,
        )
        stream = MappedOperationStream(
            source,
            lambda outcome: outcome,
            operation="batch_outcomes",
            classify=_batch_outcome_variant,
            error_items=_batch_error_items,
            deregister=self._discard_stream,
        )
        self._streams.add(cast("_PublicStream", stream))
        return cast("OperationStream[CommandOutcome[C]]", stream)

    def fan_out[C](
        self,
        commands: FanOutCommandSource[C],
        *,
        dispatch: DispatchSpec = _DEFAULT_FANOUT_DISPATCH,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[CommandSuccess[C]]:
        """Dispatch independent commands fail-fast with explicit delivery order."""
        return cast(
            "OperationStream[CommandSuccess[C]]",
            self._fanout_stream(commands, dispatch=dispatch, policy=policy, tolerant=False),
        )

    def fan_out_outcomes[C](
        self,
        commands: FanOutCommandSource[C],
        *,
        dispatch: DispatchSpec = _DEFAULT_FANOUT_DISPATCH,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[CommandOutcome[C]]:
        """Dispatch independent commands and retain every correlated terminal state."""
        return self._fanout_stream(commands, dispatch=dispatch, policy=policy, tolerant=True)

    def iter_references[C](
        self,
        request: RequestLike,
        bindings: BindingSource[C],
        *,
        traversal: TraversalSpec,
        dispatch: DispatchSpec = _DEFAULT_REFERENCE_DISPATCH,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[ReferenceEvent[C]]:
        """Traverse bound references fail-fast with explicit completion events."""
        return cast(
            "OperationStream[ReferenceEvent[C]]",
            self._reference_stream(
                request,
                bindings,
                traversal=traversal,
                dispatch=dispatch,
                policy=policy,
                tolerant=False,
            ),
        )

    def iter_reference_outcomes[C](
        self,
        request: RequestLike,
        bindings: BindingSource[C],
        *,
        traversal: TraversalSpec,
        dispatch: DispatchSpec = _DEFAULT_REFERENCE_DISPATCH,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[ReferenceOutcome[C]]:
        """Traverse bound references while retaining each correlated terminal state."""
        return self._reference_stream(
            request,
            bindings,
            traversal=traversal,
            dispatch=dispatch,
            policy=policy,
            tolerant=True,
        )

    def iter_list(  # noqa: PLR0913
        self,
        request: RequestLike,
        *,
        selector: ResultSelector = _ROOT_SELECTOR,
        identity: IdentitySpec | None = None,
        collection_shape: ResultCollectionShape = ResultCollectionShape.SEQUENCE,
        page_size: int = 50,
        offset: OffsetSpec = _DEFAULT_OFFSET,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[JsonValue]:
        """Return conservative sequential offset/server-next traversal."""
        plan = OffsetSequentialPlan(
            offset_path=offset.parameter_path,
            limit_path=offset.limit_path,
            requested_page_size=page_size if offset.limit_path is not None else None,
            continuation=OffsetContinuation.SERVER_NEXT_OR_OBSERVED_COUNT,
            terminal=frozenset({OffsetTerminalRule.EMPTY_PAGE}),
            allow_create_controls=offset.allow_create_controls,
            identity_requirement=IdentityRequirement.OPTIONAL,
            duplicate_policy=DuplicatePolicy.ERROR,
            total_semantics=TotalSemantics.IGNORE,
        )
        assurance = TraversalAssurance.IDENTITY_EXACT if identity is not None else TraversalAssurance.MECHANICS_ONLY
        return self._list_stream(
            request,
            plan=plan,
            selector=selector,
            identity=identity,
            collection_shape=collection_shape,
            page_size=page_size,
            policy=policy,
            operation="iter_list",
            assurance=assurance,
        )

    def iter_list_keyset(  # noqa: PLR0913
        self,
        request: RequestLike,
        *,
        selector: ResultSelector,
        identity: IdentitySpec,
        collection_shape: ResultCollectionShape = ResultCollectionShape.SEQUENCE,
        page_size: int = 50,
        keyset: KeysetSpec = _DEFAULT_KEYSET,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[JsonValue]:
        """Return exact sequential no-count keyset traversal."""
        direction = _direction(keyset.direction)
        plan = KeysetPlan(
            direction=direction,
            filter_path=keyset.filter_path,
            order_path=keyset.order_path,
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
        return self._list_stream(
            request,
            plan=plan,
            selector=selector,
            identity=identity,
            collection_shape=collection_shape,
            page_size=page_size,
            policy=policy,
            operation="iter_list_keyset",
            assurance=TraversalAssurance.IDENTITY_EXACT,
        )

    def iter_list_counted(  # noqa: PLR0913
        self,
        request: RequestLike,
        *,
        identity: IdentitySpec,
        selector: ResultSelector = _ROOT_SELECTOR,
        collection_shape: ResultCollectionShape = ResultCollectionShape.SEQUENCE,
        page_size: int = 50,
        batch_size: int | None = None,
        offset: OffsetSpec = _DEFAULT_OFFSET,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[JsonValue]:
        """Return exact direct-head plus physically batched counted traversal."""
        self._require_open()
        if not isinstance(identity, IdentitySpec):
            raise TypeError("identity must be an IdentitySpec")
        if not isinstance(page_size, int) or isinstance(page_size, bool) or page_size < 1:
            raise ValueError("page_size must be a positive integer")
        effective_policy = policy or self._default_policy
        effective_batch_size = min(50, effective_policy.max_buffered_commands) if batch_size is None else batch_size
        if (
            not isinstance(effective_batch_size, int)
            or isinstance(effective_batch_size, bool)
            or not 1 <= effective_batch_size <= min(50, effective_policy.max_buffered_commands)
        ):
            raise ValueError("batch_size must be within 1..50 and the command buffer ceiling")
        plan = CountedOffsetPlan(
            offset_path=offset.parameter_path,
            limit_path=offset.limit_path,
            requested_page_size=page_size if offset.limit_path is not None else None,
            allow_create_controls=offset.allow_create_controls,
            identity_requirement=IdentityRequirement.REQUIRED,
            order_semantics=OrderSemantics.UNORDERED,
            duplicate_policy=DuplicatePolicy.ERROR,
            total_semantics=TotalSemantics.FILTERED_EXACT,
        )
        source = CountedItemStream(
            self._executor,
            _canonical_request(request),
            plan=plan,
            selector=_collection_selector(selector, collection_shape),
            identity=identity,
            page_size=page_size,
            batch_size=effective_batch_size,
            policy=effective_policy,
        )
        stream = MappedOperationStream(
            source,
            lambda item: item,
            operation="iter_list_counted",
            assurance=TraversalAssurance.IDENTITY_EXACT,
            deregister=self._discard_stream,
        )
        self._streams.add(cast("_PublicStream", stream))
        return stream

    def iter_list_cursor(  # noqa: PLR0913
        self,
        request: RequestLike,
        *,
        selector: ResultSelector,
        cursor: CursorSpec,
        identity: IdentitySpec | None = None,
        collection_shape: ResultCollectionShape = ResultCollectionShape.SEQUENCE,
        page_size: int = 50,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[JsonValue]:
        """Return strict dependent cursor traversal with empty confirmation."""
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
        return self._list_stream(
            request,
            plan=plan,
            selector=selector,
            identity=cursor_identity,
            collection_shape=collection_shape,
            page_size=page_size,
            policy=policy,
            operation="iter_list_cursor",
            assurance=TraversalAssurance.IDENTITY_EXACT,
        )

    def _list_stream(  # noqa: PLR0913
        self,
        request: RequestLike,
        *,
        plan: OffsetSequentialPlan | KeysetPlan | ItemCursorPlan,
        selector: ResultSelector,
        identity: IdentitySpec | None,
        collection_shape: ResultCollectionShape,
        page_size: int,
        policy: ExecutionPolicy | None,
        operation: str,
        assurance: TraversalAssurance,
    ) -> OperationStream[JsonValue]:
        self._require_open()
        if not isinstance(collection_shape, ResultCollectionShape):
            raise TypeError("collection_shape must be a ResultCollectionShape")
        source = _iter_list(
            self._executor,
            _canonical_request(request),
            plan=plan,
            selector=_collection_selector(selector, collection_shape),
            identity=identity,
            policy=policy or self._default_policy,
            _page_cap_hint=page_size,
        )
        stream = MappedOperationStream(
            source,
            lambda item: item,
            operation=operation,
            assurance=assurance,
            deregister=self._discard_stream,
        )
        self._streams.add(cast("_PublicStream", stream))
        return stream

    def _batch_parameters(
        self,
        batch_size: int | None,
        policy: ExecutionPolicy | None,
    ) -> tuple[ExecutionPolicy, int]:
        self._require_open()
        effective_policy = policy or self._default_policy
        ceiling = min(50, effective_policy.max_buffered_commands)
        size = ceiling if batch_size is None else batch_size
        if not isinstance(size, int) or isinstance(size, bool) or not 1 <= size <= ceiling:
            raise ValueError("batch_size must be within 1..50 and the command buffer ceiling")
        return effective_policy, size

    def _reference_stream[C](  # noqa: PLR0913
        self,
        request: RequestLike,
        bindings: BindingSource[C],
        *,
        traversal: TraversalSpec,
        dispatch: DispatchSpec,
        policy: ExecutionPolicy | None,
        tolerant: bool,
    ) -> OperationStream[ReferenceOutcome[C]]:
        self._require_open()
        effective_policy = policy or self._default_policy
        source = kernel_reference_stream(
            self._executor,
            _canonical_request(request),
            bindings,
            traversal=traversal,
            dispatch=dispatch,
            policy=effective_policy,
            tolerant=tolerant,
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
            deregister=self._discard_stream,
        )
        self._streams.add(cast("_PublicStream", stream))
        return cast("OperationStream[ReferenceOutcome[C]]", stream)

    def _fanout_stream[C](
        self,
        commands: FanOutCommandSource[C],
        *,
        dispatch: DispatchSpec,
        policy: ExecutionPolicy | None,
        tolerant: bool,
    ) -> OperationStream[CommandOutcome[C]]:
        self._require_open()
        effective_policy = policy or self._default_policy
        source = kernel_fanout_stream(
            self._executor,
            commands,
            dispatch=dispatch,
            policy=effective_policy,
            tolerant=tolerant,
        )
        mapper = _FanOutMapper()
        item_mapper = (mapper) if tolerant else (lambda event: _require_command_success(mapper(event)))
        stream = MappedOperationStream(
            source,
            item_mapper,
            operation="fan_out_outcomes" if tolerant else "fan_out",
            classify=_fanout_variant,
            error_mapper=lambda error, report: _fanout_error(
                error,
                report,
                mapper,
                tolerant=tolerant,
            ),
            error_items=lambda error: _fanout_error_items(error, mapper),
            deregister=self._discard_stream,
        )
        self._streams.add(cast("_PublicStream", stream))
        return cast("OperationStream[CommandOutcome[C]]", stream)

    def _discard_stream(self, stream: object) -> None:
        self._streams.discard(cast("_PublicStream", stream))

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("client is closed")


__all__ = ["Bitrix24"]
