"""Thin v2 Bitrix24 composition facade."""

# ruff: noqa: SLF001 - class-owned alternate construction and kernel composition

from __future__ import annotations
import weakref
from collections.abc import Mapping
from typing import Literal, Self, cast

from b24api._stream import MappedOperationStream
from b24api.contracts import (
    CursorSpec,
    ExecutionPolicy,
    IdentitySpec,
    JsonValue,
    KeysetSpec,
    OffsetSpec,
    OperationStream,
    ReplaySafety,
    Request,
    Response,
    ResultCollectionShape,
    ResultSelector,
    TraversalAssurance,
)
from b24api.execution import Executor, HttpxTransport, Transport
from b24api.models import DuplicatePolicy, IdentityRequirement, OrderSemantics, TotalSemantics
from b24api.pagination import _MappingValuesResultSelector
from b24api.pagination import iter_list as _iter_list
from b24api.plans import (
    CursorTerminalRule,
    ItemCursorPlan,
    KeysetPlan,
    KeysetTerminalRule,
    OffsetContinuation,
    OffsetSequentialPlan,
    OffsetTerminalRule,
)
from b24api.settings import Settings, api_settings

type RequestSpec = Mapping[str, object]
type RequestLike = Request | RequestSpec
type _PublicStream = MappedOperationStream[object, object]
_ROOT_SELECTOR = ResultSelector.root()
_DEFAULT_OFFSET = OffsetSpec()
_DEFAULT_KEYSET = KeysetSpec()


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
        self._default_policy = policy or ExecutionPolicy()
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

    def _discard_stream(self, stream: object) -> None:
        self._streams.discard(cast("_PublicStream", stream))

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("client is closed")


__all__ = ["Bitrix24"]
