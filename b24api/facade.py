"""Canonical Bitrix24 facade and plan-driven compatibility wrappers."""

from __future__ import annotations
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Iterable, Iterator, Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, Literal, Self, cast, overload

from fast_depends import inject
from pydantic import BaseModel
from pydantic_core import to_jsonable_python

from b24api.batch import BatchExecutor, BatchInput, BatchStream
from b24api.entity import Request as LegacyRequest
from b24api.error import CapabilityError, IncompleteTraversalError
from b24api.execution import Executor, HttpxTransport
from b24api.models import (
    ExecutionPolicy,
    IdentityCoercion,
    IdentityRequirement,
    IdentitySpec,
    JsonValue,
    OrderSemantics,
    ParameterPath,
    ReferenceBinding,
    ReferenceFailure,
    ReferenceItem,
    ReferenceRequest,
    ReplaySafety,
    Request,
    Response,
    ResultSelector,
    RetryPolicy,
    TerminalState,
    TotalSemantics,
)
from b24api.pagination import ItemStream
from b24api.pagination import iter_list as iter_list_stream
from b24api.plans import (
    PORTAL_BATCH_CAP,
    BatchDispatch,
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
from b24api.references import ReferenceStream, iter_references
from b24api.references import fan_out as fan_out_stream
from b24api.settings import ApiSettings, Settings  # noqa: TC001 - runtime DI resolves this annotation

if TYPE_CHECKING:
    from collections.abc import Callable

type RequestMapping = Mapping[str, object]
type RequestLike = Request | LegacyRequest | RequestMapping
type RequestWithPayload = tuple[RequestLike, object]
type BatchSource = Iterable[RequestLike | RequestWithPayload] | AsyncIterable[RequestLike | RequestWithPayload]
type Update = Mapping[str, object]
type UpdateWithPayload = tuple[Update, object]
type UpdateSource = Iterable[Update | UpdateWithPayload] | AsyncIterable[Update | UpdateWithPayload]

_ROOT_SELECTOR = ResultSelector.root()
_PAIR_LENGTH = 2


class Bitrix24:
    """Async REST facade backed by one canonical executor and traversal engine."""

    @inject
    def __init__(self, settings: ApiSettings) -> None:
        """Create a long-lived canonical HTTP transport from injected settings."""
        transport = HttpxTransport(str(settings.webhook_url))
        self._settings: Settings | None = settings
        self._transport: HttpxTransport | None = transport
        self._executor = Executor(transport)
        self._batch_executor = BatchExecutor(self._executor)
        self._default_policy = _policy_from_settings(settings)
        host = settings.webhook_url.host
        if host is None:
            raise ValueError("webhook_url must have a host")
        self._host = host

    @classmethod
    def _from_executor(
        cls,
        executor: Executor,
        *,
        policy: ExecutionPolicy | None = None,
        host: str = "test.invalid",
    ) -> Bitrix24:
        """Construct a facade over an injected executor for deterministic tests."""
        instance = cls.__new__(cls)
        instance._settings = None  # noqa: SLF001 - class-owned alternate constructor
        instance._transport = None  # noqa: SLF001 - class-owned alternate constructor
        instance._executor = executor  # noqa: SLF001 - class-owned alternate constructor
        instance._batch_executor = BatchExecutor(executor)  # noqa: SLF001 - class-owned alternate constructor
        instance._default_policy = policy or ExecutionPolicy()  # noqa: SLF001 - class-owned alternate constructor
        instance._host = host  # noqa: SLF001 - class-owned alternate constructor
        return instance

    async def aclose(self) -> None:
        """Close the owned HTTP transport; injected executors remain caller-owned."""
        if self._transport is not None:
            await self._transport.aclose()

    async def __aenter__(self) -> Self:
        """Return this long-lived facade as an async context manager."""
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Close owned transport resources on context exit."""
        await self.aclose()

    @property
    def host(self) -> str:
        """Return the configured webhook host without path or credentials."""
        return self._host

    @overload
    async def call(
        self,
        request: RequestLike,
        *,
        raw: Literal[True],
        policy: ExecutionPolicy | None = None,
        retry: bool | None = None,
    ) -> Response: ...

    @overload
    async def call(
        self,
        request: RequestLike,
        *,
        raw: Literal[False] = False,
        policy: ExecutionPolicy | None = None,
        retry: bool | None = None,
    ) -> JsonValue: ...

    async def call(
        self,
        request: RequestLike,
        *,
        raw: bool = False,
        policy: ExecutionPolicy | None = None,
        retry: bool | None = None,
    ) -> JsonValue | Response:
        """Execute one request through the shared replay-aware executor."""
        response = await self._executor.execute(
            _canonical_request(request),
            policy=_policy_for_retry(policy or self._default_policy, retry),
        )
        return response if raw else response.result

    def batch(
        self,
        requests: BatchSource,
        *,
        batch_size: int | None = None,
        list_method: bool = False,
        with_payload: bool = False,
        policy: ExecutionPolicy | None = None,
    ) -> AsyncIterator[JsonValue | tuple[JsonValue, object]]:
        """Return a lazy fail-fast compatibility batch stream."""
        stream = self._batch_executor.batch(
            _adapt_batch_source(requests, with_payload=with_payload),
            batch_size=batch_size,
            with_payload=with_payload,
            policy=policy or self._default_policy,
        )
        return _legacy_batch_results(
            cast("AsyncIterator[JsonValue | tuple[JsonValue, object]]", stream),
            list_method=list_method,
        )

    def batch_outcomes(
        self,
        requests: BatchSource,
        *,
        batch_size: int | None = None,
        policy: ExecutionPolicy | None = None,
        fallback_failed: Literal["none", "direct"] = "none",
    ) -> BatchStream:
        """Return one typed tolerant outcome for every input command."""
        return self._batch_executor.batch_outcomes(
            _adapt_batch_source(requests, with_payload=False),
            batch_size=batch_size,
            policy=policy or self._default_policy,
            fallback_failed=fallback_failed,
        )

    def iter_list(
        self,
        request: RequestLike,
        *,
        plan: ListPlan,
        selector: ResultSelector = _ROOT_SELECTOR,
        identity: IdentitySpec | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> ItemStream:
        """Return a lazy item stream for one explicit canonical plan."""
        return iter_list_stream(
            self._executor,
            _canonical_request(request),
            plan=plan,
            selector=selector,
            identity=identity,
            policy=policy or self._default_policy,
        )

    def fan_out(
        self,
        requests: Iterable[ReferenceRequest] | AsyncIterable[ReferenceRequest],
        *,
        dispatch: DispatchPlan,
        output_order: ReferenceOutputOrder = ReferenceOutputOrder.READY,
        tolerant: bool = False,
        policy: ExecutionPolicy | None = None,
    ) -> ReferenceStream:
        """Return a lazy bounded stream over already independent requests."""
        return fan_out_stream(
            self._executor,
            requests,
            dispatch=dispatch,
            output_order=output_order,
            tolerant=tolerant,
            policy=policy or self._default_policy,
        )

    def iter_reference(  # noqa: PLR0913 - normative facade signature
        self,
        request: RequestLike,
        bindings: Iterable[ReferenceBinding] | AsyncIterable[ReferenceBinding],
        *,
        plan: ListPlan,
        dispatch: DispatchPlan,
        selector: ResultSelector = _ROOT_SELECTOR,
        identity: IdentitySpec | None = None,
        output_order: ReferenceOutputOrder = ReferenceOutputOrder.READY,
        tolerant: bool = False,
        policy: ExecutionPolicy | None = None,
    ) -> ReferenceStream:
        """Bind top-level parameter updates to one explicit reference plan."""
        base = _as_read_request(_canonical_request(request))
        sources = _binding_requests(base, bindings)
        return iter_references(
            self._executor,
            sources,
            plan=plan,
            dispatch=dispatch,
            selector=selector,
            identity=identity,
            output_order=output_order,
            tolerant=tolerant,
            policy=policy or self._default_policy,
        )

    async def list_sequential(  # noqa: PLR0913 - compatibility bridge signature
        self,
        request: RequestLike,
        *,
        list_size: int | None = None,
        plan: ListPlan | None = None,
        profile: object | None = None,
        identity: IdentitySpec | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> AsyncGenerator[JsonValue]:
        """Preserve sequential offset gathering through the shared driver."""
        _validate_positive_optional(list_size, "list_size")
        selected = _resolve_plan(
            plan,
            profile,
            OffsetSequentialPlan(
                continuation=OffsetContinuation.SERVER_NEXT_OR_OBSERVED_COUNT,
                terminal=frozenset(
                    {
                        OffsetTerminalRule.EMPTY_PAGE,
                        OffsetTerminalRule.QUALIFIED_TOTAL,
                    },
                ),
                total_semantics=TotalSemantics.FILTERED_EXACT,
            ),
        )
        stream = self.iter_list(
            _as_read_request(_canonical_request(request)),
            plan=selected,
            identity=identity,
            policy=policy,
        )
        async for item in _completed_items(stream):
            yield item

    async def list_batched(  # noqa: PLR0913 - compatibility bridge signature
        self,
        request: RequestLike,
        *,
        list_size: int | None = None,
        batch_size: int | None = None,
        plan: ListPlan | None = None,
        profile: object | None = None,
        identity: IdentitySpec | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> AsyncGenerator[JsonValue]:
        """Preserve counted gathering with the admitted sequential fallback."""
        _validate_positive_optional(list_size, "list_size")
        _validate_batch_size(batch_size)
        selected = _resolve_plan(plan, profile, CountedOffsetPlan())
        stream = self.iter_list(
            _as_read_request(_canonical_request(request)),
            plan=selected,
            identity=identity,
            policy=policy,
        )
        async for item in _completed_items(stream):
            yield item

    async def list_batched_no_count(  # noqa: PLR0913 - compatibility bridge signature
        self,
        request: RequestLike,
        *,
        id_key: str = "ID",
        list_size: int | None = None,
        batch_size: int | None = None,
        plan: ListPlan | None = None,
        profile: object | None = None,
        identity: IdentitySpec | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> AsyncGenerator[JsonValue]:
        """Preserve no-count list gathering as exact sequential keyset traversal."""
        _validate_positive_optional(list_size, "list_size")
        _validate_batch_size(batch_size)
        resolved_identity = identity or _legacy_identity(id_key)
        selected = _resolve_plan(plan, profile, _keyset_default())
        stream = self.iter_list(
            _as_read_request(_canonical_request(request)),
            plan=selected,
            identity=resolved_identity,
            policy=policy,
        )
        async for item in _completed_items(stream):
            yield item

    async def list_keyset(
        self,
        request: RequestLike,
        *,
        identity: IdentitySpec,
        plan: KeysetPlan | None = None,
        selector: ResultSelector = _ROOT_SELECTOR,
        policy: ExecutionPolicy | None = None,
    ) -> AsyncGenerator[JsonValue]:
        """Expose the accepted dirty-tree concept as a thin keyset wrapper."""
        stream = self.iter_list(
            _as_read_request(_canonical_request(request)),
            plan=plan or _keyset_default(),
            selector=selector,
            identity=identity,
            policy=policy,
        )
        async for item in _completed_items(stream):
            yield item

    async def reference_batched_no_count(  # noqa: PLR0913
        self,
        request: RequestLike,
        updates: UpdateSource,
        *,
        id_key: str = "ID",
        list_size: int | None = None,
        batch_size: int | None = None,
        with_payload: bool = False,
        plan: ListPlan | None = None,
        profile: object | None = None,
        identity: IdentitySpec | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> AsyncGenerator[JsonValue | tuple[JsonValue, object]]:
        """Map legacy per-filter references to bounded keyset batch dispatch."""
        _validate_positive_optional(list_size, "list_size")
        _validate_batch_size(batch_size)
        base = _as_read_request(_canonical_request(request))
        selected = _resolve_plan(plan, profile, _keyset_default())
        resolved_identity = identity or _legacy_identity(id_key)
        output_order = ReferenceOutputOrder.INPUT
        dispatch = BatchDispatch(
            batch_size=batch_size or self._batch_executor.portal_command_cap,
            output_order=output_order,
        )
        sources = _legacy_reference_requests(
            base,
            updates,
            with_payload=with_payload,
            nested_filter=True,
        )
        stream = iter_references(
            self._executor,
            sources,
            plan=selected,
            dispatch=dispatch,
            identity=resolved_identity,
            output_order=output_order,
            policy=policy or self._default_policy,
        )
        async for item in _legacy_reference_items(stream, with_payload=with_payload):
            yield item

    async def reference_cursor_no_count(  # noqa: PLR0913
        self,
        request: RequestLike,
        updates: UpdateSource,
        *,
        cursor_param: str = "LAST_ID",
        cursor_field: str = "id",
        cursor_take: Literal["max", "min"] = "max",
        list_size: int | None = None,
        list_size_param: str = "LIMIT",
        batch_size: int | None = None,
        result_key: str | None = None,
        with_payload: bool = False,
        plan: ListPlan | None = None,
        profile: object | None = None,
        identity: IdentitySpec | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> AsyncGenerator[JsonValue | tuple[JsonValue, object]]:
        """Map legacy cursor references to one bounded item-cursor scheduler."""
        page_size = list_size or (self._settings.list_size if self._settings is not None else 50)
        _validate_positive_optional(page_size, "list_size")
        _validate_batch_size(batch_size)
        base = _as_read_request(_canonical_request(request))
        resolved_identity = identity or IdentitySpec(
            item_path=(cursor_field,),
            filter_key=cursor_field,
            order_key=cursor_field,
            coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
        )
        default = ItemCursorPlan(
            identity_requirement=IdentityRequirement.REQUIRED,
            cursor_request_path=ParameterPath((cursor_param,)),
            cursor_item_path=(cursor_field,),
            cursor_coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
            cursor_take=cursor_take,
            limit_path=ParameterPath((list_size_param,)),
            requested_page_size=page_size,
            terminal=CursorTerminalRule.PROFILE_SHORT_PAGE,
        )
        selected = _resolve_plan(plan, profile, default)
        selector = ResultSelector((result_key,)) if result_key is not None else ResultSelector.root()
        output_order = ReferenceOutputOrder.INPUT
        dispatch = BatchDispatch(
            batch_size=batch_size or self._batch_executor.portal_command_cap,
            output_order=output_order,
        )
        sources = _legacy_reference_requests(
            base,
            updates,
            with_payload=with_payload,
            nested_filter=False,
        )
        stream = iter_references(
            self._executor,
            sources,
            plan=selected,
            dispatch=dispatch,
            selector=selector,
            identity=resolved_identity,
            output_order=output_order,
            policy=policy or self._default_policy,
        )
        async for item in _legacy_reference_items(stream, with_payload=with_payload):
            yield item


def _policy_from_settings(settings: Settings) -> ExecutionPolicy:
    attempts = max(1, settings.retry_attempts)
    maximum_delay = settings.retry_delay * settings.retry_backoff ** max(0, attempts - 1)
    return ExecutionPolicy(
        max_attempts_per_request=attempts,
        retry=RetryPolicy(
            transient_http_statuses=frozenset(settings.retry_statuses),
            transient_api_codes=frozenset(settings.retry_errors),
            initial_delay=settings.retry_delay,
            maximum_delay=max(settings.retry_delay, maximum_delay),
            backoff=settings.retry_backoff,
            jitter=0,
        ),
    )


def _policy_for_retry(policy: ExecutionPolicy, retry: bool | None) -> ExecutionPolicy:  # noqa: FBT001
    if retry is None or retry:
        return policy
    return replace(policy, max_attempts_per_request=1)


def _canonical_request(raw: RequestLike) -> Request:  # noqa: C901, PLR0912
    if isinstance(raw, Request):
        return raw
    if isinstance(raw, LegacyRequest):
        legacy_parameters = raw.parameters
        if isinstance(legacy_parameters, BaseModel):
            dumped = legacy_parameters.model_dump(mode="json", exclude_defaults=True)
        else:
            dumped = to_jsonable_python(legacy_parameters)
        if not isinstance(dumped, Mapping):
            raise TypeError("legacy request parameters must serialize to a mapping")
        return Request(raw.method, cast("Mapping[str, object]", dumped))
    if not isinstance(raw, Mapping):
        raise TypeError("request must be a canonical Request, legacy Request, or mapping")
    unknown = set(raw) - {"method", "parameters", "replay_safety"}
    if unknown:
        raise ValueError(f"unknown request fields: {sorted(unknown)}")
    method = raw.get("method")
    parameters = raw.get("parameters", {})
    safety = raw.get("replay_safety")
    if not isinstance(method, str):
        raise TypeError("mapping request method must be a string")
    if not isinstance(parameters, Mapping):
        raise TypeError("mapping request parameters must be a mapping")
    if isinstance(safety, str):
        try:
            safety = ReplaySafety(safety)
        except ValueError as error:
            raise ValueError("mapping replay_safety is invalid") from error
    if safety is not None and not isinstance(safety, ReplaySafety):
        raise TypeError("mapping replay_safety must be a ReplaySafety or enum value")
    dumped = to_jsonable_python(parameters)
    if not isinstance(dumped, Mapping):
        raise TypeError("mapping request parameters must serialize to a mapping")
    return Request(method, cast("Mapping[str, object]", dumped), safety)


def _as_read_request(request: Request) -> Request:
    if request.replay_safety is not None:
        return request
    return Request(request.method, request.copy_parameters(), ReplaySafety.SAFE)


def _resolve_plan(plan: ListPlan | None, profile: object | None, default: ListPlan) -> ListPlan:
    if plan is not None:
        if profile is not None:
            raise CapabilityError("plan and profile cannot both select wrapper traversal")
        return plan
    if profile is not None:
        raise CapabilityError("endpoint profiles are unavailable until W8")
    return default


def _legacy_identity(id_key: str) -> IdentitySpec:
    if not id_key:
        raise ValueError("id_key must not be empty")
    return IdentitySpec(
        item_path=(id_key,),
        filter_key=id_key,
        order_key=id_key,
        coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
    )


def _keyset_default() -> KeysetPlan:
    return KeysetPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
        terminal=KeysetTerminalRule.EMPTY_CONFIRMATION,
    )


def _validate_positive_optional(value: int | None, name: str) -> None:
    if value is not None and (isinstance(value, bool) or value < 1):
        raise ValueError(f"{name} must be positive")


def _validate_batch_size(value: int | None) -> None:
    if value is not None and (isinstance(value, bool) or not 1 <= value <= PORTAL_BATCH_CAP):
        raise ValueError("batch_size must be between 1 and 50")


def _adapt_batch_source(source: BatchSource, *, with_payload: bool) -> Iterable[BatchInput] | AsyncIterable[BatchInput]:
    if isinstance(source, AsyncIterable):
        return _adapt_batch_async(source, with_payload=with_payload)
    return _adapt_batch_sync(source, with_payload=with_payload)


def _adapt_batch_sync(
    source: Iterable[RequestLike | RequestWithPayload],
    *,
    with_payload: bool,
) -> Iterator[BatchInput]:
    iterator = iter(source)
    try:
        for raw in iterator:
            yield _adapt_batch_item(raw, with_payload=with_payload)
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()


async def _adapt_batch_async(
    source: AsyncIterable[RequestLike | RequestWithPayload],
    *,
    with_payload: bool,
) -> AsyncGenerator[BatchInput]:
    iterator = aiter(source)
    try:
        async for raw in iterator:
            yield _adapt_batch_item(raw, with_payload=with_payload)
    finally:
        close = getattr(iterator, "aclose", None)
        if callable(close):
            await close()


def _adapt_batch_item(raw: RequestLike | RequestWithPayload, *, with_payload: bool) -> BatchInput:
    if with_payload:
        if not isinstance(raw, tuple) or len(raw) != _PAIR_LENGTH:
            raise TypeError("with_payload=True requires (request, payload) tuples")
        request, payload = raw
        return _canonical_request(request), payload
    if isinstance(raw, tuple):
        raise TypeError("request tuples require with_payload=True")
    return _canonical_request(raw)


async def _legacy_batch_results(
    stream: AsyncIterator[JsonValue | tuple[JsonValue, object]],
    *,
    list_method: bool,
) -> AsyncGenerator[JsonValue | tuple[JsonValue, object]]:
    async for item in stream:
        if isinstance(item, tuple):
            result, payload = item
            yield (_legacy_list_result(result), payload) if list_method else item
        else:
            yield _legacy_list_result(item) if list_method else item


def _legacy_list_result(result: JsonValue) -> list[JsonValue]:
    if isinstance(result, list):
        return result
    if not isinstance(result, dict) or len(result) != 1:
        raise TypeError("list_method result must be a list or one-key mapping")
    value = next(iter(result.values()))
    if not isinstance(value, list):
        raise TypeError("list_method mapping value must be a list")
    return value


async def _completed_items(stream: ItemStream) -> AsyncGenerator[JsonValue]:
    try:
        async for item in stream:
            yield item
    finally:
        await stream.aclose()
    if stream.report.state is not TerminalState.COMPLETED:
        raise IncompleteTraversalError(report=stream.report)


def _binding_requests(
    base: Request,
    bindings: Iterable[ReferenceBinding] | AsyncIterable[ReferenceBinding],
) -> Iterable[ReferenceRequest] | AsyncIterable[ReferenceRequest]:
    if isinstance(bindings, AsyncIterable):
        return _binding_requests_async(base, bindings)
    return _binding_requests_sync(base, bindings)


def _binding_requests_sync(base: Request, bindings: Iterable[ReferenceBinding]) -> Iterator[ReferenceRequest]:
    iterator = iter(bindings)
    try:
        for binding in iterator:
            if not isinstance(binding, ReferenceBinding):
                raise TypeError("bindings must contain ReferenceBinding values")
            request = _merge_top_level(base, binding.copy_updates())
            yield ReferenceRequest(request, binding.payload_key, binding.payload)
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()


async def _binding_requests_async(
    base: Request,
    bindings: AsyncIterable[ReferenceBinding],
) -> AsyncGenerator[ReferenceRequest]:
    iterator = aiter(bindings)
    try:
        async for binding in iterator:
            if not isinstance(binding, ReferenceBinding):
                raise TypeError("bindings must contain ReferenceBinding values")
            request = _merge_top_level(base, binding.copy_updates())
            yield ReferenceRequest(request, binding.payload_key, binding.payload)
    finally:
        close = getattr(iterator, "aclose", None)
        if callable(close):
            await close()


def _legacy_reference_requests(
    base: Request,
    updates: UpdateSource,
    *,
    with_payload: bool,
    nested_filter: bool,
) -> Iterable[ReferenceRequest] | AsyncIterable[ReferenceRequest]:
    def builder(raw: Update | UpdateWithPayload, index: int) -> ReferenceRequest:
        return _legacy_reference(
            base,
            raw,
            index=index,
            with_payload=with_payload,
            nested_filter=nested_filter,
        )

    if isinstance(updates, AsyncIterable):
        return _map_reference_async(updates, builder)
    return _map_reference_sync(updates, builder)


def _map_reference_sync(
    source: Iterable[Update | UpdateWithPayload],
    builder: Callable[[Update | UpdateWithPayload, int], ReferenceRequest],
) -> Iterator[ReferenceRequest]:
    iterator = iter(source)
    try:
        for index, raw in enumerate(iterator):
            yield builder(raw, index)
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()


async def _map_reference_async(
    source: AsyncIterable[Update | UpdateWithPayload],
    builder: Callable[[Update | UpdateWithPayload, int], ReferenceRequest],
) -> AsyncGenerator[ReferenceRequest]:
    iterator = aiter(source)
    index = 0
    try:
        async for raw in iterator:
            yield builder(raw, index)
            index += 1
    finally:
        close = getattr(iterator, "aclose", None)
        if callable(close):
            await close()


def _legacy_reference(
    base: Request,
    raw: Update | UpdateWithPayload,
    *,
    index: int,
    with_payload: bool,
    nested_filter: bool,
) -> ReferenceRequest:
    payload: object = None
    update: Update
    if with_payload:
        if not isinstance(raw, tuple) or len(raw) != _PAIR_LENGTH:
            raise TypeError("with_payload=True requires (update, payload) tuples")
        update, payload = raw
    else:
        if isinstance(raw, tuple):
            raise TypeError("update tuples require with_payload=True")
        update = raw
    if not isinstance(update, Mapping):
        raise TypeError("reference update must be a mapping")
    request = _merge_filter(base, update) if nested_filter else _merge_top_level(base, update)
    return ReferenceRequest(request, f"r{index}", payload)


def _merge_top_level(base: Request, update: Mapping[str, object]) -> Request:
    parameters = base.copy_parameters()
    normalized = to_jsonable_python(update)
    if not isinstance(normalized, Mapping):
        raise TypeError("reference updates must serialize to a mapping")
    parameters.update(cast("Mapping[str, JsonValue]", normalized))
    return Request(base.method, parameters, base.replay_safety)


def _merge_filter(base: Request, update: Mapping[str, object]) -> Request:
    parameters = base.copy_parameters()
    matches = [key for key in parameters if key.casefold() == "filter"]
    if len(matches) > 1:
        raise ValueError("reference request has ambiguous filter casing")
    filter_key = matches[0] if matches else "filter"
    current = parameters.get(filter_key, {})
    if not isinstance(current, dict):
        raise TypeError("reference request filter must be an object")
    normalized = to_jsonable_python(update)
    if not isinstance(normalized, Mapping):
        raise TypeError("reference filter updates must serialize to a mapping")
    current.update(cast("Mapping[str, JsonValue]", normalized))
    parameters[filter_key] = current
    return Request(base.method, parameters, base.replay_safety)


async def _legacy_reference_items(
    stream: ReferenceStream,
    *,
    with_payload: bool,
) -> AsyncGenerator[JsonValue | tuple[JsonValue, object]]:
    try:
        async for outcome in stream:
            if isinstance(outcome, ReferenceFailure):
                raise cast("BaseException", outcome.error)
            if not isinstance(outcome, ReferenceItem):
                raise TypeError("reference stream yielded an unknown outcome")
            yield (outcome.item, outcome.payload) if with_payload else outcome.item
    finally:
        await stream.aclose()
    if stream.report.state is not TerminalState.COMPLETED:
        raise IncompleteTraversalError(report=stream.report)


__all__ = ["Bitrix24"]
