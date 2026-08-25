"""Thin v2 Bitrix24 composition facade."""

# ruff: noqa: SLF001 - class-owned alternate construction

from __future__ import annotations
import asyncio
import weakref
from typing import TYPE_CHECKING, Self, cast

from b24api.batch.facade import batch_outcome_stream, batch_stream
from b24api.contracts.dispatch import BatchDispatch, DirectDispatch, DispatchSpec
from b24api.contracts.policy import ExecutionPolicy
from b24api.contracts.request import IdentitySpec, RequestLike, ResultSelector, canonical_request
from b24api.contracts.response import Response, ResultCollectionShape
from b24api.contracts.traversal import CursorSpec, KeysetSpec, OffsetSpec, TraversalSpec
from b24api.execution import Executor, HttpxTransport, Transport, await_cleanup_resistant, rearm_cancellation
from b24api.execution.cleanup import CloseableResource, close_owned_resources
from b24api.references.facade import reference_stream
from b24api.references.fanout import CommandSource as FanOutCommandSource
from b24api.references.fanout import fanout_stream
from b24api.settings import Settings, api_settings
from b24api.traversal.facade import counted_stream, cursor_stream, keyset_stream, sequential_stream

if TYPE_CHECKING:
    from collections.abc import AsyncIterable, Iterable
    from types import TracebackType

    from b24api.contracts.command import Command, CommandOutcome, CommandSuccess
    from b24api.contracts.json import JsonValue
    from b24api.contracts.reference import ReferenceEvent, ReferenceOutcome
    from b24api.contracts.stream import OperationStream
    from b24api.references.binding import BindingSource

_ROOT_SELECTOR = ResultSelector.root()
_DEFAULT_OFFSET = OffsetSpec()
_DEFAULT_KEYSET = KeysetSpec()
_DEFAULT_REFERENCE_DISPATCH = BatchDispatch()
_DEFAULT_FANOUT_DISPATCH = DirectDispatch()


def _normalized_host(host: str) -> str:
    return host.strip().rstrip(".").casefold()


class Bitrix24:
    """Async method-agnostic client over one correctness kernel."""

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
        self._close_task: asyncio.Task[None] | None = None
        self._streams: weakref.WeakSet[CloseableResource] = weakref.WeakSet()

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
        instance._close_task = None
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

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        primary: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close owned work without replacing a primary body exception."""
        try:
            await self.aclose()
        except BaseException as cleanup_error:
            if primary is not None:
                raise primary.with_traceback(traceback) from cleanup_error
            raise

    async def aclose(self) -> None:
        """Close active streams and then the owned transport idempotently."""
        if self._close_task is not None and self._close_task.done():
            return
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(
                close_owned_resources(tuple(self._streams), self._owned_transport),
            )
        cleanup = await await_cleanup_resistant(self._close_task)
        if cleanup.error is not None:
            rearm_cancellation(cleanup.cancellation)
            raise cleanup.error
        if cleanup.cancellation is not None:
            raise cleanup.cancellation

    async def call(self, request: RequestLike, *, policy: ExecutionPolicy | None = None) -> JsonValue:
        """Execute one request and return detached decoded JSON."""
        return (await self.call_response(request, policy=policy)).result

    async def call_response(self, request: RequestLike, *, policy: ExecutionPolicy | None = None) -> Response:
        """Execute one request and return its immutable response envelope."""
        self._require_open()
        return await self._executor.execute(canonical_request(request), policy=policy or self._default_policy)

    def batch[C](
        self,
        commands: Iterable[Command[C]] | AsyncIterable[Command[C]],
        *,
        batch_size: int | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[CommandSuccess[C]]:
        """Execute an arbitrary logical batch with bounded fail-fast windows."""
        self._require_open()
        return self._register_stream(
            batch_stream(
                self._executor,
                commands,
                batch_size=batch_size,
                policy=policy or self._default_policy,
                deregister=self._discard_stream,
            ),
        )

    def batch_outcomes[C](
        self,
        commands: Iterable[Command[C]] | AsyncIterable[Command[C]],
        *,
        batch_size: int | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[CommandOutcome[C]]:
        """Execute an arbitrary logical batch and retain every correlated state."""
        self._require_open()
        return self._register_stream(
            batch_outcome_stream(
                self._executor,
                commands,
                batch_size=batch_size,
                policy=policy or self._default_policy,
                deregister=self._discard_stream,
            ),
        )

    def fan_out[C](
        self,
        commands: FanOutCommandSource[C],
        *,
        dispatch: DispatchSpec = _DEFAULT_FANOUT_DISPATCH,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[CommandSuccess[C]]:
        """Dispatch independent commands fail-fast with explicit delivery order."""
        self._require_open()
        stream = fanout_stream(
            self._executor,
            commands,
            dispatch=dispatch,
            policy=policy or self._default_policy,
            tolerant=False,
            deregister=self._discard_stream,
        )
        return cast("OperationStream[CommandSuccess[C]]", self._register_stream(stream))

    def fan_out_outcomes[C](
        self,
        commands: FanOutCommandSource[C],
        *,
        dispatch: DispatchSpec = _DEFAULT_FANOUT_DISPATCH,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[CommandOutcome[C]]:
        """Dispatch independent commands and retain every correlated terminal state."""
        self._require_open()
        return self._register_stream(
            fanout_stream(
                self._executor,
                commands,
                dispatch=dispatch,
                policy=policy or self._default_policy,
                tolerant=True,
                deregister=self._discard_stream,
            ),
        )

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
        self._require_open()
        stream = reference_stream(
            self._executor,
            request,
            bindings,
            traversal=traversal,
            dispatch=dispatch,
            policy=policy or self._default_policy,
            tolerant=False,
            deregister=self._discard_stream,
        )
        return cast("OperationStream[ReferenceEvent[C]]", self._register_stream(stream))

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
        self._require_open()
        return self._register_stream(
            reference_stream(
                self._executor,
                request,
                bindings,
                traversal=traversal,
                dispatch=dispatch,
                policy=policy or self._default_policy,
                tolerant=True,
                deregister=self._discard_stream,
            ),
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
        self._require_open()
        return self._register_stream(
            sequential_stream(
                self._executor,
                request,
                selector=selector,
                identity=identity,
                collection_shape=collection_shape,
                page_size=page_size,
                offset=offset,
                policy=policy or self._default_policy,
                deregister=self._discard_stream,
            ),
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
        return self._register_stream(
            counted_stream(
                self._executor,
                request,
                identity=identity,
                selector=selector,
                collection_shape=collection_shape,
                page_size=page_size,
                batch_size=batch_size,
                offset=offset,
                policy=policy or self._default_policy,
                deregister=self._discard_stream,
            ),
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
        self._require_open()
        return self._register_stream(
            keyset_stream(
                self._executor,
                request,
                selector=selector,
                identity=identity,
                collection_shape=collection_shape,
                page_size=page_size,
                keyset=keyset,
                policy=policy or self._default_policy,
                deregister=self._discard_stream,
            ),
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
        self._require_open()
        return self._register_stream(
            cursor_stream(
                self._executor,
                request,
                selector=selector,
                cursor=cursor,
                identity=identity,
                collection_shape=collection_shape,
                page_size=page_size,
                policy=policy or self._default_policy,
                deregister=self._discard_stream,
            ),
        )

    def _register_stream[T](self, stream: OperationStream[T]) -> OperationStream[T]:
        self._streams.add(cast("CloseableResource", stream))
        return stream

    def _discard_stream(self, stream: object) -> None:
        self._streams.discard(cast("CloseableResource", stream))

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("client is closed")


__all__ = ["Bitrix24"]
