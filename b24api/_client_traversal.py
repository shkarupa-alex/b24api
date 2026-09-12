"""Stateless traversal operation helpers for the public facade."""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.contracts.keyset_execution import AutoKeysetExecution, KeysetExecution, StableIntegerKeysetContract
from b24api.contracts.page import IdentityPageAdapter, PageAdapter
from b24api.contracts.request import IdentitySpec, RequestLike, ResultSelector, TraversalIdentity, canonical_request
from b24api.contracts.response import ResultCollectionShape
from b24api.contracts.traversal import CursorSpec, KeysetSpec, OffsetSpec, TotalTermination
from b24api.traversal.facade import counted_stream, cursor_stream, keyset_stream, sequential_stream

if TYPE_CHECKING:
    from b24api.contracts.json import JsonValue
    from b24api.contracts.policy import ExecutionPolicy
    from b24api.contracts.report import Violation
    from b24api.contracts.request import Request
    from b24api.contracts.stream import OperationStream
    from b24api.execution import Executor

_ROOT_SELECTOR = ResultSelector.root()
_DEFAULT_OFFSET = OffsetSpec()
_DEFAULT_COUNTED_OFFSET = OffsetSpec(total_termination=TotalTermination.EXACT_QUALIFIED)
_DEFAULT_KEYSET = KeysetSpec()
_DEFAULT_AUTO_KEYSET_EXECUTION = AutoKeysetExecution(StableIntegerKeysetContract())
_IDENTITY_PAGE_ADAPTER = IdentityPageAdapter()


class _TraversalFacade:
    """Traversal methods sharing the lifecycle owned by ``Bitrix24``."""

    _executor: Executor
    _default_policy: ExecutionPolicy

    def _require_open(self) -> None:
        raise NotImplementedError

    def _audit_unknown(self, request: Request) -> Violation | None:
        raise NotImplementedError

    def _register_stream[T](self, stream: OperationStream[T]) -> OperationStream[T]:
        raise NotImplementedError

    def _discard_stream(self, stream: object) -> None:
        raise NotImplementedError

    def iter_list(  # noqa: PLR0913
        self,
        request: RequestLike,
        *,
        selector: ResultSelector = _ROOT_SELECTOR,
        identity: TraversalIdentity | None = None,
        collection_shape: ResultCollectionShape = ResultCollectionShape.SEQUENCE,
        page_size: int = 50,
        offset: OffsetSpec = _DEFAULT_OFFSET,
        page_adapter: PageAdapter = _IDENTITY_PAGE_ADAPTER,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[JsonValue]:
        """Return conservative sequential offset/server-next traversal."""
        self._require_open()
        canonical = canonical_request(request)
        audit_violation = self._audit_unknown(canonical)
        return self._register_stream(
            sequential_stream(
                self._executor,
                canonical,
                selector=selector,
                identity=identity,
                collection_shape=collection_shape,
                page_size=page_size,
                offset=offset,
                page_adapter=page_adapter,
                policy=policy or self._default_policy,
                deregister=self._discard_stream,
                audit_violations=(() if audit_violation is None else (audit_violation,)),
            ),
        )

    def iter_list_counted(  # noqa: PLR0913
        self,
        request: RequestLike,
        *,
        identity: TraversalIdentity | None = None,
        selector: ResultSelector = _ROOT_SELECTOR,
        collection_shape: ResultCollectionShape = ResultCollectionShape.SEQUENCE,
        page_size: int = 50,
        batch_size: int | None = None,
        offset: OffsetSpec = _DEFAULT_COUNTED_OFFSET,
        page_adapter: PageAdapter = _IDENTITY_PAGE_ADAPTER,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[JsonValue]:
        """Return exact direct-head plus physically batched counted traversal."""
        self._require_open()
        canonical = canonical_request(request)
        audit_violation = self._audit_unknown(canonical)
        return self._register_stream(
            counted_stream(
                self._executor,
                canonical,
                identity=identity,
                selector=selector,
                collection_shape=collection_shape,
                page_size=page_size,
                batch_size=batch_size,
                offset=offset,
                page_adapter=page_adapter,
                policy=policy or self._default_policy,
                deregister=self._discard_stream,
                audit_violations=(() if audit_violation is None else (audit_violation,)),
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
        execution: KeysetExecution = _DEFAULT_AUTO_KEYSET_EXECUTION,
        page_adapter: PageAdapter = _IDENTITY_PAGE_ADAPTER,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[JsonValue]:
        """Return automatic no-count keyset traversal with explicit execution override."""
        self._require_open()
        canonical = canonical_request(request)
        audit_violation = self._audit_unknown(canonical)
        return self._register_stream(
            keyset_stream(
                self._executor,
                canonical,
                selector=selector,
                identity=identity,
                collection_shape=collection_shape,
                page_size=page_size,
                keyset=keyset,
                execution=execution,
                page_adapter=page_adapter,
                policy=policy or self._default_policy,
                deregister=self._discard_stream,
                audit_violations=(() if audit_violation is None else (audit_violation,)),
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
        page_adapter: PageAdapter = _IDENTITY_PAGE_ADAPTER,
        policy: ExecutionPolicy | None = None,
    ) -> OperationStream[JsonValue]:
        """Return strict dependent cursor traversal with empty confirmation."""
        self._require_open()
        canonical = canonical_request(request)
        audit_violation = self._audit_unknown(canonical)
        return self._register_stream(
            cursor_stream(
                self._executor,
                canonical,
                selector=selector,
                cursor=cursor,
                identity=identity,
                collection_shape=collection_shape,
                page_size=page_size,
                page_adapter=page_adapter,
                policy=policy or self._default_policy,
                deregister=self._discard_stream,
                audit_violations=(() if audit_violation is None else (audit_violation,)),
            ),
        )


__all__: list[str] = []
