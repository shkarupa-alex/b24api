"""Canonical Bitrix24 facade and plan-driven compatibility wrappers."""

from __future__ import annotations
import asyncio
import logging
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Iterable, Iterator, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal, Never, Self, cast, overload

from pydantic import BaseModel
from pydantic_core import to_jsonable_python

from b24api.batch import BatchExecutor, BatchInput, BatchOutcomeStream, BatchStream
from b24api.entity import LegacyRequest
from b24api.error import CapabilityError, IncompleteTraversalError
from b24api.execution import ExecutionContext, ExecutionSnapshot, Executor, HttpxTransport, rearm_cancellation
from b24api.models import (
    CompletionAssurance,
    ExecutionPolicy,
    IdentityCoercion,
    IdentityRequirement,
    IdentitySpec,
    JsonValue,
    OperationReport,
    OrderSemantics,
    ParameterPath,
    ReferenceBinding,
    ReferenceFailure,
    ReferenceItem,
    ReferenceRequest,
    ReplaySafety,
    Request,
    Response,
    ResponseEvidence,
    ResultSelector,
    RetryPolicy,
    SnapshotRequirement,
    SnapshotState,
    TerminalState,
    TotalSemantics,
    Violation,
    ViolationSeverity,
    inject_controls,
)
from b24api.pagination import _LEGACY_RESULT_SELECTOR, ItemStream, PaginationDriver
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
    PartitionedKeysetPlan,
    ReferenceOutputOrder,
)
from b24api.profiles import EndpointProfile, apply_probe_observations, choose_plan, query_shape_from_request
from b24api.references import ReferenceStream, iter_references
from b24api.references import fan_out as fan_out_stream
from b24api.settings import Settings, api_settings

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


class _ImplicitCompatibilityString(str):
    """Distinguish an omitted legacy string default without changing its public value."""

    __slots__ = ()


_IMPLICIT_ID_KEY = _ImplicitCompatibilityString("ID")
_IMPLICIT_CURSOR_PARAM = _ImplicitCompatibilityString("LAST_ID")
_IMPLICIT_CURSOR_FIELD = _ImplicitCompatibilityString("id")
_IMPLICIT_CURSOR_TAKE = cast('Literal["max", "min"]', _ImplicitCompatibilityString("max"))
_IMPLICIT_LIST_SIZE_PARAM = _ImplicitCompatibilityString("LIMIT")


@dataclass(frozen=True, slots=True)
class _ResolvedTraversal:
    plan: ListPlan
    identity: IdentitySpec | None
    selector: ResultSelector
    assurance: CompletionAssurance = CompletionAssurance.CALLER_ASSERTED
    profile_id: str | None = None
    profile_version: int | None = None
    profile_source_sha256: str | None = None
    profile_evidence_sha256: tuple[str, ...] = ()
    profile_evidence_candidate_sha: str | None = None
    page_cap_hint: int | None = None


class Bitrix24:
    """Async REST facade backed by one canonical executor and traversal engine."""

    def __init__(self, settings: Settings | None = None) -> None:
        """Create a transport from explicit positional/keyword or environment settings."""
        resolved = settings if settings is not None else api_settings()
        if not isinstance(resolved, Settings):
            raise TypeError("settings must be a Settings value")
        transport = HttpxTransport(str(resolved.webhook_url))
        self._settings: Settings | None = resolved
        self._transport: HttpxTransport | None = transport
        self._executor = Executor(transport)
        self._batch_executor = BatchExecutor(self._executor)
        self._default_policy = _policy_from_settings(resolved)
        self._logger = logging.getLogger(resolved.logger_name)
        self._portal_build = resolved.portal_build
        self._portal_scopes = resolved.scopes
        host = resolved.webhook_url.host
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
        portal_build: str | None = None,
        scopes: Iterable[str] = (),
    ) -> Bitrix24:
        """Construct a facade over an injected executor for deterministic tests."""
        instance = cls.__new__(cls)
        instance._settings = None  # noqa: SLF001 - class-owned alternate constructor
        instance._transport = None  # noqa: SLF001 - class-owned alternate constructor
        instance._executor = executor  # noqa: SLF001 - class-owned alternate constructor
        instance._batch_executor = BatchExecutor(executor)  # noqa: SLF001 - class-owned alternate constructor
        instance._default_policy = policy or ExecutionPolicy()  # noqa: SLF001 - class-owned alternate constructor
        instance._host = host  # noqa: SLF001 - class-owned alternate constructor
        instance._portal_build = portal_build  # noqa: SLF001 - class-owned alternate constructor
        instance._portal_scopes = frozenset(scopes)  # noqa: SLF001 - class-owned alternate constructor
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
            batch_size=self._effective_batch_size(batch_size),
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
    ) -> BatchOutcomeStream:
        """Return one typed tolerant outcome for every input command."""
        return self._batch_executor.batch_outcomes(
            _adapt_batch_source(requests, with_payload=None),
            batch_size=self._effective_batch_size(batch_size),
            policy=policy or self._default_policy,
            fallback_failed=fallback_failed,
        )

    def _configured_batch_size(self, requested: int | None) -> int | None:
        if requested is not None:
            return requested
        return self._settings.batch_size if self._settings is not None else None

    def _effective_batch_size(self, requested: int | None) -> int:
        configured = self._configured_batch_size(requested)
        value = self._batch_executor.portal_command_cap if configured is None else configured
        _validate_batch_size(value)
        return value

    def _configured_list_size(self, requested: int | None) -> int:
        if requested is not None:
            return requested
        return self._settings.list_size if self._settings is not None else 50

    def _wrapper_page_cap(
        self,
        requested: int | None,
        plan: ListPlan | None,
        profile: EndpointProfile | None,
    ) -> int | None:
        if requested is not None or (plan is None and profile is None):
            return self._configured_list_size(requested)
        return None

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

    def _resolve_traversal(  # noqa: PLR0913
        self,
        request: Request,
        *,
        plan: ListPlan | None,
        profile: EndpointProfile | None,
        default: ListPlan,
        selector: ResultSelector,
        identity: IdentitySpec | None,
        policy: ExecutionPolicy,
        page_cap_hint: int | None,
        selector_explicit: bool = False,
    ) -> _ResolvedTraversal:
        if plan is not None:
            if profile is not None:
                raise CapabilityError("plan and profile cannot both select wrapper traversal")
            return _ResolvedTraversal(
                _narrow_plan_page_size(plan, page_cap_hint),
                identity,
                selector,
                page_cap_hint=page_cap_hint,
            )
        if profile is None:
            return _ResolvedTraversal(default, identity, selector, page_cap_hint=page_cap_hint)
        if profile.source_sha256 is None:
            raise CapabilityError("endpoint profile requires immutable source provenance")
        filter_path, order_path = _profile_query_paths(profile)
        query = query_shape_from_request(
            request,
            selector=selector if selector_explicit else profile.query.selector,
            filter_path=filter_path,
            order_path=order_path,
            scopes=self._portal_scopes,
            portal_build=self._portal_build,
            observed_at=datetime.now(UTC),
        )
        decision = apply_probe_observations(choose_plan(profile, query, policy), ())
        if not decision.accepted or decision.selected_plan is None:
            codes = ",".join(reason.code.value for reason in decision.reasons)
            raise CapabilityError(f"endpoint profile is not applicable: {codes}")
        if identity is not None and identity != profile.identity:
            raise CapabilityError("explicit identity contradicts endpoint profile identity")
        effective_page_cap = _minimum_optional(page_cap_hint, profile.page_cap)
        return _ResolvedTraversal(
            _narrow_plan_page_size(decision.selected_plan, effective_page_cap),
            identity or profile.identity,
            profile.query.selector,
            assurance=decision.assurance,
            profile_id=profile.profile_id,
            profile_version=profile.version,
            profile_source_sha256=profile.source_sha256,
            profile_evidence_sha256=tuple(anchor.artifact_sha256 for anchor in profile.evidence),
            profile_evidence_candidate_sha=profile.evidence[0].candidate_sha,
            page_cap_hint=effective_page_cap,
        )

    async def list_sequential(  # noqa: PLR0913 - compatibility bridge signature
        self,
        request: RequestLike,
        *,
        list_size: int | None = None,
        plan: ListPlan | None = None,
        profile: EndpointProfile | None = None,
        identity: IdentitySpec | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> AsyncGenerator[JsonValue]:
        """Preserve sequential offset gathering through the shared driver."""
        page_size = self._configured_list_size(list_size)
        _validate_positive_optional(page_size, "list_size")
        base = _as_profiled_read_request(_canonical_request(request), profile, plan=plan)
        effective_policy = policy or self._default_policy
        selected = self._resolve_traversal(
            base,
            plan=plan,
            profile=profile,
            default=OffsetSequentialPlan(
                continuation=OffsetContinuation.SERVER_NEXT_OR_OBSERVED_COUNT,
                terminal=frozenset(
                    {
                        OffsetTerminalRule.EMPTY_PAGE,
                        OffsetTerminalRule.QUALIFIED_TOTAL,
                    },
                ),
                total_semantics=TotalSemantics.ADVISORY,
            ),
            selector=_LEGACY_RESULT_SELECTOR,
            identity=identity,
            policy=effective_policy,
            page_cap_hint=self._wrapper_page_cap(list_size, plan, profile),
        )
        stream = iter_list_stream(
            self._executor,
            base,
            plan=selected.plan,
            selector=selected.selector,
            identity=selected.identity,
            policy=effective_policy,
            _page_cap_hint=selected.page_cap_hint,
            _assurance=selected.assurance,
            _profile_id=selected.profile_id,
            _profile_version=selected.profile_version,
            _profile_source_sha256=selected.profile_source_sha256,
            _profile_evidence_sha256=selected.profile_evidence_sha256,
            _profile_evidence_candidate_sha=selected.profile_evidence_candidate_sha,
        )
        items = _completed_items(stream)
        try:
            async for item in items:
                yield item
        finally:
            await items.aclose()

    async def list_batched(  # noqa: PLR0913 - compatibility bridge signature
        self,
        request: RequestLike,
        *,
        list_size: int | None = None,
        batch_size: int | None = None,
        plan: ListPlan | None = None,
        profile: EndpointProfile | None = None,
        identity: IdentitySpec | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> AsyncGenerator[JsonValue]:
        """Preserve committed head-plus-batched-tail counted gathering."""
        page_size = self._configured_list_size(list_size)
        _validate_positive_optional(page_size, "list_size")
        effective_batch_size = self._effective_batch_size(batch_size)
        base = _as_profiled_read_request(_canonical_request(request), profile, plan=plan)
        effective_policy = policy or self._default_policy
        if plan is None and profile is None:
            items = self._list_batched_default(
                base,
                page_size=page_size,
                batch_size=effective_batch_size,
                identity=identity,
                policy=effective_policy,
            )
            try:
                async for item in items:
                    yield item
            finally:
                await items.aclose()
            return
        selected = self._resolve_traversal(
            base,
            plan=plan,
            profile=profile,
            default=CountedOffsetPlan(),
            selector=_LEGACY_RESULT_SELECTOR,
            identity=identity,
            policy=effective_policy,
            page_cap_hint=self._wrapper_page_cap(list_size, plan, profile),
        )
        stream = iter_list_stream(
            self._executor,
            base,
            plan=selected.plan,
            selector=selected.selector,
            identity=selected.identity,
            policy=effective_policy,
            _page_cap_hint=selected.page_cap_hint,
            _assurance=selected.assurance,
            _profile_id=selected.profile_id,
            _profile_version=selected.profile_version,
            _profile_source_sha256=selected.profile_source_sha256,
            _profile_evidence_sha256=selected.profile_evidence_sha256,
            _profile_evidence_candidate_sha=selected.profile_evidence_candidate_sha,
        )
        items = _completed_items(stream)
        try:
            async for item in items:
                yield item
        finally:
            await items.aclose()

    async def _list_batched_default(  # noqa: C901 - compatibility report boundary
        self,
        base: Request,
        *,
        page_size: int,
        batch_size: int,
        identity: IdentitySpec | None,
        policy: ExecutionPolicy,
    ) -> AsyncGenerator[JsonValue]:
        evidence: list[ResponseEvidence] = []
        emitted = 0
        unique = 0
        context = self._executor.context(policy)
        validation_plan = CountedOffsetPlan()
        validator = PaginationDriver(
            self._executor,
            base,
            validation_plan,
            selector=_LEGACY_RESULT_SELECTOR,
            identity=identity,
            context=context,
            page_cap_hint=page_size,
        )
        try:
            async for page in validator.counted_batch_pages(batch_size=batch_size, page_size=page_size):
                evidence.append(page.response.evidence)
                for item, is_unique in zip(page.items, validator.last_page_unique_mask, strict=True):
                    emitted += 1
                    unique += int(is_unique)
                    yield item
            if policy.consistency.snapshot_requirement is not SnapshotRequirement.TRAVERSAL_ONLY:
                report = await _counted_report(
                    context,
                    TerminalState.COMPLETED,
                    "parallel counted traversal completed",
                    emitted=emitted,
                    unique=unique,
                    batch_report=validator.batch_report,
                    violations=tuple(validator.violations),
                    evidence=tuple(evidence),
                )
                _raise_incomplete(report, None)
        except asyncio.CancelledError as error:
            report, repeated = await _counted_report_resistant(
                context,
                TerminalState.CANCELLED,
                "iteration cancelled",
                emitted=emitted,
                unique=unique,
                batch_report=validator.batch_report,
                violations=tuple(validator.violations),
                evidence=tuple(evidence),
            )
            propagated = repeated or error
            _attach_compatibility_report(propagated, report)
            if repeated is not None:
                raise repeated from error
            raise
        except GeneratorExit as error:
            report, repeated = await _counted_report_resistant(
                context,
                TerminalState.CANCELLED,
                "stream closed before exhaustion",
                emitted=emitted,
                unique=unique,
                batch_report=validator.batch_report,
                violations=tuple(validator.violations),
                evidence=tuple(evidence),
            )
            if repeated is not None:
                _attach_compatibility_report(repeated, report)
                raise repeated from error
            raise IncompleteTraversalError(report=report) from error
        except BaseException as error:
            preflight, _snapshot_cancellation = await _snapshot_resistant(context)
            if isinstance(error, CapabilityError) and preflight.counters.physical_requests == 0:
                raise
            cause_report = getattr(error, "report", None)
            state = cause_report.state if isinstance(cause_report, OperationReport) else TerminalState.FAILED
            if state is TerminalState.COMPLETED:
                state = TerminalState.FAILED
            report, repeated = await _counted_report_resistant(
                context,
                state,
                type(error).__name__,
                emitted=emitted,
                unique=unique,
                batch_report=validator.batch_report
                or (cause_report if isinstance(cause_report, OperationReport) else None),
                violations=tuple(validator.violations),
                evidence=tuple(evidence),
            )
            if repeated is not None:
                _attach_compatibility_report(repeated, report)
            rearm_cancellation(repeated or _snapshot_cancellation)
            raise IncompleteTraversalError(report=report) from error

    async def list_batched_no_count(  # noqa: PLR0913 - compatibility bridge signature
        self,
        request: RequestLike,
        *,
        id_key: str = _IMPLICIT_ID_KEY,
        list_size: int | None = None,
        batch_size: int | None = None,
        plan: ListPlan | None = None,
        profile: EndpointProfile | None = None,
        identity: IdentitySpec | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> AsyncGenerator[JsonValue]:
        """Preserve no-count list gathering as exact sequential keyset traversal."""
        id_key_explicit = id_key is not _IMPLICIT_ID_KEY
        resolved_id_key = _compatibility_string(id_key, default="ID", field="id_key")
        page_size = self._configured_list_size(list_size)
        _validate_positive_optional(page_size, "list_size")
        self._effective_batch_size(batch_size)
        resolved_identity = identity if profile is not None else identity or _legacy_identity(resolved_id_key)
        base = _as_profiled_read_request(_canonical_request(request), profile, plan=plan)
        if identity is None and profile is None:
            base = _ensure_legacy_select_identity(base, resolved_id_key)
        effective_policy = policy or self._default_policy
        selected = self._resolve_traversal(
            base,
            plan=plan,
            profile=profile,
            default=_keyset_default(),
            selector=_LEGACY_RESULT_SELECTOR,
            identity=resolved_identity,
            policy=effective_policy,
            page_cap_hint=self._wrapper_page_cap(list_size, plan, profile),
        )
        _require_identity_control(selected, id_key=resolved_id_key, explicit=id_key_explicit)
        stream = iter_list_stream(
            self._executor,
            base,
            plan=selected.plan,
            selector=selected.selector,
            identity=selected.identity,
            policy=effective_policy,
            _page_cap_hint=selected.page_cap_hint,
            _assurance=selected.assurance,
            _profile_id=selected.profile_id,
            _profile_version=selected.profile_version,
            _profile_source_sha256=selected.profile_source_sha256,
            _profile_evidence_sha256=selected.profile_evidence_sha256,
            _profile_evidence_candidate_sha=selected.profile_evidence_candidate_sha,
        )
        items = _completed_items(stream)
        try:
            async for item in items:
                yield item
        finally:
            await items.aclose()

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
        items = _completed_items(stream)
        try:
            async for item in items:
                yield item
        finally:
            await items.aclose()

    async def reference_batched_no_count(  # noqa: PLR0913
        self,
        request: RequestLike,
        updates: UpdateSource,
        *,
        id_key: str = _IMPLICIT_ID_KEY,
        list_size: int | None = None,
        batch_size: int | None = None,
        with_payload: bool = False,
        plan: ListPlan | None = None,
        profile: EndpointProfile | None = None,
        identity: IdentitySpec | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> AsyncGenerator[JsonValue | tuple[JsonValue, object]]:
        """Map legacy per-filter references to bounded keyset batch dispatch."""
        id_key_explicit = id_key is not _IMPLICIT_ID_KEY
        resolved_id_key = _compatibility_string(id_key, default="ID", field="id_key")
        page_size = self._configured_list_size(list_size)
        _validate_positive_optional(page_size, "list_size")
        effective_batch_size = self._effective_batch_size(batch_size)
        base = _as_profiled_read_request(_canonical_request(request), profile, plan=plan)
        if identity is None and profile is None:
            base = _ensure_legacy_select_identity(base, resolved_id_key)
        resolved_identity = identity if profile is not None else identity or _legacy_identity(resolved_id_key)
        effective_policy = policy or self._default_policy
        selected = self._resolve_traversal(
            base,
            plan=plan,
            profile=profile,
            default=_keyset_default(),
            selector=_LEGACY_RESULT_SELECTOR,
            identity=resolved_identity,
            policy=effective_policy,
            page_cap_hint=self._wrapper_page_cap(list_size, plan, profile),
        )
        _require_identity_control(selected, id_key=resolved_id_key, explicit=id_key_explicit)
        _require_profile_batch_support(profile)
        output_order = ReferenceOutputOrder.READY
        dispatch = BatchDispatch(
            batch_size=effective_batch_size,
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
            plan=selected.plan,
            dispatch=dispatch,
            selector=selected.selector,
            identity=selected.identity,
            output_order=output_order,
            policy=effective_policy,
            _page_cap_hint=selected.page_cap_hint,
            _assurance=selected.assurance,
            _profile_id=selected.profile_id,
            _profile_version=selected.profile_version,
            _profile_source_sha256=selected.profile_source_sha256,
            _profile_evidence_sha256=selected.profile_evidence_sha256,
            _profile_evidence_candidate_sha=selected.profile_evidence_candidate_sha,
        )
        items = _legacy_reference_items(stream, with_payload=with_payload)
        try:
            async for item in items:
                yield item
        finally:
            await items.aclose()

    async def reference_cursor_no_count(  # noqa: PLR0913
        self,
        request: RequestLike,
        updates: UpdateSource,
        *,
        cursor_param: str = _IMPLICIT_CURSOR_PARAM,
        cursor_field: str = _IMPLICIT_CURSOR_FIELD,
        cursor_take: Literal["max", "min"] = _IMPLICIT_CURSOR_TAKE,
        list_size: int | None = None,
        list_size_param: str = _IMPLICIT_LIST_SIZE_PARAM,
        batch_size: int | None = None,
        result_key: str | None = None,
        with_payload: bool = False,
        plan: ListPlan | None = None,
        profile: EndpointProfile | None = None,
        identity: IdentitySpec | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> AsyncGenerator[JsonValue | tuple[JsonValue, object]]:
        """Map legacy cursor references to one bounded item-cursor scheduler."""
        explicit_cursor_param = cursor_param is not _IMPLICIT_CURSOR_PARAM
        explicit_cursor_field = cursor_field is not _IMPLICIT_CURSOR_FIELD
        explicit_cursor_take = cursor_take is not _IMPLICIT_CURSOR_TAKE
        explicit_list_size_param = list_size_param is not _IMPLICIT_LIST_SIZE_PARAM
        resolved_cursor_param = _compatibility_string(cursor_param, default="LAST_ID", field="cursor_param")
        resolved_cursor_field = _compatibility_string(cursor_field, default="id", field="cursor_field")
        resolved_cursor_take = _cursor_take(cursor_take)
        resolved_list_size_param = _compatibility_string(list_size_param, default="LIMIT", field="list_size_param")
        page_size = self._configured_list_size(list_size)
        _validate_positive_optional(page_size, "list_size")
        effective_batch_size = self._effective_batch_size(batch_size)
        base = _as_profiled_read_request(_canonical_request(request), profile, plan=plan)
        resolved_identity = (
            identity
            if profile is not None
            else identity
            or IdentitySpec(
                item_path=(resolved_cursor_field,),
                filter_key=resolved_cursor_field,
                order_key=resolved_cursor_field,
                coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
            )
        )
        default = ItemCursorPlan(
            identity_requirement=IdentityRequirement.REQUIRED,
            cursor_request_path=ParameterPath((resolved_cursor_param,)),
            cursor_item_path=(resolved_cursor_field,),
            cursor_coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
            direction="desc" if resolved_cursor_take == "min" else "asc",
            cursor_take=resolved_cursor_take,
            limit_path=ParameterPath((resolved_list_size_param,)),
            requested_page_size=page_size,
            terminal=CursorTerminalRule.EMPTY_CONFIRMATION,
        )
        selector = ResultSelector((result_key,)) if result_key is not None else ResultSelector.root()
        effective_policy = policy or self._default_policy
        selected = self._resolve_traversal(
            base,
            plan=plan,
            profile=profile,
            default=default,
            selector=selector if result_key is not None else _LEGACY_RESULT_SELECTOR,
            identity=resolved_identity,
            policy=effective_policy,
            page_cap_hint=self._wrapper_page_cap(list_size, plan, profile),
            selector_explicit=result_key is not None,
        )
        _require_cursor_controls(
            selected.plan,
            cursor_param=(resolved_cursor_param, explicit_cursor_param),
            cursor_field=(resolved_cursor_field, explicit_cursor_field),
            cursor_take=(resolved_cursor_take, explicit_cursor_take),
            list_size_param=(resolved_list_size_param, explicit_list_size_param),
        )
        _require_profile_batch_support(profile)
        output_order = ReferenceOutputOrder.READY
        dispatch = BatchDispatch(
            batch_size=effective_batch_size,
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
            plan=selected.plan,
            dispatch=dispatch,
            selector=selected.selector,
            identity=selected.identity,
            output_order=output_order,
            policy=effective_policy,
            _page_cap_hint=selected.page_cap_hint,
            _assurance=selected.assurance,
            _profile_id=selected.profile_id,
            _profile_version=selected.profile_version,
            _profile_source_sha256=selected.profile_source_sha256,
            _profile_evidence_sha256=selected.profile_evidence_sha256,
            _profile_evidence_candidate_sha=selected.profile_evidence_candidate_sha,
        )
        items = _legacy_reference_items(stream, with_payload=with_payload)
        try:
            async for item in items:
                yield item
        finally:
            await items.aclose()


def _policy_from_settings(settings: Settings) -> ExecutionPolicy:
    attempts = max(1, settings.retry_attempts)
    maximum_delay = settings.retry_delay * settings.retry_backoff ** max(0, attempts - 1)
    return ExecutionPolicy(
        max_attempts_per_request=attempts,
        max_retry_elapsed_per_request=float(settings.http_timeout),
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


def _as_profiled_read_request(
    request: Request,
    profile: EndpointProfile | None,
    *,
    plan: ListPlan | None,
) -> Request:
    if plan is not None and profile is not None:
        raise CapabilityError("plan and profile cannot both select wrapper traversal")
    if profile is None:
        return _as_read_request(request)
    if not isinstance(profile, EndpointProfile):
        raise TypeError("profile must be an EndpointProfile")
    if request.replay_safety is not None and request.replay_safety is not profile.replay_safety:
        raise CapabilityError("request replay safety contradicts the endpoint profile")
    return Request(request.method, request.copy_parameters(), profile.replay_safety)


def _legacy_identity(id_key: str) -> IdentitySpec:
    if not id_key:
        raise ValueError("id_key must not be empty")
    return IdentitySpec(
        item_path=(id_key,),
        filter_key=id_key,
        order_key=id_key,
        coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
    )


def _counted_offset_request(base: Request, plan: CountedOffsetPlan, start: int) -> Request:
    try:
        parameters = inject_controls(
            base.copy_parameters(),
            {plan.offset_path: start},
            allow_create=plan.allow_create_controls,
        )
    except (KeyError, TypeError, ValueError) as error:
        raise CapabilityError("request parameters conflict with counted traversal controls") from error
    return Request(base.method, parameters, base.replay_safety)


def _require_profile_batch_support(profile: EndpointProfile | None) -> None:
    if profile is not None and not profile.capabilities.batch_supported:
        raise CapabilityError("endpoint profile does not authorize batch dispatch")


def _require_identity_control(selected: _ResolvedTraversal, *, id_key: str, explicit: bool) -> None:
    if not explicit:
        return
    if selected.identity is None or selected.identity.item_path != (id_key,):
        raise CapabilityError("explicit id_key contradicts the selected traversal identity")


def _require_cursor_controls(
    selected: ListPlan,
    *,
    cursor_param: tuple[str, bool],
    cursor_field: tuple[str, bool],
    cursor_take: tuple[Literal["max", "min"], bool],
    list_size_param: tuple[str, bool],
) -> None:
    checks = (
        (
            cursor_param[1],
            isinstance(selected, ItemCursorPlan) and selected.cursor_request_path.path == (cursor_param[0],),
        ),
        (cursor_field[1], isinstance(selected, ItemCursorPlan) and selected.cursor_item_path == (cursor_field[0],)),
        (cursor_take[1], isinstance(selected, ItemCursorPlan) and selected.cursor_take == cursor_take[0]),
        (
            list_size_param[1],
            isinstance(selected, ItemCursorPlan)
            and selected.limit_path is not None
            and selected.limit_path.path == (list_size_param[0],),
        ),
    )
    if any(explicit and not matches for explicit, matches in checks):
        raise CapabilityError("explicit cursor controls contradict the selected traversal plan")


def _compatibility_string(value: object, *, default: str, field: str) -> str:
    resolved = default if isinstance(value, _ImplicitCompatibilityString) else value
    if not isinstance(resolved, str) or not resolved:
        raise ValueError(f"{field} must be a non-empty string")
    return resolved


def _cursor_take(value: object) -> Literal["max", "min"]:
    resolved = str(value) if isinstance(value, _ImplicitCompatibilityString) else value
    if resolved not in {"max", "min"}:
        raise ValueError("cursor_take must be max or min")
    return cast('Literal["max", "min"]', resolved)


def _minimum_optional(first: int | None, second: int | None) -> int | None:
    values = tuple(value for value in (first, second) if value is not None)
    return min(values) if values else None


def _narrow_plan_page_size(plan: ListPlan, page_cap: int | None) -> ListPlan:
    """Keep the emitted request size consistent with a narrower wrapper cap."""
    requested = getattr(plan, "requested_page_size", None)
    if page_cap is None or requested is None or requested <= page_cap:
        return plan
    if isinstance(plan, CountedOffsetPlan):
        fixed_stride = page_cap if plan.fixed_stride is not None else None
        return replace(plan, requested_page_size=page_cap, fixed_stride=fixed_stride)
    if isinstance(plan, OffsetSequentialPlan | KeysetPlan | ItemCursorPlan | PartitionedKeysetPlan):
        return replace(plan, requested_page_size=page_cap)
    return plan


def _profile_query_paths(profile: EndpointProfile) -> tuple[ParameterPath, ParameterPath]:
    selected = profile.plan
    if isinstance(selected, KeysetPlan | PartitionedKeysetPlan):
        return selected.filter_path, selected.order_path
    return ParameterPath(("filter",)), ParameterPath(("order",))


async def _counted_report_resistant(  # noqa: PLR0913
    context: ExecutionContext,
    state: TerminalState,
    reason: str,
    *,
    emitted: int,
    unique: int,
    batch_report: OperationReport | None = None,
    violations: tuple[Violation, ...] = (),
    evidence: tuple[ResponseEvidence, ...] = (),
) -> tuple[OperationReport, asyncio.CancelledError | None]:
    task = asyncio.create_task(
        _counted_report(
            context,
            state,
            reason,
            emitted=emitted,
            unique=unique,
            batch_report=batch_report,
            violations=violations,
            evidence=evidence,
        ),
    )
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancellation = error
    return await task, cancellation


async def _snapshot_resistant(
    context: ExecutionContext,
) -> tuple[ExecutionSnapshot, asyncio.CancelledError | None]:
    task = asyncio.create_task(context.snapshot(), name="b24api-counted-failure-snapshot")
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancellation = error
    return await task, cancellation


async def _counted_report(  # noqa: PLR0913
    context: ExecutionContext,
    state: TerminalState,
    reason: str,
    *,
    emitted: int,
    unique: int,
    batch_report: OperationReport | None = None,
    violations: tuple[Violation, ...] = (),
    evidence: tuple[ResponseEvidence, ...] = (),
) -> OperationReport:
    snapshot = await context.snapshot()
    snapshot_state = (
        SnapshotState.NOT_REQUESTED
        if context.policy.consistency.snapshot_requirement is SnapshotRequirement.TRAVERSAL_ONLY
        else SnapshotState.UNVERIFIED
    )
    report_violations = (*violations, *(batch_report.violations if batch_report is not None else ()))
    if state is TerminalState.COMPLETED and snapshot_state is SnapshotState.UNVERIFIED:
        state = TerminalState.INCOMPLETE
        reason = "required snapshot was not verified"
        report_violations = (
            *report_violations,
            Violation(
                severity=ViolationSeverity.BLOCKING,
                code="snapshot_unverified",
                message="the requested stable snapshot was not verified",
            ),
        )
    return OperationReport(
        state=state,
        assurance=CompletionAssurance.CALLER_ASSERTED,
        snapshot=snapshot_state,
        plan_id="CountedOffsetPlan",
        dispatch_id="batch",
        emitted_rows=emitted,
        unique_rows=unique,
        physical_requests=snapshot.counters.physical_requests,
        logical_pages=snapshot.counters.logical_pages,
        batch_requests=batch_report.batch_requests if batch_report is not None else 0,
        batch_commands=batch_report.batch_commands if batch_report is not None else 0,
        retries=snapshot.retries,
        cooldown_seconds=snapshot.cooldown_seconds,
        buffered_rows_high_water=snapshot.counters.buffered_rows_high_water,
        violations=report_violations,
        terminal_reason=reason,
        evidence=evidence,
    )


def _attach_compatibility_report(error: BaseException, report: OperationReport) -> None:
    try:
        error.report = report  # type: ignore[attr-defined]
    except (AttributeError, TypeError):
        return


def _raise_capability(message: str) -> Never:
    raise CapabilityError(message)


def _raise_error(error: BaseException) -> Never:
    raise error


def _raise_incomplete(report: OperationReport, cause: BaseException | None) -> Never:
    raise IncompleteTraversalError(report=report) from cause


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


def _adapt_batch_source(
    source: BatchSource,
    *,
    with_payload: bool | None,
) -> Iterable[BatchInput] | AsyncIterable[BatchInput]:
    if isinstance(source, AsyncIterable):
        return _adapt_batch_async(source, with_payload=with_payload)
    return _adapt_batch_sync(source, with_payload=with_payload)


def _adapt_batch_sync(
    source: Iterable[RequestLike | RequestWithPayload],
    *,
    with_payload: bool | None,
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
    with_payload: bool | None,
) -> AsyncGenerator[BatchInput]:
    iterator = aiter(source)
    try:
        async for raw in iterator:
            yield _adapt_batch_item(raw, with_payload=with_payload)
    finally:
        close = getattr(iterator, "aclose", None)
        if callable(close):
            await close()


def _adapt_batch_item(raw: RequestLike | RequestWithPayload, *, with_payload: bool | None) -> BatchInput:
    if with_payload is True:
        if not isinstance(raw, tuple) or len(raw) != _PAIR_LENGTH:
            raise TypeError("with_payload=True requires (request, payload) tuples")
        request, payload = raw
        return _canonical_request(request), payload
    if with_payload is None and isinstance(raw, tuple) and len(raw) == _PAIR_LENGTH:
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
    try:
        async for item in stream:
            if isinstance(item, tuple):
                result, payload = item
                yield (_legacy_list_result(result), payload) if list_method else item
            else:
                yield _legacy_list_result(item) if list_method else item
    finally:
        if isinstance(stream, BatchStream):
            await stream.aclose()


def _legacy_list_result(result: JsonValue) -> list[JsonValue]:
    if isinstance(result, list):
        return result
    if not isinstance(result, dict):
        raise TypeError("list_method result must be a list or one-key mapping")
    if not result:
        return []
    if len(result) != 1:
        raise TypeError("list_method result must be a list or one-key mapping")
    value = next(iter(result.values()))
    if not isinstance(value, list):
        raise TypeError("list_method mapping value must be a list")
    return value


async def _completed_items(stream: ItemStream) -> AsyncGenerator[JsonValue]:
    primary: BaseException | None = None
    try:
        async for item in stream:
            yield item
    except BaseException as error:  # noqa: BLE001 - cancellation also needs the final report
        primary = error
    finally:
        try:
            await stream.aclose()
        except BaseException as cleanup_error:  # noqa: BLE001 - cleanup can fail with cancellation
            if primary is None:
                primary = cleanup_error
    if stream.report.state is not TerminalState.COMPLETED:
        raise IncompleteTraversalError(report=stream.report) from primary
    if primary is not None:
        raise primary


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
    if updates.__class__ is list or updates.__class__ is tuple:
        return tuple(builder(raw, index) for index, raw in enumerate(updates))
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


def _ensure_legacy_select_identity(base: Request, identity_key: str) -> Request:
    parameters = base.copy_parameters()
    matches = [key for key in parameters if key.casefold() == "select"]
    if len(matches) > 1:
        raise ValueError("reference request has ambiguous select casing")
    select_key = matches[0] if matches else "select"
    selected = parameters.get(select_key, [])
    if not isinstance(selected, list):
        raise TypeError("reference request select must be an array")
    if "*" not in selected and identity_key not in selected:
        selected.append(identity_key)
    parameters[select_key] = selected
    return Request(base.method, parameters, base.replay_safety)


async def _legacy_reference_items(
    stream: ReferenceStream,
    *,
    with_payload: bool,
) -> AsyncGenerator[JsonValue | tuple[JsonValue, object]]:
    primary: BaseException | None = None
    try:
        async for outcome in stream:
            yield _legacy_reference_value(outcome, with_payload=with_payload)
    except BaseException as error:  # noqa: BLE001 - cancellation also needs the final report
        primary = error
    finally:
        try:
            await stream.aclose()
        except BaseException as cleanup_error:  # noqa: BLE001 - cleanup can fail with cancellation
            if primary is None:
                primary = cleanup_error
    if stream.report.state is not TerminalState.COMPLETED:
        raise IncompleteTraversalError(report=stream.report) from primary
    if primary is not None:
        raise primary


def _legacy_reference_value(
    outcome: ReferenceItem | ReferenceFailure,
    *,
    with_payload: bool,
) -> JsonValue | tuple[JsonValue, object]:
    if isinstance(outcome, ReferenceFailure):
        if not isinstance(outcome.error, BaseException):
            raise TypeError("reference failure does not contain an exception")
        raise outcome.error
    if not isinstance(outcome, ReferenceItem):
        raise TypeError("reference stream yielded an unknown outcome")
    return (outcome.item, outcome.payload) if with_payload else outcome.item


__all__ = ["Bitrix24"]
