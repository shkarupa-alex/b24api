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
from b24api.execution import ExecutionContext, Executor, HttpxTransport, await_cancellation_resistant
from b24api.models import (
    BatchFailure,
    BatchSuccess,
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
            return _ResolvedTraversal(plan, identity, selector, page_cap_hint=page_cap_hint)
        if profile is None:
            return _ResolvedTraversal(default, identity, selector, page_cap_hint=page_cap_hint)
        if profile.source_sha256 is None:
            raise CapabilityError("endpoint profile requires immutable source provenance")
        query = query_shape_from_request(
            request,
            selector=selector if selector_explicit else profile.query.selector,
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
        return _ResolvedTraversal(
            decision.selected_plan,
            identity or profile.identity,
            profile.query.selector,
            assurance=decision.assurance,
            profile_id=profile.profile_id,
            profile_version=profile.version,
            profile_source_sha256=profile.source_sha256,
            profile_evidence_sha256=tuple(anchor.artifact_sha256 for anchor in profile.evidence),
            page_cap_hint=profile.page_cap,
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
        )
        items = _completed_items(stream)
        try:
            async for item in items:
                yield item
        finally:
            await items.aclose()

    async def _list_batched_default(  # noqa: C901, PLR0912, PLR0915
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
        context = self._executor.context(policy)
        outcomes: BatchStream | None = None
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
        validator.begin_external_validation()
        try:
            head_reservation = await context.reserve_page()
            try:
                head = await self._executor.execute(
                    _counted_offset_request(base, validation_plan, 0),
                    context=context,
                )
            except BaseException:
                context.release_page(head_reservation)
                raise
            context.commit_page(head_reservation)
            evidence.append(head.evidence)
            head_items = head.list_result
            await context.set_buffered_rows(len(head_items))
            total = head.total
            if total is None or total < 0:
                _raise_capability("parallel counted traversal requires a non-negative total")
            if total < len(head_items):
                _raise_capability("parallel counted traversal observed total below the head page")
            if identity is not None and total > policy.max_tracked_identities:
                _raise_capability("parallel counted traversal exceeds the exact identity budget")
            stride = head.next if head.next is not None else page_size
            if isinstance(stride, bool) or stride < 1:
                _raise_capability("parallel counted traversal requires a positive in-band stride")
            if total > len(head_items) and head.next is None:
                _raise_capability("parallel counted traversal has no in-band tail stride")
            if head.next is not None and head.next != len(head_items):
                _raise_capability("parallel counted head continuation contradicts its row count")
            if total == len(head_items) and head.next is not None:
                _raise_capability("parallel counted traversal completed while continuation remained")
            tail_pages = (total - 1) // stride if total > len(head_items) else 0
            if 1 + tail_pages > policy.max_pages:
                _raise_capability("parallel counted traversal exceeds the logical page budget")
            if tail_pages and stride > policy.max_buffered_rows:
                _raise_capability("parallel counted page exceeds the decoded row buffer budget")
            effective_batch_size = min(
                batch_size,
                max(1, policy.max_buffered_rows // stride),
            )
            minimum_requests = 1 + (tail_pages + effective_batch_size - 1) // effective_batch_size
            if minimum_requests > policy.max_requests:
                _raise_capability("parallel counted traversal exceeds the physical request budget")
            validator.validate_external_page(head_items, head)
            for item in head_items:
                emitted += 1
                yield item
                await context.adjust_buffered_rows(-1)
            if total == emitted:
                validator.finish_external_validation()
                if policy.consistency.snapshot_requirement is not SnapshotRequirement.TRAVERSAL_ONLY:
                    report = await _counted_report(
                        context,
                        TerminalState.COMPLETED,
                        "parallel counted traversal completed",
                        emitted=emitted,
                        unique=emitted,
                        violations=tuple(validator.violations),
                        evidence=tuple(evidence),
                    )
                    _raise_incomplete(report, None)
                return
            requests = (
                _counted_offset_request(base, validation_plan, start)
                for start in range(stride, total, stride)
            )
            outcomes = BatchStream(
                self._batch_executor,
                requests,
                batch_size=effective_batch_size,
                tolerant=True,
                with_payload=False,
                fallback_failed="none",
                policy=policy,
                context=context,
                logical_page_per_command=True,
            )
            primary: BaseException | None = None
            try:
                async for outcome in outcomes:
                    if isinstance(outcome, BatchFailure):
                        if isinstance(outcome.error, BaseException):
                            _raise_error(outcome.error)
                        _raise_capability("parallel counted batch command failed")
                    if not isinstance(outcome, BatchSuccess) or outcome.response is None:
                        _raise_capability("parallel counted batch outcome lacks correlated response evidence")
                    response = outcome.response
                    evidence.append(response.evidence)
                    start = stride * (outcome.command_index + 1)
                    items = response.list_result
                    expected_rows = min(stride, total - start)
                    if len(items) != expected_rows:
                        _raise_capability("parallel counted page length contradicts the planned exact range")
                    if response.total is not None and response.total != total:
                        _raise_capability("parallel counted page total contradicts the head total")
                    expected_next = start + stride if start + stride < total else None
                    if response.next != expected_next:
                        _raise_capability("parallel counted continuation contradicts the planned exact range")
                    validator.validate_external_page(items, response)
                    for item in items:
                        emitted += 1
                        yield item
            except BaseException as error:  # noqa: BLE001 - preserve the canonical stream report
                primary = error
            finally:
                try:
                    cleanup_cancellation = await await_cancellation_resistant(outcomes.aclose())
                except BaseException as cleanup_error:  # noqa: BLE001 - cleanup retains primary failure
                    if primary is None:
                        primary = cleanup_error
                else:
                    if cleanup_cancellation is not None:
                        primary = cleanup_cancellation
            if primary is not None:
                _raise_error(primary)
            if outcomes.report.state is not TerminalState.COMPLETED:
                _raise_incomplete(outcomes.report, None)
            if emitted != total:
                _raise_capability("parallel counted traversal did not emit its exact total")
            validator.finish_external_validation()
        except asyncio.CancelledError as error:
            report, repeated = await _counted_report_resistant(
                context,
                TerminalState.CANCELLED,
                "iteration cancelled",
                emitted=emitted,
                unique=emitted,
                batch_report=outcomes.report if outcomes is not None else None,
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
                unique=emitted,
                batch_report=outcomes.report if outcomes is not None else None,
                violations=tuple(validator.violations),
                evidence=tuple(evidence),
            )
            if repeated is not None:
                _attach_compatibility_report(repeated, report)
                raise repeated from error
            raise IncompleteTraversalError(report=report) from error
        except BaseException as error:
            cause_report = getattr(error, "report", None)
            state = cause_report.state if isinstance(cause_report, OperationReport) else TerminalState.FAILED
            if state is TerminalState.COMPLETED:
                state = TerminalState.FAILED
            report, repeated = await _counted_report_resistant(
                context,
                state,
                type(error).__name__,
                emitted=emitted,
                unique=emitted,
                batch_report=(
                    outcomes.report
                    if outcomes is not None
                    else cause_report if isinstance(cause_report, OperationReport) else None
                ),
                violations=tuple(validator.violations),
                evidence=tuple(evidence),
            )
            if repeated is not None:
                _attach_compatibility_report(repeated, report)
                raise repeated from error
            raise IncompleteTraversalError(report=report) from error
        finally:
            validator.close_external_validation()

    async def list_batched_no_count(  # noqa: PLR0913 - compatibility bridge signature
        self,
        request: RequestLike,
        *,
        id_key: str = "ID",
        list_size: int | None = None,
        batch_size: int | None = None,
        plan: ListPlan | None = None,
        profile: EndpointProfile | None = None,
        identity: IdentitySpec | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> AsyncGenerator[JsonValue]:
        """Preserve no-count list gathering as exact sequential keyset traversal."""
        page_size = self._configured_list_size(list_size)
        _validate_positive_optional(page_size, "list_size")
        self._effective_batch_size(batch_size)
        resolved_identity = identity if profile is not None else identity or _legacy_identity(id_key)
        base = _as_profiled_read_request(_canonical_request(request), profile, plan=plan)
        if identity is None and profile is None:
            base = _ensure_legacy_select_identity(base, id_key)
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
        id_key: str = "ID",
        list_size: int | None = None,
        batch_size: int | None = None,
        with_payload: bool = False,
        plan: ListPlan | None = None,
        profile: EndpointProfile | None = None,
        identity: IdentitySpec | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> AsyncGenerator[JsonValue | tuple[JsonValue, object]]:
        """Map legacy per-filter references to bounded keyset batch dispatch."""
        page_size = self._configured_list_size(list_size)
        _validate_positive_optional(page_size, "list_size")
        effective_batch_size = self._effective_batch_size(batch_size)
        base = _as_profiled_read_request(_canonical_request(request), profile, plan=plan)
        if identity is None and profile is None:
            base = _ensure_legacy_select_identity(base, id_key)
        resolved_identity = identity if profile is not None else identity or _legacy_identity(id_key)
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
        cursor_param: str = "LAST_ID",
        cursor_field: str = "id",
        cursor_take: Literal["max", "min"] = "max",
        list_size: int | None = None,
        list_size_param: str = "LIMIT",
        batch_size: int | None = None,
        result_key: str | None = None,
        with_payload: bool = False,
        plan: ListPlan | None = None,
        profile: EndpointProfile | None = None,
        identity: IdentitySpec | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> AsyncGenerator[JsonValue | tuple[JsonValue, object]]:
        """Map legacy cursor references to one bounded item-cursor scheduler."""
        page_size = self._configured_list_size(list_size)
        _validate_positive_optional(page_size, "list_size")
        effective_batch_size = self._effective_batch_size(batch_size)
        base = _as_profiled_read_request(_canonical_request(request), profile, plan=plan)
        resolved_identity = (
            identity
            if profile is not None
            else identity
            or IdentitySpec(
                item_path=(cursor_field,),
                filter_key=cursor_field,
                order_key=cursor_field,
                coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
            )
        )
        default = ItemCursorPlan(
            identity_requirement=IdentityRequirement.REQUIRED,
            cursor_request_path=ParameterPath((cursor_param,)),
            cursor_item_path=(cursor_field,),
            cursor_coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
            direction="desc" if cursor_take == "min" else "asc",
            cursor_take=cursor_take,
            limit_path=ParameterPath((list_size_param,)),
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
