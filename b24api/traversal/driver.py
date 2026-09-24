"""Lazy correctness-first sequential traversal streams and state machines."""

from __future__ import annotations
import contextlib
from dataclasses import replace
from typing import TYPE_CHECKING, cast

from b24api.contracts.completion import EMPTY_SOURCE_WITNESS, CommandSettlement, EmptySourceWitness
from b24api.contracts.json import FrozenJson, _json_type_name
from b24api.contracts.page import IdentityPageAdapter, PageAdapter
from b24api.contracts.policy import (
    ConfirmationPolicy,
    DuplicatePolicy,
    ExecutionPolicy,
    IdentityCoercion,
    IdentityRequirement,
    TotalSemantics,
)
from b24api.contracts.report import (
    PageDispatch,
    PageOutcome,
    PageRecord,
    PageRejectionCode,
    Violation,
    ViolationSeverity,
    retain_page_trace,
)
from b24api.contracts.request import (
    CompositeIdentitySpec,
    IdentityComponent,
    IdentitySpec,
    Request,
    ResultSelector,
    TraversalIdentity,
)
from b24api.contracts.traversal import OffsetContinuation
from b24api.errors import (
    AmbiguousExecutionError,
    CapabilityError,
    IdentityContractError,
    PageAdaptationError,
    PaginationError,
    ResultShapeError,
)
from b24api.execution import WorkClass
from b24api.traversal.control_preflight import preflight_controls
from b24api.traversal.counted_batch import CountedBatchStrategy, empty_source_head_eligible
from b24api.traversal.cursor import ItemCursorStrategy
from b24api.traversal.identity import (
    _PLAN_TYPES,
    PageFetch,
    _effective_duplicate_policy,
    _effective_order_direction,
    _effective_total_semantics,
    _EffectiveConsistency,
    _identity_store,
    _IdentityStore,
    _Page,
    _PageRejectionError,
    _validate_confirmation_policy,
)
from b24api.traversal.keyset import KeysetStrategy
from b24api.traversal.page_adaptation import _SourcePageState, adapt_page
from b24api.traversal.plans import (
    CountedOffsetPlan,
    ItemCursorPlan,
    KeysetPlan,
    KeysetTerminalRule,
    ListPlan,
    OffsetSequentialPlan,
    SingleResponsePlan,
)
from b24api.traversal.sequential import CountedStrategy, OffsetStrategy, SingleResponseStrategy
from b24api.traversal.strategy_context import PagedStrategy, PageStop, SequentialPageStrategy
from b24api.traversal.values import (
    IdentityValue,
    _coerce_identity,
    _compare_identities,
    _extract_path,
    _mapping_shape_degraded,
    _page_fingerprint_policy,
    _response_items,
    _validate_order,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence

    from b24api.completion.recorder import CompletionSink
    from b24api.contracts.identity_store import IdentityStore
    from b24api.contracts.json import JsonValue
    from b24api.contracts.response import Response
    from b24api.execution import ExecutionContext, Executor
    from b24api.execution.snapshot import KernelReport

_IDENTITY_PAGE_ADAPTER = IdentityPageAdapter()


def _strategy(plan: ListPlan) -> SequentialPageStrategy | PagedStrategy:
    """Compose the strategy that owns a validated plan's requests and verdicts."""
    if isinstance(plan, SingleResponsePlan):
        return SingleResponseStrategy(plan)
    if isinstance(plan, OffsetSequentialPlan):
        return OffsetStrategy(plan)
    if isinstance(plan, CountedOffsetPlan):
        return CountedStrategy(plan)
    if isinstance(plan, KeysetPlan):
        return KeysetStrategy(plan)
    return ItemCursorStrategy(plan)


class PaginationDriver:
    """One operation-local state machine over an explicit immutable plan."""

    def __init__(  # noqa: PLR0913
        self,
        executor: Executor,
        request: Request,
        plan: ListPlan,
        *,
        selector: ResultSelector | None,
        identity: TraversalIdentity | None,
        context: ExecutionContext,
        fetch: PageFetch | None = None,
        single_result_as_item: bool = False,
        page_cap_hint: int | None = None,
        page_adapter: PageAdapter = _IDENTITY_PAGE_ADAPTER,
        initial_cursor: IdentityValue | None = None,
        completion_recorder: CompletionSink | None = None,
        identity_store: IdentityStore | None = None,
    ) -> None:
        """Initialize instance state."""
        self.executor = executor
        self.request = request
        self.plan = plan
        self.selector = plan.selector or selector or ResultSelector.root()
        self.identity = identity
        self.context = context
        self.fetch_override = fetch
        self.completion_recorder = completion_recorder
        self.single_result_as_item = single_result_as_item
        if page_cap_hint is not None and (
            not isinstance(page_cap_hint, int) or isinstance(page_cap_hint, bool) or page_cap_hint < 1
        ):
            raise ValueError("page cap hint must be a positive integer")
        self._page_cap_hint = page_cap_hint
        self.page_adapter = page_adapter
        self.terminal_reason: str | None = None
        self.initial_cursor = initial_cursor
        self.cursor_state: JsonValue | IdentityValue = initial_cursor
        self.violations: list[Violation] = []
        self.validated_rows = 0
        self._fingerprints: set[str] = set()
        self._identity_store: _IdentityStore | None = None
        self._external_identity_store = identity_store
        self.duplicate_identities = 0
        self._unique_rows_final: int | None = None
        self._last_identity: IdentityValue | None = None
        self._last_page_unique_mask: tuple[bool, ...] = ()
        self._duplicate_policy = plan.duplicate_policy
        self._total_semantics = plan.total_semantics
        self._order_direction: str | None = None
        self._confirmation_policy = ConfirmationPolicy.NONE
        self._expected_total: int | None = None
        self._advisory_totals: set[int] = set()
        self._advisory_total_drift_reported = False
        self._advisory_total_mismatch_reported = False
        self.batch_report: KernelReport | None = None
        self.page_trace: list[PageRecord] = []
        self._page_trace_count = 0
        self.page_trace_truncated = False
        self._last_page_record: PageRecord | None = None
        self._page_dispatch = PageDispatch.DIRECT
        self._page_batch_index: int | None = None
        self._page_offset: int | None = None
        self.source_page = _SourcePageState()
        self.empty_source_witness: EmptySourceWitness | None = None
        self._empty_source_allowance = False

    async def pages(self) -> AsyncGenerator[_Page]:
        """Yield validated traversal pages."""
        self.begin_external_validation()
        try:
            strategy = _strategy(self.plan)
            pages = strategy.pages(self) if isinstance(strategy, PagedStrategy) else self._sequential(strategy)
            async for page in pages:
                yield page
        finally:
            self.close_external_validation()

    async def _sequential(self, strategy: SequentialPageStrategy) -> AsyncGenerator[_Page]:
        """Own each page transaction: fetch, select, judge, validate, record the rejection or yield."""
        request = strategy.first_request(self)
        while True:
            response = await self.fetch(request)
            trace_count = self.page_trace_count
            items: tuple[FrozenJson, ...] = ()
            try:
                items = self.select_page(response)
                verdict = strategy.judge(self, response, items)
                identities = self.validate_page(
                    items,
                    response=response,
                    terminal=verdict.terminal,
                    identities=verdict.identities,
                )
            except BaseException as error:
                if self.page_trace_count == trace_count:
                    self.reject_external_page(items, response, error)
                raise
            if items:
                yield _Page(tuple(items), response, (1,) * len(items), not verdict.terminal)
            step = strategy.advance(self, identities)
            if isinstance(step, PageStop):
                self.terminal_reason = step.reason
                return
            request = step

    def counted_batch_pages(self, *, batch_size: int, page_size: int) -> AsyncGenerator[_Page]:
        """Execute the committed direct-head/batched-tail counted traversal."""
        return CountedBatchStrategy(batch_size=batch_size, page_size=page_size).pages(self)

    def empty_source_head_eligible(
        self,
        response: Response,
        source: tuple[FrozenJson, ...],
        adapted: tuple[FrozenJson, ...],
    ) -> bool:
        """Return, without side effects, whether an unvalidated head may witness an empty source."""
        return empty_source_head_eligible(self, response, source, adapted)

    async def fetch(self, request: Request) -> Response:
        """Fetch one page directly, accounting for it in the operation budget."""
        if self.fetch_override is not None:
            return await self.fetch_override(request)
        recorder = self.completion_recorder
        if recorder is not None:
            recorder.scheduled()
        reservation = None
        try:
            reservation = await self.context.reserve_page()
            response = await self.executor.execute(
                request,
                context=self.context,
                work_class=WorkClass.TRAVERSAL_DIRECT,
            )
            self.context.commit_page(reservation)
            if recorder is not None:
                recorder.settled(CommandSettlement.SUCCESS)
        except BaseException as error:
            if recorder is not None:
                recorder.settled(
                    CommandSettlement.UNKNOWN
                    if bool(getattr(error, "_b24api_dispatch_started", False))
                    else CommandSettlement.NOT_EXECUTED,
                )
            if reservation is not None:
                self.context.release_page(reservation)
            if bool(getattr(error, "_b24api_dispatch_started", False)):
                self.set_page_dispatch(dispatch=PageDispatch.DIRECT)
                self.record_unknown_page(
                    dispatch=PageDispatch.DIRECT,
                    batch_index=None,
                    error=error,
                )
            raise
        return response

    def begin_external_validation(self) -> None:
        """Start canonical contract and identity validation for an external page dispatcher."""
        if self._identity_store is not None:
            raise RuntimeError("page validation is already active")
        self._validate_capabilities()
        self._identity_store = _identity_store(self.context, self.plan, self.identity, self._external_identity_store)

    def validate_external_page(
        self,
        items: tuple[FrozenJson, ...],
        response: Response,
        *,
        terminal: bool = False,
        empty_source: bool = False,
    ) -> None:
        """Validate one externally dispatched page with the canonical traversal state machine."""
        if self._identity_store is None:
            raise RuntimeError("page validation is not active")
        self.validate_page(items, response=response, terminal=terminal, empty_source=empty_source)

    def select_page(self, response: Response, *, single: bool = False) -> tuple[FrozenJson, ...]:
        """Select one scheduled page and retain value-free evidence on shape rejection."""
        source: tuple[FrozenJson, ...] = ()
        try:
            source = _response_items(response, self.selector, single=single)
            adapted = self._adapt_page(response, source)
            self.source_page.remember(source)
        except ResultShapeError as error:
            enriched = ResultShapeError(
                selector=error.selector,
                expected_shape=error.expected_shape,
                observed_type=error.observed_type,
                request_summary=self.request.summary,
                page_offset=self._page_offset,
            )
            self._record_rejected_page((), response, enriched)
            raise enriched from error
        except BaseException as error:
            self._record_rejected_page(source, response, error)
            raise
        return adapted

    def reject_external_page(self, items: tuple[FrozenJson, ...], response: Response, error: BaseException) -> None:
        """Record a pre-commit external range or capability rejection exactly once."""
        self._record_rejected_page(self.source_page.take(items), response, error)

    def _adapt_page(self, response: Response, source: tuple[FrozenJson, ...]) -> tuple[FrozenJson, ...]:
        """Apply one page strategy and enforce its value-free structural contract."""
        return adapt_page(
            response,
            source,
            adapter=self.page_adapter,
            identities=self._adaptation_specs(),
            request_summary=self.request.summary,
            page_offset=self.page_trace_count,
        )

    def _adaptation_specs(self) -> tuple[tuple[tuple[str | int, ...], IdentityCoercion], ...]:
        specs: list[tuple[tuple[str | int, ...], IdentityCoercion]] = []
        if isinstance(self.identity, CompositeIdentitySpec):
            specs.extend((component.item_path, component.coercion) for component in self.identity.components)
        elif isinstance(self.identity, IdentitySpec):
            specs.append((self.identity.item_path, self.identity.coercion))
        if isinstance(self.plan, ItemCursorPlan):
            cursor_spec = (self.plan.cursor_item_path, self.plan.cursor_coercion)
            if cursor_spec not in specs:
                specs.append(cursor_spec)
        return tuple(specs)

    def close_external_validation(self) -> None:
        """Release the canonical identity store retained by external page validation."""
        if self._identity_store is None:
            return
        self._unique_rows_final = self._identity_store.count
        self._identity_store.close()
        self._identity_store = None

    @staticmethod
    def validate_plan(plan: object) -> None:
        """Reject values outside the closed canonical plan union."""
        if not isinstance(plan, _PLAN_TYPES):
            raise TypeError("plan must be a canonical ListPlan")

    @staticmethod
    def validate_contract(
        plan: ListPlan,
        identity: TraversalIdentity | None,
        policy: ExecutionPolicy,
    ) -> _EffectiveConsistency:
        """Validate request-independent plan/policy capabilities before input or I/O."""
        PaginationDriver.validate_plan(plan)
        consistency = policy.consistency
        if (
            plan.identity_requirement is IdentityRequirement.COMPOSITE
            or consistency.identity_requirement is IdentityRequirement.COMPOSITE
        ) and not isinstance(identity, CompositeIdentitySpec):
            raise CapabilityError("composite identity contract requires CompositeIdentitySpec")
        if isinstance(identity, CompositeIdentitySpec) and isinstance(plan, KeysetPlan | ItemCursorPlan):
            raise CapabilityError("composite identity is supported only by sequential and counted traversal")
        if (
            plan.identity_requirement is IdentityRequirement.REQUIRED
            or consistency.identity_requirement is IdentityRequirement.REQUIRED
        ) and identity is None:
            raise CapabilityError("plan requires IdentitySpec")
        duplicate_policy = _effective_duplicate_policy(
            plan.duplicate_policy,
            consistency.duplicate_policy,
        )
        total_semantics = _effective_total_semantics(
            plan.total_semantics,
            consistency.total_semantics,
        )
        order_direction = _effective_order_direction(
            plan.order_semantics,
            consistency.order_semantics,
        )
        if order_direction is not None and identity is None:
            raise CapabilityError("ordered traversal requires IdentitySpec")
        _validate_confirmation_policy(
            plan,
            consistency.confirmation_policy,
            total_semantics,
        )
        if isinstance(plan, KeysetPlan) and plan.terminal is KeysetTerminalRule.BOUNDARY_ID_SEEN:
            raise CapabilityError("boundary-id keyset requires an externally reviewed boundary contract")
        return _EffectiveConsistency(
            duplicate_policy,
            total_semantics,
            order_direction,
            consistency.confirmation_policy,
        )

    def require_identity(self, plan_name: str) -> IdentitySpec:
        """Return the scalar identity a plan requires, or reject the plan."""
        if not isinstance(self.identity, IdentitySpec):
            raise CapabilityError(f"{plan_name} traversal requires IdentitySpec")
        return self.identity

    def _validate_capabilities(self) -> None:
        effective = self.validate_contract(
            self.plan,
            self.identity,
            self.context.policy,
        )
        self._duplicate_policy = effective.duplicate_policy
        self._total_semantics = effective.total_semantics
        self._order_direction = effective.order_direction
        self._confirmation_policy = effective.confirmation_policy
        preflight_controls(self)

    def validate_page(  # noqa: PLR0913
        self,
        items: tuple[FrozenJson, ...],
        *,
        response: Response,
        qualified_count: int | None = None,
        terminal: bool = False,
        identities: list[IdentityValue] | None = None,
        empty_source: bool = False,
    ) -> list[IdentityValue]:
        """Evaluate a page transactionally and commit only after all checks pass."""
        source_items = self.source_page.take(items)
        # The allowance is re-derived from pre-commit state and lives only inside this transaction.
        self._empty_source_allowance = empty_source and self.empty_source_head_eligible(response, source_items, items)
        snapshot = (
            self.validated_rows,
            self._expected_total,
            set(self._advisory_totals),
            self._advisory_total_drift_reported,
            self._advisory_total_mismatch_reported,
            len(self.violations),
            self._last_identity,
            self._last_page_unique_mask,
            self.duplicate_identities,
        )
        try:
            accepted = self._validate_page_impl(
                source_items,
                response=response,
                qualified_count=qualified_count,
                terminal=terminal,
                identities=identities,
            )
        except BaseException as error:
            (
                self.validated_rows,
                self._expected_total,
                advisory_totals,
                self._advisory_total_drift_reported,
                self._advisory_total_mismatch_reported,
                violation_count,
                self._last_identity,
                self._last_page_unique_mask,
                self.duplicate_identities,
            ) = snapshot
            self._advisory_totals = advisory_totals
            del self.violations[violation_count:]
            self._record_rejected_page(source_items, response, error)
            raise
        finally:
            witnessed, self._empty_source_allowance = self._empty_source_allowance, False
        if witnessed:
            self.empty_source_witness = EMPTY_SOURCE_WITNESS
            self.terminal_reason = EMPTY_SOURCE_WITNESS.terminal_reason
        return accepted

    def _validate_page_impl(  # noqa: C901, PLR0912, PLR0915
        self,
        items: tuple[FrozenJson, ...],
        *,
        response: Response,
        qualified_count: int | None = None,
        terminal: bool = False,
        identities: list[IdentityValue] | None = None,
    ) -> list[IdentityValue]:
        requested_page_size = getattr(self.plan, "requested_page_size", None)
        page_caps = tuple(value for value in (requested_page_size, self._page_cap_hint) if value is not None)
        if page_caps and len(items) > min(page_caps):
            raise PaginationError("response exceeded the declared page cap")
        fixed_step = (
            isinstance(self.plan, OffsetSequentialPlan | CountedOffsetPlan)
            and self.plan.continuation is OffsetContinuation.FIXED_STEP
        )
        if not items and response.next is not None and not fixed_step:
            raise PaginationError("empty response retained a continuation")
        if _mapping_shape_degraded(response, self.selector):
            self.violations.append(
                Violation(
                    severity=ViolationSeverity.WARNING,
                    code="collection_shape_degraded",
                    message="mapping collection terminated as an empty sequence",
                ),
            )
        fingerprint, track_fingerprint = _page_fingerprint_policy(items, self.plan)
        if fingerprint in self._fingerprints and track_fingerprint:
            raise _PageRejectionError(
                "repeated page fingerprint detected",
                PageRejectionCode.REPEATED_FINGERPRINT,
            )
        accepted_count = len(items) if qualified_count is None else qualified_count
        self._validate_response_total(response, accepted_count)
        if self.identity is None:
            self._last_page_unique_mask = (False,) * len(items)
            self.validated_rows += accepted_count
            self._validate_total_not_overshot()
            if terminal:
                self._validate_terminal_total()
            self._fingerprints.update((fingerprint,) if track_fingerprint else ())
            self._record_committed_page(items, response)
            return []
        identities = self.extract_identities(items) if identities is None else identities
        if self._order_direction is not None:
            _validate_order(identities, self._order_direction)
            if self._last_identity is not None and identities:
                if self._order_direction == "asc" and _compare_identities(identities[0], self._last_identity) <= 0:
                    raise PaginationError("identity order did not advance")
                if self._order_direction == "desc" and _compare_identities(identities[0], self._last_identity) >= 0:
                    raise PaginationError("identity order did not advance")
        local: set[IdentityValue] = set()
        duplicates: list[IdentityValue] = []
        unique_mask: list[bool] = []
        for value in identities:
            duplicate = value in local or self._store.contains(value)
            unique_mask.append(not duplicate)
            if duplicate:
                duplicates.append(value)
            local.add(value)
        self._last_page_unique_mask = tuple(unique_mask)
        if duplicates and self._duplicate_policy is DuplicatePolicy.ERROR:
            raise _PageRejectionError("duplicate identity detected", PageRejectionCode.DUPLICATE_IDENTITY)
        if self._expected_total is not None and self.validated_rows + accepted_count > self._expected_total:
            raise _PageRejectionError("traversal exceeded its exact total", PageRejectionCode.TOTAL_DRIFT)
        new_values = tuple(dict.fromkeys(value for value in identities if not self._store.contains(value)))
        self._store.ensure_capacity(len(new_values))
        self.validated_rows += accepted_count
        if terminal:
            self._validate_terminal_total()
        present = self._store.commit(new_values)
        if present:
            self._last_page_unique_mask = _first_unique_mask(identities, present)
        self._settle_duplicates(len(identities) - sum(self._last_page_unique_mask))
        if identities:
            self._last_identity = identities[-1]
        self._fingerprints.update((fingerprint,) if track_fingerprint else ())
        self._record_committed_page(items, response, identities)
        return identities

    def _settle_duplicates(self, duplicates: int) -> None:
        """Apply the duplicate policy once per page after the identity ledger answered."""
        if not duplicates:
            return
        if self._duplicate_policy is DuplicatePolicy.ERROR:
            raise _PageRejectionError("duplicate identity detected", PageRejectionCode.DUPLICATE_IDENTITY)
        if self._duplicate_policy is DuplicatePolicy.REPORT:
            self.duplicate_identities += duplicates
            self.violations.append(
                Violation(
                    severity=ViolationSeverity.WARNING,
                    code="duplicate_identity",
                    message=f"observed {duplicates} duplicate identities",
                ),
            )

    def _record_committed_page(
        self,
        items: tuple[FrozenJson, ...],
        response: Response,
        identities: Sequence[IdentityValue] = (),
    ) -> None:
        self._append_page_record(
            PageRecord(
                sequence=0,
                offset=self._page_offset,
                dispatch=self._page_dispatch,
                batch_index=self._page_batch_index,
                rows_selected=len(items),
                rows_admitted=len(items),
                reported_total=None if response.total in {None, -1} else response.total,
                reported_next=None if response.next in {None, -1} else response.next,
                outcome=PageOutcome.COMMITTED,
                rejection_code=None,
            ),
        )
        if self.completion_recorder is not None:
            self.completion_recorder.validated(identities, len(items))

    def schedule_page(
        self,
        *,
        offset: int | None,
        dispatch: PageDispatch,
        batch_index: int | None = None,
    ) -> None:
        """Set value-free provenance before one logical page is decoded."""
        self.cursor_state = offset
        self._page_offset = offset
        self._page_dispatch = dispatch
        self._page_batch_index = batch_index

    def set_page_dispatch(self, *, dispatch: PageDispatch, batch_index: int | None = None) -> None:
        """Attach physical dispatch provenance without changing the logical cursor."""
        self._page_dispatch = dispatch
        self._page_batch_index = batch_index

    def record_unknown_page(
        self,
        *,
        dispatch: PageDispatch,
        batch_index: int | None,
        error: BaseException,
    ) -> None:
        """Record a scheduled page whose rows could not be decoded."""
        code = (
            PageRejectionCode.AMBIGUOUS_EXECUTION
            if isinstance(error, AmbiguousExecutionError)
            else PageRejectionCode.COMMAND_FAILURE
        )
        self._append_page_record(
            PageRecord(
                sequence=0,
                offset=self._page_offset,
                dispatch=dispatch,
                batch_index=batch_index,
                rows_selected=0,
                rows_admitted=0,
                reported_total=None,
                reported_next=None,
                outcome=PageOutcome.UNKNOWN,
                rejection_code=code,
            ),
        )

    def _record_rejected_page(
        self,
        items: tuple[FrozenJson, ...],
        response: Response,
        error: BaseException,
    ) -> None:
        if isinstance(error, IdentityContractError):
            code = PageRejectionCode.IDENTITY_CONTRACT
        elif isinstance(error, PageAdaptationError):
            code = PageRejectionCode.PAGE_ADAPTATION
        elif isinstance(error, ResultShapeError):
            code = PageRejectionCode.SHAPE_CONTRACT
        elif isinstance(error, _PageRejectionError):
            code = error.rejection_code
        else:
            code = PageRejectionCode.RANGE_CONTRADICTION
        self._append_page_record(
            PageRecord(
                sequence=0,
                offset=self._page_offset,
                dispatch=self._page_dispatch,
                batch_index=self._page_batch_index,
                rows_selected=len(items),
                rows_admitted=0,
                reported_total=None if response.total in {None, -1} else response.total,
                reported_next=None if response.next in {None, -1} else response.next,
                outcome=PageOutcome.REJECTED,
                rejection_code=code,
            ),
        )
        if self.completion_recorder is not None:
            self.completion_recorder.rejected(code.value)

    def extract_identities(self, items: tuple[FrozenJson, ...]) -> list[IdentityValue]:
        """Return the identities of the page's items."""
        if self.identity is None:
            return []
        composite = isinstance(self.identity, CompositeIdentitySpec)
        if isinstance(self.identity, CompositeIdentitySpec):
            components = self.identity.components
        else:
            components = (IdentityComponent(self.identity.item_path, self.identity.coercion),)
        identities: list[IdentityValue] = []
        for index, item in enumerate(items):
            row_offset = self.validated_rows + index
            values: list[str | int] = []
            for component_index, component in enumerate(components):
                path = component.item_path
                coercion = component.coercion
                try:
                    raw = _extract_path(item, path)
                    values.append(cast("str | int", _coerce_identity(raw, coercion)))
                except PaginationError as error:
                    observed_type = "missing"
                    with contextlib.suppress(PaginationError):
                        observed_type = _json_type_name(_extract_path(item, path))
                    raise IdentityContractError(
                        path=path,
                        coercion=coercion,
                        observed_type=observed_type,
                        row_offset=row_offset,
                        request_summary=self.request.summary,
                        component_index=(component_index if composite else None),
                        component_label=(component.label if composite else None),
                    ) from error
            identities.append(tuple(values) if composite else values[0])
        return identities

    def _validate_response_total(self, response: Response, accepted_count: int) -> None:
        if self._total_semantics is TotalSemantics.FILTERED_EXACT:
            if response.total in {None, -1}:
                if not self._empty_source_allowance:
                    raise CapabilityError("filtered exact total requires a non-negative total")
            elif self._expected_total is None:
                self._expected_total = response.total
            elif response.total != self._expected_total:
                raise _PageRejectionError("traversal exact total drifted", PageRejectionCode.TOTAL_DRIFT)
        elif self._total_semantics is TotalSemantics.ADVISORY and response.total is not None and response.total >= 0:
            self._advisory_totals.add(response.total)
            if len(self._advisory_totals) > 1 and not self._advisory_total_drift_reported:
                self.violations.append(
                    Violation(
                        severity=ViolationSeverity.WARNING,
                        code="advisory_total_drift",
                        message="advisory totals changed during traversal",
                    ),
                )
                self._advisory_total_drift_reported = True
        elif self._total_semantics is TotalSemantics.GLOBAL and (response.total is None or response.total < 0):
            raise CapabilityError("global total semantics require a non-negative total")
        if accepted_count < 0:
            raise RuntimeError("qualified response count cannot be negative")

    def _validate_total_not_overshot(self) -> None:
        if self._expected_total is not None and self.validated_rows > self._expected_total:
            raise _PageRejectionError("traversal exceeded its exact total", PageRejectionCode.TOTAL_DRIFT)

    def _validate_terminal_total(self) -> None:
        if self._total_semantics is TotalSemantics.ADVISORY:
            if (
                self._advisory_totals
                and self.validated_rows not in self._advisory_totals
                and not self._advisory_total_mismatch_reported
            ):
                self.violations.append(
                    Violation(
                        severity=ViolationSeverity.WARNING,
                        code="advisory_total_mismatch",
                        message="delivered rows differ from the observed advisory total",
                    ),
                )
                self._advisory_total_mismatch_reported = True
            return
        if self._total_semantics is not TotalSemantics.FILTERED_EXACT or self._empty_source_allowance:
            return
        if self._expected_total is None:
            raise CapabilityError("terminal traversal lacks its filtered exact total")
        if self.validated_rows != self._expected_total:
            raise _PageRejectionError(
                "traversal terminated before its exact total",
                PageRejectionCode.TOTAL_DRIFT,
            )

    def _append_page_record(self, record: PageRecord) -> None:
        record = replace(record, sequence=self._page_trace_count)
        self._page_trace_count += 1
        self._last_page_record = record
        retained, truncated = retain_page_trace(
            (*self.page_trace, record),
            self.context.policy.page_trace_limit,
        )
        self.page_trace[:] = retained
        self.page_trace_truncated = self.page_trace_truncated or truncated

    @property
    def expected_total(self) -> int | None:
        """Return the exact total the traversal has committed to, if any."""
        return self._expected_total

    @property
    def confirmation_policy(self) -> ConfirmationPolicy:
        """Return the effective completion confirmation policy."""
        return self._confirmation_policy

    @property
    def page_offset(self) -> int | None:
        """Return the logical offset of the scheduled page."""
        return self._page_offset

    @property
    def page_trace_count(self) -> int:
        """Return the number of records observed before online retention."""
        return self._page_trace_count

    @property
    def last_page_record(self) -> PageRecord | None:
        """Return the most recently observed page record, retained or not."""
        return self._last_page_record

    @property
    def unique_rows(self) -> int:
        """Return the unique rows."""
        if self.identity is None:
            return 0
        if self._unique_rows_final is not None:
            return self._unique_rows_final
        if self._identity_store is None:
            return 0
        return self._store.count

    @property
    def last_page_unique_mask(self) -> tuple[bool, ...]:
        """Return the last page unique mask."""
        return self._last_page_unique_mask

    @property
    def _store(self) -> _IdentityStore:
        if self._identity_store is None:
            raise RuntimeError("identity store is not active")
        return self._identity_store


def _first_unique_mask(identities: Sequence[IdentityValue], present: frozenset[IdentityValue]) -> tuple[bool, ...]:
    """Mark first in-page occurrences that the external ledger had not already recorded."""
    seen: set[IdentityValue] = set()
    mask: list[bool] = []
    for value in identities:
        mask.append(value not in seen and value not in present)
        seen.add(value)
    return tuple(mask)
