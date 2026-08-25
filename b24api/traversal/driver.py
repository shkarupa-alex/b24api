"""Lazy correctness-first sequential traversal streams and state machines."""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.contracts.policy import (
    ConfirmationPolicy,
    DuplicatePolicy,
    ExecutionPolicy,
    IdentityRequirement,
    TotalSemantics,
)
from b24api.contracts.report import Violation, ViolationSeverity
from b24api.contracts.request import IdentitySpec, ParameterPath, Request, ResultSelector
from b24api.errors import CapabilityError, PaginationError
from b24api.traversal.counted_batch import _CountedBatchMixin
from b24api.traversal.cursor import _CursorMixin
from b24api.traversal.identity import (
    _PLAN_TYPES,
    PageFetch,
    _child_path,
    _effective_duplicate_policy,
    _effective_order_direction,
    _effective_total_semantics,
    _EffectiveConsistency,
    _identity_store,
    _IdentityStore,
    _initial_offset,
    _Page,
    _request_with_controls,
    _validate_confirmation_policy,
)
from b24api.traversal.keyset import _KeysetMixin
from b24api.traversal.plans import (
    CountedOffsetMode,
    CountedOffsetPlan,
    ItemCursorPlan,
    KeysetPlan,
    KeysetTerminalRule,
    ListPlan,
    OffsetSequentialPlan,
    SingleResponsePlan,
)
from b24api.traversal.sequential import _SequentialMixin
from b24api.traversal.values import (
    IdentityValue,
    _coerce_identity,
    _compare_identities,
    _extract_path,
    _page_fingerprint,
    _validate_order,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from b24api.contracts.json import JsonValue
    from b24api.contracts.response import Response
    from b24api.execution import (
        ExecutionContext,
        Executor,
    )
    from b24api.execution.snapshot import KernelReport


class PaginationDriver(_CountedBatchMixin, _SequentialMixin, _KeysetMixin, _CursorMixin):
    """One operation-local state machine over an explicit immutable plan."""

    def __init__(  # noqa: PLR0913
        self,
        executor: Executor,
        request: Request,
        plan: ListPlan,
        *,
        selector: ResultSelector | None,
        identity: IdentitySpec | None,
        context: ExecutionContext,
        fetch: PageFetch | None = None,
        single_result_as_item: bool = False,
        page_cap_hint: int | None = None,
    ) -> None:
        """Initialize instance state."""
        self.executor = executor
        self.request = request
        self.plan = plan
        self.selector = plan.selector or selector or ResultSelector.root()
        self.identity = identity
        self.context = context
        self._fetch_override = fetch
        self._single_result_as_item = single_result_as_item
        if page_cap_hint is not None and (
            not isinstance(page_cap_hint, int) or isinstance(page_cap_hint, bool) or page_cap_hint < 1
        ):
            raise ValueError("page cap hint must be a positive integer")
        self._page_cap_hint = page_cap_hint
        self.terminal_reason: str | None = None
        self.cursor_state: JsonValue = None
        self.violations: list[Violation] = []
        self.validated_rows = 0
        self._fingerprints: set[str] = set()
        self._identity_store: _IdentityStore | None = None
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

    async def pages(self) -> AsyncGenerator[_Page]:  # noqa: C901
        """Yield validated traversal pages."""
        self.begin_external_validation()
        try:
            if isinstance(self.plan, SingleResponsePlan):
                async for page in self._single(self.plan):
                    yield page
                return
            if isinstance(self.plan, OffsetSequentialPlan):
                async for page in self._offset(self.plan):
                    yield page
                return
            if isinstance(self.plan, CountedOffsetPlan):
                async for page in self._counted(self.plan):
                    yield page
                return
            if isinstance(self.plan, KeysetPlan):
                async for page in self._keyset(self.plan):
                    yield page
                return
            if isinstance(self.plan, ItemCursorPlan):
                async for page in self._cursor(self.plan):
                    yield page
                return
            raise AssertionError("validated list plan was not dispatched")
        finally:
            self.close_external_validation()

    def begin_external_validation(self) -> None:
        """Start canonical contract and identity validation for an external page dispatcher."""
        if self._identity_store is not None:
            raise RuntimeError("page validation is already active")
        self._validate_capabilities()
        self._identity_store = _identity_store(self.context.policy, self.plan, self.identity)

    def validate_external_page(self, items: list[JsonValue], response: Response) -> None:
        """Validate one externally dispatched page with the canonical traversal state machine."""
        if self._identity_store is None:
            raise RuntimeError("page validation is not active")
        self._validate_page(items, response=response)

    def finish_external_validation(self) -> None:
        """Validate the canonical terminal-total contract for externally dispatched pages."""
        if self._identity_store is None:
            raise RuntimeError("page validation is not active")
        self._validate_terminal_total()

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
        identity: IdentitySpec | None,
        policy: ExecutionPolicy,
    ) -> _EffectiveConsistency:
        """Validate request-independent plan/policy capabilities before input or I/O."""
        PaginationDriver.validate_plan(plan)
        consistency = policy.consistency
        if (
            plan.identity_requirement is IdentityRequirement.COMPOSITE
            or consistency.identity_requirement is IdentityRequirement.COMPOSITE
        ):
            raise CapabilityError("composite identity requires a separately reviewed identity contract")
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
        if isinstance(plan, CountedOffsetPlan) and plan.mode is CountedOffsetMode.PARALLEL_FIXED_STRIDE:
            raise CapabilityError("parallel fixed-stride counted traversal requires separate reviewed authorization")
        if isinstance(plan, KeysetPlan) and plan.terminal is KeysetTerminalRule.BOUNDARY_ID_SEEN:
            raise CapabilityError("boundary-id keyset requires an externally reviewed boundary contract")
        return _EffectiveConsistency(
            duplicate_policy,
            total_semantics,
            order_direction,
            consistency.confirmation_policy,
        )

    def _require_identity(self, plan_name: str) -> IdentitySpec:
        if self.identity is None:
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
        self._preflight_controls()

    def _preflight_controls(self) -> None:
        """Prove every current and future injected control is writable before I/O."""
        first: dict[ParameterPath, object] = {}
        second: dict[ParameterPath, object] = {}
        allow_create = getattr(self.plan, "allow_create_controls", True)
        if isinstance(self.plan, OffsetSequentialPlan):
            initial_offset = _initial_offset(self.request, self.plan.offset_path)
            first[self.plan.offset_path] = initial_offset
            second[self.plan.offset_path] = initial_offset + 1
        elif isinstance(self.plan, CountedOffsetPlan):
            first[self.plan.offset_path] = 0
            second[self.plan.offset_path] = 1
        elif isinstance(self.plan, KeysetPlan):
            identity = self._require_identity("keyset")
            order_path = _child_path(self.plan.order_path, identity.order_key)
            operator = ">" if self.plan.direction == "asc" else "<"
            filter_path = _child_path(self.plan.filter_path, f"{operator}{identity.filter_key}")
            first[order_path] = "ASC" if self.plan.direction == "asc" else "DESC"
            second[order_path] = first[order_path]
            first[filter_path] = 0
            second[filter_path] = 1
            if self.plan.start_suppression_path is not None:
                first[self.plan.start_suppression_path] = -1
                second[self.plan.start_suppression_path] = -1
        elif isinstance(self.plan, ItemCursorPlan):
            first[self.plan.cursor_request_path] = 0
            second[self.plan.cursor_request_path] = 1
        if (
            isinstance(self.plan, OffsetSequentialPlan | CountedOffsetPlan | KeysetPlan | ItemCursorPlan)
            and self.plan.limit_path is not None
            and self.plan.requested_page_size is not None
        ):
            first[self.plan.limit_path] = self.plan.requested_page_size
            second[self.plan.limit_path] = self.plan.requested_page_size
        if first:
            replace = frozenset({self.plan.offset_path}) if isinstance(self.plan, OffsetSequentialPlan) else frozenset()
            _request_with_controls(self.request, first, allow_create=allow_create, replace=replace)
            _request_with_controls(self.request, second, allow_create=allow_create, replace=replace)

    def _validate_page(  # noqa: C901, PLR0912
        self,
        items: list[JsonValue],
        *,
        response: Response,
        qualified_count: int | None = None,
    ) -> list[IdentityValue]:
        requested_page_size = getattr(self.plan, "requested_page_size", None)
        page_caps = tuple(value for value in (requested_page_size, self._page_cap_hint) if value is not None)
        if page_caps and len(items) > min(page_caps):
            raise PaginationError("response exceeded the declared page cap")
        if not items and response.next is not None:
            raise PaginationError("empty response retained a continuation")
        fingerprint = _page_fingerprint(items)
        if fingerprint in self._fingerprints:
            raise PaginationError("repeated page fingerprint detected")
        self._fingerprints.add(fingerprint)
        accepted_count = len(items) if qualified_count is None else qualified_count
        self._validate_response_total(response, accepted_count)
        if self.identity is None:
            self._last_page_unique_mask = (True,) * len(items)
            self.validated_rows += accepted_count
            self._validate_total_not_overshot()
            return []
        identities = [
            _coerce_identity(_extract_path(item, self.identity.item_path), self.identity.coercion) for item in items
        ]
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
            raise PaginationError("duplicate identity detected")
        if duplicates and self._duplicate_policy is DuplicatePolicy.REPORT:
            self.violations.append(
                Violation(
                    severity=ViolationSeverity.WARNING,
                    code="duplicate_identity",
                    message=f"observed {len(duplicates)} duplicate identities",
                ),
            )
        for value in local:
            self._store.add(value)
        if identities:
            self._last_identity = identities[-1]
        self.validated_rows += accepted_count
        self._validate_total_not_overshot()
        return identities

    def _validate_response_total(self, response: Response, accepted_count: int) -> None:
        if self._total_semantics is TotalSemantics.FILTERED_EXACT:
            if response.total is None or response.total < 0:
                raise CapabilityError("filtered exact total requires a non-negative total")
            if self._expected_total is None:
                self._expected_total = response.total
            elif response.total != self._expected_total:
                raise PaginationError("traversal exact total drifted")
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
            raise PaginationError("traversal exceeded its exact total")

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
        if self._total_semantics is not TotalSemantics.FILTERED_EXACT:
            return
        if self._expected_total is None:
            raise CapabilityError("terminal traversal lacks its filtered exact total")
        if self.validated_rows != self._expected_total:
            raise PaginationError("traversal terminated before its exact total")

    @property
    def unique_rows(self) -> int:
        """Return the unique rows."""
        if self.identity is None:
            return self.validated_rows
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
