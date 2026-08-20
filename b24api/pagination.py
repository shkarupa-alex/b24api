"""Lazy correctness-first sequential traversal streams and state machines."""

from __future__ import annotations
import asyncio
import contextlib
import hashlib
import itertools
import json
import sqlite3
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Protocol, Self, cast

from b24api.error import CapabilityError, IncompleteTraversalError, PaginationError
from b24api.execution import ExecutionContext, Executor, WorkClass, await_cancellation_resistant
from b24api.models import (
    CompletionAssurance,
    ConfirmationPolicy,
    DuplicatePolicy,
    ExecutionPolicy,
    IdentityCoercion,
    IdentityRequirement,
    IdentitySpec,
    IdentityTracker,
    JsonValue,
    OperationReport,
    OrderSemantics,
    ParameterPath,
    Request,
    Response,
    ResultSelector,
    SnapshotRequirement,
    SnapshotState,
    TerminalState,
    TotalSemantics,
    Violation,
    ViolationSeverity,
    inject_controls,
)
from b24api.plans import (
    CountedOffsetMode,
    CountedOffsetPlan,
    CursorTerminalRule,
    ItemCursorPlan,
    KeysetPlan,
    KeysetTerminalRule,
    ListPlan,
    OffsetContinuation,
    OffsetSequentialPlan,
    OffsetTerminalRule,
    PartitionedKeysetPlan,
    SingleResponsePlan,
)

type IdentityValue = str | int
type PageFetch = Callable[[Request], Awaitable[Response]]
_MISSING = object()
_PLAN_TYPES = (
    SingleResponsePlan,
    OffsetSequentialPlan,
    CountedOffsetPlan,
    KeysetPlan,
    ItemCursorPlan,
    PartitionedKeysetPlan,
)


class _IdentityStore(Protocol):
    @property
    def count(self) -> int: ...

    def contains(self, value: IdentityValue) -> bool: ...

    def add(self, value: IdentityValue) -> None: ...

    def close(self) -> None: ...


class _MemoryIdentityStore:
    def __init__(self, maximum: int) -> None:
        self._maximum = maximum
        self._values: set[IdentityValue] = set()

    @property
    def count(self) -> int:
        return len(self._values)

    def contains(self, value: IdentityValue) -> bool:
        return value in self._values

    def add(self, value: IdentityValue) -> None:
        if value in self._values:
            return
        if len(self._values) >= self._maximum:
            from b24api.error import BudgetExceededError  # noqa: PLC0415

            raise BudgetExceededError("identity tracking budget exhausted")
        self._values.add(value)

    def close(self) -> None:
        self._values.clear()


class _SqliteIdentityStore:
    def __init__(self, maximum: int) -> None:
        self._maximum = maximum
        self._count = 0
        self._connection = sqlite3.connect("")
        self._connection.execute(
            "CREATE TABLE identities (kind TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(kind, value))",
        )

    @property
    def count(self) -> int:
        return self._count

    @staticmethod
    def _key(value: IdentityValue) -> tuple[str, str]:
        return ("integer", str(value)) if isinstance(value, int) else ("string", value)

    def contains(self, value: IdentityValue) -> bool:
        row = self._connection.execute(
            "SELECT 1 FROM identities WHERE kind = ? AND value = ?",
            self._key(value),
        ).fetchone()
        return row is not None

    def add(self, value: IdentityValue) -> None:
        if self.contains(value):
            return
        if self._count >= self._maximum:
            from b24api.error import BudgetExceededError  # noqa: PLC0415

            raise BudgetExceededError("identity tracking budget exhausted")
        self._connection.execute("INSERT INTO identities(kind, value) VALUES (?, ?)", self._key(value))
        self._count += 1

    def close(self) -> None:
        self._connection.close()


class _MonotonicIdentityStore:
    def __init__(self, maximum: int) -> None:
        self._maximum = maximum
        self._count = 0
        self._last: IdentityValue | None = None

    @property
    def count(self) -> int:
        return self._count

    def contains(self, value: IdentityValue) -> bool:
        return value == self._last

    def add(self, value: IdentityValue) -> None:
        if value == self._last:
            return
        if self._count >= self._maximum:
            from b24api.error import BudgetExceededError  # noqa: PLC0415

            raise BudgetExceededError("identity tracking budget exhausted")
        self._last = value
        self._count += 1

    def close(self) -> None:
        self._last = None


@dataclass(frozen=True, slots=True)
class _Page:
    items: tuple[JsonValue, ...]
    response: Response
    item_weights: tuple[int, ...]

    @property
    def retained_rows(self) -> int:
        return sum(self.item_weights)


@dataclass(frozen=True, slots=True)
class _EffectiveConsistency:
    duplicate_policy: DuplicatePolicy
    total_semantics: TotalSemantics
    order_direction: str | None
    confirmation_policy: ConfirmationPolicy


class PaginationDriver:
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
        self.batch_report: OperationReport | None = None

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

    async def counted_batch_pages(  # noqa: C901, PLR0912, PLR0915
        self,
        *,
        batch_size: int,
        page_size: int,
    ) -> AsyncGenerator[_Page]:
        """Execute the committed direct-head/batched-tail counted traversal."""
        from b24api.batch import BatchExecutor, BatchStream  # noqa: PLC0415
        from b24api.models import BatchFailure, BatchSuccess  # noqa: PLC0415

        if not isinstance(self.plan, CountedOffsetPlan):
            raise TypeError("counted batch traversal requires CountedOffsetPlan")
        if self.plan.mode is not CountedOffsetMode.SEQUENTIAL_NEXT:
            raise CapabilityError("compatibility counted batch traversal requires the canonical counted plan")
        self.begin_external_validation()
        outcomes: BatchStream | None = None
        try:
            head_reservation = await self.context.reserve_page()
            try:
                head = await self.executor.execute(
                    _request_with_controls(
                        self.request,
                        {self.plan.offset_path: 0},
                        allow_create=self.plan.allow_create_controls,
                    ),
                    context=self.context,
                )
            except BaseException:
                self.context.release_page(head_reservation)
                raise
            self.context.commit_page(head_reservation)
            head_items = _response_items(head, self.selector)
            await self.context.set_buffered_rows(len(head_items))
            total = head.total
            if total is None or total < 0:
                raise CapabilityError("parallel counted traversal requires a non-negative total")
            if total < len(head_items):
                raise CapabilityError("parallel counted traversal observed total below the head page")
            if self.identity is not None and total > self.context.policy.max_tracked_identities:
                raise CapabilityError("parallel counted traversal exceeds the exact identity budget")
            stride = head.next if head.next is not None else page_size
            if isinstance(stride, bool) or stride < 1:
                raise CapabilityError("parallel counted traversal requires a positive in-band stride")
            if total > len(head_items) and head.next is None:
                raise CapabilityError("parallel counted traversal has no in-band tail stride")
            if head.next is not None and head.next != len(head_items):
                raise CapabilityError("parallel counted head continuation contradicts its row count")
            if total == len(head_items) and head.next is not None:
                raise CapabilityError("parallel counted traversal completed while continuation remained")
            tail_pages = (total - 1) // stride if total > len(head_items) else 0
            budget = await self.context.snapshot()
            if budget.counters.logical_pages + tail_pages > self.context.policy.max_pages:
                raise CapabilityError("parallel counted traversal exceeds the logical page budget")
            if tail_pages and stride > self.context.policy.max_buffered_rows:
                raise CapabilityError("parallel counted page exceeds the decoded row buffer budget")
            effective_batch_size = min(
                batch_size,
                max(1, self.context.policy.max_buffered_rows // stride),
            )
            minimum_tail_requests = (tail_pages + effective_batch_size - 1) // effective_batch_size
            if budget.counters.physical_requests + minimum_tail_requests > self.context.policy.max_requests:
                raise CapabilityError("parallel counted traversal exceeds the physical request budget")
            self.validate_external_page(head_items, head)
            yield _Page(tuple(head_items), head, (1,) * len(head_items))
            await self.context.set_buffered_rows(0)
            if total == len(head_items):
                self.finish_external_validation()
                self.terminal_reason = "parallel counted traversal completed"
                return
            requests = (
                _request_with_controls(
                    self.request,
                    {self.plan.offset_path: start},
                    allow_create=self.plan.allow_create_controls,
                )
                for start in range(stride, total, stride)
            )
            outcomes = BatchStream(
                BatchExecutor(self.executor),
                requests,
                batch_size=effective_batch_size,
                tolerant=True,
                with_payload=False,
                fallback_failed="none",
                policy=self.context.policy,
                context=self.context,
                logical_page_per_command=True,
            )

            def validated_outcome(outcome: object) -> tuple[Response, list[JsonValue]]:
                if isinstance(outcome, BatchFailure):
                    if isinstance(outcome.error, BaseException):
                        raise outcome.error
                    raise CapabilityError("parallel counted batch command failed")
                if not isinstance(outcome, BatchSuccess) or outcome.response is None:
                    raise CapabilityError("parallel counted batch outcome lacks correlated response evidence")
                response = outcome.response
                start = stride * (outcome.command_index + 1)
                items = _response_items(response, self.selector)
                expected_rows = min(stride, total - start)
                if len(items) != expected_rows:
                    raise CapabilityError("parallel counted page length contradicts the planned exact range")
                if response.total is not None and response.total != total:
                    raise CapabilityError("parallel counted page total contradicts the head total")
                expected_next = start + stride if start + stride < total else None
                if response.next != expected_next:
                    raise CapabilityError("parallel counted continuation contradicts the planned exact range")
                self.validate_external_page(items, response)
                return response, items

            primary_error: BaseException | None = None
            try:
                async for outcome in outcomes:
                    response, items = validated_outcome(outcome)
                    yield _Page(tuple(items), response, (1,) * len(items))
            except BaseException as error:
                primary_error = error
                raise
            finally:
                preserve_primary = primary_error is not None and not isinstance(
                    primary_error,
                    asyncio.CancelledError | GeneratorExit,
                )
                try:
                    cleanup_cancellation = await await_cancellation_resistant(outcomes.aclose())
                except BaseException:
                    if not preserve_primary:
                        raise
                else:
                    if cleanup_cancellation is not None and not preserve_primary:
                        raise cleanup_cancellation
                self.batch_report = outcomes.report
            if outcomes.report.state is not TerminalState.COMPLETED:
                raise IncompleteTraversalError(report=outcomes.report)
            if self.validated_rows != total:
                raise CapabilityError("parallel counted traversal did not emit its exact total")
            self.finish_external_validation()
            self.terminal_reason = "parallel counted traversal completed"
        finally:
            self.close_external_validation()

    async def _single(self, plan: SingleResponsePlan) -> AsyncGenerator[_Page]:
        response = await self._fetch(self.request)
        qualified_count = (
            len(response.result)
            if self._single_result_as_item and self.selector.path == () and isinstance(response.result, list)
            else None
        )
        items = (
            [response.result]
            if self._single_result_as_item and self.selector.path == ()
            else _response_items(response, self.selector, single=True)
        )
        if qualified_count is None:
            qualified_count = len(items)
        if plan.reject_continuation and response.next is not None:
            raise CapabilityError("single-response plan observed a continuation")
        if plan.reject_positive_total_over_result and response.total is not None and response.total > qualified_count:
            raise CapabilityError("single-response plan observed a larger qualified total")
        self._validate_page(items, response=response, qualified_count=qualified_count)
        self._validate_terminal_total()
        self.terminal_reason = "single response complete"
        item_weights = (qualified_count,) if self._single_result_as_item else (1,) * len(items)
        yield _Page(tuple(items), response, item_weights)

    async def _offset(self, plan: OffsetSequentialPlan) -> AsyncGenerator[_Page]:
        offset = 0
        self.cursor_state = offset
        visited_offsets: set[int] = set()
        while True:
            if offset in visited_offsets:
                raise PaginationError("offset cycle detected")
            visited_offsets.add(offset)
            updates: dict[ParameterPath, object] = {plan.offset_path: offset}
            if plan.limit_path is not None and plan.requested_page_size is not None:
                updates[plan.limit_path] = plan.requested_page_size
            response = await self._fetch(
                _request_with_controls(
                    self.request,
                    updates,
                    allow_create=plan.allow_create_controls,
                ),
            )
            items = _response_items(response, self.selector)
            self._validate_page(items, response=response)
            terminal = _offset_terminal(
                plan,
                response,
                page_size=len(items),
                accepted=self.validated_rows,
                confirmation=self._confirmation_policy,
            )
            if terminal is not None:
                self._validate_terminal_total()
            if items:
                yield _Page(tuple(items), response, (1,) * len(items))
            if terminal is not None:
                self.terminal_reason = terminal
                return
            next_offset = _next_offset(plan, response, current=offset, observed=len(items))
            if next_offset <= offset:
                raise PaginationError("offset did not advance")
            offset = next_offset
            self.cursor_state = offset

    async def _counted(self, plan: CountedOffsetPlan) -> AsyncGenerator[_Page]:
        offset = 0
        self.cursor_state = offset
        visited_offsets: set[int] = set()
        while True:
            if offset in visited_offsets:
                raise PaginationError("counted offset cycle detected")
            visited_offsets.add(offset)
            updates: dict[ParameterPath, object] = {plan.offset_path: offset}
            if plan.limit_path is not None and plan.requested_page_size is not None:
                updates[plan.limit_path] = plan.requested_page_size
            response = await self._fetch(
                _request_with_controls(
                    self.request,
                    updates,
                    allow_create=plan.allow_create_controls,
                ),
            )
            items = _response_items(response, self.selector)
            self._validate_page(items, response=response)
            if items:
                yield _Page(tuple(items), response, (1,) * len(items))
            if self._expected_total is not None and self.validated_rows == self._expected_total:
                self._validate_terminal_total()
                self.terminal_reason = "qualified total reached"
                return
            if not items:
                raise PaginationError("counted traversal ended before its exact total")
            next_offset = response.next if response.next is not None else offset + len(items)
            if next_offset <= offset:
                raise PaginationError("counted offset did not advance")
            offset = next_offset
            self.cursor_state = offset

    async def _keyset(self, plan: KeysetPlan) -> AsyncGenerator[_Page]:  # noqa: C901
        identity = self._require_identity("keyset")
        cursor: IdentityValue | None = None
        while True:
            updates: dict[ParameterPath, object] = {
                _child_path(plan.order_path, identity.order_key): "ASC" if plan.direction == "asc" else "DESC",
            }
            if plan.limit_path is not None and plan.requested_page_size is not None:
                updates[plan.limit_path] = plan.requested_page_size
            if plan.start_suppression_path is not None:
                updates[plan.start_suppression_path] = -1
            if cursor is not None:
                operator = ">" if plan.direction == "asc" else "<"
                updates[_child_path(plan.filter_path, f"{operator}{identity.filter_key}")] = cursor
            response = await self._fetch(
                _request_with_controls(
                    self.request,
                    updates,
                    allow_create=plan.allow_create_controls,
                ),
            )
            items = _response_items(response, self.selector)
            identities = self._validate_page(items, response=response)
            if cursor is not None and identities:
                if plan.direction == "asc" and _compare_identities(identities[0], cursor) <= 0:
                    raise PaginationError("keyset page ignored its lower bound")
                if plan.direction == "desc" and _compare_identities(identities[0], cursor) >= 0:
                    raise PaginationError("keyset page ignored its upper bound")
            terminal = _keyset_terminal(plan, len(items))
            if terminal is not None:
                self._validate_terminal_total()
            if items:
                yield _Page(tuple(items), response, (1,) * len(items))
            if terminal is not None:
                self.terminal_reason = terminal
                return
            if not identities:
                raise PaginationError("keyset cursor could not advance")
            next_cursor = identities[-1]
            if cursor is not None and next_cursor == cursor:
                raise PaginationError("keyset cursor repeated")
            cursor = next_cursor
            self.cursor_state = cursor

    async def _cursor(self, plan: ItemCursorPlan) -> AsyncGenerator[_Page]:  # noqa: C901, PLR0912
        self._require_identity("item cursor")
        cursor: IdentityValue | None = None
        while True:
            updates: dict[ParameterPath, object] = {}
            if cursor is not None:
                updates[plan.cursor_request_path] = cursor
            if plan.limit_path is not None and plan.requested_page_size is not None:
                updates[plan.limit_path] = plan.requested_page_size
            request = (
                self.request
                if not updates
                else _request_with_controls(
                    self.request,
                    updates,
                    allow_create=plan.allow_create_controls,
                )
            )
            response = await self._fetch(request)
            items = _response_items(response, self.selector)
            self._validate_page(items, response=response)
            cursor_values, cursor_exhausted = _cursor_values(items, plan)
            _validate_order(cursor_values, plan.direction)
            if cursor is not None and cursor_values:
                comparison = _compare_identities(cursor_values[0], cursor)
                if plan.direction == "asc" and comparison <= 0:
                    raise PaginationError("item cursor page ignored its lower bound")
                if plan.direction == "desc" and comparison >= 0:
                    raise PaginationError("item cursor page ignored its upper bound")
            terminal = _cursor_terminal(plan, len(items), cursor_exhausted=cursor_exhausted)
            if terminal is not None:
                self._validate_terminal_total()
            if items:
                yield _Page(tuple(items), response, (1,) * len(items))
            if terminal is not None:
                self.terminal_reason = terminal
                return
            if not cursor_values:
                raise PaginationError("item cursor is absent before terminal confirmation")
            next_cursor = _take_cursor(cursor_values, plan.cursor_take)
            if cursor is not None:
                if next_cursor == cursor:
                    raise PaginationError("item cursor repeated")
                if plan.direction == "asc" and _compare_identities(next_cursor, cursor) < 0:
                    raise PaginationError("item cursor moved in the wrong direction")
                if plan.direction == "desc" and _compare_identities(next_cursor, cursor) > 0:
                    raise PaginationError("item cursor moved in the wrong direction")
            cursor = next_cursor
            self.cursor_state = cursor

    async def _fetch(self, request: Request) -> Response:
        if self._fetch_override is not None:
            return await self._fetch_override(request)
        reservation = await self.context.reserve_page()
        try:
            response = await self.executor.execute(
                request,
                context=self.context,
                work_class=WorkClass.TRAVERSAL_DIRECT,
            )
            self.context.commit_page(reservation)
        except BaseException:
            self.context.release_page(reservation)
            raise
        return response

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
        if isinstance(plan, PartitionedKeysetPlan):
            raise CapabilityError("partitioned keyset requires separate reviewed authorization")
        if isinstance(plan, KeysetPlan) and plan.terminal is KeysetTerminalRule.BOUNDARY_ID_SEEN:
            raise CapabilityError("boundary-id keyset requires an externally reviewed boundary contract")
        if policy.identity_tracker is IdentityTracker.MONOTONIC and not isinstance(plan, KeysetPlan):
            raise CapabilityError("monotonic identity tracking requires identity-ordered keyset traversal")
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
        if isinstance(self.plan, OffsetSequentialPlan | CountedOffsetPlan):
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
            _request_with_controls(self.request, first, allow_create=allow_create)
            _request_with_controls(self.request, second, allow_create=allow_create)

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


class ItemStream(AsyncIterator[JsonValue]):
    """Lazy item traversal stream with deterministic cleanup and final report."""

    def __init__(  # noqa: PLR0913
        self,
        executor: Executor,
        request: Request,
        plan: ListPlan,
        *,
        selector: ResultSelector | None = None,
        identity: IdentitySpec | None = None,
        policy: ExecutionPolicy | None = None,
        page_cap_hint: int | None = None,
        assurance: CompletionAssurance = CompletionAssurance.CALLER_ASSERTED,
        profile_id: str | None = None,
        profile_version: int | None = None,
        profile_source_sha256: str | None = None,
        profile_evidence_sha256: tuple[str, ...] = (),
        profile_evidence_candidate_sha: str | None = None,
    ) -> None:
        """Initialize instance state."""
        PaginationDriver.validate_plan(plan)
        self._context = executor.context(policy)
        self._driver = PaginationDriver(
            executor,
            request,
            plan,
            selector=selector,
            identity=identity,
            context=self._context,
            page_cap_hint=page_cap_hint,
        )
        self._assurance = assurance
        self._profile_id = profile_id
        self._profile_version = profile_version
        self._profile_source_sha256 = profile_source_sha256
        self._profile_evidence_sha256 = profile_evidence_sha256
        self._profile_evidence_candidate_sha = profile_evidence_candidate_sha
        self._runner: AsyncGenerator[tuple[JsonValue, bool]] | None = None
        self._prefetched: tuple[JsonValue, bool] | object = _MISSING
        self._closed = False
        self._emitted = 0
        self._unique_emitted = 0
        self.report = OperationReport(
            assurance=assurance,
            profile_id=profile_id,
            profile_version=profile_version,
            profile_applicable=True if profile_id is not None else None,
            profile_source_sha256=profile_source_sha256,
            profile_evidence_sha256=profile_evidence_sha256,
            profile_evidence_candidate_sha=profile_evidence_candidate_sha,
        )

    def __aiter__(self) -> Self:
        """Return this asynchronous iterator."""
        return self

    async def __anext__(self) -> JsonValue:
        """Return the next asynchronous item."""
        if self._closed:
            raise StopAsyncIteration
        if self._prefetched is not _MISSING:
            item, is_unique = cast("tuple[JsonValue, bool]", self._prefetched)
            self._prefetched = _MISSING
            self._record_delivery(is_unique=is_unique)
            return item
        if self._runner is None:
            self._runner = self._run()
        item, is_unique = await anext(self._runner)
        self._record_delivery(is_unique=is_unique)
        return item

    def _record_delivery(self, *, is_unique: bool) -> None:
        self._emitted += 1
        if is_unique:
            self._unique_emitted += 1

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        if self._closed:
            raise RuntimeError("stream is closed")
        if self._runner is None:
            self._runner = self._run()
            with contextlib.suppress(StopAsyncIteration):
                self._prefetched = await anext(self._runner)
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Exit the asynchronous context."""
        await self.aclose()

    async def aclose(self) -> None:
        """Close owned asynchronous resources."""
        if self._closed:
            return
        self._closed = True
        try:
            if self._runner is not None:
                await self._runner.aclose()
        except BaseException as error:
            if self.report.state is TerminalState.NOT_STARTED:
                cancellation = await await_cancellation_resistant(
                    self._finalize(TerminalState.CANCELLED, "stream cleanup failed"),
                )
                if cancellation is not None:
                    _attach_report(cancellation, self.report)
                    raise cancellation from error
            _attach_report(error, self.report)
            raise
        finally:
            self._prefetched = _MISSING
        if self.report.state is TerminalState.NOT_STARTED and self._runner is not None:
            await self._finalize(TerminalState.CANCELLED, "stream closed before exhaustion")

    async def _run(self) -> AsyncGenerator[tuple[JsonValue, bool]]:  # noqa: C901, PLR0912, PLR0915
        pages = self._driver.pages()
        naturally_exhausted = False
        try:
            async for page in pages:
                buffered = deque(zip(page.items, self._driver.last_page_unique_mask, strict=True))
                await self._context.set_buffered_rows(len(buffered))
                while buffered:
                    item, is_unique = buffered.popleft()
                    await self._context.set_buffered_rows(len(buffered) + 1)
                    yield item, is_unique
                    await self._context.set_buffered_rows(len(buffered))
            naturally_exhausted = True
            await self._finalize(TerminalState.COMPLETED, self._driver.terminal_reason or "terminal confirmed")
        except asyncio.CancelledError as error:
            repeated = await await_cancellation_resistant(
                self._finalize(TerminalState.CANCELLED, "iteration cancelled"),
            )
            if repeated is not None:
                _attach_report(repeated, self.report)
                raise repeated from error
            _attach_report(error, self.report)
            raise
        except GeneratorExit as error:
            cancellation = await await_cancellation_resistant(
                self._finalize(TerminalState.CANCELLED, "stream closed before exhaustion"),
            )
            if cancellation is not None:
                _attach_report(cancellation, self.report)
                raise cancellation from error
            _attach_report(error, self.report)
            raise
        except BaseException as error:
            cancellation = await await_cancellation_resistant(
                self._finalize(TerminalState.FAILED, type(error).__name__),
            )
            if cancellation is not None:
                _attach_report(cancellation, self.report)
                raise cancellation from error
            _attach_report(error, self.report)
            raise
        finally:
            try:
                cleanup_cancellation = await await_cancellation_resistant(self._cleanup_pages(pages))
            except BaseException as cleanup_error:
                if self.report.state is TerminalState.NOT_STARTED:
                    cancellation = await await_cancellation_resistant(
                        self._finalize(TerminalState.CANCELLED, "stream cleanup failed"),
                    )
                    if cancellation is not None:
                        _attach_report(cancellation, self.report)
                        raise cancellation from cleanup_error
                _attach_report(cleanup_error, self.report)
                raise
            if cleanup_cancellation is not None:
                _attach_report(cleanup_cancellation, self.report)
                raise cleanup_cancellation
            if not naturally_exhausted and self.report.state is TerminalState.NOT_STARTED:
                await self._finalize(TerminalState.CANCELLED, "stream abandoned")
            if self.report.state is not TerminalState.NOT_STARTED:
                self._closed = True

    async def _cleanup_pages(self, pages: AsyncGenerator[_Page]) -> None:
        await pages.aclose()
        await self._context.set_buffered_rows(0)

    async def _finalize(self, state: TerminalState, reason: str) -> None:
        if self.report.state is not TerminalState.NOT_STARTED:
            return
        snapshot = await self._context.snapshot()
        consistency = self._context.policy.consistency
        snapshot_state = (
            SnapshotState.NOT_REQUESTED
            if consistency.snapshot_requirement is SnapshotRequirement.TRAVERSAL_ONLY
            else SnapshotState.UNVERIFIED
        )
        violations = tuple(self._driver.violations)
        if state is TerminalState.COMPLETED and snapshot_state is SnapshotState.UNVERIFIED:
            state = TerminalState.INCOMPLETE
            reason = "required snapshot was not verified"
            violations = (
                *violations,
                Violation(
                    severity=ViolationSeverity.BLOCKING,
                    code="snapshot_unverified",
                    message="the requested stable snapshot was not verified",
                ),
            )
        self.report = OperationReport(
            state=state,
            assurance=self._assurance,
            snapshot=snapshot_state,
            plan_id=type(self._driver.plan).__name__,
            dispatch_id="sequential_direct",
            profile_id=self._profile_id,
            profile_version=self._profile_version,
            profile_applicable=True if self._profile_id is not None else None,
            profile_source_sha256=self._profile_source_sha256,
            profile_evidence_sha256=self._profile_evidence_sha256,
            profile_evidence_candidate_sha=self._profile_evidence_candidate_sha,
            emitted_rows=self._emitted,
            unique_rows=self._unique_emitted,
            physical_requests=snapshot.counters.physical_requests,
            logical_pages=snapshot.counters.logical_pages,
            retries=snapshot.retries,
            cooldown_seconds=snapshot.cooldown_seconds,
            buffered_rows_high_water=snapshot.counters.buffered_rows_high_water,
            violations=violations,
            terminal_reason=reason,
        )


def iter_list(  # noqa: PLR0913
    executor: Executor,
    request: Request,
    *,
    plan: ListPlan,
    selector: ResultSelector | None = None,
    identity: IdentitySpec | None = None,
    policy: ExecutionPolicy | None = None,
    _page_cap_hint: int | None = None,
    _assurance: CompletionAssurance = CompletionAssurance.CALLER_ASSERTED,
    _profile_id: str | None = None,
    _profile_version: int | None = None,
    _profile_source_sha256: str | None = None,
    _profile_evidence_sha256: tuple[str, ...] = (),
    _profile_evidence_candidate_sha: str | None = None,
) -> ItemStream:
    """Construct a lazy canonical item stream without performing I/O."""
    return ItemStream(
        executor,
        request,
        plan,
        selector=selector,
        identity=identity,
        policy=policy,
        page_cap_hint=_page_cap_hint,
        assurance=_assurance,
        profile_id=_profile_id,
        profile_version=_profile_version,
        profile_source_sha256=_profile_source_sha256,
        profile_evidence_sha256=_profile_evidence_sha256,
        profile_evidence_candidate_sha=_profile_evidence_candidate_sha,
    )


def _attach_report(error: BaseException, report: OperationReport) -> None:
    with contextlib.suppress(AttributeError, TypeError):
        error.report = report  # type: ignore[attr-defined]


def _request_with_controls(
    request: Request,
    updates: dict[ParameterPath, object],
    *,
    allow_create: bool,
) -> Request:
    try:
        parameters = inject_controls(request.copy_parameters(), updates, allow_create=allow_create)
    except (KeyError, TypeError, ValueError) as error:
        raise CapabilityError("request parameters conflict with required traversal controls") from error
    return Request(request.method, parameters, request.replay_safety)


def _child_path(parent: ParameterPath, child: str) -> ParameterPath:
    return ParameterPath((*parent.path, child))


class _LegacyResultSelector(ResultSelector):
    """Internal marker for the committed one-key result fallback."""


_LEGACY_RESULT_SELECTOR = _LegacyResultSelector.root()


def _response_items(response: Response, selector: ResultSelector, *, single: bool = False) -> list[JsonValue]:
    if isinstance(selector, _LegacyResultSelector):
        try:
            return response.list_result
        except TypeError as error:
            raise CapabilityError("response result is not an unambiguous legacy list") from error
    if single and selector.path == () and not isinstance(response.result, list):
        return [response.result]
    try:
        return response.list_items(selector)
    except (KeyError, TypeError) as error:
        raise CapabilityError("response result does not satisfy the declared selector") from error


def _page_fingerprint(items: Iterable[JsonValue]) -> str:
    canonical = json.dumps(
        list(items),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _effective_duplicate_policy(plan: DuplicatePolicy, policy: DuplicatePolicy) -> DuplicatePolicy:
    strength = {
        DuplicatePolicy.ALLOW_DECLARED_MULTISET: 0,
        DuplicatePolicy.REPORT: 1,
        DuplicatePolicy.ERROR: 2,
    }
    return plan if strength[plan] >= strength[policy] else policy


def _effective_total_semantics(plan: TotalSemantics, policy: TotalSemantics) -> TotalSemantics:
    if plan is policy or policy is TotalSemantics.IGNORE:
        return plan
    if plan is TotalSemantics.IGNORE:
        return policy
    if policy is TotalSemantics.ADVISORY:
        return plan
    if plan is TotalSemantics.ADVISORY:
        return policy
    raise CapabilityError("plan and consistency policy declare incompatible total semantics")


def _effective_order_direction(plan: OrderSemantics, policy: OrderSemantics) -> str | None:
    if OrderSemantics.INPUT in {plan, policy}:
        raise CapabilityError("input order semantics are not an item traversal contract")
    declared = {value for value in (plan, policy) if value in {OrderSemantics.ASCENDING, OrderSemantics.DESCENDING}}
    if len(declared) > 1:
        raise CapabilityError("plan and consistency policy declare incompatible order semantics")
    if not declared:
        return None
    return "asc" if declared.pop() is OrderSemantics.ASCENDING else "desc"


def _validate_confirmation_policy(
    plan: ListPlan,
    policy: ConfirmationPolicy,
    total_semantics: TotalSemantics,
) -> None:
    if policy is ConfirmationPolicy.NONE:
        return
    if policy is ConfirmationPolicy.INDEPENDENT_ORACLE:
        raise CapabilityError("independent oracle confirmation is unavailable inside traversal")
    if policy is ConfirmationPolicy.QUALIFIED_TOTAL:
        if total_semantics is not TotalSemantics.FILTERED_EXACT:
            raise CapabilityError("qualified-total confirmation requires filtered exact total semantics")
        return
    if policy is ConfirmationPolicy.EMPTY_AFTER_BOUNDARY:
        empty_confirmed = (
            (isinstance(plan, OffsetSequentialPlan) and OffsetTerminalRule.EMPTY_PAGE in plan.terminal)
            or (isinstance(plan, KeysetPlan) and plan.terminal is KeysetTerminalRule.EMPTY_CONFIRMATION)
            or (isinstance(plan, ItemCursorPlan) and plan.terminal is CursorTerminalRule.EMPTY_CONFIRMATION)
        )
        if not empty_confirmed:
            raise CapabilityError("plan does not provide the requested empty-boundary confirmation")
        return
    if policy is ConfirmationPolicy.BOUNDARY_ID_SEEN:
        if not isinstance(plan, KeysetPlan) or plan.terminal is not KeysetTerminalRule.BOUNDARY_ID_SEEN:
            raise CapabilityError("plan does not provide the requested boundary identity confirmation")
        return
    raise AssertionError("unhandled confirmation policy")


def _extract_path(value: JsonValue, path: tuple[str | int, ...]) -> JsonValue:
    current = value
    for part in path:
        if isinstance(part, str):
            if not isinstance(current, dict) or part not in current:
                raise PaginationError(f"identity path is missing: {path!r}")
            current = current[part]
        else:
            if not isinstance(current, list) or part >= len(current):
                raise PaginationError(f"identity path is missing: {path!r}")
            current = current[part]
    return current


def _coerce_identity(value: JsonValue, coercion: IdentityCoercion) -> IdentityValue:
    if coercion is IdentityCoercion.EXACT_STRING:
        if not isinstance(value, str):
            raise PaginationError("identity must be an exact string")
        return value
    if coercion is IdentityCoercion.EXACT_INTEGER:
        if not isinstance(value, int) or isinstance(value, bool):
            raise PaginationError("identity must be an exact integer")
        return value
    if isinstance(value, bool) or not isinstance(value, str | int):
        raise PaginationError("identity must be a decimal string or integer")
    try:
        return int(value)
    except ValueError as error:
        raise PaginationError("identity is not a decimal integer") from error


def _validate_order(values: list[IdentityValue], direction: str | None) -> None:
    for previous, current in itertools.pairwise(values):
        comparison = _compare_identities(current, previous)
        if direction == "asc" and comparison <= 0:
            raise PaginationError("page identities are not strictly ascending")
        if direction == "desc" and comparison >= 0:
            raise PaginationError("page identities are not strictly descending")


def _compare_identities(left: IdentityValue, right: IdentityValue) -> int:
    if type(left) is not type(right):
        raise PaginationError("identity values are not mutually orderable")
    if left == right:
        return 0
    if isinstance(left, int) and isinstance(right, int):
        return 1 if left > right else -1
    if isinstance(left, str) and isinstance(right, str):
        return 1 if left > right else -1
    raise PaginationError("identity values are not mutually orderable")


def _offset_terminal(
    plan: OffsetSequentialPlan,
    response: Response,
    *,
    page_size: int,
    accepted: int,
    confirmation: ConfirmationPolicy,
) -> str | None:
    if page_size == 0 and OffsetTerminalRule.EMPTY_PAGE in plan.terminal:
        return "empty page confirmed terminal"
    if confirmation is ConfirmationPolicy.EMPTY_AFTER_BOUNDARY:
        return None
    if (
        OffsetTerminalRule.QUALIFIED_TOTAL in plan.terminal
        and response.total is not None
        and response.total >= 0
        and accepted == response.total
        and response.next is None
    ):
        return "qualified total reached"
    if OffsetTerminalRule.PROFILE_ABSENT_NEXT in plan.terminal and response.next is None:
        return "profile-authorized absent continuation"
    if (
        OffsetTerminalRule.PROFILE_SHORT_PAGE in plan.terminal
        and plan.requested_page_size is not None
        and page_size < plan.requested_page_size
    ):
        return "profile-authorized short page"
    return None


def _next_offset(plan: OffsetSequentialPlan, response: Response, *, current: int, observed: int) -> int:
    if plan.continuation is OffsetContinuation.SERVER_NEXT:
        if response.next is None:
            raise PaginationError("server-next traversal has no continuation")
        return response.next
    if plan.continuation is OffsetContinuation.SERVER_NEXT_OR_OBSERVED_COUNT and response.next is not None:
        return response.next
    return current + observed


def _keyset_terminal(plan: KeysetPlan, page_size: int) -> str | None:
    if plan.terminal is KeysetTerminalRule.EMPTY_CONFIRMATION and page_size == 0:
        return "empty keyset confirmation"
    if (
        plan.terminal is KeysetTerminalRule.PROFILE_SHORT_PAGE
        and plan.requested_page_size is not None
        and page_size < plan.requested_page_size
    ):
        return "profile-authorized short keyset page"
    return None


def _cursor_terminal(plan: ItemCursorPlan, page_size: int, *, cursor_exhausted: bool) -> str | None:
    if plan.terminal is CursorTerminalRule.EMPTY_CONFIRMATION and page_size == 0:
        return "empty cursor confirmation"
    if (
        plan.terminal is CursorTerminalRule.PROFILE_SHORT_PAGE
        and plan.requested_page_size is not None
        and page_size < plan.requested_page_size
    ):
        return "profile-authorized short cursor page"
    if plan.terminal is CursorTerminalRule.PROFILE_CURSOR_EXHAUSTED and cursor_exhausted:
        return "profile-authorized cursor exhaustion"
    return None


def _cursor_values(items: list[JsonValue], plan: ItemCursorPlan) -> tuple[list[IdentityValue], bool]:
    raw_values = [_extract_optional_path(item, plan.cursor_item_path) for item in items]
    exhausted = [value is _MISSING or value is None for value in raw_values]
    if plan.terminal is CursorTerminalRule.PROFILE_CURSOR_EXHAUSTED and (not raw_values or all(exhausted)):
        return [], True
    if any(exhausted):
        if plan.terminal is CursorTerminalRule.PROFILE_CURSOR_EXHAUSTED:
            raise PaginationError("cursor exhaustion is inconsistent within the page")
        raise PaginationError(f"cursor path is missing: {plan.cursor_item_path!r}")
    return [_coerce_cursor(cast("JsonValue", value), plan.cursor_coercion) for value in raw_values], False


def _extract_optional_path(value: JsonValue, path: tuple[str | int, ...]) -> JsonValue | object:
    current = value
    for part in path:
        if isinstance(part, str):
            if not isinstance(current, dict) or part not in current:
                return _MISSING
            current = current[part]
        else:
            if not isinstance(current, list) or part >= len(current):
                return _MISSING
            current = current[part]
    return current


def _coerce_cursor(value: JsonValue, coercion: IdentityCoercion) -> IdentityValue:
    try:
        return _coerce_identity(value, coercion)
    except PaginationError as error:
        raise PaginationError("cursor value does not satisfy cursor_coercion") from error


def _take_cursor(values: list[IdentityValue], mode: str) -> IdentityValue:
    if mode == "first":
        return values[0]
    if mode == "last":
        return values[-1]
    if any(type(value) is not type(values[0]) for value in values):
        raise PaginationError("cursor values are not mutually orderable")
    return min(values) if mode == "min" else max(values)


def _identity_store(policy: ExecutionPolicy, plan: ListPlan, identity: IdentitySpec | None) -> _IdentityStore:
    if identity is None:
        return _MemoryIdentityStore(policy.max_tracked_identities)
    if policy.identity_tracker is IdentityTracker.SQLITE:
        return _SqliteIdentityStore(policy.max_tracked_identities)
    if policy.identity_tracker is IdentityTracker.MONOTONIC:
        if not isinstance(plan, KeysetPlan):
            raise CapabilityError("monotonic identity tracking requires identity-ordered keyset traversal")
        return _MonotonicIdentityStore(policy.max_tracked_identities)
    return _MemoryIdentityStore(policy.max_tracked_identities)


__all__ = ["ItemStream", "PaginationDriver", "iter_list"]
