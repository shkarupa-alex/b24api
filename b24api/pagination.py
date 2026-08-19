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

from b24api.error import CapabilityError, PaginationError
from b24api.execution import ExecutionContext, Executor, WorkClass
from b24api.models import (
    CompletionAssurance,
    DuplicatePolicy,
    ExecutionPolicy,
    IdentityCoercion,
    IdentityRequirement,
    IdentitySpec,
    IdentityTracker,
    JsonValue,
    OperationReport,
    ParameterPath,
    ReplaySafety,
    Request,
    Response,
    ResultSelector,
    SnapshotRequirement,
    SnapshotState,
    TerminalState,
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
    ) -> None:
        self.executor = executor
        self.request = _traversal_request(request)
        self.plan = plan
        self.selector = plan.selector or selector or ResultSelector.root()
        self.identity = identity
        self.context = context
        self._fetch_override = fetch
        self._single_result_as_item = single_result_as_item
        self.terminal_reason: str | None = None
        self.cursor_state: JsonValue = None
        self.violations: list[Violation] = []
        self.validated_rows = 0
        self._fingerprints: set[str] = set()
        self._identity_store: _IdentityStore | None = None
        self._unique_rows_final: int | None = None
        self._last_identity: IdentityValue | None = None

    async def pages(self) -> AsyncGenerator[_Page]:  # noqa: C901
        self._validate_capabilities()
        self._identity_store = _identity_store(self.context.policy, self.plan, self.identity)
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
            if self._identity_store is not None:
                self._unique_rows_final = self._identity_store.count
                self._identity_store.close()

    async def _single(self, plan: SingleResponsePlan) -> AsyncGenerator[_Page]:
        response = await self._fetch(self.request)
        items = (
            [response.result]
            if self._single_result_as_item and self.selector.path == ()
            else _response_items(response, self.selector, single=True)
        )
        if plan.reject_continuation and response.next is not None:
            raise CapabilityError("single-response plan observed a continuation")
        if plan.reject_positive_total_over_result and response.total is not None and response.total > len(items):
            raise CapabilityError("single-response plan observed a larger qualified total")
        self._validate_page(items, response=response, ordered=False)
        self.terminal_reason = "single response complete"
        yield _Page(tuple(items), response)

    async def _offset(self, plan: OffsetSequentialPlan) -> AsyncGenerator[_Page]:  # noqa: C901
        offset = 0
        self.cursor_state = offset
        expected_total: int | None = None
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
            if OffsetTerminalRule.QUALIFIED_TOTAL in plan.terminal:
                if response.total is None or response.total < 0:
                    raise CapabilityError("qualified-total traversal requires a non-negative exact total")
                if expected_total is None:
                    expected_total = response.total
                elif response.total != expected_total:
                    raise PaginationError("offset traversal total drifted")
            self._validate_page(items, response=response, ordered=False)
            if expected_total is not None and self.validated_rows > expected_total:
                raise PaginationError("offset traversal exceeded its exact total")
            terminal = _offset_terminal(plan, response, page_size=len(items), accepted=self.validated_rows)
            if terminal is not None and expected_total is not None and self.validated_rows != expected_total:
                raise PaginationError("offset traversal terminated before its exact total")
            if items:
                yield _Page(tuple(items), response)
            if terminal is not None:
                self.terminal_reason = terminal
                return
            next_offset = _next_offset(plan, response, current=offset, observed=len(items))
            if next_offset <= offset:
                raise PaginationError("offset did not advance")
            offset = next_offset
            self.cursor_state = offset

    async def _counted(self, plan: CountedOffsetPlan) -> AsyncGenerator[_Page]:  # noqa: C901
        offset = 0
        self.cursor_state = offset
        expected_total: int | None = None
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
            if response.total is None or response.total < 0:
                raise CapabilityError("counted traversal requires a non-negative filtered exact total")
            if expected_total is None:
                expected_total = response.total
            elif response.total != expected_total:
                raise PaginationError("counted traversal total drifted")
            self._validate_page(items, response=response, ordered=False)
            if self.validated_rows > expected_total:
                raise PaginationError("counted traversal exceeded its exact total")
            if items:
                yield _Page(tuple(items), response)
            if self.validated_rows == expected_total:
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
            identities = self._validate_page(items, response=response, ordered=True, direction=plan.direction)
            if cursor is not None and identities:
                if plan.direction == "asc" and _compare_identities(identities[0], cursor) <= 0:
                    raise PaginationError("keyset page ignored its lower bound")
                if plan.direction == "desc" and _compare_identities(identities[0], cursor) >= 0:
                    raise PaginationError("keyset page ignored its upper bound")
            if items:
                yield _Page(tuple(items), response)
            terminal = _keyset_terminal(plan, len(items))
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
        identity = self._require_identity("item cursor")
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
            self._validate_page(items, response=response, ordered=False)
            cursor_values = [
                _coerce_identity(_extract_path(item, plan.cursor_item_path), identity.coercion) for item in items
            ]
            _validate_order(cursor_values, plan.direction)
            if cursor is not None and cursor_values:
                comparison = _compare_identities(cursor_values[0], cursor)
                if plan.direction == "asc" and comparison <= 0:
                    raise PaginationError("item cursor page ignored its lower bound")
                if plan.direction == "desc" and comparison >= 0:
                    raise PaginationError("item cursor page ignored its upper bound")
            if items:
                yield _Page(tuple(items), response)
            terminal = _cursor_terminal(plan, len(items), has_cursor=bool(cursor_values))
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
        await self.context.reserve_page()
        return await self.executor.execute(
            request,
            context=self.context,
            work_class=WorkClass.TRAVERSAL_DIRECT,
        )

    def _require_identity(self, plan_name: str) -> IdentitySpec:
        if self.identity is None:
            raise CapabilityError(f"{plan_name} traversal requires IdentitySpec")
        return self.identity

    def _validate_capabilities(self) -> None:
        if self.plan.identity_requirement is IdentityRequirement.COMPOSITE:
            raise CapabilityError("composite identity requires a separately reviewed identity contract")
        if self.plan.identity_requirement is IdentityRequirement.REQUIRED and self.identity is None:
            raise CapabilityError("plan requires IdentitySpec")
        if isinstance(self.plan, CountedOffsetPlan) and self.plan.mode is CountedOffsetMode.PARALLEL_FIXED_STRIDE:
            raise CapabilityError("parallel fixed-stride counted traversal requires separate reviewed authorization")
        if isinstance(self.plan, PartitionedKeysetPlan):
            raise CapabilityError("partitioned keyset requires separate reviewed authorization")
        if isinstance(self.plan, KeysetPlan) and self.plan.terminal is KeysetTerminalRule.BOUNDARY_ID_SEEN:
            raise CapabilityError("boundary-id keyset requires an externally reviewed boundary contract")
        if self.context.policy.identity_tracker is IdentityTracker.MONOTONIC and not isinstance(self.plan, KeysetPlan):
            raise CapabilityError("monotonic identity tracking requires identity-ordered keyset traversal")
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
        ordered: bool,
        direction: str | None = None,
    ) -> list[IdentityValue]:
        requested_page_size = getattr(self.plan, "requested_page_size", None)
        if requested_page_size is not None and len(items) > requested_page_size:
            raise PaginationError("response exceeded the requested page cap")
        fingerprint = _page_fingerprint(items, response)
        if fingerprint in self._fingerprints:
            raise PaginationError("repeated page fingerprint detected")
        self._fingerprints.add(fingerprint)
        if self.identity is None:
            self.validated_rows += len(items)
            return []
        identities = [
            _coerce_identity(_extract_path(item, self.identity.item_path), self.identity.coercion) for item in items
        ]
        if ordered:
            _validate_order(identities, direction)
            if self._last_identity is not None and identities:
                if direction == "asc" and _compare_identities(identities[0], self._last_identity) <= 0:
                    raise PaginationError("identity order did not advance")
                if direction == "desc" and _compare_identities(identities[0], self._last_identity) >= 0:
                    raise PaginationError("identity order did not advance")
        local: set[IdentityValue] = set()
        duplicates: list[IdentityValue] = []
        for value in identities:
            if value in local or self._store.contains(value):
                duplicates.append(value)
            local.add(value)
        if duplicates and self.plan.duplicate_policy is DuplicatePolicy.ERROR:
            raise PaginationError("duplicate identity detected")
        if duplicates and self.plan.duplicate_policy is DuplicatePolicy.REPORT:
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
        self.validated_rows += len(items)
        return identities

    @property
    def unique_rows(self) -> int:
        if self.identity is None:
            return self.validated_rows
        if self._unique_rows_final is not None:
            return self._unique_rows_final
        if self._identity_store is None:
            return 0
        return self._store.count

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
    ) -> None:
        if not isinstance(plan, _PLAN_TYPES):
            raise TypeError("plan must be a canonical ListPlan")
        self._context = executor.context(policy)
        self._driver = PaginationDriver(
            executor,
            request,
            plan,
            selector=selector,
            identity=identity,
            context=self._context,
        )
        self._assurance = CompletionAssurance.CALLER_ASSERTED
        self._runner: AsyncGenerator[JsonValue] | None = None
        self._prefetched: JsonValue | object = _MISSING
        self._closed = False
        self._emitted = 0
        self.report = OperationReport()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> JsonValue:
        if self._closed:
            raise StopAsyncIteration
        if self._prefetched is not _MISSING:
            item = cast("JsonValue", self._prefetched)
            self._prefetched = _MISSING
            self._emitted += 1
            return item
        if self._runner is None:
            self._runner = self._run()
        item = await anext(self._runner)
        self._emitted += 1
        return item

    async def __aenter__(self) -> Self:
        if self._closed:
            raise RuntimeError("stream is closed")
        if self._runner is None:
            self._runner = self._run()
            with contextlib.suppress(StopAsyncIteration):
                self._prefetched = await anext(self._runner)
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._runner is not None:
            await self._runner.aclose()
        self._prefetched = _MISSING
        if self.report.state is TerminalState.NOT_STARTED and self._runner is not None:
            await self._finalize(TerminalState.CANCELLED, "stream closed before exhaustion")

    async def _run(self) -> AsyncGenerator[JsonValue]:
        pages = self._driver.pages()
        naturally_exhausted = False
        try:
            async for page in pages:
                buffered = deque(page.items)
                await self._context.set_buffered_rows(len(buffered))
                while buffered:
                    item = buffered.popleft()
                    await self._context.set_buffered_rows(len(buffered) + 1)
                    yield item
                    await self._context.set_buffered_rows(len(buffered))
            naturally_exhausted = True
            await self._finalize(TerminalState.COMPLETED, self._driver.terminal_reason or "terminal confirmed")
        except asyncio.CancelledError as error:
            await self._finalize(TerminalState.CANCELLED, "iteration cancelled")
            _attach_report(error, self.report)
            raise
        except GeneratorExit as error:
            await self._finalize(TerminalState.CANCELLED, "stream closed before exhaustion")
            _attach_report(error, self.report)
            raise
        except BaseException as error:
            await self._finalize(TerminalState.FAILED, type(error).__name__)
            _attach_report(error, self.report)
            raise
        finally:
            await pages.aclose()
            await self._context.set_buffered_rows(0)
            if not naturally_exhausted and self.report.state is TerminalState.NOT_STARTED:
                await self._finalize(TerminalState.CANCELLED, "stream abandoned")
            if self.report.state is not TerminalState.NOT_STARTED:
                self._closed = True

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
            emitted_rows=self._emitted,
            unique_rows=min(self._emitted, self._driver.unique_rows),
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
) -> ItemStream:
    """Construct a lazy canonical item stream without performing I/O."""
    return ItemStream(
        executor,
        request,
        plan,
        selector=selector,
        identity=identity,
        policy=policy,
    )


def _traversal_request(request: Request) -> Request:
    if request.replay_safety is not None:
        return request
    return Request(request.method, request.copy_parameters(), ReplaySafety.SAFE)


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


def _response_items(response: Response, selector: ResultSelector, *, single: bool = False) -> list[JsonValue]:
    if single and selector.path == () and not isinstance(response.result, list):
        return [response.result]
    try:
        return response.list_items(selector)
    except (KeyError, TypeError) as error:
        raise CapabilityError("response result does not satisfy the declared selector") from error


def _page_fingerprint(items: Iterable[JsonValue], response: Response) -> str:
    canonical = json.dumps(
        {"items": list(items), "next": response.next, "total": response.total},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


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
) -> str | None:
    if page_size == 0 and OffsetTerminalRule.EMPTY_PAGE in plan.terminal:
        return "empty page confirmed terminal"
    if (
        OffsetTerminalRule.QUALIFIED_TOTAL in plan.terminal
        and response.total is not None
        and response.total >= 0
        and accepted == response.total
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


def _cursor_terminal(plan: ItemCursorPlan, page_size: int, *, has_cursor: bool) -> str | None:
    if plan.terminal is CursorTerminalRule.EMPTY_CONFIRMATION and page_size == 0:
        return "empty cursor confirmation"
    if (
        plan.terminal is CursorTerminalRule.PROFILE_SHORT_PAGE
        and plan.requested_page_size is not None
        and page_size < plan.requested_page_size
    ):
        return "profile-authorized short cursor page"
    if plan.terminal is CursorTerminalRule.PROFILE_CURSOR_EXHAUSTED and not has_cursor:
        return "profile-authorized cursor exhaustion"
    return None


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
