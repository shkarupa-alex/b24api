"""Lazy correctness-first sequential traversal streams and state machines."""

from __future__ import annotations
import contextlib
import hashlib
import itertools
import json
import warnings
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Protocol, cast

from b24api.error import CapabilityError, PaginationError
from b24api.models import (
    ConfirmationPolicy,
    DuplicatePolicy,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    JsonValue,
    OperationReport,
    OrderSemantics,
    ParameterPath,
    Request,
    Response,
    ResultSelector,
    TotalSemantics,
    inject_controls,
)
from b24api.plans import (
    CountedOffsetPlan,
    CursorTerminalRule,
    ItemCursorPlan,
    KeysetPlan,
    KeysetTerminalRule,
    ListPlan,
    OffsetContinuation,
    OffsetSequentialPlan,
    OffsetTerminalRule,
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
)


class _IdentityStore(Protocol):
    @property
    def count(self) -> int: ...

    def contains(self, value: IdentityValue) -> bool: ...

    def add(self, value: IdentityValue) -> None: ...

    def close(self) -> None: ...


_LARGE_IDENTITY_WARNING_THRESHOLD = 100_000


class _MemoryIdentityStore:
    def __init__(self) -> None:
        self._values: set[IdentityValue] = set()
        self._warned = False

    @property
    def count(self) -> int:
        return len(self._values)

    def contains(self, value: IdentityValue) -> bool:
        return value in self._values

    def add(self, value: IdentityValue) -> None:
        if value in self._values:
            return
        self._values.add(value)
        if not self._warned and len(self._values) > _LARGE_IDENTITY_WARNING_THRESHOLD:
            warnings.warn(
                "exact duplicate/loss detection continues in memory; a very large result may consume "
                "additional memory or run more slowly",
                RuntimeWarning,
                stacklevel=3,
            )
            self._warned = True

    def close(self) -> None:
        self._values.clear()


class _MonotonicIdentityStore:
    def __init__(self) -> None:
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


class _MappingValuesResultSelector(ResultSelector):
    """Internal marker for explicit ordered mapping-value selection."""


_LEGACY_RESULT_SELECTOR = _LegacyResultSelector.root()


def _mapping_values(response: Response, selector: ResultSelector) -> list[JsonValue]:
    selected: JsonValue = response.result
    for part in selector.path:
        if isinstance(part, str):
            if not isinstance(selected, dict) or part not in selected:
                raise CapabilityError("response result does not satisfy the declared selector")
            selected = selected[part]
        else:
            if not isinstance(selected, list) or part >= len(selected):
                raise CapabilityError("response result does not satisfy the declared selector")
            selected = selected[part]
    if not isinstance(selected, dict):
        raise CapabilityError("mapping_values collection shape requires a selected mapping")
    return list(selected.values())


def _response_items(response: Response, selector: ResultSelector, *, single: bool = False) -> list[JsonValue]:
    if isinstance(selector, _LegacyResultSelector):
        try:
            return response.list_result
        except TypeError as error:
            raise CapabilityError("response result is not an unambiguous legacy list") from error
    if isinstance(selector, _MappingValuesResultSelector):
        return _mapping_values(response, selector)
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
    return None


def _cursor_terminal(plan: ItemCursorPlan, page_size: int) -> str | None:
    if plan.terminal is CursorTerminalRule.EMPTY_CONFIRMATION and page_size == 0:
        return "empty cursor confirmation"
    return None


def _cursor_values(items: list[JsonValue], plan: ItemCursorPlan) -> list[IdentityValue]:
    raw_values = [_extract_optional_path(item, plan.cursor_item_path) for item in items]
    exhausted = [value is _MISSING or value is None for value in raw_values]
    if any(exhausted):
        raise PaginationError(f"cursor path is missing: {plan.cursor_item_path!r}")
    return [_coerce_cursor(cast("JsonValue", value), plan.cursor_coercion) for value in raw_values]


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


def _identity_store(_policy: ExecutionPolicy, plan: ListPlan, identity: IdentitySpec | None) -> _IdentityStore:
    if isinstance(plan, KeysetPlan) or (
        isinstance(plan, ItemCursorPlan)
        and identity is not None
        and identity.item_path == plan.cursor_item_path
        and identity.coercion is plan.cursor_coercion
    ):
        return _MonotonicIdentityStore()
    return _MemoryIdentityStore()
