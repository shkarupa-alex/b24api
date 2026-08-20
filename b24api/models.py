# ruff: noqa: ANN401
"""Immutable public values shared by execution and evidence layers."""

from __future__ import annotations
import math
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any, cast

from b24api.query import build_query
from b24api.redaction import DEFAULT_REDACTOR, Redactor

type JsonScalar = None | bool | int | float | str
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type FrozenJson = JsonScalar | tuple[FrozenJson, ...] | FrozenMapping

_METHOD_RE = re.compile(r"^[A-Za-z0-9_.]+$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
HTTP_STATUS_MINIMUM = 100
HTTP_STATUS_MAXIMUM = 599
VIOLATION_CODE_MAXIMUM = 100
VIOLATION_MESSAGE_MAXIMUM = 500
STABLE_KEY_MAXIMUM = 100


class FrozenMapping(Mapping[str, FrozenJson]):
    """Private immutable mapping used for canonical JSON storage."""

    __slots__ = ("_values",)

    def __init__(self, values: Mapping[str, FrozenJson]) -> None:
        self._values = dict(values)

    def __getitem__(self, key: str) -> FrozenJson:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)


def _freeze_json(value: object, *, active: set[int] | None = None) -> FrozenJson:
    active = active if active is not None else set()
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON numbers must be finite")
        return value
    if isinstance(value, Mapping):
        identity = id(value)
        if identity in active:
            raise ValueError("cyclic JSON mappings are not supported")
        active.add(identity)
        try:
            frozen: dict[str, FrozenJson] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError("JSON object keys must be strings")
                frozen[key] = _freeze_json(item, active=active)
            return FrozenMapping(frozen)
        finally:
            active.remove(identity)
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray | str):
        identity = id(value)
        if identity in active:
            raise ValueError("cyclic JSON arrays are not supported")
        active.add(identity)
        try:
            return tuple(_freeze_json(item, active=active) for item in value)
        finally:
            active.remove(identity)
    raise TypeError(f"unsupported JSON value type: {type(value).__name__}")


def _thaw_json(value: FrozenJson) -> JsonValue:
    if isinstance(value, FrozenMapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _is_plain_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


@dataclass(frozen=True, slots=True)
class RequestSummary:
    """Bounded request identity that intentionally excludes parameter values."""

    method: str
    parameter_keys: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "method", DEFAULT_REDACTOR.redact_text(self.method))
        object.__setattr__(
            self,
            "parameter_keys",
            tuple(DEFAULT_REDACTOR.redact_text(str(key)) for key in self.parameter_keys[: DEFAULT_REDACTOR.max_items]),
        )

    def to_dict(self) -> dict[str, object]:
        return {"method": self.method, "parameter_keys": list(self.parameter_keys)}


@dataclass(frozen=True, slots=True)
class ResponseEvidence:
    """Bounded redacted HTTP evidence safe for default serialization."""

    http_status: int | None = None
    request_id: str | None = None
    headers: tuple[tuple[str, str], ...] = ()
    body_preview: str | None = None

    def __post_init__(self) -> None:
        redacted_headers = DEFAULT_REDACTOR.redact(dict(self.headers))
        object.__setattr__(
            self,
            "headers",
            tuple(sorted((str(key), str(value)) for key, value in redacted_headers.items()))[:50],
        )
        if self.request_id is not None:
            object.__setattr__(self, "request_id", DEFAULT_REDACTOR.redact_text(self.request_id))
        if self.body_preview is not None:
            object.__setattr__(self, "body_preview", DEFAULT_REDACTOR.redact_text(self.body_preview))

    def to_dict(self) -> dict[str, object]:
        return {
            "http_status": self.http_status,
            "request_id": self.request_id,
            "headers": dict(self.headers),
            "body_preview": self.body_preview,
        }


@dataclass(frozen=True, slots=True)
class BatchCommandEvidence:
    """Safe correlation facts for one batch command."""

    command_index: int
    stable_key: str
    original_code: str | int | None = None
    normalized_code: str | None = None

    def __post_init__(self) -> None:
        if not _is_plain_int(self.command_index) or self.command_index < 0:
            raise ValueError("command_index cannot be negative")
        if not self.stable_key or len(self.stable_key) > STABLE_KEY_MAXIMUM:
            raise ValueError("stable_key must be 1..100 characters")

    def to_dict(self) -> dict[str, object]:
        return {
            "command_index": self.command_index,
            "stable_key": self.stable_key,
            "original_code": self.original_code,
            "normalized_code": self.normalized_code,
        }


def summarize_request(
    method: object,
    parameters: object = None,
    *,
    redactor: Redactor = DEFAULT_REDACTOR,
) -> RequestSummary:
    """Build a safe summary from canonical or legacy request-like values."""
    safe_method = redactor.redact_text(str(method))
    keys: tuple[str, ...] = ()
    if isinstance(parameters, Mapping):
        keys = tuple(sorted(redactor.redact_text(str(key)) for key in parameters)[: redactor.max_items])
    return RequestSummary(method=safe_method, parameter_keys=keys)


def summarize_request_like(request: Any, *, redactor: Redactor = DEFAULT_REDACTOR) -> RequestSummary | None:
    """Summarize an optional duck-typed request without importing legacy models."""
    if request is None:
        return None
    return summarize_request(
        getattr(request, "method", type(request).__name__),
        getattr(request, "parameters", None),
        redactor=redactor,
    )


class ReplaySafety(StrEnum):
    """Whether repeating a request is proven safe."""

    SAFE = "safe"
    UNSAFE = "unsafe"
    UNKNOWN = "unknown"


class ReplayDisposition(StrEnum):
    """Recorded automatic replay/fallback decision for a failed command."""

    NOT_ELIGIBLE = "not_eligible"
    ELIGIBLE = "eligible"
    REPLAYED_DIRECT = "replayed_direct"
    DIRECT_REPLAY_FAILED = "direct_replay_failed"


class IdentityCoercion(StrEnum):
    """Reproducible identity normalization modes."""

    EXACT_STRING = "exact_string"
    EXACT_INTEGER = "exact_integer"
    DECIMAL_STRING_INTEGER = "decimal_string_integer"


class IdentityRequirement(StrEnum):
    """Identity requirement for a result contract."""

    REQUIRED = "required"
    OPTIONAL = "optional"
    COMPOSITE = "composite"


class OrderSemantics(StrEnum):
    """Declared result ordering semantics."""

    UNORDERED = "unordered"
    ASCENDING = "ascending"
    DESCENDING = "descending"
    INPUT = "input"


class DuplicatePolicy(StrEnum):
    """How duplicate identities affect completion."""

    ERROR = "error"
    ALLOW_DECLARED_MULTISET = "allow_declared_multiset"
    REPORT = "report"


class TotalSemantics(StrEnum):
    """Meaning assigned to a server-provided total."""

    IGNORE = "ignore"
    ADVISORY = "advisory"
    FILTERED_EXACT = "filtered_exact"
    GLOBAL = "global"


class SnapshotRequirement(StrEnum):
    """Required snapshot strength."""

    TRAVERSAL_ONLY = "traversal_only"
    FROZEN_MANIFEST = "frozen_manifest"
    INDEPENDENT_PRE_POST_ORACLE = "independent_pre_post_oracle"


class ConfirmationPolicy(StrEnum):
    """Evidence required to confirm a terminal traversal state."""

    NONE = "none"
    EMPTY_AFTER_BOUNDARY = "empty_after_boundary"
    BOUNDARY_ID_SEEN = "boundary_id_seen"
    QUALIFIED_TOTAL = "qualified_total"
    INDEPENDENT_ORACLE = "independent_oracle"


class TerminalState(StrEnum):
    """Operation lifecycle result."""

    NOT_STARTED = "not_started"
    COMPLETED = "completed"
    INCOMPLETE = "incomplete"
    FAILED = "failed"
    CANCELLED = "cancelled"


class CompletionAssurance(StrEnum):
    """Strength of evidence supporting completion."""

    ORACLE_VERIFIED = "oracle_verified"
    PROFILE_VERIFIED = "profile_verified"
    CALLER_ASSERTED = "caller_asserted"


class SnapshotState(StrEnum):
    """Observed snapshot condition."""

    NOT_REQUESTED = "not_requested"
    VERIFIED = "verified"
    UNVERIFIED = "unverified"
    CHANGED = "changed"


class ViolationSeverity(StrEnum):
    """Whether a violation blocks completion."""

    WARNING = "warning"
    BLOCKING = "blocking"


class IdentityTracker(StrEnum):
    """Exact identity storage strategy."""

    MONOTONIC = "monotonic"
    MEMORY = "memory"
    SQLITE = "sqlite"


PathPart = str | int


@dataclass(frozen=True, slots=True)
class ParameterPath:
    """Exact case-sensitive path to a wire control."""

    path: tuple[PathPart, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", tuple(self.path))
        _validate_path(self.path, allow_empty=False)


@dataclass(frozen=True, slots=True)
class ResultSelector:
    """Exact case-sensitive path to a result value."""

    path: tuple[PathPart, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", tuple(self.path))
        _validate_path(self.path, allow_empty=True)

    @classmethod
    def root(cls) -> ResultSelector:
        return cls(())


@dataclass(frozen=True, slots=True)
class IdentitySpec:
    """Independent item, filter, and order identity roles."""

    item_path: tuple[PathPart, ...]
    filter_key: str
    order_key: str
    coercion: IdentityCoercion = IdentityCoercion.EXACT_STRING

    def __post_init__(self) -> None:
        object.__setattr__(self, "item_path", tuple(self.item_path))
        _validate_path(self.item_path, allow_empty=False)
        if not self.filter_key or not self.order_key:
            raise ValueError("identity filter_key and order_key must be non-empty")
        if not isinstance(self.coercion, IdentityCoercion):
            raise TypeError("coercion must be an IdentityCoercion")


def _validate_path(path: tuple[PathPart, ...], *, allow_empty: bool) -> None:
    if not path and not allow_empty:
        raise ValueError("path must not be empty")
    for part in path:
        if isinstance(part, str) and not part:
            raise ValueError("string path parts must not be empty")
        if isinstance(part, int) and (isinstance(part, bool) or part < 0):
            raise ValueError("integer path parts must be non-negative")
        if not isinstance(part, str | int):
            raise TypeError("path parts must be strings or integers")


@dataclass(frozen=True, slots=True, init=False)
class Request:
    """Deeply immutable canonical request with detached accessors."""

    method: str
    replay_safety: ReplaySafety | None
    _parameters: FrozenMapping = field(repr=False)

    def __init__(
        self,
        method: str,
        parameters: Mapping[str, object] | None = None,
        replay_safety: ReplaySafety | None = None,
    ) -> None:
        if not _METHOD_RE.fullmatch(method):
            raise ValueError("method must contain only letters, digits, dots, and underscores")
        if replay_safety is not None and not isinstance(replay_safety, ReplaySafety):
            raise TypeError("replay_safety must be a ReplaySafety or None")
        frozen = _freeze_json(parameters or {})
        if not isinstance(frozen, FrozenMapping):
            raise TypeError("request parameters must be a mapping")
        object.__setattr__(self, "method", method)
        object.__setattr__(self, "replay_safety", replay_safety)
        object.__setattr__(self, "_parameters", frozen)

    @property
    def parameters(self) -> Mapping[str, JsonValue]:
        return MappingProxyType(self.copy_parameters())

    def copy_parameters(self) -> dict[str, JsonValue]:
        return cast("dict[str, JsonValue]", _thaw_json(self._parameters))

    def to_wire_parameters(self) -> dict[str, JsonValue]:
        return self.copy_parameters()

    @property
    def query(self) -> str:
        """Return the committed PHP-style method/query representation."""
        parameters = self.to_wire_parameters()
        query = build_query(cast("dict[Any, Any]", parameters))
        return self.method if not query else f"{self.method}?{query}"

    @property
    def summary(self) -> RequestSummary:
        return summarize_request(self.method, self._parameters)

    def __repr__(self) -> str:
        return f"Request(summary={self.summary!r}, replay_safety={self.replay_safety!r})"


@dataclass(frozen=True, slots=True)
class ResponseTime:
    """Immutable server timing values."""

    start: float
    finish: float
    duration: float
    processing: float
    date_start: str
    date_finish: str
    operating_reset_at: float | None = None
    operating: float | None = None

    def __post_init__(self) -> None:
        for value in (self.start, self.finish, self.duration, self.processing, self.operating_reset_at, self.operating):
            if value is not None and not math.isfinite(value):
                raise ValueError("response time values must be finite")
        if self.duration < 0 or self.processing < 0 or (self.operating is not None and self.operating < 0):
            raise ValueError("response durations must be non-negative")


@dataclass(frozen=True, slots=True, init=False)
class Response:
    """Deeply immutable response with explicit result selection."""

    time: ResponseTime | None
    total: int | None
    next: int | None
    evidence: ResponseEvidence
    _result: FrozenJson = field(repr=False)

    def __init__(
        self,
        result: object,
        *,
        time: ResponseTime | None = None,
        total: int | None = None,
        next: int | None = None,  # noqa: A002
        evidence: ResponseEvidence | None = None,
    ) -> None:
        if total is not None and (not _is_plain_int(total) or total < -1):
            raise ValueError("total must be -1 or non-negative")
        if next is not None and (not _is_plain_int(next) or next < 0):
            raise ValueError("next must be non-negative")
        if evidence is not None and not isinstance(evidence, ResponseEvidence):
            raise TypeError("evidence must be ResponseEvidence")
        object.__setattr__(self, "_result", _freeze_json(result))
        object.__setattr__(self, "time", time)
        object.__setattr__(self, "total", total)
        object.__setattr__(self, "next", next)
        object.__setattr__(self, "evidence", evidence or ResponseEvidence())

    @property
    def result(self) -> JsonValue:
        return _thaw_json(self._result)

    @property
    def list_result(self) -> list[JsonValue]:
        result = self.result
        if isinstance(result, list):
            return result
        if not isinstance(result, dict):
            raise TypeError("result must be a list or one-key mapping")
        if not result:
            return []
        if len(result) != 1:
            raise TypeError("mapping result must contain exactly one list")
        value = next(iter(result.values()))
        if not isinstance(value, list):
            raise TypeError("mapping result value must be a list")
        return value

    def list_items(self, selector: ResultSelector | None = None) -> list[JsonValue]:
        selector = selector or ResultSelector.root()
        selected: JsonValue = self.result
        for part in selector.path:
            if isinstance(part, str):
                if not isinstance(selected, dict) or part not in selected:
                    raise KeyError(f"result selector path not found: {selector.path!r}")
                selected = selected[part]
                continue
            if not isinstance(selected, list) or part >= len(selected):
                raise KeyError(f"result selector path not found: {selector.path!r}")
            selected = selected[part]
        if not isinstance(selected, list):
            raise TypeError("selected result must be a list")
        return selected

    def __repr__(self) -> str:
        result = self.result
        size = len(result) if isinstance(result, list | dict) else None
        return (
            "Response("
            f"result_type={type(result).__name__!r}, result_size={size!r}, total={self.total!r}, "
            f"next={self.next!r}, evidence={self.evidence!r})"
        )


def inject_controls(
    parameters: Mapping[str, object],
    updates: Mapping[ParameterPath, object],
    *,
    allow_create: bool,
) -> dict[str, JsonValue]:
    """Inject plan controls into a detached tree or reject ambiguity/conflicts."""
    frozen = _freeze_json(parameters)
    if not isinstance(frozen, FrozenMapping):
        raise TypeError("parameters must be a mapping")
    result = cast("dict[str, JsonValue]", _thaw_json(frozen))
    normalized_paths: set[tuple[PathPart, ...]] = set()
    for path, value in updates.items():
        normalized = tuple(part.casefold() if isinstance(part, str) else part for part in path.path)
        if normalized in normalized_paths:
            raise ValueError("two injected controls address the same case-insensitive path")
        normalized_paths.add(normalized)
        _inject_one(result, path.path, _thaw_json(_freeze_json(value)), allow_create=allow_create)
    return result


def _inject_one(  # noqa: C901, PLR0912
    root: dict[str, JsonValue],
    path: tuple[PathPart, ...],
    value: JsonValue,
    *,
    allow_create: bool,
) -> None:
    current: JsonValue = root
    for part in path[:-1]:
        if isinstance(part, str):
            if not isinstance(current, dict):
                raise TypeError("control path traverses a non-mapping value")
            actual = _case_insensitive_key(current, part)
            if actual is None:
                if not allow_create:
                    raise KeyError(f"missing control container: {part}")
                current[part] = {}
                actual = part
            current = current[actual]
        else:
            if not isinstance(current, list) or part >= len(current):
                raise KeyError(f"missing control list index: {part}")
            current = current[part]
    final = path[-1]
    if isinstance(final, str):
        if not isinstance(current, dict):
            raise TypeError("control path terminates in a non-mapping value")
        actual = _case_insensitive_key(current, final)
        if actual is None:
            current[final] = value
        elif current[actual] != value:
            raise ValueError(f"caller control conflicts with injected value at {path!r}")
    else:
        if not isinstance(current, list) or final >= len(current):
            raise KeyError(f"missing control list index: {final}")
        if current[final] != value:
            raise ValueError(f"caller control conflicts with injected value at {path!r}")


def _case_insensitive_key(mapping: Mapping[str, object], requested: str) -> str | None:
    matches = [key for key in mapping if key.casefold() == requested.casefold()]
    if len(matches) > 1:
        raise ValueError(f"ambiguous case-insensitive control key: {requested}")
    return matches[0] if matches else None


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Explicit retry classifications and delay bounds."""

    transient_http_statuses: frozenset[int] = frozenset({423, 425, 429, 500, 502, 503, 507})
    transient_api_codes: frozenset[str] = frozenset({"query_limit_exceeded", "operation_time_limit"})
    initial_delay: float = 0.5
    maximum_delay: float = 30.0
    backoff: float = 2.0
    jitter: float = 0.1

    def __post_init__(self) -> None:
        object.__setattr__(self, "transient_http_statuses", frozenset(self.transient_http_statuses))
        object.__setattr__(
            self,
            "transient_api_codes",
            frozenset(str(code).strip().casefold() for code in self.transient_api_codes),
        )
        if any(
            not _is_plain_int(status) or status < HTTP_STATUS_MINIMUM or status > HTTP_STATUS_MAXIMUM
            for status in self.transient_http_statuses
        ):
            raise ValueError("retry HTTP statuses must be between 100 and 599")
        retry_numbers = (self.initial_delay, self.maximum_delay, self.backoff, self.jitter)
        if not all(math.isfinite(value) for value in retry_numbers):
            raise ValueError("retry numeric controls must be finite")
        if self.initial_delay < 0 or self.maximum_delay < self.initial_delay:
            raise ValueError("retry delay bounds are invalid")
        if self.backoff < 1:
            raise ValueError("retry backoff must be at least one")
        if not 0 <= self.jitter <= 1:
            raise ValueError("retry jitter must be between zero and one")


@dataclass(frozen=True, slots=True)
class ConsistencyPolicy:
    """Immutable correctness and snapshot requirements."""

    duplicate_policy: DuplicatePolicy = DuplicatePolicy.ERROR
    total_semantics: TotalSemantics = TotalSemantics.IGNORE
    identity_requirement: IdentityRequirement = IdentityRequirement.OPTIONAL
    order_semantics: OrderSemantics = OrderSemantics.UNORDERED
    snapshot_requirement: SnapshotRequirement = SnapshotRequirement.TRAVERSAL_ONLY
    confirmation_policy: ConfirmationPolicy = ConfirmationPolicy.NONE

    def __post_init__(self) -> None:
        expected = (
            (self.duplicate_policy, DuplicatePolicy),
            (self.total_semantics, TotalSemantics),
            (self.identity_requirement, IdentityRequirement),
            (self.order_semantics, OrderSemantics),
            (self.snapshot_requirement, SnapshotRequirement),
            (self.confirmation_policy, ConfirmationPolicy),
        )
        if any(not isinstance(value, enum_type) for value, enum_type in expected):
            raise TypeError("consistency controls must use their declared enum types")

    @classmethod
    def traversal(cls) -> ConsistencyPolicy:
        return cls()


@dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    """Universal execution ceilings; every value is checked before scheduling."""

    max_requests: int = 10_000
    max_pages: int = 10_000
    max_pages_per_reference: int = 10_000
    max_elapsed: float = 900.0
    max_attempts_per_request: int = 5
    max_retry_elapsed_per_request: float = 120.0
    max_buffered_rows: int = 1_000
    max_direct_concurrency: int = 10
    max_active_references: int = 100
    max_tracked_identities: int = 100_000
    identity_tracker: IdentityTracker = IdentityTracker.MEMORY
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    consistency: ConsistencyPolicy = field(default_factory=ConsistencyPolicy.traversal)
    debug_evidence: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.identity_tracker, IdentityTracker):
            raise TypeError("identity_tracker must be an IdentityTracker")
        if not isinstance(self.retry, RetryPolicy) or not isinstance(self.consistency, ConsistencyPolicy):
            raise TypeError("retry and consistency must be typed policies")
        positive_integers = (
            self.max_requests,
            self.max_pages,
            self.max_pages_per_reference,
            self.max_attempts_per_request,
            self.max_buffered_rows,
            self.max_direct_concurrency,
            self.max_active_references,
            self.max_tracked_identities,
        )
        if any(not _is_plain_int(value) or value < 1 for value in positive_integers):
            raise ValueError("execution count limits must be positive")
        if not math.isfinite(self.max_elapsed) or not math.isfinite(self.max_retry_elapsed_per_request):
            raise ValueError("execution time limits must be finite")
        if self.max_elapsed <= 0 or self.max_retry_elapsed_per_request <= 0:
            raise ValueError("execution time limits must be positive")


@dataclass(frozen=True, slots=True)
class BudgetCounters:
    """Immutable counters implementing pre-scheduling budget semantics."""

    physical_requests: int = 0
    logical_pages: int = 0
    pages_per_reference: tuple[tuple[str, int], ...] = ()
    buffered_rows: int = 0
    buffered_rows_high_water: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "pages_per_reference", tuple(sorted(self.pages_per_reference)))
        if any(
            not _is_plain_int(value) or value < 0
            for value in (
                self.physical_requests,
                self.logical_pages,
                self.buffered_rows,
                self.buffered_rows_high_water,
            )
        ) or any(not _is_plain_int(value) or value < 0 for _, value in self.pages_per_reference):
            raise ValueError("budget counters cannot be negative")
        if self.buffered_rows > self.buffered_rows_high_water:
            raise ValueError("current buffer cannot exceed its high-water mark")

    def reserve_attempt(
        self,
        policy: ExecutionPolicy,
        *,
        attempts_for_request: int,
        retry_elapsed: float,
        total_elapsed: float,
    ) -> BudgetCounters:
        if self.physical_requests >= policy.max_requests:
            _raise_budget("physical request budget exhausted")
        if attempts_for_request >= policy.max_attempts_per_request:
            _raise_budget("per-request attempt budget exhausted")
        if retry_elapsed >= policy.max_retry_elapsed_per_request:
            _raise_budget("per-request retry time budget exhausted")
        if total_elapsed >= policy.max_elapsed:
            _raise_budget("operation elapsed budget exhausted")
        return BudgetCounters(
            physical_requests=self.physical_requests + 1,
            logical_pages=self.logical_pages,
            pages_per_reference=self.pages_per_reference,
            buffered_rows=self.buffered_rows,
            buffered_rows_high_water=self.buffered_rows_high_water,
        )

    def reserve_page(self, policy: ExecutionPolicy, *, reference: str | None = None) -> BudgetCounters:
        if self.logical_pages >= policy.max_pages:
            _raise_budget("logical page budget exhausted")
        per_reference = dict(self.pages_per_reference)
        if reference is not None:
            current = per_reference.get(reference, 0)
            if current >= policy.max_pages_per_reference:
                _raise_budget("per-reference page budget exhausted")
            per_reference[reference] = current + 1
        return BudgetCounters(
            physical_requests=self.physical_requests,
            logical_pages=self.logical_pages + 1,
            pages_per_reference=tuple(sorted(per_reference.items())),
            buffered_rows=self.buffered_rows,
            buffered_rows_high_water=self.buffered_rows_high_water,
        )

    def with_buffered_rows(self, policy: ExecutionPolicy, rows: int) -> BudgetCounters:
        if rows < 0:
            raise ValueError("buffered row count cannot be negative")
        if rows > policy.max_buffered_rows:
            _raise_budget("buffered row budget exhausted")
        return BudgetCounters(
            physical_requests=self.physical_requests,
            logical_pages=self.logical_pages,
            pages_per_reference=self.pages_per_reference,
            buffered_rows=rows,
            buffered_rows_high_water=max(rows, self.buffered_rows_high_water),
        )


def _raise_budget(message: str) -> None:
    from b24api.error import BudgetExceededError  # noqa: PLC0415

    raise BudgetExceededError(message)


@dataclass(frozen=True, slots=True)
class Violation:
    """Typed bounded report violation."""

    severity: ViolationSeverity
    code: str
    message: str
    field: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.severity, ViolationSeverity):
            raise TypeError("severity must be a ViolationSeverity")
        object.__setattr__(self, "code", DEFAULT_REDACTOR.redact_text(self.code))
        object.__setattr__(self, "message", DEFAULT_REDACTOR.redact_text(self.message))
        if self.field is not None:
            object.__setattr__(self, "field", DEFAULT_REDACTOR.redact_text(self.field))
        if not self.code or len(self.code) > VIOLATION_CODE_MAXIMUM:
            raise ValueError("violation code must be 1..100 characters")
        if not self.message or len(self.message) > VIOLATION_MESSAGE_MAXIMUM:
            raise ValueError("violation message must be 1..500 characters")


@dataclass(frozen=True, slots=True)
class OperationReport:
    """Immutable terminal report snapshot."""

    state: TerminalState = TerminalState.NOT_STARTED
    assurance: CompletionAssurance = CompletionAssurance.CALLER_ASSERTED
    snapshot: SnapshotState = SnapshotState.NOT_REQUESTED
    plan_id: str | None = None
    dispatch_id: str | None = None
    profile_id: str | None = None
    profile_version: int | None = None
    profile_applicable: bool | None = None
    profile_source_sha256: str | None = None
    profile_evidence_sha256: tuple[str, ...] = ()
    emitted_rows: int = 0
    unique_rows: int = 0
    physical_requests: int = 0
    logical_pages: int = 0
    batch_requests: int = 0
    batch_commands: int = 0
    retries: int = 0
    cooldown_seconds: float = 0.0
    buffered_rows_high_water: int = 0
    violations: tuple[Violation, ...] = ()
    terminal_reason: str | None = None
    evidence: tuple[ResponseEvidence, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.state, TerminalState):
            raise TypeError("state must be a TerminalState")
        if not isinstance(self.assurance, CompletionAssurance):
            raise TypeError("assurance must be a CompletionAssurance")
        if not isinstance(self.snapshot, SnapshotState):
            raise TypeError("snapshot must be a SnapshotState")
        object.__setattr__(self, "violations", tuple(self.violations))
        object.__setattr__(self, "evidence", tuple(self.evidence))
        object.__setattr__(self, "profile_evidence_sha256", tuple(self.profile_evidence_sha256))
        if self.terminal_reason is not None:
            object.__setattr__(self, "terminal_reason", DEFAULT_REDACTOR.redact_text(self.terminal_reason))
        counters = (
            self.emitted_rows,
            self.unique_rows,
            self.physical_requests,
            self.logical_pages,
            self.batch_requests,
            self.batch_commands,
            self.retries,
            self.buffered_rows_high_water,
        )
        if any(not _is_plain_int(value) or value < 0 for value in counters) or self.cooldown_seconds < 0:
            raise ValueError("report counters must be non-negative")
        if self.unique_rows > self.emitted_rows:
            raise ValueError("unique_rows cannot exceed emitted_rows")
        _validate_report_profile(self)
        if self.completed and any(item.severity is ViolationSeverity.BLOCKING for item in self.violations):
            raise ValueError("completed report cannot contain blocking violations")

    @property
    def completed(self) -> bool:
        return self.state is TerminalState.COMPLETED


def _validate_report_profile(report: OperationReport) -> None:
    if report.profile_id is None:
        if any(
            value is not None
            for value in (
                report.profile_version,
                report.profile_applicable,
                report.profile_source_sha256,
            )
        ) or report.profile_evidence_sha256:
            raise ValueError("profile metadata requires profile_id")
    else:
        if not report.profile_id or len(report.profile_id) > STABLE_KEY_MAXIMUM:
            raise ValueError("profile_id must be 1..100 characters")
        if not _is_plain_int(report.profile_version) or cast("int", report.profile_version) < 1:
            raise ValueError("profile_version must be positive")
        if not isinstance(report.profile_applicable, bool):
            raise TypeError("profile_applicable must be a boolean")
        if report.profile_source_sha256 is None or not _is_sha256(report.profile_source_sha256):
            raise ValueError("profile_source_sha256 must be a lowercase SHA-256")
        if not report.profile_evidence_sha256 or any(
            not _is_sha256(value) for value in report.profile_evidence_sha256
        ):
            raise ValueError("profile evidence must contain lowercase SHA-256 values")
    if report.assurance is CompletionAssurance.PROFILE_VERIFIED and (
        report.profile_id is None or report.profile_applicable is not True
    ):
        raise ValueError("profile-verified assurance requires applicable profile provenance")


@dataclass(frozen=True, slots=True, init=False)
class BatchSuccess:
    """One successful command with raw values excluded from repr."""

    command_index: int
    stable_key: str
    request: Request = field(repr=False)
    _result: FrozenJson = field(repr=False)
    _decoded_rows: int = field(repr=False)
    payload: object = field(default=None, repr=False)
    evidence: BatchCommandEvidence | None = None
    replay_disposition: ReplayDisposition | None = None
    response: Response | None = field(default=None, repr=False)

    def __init__(  # noqa: PLR0913
        self,
        command_index: int,
        stable_key: str,
        request: Request,
        result: object,
        payload: object = None,
        evidence: BatchCommandEvidence | None = None,
        replay_disposition: ReplayDisposition | None = None,
        response: Response | None = None,
    ) -> None:
        _validate_batch_correlation(command_index, stable_key, request, evidence)
        if replay_disposition is not None and not isinstance(replay_disposition, ReplayDisposition):
            raise TypeError("replay_disposition must be a ReplayDisposition or None")
        if response is not None and not isinstance(response, Response):
            raise TypeError("response must be a Response or None")
        frozen_result = _freeze_json(result)
        if response is not None and response.result != _thaw_json(frozen_result):
            raise ValueError("batch response result must match the correlated command result")
        object.__setattr__(self, "command_index", command_index)
        object.__setattr__(self, "stable_key", stable_key)
        object.__setattr__(self, "request", request)
        object.__setattr__(self, "_result", frozen_result)
        object.__setattr__(self, "_decoded_rows", len(frozen_result) if isinstance(frozen_result, tuple) else 1)
        object.__setattr__(self, "payload", payload)
        object.__setattr__(self, "evidence", evidence)
        object.__setattr__(self, "replay_disposition", replay_disposition)
        object.__setattr__(self, "response", response)

    @property
    def result(self) -> JsonValue:
        return _thaw_json(self._result)

    @property
    def decoded_rows(self) -> int:
        """Top-level decoded row weight retained by this command outcome."""
        return self._decoded_rows


@dataclass(frozen=True, slots=True)
class BatchFailure:
    """One failed command with total correlation and safe repr."""

    command_index: int
    stable_key: str
    request: Request = field(repr=False)
    error: object = field(repr=False)
    replay_safety: ReplaySafety = ReplaySafety.UNKNOWN
    replay_disposition: ReplayDisposition = ReplayDisposition.NOT_ELIGIBLE
    payload: object = field(default=None, repr=False)
    evidence: BatchCommandEvidence | None = None

    def __post_init__(self) -> None:
        _validate_batch_correlation(self.command_index, self.stable_key, self.request, self.evidence)
        if not isinstance(self.replay_safety, ReplaySafety):
            raise TypeError("replay_safety must be a ReplaySafety")
        if not isinstance(self.replay_disposition, ReplayDisposition):
            raise TypeError("replay_disposition must be a ReplayDisposition")


def _validate_batch_correlation(
    command_index: int,
    stable_key: str,
    request: Request,
    evidence: BatchCommandEvidence | None,
) -> None:
    if not _is_plain_int(command_index) or command_index < 0:
        raise ValueError("command_index cannot be negative")
    if not stable_key or len(stable_key) > STABLE_KEY_MAXIMUM:
        raise ValueError("stable_key must be 1..100 characters")
    if not isinstance(request, Request):
        raise TypeError("request must be canonical Request")
    if evidence is not None and not isinstance(evidence, BatchCommandEvidence):
        raise TypeError("evidence must be BatchCommandEvidence or None")


type BatchOutcome = BatchSuccess | BatchFailure


@dataclass(frozen=True, slots=True)
class ReferenceRequest:
    """Immutable request correlated to a reference key."""

    request: Request = field(repr=False)
    reference_key: str = field(repr=False)
    payload: object = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.request, Request):
            raise TypeError("reference request must contain a canonical Request")
        if not self.reference_key or len(self.reference_key) > STABLE_KEY_MAXIMUM:
            raise ValueError("reference_key must be 1..100 characters")


@dataclass(frozen=True, slots=True, init=False)
class ReferenceBinding:
    """Immutable top-level request updates and safe reference correlation."""

    reference_summary: str
    payload_key: str = field(repr=False)
    _updates: FrozenMapping = field(repr=False)
    payload: object = field(default=None, repr=False)

    def __init__(
        self,
        reference_summary: str,
        payload_key: str,
        updates: Mapping[str, object] | None = None,
        payload: object = None,
    ) -> None:
        frozen = _freeze_json(updates or {})
        if not isinstance(frozen, FrozenMapping):
            raise TypeError("reference updates must be a mapping")
        object.__setattr__(self, "reference_summary", DEFAULT_REDACTOR.redact_text(reference_summary))
        object.__setattr__(self, "payload_key", payload_key)
        object.__setattr__(self, "payload", payload)
        object.__setattr__(self, "_updates", frozen)
        self.__post_init__()

    def __post_init__(self) -> None:
        if not self.reference_summary or len(self.reference_summary) > VIOLATION_MESSAGE_MAXIMUM:
            raise ValueError("reference_summary must be 1..500 characters")
        if not self.payload_key or len(self.payload_key) > STABLE_KEY_MAXIMUM:
            raise ValueError("payload_key must be 1..100 characters")

    @property
    def updates(self) -> Mapping[str, JsonValue]:
        """Return detached top-level parameter updates for one reference."""
        return MappingProxyType(self.copy_updates())

    def copy_updates(self) -> dict[str, JsonValue]:
        """Return a mutable detached copy of the binding updates."""
        return cast("dict[str, JsonValue]", _thaw_json(self._updates))


@dataclass(frozen=True, slots=True, init=False)
class ReferenceItem:
    """Successful reference item; raw item and payload are hidden from repr."""

    reference_key: str = field(repr=False)
    _item: FrozenJson = field(repr=False)
    payload: object = field(default=None, repr=False)

    def __init__(self, reference_key: str, item: object, payload: object = None) -> None:
        if not reference_key or len(reference_key) > STABLE_KEY_MAXIMUM:
            raise ValueError("reference_key must be 1..100 characters")
        object.__setattr__(self, "reference_key", reference_key)
        object.__setattr__(self, "_item", _freeze_json(item))
        object.__setattr__(self, "payload", payload)

    @property
    def item(self) -> JsonValue:
        return _thaw_json(self._item)


@dataclass(frozen=True, slots=True, init=False)
class ReferenceFailure:
    """Failed reference state with raw correlation excluded from repr."""

    reference_key: str = field(repr=False)
    request: Request = field(repr=False)
    error: object = field(repr=False)
    _cursor: FrozenJson = field(repr=False)
    page_state: int = 0
    partial_rows: int = 0
    replay_safety: ReplaySafety = ReplaySafety.UNKNOWN
    replay_disposition: ReplayDisposition = ReplayDisposition.NOT_ELIGIBLE
    payload: object = field(default=None, repr=False)

    def __init__(  # noqa: PLR0913
        self,
        reference_key: str,
        request: Request,
        error: object,
        cursor: object = None,
        page_state: int = 0,
        partial_rows: int = 0,
        replay_safety: ReplaySafety = ReplaySafety.UNKNOWN,
        replay_disposition: ReplayDisposition = ReplayDisposition.NOT_ELIGIBLE,
        payload: object = None,
    ) -> None:
        if not reference_key or len(reference_key) > STABLE_KEY_MAXIMUM:
            raise ValueError("reference_key must be 1..100 characters")
        if not isinstance(request, Request):
            raise TypeError("reference failure request must be canonical Request")
        if not _is_plain_int(page_state) or page_state < 0:
            raise ValueError("page_state cannot be negative")
        if not _is_plain_int(partial_rows) or partial_rows < 0:
            raise ValueError("partial_rows cannot be negative")
        if not isinstance(replay_safety, ReplaySafety):
            raise TypeError("replay_safety must be ReplaySafety")
        if not isinstance(replay_disposition, ReplayDisposition):
            raise TypeError("replay_disposition must be ReplayDisposition")
        object.__setattr__(self, "reference_key", reference_key)
        object.__setattr__(self, "request", request)
        object.__setattr__(self, "error", error)
        object.__setattr__(self, "_cursor", _freeze_json(cursor))
        object.__setattr__(self, "page_state", page_state)
        object.__setattr__(self, "partial_rows", partial_rows)
        object.__setattr__(self, "replay_safety", replay_safety)
        object.__setattr__(self, "replay_disposition", replay_disposition)
        object.__setattr__(self, "payload", payload)

    @property
    def cursor(self) -> JsonValue:
        return _thaw_json(self._cursor)


type ReferenceOutcome = ReferenceItem | ReferenceFailure

__all__ = [
    "BatchCommandEvidence",
    "BatchFailure",
    "BatchOutcome",
    "BatchSuccess",
    "BudgetCounters",
    "CompletionAssurance",
    "ConfirmationPolicy",
    "ConsistencyPolicy",
    "DuplicatePolicy",
    "ExecutionPolicy",
    "IdentityCoercion",
    "IdentityRequirement",
    "IdentitySpec",
    "IdentityTracker",
    "JsonScalar",
    "JsonValue",
    "OperationReport",
    "OrderSemantics",
    "ParameterPath",
    "ReferenceBinding",
    "ReferenceFailure",
    "ReferenceItem",
    "ReferenceOutcome",
    "ReferenceRequest",
    "ReplayDisposition",
    "ReplaySafety",
    "Request",
    "RequestSummary",
    "Response",
    "ResponseEvidence",
    "ResponseTime",
    "ResultSelector",
    "RetryPolicy",
    "SnapshotRequirement",
    "SnapshotState",
    "TerminalState",
    "TotalSemantics",
    "Violation",
    "ViolationSeverity",
    "inject_controls",
]
