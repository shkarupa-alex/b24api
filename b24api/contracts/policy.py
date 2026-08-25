"""Immutable public values shared by execution and evidence layers."""

from __future__ import annotations
import math
from dataclasses import dataclass, field
from enum import StrEnum

from b24api.contracts.json import _is_plain_int

HTTP_STATUS_MINIMUM = 100
HTTP_STATUS_MAXIMUM = 599


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


class KernelState(StrEnum):
    """Operation lifecycle result."""

    NOT_STARTED = "not_started"
    COMPLETED = "completed"
    INCOMPLETE = "incomplete"
    FAILED = "failed"
    CANCELLED = "cancelled"


class CompletionAssurance(StrEnum):
    """Strength of evidence supporting completion."""

    ORACLE_VERIFIED = "oracle_verified"
    CALLER_ASSERTED = "caller_asserted"


class SnapshotState(StrEnum):
    """Observed snapshot condition."""

    NOT_REQUESTED = "not_requested"
    VERIFIED = "verified"
    UNVERIFIED = "unverified"
    CHANGED = "changed"


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
        """Validate and normalize instance state."""
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
        """Validate and normalize instance state."""
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
        """Return the default traversal-only consistency policy."""
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
    max_response_bytes: int = 16 * 1024 * 1024
    max_buffered_commands: int = 50
    # A full Bitrix batch may contain 50 list commands and each command may
    # decode the portal page cap of 50 rows. Keeping the default below that
    # product split one safe batch into several physical requests.
    max_buffered_rows: int = 2_500
    max_direct_concurrency: int = 10
    max_active_references: int = 100
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    consistency: ConsistencyPolicy = field(default_factory=ConsistencyPolicy.traversal)
    debug_evidence: bool = False

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        if not isinstance(self.retry, RetryPolicy) or not isinstance(self.consistency, ConsistencyPolicy):
            raise TypeError("retry and consistency must be typed policies")
        positive_integers = (
            self.max_requests,
            self.max_pages,
            self.max_pages_per_reference,
            self.max_attempts_per_request,
            self.max_response_bytes,
            self.max_buffered_commands,
            self.max_buffered_rows,
            self.max_direct_concurrency,
            self.max_active_references,
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
        """Validate and normalize instance state."""
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
        """Reserve the attempt budget."""
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
        """Reserve the page budget."""
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
        """Return counters with buffered rows."""
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
