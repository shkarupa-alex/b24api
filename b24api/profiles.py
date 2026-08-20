"""Versioned endpoint profiles, pure selection, and downgrade-only probes."""

from __future__ import annotations
import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from importlib.resources import files
from typing import Any, cast

from b24api.models import (
    CompletionAssurance,
    DuplicatePolicy,
    ExecutionPolicy,
    IdentityCoercion,
    IdentityRequirement,
    IdentitySpec,
    JsonValue,
    OrderSemantics,
    ParameterPath,
    ReplaySafety,
    Request,
    ResultSelector,
    TotalSemantics,
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
from b24api.redaction import DEFAULT_REDACTOR

PROFILE_SCHEMA_VERSION = "1.0"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,99}$")
_BUILD_RE = re.compile(r"^[A-Za-z0-9._-]{1,100}$")
_SCOPE_RE = re.compile(r"^[A-Za-z0-9._-]{1,100}$")
_METHOD_RE = re.compile(r"^[A-Za-z0-9_.]{1,200}$")
_FILTER_KEY_MAX_LENGTH = 200
_PROBE_BOUND = 100
_REASON_MAX_LENGTH = 300
_ORDER_PAIR_LENGTH = 2
_DEFAULT_FILTER_PATH = ParameterPath(("filter",))
_DEFAULT_ORDER_PATH = ParameterPath(("order",))


class ProfileReasonCode(StrEnum):
    """Typed reason why a profile or its selected plan was refused."""

    SCHEMA_UNSUPPORTED = "schema_unsupported"
    NOT_YET_VALID = "not_yet_valid"
    EXPIRED = "expired"
    UNKNOWN_BUILD = "unknown_build"
    BUILD_MISMATCH = "build_mismatch"
    MISSING_SCOPE = "missing_scope"
    METHOD_MISMATCH = "method_mismatch"
    QUERY_SHAPE_MISMATCH = "query_shape_mismatch"
    EVIDENCE_UNREVIEWED = "evidence_unreviewed"
    POLICY_INCOMPATIBLE = "policy_incompatible"
    PROBE_MISSING = "probe_missing"
    PROBE_INCONCLUSIVE = "probe_inconclusive"
    PROBE_CONTRADICTION = "probe_contradiction"
    PROVENANCE_MISMATCH = "provenance_mismatch"


class ProbeStatus(StrEnum):
    """Result of a bounded read-only contradiction probe."""

    PASS = "pass"  # noqa: S105 - probe outcome, not a password
    INCONCLUSIVE = "inconclusive"
    CONTRADICTED = "contradicted"


@dataclass(frozen=True, slots=True)
class EvidenceAnchor:
    """Immutable reviewed evidence reference with an independent expiry."""

    artifact_sha256: str
    candidate_sha: str
    reviewed_at: datetime
    expires_at: datetime
    review_status: str = "accepted"

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        if not _SHA256_RE.fullmatch(self.artifact_sha256):
            raise ValueError("artifact_sha256 must be a lowercase SHA-256")
        if not re.fullmatch(r"[0-9a-f]{40}", self.candidate_sha):
            raise ValueError("candidate_sha must be a lowercase Git SHA-1")
        _require_utc(self.reviewed_at, "reviewed_at")
        _require_utc(self.expires_at, "expires_at")
        if self.expires_at <= self.reviewed_at:
            raise ValueError("evidence expiry must follow review time")
        if self.review_status not in {"accepted", "rejected"}:
            raise ValueError("review_status must be accepted or rejected")


@dataclass(frozen=True, slots=True)
class QueryPredicate:
    """Exact value-free query-shape predicate stored in a profile."""

    parameter_paths: frozenset[tuple[str | int, ...]] = frozenset()
    filter_keys: frozenset[str] = frozenset()
    filter_operators: frozenset[str] = frozenset()
    order: tuple[tuple[str, str], ...] = ()
    selector: ResultSelector = field(default_factory=ResultSelector.root)

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        paths = frozenset(tuple(path) for path in self.parameter_paths)
        for path in paths:
            ParameterPath(path)
        object.__setattr__(self, "parameter_paths", paths)
        object.__setattr__(self, "filter_keys", frozenset(self.filter_keys))
        object.__setattr__(self, "filter_operators", frozenset(self.filter_operators))
        object.__setattr__(self, "order", tuple(self.order))
        if any(not key or len(key) > _FILTER_KEY_MAX_LENGTH for key in self.filter_keys):
            raise ValueError("filter keys must be non-empty and bounded")
        if any(operator not in {"=", "!", ">", ">=", "<", "<=", "%", "!%"} for operator in self.filter_operators):
            raise ValueError("unsupported filter operator")
        if any(not key or direction not in {"ASC", "DESC"} for key, direction in self.order):
            raise ValueError("order entries require a key and ASC or DESC")
        if not isinstance(self.selector, ResultSelector):
            raise TypeError("selector must be a ResultSelector")


@dataclass(frozen=True, slots=True)
class CapabilitySet:
    """Profile-authorized endpoint facts; false never implies capability."""

    offset_honored: bool = False
    stable_order: bool = False
    filter_honored: bool = False
    cursor_honored: bool = False
    batch_supported: bool = False
    fixed_page_cap: bool = False

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        if any(type(getattr(self, field_name)) is not bool for field_name in self.__dataclass_fields__):
            raise TypeError("capability controls must be booleans")


@dataclass(frozen=True, slots=True)
class ProbeSpec:
    """Bounded safe probe declaration; execution belongs to the evidence harness."""

    probe_id: str
    method: str
    max_rows: int
    selector: ResultSelector = field(default_factory=ResultSelector.root)
    minimal_select: tuple[str, ...] = ()
    within_caller_filter: bool = True

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        if not _ID_RE.fullmatch(self.probe_id):
            raise ValueError("probe_id is invalid")
        if not _METHOD_RE.fullmatch(self.method):
            raise ValueError("probe method is invalid")
        if isinstance(self.max_rows, bool) or not 1 <= self.max_rows <= _PROBE_BOUND:
            raise ValueError("probe max_rows must be between 1 and 100")
        if not isinstance(self.selector, ResultSelector):
            raise TypeError("probe selector must be a ResultSelector")
        object.__setattr__(self, "minimal_select", tuple(self.minimal_select))
        if not self.minimal_select or any(not value or len(value) > _PROBE_BOUND for value in self.minimal_select):
            raise ValueError("probe minimal_select must contain bounded field names")
        if not self.within_caller_filter:
            raise ValueError("packaged probes must remain within the caller filter")


@dataclass(frozen=True, slots=True)
class EndpointProfile:
    """Reviewed immutable authorization for one exact endpoint query shape."""

    schema_version: str
    profile_id: str
    version: int
    endpoint: str
    method: str
    verified_at: datetime
    expires_at: datetime
    applicable_builds: frozenset[str]
    required_scopes: frozenset[str]
    query: QueryPredicate
    plan: ListPlan
    identity: IdentitySpec | None
    page_cap: int | None
    replay_safety: ReplaySafety
    capabilities: CapabilitySet
    evidence: tuple[EvidenceAnchor, ...]
    required_probes: tuple[ProbeSpec, ...] = ()
    source_sha256: str | None = field(default=None, repr=False, compare=False)
    _source_document: str | None = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:  # noqa: C901, PLR0912, PLR0915 - strict immutable boundary
        """Validate and normalize instance state."""
        if self.schema_version != PROFILE_SCHEMA_VERSION:
            raise ValueError("unsupported endpoint profile schema version")
        if not _ID_RE.fullmatch(self.profile_id):
            raise ValueError("profile_id is invalid")
        if isinstance(self.version, bool) or self.version < 1:
            raise ValueError("profile version must be positive")
        if not _ID_RE.fullmatch(self.endpoint):
            raise ValueError("endpoint is invalid")
        if not _METHOD_RE.fullmatch(self.method):
            raise ValueError("profile method is invalid")
        _require_utc(self.verified_at, "verified_at")
        _require_utc(self.expires_at, "expires_at")
        if self.expires_at <= self.verified_at:
            raise ValueError("profile expiry must follow verification")
        builds = frozenset(self.applicable_builds)
        scopes = frozenset(self.required_scopes)
        if not builds or any(not _BUILD_RE.fullmatch(value) for value in builds):
            raise ValueError("applicable_builds must contain exact bounded build IDs")
        if any(not _SCOPE_RE.fullmatch(value) for value in scopes):
            raise ValueError("required scopes are invalid")
        object.__setattr__(self, "applicable_builds", builds)
        object.__setattr__(self, "required_scopes", scopes)
        if not isinstance(self.query, QueryPredicate):
            raise TypeError("query must be a QueryPredicate")
        if not isinstance(
            self.plan,
            SingleResponsePlan
            | OffsetSequentialPlan
            | CountedOffsetPlan
            | KeysetPlan
            | ItemCursorPlan
            | PartitionedKeysetPlan,
        ):
            raise TypeError("plan must be a canonical ListPlan")
        if not isinstance(self.identity, IdentitySpec | type(None)):
            raise TypeError("identity must be an IdentitySpec or None")
        if self.page_cap is not None and (isinstance(self.page_cap, bool) or self.page_cap < 1):
            raise ValueError("page_cap must be positive")
        if not isinstance(self.replay_safety, ReplaySafety):
            raise TypeError("replay_safety must be a ReplaySafety")
        if not isinstance(self.capabilities, CapabilitySet):
            raise TypeError("capabilities must be a CapabilitySet")
        object.__setattr__(self, "evidence", tuple(self.evidence))
        object.__setattr__(self, "required_probes", tuple(self.required_probes))
        if not self.evidence:
            raise ValueError("profile requires reviewed evidence anchors")
        if any(not isinstance(anchor, EvidenceAnchor) for anchor in self.evidence):
            raise TypeError("profile evidence must contain EvidenceAnchor values")
        if any(not isinstance(probe, ProbeSpec) for probe in self.required_probes):
            raise TypeError("required_probes must contain ProbeSpec values")
        if any(anchor.reviewed_at > self.verified_at for anchor in self.evidence):
            raise ValueError("profile verification cannot predate its evidence review")
        if any(anchor.expires_at < self.expires_at for anchor in self.evidence):
            raise ValueError("profile cannot outlive an evidence anchor")
        if len({probe.probe_id for probe in self.required_probes}) != len(self.required_probes):
            raise ValueError("profile probe IDs must be unique")
        if any(probe.method != self.method for probe in self.required_probes):
            raise ValueError("profile probes must target the profiled method")
        if self.source_sha256 is not None and not _SHA256_RE.fullmatch(self.source_sha256):
            raise ValueError("source_sha256 must be a lowercase SHA-256")
        _validate_profile_plan(self)


@dataclass(frozen=True, slots=True)
class QueryShape:
    """Observed value-free query shape and portal applicability facts."""

    method: str
    parameter_paths: frozenset[tuple[str | int, ...]]
    filter_keys: frozenset[str]
    filter_operators: frozenset[str]
    order: tuple[tuple[str, str], ...]
    selector: ResultSelector
    scopes: frozenset[str]
    portal_build: str | None
    observed_at: datetime

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        if not _METHOD_RE.fullmatch(self.method):
            raise ValueError("query method is invalid")
        predicate = QueryPredicate(
            parameter_paths=self.parameter_paths,
            filter_keys=self.filter_keys,
            filter_operators=self.filter_operators,
            order=self.order,
            selector=self.selector,
        )
        object.__setattr__(self, "parameter_paths", predicate.parameter_paths)
        object.__setattr__(self, "filter_keys", predicate.filter_keys)
        object.__setattr__(self, "filter_operators", predicate.filter_operators)
        object.__setattr__(self, "order", predicate.order)
        object.__setattr__(self, "scopes", frozenset(self.scopes))
        if any(not _SCOPE_RE.fullmatch(value) for value in self.scopes):
            raise ValueError("query scopes are invalid")
        if self.portal_build is not None and not _BUILD_RE.fullmatch(self.portal_build):
            raise ValueError("portal_build is invalid")
        _require_utc(self.observed_at, "observed_at")

    @property
    def predicate(self) -> QueryPredicate:
        """Return the exact comparable query predicate without applicability facts."""
        return QueryPredicate(
            parameter_paths=self.parameter_paths,
            filter_keys=self.filter_keys,
            filter_operators=self.filter_operators,
            order=self.order,
            selector=self.selector,
        )


@dataclass(frozen=True, slots=True)
class DecisionReason:
    """Bounded typed selection or probe refusal reason."""

    code: ProfileReasonCode
    message: str

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        if not isinstance(self.code, ProfileReasonCode):
            raise TypeError("decision reason code must be a ProfileReasonCode")
        object.__setattr__(self, "message", DEFAULT_REDACTOR.redact_text(self.message))
        if not self.message or len(self.message) > _REASON_MAX_LENGTH:
            raise ValueError("decision reason message must be 1..300 characters")


@dataclass(frozen=True, slots=True)
class PlanDecision:
    """Pure selection result with explicit profile provenance and refusals."""

    selected_plan: ListPlan | None
    assurance: CompletionAssurance
    profile: EndpointProfile
    rejected_alternatives: tuple[str, ...] = ()
    reasons: tuple[DecisionReason, ...] = ()

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        if not isinstance(self.assurance, CompletionAssurance):
            raise TypeError("decision assurance must be a CompletionAssurance")
        object.__setattr__(self, "rejected_alternatives", tuple(self.rejected_alternatives))
        object.__setattr__(self, "reasons", tuple(self.reasons))
        if self.selected_plan is None and self.assurance is CompletionAssurance.PROFILE_VERIFIED:
            raise ValueError("a refused decision cannot be profile verified")
        if self.selected_plan is not None and self.reasons:
            raise ValueError("an accepted decision cannot contain refusal reasons")

    @property
    def accepted(self) -> bool:
        """Whether the profile selected an executable plan."""
        return self.selected_plan is not None


@dataclass(frozen=True, slots=True)
class PlanExplanation:
    """Redacted serializable view of a plan decision."""

    accepted: bool
    profile_id: str
    profile_version: int
    profile_source_sha256: str | None
    selected_plan: str | None
    assurance: str
    reason_codes: tuple[str, ...]
    rejected_alternatives: tuple[str, ...]
    evidence_sha256: tuple[str, ...]

    def to_safe_dict(self) -> dict[str, JsonValue]:
        """Serialize only bounded identifiers and hashes, never query values."""
        return {
            "accepted": self.accepted,
            "profile_id": self.profile_id,
            "profile_version": self.profile_version,
            "profile_source_sha256": self.profile_source_sha256,
            "selected_plan": self.selected_plan,
            "assurance": self.assurance,
            "reason_codes": list(self.reason_codes),
            "rejected_alternatives": list(self.rejected_alternatives),
            "evidence_sha256": list(self.evidence_sha256),
        }


@dataclass(frozen=True, slots=True)
class ProbeObservation:
    """Value-free result of a bounded contradiction probe."""

    probe_id: str
    status: ProbeStatus
    observed_rows: int
    note_code: str | None = None

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        if not _ID_RE.fullmatch(self.probe_id):
            raise ValueError("probe observation ID is invalid")
        if not isinstance(self.status, ProbeStatus):
            raise TypeError("probe status must be a ProbeStatus")
        if isinstance(self.observed_rows, bool) or self.observed_rows < 0:
            raise ValueError("observed_rows cannot be negative")
        if self.note_code is not None and not _ID_RE.fullmatch(self.note_code):
            raise ValueError("probe note_code is invalid")


def choose_plan(  # noqa: C901, PLR0912 - typed applicability checks stay explicit
    profile: EndpointProfile,
    query: QueryShape,
    policy: ExecutionPolicy,
) -> PlanDecision:
    """Select a profile plan without I/O, clocks, global lookup, or mutation."""
    if not isinstance(profile, EndpointProfile):
        raise TypeError("profile must be an EndpointProfile")
    if not isinstance(query, QueryShape):
        raise TypeError("query must be a QueryShape")
    if not isinstance(policy, ExecutionPolicy):
        raise TypeError("policy must be an ExecutionPolicy")
    reasons: list[DecisionReason] = []
    if not _profile_source_matches(profile):
        reasons.append(_reason(ProfileReasonCode.PROVENANCE_MISMATCH, "profile content does not match its source"))
    if query.observed_at < profile.verified_at:
        reasons.append(_reason(ProfileReasonCode.NOT_YET_VALID, "profile evidence post-dates the observation"))
    if query.observed_at >= profile.expires_at:
        reasons.append(_reason(ProfileReasonCode.EXPIRED, "profile evidence is expired"))
    if any(anchor.review_status != "accepted" for anchor in profile.evidence):
        reasons.append(_reason(ProfileReasonCode.EVIDENCE_UNREVIEWED, "profile includes unaccepted evidence"))
    if any(query.observed_at >= anchor.expires_at for anchor in profile.evidence):
        reasons.append(_reason(ProfileReasonCode.EXPIRED, "profile evidence anchor is expired"))
    if query.portal_build is None:
        reasons.append(_reason(ProfileReasonCode.UNKNOWN_BUILD, "portal build is unknown"))
    elif query.portal_build not in profile.applicable_builds:
        reasons.append(_reason(ProfileReasonCode.BUILD_MISMATCH, "portal build is outside profile applicability"))
    missing_scopes = profile.required_scopes - query.scopes
    if missing_scopes:
        reasons.append(_reason(ProfileReasonCode.MISSING_SCOPE, "required portal scope is missing"))
    if query.method != profile.method:
        reasons.append(_reason(ProfileReasonCode.METHOD_MISMATCH, "request method does not match the profile"))
    if query.predicate != profile.query:
        reasons.append(_reason(ProfileReasonCode.QUERY_SHAPE_MISMATCH, "query shape does not match the profile"))
    if profile.page_cap is not None and profile.page_cap > policy.max_buffered_rows:
        reasons.append(_reason(ProfileReasonCode.POLICY_INCOMPATIBLE, "page cap exceeds the row buffer policy"))
    if reasons:
        return PlanDecision(
            selected_plan=None,
            assurance=CompletionAssurance.CALLER_ASSERTED,
            profile=profile,
            rejected_alternatives=(type(profile.plan).__name__,),
            reasons=tuple(_deduplicate_reasons(reasons)),
        )
    return PlanDecision(
        selected_plan=profile.plan,
        assurance=CompletionAssurance.PROFILE_VERIFIED,
        profile=profile,
    )


def explain_plan(decision: PlanDecision) -> PlanExplanation:
    """Build a deterministic redacted explanation with no row or parameter values."""
    if not isinstance(decision, PlanDecision):
        raise TypeError("decision must be a PlanDecision")
    profile = decision.profile
    return PlanExplanation(
        accepted=decision.accepted,
        profile_id=profile.profile_id,
        profile_version=profile.version,
        profile_source_sha256=profile.source_sha256,
        selected_plan=type(decision.selected_plan).__name__ if decision.selected_plan is not None else None,
        assurance=decision.assurance.value,
        reason_codes=tuple(reason.code.value for reason in decision.reasons),
        rejected_alternatives=decision.rejected_alternatives,
        evidence_sha256=tuple(anchor.artifact_sha256 for anchor in profile.evidence),
    )


def apply_probe_observations(  # noqa: C901 - explicit downgrade-only state table
    decision: PlanDecision,
    observations: Iterable[ProbeObservation],
) -> PlanDecision:
    """Refuse on missing/inconclusive/contradictory probes; never promote assurance."""
    if not isinstance(decision, PlanDecision):
        raise TypeError("decision must be a PlanDecision")
    observed: dict[str, ProbeObservation] = {}
    for item in observations:
        if not isinstance(item, ProbeObservation):
            raise TypeError("observations must contain ProbeObservation values")
        if item.probe_id in observed:
            raise ValueError("probe observations must have unique IDs")
        observed[item.probe_id] = item
    reasons = list(decision.reasons)
    for specification in decision.profile.required_probes:
        observation = observed.get(specification.probe_id)
        if observation is None:
            reasons.append(_reason(ProfileReasonCode.PROBE_MISSING, "required contradiction probe is missing"))
        elif observation.status is ProbeStatus.INCONCLUSIVE:
            reasons.append(
                _reason(ProfileReasonCode.PROBE_INCONCLUSIVE, "required contradiction probe is inconclusive"),
            )
        elif observation.status is ProbeStatus.CONTRADICTED:
            reasons.append(_reason(ProfileReasonCode.PROBE_CONTRADICTION, "runtime probe contradicted the profile"))
        if observation is not None and observation.observed_rows > specification.max_rows:
            reasons.append(_reason(ProfileReasonCode.PROBE_CONTRADICTION, "probe exceeded its reviewed row bound"))
    if reasons:
        rejected = set(decision.rejected_alternatives)
        if decision.selected_plan is not None:
            rejected.add(type(decision.selected_plan).__name__)
        return PlanDecision(
            selected_plan=None,
            assurance=CompletionAssurance.CALLER_ASSERTED,
            profile=decision.profile,
            rejected_alternatives=tuple(sorted(rejected)),
            reasons=tuple(_deduplicate_reasons(reasons)),
        )
    return decision


def query_shape_from_request(  # noqa: PLR0913
    request: Request,
    *,
    selector: ResultSelector,
    filter_path: ParameterPath = _DEFAULT_FILTER_PATH,
    order_path: ParameterPath = _DEFAULT_ORDER_PATH,
    scopes: Iterable[str],
    portal_build: str | None,
    observed_at: datetime,
) -> QueryShape:
    """Derive a value-free exact shape; literal values never enter the result."""
    if not isinstance(request, Request):
        raise TypeError("request must be canonical Request")
    parameters = request.copy_parameters()
    paths = frozenset(_mapping_paths(parameters))
    filter_value = _value_at_path(parameters, filter_path.path)
    filter_keys, operators = _filter_shape(filter_value)
    order_value = _value_at_path(parameters, order_path.path)
    order = _order_shape(order_value)
    return QueryShape(
        method=request.method,
        parameter_paths=paths,
        filter_keys=filter_keys,
        filter_operators=operators,
        order=order,
        selector=selector,
        scopes=frozenset(scopes),
        portal_build=portal_build,
        observed_at=observed_at,
    )


def load_profile_document(raw: Mapping[str, object]) -> EndpointProfile:
    """Strictly validate one JSON-compatible profile mapping into immutable values."""
    document = _strict_mapping(raw, _PROFILE_KEYS, "endpoint profile")
    _require_finite_json(document)
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    source_sha256 = hashlib.sha256(canonical.encode()).hexdigest()
    query_raw = _strict_mapping(_required(document, "query"), _QUERY_KEYS, "query")
    capabilities_raw = _strict_mapping(_required(document, "capabilities"), _CAPABILITY_KEYS, "capabilities")
    identity_raw = document["identity"]
    identity = (
        None if identity_raw is None else _parse_identity(_strict_mapping(identity_raw, _IDENTITY_KEYS, "identity"))
    )
    plan = _parse_plan(_strict_mapping(_required(document, "plan"), None, "plan"), query_raw)
    evidence = tuple(
        _parse_evidence(_strict_mapping(value, _EVIDENCE_KEYS, "evidence anchor"))
        for value in _sequence(_required(document, "evidence"), "evidence")
    )
    probes = tuple(
        _parse_probe(_strict_mapping(value, _PROBE_KEYS, "probe"))
        for value in _sequence(document["required_probes"], "required_probes")
    )
    profile = EndpointProfile(
        schema_version=_string(document["schema_version"], "schema_version"),
        profile_id=_string(document["profile_id"], "profile_id"),
        version=_integer(document["version"], "version"),
        endpoint=_string(document["endpoint"], "endpoint"),
        method=_string(document["method"], "method"),
        verified_at=_datetime(document["verified_at"], "verified_at"),
        expires_at=_datetime(document["expires_at"], "expires_at"),
        applicable_builds=frozenset(_string_sequence(document["applicable_builds"], "applicable_builds")),
        required_scopes=frozenset(_string_sequence(document["required_scopes"], "required_scopes")),
        query=_parse_query(query_raw),
        plan=plan,
        identity=identity,
        page_cap=_optional_integer(document["page_cap"], "page_cap"),
        replay_safety=ReplaySafety(_string(document["replay_safety"], "replay_safety")),
        capabilities=CapabilitySet(**{key: _boolean(capabilities_raw[key], key) for key in _CAPABILITY_KEYS}),
        evidence=evidence,
        required_probes=probes,
        source_sha256=source_sha256,
    )
    object.__setattr__(profile, "_source_document", canonical)
    return profile


def _profile_source_matches(profile: EndpointProfile) -> bool:
    document = profile._source_document  # noqa: SLF001 - class-owned provenance capsule
    if document is None or profile.source_sha256 is None:
        return False
    if hashlib.sha256(document.encode()).hexdigest() != profile.source_sha256:
        return False
    try:
        raw = json.loads(document, parse_constant=_reject_json_constant)
        if not isinstance(raw, Mapping):
            return False
        loaded = load_profile_document(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    return loaded.source_sha256 == profile.source_sha256 and loaded == profile


def load_packaged_profiles() -> tuple[EndpointProfile, ...]:
    """Load the packaged reviewed defaults; an empty list is a valid core release."""
    resource = files("b24api").joinpath("data/endpoint-profiles.json")
    raw = json.loads(resource.read_text(encoding="utf-8"), parse_constant=_reject_json_constant)
    _require_finite_json(raw)
    if not isinstance(raw, list):
        raise TypeError("packaged endpoint profiles must be a JSON array")
    profiles = tuple(load_profile_document(_strict_mapping(item, None, "packaged profile")) for item in raw)
    keys = {(profile.profile_id, profile.version) for profile in profiles}
    if len(keys) != len(profiles):
        raise ValueError("packaged endpoint profile IDs and versions must be unique")
    return profiles


def load_profile_schema() -> dict[str, JsonValue]:
    """Load the packaged Draft 2020-12 machine-readable profile schema."""
    resource = files("b24api").joinpath("data/endpoint-profile.schema.json")
    raw = json.loads(resource.read_text(encoding="utf-8"), parse_constant=_reject_json_constant)
    _require_finite_json(raw)
    if not isinstance(raw, dict):
        raise TypeError("packaged endpoint profile schema must be an object")
    return cast("dict[str, JsonValue]", raw)


def _validate_profile_plan(profile: EndpointProfile) -> None:
    if isinstance(profile.plan, CountedOffsetPlan) and profile.plan.mode is CountedOffsetMode.PARALLEL_FIXED_STRIDE:
        raise ValueError("parallel counted profiles are not admitted by the current traversal foundation")
    if profile.identity is None and profile.plan.identity_requirement is IdentityRequirement.REQUIRED:
        raise ValueError("profile plan requires identity roles")
    requested_size = getattr(profile.plan, "requested_page_size", None)
    if requested_size is not None and requested_size != profile.page_cap:
        raise ValueError("profile page_cap must equal the selected plan requested_page_size")
    if profile.page_cap is not None and not profile.capabilities.fixed_page_cap:
        raise ValueError("profile page_cap requires the fixed-page-cap capability")
    if profile.plan.selector != profile.query.selector:
        raise ValueError("profile query selector must equal the selected plan selector")
    _validate_plan_capabilities(profile)
    if profile.replay_safety is not ReplaySafety.SAFE:
        raise ValueError("endpoint traversal profiles must authorize only replay-safe reads")


def _validate_plan_capabilities(profile: EndpointProfile) -> None:
    if isinstance(profile.plan, OffsetSequentialPlan | CountedOffsetPlan) and not profile.capabilities.offset_honored:
        raise ValueError("offset plans require the offset-honored capability")
    if isinstance(profile.plan, KeysetPlan) and (
        not profile.capabilities.filter_honored or not profile.capabilities.stable_order
    ):
        raise ValueError("keyset plans require filter and stable-order capabilities")
    if isinstance(profile.plan, ItemCursorPlan) and (
        not profile.capabilities.cursor_honored or not profile.capabilities.stable_order
    ):
        raise ValueError("item-cursor plans require cursor and stable-order capabilities")
    if (
        profile.plan.order_semantics in {OrderSemantics.ASCENDING, OrderSemantics.DESCENDING}
        and not profile.capabilities.stable_order
    ):
        raise ValueError("ordered plans require the stable-order capability")
    if _uses_profile_short_page(profile.plan) and (profile.page_cap is None or not profile.capabilities.fixed_page_cap):
        raise ValueError("short-page terminal requires a fixed profile page cap")


def _parse_plan(raw: Mapping[str, object], query_raw: Mapping[str, object]) -> ListPlan:
    kind = _string(_required(raw, "kind"), "plan.kind")
    selector = ResultSelector(tuple(_path(query_raw["selector"], "query.selector")))
    common_keys = {"kind", "identity_requirement", "order_semantics", "duplicate_policy", "total_semantics"}
    identity_requirement = IdentityRequirement(
        _string(_required(raw, "identity_requirement"), "identity_requirement"),
    )
    order_semantics = OrderSemantics(_string(_required(raw, "order_semantics"), "order_semantics"))
    duplicate_policy = DuplicatePolicy(_string(_required(raw, "duplicate_policy"), "duplicate_policy"))
    total_semantics = TotalSemantics(_string(_required(raw, "total_semantics"), "total_semantics"))
    if kind == "single_response":
        _strict_mapping(raw, common_keys | {"reject_continuation", "reject_positive_total_over_result"}, "single plan")
        return SingleResponsePlan(
            selector=selector,
            identity_requirement=identity_requirement,
            order_semantics=order_semantics,
            duplicate_policy=duplicate_policy,
            total_semantics=total_semantics,
            reject_continuation=_boolean(raw["reject_continuation"], "reject_continuation"),
            reject_positive_total_over_result=_boolean(
                raw["reject_positive_total_over_result"],
                "reject_positive_total_over_result",
            ),
        )
    if kind == "offset_sequential":
        keys = common_keys | {
            "offset_path",
            "limit_path",
            "requested_page_size",
            "continuation",
            "terminal",
            "allow_create_controls",
        }
        _strict_mapping(raw, keys, "offset plan")
        return OffsetSequentialPlan(
            selector=selector,
            identity_requirement=identity_requirement,
            order_semantics=order_semantics,
            duplicate_policy=duplicate_policy,
            total_semantics=total_semantics,
            offset_path=ParameterPath(tuple(_path(raw["offset_path"], "offset_path"))),
            limit_path=_optional_path(raw["limit_path"], "limit_path"),
            requested_page_size=_optional_integer(raw["requested_page_size"], "requested_page_size"),
            continuation=OffsetContinuation(_string(raw["continuation"], "continuation")),
            terminal=frozenset(OffsetTerminalRule(value) for value in _string_sequence(raw["terminal"], "terminal")),
            allow_create_controls=_boolean(raw["allow_create_controls"], "allow_create_controls"),
        )
    if kind == "counted_offset":
        keys = common_keys | {
            "mode",
            "offset_path",
            "limit_path",
            "requested_page_size",
            "fixed_stride",
            "allow_create_controls",
        }
        _strict_mapping(raw, keys, "counted plan")
        return CountedOffsetPlan(
            selector=selector,
            identity_requirement=identity_requirement,
            order_semantics=order_semantics,
            duplicate_policy=duplicate_policy,
            total_semantics=total_semantics,
            mode=CountedOffsetMode(_string(raw["mode"], "mode")),
            offset_path=ParameterPath(tuple(_path(raw["offset_path"], "offset_path"))),
            limit_path=_optional_path(raw["limit_path"], "limit_path"),
            requested_page_size=_optional_integer(raw["requested_page_size"], "requested_page_size"),
            fixed_stride=_optional_integer(raw["fixed_stride"], "fixed_stride"),
            allow_create_controls=_boolean(raw["allow_create_controls"], "allow_create_controls"),
        )
    if kind == "keyset":
        keys = common_keys | {
            "direction",
            "filter_path",
            "order_path",
            "limit_path",
            "requested_page_size",
            "start_suppression_path",
            "terminal",
            "allow_create_controls",
        }
        _strict_mapping(raw, keys, "keyset plan")
        return KeysetPlan(
            selector=selector,
            identity_requirement=identity_requirement,
            order_semantics=order_semantics,
            duplicate_policy=duplicate_policy,
            total_semantics=total_semantics,
            direction=cast("Any", _string(raw["direction"], "direction")),
            filter_path=ParameterPath(tuple(_path(raw["filter_path"], "filter_path"))),
            order_path=ParameterPath(tuple(_path(raw["order_path"], "order_path"))),
            limit_path=_optional_path(raw["limit_path"], "limit_path"),
            requested_page_size=_optional_integer(raw["requested_page_size"], "requested_page_size"),
            start_suppression_path=_optional_path(raw["start_suppression_path"], "start_suppression_path"),
            terminal=KeysetTerminalRule(_string(raw["terminal"], "terminal")),
            allow_create_controls=_boolean(raw["allow_create_controls"], "allow_create_controls"),
        )
    if kind == "item_cursor":
        keys = common_keys | {
            "cursor_request_path",
            "cursor_item_path",
            "cursor_coercion",
            "direction",
            "cursor_take",
            "limit_path",
            "requested_page_size",
            "terminal",
            "allow_create_controls",
        }
        _strict_mapping(raw, keys, "cursor plan")
        return ItemCursorPlan(
            selector=selector,
            identity_requirement=identity_requirement,
            order_semantics=order_semantics,
            duplicate_policy=duplicate_policy,
            total_semantics=total_semantics,
            cursor_request_path=ParameterPath(tuple(_path(raw["cursor_request_path"], "cursor_request_path"))),
            cursor_item_path=tuple(_path(raw["cursor_item_path"], "cursor_item_path")),
            cursor_coercion=IdentityCoercion(_string(raw["cursor_coercion"], "cursor_coercion")),
            direction=cast("Any", _string(raw["direction"], "direction")),
            cursor_take=cast("Any", _string(raw["cursor_take"], "cursor_take")),
            limit_path=_optional_path(raw["limit_path"], "limit_path"),
            requested_page_size=_optional_integer(raw["requested_page_size"], "requested_page_size"),
            terminal=CursorTerminalRule(_string(raw["terminal"], "terminal")),
            allow_create_controls=_boolean(raw["allow_create_controls"], "allow_create_controls"),
        )
    raise ValueError(f"unsupported profile plan kind: {kind}")


def _parse_query(raw: Mapping[str, object]) -> QueryPredicate:
    return QueryPredicate(
        parameter_paths=frozenset(
            tuple(_path(value, "parameter path")) for value in _sequence(raw["parameter_paths"], "parameter_paths")
        ),
        filter_keys=frozenset(_string_sequence(raw["filter_keys"], "filter_keys")),
        filter_operators=frozenset(_string_sequence(raw["filter_operators"], "filter_operators")),
        order=_parse_order_entries(raw["order"]),
        selector=ResultSelector(tuple(_path(raw["selector"], "selector"))),
    )


def _parse_identity(raw: Mapping[str, object]) -> IdentitySpec:
    return IdentitySpec(
        item_path=tuple(_path(raw["item_path"], "identity.item_path")),
        filter_key=_string(raw["filter_key"], "identity.filter_key"),
        order_key=_string(raw["order_key"], "identity.order_key"),
        coercion=IdentityCoercion(_string(raw["coercion"], "identity.coercion")),
    )


def _parse_evidence(raw: Mapping[str, object]) -> EvidenceAnchor:
    return EvidenceAnchor(
        artifact_sha256=_string(raw["artifact_sha256"], "artifact_sha256"),
        candidate_sha=_string(raw["candidate_sha"], "candidate_sha"),
        reviewed_at=_datetime(raw["reviewed_at"], "reviewed_at"),
        expires_at=_datetime(raw["expires_at"], "expires_at"),
        review_status=_string(raw["review_status"], "review_status"),
    )


def _parse_probe(raw: Mapping[str, object]) -> ProbeSpec:
    return ProbeSpec(
        probe_id=_string(raw["probe_id"], "probe_id"),
        method=_string(raw["method"], "probe.method"),
        max_rows=_integer(raw["max_rows"], "probe.max_rows"),
        selector=ResultSelector(tuple(_path(raw["selector"], "probe.selector"))),
        minimal_select=tuple(_string_sequence(raw["minimal_select"], "probe.minimal_select")),
        within_caller_filter=_boolean(raw["within_caller_filter"], "within_caller_filter"),
    )


def _mapping_paths(
    value: Mapping[str, JsonValue],
    prefix: tuple[str | int, ...] = (),
) -> Iterable[tuple[str | int, ...]]:
    for key, item in value.items():
        path = (*prefix, key)
        yield path
        if isinstance(item, dict):
            yield from _mapping_paths(item, path)


def _value_at_path(value: JsonValue, path: Sequence[str | int]) -> JsonValue | None:
    current = value
    for part in path:
        if isinstance(part, str):
            if not isinstance(current, dict) or part not in current:
                return None
            current = current[part]
        else:
            if not isinstance(current, list) or part >= len(current):
                return None
            current = current[part]
    return current


def _filter_shape(value: JsonValue | None) -> tuple[frozenset[str], frozenset[str]]:
    if value is None:
        return frozenset(), frozenset()
    if not isinstance(value, dict):
        raise TypeError("declared filter path must contain an object")
    keys: set[str] = set()
    operators: set[str] = set()
    for raw_key in value:
        operator, key = _split_filter_operator(raw_key)
        keys.add(key)
        operators.add(operator)
    return frozenset(keys), frozenset(operators)


def _split_filter_operator(key: str) -> tuple[str, str]:
    for operator in ("!%", ">=", "<=", ">", "<", "!", "%"):
        if key.startswith(operator):
            return operator, key[len(operator) :]
    return "=", key


def _order_shape(value: JsonValue | None) -> tuple[tuple[str, str], ...]:
    if value is None:
        return ()
    if not isinstance(value, dict):
        raise TypeError("declared order path must contain an object")
    order: list[tuple[str, str]] = []
    for key, direction in value.items():
        if not isinstance(direction, str):
            raise TypeError("order directions must be strings")
        order.append((key, direction.upper()))
    return tuple(order)


def _parse_order_entries(raw: object) -> tuple[tuple[str, str], ...]:
    order: list[tuple[str, str]] = []
    for value in _sequence(raw, "order"):
        pair = _sequence(value, "order entry")
        if len(pair) != _ORDER_PAIR_LENGTH:
            raise ValueError("order entries must contain exactly two values")
        order.append((_string(pair[0], "order key"), _string(pair[1], "order direction")))
    if len({key for key, _ in order}) != len(order):
        raise ValueError("order keys must be unique")
    return tuple(order)


def _uses_profile_short_page(plan: ListPlan) -> bool:
    if isinstance(plan, OffsetSequentialPlan):
        return OffsetTerminalRule.PROFILE_SHORT_PAGE in plan.terminal
    if isinstance(plan, KeysetPlan):
        return plan.terminal is KeysetTerminalRule.PROFILE_SHORT_PAGE
    if isinstance(plan, ItemCursorPlan):
        return plan.terminal is CursorTerminalRule.PROFILE_SHORT_PAGE
    return False


def _strict_mapping(raw: object, keys: set[str] | None, name: str) -> dict[str, object]:
    if not isinstance(raw, Mapping):
        raise TypeError(f"{name} must be an object")
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} keys must be strings")
    value = {cast("str", key): item for key, item in raw.items()}
    if keys is not None:
        missing = keys - value.keys()
        unknown = value.keys() - keys
        if missing:
            raise ValueError(f"{name} is missing fields: {sorted(missing)}")
        if unknown:
            raise ValueError(f"{name} has unknown fields: {sorted(unknown)}")
    return value


def _required(raw: Mapping[str, object], key: str) -> object:
    if key not in raw:
        raise ValueError(f"required field is missing: {key}")
    return raw[key]


def _sequence(raw: object, name: str) -> list[object]:
    if not isinstance(raw, Sequence) or isinstance(raw, bytes | bytearray | str):
        raise TypeError(f"{name} must be an array")
    return list(raw)


def _string_sequence(raw: object, name: str) -> list[str]:
    return [_string(item, name) for item in _sequence(raw, name)]


def _path(raw: object, name: str) -> list[str | int]:
    result: list[str | int] = []
    for part in _sequence(raw, name):
        if not isinstance(part, str | int) or isinstance(part, bool):
            raise TypeError(f"{name} path parts must be strings or integers")
        result.append(part)
    return result


def _optional_path(raw: object, name: str) -> ParameterPath | None:
    return None if raw is None else ParameterPath(tuple(_path(raw, name)))


def _string(raw: object, name: str) -> str:
    if not isinstance(raw, str):
        raise TypeError(f"{name} must be a string")
    return raw


def _integer(raw: object, name: str) -> int:
    if not isinstance(raw, int) or isinstance(raw, bool):
        raise TypeError(f"{name} must be an integer")
    return raw


def _optional_integer(raw: object, name: str) -> int | None:
    return None if raw is None else _integer(raw, name)


def _boolean(raw: object, name: str) -> bool:
    if not isinstance(raw, bool):
        raise TypeError(f"{name} must be a boolean")
    return raw


def _datetime(raw: object, name: str) -> datetime:
    value = _string(raw, name)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from error
    _require_utc(parsed, name)
    return parsed


def _require_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must be timezone-aware UTC")


def _reason(code: ProfileReasonCode, message: str) -> DecisionReason:
    return DecisionReason(code, message)


def _deduplicate_reasons(reasons: Iterable[DecisionReason]) -> list[DecisionReason]:
    result: list[DecisionReason] = []
    seen: set[ProfileReasonCode] = set()
    for reason in reasons:
        if reason.code not in seen:
            result.append(reason)
            seen.add(reason.code)
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


def _require_finite_json(value: object) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("JSON numbers must be finite")
    if isinstance(value, Mapping):
        for item in value.values():
            _require_finite_json(item)
    elif isinstance(value, Sequence) and not isinstance(value, bytes | bytearray | str):
        for item in value:
            _require_finite_json(item)


_PROFILE_KEYS = {
    "schema_version",
    "profile_id",
    "version",
    "endpoint",
    "method",
    "verified_at",
    "expires_at",
    "applicable_builds",
    "required_scopes",
    "query",
    "plan",
    "identity",
    "page_cap",
    "replay_safety",
    "capabilities",
    "evidence",
    "required_probes",
}
_QUERY_KEYS = {"parameter_paths", "filter_keys", "filter_operators", "order", "selector"}
_IDENTITY_KEYS = {"item_path", "filter_key", "order_key", "coercion"}
_CAPABILITY_KEYS = {
    "offset_honored",
    "stable_order",
    "filter_honored",
    "cursor_honored",
    "batch_supported",
    "fixed_page_cap",
}
_EVIDENCE_KEYS = {"artifact_sha256", "candidate_sha", "reviewed_at", "expires_at", "review_status"}
_PROBE_KEYS = {"probe_id", "method", "max_rows", "selector", "minimal_select", "within_caller_filter"}


__all__ = [
    "CapabilitySet",
    "DecisionReason",
    "EndpointProfile",
    "EvidenceAnchor",
    "PlanDecision",
    "PlanExplanation",
    "ProbeObservation",
    "ProbeSpec",
    "ProbeStatus",
    "ProfileReasonCode",
    "QueryPredicate",
    "QueryShape",
    "apply_probe_observations",
    "choose_plan",
    "explain_plan",
    "load_packaged_profiles",
    "load_profile_document",
    "load_profile_schema",
    "query_shape_from_request",
]
