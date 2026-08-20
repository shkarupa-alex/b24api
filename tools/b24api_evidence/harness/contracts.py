# ruff: noqa: ANN401
"""Strict contracts, lineage checks, and secret scanning for evidence bundles."""

from __future__ import annotations
import base64
import fcntl
import hashlib
import hmac
import json
import math
import os
import re
import shutil
import subprocess
import tempfile
import uuid
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import urlsplit

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError

SCHEMA_VERSION: Final = "2.0"
ORIGINAL_HEAD_SHA: Final = "08277c4d921b83b9252177b3e72a21a4c0c86109"
FIXED_1X_SHA: Final = "82f2d9ddafa50e5229c4191b66b0ad6db4ad4600"
SKILLS_CORPUS_SHA: Final = "a353a58fb1de7abf2c81dd86fc7d8ad1caea8f2f"
SKILLS_RECIPE_TREE_SHA256: Final = "7c6c83487df9913076f16f1cab8630b065df2250f026c4679871957e51343b66"
INSTRUMENTATION_REVIEW_SHA: Final = "521f0eb7cb107ec948c693496154f94e57dbf7c9"
REVIEWED_PROFILE_SET_ID: Final = "w0-disposable-entities-v1"
REVIEWED_PROFILE_SET_SHA256: Final = "425cdca3d9f0682974c50afc9af4d4d3fa90dc6233ee785290ba7632bd30b754"
REVIEWED_MAX_ENTITIES_PER_CELL: Final = 500
REVIEWED_MAX_DATASET_CELLS: Final = 2
REVIEWED_MAX_TOTAL_ENTITIES: Final = 500
FINGERPRINT_ALGORITHM: Final = "hmac-sha256-host-role-principal-v1"
FINGERPRINT_KEY_FORMAT: Final = "base64url-no-padding-32-bytes"
NORMATIVE_MINIMUM_MEDIAN_IMPROVEMENT: Final = 0.15
NORMATIVE_MAXIMUM_SMALL_P95_RATIO: Final = 1.05
NORMATIVE_MAXIMUM_OPERATING_RATIO: Final = 1.10
MAX_MUTATION_RETRIES: Final = 3
FINGERPRINT_KEY_BYTES: Final = 32
WEBHOOK_PATH_PARTS: Final = 3
MINIMUM_WEBHOOK_TOKEN_LENGTH: Final = 6
MINIMUM_COMPARED_PLANS: Final = 2
MINIMUM_KEY_DISTINCT_BYTES: Final = 16
MINIMUM_KEY_SHANNON_ENTROPY: Final = 3.5
MAX_ASCII_CODEPOINT: Final = 0x7F
MAX_JSON_INPUT_BYTES: Final = 16 * 1024 * 1024
MAX_MANIFEST_INPUT_BYTES: Final = 64 * 1024 * 1024
MAX_SCANNED_FILE_BYTES: Final = 64 * 1024 * 1024
SCHEMA_DIR: Final = Path(__file__).resolve().parents[1] / "schemas"

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_NAMESPACE_RE = re.compile(r"^b24api-evidence-[0-9a-f-]{36}$")
_BASE64URL_KEY_RE = re.compile(r"^[A-Za-z0-9_-]{43}$")
_DNS_HOST_RE = re.compile(
    r"^(?=.{1,253}$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$",
    re.IGNORECASE,
)
_WEBHOOK_RE = re.compile(rb"https?://[^\s/'\"<>]+/rest/[0-9]+/[A-Za-z0-9_-]{6,}/?", re.IGNORECASE)
_QUERY_SECRET_RE = re.compile(
    rb"(?:[?&](?:auth|access_token|refresh_token)=|\bBearer\s+)[A-Za-z0-9._~+/-]{8,}",
    re.IGNORECASE,
)
_ENV_SECRET_RE = re.compile(
    rb"\b(?:AUTH_ID|APPLICATION_TOKEN|ACCESS_TOKEN|REFRESH_TOKEN|CLIENT_SECRET)=[A-Za-z0-9._~-]{8,}",
    re.IGNORECASE,
)
_ASCII_UNICODE_ESCAPE_RE = re.compile(rb"\\u([0-9a-fA-F]{4})")
_CONTRACT_TEST_SECRET_FIXTURE_LINE_SHA256 = frozenset(
    {
        "4d6ee1de7c64da4f49365980897cc01fff1240a0258091d1df92a5505ad4da6e",
        "aabd00436576a28dca9ee75a578f722247109dce7bc7325188cc645a98610b8b",
        "f1462274256a074c4c6c6e413856341013efb9f77e73d9bfcc5f822e63954181",
        "4bfbd7e3a59ca085a2c11043553b98f2c008db2b879546c9be4301aefcbf568c",
        "62812b3cdbc6519f96f61a3fbc05c97120bcf96b9dfe522b457d7163ca3f876e",
        "8c87d639114081d310c484fc215f8cc3b1440bfa992b90127bcb732f3379ec12",
    },
)


class ExitCode(IntEnum):
    """Normative evidence CLI process outcomes."""

    COMPLETED = 0
    INVALID = 2
    UNAVAILABLE = 3
    CORRECTNESS = 4
    INCOMPLETE = 5
    ORPHANS = 6
    SECRET_LEAK = 7


class ContractError(ValueError):
    """A deterministic configuration, artifact, or lineage rejection."""


class SecretLeakError(ContractError):
    """A credential-bearing value was found in a persisted or tracked file."""


@dataclass(frozen=True, slots=True)
class PortalIdentity:
    """Non-secret identity retained by evidence artifacts."""

    host: str
    role: str
    principal_id: str
    fingerprint: str


@dataclass(frozen=True, slots=True)
class ManifestLineage:
    """Fields that must remain identical throughout one manifest."""

    run_id: str
    lineage_id: str
    original_head_sha: str
    fixed_1x_sha: str
    skills_corpus_sha: str
    skills_recipe_tree_sha256: str
    dataset_plan_content_hash: str
    portal_fingerprint: str
    candidate_sha: str
    namespace: str


def strict_json_loads(raw: str | bytes) -> Any:
    """Parse UTF-8 RFC JSON and reject duplicate keys and every non-finite value."""

    def reject_constant(value: str) -> None:
        raise ContractError(f"non-finite JSON constant is forbidden: {value}")

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ContractError("duplicate JSON object keys are forbidden")
            result[key] = value
        return result

    try:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        value = json.loads(text, parse_constant=reject_constant, object_pairs_hook=reject_duplicate_keys)
        _require_finite(value)
    except ContractError:
        raise
    except (RecursionError, UnicodeDecodeError, ValueError) as error:
        raise ContractError(f"invalid JSON: {error}") from error
    return value


def _require_finite(value: Any, path: str = "$") -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ContractError(f"non-finite number at {path}")
    if isinstance(value, Mapping):
        for key, item in value.items():
            _require_finite(item, f"{path}.{key}")
    elif isinstance(value, list | tuple):
        for index, item in enumerate(value):
            _require_finite(item, f"{path}[{index}]")


def canonical_json(value: Any) -> bytes:
    """Encode one finite JSON value in the bundle's canonical hash form."""
    _require_finite(value)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


def content_sha256(value: Any) -> str:
    """Hash one canonical JSON value."""
    return hashlib.sha256(canonical_json(value)).hexdigest()


def file_sha256(path: Path) -> str:
    """Hash a file without interpreting its contents."""
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as error:
        raise ContractError(f"cannot hash required file: {path.name}") from error
    return digest.hexdigest()


def read_json_object(path: Path) -> dict[str, Any]:
    """Read a finite JSON object."""
    raw = _read_bounded_bytes(path, ceiling=MAX_JSON_INPUT_BYTES, kind="JSON input")
    value = strict_json_loads(raw)
    if not isinstance(value, dict):
        raise ContractError(f"{path} must contain a JSON object")
    return value


def _read_bounded_bytes(path: Path, *, ceiling: int, kind: str) -> bytes:
    """Read one local input only after enforcing its reviewed memory ceiling."""
    try:
        size = path.stat().st_size
    except OSError as error:
        raise ContractError(f"cannot read {path}: {error}") from error
    if size > ceiling:
        raise ContractError(f"{kind} exceeds the reviewed byte ceiling: {path.name}")
    try:
        with path.open("rb") as source:
            payload = source.read(ceiling + 1)
    except OSError as error:
        raise ContractError(f"cannot read {path}: {error}") from error
    if len(payload) > ceiling:
        raise ContractError(f"{kind} exceeds the reviewed byte ceiling: {path.name}")
    return payload


def atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    """Persist one artifact through fsync and same-directory atomic replace."""
    payload = canonical_json(value) + b"\n"
    scan_bytes_for_secrets(payload, source=str(path))
    _atomic_write_bytes(path, payload)


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    """Persist already validated bytes without exposing a partial destination."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(path)
        _fsync_directory(path.parent)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def parse_fingerprint_key(encoded: str) -> bytes:
    """Decode the one admitted 32-byte base64url key form."""
    if not _BASE64URL_KEY_RE.fullmatch(encoded):
        raise ContractError(
            "BITRIX24_EVIDENCE_FINGERPRINT_KEY must be 43-character unpadded base64url encoding of 32 bytes",
        )
    try:
        key = base64.urlsafe_b64decode(encoded + "=")
    except ValueError as error:
        raise ContractError("invalid BITRIX24_EVIDENCE_FINGERPRINT_KEY encoding") from error
    if len(key) != FINGERPRINT_KEY_BYTES:
        raise ContractError("BITRIX24_EVIDENCE_FINGERPRINT_KEY must decode to exactly 32 bytes")
    counts = Counter(key)
    entropy = -sum((count / len(key)) * math.log2(count / len(key)) for count in counts.values())
    if len(counts) < MINIMUM_KEY_DISTINCT_BYTES or entropy < MINIMUM_KEY_SHANNON_ENTROPY:
        raise ContractError("BITRIX24_EVIDENCE_FINGERPRINT_KEY does not satisfy the diversity safety check")
    return key


def portal_identity(webhook_url: str, *, role: str, fingerprint_key: str) -> PortalIdentity:
    """Validate a webhook and derive a non-reversible host/role/principal fingerprint."""
    key = parse_fingerprint_key(fingerprint_key)
    try:
        parts = urlsplit(webhook_url)
        port = parts.port
    except ValueError as error:
        raise ContractError("live webhook has an invalid port") from error
    path_parts = [part for part in parts.path.split("/") if part]
    if (
        parts.scheme != "https"
        or parts.hostname is None
        or parts.username is not None
        or parts.password is not None
        or port not in {None, 443}
        or len(path_parts) != WEBHOOK_PATH_PARTS
        or path_parts[0] != "rest"
    ):
        raise ContractError("live webhook must be an HTTPS /rest/<principal>/<token>/ URL")
    principal_id = path_parts[1]
    token = path_parts[2]
    if not principal_id.isdigit() or len(token) < MINIMUM_WEBHOOK_TOKEN_LENGTH or parts.query or parts.fragment:
        raise ContractError("live webhook has an invalid principal or token shape")
    try:
        host = parts.hostname.encode("idna").decode("ascii").lower()
    except UnicodeError as error:
        raise ContractError("live webhook host is not valid IDNA") from error
    if not _DNS_HOST_RE.fullmatch(host):
        raise ContractError("live webhook host is not a valid DNS name")
    message = canonical_json([host, role, principal_id])
    fingerprint = hmac.new(key, message, hashlib.sha256).hexdigest()
    return PortalIdentity(host=host, role=role, principal_id=principal_id, fingerprint=fingerprint)


def validate_reviewed_profile_set(path: Path) -> dict[str, Any]:
    """Reject any allowlist content not pinned by the accepted W0 review."""
    if file_sha256(path) != REVIEWED_PROFILE_SET_SHA256:
        raise ContractError("disposable entity profile set differs from the reviewed immutable SHA-256")
    profile_set = read_json_object(path)
    if profile_set.get("profile_set_id") != REVIEWED_PROFILE_SET_ID:
        raise ContractError("disposable entity profile_set_id does not match the reviewed set")
    profiles = profile_set.get("profiles")
    if not isinstance(profiles, list) or not profiles:
        raise ContractError("reviewed disposable entity profile set is empty")
    return profile_set


def validate_dataset_plan(plan: Mapping[str, Any]) -> None:  # noqa: C901, PLR0912, PLR0915
    """Apply semantic live-write invariants that JSON Schema cannot express."""
    validate_schema(plan, "dataset-plan")
    _require_exact_keys(
        plan,
        {
            "schema_version",
            "run_id",
            "lineage_id",
            "original_head_sha",
            "candidate_sha",
            "generator_sha",
            "skills_corpus_sha",
            "skills_recipe_tree_sha256",
            "credential_role",
            "disposable_profile_set_id",
            "disposable_profiles_content_hash",
            "portal",
            "namespace",
            "cells",
            "estimated",
            "cleanup",
            "authorization",
        },
        "dataset plan",
    )
    if plan["schema_version"] != SCHEMA_VERSION:
        raise ContractError("unsupported dataset plan schema_version")
    _require_uuid(plan["run_id"], "run_id")
    _require_uuid(plan["lineage_id"], "lineage_id")
    _require_sha(plan["candidate_sha"], "candidate_sha")
    if plan["generator_sha"] != plan["candidate_sha"]:
        raise ContractError("dataset plan generator_sha must equal the executing candidate")
    if plan["disposable_profile_set_id"] != REVIEWED_PROFILE_SET_ID:
        raise ContractError("dataset plan profile_set_id is not reviewed")
    if plan["disposable_profiles_content_hash"] != REVIEWED_PROFILE_SET_SHA256:
        raise ContractError("dataset plan profile content hash is not reviewed")
    namespace = plan["namespace"]
    if not isinstance(namespace, str) or not _NAMESPACE_RE.fullmatch(namespace):
        raise ContractError("dataset plan namespace is invalid")
    portal = _mapping(plan["portal"], "portal")
    if plan["credential_role"] != portal["role"]:
        raise ContractError("dataset plan credential_role must equal portal.role")
    if portal["role"] == "model":
        if portal["fingerprint_algorithm"] != "sha256-public-model-v1":
            raise ContractError("model portal must use the public deterministic fingerprint")
    elif (
        portal["fingerprint_algorithm"] != FINGERPRINT_ALGORITHM
        or portal["fingerprint_key_format"] != FINGERPRINT_KEY_FORMAT
    ):
        raise ContractError("live portal fingerprint contract is not the reviewed HMAC construction")
    cells = plan["cells"]
    if not isinstance(cells, list) or not cells:
        raise ContractError("dataset plan requires at least one cell")
    if len(cells) > REVIEWED_MAX_DATASET_CELLS:
        raise ContractError("dataset plan exceeds the reviewed cell-count ceiling")
    total_entities = 0
    total_relationships = 0
    seen_cells: set[str] = set()
    seen_profiles: set[str] = set()
    for index, cell in enumerate(cells):
        if not isinstance(cell, dict):
            raise ContractError(f"cells[{index}] must be an object")
        _validate_cell(cell, index=index)
        cell_id = str(cell["id"])
        if cell_id in seen_cells:
            raise ContractError(f"duplicate dataset cell id: {cell_id}")
        seen_cells.add(cell_id)
        profile_id = str(cell["disposable_profile_id"])
        if profile_id in seen_profiles:
            raise ContractError(f"duplicate disposable profile in dataset plan: {profile_id}")
        seen_profiles.add(profile_id)
        total_entities += max(
            _integer(cell.get("target_count"), f"cells[{index}].target_count"),
            _integer(cell.get("base_count", 0), f"cells[{index}].base_count"),
        )
        total_relationships += _integer(cell.get("relationship_count", 0), f"cells[{index}].relationship_count")
    if total_entities > REVIEWED_MAX_TOTAL_ENTITIES:
        raise ContractError("dataset plan exceeds the reviewed aggregate entity ceiling")
    estimated = _mapping(plan["estimated"], "estimated")
    entities = _nonnegative_int(estimated.get("entities"), "estimated.entities")
    relationships = _nonnegative_int(estimated.get("relationships"), "estimated.relationships")
    create_strategy = estimated.get("create_strategy")
    delete_strategy = estimated.get("delete_strategy")
    requests = _nonnegative_int(estimated.get("requests"), "estimated.requests")
    batch_commands = _nonnegative_int(estimated.get("batch_commands"), "estimated.batch_commands")
    duration = _nonnegative_number(estimated.get("duration_seconds"), "estimated.duration_seconds")
    quota = _nonnegative_number(estimated.get("quota_impact"), "estimated.quota_impact")
    if entities < total_entities or relationships < total_relationships:
        raise ContractError("dataset estimates understate entities or relationships")
    if total_entities and (requests == 0 or duration <= 0 or quota <= 0):
        raise ContractError("non-empty seed requires positive request, duration, and quota estimates")
    if create_strategy == delete_strategy == "direct":
        if batch_commands != 0:
            raise ContractError("direct lifecycle strategy cannot claim batch commands")
        if total_entities and requests < total_entities * 5 + 4:
            raise ContractError("direct lifecycle request estimate is not conservative")
    else:
        raise ContractError("only the reviewed direct/direct lifecycle strategy is implemented")
    if quota < requests:
        raise ContractError("quota estimate cannot be lower than physical requests")
    cleanup = _mapping(plan["cleanup"], "cleanup")
    if cleanup.get("feasible") is not True or cleanup.get("absence_verification") != "exact_id_point_read":
        raise ContractError("cleanup must be feasible with exact-id point-read absence verification")
    dependency_order = cleanup.get("dependency_order")
    if not isinstance(dependency_order, list) or len(dependency_order) != len(seen_cells):
        raise ContractError("cleanup dependency_order must list every dataset cell exactly once")
    if len(set(dependency_order)) != len(dependency_order) or set(dependency_order) != seen_cells:
        raise ContractError("cleanup dependency_order must be an exact permutation of dataset cell ids")
    authorization = _mapping(plan["authorization"], "authorization")
    _validate_authorization(authorization, cells=cells)
    if authorization["state"] == "approved_for_seed" and portal["role"] == "model":
        raise ContractError("approved live-write plan cannot target the deterministic model portal")


def _validate_cell(cell: Mapping[str, Any], *, index: int) -> None:
    target = _nonnegative_int(cell.get("target_count"), f"cells[{index}].target_count")
    base = _nonnegative_int(cell.get("base_count", 0), f"cells[{index}].base_count")
    if max(target, base) > REVIEWED_MAX_ENTITIES_PER_CELL:
        raise ContractError(f"cells[{index}] exceeds reviewed hard scale ceiling")
    relationships = _nonnegative_int(cell.get("relationship_count", 0), f"cells[{index}].relationship_count")
    expected_distribution = "boundary" if target else "empty"
    if base != 0 or relationships != 0 or cell.get("distribution") != expected_distribution:
        raise ContractError(f"cells[{index}] requests an unimplemented seed distribution or relationship shape")
    expected = {
        "tasks-task-v1": (
            "task",
            "tasks.task.add",
            "tasks.task.get",
            "tasks.task.delete",
            "TITLE",
            frozenset({"task"}),
        ),
        "crm-deal-v1": (
            "crm_deal",
            "crm.deal.add",
            "crm.deal.get",
            "crm.deal.delete",
            "TITLE",
            frozenset({"crm"}),
        ),
    }
    profile_id = cell.get("disposable_profile_id")
    actual = (
        cell.get("entity_family"),
        cell.get("create_method"),
        cell.get("read_method"),
        cell.get("delete_method"),
        cell.get("marker_field"),
        frozenset(cell.get("required_scopes", [])),
    )
    if profile_id not in expected or actual != expected[profile_id]:
        raise ContractError(f"cells[{index}] does not exactly match a reviewed disposable profile")


def _validate_authorization(authorization: Mapping[str, Any], *, cells: list[Any]) -> None:  # noqa: C901
    ceiling = _nonnegative_int(authorization.get("max_entities_per_cell"), "authorization.max_entities_per_cell")
    if ceiling > REVIEWED_MAX_ENTITIES_PER_CELL:
        raise ContractError("plan cannot raise the reviewed entity ceiling")
    if any(max(int(cell["target_count"]), int(cell.get("base_count", 0))) > ceiling for cell in cells):
        raise ContractError("dataset cell exceeds the plan's lower authorization ceiling")
    state = authorization.get("state")
    if state == "preview":
        if any(
            (
                authorization.get("live") is not False,
                authorization.get("allow_writes") is not False,
                authorization.get("approved_by_user") is not False,
                authorization.get("plan_review_sha") is not None,
            ),
        ):
            raise ContractError("preview authorization cannot claim live write approval")
    elif state == "approved_for_seed":
        if authorization.get("live") is not True or authorization.get("allow_writes") is not True:
            raise ContractError("approved seed plan must bind both live and allow_writes")
        if authorization.get("approved_by_user") is not True:
            raise ContractError("approved seed plan requires explicit human approval")
        _require_sha(authorization.get("plan_review_sha"), "authorization.plan_review_sha")
        approved_at = authorization.get("approved_at")
        if not isinstance(approved_at, str):
            raise ContractError("approved seed plan requires approved_at")
        try:
            parsed_approval = datetime.fromisoformat(approved_at)
        except ValueError as error:
            raise ContractError("approved_at must be an ISO-8601 timestamp") from error
        if parsed_approval.tzinfo is None:
            raise ContractError("approved_at must include a timezone")
    else:
        raise ContractError("authorization.state must be preview or approved_for_seed")


def marker_value(namespace: str, correlation_key: str) -> str:
    """Bind a human-searchable marker to its exact manifest ownership key."""
    if not _NAMESPACE_RE.fullmatch(namespace) or not _SHA256_RE.fullmatch(correlation_key):
        raise ContractError("cannot construct marker from invalid namespace/correlation key")
    return f"{namespace}:{correlation_key}"


def marker_sha256(value: str) -> str:
    """Hash a marker using the normative UTF-8 SHA-256 algorithm."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def validate_manifest_record(  # noqa: C901
    record: Mapping[str, Any],
    *,
    previous: Mapping[str, Any] | None,
) -> None:
    """Validate one append-only record, including genesis, marker, and record hash."""
    validate_schema(record, "manifest-record")
    required = {
        "schema_version",
        "sequence",
        "run_id",
        "lineage_id",
        "original_head_sha",
        "fixed_1x_sha",
        "skills_corpus_sha",
        "skills_recipe_tree_sha256",
        "dataset_plan_content_hash",
        "portal_fingerprint",
        "candidate_sha",
        "namespace",
        "event",
        "cell_id",
        "entity_family",
        "correlation_key",
        "entity_id",
        "marker_hash",
        "marker_value",
        "request_fingerprint",
        "previous_record_hash",
        "record_hash",
        "recorded_at",
        "safe_error",
    }
    optional = {"parent_correlation_keys"}
    _require_exact_keys(record, required, "manifest record", optional=optional)
    if record["schema_version"] != SCHEMA_VERSION:
        raise ContractError("unsupported manifest schema_version")
    _require_uuid(record["run_id"], "run_id")
    _require_uuid(record["lineage_id"], "lineage_id")
    _require_sha(record["candidate_sha"], "candidate_sha")
    _require_sha256(record["dataset_plan_content_hash"], "dataset_plan_content_hash")
    _require_sha256(record["portal_fingerprint"], "portal_fingerprint")
    sequence = _nonnegative_int(record["sequence"], "sequence")
    expected_sequence = 0 if previous is None else int(previous["sequence"]) + 1
    if sequence != expected_sequence:
        raise ContractError("manifest sequence is not contiguous")
    expected_previous = None if previous is None else previous["record_hash"]
    if record["previous_record_hash"] != expected_previous:
        raise ContractError("manifest previous_record_hash does not match the chain")
    if (sequence == 0) != (record["previous_record_hash"] is None):
        raise ContractError("manifest genesis invariant failed")
    correlation_key = record["correlation_key"]
    if not isinstance(correlation_key, str) or not _SHA256_RE.fullmatch(correlation_key):
        raise ContractError("manifest correlation_key must be SHA-256")
    expected_marker = marker_value(str(record["namespace"]), correlation_key)
    if record["marker_value"] != expected_marker or record["marker_hash"] != marker_sha256(expected_marker):
        raise ContractError("manifest marker is not bound to its correlation key")
    if previous is not None:
        for field in ManifestLineage.__dataclass_fields__:
            record_field = field
            if field == "portal_fingerprint":
                record_field = "portal_fingerprint"
            if record.get(record_field) != previous.get(record_field):
                raise ContractError(f"manifest lineage changed at {field}")
    unhashed = dict(record)
    actual_hash = unhashed.pop("record_hash")
    if actual_hash != content_sha256(unhashed):
        raise ContractError("manifest record_hash does not match canonical content")


def load_manifest(path: Path, *, expected: ManifestLineage | None = None) -> list[dict[str, Any]]:
    """Load and fully validate an append-only manifest chain."""
    lines = _read_bounded_bytes(path, ceiling=MAX_MANIFEST_INPUT_BYTES, kind="manifest").splitlines()
    if not lines:
        raise ContractError("manifest is empty")
    records: list[dict[str, Any]] = []
    previous: dict[str, Any] | None = None
    for line_number, raw in enumerate(lines, start=1):
        value = strict_json_loads(raw)
        if not isinstance(value, dict):
            raise ContractError(f"manifest line {line_number} is not an object")
        validate_manifest_record(value, previous=previous)
        records.append(value)
        previous = value
    if expected is not None:
        first = records[0]
        for field in ManifestLineage.__dataclass_fields__:
            if first[field] != getattr(expected, field):
                raise ContractError(f"manifest {field} is incompatible with requested resume lineage")
    return records


def validate_manifest_against_plan(records: Sequence[Mapping[str, Any]], plan: Mapping[str, Any]) -> None:
    """Bind every manifest entity and lifecycle transition to the reviewed dataset plan."""
    expected: dict[str, Mapping[str, Any]] = {}
    for cell in cast("list[Mapping[str, Any]]", plan["cells"]):
        for index in range(int(cell["target_count"])):
            correlation = content_sha256([plan["run_id"], cell["id"], index])
            expected[correlation] = cell
    current_events: dict[str, str] = {}
    allowed: dict[str | None, frozenset[str]] = {
        None: frozenset({"planned"}),
        "planned": frozenset({"create_dispatched"}),
        "create_dispatched": frozenset({"created", "ambiguous", "reconciled", "absence_verified"}),
        "ambiguous": frozenset({"reconciled", "absence_verified"}),
        "created": frozenset({"reconciled", "verified", "delete_dispatched", "absence_verified", "orphan"}),
        "reconciled": frozenset({"verified", "delete_dispatched", "absence_verified", "orphan"}),
        "verified": frozenset({"delete_dispatched", "absence_verified", "orphan"}),
        "delete_dispatched": frozenset({"delete_dispatched", "deleted", "absence_verified", "orphan"}),
        "deleted": frozenset({"delete_dispatched", "absence_verified", "orphan"}),
        "orphan": frozenset({"delete_dispatched", "absence_verified"}),
        "absence_verified": frozenset(),
    }
    for record in records:
        correlation = str(record["correlation_key"])
        matched_cell = expected.get(correlation)
        if matched_cell is None:
            raise ContractError("manifest correlation_key is not a member of the reviewed dataset plan")
        if record["cell_id"] != matched_cell["id"] or record["entity_family"] != matched_cell["entity_family"]:
            raise ContractError("manifest entity metadata does not match its reviewed dataset cell")
        event = str(record["event"])
        previous_event = current_events.get(correlation)
        recovered_genesis = (
            previous_event is None
            and event == "reconciled"
            and record.get("request_fingerprint") == content_sha256(["recover-manifest", correlation])
        )
        if not recovered_genesis and event not in allowed.get(previous_event, frozenset()):
            raise ContractError(f"manifest lifecycle transition {previous_event!r} -> {event!r} is invalid")
        current_events[correlation] = event


def append_manifest_record(path: Path, record: Mapping[str, Any]) -> None:
    """Commit one validated logical append through an atomic whole-file replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(f"{path.suffix}.lock")
    lock_descriptor = os.open(lock_path, os.O_WRONLY | os.O_CREAT, 0o600)
    try:
        fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
        manifest_existed = path.exists()
        existing = (
            _read_bounded_bytes(path, ceiling=MAX_MANIFEST_INPUT_BYTES, kind="manifest") if manifest_existed else b""
        )
        previous_records = load_manifest(path) if manifest_existed else []
        previous = previous_records[-1] if previous_records else None
        validate_manifest_record(record, previous=previous)
        record_payload = canonical_json(record) + b"\n"
        scan_bytes_for_secrets(record_payload, source=str(path))
        separator = b"" if not existing or existing.endswith(b"\n") else b"\n"
        payload = existing + separator + record_payload
        if len(payload) > MAX_MANIFEST_INPUT_BYTES:
            raise ContractError("manifest exceeds the reviewed byte ceiling")
        _atomic_write_bytes(path, payload)
    finally:
        fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
        os.close(lock_descriptor)


def build_manifest_record(base: Mapping[str, Any], *, previous: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Add sequence/link/hash fields to a record body and validate it."""
    record = dict(base)
    record["sequence"] = 0 if previous is None else int(previous["sequence"]) + 1
    record["previous_record_hash"] = None if previous is None else previous["record_hash"]
    unhashed = dict(record)
    record["record_hash"] = content_sha256(unhashed)
    validate_manifest_record(record, previous=previous)
    return record


def validate_oracle_record(record: Mapping[str, Any]) -> None:
    """Enforce qualification and mutation invariants for oracle PASS."""
    validate_schema(record, "oracle-record")
    _require_uuid(record.get("run_id"), "run_id")
    _require_uuid(record.get("lineage_id"), "lineage_id")
    _require_sha(record.get("candidate_sha"), "candidate_sha")
    for field in (
        "dataset_plan_content_hash",
        "manifest_content_hash",
        "portal_fingerprint",
        "expected_result_hash",
        "actual_result_hash",
    ):
        _require_sha256(record.get(field), field)
    outcome = record.get("outcome")
    requirement = record.get("snapshot_requirement")
    state = record.get("snapshot_state")
    pre_hash = record.get("pre_hash")
    post_hash = record.get("post_hash")
    retries = _nonnegative_int(record.get("mutation_retries", 0), "mutation_retries")
    raw_count = _nonnegative_int(record.get("raw_count"), "raw_count")
    unique_count = _nonnegative_int(record.get("unique_count"), "unique_count")
    if unique_count > raw_count:
        raise ContractError("oracle unique_count cannot exceed raw_count")
    if retries > MAX_MUTATION_RETRIES:
        raise ContractError("oracle mutation retries exceed the reviewed maximum")
    if outcome == "PASS":
        if record.get("expected_result_hash") != record.get("actual_result_hash"):
            raise ContractError("oracle PASS requires exact candidate/oracle result hash equality")
        if record.get("qualification") not in {
            "immutable_manifest",
            "bounded_point_read",
            "independent_cross_method",
            "independent_serial_reference",
        }:
            raise ContractError("PASS requires an independent qualified oracle")
        if requirement in {"frozen_manifest", "independent_pre_post_oracle"} and (
            state != "verified" or not _is_sha256(pre_hash) or pre_hash != post_hash
        ):
            raise ContractError("snapshot-qualified PASS requires equal non-null pre/post hashes")
    if state == "changed" and outcome not in {"INCONCLUSIVE", "ORACLE_SUSPECT"}:
        raise ContractError("changed snapshot cannot pass or fail the candidate")
    if retries == MAX_MUTATION_RETRIES and state == "changed" and outcome != "INCONCLUSIVE":
        raise ContractError("persistent mutation must be INCONCLUSIVE")


def validate_benchmark_plan(plan: Mapping[str, Any]) -> None:  # noqa: C901
    """Enforce preregistered normative performance gates."""
    validate_schema(plan, "benchmark-plan")
    state = plan.get("admission_state")
    controls = _mapping(plan.get("controls"), "controls")
    drift = _mapping(controls.get("drift"), "controls.drift")
    cases = plan.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ContractError("benchmark plan requires at least one case")
    if state == "draft":
        if plan.get("thresholds_normative") is not False:
            raise ContractError("draft benchmark thresholds must be explicitly non-normative")
        return
    if state != "admission_ready" or plan.get("thresholds_normative") is not True:
        raise ContractError("benchmark admission state is invalid")
    if drift.get("status") != "preregistered":
        raise ContractError("admission-ready drift thresholds cannot be TBD-LIVE")
    _ratio_threshold(drift.get("max_rtt_ratio"), "max_rtt_ratio")
    _ratio_threshold(drift.get("max_operating_ratio"), "max_operating_ratio")
    for index, case in enumerate(cases):
        case_map = _mapping(case, f"cases[{index}]")
        plans = case_map.get("compared_plans")
        gate = _mapping(case_map.get("benefit_gate"), f"cases[{index}].benefit_gate")
        if gate.get("blocking") is not True or not isinstance(plans, list) or len(set(plans)) < MINIMUM_COMPARED_PLANS:
            raise ContractError("each admission case requires a blocking comparison of two distinct plans")
        improvement = _nonnegative_number(gate.get("minimum_median_improvement"), "minimum_median_improvement")
        small = _nonnegative_number(gate.get("maximum_small_p95_ratio"), "maximum_small_p95_ratio")
        operating = _nonnegative_number(gate.get("maximum_server_operating_ratio"), "maximum_server_operating_ratio")
        if improvement < NORMATIVE_MINIMUM_MEDIAN_IMPROVEMENT:
            raise ContractError("blocking median improvement is below the normative 15%")
        if gate.get("paired_95_interval_excludes_parity") is not True:
            raise ContractError("blocking comparison requires paired 95% support")
        if small > NORMATIVE_MAXIMUM_SMALL_P95_RATIO or operating > NORMATIVE_MAXIMUM_OPERATING_RATIO:
            raise ContractError("blocking loss/operating ratio exceeds the normative ceiling")


def derive_drift_controls(  # noqa: PLR0913
    *,
    rtt_before: float,
    rtt_after: float,
    operating_before: float,
    operating_after: float,
    max_rtt_ratio: float,
    max_operating_ratio: float,
) -> dict[str, float | bool]:
    """Compute, rather than accept, benchmark control ratios and quarantine."""
    values = (rtt_before, rtt_after, operating_before, operating_after, max_rtt_ratio, max_operating_ratio)
    if any(not isinstance(value, int | float) or not math.isfinite(value) or value <= 0 for value in values):
        raise ContractError("benchmark drift inputs must be positive finite numbers")
    rtt_ratio = max(rtt_before, rtt_after) / min(rtt_before, rtt_after)
    operating_ratio = max(operating_before, operating_after) / min(operating_before, operating_after)
    return {
        "rtt_before_seconds": rtt_before,
        "rtt_after_seconds": rtt_after,
        "operating_before_seconds": operating_before,
        "operating_after_seconds": operating_after,
        "rtt_ratio": rtt_ratio,
        "operating_ratio": operating_ratio,
        "max_rtt_ratio": max_rtt_ratio,
        "max_operating_ratio": max_operating_ratio,
        "drift_quarantined": rtt_ratio > max_rtt_ratio or operating_ratio > max_operating_ratio,
    }


def validate_evidence_artifact(artifact: Mapping[str, Any]) -> None:  # noqa: C901, PLR0912
    """Reject contradictory outcomes, metrics, cleanup, drift, or lineage."""
    early_violations = artifact.get("safe_violations")
    if (
        artifact.get("outcome") == "PASS"
        and isinstance(early_violations, list)
        and any(isinstance(item, dict) and item.get("severity") == "blocking" for item in early_violations)
    ):
        raise ContractError("PASS cannot contain a blocking violation")
    early_metrics = artifact.get("metrics")
    if (
        artifact.get("command") == "cleanup"
        and artifact.get("outcome") == "PASS"
        and isinstance(early_metrics, dict)
        and (early_metrics.get("orphan_count") != 0 or early_metrics.get("absence_verified") is not True)
    ):
        raise ContractError("cleanup PASS requires zero orphans and verified absence")
    validate_schema(artifact, "evidence-artifact")
    if artifact.get("schema_version") != SCHEMA_VERSION:
        raise ContractError("unsupported evidence artifact schema_version")
    _require_uuid(artifact.get("run_id"), "run_id")
    _require_uuid(artifact.get("lineage_id"), "lineage_id")
    _require_sha(artifact.get("candidate_sha"), "candidate_sha")
    outcome = artifact.get("outcome")
    command = artifact.get("command")
    violations = artifact.get("safe_violations")
    if not isinstance(violations, list):
        raise ContractError("safe_violations must be an array")
    if outcome == "PASS":
        if artifact.get("terminal_state") != "completed":
            raise ContractError("PASS requires terminal_state=completed")
        if any(isinstance(item, dict) and item.get("severity") == "blocking" for item in violations):
            raise ContractError("PASS cannot contain a blocking violation")
        requirement = artifact.get("snapshot_requirement")
        if (
            requirement in {"frozen_manifest", "independent_pre_post_oracle"}
            and artifact.get("snapshot_state") != "verified"
        ):
            raise ContractError("snapshot-qualified PASS requires verified snapshot")
    metrics = _mapping(artifact.get("metrics"), "metrics")
    _require_finite(metrics)
    if metrics.get("kind") == "benchmark":
        raw = _nonnegative_int(metrics.get("raw_rows"), "metrics.raw_rows")
        unique = _nonnegative_int(metrics.get("unique_rows"), "metrics.unique_rows")
        duplicates = _nonnegative_int(metrics.get("duplicates"), "metrics.duplicates")
        if unique > raw or duplicates != raw - unique:
            raise ContractError("duplicates must equal raw_rows - unique_rows")
        if outcome == "PASS" and any(
            _nonnegative_int(metrics.get(field), f"metrics.{field}") != 0
            for field in ("shortfall", "overfetch", "reference_failures")
        ):
            raise ContractError("benchmark PASS requires zero correctness shortfall/overfetch/reference failures")
        if outcome == "PASS" and artifact.get("assurance") != "oracle_verified":
            raise ContractError("benchmark PASS requires oracle_verified assurance")
        _require_sha256(artifact.get("benchmark_plan_content_hash"), "benchmark_plan_content_hash")
        controls = _mapping(artifact.get("controls"), "controls")
        derived = derive_drift_controls(
            rtt_before=_number(controls.get("rtt_before_seconds"), "rtt_before_seconds"),
            rtt_after=_number(controls.get("rtt_after_seconds"), "rtt_after_seconds"),
            operating_before=_number(controls.get("operating_before_seconds"), "operating_before_seconds"),
            operating_after=_number(controls.get("operating_after_seconds"), "operating_after_seconds"),
            max_rtt_ratio=_number(controls.get("max_rtt_ratio"), "max_rtt_ratio"),
            max_operating_ratio=_number(controls.get("max_operating_ratio"), "max_operating_ratio"),
        )
        for key in ("rtt_ratio", "operating_ratio", "drift_quarantined"):
            if controls.get(key) != derived[key]:
                raise ContractError(f"benchmark control {key} was asserted instead of derived")
        if outcome == "PASS" and derived["drift_quarantined"]:
            raise ContractError("drift-quarantined benchmark cannot PASS")
    if command == "cleanup":
        orphan_count = _nonnegative_int(metrics.get("orphan_count"), "metrics.orphan_count")
        absence_verified = metrics.get("absence_verified")
        if outcome == "PASS" and (orphan_count != 0 or absence_verified is not True):
            raise ContractError("cleanup PASS requires zero orphans and verified absence")


def validate_probe_envelope(envelope: Mapping[str, Any], *, who_id: str) -> dict[str, Any]:
    """Validate the PHP-polymorphic result_error shape and exact dependent result."""
    if "result_error" not in envelope:
        raise ContractError("batch probe response is missing result_error")
    raw_errors = envelope["result_error"]
    if raw_errors == []:
        shape = "empty_array"
        errors: Mapping[str, Any] = {}
    elif isinstance(raw_errors, dict):
        shape = "associative_object"
        errors = raw_errors
    else:
        raise ContractError("result_error must be an empty PHP array or associative object")
    result = envelope.get("result")
    if result == []:
        result_map: Mapping[str, Any] = {}
    elif isinstance(result, dict):
        result_map = result
    else:
        raise ContractError("result must be an empty PHP array or associative object")
    dependent = result_map.get("dependent")
    if not isinstance(dependent, list):
        raise ContractError("probe dependent result must be an array")
    unexpected_result_keys = sorted(str(key) for key in result_map if key not in {"who", "dependent"})
    dependent_ids = [str(row.get("ID")) for row in dependent if isinstance(row, dict) and "ID" in row]
    unexpected_error_keys = sorted(str(key) for key in errors if key not in {"who", "dependent"})
    matched = (
        len(dependent) == 1
        and dependent_ids == [who_id]
        and not errors
        and not unexpected_error_keys
        and not unexpected_result_keys
    )
    return {
        "result_error_shape": shape,
        "dependent_ids": dependent_ids,
        "unexpected_error_keys": unexpected_error_keys,
        "matched": matched,
    }


def validate_schema(document: Mapping[str, Any], schema_name: str) -> None:
    """Validate one document without including rejected values in diagnostics."""
    schema_path = SCHEMA_DIR / f"{schema_name}.schema.json"
    schema = strict_json_loads(schema_path.read_bytes())
    if not isinstance(schema, dict):
        raise ContractError(f"{schema_name} schema is not an object")
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as error:
        raise ContractError(f"{schema_name} schema is invalid at {error.json_path}") from error
    errors = list(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(document))
    if errors:
        first = sorted(errors, key=lambda error: (str(error.json_path), str(error.validator)))[0]
        raise ContractError(
            f"{schema_name} schema rejection at {first.json_path or '$'} ({first.validator})",
        )


def scan_bytes_for_secrets(data: bytes, *, source: str) -> None:
    """Fail on credential forms without echoing the matched secret."""
    allowlisted = (
        b"https://bitrix24.com/rest/0/test/",
        b"https://environment.invalid/rest/1/redacted/",
        b"https://explicit.invalid/rest/1/redacted/",
        b"Bearer n1x2y3z4q5w6e7r8",
        b"Bearer {EXAMPLE_CREDENTIAL}",
        b"{EXAMPLE_CREDENTIAL}",
        b"abcdef1234567890",
        b"zyxwvutsrqponmlk",
    )

    def decode_ascii_escape(match: re.Match[bytes]) -> bytes:
        value = int(match.group(1), 16)
        return bytes((value,)) if value <= MAX_ASCII_CODEPOINT else match.group(0)

    normalized = data
    for encoding, bom in (
        ("utf-32", (b"\xff\xfe\x00\x00", b"\x00\x00\xfe\xff")),
        ("utf-16", (b"\xff\xfe", b"\xfe\xff")),
    ):
        if data.startswith(bom):
            try:
                decoded = data.decode(encoding).encode("utf-8")
            except UnicodeDecodeError:
                decoded = b""
            normalized += decoded
            break
    sanitized = _ASCII_UNICODE_ESCAPE_RE.sub(decode_ascii_escape, normalized).replace(b"\\/", b"/")
    for literal in allowlisted:
        sanitized = sanitized.replace(literal, b"")
    if _WEBHOOK_RE.search(sanitized) or _QUERY_SECRET_RE.search(sanitized) or _ENV_SECRET_RE.search(sanitized):
        raise SecretLeakError(f"credential pattern detected in {source}")


def scan_paths_for_secrets(paths: Iterable[Path]) -> None:
    """Scan a bounded set of files and report only the path containing a leak."""
    for path in paths:
        if path.is_file():
            data = _read_bounded_bytes(path, ceiling=MAX_SCANNED_FILE_BYTES, kind="secret-scan input")
            if path.name == "contracts_test.py":
                data = b"\n".join(
                    line
                    for line in data.splitlines()
                    if hashlib.sha256(line).hexdigest() not in _CONTRACT_TEST_SECRET_FIXTURE_LINE_SHA256
                )
            scan_bytes_for_secrets(data, source=str(path))


def tracked_repository_paths(root: Path) -> list[Path]:
    """Return tracked regular files only; untracked credentials are never read."""
    git = shutil.which("git")
    if git is None:
        raise ContractError("git executable is unavailable")
    try:
        result = subprocess.run(  # noqa: S603 - fixed git executable and literal arguments
            [git, "ls-files", "-z"],
            cwd=root,
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise ContractError("cannot enumerate tracked repository files") from error
    return [root / item.decode() for item in result.stdout.split(b"\0") if item]


def git_sha(root: Path) -> str:
    """Resolve the exact candidate commit without accepting dirty content as evidence."""
    git = shutil.which("git")
    if git is None:
        raise ContractError("git executable is unavailable")
    try:
        value = subprocess.run(  # noqa: S603 - fixed git executable and literal arguments
            [git, "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise ContractError("cannot resolve candidate git SHA") from error
    _require_sha(value, "candidate git SHA")
    return value


def require_clean_tracked_tree(root: Path) -> None:
    """Reject evidence from staged or unstaged tracked content outside HEAD."""
    git = shutil.which("git")
    if git is None:
        raise ContractError("git executable is unavailable")
    for arguments in (("diff", "--quiet", "--"), ("diff", "--cached", "--quiet", "--")):
        result = subprocess.run(  # noqa: S603 - fixed git executable and literal arguments
            [git, *arguments],
            cwd=root,
            check=False,
            capture_output=True,
        )
        if result.returncode == 1:
            raise ContractError("evidence requires a clean tracked tree at the exact candidate SHA")
        if result.returncode != 0:
            raise ContractError("cannot verify tracked-tree cleanliness")


def manifest_content_hash(path: Path) -> str:
    """Hash exact manifest bytes after validating the full chain."""
    load_manifest(path)
    return file_sha256(path)


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ContractError(f"{field} must be an object")
    return value


def _integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError(f"{field} must be an integer")
    return cast("int", value)


def _nonnegative_int(value: Any, field: str) -> int:
    result = _integer(value, field)
    if result < 0:
        raise ContractError(f"{field} must be non-negative")
    return result


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value):
        raise ContractError(f"{field} must be a finite number")
    return float(value)


def _nonnegative_number(value: Any, field: str) -> float:
    result = _number(value, field)
    if result < 0:
        raise ContractError(f"{field} must be non-negative")
    return result


def _ratio_threshold(value: Any, field: str) -> float:
    result = _number(value, field)
    if result < 1:
        raise ContractError(f"{field} must be at least parity")
    return result


def _require_uuid(value: Any, field: str) -> None:
    if not isinstance(value, str) or not _UUID_RE.fullmatch(value):
        raise ContractError(f"{field} must be a lowercase UUID")
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise ContractError(f"{field} must be a lowercase UUID") from error
    if str(parsed) != value:
        raise ContractError(f"{field} must be a lowercase canonical UUID")


def _require_sha(value: Any, field: str) -> None:
    if not isinstance(value, str) or not _SHA_RE.fullmatch(value):
        raise ContractError(f"{field} must be a 40-character lowercase git SHA")


def _require_sha256(value: Any, field: str) -> None:
    if not _is_sha256(value):
        raise ContractError(f"{field} must be a lowercase SHA-256")


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


def _require_exact_keys(
    value: Mapping[str, Any],
    required: set[str],
    label: str,
    *,
    optional: set[str] | None = None,
) -> None:
    actual = set(value)
    missing = required - actual
    unexpected = actual - required - (optional or set())
    if missing or unexpected:
        raise ContractError(f"{label} keys mismatch; missing={sorted(missing)}, unexpected={sorted(unexpected)}")
