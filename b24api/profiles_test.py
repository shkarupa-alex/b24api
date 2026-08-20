"""W8 endpoint-profile validation, selection, explanation, and probe tests."""

from __future__ import annotations
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from b24api.models import CompletionAssurance, ExecutionPolicy, ReplaySafety, Request, ResultSelector
from b24api.profiles import (
    ProbeObservation,
    ProbeStatus,
    ProfileReasonCode,
    QueryShape,
    apply_probe_observations,
    choose_plan,
    explain_plan,
    load_packaged_profiles,
    load_profile_document,
    load_profile_schema,
    query_shape_from_request,
)

_OBSERVED_AT = datetime(2026, 8, 20, tzinfo=UTC)
_ARTIFACT_SHA = "b" * 64
_CANDIDATE_SHA = "a" * 40
_PAGE_CAP = 50


def _document() -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "profile_id": "crm-item-keyset-v1",
        "version": 1,
        "endpoint": "crm-item-list",
        "method": "crm.item.list",
        "verified_at": "2026-01-01T00:00:00+00:00",
        "expires_at": "2027-01-01T00:00:00+00:00",
        "applicable_builds": ["build-1"],
        "required_scopes": ["crm"],
        "query": {
            "parameter_paths": [
                ["filter"],
                ["filter", ">ID"],
                ["order"],
                ["order", "ID"],
                ["select"],
            ],
            "filter_keys": ["ID"],
            "filter_operators": [">"],
            "order": [["ID", "ASC"]],
            "selector": [],
        },
        "plan": {
            "kind": "keyset",
            "identity_requirement": "required",
            "order_semantics": "ascending",
            "duplicate_policy": "error",
            "total_semantics": "ignore",
            "direction": "asc",
            "filter_path": ["filter"],
            "order_path": ["order"],
            "limit_path": ["limit"],
            "requested_page_size": _PAGE_CAP,
            "start_suppression_path": ["start"],
            "terminal": "empty_confirmation",
            "allow_create_controls": True,
        },
        "identity": {
            "item_path": ["ID"],
            "filter_key": "ID",
            "order_key": "ID",
            "coercion": "decimal_string_integer",
        },
        "page_cap": _PAGE_CAP,
        "replay_safety": "safe",
        "capabilities": {
            "offset_honored": False,
            "stable_order": True,
            "filter_honored": True,
            "cursor_honored": False,
            "batch_supported": True,
            "fixed_page_cap": True,
        },
        "evidence": [
            {
                "artifact_sha256": _ARTIFACT_SHA,
                "candidate_sha": _CANDIDATE_SHA,
                "reviewed_at": "2025-12-20T00:00:00+00:00",
                "expires_at": "2027-06-01T00:00:00+00:00",
                "review_status": "accepted",
            },
        ],
        "required_probes": [
            {
                "probe_id": "bounded-keyset-check",
                "method": "crm.item.list",
                "max_rows": 2,
                "selector": [],
                "minimal_select": ["ID"],
                "within_caller_filter": True,
            },
        ],
    }


def _query(profile_document: dict[str, object] | None = None) -> QueryShape:
    profile = load_profile_document(profile_document or _document())
    return QueryShape(
        method=profile.method,
        parameter_paths=profile.query.parameter_paths,
        filter_keys=profile.query.filter_keys,
        filter_operators=profile.query.filter_operators,
        order=profile.query.order,
        selector=profile.query.selector,
        scopes=frozenset({"crm"}),
        portal_build="build-1",
        observed_at=_OBSERVED_AT,
    )


def test_packaged_zero_profile_release_and_schema_are_loadable() -> None:
    assert load_packaged_profiles() == ()
    schema = load_profile_schema()
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["additionalProperties"] is False
    assert "global" in str(schema)
    assert "unfiltered" not in str(schema)


def test_strict_loader_builds_immutable_profile_with_content_provenance() -> None:
    document = _document()
    profile = load_profile_document(document)
    same = load_profile_document(deepcopy(document))

    assert profile == same
    assert profile.source_sha256 == same.source_sha256
    assert profile.replay_safety is ReplaySafety.SAFE
    assert profile.page_cap == _PAGE_CAP
    assert profile.identity is not None
    assert profile.evidence[0].artifact_sha256 == _ARTIFACT_SHA


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        (lambda value: value.__setitem__("webhook_url", "https://host/rest/1/token/"), "unknown fields"),
        (lambda value: value.__setitem__("version", 1e400), "finite"),
        (lambda value: value["query"].__setitem__("order", [["ID"]]), "exactly two"),
    ],
)
def test_loader_rejects_unknown_secret_bearing_fields_nonfinite_values_and_malformed_order(
    mutation: object,
    error: str,
) -> None:
    document = _document()
    assert callable(mutation)
    mutation(document)

    with pytest.raises((TypeError, ValueError), match=error):
        load_profile_document(document)


def test_profile_capabilities_are_machine_bound_to_selected_plan() -> None:
    document = _document()
    capabilities = document["capabilities"]
    assert isinstance(capabilities, dict)
    capabilities["filter_honored"] = False

    with pytest.raises(ValueError, match="filter and stable-order"):
        load_profile_document(document)


def test_profile_cannot_outlive_or_predate_its_reviewed_evidence() -> None:
    postdated = _document()
    evidence = postdated["evidence"]
    assert isinstance(evidence, list)
    assert isinstance(evidence[0], dict)
    evidence[0]["reviewed_at"] = "2026-02-01T00:00:00+00:00"
    with pytest.raises(ValueError, match="predate"):
        load_profile_document(postdated)

    outlives = _document()
    evidence = outlives["evidence"]
    assert isinstance(evidence, list)
    assert isinstance(evidence[0], dict)
    evidence[0]["expires_at"] = "2026-12-01T00:00:00+00:00"
    with pytest.raises(ValueError, match="outlive"):
        load_profile_document(outlives)


def test_pure_chooser_accepts_only_exact_applicable_reviewed_shape() -> None:
    profile = load_profile_document(_document())
    decision = choose_plan(profile, _query(), ExecutionPolicy())

    assert decision.accepted
    assert decision.selected_plan is profile.plan
    assert decision.assurance is CompletionAssurance.PROFILE_VERIFIED
    explanation = explain_plan(decision).to_safe_dict()
    assert explanation["profile_id"] == profile.profile_id
    assert explanation["evidence_sha256"] == [_ARTIFACT_SHA]
    assert "rest/" not in str(explanation)


@pytest.mark.parametrize(
    ("query_case", "reason"),
    [
        ("unknown_build", ProfileReasonCode.UNKNOWN_BUILD),
        ("other_build", ProfileReasonCode.BUILD_MISMATCH),
        ("missing_scope", ProfileReasonCode.MISSING_SCOPE),
        ("other_method", ProfileReasonCode.METHOD_MISMATCH),
        ("other_filter", ProfileReasonCode.QUERY_SHAPE_MISMATCH),
        ("expired", ProfileReasonCode.EXPIRED),
    ],
)
def test_chooser_refuses_inapplicable_profile_without_fallback_promotion(
    query_case: str,
    reason: ProfileReasonCode,
) -> None:
    profile = load_profile_document(_document())
    query = _inapplicable_query(query_case)
    decision = choose_plan(profile, query, ExecutionPolicy())

    assert not decision.accepted
    assert decision.assurance is CompletionAssurance.CALLER_ASSERTED
    assert reason in {item.code for item in decision.reasons}


def _inapplicable_query(query_case: str) -> QueryShape:
    query = _query()
    if query_case == "unknown_build":
        return replace(query, portal_build=None)
    if query_case == "other_build":
        return replace(query, portal_build="other-build")
    if query_case == "missing_scope":
        return replace(query, scopes=frozenset())
    if query_case == "other_method":
        return replace(query, method="tasks.task.list")
    if query_case == "other_filter":
        return replace(query, filter_keys=frozenset({"OTHER"}))
    if query_case == "expired":
        return replace(query, observed_at=datetime(2027, 1, 1, tzinfo=UTC))
    raise AssertionError(f"unknown query test case: {query_case}")


def test_chooser_refuses_profile_whose_page_cap_exceeds_policy() -> None:
    profile = load_profile_document(_document())
    policy = ExecutionPolicy(max_buffered_rows=_PAGE_CAP - 1)

    decision = choose_plan(profile, _query(), policy)

    assert not decision.accepted
    assert [reason.code for reason in decision.reasons] == [ProfileReasonCode.POLICY_INCOMPATIBLE]


def test_probe_observations_only_preserve_or_downgrade_decision() -> None:
    profile = load_profile_document(_document())
    accepted = choose_plan(profile, _query(), ExecutionPolicy())
    passing = ProbeObservation("bounded-keyset-check", ProbeStatus.PASS, observed_rows=2)

    assert apply_probe_observations(accepted, [passing]) is accepted
    missing = apply_probe_observations(accepted, [])
    assert not missing.accepted
    assert [reason.code for reason in missing.reasons] == [ProfileReasonCode.PROBE_MISSING]

    contradicted = apply_probe_observations(
        accepted,
        [ProbeObservation("bounded-keyset-check", ProbeStatus.CONTRADICTED, observed_rows=1)],
    )
    assert not contradicted.accepted
    assert [reason.code for reason in contradicted.reasons] == [ProfileReasonCode.PROBE_CONTRADICTION]

    refused = choose_plan(profile, replace(_query(), portal_build=None), ExecutionPolicy())
    still_refused = apply_probe_observations(refused, [passing])
    assert not still_refused.accepted
    assert ProfileReasonCode.UNKNOWN_BUILD in {reason.code for reason in still_refused.reasons}


def test_query_shape_records_structure_without_literal_values() -> None:
    first = query_shape_from_request(
        Request(
            "crm.item.list",
            {"filter": {">ID": 1, "STATUS": "NEW"}, "order": {"ID": "asc"}, "select": ["ID"]},
        ),
        selector=ResultSelector.root(),
        scopes={"crm"},
        portal_build="build-1",
        observed_at=_OBSERVED_AT,
    )
    second = query_shape_from_request(
        Request(
            "crm.item.list",
            {"filter": {">ID": 999, "STATUS": "SECRET"}, "order": {"ID": "ASC"}, "select": ["ID"]},
        ),
        selector=ResultSelector.root(),
        scopes={"crm"},
        portal_build="build-1",
        observed_at=_OBSERVED_AT,
    )

    assert first == second
    assert first.filter_keys == frozenset({"ID", "STATUS"})
    assert first.filter_operators == frozenset({">", "="})
    assert "SECRET" not in repr(first)
