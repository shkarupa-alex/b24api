"""Tests for deep immutability, controls, policies, budgets, and reports."""

from collections.abc import Mapping
from dataclasses import FrozenInstanceError

import pytest

from b24api.error import BudgetExceededError
from b24api.models import (
    BatchFailure,
    BudgetCounters,
    CompletionAssurance,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    JsonValue,
    OperationReport,
    ParameterPath,
    ReferenceFailure,
    ReferenceItem,
    ReferenceRequest,
    ReplayDisposition,
    ReplaySafety,
    Request,
    Response,
    ResultSelector,
    SnapshotState,
    TerminalState,
    Violation,
    ViolationSeverity,
    inject_controls,
)

EXAMPLE_CREDENTIAL = "n1x2y3z4q5w6e7r8"
TEST_LIMIT = 2
TEST_CURSOR = 99


def _identity_list(parameters: Mapping[str, JsonValue]) -> list[JsonValue]:
    filter_value = parameters["filter"]
    assert isinstance(filter_value, dict)
    identities = filter_value["ID"]
    assert isinstance(identities, list)
    return identities


def test_request_is_deeply_immutable_and_accessors_are_detached() -> None:
    caller_ids = [1, 2]
    caller = {"filter": {"ID": caller_ids}, "select": ["ID"]}
    request = Request("tasks.task.list", caller, replay_safety=ReplaySafety.SAFE)
    caller_ids.append(3)

    first = request.copy_parameters()
    assert first == {"filter": {"ID": [1, 2]}, "select": ["ID"]}
    _identity_list(first).append(9)
    assert _identity_list(request.copy_parameters()) == [1, 2]

    exposed = request.parameters
    _identity_list(exposed).append(8)
    assert _identity_list(request.to_wire_parameters()) == [1, 2]
    with pytest.raises(TypeError):
        exposed["new"] = 1  # type: ignore[index]


def test_request_rejects_non_json_nonfinite_and_cycles() -> None:
    cyclic: list[object] = []
    cyclic.append(cyclic)

    with pytest.raises(TypeError):
        Request("profile", {1: "bad"})  # type: ignore[dict-item]
    with pytest.raises(TypeError):
        Request("profile", {"bad": {1, 2}})
    with pytest.raises(ValueError, match="finite"):
        Request("profile", {"bad": float("nan")})
    with pytest.raises(ValueError, match="finite"):
        Request("profile", {"bad": 1e400})
    with pytest.raises(ValueError, match="cyclic"):
        Request("profile", {"bad": cyclic})
    with pytest.raises(ValueError, match="method"):
        Request("bad/method")
    with pytest.raises(TypeError, match="replay_safety"):
        Request("profile", replay_safety="safe")  # type: ignore[arg-type]


def test_request_repr_contains_shape_not_values() -> None:
    request = Request("profile", {"auth": EXAMPLE_CREDENTIAL, "select": ["ID"]})

    assert EXAMPLE_CREDENTIAL not in repr(request)
    assert "auth" in repr(request)
    assert request.replay_safety is None
    assert Request("profile", replay_safety=ReplaySafety.UNKNOWN).replay_safety is ReplaySafety.UNKNOWN


def test_response_is_deeply_immutable_and_selectors_are_exact() -> None:
    source = {"tasks": [{"ID": "1"}]}
    response = Response(source, total=1, next=1)
    source["tasks"][0]["ID"] = "changed"

    assert response.list_result == [{"ID": "1"}]
    selected = response.list_items(ResultSelector(("tasks",)))
    first_item = selected[0]
    assert isinstance(first_item, dict)
    first_item["ID"] = "changed-again"
    assert response.list_items(ResultSelector(("tasks",))) == [{"ID": "1"}]
    with pytest.raises(KeyError):
        response.list_items(ResultSelector(("TASKS",)))
    assert "changed" not in repr(response)
    with pytest.raises(ValueError, match="total"):
        Response([], total=True)
    assert Response([], total=-1).total == -1
    with pytest.raises(ValueError, match="total"):
        Response([], total=-2)


def test_paths_and_identity_validate_before_use() -> None:
    identity = IdentitySpec(
        item_path=("id",),
        filter_key="ID",
        order_key="id",
        coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
    )

    assert identity.item_path == ("id",)
    with pytest.raises(ValueError, match="empty"):
        ParameterPath(())
    with pytest.raises(ValueError, match="empty"):
        ResultSelector(("",))
    with pytest.raises(ValueError, match="non-empty"):
        IdentitySpec(item_path=("id",), filter_key="", order_key="id")


def test_control_injection_rejects_ambiguity_conflicts_and_missing_containers() -> None:
    path = ParameterPath(("filter", ">ID"))

    assert inject_controls({"FILTER": {">ID": 10}}, {path: 10}, allow_create=False) == {"FILTER": {">ID": 10}}
    assert inject_controls({}, {path: 10}, allow_create=True) == {"filter": {">ID": 10}}
    with pytest.raises(ValueError, match="ambiguous"):
        inject_controls({"filter": {}, "FILTER": {}}, {path: 10}, allow_create=False)
    with pytest.raises(ValueError, match="conflicts"):
        inject_controls({"filter": {">ID": 9}}, {path: 10}, allow_create=False)
    with pytest.raises(KeyError, match="missing"):
        inject_controls({}, {path: 10}, allow_create=False)
    with pytest.raises(TypeError, match="non-mapping"):
        inject_controls({"filter": 1}, {path: 10}, allow_create=False)
    with pytest.raises(ValueError, match="same case-insensitive path"):
        inject_controls(
            {},
            {ParameterPath(("filter", "ID")): 1, ParameterPath(("FILTER", "id")): 1},
            allow_create=True,
        )


def test_execution_policy_rejects_invalid_limits() -> None:
    with pytest.raises(ValueError, match="positive"):
        ExecutionPolicy(max_requests=0)
    with pytest.raises(ValueError, match="positive"):
        ExecutionPolicy(max_elapsed=0)
    with pytest.raises(TypeError, match="identity_tracker"):
        ExecutionPolicy(identity_tracker="memory")  # type: ignore[arg-type]


def test_budget_counters_count_attempts_pages_references_and_buffers_before_scheduling() -> None:
    policy = ExecutionPolicy(
        max_requests=TEST_LIMIT,
        max_pages=TEST_LIMIT,
        max_pages_per_reference=1,
        max_attempts_per_request=TEST_LIMIT,
        max_elapsed=10,
        max_retry_elapsed_per_request=5,
        max_buffered_rows=TEST_LIMIT,
    )
    counters = BudgetCounters().reserve_attempt(
        policy,
        attempts_for_request=0,
        retry_elapsed=0,
        total_elapsed=0,
    )
    counters = counters.reserve_attempt(policy, attempts_for_request=1, retry_elapsed=1, total_elapsed=1)
    assert counters.physical_requests == TEST_LIMIT
    with pytest.raises(BudgetExceededError):
        counters.reserve_attempt(policy, attempts_for_request=0, retry_elapsed=0, total_elapsed=0)

    pages = BudgetCounters().reserve_page(policy, reference="A")
    with pytest.raises(BudgetExceededError):
        pages.reserve_page(policy, reference="A")
    pages = pages.reserve_page(policy, reference="B")
    assert pages.logical_pages == TEST_LIMIT
    with pytest.raises(BudgetExceededError):
        pages.reserve_page(policy, reference="C")

    buffered = BudgetCounters().with_buffered_rows(policy, TEST_LIMIT).with_buffered_rows(policy, 1)
    assert buffered.buffered_rows == 1
    assert buffered.buffered_rows_high_water == TEST_LIMIT
    with pytest.raises(BudgetExceededError):
        buffered.with_buffered_rows(policy, 3)


def test_report_is_frozen_and_completion_rejects_blocking_violation() -> None:
    report = OperationReport(
        state=TerminalState.COMPLETED,
        assurance=CompletionAssurance.CALLER_ASSERTED,
        snapshot=SnapshotState.UNVERIFIED,
        emitted_rows=1,
        unique_rows=1,
    )

    assert report.completed is True
    with pytest.raises(FrozenInstanceError):
        report.state = TerminalState.FAILED  # type: ignore[misc]
    with pytest.raises(ValueError, match="blocking"):
        OperationReport(
            state=TerminalState.COMPLETED,
            violations=(
                Violation(
                    severity=ViolationSeverity.BLOCKING,
                    code="shortfall",
                    message="missing row",
                ),
            ),
        )


def test_profile_verified_report_requires_complete_hash_only_provenance() -> None:
    profile_hash = "a" * 64
    evidence_hash = "b" * 64
    report = OperationReport(
        state=TerminalState.COMPLETED,
        assurance=CompletionAssurance.PROFILE_VERIFIED,
        profile_id="crm-item-v1",
        profile_version=1,
        profile_applicable=True,
        profile_source_sha256=profile_hash,
        profile_evidence_sha256=(evidence_hash,),
        profile_evidence_candidate_sha="c" * 40,
    )

    assert report.profile_source_sha256 == profile_hash
    with pytest.raises(ValueError, match="provenance"):
        OperationReport(assurance=CompletionAssurance.PROFILE_VERIFIED)
    with pytest.raises(ValueError, match="profile metadata"):
        OperationReport(profile_version=1)


def test_failure_repr_excludes_request_error_and_payload_values() -> None:
    request = Request("profile", {"auth": EXAMPLE_CREDENTIAL})
    failure = BatchFailure(
        command_index=0,
        stable_key="_0",
        request=request,
        error={"auth": EXAMPLE_CREDENTIAL},
        payload={"secret": EXAMPLE_CREDENTIAL},
    )

    assert EXAMPLE_CREDENTIAL not in repr(failure)


def test_reference_values_hide_correlation_and_detach_mutable_items() -> None:
    request = Request("profile", {"auth": EXAMPLE_CREDENTIAL})
    reference = ReferenceRequest(request, EXAMPLE_CREDENTIAL)
    source = {"rows": [1]}
    item = ReferenceItem(EXAMPLE_CREDENTIAL, source, payload={"secret": EXAMPLE_CREDENTIAL})
    source["rows"].append(2)
    exposed = item.item
    assert isinstance(exposed, dict)
    rows = exposed["rows"]
    assert isinstance(rows, list)
    rows.append(3)
    failure = ReferenceFailure(
        EXAMPLE_CREDENTIAL,
        request,
        RuntimeError(EXAMPLE_CREDENTIAL),
        cursor=TEST_CURSOR,
        page_state=TEST_LIMIT,
        partial_rows=1,
        replay_disposition=ReplayDisposition.NOT_ELIGIBLE,
        payload={"secret": EXAMPLE_CREDENTIAL},
    )

    assert item.item == {"rows": [1]}
    assert EXAMPLE_CREDENTIAL not in repr(reference)
    assert EXAMPLE_CREDENTIAL not in repr(item)
    assert EXAMPLE_CREDENTIAL not in repr(failure)
    assert failure.cursor == TEST_CURSOR
    assert failure.page_state == TEST_LIMIT
