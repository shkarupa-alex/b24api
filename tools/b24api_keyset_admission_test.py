"""Deterministic keyset admission fixture timing tests."""

# ruff: noqa: SLF001

from __future__ import annotations
import json
from types import SimpleNamespace

import pytest

from b24api import AutoKeysetExecution, PartitionedKeysetExecution, RangeKeysetExecution, ReplaySafety, Request
from tools import b24api_keyset_admission as harness


@pytest.mark.asyncio
async def test_fixture_latency_is_paid_inside_the_measured_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    delays: list[float] = []

    async def record_delay(seconds: float) -> None:
        delays.append(seconds)

    monkeypatch.setattr("tools.b24api_keyset_admission.asyncio.sleep", record_delay)
    portal = harness.FixturePortal((1, 2, 3))

    await portal.send(
        Request("fixture.list", replay_safety=ReplaySafety.SAFE),
        attempt_timeout=1.0,
        max_response_bytes=1_000,
    )

    assert delays == [harness.FIXTURE_REQUEST_LATENCY_SECONDS]


@pytest.mark.asyncio
async def test_fixture_artifact_discloses_the_controlled_latency_model(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(harness, "_cells", lambda: ())

    artifact = await harness.generate(20, sha="candidate")

    timing = artifact["manifest"]["timing_model"]
    assert isinstance(timing, dict)
    assert timing == {
        "kind": "measured_wall_with_fixed_request_latency",
        "request_latency_seconds": harness.FIXTURE_REQUEST_LATENCY_SECONDS,
    }
    assert artifact["samples"] == []


def test_fixture_uses_contractual_sample_counts_without_oversampling() -> None:
    cases = (("small", 20, 20), ("dense_large", 20, 5), ("dense_large", 3, 3))
    for name, requested, expected in cases:
        assert harness._fixture_sample_count(harness.Cell(name, (1,)), requested) == expected


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("range", RangeKeysetExecution),
        ("partitioned", PartitionedKeysetExecution),
        ("auto", AutoKeysetExecution),
    ],
)
def test_live_harness_constructs_every_declared_execution(mode: str, expected: type[object]) -> None:
    execution = harness._live_execution(mode)

    assert isinstance(execution, expected)
    assert isinstance(execution, RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution)
    assert execution.contract.endpoint_page_cap == harness.PAGE_SIZE


@pytest.mark.asyncio
async def test_live_generator_sandwiches_every_declared_mode_without_network(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str | None]] = []

    async def fake_run(cell: harness.LiveCell, mode: str | None) -> SimpleNamespace:
        calls.append((cell.name, mode))
        return SimpleNamespace(identities=list(range(1, 1_001)))

    def fake_record(_run: object, *, oracle: set[int] | None = None) -> dict[str, object]:
        del oracle
        return {"requests": 1, "wall_seconds": 0.01}

    monkeypatch.setattr(harness, "_run_live", fake_run)
    monkeypatch.setattr(harness, "_record", fake_record)
    monkeypatch.setattr(
        harness,
        "_live_cells",
        lambda: (harness._task_live_cell("tasks_all", {}, harness.TotalHintMode.IGNORE, "range"),),
    )

    artifact = await harness.generate_live_range(0, sha="candidate")

    assert calls == [
        ("tasks_all", None), ("tasks_all", "range"), ("tasks_all", None),
        ("tasks_all", None), ("tasks_all", "partitioned"), ("tasks_all", None),
        ("tasks_all", None), ("tasks_all", "auto"), ("tasks_all", None),
    ]
    assert [sample["mode"] for sample in artifact["samples"]] == list(harness.MODES)
    manifest = artifact["manifest"]
    assert manifest["correctness_scope"] == [["tasks_all", mode] for mode in harness.MODES]
    assert manifest["modes"] == list(harness.MODES)
    assert manifest["attempt_windows"] == harness.LIVE_ATTEMPT_WINDOWS
    assert manifest["live_matrix"]["complete"] is True
    assert manifest["live_matrix"]["shortfalls"]
    assert manifest["live_matrix"]["fallback"]["kind"] == "deterministic_fixture"


@pytest.mark.asyncio
async def test_legacy_live_range_alias_can_retain_range_only_semantics(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str | None]] = []

    async def fake_run(cell: harness.LiveCell, mode: str | None) -> SimpleNamespace:
        calls.append((cell.name, mode))
        return SimpleNamespace(identities=[])

    def fake_record(_run: object, *, oracle: set[int] | None = None) -> dict[str, object]:
        del oracle
        return {"requests": 1, "wall_seconds": 0.01}

    monkeypatch.setattr(harness, "_run_live", fake_run)
    monkeypatch.setattr(harness, "_record", fake_record)
    monkeypatch.setattr(
        harness,
        "_live_cells",
        lambda: (harness._task_live_cell("tasks_all", {}, harness.TotalHintMode.IGNORE, "range"),),
    )

    artifact = await harness.generate_live_range(0, sha="candidate", modes=("range",))

    assert calls == [("tasks_all", None), ("tasks_all", "range"), ("tasks_all", None)]
    assert artifact["manifest"]["modes"] == ["range"]


def test_live_matrix_freezes_requested_filters_hints_and_identity_roles() -> None:
    cells = {cell.name: cell for cell in harness._live_cells()}

    assert json.loads(cells["tasks_all"].parameters_json)["filter"] == {}
    assert json.loads(cells["tasks_responsible_1"].parameters_json)["filter"] == {"RESPONSIBLE_ID": 1}
    assert json.loads(cells["tasks_created_by_1"].parameters_json)["filter"] == {"CREATED_BY": 1}
    assert json.loads(cells["tasks_status_2"].parameters_json)["filter"] == {"STATUS": 2}
    assert {cell.total_hint for cell in cells.values()} == {
        harness.TotalHintMode.IGNORE,
        harness.TotalHintMode.REQUEST_ADVISORY,
    }
    assert cells["tasks_all"].filter_role != cells["tasks_all"].order_role
    assert all(cell.filter_role != cell.order_role for cell in cells.values())


def test_same_case_live_cell_requires_explicit_recordable_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "B24API_KEYSET_SAME_CASE_CELL",
        json.dumps(
            {
                "method": "crm.item.list",
                "parameters": {"entityTypeId": 2, "filter": {}, "select": ["id"]},
                "selector_path": ["items"],
                "item_path": ["id"],
                "identity_field": "id",
                "expected_auto_selection": "range",
            },
        ),
    )

    cell = harness._configured_same_case_cell()

    assert cell is not None
    assert cell.method == "crm.item.list"
    assert cell.filter_role == cell.order_role == "id"
    assert json.loads(cell.parameters_json) == {"entityTypeId": 2, "filter": {}, "select": ["id"]}
