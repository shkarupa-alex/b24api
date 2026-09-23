"""Run the 19 offline recipes or verify explicitly supplied LIVE evidence.

Offline: ``uv run python -m examples.run --scenario all``.
LIVE evidence: ``uv run python -m examples.run --scenario all --live-evidence evidence.jsonl``.
The latter never contacts a portal implicitly and never promotes a missing fixture to a pass.
"""

from __future__ import annotations
import argparse
import asyncio
import hashlib
import hmac
import importlib
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from b24api.testing import ScriptedExchange, ScriptedTransport
from examples._support.evidence import RecipeEvidence

if TYPE_CHECKING:
    from collections.abc import Sequence

    from b24api import Request

METHOD_CARD_SHA = "909c6bf14b29961a365dc6d6d4c2c47b5e2533d4"
LIVE_ATTESTATION_KEY_ENV = "B24API_LIVE_EVIDENCE_ATTESTATION_KEY"
MINIMUM_ATTESTATION_KEY_BYTES = 32
MINIMUM_HTTP_STATUS = 100
MAXIMUM_HTTP_STATUS = 599
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class Scenario:
    """One runnable recipe and the oracle summarized after its assertions pass."""

    number: int
    module: str
    oracle_name: str
    report_state: str
    assurance: str


SCENARIOS = (
    Scenario(1, "chat_bounded_mirror", "EXPECTED_IDS", "completed", "bounded_prefix"),
    Scenario(2, "message_cursor_direction", "EXPECTED_IDS", "completed", "identity_exact"),
    Scenario(3, "recent_dialogs", "EXPECTED_IDS", "completed", "identity_exact"),
    Scenario(4, "search_chat_messages", "EXPECTED_IDS", "completed_with_failures", "not_applicable"),
    Scenario(5, "task_role_union", "EXPECTED_IDS", "completed", "count_matched"),
    Scenario(6, "task_comments", "EXPECTED_MODERN", "completed_with_failures", "not_applicable"),
    Scenario(7, "chat_resume", "EXPECTED", "completed", "not_applicable"),
    Scenario(8, "crm_item_delta", "EXPECTED_IDS", "completed", "identity_exact"),
    Scenario(9, "calendar_delta", "LIVE_ROWS", "not_applicable", "not_applicable"),
    Scenario(10, "timeline_comments", "EXPECTED_STORAGE_IDS", "completed_with_failures", "not_applicable"),
    Scenario(11, "sparse_user_search", "EXPECTED_IDS", "completed", "raw_range_covered"),
    Scenario(12, "page_index_members", "EXPECTED_IDS", "completed", "mechanics_only"),
    Scenario(13, "elapsed_task_items", "EXPECTED_IDS", "completed", "mechanics_only"),
    Scenario(14, "requisite_links", "EXPECTED_KEYS", "completed", "identity_and_count_matched"),
    Scenario(15, "disk_mirror", "EXPECTED_OBJECT_IDS", "completed", "identity_and_count_matched"),
    Scenario(16, "binary_download", "EXPECTED_PDF", "not_applicable", "not_applicable"),
    Scenario(17, "numerator_list", "EXPECTED_IDS", "completed", "mechanics_only"),
    Scenario(18, "content_viewers", "EXPECTED_EXTRANET_IDS", "completed", "mechanics_only"),
    Scenario(19, "v3_task_results", "EXPECTED_RESULT_IDS", "not_applicable", "not_applicable"),
)


def _client_sha() -> str:
    git = shutil.which("git")
    if git is None:
        raise RuntimeError("git is required to bind recipe evidence to a candidate")
    result = subprocess.run(  # noqa: S603 - resolved executable and fixed arguments
        [git, "rev-parse", "HEAD"],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )
    status = subprocess.run(  # noqa: S603 - resolved executable and fixed arguments
        [git, "status", "--porcelain", "--untracked-files=all"],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )
    if status.stdout:
        raise RuntimeError("recipe evidence requires a clean candidate checkout")
    return result.stdout.strip()


def _oracle_count(value: object) -> int:
    if isinstance(value, dict):
        return sum(_oracle_count(item) for item in value.values())
    if isinstance(value, bytes | str):
        return len(value)
    if isinstance(value, tuple | list | set | frozenset):
        return len(value)
    raise TypeError("scenario oracle must be a bounded collection")


def _logical_requests(calls: Sequence[Request]) -> int:
    total = 0
    for request in calls:
        if request.positional is not None:
            total += 1
            continue
        parameters = request.copy_parameters()
        commands = parameters.get("cmd") if request.method == "batch" else None
        total += len(commands) if isinstance(commands, dict) else 1
    return total


async def _offline(scenario: Scenario) -> dict[str, object]:
    transports: list[ScriptedTransport] = []
    original_init = ScriptedTransport.__init__

    def observe_init(
        self: ScriptedTransport,
        exchanges: tuple[ScriptedExchange, ...],
        *,
        host: str = "fixture.invalid",
    ) -> None:
        original_init(self, exchanges, host=host)
        transports.append(self)

    ScriptedTransport.__init__ = observe_init  # type: ignore[method-assign]
    try:
        module = importlib.import_module(f"examples.{scenario.module}")
        evidence = await module.run()
    finally:
        ScriptedTransport.__init__ = original_init  # type: ignore[method-assign]
    expected = _oracle_count(getattr(module, scenario.oracle_name))
    if not isinstance(evidence, RecipeEvidence):
        raise TypeError(f"scenario {scenario.number} did not return measured RecipeEvidence")
    if evidence.observed_count != expected:
        raise AssertionError(
            f"scenario {scenario.number} observed {evidence.observed_count} rows, expected {expected}",
        )
    if evidence.report_state != scenario.report_state:
        raise AssertionError(
            f"scenario {scenario.number} report state {evidence.report_state!r} differs from {scenario.report_state!r}",
        )
    if evidence.assurance != scenario.assurance:
        raise AssertionError(
            f"scenario {scenario.number} assurance {evidence.assurance!r} differs from {scenario.assurance!r}",
        )
    calls = [request for transport in transports for request in transport.calls]
    return {
        "scenario": scenario.number,
        "client_sha": _client_sha(),
        "method_card_sha": METHOD_CARD_SHA,
        "report_state": evidence.report_state,
        "assurance": evidence.assurance,
        "expected_count": expected,
        "observed_count": evidence.observed_count,
        "physical_requests": len(calls),
        "logical_requests": _logical_requests(calls),
        "buffered_commands_high_water": evidence.high_water("buffered_commands_high_water"),
        "buffered_rows_high_water": evidence.high_water("buffered_rows_high_water"),
        "active_references_high_water": evidence.high_water("active_references_high_water"),
        "provenance": "fixture",
        "status": "passed",
    }


def _live_attestation_payload(record: dict[str, object]) -> bytes:
    unsigned = {key: value for key, value in record.items() if key != "attestation"}
    return json.dumps(unsigned, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _validate_captured_requests(record: dict[str, object], requests: object, scenario: Scenario) -> None:
    if not isinstance(requests, list) or len(requests) != record.get("physical_requests"):
        raise ValueError(f"LIVE evidence for scenario {scenario.number} has inconsistent request capture")
    logical_requests = 0
    required = {"method", "http_status", "logical_requests", "response_sha256"}
    for request in requests:
        if not isinstance(request, dict) or set(request) != required:
            raise ValueError(f"LIVE evidence for scenario {scenario.number} has invalid request capture")
        method = request["method"]
        status = request["http_status"]
        logical = request["logical_requests"]
        response_sha256 = request["response_sha256"]
        if (
            not isinstance(method, str)
            or not method
            or type(status) is not int
            or not MINIMUM_HTTP_STATUS <= status <= MAXIMUM_HTTP_STATUS
            or type(logical) is not int
            or logical < 1
            or not isinstance(response_sha256, str)
            or _SHA256_RE.fullmatch(response_sha256) is None
        ):
            raise ValueError(f"LIVE evidence for scenario {scenario.number} has invalid request capture")
        logical_requests += logical
    if logical_requests != record.get("logical_requests"):
        raise ValueError(f"LIVE evidence for scenario {scenario.number} has inconsistent logical request capture")


def _validate_live_capture(record: dict[str, object], scenario: Scenario) -> None:
    key = os.environ.get(LIVE_ATTESTATION_KEY_ENV)
    if key is None or len(key.encode()) < MINIMUM_ATTESTATION_KEY_BYTES:
        raise ValueError(f"LIVE evidence requires a 32-byte {LIVE_ATTESTATION_KEY_ENV}")
    attestation = record.get("attestation")
    expected = hmac.new(key.encode(), _live_attestation_payload(record), hashlib.sha256).hexdigest()
    if not isinstance(attestation, str) or not hmac.compare_digest(attestation, f"hmac-sha256:{expected}"):
        raise ValueError(f"LIVE evidence for scenario {scenario.number} has no valid capture attestation")
    capture = record.get("capture")
    required = {"kind", "captured_at", "portal_fingerprint", "fixture_id", "requests"}
    if not isinstance(capture, dict) or set(capture) != required:
        raise ValueError(f"LIVE evidence for scenario {scenario.number} has no verifiable capture detail")
    if capture["kind"] != "b24api-live-capture-v1" or capture["fixture_id"] != record.get("fixture_id"):
        raise ValueError(f"LIVE evidence for scenario {scenario.number} has inconsistent capture provenance")
    captured_at = capture["captured_at"]
    try:
        timestamp = datetime.fromisoformat(captured_at) if isinstance(captured_at, str) else None
    except ValueError as error:
        raise ValueError(f"LIVE evidence for scenario {scenario.number} has an invalid capture timestamp") from error
    if timestamp is None or timestamp.tzinfo is None:
        raise ValueError(f"LIVE evidence for scenario {scenario.number} has an invalid capture timestamp")
    fingerprint = capture["portal_fingerprint"]
    if not isinstance(fingerprint, str) or _SHA256_RE.fullmatch(fingerprint) is None:
        raise ValueError(f"LIVE evidence for scenario {scenario.number} has an invalid portal fingerprint")
    _validate_captured_requests(record, capture["requests"], scenario)


def _validate_live_record(record: object, scenario: Scenario, current_sha: str) -> dict[str, object]:
    required = {
        "scenario",
        "client_sha",
        "method_card_sha",
        "report_state",
        "assurance",
        "expected_count",
        "observed_count",
        "physical_requests",
        "logical_requests",
        "buffered_commands_high_water",
        "buffered_rows_high_water",
        "active_references_high_water",
        "provenance",
        "fixture_id",
        "capture",
        "attestation",
        "status",
    }
    if not isinstance(record, dict) or required - record.keys():
        raise ValueError(f"LIVE evidence for scenario {scenario.number} is missing required fields")
    if type(record["scenario"]) is not int or record["scenario"] != scenario.number:
        raise ValueError(f"LIVE evidence for scenario {scenario.number} has the wrong scenario identity")
    if (
        record["client_sha"] != current_sha
        or record["method_card_sha"] != METHOD_CARD_SHA
        or record["provenance"] != "live"
        or record["status"] != "passed"
    ):
        raise ValueError(f"LIVE evidence for scenario {scenario.number} is not bound to this passing candidate")
    module = importlib.import_module(f"examples.{scenario.module}")
    expected = _oracle_count(getattr(module, scenario.oracle_name))
    integers = (
        "expected_count",
        "observed_count",
        "physical_requests",
        "logical_requests",
        "buffered_commands_high_water",
        "buffered_rows_high_water",
        "active_references_high_water",
    )
    if any(
        not isinstance(record[field], int) or isinstance(record[field], bool) or record[field] < 0 for field in integers
    ):
        raise ValueError(f"LIVE evidence for scenario {scenario.number} has invalid counters")
    if record["expected_count"] != expected or record["observed_count"] != expected:
        raise ValueError(f"LIVE evidence for scenario {scenario.number} contradicts its independent oracle")
    if record["report_state"] != scenario.report_state:
        raise ValueError(f"LIVE evidence for scenario {scenario.number} has the wrong report state")
    if record["assurance"] != scenario.assurance:
        raise ValueError(f"LIVE evidence for scenario {scenario.number} has the wrong assurance")
    if record["physical_requests"] < 1 or record["logical_requests"] < 1:
        raise ValueError(f"LIVE evidence for scenario {scenario.number} has no request evidence")
    if not isinstance(record["fixture_id"], str) or not record["fixture_id"].strip():
        raise ValueError(f"LIVE evidence for scenario {scenario.number} has no fixture provenance")
    _validate_live_capture(record, scenario)
    return record


def _live_records(path: Path, selected: tuple[Scenario, ...]) -> list[dict[str, object]]:
    if not path.is_file():
        return [
            {
                "scenario": scenario.number,
                "client_sha": _client_sha(),
                "method_card_sha": METHOD_CARD_SHA,
                "provenance": "live",
                "status": "skipped",
                "reason": "qualified disposable-portal evidence file is absent",
            }
            for scenario in selected
        ]
    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if any(not isinstance(record, dict) for record in records):
        raise ValueError("LIVE evidence rows must be JSON objects")
    scenario_numbers = [record.get("scenario") for record in records]
    if len(set(scenario_numbers)) != len(scenario_numbers):
        raise ValueError("LIVE evidence contains duplicate scenario rows")
    by_scenario = {record.get("scenario"): record for record in records}
    current_sha = _client_sha()
    return [_validate_live_record(by_scenario.get(scenario.number), scenario, current_sha) for scenario in selected]


async def _main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", default="all")
    parser.add_argument("--live-evidence", type=Path)
    args = parser.parse_args()
    selected = (
        SCENARIOS if args.scenario == "all" else tuple(item for item in SCENARIOS if item.number == int(args.scenario))
    )
    if not selected:
        parser.error("unknown scenario")
    if args.live_evidence is not None:
        records = _live_records(args.live_evidence, selected)
    else:
        records = [await _offline(scenario) for scenario in selected]
    sys.stdout.write("".join(f"{json.dumps(record, sort_keys=True, separators=(',', ':'))}\n" for record in records))
    return 3 if any(record["status"] == "skipped" for record in records) else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
