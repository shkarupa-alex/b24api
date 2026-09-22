"""Run the 19 offline recipes or verify explicitly supplied LIVE evidence.

Offline: ``uv run python -m examples.run --scenario all``.
LIVE evidence: ``uv run python -m examples.run --scenario all --live-evidence evidence.jsonl``.
The latter never contacts a portal implicitly and never promotes a missing fixture to a pass.
"""

from __future__ import annotations
import argparse
import asyncio
import importlib
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from b24api.testing import ScriptedExchange, ScriptedTransport

if TYPE_CHECKING:
    from collections.abc import Sequence

    from b24api import Request

METHOD_CARD_SHA = "909c6bf14b29961a365dc6d6d4c2c47b5e2533d4"


@dataclass(frozen=True, slots=True)
class Scenario:
    """One runnable recipe and the oracle summarized after its assertions pass."""

    number: int
    module: str
    oracle_name: str
    report_state: str


SCENARIOS = (
    Scenario(1, "chat_bounded_mirror", "EXPECTED_IDS", "completed"),
    Scenario(2, "message_cursor_direction", "EXPECTED_IDS", "completed"),
    Scenario(3, "recent_dialogs", "EXPECTED_IDS", "completed"),
    Scenario(4, "search_chat_messages", "EXPECTED_IDS", "completed_with_failures"),
    Scenario(5, "task_role_union", "EXPECTED_IDS", "completed"),
    Scenario(6, "task_comments", "EXPECTED_MODERN", "completed_with_failures"),
    Scenario(7, "chat_resume", "EXPECTED", "completed"),
    Scenario(8, "crm_item_delta", "EXPECTED_IDS", "completed"),
    Scenario(9, "calendar_delta", "LIVE_ROWS", "completed"),
    Scenario(10, "timeline_comments", "EXPECTED_STORAGE_IDS", "completed_with_failures"),
    Scenario(11, "sparse_user_search", "EXPECTED_IDS", "completed"),
    Scenario(12, "page_index_members", "EXPECTED_IDS", "completed"),
    Scenario(13, "elapsed_task_items", "EXPECTED_IDS", "completed"),
    Scenario(14, "requisite_links", "EXPECTED_KEYS", "completed"),
    Scenario(15, "disk_mirror", "EXPECTED_OBJECT_IDS", "completed"),
    Scenario(16, "binary_download", "EXPECTED_PDF", "completed"),
    Scenario(17, "numerator_list", "EXPECTED_IDS", "completed"),
    Scenario(18, "content_viewers", "EXPECTED_EXTRANET_IDS", "completed"),
    Scenario(19, "v3_task_results", "EXPECTED_RESULT_IDS", "completed"),
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
        await module.run()
    finally:
        ScriptedTransport.__init__ = original_init  # type: ignore[method-assign]
    expected = _oracle_count(getattr(module, scenario.oracle_name))
    calls = [request for transport in transports for request in transport.calls]
    return {
        "scenario": scenario.number,
        "client_sha": _client_sha(),
        "method_card_sha": METHOD_CARD_SHA,
        "report_state": scenario.report_state,
        "expected_count": expected,
        "observed_count": expected,
        "physical_requests": len(calls),
        "logical_requests": _logical_requests(calls),
        "provenance": "fixture",
        "status": "passed",
    }


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
    current_sha = _client_sha()
    by_scenario = {record.get("scenario"): record for record in records}
    for scenario in selected:
        record = by_scenario.get(scenario.number)
        required = {
            "scenario",
            "client_sha",
            "method_card_sha",
            "report_state",
            "expected_count",
            "observed_count",
            "physical_requests",
            "logical_requests",
            "provenance",
            "status",
        }
        if not isinstance(record, dict) or required - record.keys():
            raise ValueError(f"LIVE evidence for scenario {scenario.number} is missing required fields")
        if record["client_sha"] != current_sha or record["provenance"] != "live" or record["status"] != "passed":
            raise ValueError(f"LIVE evidence for scenario {scenario.number} is not bound to this passing candidate")
    return records


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
