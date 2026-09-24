"""Scenario 5: four task roles with a fixed 50-row stride and exact total.

The responsible source has 932 rows. The server rounds start=932 down to
start=900 and repeats the last 32; the public iterator closes on the qualified
total after the page at 900 and never derives the unsafe raw offset 932.
The role union retains all 932 task identities and records overlapping roles.
Run: `uv run python -m examples.task_role_union`.
"""

from __future__ import annotations
import asyncio

from b24api import (
    Bitrix24,
    OffsetContinuation,
    OffsetSpec,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    Settings,
    TotalTermination,
)
from b24api.contracts import PageStride
from b24api.testing import ScriptedExchange, ScriptedTransport
from examples._support.evidence import RecipeEvidence

METHOD = "tasks.task.list"
CHANGED_SINCE = "2026-09-01T00:00:00+03:00"
PAGE_SIZE = 50
ROLE_IDS = {
    "RESPONSIBLE_ID": tuple(range(1, 933)),
    "CREATED_BY": tuple(range(500, 511)),
    "ACCOMPLICE": tuple(range(901, 921)),
    "AUDITOR": tuple(range(930, 933)),
}
EXPECTED_IDS = tuple(range(1, 933))


def _task_id(row: object) -> int:
    if not isinstance(row, dict):
        raise TypeError("task row must be an object")
    value = row.get("id")
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise TypeError("task identity must be a string or integer")
    return int(value)


def _tasks(response: object) -> list[object]:
    if not isinstance(response, dict):
        raise TypeError("task list response must contain tasks array")
    rows = response.get("tasks")
    if not isinstance(rows, list):
        raise TypeError("task list response must contain tasks array")
    return rows


def _request(role: str, offset: int) -> Request:
    return Request(
        METHOD,
        {
            "filter": {role: 7, ">=CHANGED_DATE": CHANGED_SINCE},
            "order": {"ID": "ASC"},
            "select": ["ID", "CHANGED_DATE"],
            "start": offset,
        },
        replay_safety=ReplaySafety.SAFE,
        route=RouteKind.BARE,
    )


def _fixture() -> ScriptedTransport:
    exchanges: list[ScriptedExchange] = []
    for role, ids in ROLE_IDS.items():
        for offset in range(0, len(ids), PAGE_SIZE):
            page = ids[offset : offset + PAGE_SIZE]
            exchanges.append(
                ScriptedExchange.json(
                    _request(role, offset),
                    {
                        "result": {"tasks": [{"id": str(task_id)} for task_id in page]},
                        "total": len(ids),
                    },
                )
            )
    # A separate negative control proves the endpoint's rounded offset hazard.
    exchanges.append(
        ScriptedExchange.json(
            _request("RESPONSIBLE_ID", 932),
            {"result": {"tasks": [{"id": str(task_id)} for task_id in ROLE_IDS["RESPONSIBLE_ID"][900:]]}},
        )
    )
    return ScriptedTransport(tuple(exchanges))


def _assert_responsible_offsets(transport: ScriptedTransport) -> None:
    responsible_offsets = []
    for request in transport.calls:
        parameters = request.copy_parameters()
        filters = parameters["filter"]
        if isinstance(filters, dict) and "RESPONSIBLE_ID" in filters:
            responsible_offsets.append(parameters["start"])
    if tuple(responsible_offsets) != (*range(0, 901, PAGE_SIZE), 932):
        raise AssertionError("scenario 5 public offset did not stop at its qualified total")


async def run() -> RecipeEvidence:
    """Compare a naive 932 offset with four completed public role traversals."""
    transport = _fixture()
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    union: dict[int, set[str]] = {}
    reports = []
    async with Bitrix24(settings, transport=transport) as client:
        for role, expected_role_ids in ROLE_IDS.items():
            stream = client.iter_list(
                _request(role, 0),
                selector=ResultSelector(("tasks",)),
                page_size=PAGE_SIZE,
                offset=OffsetSpec(
                    continuation=OffsetContinuation.FIXED_STEP,
                    step=PAGE_SIZE,
                    page_stride=PageStride(PAGE_SIZE, PAGE_SIZE, PAGE_SIZE),
                    total_termination=TotalTermination.EXACT_QUALIFIED,
                ),
            )
            observed = tuple([_task_id(row) async for row in stream])
            if observed != expected_role_ids:
                raise AssertionError(f"scenario 5 {role} rows differed from role oracle")
            if stream.report is None or not stream.report.exhausted:
                raise AssertionError(f"scenario 5 {role} lacked empty-page closure")
            reports.append(stream.report)
            for task_id in observed:
                union.setdefault(task_id, set()).add(role)
        repeated = await client.call(_request("RESPONSIBLE_ID", 932))
        repeated_ids = tuple(_task_id(row) for row in _tasks(repeated))
        if repeated_ids != ROLE_IDS["RESPONSIBLE_ID"][900:]:
            raise AssertionError("scenario 5 rounded 932 control did not repeat page 900")
    transport.assert_exhausted()
    if tuple(sorted(union)) != EXPECTED_IDS:
        raise AssertionError("scenario 5 task union omitted an independent identity")
    if union[930] != {"RESPONSIBLE_ID", "AUDITOR"}:
        raise AssertionError("scenario 5 lost overlapping task roles")
    _assert_responsible_offsets(transport)
    return RecipeEvidence(len(union), reports[-1], tuple(reports))


if __name__ == "__main__":
    asyncio.run(run())
