"""Golden gate: public behavior matches the base-commit projection except for declared deltas."""

from __future__ import annotations
import json
import re
from copy import deepcopy
from typing import Any, cast

import pytest

from tests.golden.capture import BASELINE, DELTAS, capture, fixture_names
from tests.golden.projection import ABSENT, flatten

_ROW = re.compile(r"^\|(?P<cells>.+)\|\s*$")
_COLUMNS = ("id", "fixture", "field", "old", "new", "reason", "evidence")
_CONCURRENT_FIXTURES = frozenset(
    {
        "recipe:chat_bounded_mirror",
        "recipe:chat_resume",
        "recipe:search_chat_messages",
        "recipe:task_comments",
        "recipe:timeline_comments",
        "scenario:fan_out_direct",
        "scenario:fan_out_outcomes_batch",
        "scenario:references_counted",
        "scenario:references_sequential",
    }
)


def _stable_projection(fixture: str, projection: dict[str, object]) -> dict[str, object]:
    """Keep observable outcomes while ignoring scheduling details of concurrent fixtures."""
    if fixture not in _CONCURRENT_FIXTURES:
        return projection
    stable = cast("Any", deepcopy(projection))
    for stream in stable["streams"]:
        report = stream["report"]
        if report is not None:
            report.pop("active_references_high_water")
            report.pop("buffered_rows_high_water")
            report.pop("buffered_commands_high_water")
            report.pop("page_trace")
        if fixture.startswith("recipe:"):
            stream["items"].pop("sha256")
    if "evidence" in stable:
        for report in stable["evidence"]["reports"]:
            report.pop("active_references_high_water")
            report.pop("buffered_rows_high_water")
            report.pop("buffered_commands_high_water")
            report.pop("page_trace")
    if fixture == "recipe:chat_resume":
        # An early close can race with a speculative request already in flight.
        stable.pop("requests")
        stable["streams"][0]["report"].pop("physical_requests")
        stable["evidence"]["reports"][0].pop("physical_requests")
    else:
        for requests in stable["requests"]:
            requests.sort(key=lambda request: json.dumps(request, sort_keys=True))
    return cast("dict[str, object]", stable)


def _declared() -> dict[tuple[str, str], tuple[object, object]]:
    """Parse ``DELTAS.md``: each row declares one fixture field with its exact old and new value."""
    declared: dict[tuple[str, str], tuple[object, object]] = {}
    for line in DELTAS.read_text(encoding="utf-8").splitlines():
        match = _ROW.match(line)
        if match is None:
            continue
        cells = [cell.strip().strip("`") for cell in match["cells"].split("|")]
        if len(cells) != len(_COLUMNS) or cells[0] in {"ID", ""} or set(cells[0]) <= {"-", ":"}:
            continue
        row = dict(zip(_COLUMNS, cells, strict=True))
        assert row["reason"], f"delta {row['id']} {row['fixture']} {row['field']} needs a reason"
        assert row["evidence"], f"delta {row['id']} {row['fixture']} {row['field']} needs a test or live evidence"
        key = (row["fixture"], row["field"])
        assert key not in declared, f"duplicate delta row for {key}"
        declared[key] = (_value(row["old"]), _value(row["new"]))
    return declared


def _value(text: str) -> object:
    return ABSENT if text == ABSENT else json.loads(text)


_BASELINE = json.loads(BASELINE.read_text(encoding="utf-8"))
_DECLARED = _declared()


def test_every_fixture_has_a_baseline_and_every_delta_names_a_fixture() -> None:
    assert sorted(_BASELINE) == sorted(fixture_names())
    assert {fixture for fixture, _field in _DECLARED} <= set(_BASELINE)


@pytest.mark.asyncio
@pytest.mark.parametrize("fixture", fixture_names())
async def test_public_projection_matches_baseline_or_declared_delta(fixture: str) -> None:
    baseline = dict(flatten(_stable_projection(fixture, _BASELINE[fixture])))
    current = dict(flatten(_stable_projection(fixture, await capture(fixture))))
    declared = {field: values for (name, field), values in _DECLARED.items() if name == fixture}

    undeclared = []
    for field in sorted(set(baseline) | set(current)):
        old = baseline.get(field, ABSENT)
        new = current.get(field, ABSENT)
        if field in declared:
            observed = (old, new)
            assert declared[field] == observed, f"{fixture} {field}: declared {declared[field]}, observed {observed}"
        elif old != new:
            undeclared.append(f"{field}: {old!r} -> {new!r}")
    assert not undeclared, f"undeclared golden delta in {fixture}; add rows to tests/golden/DELTAS.md:\n" + "\n".join(
        undeclared
    )
