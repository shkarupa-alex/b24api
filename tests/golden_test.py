"""Golden gate: public behavior matches the base-commit projection except for declared deltas."""

from __future__ import annotations
import json
import re

import pytest

from tests.golden.capture import BASELINE, DELTAS, capture, fixture_names
from tests.golden.projection import ABSENT, flatten

_ROW = re.compile(r"^\|(?P<cells>.+)\|\s*$")
_COLUMNS = ("id", "fixture", "field", "old", "new", "reason", "evidence")


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
    baseline = dict(flatten(_BASELINE[fixture]))
    current = dict(flatten(await capture(fixture)))
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
