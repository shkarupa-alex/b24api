# ruff: noqa: INP001 - a standalone workflow script, not an importable package module
"""Aggregate CI gate: succeed only when every blocking job it depends on succeeded.

The ``gate`` job of ``ci.yml`` runs with ``if: always()`` and passes ``toJSON(needs)`` through the
``NEEDS_JSON`` environment variable, never through the script text. Branch protection requires
the single ``ci`` check produced by that job, so any result other than ``success`` -- ``failure``,
``cancelled`` or ``skipped`` -- must fail it. An empty or malformed ``needs`` object also fails:
it means the gate lost its wiring, not that there was nothing to check.
"""

from __future__ import annotations
import json
import os
import sys
from collections.abc import Mapping

ENVIRONMENT_VARIABLE = "NEEDS_JSON"


class GateError(Exception):
    """The dependency results cannot prove that every blocking job succeeded."""


def unsuccessful_jobs(needs: object) -> list[tuple[str, str]]:
    """Return ``(job, result)`` for each dependency whose result is not ``success``."""
    if not isinstance(needs, Mapping) or not needs:
        raise GateError("the gate received no dependency results; wire its needs to the blocking jobs")
    failures: list[tuple[str, str]] = []
    for job, state in sorted(needs.items()):
        result = state.get("result") if isinstance(state, Mapping) else None
        if result != "success":
            failures.append((str(job), str(result)))
    return failures


def main(environ: Mapping[str, str] | None = None) -> int:
    """Check the dependency results and report every unsuccessful job as a GitHub annotation."""
    source = os.environ if environ is None else environ
    try:
        needs = json.loads(source.get(ENVIRONMENT_VARIABLE, ""))
        failures = unsuccessful_jobs(needs)
    except (GateError, json.JSONDecodeError) as error:
        sys.stdout.write(f"::error title=CI gate::{error}\n")
        return 1
    for job, result in failures:
        sys.stdout.write(f"::error title=CI gate::blocking job {job} finished with result {result}\n")
    if failures:
        return 1
    sys.stdout.write(f"all {len(needs)} blocking jobs succeeded: {', '.join(sorted(needs))}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
