# ruff: noqa: INP001 - a standalone workflow script, not an importable package module
"""Ratchet the mypy error count over ``tests/`` against a committed baseline (B17).

The count may only go down. A run with more errors than the baseline fails; a run with fewer
errors also fails until the baseline is lowered (``--update`` rewrites it, and refuses to raise it),
so every improvement is recorded and cannot be silently lost later. A mypy run that ends without its summary line
(a crash or a usage error) fails instead of counting as zero.
"""

from __future__ import annotations
import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BASELINE = ROOT / "tests" / "mypy_baseline.txt"
TARGETS = ("tests",)
_FOUND = re.compile(r"^Found (\d+) errors? in \d+ files? \(checked \d+ source files?\)$", re.MULTILINE)
_SUCCESS = re.compile(r"^Success: no issues found in \d+ source files?$", re.MULTILINE)
_HEADER = (
    "# mypy error count over tests/ (B17 ratchet). It may only decrease: fix errors, then lower it with\n"
    "#   uv run python .github/scripts/mypy_ratchet.py --update\n"
)


class RatchetError(Exception):
    """The mypy output or the baseline cannot be compared."""


def error_count(output: str) -> int:
    """Return the error count from mypy's closing summary line."""
    if _SUCCESS.search(output):
        return 0
    found = _FOUND.findall(output)
    if not found:
        raise RatchetError("mypy output has no summary line; it crashed or was misconfigured")
    return int(found[-1])


def read_baseline(path: Path) -> int:
    """Return the single integer recorded in the baseline file, ignoring ``#`` comments."""
    try:
        lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    except OSError as error:
        raise RatchetError(f"cannot read the baseline {path}") from error
    values = [line for line in lines if line and not line.startswith("#")]
    if len(values) != 1 or not values[0].isdigit():
        raise RatchetError(f"{path} must hold exactly one non-negative integer")
    return int(values[0])


def write_baseline(path: Path, count: int) -> None:
    """Record ``count`` as the new baseline."""
    path.write_text(f"{_HEADER}{count}\n", encoding="utf-8")


def lower_baseline(path: Path, count: int) -> None:
    """Record ``count`` unless it would raise an existing baseline."""
    if path.exists() and count > read_baseline(path):
        raise RatchetError(f"--update only lowers the baseline; {count} errors exceed it, fix them first")
    write_baseline(path, count)


def compare(count: int, baseline: int) -> str | None:
    """Return why ``count`` violates the ratchet, or ``None`` when it equals the baseline."""
    if count > baseline:
        return f"mypy over tests/ reports {count} errors, more than the baseline {baseline}; fix the new errors"
    if count < baseline:
        return (
            f"mypy over tests/ reports {count} errors, fewer than the baseline {baseline}; "
            "record the improvement with --update"
        )
    return None


def run_mypy(targets: tuple[str, ...]) -> str:
    """Run mypy with the repository configuration and return its combined output."""
    completed = subprocess.run(  # noqa: S603 - fixed interpreter and module
        [sys.executable, "-m", "mypy", *targets],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    return completed.stdout + completed.stderr


def main(argv: list[str] | None = None) -> int:
    """Compare the current mypy error count with the baseline, or rewrite it with ``--update``."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, default=BASELINE)
    parser.add_argument("--mypy-output", type=Path, help="read a saved mypy output instead of running mypy")
    parser.add_argument("--update", action="store_true", help="rewrite the baseline to the current count")
    arguments = parser.parse_args(argv)
    try:
        output = arguments.mypy_output.read_text(encoding="utf-8") if arguments.mypy_output else run_mypy(TARGETS)
        count = error_count(output)
        if arguments.update:
            lower_baseline(arguments.baseline, count)
            sys.stdout.write(f"baseline set to {count} mypy errors\n")
            return 0
        violation = compare(count, read_baseline(arguments.baseline))
    except (OSError, RatchetError) as error:
        sys.stdout.write(f"::error title=mypy ratchet::{error}\n")
        return 1
    if violation is not None:
        if arguments.mypy_output is None:
            sys.stdout.write(output)
        sys.stdout.write(f"::error title=mypy ratchet::{violation}\n")
        return 1
    sys.stdout.write(f"mypy over tests/ reports {count} errors, equal to the baseline\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
