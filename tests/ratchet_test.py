"""Quality ratchets: measured structural debt may only go down.

``tests/ratchets.json`` holds the measured baseline. A change that lowers a count must lower the
baseline in the same commit, so the next change cannot silently spend the improvement. Long
functions are tracked by qualified name: a new function over the limit, or growth of a listed one,
fails; a listed function either shrinks below the limit or keeps a recorded justification.
"""

from __future__ import annotations
import ast
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "b24api"
BASELINE = json.loads((ROOT / "tests" / "ratchets.json").read_text(encoding="utf-8"))
FUNCTION_LINE_LIMIT = 100


def _private_member_accesses(*paths: str, exclude: str | None = None) -> int:
    """Count real SLF001 findings, including the ones an inline or file-level noqa hides."""
    command = [sys.executable, "-m", "ruff", "check", "--select", "SLF001", "--ignore-noqa"]
    command += ["--no-fix", "--output-format", "json", "--exit-zero", *paths]
    if exclude is not None:
        command += ["--extend-exclude", exclude]
    result = subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True)  # noqa: S603
    return len(json.loads(result.stdout))


def _long_functions() -> dict[str, int]:
    found: dict[str, int] = {}

    def visit(node: ast.AST, path: str, prefix: str) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
                name = f"{prefix}{child.name}"
                length = (child.end_lineno or child.lineno) - child.lineno + 1
                if length > FUNCTION_LINE_LIMIT:
                    found[f"{path}::{name}"] = length
                visit(child, path, f"{name}.")
            elif isinstance(child, ast.ClassDef):
                visit(child, path, f"{prefix}{child.name}.")

    for source in sorted(PACKAGE.rglob("*.py")):
        visit(ast.parse(source.read_text(encoding="utf-8")), source.relative_to(ROOT).as_posix(), "")
    return found


def _assert_ratchet(name: str, measured: int) -> None:
    baseline = BASELINE[name]
    assert measured <= baseline, f"{name} grew from {baseline} to {measured}"
    assert measured == baseline, f"{name} improved to {measured}; lower tests/ratchets.json to lock it in"


def test_private_member_access_only_goes_down() -> None:
    # Tests of internal components belong in tests/internal/, whose files justify the access.
    _assert_ratchet("slf001_tests", _private_member_accesses("tests", exclude="tests/internal"))
    _assert_ratchet("slf001_package", _private_member_accesses("b24api"))
    _assert_ratchet("slf001_traversal", _private_member_accesses("b24api/traversal"))


def test_package_noqa_and_untyped_self_only_go_down() -> None:
    sources = [path.read_text(encoding="utf-8") for path in sorted(PACKAGE.rglob("*.py"))]
    _assert_ratchet("noqa_package", sum(source.count("noqa") for source in sources))
    # Strategies are typed against StrategyContext (§3.6), so an untyped self is never needed again.
    untyped = sorted(
        str(path.relative_to(PACKAGE))
        for path in (PACKAGE / "traversal").rglob("*.py")
        if "self: Any" in path.read_text()
    )
    assert not untyped, f"type strategy self as StrategyContext in {untyped}"


def test_long_functions_only_shrink() -> None:
    baseline: dict[str, dict[str, object]] = BASELINE["long_functions"]
    measured = _long_functions()
    for name, length in measured.items():
        assert name in baseline, f"{name} has {length} lines; the limit is {FUNCTION_LINE_LIMIT}"
        recorded = baseline[name]["lines"]
        assert isinstance(recorded, int)
        assert length <= recorded, f"{name} grew from {recorded} to {length} lines"
        assert length == recorded, f"{name} shrank to {length} lines; update tests/ratchets.json"
    stale = sorted(set(baseline) - set(measured))
    assert not stale, f"no longer over the limit, remove from tests/ratchets.json: {stale}"
