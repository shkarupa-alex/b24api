"""Package import graph: runtime layers form a DAG, and type-only cycles only shrink.

The runtime graph holds every import executed when its code runs, top-level and function-local
alike, and leaves out ``if TYPE_CHECKING:`` blocks. The full graph adds those blocks back. Each
failing assertion prints the offending edges or cycles.
"""

from __future__ import annotations
import ast
from functools import cache
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "b24api"

# A layer may import only itself and layers to its left.
LAYERS = (
    "leaf",
    "redaction",
    "contracts",
    "errors",
    "transport",
    "execution",
    "completion",
    "batch",
    "traversal",
    "references",
    "testing",
    "client",
    "cli",
)
# Files directly under ``b24api/``; files inside a package inherit the package's layer.
_ROOT_FILE_LAYERS = {
    "_error_types.py": "leaf",
    "encoding.py": "leaf",
    "migration.py": "leaf",
    "settings.py": "leaf",
    "redaction.py": "redaction",
    "_diagnostics.py": "redaction",
    "errors.py": "errors",
    "_audit.py": "execution",
    "_client_traversal.py": "client",
    "client.py": "client",
    "cli.py": "cli",
    "cli_contract.py": "cli",
}
# The root ``b24api/__init__.py`` is the public aggregator, outside every layer.
_AGGREGATOR = "b24api"

# Type-only cycles that remain, each with the reason it cannot go yet. A new cycle fails the gate;
# a removed one must be deleted here, so the baseline only shrinks.
_TYPE_ONLY_CYCLE_BASELINE = {
    # The redactor renders through an optional DiagnosticContext that is built from redaction rules.
    ("_diagnostics", "redaction"),
    # The error base annotates RequestSummary; request needs policy enums; policy raises the budget error.
    ("contracts.error_base", "contracts.request", "contracts.policy"),
    # Policy annotates RequestSummary in ambiguity decisions; request coerces identities by policy enums.
    ("contracts.policy", "contracts.request"),
    # The driver and its control preflight share plan types; removed by the StrategyContext split (§3.6).
    ("traversal.control_preflight", "traversal.driver"),
    # Keyset planning and runtime modules see each other's types; removed by keyset consolidation (C5).
    ("traversal.keyset_auto", "traversal.keyset_costs", "traversal.keyset_range", "traversal.keyset_fast_plan"),
    ("traversal.keyset_fast_stream", "traversal.keyset_observation", "traversal.keyset_scheduler"),
    ("traversal.keyset_fast_stream", "traversal.keyset_scheduler"),
    ("traversal.keyset_observation", "traversal.keyset_scheduler"),
    ("traversal.keyset_observation", "traversal.keyset_scheduler", "traversal.keyset_transactions"),
    ("traversal.keyset_reporting", "traversal.keyset_scheduler"),
    ("traversal.keyset_scheduler", "traversal.keyset_transaction_contract"),
    ("traversal.keyset_scheduler", "traversal.keyset_transactions"),
    ("traversal.keyset_scheduler", "traversal.keyset_transactions", "traversal.keyset_transaction_contract"),
}
# Only the client composes reference traversal; no lower layer reaches into it.
_REFERENCES_IMPORTERS = {"b24api.client"}

type Graph = dict[str, frozenset[str]]


def _module_name(path: Path) -> str:
    parts = list(path.relative_to(ROOT).with_suffix("").parts)
    return ".".join(parts[:-1] if parts[-1] == "__init__" else parts)


@cache
def _modules() -> dict[str, Path]:
    return {_module_name(path): path for path in sorted(PACKAGE.rglob("*.py"))}


def _layer(module: str) -> str:
    relative = _modules()[module].relative_to(PACKAGE)
    if len(relative.parts) > 1:
        return relative.parts[0]
    return _ROOT_FILE_LAYERS.get(relative.name, "unassigned")


def _targets(tree: ast.AST, module: str, *, type_only: bool) -> frozenset[str]:
    guarded = {
        id(child)
        for node in ast.walk(tree)
        if isinstance(node, ast.If) and isinstance(node.test, ast.Name) and node.test.id == "TYPE_CHECKING"
        for statement in node.body
        for child in ast.walk(statement)
    }
    modules = _modules()
    found: set[str] = set()
    for node in ast.walk(tree):
        if (id(node) in guarded) is not type_only:
            continue
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names if alias.name in modules)
        elif isinstance(node, ast.ImportFrom) and node.module is not None and node.module in modules:
            # ``from package import submodule`` depends on the submodule, not only on the package.
            for alias in node.names:
                submodule = f"{node.module}.{alias.name}"
                found.add(submodule if submodule in modules else node.module)
    return frozenset(found - {module, _AGGREGATOR})


@cache
def _graphs() -> tuple[Graph, Graph]:
    runtime: Graph = {}
    full: Graph = {}
    for module, path in _modules().items():
        if module == _AGGREGATOR:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        runtime[module] = _targets(tree, module, type_only=False)
        full[module] = runtime[module] | _targets(tree, module, type_only=True)
    return runtime, full


def _reachable(graph: Graph, start: str) -> set[str]:
    seen: set[str] = set()
    pending = [start]
    while pending:
        for successor in graph[pending.pop()] - seen:
            seen.add(successor)
            pending.append(successor)
    return seen


def _components(graph: Graph) -> set[frozenset[str]]:
    """Return the strongly connected components with more than one module (mutual reachability)."""
    reach = {module: _reachable(graph, module) for module in graph}
    return {
        frozenset({module} | {other for other in reach[module] if module in reach[other]})
        for module in graph
        if module in reach[module]
    }


def _elementary_cycles(graph: Graph) -> set[tuple[str, ...]]:
    """Enumerate simple cycles, each rotated to start at its smallest module name."""
    cycles: set[tuple[str, ...]] = set()
    for component in _components(graph):
        for start in sorted(component):
            pending = [(start, (start,))]
            while pending:
                node, path = pending.pop()
                for successor in sorted(graph[node] & component):
                    if successor == start:
                        cycles.add(path)
                    elif successor > start and successor not in path:
                        pending.append((successor, (*path, successor)))
    return cycles


def _short(cycle: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(module.removeprefix("b24api.") for module in cycle)


def test_every_package_module_has_a_layer() -> None:
    unassigned = sorted(module for module in _graphs()[0] if _layer(module) not in LAYERS)
    assert not unassigned, f"assign a layer to {unassigned}"


def test_package_import_graph_respects_layers() -> None:
    runtime, _full = _graphs()
    upward = sorted(
        f"{module} [{_layer(module)}] -> {target} [{_layer(target)}]"
        for module, targets in runtime.items()
        for target in targets
        if LAYERS.index(_layer(target)) > LAYERS.index(_layer(module))
    )
    assert not upward, "runtime imports point to a later layer:\n" + "\n".join(upward)
    cycles = [sorted(component) for component in _components(runtime)]
    assert not cycles, f"runtime import cycles: {cycles}"


def test_type_checking_cycles_do_not_grow() -> None:
    _runtime, full = _graphs()
    cycles = {_short(cycle) for cycle in _elementary_cycles(full)}
    assert not cycles - _TYPE_ONLY_CYCLE_BASELINE, f"new import cycles: {sorted(cycles - _TYPE_ONLY_CYCLE_BASELINE)}"
    assert not _TYPE_ONLY_CYCLE_BASELINE - cycles, (
        f"remove resolved cycles from the baseline: {sorted(_TYPE_ONLY_CYCLE_BASELINE - cycles)}"
    )


def test_no_cycle_passes_through_errors() -> None:
    _runtime, full = _graphs()
    through = sorted(_short(cycle) for cycle in _elementary_cycles(full) if "b24api.errors" in cycle)
    assert not through, f"cycles through errors.py: {through}"


def test_only_the_client_imports_reference_traversal() -> None:
    _runtime, full = _graphs()
    importers = {
        module
        for module, targets in full.items()
        if not module.startswith("b24api.references")
        and any(target == "b24api.references" or target.startswith("b24api.references.") for target in targets)
    }
    assert importers == _REFERENCES_IMPORTERS


def test_graph_counts_function_local_imports_and_separates_type_only_ones() -> None:
    tree = ast.parse(
        "from typing import TYPE_CHECKING\n"
        "from b24api.contracts import report\n"
        "if TYPE_CHECKING:\n"
        "    from b24api.errors import B24ApiError\n"
        "def build():\n"
        "    from b24api.batch.stream import batch_outcome_stream\n",
    )
    assert _targets(tree, "sample", type_only=False) == {"b24api.contracts.report", "b24api.batch.stream"}
    assert _targets(tree, "sample", type_only=True) == {"b24api.errors"}
    assert _elementary_cycles({"a": frozenset({"b"}), "b": frozenset({"a", "c"}), "c": frozenset({"b"})}) == {
        ("a", "b"),
        ("b", "c"),
    }
