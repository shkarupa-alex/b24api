"""Package import graph: runtime layers form a DAG, and type-only cycles only shrink.

The runtime graph holds every import executed when its code runs, top-level and function-local
alike: absolute and relative ``import`` statements, imports from the root ``b24api`` facade, and
``importlib.import_module`` with a literal name (a dynamic import with any other argument fails the
suite, since the graph cannot follow it). It leaves out ``if TYPE_CHECKING:`` blocks (bare or as
``typing.TYPE_CHECKING``); the full graph adds those blocks back. Importing a submodule is an edge to
that submodule only: the implicit initialization of its parent packages is not an edge, because
a package that re-exports its submodules is not a cycle. Each failing assertion prints the offending
edges or cycles.
"""

from __future__ import annotations
import ast
import importlib.util
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
    "facade",
    "cli",
)
# Files directly under ``b24api/``; files inside a package inherit the package's layer.
_ROOT_FILE_LAYERS = {
    "_error_types.py": "leaf",
    "encoding.py": "leaf",
    "migration.py": "leaf",
    "settings.py": "leaf",
    "_webhook.py": "leaf",
    "redaction.py": "redaction",
    "_diagnostics.py": "redaction",
    "errors.py": "errors",
    "_sources.py": "execution",
    "_client_traversal.py": "client",
    "client.py": "client",
    "cli.py": "cli",
    "cli_contract.py": "cli",
    # The root facade re-exports the public surface: only the CLI, above it, may import from it.
    "__init__.py": "facade",
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


def _is_type_checking(test: ast.expr) -> bool:
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def _dynamic_import(node: ast.AST) -> ast.Call | None:
    """Return a call of ``importlib.import_module``, ``import_module`` or ``__import__``."""
    if not isinstance(node, ast.Call):
        return None
    function = node.func
    name = function.attr if isinstance(function, ast.Attribute) else getattr(function, "id", None)
    return node if name in {"import_module", "__import__"} else None


def _literal_name(call: ast.Call) -> str | None:
    first = call.args[0] if call.args else None
    return first.value if isinstance(first, ast.Constant) and isinstance(first.value, str) else None


def _targets(tree: ast.AST, module: str, *, type_only: bool, package: bool = False) -> frozenset[str]:
    guarded = {
        id(child)
        for node in ast.walk(tree)
        if isinstance(node, ast.If) and _is_type_checking(node.test)
        for statement in node.body
        for child in ast.walk(statement)
    }
    modules = _modules()
    anchor = module if package else module.rpartition(".")[0]
    found: set[str] = set()
    for node in ast.walk(tree):
        if (id(node) in guarded) is not type_only:
            continue
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names if alias.name in modules)
        elif isinstance(node, ast.ImportFrom):
            source = importlib.util.resolve_name("." * node.level + (node.module or ""), anchor)
            if source not in modules:
                continue
            # ``from package import submodule`` depends on the submodule, not only on the package.
            for alias in node.names:
                submodule = f"{source}.{alias.name}"
                found.add(submodule if submodule in modules else source)
        elif (call := _dynamic_import(node)) is not None and _literal_name(call) in modules:
            found.add(str(_literal_name(call)))
    return frozenset(found - {module})


@cache
def _graphs() -> tuple[Graph, Graph]:
    runtime: Graph = {}
    full: Graph = {}
    for module, path in _modules().items():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        package = path.name == "__init__.py"
        runtime[module] = _targets(tree, module, type_only=False, package=package)
        full[module] = runtime[module] | _targets(tree, module, type_only=True, package=package)
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


def test_no_import_cycle_even_through_type_checking_imports() -> None:
    _runtime, full = _graphs()
    cycles = sorted(_short(cycle) for cycle in _elementary_cycles(full))
    assert not cycles, f"import cycles, counting TYPE_CHECKING imports: {cycles}"


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


def _unfollowed_dynamic_imports(tree: ast.AST) -> list[int]:
    """Return the lines of dynamic imports the graph cannot follow: a name that is not an absolute literal."""
    return sorted(
        call.lineno
        for node in ast.walk(tree)
        if (call := _dynamic_import(node)) is not None
        and ((name := _literal_name(call)) is None or name.startswith(".") or len(call.args) > 1)
    )


def test_every_dynamic_import_names_its_module_literally() -> None:
    unfollowed = [
        f"{path.relative_to(ROOT)}:{line}"
        for path in _modules().values()
        for line in _unfollowed_dynamic_imports(ast.parse(path.read_text(encoding="utf-8")))
    ]
    assert not unfollowed, f"the import graph cannot follow these dynamic imports: {unfollowed}"


def test_a_dynamic_import_the_graph_cannot_follow_is_flagged() -> None:
    tree = ast.parse(
        "import importlib\n"
        "name = 'b24api.client'\n"
        "importlib.import_module('b24api.migration')\n"
        "importlib.import_module(name)\n"
        "importlib.import_module('.client', 'b24api')\n"
        "__import__(name)\n"
    )

    assert _unfollowed_dynamic_imports(tree) == [4, 5, 6]


def test_graph_resolves_relative_facade_dynamic_and_qualified_type_checking_imports() -> None:
    tree = ast.parse(
        "import importlib\n"
        "import typing\n"
        "from b24api import Bitrix24\n"
        "from . import stream\n"
        "from .engine import run\n"
        "from ..contracts import report\n"
        "if typing.TYPE_CHECKING:\n"
        "    from ..errors import B24ApiError\n"
        "def load():\n"
        "    return importlib.import_module('b24api.migration')\n",
    )
    module = "b24api.batch.logical"
    assert _targets(tree, module, type_only=False) == {
        "b24api",
        "b24api.batch.stream",
        "b24api.batch.engine",
        "b24api.contracts.report",
        "b24api.migration",
    }
    assert _targets(tree, module, type_only=True) == {"b24api.errors"}
    # In a package's own ``__init__``, one leading dot names the package itself.
    assert _targets(ast.parse("from .engine import run\n"), "b24api.batch", type_only=False, package=True) == {
        "b24api.batch.engine"
    }


def test_a_lower_layer_importing_from_the_root_facade_is_an_upward_edge() -> None:
    assert _layer("b24api") == "facade"
    assert LAYERS.index("facade") > LAYERS.index("client")
    assert LAYERS.index("facade") < LAYERS.index("cli")
    assert "b24api" in _graphs()[0]["b24api.cli"]
