"""Executable package-boundary and legacy-removal gates for the v2 architecture."""

from __future__ import annotations
import ast
import inspect
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

import b24api.client
import b24api.completion
import b24api.contracts
import b24api.errors
import b24api.testing
import b24api.transport

if TYPE_CHECKING:
    from collections.abc import Callable

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "b24api"

_FORBIDDEN_ROOT_MODULES = {
    "api.py",
    "helper.py",
    "models.py",
    "plans.py",
    "profiles.py",
    "protocol.py",
    "query.py",
    "type.py",
}
_REMOVED_CLIENT_METHODS = {
    "list_batched",
    "list_batched_no_count",
    "list_keyset",
    "list_sequential",
    "reference_batch",
    "reference_list",
}


def _sources() -> tuple[Path, ...]:
    return tuple(sorted(PACKAGE.rglob("*.py")))


def _imports(path: Path) -> set[str]:
    result: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            result.add(node.module)
    return result


def _runtime_imports(path: Path) -> set[str]:
    """Imports executed at module load, leaving out ``if TYPE_CHECKING:`` blocks."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    type_only = {
        id(child)
        for node in ast.walk(tree)
        if isinstance(node, ast.If) and isinstance(node.test, ast.Name) and node.test.id == "TYPE_CHECKING"
        for statement in node.body
        for child in ast.walk(statement)
    }
    result: set[str] = set()
    for node in ast.walk(tree):
        if id(node) in type_only:
            continue
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            result.add(node.module)
    return result


class _ResponseItemsVisitor(ast.NodeVisitor):
    def __init__(self, relative: str, call_sites: set[tuple[str, str]]) -> None:
        self.relative = relative
        self.call_sites = call_sites
        self.stack: list[str] = []

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Name) and node.func.id == "_response_items":
            self.call_sites.add((self.relative, self.stack[-1]))
        self.generic_visit(node)


def test_runtime_layers_stay_evidence_free_and_io_free() -> None:
    for path in _sources():
        imports = _imports(path)
        assert not any(name == "tools" or name.startswith("tools.") or "b24api_evidence" in name for name in imports)
        relative = path.relative_to(PACKAGE)
        if relative.parts[0] == "contracts":
            assert not any(
                name == forbidden or name.startswith(f"{forbidden}.")
                for name in imports
                for forbidden in (
                    "asyncio",
                    "httpx",
                    "b24api.execution",
                    "b24api.transport",
                    "b24api.batch",
                    "b24api.traversal",
                    "b24api.references",
                )
            )
        if relative.parts[0] == "transport":
            assert not any(
                name == forbidden or name.startswith(f"{forbidden}.")
                for name in imports
                for forbidden in ("b24api.batch", "b24api.traversal", "b24api.references")
            )
        if relative.parts[0] == "testing":
            assert not any(
                name == forbidden or name.startswith(f"{forbidden}.")
                for name in imports
                for forbidden in (
                    "httpx",
                    "pytest",
                    "b24api.client",
                    "b24api.batch",
                    "b24api.traversal",
                    "b24api.references",
                )
            )


def test_completion_layer_does_not_import_traversal_families() -> None:
    completion = sorted((PACKAGE / "completion").glob("*.py"))
    assert completion
    for path in completion:
        imports = _runtime_imports(path)
        assert not any(
            name == family or name.startswith(f"{family}.")
            for name in imports
            for family in ("b24api.traversal", "b24api.references", "b24api.batch")
        ), f"{path.relative_to(PACKAGE)} imports a traversal family at runtime"


def test_runtime_imports_ignore_only_type_checking_blocks(tmp_path: Path) -> None:
    module = tmp_path / "module.py"
    module.write_text(
        "from typing import TYPE_CHECKING\n"
        "import b24api.contracts\n"
        "if TYPE_CHECKING:\n"
        "    from b24api.traversal.values import IdentityValue\n"
        "else:\n"
        "    from b24api.traversal import plans\n",
        encoding="utf-8",
    )
    assert _runtime_imports(module) == {"typing", "b24api.contracts", "b24api.traversal"}


def test_completion_recorder_imports_without_the_package_facade() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            # A bare parent package keeps the b24api facade, which imports every family, out of the way.
            "import importlib, sys, types; package = types.ModuleType('b24api');"
            " package.__path__ = ['b24api']; sys.modules['b24api'] = package;"
            " importlib.import_module('b24api.completion.recorder');"
            " assert not any(name.startswith(('b24api.traversal', 'b24api.references', 'b24api.batch'))"
            " for name in sys.modules), sorted(sys.modules)",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_executor_binds_host_through_transport_protocol() -> None:
    from b24api.execution.executor import Executor  # noqa: PLC0415 - focused contract import
    from b24api.execution.rate import RateCoordinator  # noqa: PLC0415 - focused contract import
    from b24api.testing.scripted import ScriptedTransport  # noqa: PLC0415 - focused contract import
    from b24api.transport.httpx import HttpxTransport  # noqa: PLC0415 - focused contract import

    assert "b24api.transport.httpx" not in _imports(PACKAGE / "execution" / "executor.py")

    shared = RateCoordinator()
    Executor(ScriptedTransport((), host="a.example"), coordinator=shared)
    Executor(ScriptedTransport((), host="a.example"), coordinator=shared)
    with pytest.raises(ValueError, match="already bound to another host"):
        Executor(ScriptedTransport((), host="b.example"), coordinator=shared)

    httpx_shared = RateCoordinator()
    segment = "synthetic-segment"  # composed so the tracked file carries no webhook-shaped literal
    Executor(HttpxTransport(f"https://portal.example/rest/1/{segment}/"), coordinator=httpx_shared)
    with pytest.raises(ValueError, match="already bound to another host"):
        Executor(ScriptedTransport((), host="other.example"), coordinator=httpx_shared)

    class HostlessTransport:
        async def send(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("a transport without a host is refused before any send")

    with pytest.raises(TypeError, match="normalized portal host"):
        Executor(cast("Any", HostlessTransport()))


def test_report_replacements_stay_on_reviewed_downgrade_sites() -> None:
    # Only the gate builds terminal reports; ``dataclasses.replace`` on a report is a second
    # construction path, so every site that does it is pinned here for review. The AST cannot see
    # types, so any first argument whose source names a report counts.
    sites = {
        path.relative_to(PACKAGE).as_posix()
        for path in _sources()
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.Call)
        and (
            (isinstance(node.func, ast.Name) and node.func.id == "replace")
            or (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "replace"
                and ast.unparse(node.func.value) == "dataclasses"
            )
        )
        and node.args
        and "report" in ast.unparse(node.args[0]).casefold()
    }
    # execution/failure.py only downgrades a report to FAILED/INCOMPLETE with exhausted=False or adds a
    # cleanup-failure violation, for the lifecycle runner and the public failure finalizer.
    assert sites == {"execution/failure.py"}


def test_failure_classification_importers_stay_inside_state_machine_layers() -> None:
    owner = "b24api.execution.failure"
    for path in _sources():
        if owner not in _imports(path):
            continue
        relative = path.relative_to(PACKAGE)
        assert relative == Path("completion/operation_stream.py") or relative.parts[0] in {
            "batch",
            "execution",
            "references",
            "traversal",
        }, f"{relative} imports the internal failure-classification seam"


def test_request_derivation_uses_keyword_fields_outside_owning_module() -> None:
    owner = PACKAGE / "contracts" / "request.py"
    for path in _sources():
        if path == owner:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "Request":
                assert len(node.args) <= 1, f"{path.relative_to(PACKAGE)} reconstructs Request positionally"


def test_cli_uses_only_root_contracts_and_its_closed_router() -> None:
    for relative in (Path("cli.py"), Path("cli_contract.py")):
        imports = {name for name in _imports(PACKAGE / relative) if name.startswith("b24api")}
        assert all(
            name in {"b24api", "b24api.cli_contract", "b24api.contracts"} or name.startswith("b24api.contracts.")
            for name in imports
        )


def test_no_endpoint_catalog_mutable_registry_or_evidence_literal_exists() -> None:
    for path in _sources():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                lowered = node.value.casefold()
                assert not lowered.startswith(("tasks.", "crm.", "im."))
        for node in tree.body:
            if not isinstance(node, ast.Assign | ast.AnnAssign):
                continue
            value = node.value
            targets = node.targets if isinstance(node, ast.Assign) else (node.target,)
            names = {target.id for target in targets if isinstance(target, ast.Name)}
            if isinstance(value, ast.Dict | ast.List | ast.Set):
                assert names <= {"__all__"}


def test_removed_modules_symbols_and_storage_backends_are_absent() -> None:
    assert not (_FORBIDDEN_ROOT_MODULES & {path.name for path in PACKAGE.iterdir() if path.is_file()})
    assert not (_REMOVED_CLIENT_METHODS & set(vars(b24api.client.Bitrix24)))
    # Removed v1 public names and parameters stay absent from every public namespace and signature.
    namespaces = (b24api, b24api.contracts, b24api.errors, b24api.transport, b24api.completion, b24api.testing)
    assert not {"IdentityTracker", "RequestWithPayload"} & {name for module in namespaces for name in dir(module)}
    parameters = {
        name
        for method in vars(b24api.client.Bitrix24).values()
        if callable(method)
        for name in inspect.signature(method).parameters
    }
    assert not {"max_tracked_identities", "with_payload", "fallback_failed"} & parameters
    # Protective ban (B21): the client keeps no durable storage, so no storage backend may appear.
    runtime = "\n".join(path.read_text(encoding="utf-8") for path in _sources())
    assert "sqlite" not in runtime.casefold()
    forbidden_imports = ("sqlalchemy", "django.db", "peewee", "sqlmodel", "tortoise")
    assert not any(
        name == forbidden or name.startswith(f"{forbidden}.")
        for path in _sources()
        for name in _imports(path)
        for forbidden in forbidden_imports
    )
    # Protective ban (B21): endpoint-specific adapters live in recipes, never in the package.
    assert "im.dialog.messages.get" not in runtime
    assert "ImMessagePageAdapter" not in runtime


def test_response_selection_funnels_and_dead_batch_sentinel_stay_closed() -> None:
    call_sites: set[tuple[str, str]] = set()
    for path in _sources():
        relative = path.relative_to(PACKAGE).as_posix()
        _ResponseItemsVisitor(relative, call_sites).visit(ast.parse(path.read_text(encoding="utf-8")))

    assert call_sites == {
        ("traversal/driver.py", "select_page"),
        ("traversal/keyset_verifier.py", "_identities"),
        ("traversal/keyset_verifier.py", "_record_response"),
        ("traversal/keyset_page_validation.py", "select_rows"),
        ("traversal/keyset_page_validation.py", "validate_lane_receipt"),
    }
    tree = ast.parse((PACKAGE / "references" / "dispatch.py").read_text(encoding="utf-8"))
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"put", "put_nowait"}
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value is None
        for node in ast.walk(tree)
    )


def test_project_configuration_has_no_removed_v1_runtime_or_test_knobs() -> None:
    # Protective ban (B21): removed v1 settings must not come back through project configuration.
    configuration = (ROOT / "pyproject.toml").read_text(encoding="utf-8")

    for removed in (
        "tests.api_test",
        "BITRIX24_API_RETRY_DELAY",
        "BITRIX24_API_RETRY_BACKOFF",
    ):
        assert removed not in configuration


def test_response_bodies_are_read_only_as_raw_bytes_through_the_bounded_decoder() -> None:
    # Protective source guard, kept with this justification under B21: HTTPX's decoded readers inflate a
    # whole received chunk before any byte ceiling can see its size (A2: a 32 KiB gzip allocated 81 MB, a
    # ``gzip, gzip`` cascade 1.1 GB). Bodies must come from ``aiter_raw``/``iter_raw`` through
    # ``_BoundedDecoder``. A behavioural test cannot prove that no future call site reintroduces one.
    decoded_readers = {"aiter_bytes", "iter_bytes", "aiter_text", "iter_text", "aiter_lines", "iter_lines", "aread"}
    paths = (*sorted((PACKAGE / "transport").glob("*.py")), ROOT / "tools/b24api_evidence/harness/live.py")
    for path in paths:
        calls = {
            node.func.attr
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        assert not calls & decoded_readers, f"{path.relative_to(ROOT)} reads decoded HTTPX bytes"


def test_module_sizes_are_recorded(record_property: Callable[[str, object], None]) -> None:
    # Module size is a signal, not a gate (B21): the import DAG, the private-access and the
    # function-length ratchets block instead. The sizes land in the JUnit report for review.
    for path in _sources():
        relative = path.relative_to(PACKAGE).as_posix()
        record_property(f"lines:{relative}", len(path.read_text(encoding="utf-8").splitlines()))


def test_fast_keyset_state_and_selector_boundaries_are_enforced() -> None:
    fast_sources = tuple(sorted((PACKAGE / "traversal").glob("keyset_*.py")))
    # Protective (B21): the runtime is the only owner of the fast buffered-row counter.
    delta_callers = {path.name for path in fast_sources if "adjust_buffered_rows(" in path.read_text(encoding="utf-8")}
    assert delta_callers == {"keyset_runtime.py"}
    assert all("set_buffered_rows(" not in path.read_text(encoding="utf-8") for path in fast_sources)
    # Protective (B21, §3.6): the transaction host is a protocol, never a shared scheduler-state bag.
    assert not (PACKAGE / "traversal" / "keyset_scheduler_support.py").exists()
    assert not (PACKAGE / "traversal" / "keyset_scheduler_state.py").exists()
    assert all("SchedulerState" not in path.read_text(encoding="utf-8") for path in fast_sources)
    # Sequential and fast keyset build every page request through the one shared step.
    for name in ("keyset.py", "keyset_runtime.py"):
        path = PACKAGE / "traversal" / name
        tree = ast.parse(path.read_text(encoding="utf-8"))
        assert any(
            isinstance(node, ast.ImportFrom)
            and (
                node.module == "b24api.traversal.keyset_step"
                or (node.module == "b24api.traversal" and any(alias.name == "keyset_step" for alias in node.names))
            )
            for node in ast.walk(tree)
        ), f"{name} does not import the shared keyset step"
        called = {
            node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", None)
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
        }
        assert "keyset_page_request" in called, f"{name} builds keyset pages without the shared step"

    # Protective (B21): the auto selector is deterministic integer arithmetic, so its inputs never
    # include randomness, clocks, environment or floats.
    for name in ("keyset_auto.py", "keyset_geometry.py"):
        selector = (PACKAGE / "traversal" / name).read_text(encoding="utf-8")
        for forbidden in ("random", "time.", "os.environ", "float("):
            assert forbidden not in selector
        tree = ast.parse(selector)
        assert not any(isinstance(node, ast.Constant) and isinstance(node.value, float) for node in ast.walk(tree))

    transactions = ast.parse((PACKAGE / "traversal" / "keyset_transactions.py").read_text(encoding="utf-8"))
    assert not any(isinstance(node, ast.ClassDef) for node in transactions.body)
    assignments = (ast.Assign, ast.AnnAssign, ast.AugAssign)
    assert all(
        isinstance(node, ast.Assign)
        and all(isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets)
        for node in transactions.body
        if isinstance(node, assignments)
    )


def test_only_completion_gate_constructs_terminal_operation_reports() -> None:
    constructors = {
        path.relative_to(PACKAGE).as_posix()
        for path in _sources()
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "OperationReport"
    }
    assert constructors == {"completion/gate.py"}


def test_closure_reason_constants_map_to_their_qualified_closure() -> None:
    from b24api.completion import closure  # noqa: PLC0415 - focused contract import
    from b24api.contracts.completion import BindingClosure  # noqa: PLC0415 - focused contract import

    assert closure.qualified_closure(closure.SINGLE_RESPONSE_COMPLETE) is BindingClosure.SINGLE_RESPONSE
    assert closure.qualified_closure(closure.QUALIFIED_TOTAL_REACHED) is BindingClosure.QUALIFIED_TOTAL
    assert closure.qualified_closure(closure.SPARSE_RAW_RANGE_COVERED) is BindingClosure.RAW_RANGE_COVERED
    assert closure.qualified_closure(closure.ADMITTED_UPPER_BOUNDARY_REACHED) is BindingClosure.BOUNDARY_SEEN
    assert closure.qualified_closure("source exhausted") is None
    assert closure.qualified_closure(None) is None


def test_pagination_driver_composes_strategies_instead_of_inheriting_them() -> None:
    # The driver owns the page transaction and composes one strategy per plan (§3.6 step 2).
    from b24api.traversal.counted_batch import CountedBatchStrategy  # noqa: PLC0415
    from b24api.traversal.driver import PaginationDriver  # noqa: PLC0415
    from b24api.traversal.strategy_context import PagedStrategy  # noqa: PLC0415

    assert PaginationDriver.__bases__ == (object,)
    assert isinstance(CountedBatchStrategy(batch_size=1, page_size=1), PagedStrategy)
    strategy_modules = ("sequential", "keyset", "cursor", "counted_batch")
    mixins = [
        node.name
        for module in strategy_modules
        for node in ast.parse((PACKAGE / "traversal" / f"{module}.py").read_text(encoding="utf-8")).body
        if isinstance(node, ast.ClassDef) and node.name.endswith("Mixin")
    ]
    assert not mixins
