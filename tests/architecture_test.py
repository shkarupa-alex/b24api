"""Executable package-boundary and legacy-removal gates for the v2 architecture."""

from __future__ import annotations
import ast
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest

import b24api.client

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "b24api"

_STATE_MACHINES = {
    "_stream.py",
    "completion/gate.py",
    "batch/engine.py",
    "batch/logical.py",
    "batch/stream.py",
    "execution/context.py",
    "execution/executor.py",
    "references/dispatch.py",
    "references/scheduler.py",
    "references/stream.py",
    "traversal/driver.py",
    "traversal/keyset_scheduler.py",
    "traversal/keyset_transactions.py",
    "traversal/keyset_verifier.py",
    "traversal/stream.py",
}
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


def test_runtime_layer_import_boundaries_are_acyclic_and_evidence_free() -> None:
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
    Executor(HttpxTransport("https://portal.example/rest/1/synthetic-token/"), coordinator=httpx_shared)
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
    # batch/stream.py and traversal/stream.py replace their KernelReport; references/support.py and
    # execution/failure.py only downgrade a report to FAILED/INCOMPLETE with exhausted=False.
    assert sites == {"batch/stream.py", "traversal/stream.py", "references/support.py", "execution/failure.py"}


def test_failure_classification_importers_stay_inside_state_machine_layers() -> None:
    owner = "b24api.execution.failure"
    for path in _sources():
        if owner not in _imports(path):
            continue
        relative = path.relative_to(PACKAGE)
        assert relative == Path("_stream.py") or relative.parts[0] in {
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
    runtime = "\n".join(path.read_text(encoding="utf-8") for path in _sources())
    for removed in (
        "IdentityTracker",
        "max_tracked_identities",
        "_ImplicitCompatibilityString",
        "_legacy_",
        "with_payload",
        "fallback_failed",
        "RequestWithPayload",
    ):
        assert removed not in runtime
    assert "sqlite" not in runtime.casefold()
    forbidden_imports = ("sqlalchemy", "django.db", "peewee", "sqlmodel", "tortoise")
    assert not any(
        name == forbidden or name.startswith(f"{forbidden}.")
        for path in _sources()
        for name in _imports(path)
        for forbidden in forbidden_imports
    )
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
        ("traversal/page_validation.py", "select_rows"),
        ("traversal/page_validation.py", "validate_lane_receipt"),
    }
    dispatch = (PACKAGE / "references" / "dispatch.py").read_text(encoding="utf-8")
    assert "self._queue: asyncio.Queue[_PendingBatch]" in dispatch
    tree = ast.parse(dispatch)
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
    configuration = (ROOT / "pyproject.toml").read_text(encoding="utf-8")

    for removed in (
        "tests.api_test",
        "BITRIX24_API_RETRY_DELAY",
        "BITRIX24_API_RETRY_BACKOFF",
    ):
        assert removed not in configuration


def test_module_sizes_keep_facades_small_and_state_machines_bounded() -> None:
    for path in _sources():
        relative = path.relative_to(PACKAGE).as_posix()
        line_count = len(path.read_text(encoding="utf-8").splitlines())
        ceiling = (
            750
            if relative == "traversal/driver.py"
            else 710
            if relative in _STATE_MACHINES
            else 550
            if relative == "errors.py"
            else 450
            if relative == "cli_contract.py"
            else 410
        )
        assert line_count <= ceiling, f"{relative} has {line_count} lines; ceiling is {ceiling}"


def test_fast_keyset_state_and_selector_boundaries_are_enforced() -> None:
    fast_sources = tuple(sorted((PACKAGE / "traversal").glob("keyset_*.py")))
    delta_callers = {path.name for path in fast_sources if "adjust_buffered_rows(" in path.read_text(encoding="utf-8")}
    assert delta_callers == {"keyset_scheduler.py"}
    assert all("set_buffered_rows(" not in path.read_text(encoding="utf-8") for path in fast_sources)
    scheduler = (PACKAGE / "traversal" / "keyset_scheduler.py").read_text(encoding="utf-8")
    assert "class KeysetFastScheduler:" in scheduler
    assert not (PACKAGE / "traversal" / "keyset_scheduler_support.py").exists()
    assert not (PACKAGE / "traversal" / "keyset_scheduler_state.py").exists()
    assert all("SchedulerState" not in path.read_text(encoding="utf-8") for path in fast_sources)
    sequential = (PACKAGE / "traversal" / "keyset.py").read_text(encoding="utf-8")
    assert "from b24api.traversal.keyset_step import" in sequential
    assert "from b24api.traversal import keyset_step" in scheduler
    assert "keyset_page_request(" in sequential
    assert "keyset_step.keyset_page_request(" in scheduler

    for name in ("keyset_auto.py", "keyset_costs.py"):
        selector = (PACKAGE / "traversal" / name).read_text(encoding="utf-8")
        for forbidden in ("random", "time.", "os.environ", "float("):
            assert forbidden not in selector
        tree = ast.parse(selector)
        assert not any(isinstance(node, ast.Constant) and isinstance(node.value, float) for node in ast.walk(tree))

    ceilings = {
        PACKAGE / "contracts" / "keyset_execution.py": 250,
        PACKAGE / "traversal" / "keyset_auto.py": 250,
        PACKAGE / "traversal" / "keyset_fast_plan.py": 300,
        PACKAGE / "traversal" / "keyset_fast_stream.py": 400,
        PACKAGE / "traversal" / "keyset_scheduler.py": 700,
    }
    for path, ceiling in ceilings.items():
        assert len(path.read_text(encoding="utf-8").splitlines()) <= ceiling

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
