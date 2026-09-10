"""Executable package-boundary and legacy-removal gates for the v2 architecture."""

from __future__ import annotations
import ast
from pathlib import Path

import b24api.client

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "b24api"

_STATE_MACHINES = {
    "_stream.py",
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
        ceiling = 700 if relative in _STATE_MACHINES else 400
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

    selector = (PACKAGE / "traversal" / "keyset_auto.py").read_text(encoding="utf-8")
    for forbidden in ("random", "time.", "os.environ", "float("):
        assert forbidden not in selector

    ceilings = {
        PACKAGE / "contracts" / "keyset_execution.py": 250,
        PACKAGE / "traversal" / "keyset_auto.py": 250,
        PACKAGE / "traversal" / "keyset_fast_stream.py": 400,
        PACKAGE / "traversal" / "keyset_scheduler.py": 700,
    }
    for path, ceiling in ceilings.items():
        assert len(path.read_text(encoding="utf-8").splitlines()) <= ceiling
