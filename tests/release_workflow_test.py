"""The CI gate guards every merge and release; releases publish only distributions that match their tag."""

from __future__ import annotations
import importlib.util
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import yaml

from b24api import Request

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import ModuleType

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "publish-to-pypi.yml"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
SCRIPT = ROOT / ".github" / "scripts" / "verify_release.py"
CI_GATE = ROOT / ".github" / "scripts" / "ci_gate.py"
MYPY_RATCHET = ROOT / ".github" / "scripts" / "mypy_ratchet.py"
MYPY_BASELINE = ROOT / "tests" / "mypy_baseline.txt"
PYPROJECT = ROOT / "pyproject.toml"
VERSION = "2.2.1"
# Jobs of ci.yml the aggregate gate must not wait for: the gate itself and the non-blocking
# httpx-latest evidence job. Every other job is blocking and must be in the gate's needs.
NON_BLOCKING_JOBS = frozenset({"gate", "httpx-latest"})
SPECIFIED_BLOCKING_JOBS = frozenset({"lint", "types", "tests", "min-deps", "wheel-typing", "slow"})
MYPY_TESTS_ERRORS_AT_SPECIFICATION = 385


def _load_script(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _gate() -> ModuleType:
    return _load_script("verify_release", SCRIPT)


def _run(*arguments: str, tag: str, ref_type: str = "tag", cwd: Path = ROOT) -> subprocess.CompletedProcess[str]:
    environment = {key: value for key, value in os.environ.items() if key not in {"GITHUB_REF_NAME", "GITHUB_REF_TYPE"}}
    environment.update({"GITHUB_REF_NAME": tag, "GITHUB_REF_TYPE": ref_type})
    return subprocess.run(  # noqa: S603 - fixed interpreter and repository script
        [sys.executable, str(SCRIPT), *arguments],
        cwd=cwd,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )


def _metadata(name: str = "b24api", version: str = VERSION) -> bytes:
    return f"Metadata-Version: 2.4\nName: {name}\nVersion: {version}\nSummary: Bitrix24 API\n".encode()


def _write_wheel(
    dist: Path,
    *,
    filename: str | None = None,
    metadata: bytes | None = None,
    typed: bool = True,
    extra: tuple[str, ...] = (),
) -> Path:
    path = dist / (filename or f"b24api-{VERSION}-py3-none-any.whl")
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(f"b24api-{VERSION}.dist-info/METADATA", metadata or _metadata())
        archive.writestr("b24api/__init__.py", "")
        if typed:
            archive.writestr("b24api/py.typed", "")
        for member in extra:
            archive.writestr(member, "")
    return path


def _write_sdist(dist: Path, *, filename: str | None = None, metadata: bytes | None = None) -> Path:
    path = dist / (filename or f"b24api-{VERSION}.tar.gz")
    content = metadata or _metadata()
    with tarfile.open(path, "w:gz") as archive:
        info = tarfile.TarInfo(f"b24api-{VERSION}/PKG-INFO")
        info.size = len(content)
        archive.addfile(info, io.BytesIO(content))
    return path


@pytest.mark.parametrize("tag", ["2.2.1", "0.0.0", "2.3.0", "10.20.30"])
def test_canonical_stable_tags_pass_the_prebuild_gate(tag: str) -> None:
    result = _run("tag", tag=tag)

    assert result.returncode == 0, result.stdout
    assert f"release tag {tag} is canonical" in result.stdout


@pytest.mark.parametrize(
    "tag",
    [
        "fix-2.2.1",
        "v2.2.1",
        "2.2",
        "2.2.1.0",
        "02.2.1",
        "2.02.1",
        "2.2.01",
        "2.2.1rc1",
        "2.2.1-rc.1",
        "2.2.1a1",
        "2.2.1.dev0",
        "2.2.1.post1",
        "2.2.1+local",
        "release/2.2.1",
        "",
    ],
)
def test_noncanonical_tags_fail_before_the_build_with_the_accepted_form(tag: str) -> None:
    result = _run("tag", tag=tag)

    assert result.returncode == 1
    # The exact line proves the actionable form is named and the rejected tag is never echoed.
    assert result.stdout == (
        "::error title=Invalid release tag::release tags must be canonical stable versions "
        "MAJOR.MINOR.PATCH, for example 2.2.1, without a prefix such as v or fix-, leading zeros, "
        "or a prerelease or build suffix; push a canonical release tag instead\n"
    )


def test_branch_refs_are_refused_even_with_a_version_shaped_name() -> None:
    result = _run("tag", tag=VERSION, ref_type="branch")

    assert result.returncode == 1
    assert result.stdout.startswith("::error title=Invalid release ref::")


def test_matching_sdist_and_wheel_pass_the_distribution_gate(tmp_path: Path) -> None:
    _write_sdist(tmp_path)
    _write_wheel(tmp_path)

    result = _run("dists", str(tmp_path), tag=VERSION)

    assert result.returncode == 0, result.stdout
    assert f"verified b24api-{VERSION}.tar.gz and b24api-{VERSION}-py3-none-any.whl" in result.stdout


def _missing_wheel(dist: Path) -> None:
    _write_sdist(dist)


def _missing_sdist(dist: Path) -> None:
    _write_wheel(dist)


def _extra_file(dist: Path) -> None:
    _write_sdist(dist)
    _write_wheel(dist)
    (dist / "notes.txt").write_text("unexpected", encoding="utf-8")


def _two_wheels(dist: Path) -> None:
    _write_sdist(dist)
    _write_wheel(dist)
    _write_wheel(dist, filename=f"b24api-{VERSION}-py3-none-manylinux_2_17_x86_64.whl")


def _dirty_sdist_name(dist: Path) -> None:
    _write_sdist(dist, filename=f"b24api-{VERSION}.post1+dirty.tar.gz")
    _write_wheel(dist)


def _other_wheel_version(dist: Path) -> None:
    _write_sdist(dist)
    _write_wheel(dist, filename="b24api-2.3.0-py3-none-any.whl")


def _wheel_metadata_version(dist: Path) -> None:
    _write_sdist(dist)
    _write_wheel(dist, metadata=_metadata(version="fix.2.2.1"))


def _sdist_metadata_version(dist: Path) -> None:
    _write_sdist(dist, metadata=_metadata(version="2.2.1.post1"))
    _write_wheel(dist)


def _wheel_metadata_name(dist: Path) -> None:
    _write_sdist(dist)
    _write_wheel(dist, metadata=_metadata(name="other"))


def _wheel_without_metadata(dist: Path) -> None:
    _write_sdist(dist)
    path = dist / f"b24api-{VERSION}-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("b24api/__init__.py", "")


def _corrupt_sdist(dist: Path) -> None:
    (dist / f"b24api-{VERSION}.tar.gz").write_bytes(b"not a tarball")
    _write_wheel(dist)


def _untyped_wheel(dist: Path) -> None:
    _write_sdist(dist)
    _write_wheel(dist, typed=False)


def _wheel_with_specifications(dist: Path) -> None:
    _write_sdist(dist)
    _write_wheel(dist, extra=("docs/specifications/README.md",))


@pytest.mark.parametrize(
    ("arrange", "title"),
    [
        (_missing_wheel, "Unexpected release artifacts"),
        (_missing_sdist, "Unexpected release artifacts"),
        (_extra_file, "Unexpected release artifacts"),
        (_two_wheels, "Unexpected release artifacts"),
        (_dirty_sdist_name, "Version mismatch"),
        (_other_wheel_version, "Version mismatch"),
        (_wheel_metadata_version, "Version mismatch"),
        (_sdist_metadata_version, "Version mismatch"),
        (_wheel_metadata_name, "Invalid metadata"),
        (_wheel_without_metadata, "Invalid metadata"),
        (_corrupt_sdist, "Invalid metadata"),
        (_untyped_wheel, "Missing typing marker"),
        (_wheel_with_specifications, "Unexpected wheel contents"),
    ],
)
def test_distribution_gate_refuses_missing_extra_mismatched_or_invalid_artifacts(
    tmp_path: Path,
    arrange: Callable[[Path], None],
    title: str,
) -> None:
    arrange(tmp_path)

    result = _run("dists", str(tmp_path), tag=VERSION)

    assert result.returncode == 1
    assert result.stdout.startswith(f"::error title={title}::")


def test_distribution_gate_refuses_a_missing_directory(tmp_path: Path) -> None:
    result = _run("dists", str(tmp_path / "dist"), tag=VERSION)

    assert result.returncode == 1
    assert result.stdout.startswith("::error title=Unexpected release artifacts::")


def test_distribution_gate_revalidates_the_tag(tmp_path: Path) -> None:
    _write_sdist(tmp_path)
    _write_wheel(tmp_path)

    result = _run("dists", str(tmp_path), tag="fix-2.2.1")

    assert result.returncode == 1
    assert result.stdout.startswith("::error title=Invalid release tag::")


def test_gate_regex_matches_the_specified_release_rule() -> None:
    gate = _gate()
    assert gate.RELEASE_TAG.pattern == r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    assert gate.release_version({"GITHUB_REF_NAME": VERSION}) == VERSION


def _steps(job: str) -> list[str]:
    text = WORKFLOW.read_text(encoding="utf-8")
    body = text.split(f"  {job}:\n", 1)[1]
    body = re.split(r"\n  [a-z][a-z-]*:\n", body, maxsplit=1)[0]
    return re.split(r"\n      - ", body.split("    steps:\n", 1)[1])


def test_workflow_validates_before_building_and_verifies_before_upload() -> None:
    steps = _steps("release-build")
    names = [next(line for line in step.splitlines() if line.strip()) for step in steps]
    header = WORKFLOW.read_text(encoding="utf-8").split("  release-build:\n", 1)[1].split("    steps:\n", 1)[0]

    assert "    needs:\n      - ci\n" in header
    assert names == [
        "      - uses: actions/checkout@v7",
        "uses: actions/setup-python@v7",
        "name: Validate release tag",
        "name: Build release distributions",
        "name: Verify release distributions",
        "name: Upload distributions",
    ]
    assert 'python-version: "3.12"' in steps[1]
    assert "python .github/scripts/verify_release.py tag" in steps[2]
    assert "python -m build" in steps[3]
    assert "python .github/scripts/verify_release.py dists dist" in steps[4]
    assert "twine check --strict dist/*" in steps[4]
    assert "actions/upload-artifact@v7" in steps[5]
    assert "name: release-dists" in steps[5]
    assert "path: dist/" in steps[5]
    assert "if-no-files-found: error" in steps[5]


def test_workflow_publishes_only_after_the_build_job_with_trusted_publishing() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    publish = text.split("  pypi-publish:\n", 1)[1]
    steps = _steps("pypi-publish")

    assert "    needs:\n      - release-build\n" in publish
    assert "      id-token: write\n" in publish
    assert "      name: pypi\n      url: https://pypi.org/p/b24api\n" in publish
    assert "actions/download-artifact@v7" in steps[0]
    assert "name: release-dists" in steps[0]
    assert "path: dist/" in steps[0]
    assert "pypa/gh-action-pypi-publish@release/v1" in steps[1]
    assert "packages-dir: dist/" in steps[1]
    assert text.count("id-token: write") == 1
    assert "permissions:\n  contents: read\n" in text


def test_workflow_uses_node24_action_majors_and_never_interpolates_the_ref() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    actions = re.findall(r"uses: (actions/[a-z-]+)@(\S+)", text)

    assert sorted(actions) == [
        ("actions/checkout", "v7"),
        ("actions/download-artifact", "v7"),
        ("actions/setup-python", "v7"),
        ("actions/upload-artifact", "v7"),
    ]
    assert "${{" not in text
    assert "tags:\n      - '*'\n" in text


def test_wheel_check_needs_no_tag_and_accepts_the_typed_package_with_its_dist_info(tmp_path: Path) -> None:
    wheel = _write_wheel(tmp_path)

    result = _run("wheel", str(wheel), tag="", ref_type="branch")

    assert result.returncode == 0, result.stdout
    assert f"verified the contents of {wheel.name}" in result.stdout


@pytest.mark.parametrize(
    ("typed", "extra", "title"),
    [
        (False, (), "Missing typing marker"),
        (True, ("docs/specifications/b24api-issues-architecture/specification.md",), "Unexpected wheel contents"),
        (True, ("docs/architecture.md",), "Unexpected wheel contents"),
        (True, ("tests/__init__.py",), "Unexpected wheel contents"),
        (True, ("tools/b24api_evidence.py",), "Unexpected wheel contents"),
        (True, ("examples/run.py",), "Unexpected wheel contents"),
        (True, ("b24api-9.9.9.dist-info/METADATA",), "Unexpected wheel contents"),
    ],
)
def test_wheel_check_refuses_an_untyped_wheel_or_files_beside_the_package(
    tmp_path: Path,
    *,
    typed: bool,
    extra: tuple[str, ...],
    title: str,
) -> None:
    wheel = _write_wheel(tmp_path, typed=typed, extra=extra)

    result = _run("wheel", str(wheel), tag="")

    assert result.returncode == 1
    assert result.stdout.startswith(f"::error title={title}::")


def test_wheel_check_refuses_an_unreadable_or_foreign_wheel(tmp_path: Path) -> None:
    corrupt = tmp_path / f"b24api-{VERSION}-py3-none-any.whl"
    corrupt.write_bytes(b"not a zip")
    foreign = _write_wheel(tmp_path, filename=f"other-{VERSION}-py3-none-any.whl")

    assert _run("wheel", str(corrupt), tag="").stdout.startswith("::error title=Invalid wheel::")
    assert _run("wheel", str(foreign), tag="").stdout.startswith("::error title=Unexpected release artifacts::")


def test_the_typing_marker_is_shipped_as_package_data() -> None:
    config = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))

    assert (ROOT / "b24api" / "py.typed").is_file()
    assert "py.typed" in config["tool"]["setuptools"]["package-data"]["b24api"]


def _yaml(path: Path) -> dict[Any, Any]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    return document


def _ci_jobs() -> dict[str, dict[str, Any]]:
    jobs = _yaml(CI_WORKFLOW)["jobs"]
    assert isinstance(jobs, dict)
    return jobs


def _blocking_jobs() -> set[str]:
    return set(_ci_jobs()) - NON_BLOCKING_JOBS


def _runs(job: str) -> str:
    return "\n".join(step.get("run", "") for step in _ci_jobs()[job]["steps"])


def test_ci_workflow_is_named_ci_and_runs_on_push_pull_request_and_workflow_call() -> None:
    workflow = _yaml(CI_WORKFLOW)
    # PyYAML reads the bare key ``on`` as the boolean True (YAML 1.1).
    triggers = workflow.get("on", workflow.get(True))

    assert workflow["name"] == "ci"
    assert triggers == ["push", "pull_request", "workflow_call"]
    assert workflow["permissions"] == {"contents": "read"}
    assert _blocking_jobs() >= SPECIFIED_BLOCKING_JOBS
    assert set(_ci_jobs()) >= NON_BLOCKING_JOBS


def test_gate_is_the_only_ci_check_and_waits_for_every_blocking_job() -> None:
    jobs = _ci_jobs()
    gate = jobs["gate"]

    assert gate["name"] == "ci"
    assert gate["if"] == "always()"
    # Computed from the file: a job added to ci.yml but not wired into the gate fails here.
    assert sorted(gate["needs"]) == sorted(_blocking_jobs())
    assert len(gate["needs"]) == len(set(gate["needs"]))
    assert [name for name, job in jobs.items() if job.get("name") == "ci"] == ["gate"]
    assert not any(job.get("if") for name, job in jobs.items() if name != "gate")


def test_gate_step_reads_needs_from_the_environment_and_scripts_never_interpolate() -> None:
    gate_steps = [step for step in _ci_jobs()["gate"]["steps"] if "run" in step]

    assert len(gate_steps) == 1
    assert gate_steps[0]["run"] == "python3 .github/scripts/ci_gate.py"
    assert gate_steps[0]["env"] == {"NEEDS_JSON": "${{ toJSON(needs) }}"}
    for name in _ci_jobs():
        assert "${{" not in _runs(name), name


def test_httpx_latest_is_non_blocking_evidence_on_the_latest_httpx() -> None:
    jobs = _ci_jobs()
    runs = _runs("httpx-latest")

    assert jobs["httpx-latest"]["continue-on-error"] is True
    assert "httpx-latest" not in jobs["gate"]["needs"]
    assert 'uv pip install --upgrade "httpx[http2]"' in runs
    assert "uv run --no-sync pytest tests/httpx_logging_shield_test.py" in runs
    assert not any(job.get("continue-on-error") for name, job in jobs.items() if name != "httpx-latest")


def test_blocking_jobs_run_the_specified_checks() -> None:
    jobs = _ci_jobs()

    assert "ruff check --no-fix" in _runs("lint")
    assert "ruff format --check" in _runs("lint")
    assert "uv run --locked mypy b24api" in _runs("types")
    assert "python .github/scripts/mypy_ratchet.py" in _runs("types")
    assert jobs["tests"]["strategy"]["matrix"]["python-version"] == ["3.12", "3.13"]
    assert 'pytest -m "not slow and not benchmark" --cov=b24api' in _runs("tests")
    assert "--resolution lowest-direct" in _runs("min-deps")
    assert "-m pytest" in _runs("min-deps")
    assert "not slow" not in _runs("min-deps")
    assert "pytest -m slow" in _runs("slow")
    assert "uv build --wheel" in _runs("wheel-typing")
    assert "python .github/scripts/verify_release.py wheel" in _runs("wheel-typing")
    assert "cp tests/typing_smoke/app.py" in _runs("wheel-typing")
    assert "import-untyped" in _runs("wheel-typing")


def test_wheel_typing_passes_every_canonical_import_and_flags_an_old_root_import() -> None:
    runs = _runs("wheel-typing")

    assert "cp tests/typing_smoke/moved_names.py" in runs
    assert "mypy --strict --no-incremental app.py moved_names.py | tee mypy.txt" in runs
    assert 'cp tests/typing_smoke/root_alias.txt "$consumer/root_alias.py"' in runs
    assert "test \"$(grep -c ': error: ' negative.txt)\" = 1" in runs
    assert 'Module "b24api" has no attribute "CommandSuccess"  [attr-defined]' in runs


def test_wheel_typing_expects_the_real_request_location_revealed_by_the_smoke_consumer() -> None:
    smoke = (ROOT / "tests" / "typing_smoke" / "app.py").read_text(encoding="utf-8")
    revealed = f'Revealed type is "{Request.__module__}.{Request.__qualname__}"'

    assert "reveal_type(Request(" in smoke
    assert f"grep -F '{revealed}' mypy.txt" in _runs("wheel-typing")


def test_coverage_floor_has_one_source_in_pyproject() -> None:
    config = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    floor = config["tool"]["coverage"]["report"]["fail_under"]

    assert isinstance(floor, int)
    assert floor > 0
    assert any(dependency.startswith("pytest-cov") for dependency in config["dependency-groups"]["dev"])
    assert "--cov-fail-under" not in CI_WORKFLOW.read_text(encoding="utf-8")


def _run_ci_gate(payload: str | None) -> subprocess.CompletedProcess[str]:
    environment = {key: value for key, value in os.environ.items() if key != "NEEDS_JSON"}
    if payload is not None:
        environment["NEEDS_JSON"] = payload
    return subprocess.run(  # noqa: S603 - fixed interpreter and repository script
        [sys.executable, str(CI_GATE)],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )


def _needs(results: dict[str, str] | None = None) -> str:
    chosen = results or {}
    return json.dumps({job: {"result": chosen.get(job, "success"), "outputs": {}} for job in _blocking_jobs()})


def test_gate_script_passes_when_every_blocking_job_succeeded() -> None:
    result = _run_ci_gate(_needs())

    assert result.returncode == 0, result.stdout
    assert f"all {len(_blocking_jobs())} blocking jobs succeeded" in result.stdout


@pytest.mark.parametrize("outcome", ["failure", "cancelled", "skipped"])
@pytest.mark.parametrize("job", sorted(SPECIFIED_BLOCKING_JOBS))
def test_gate_script_fails_on_any_unsuccessful_blocking_job(job: str, outcome: str) -> None:
    result = _run_ci_gate(_needs({job: outcome}))

    assert result.returncode == 1
    assert result.stdout == f"::error title=CI gate::blocking job {job} finished with result {outcome}\n"


@pytest.mark.parametrize("payload", [None, "", "not json", "{}", "[]", '{"lint": {}}', '{"lint": "success"}'])
def test_gate_script_fails_closed_without_well_formed_results(payload: str | None) -> None:
    result = _run_ci_gate(payload)

    assert result.returncode == 1
    assert result.stdout.startswith("::error title=CI gate::")


def test_publish_workflow_runs_the_full_ci_before_building() -> None:
    jobs = _yaml(WORKFLOW)["jobs"]
    needs = jobs["release-build"]["needs"]

    assert jobs["ci"] == {"uses": "./.github/workflows/ci.yml"}
    assert "ci" in ([needs] if isinstance(needs, str) else needs)
    assert jobs["pypi-publish"]["needs"] == ["release-build"]


def _ratchet() -> ModuleType:
    return _load_script("mypy_ratchet", MYPY_RATCHET)


@pytest.mark.parametrize(
    ("output", "count"),
    [
        ("tests/a_test.py:1: error: boom  [misc]\nFound 385 errors in 32 files (checked 64 source files)\n", 385),
        ("tests/a_test.py:1: error: boom  [misc]\nFound 1 error in 1 file (checked 1 source file)\n", 1),
        ("Success: no issues found in 64 source files\n", 0),
    ],
)
def test_ratchet_reads_the_count_from_the_mypy_summary(output: str, count: int) -> None:
    assert _ratchet().error_count(output) == count


@pytest.mark.parametrize(
    "output",
    [
        "",
        "Traceback (most recent call last):\n  ...\nRuntimeError: internal error\n",
        "tests/a.py: error: Duplicate module named 'a'\nFound 1 error in 1 file (errors prevented further checking)\n",
    ],
)
def test_ratchet_refuses_output_without_a_complete_summary(output: str) -> None:
    ratchet = _ratchet()

    with pytest.raises(ratchet.RatchetError, match="no summary line"):
        ratchet.error_count(output)


def test_ratchet_allows_only_the_recorded_count() -> None:
    ratchet = _ratchet()

    assert ratchet.compare(10, 10) is None
    assert "more than the baseline 10" in ratchet.compare(11, 10)
    assert "fewer than the baseline 10" in ratchet.compare(9, 10)
    assert "--update" in ratchet.compare(9, 10)


def test_ratchet_cli_fails_on_growth_demands_recording_improvement_and_only_lowers(tmp_path: Path) -> None:
    ratchet = _ratchet()
    baseline = tmp_path / "baseline.txt"
    recorded, improved = 10, 9
    ratchet.write_baseline(baseline, recorded)

    def check(count: int, *options: str) -> int:
        output = tmp_path / f"mypy-{count}.txt"
        output.write_text(f"Found {count} errors in 3 files (checked 9 source files)\n", encoding="utf-8")
        return int(ratchet.main(["--baseline", str(baseline), "--mypy-output", str(output), *options]))

    assert check(recorded) == 0
    assert check(recorded + 1) == 1
    assert check(improved) == 1
    assert check(recorded + 2, "--update") == 1
    assert ratchet.read_baseline(baseline) == recorded
    assert check(improved, "--update") == 0
    assert ratchet.read_baseline(baseline) == improved
    assert check(improved) == 0


def test_ratchet_refuses_a_malformed_or_missing_baseline(tmp_path: Path) -> None:
    ratchet = _ratchet()
    baseline = tmp_path / "baseline.txt"

    for text in ("", "# only a comment\n", "10\n11\n", "-1\n", "ten\n"):
        baseline.write_text(text, encoding="utf-8")
        with pytest.raises(ratchet.RatchetError, match="exactly one non-negative integer"):
            ratchet.read_baseline(baseline)
    with pytest.raises(ratchet.RatchetError, match="cannot read"):
        ratchet.read_baseline(tmp_path / "missing.txt")


def test_committed_mypy_baseline_never_exceeds_the_specified_starting_count() -> None:
    assert 0 <= _ratchet().read_baseline(MYPY_BASELINE) <= MYPY_TESTS_ERRORS_AT_SPECIFICATION
