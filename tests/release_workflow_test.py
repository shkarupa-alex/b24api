"""The release workflow refuses noncanonical tags early and publishes only distributions that match them."""

from __future__ import annotations
import importlib.util
import io
import os
import re
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import ModuleType

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "publish-to-pypi.yml"
SCRIPT = ROOT / ".github" / "scripts" / "verify_release.py"
VERSION = "2.2.1"


def _gate() -> ModuleType:
    spec = importlib.util.spec_from_file_location("verify_release", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


def _write_wheel(dist: Path, *, filename: str | None = None, metadata: bytes | None = None) -> Path:
    path = dist / (filename or f"b24api-{VERSION}-py3-none-any.whl")
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(f"b24api-{VERSION}.dist-info/METADATA", metadata or _metadata())
        archive.writestr("b24api/__init__.py", "")
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
