# ruff: noqa: INP001 - a standalone workflow script, not an importable package module
"""Release gate: refuse a noncanonical tag before building and unmatched distributions before upload.

``tag`` validates ``GITHUB_REF_NAME`` before the build backend runs. ``dists DIR`` checks that the
directory holds exactly one wheel and one sdist whose filenames and embedded metadata carry the
validated tag as their version, and that the wheel passes the ``wheel`` check. ``wheel FILE`` needs
no tag: it checks that a built wheel carries the ``b24api/py.typed`` marker and ships nothing beside
the package and its ``.dist-info`` (no ``docs/``, ``tests/``, ``tools/`` or ``examples/``). The
package version itself stays owned by setuptools-git-versioning; this gate only compares what the
build produced with the tag that triggered it.
"""

from __future__ import annotations
import argparse
import os
import re
import sys
import tarfile
import zipfile
from email.parser import HeaderParser
from pathlib import Path

PROJECT = "b24api"
RELEASE_TAG = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
ACCEPTED_FORM = (
    "release tags must be canonical stable versions MAJOR.MINOR.PATCH, for example 2.2.1, "
    "without a prefix such as v or fix-, leading zeros, or a prerelease or build suffix"
)
_WHEEL_PARTS = (5, 6)
TYPING_MARKER = f"{PROJECT}/py.typed"


class ReleaseGateError(Exception):
    """The tag or the built distributions do not satisfy the release contract."""

    def __init__(self, title: str, message: str) -> None:
        """Keep a short annotation title beside the actionable message."""
        super().__init__(message)
        self.title = title


def release_version(environ: dict[str, str] | os._Environ[str]) -> str:
    """Return the pushed tag when it is a canonical stable release version."""
    if environ.get("GITHUB_REF_TYPE", "tag") != "tag":
        raise ReleaseGateError("Invalid release ref", f"releases run only for pushed tags; {ACCEPTED_FORM}")
    tag = environ.get("GITHUB_REF_NAME", "")
    if RELEASE_TAG.fullmatch(tag) is None:
        raise ReleaseGateError(
            "Invalid release tag",
            f"{ACCEPTED_FORM}; push a canonical release tag instead",
        )
    return tag


def verify_distributions(dist: Path, version: str) -> tuple[Path, Path]:
    """Return the sole sdist and wheel after proving both carry ``version`` exactly."""
    files = sorted(path for path in dist.iterdir()) if dist.is_dir() else []
    sdists = [path for path in files if path.name.endswith(".tar.gz")]
    wheels = [path for path in files if path.name.endswith(".whl")]
    if len(sdists) != 1 or len(wheels) != 1 or len(files) != len(sdists) + len(wheels):
        raise ReleaseGateError(
            "Unexpected release artifacts",
            f"{dist} must contain exactly one sdist and one wheel and nothing else",
        )
    sdist, wheel = sdists[0], wheels[0]
    if sdist.name != f"{PROJECT}-{version}.tar.gz":
        raise ReleaseGateError("Version mismatch", f"sdist filename {sdist.name} does not carry version {version}")
    parts = wheel.name.removesuffix(".whl").split("-")
    if len(parts) not in _WHEEL_PARTS or parts[:2] != [PROJECT, version]:
        raise ReleaseGateError("Version mismatch", f"wheel filename {wheel.name} does not carry version {version}")
    _check_metadata(_wheel_metadata(wheel, version), version, wheel.name)
    verify_wheel_contents(wheel)
    _check_metadata(_sdist_metadata(sdist, version), version, sdist.name)
    return sdist, wheel


def verify_wheel_contents(wheel: Path) -> None:
    """Require the typing marker and refuse any top-level entry beside the package and its dist-info."""
    parts = wheel.name.removesuffix(".whl").split("-")
    if not wheel.name.endswith(".whl") or len(parts) not in _WHEEL_PARTS or parts[0] != PROJECT:
        raise ReleaseGateError("Unexpected release artifacts", f"{wheel.name} is not a {PROJECT} wheel")
    try:
        with zipfile.ZipFile(wheel) as archive:
            names = archive.namelist()
    except (OSError, zipfile.BadZipFile) as error:
        raise ReleaseGateError("Invalid wheel", f"{wheel.name} is not a readable wheel") from error
    if TYPING_MARKER not in names:
        raise ReleaseGateError(
            "Missing typing marker",
            f"{wheel.name} lacks {TYPING_MARKER}; declare it in [tool.setuptools.package-data]",
        )
    allowed = {PROJECT, f"{PROJECT}-{parts[1]}.dist-info"}
    unexpected = sorted({name.split("/", 1)[0] for name in names} - allowed)
    if unexpected:
        raise ReleaseGateError(
            "Unexpected wheel contents",
            f"{wheel.name} ships {', '.join(unexpected)} beside the {PROJECT} package and its dist-info",
        )


def _wheel_metadata(wheel: Path, version: str) -> str:
    member = f"{PROJECT}-{version}.dist-info/METADATA"
    try:
        with zipfile.ZipFile(wheel) as archive:
            return archive.read(member).decode("utf-8")
    except (KeyError, OSError, UnicodeDecodeError, zipfile.BadZipFile) as error:
        raise ReleaseGateError("Invalid metadata", f"{wheel.name} lacks readable {member}") from error


def _sdist_metadata(sdist: Path, version: str) -> str:
    member = f"{PROJECT}-{version}/PKG-INFO"
    try:
        with tarfile.open(sdist, "r:gz") as archive:
            extracted = archive.extractfile(member)
            text = None if extracted is None else extracted.read().decode("utf-8")
    except (KeyError, OSError, UnicodeDecodeError, tarfile.TarError) as error:
        raise ReleaseGateError("Invalid metadata", f"{sdist.name} lacks readable {member}") from error
    if text is None:
        raise ReleaseGateError("Invalid metadata", f"{sdist.name} lacks readable {member}")
    return text


def _check_metadata(text: str, version: str, artifact: str) -> None:
    headers = HeaderParser().parsestr(text)
    if not headers.get("Metadata-Version") or headers.get("Name") != PROJECT:
        raise ReleaseGateError("Invalid metadata", f"{artifact} metadata does not describe {PROJECT}")
    if headers.get("Version") != version:
        raise ReleaseGateError("Version mismatch", f"{artifact} metadata version is not {version}")


def main(argv: list[str] | None = None) -> int:
    """Run one gate and report a failure as a GitHub error annotation."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("tag", help="validate GITHUB_REF_NAME before building")
    dists = commands.add_parser("dists", help="verify built distributions against GITHUB_REF_NAME")
    dists.add_argument("directory", type=Path)
    wheel_check = commands.add_parser("wheel", help="verify the contents of one built wheel; needs no tag")
    wheel_check.add_argument("file", type=Path)
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "wheel":
            verify_wheel_contents(arguments.file)
            sys.stdout.write(f"verified the contents of {arguments.file.name}\n")
            return 0
        version = release_version(os.environ)
        if arguments.command == "dists":
            sdist, wheel = verify_distributions(arguments.directory, version)
            sys.stdout.write(f"verified {sdist.name} and {wheel.name} as version {version}\n")
        else:
            sys.stdout.write(f"release tag {version} is canonical\n")
    except ReleaseGateError as error:
        # The rejected tag is never echoed into the annotation command itself.
        sys.stdout.write(f"::error title={error.title}::{error}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
