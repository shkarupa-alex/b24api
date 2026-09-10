"""Lightweight repository identity checks shared by standalone evidence tools."""

from __future__ import annotations
import shutil
import subprocess
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

_GIT_SHA_LENGTH = 40


class RepositoryStateError(RuntimeError):
    """Reject evidence whose tracked source cannot be bound to one clean commit."""


def git_sha(root: Path) -> str:
    """Resolve a repository HEAD as an exact lowercase SHA."""
    git = shutil.which("git")
    if git is None:
        raise RepositoryStateError("git executable is unavailable")
    try:
        value = subprocess.run(  # noqa: S603 - resolved executable and fixed arguments
            [git, "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise RepositoryStateError("cannot resolve candidate git SHA") from error
    if len(value) != _GIT_SHA_LENGTH or any(character not in "0123456789abcdef" for character in value):
        raise RepositoryStateError("candidate git SHA must be a 40-character lowercase git SHA")
    return value


def require_clean_tracked_tree(root: Path) -> None:
    """Reject staged or unstaged tracked content that differs from HEAD."""
    git = shutil.which("git")
    if git is None:
        raise RepositoryStateError("git executable is unavailable")
    for arguments in (("diff", "--quiet", "--"), ("diff", "--cached", "--quiet", "--")):
        result = subprocess.run(  # noqa: S603 - resolved executable and fixed arguments
            [git, *arguments],
            cwd=root,
            check=False,
            capture_output=True,
        )
        if result.returncode == 1:
            raise RepositoryStateError("evidence requires a clean tracked tree at the exact candidate SHA")
        if result.returncode != 0:
            raise RepositoryStateError("cannot verify tracked-tree cleanliness")


def clean_candidate_sha(root: Path) -> str:
    """Return HEAD only after proving all tracked source still matches it."""
    require_clean_tracked_tree(root)
    return git_sha(root)


__all__ = ["RepositoryStateError", "clean_candidate_sha", "git_sha", "require_clean_tracked_tree"]
