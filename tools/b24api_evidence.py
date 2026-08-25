# ruff: noqa: INP001
"""Repository-only entry point for the Bitrix24 evidence harness."""

from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).with_suffix("")))

from harness.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
