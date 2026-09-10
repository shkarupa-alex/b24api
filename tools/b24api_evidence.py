"""Repository-only entry point for the Bitrix24 evidence harness."""

from __future__ import annotations
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.b24api_evidence.harness.cli import main
else:
    sys.path.insert(0, str(Path(__file__).with_suffix("")))
    from harness.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
