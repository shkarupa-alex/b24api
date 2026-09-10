"""Repository-only entry point for the Bitrix24 evidence harness."""

from __future__ import annotations

from tools.b24api_evidence.harness.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
