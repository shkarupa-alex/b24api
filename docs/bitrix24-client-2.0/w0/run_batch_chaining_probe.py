# ruff: noqa: INP001
"""Run the W0 read-only Bitrix24 batch command-chaining probe."""

from __future__ import annotations
import hashlib
import hmac
import importlib.metadata
import json
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

import httpx

WEBHOOK_ENV = "BITRIX24_API_WEBHOOK_URL"
FINGERPRINT_KEY_ENV = "BITRIX24_EVIDENCE_FINGERPRINT_KEY"
ROLE_ENV = "BITRIX24_EVIDENCE_CREDENTIAL_ROLE"
ALLOWED_ROLES = {"admin_full", "admin_limited", "employee_full", "employee_limited"}
COMMANDS = {
    "who": "profile",
    "dependent": "user.get?ID=$result[who][ID]",
}
REQUEST_SHAPE: dict[str, object] = {
    "halt": 0,
    "cmd": COMMANDS,
}
HTTP_OK = 200
GIT_EXECUTABLE = shutil.which("git")


def _repository_head() -> str:
    if GIT_EXECUTABLE is None:
        raise RuntimeError("git executable is required")
    return subprocess.run(  # noqa: S603 - executable is resolved to an absolute local path.
        [GIT_EXECUTABLE, "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _source_sha256() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _portal_fingerprint(host: str, role: str, principal_id: str | None, key: str) -> str:
    principal = principal_id if principal_id is not None else "missing"
    message = json.dumps([host, role, principal], ensure_ascii=True, separators=(",", ":"))
    return hmac.new(key.encode(), message.encode(), hashlib.sha256).hexdigest()


def _timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def main() -> int:
    """Execute one safe batch request and print only the sanitized artifact."""
    webhook = os.environ.get(WEBHOOK_ENV, "").strip()
    fingerprint_key = os.environ.get(FINGERPRINT_KEY_ENV, "")
    role = os.environ.get(ROLE_ENV, "")
    parsed = urlsplit(webhook)
    host = parsed.hostname or ""

    if parsed.scheme != "https" or not host or not parsed.path.endswith("/"):
        sys.stderr.write("invalid live configuration\n")
        return 2
    if not fingerprint_key:
        sys.stderr.write("missing fingerprint key\n")
        return 2
    if role not in ALLOWED_ROLES:
        sys.stderr.write("invalid credential role\n")
        return 2

    try:
        response = httpx.post(f"{webhook}batch.json", json=REQUEST_SHAPE, timeout=30.0)
        envelope = cast("Any", response.json())
    except (httpx.HTTPError, ValueError):
        sys.stderr.write(f"probe transport or protocol failure for host {host}\n")
        return 3

    result_envelope = envelope.get("result", {}) if isinstance(envelope, dict) else {}
    result = result_envelope.get("result", {}) if isinstance(result_envelope, dict) else {}
    who = result.get("who", {}) if isinstance(result, dict) else {}
    dependent = result.get("dependent", []) if isinstance(result, dict) else []
    who_id = str(who.get("ID")) if isinstance(who, dict) and who.get("ID") is not None else None
    dependent_ids = (
        [str(row["ID"]) for row in dependent if isinstance(row, dict) and row.get("ID") is not None]
        if isinstance(dependent, list)
        else []
    )
    command_errors = result_envelope.get("result_error", {}) if isinstance(result_envelope, dict) else {}
    envelope_shape_valid = all(
        (
            isinstance(envelope, dict),
            isinstance(result_envelope, dict),
            isinstance(result, dict),
            isinstance(command_errors, dict),
        ),
    )
    command_error_keys = (
        sorted(key for key in command_errors if isinstance(key, str) and key in COMMANDS)
        if isinstance(command_errors, dict)
        else []
    )
    unexpected_command_error_count = (
        sum(1 for key in command_errors if not isinstance(key, str) or key not in COMMANDS)
        if isinstance(command_errors, dict)
        else 0
    )
    structured_error = bool(envelope.get("error")) if isinstance(envelope, dict) else True
    matched = who_id is not None and dependent_ids == [who_id]
    passed = all(
        (
            response.status_code == HTTP_OK,
            envelope_shape_valid,
            not structured_error,
            not command_error_keys,
            unexpected_command_error_count == 0,
            matched,
        ),
    )

    artifact = {
        "schema_version": "1.2",
        "observed_at": _timestamp(),
        "runner": {
            "kind": "committed_python_httpx",
            "repository_head_sha": _repository_head(),
            "source_sha256": _source_sha256(),
            "python": ".".join(map(str, sys.version_info[:3])),
            "httpx": importlib.metadata.version("httpx"),
        },
        "host": host,
        "portal_fingerprint": _portal_fingerprint(host, role, who_id, fingerprint_key),
        "portal_fingerprint_algorithm": "hmac-sha256-portal-role-principal-v1",
        "credential_role": role,
        "read_only": True,
        "request_count": 1,
        "command_keys": list(COMMANDS),
        "request_shape": REQUEST_SHAPE,
        "response_summary": {
            "http_status": response.status_code,
            "envelope_shape_valid": envelope_shape_valid,
            "structured_error": structured_error,
            "command_error_keys": command_error_keys,
            "unexpected_command_error_count": unexpected_command_error_count,
            "profile_identity_present": who_id is not None,
            "dependent_row_count": len(dependent_ids),
            "dependent_identity_matched": matched,
        },
        "outcome": "PASS" if passed else "INCONCLUSIVE",
        "conclusion": "supported_for_observed_query_shape_only" if passed else "not_observed",
        "authorization_effect": "none",
    }
    sys.stdout.write(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    return 0 if passed else 5


def _safe_entrypoint() -> int:
    try:
        return main()
    except Exception:  # noqa: BLE001 - never render unexpected exception data beside a webhook.
        sys.stderr.write("probe failed safely\n")
        return 3


if __name__ == "__main__":
    raise SystemExit(_safe_entrypoint())
