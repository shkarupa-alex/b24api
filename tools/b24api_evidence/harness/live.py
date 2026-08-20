# ruff: noqa: ANN401
"""Opt-in live transport isolated from the distributable ``b24api`` package."""

from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Any, Self
from urllib.parse import urljoin

import httpx

from .contracts import ContractError, PortalIdentity, parse_fingerprint_key, portal_identity, strict_json_loads

HTTP_OK = 200
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_MARKER_SCAN_PAGES = 1_000


class LiveUnavailableError(RuntimeError):
    """A portal, scope, tariff, or endpoint is unavailable for live evidence."""


class LiveCorrectnessError(RuntimeError):
    """A live response contradicts the reviewed disposable-entity contract."""


@dataclass(frozen=True, slots=True)
class LivePreflight:
    """Redacted portal facts retained after scope/app preflight."""

    identity: PortalIdentity
    build: str | None
    scopes: frozenset[str]


class LivePortal:
    """Minimal synchronous webhook session whose URL never enters artifacts or errors."""

    def __init__(self, *, role: str, timeout: float = 30.0) -> None:
        encoded_key = os.environ.get("BITRIX24_EVIDENCE_FINGERPRINT_KEY")
        if encoded_key is None:
            raise ContractError("BITRIX24_EVIDENCE_FINGERPRINT_KEY is required for --live")
        # Key strength is rejected before the credential is even read.
        parse_fingerprint_key(encoded_key)
        webhook_url = os.environ.get("BITRIX24_API_WEBHOOK_URL")
        if webhook_url is None:
            raise ContractError("BITRIX24_API_WEBHOOK_URL is required for --live")
        self.identity = portal_identity(webhook_url, role=role, fingerprint_key=encoded_key)
        self._webhook_url = webhook_url.rstrip("/") + "/"
        self._client = httpx.Client(timeout=timeout, follow_redirects=False)
        self.attempts = 0

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def call_envelope(self, method: str, parameters: dict[str, Any] | None = None) -> dict[str, Any]:
        """Call one REST method and return a strictly decoded, bounded envelope."""
        self.attempts += 1
        try:
            response = self._client.post(urljoin(self._webhook_url, method), json=parameters or {})
        except httpx.HTTPError as error:
            raise LiveUnavailableError(f"live transport unavailable for {method}") from error
        if response.status_code != HTTP_OK:
            raise LiveUnavailableError(f"live HTTP status {response.status_code} for {method}")
        if len(response.content) > MAX_RESPONSE_BYTES:
            raise LiveCorrectnessError(f"live response exceeds the reviewed byte ceiling for {method}")
        try:
            envelope = strict_json_loads(response.content)
        except ContractError as error:
            raise LiveCorrectnessError(f"live response is not JSON for {method}") from error
        if not isinstance(envelope, dict):
            raise LiveCorrectnessError(f"live response envelope is not an object for {method}")
        if "error" in envelope:
            code = str(envelope.get("error", "unknown"))[:100].casefold()
            if code in {"error_method_not_found", "insufficient_scope", "access_denied"}:
                raise LiveUnavailableError(f"live method unavailable for {method}: {code}")
            raise LiveCorrectnessError(f"live API returned an unexpected error for {method}")
        if "result" not in envelope:
            raise LiveCorrectnessError(f"live response has no result for {method}")
        return envelope

    def call(self, method: str, parameters: dict[str, Any] | None = None) -> Any:
        """Call one REST method and return its result without retaining raw bodies."""
        return self.call_envelope(method, parameters)["result"]

    def preflight(self, *, required_scopes: set[str]) -> LivePreflight:
        """Call scope/app.info and classify missing environment as unavailable."""
        scope_result = self.call("scope")
        if isinstance(scope_result, list):
            scopes = frozenset(str(item) for item in scope_result)
        elif isinstance(scope_result, dict):
            scopes = frozenset(str(key) for key, enabled in scope_result.items() if enabled)
        else:
            raise LiveCorrectnessError("scope result has an unsupported shape")
        missing = sorted(required_scopes - scopes)
        if missing:
            raise LiveUnavailableError(f"required scope unavailable: {','.join(missing)}")
        app = self.call("app.info")
        build: str | None = None
        if isinstance(app, dict):
            for key in ("VERSION", "version", "BUILD", "build"):
                value = app.get(key)
                if isinstance(value, str | int):
                    build = str(value)[:100]
                    break
        return LivePreflight(identity=self.identity, build=build, scopes=scopes)


@dataclass(frozen=True, slots=True)
class DisposableAdapter:
    """Reviewed wire mapping for one exact disposable profile tuple."""

    profile_id: str
    create_method: str
    read_method: str
    delete_method: str
    list_method: str
    id_parameter: str
    result_container: str | None
    marker_field: str = "TITLE"

    def create(self, portal: LivePortal, marker: str) -> str:
        result = portal.call(self.create_method, {"fields": {self.marker_field: marker}})
        if self.result_container is not None:
            if not isinstance(result, dict) or not isinstance(result.get(self.result_container), dict):
                raise LiveCorrectnessError("create result lacks reviewed entity container")
            result = result[self.result_container]
            entity_id = result.get("id") or result.get("ID")
        else:
            entity_id = result
        if isinstance(entity_id, bool) or not isinstance(entity_id, str | int):
            raise LiveCorrectnessError("create result lacks a scalar entity id")
        return str(entity_id)

    def read(self, portal: LivePortal, entity_id: str) -> dict[str, Any] | None:
        try:
            result = portal.call(self.read_method, {self.id_parameter: entity_id})
        except LiveCorrectnessError as error:
            if "not_found" in str(error).casefold() or "not found" in str(error).casefold():
                return None
            raise
        if self.result_container is not None:
            if not isinstance(result, dict):
                raise LiveCorrectnessError("read result lacks reviewed entity container")
            result = result.get(self.result_container)
        if result is None:
            return None
        if not isinstance(result, dict):
            raise LiveCorrectnessError("read result is not an entity object")
        return result

    def delete(self, portal: LivePortal, entity_id: str) -> None:
        portal.call(self.delete_method, {self.id_parameter: entity_id})

    def find_exact_marker(self, portal: LivePortal, marker: str) -> list[str]:  # noqa: C901
        matches: list[str] = []
        start: int | None = None
        seen_starts: set[int | None] = set()
        for _page in range(MAX_MARKER_SCAN_PAGES):
            if start in seen_starts:
                raise LiveCorrectnessError("exact-marker search continuation did not advance")
            seen_starts.add(start)
            parameters: dict[str, Any] = {
                "filter": {self.marker_field: marker},
                "select": ["ID", self.marker_field],
            }
            if start is not None:
                parameters["start"] = start
            envelope = portal.call_envelope(self.list_method, parameters)
            result = envelope["result"]
            if isinstance(result, dict) and len(result) == 1:
                result = next(iter(result.values()))
            if not isinstance(result, list):
                raise LiveCorrectnessError("exact-marker search did not return a list")
            for row in result:
                if not isinstance(row, dict):
                    raise LiveCorrectnessError("exact-marker search returned a malformed row")
                value = row.get(self.marker_field) or row.get(self.marker_field.casefold())
                entity_id = row.get("ID") or row.get("id")
                if value == marker and isinstance(entity_id, str | int) and not isinstance(entity_id, bool):
                    matches.append(str(entity_id))
            continuation = envelope.get("next")
            if continuation is None:
                return matches
            if isinstance(continuation, bool) or not isinstance(continuation, int) or continuation < 0:
                raise LiveCorrectnessError("exact-marker search continuation is malformed")
            start = continuation
        raise LiveCorrectnessError("exact-marker search exceeded the reviewed page ceiling")


ADAPTERS = {
    "tasks-task-v1": DisposableAdapter(
        profile_id="tasks-task-v1",
        create_method="tasks.task.add",
        read_method="tasks.task.get",
        delete_method="tasks.task.delete",
        list_method="tasks.task.list",
        id_parameter="taskId",
        result_container="task",
    ),
    "crm-deal-v1": DisposableAdapter(
        profile_id="crm-deal-v1",
        create_method="crm.deal.add",
        read_method="crm.deal.get",
        delete_method="crm.deal.delete",
        list_method="crm.deal.list",
        id_parameter="id",
        result_container=None,
    ),
}
