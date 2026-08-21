# ruff: noqa: ANN401
"""Opt-in live transport isolated from the distributable ``b24api`` package."""

from __future__ import annotations
import os
import uuid
from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Any, Self, cast
from urllib.parse import urljoin

import httpx

from .contracts import ContractError, PortalIdentity, parse_fingerprint_key, portal_identity, strict_json_loads

if TYPE_CHECKING:
    from collections.abc import Callable

HTTP_OK = 200
HTTP_STATUS_MINIMUM = 100
HTTP_STATUS_MAXIMUM = 599
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_MARKER_SCAN_PAGES = 1_000
MAX_EXACT_MARKER_MATCHES = 2
MAX_BUILD_LENGTH = 100
UNAVAILABLE_API_CODES = frozenset({"error_method_not_found", "insufficient_scope", "access_denied"})
CLASSIFIED_API_CODES = frozenset({"error_not_found"})
UNKNOWN_API_CODE = "unexpected_api_error"
_WEBHOOK_VAULT: dict[str, str] = {}


def _store_webhook(webhook_url: str) -> str:
    handle = uuid.uuid4().hex
    _WEBHOOK_VAULT[handle] = webhook_url
    return handle


def _webhook_for(handle: str) -> str:
    try:
        return _WEBHOOK_VAULT[handle]
    except KeyError as error:
        raise LiveUnavailableError("live credential is unavailable") from error


class LiveUnavailableError(RuntimeError):
    """A portal, scope, tariff, or endpoint is unavailable for live evidence."""


class LiveCorrectnessError(RuntimeError):
    """A live response contradicts the reviewed disposable-entity contract."""


class LiveApiError(LiveCorrectnessError):
    """A typed API error whose safe code is available without rendering portal text."""

    def __init__(self, *, method: str, code: str = UNKNOWN_API_CODE) -> None:
        """Initialize instance state."""
        self.code = code if code in CLASSIFIED_API_CODES else UNKNOWN_API_CODE
        super().__init__(f"live API returned an unexpected error for {method}")


def _redacted_live_errors[**P, T](function: Callable[P, T]) -> Callable[P, T]:
    """Discard hostile inner frames before an error crosses a live boundary."""

    @wraps(function)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
        failure: LiveCorrectnessError | LiveUnavailableError | None = None
        try:
            return function(*args, **kwargs)
        except LiveApiError as error:
            failure = LiveApiError(method="redacted live operation", code=error.code)
        except LiveUnavailableError as error:
            failure = LiveUnavailableError(str(error))
        except LiveCorrectnessError as error:
            failure = LiveCorrectnessError(str(error))
        if failure is not None:
            raise failure
        raise RuntimeError("unreachable live error boundary")

    return wrapped


@dataclass(frozen=True, slots=True)
class LivePreflight:
    """Redacted portal facts retained after scope/app preflight."""

    identity: PortalIdentity
    build: str | None
    scopes: frozenset[str]


def _bounded_response_payload(response: httpx.Response, *, method: str) -> bytes:
    """Consume a streamed response without crossing the reviewed memory ceiling."""
    content_length = response.headers.get("content-length")
    invalid_content_length = False
    declared: int | None = None
    if content_length is not None:
        try:
            declared = int(content_length)
        except ValueError:
            invalid_content_length = True
    content_length = None
    if invalid_content_length:
        response = None  # type: ignore[assignment]
        raise LiveCorrectnessError(f"live response has invalid content length for {method}")
    if declared is not None and declared > MAX_RESPONSE_BYTES:
        response = None  # type: ignore[assignment]
        raise LiveCorrectnessError(f"live response exceeds the reviewed byte ceiling for {method}")
    payload = bytearray()
    received = 0
    for chunk in response.iter_bytes():
        received += len(chunk)
        if received > MAX_RESPONSE_BYTES:
            del chunk
            payload.clear()
            response = None  # type: ignore[assignment]
            raise LiveCorrectnessError(f"live response exceeds the reviewed byte ceiling for {method}")
        payload.extend(chunk)
    return bytes(payload)


class LivePortal:
    """Minimal synchronous webhook session whose URL never enters artifacts or errors."""

    def __init__(self, *, role: str, timeout: float = 30.0) -> None:
        """Initialize instance state."""
        encoded_key = os.environ.get("BITRIX24_EVIDENCE_FINGERPRINT_KEY")
        if encoded_key is None:
            raise ContractError("BITRIX24_EVIDENCE_FINGERPRINT_KEY is required for --live")
        # Key strength is rejected before the credential is even read.
        invalid_fingerprint_key = False
        try:
            parse_fingerprint_key(encoded_key)
        except ContractError:
            invalid_fingerprint_key = True
        if invalid_fingerprint_key:
            encoded_key = None
            raise ContractError("BITRIX24_EVIDENCE_FINGERPRINT_KEY is invalid")
        webhook_url = os.environ.get("BITRIX24_API_WEBHOOK_URL")
        if webhook_url is None:
            raise ContractError("BITRIX24_API_WEBHOOK_URL is required for --live")
        identity: PortalIdentity | None = None
        normalized_webhook: str | None = None
        invalid_configuration = False
        try:
            identity = portal_identity(webhook_url, role=role, fingerprint_key=encoded_key)
            normalized_webhook = webhook_url.rstrip("/") + "/"
        except (ContractError, TypeError, ValueError):
            invalid_configuration = True
        encoded_key = None
        webhook_url = None
        if invalid_configuration:
            raise ContractError("live credential configuration is invalid")
        resolved_client: httpx.Client | None = None
        client_initialization_failed = False
        try:
            resolved_client = httpx.Client(timeout=timeout, follow_redirects=False)
        except Exception:  # noqa: BLE001 - sanitize environment/proxy constructor failures
            client_initialization_failed = True
        if client_initialization_failed:
            normalized_webhook = None
            raise LiveUnavailableError("live HTTP client configuration is invalid")
        self.identity = cast("PortalIdentity", identity)
        self._webhook_handle = _store_webhook(cast("str", normalized_webhook))
        self._client = cast("httpx.Client", resolved_client)
        self.attempts = 0

    def close(self) -> None:
        """Close owned resources."""
        try:
            self._client.close()
        finally:
            _WEBHOOK_VAULT.pop(self._webhook_handle, None)

    def __enter__(self) -> Self:
        """Enter the context."""
        return self

    def __exit__(self, *_args: object) -> None:
        """Exit the context."""
        self.close()

    def call_envelope(  # noqa: C901, PLR0912, PLR0915 - one boundary owns all raw response state
        self,
        method: str,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Call one REST method and return a strictly decoded, bounded envelope."""
        self.attempts += 1
        payload: bytes | None = None
        status_code: int | None = None
        transport_failed = False
        decoding_failed = False
        bounded_failure: str | None = None
        response: httpx.Response | None = None
        try:
            with self._client.stream(
                "POST",
                urljoin(_webhook_for(self._webhook_handle), method),
                json=parameters or {},
            ) as response:
                status_code = response.status_code
                try:
                    payload = _bounded_response_payload(response, method=method)
                except LiveCorrectnessError as error:
                    bounded_failure = str(error)
        except httpx.DecodingError:
            decoding_failed = True
        except httpx.HTTPError:
            transport_failed = True
        response = None
        if status_code is not None and not HTTP_STATUS_MINIMUM <= status_code <= HTTP_STATUS_MAXIMUM:
            payload = None
            raise LiveCorrectnessError(f"live response has an invalid HTTP status for {method}")
        if decoding_failed:
            raise LiveCorrectnessError(f"live response decoding failed for {method}")
        if transport_failed:
            raise LiveUnavailableError(f"live transport unavailable for {method}")
        if bounded_failure is not None:
            raise LiveCorrectnessError(bounded_failure)
        if payload is None:
            raise LiveCorrectnessError(f"live response payload is unavailable for {method}")
        parse_failed = False
        try:
            envelope = strict_json_loads(payload)
        except ContractError:
            parse_failed = True
            envelope = None
        payload = None
        if parse_failed:
            if status_code != HTTP_OK:
                raise LiveUnavailableError(f"live HTTP status {status_code} for {method}")
            raise LiveCorrectnessError(f"live response is not JSON for {method}")
        if not isinstance(envelope, dict):
            envelope = None
            if status_code != HTTP_OK:
                raise LiveUnavailableError(f"live HTTP status {status_code} for {method}")
            raise LiveCorrectnessError(f"live response envelope is not an object for {method}")
        if "error" in envelope:
            raw_code = envelope.get("error")
            unavailable_code = next(
                (
                    candidate
                    for candidate in UNAVAILABLE_API_CODES
                    if isinstance(raw_code, str) and raw_code.casefold() == candidate
                ),
                None,
            )
            classified_code = next(
                (
                    candidate
                    for candidate in CLASSIFIED_API_CODES
                    if isinstance(raw_code, str) and raw_code.casefold() == candidate
                ),
                UNKNOWN_API_CODE,
            )
            raw_code = None
            envelope = None
            if unavailable_code is not None:
                raise LiveUnavailableError(f"live method unavailable for {method}: {unavailable_code}")
            raise LiveApiError(method=method, code=classified_code)
        if status_code != HTTP_OK:
            envelope = None
            raise LiveUnavailableError(f"live HTTP status {status_code} for {method}")
        if "result" not in envelope:
            envelope = None
            raise LiveCorrectnessError(f"live response has no result for {method}")
        return envelope

    def call(self, method: str, parameters: dict[str, Any] | None = None) -> Any:
        """Call one REST method and return its result without retaining raw bodies."""
        return self.call_envelope(method, parameters)["result"]

    @_redacted_live_errors
    def preflight(self, *, required_scopes: set[str]) -> LivePreflight:  # noqa: C901 - hostile wire union
        """Call scope/app.info and classify missing environment as unavailable."""
        scope_result = self.call("scope")
        if isinstance(scope_result, list):
            if any(not isinstance(item, str) or not item for item in scope_result):
                raise LiveCorrectnessError("scope result contains an invalid scope name")
            scopes = frozenset(scope_result)
        elif isinstance(scope_result, dict):
            if any(
                not isinstance(key, str) or not key or type(enabled) is not bool
                for key, enabled in scope_result.items()
            ):
                raise LiveCorrectnessError("scope result contains invalid capability values")
            scopes = frozenset(key for key, enabled in scope_result.items() if enabled)
        else:
            raise LiveCorrectnessError("scope result has an unsupported shape")
        missing = sorted(required_scopes - scopes)
        if missing:
            raise LiveUnavailableError(f"required scope unavailable: {','.join(missing)}")
        app = self.call("app.info")
        build: str | None = None
        if isinstance(app, dict):
            for key in ("VERSION", "version", "BUILD", "build"):
                if key not in app:
                    continue
                value = app.get(key)
                if isinstance(value, str) and value.strip() and len(value) <= MAX_BUILD_LENGTH:
                    build = value
                    break
                if isinstance(value, int) and not isinstance(value, bool):
                    rendered = str(value)
                    if len(rendered) > MAX_BUILD_LENGTH:
                        raise LiveCorrectnessError("app.info build exceeds the semantic length ceiling")
                    build = rendered
                    break
                raise LiveCorrectnessError("app.info build has an invalid semantic type")
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
    not_found_codes: frozenset[str] = frozenset({"error_not_found"})

    @_redacted_live_errors
    def create(self, portal: LivePortal, marker: str) -> str:
        """Create one disposable portal entity."""
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

    @_redacted_live_errors
    def read(self, portal: LivePortal, entity_id: str) -> dict[str, Any] | None:
        """Read one portal entity by identifier."""
        try:
            result = portal.call(self.read_method, {self.id_parameter: entity_id})
        except LiveApiError as error:
            if error.code in self.not_found_codes:
                return None
            raise
        if self.result_container is not None:
            if not isinstance(result, dict):
                raise LiveCorrectnessError("read result lacks reviewed entity container")
            result = result.get(self.result_container)
        if result is None:
            raise LiveCorrectnessError("successful point read returned no entity")
        if not isinstance(result, dict):
            raise LiveCorrectnessError("read result is not an entity object")
        return result

    @_redacted_live_errors
    def delete(self, portal: LivePortal, entity_id: str) -> None:
        """Delete one owned portal entity."""
        portal.call(self.delete_method, {self.id_parameter: entity_id})

    @_redacted_live_errors
    def find_exact_marker(self, portal: LivePortal, marker: str) -> list[str]:  # noqa: C901
        """Find entities matching the exact ownership marker."""
        matches: set[str] = set()
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
                    matches.add(str(entity_id))
                    if len(matches) >= MAX_EXACT_MARKER_MATCHES:
                        return sorted(matches)
            continuation = envelope.get("next")
            if continuation is None:
                return sorted(matches)
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
