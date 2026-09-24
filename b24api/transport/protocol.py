"""Transport decoding with structured-body-before-status error precedence."""

from __future__ import annotations
import json
from collections.abc import Collection, Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from b24api._error_types import ErrorOrigin
from b24api.contracts.response import ResponseEvidence
from b24api.contracts.v3_codes import NON_RETRYABLE_V3_ERROR_CODES
from b24api.errors import (
    ApiResponseError,
    B24ApiError,
    HTTPGatewayError,
    ProtocolError,
    ValidationIssue,
)
from b24api.redaction import DEFAULT_REDACTOR, Redactor

if TYPE_CHECKING:
    from b24api._diagnostics import DiagnosticContext
    from b24api.contracts.request import RequestSummary

_SAFE_HEADER_NAMES = frozenset(
    {
        "content-type",
        "retry-after",
        "x-request-id",
        "x-bitrix-ratelimit-limit",
        "x-bitrix-ratelimit-remaining",
        "x-bitrix-ratelimit-reset",
    },
)
HTTP_ERROR_MINIMUM = 400
_MAX_VALIDATION_ITEMS = 32
_MAX_VALIDATION_TEXT = 256
_MAX_V3_SAFE_BYTES = 8 * 1024


class ProtocolCodec:
    """Decode bounded HTTP error evidence without retaining raw bodies."""

    def __init__(self, *, redactor: Redactor = DEFAULT_REDACTOR) -> None:
        """Initialize instance state."""
        self._redactor = redactor

    @property
    def redactor(self) -> Redactor:
        """Return the redactor, with its registered exact secrets, that renders every decoded error."""
        return self._redactor

    def error_from_http(  # noqa: PLR0911, PLR0913 - distinct structured, gateway, and malformed response exits
        self,
        *,
        status_code: int,
        body: bytes | str | Mapping[str, Any] | None,
        request_summary: RequestSummary | None = None,
        headers: Mapping[str, str] | None = None,
        retry_codes: Collection[str] = (),
        diagnostics: DiagnosticContext | None = None,
    ) -> B24ApiError | None:
        """Return a structured error before generic HTTP status; ``diagnostics`` aliases the request's fields."""
        parsed, malformed = self._parse_body(body)

        if isinstance(parsed, Mapping) and "error" in parsed:
            safe_headers = self._safe_headers(headers or {})
            preview = self._body_preview(body, diagnostics)
            original_code = parsed["error"]
            description: str | None
            validation: tuple[ValidationIssue, ...] = ()
            truncated = False
            code_is_exact = isinstance(original_code, Mapping)
            if isinstance(original_code, Mapping):
                try:
                    original_code, description, validation, truncated = self._v3_error(original_code, diagnostics)
                except (TypeError, ValueError):
                    return self._protocol_error(
                        "Malformed V3 error object",
                        status_code=status_code,
                        request_summary=request_summary,
                        headers=safe_headers,
                        body_preview=preview,
                    )
            else:
                raw = parsed.get("error_description")
                description = None if raw is None else str(raw)
            if not isinstance(original_code, str | int):
                return self._protocol_error(
                    "Structured error code must be a string or integer",
                    status_code=status_code,
                    request_summary=request_summary,
                    headers=safe_headers,
                    body_preview=preview,
                )
            retryable = str(original_code).strip().casefold() in {code.casefold() for code in retry_codes} and not (
                code_is_exact and original_code in NON_RETRYABLE_V3_ERROR_CODES
            )
            if code_is_exact:
                return self._bounded_v3_error(
                    code=str(original_code),
                    description=description,
                    validation=validation,
                    truncated=truncated,
                    request_summary=request_summary,
                    status_code=status_code,
                    headers=dict(safe_headers),
                    preview=preview,
                    retryable=retryable,
                    diagnostics=diagnostics,
                )
            return ApiResponseError(
                code=original_code,
                description=description,
                request_summary=request_summary,
                http_status=status_code,
                headers=dict(safe_headers),
                body_preview=preview,
                validation=validation,
                truncated=truncated,
                retryable=retryable,
                redactor=self._redactor,
                diagnostics=diagnostics,
            )

        if status_code >= HTTP_ERROR_MINIMUM:
            safe_headers = self._safe_headers(headers or {})
            preview = self._body_preview(body, diagnostics)
            evidence = ResponseEvidence(
                http_status=status_code,
                request_id=dict(safe_headers).get("x-request-id"),
                headers=safe_headers,
                body_preview=preview,
            )
            return HTTPGatewayError(
                f"HTTP gateway error {status_code}",
                origin=ErrorOrigin.HTTP_GATEWAY,
                request_summary=request_summary,
                evidence=evidence,
                redactor=self._redactor,
            )

        if malformed:
            safe_headers = self._safe_headers(headers or {})
            preview = self._body_preview(body, diagnostics)
            return self._protocol_error(
                "Malformed JSON response",
                status_code=status_code,
                request_summary=request_summary,
                headers=safe_headers,
                body_preview=preview,
            )
        return None

    def _bounded_v3_error(  # noqa: PLR0913
        self,
        *,
        code: str,
        description: str | None,
        validation: tuple[ValidationIssue, ...],
        truncated: bool,
        request_summary: RequestSummary | None,
        status_code: int,
        headers: dict[str, str],
        preview: str | None,
        retryable: bool,
        diagnostics: DiagnosticContext | None,
    ) -> ApiResponseError:
        """Bound the full redacted serialization, including contextual evidence."""
        while True:
            error = ApiResponseError(
                code=code,
                description=description,
                validation=validation,
                truncated=truncated,
                code_is_exact=True,
                request_summary=request_summary,
                http_status=status_code,
                headers=headers,
                body_preview=preview,
                retryable=retryable,
                redactor=self._redactor,
                diagnostics=diagnostics,
            )
            size = len(json.dumps(error.to_safe_dict(), ensure_ascii=False).encode("utf-8"))
            if size <= _MAX_V3_SAFE_BYTES:
                return error
            truncated = True
            if validation:
                validation = validation[:-1]
            elif request_summary is not None and request_summary.parameter_keys:
                request_summary = replace(
                    request_summary,
                    parameter_keys=request_summary.parameter_keys[: len(request_summary.parameter_keys) // 2],
                )
            elif preview is not None:
                preview = None
            elif headers:
                headers = {}
            else:
                raise AssertionError("bounded V3 code and message exceeded the safe serialization limit")

    def _v3_error(
        self,
        value: Mapping[str, Any],
        diagnostics: DiagnosticContext | None,
    ) -> tuple[str, str, tuple[ValidationIssue, ...], bool]:
        code = value.get("code")
        message = value.get("message")
        issues = value.get("validation", [])
        if not isinstance(code, str) or not code.strip() or not isinstance(message, str):
            raise ValueError("V3 code and message must be non-empty strings")
        if not isinstance(issues, list):
            raise TypeError("V3 validation must be a list")
        bounded: list[ValidationIssue] = []
        truncated = (
            len(issues) > _MAX_VALIDATION_ITEMS
            or len(message) > _MAX_VALIDATION_TEXT
            or len(code) > _MAX_VALIDATION_TEXT
        )
        for issue in issues[:_MAX_VALIDATION_ITEMS]:
            if not isinstance(issue, Mapping):
                raise TypeError("V3 validation item must be an object")
            field = issue.get("field")
            detail = issue.get("message")
            if not isinstance(field, str) or not isinstance(detail, str):
                raise TypeError("V3 validation field and message must be strings")
            truncated |= len(field) > _MAX_VALIDATION_TEXT or len(detail) > _MAX_VALIDATION_TEXT
            bounded.append(
                ValidationIssue(
                    self._redactor.render_text(field[:_MAX_VALIDATION_TEXT], context=diagnostics),
                    self._redactor.render_text(detail[:_MAX_VALIDATION_TEXT], context=diagnostics),
                )
            )
        return code[:_MAX_VALIDATION_TEXT], message[:_MAX_VALIDATION_TEXT], tuple(bounded), truncated

    @staticmethod
    def _parse_body(body: bytes | str | Mapping[str, Any] | None) -> tuple[object, bool]:
        if body is None:
            return None, False
        if isinstance(body, Mapping):
            return body, False
        if body in {b"", ""}:
            return None, False
        text = body.decode("utf-8", errors="replace") if isinstance(body, bytes) else body
        try:
            return json.loads(text), False
        except (json.JSONDecodeError, ValueError):
            return None, True

    def _safe_headers(self, headers: Mapping[str, str]) -> tuple[tuple[str, str], ...]:
        return tuple(
            sorted(
                (name.casefold(), self._redactor.redact_text(value))
                for name, value in headers.items()
                if name.casefold() in _SAFE_HEADER_NAMES
            ),
        )

    def _body_preview(
        self,
        body: bytes | str | Mapping[str, Any] | None,
        diagnostics: DiagnosticContext | None,
    ) -> str | None:
        if isinstance(body, Mapping):
            bounded_redactor = replace(self._redactor, max_depth=min(self._redactor.max_depth, 4))
            encoded = json.dumps(bounded_redactor.redact(body, context=diagnostics), ensure_ascii=False, default=str)
            # The context was applied once above; rendering aliases again would re-alias their own text.
            return self._redactor.safe_preview(encoded)
        return self._redactor.safe_preview(body, context=diagnostics)

    @staticmethod
    def _protocol_error(
        message: str,
        *,
        status_code: int,
        request_summary: RequestSummary | None,
        headers: tuple[tuple[str, str], ...],
        body_preview: str | None,
    ) -> ProtocolError:
        return ProtocolError(
            message,
            origin=ErrorOrigin.PROTOCOL,
            request_summary=request_summary,
            evidence=ResponseEvidence(
                http_status=status_code,
                request_id=dict(headers).get("x-request-id"),
                headers=headers,
                body_preview=body_preview,
            ),
        )
