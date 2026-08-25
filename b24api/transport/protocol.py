"""Transport decoding with structured-body-before-status error precedence."""

from __future__ import annotations
import json
from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any

from b24api.contracts.response import ResponseEvidence
from b24api.errors import (
    ApiResponseError,
    B24ApiError,
    ErrorOrigin,
    HTTPGatewayError,
    ProtocolError,
)
from b24api.redaction import DEFAULT_REDACTOR, Redactor

if TYPE_CHECKING:
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


class ProtocolCodec:
    """Decode bounded HTTP error evidence without retaining raw bodies."""

    def __init__(self, *, redactor: Redactor = DEFAULT_REDACTOR) -> None:
        """Initialize instance state."""
        self._redactor = redactor

    def error_from_http(
        self,
        *,
        status_code: int,
        body: bytes | str | Mapping[str, Any] | None,
        request_summary: RequestSummary | None = None,
        headers: Mapping[str, str] | None = None,
        retry_codes: Collection[str] = (),
    ) -> B24ApiError | None:
        """Return a structured error before considering generic HTTP status."""
        parsed, malformed = self._parse_body(body)

        if isinstance(parsed, Mapping) and "error" in parsed:
            safe_headers = self._safe_headers(headers or {})
            preview = self._body_preview(body)
            original_code = parsed["error"]
            if not isinstance(original_code, str | int):
                return self._protocol_error(
                    "Structured error code must be a string or integer",
                    status_code=status_code,
                    request_summary=request_summary,
                    headers=safe_headers,
                    body_preview=preview,
                )
            raw_description = parsed.get("error_description")
            description = str(raw_description) if raw_description is not None else None
            normalized = str(original_code).strip().casefold()
            normalized_retry_codes = {code.casefold() for code in retry_codes}
            return ApiResponseError(
                code=original_code,
                description=description,
                request_summary=request_summary,
                http_status=status_code,
                headers=dict(safe_headers),
                body_preview=preview,
                retryable=normalized in normalized_retry_codes,
                redactor=self._redactor,
            )

        if status_code >= HTTP_ERROR_MINIMUM:
            safe_headers = self._safe_headers(headers or {})
            preview = self._body_preview(body)
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
            preview = self._body_preview(body)
            return self._protocol_error(
                "Malformed JSON response",
                status_code=status_code,
                request_summary=request_summary,
                headers=safe_headers,
                body_preview=preview,
            )
        return None

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

    def _body_preview(self, body: bytes | str | Mapping[str, Any] | None) -> str | None:
        if isinstance(body, Mapping):
            encoded = json.dumps(body, ensure_ascii=False, default=str)
            return self._redactor.safe_preview(encoded)
        return self._redactor.safe_preview(body)

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
