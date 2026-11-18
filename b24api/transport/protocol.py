"""Transport decoding with structured-body-before-status error precedence."""

from __future__ import annotations
import json
from collections.abc import Collection, Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from b24api._error_types import ErrorOrigin
from b24api.contracts.evidence import ResponseEvidence
from b24api.contracts.json import (
    MAX_JSON_DEPTH,
    DecodedJsonObject,
    JsonValue,
    nests_deeper_than_supported,
    repeated_names,
)
from b24api.contracts.v3_codes import NON_RETRYABLE_V3_ERROR_CODES, render_code
from b24api.errors import (
    ApiResponseError,
    B24ApiError,
    HTTPGatewayError,
    ProtocolError,
    ValidationIssue,
)
from b24api.redaction import DEFAULT_REDACTOR, Redactor, SafeText

if TYPE_CHECKING:
    from b24api._diagnostics import DiagnosticContext
    from b24api.contracts.request_summary import RequestSummary

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


class RepeatedControlMemberError(ValueError):
    """A JSON body the server wrote that repeats a member name where it decides the answer: unreadable, not foreign."""


def decode_json(text: str, *, allow_constants: bool = True) -> object:
    """Decode a response body, refusing one whose answer cannot be read unambiguously; raises ``ValueError``.

    What any body proves about whether its request ran is read from its top-level members (``result``, ``error``
    and the paging and timing metadata) and a REST 3.0 ``error`` object. A name repeated among them keeps two
    answers where the decoder would see one, so the body is refused. Elsewhere a repeated name keeps its last
    value, as any JSON decoder does, and the decoded object records it (``contracts.json.repeated_names``) for the
    reader that knows the member decides an answer: a classic batch envelope and a result's embedded error. A body
    holding a value more than ``MAX_JSON_DEPTH`` levels below a top-level member, deeper than that member frozen
    may be, is refused as any other malformed body, whatever depth this interpreter's parser could follow, and so,
    unless ``allow_constants``, is ``NaN`` or ``Infinity``. A leading byte order mark is ignored, as RFC 8259 lets
    a parser ignore it.
    """
    text = text.removeprefix("\ufeff")
    try:
        payload = json.loads(
            text,
            object_pairs_hook=_members,
            parse_constant=None if allow_constants else _reject_json_constant,
        )
    except RecursionError as error:
        raise ValueError("JSON nesting exceeds the supported depth") from error
    # A value too deep has the body and MAX_JSON_DEPTH + 1 containers above it: a body with fewer openers is not walked.
    if text.count("[") + text.count("{") > MAX_JSON_DEPTH + 1 and nests_deeper_than_supported(payload):
        raise ValueError("JSON nesting exceeds the supported depth")
    if isinstance(payload, Mapping) and (repeated_names(payload) or repeated_names(payload.get("error"))):
        raise RepeatedControlMemberError("a JSON member name is repeated where it decides the answer")
    return payload


def _members(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    """Build one decoded object, recording the names it repeats."""
    decoded = dict(pairs)
    return decoded if len(decoded) == len(pairs) else DecodedJsonObject(pairs)


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant is forbidden: {value}")


class ProtocolCodec:
    """Decode bounded HTTP error evidence without retaining raw bodies."""

    def __init__(self, *, redactor: Redactor = DEFAULT_REDACTOR) -> None:
        """Initialize instance state."""
        self._redactor = redactor

    @property
    def redactor(self) -> Redactor:
        """Return the redactor, with its registered exact secrets, that renders every decoded error."""
        return self._redactor

    def error_from_http(  # noqa: PLR0913 - the response's status, body, headers and the request's context
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
        # A body the decoder refuses for a repeated member is the server's own answer, whether or not it names an
        # `error`, so it is never a gateway's page (a refusal); its status decides the rest. Any other body without
        # a readable top-level `error` (not JSON, malformed, or JSON without one) outside 2xx is the gateway's.
        repeated = isinstance(malformed, RepeatedControlMemberError)

        if isinstance(parsed, Mapping) and "error" in parsed:
            safe_headers = self._safe_headers(headers or {})
            preview = self._body_preview(body, diagnostics)
            defect, original_code, description, validation, truncated = self._read_structured(parsed, diagnostics)
            if defect is not None:
                return self._protocol_error(
                    defect,
                    status_code=status_code,
                    request_summary=request_summary,
                    headers=safe_headers,
                    body_preview=preview,
                )
            code_is_exact = isinstance(parsed["error"], Mapping)
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

        if status_code >= HTTP_ERROR_MINIMUM and not repeated:
            return self._gateway_error(
                status_code,
                request_summary,
                self._safe_headers(headers or {}),
                self._body_preview(body, diagnostics),
            )

        if malformed is not None:
            safe_headers = self._safe_headers(headers or {})
            preview = self._body_preview(body, diagnostics)
            return self._protocol_error(
                "JSON response repeats a member name where it decides the answer"
                if repeated
                else "Malformed JSON response",
                status_code=status_code,
                request_summary=request_summary,
                headers=safe_headers,
                body_preview=preview,
            )
        return None

    def _read_structured(
        self, parsed: Mapping[str, Any], diagnostics: DiagnosticContext | None
    ) -> tuple[str | None, str | int, str | None, tuple[ValidationIssue, ...], bool]:
        """Read a structured error body: its defect, or its code, description, validation issues and truncation.

        A body that is both a ``result`` and an ``error`` answers the request two ways: neither is its answer, so
        it proves nothing, a refusal least of all.
        """
        if "result" in parsed:
            return "Response carries both a result and an error", "", None, (), False
        original_code = parsed["error"]
        if isinstance(original_code, Mapping):
            try:
                code, description, validation, truncated = self._v3_error(original_code, diagnostics)
            except (TypeError, ValueError):
                return "Malformed V3 error object", "", None, (), False
            return None, code, description, validation, truncated
        if not isinstance(original_code, str | int):
            return "Structured error code must be a string or integer", "", None, (), False
        raw = parsed.get("error_description")
        return None, original_code, None if raw is None else str(raw), (), False

    def _gateway_error(
        self,
        status_code: int,
        request_summary: RequestSummary | None,
        safe_headers: tuple[tuple[str, str], ...],
        preview: str | None,
    ) -> HTTPGatewayError:
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
        truncated = len(issues) > _MAX_VALIDATION_ITEMS or len(code) > _MAX_VALIDATION_TEXT

        def render(text: str) -> str:
            # Aliasing and secret redaction see the whole text; only the safe result is bounded, so a cut
            # can never split a request field name out of reach of its alias.
            nonlocal truncated
            rendered = self._redactor.render_text(text, context=diagnostics)
            truncated |= len(text) > _MAX_VALIDATION_TEXT or len(rendered) > _MAX_VALIDATION_TEXT
            return rendered[:_MAX_VALIDATION_TEXT]

        for issue in issues[:_MAX_VALIDATION_ITEMS]:
            if not isinstance(issue, Mapping):
                raise TypeError("V3 validation item must be an object")
            # The server omits ``field`` for a refusal that names no field: absent stays None, never invented.
            # A present key, including an explicit null, must still be a string.
            field = issue.get("field")
            detail = issue.get("message")
            if ("field" in issue and not isinstance(field, str)) or not isinstance(detail, str):
                raise TypeError("V3 validation field, when present, and message must be strings")
            bounded.append(ValidationIssue(None if field is None else render(field), render(detail)))
        if len(code) > _MAX_VALIDATION_TEXT:
            # A code is cut only after redaction, like the texts above: cutting first would turn a known secret
            # into a prefix that exact-secret redaction no longer recognizes.
            code = render_code(code, redactor=self._redactor, context=diagnostics, v3_envelope=True)[
                :_MAX_VALIDATION_TEXT
            ]
        return code, SafeText(render(message)), tuple(bounded), truncated

    @staticmethod
    def _parse_body(body: bytes | str | Mapping[str, Any] | None) -> tuple[object, ValueError | None]:
        """Return the decoded body, or the failure that made it unreadable."""
        if isinstance(body, Mapping):
            return body, None
        if body is None or body in {b"", ""}:
            return None, None
        text = body.decode("utf-8", errors="replace") if isinstance(body, bytes) else body
        try:
            return decode_json(text), None
        except ValueError as error:
            return None, error

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
