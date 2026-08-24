"""Compatibility values and aliases for the canonical immutable models."""

from __future__ import annotations
import logging
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from b24api.error import ApiResponseError, RetryApiResponseError
from b24api.models import Request, Response, ResponseTime
from b24api.query import build_query
from b24api.type import ApiTypes  # noqa: TC001 - Pydantic resolves this field type at runtime

if TYPE_CHECKING:
    from b24api.settings import Settings


class LegacyRequest(BaseModel):
    """Pydantic input retained only for ListRequest compatibility."""

    method: str
    parameters: dict[str, ApiTypes] = {}

    @property
    def query(self) -> str:
        """Serialize this compatibility request using the committed PHP shape."""
        if not self.parameters:
            return self.method
        parameters: object = self.parameters
        if isinstance(parameters, BaseModel):
            parameters = parameters.model_dump(exclude_defaults=True)
        if not isinstance(parameters, dict):
            raise TypeError("legacy request parameters must serialize to a mapping")
        query = build_query(parameters)
        return self.method if not query else f"{self.method}?{query}"


class ListRequestParameters(BaseModel):
    """Parameters of legacy list request inputs."""

    model_config = ConfigDict(extra="allow")

    select: list[str] = []
    filter: dict[str, ApiTypes] = {}
    order: dict[str, str] = {}
    start: int | None = None


class ListRequest(LegacyRequest):
    """Import-compatible validated input translated by the W7 facade."""

    parameters: ListRequestParameters = Field(default_factory=ListRequestParameters)  # type: ignore[assignment]


class ErrorResponse(BaseModel):
    """Import-compatible structured REST error value."""

    error: str
    error_description: str

    @field_validator("error", mode="before")
    @classmethod
    def error_to_lower_str(cls, value: int | str) -> str:
        """Preserve the committed lowercase string comparison form."""
        return str(value).lower()

    def raise_error(self, request: Request | LegacyRequest, settings: Settings) -> None:
        """Raise the compatibility error alias without exposing parameters."""
        logger = logging.getLogger(settings.logger_name)
        error_cls: type[ApiResponseError]
        if self.error in settings.retry_errors:
            logger.debug("Request method: %s", request.method)
            error_cls = RetryApiResponseError
        else:
            logger.warning("Request method: %s", request.method)
            error_cls = ApiResponseError
        raise error_cls(
            code=self.error,
            description=self.error_description,
            request=request,
        )


class BatchResult(BaseModel):
    """Import-compatible decoded legacy batch envelope."""

    result: dict[str, ApiTypes]
    result_time: dict[str, ResponseTime]
    result_error: dict[str, ErrorResponse]
    result_total: dict[str, int]
    result_next: dict[str, int]

    @field_validator("result", "result_time", "result_error", "result_total", "result_next", mode="before")
    @classmethod
    def php_dict(cls, value: Any) -> Any:  # noqa: ANN401
        """Accept the observed Bitrix/PHP empty-array map representation."""
        if isinstance(value, list) and not value:
            return {}
        return value


__all__ = [
    "BatchResult",
    "ErrorResponse",
    "LegacyRequest",
    "ListRequest",
    "ListRequestParameters",
    "Request",
    "Response",
    "ResponseTime",
]
