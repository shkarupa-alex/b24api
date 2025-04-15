from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, field_validator

from b24api.error import ApiResponseError, RetryApiResponseError
from b24api.query import build_query
from b24api.type import ApiTypes


class Request(BaseModel):
    """API request."""

    method: str
    parameters: dict[str, ApiTypes] = {}

    @property
    def query(self) -> str:
        if not self.parameters:
            return self.method

        parameters = self.parameters
        if isinstance(self.parameters, BaseModel):
            parameters = self.parameters.model_dump()
        query = build_query(parameters)

        return f"{self.method}?{query}"


class ListRequestParameters(BaseModel):
    """Parameters of `*.list` requests."""

    select: list[str] = []
    filter: dict[str, ApiTypes] = {}
    order: dict[str, str] = {}
    start: int | None = None


class ListRequest(Request):
    """API `*.list` request."""

    parameters: ListRequestParameters


class ErrorResponse(BaseModel):
    """API error response."""

    error: str | int
    error_description: str

    @field_validator("error")
    @classmethod
    def error_to_lower_str(cls, value: Any) -> Any:  # noqa: ANN401
        if not isinstance(value, int):
            value = str(value)
        if isinstance(value, str):
            value = value.lower()
        return value

    def raise_error(self, retry_errors: list[str]) -> None:
        if self.error in retry_errors:
            raise RetryApiResponseError(
                code=self.error,
                description=self.error_description,
            )
        raise ApiResponseError(
            code=self.error,
            description=self.error_description,
        )


class ResponseTime(BaseModel):
    """API Response `time` structure."""

    start: float
    finish: float
    duration: float
    processing: float
    date_start: datetime
    date_finish: datetime
    operating_reset_at: float | None = None
    operating: float | None = None


class Response(BaseModel):
    """API response."""

    result: ApiTypes
    time: ResponseTime
    total: int | None = None
    next: int | None = None


def _php_dict(value: Any) -> Any:  # noqa: ANN401
    if isinstance(value, list) and not value:
        return {}
    return value


class BatchResult(BaseModel):
    """API response `result` structure for `batch` method."""

    result: Annotated[dict[str, ApiTypes], BeforeValidator(_php_dict)]
    result_time: Annotated[dict[str, ResponseTime], BeforeValidator(_php_dict)]
    result_error: Annotated[dict[str, ErrorResponse], BeforeValidator(_php_dict)]
    result_total: Annotated[dict[str, int], BeforeValidator(_php_dict)]
    result_next: Annotated[dict[str, int], BeforeValidator(_php_dict)]
