from datetime import datetime
from typing import Self

from pydantic import BaseModel

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

    select: list[str]
    filter: dict[str, ApiTypes] = {}
    order: dict[str, str] = {}
    start: int = -1


class ListRequest(Request):
    """API `*.list` request."""

    parameters: ListRequestParameters


class ErrorResponse(BaseModel):
    """API error response."""

    error: str
    error_description: str | None = None

    def raise_error(self, retry_errors: list[str]) -> Self:
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


class BatchResult(BaseModel):
    """API response `result` structure for `batch` method."""

    result: dict[str, ApiTypes]
    result_time: dict[str, ResponseTime]
    result_error: dict[str, ErrorResponse]
    result_total: dict[str, int]
    result_next: dict[str, int]
