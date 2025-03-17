from datetime import datetime
from typing import Self

from pydantic import BaseModel

from b24api.error import ApiResponseError, RetryApiResponseError
from b24api.query import build_query
from b24api.type import ApiTypes


class Request(BaseModel):
    """Common request structure."""

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
    filter: dict[str, ApiTypes] | None = None
    order: dict[str, str] | None = None
    limit: int | None = None
    start: int | None = None


class ListRequest(Request):
    """List request structure."""

    parameters: ListRequestParameters = None


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
    """Time structure of response."""

    start: float
    finish: float
    duration: float
    processing: float
    date_start: datetime
    date_finish: datetime
    operating_reset_at: float
    operating: float


class ResultResponse(BaseModel):
    """API data response."""

    result: ApiTypes
    time: ResponseTime
    total: int | None = None
    next: int | None = None


class BatchResult(BaseModel):
    result: dict[str, ApiTypes]
    result_time: dict[str, ResponseTime]
    result_error: dict[str, ErrorResponse]
    result_total: dict[str, int]
    result_next: dict[str, int]
