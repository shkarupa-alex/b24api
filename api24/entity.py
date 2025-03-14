from datetime import datetime
from typing import Self

from pydantic import BaseModel, model_validator

from api24.error import ApiResponseError, RetryApiResponseError
from api24.query import build_query
from api24.type import BaseType


class Request(BaseModel):
    """Common request structure."""

    method: str
    parameters: dict[str, BaseType] = {}

    @property
    def flat(self) -> str:
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
    filter: dict[str, BaseType] | None = None
    order: dict[str, str] | None = None
    limit: int | None = None
    start: int | None = None


class ListRequest(Request):
    """List request structure."""

    parameters: ListRequestParameters = None


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


class ErrorResponse(BaseModel):
    """API error response."""

    error: str
    error_description: str | None = None

    @model_validator(mode="after")
    def raise_after_create(self) -> Self:
        if self.error in {"query_limit_exceeded", "operation_time_limit"}:
            raise RetryApiResponseError(
                code=self.error,
                description=self.error_description,
            )
        raise ApiResponseError(
            code=self.error,
            description=self.error_description,
        )


class ResultResponse(BaseModel):
    """API data response."""

    result: BaseType
    time: ResponseTime
    total: int | None = None
    next: int | None = None


class ResultBatch(BaseModel):
    result: dict[str, BaseType]
    result_time: dict[str, ResponseTime]
    result_error: dict[str, ErrorResponse]
    result_total: dict[str, int]
    result_next: dict[str, int]
