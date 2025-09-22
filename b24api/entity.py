import logging
from datetime import datetime
from typing import Any

from pydantic import BaseModel, field_validator

from b24api.error import ApiResponseError, RetryApiResponseError
from b24api.query import build_query
from b24api.settings import Settings
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

    def raise_error(self, request: Request, settings: Settings) -> None:
        logger = logging.getLogger(settings.logger_name)

        if self.error in settings.retry_errors:
            logger.debug("Request: %s", request)
            error_cls = RetryApiResponseError
        else:
            logger.warning("Request: %s", request)
            error_cls = ApiResponseError

        raise error_cls(
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

    @property
    def list_result(self) -> list[ApiTypes]:
        """Fix `list` methods result to `list of items` structure.

        There are two kinds of what `list` method `result` may contain:
        - a list of items (e.g. `department-get` and `disk.folder.getchildren`),
        - a dictionary with single item that contains the desired list of items
            (e.g. `tasks` in `tasks.task.list`).
        """
        if not isinstance(self.result, list | dict):
            raise TypeError(f"Expecting `result` to be a `list` or a `dict`. Got: {self.result}")

        if not self.result:
            return []

        if isinstance(self.result, list):
            return self.result

        if len(self.result) != 1:
            raise TypeError(
                f"If `result` is a `dict`, expecting single item. Got: {self.result}",
            )

        key = next(iter(self.result))
        value = self.result[key]

        if not isinstance(value, list):
            raise TypeError(f"If `result` is a `dict`, expecting single item to be a `list`. Got: {self.result}")

        return value


class BatchResult(BaseModel):
    """API response `result` structure for `batch` method."""

    result: dict[str, ApiTypes]
    result_time: dict[str, ResponseTime]
    result_error: dict[str, ErrorResponse]
    result_total: dict[str, int]
    result_next: dict[str, int]

    @field_validator("result", "result_time", "result_error", "result_total", "result_next", mode="before")
    @classmethod
    def php_dict(cls, value: Any) -> Any:  # noqa: ANN401
        if isinstance(value, list) and not value:
            return {}
        return value
