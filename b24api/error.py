import httpx


class RetryHTTPStatusError(httpx.HTTPStatusError):
    """HTTP error that may be retried."""


class ApiResponseError(Exception):
    """API error with description."""

    def __init__(
        self,
        *,
        code: str,
        description: str | None,
    ) -> None:
        message = f"API error [{code}]: {description}"
        super().__init__(message)


class RetryApiResponseError(ApiResponseError):
    """API error that may be retried."""
