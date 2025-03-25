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
        if code and description:
            message = f"API error [{code}]: {description}"
        elif code:
            message = f"API error [{code}]"
        else:
            message = f"API error: {description}"
        super().__init__(message)


class RetryApiResponseError(ApiResponseError):
    """API error that may be retried."""
