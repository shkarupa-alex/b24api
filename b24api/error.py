import httpx


class RetryHTTPStatusError(httpx.HTTPStatusError):
    """HTTP error that may be retried."""


class ApiResponseError(Exception):
    """API error with description."""

    def __init__(self, *, code: str, description: str | None, request: str) -> None:
        if code and description:
            message = f"API error [{code}]: {description} for request {request}"
        elif code:
            message = f"API error [{code}] for request {request}"
        else:
            message = f"API error: {description} for request {request}"
        super().__init__(message)


class RetryApiResponseError(ApiResponseError):
    """API error that may be retried."""
