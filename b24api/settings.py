"""Support settings."""

from typing import Annotated, Any

from fast_depends import Depends
from httpx import codes
from pydantic import Field, HttpUrl, field_serializer
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Environment-backed client settings and optional verified portal context."""

    webhook_url: HttpUrl = Field(repr=False)

    logger_name: str = "b24api"

    http_timeout: int = 30

    retry_statuses: list[int] = [
        codes.LOCKED,
        codes.TOO_EARLY,
        codes.TOO_MANY_REQUESTS,
        codes.INTERNAL_SERVER_ERROR,
        codes.BAD_GATEWAY,
        codes.SERVICE_UNAVAILABLE,
        codes.INSUFFICIENT_STORAGE,
    ]
    retry_errors: list[str] = ["query_limit_exceeded", "operation_time_limit"]

    retry_attempts: int = 5
    retry_delay: float = 5
    retry_backoff: float = 2

    list_size: int = 50
    batch_size: int = Field(default=50, ge=1, le=50)
    portal_build: str | None = None
    scopes: frozenset[str] = frozenset()

    model_config = SettingsConfigDict(
        env_prefix="bitrix24_api_",
        env_file=".env",
        extra="ignore",
    )

    @field_serializer("webhook_url")
    def _serialize_webhook_url(self, _value: HttpUrl) -> str:
        """Never expose webhook credentials through public serialization."""
        return "[REDACTED]"


def api_settings(**kwargs: Any) -> Settings:  # noqa: ANN401
    """Return validated API settings."""
    return Settings(**kwargs)


ApiSettings = Annotated[Settings, Depends(api_settings)]
