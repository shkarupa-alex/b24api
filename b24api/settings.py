from collections.abc import Generator
from typing import Annotated

from fast_depends import Depends
from httpx import codes
from pydantic import HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="bitrix_24_api_",
        env_file=".env",
        extra="ignore",
    )

    webhook_url: HttpUrl

    # TODO
    # http_http2: bool = True
    # http_timeout: float = 30

    retry_statuses: list[int] = (
        codes.LOCKED,
        codes.TOO_EARLY,
        codes.TOO_MANY_REQUESTS,
        codes.INTERNAL_SERVER_ERROR,
        codes.BAD_GATEWAY,
        codes.SERVICE_UNAVAILABLE,
        codes.INSUFFICIENT_STORAGE,
    )
    retry_errors: list[str] = ["query_limit_exceeded", "operation_time_limit"]

    retry_tries: int = 5
    retry_delay: float = 5
    retry_backoff: float = 2

    list_size: int = 50
    batch_size: int = 50


def api_settings() -> Generator[Settings, None, None]:
    settings = Settings()  # TODO
    yield settings


ApiSettings = Annotated[Settings, Depends(api_settings)]
