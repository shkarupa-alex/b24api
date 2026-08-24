"""Support settings."""

from collections.abc import Generator
from typing import Annotated, Any, Self, cast

from fast_depends import Depends
from httpx import codes
from pydantic import Field, HttpUrl, TypeAdapter, ValidationError, field_serializer, field_validator
from pydantic.config import ExtraValues
from pydantic_core import InitErrorDetails, PydanticCustomError
from pydantic_settings import BaseSettings, SettingsConfigDict, SettingsError

_WEBHOOK_ADAPTER = TypeAdapter(HttpUrl)
_REDACTED = "[REDACTED]"


def _settings_validation_error(error: ValidationError | None = None) -> ValidationError:
    if error is None:
        details: list[InitErrorDetails] = [
            {
                "type": PydanticCustomError("settings_validation", "Settings validation failed"),
                "loc": (),
                "input": _REDACTED,
            },
        ]
    else:
        details = []
        for item in error.errors(include_url=False):
            safe_item = dict(item, input=_REDACTED)
            if item["type"] == "webhook_url":
                safe_item["type"] = PydanticCustomError("webhook_url", "Webhook URL is invalid")
            details.append(cast("InitErrorDetails", safe_item))
    return ValidationError.from_exception_data("Settings", details)


def _settings_loading_error() -> SettingsError:
    return SettingsError("Settings loading failed")


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
        hide_input_in_errors=True,
    )

    def __init__(self, **data: Any) -> None:  # noqa: ANN401
        """Validate without retaining credential-bearing framework traceback frames."""
        validation_failure: ValidationError | None = None
        loading_failed = False
        try:
            super().__init__(**data)
        except ValidationError as error:
            validation_failure = _settings_validation_error(error)
        except SettingsError:
            loading_failed = True
        data.clear()
        if validation_failure is not None:
            raise validation_failure
        if loading_failed:
            raise _settings_loading_error()

    @classmethod
    def model_validate(  # noqa: PLR0913
        cls,
        obj: Any,  # noqa: ANN401
        *,
        strict: bool | None = None,
        extra: ExtraValues | None = None,
        from_attributes: bool | None = None,
        context: Any | None = None,  # noqa: ANN401
        by_alias: bool | None = None,
        by_name: bool | None = None,
    ) -> Self:
        """Validate mappings without retaining credential-bearing inputs."""
        result: Self | None = None
        failure: ValidationError | None = None
        try:
            result = super().model_validate(
                obj,
                strict=strict,
                extra=extra,
                from_attributes=from_attributes,
                context=context,
                by_alias=by_alias,
                by_name=by_name,
            )
        except ValidationError as error:
            failure = _settings_validation_error(error)
        obj = None
        context = None
        if failure is not None:
            raise failure
        return cast("Self", result)

    @classmethod
    def model_validate_json(  # noqa: PLR0913
        cls,
        json_data: str | bytes | bytearray,
        *,
        strict: bool | None = None,
        extra: ExtraValues | None = None,
        context: Any | None = None,  # noqa: ANN401
        by_alias: bool | None = None,
        by_name: bool | None = None,
    ) -> Self:
        """Validate JSON without retaining credential-bearing inputs."""
        result: Self | None = None
        failure: ValidationError | None = None
        try:
            result = super().model_validate_json(
                json_data,
                strict=strict,
                extra=extra,
                context=context,
                by_alias=by_alias,
                by_name=by_name,
            )
        except ValidationError as error:
            failure = _settings_validation_error(error)
        json_data = b""
        context = None
        if failure is not None:
            raise failure
        return cast("Self", result)

    @classmethod
    def model_validate_strings(  # noqa: PLR0913
        cls,
        obj: Any,  # noqa: ANN401
        *,
        strict: bool | None = None,
        extra: ExtraValues | None = None,
        context: Any | None = None,  # noqa: ANN401
        by_alias: bool | None = None,
        by_name: bool | None = None,
    ) -> Self:
        """Validate string mappings without retaining credential-bearing inputs."""
        result: Self | None = None
        failure: ValidationError | None = None
        try:
            result = super().model_validate_strings(
                obj,
                strict=strict,
                extra=extra,
                context=context,
                by_alias=by_alias,
                by_name=by_name,
            )
        except ValidationError as error:
            failure = _settings_validation_error(error)
        obj = None
        context = None
        if failure is not None:
            raise failure
        return cast("Self", result)

    @field_validator("webhook_url", mode="before")
    @classmethod
    def _sanitize_invalid_webhook_input(cls, value: Any) -> HttpUrl | str:  # noqa: ANN401
        """Keep invalid credential-bearing input out of validation errors."""
        try:
            return _WEBHOOK_ADAPTER.validate_python(value)
        except (TypeError, ValueError, ValidationError):
            raise PydanticCustomError("webhook_url", "Webhook URL is invalid") from None

    @field_serializer("webhook_url")
    def _serialize_webhook_url(self, _value: HttpUrl) -> str:
        """Never expose webhook credentials through public serialization."""
        return _REDACTED

    def __iter__(self) -> Generator[tuple[str, Any], None, None]:
        """Iterate with the credential-bearing field redacted."""
        for name, value in super().__iter__():
            yield name, _REDACTED if name == "webhook_url" else value

    def __getstate__(self) -> dict[str, Any]:
        """Return pickle state without credential-bearing settings."""
        state = super().__getstate__()
        values = dict(state["__dict__"])
        values["webhook_url"] = _REDACTED
        state["__dict__"] = values
        return state


def api_settings(**kwargs: Any) -> Settings:  # noqa: ANN401
    """Return validated API settings."""
    result: Settings | None = None
    validation_failure: ValidationError | None = None
    loading_failed = False
    try:
        result = Settings(**kwargs)
    except ValidationError as error:
        validation_failure = _settings_validation_error(error)
    except SettingsError:
        loading_failed = True
    kwargs.clear()
    if validation_failure is not None:
        raise validation_failure
    if loading_failed:
        raise _settings_loading_error()
    return cast("Settings", result)


ApiSettings = Annotated[Settings, Depends(api_settings)]
