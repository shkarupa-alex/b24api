"""Environment-backed credential and transport settings."""

import contextlib
from collections.abc import Generator
from types import MappingProxyType
from typing import Any, Self, cast

from pydantic import AnyUrl, Field, HttpUrl, ValidationError, field_serializer, field_validator
from pydantic.config import ExtraValues
from pydantic_core import InitErrorDetails, PydanticCustomError
from pydantic_settings import BaseSettings, SettingsConfigDict, SettingsError

from b24api._webhook import WebhookRefusal, parse_webhook

_REDACTED = "[REDACTED]"
# The error type of each refusal; its message is the refusal's own, which quotes no part of the URL.
_WEBHOOK_ERROR_TYPES = MappingProxyType(
    {
        WebhookRefusal.INVALID: "webhook_url",
        WebhookRefusal.USERINFO: "webhook_url_userinfo",
        WebhookRefusal.HOST: "webhook_url_host",
        WebhookRefusal.SHAPE: "webhook_url_shape",
    }
)


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
            if item["type"] in _WEBHOOK_ERROR_TYPES.values():
                safe_item["type"] = PydanticCustomError(item["type"], item["msg"])
            details.append(cast("InitErrorDetails", safe_item))
    return ValidationError.from_exception_data("Settings", details)


def _settings_loading_error() -> SettingsError:
    return SettingsError("Settings loading failed")


def _unknown_settings_error(fields: set[str]) -> ValidationError:
    details: list[InitErrorDetails] = [
        {
            "type": PydanticCustomError("settings_field", "Unknown Settings field"),
            "loc": (field,),
            "input": _REDACTED,
        }
        for field in sorted(fields)
    ]
    return ValidationError.from_exception_data("Settings", details)


class Settings(BaseSettings):
    """Environment-backed webhook credential and request timeout.

    ``http_timeout`` is the retry window of each request under the client's default policy
    (``ExecutionPolicy.max_retry_elapsed_per_request``): it opens when the request's first send is granted its
    permit and bounds that send and every retry after it.
    """

    webhook_url: HttpUrl = Field(repr=False)
    http_timeout: float = Field(default=30.0, gt=0)

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
        unknown = set(data) - {"webhook_url", "http_timeout"}
        if unknown:
            validation_failure = _unknown_settings_error(unknown)
        else:
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
    def _sanitize_invalid_webhook_input(cls, value: Any) -> str:  # noqa: ANN401
        """Accept exactly what the transport accepts and store its canonical base; keep the input out of errors.

        The returned base is what pydantic then validates as ``HttpUrl``; the shared parse admits only
        bases it represents.
        """
        # A pydantic URL (a copied or re-validated Settings) is read back from its string form; bytes are
        # read as UTF-8, as pydantic reads them.
        if isinstance(value, AnyUrl):
            value = str(value)
        elif isinstance(value, bytes | bytearray):
            with contextlib.suppress(UnicodeDecodeError):
                value = bytes(value).decode()
        webhook = parse_webhook(value)
        if isinstance(webhook, WebhookRefusal):
            raise PydanticCustomError(_WEBHOOK_ERROR_TYPES[webhook], webhook.value)
        return webhook.url

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
