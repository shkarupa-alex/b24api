"""Credential-safe v2 settings behavior."""

from __future__ import annotations
import json
import pickle
from pathlib import Path

import pytest
from pydantic import ValidationError

from b24api import Bitrix24, Settings
from b24api.settings import api_settings

_WEBHOOK = "https://bitrix24.com/rest/0/test/"
_TIMEOUT = 17.5


def _assert_traceback_excludes(error: BaseException, value: str, *, test_file: Path) -> None:
    traceback = error.__traceback__
    while traceback is not None:
        if Path(traceback.tb_frame.f_code.co_filename) != test_file:
            assert all(value not in repr(local) for local in traceback.tb_frame.f_locals.values())
        traceback = traceback.tb_next


def test_settings_contains_only_credential_and_transport_configuration() -> None:
    settings = Settings(webhook_url=_WEBHOOK, http_timeout=_TIMEOUT)

    assert set(type(settings).model_fields) == {"webhook_url", "http_timeout"}
    assert settings.http_timeout == _TIMEOUT
    with pytest.raises(ValidationError, match="Unknown Settings field"):
        Settings(webhook_url=_WEBHOOK, batch_size=50)


def test_settings_redacts_webhook_from_all_public_representations() -> None:
    credential = "n1x2y3z4q5w6e7r8"
    settings = Settings(webhook_url=f"https://example.invalid/rest/1/{credential}/")

    surfaces = (
        repr(settings),
        str(settings),
        repr(dict(settings)),
        repr(list(settings)),
        repr(settings.model_dump()),
        settings.model_dump_json(),
        repr(settings.__getstate__()),
        repr(pickle.dumps(settings)),
    )

    assert all(credential not in surface for surface in surfaces)
    assert credential in str(settings.webhook_url)


@pytest.mark.parametrize("entrypoint", ["constructor", "api", "mapping", "json", "strings"])
def test_invalid_settings_drop_credential_from_errors_and_framework_tracebacks(
    entrypoint: str,
    request: pytest.FixtureRequest,
) -> None:
    credential = "n1x2y3z4q5w6e7r8"
    webhook = f"https://example.invalid:bad/{credential}/"

    def validate() -> None:
        if entrypoint == "constructor":
            Settings(webhook_url=webhook)
        elif entrypoint == "api":
            api_settings(webhook_url=webhook)
        elif entrypoint == "mapping":
            Settings.model_validate({"webhook_url": webhook})
        elif entrypoint == "json":
            Settings.model_validate_json(json.dumps({"webhook_url": webhook}))
        else:
            Settings.model_validate_strings({"webhook_url": webhook})

    with pytest.raises(ValidationError) as captured:
        validate()

    error = captured.value
    assert all(credential not in surface for surface in (str(error), repr(error), error.json()))
    _assert_traceback_excludes(error, credential, test_file=request.path)


@pytest.mark.asyncio
async def test_environment_loading_and_explicit_settings_select_the_expected_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BITRIX24_API_WEBHOOK_URL", "https://environment.invalid/rest/1/redacted/")
    environment_client = Bitrix24()
    explicit_client = Bitrix24(Settings(webhook_url="https://explicit.invalid/rest/1/redacted/"))

    try:
        assert environment_client.host == "environment.invalid"
        assert explicit_client.host == "explicit.invalid"
    finally:
        await environment_client.aclose()
        await explicit_client.aclose()


@pytest.mark.asyncio
async def test_settings_timeout_becomes_the_default_per_request_elapsed_ceiling() -> None:
    client = Bitrix24(Settings(webhook_url=_WEBHOOK, http_timeout=_TIMEOUT))

    try:
        assert client._default_policy.max_retry_elapsed_per_request == _TIMEOUT  # noqa: SLF001 - composition proof
    finally:
        await client.aclose()
