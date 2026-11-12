"""Credential-safe v2 settings behavior."""

from __future__ import annotations
import asyncio
import dataclasses
import json
import math
import pickle
from pathlib import Path

import pytest
from pydantic import ValidationError

import b24api.client
from b24api import Bitrix24, DirectDispatch, ExecutionPolicy, ReplaySafety, Request, RouteKind, Settings
from b24api.contracts import Command
from b24api.settings import api_settings
from b24api.transport import WireResponse
from tests.scripting import ResponderTransport, on_loop, reached

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

    assert set(type(settings).model_fields) == {"webhook_url", "http_timeout", "max_concurrent_requests"}
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


class _RecordingTransport:
    """Answer every call and record the ceilings the executor passed to the wire."""

    host = "bitrix24.com"

    def __init__(self) -> None:
        self.ceilings: list[tuple[float, int]] = []

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del request
        self.ceilings.append((attempt_timeout, max_response_bytes))
        return WireResponse(200, (), b'{"result": true}')

    async def aclose(self) -> None:
        return None


def test_policy_from_settings_is_the_library_default_with_the_settings_timeout() -> None:
    policy = ExecutionPolicy.from_settings(Settings(webhook_url=_WEBHOOK, http_timeout=_TIMEOUT))

    assert policy == dataclasses.replace(ExecutionPolicy(), max_retry_elapsed_per_request=_TIMEOUT)


@pytest.mark.asyncio
async def test_a_per_call_policy_replaces_the_client_default_without_merging() -> None:
    transport = _RecordingTransport()
    settings = Settings(webhook_url=_WEBHOOK, http_timeout=_TIMEOUT)
    small = dataclasses.replace(ExecutionPolicy.from_settings(settings), max_response_bytes=4096)

    async with Bitrix24(settings, transport=transport) as client:
        await client.call(Request("server.time", route=RouteKind.BARE))
        await client.call(Request("server.time", route=RouteKind.BARE), policy=ExecutionPolicy())
        await client.call(Request("server.time", route=RouteKind.BARE), policy=small)

    (default_timeout, default_bytes), (replaced_timeout, replaced_bytes), (derived_timeout, derived_bytes) = (
        transport.ceilings
    )
    # The client default carries the settings timeout; a bare per-call policy drops it (no hidden merge).
    assert default_timeout <= _TIMEOUT < replaced_timeout
    assert default_bytes == replaced_bytes == ExecutionPolicy().max_response_bytes
    # Deriving the per-call policy from the settings keeps the timeout and changes only what it names.
    assert derived_timeout <= _TIMEOUT
    assert derived_bytes == small.max_response_bytes


@pytest.mark.parametrize("timeout", [math.inf, -math.inf, math.nan, "inf", "nan"])
def test_settings_refuse_a_non_finite_http_timeout(timeout: float | str) -> None:
    # An infinite window used to pass Settings and fail only in the client, after it had opened its transport.
    with pytest.raises(ValidationError, match="http_timeout"):
        Settings(webhook_url=_WEBHOOK, http_timeout=timeout)
    with pytest.raises(ValidationError, match="http_timeout"):
        Bitrix24.from_webhook(_WEBHOOK, http_timeout=float(timeout))


@pytest.mark.parametrize(
    ("field", "value", "refusal"),
    [
        ("http_timeout", math.inf, "execution time limits must be finite"),
        ("max_concurrent_requests", 0, "max_concurrency must be positive"),
    ],
)
def test_a_refused_client_configuration_opens_no_transport(
    monkeypatch: pytest.MonkeyPatch, field: str, value: float, refusal: str
) -> None:
    # A Settings built without validation reaches the client's own checks, which all run before it opens one.
    opened: list[str] = []
    monkeypatch.setattr(b24api.client, "HttpxTransport", opened.append)
    settings = Settings.model_construct(webhook_url=_WEBHOOK).model_copy(update={field: value})

    with pytest.raises(ValueError, match=refusal):
        Bitrix24(settings)

    assert opened == []


def test_max_concurrent_requests_is_a_positive_setting_read_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert Settings(webhook_url=_WEBHOOK).max_concurrent_requests == _DEFAULT_CONCURRENT_REQUESTS
    monkeypatch.setenv("BITRIX24_API_MAX_CONCURRENT_REQUESTS", "25")
    assert Settings(webhook_url=_WEBHOOK).max_concurrent_requests == 25  # noqa: PLR2004
    with pytest.raises(ValidationError, match="max_concurrent_requests"):
        Settings(webhook_url=_WEBHOOK, max_concurrent_requests=0)
    with pytest.raises(ValidationError, match="max_concurrent_requests"):
        Bitrix24.from_webhook(_WEBHOOK, max_concurrent_requests=0)


_DEFAULT_CONCURRENT_REQUESTS = 10
_WIDE = 40


async def _settled(transport: ResponderTransport, full: asyncio.Event) -> int:
    """Return the sends in flight once the expected peak is reached and the loop has had room to exceed it.

    Every send blocks until released and the input is fed on the loop, so after ``full`` no further send can
    start except through a limit that admits more than the peak; the extra turns of the loop give it that room.
    """
    await reached(full)
    for _ in range(50):
        await asyncio.sleep(0)
    return len(transport.requests)


@pytest.mark.asyncio
@pytest.mark.parametrize("construction", ["settings", "from_webhook"])
@pytest.mark.parametrize(
    ("limit", "dispatch", "policy", "peak"),
    [
        (None, _WIDE, _WIDE, _DEFAULT_CONCURRENT_REQUESTS),
        (_WIDE, _WIDE, _WIDE, _WIDE),
        (_WIDE, 4, _WIDE, 4),
        (_WIDE, _WIDE, 6, 6),
        (3, _WIDE, _WIDE, 3),
    ],
)
async def test_effective_concurrency_is_the_smallest_of_the_client_and_operation_limits(  # noqa: PLR0913
    monkeypatch: pytest.MonkeyPatch,
    construction: str,
    limit: int | None,
    dispatch: int,
    policy: int,
    peak: int,
) -> None:
    # The client's limit used to be a fixed 10 that no setting raised: DirectDispatch(concurrency=40) under
    # max_direct_concurrency=40 still peaked at 10.
    release = asyncio.Event()
    full = asyncio.Event()

    async def respond(_request: Request) -> object:
        if len(transport.requests) >= peak:
            full.set()
        await release.wait()
        return {"result": 1}

    transport = ResponderTransport(respond, host="bitrix24.com")
    limits = {} if limit is None else {"max_concurrent_requests": limit}
    if construction == "settings":
        client = Bitrix24(Settings(webhook_url=_WEBHOOK, **limits), transport=transport)
    else:
        monkeypatch.setattr(b24api.client, "HttpxTransport", lambda _url: transport)
        client = Bitrix24.from_webhook(_WEBHOOK, max_concurrent_requests=limit)
    commands = [Command(Request.bare("crm.deal.get", {"ID": index}, ReplaySafety.SAFE), index) for index in range(60)]

    async with client:
        stream = client.fan_out_outcomes(
            on_loop(commands),
            dispatch=DirectDispatch(concurrency=dispatch),
            policy=ExecutionPolicy(max_direct_concurrency=policy),
        )

        async def drain() -> int:
            async with stream:
                return len([outcome async for outcome in stream])

        drained = asyncio.create_task(drain())
        in_flight = await _settled(transport, full)
        release.set()
        assert await drained == len(commands)

    assert in_flight == peak
