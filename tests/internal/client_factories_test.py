"""§3.12: ``Bitrix24.from_webhook`` owns what it builds; an injected transport stays with its caller.

Both paths run under ``async with``, fail cleanly in the constructor and tolerate a repeated close.
The ownership checks read the client's private resources, which is why the file lives here.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from b24api import Bitrix24, ExecutionPolicy, HttpxTransport, Request, Settings
from b24api.execution import CoordinatorState, WireResponse

HOST = "fixture.invalid"
MARKER = "factorymarker1234"
URL = f"https://{HOST}/rest/1/{MARKER}/"
TIMEOUT = 7.5


class _Transport:
    host = HOST

    def __init__(self) -> None:
        self.closed = False

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del request, attempt_timeout, max_response_bytes
        return WireResponse(200, (("content-type", "application/json"),), b'{"result":true}')

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_from_webhook_owns_and_closes_its_transport_and_coordinator() -> None:
    async with Bitrix24.from_webhook(URL) as client:
        transport = client._owned_transport  # noqa: SLF001 - ownership observation
        coordinator = client._owned_coordinator  # noqa: SLF001 - ownership observation
        assert isinstance(transport, HttpxTransport)
        assert coordinator is not None

    assert transport._closed  # noqa: SLF001 - the owned transport was closed
    assert (await coordinator.snapshot()).state is CoordinatorState.CLOSED
    await client.aclose()


def test_from_webhook_keeps_the_settings_timeout_unless_one_is_given() -> None:
    default = Bitrix24.from_webhook(URL)
    explicit = Bitrix24.from_webhook(URL, http_timeout=TIMEOUT)
    policy = ExecutionPolicy(max_pages=3)
    custom = Bitrix24.from_webhook(URL, policy=policy)

    expected = float(Settings(webhook_url=URL).http_timeout)
    assert default._default_policy.max_retry_elapsed_per_request == expected  # noqa: SLF001
    assert explicit._default_policy.max_retry_elapsed_per_request == TIMEOUT  # noqa: SLF001
    assert custom._default_policy is policy  # noqa: SLF001


def test_from_webhook_rejects_a_bad_url_without_echoing_it() -> None:
    with pytest.raises(ValidationError) as captured:
        Bitrix24.from_webhook(f"ftp:/{MARKER}")
    assert MARKER not in str(captured.value)
    with pytest.raises(TypeError, match="webhook url must be a string"):
        Bitrix24.from_webhook(b"https://fixture.invalid/rest/1/x/")  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        Bitrix24.from_webhook(URL, http_timeout=0)


@pytest.mark.asyncio
async def test_injected_transport_stays_with_the_caller_and_the_coordinator_with_the_client() -> None:
    transport = _Transport()

    async with Bitrix24(Settings(webhook_url=URL), transport=transport) as client:
        coordinator = client._owned_coordinator  # noqa: SLF001 - ownership observation
        assert coordinator is not None
        assert client._owned_transport is None  # noqa: SLF001 - ownership observation

    assert not transport.closed
    assert (await coordinator.snapshot()).state is CoordinatorState.CLOSED
    await client.aclose()
    assert not transport.closed


def test_injected_transport_for_another_host_is_refused() -> None:
    transport = _Transport()
    transport.host = "other.invalid"

    with pytest.raises(ValueError, match="injected transport host does not match Settings"):
        Bitrix24(Settings(webhook_url=URL), transport=transport)
