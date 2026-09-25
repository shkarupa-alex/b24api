"""Shared fixtures: an offline responder transport and public clients closed after each test (B15)."""

from __future__ import annotations
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from tests.scripting import HOST, ResponderTransport, client_for

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from b24api import Bitrix24, ExecutionPolicy, Transport
    from b24api.contracts import UnknownRequestAudit
    from tests.scripting import Responder


@pytest.fixture
def scripted_transport() -> Callable[..., ResponderTransport]:
    """Build a transport that answers each request from a function (exact exchanges: ``ScriptedTransport``)."""

    def build(respond: Responder, *, host: str = HOST) -> ResponderTransport:
        return ResponderTransport(respond, host=host)

    return build


@pytest_asyncio.fixture
async def scripted_client() -> AsyncIterator[Callable[..., Bitrix24]]:
    """Build public clients over a transport; every client is closed when the test ends."""
    clients: list[Bitrix24] = []

    def build(
        transport: Transport,
        *,
        policy: ExecutionPolicy | None = None,
        unknown_request_audit: UnknownRequestAudit | None = None,
    ) -> Bitrix24:
        client = client_for(transport, policy=policy, unknown_request_audit=unknown_request_audit)
        clients.append(client)
        return client

    yield build
    for client in clients:
        await client.aclose()
