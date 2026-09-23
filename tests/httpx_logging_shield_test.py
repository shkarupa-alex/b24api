"""Positive controls for credential-safe HTTPX INFO logging."""

from __future__ import annotations
import asyncio
import io
import logging
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import pytest

from b24api import BodyEncoding, Request, RouteKind
from b24api.errors import TransportError
from b24api.transport import HttpxTransport
from b24api.transport.logging_shield import HTTPX_LOG_SHIELD

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

_OWNED_MARKER = "synthetic-owned-secret-123456"
_FOREIGN_MARKER = "synthetic-foreign-secret-123456"
_SUCCESS_STATUS = 200
_RESPONSE_STATUS = 403
_OWNED_RECORDS = 2
_TOTAL_RECORDS = 3
_REDIRECT_RECORDS = 2


def test_httpx_dependency_range_matches_the_positive_controlled_minor() -> None:
    project = tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text())

    assert "httpx[http2]>=0.28.1,<0.29" in project["project"]["dependencies"]


class _CollectingHandler(logging.StreamHandler[io.StringIO]):
    """Retain the original LogRecord as well as the formatted handler output."""

    def __init__(self) -> None:
        self.output = io.StringIO()
        self.records: list[logging.LogRecord] = []
        super().__init__(self.output)

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
        super().emit(record)


@pytest.mark.asyncio
async def test_httpx_info_record_is_emitted_and_rewritten_before_handler_formatting() -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    def respond(request: httpx.Request) -> httpx.Response:
        status = _SUCCESS_STATUS if request.headers.get("content-type") == "application/json" else _RESPONSE_STATUS
        return httpx.Response(status, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    first = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    second = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    try:
        responses = await asyncio.gather(
            *(
                first.send(
                    Request("profile", route=RouteKind.JSON, encoding=encoding),
                    attempt_timeout=1,
                    max_response_bytes=1024,
                )
                for encoding in (BodyEncoding.JSON, BodyEncoding.FORM_URLENCODED)
            )
        )
        assert {response.status_code for response in responses} == {_SUCCESS_STATUS, _RESPONSE_STATUS}
        assert len(handler.records) == _OWNED_RECORDS
        for record in handler.records:
            assert record.name == "httpx"
            assert record.levelno == logging.INFO
            assert record.pathname.endswith("httpx/_client.py")
            raw = f"{record.msg!r} {record.args!r}"
            assert _OWNED_MARKER not in raw
            assert _OWNED_MARKER not in record.getMessage()
            assert "[REDACTED]" in record.getMessage()
        assert _OWNED_MARKER not in handler.output.getvalue()

        await first.aclose()
        assert HTTPX_LOG_SHIELD._filter in logger.filters  # noqa: SLF001 - lifecycle control
        logger.removeFilter(HTTPX_LOG_SHIELD._filter)  # noqa: SLF001 - simulate logger drift
        await second.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024)
        assert len(handler.records) == _TOTAL_RECORDS
        assert _OWNED_MARKER not in handler.records[-1].getMessage()
        assert HTTPX_LOG_SHIELD._filter in logger.filters  # noqa: SLF001 - reinstallation control

        await client.post(f"https://portal.invalid/rest/1/{_FOREIGN_MARKER}/profile")
        assert _FOREIGN_MARKER in handler.records[-1].getMessage()
    finally:
        await first.aclose()
        await second.aclose()
        await client.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - final cleanup control


@pytest.mark.asyncio
async def test_httpx_timeout_uses_redacted_exception_after_inflight_cleanup() -> None:
    async def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("synthetic timeout", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(timeout))
    transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    try:
        with pytest.raises(TransportError) as captured:
            await transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024)
        assert _OWNED_MARKER not in repr(captured.value)
    finally:
        await transport.aclose()
        await client.aclose()


@pytest.mark.asyncio
async def test_closing_transport_keeps_filter_until_inflight_httpx_record_is_emitted() -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def respond(request: httpx.Request) -> httpx.Response:
        entered.set()
        await release.wait()
        return httpx.Response(_SUCCESS_STATUS, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    task = asyncio.create_task(
        transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024),
    )
    try:
        await entered.wait()
        await transport.aclose()
        assert HTTPX_LOG_SHIELD._filter in logger.filters  # noqa: SLF001 - in-flight lifecycle control
        release.set()
        await task
        assert len(handler.records) == 1
        assert _OWNED_MARKER not in handler.records[0].getMessage()
    finally:
        release.set()
        await task
        await transport.aclose()
        await client.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - in-flight cleanup control


@pytest.mark.asyncio
@pytest.mark.parametrize("route", [RouteKind.BARE, RouteKind.JSON, RouteKind.API_V3])
async def test_redirect_hops_keep_the_owned_webhook_credential_out_of_httpx_info(
    route: RouteKind,
) -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.host == "portal.invalid":
            redirected = request.url.copy_with(scheme="https", host="redirect.invalid")
            return httpx.Response(301, headers={"location": str(redirected)}, request=request)
        return httpx.Response(_SUCCESS_STATUS, json={"result": True}, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond), follow_redirects=True)
    transport = HttpxTransport(f"http://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    try:
        response = await transport.send(
            Request("profile", route=route),
            attempt_timeout=1,
            max_response_bytes=1024,
        )
        assert response.status_code == _SUCCESS_STATUS
        assert len(handler.records) == _REDIRECT_RECORDS
        for record in handler.records:
            assert _OWNED_MARKER not in f"{record.msg!r} {record.args!r}"
            assert _OWNED_MARKER not in record.getMessage()
        assert _OWNED_MARKER not in handler.output.getvalue()
    finally:
        await transport.aclose()
        await client.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)


_SECOND_MARKER = "synthetic-second-secret-654321"
_REDIRECT_TARGETS = (
    f"/rest/1/{_SECOND_MARKER}/profile",
    f"/rest/api/1/{_SECOND_MARKER}/profile",
    f"/profile?auth={_SECOND_MARKER}",
)


@pytest.mark.asyncio
@pytest.mark.parametrize("target", _REDIRECT_TARGETS, ids=["classic", "api-v3", "auth-query"])
@pytest.mark.parametrize("route", [RouteKind.BARE, RouteKind.JSON, RouteKind.API_V3])
async def test_redirect_replaces_webhook_token_without_logging_either_secret(route: RouteKind, target: str) -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.host == "portal.invalid":
            return httpx.Response(301, headers={"location": f"https://redirect.invalid{target}"}, request=request)
        logger.info("hop observed", extra={"url": str(request.url), "request_url": str(request.url)})
        return httpx.Response(_SUCCESS_STATUS, json={"result": True}, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond), follow_redirects=True)
    transport = HttpxTransport(f"http://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    try:
        response = await transport.send(Request("profile", route=route), attempt_timeout=1, max_response_bytes=1024)
        assert response.status_code == _SUCCESS_STATUS
        assert len(handler.records) == _REDIRECT_RECORDS + 1
        for record in handler.records:
            extras = (record.__dict__.get("url"), record.__dict__.get("request_url"))
            raw = f"{record.msg!r} {record.args!r} {extras!r}"
            for marker in (_OWNED_MARKER, _SECOND_MARKER):
                assert marker not in raw
                assert marker not in record.getMessage()
        assert _OWNED_MARKER not in handler.output.getvalue()
        assert _SECOND_MARKER not in handler.output.getvalue()

        await client.get(f"https://redirect.invalid/rest/1/{_FOREIGN_MARKER}/profile")
        assert _FOREIGN_MARKER in handler.records[-1].getMessage()
    finally:
        await transport.aclose()
        await client.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - final cleanup control


_THIRD_MARKER = "synthetic-third-secret-777777"


@pytest.mark.asyncio
@pytest.mark.parametrize("hook_kind", ["request", "response"])
@pytest.mark.parametrize("same_client", [True, False], ids=["same-client", "other-client"])
async def test_unrelated_httpx_request_inside_owned_hook_is_unchanged(same_client: bool, hook_kind: str) -> None:  # noqa: FBT001
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    foreign_url = f"https://other.invalid/rest/1/{_FOREIGN_MARKER}/profile"
    foreign = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(_SUCCESS_STATUS, request=request)),
    )
    hook_calls: list[str] = []

    async def hook(event: httpx.Request | httpx.Response) -> None:
        if not hook_calls:
            hook_calls.append(str(event.url))
            await (client if same_client else foreign).get(foreign_url)

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.host == "portal.invalid":
            location = f"https://redirect.invalid/rest/1/{_THIRD_MARKER}/profile"
            return httpx.Response(301, headers={"location": location}, request=request)
        return httpx.Response(_SUCCESS_STATUS, json={"result": True}, request=request)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(respond),
        follow_redirects=True,
        event_hooks={hook_kind: [hook]},
    )
    transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    try:
        response = await transport.send(
            Request("profile", route=RouteKind.BARE),
            attempt_timeout=1,
            max_response_bytes=1024,
        )
        assert response.status_code == _SUCCESS_STATUS
        messages = [record.getMessage() for record in handler.records]
        assert len(messages) == _TOTAL_RECORDS
        foreign_records = [record for record in handler.records if "other.invalid" in record.getMessage()]
        assert len(foreign_records) == 1
        assert _FOREIGN_MARKER in f"{foreign_records[0].args!r}"
        assert foreign_url in foreign_records[0].getMessage()
        assert foreign_url in handler.output.getvalue()
        for record in handler.records:
            if record in foreign_records:
                continue
            for marker in (_OWNED_MARKER, _THIRD_MARKER):
                assert marker not in f"{record.msg!r} {record.args!r}"
                assert marker not in record.getMessage()
        for marker in (_OWNED_MARKER, _THIRD_MARKER):
            assert marker not in handler.output.getvalue()
    finally:
        await transport.aclose()
        await client.aclose()
        await foreign.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - final cleanup control


_UNAUTHORIZED = 401
_ROTATED_MARKER = "synthetic-rotated-secret-424242"
_DIGEST_CHALLENGE = 'Digest realm="portal", nonce="synthetic-nonce", qop="auth"'


class _RefreshingAuth(httpx.Auth):
    """Send an unrelated same-client request, then authorize the owned request in place."""

    def __init__(self, client: httpx.AsyncClient, foreign_url: str) -> None:
        self.client = client
        self.foreign_url = foreign_url

    async def async_auth_flow(self, request: httpx.Request) -> AsyncGenerator[httpx.Request, httpx.Response]:
        await self.client.get(self.foreign_url, auth=None)
        request.headers["x-synthetic-auth"] = "in-place"
        yield request


@pytest.mark.asyncio
@pytest.mark.parametrize("flow", ["basic", "digest-challenge", "refresh", "redirect"])
@pytest.mark.parametrize("route", [RouteKind.BARE, RouteKind.API_V3])
async def test_in_place_injected_auth_keeps_owned_webhook_out_of_info(route: RouteKind, flow: str) -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    foreign_url = f"https://other.invalid/rest/1/{_FOREIGN_MARKER}/profile"

    def respond(request: httpx.Request) -> httpx.Response:
        if flow == "digest-challenge" and "authorization" not in request.headers:
            return httpx.Response(_UNAUTHORIZED, headers={"www-authenticate": _DIGEST_CHALLENGE}, request=request)
        if flow == "redirect" and request.url.host == "portal.invalid":
            location = f"https://redirect.invalid/rest/1/{_THIRD_MARKER}/profile"
            return httpx.Response(301, headers={"location": location}, request=request)
        return httpx.Response(_SUCCESS_STATUS, json={"result": True}, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond), follow_redirects=True)
    client.auth = {
        "basic": httpx.BasicAuth("synthetic-user", "synthetic-password"),
        "digest-challenge": httpx.DigestAuth("synthetic-user", "synthetic-password"),
        "refresh": _RefreshingAuth(client, foreign_url),
        "redirect": httpx.BasicAuth("synthetic-user", "synthetic-password"),
    }[flow]
    transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    try:
        response = await transport.send(Request("profile", route=route), attempt_timeout=1, max_response_bytes=1024)
        assert response.status_code == _SUCCESS_STATUS
        assert len(handler.records) == (1 if flow == "basic" else 2)
        for record in handler.records:
            if "other.invalid" in record.getMessage():
                assert foreign_url in record.getMessage()
                assert isinstance(record.args, tuple)
                assert isinstance(record.args[1], httpx.URL)
                continue
            for marker in (_OWNED_MARKER, _THIRD_MARKER):
                assert marker not in f"{record.msg!r} {record.args!r}"
                assert marker not in record.getMessage()
        for marker in (_OWNED_MARKER, _THIRD_MARKER):
            assert marker not in handler.output.getvalue()
        assert (foreign_url in handler.output.getvalue()) is (flow == "refresh")
    finally:
        await transport.aclose()
        await client.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - final cleanup control


def _substitute(url: httpx.URL, target: str) -> str:
    """Build a request URL an auth flow might yield instead of the owned one."""
    rendered = str(url)
    return {
        "clone": rendered,
        "rotated-token": rendered.replace(_OWNED_MARKER, _ROTATED_MARKER),
        "rotated-user": rendered.replace(f"/1/{_OWNED_MARKER}/", f"/2/{_ROTATED_MARKER}/"),
        "same-operation-foreign": rendered.replace(f"/1/{_OWNED_MARKER}/", f"/2/{_FOREIGN_MARKER}/"),
        "case-foreign": rendered.replace(f"/1/{_OWNED_MARKER}/profile", f"/2/{_FOREIGN_MARKER}/PROFILE"),
        "other-path-foreign": rendered.replace(f"/1/{_OWNED_MARKER}/profile", f"/2/{_FOREIGN_MARKER}/user.get"),
        "rebased-host": rendered.replace("portal.invalid", "moved.invalid").replace(_OWNED_MARKER, _ROTATED_MARKER),
        "rebased-route": rendered.replace("/rest/api/", "/rest/").replace(_OWNED_MARKER, _ROTATED_MARKER),
        "other-host-foreign": f"https://other.invalid/rest/1/{_FOREIGN_MARKER}/profile",
        "portal-oauth": "https://portal.invalid/oauth/token/?grant_type=refresh_token",
        "scheme": rendered.replace("https://", "http://", 1),
    }[target]


class _SubstitutingAuth(httpx.Auth):
    """Yield a fresh Request instead of, or before, the owned one."""

    def __init__(self, target: str, *, after_challenge: bool) -> None:
        self.target = target
        self.after_challenge = after_challenge

    async def async_auth_flow(self, request: httpx.Request) -> AsyncGenerator[httpx.Request, httpx.Response]:
        await request.aread()
        if self.after_challenge:
            response = yield request
            if response.status_code != _UNAUTHORIZED:
                return
        yield httpx.Request(
            "POST"
            if self.target in {"clone", "rotated-token", "rotated-user", "rebased-host", "rebased-route", "scheme"}
            else "GET",
            _substitute(request.url, self.target),
            headers=request.headers,
            content=request.content,
        )


_SUBSTITUTES = (
    "clone",
    "rotated-token",
    "rotated-user",
    "same-operation-foreign",
    "case-foreign",
    "other-path-foreign",
    "rebased-host",
    "rebased-route",
    "other-host-foreign",
    "portal-oauth",
    "scheme",
)


@pytest.mark.asyncio
@pytest.mark.parametrize("after_challenge", [False, True], ids=["first-yield", "after-401"])
@pytest.mark.parametrize("target", _SUBSTITUTES)
@pytest.mark.parametrize("route", [RouteKind.BARE, RouteKind.API_V3])
async def test_injected_auth_substitute_is_refused_before_dispatch(
    route: RouteKind,
    target: str,
    after_challenge: bool,  # noqa: FBT001
) -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    dispatched: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        dispatched.append(request)
        status = _UNAUTHORIZED if after_challenge else _SUCCESS_STATUS
        return httpx.Response(status, json={"result": True}, request=request)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(respond),
        auth=_SubstitutingAuth(target, after_challenge=after_challenge),
    )
    transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    try:
        with pytest.raises(TransportError, match="replaced the owned request") as caught:
            await transport.send(Request("profile", route=route), attempt_timeout=1, max_response_bytes=1024)
        assert caught.value.retryable is False
        assert all("b24api_log_owner" in request.extensions for request in dispatched)
        assert len(dispatched) == len(handler.records) == int(after_challenge)
        assert caught.value.possible_acceptance is after_challenge
        for marker in (_OWNED_MARKER, _ROTATED_MARKER, _FOREIGN_MARKER):
            assert marker not in handler.output.getvalue()
            assert all(marker not in f"{record.msg!r} {record.args!r}" for record in handler.records)
    finally:
        await transport.aclose()
        await client.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - final cleanup control


@pytest.mark.asyncio
async def test_foreign_root_redirect_to_the_owned_webhook_never_logs_its_secret() -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    owned_url = f"https://portal.invalid/rest/1/{_OWNED_MARKER}/profile"
    foreign_url = f"https://other.invalid/rest/1/{_FOREIGN_MARKER}/profile"
    hook_calls: list[str] = []

    async def hook(response: httpx.Response) -> None:
        if not hook_calls:
            hook_calls.append(str(response.url))
            await client.get(foreign_url)

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.host == "other.invalid":
            return httpx.Response(301, headers={"location": owned_url}, request=request)
        return httpx.Response(_SUCCESS_STATUS, json={"result": True}, request=request)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(respond),
        follow_redirects=True,
        event_hooks={"response": [hook]},
    )
    transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    try:
        response = await transport.send(
            Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024
        )
        assert response.status_code == _SUCCESS_STATUS
        assert len(handler.records) == _TOTAL_RECORDS
        foreign_root = next(record for record in handler.records if "other.invalid" in record.getMessage())
        assert isinstance(foreign_root.args, tuple)
        assert isinstance(foreign_root.args[1], httpx.URL)
        assert foreign_url in foreign_root.getMessage()
        for record in handler.records:
            assert _OWNED_MARKER not in f"{record.msg!r} {record.args!r}"
            assert _OWNED_MARKER not in record.getMessage()
        assert _OWNED_MARKER not in handler.output.getvalue()
    finally:
        await transport.aclose()
        await client.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - final cleanup control


@pytest.mark.asyncio
async def test_concurrent_owned_sends_with_distinct_tokens_share_one_client() -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    arrived = 0
    both_arrived = asyncio.Event()

    async def respond(request: httpx.Request) -> httpx.Response:
        nonlocal arrived
        arrived += 1
        if arrived == _OWNED_RECORDS:
            both_arrived.set()
        await both_arrived.wait()
        return httpx.Response(_SUCCESS_STATUS, json={"result": True}, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    first = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    second = HttpxTransport(f"https://portal.invalid/rest/2/{_SECOND_MARKER}/", client=client)
    try:
        await asyncio.gather(
            first.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024),
            second.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024),
        )
        assert len(handler.records) == _OWNED_RECORDS
        for marker in (_OWNED_MARKER, _SECOND_MARKER):
            assert marker not in handler.output.getvalue()
            assert all(marker not in f"{record.msg!r} {record.args!r}" for record in handler.records)
        await client.get(f"https://other.invalid/rest/1/{_FOREIGN_MARKER}/profile")
        assert _FOREIGN_MARKER in handler.records[-1].getMessage()
    finally:
        await first.aclose()
        await second.aclose()
        await client.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - final cleanup control


class _MutatingAuth(httpx.Auth):
    """Authorize the owned request in place while stripping its ownership label or rotating its URL."""

    def __init__(self, mutation: str) -> None:
        self.mutation = mutation

    async def async_auth_flow(self, request: httpx.Request) -> AsyncGenerator[httpx.Request, httpx.Response]:
        if self.mutation == "auth-replace-extensions":
            request.extensions = {key: value for key, value in request.extensions.items() if key != "b24api_log_owner"}
        else:
            request.extensions.pop("b24api_log_owner", None)
        if self.mutation == "auth-rotate-url":
            request.url = httpx.URL(str(request.url).replace(_OWNED_MARKER, _ROTATED_MARKER))
        yield request


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["auth-pop", "auth-replace-extensions", "auth-rotate-url", "request-hook-pop"])
@pytest.mark.parametrize("route", [RouteKind.BARE, RouteKind.API_V3])
async def test_mutating_the_owned_request_in_place_keeps_every_credential_private(
    route: RouteKind, mutation: str
) -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    async def strip_label(request: httpx.Request) -> None:
        request.extensions.pop("b24api_log_owner", None)

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.host == "portal.invalid":
            hop = "/rest/api/1" if route is RouteKind.API_V3 else "/rest/1"
            location = f"https://redirect.invalid{hop}/{_THIRD_MARKER}/profile"
            return httpx.Response(301, headers={"location": location}, request=request)
        return httpx.Response(_SUCCESS_STATUS, json={"result": True}, request=request)

    hooked = mutation == "request-hook-pop"
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(respond),
        follow_redirects=True,
        auth=None if hooked else _MutatingAuth(mutation),
        event_hooks={"request": [strip_label]} if hooked else None,
    )
    transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    try:
        response = await transport.send(Request("profile", route=route), attempt_timeout=1, max_response_bytes=1024)
        assert response.status_code == _SUCCESS_STATUS
        assert len(handler.records) == _REDIRECT_RECORDS
        for marker in (_OWNED_MARKER, _ROTATED_MARKER, _THIRD_MARKER):
            assert marker not in handler.output.getvalue()
            for record in handler.records:
                assert marker not in f"{record.msg!r} {record.args!r}"
                assert marker not in record.getMessage()
    finally:
        await transport.aclose()
        await client.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - final cleanup control


class _SilentAuth(httpx.Auth):
    """An auth flow that yields no request at all."""

    async def async_auth_flow(self, request: httpx.Request) -> AsyncGenerator[httpx.Request, httpx.Response]:
        if request.method == "NEVER":
            yield request


@pytest.mark.asyncio
async def test_injected_auth_yielding_nothing_is_refused_before_dispatch() -> None:
    dispatched: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        dispatched.append(request)
        return httpx.Response(_SUCCESS_STATUS, json={"result": True}, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond), auth=_SilentAuth())
    transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    try:
        with pytest.raises(TransportError, match="replaced the owned request") as caught:
            await transport.send(Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024)
        assert caught.value.retryable is False
        assert caught.value.possible_acceptance is False
        assert dispatched == []
    finally:
        await transport.aclose()
        await client.aclose()
    assert HTTPX_LOG_SHIELD._filter not in logging.getLogger("httpx").filters  # noqa: SLF001 - final cleanup control


@pytest.mark.asyncio
async def test_nested_foreign_redirect_with_a_distinct_token_stays_unchanged() -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    foreign_url = f"https://other.invalid/rest/1/{_FOREIGN_MARKER}/profile"
    hop_url = f"https://hop.invalid/rest/1/{_THIRD_MARKER}/profile"
    hook_calls: list[str] = []

    async def hook(response: httpx.Response) -> None:
        if not hook_calls:
            hook_calls.append(str(response.url))
            await client.get(foreign_url)

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.host == "other.invalid":
            return httpx.Response(301, headers={"location": hop_url}, request=request)
        return httpx.Response(_SUCCESS_STATUS, json={"result": True}, request=request)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(respond),
        follow_redirects=True,
        event_hooks={"response": [hook]},
    )
    transport = HttpxTransport(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/", client=client)
    try:
        response = await transport.send(
            Request("profile", route=RouteKind.BARE), attempt_timeout=1, max_response_bytes=1024
        )
        assert response.status_code == _SUCCESS_STATUS
        assert len(handler.records) == _TOTAL_RECORDS
        foreign = [record for record in handler.records if "portal.invalid" not in record.getMessage()]
        assert [str(record.args[1]) for record in foreign if isinstance(record.args, tuple)] == [foreign_url, hop_url]
        assert all(isinstance(record.args, tuple) and isinstance(record.args[1], httpx.URL) for record in foreign)
        assert foreign_url in handler.output.getvalue()
        assert hop_url in handler.output.getvalue()
        assert _OWNED_MARKER not in handler.output.getvalue()
    finally:
        await transport.aclose()
        await client.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - final cleanup control


def test_invalid_owned_url_does_not_leave_the_filter_in_flight() -> None:
    logger = logging.getLogger("httpx")
    HTTPX_LOG_SHIELD.register_transport()
    try:
        with (
            pytest.raises(ValueError, match="credentialed Bitrix method URL"),
            HTTPX_LOG_SHIELD.request("https://portal.invalid/profile"),
        ):
            pass
        assert HTTPX_LOG_SHIELD._in_flight == 0  # noqa: SLF001 - in-flight accounting control
    finally:
        HTTPX_LOG_SHIELD.release_transport()
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - final cleanup control


def test_record_without_an_emitting_httpx_request_is_scrubbed_conservatively() -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    HTTPX_LOG_SHIELD.register_transport()
    try:
        with HTTPX_LOG_SHIELD.request(f"https://portal.invalid/rest/1/{_OWNED_MARKER}/profile"):
            logger.info("hop %s", f"https://redirect.invalid/rest/1/{_SECOND_MARKER}/profile")
        assert len(handler.records) == 1
        assert _SECOND_MARKER not in handler.records[0].getMessage()
    finally:
        HTTPX_LOG_SHIELD.release_transport()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    assert HTTPX_LOG_SHIELD._filter not in logger.filters  # noqa: SLF001 - final cleanup control
