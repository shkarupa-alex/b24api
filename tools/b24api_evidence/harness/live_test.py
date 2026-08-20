"""Offline transport-shape regressions for the opt-in live harness."""

from __future__ import annotations
import base64
import json
import tracemalloc
from typing import TYPE_CHECKING

import httpx
import pytest

from .contracts import ContractError
from .live import (
    ADAPTERS,
    MAX_RESPONSE_BYTES,
    LiveApiError,
    LiveCorrectnessError,
    LivePortal,
    LiveUnavailableError,
    _bounded_response_payload,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


def _portal(monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]) -> LivePortal:
    key = base64.urlsafe_b64encode(bytes(range(32))).decode().rstrip("=")
    monkeypatch.setenv("BITRIX24_EVIDENCE_FINGERPRINT_KEY", key)
    monkeypatch.setenv("BITRIX24_API_WEBHOOK_URL", "https://example.invalid" + "/rest/1/" + "not-a-secret/")
    portal = LivePortal(role="admin_full")
    portal._client.close()  # noqa: SLF001 - replace the network transport for an offline regression
    portal._client = httpx.Client(transport=httpx.MockTransport(handler))  # noqa: SLF001
    return portal


def _assert_live_error_redacted(error: BaseException, sensitive_fragment: str) -> None:
    assert sensitive_fragment not in str(error)
    assert sensitive_fragment not in repr(error.__dict__)
    assert error.__cause__ is None
    assert error.__context__ is None
    traceback = error.__traceback__
    while traceback is not None:
        if traceback.tb_frame.f_code.co_filename.endswith("harness/live.py"):
            for value in traceback.tb_frame.f_locals.values():
                assert sensitive_fragment not in repr(value)
        traceback = traceback.tb_next


def test_exact_marker_scan_follows_every_continuation(monkeypatch: pytest.MonkeyPatch) -> None:
    starts: list[int | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        parameters = json.loads(request.content)
        starts.append(parameters.get("start"))
        if len(starts) == 1:
            return httpx.Response(200, json={"result": [{"ID": 1, "TITLE": "owned"}], "next": 2})
        return httpx.Response(200, json={"result": [{"ID": 2, "TITLE": "owned"}]})

    with _portal(monkeypatch, handler) as portal:
        assert ADAPTERS["tasks-task-v1"].find_exact_marker(portal, "owned") == ["1", "2"]
        assert portal.attempts == len(starts)
    assert starts == [None, 2]


def test_live_envelope_rejects_overflow_json_and_hides_unknown_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(
        (
            httpx.Response(200, content=b'{"result":1e400}'),
            httpx.Response(200, json={"error": "Bear" + "er should-never-be-rendered"}),
        ),
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return next(responses)

    with _portal(monkeypatch, handler) as portal:
        with pytest.raises(LiveCorrectnessError, match="not JSON"):
            portal.call("scope")
        with pytest.raises(LiveCorrectnessError) as captured:
            portal.call("scope")
    assert "should-never-be-rendered" not in str(captured.value)


@pytest.mark.parametrize("payload_case", ["api_error", "malformed"])
def test_live_envelope_drops_hostile_payload_from_error_and_traceback(
    monkeypatch: pytest.MonkeyPatch,
    payload_case: str,
) -> None:
    sensitive_fragment = "synthetic-live-hostile-fragment"

    def handler(_request: httpx.Request) -> httpx.Response:
        if payload_case == "malformed":
            return httpx.Response(200, content=b'{"result":"' + sensitive_fragment.encode())
        return httpx.Response(200, json={"error": sensitive_fragment})

    expected = LiveCorrectnessError if payload_case == "malformed" else LiveApiError
    with _portal(monkeypatch, handler) as portal, pytest.raises(expected) as captured:
        portal.call("scope")

    error = captured.value
    _assert_live_error_redacted(error, sensitive_fragment)
    if isinstance(error, LiveApiError):
        assert error.code == "unexpected_api_error"


def test_live_transport_error_drops_credentialed_httpx_cause_and_locals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sensitive_fragment = "not-a-secret"

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("synthetic connection failure", request=request)

    with _portal(monkeypatch, handler) as portal, pytest.raises(LiveUnavailableError) as captured:
        portal.call("scope")

    _assert_live_error_redacted(captured.value, sensitive_fragment)


@pytest.mark.parametrize("configuration_case", ["fingerprint", "webhook"])
def test_live_portal_configuration_error_drops_credential_locals(
    monkeypatch: pytest.MonkeyPatch,
    configuration_case: str,
) -> None:
    sensitive_fragment = "synthetic-live-configuration-fragment"
    key = base64.urlsafe_b64encode(bytes(range(32))).decode().rstrip("=")
    monkeypatch.setenv("BITRIX24_EVIDENCE_FINGERPRINT_KEY", key)
    monkeypatch.setenv(
        "BITRIX24_API_WEBHOOK_URL",
        f"https://example.invalid/rest/1/{sensitive_fragment}/",
    )
    if configuration_case == "fingerprint":
        monkeypatch.setenv("BITRIX24_EVIDENCE_FINGERPRINT_KEY", sensitive_fragment)
    else:
        monkeypatch.setenv(
            "BITRIX24_API_WEBHOOK_URL",
            f"https://example.invalid:bad/rest/1/{sensitive_fragment}/",
        )

    with pytest.raises(ContractError) as captured:
        LivePortal(role="admin_full")

    _assert_live_error_redacted(captured.value, sensitive_fragment)


@pytest.mark.parametrize("response_case", ["scope", "build", "create", "missing_result"])
def test_live_semantic_errors_drop_hostile_response_locals(
    monkeypatch: pytest.MonkeyPatch,
    response_case: str,
) -> None:
    sensitive_fragment = "synthetic-live-semantic-fragment"

    def handler(request: httpx.Request) -> httpx.Response:
        if response_case == "scope":
            return httpx.Response(200, json={"result": {sensitive_fragment: "N"}})
        if response_case == "build":
            result = ["task"] if request.url.path.endswith("scope") else {"VERSION": sensitive_fragment * 5}
            return httpx.Response(200, json={"result": result})
        if response_case == "create":
            return httpx.Response(200, json={"result": {"hostile": sensitive_fragment}})
        return httpx.Response(200, json={"hostile": sensitive_fragment})

    with _portal(monkeypatch, handler) as portal:

        def invoke() -> None:
            if response_case in {"scope", "build"}:
                portal.preflight(required_scopes={"task"})
            elif response_case == "create":
                ADAPTERS["crm-deal-v1"].create(portal, "owned-marker")
            else:
                portal.call("scope")

        with pytest.raises(LiveCorrectnessError) as captured:
            invoke()

    _assert_live_error_redacted(captured.value, sensitive_fragment)


def test_live_envelope_refuses_oversized_content_before_buffering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-length": str(MAX_RESPONSE_BYTES + 1)},
            content=b"{}",
        )

    with _portal(monkeypatch, handler) as portal, pytest.raises(LiveCorrectnessError, match="byte ceiling"):
        portal.call("scope")


def test_bounded_payload_error_drops_credentialed_response_local() -> None:
    sensitive_fragment = "synthetic-bounded-response-fragment"
    request = httpx.Request(
        "GET",
        f"https://example.invalid/rest/1/{sensitive_fragment}/scope",
    )
    response = httpx.Response(200, headers={"content-length": sensitive_fragment}, request=request)

    with pytest.raises(LiveCorrectnessError) as captured:
        _bounded_response_payload(response, method="scope")

    _assert_live_error_redacted(captured.value, sensitive_fragment)


def test_live_http_status_error_drops_credentialed_response_local(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sensitive_fragment = "not-a-secret"

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"hostile": "ignored"})

    with _portal(monkeypatch, handler) as portal, pytest.raises(LiveUnavailableError) as captured:
        portal.call("scope")

    _assert_live_error_redacted(captured.value, sensitive_fragment)


@pytest.mark.parametrize(
    ("scope_result", "app_result"),
    [
        ({"task": "N"}, {"VERSION": "build-1"}),
        (["task"], {"VERSION": True}),
        (["task"], {"VERSION": " "}),
        (["task"], {"VERSION": "A" * 101}),
        (["task"], {"VERSION": int("9" * 101)}),
    ],
)
def test_live_preflight_rejects_hostile_scope_and_build_types(
    monkeypatch: pytest.MonkeyPatch,
    scope_result: object,
    app_result: object,
) -> None:
    results = iter((scope_result, app_result))

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"result": next(results)})

    with _portal(monkeypatch, handler) as portal, pytest.raises(LiveCorrectnessError):
        portal.preflight(required_scopes={"task"})


def test_live_preflight_preserves_exact_build_at_schema_ceiling(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = "A" * 99 + "X"
    results = iter((["task"], {"VERSION": expected}))

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"result": next(results)})

    with _portal(monkeypatch, handler) as portal:
        assert portal.preflight(required_scopes={"task"}).build == expected


def test_streamed_response_has_no_per_chunk_retention_amplification() -> None:
    chunk_count = 100_000

    class OneByteStream(httpx.SyncByteStream):
        def __iter__(self) -> Iterator[bytes]:
            for _ in range(chunk_count):
                yield b"x"

    response = httpx.Response(200, stream=OneByteStream())
    tracemalloc.start()
    try:
        payload = _bounded_response_payload(response, method="scope")
        _, peak = tracemalloc.get_traced_memory()
    finally:
        response.close()
        tracemalloc.stop()
    assert len(payload) == chunk_count
    assert peak < chunk_count * 10


def test_point_read_classifies_typed_not_found_without_rendering_portal_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"error": "ERROR_NOT_FOUND", "error_description": "secret-like text"})

    with _portal(monkeypatch, handler) as portal:
        assert ADAPTERS["crm-deal-v1"].read(portal, "42") is None
        assert portal.attempts == 1


def test_point_read_never_treats_not_found_substrings_as_absence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"error": "permission_not_found_but_entity_still_exists"})

    with _portal(monkeypatch, handler) as portal, pytest.raises(LiveApiError):
        ADAPTERS["crm-deal-v1"].read(portal, "42")
