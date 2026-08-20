"""Offline transport-shape regressions for the opt-in live harness."""

from __future__ import annotations
import base64
import json
from typing import TYPE_CHECKING

import httpx
import pytest

from .live import ADAPTERS, LiveCorrectnessError, LivePortal

if TYPE_CHECKING:
    from collections.abc import Callable


def _portal(monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]) -> LivePortal:
    key = base64.urlsafe_b64encode(bytes(range(32))).decode().rstrip("=")
    monkeypatch.setenv("BITRIX24_EVIDENCE_FINGERPRINT_KEY", key)
    monkeypatch.setenv("BITRIX24_API_WEBHOOK_URL", "https://example.invalid" + "/rest/1/" + "not-a-secret/")
    portal = LivePortal(role="admin_full")
    portal._client.close()  # noqa: SLF001 - replace the network transport for an offline regression
    portal._client = httpx.Client(transport=httpx.MockTransport(handler))  # noqa: SLF001
    return portal


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
