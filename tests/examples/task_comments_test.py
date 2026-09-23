"""Task comment recipe distinguishes modern, legacy, empty and denied states."""

from __future__ import annotations
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from b24api import CapabilityError, KeysetCapabilityVerdict, Request
from b24api.execution import WireResponse
from b24api.transport import TransportCapabilities, WireRequest
from examples import task_comments


class _LegacyKeysetTransport:
    host = "fixture.invalid"
    capabilities = TransportCapabilities(positional_json=True)

    def __init__(self) -> None:
        self.requests: list[Request | WireRequest] = []

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        return await self._send(request, attempt_timeout=attempt_timeout, max_response_bytes=max_response_bytes)

    async def send_wire(
        self,
        request: WireRequest,
        *,
        attempt_timeout: float,
        max_response_bytes: int,
    ) -> WireResponse:
        return await self._send(request, attempt_timeout=attempt_timeout, max_response_bytes=max_response_bytes)

    async def _send(
        self,
        request: Request | WireRequest,
        *,
        attempt_timeout: float,
        max_response_bytes: int,
    ) -> WireResponse:
        del attempt_timeout, max_response_bytes
        positional = request.positional
        assert positional is not None
        slots = positional.to_wire_slots()
        self.requests.append(request)
        order, keyset_filter = slots[1], slots[2]
        assert isinstance(order, dict)
        assert isinstance(keyset_filter, dict)
        selected = (1, 2)
        if "ID" in keyset_filter:
            selected = tuple(value for value in selected if value == int(keyset_filter["ID"]))
        else:
            if ">ID" in keyset_filter:
                selected = tuple(value for value in selected if value > int(keyset_filter[">ID"]))
            if "<ID" in keyset_filter:
                selected = tuple(value for value in selected if value < int(keyset_filter["<ID"]))
        rows = [{"ID": str(value)} for value in sorted(selected, reverse=order.get("ID") == "DESC")]
        return WireResponse(
            200,
            (("content-type", "application/json"),),
            json.dumps({"result": rows}).encode(),
        )

    async def aclose(self) -> None:
        return None


def test_task_comments_recipe_uses_correlated_batch_and_legacy_slots() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-m", "examples.task_comments"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "ENV": "PROD"},
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_task_comments_non_production_runs_real_positional_keyset_verifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ENV", raising=False)
    transport = _LegacyKeysetTransport()
    settings = task_comments.Settings(webhook_url="https://fixture.invalid/rest/1/test/")

    async with task_comments.Bitrix24(settings, transport=transport) as client:
        request = task_comments._legacy_request()  # noqa: SLF001
        report = await client.verify_keyset_capability(
            request,
            selector=task_comments.ResultSelector.root(),
            identity=task_comments.IdentitySpec(
                ("ID",),
                "ID",
                "ID",
                task_comments.IdentityCoercion.DECIMAL_STRING_INTEGER,
            ),
            page_size=2,
            keyset=task_comments.KeysetSpec(
                filter_path=task_comments.ParameterPath((2,)),
                order_path=task_comments.ParameterPath((1,)),
                start_suppression_path=None,
            ),
        )
        assert report.verdict is KeysetCapabilityVerdict.VERIFIED
        await task_comments._read_legacy(client)  # noqa: SLF001

    assert len(transport.requests) == report.physical_requests * 2 + len(task_comments.EXPECTED_LEGACY)


@pytest.mark.asyncio
async def test_positional_keyset_verifier_rejects_preseeded_controls_before_io() -> None:
    transport = _LegacyKeysetTransport()
    settings = task_comments.Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    request = task_comments._legacy_request(cursor=0, ordered=True)  # noqa: SLF001
    async with task_comments.Bitrix24(settings, transport=transport) as client:
        with pytest.raises(CapabilityError, match="conflict"):
            await client.verify_keyset_capability(
                request,
                selector=task_comments.ResultSelector.root(),
                identity=task_comments.IdentitySpec(
                    ("ID",),
                    "ID",
                    "ID",
                    task_comments.IdentityCoercion.DECIMAL_STRING_INTEGER,
                ),
                page_size=2,
                keyset=task_comments.KeysetSpec(
                    filter_path=task_comments.ParameterPath((2,)),
                    order_path=task_comments.ParameterPath((1,)),
                    start_suppression_path=None,
                ),
            )

    assert transport.requests == []
