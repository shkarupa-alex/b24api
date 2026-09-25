"""A boundary read the portal answers wrongly is an UNSUPPORTED verdict, not a raw error (A10).

The verifier reads the two unbounded boundary pages (ASC, then DESC) before it plans any canary.
A portal that ignores ``DESC``, ignores the page cap or returns rows without integer identities
already proves the keyset contract unusable, so the verifier reports the failed read as the
two-row check of that direction and marks every other check ``NOT_EXECUTED``.
"""

from __future__ import annotations
import json
from urllib.parse import parse_qs

import pytest

from b24api import (
    Bitrix24,
    IdentityCoercion,
    IdentitySpec,
    KeysetCapabilityError,
    KeysetSpec,
    ParameterPath,
    Request,
    ResultSelector,
    RouteKind,
    Settings,
)
from b24api.contracts.keyset_capability import (
    KeysetCapabilityCheckName,
    KeysetCapabilityCheckOutcome,
    KeysetCapabilityVerdict,
)
from b24api.contracts.keyset_execution import KeysetPhase
from b24api.contracts.report import ViolationSeverity
from b24api.transport import WireResponse

HOST = "fixture.invalid"
PAGE = 3
IDENTITIES = (1, 2, 3, 4, 5, 6)
BOUNDARY_READS = 2


class _BoundaryPortal:
    """Answer every batch command from ``IDENTITIES`` with one scripted defect."""

    host = HOST

    def __init__(self, defect: str) -> None:
        self.defect = defect
        self.commands: list[str] = []

    def _rows(self, command: str) -> list[dict[str, object]]:
        query = parse_qs(command.split("?", 1)[1])
        descending = query.get("order[id]") == ["DESC"] and self.defect != "ignores_desc"
        limit = len(IDENTITIES) if self.defect == "ignores_cap" else int(query["limit"][0])
        values = sorted(IDENTITIES, reverse=descending)[:limit]
        if self.defect == "text_identity" and descending:
            return [{"id": f"row-{value}"} for value in values]
        return [{"id": value} for value in values]

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        assert request.method == "batch"
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        results = {}
        for key, command in commands.items():
            assert isinstance(command, str)
            self.commands.append(command)
            results[key] = self._rows(command)
        payload = {"result": {"result": results, "result_error": []}}
        return WireResponse(200, (("content-type", "application/json"),), json.dumps(payload).encode())

    async def aclose(self) -> None:
        return None


async def _verify(portal: _BoundaryPortal) -> KeysetCapabilityError:
    client = Bitrix24(Settings(webhook_url=f"https://{HOST}/rest/1/kv/"), transport=portal)
    async with client:
        with pytest.raises(KeysetCapabilityError) as raised:
            await client.verify_keyset_capability(
                Request("item.list", {"select": ["id"]}, route=RouteKind.BARE),
                selector=ResultSelector.root(),
                identity=IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER),
                keyset=KeysetSpec(limit_path=ParameterPath(("limit",))),
                page_size=PAGE,
            )
    return raised.value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("defect", "failed"),
    [
        (
            "ignores_desc",
            {KeysetCapabilityCheckName.TWO_ROW_DESC: KeysetCapabilityCheckOutcome.ORDER_INVALID},
        ),
        (
            "ignores_cap",
            {
                KeysetCapabilityCheckName.TWO_ROW_ASC: KeysetCapabilityCheckOutcome.CAP_EXCEEDED,
                KeysetCapabilityCheckName.TWO_ROW_DESC: KeysetCapabilityCheckOutcome.CAP_EXCEEDED,
            },
        ),
        (
            "text_identity",
            {KeysetCapabilityCheckName.TWO_ROW_DESC: KeysetCapabilityCheckOutcome.SHAPE_INVALID},
        ),
    ],
)
async def test_failed_boundary_read_is_an_unsupported_report(
    defect: str,
    failed: dict[KeysetCapabilityCheckName, KeysetCapabilityCheckOutcome],
) -> None:
    portal = _BoundaryPortal(defect)

    error = await _verify(portal)

    report = error.report
    assert error.verdict is KeysetCapabilityVerdict.UNSUPPORTED
    assert error.retryable is False
    assert tuple(check.name for check in report.checks) == tuple(KeysetCapabilityCheckName)
    assert {check.name: check.outcome for check in report.checks} == {
        name: failed.get(name, KeysetCapabilityCheckOutcome.NOT_EXECUTED) for name in KeysetCapabilityCheckName
    }
    assert report.inconclusive_reason is None
    assert [(violation.severity, violation.code) for violation in report.violations] == [
        (ViolationSeverity.BLOCKING, f"keyset_capability_{outcome.value}") for outcome in failed.values()
    ]
    # Only the two boundary reads ran; no canary was planned from unusable boundaries.
    assert len(portal.commands) == report.logical_commands == BOUNDARY_READS
    assert report.batch_waves == report.physical_requests == 1
    assert [record.phase for record in report.page_trace] == [KeysetPhase.BOUNDARY] * BOUNDARY_READS
    assert "row-" not in repr(error.to_safe_dict())


@pytest.mark.asyncio
async def test_rows_counted_by_a_failed_boundary_read_are_reported() -> None:
    error = await _verify(_BoundaryPortal("ignores_cap"))

    rows = {check.name: check.rows_selected for check in error.report.checks}
    ascending, descending = KeysetCapabilityCheckName.TWO_ROW_ASC, KeysetCapabilityCheckName.TWO_ROW_DESC
    assert rows[ascending] == rows[descending] == len(IDENTITIES)
