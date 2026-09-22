"""Scenario 17: numerator offsets 0/50/100 ignore misleading page-local total.

Offline fixture for `documentgenerator.numerator.list`: 53 independently
expected IDs, local totals 50/3/0, no `next`. A fixed wire step of 50 reaches
an empty confirmation. The report proves traversal mechanics only; a mutable
portal snapshot is not verified. Run: `uv run python -m examples.numerator_list`.
"""

from __future__ import annotations
import asyncio

from b24api import (
    Bitrix24,
    OffsetContinuation,
    OffsetSpec,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    Settings,
    TraversalAssurance,
)
from b24api.testing import ScriptedExchange, ScriptedTransport

METHOD = "documentgenerator.numerator.list"
EXPECTED_IDS = tuple(range(1, 54))
STEP = 50


def _request(offset: int) -> Request:
    return Request(METHOD, {"start": offset}, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)


def _fixture() -> ScriptedTransport:
    pages = ((0, EXPECTED_IDS[:50]), (50, EXPECTED_IDS[50:]), (100, ()))
    return ScriptedTransport(tuple(
        ScriptedExchange.json(
            _request(offset), {"result": {"numerators": [{"id": str(value)} for value in ids]}, "total": len(ids)},
        )
        for offset, ids in pages
    ))


async def run() -> None:
    """Verify all fixed-step windows without treating page-local total as exact."""
    transport = _fixture()
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    async with Bitrix24(settings, transport=transport) as client:
        stream = client.iter_list(
            Request(METHOD, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
            selector=ResultSelector(("numerators",)),
            page_size=STEP,
            offset=OffsetSpec(continuation=OffsetContinuation.FIXED_STEP, step=STEP),
        )
        observed = tuple([int(row["id"]) async for row in stream])
        if observed != EXPECTED_IDS:
            raise AssertionError("scenario 17 numeral IDs differed from independent oracle")
        if stream.report is None or not stream.report.exhausted:
            raise AssertionError("scenario 17 lacked empty-page closure")
        if stream.report.assurance is not TraversalAssurance.MECHANICS_ONLY:
            raise AssertionError("scenario 17 falsely promoted page-local total assurance")
    transport.assert_exhausted()
    if tuple(request.copy_parameters()["start"] for request in transport.calls) != (0, 50, 100):
        raise AssertionError("scenario 17 did not cover all fixed wire offsets")


if __name__ == "__main__":
    asyncio.run(run())
