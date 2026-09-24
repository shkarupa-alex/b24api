"""Scenario 17: numerator offsets 0/50 ignore misleading page-local total.

Offline fixture for `documentgenerator.numerator.list`: 53 independently
expected IDs, local totals 50/3, no `next`. A fixed wire step of 50 observes a
short window that no further answer could close, so it fails closed without a
confirming request. The report
proves observed mechanics only; a mutable portal snapshot is not verified. Run:
`uv run python -m examples.numerator_list`.
"""

from __future__ import annotations
import asyncio

from b24api import (
    Bitrix24,
    IncompleteTraversalError,
    OffsetContinuation,
    OffsetSpec,
    PaginationError,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    Settings,
    TerminalState,
    TraversalAssurance,
)
from b24api.testing import ScriptedExchange, ScriptedTransport
from examples._support.evidence import RecipeEvidence

METHOD = "documentgenerator.numerator.list"
EXPECTED_IDS = tuple(range(1, 54))
STEP = 50


def _request(offset: int) -> Request:
    return Request(METHOD, {"start": offset}, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)


def _fixture() -> ScriptedTransport:
    pages = ((0, EXPECTED_IDS[:50]), (50, EXPECTED_IDS[50:]))
    return ScriptedTransport(
        tuple(
            ScriptedExchange.json(
                _request(offset),
                {"result": {"numerators": [{"id": str(value)} for value in ids]}, "total": len(ids)},
            )
            for offset, ids in pages
        )
    )


async def run() -> RecipeEvidence:
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
        observed_rows: list[int] = []
        try:
            async for row in stream:
                observed_rows.append(int(row["id"]))  # noqa: PERF401 - retain partial fail-closed evidence
        except IncompleteTraversalError as error:
            if not isinstance(error.__cause__, PaginationError):
                raise TypeError("scenario 17 failed for an unexpected reason") from error
        else:
            raise AssertionError("scenario 17 falsely proved fixed-step closure")
        observed = tuple(observed_rows)
        if observed != EXPECTED_IDS:
            raise AssertionError("scenario 17 numeral IDs differed from independent oracle")
        if stream.report is None or stream.report.state is not TerminalState.INCOMPLETE or stream.report.exhausted:
            raise AssertionError("scenario 17 did not retain its fail-closed boundary")
        if stream.report.assurance is not TraversalAssurance.MECHANICS_ONLY:
            raise AssertionError("scenario 17 falsely promoted page-local total assurance")
    transport.assert_exhausted()
    if tuple(request.copy_parameters()["start"] for request in transport.calls) != (0, 50):
        raise AssertionError("scenario 17 did not cover all fixed wire offsets")
    report = stream.report
    if report is None:
        raise AssertionError("scenario 17 lost its terminal report")
    return RecipeEvidence(len(observed), report, (report,))


if __name__ == "__main__":
    asyncio.run(run())
