"""Scenario 10: timeline comments per entity with one shared storage ID.

The deal has 53 comments over two fixed offset pages; a contact binds one of
the same IDs; a third entity denies access. Reference outcomes remain separate
while caller storage is keyed by comment ID. A direct `>ID` probe repeats the
first page because this filter key is ignored. This is an offline fixture.
Run: `uv run python -m examples.timeline_comments`.
"""

from __future__ import annotations
import asyncio

from b24api import (
    ApiResponseError,
    Binding,
    Bitrix24,
    DirectDispatch,
    IdentityCoercion,
    IdentitySpec,
    OffsetContinuation,
    OffsetSpec,
    OperationReport,
    PageStride,
    ParameterPath,
    ParameterUpdate,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    SequentialTraversal,
    Settings,
    TerminalState,
    TotalTermination,
)
from b24api.testing import ScriptedExchange, ScriptedTransport
from examples._support.evidence import RecipeEvidence

METHOD = "crm.timeline.comment.list"
PAGE_SIZE = 50
DEAL_IDS = tuple(range(1000, 1053))
EXPECTED_STORAGE_IDS = DEAL_IDS
SELECT = ["ID", "ENTITY_TYPE", "ENTITY_ID", "CREATED", "COMMENT"]
ENTITIES = (("deal", 71), ("contact", 22), ("lead", 999))


def _request(entity_type: str, entity_id: int, offset: int, *, cursor_probe: bool = False) -> Request:
    return Request(
        METHOD,
        {
            "filter": {"ENTITY_TYPE": entity_type, "ENTITY_ID": entity_id, **({">ID": 1052} if cursor_probe else {})},
            "select": SELECT,
            "order": {"ID": "ASC"},
            "start": offset,
        },
        replay_safety=ReplaySafety.SAFE,
        route=RouteKind.BARE,
    )


def _row(comment_id: int, entity_type: str, entity_id: int) -> dict[str, str]:
    return {
        "ID": str(comment_id),
        "ENTITY_TYPE": entity_type,
        "ENTITY_ID": str(entity_id),
        "CREATED": "2026-09-22T10:00:00+03:00",
        "COMMENT": "fixture",
    }


def _fixture() -> ScriptedTransport:
    return ScriptedTransport(
        (
            ScriptedExchange.json(
                _request("deal", 71, 0),
                {"result": [_row(value, "deal", 71) for value in DEAL_IDS[:PAGE_SIZE]], "total": len(DEAL_IDS)},
            ),
            ScriptedExchange.json(
                _request("deal", 71, 50),
                {"result": [_row(value, "deal", 71) for value in DEAL_IDS[PAGE_SIZE:]], "total": len(DEAL_IDS)},
            ),
            ScriptedExchange.json(
                _request("contact", 22, 0),
                {"result": [_row(1000, "contact", 22)], "total": 1},
            ),
            ScriptedExchange.json(
                _request("lead", 999, 0),
                {"error": "", "error_description": "Access denied."},
            ),
            ScriptedExchange.json(
                _request("deal", 71, 0, cursor_probe=True),
                {"result": [_row(value, "deal", 71) for value in DEAL_IDS[:PAGE_SIZE]], "total": len(DEAL_IDS)},
            ),
        )
    )


def _id(row: object) -> int:
    if not isinstance(row, dict):
        raise TypeError("timeline comment must be an object")
    value = row.get("ID")
    if not isinstance(value, str):
        raise TypeError("timeline comment ID must be a string")
    return int(value)


def _verify_outcomes(
    per_entity: dict[tuple[str, int], list[int]],
    completed: set[tuple[str, int]],
    failures: dict[tuple[str, int], ReferenceFailure[tuple[str, int]]],
    report: OperationReport | None,
) -> None:
    if tuple(per_entity[("deal", 71)]) != DEAL_IDS or per_entity[("contact", 22)] != [1000]:
        raise AssertionError("scenario 10 per-entity comments differed from oracle")
    if completed != {("deal", 71), ("contact", 22)} or set(failures) != {("lead", 999)}:
        raise AssertionError("scenario 10 lost separate reference terminals")
    if not isinstance(failures[("lead", 999)].error, ApiResponseError):
        raise TypeError("scenario 10 access denial lost its typed error")
    if report is None or report.state is not TerminalState.COMPLETED_WITH_FAILURES:
        raise AssertionError("scenario 10 denied entity did not weaken global completion")
    storage = {comment_id for ids in per_entity.values() for comment_id in ids}
    if tuple(sorted(storage)) != EXPECTED_STORAGE_IDS:
        raise AssertionError("scenario 10 keyed storage differs from independent oracle")


async def run() -> RecipeEvidence:
    """Prove per-entity completion and caller-owned global deduplication."""
    transport = _fixture()
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    async with Bitrix24(settings, transport=transport) as client:
        base = Request(
            METHOD,
            {"filter": {"ENTITY_TYPE": "deal", "ENTITY_ID": 71}, "select": SELECT, "order": {"ID": "ASC"}},
            replay_safety=ReplaySafety.SAFE,
            route=RouteKind.BARE,
        )
        bindings = tuple(
            Binding(
                f"{entity_type}:{entity_id}",
                (
                    ParameterUpdate(ParameterPath(("filter", "ENTITY_TYPE")), entity_type),
                    ParameterUpdate(ParameterPath(("filter", "ENTITY_ID")), entity_id),
                ),
                (entity_type, entity_id),
            )
            for entity_type, entity_id in ENTITIES
        )
        stream = client.iter_reference_outcomes(
            base,
            bindings,
            traversal=SequentialTraversal(
                selector=ResultSelector.root(),
                identity=IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.DECIMAL_STRING_INTEGER),
                page_size=PAGE_SIZE,
                offset=OffsetSpec(
                    continuation=OffsetContinuation.FIXED_STEP,
                    step=PAGE_SIZE,
                    total_termination=TotalTermination.EXACT_QUALIFIED,
                    page_stride=PageStride(PAGE_SIZE, PAGE_SIZE, PAGE_SIZE),
                ),
            ),
            dispatch=DirectDispatch(concurrency=1),
        )
        per_entity: dict[tuple[str, int], list[int]] = {key: [] for key in ENTITIES}
        completed: set[tuple[str, int]] = set()
        failures: dict[tuple[str, int], ReferenceFailure[tuple[str, int]]] = {}
        async for outcome in stream:
            if isinstance(outcome, ReferenceItem):
                per_entity[outcome.correlation].append(_id(outcome.item))
            elif isinstance(outcome, ReferenceComplete):
                completed.add(outcome.correlation)
            elif isinstance(outcome, ReferenceFailure):
                failures[outcome.correlation] = outcome
        _verify_outcomes(per_entity, completed, failures, stream.report)
        probe = await client.call(_request("deal", 71, 0, cursor_probe=True))
        if not isinstance(probe, list) or tuple(_id(row) for row in probe) != DEAL_IDS[:PAGE_SIZE]:
            raise AssertionError("scenario 10 >ID negative baseline did not repeat first page")
    transport.assert_exhausted()
    report = stream.report
    if report is None:
        raise AssertionError("scenario 10 lost its terminal report")
    storage = {comment_id for ids in per_entity.values() for comment_id in ids}
    return RecipeEvidence(len(storage), report, (report,))


if __name__ == "__main__":
    asyncio.run(run())
