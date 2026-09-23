"""Scenario 8: replay an updatedTime window, not just the highest item ID.

An existing smart-process item 1 changes after an initial ID sweep; new item
3 appears. A later `>id=2` request sees only 3, while the time-window replay
reconciles both through a keyed sink. This is FIXTURE behavior; the live gate
must confirm that `>=updatedTime` filters on the target portal.
Run the qualified frozen fixture: `ENV=PROD uv run python -m examples.crm_item_delta`.
"""

from __future__ import annotations
import asyncio
import os

from b24api import (
    Bitrix24,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    OperationReport,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    SequentialKeysetExecution,
    Settings,
    TraversalAssurance,
)
from b24api.testing import ScriptedExchange, ScriptedTransport
from examples._support.evidence import RecipeEvidence

METHOD = "crm.item.list"
ENTITY_TYPE_ID = 1256
INITIAL_BORDER = "2026-09-01T00:00:00+03:00"
REPLAY_BORDER = "2026-09-02T09:59:59+03:00"
T0 = "2026-09-01T12:00:00+03:00"
T1 = "2026-09-02T10:00:00+03:00"
INITIAL = ({"id": 1, "updatedTime": T0}, {"id": 2, "updatedTime": T0})
DELTA = ({"id": 1, "updatedTime": T1}, {"id": 3, "updatedTime": T1})
EXPECTED_IDS = (1, 2, 3)


def _request(border: str, cursor: int | None = None) -> Request:
    return Request(
        METHOD,
        {
            "entityTypeId": ENTITY_TYPE_ID,
            "select": ["id", "updatedTime"],
            "filter": {">=updatedTime": border, **({">id": cursor} if cursor is not None else {})},
            "order": {"id": "ASC"},
            "start": -1,
        },
        replay_safety=ReplaySafety.SAFE,
        route=RouteKind.BARE,
    )


def _verifier_request(
    border: str,
    direction: str,
    lower: int | None,
    upper: int | None,
) -> Request:
    filters: dict[str, object] = {
        ">=updatedTime": border,
        **({">id": lower} if lower is not None else {}),
        **({"<id": upper} if upper is not None else {}),
    }
    return Request(
        METHOD,
        {
            "entityTypeId": ENTITY_TYPE_ID,
            "select": ["id", "updatedTime"],
            "filter": filters,
            "order": {"id": direction},
            "start": -1,
        },
        replay_safety=ReplaySafety.SAFE,
        route=RouteKind.BARE,
    )


def _verifier_exchanges(border: str, rows: tuple[dict[str, object], ...]) -> tuple[ScriptedExchange, ...]:
    identities = tuple(int(row["id"]) for row in rows)

    def selected(direction: str, lower: int | None, upper: int | None) -> dict[str, object]:
        values = tuple(
            row
            for row in rows
            if (lower is None or int(row["id"]) > lower) and (upper is None or int(row["id"]) < upper)
        )
        return {"items": list(reversed(values)) if direction == "DESC" else list(values)}

    low, high = min(identities), max(identities)
    boundaries = (("ASC", None, None), ("DESC", None, None))
    canaries = (
        ("ASC", low - 1, low),
        ("ASC", low, low + 1),
        ("ASC", low - 1, low + 1),
        ("ASC", low - 1, high + 1),
        ("DESC", low - 1, high + 1),
    )
    return (
        ScriptedExchange.batch(
            tuple(_verifier_request(border, *control) for control in boundaries),
            tuple(selected(*control) for control in boundaries),
            total=len(rows),
        ),
        ScriptedExchange.batch(
            tuple(_verifier_request(border, *control) for control in canaries),
            tuple(selected(*control) for control in canaries),
            total=len(rows),
        ),
    )


def _fixture() -> ScriptedTransport:
    initial_verifier = _verifier_exchanges(INITIAL_BORDER, INITIAL) if os.environ.get("ENV") != "PROD" else ()
    delta_verifier = _verifier_exchanges(REPLAY_BORDER, DELTA) if os.environ.get("ENV") != "PROD" else ()
    return ScriptedTransport(
        (
            ScriptedExchange.json(
                Request("crm.type.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
                {"result": {"types": [{"title": "Mirror fixture", "entityTypeId": ENTITY_TYPE_ID}]}},
            ),
            *initial_verifier,
            ScriptedExchange.json(_request(INITIAL_BORDER), {"result": {"items": list(INITIAL)}}),
            ScriptedExchange.json(_request(INITIAL_BORDER, 2), {"result": {"items": []}}),
            ScriptedExchange.json(_request(INITIAL_BORDER, 2), {"result": {"items": [DELTA[1]]}}),
            *delta_verifier,
            ScriptedExchange.json(_request(REPLAY_BORDER), {"result": {"items": list(DELTA)}}),
            ScriptedExchange.json(_request(REPLAY_BORDER, 3), {"result": {"items": []}}),
        )
    )


def _item(row: object) -> tuple[int, str]:
    if not isinstance(row, dict):
        raise TypeError("CRM item must be an object")
    item_id, mark = row.get("id"), row.get("updatedTime")
    if isinstance(item_id, bool) or not isinstance(item_id, int) or not isinstance(mark, str):
        raise TypeError("CRM item must have integer id and string timestamp")
    return item_id, mark


async def _scan_window(
    client: Bitrix24,
    border: str,
    expected: tuple[dict[str, object], ...],
) -> tuple[dict[int, str], OperationReport]:
    request = Request(
        METHOD,
        {"entityTypeId": ENTITY_TYPE_ID, "select": ["id", "updatedTime"], "filter": {">=updatedTime": border}},
        replay_safety=ReplaySafety.SAFE,
        route=RouteKind.BARE,
    )
    selector = ResultSelector(("items",))
    identity = IdentitySpec(("id",), "id", "id", IdentityCoercion.EXACT_INTEGER)
    keyset = KeysetSpec()
    if os.environ.get("ENV") != "PROD":
        # An endpoint accepting ID filters may still ignore strict bounds or ordering.
        # Keep this fail-closed guard until this exact portal/request is qualified;
        # after qualification, set ENV=PROD or deliberately remove the guard.
        await client.verify_keyset_capability(
            request,
            selector=selector,
            identity=identity,
            page_size=2,
            keyset=keyset,
        )
    stream = client.iter_list_keyset(
        request,
        selector=selector,
        identity=identity,
        page_size=2,
        keyset=keyset,
        execution=SequentialKeysetExecution(),
    )
    observed = tuple([_item(row) async for row in stream])
    if observed != tuple(_item(row) for row in expected):
        raise AssertionError("scenario 8 keyset window differed from oracle")
    if stream.report is None or not stream.report.exhausted:
        raise AssertionError("scenario 8 keyset window lacked empty confirmation")
    if stream.report.assurance is not TraversalAssurance.IDENTITY_EXACT:
        raise AssertionError("scenario 8 ID traversal lacked identity assurance")
    return dict(observed), stream.report


async def run() -> RecipeEvidence:
    """Compare an ID-only cursor with a public time-filtered keyset replay."""
    transport = _fixture()
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    sink: dict[int, str] = {}
    async with Bitrix24(settings, transport=transport) as client:
        types = await client.call(Request("crm.type.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE))
        if not isinstance(types, dict):
            raise TypeError("scenario 8 type discovery did not return types")
        type_rows = types.get("types")
        if not isinstance(type_rows, list) or len(type_rows) != 1 or not isinstance(type_rows[0], dict):
            raise TypeError("scenario 8 type discovery did not return the expected row")
        if type_rows[0].get("entityTypeId") != ENTITY_TYPE_ID:
            raise AssertionError("scenario 8 discovered the wrong smart process")
        initial, initial_report = await _scan_window(client, INITIAL_BORDER, INITIAL)
        sink.update(initial)
        id_only = await client.call(_request(INITIAL_BORDER, 2))
        if not isinstance(id_only, dict) or id_only.get("items") != [DELTA[1]]:
            raise AssertionError("scenario 8 ID-only baseline did not miss the edited old ID")
        delta, delta_report = await _scan_window(client, REPLAY_BORDER, DELTA)
        sink.update(delta)
    transport.assert_exhausted()
    if tuple(sorted(sink)) != EXPECTED_IDS or sink[1] != T1 or sink[3] != T1:
        raise AssertionError("scenario 8 keyed sink missed the edit or new item")
    return RecipeEvidence(len(sink), delta_report, (initial_report, delta_report))


if __name__ == "__main__":
    asyncio.run(run())
