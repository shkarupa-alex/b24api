"""Scenario 14: exact counted tail beyond 400 composite CRM identities.

Frozen filtered source has 582 independent `(ENTITY_TYPE_ID, ENTITY_ID)` keys.
Direct windows and a physical batch tail must cover the same 0..550 offsets.
A truncated 400-row batch tail is rejected as incomplete. The historical
582/400 portal observation is not claimed reproduced here.
Run: `uv run python -m examples.requisite_links`.
"""

from __future__ import annotations
import asyncio

from b24api import (
    Bitrix24,
    CompositeIdentitySpec,
    IdentityCoercion,
    IdentityComponent,
    IncompleteTraversalError,
    OffsetContinuation,
    OffsetSpec,
    PageOutcome,
    PageStride,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    Settings,
    TerminalState,
    TotalTermination,
    TraversalAssurance,
)
from b24api.testing import ScriptedExchange, ScriptedTransport

METHOD = "crm.requisitelink.list"
PAGE_SIZE = 50
TOTAL = 582
TRUNCATED_OFFSET = 400
EXPECTED_KEYS = tuple((2, value) for value in range(1, TOTAL + 1))
OFFSETS = tuple(range(0, TOTAL, PAGE_SIZE))


def _request(offset: int | None) -> Request:
    return Request(
        METHOD,
        {"select": ["ENTITY_TYPE_ID", "ENTITY_ID"],
         "filter": {"ENTITY_TYPE_ID": 2}, "order": {"ENTITY_ID": "ASC"},
         **({"start": offset} if offset is not None else {})},
        replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE,
    )


def _rows(offset: int) -> list[dict[str, int]]:
    return [
        {"ENTITY_TYPE_ID": entity_type, "ENTITY_ID": entity_id}
        for entity_type, entity_id in EXPECTED_KEYS[offset:offset + PAGE_SIZE]
    ]


def _result(offset: int, *, truncated: bool = False) -> dict[str, object]:
    rows = [] if truncated and offset == TRUNCATED_OFFSET else _rows(offset)
    return {"REQUISITE_LINKS": rows}


def _envelope(offset: int) -> dict[str, object]:
    return {"result": _result(offset), "total": TOTAL,
            **({"next": offset + PAGE_SIZE} if offset + PAGE_SIZE < TOTAL else {})}


def _direct_fixture() -> ScriptedTransport:
    return ScriptedTransport(tuple(
        ScriptedExchange.json(_request(offset), _envelope(offset)) for offset in OFFSETS
    ))


def _batch_fixture(*, truncated: bool = False) -> ScriptedTransport:
    tail = OFFSETS[1:]
    return ScriptedTransport((
        ScriptedExchange.json(_request(0), _envelope(0)),
        ScriptedExchange.batch(
            tuple(_request(offset) for offset in tail),
            tuple(_result(offset, truncated=truncated) for offset in tail),
            total=TOTAL,
            continuations=tuple(offset + PAGE_SIZE if offset + PAGE_SIZE < TOTAL else None for offset in tail),
        ),
    ))


def _identity() -> CompositeIdentitySpec:
    return CompositeIdentitySpec((
        IdentityComponent(("ENTITY_TYPE_ID",), IdentityCoercion.EXACT_INTEGER, "entityTypeId"),
        IdentityComponent(("ENTITY_ID",), IdentityCoercion.EXACT_INTEGER, "entityId"),
    ))


def _offset() -> OffsetSpec:
    return OffsetSpec(
        continuation=OffsetContinuation.FIXED_STEP,
        step=PAGE_SIZE,
        total_termination=TotalTermination.EXACT_QUALIFIED,
        page_stride=PageStride(PAGE_SIZE, PAGE_SIZE, PAGE_SIZE),
    )


def _key(row: object) -> tuple[int, int]:
    if not isinstance(row, dict):
        raise TypeError("requisite link row must be an object")
    left, right = row.get("ENTITY_TYPE_ID"), row.get("ENTITY_ID")
    if isinstance(left, bool) or isinstance(right, bool) or not isinstance(left, int) or not isinstance(right, int):
        raise TypeError("requisite link identity components must be integers")
    return left, right


async def _run_complete(transport: ScriptedTransport, *, counted: bool) -> tuple[tuple[int, int], ...]:
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/offline/")
    async with Bitrix24(settings, transport=transport) as client:
        stream = (
            client.iter_list_counted(
                _request(None), selector=ResultSelector(("REQUISITE_LINKS",)),
                identity=_identity(), page_size=PAGE_SIZE, offset=_offset(),
            )
            if counted else client.iter_list(
                _request(None), selector=ResultSelector(("REQUISITE_LINKS",)),
                identity=_identity(), page_size=PAGE_SIZE, offset=_offset(),
            )
        )
        keys = tuple([_key(row) async for row in stream])
        report = stream.report
        if report is None or report.state is not TerminalState.COMPLETED or not report.exhausted:
            raise AssertionError("scenario 14 lacked counted structural completion")
        if report.assurance is not TraversalAssurance.IDENTITY_AND_COUNT_MATCHED:
            raise AssertionError("scenario 14 did not retain composite identity and exact total")
        if tuple(sorted(record.offset for record in report.page_trace if record.offset is not None)) != OFFSETS:
            raise AssertionError("scenario 14 left a raw offset window unaccounted")
    transport.assert_exhausted()
    return keys


async def _reject_truncated_tail() -> None:
    transport = _batch_fixture(truncated=True)
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/offline/")
    async with Bitrix24(settings, transport=transport) as client:
        stream = client.iter_list_counted(
            _request(None), selector=ResultSelector(("REQUISITE_LINKS",)),
            identity=_identity(), page_size=PAGE_SIZE, offset=_offset(),
        )
        try:
            _ = [row async for row in stream]
        except IncompleteTraversalError:
            pass
        else:
            raise AssertionError("scenario 14 accepted a truncated batch tail")
        if stream.report is None or stream.report.state is not TerminalState.INCOMPLETE:
            raise AssertionError("scenario 14 promoted an incomplete tail to checkpoint")
        if not any(record.offset == TRUNCATED_OFFSET and record.outcome is PageOutcome.REJECTED
                   for record in stream.report.page_trace):
            raise AssertionError("scenario 14 did not identify the first divergent raw offset")
    transport.assert_exhausted()


async def run() -> None:
    """Compare direct and batched exact keys and reject a truncated tail."""
    direct = await _run_complete(_direct_fixture(), counted=False)
    batched = await _run_complete(_batch_fixture(), counted=True)
    if direct != EXPECTED_KEYS or batched != EXPECTED_KEYS:
        raise AssertionError("scenario 14 direct/batch composite keys differ from oracle")
    await _reject_truncated_tail()


if __name__ == "__main__":
    asyncio.run(run())
