"""Scenario 15: bounded recursive Disk mirror with counted folder closure.

Two storage roots lead to four distinct real folders. One folder has 53
children and therefore a physical batch tail; links point to an already
queued folder and back to a root. Every expected object ID is retained once.
Deleted objects cannot appear because the endpoint forces active-only rows.
Run: `uv run python -m examples.disk_mirror`.
"""

from __future__ import annotations
import asyncio
from collections import deque

from b24api import (
    Bitrix24,
    IdentityCoercion,
    IdentitySpec,
    OffsetContinuation,
    OffsetSpec,
    OperationReport,
    PageStride,
    ReplaySafety,
    Request,
    RouteKind,
    Settings,
    TerminalState,
    TotalTermination,
    TraversalAssurance,
)
from b24api.testing import ScriptedExchange, ScriptedTransport
from examples._support.evidence import RecipeEvidence

STORAGE_METHOD = "disk.storage.getlist"
CHILDREN_METHOD = "disk.folder.getchildren"
PAGE_SIZE = 50
MAX_FOLDERS = 10
ROOTS = (100, 200)
FOLDER_IDS = (100, 200, 120, 130)
EXPECTED_OBJECT_IDS = tuple(sorted({*ROOTS, *range(1001, 1054), 120, 130, 301, 302, 2001}))


def _storage_request(offset: int | None) -> Request:
    return Request(
        STORAGE_METHOD,
        {"order": {"ID": "ASC"}, **({"start": offset} if offset is not None else {})},
        replay_safety=ReplaySafety.SAFE,
        route=RouteKind.BARE,
    )


def _children_request(folder_id: int, offset: int | None) -> Request:
    return Request(
        CHILDREN_METHOD,
        {"id": folder_id, "order": {"ID": "ASC"}, **({"start": offset} if offset is not None else {})},
        replay_safety=ReplaySafety.SAFE,
        route=RouteKind.BARE,
    )


def _folder(object_id: int, real_id: int) -> dict[str, str]:
    return {"ID": str(object_id), "TYPE": "folder", "REAL_OBJECT_ID": str(real_id), "DELETED_TYPE": "0"}


def _file(object_id: int) -> dict[str, str]:
    return {"ID": str(object_id), "TYPE": "file", "DELETED_TYPE": "0"}


def _fixture() -> ScriptedTransport:
    root_children = [*(_file(value) for value in range(1001, 1051))]
    root_children.extend((_folder(120, 120), _folder(301, 120), _file(1051)))
    return ScriptedTransport(
        (
            ScriptedExchange.json(
                _storage_request(0),
                {"result": [{"ID": "1", "ROOT_OBJECT_ID": "100"}, {"ID": "2", "ROOT_OBJECT_ID": "200"}], "total": 2},
            ),
            ScriptedExchange.json(
                _children_request(100, 0),
                {"result": root_children[:PAGE_SIZE], "total": len(root_children)},
            ),
            ScriptedExchange.batch(
                (_children_request(100, 50),),
                (root_children[PAGE_SIZE:],),
                total=len(root_children),
            ),
            ScriptedExchange.json(
                _children_request(200, 0),
                {"result": [_file(2001)], "total": 1},
            ),
            ScriptedExchange.json(
                _children_request(120, 0),
                {"result": [_folder(130, 130), _file(1052)], "total": 2},
            ),
            ScriptedExchange.json(
                _children_request(130, 0),
                {"result": [_folder(302, 100), _file(1053)], "total": 2},
            ),
        )
    )


def _offset() -> OffsetSpec:
    return OffsetSpec(
        continuation=OffsetContinuation.FIXED_STEP,
        step=PAGE_SIZE,
        total_termination=TotalTermination.EXACT_QUALIFIED,
        page_stride=PageStride(PAGE_SIZE, PAGE_SIZE, PAGE_SIZE),
    )


def _object_id(row: object, key: str) -> int:
    if not isinstance(row, dict):
        raise TypeError("Disk object must be a mapping")
    value = row.get(key)
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise TypeError("Disk object identity must be a string or integer")
    return int(value)


async def _counted_rows(
    client: Bitrix24,
    request: Request,
    *,
    expected_offsets: tuple[int, ...],
) -> tuple[list[object], OperationReport]:
    stream = client.iter_list_counted(
        request,
        identity=IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.DECIMAL_STRING_INTEGER),
        page_size=PAGE_SIZE,
        offset=_offset(),
    )
    rows: list[object] = [row async for row in stream]
    report = stream.report
    if report is None or report.state is not TerminalState.COMPLETED or not report.exhausted:
        raise AssertionError("scenario 15 folder did not reach counted completion")
    if report.assurance is not TraversalAssurance.IDENTITY_AND_COUNT_MATCHED:
        raise AssertionError("scenario 15 folder lacked identity/total assurance")
    offsets = tuple(sorted(record.offset for record in report.page_trace if record.offset is not None))
    if offsets != expected_offsets:
        raise AssertionError("scenario 15 folder left an offset window uncovered")
    return rows, report


async def run() -> RecipeEvidence:
    """Traverse a bounded folder queue without following link cycles."""
    transport = _fixture()
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    async with Bitrix24(settings, transport=transport) as client:
        reports = []
        storages, storage_report = await _counted_rows(client, _storage_request(None), expected_offsets=(0,))
        reports.append(storage_report)
        roots = tuple(_object_id(row, "ROOT_OBJECT_ID") for row in storages)
        if roots != ROOTS:
            raise AssertionError("scenario 15 storage roots differed from oracle")
        queue = deque(roots)
        scheduled_real = set(roots)
        visited_real: list[int] = []
        object_ids = set(roots)
        while queue:
            if len(visited_real) >= MAX_FOLDERS:
                raise AssertionError("scenario 15 exceeded the bounded folder budget")
            folder_id = queue.popleft()
            visited_real.append(folder_id)
            offsets = (0, PAGE_SIZE) if folder_id == ROOTS[0] else (0,)
            children, report = await _counted_rows(client, _children_request(folder_id, None), expected_offsets=offsets)
            reports.append(report)
            for child in children:
                object_ids.add(_object_id(child, "ID"))
                if not isinstance(child, dict) or child.get("TYPE") != "folder":
                    continue
                real_id = _object_id(child, "REAL_OBJECT_ID")
                if real_id not in scheduled_real:
                    scheduled_real.add(real_id)
                    queue.append(real_id)
        if tuple(visited_real) != FOLDER_IDS:
            raise AssertionError("scenario 15 link caused a repeated or missing folder fetch")
        if tuple(sorted(object_ids)) != EXPECTED_OBJECT_IDS:
            raise AssertionError("scenario 15 object IDs differed from independent oracle")
    transport.assert_exhausted()
    return RecipeEvidence(len(object_ids), reports[-1], tuple(reports))


if __name__ == "__main__":
    asyncio.run(run())
