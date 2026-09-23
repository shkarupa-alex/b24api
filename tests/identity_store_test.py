"""Caller-owned identity ledgers and duplicate-observed assurance for offset traversal."""

from __future__ import annotations
import json
import re
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from b24api import (
    Bitrix24,
    ConsistencyPolicy,
    DuplicatePolicy,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    IdentityStore,
    OffsetSpec,
    ReplaySafety,
    Request,
    RouteKind,
    TerminalState,
    TotalTermination,
    TraversalAssurance,
    WireResponse,
    identity_store_key,
)
from b24api.errors import BudgetExceededError, IncompleteTraversalError
from b24api.execution import Executor

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

IDENTITY = IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_INTEGER)
LEDGER_UNIQUE_ROWS = 3
REPORTED_UNIQUE_ROWS = 4
LONG_TRAVERSAL_ROWS = 2_000


class _Transport:
    host = "test.invalid"

    def __init__(self, handler: Callable[[Request], WireResponse]) -> None:
        self.handler = handler
        self.requests: list[Request] = []

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        self.requests.append(request)
        return self.handler(request)


class _SqliteLedger:
    """Application-owned ledger on the standard library, as the public contract expects."""

    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.execute("CREATE TABLE seen (key TEXT PRIMARY KEY)")

    def add_if_absent(self, key: str) -> bool:
        return self.db.execute("INSERT OR IGNORE INTO seen(key) VALUES (?)", (key,)).rowcount == 1

    def keys(self) -> list[str]:
        return [row[0] for row in self.db.execute("SELECT key FROM seen ORDER BY key")]


class _FailingLedger:
    def add_if_absent(self, key: str) -> bool:
        del key
        raise OSError("ledger unavailable")


def _pages(*pages: Sequence[int], total: int | None = None) -> Callable[[Request], WireResponse]:
    by_offset: dict[int, Sequence[int]] = {}
    offset = 0
    for page in pages:
        by_offset[offset] = page
        offset += len(page)

    def handler(request: Request) -> WireResponse:
        start = request.copy_parameters().get("start", 0)
        if start not in by_offset:
            raise AssertionError(f"traversal requested undeclared offset {start}")
        payload: dict[str, object] = {"result": [{"ID": value} for value in by_offset[start]]}
        if total is not None:
            payload["total"] = total
        return WireResponse(200, (("Content-Type", "application/json"),), json.dumps(payload).encode())

    return handler


def _client(transport: _Transport) -> Bitrix24:
    return Bitrix24._from_executor(Executor(transport))  # noqa: SLF001


def _request() -> Request:
    return Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)


def _policy(*, keys: int = 100_000, duplicates: DuplicatePolicy = DuplicatePolicy.ERROR) -> ExecutionPolicy:
    return ExecutionPolicy(max_identity_keys=keys, consistency=ConsistencyPolicy(duplicate_policy=duplicates))


def test_identity_store_key_is_canonical_and_type_distinct() -> None:
    assert identity_store_key(1) == "1"
    assert identity_store_key("1") == '"1"'
    assert identity_store_key((2, "a")) == '[2,"a"]'
    assert isinstance(_SqliteLedger(), IdentityStore)


@pytest.mark.asyncio
async def test_external_ledger_proves_uniqueness_beyond_the_in_memory_key_budget() -> None:
    ledger = _SqliteLedger()
    transport = _Transport(_pages([1, 2], [3], []))
    stream = _client(transport).iter_list(
        _request(),
        identity=IDENTITY,
        page_size=2,
        policy=_policy(keys=2),
        identity_store=ledger,
    )

    assert [row["ID"] async for row in stream] == [1, 2, 3]
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.COMPLETED
    assert report.exhausted
    assert report.assurance is TraversalAssurance.IDENTITY_EXACT
    assert report.unique_rows == LEDGER_UNIQUE_ROWS
    assert ledger.keys() == ["1", "2", "3"]
    assert [request.copy_parameters().get("start", 0) for request in transport.requests] == [0, 2, 3]


@pytest.mark.asyncio
async def test_in_memory_budget_still_fails_closed_without_a_ledger() -> None:
    stream = _client(_Transport(_pages([1, 2], [3], []))).iter_list(
        _request(),
        identity=IDENTITY,
        page_size=2,
        policy=_policy(keys=2),
    )

    with pytest.raises(BudgetExceededError):
        _ = [row async for row in stream]
    assert stream.report is not None
    assert stream.report.state is not TerminalState.COMPLETED


@pytest.mark.asyncio
async def test_external_ledger_duplicate_under_error_policy_rejects_the_page() -> None:
    stream = _client(_Transport(_pages([1, 2], [2, 3], []))).iter_list(
        _request(),
        identity=IDENTITY,
        page_size=2,
        policy=_policy(keys=2),
        identity_store=_SqliteLedger(),
    )

    with pytest.raises(IncompleteTraversalError):
        _ = [row async for row in stream]
    assert stream.report is not None
    assert stream.report.state is TerminalState.INCOMPLETE
    assert not stream.report.exhausted


@pytest.mark.asyncio
async def test_failing_ledger_never_acknowledges_or_completes() -> None:
    stream = _client(_Transport(_pages([1, 2], []))).iter_list(
        _request(),
        identity=IDENTITY,
        page_size=2,
        identity_store=_FailingLedger(),
    )

    with pytest.raises(IncompleteTraversalError):
        _ = [row async for row in stream]
    assert stream.report is not None
    assert stream.report.state is TerminalState.INCOMPLETE
    assert stream.report.emitted == 0


def test_ledger_requires_identity_and_the_public_protocol() -> None:
    client = _client(_Transport(_pages([])))
    with pytest.raises(ValueError, match="identity declaration"):
        client.iter_list(_request(), identity_store=_SqliteLedger())
    with pytest.raises(TypeError, match="add_if_absent"):
        client.iter_list(_request(), identity=IDENTITY, identity_store=object())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_counted_traversal_accepts_a_ledger_for_its_identity_evidence() -> None:
    ledger = _SqliteLedger()
    stream = _client(_Transport(_pages([1, 2], total=2))).iter_list_counted(
        _request(),
        identity=IDENTITY,
        page_size=2,
        policy=_policy(keys=1),
        identity_store=ledger,
    )

    assert [row["ID"] async for row in stream] == [1, 2]
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.assurance is TraversalAssurance.IDENTITY_AND_COUNT_MATCHED
    assert ledger.keys() == ["1", "2"]


@pytest.mark.asyncio
@pytest.mark.parametrize("ledger", [None, _SqliteLedger], ids=["memory", "external"])
async def test_reported_cross_page_duplicate_withdraws_identity_strength(ledger: type[_SqliteLedger] | None) -> None:
    stream = _client(_Transport(_pages([10, 20], [20, 30], [40], []))).iter_list(
        _request(),
        identity=IDENTITY,
        page_size=2,
        policy=_policy(duplicates=DuplicatePolicy.REPORT),
        identity_store=None if ledger is None else ledger(),
    )

    assert [row["ID"] async for row in stream] == [10, 20, 20, 30, 40]
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.COMPLETED
    assert report.assurance is TraversalAssurance.MECHANICS_ONLY
    assert report.unique_rows == REPORTED_UNIQUE_ROWS
    assert "duplicate_identity" in {violation.code for violation in report.violations}


@pytest.mark.asyncio
async def test_reported_policy_without_duplicates_keeps_identity_exact() -> None:
    stream = _client(_Transport(_pages([10, 20], [30], []))).iter_list(
        _request(),
        identity=IDENTITY,
        page_size=2,
        policy=_policy(duplicates=DuplicatePolicy.REPORT),
    )

    assert [row["ID"] async for row in stream] == [10, 20, 30]
    assert stream.report is not None
    assert stream.report.assurance is TraversalAssurance.IDENTITY_EXACT


@pytest.mark.asyncio
async def test_exact_total_duplicate_under_report_withdraws_identity_and_count_strength() -> None:
    stream = _client(_Transport(_pages([10, 20], [20, 30], total=4))).iter_list(
        _request(),
        identity=IDENTITY,
        page_size=2,
        offset=OffsetSpec(total_termination=TotalTermination.EXACT_QUALIFIED),
        policy=_policy(duplicates=DuplicatePolicy.REPORT),
    )

    _ = [row async for row in stream]
    assert stream.report is not None
    assert stream.report.assurance is not TraversalAssurance.IDENTITY_AND_COUNT_MATCHED


@pytest.mark.asyncio
async def test_external_ledger_completes_a_long_traversal_within_the_page_budget() -> None:
    ids = list(range(1, LONG_TRAVERSAL_ROWS + 1))
    ledger = _SqliteLedger()
    stream = _client(_Transport(_pages(*([value] for value in ids), []))).iter_list(
        _request(),
        identity=IDENTITY,
        page_size=1,
        policy=ExecutionPolicy(
            max_identity_keys=1,
            max_pages=LONG_TRAVERSAL_ROWS + 10,
            max_requests=LONG_TRAVERSAL_ROWS + 10,
        ),
        identity_store=ledger,
    )

    assert [row["ID"] async for row in stream] == ids
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert stream.report.assurance is TraversalAssurance.IDENTITY_EXACT
    assert stream.report.unique_rows == LONG_TRAVERSAL_ROWS


def test_ledger_documentation_names_the_page_fingerprint_bound() -> None:
    root = Path(__file__).resolve().parents[1]
    for path in (root / "README.md", root / "docs" / "performance.md", root / "b24api/traversal/identity_ledger.py"):
        text = " ".join(path.read_text(encoding="utf-8").split())
        assert re.search(r"(?<!identity )memory (?:stays )?bounded by one page", text) is None
        assert "max_pages" in text
