"""Fast and automatic keyset references on the shared queue (#27).

Acceptance: A27, A27-SHARED, A27-N, A27-VIEW, A27-RETAIN, A27-WAVE1, A27-GRANT-LOW, A27-GRANT-OK,
A27-INPUT-TIGHT, A27-INPUT-OK.

Every binding runs its own keyset runtime under a row lease ``G`` of the shared row pool and sends
its waves through the shared dispatcher; a bounded plan may trim ``G`` once it has planned. The
oracle for a binding is the flat traversal of the same selection under ``max_buffered_rows=G``:
selection, geometry and cost must match it.
"""

# ruff: noqa: PLR2004

from __future__ import annotations
import asyncio
import dataclasses
import json
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import pytest

from b24api import (
    AutoKeysetExecution,
    BatchDispatch,
    Binding,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    KeysetTraversal,
    ParameterPath,
    ParameterUpdate,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    ReplaySafety,
    Request,
    ResultSelector,
    SequentialKeysetExecution,
    StableIntegerKeysetContract,
    TerminalState,
)
from b24api.batch.engine import BatchExecutor
from b24api.completion import CompletionGate
from b24api.contracts import (
    DeliveryOrder,
    KeysetExecutionKind,
    KeysetSelectionReason,
    KeysetSelectionSummary,
    PageDispatch,
    PageOutcome,
    PageRejectionCode,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ReferenceNotExecuted,
    ReferenceOutcomeUnknown,
    ReferenceSource,
)
from b24api.contracts.completion import (
    CommandSettlement,
    PageAcknowledged,
    PageCommandOutcome,
    PageRejected,
    PageScheduled,
)
from b24api.contracts.page_stop import ContinuePage
from b24api.contracts.policy import KernelState, RetryPolicy
from b24api.contracts.request import RouteKind
from b24api.errors import (
    ApiResponseError,
    BudgetExceededError,
    CapabilityError,
    IdentityContractError,
    IncompleteTraversalError,
    PaginationError,
    ProtocolError,
)
from b24api.execution import ExecutionContext, Executor, RateCoordinator, WorkClass
from b24api.references import wave_engine
from b24api.references.budget_view import BindingBudgetView, RowLease
from b24api.references.dispatch import _RowBuffer
from b24api.transport import WireResponse
from b24api.traversal.keyset_fast_plan import fit_wave
from b24api.traversal.keyset_runtime import KeysetRuntime
from tests.scripting import HANG_BOUND, BatchPortal, ResponderTransport, client_for

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from b24api.contracts import KeysetExecutionReport, ReferenceOutcome
    from b24api.contracts.completion import CompletionEvidence
    from b24api.contracts.report import OperationReport
    from b24api.contracts.stream import OperationStream
    from b24api.execution.executor import _Pause, _RequestAttempts
    from b24api.traversal.keyset_context import KeysetContext
    from tests.scripting import Envelope, Query

PAGE = 10
LANES = 4
_CONTRACT = StableIntegerKeysetContract()
_BROKEN_ID = 77

type Execution = RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution


def _range() -> RangeKeysetExecution:
    return RangeKeysetExecution(_CONTRACT)


def _partitioned(lanes: int = LANES) -> PartitionedKeysetExecution:
    return PartitionedKeysetExecution(_CONTRACT, target_lanes=lanes)


def _auto(lanes: int = LANES) -> AutoKeysetExecution:
    return AutoKeysetExecution(_CONTRACT, target_lanes=lanes)


DISTRIBUTIONS: dict[str, tuple[int, ...]] = {
    "dense": tuple(range(1, 201)),
    "skewed": (*range(1, 151), *range(10_000, 10_051)),
    "sparse": tuple(range(37, 5_000, 37)),
}


def _rows(ids: tuple[int, ...], query: Query, *, ignore_bounds: bool = False) -> list[dict[str, int]]:
    selected = ids
    if not ignore_bounds:
        if "filter[>ID]" in query:
            selected = tuple(value for value in selected if value > int(query["filter[>ID]"]))
        if "filter[<ID]" in query:
            selected = tuple(value for value in selected if value < int(query["filter[<ID]"]))
    descending = query.get("order[id]") == "DESC"
    limit = int(query.get("limit", PAGE))
    return [{"id": value} for value in sorted(selected, reverse=descending)[:limit]]


def _answer(
    data: Mapping[str, tuple[int, ...]], *, ignore_bounds: frozenset[str] = frozenset()
) -> Callable[..., Envelope]:
    """Serve each parent's own stable integer selection from ``filter[=parent]``."""

    def answer(method: str, query: Query) -> Envelope:
        del method
        parent = query.get("filter[=parent]", "")
        return {"result": _rows(data[parent], query, ignore_bounds=parent in ignore_bounds)}

    return answer


def _flat_answer(ids: tuple[int, ...]) -> Callable[..., Envelope]:
    def answer(method: str, query: Query) -> Envelope:
        del method
        return {"result": _rows(ids, query)}

    return answer


def _identity() -> IdentitySpec:
    return IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER)


def _keyset() -> KeysetSpec:
    return KeysetSpec(limit_path=ParameterPath(("limit",)))


def _traversal(execution: Execution | SequentialKeysetExecution) -> KeysetTraversal:
    return KeysetTraversal(
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE,
        keyset=_keyset(),
        execution=execution,
    )


def _base() -> Request:
    return Request("item.list", {"filter": {"STATUS": "open"}}, route=RouteKind.BARE)


def _parent(name: str, correlation: object = None) -> Binding[object]:
    return Binding(
        name,
        (ParameterUpdate(ParameterPath(("filter", "=parent")), name),),
        name if correlation is None else correlation,
    )


@dataclass
class _Leases:
    """Spy on the shared pool: every lease granted, the leases outstanding and their high water."""

    granted: list[tuple[int, int]] = field(default_factory=list)
    high_water: int = 0
    pools: list[_RowBuffer] = field(default_factory=list)
    live: list[object] = field(default_factory=list)

    @property
    def outstanding(self) -> int:
        """Return the rows still held by granted leases (a closed pool zeroes every reservation)."""
        return sum(getattr(reservation, "amount", 0) for reservation in self.live)


@pytest.fixture
def leases(monkeypatch: pytest.MonkeyPatch) -> _Leases:
    spy = _Leases()
    lease = _RowBuffer.lease

    async def tracked_lease(self: _RowBuffer, index: int, grant: Callable[[int], int | None]) -> object:
        reservation = await lease(self, index, grant)
        spy.granted.append((index, reservation.amount))
        spy.live.append(reservation)
        spy.high_water = max(spy.high_water, spy.outstanding)
        if self not in spy.pools:
            spy.pools.append(self)
        return reservation

    monkeypatch.setattr(_RowBuffer, "lease", tracked_lease)
    return spy


async def _drain[T](stream: OperationStream[T]) -> tuple[list[T], OperationReport]:
    async with stream:
        items = [item async for item in stream]
    report = stream.report
    assert report is not None
    return items, report


def _rows_by_parent(outcomes: list[ReferenceOutcome[object]]) -> dict[object, list[int]]:
    rows: dict[object, list[int]] = {}
    for outcome in outcomes:
        if isinstance(outcome, ReferenceItem):
            item = cast("dict[str, int]", outcome.item)
            rows.setdefault(outcome.correlation, []).append(item["id"])
    return rows


def _completes(outcomes: list[ReferenceOutcome[object]]) -> dict[object, ReferenceComplete[object]]:
    return {outcome.correlation: outcome for outcome in outcomes if isinstance(outcome, ReferenceComplete)}


async def _flat_oracle(ids: tuple[int, ...], execution: Execution, rows: int) -> KeysetExecutionReport:
    """Run the flat traversal of one selection under ``max_buffered_rows=rows``; return its evidence."""
    portal = BatchPortal(_flat_answer(ids))
    async with client_for(portal, policy=ExecutionPolicy(max_buffered_rows=rows)) as client:
        stream = client.iter_list_keyset(
            _base(),
            selector=ResultSelector.root(),
            identity=_identity(),
            page_size=PAGE,
            keyset=_keyset(),
            execution=execution,
        )
        delivered, report = await _drain(stream)
    assert [cast("dict[str, int]", row)["id"] for row in delivered] == sorted(ids)
    assert report.keyset_execution is not None
    return report.keyset_execution


def _cost(report: KeysetExecutionReport) -> dict[str, object]:
    """Selection, geometry and cost of one fast run: every evidence field but the shared batch counts."""
    return {
        item.name: getattr(report, item.name)
        for item in dataclasses.fields(report)
        if item.name not in {"batch_requests", "batch_commands", "page_trace_class_counts"}
    }


# A27 / A27-SHARED: completeness and flat-oracle equality across executions, fan-out and density.


@pytest.mark.parametrize("execution", [_range(), _partitioned(), _auto()], ids=["range", "partitioned", "auto"])
@pytest.mark.parametrize("parents", [1, 4, 16])
@pytest.mark.parametrize("distribution", sorted(DISTRIBUTIONS))
@pytest.mark.asyncio
async def test_fast_references_match_the_flat_oracle_at_their_lease(
    execution: Execution,
    parents: int,
    distribution: str,
    leases: _Leases,
) -> None:
    ids = DISTRIBUTIONS[distribution]
    names = [f"p{index}" for index in range(parents)]
    # Every parent holds the same identities: per-binding identity state must keep them apart.
    portal = BatchPortal(_answer(dict.fromkeys(names, ids)))
    # Logical pages are one operation budget: sixteen sparse range sweeps need more than one flat sweep.
    policy = ExecutionPolicy(max_pages=50_000)
    async with client_for(portal, policy=policy) as client:
        outcomes, report = await _drain(
            client.iter_reference_outcomes(
                _base(),
                [_parent(name) for name in names],
                traversal=_traversal(execution),
                dispatch=BatchDispatch(batch_size=50),
            )
        )

    rows = _rows_by_parent(outcomes)
    completes = _completes(outcomes)
    assert set(rows) == set(names) == set(completes)
    assert all(rows[name] == sorted(ids) for name in names)
    assert all(complete.exhausted and complete.row_count == len(ids) for complete in completes.values())
    assert report.state is TerminalState.COMPLETED
    assert leases.outstanding == 0
    assert all(pool.maximum == ExecutionPolicy().max_buffered_rows for pool in leases.pools)
    grants = dict(leases.granted)
    for index, name in enumerate(names):
        evidence = completes[name].keyset_execution
        assert evidence is not None
        assert completes[name].keyset_selection == KeysetSelectionSummary.of(evidence)
        oracle = await _flat_oracle(ids, execution, grants[index])
        assert _cost(evidence) == _cost(oracle)


# A27-N: fail closed on bounds the binding or the server breaks; planning cancel; page stop.


@pytest.mark.asyncio
async def test_a_binding_that_writes_a_fast_owned_bound_fails_closed_before_any_request() -> None:
    portal = BatchPortal(_answer({"good": DISTRIBUTIONS["dense"], "bad": DISTRIBUTIONS["dense"]}))
    bad = Binding(
        "bad",
        (
            ParameterUpdate(ParameterPath(("filter", "=parent")), "bad"),
            ParameterUpdate(ParameterPath(("filter", ">ID")), 5),
        ),
        "bad",
    )
    async with client_for(portal) as client:
        outcomes, _report = await _drain(
            client.iter_reference_outcomes(_base(), [_parent("good"), bad], traversal=_traversal(_auto()))
        )

    terminal = {outcome.correlation: outcome for outcome in outcomes if not isinstance(outcome, ReferenceItem)}
    assert isinstance(terminal["good"], ReferenceComplete)
    assert isinstance(terminal["bad"], ReferenceNotExecuted | ReferenceFailure)
    assert "bad" not in _rows_by_parent(outcomes)
    assert all(query.get("filter[=parent]") != "bad" for query in portal.queries)


@pytest.mark.parametrize("execution", [_range(), _partitioned(), _auto()], ids=["range", "partitioned", "auto"])
@pytest.mark.asyncio
async def test_a_server_ignoring_bounds_fails_its_binding_closed_while_neighbours_complete(
    execution: Execution,
    leases: _Leases,
) -> None:
    portal = BatchPortal(
        _answer({"honest": DISTRIBUTIONS["dense"], "liar": DISTRIBUTIONS["dense"]}, ignore_bounds=frozenset({"liar"}))
    )
    async with client_for(portal) as client:
        outcomes, report = await _drain(
            client.iter_reference_outcomes(
                _base(), [_parent("honest"), _parent("liar")], traversal=_traversal(execution)
            )
        )

    terminal = {outcome.correlation: outcome for outcome in outcomes if not isinstance(outcome, ReferenceItem)}
    assert isinstance(terminal["honest"], ReferenceComplete)
    assert _rows_by_parent(outcomes)["honest"] == sorted(DISTRIBUTIONS["dense"])
    liar = terminal["liar"]
    assert isinstance(liar, ReferenceFailure)
    # Whatever reached the caller is an ordered, duplicate-free prefix; the binding never claims exhaustion.
    delivered = _rows_by_parent(outcomes).get("liar", [])
    assert delivered == sorted(DISTRIBUTIONS["dense"])[: len(delivered)]
    assert liar.partial_rows == len(delivered) < len(DISTRIBUTIONS["dense"])
    assert report.state is TerminalState.COMPLETED_WITH_FAILURES
    assert leases.outstanding == 0


@pytest.mark.parametrize("execution", [_range(), _partitioned(), _auto()], ids=["range", "partitioned", "auto"])
@pytest.mark.asyncio
async def test_a_row_breaking_its_identity_fails_its_binding_with_identity_contract_error(execution: Execution) -> None:
    def answer(method: str, query: Query) -> Envelope:
        del method
        rows = _rows(DISTRIBUTIONS["dense"], query)
        if query.get("filter[=parent]") == "broken":
            return {"result": [{"id": f"0{row['id']}"} if row["id"] == _BROKEN_ID else row for row in rows]}
        return {"result": rows}

    async with client_for(BatchPortal(answer)) as client:
        outcomes, _ = await _drain(
            client.iter_reference_outcomes(
                _base(), [_parent("honest"), _parent("broken")], traversal=_traversal(execution)
            )
        )

    terminal = {outcome.correlation: outcome for outcome in outcomes if not isinstance(outcome, ReferenceItem)}
    assert isinstance(terminal["honest"], ReferenceComplete)
    broken = terminal["broken"]
    assert isinstance(broken, ReferenceFailure)
    cause = broken.error.__cause__
    assert isinstance(cause, IdentityContractError)
    assert (cause.path, cause.observed_type) == (("id",), "string")


@pytest.mark.asyncio
@pytest.mark.parametrize("wave", [1, 2])
async def test_a_binding_cancelled_while_its_wave_is_on_the_wire_settles_each_page_of_it_as_unknown(
    monkeypatch: pytest.MonkeyPatch, wave: int
) -> None:
    # The shared engine settles each page of a wave its binding's cancellation interrupts, by its command id, from
    # what its command did: on the wire, it may have run. The first wave is the boundary wave, the next a range one.
    events: list[object] = []
    emit = CompletionGate.emit

    def observe(gate: CompletionGate, event: CompletionEvidence) -> None:
        events.append(event)
        emit(gate, event)

    monkeypatch.setattr(CompletionGate, "emit", observe)
    started = asyncio.Event()
    serve = _answer({"p0": tuple(range(1, 400))})

    async def answer(method: str, query: Query) -> Envelope:
        if len(portal.waves) == wave:
            started.set()
            await asyncio.Future[None]()
        return serve(method, query)

    portal = BatchPortal(answer)
    async with client_for(portal, policy=ExecutionPolicy(page_trace_limit=1024)) as client:
        stream = client.iter_reference_outcomes(_base(), [_parent("p0")], traversal=_traversal(_range()))
        pull = asyncio.create_task(_drain(stream))
        await asyncio.wait_for(started.wait(), 5)
        settled_before = sum(isinstance(event, PageCommandOutcome) for event in events)
        unsettled = sum(isinstance(event, PageScheduled) for event in events) - settled_before
        pull.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pull

    settled = [event.outcome for event in events if isinstance(event, PageCommandOutcome)][settled_before:]
    assert unsettled == portal.waves[-1]
    assert settled == [CommandSettlement.UNKNOWN] * unsettled
    # A cancellation is no failure of the wave's pages: none is traced as one.
    report = stream.report
    assert report is not None
    assert _failed_pages(report) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("second", ["answered", "denied"])
async def test_a_binding_cancelled_while_its_wave_waits_to_replay_settles_each_page_from_its_own_command(
    monkeypatch: pytest.MonkeyPatch, second: str
) -> None:
    # The second wave is answered, but the server refuses its first command (QUERY_LIMIT_EXCEEDED), and the binding
    # is cancelled while the wave waits to send it again. The shared engine settles each page by its command id:
    # the refused one is an answered failure, as is a second one answered with a final failure (ACCESS_DENIED),
    # and every other page succeeded.
    events: list[object] = []
    emit = CompletionGate.emit

    def observe(gate: CompletionGate, event: CompletionEvidence) -> None:
        events.append(event)
        emit(gate, event)

    monkeypatch.setattr(CompletionGate, "emit", observe)
    waiting = asyncio.Event()
    replay_after = Executor.replay_after

    async def observed_replay_after(
        self: Executor, context: ExecutionContext, *, attempts: _RequestAttempts, pause: _Pause
    ) -> None:
        waiting.set()
        await replay_after(self, context, attempts=attempts, pause=pause)

    monkeypatch.setattr(Executor, "replay_after", observed_replay_after)
    serve = _answer({"p0": tuple(range(1, 2000))})
    failures = [("QUERY_LIMIT_EXCEEDED", "slow"), *([("ACCESS_DENIED", "no")] if second == "denied" else [])]

    def answer(method: str, query: Query) -> Envelope:
        # The first commands of the second wave, the first range wave.
        if len(portal.waves) == 2 and failures:
            error, description = failures.pop(0)
            return {"error": error, "error_description": description}
        return serve(method, query)

    portal = BatchPortal(answer)
    async with client_for(portal) as client:
        stream = client.iter_reference_outcomes(_base(), [_parent("p0")], traversal=_traversal(_range()))
        pull = asyncio.create_task(_drain(stream))
        await asyncio.wait_for(waiting.wait(), 5)
        settled_before = {event.page_id for event in events if isinstance(event, PageCommandOutcome)}
        wave = [event.page_id for event in events if isinstance(event, PageScheduled)]
        wave = [page for page in wave if page not in settled_before]
        pull.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pull

    settled = {event.page_id: event.outcome for event in events if isinstance(event, PageCommandOutcome)}
    negative = [CommandSettlement.FAILURE] * (2 if second == "denied" else 1)
    assert len(wave) == portal.waves[1]
    assert [settled[page] for page in wave] == [*negative, *[CommandSettlement.SUCCESS] * (len(wave) - len(negative))]


_NESTED = 100_000


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("replay", "outcome"), [("transport", ReferenceOutcomeUnknown), ("undecodable", ReferenceFailure)]
)
async def test_a_wave_whose_replay_fails_settles_each_page_from_its_own_command(
    monkeypatch: pytest.MonkeyPatch, replay: str, outcome: type[object]
) -> None:
    # The second wave is answered, but the server refuses its first command (QUERY_LIMIT_EXCEEDED); the send that
    # replays it then fails: the transport breaks, or the answer cannot be decoded (nested too deep for the JSON
    # parser, a failure that is not the client's own). Each page settles from what its own command did: the
    # replayed one may have run, every other one was answered with a success, as the flat path settles them.
    events: list[object] = []
    emit = CompletionGate.emit

    def observe(gate: CompletionGate, event: CompletionEvidence) -> None:
        events.append(event)
        emit(gate, event)

    monkeypatch.setattr(CompletionGate, "emit", observe)
    portal = BatchPortal(_answer({"p0": tuple(range(1, 2000))}))
    wave: list[int] = []

    async def respond(request: Request) -> WireResponse:
        sent = len(portal.waves)
        if sent == 2:  # the replay of the first range wave's refused command
            settled = {event.page_id for event in events if isinstance(event, PageCommandOutcome)}
            wave.extend(e.page_id for e in events if isinstance(e, PageScheduled) and e.page_id not in settled)
            if replay == "transport":
                raise RuntimeError("connection reset")
            portal.waves.append(1)
            return WireResponse(200, (("content-type", "application/json"),), b"[" * _NESTED + b"]" * _NESTED)
        answer = await portal.send(request, attempt_timeout=1.0, max_response_bytes=1 << 22)
        if sent == 1:
            commands = request.copy_parameters()["cmd"]
            assert isinstance(commands, dict)
            first = next(iter(commands))
            body = json.loads(answer.body)
            del body["result"]["result"][first]
            body["result"]["result_error"] = {first: {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "slow"}}
            return WireResponse(200, answer.headers, json.dumps(body).encode())
        return answer

    async with client_for(
        ResponderTransport(respond, host=portal.host), policy=ExecutionPolicy(page_trace_limit=1024)
    ) as client:
        outcomes, report = await _drain(
            client.iter_reference_outcomes(_base(), [_parent("p0")], traversal=_traversal(_range()))
        )

    settled = {event.page_id: event.outcome for event in events if isinstance(event, PageCommandOutcome)}
    assert len(wave) == portal.waves[1]
    assert [settled[page] for page in wave] == [
        CommandSettlement.UNKNOWN,
        *[CommandSettlement.SUCCESS] * (len(wave) - 1),
    ]
    assert isinstance(outcomes[-1], outcome)
    # Each page is traced the way it settled, at its command's index in the batch: the answered ones are rejected
    # with the wave that failed around them.
    assert _failed_pages(report) == [
        (0, "body", PageDispatch.BATCH, 0, *_UNKNOWN_PAGE),
        *[
            (0, "body", PageDispatch.BATCH, index, PageOutcome.REJECTED, PageRejectionCode.TRANSACTION_ABORTED)
            for index in range(1, len(wave))
        ],
    ]


@pytest.mark.asyncio
async def test_sequential_pages_of_a_chunk_whose_replay_fails_settle_and_trace_from_their_own_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Two bindings' first pages share one chunk. The server answers one and refuses the other
    # (QUERY_LIMIT_EXCEEDED); the send that replays the refused one cannot be decoded, a failure that is not the
    # client's own, and the chunk fails as a whole. Each page settles and is traced from what its own command did:
    # the answered one succeeded and is rejected with the chunk that failed around it (``transaction_aborted``: its
    # own command did not fail), the replayed one may have run.
    events: list[object] = []
    emit = CompletionGate.emit

    def observe(gate: CompletionGate, event: CompletionEvidence) -> None:
        events.append(event)
        emit(gate, event)

    monkeypatch.setattr(CompletionGate, "emit", observe)
    portal = BatchPortal(_answer({"p0": tuple(range(1, 2000)), "p1": tuple(range(1, 2000))}))

    async def respond(request: Request) -> WireResponse:
        sent = len(portal.waves)
        if sent == 1:  # the replay of the refused command
            portal.waves.append(1)
            return WireResponse(200, (("content-type", "application/json"),), b"[" * _NESTED + b"]" * _NESTED)
        answer = await portal.send(request, attempt_timeout=1.0, max_response_bytes=1 << 22)
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        first = next(iter(commands))
        body = json.loads(answer.body)
        del body["result"]["result"][first]
        body["result"]["result_error"] = {first: {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "slow"}}
        return WireResponse(200, answer.headers, json.dumps(body).encode())

    async with client_for(ResponderTransport(respond, host=portal.host)) as client:
        outcomes, report = await _drain(
            client.iter_reference_outcomes(
                _base(), [_parent("p0"), _parent("p1")], traversal=_traversal(SequentialKeysetExecution())
            )
        )

    assert portal.waves == [2, 1]
    # The first command of the chunk (the first binding's page) is the refused one.
    settled = {event.binding_id: event.outcome for event in events if isinstance(event, PageCommandOutcome)}
    assert settled == {0: CommandSettlement.UNKNOWN, 1: CommandSettlement.SUCCESS}
    traced = sorted(
        (record.outcome, record.rejection_code)
        for record in report.page_trace
        if record.outcome is not PageOutcome.COMMITTED
    )
    assert traced == sorted(
        [
            (PageOutcome.REJECTED, PageRejectionCode.TRANSACTION_ABORTED),
            (PageOutcome.UNKNOWN, PageRejectionCode.AMBIGUOUS_EXECUTION),
        ]
    )
    assert [type(outcome) for outcome in outcomes] == [ReferenceFailure, ReferenceFailure]


@pytest.mark.asyncio
@pytest.mark.parametrize("end", ["undecodable", "cancelled"])
async def test_a_page_whose_answer_was_lost_settles_and_traces_as_possibly_run_when_its_chunk_ends_early(
    monkeypatch: pytest.MonkeyPatch, end: str
) -> None:
    # Two bindings' first pages share one chunk. The batch answer omits the first command's entry, so its final
    # outcome is a failure that may have run; it refuses the second (QUERY_LIMIT_EXCEEDED). The send that replays
    # the second then cannot be decoded, or the reader is cancelled while it is on the wire. The first page settles
    # from its own final outcome, and is traced so: it may have run, though its command has an outcome.
    events: list[object] = []
    emit = CompletionGate.emit

    def observe(gate: CompletionGate, event: CompletionEvidence) -> None:
        events.append(event)
        emit(gate, event)

    monkeypatch.setattr(CompletionGate, "emit", observe)
    portal = BatchPortal(_answer({"p0": tuple(range(1, 2000)), "p1": tuple(range(1, 2000))}))
    replaying = asyncio.Event()

    async def respond(request: Request) -> WireResponse:
        if portal.waves:  # the replay of the refused command
            replaying.set()
            if end == "cancelled":
                await asyncio.Future()
            portal.waves.append(1)
            return WireResponse(200, (("content-type", "application/json"),), b"[" * _NESTED + b"]" * _NESTED)
        answer = await portal.send(request, attempt_timeout=1.0, max_response_bytes=1 << 22)
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        lost, refused = list(commands)
        body = json.loads(answer.body)
        del body["result"]["result"][lost]
        del body["result"]["result"][refused]
        body["result"]["result_error"] = {refused: {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "slow"}}
        return WireResponse(200, answer.headers, json.dumps(body).encode())

    async with client_for(ResponderTransport(respond, host=portal.host)) as client:
        stream = client.iter_reference_outcomes(
            _base(), [_parent("p0"), _parent("p1")], traversal=_traversal(SequentialKeysetExecution())
        )
        reader = asyncio.create_task(_drain(stream))
        if end == "cancelled":
            await asyncio.wait_for(replaying.wait(), HANG_BOUND)
            reader.cancel()
        done, _pending = await asyncio.wait({reader}, timeout=HANG_BOUND)
        assert done == {reader}

    settled = {event.binding_id: event.outcome for event in events if isinstance(event, PageCommandOutcome)}
    assert settled == {0: CommandSettlement.UNKNOWN, 1: CommandSettlement.UNKNOWN}
    if end == "undecodable":
        _outcomes, report = reader.result()
        traced = [(r.outcome, r.rejection_code) for r in report.page_trace if r.outcome is not PageOutcome.COMMITTED]
        assert traced == [(PageOutcome.UNKNOWN, PageRejectionCode.AMBIGUOUS_EXECUTION)] * 2
    else:
        assert reader.cancelled()


@pytest.mark.asyncio
@pytest.mark.parametrize("end", ["on_the_wire", "at_its_permit"])
async def test_fast_reference_bindings_cancelled_mid_run_settle_each_page_from_what_its_command_did(
    monkeypatch: pytest.MonkeyPatch, end: str
) -> None:
    # Two fast keyset bindings share the dispatcher; the reader is cancelled while a send is on the wire, or while
    # it waits at its rate permit. Each page settles from what its own command did: one answered before the end
    # succeeded (also one whose wave task had already taken its answer), one on the wire may have run, and one
    # never sent, whether its send waited at its permit or it was never handed to a send, was not executed.
    events: list[object] = []
    emit = CompletionGate.emit

    def observe(gate: CompletionGate, event: CompletionEvidence) -> None:
        events.append(event)
        emit(gate, event)

    monkeypatch.setattr(CompletionGate, "emit", observe)
    held = asyncio.Event()
    acquire = RateCoordinator.acquire
    permits = [0]

    async def hold_third(coordinator: RateCoordinator, *args: object, **kwargs: object) -> object:
        if end == "at_its_permit" and args and args[0] is WorkClass.BATCH:
            permits[0] += 1
            if permits[0] == 3:
                held.set()
                await asyncio.Future()
        return await acquire(coordinator, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(RateCoordinator, "acquire", hold_third)
    portal = BatchPortal(_answer({"p0": tuple(range(1, 2000)), "p1": tuple(range(5, 900, 3))}))
    answered: list[int] = []
    on_the_wire: list[int] = []

    async def respond(request: Request) -> WireResponse:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        if end == "on_the_wire" and len(answered) == 1:
            on_the_wire.append(len(commands))
            held.set()
            await asyncio.Future()
        answer = await portal.send(request, attempt_timeout=1.0, max_response_bytes=1 << 22)
        answered.append(len(commands))
        return answer

    async with client_for(ResponderTransport(respond, host=portal.host)) as client:
        stream = client.iter_reference_outcomes(_base(), [_parent("p0"), _parent("p1")], traversal=_traversal(_auto()))
        reader = asyncio.create_task(_drain(stream))
        await asyncio.wait_for(held.wait(), HANG_BOUND)
        reader.cancel()
        done, _pending = await asyncio.wait({reader}, timeout=HANG_BOUND)
        assert done == {reader}

    scheduled = sum(isinstance(event, PageScheduled) for event in events)
    settled = Counter(event.outcome for event in events if isinstance(event, PageCommandOutcome))
    never_sent = scheduled - sum(answered) - sum(on_the_wire)
    assert never_sent > 0
    assert sum(answered) > len(answered)  # a wave carried more than one page
    expected = {CommandSettlement.SUCCESS: sum(answered), CommandSettlement.NOT_EXECUTED: never_sent}
    if on_the_wire:
        expected[CommandSettlement.UNKNOWN] = sum(on_the_wire)
    assert settled == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("execution", ["sequential", "range"])
async def test_a_batch_that_fails_as_a_whole_fails_its_binding_with_the_batch_s_own_error(
    monkeypatch: pytest.MonkeyPatch, execution: str
) -> None:
    # A batch the engine ends with one error for all of its commands fails each page with it, sequential pages and
    # a fast wave alike: the binding reports that error, not a failure of its own.
    broken = ProtocolError("the batch broke")

    async def fails(_engine: BatchExecutor, _requests: tuple[Request, ...], **_options: object) -> tuple[()]:
        raise broken

    monkeypatch.setattr(BatchExecutor, "execute_requests", fails)
    traversal = _traversal(SequentialKeysetExecution() if execution == "sequential" else _range())
    async with client_for(BatchPortal(_answer({"p0": tuple(range(1, 2000))}))) as client:
        outcomes, _report = await _drain(client.iter_reference_outcomes(_base(), [_parent("p0")], traversal=traversal))

    (failure,) = outcomes
    assert isinstance(failure, ReferenceFailure)
    assert failure.error is broken


@pytest.mark.asyncio
async def test_a_binding_that_fails_retires_every_page_it_scheduled() -> None:
    # A binding whose pages contradict their ranges fails while a page it has accepted is still undelivered: it
    # rejects that page as it fails, so each page it scheduled is acknowledged or rejected exactly once.
    events: list[object] = []
    emit = CompletionGate.emit

    def observe(gate: CompletionGate, event: CompletionEvidence) -> None:
        events.append(event)
        emit(gate, event)

    portal = BatchPortal(_answer({"p0": tuple(range(1, 2000))}, ignore_bounds=frozenset({"p0"})))
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(CompletionGate, "emit", observe)
        async with client_for(portal) as client:
            outcomes, report = await _drain(
                client.iter_reference_outcomes(_base(), [_parent("p0")], traversal=_traversal(_range()))
            )

    assert report.state is TerminalState.COMPLETED_WITH_FAILURES
    assert isinstance(outcomes[-1], ReferenceFailure)
    scheduled = [event.page_id for event in events if isinstance(event, PageScheduled)]
    retired = [event.page_id for event in events if isinstance(event, PageAcknowledged | PageRejected)]
    assert sorted(retired) == scheduled
    assert KernelState.FAILED.value in {event.reason for event in events if isinstance(event, PageRejected)}


@pytest.mark.asyncio
async def test_fast_bindings_keep_no_more_batches_in_flight_than_the_active_references_allow() -> None:
    # A fast keyset binding sends several pages of its own at once, so the active references alone do not bound
    # the batches in flight: the policy's max_active_references does, below a wider dispatch concurrency.
    active = 2
    in_flight = {"now": 0, "most": 0}
    data = {f"p{index}": tuple(range(1 + 1000 * index, 600 + 1000 * index)) for index in range(6)}
    portal = BatchPortal(_answer(data), wire_turns=50)

    async def respond(request: Request) -> WireResponse:
        in_flight["now"] += 1
        in_flight["most"] = max(in_flight["most"], in_flight["now"])
        try:
            return await portal.send(request, attempt_timeout=1.0, max_response_bytes=1 << 22)
        finally:
            in_flight["now"] -= 1

    transport = ResponderTransport(respond, host=portal.host)
    async with client_for(transport, policy=ExecutionPolicy(max_active_references=active)) as client:
        outcomes, report = await _drain(
            client.iter_reference_outcomes(
                _base(),
                [_parent(name) for name in data],
                traversal=_traversal(_auto()),
                dispatch=BatchDispatch(batch_size=16, concurrency=8, coalesce_wait=0.0),
            )
        )

    assert report.state is TerminalState.COMPLETED
    assert sum(isinstance(outcome, ReferenceItem) for outcome in outcomes) == sum(map(len, data.values()))
    assert in_flight["most"] == active


@pytest.mark.asyncio
async def test_closing_during_planning_returns_every_lease_without_a_hang(leases: _Leases) -> None:
    started = asyncio.Event()

    async def blocked(method: str, query: Query) -> Envelope:
        del method, query
        started.set()
        await asyncio.Future[None]()
        raise AssertionError("unreachable")

    portal = BatchPortal(blocked)
    async with client_for(portal) as client:
        stream = client.iter_reference_outcomes(
            _base(), [_parent("p0"), _parent("p1")], traversal=_traversal(_partitioned())
        )
        pull = asyncio.create_task(anext(aiter(stream)))
        await asyncio.wait_for(started.wait(), 5)
        pull.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pull
        await asyncio.wait_for(stream.aclose(), 5)

    assert leases.granted
    assert leases.outstanding == 0
    assert portal.waves  # planning was dispatched, then cancelled after send


@pytest.mark.asyncio
async def test_auto_with_a_page_stop_runs_sequentially_and_says_why() -> None:
    portal = BatchPortal(_answer({"p0": DISTRIBUTIONS["dense"]}))
    stops: list[int] = []

    def on_page(boundary: object) -> ContinuePage:
        stops.append(1)
        del boundary
        return ContinuePage.CONTINUE

    page_stop = type("_Stop", (), {"on_page": staticmethod(on_page)})()
    async with client_for(portal) as client:
        outcomes, _report = await _drain(
            client.iter_reference_outcomes(
                _base(),
                [_parent("p0")],
                traversal=_traversal(_auto()),
                page_stop=page_stop,
            )
        )

    complete = _completes(outcomes)["p0"]
    assert complete.keyset_selection == KeysetSelectionSummary(
        KeysetExecutionKind.AUTO, KeysetExecutionKind.SEQUENTIAL, KeysetSelectionReason.PAGE_STOP
    )
    assert complete.keyset_execution is None
    assert _rows_by_parent(outcomes)["p0"] == sorted(DISTRIBUTIONS["dense"])
    assert stops
    assert all(len(methods) == 1 for methods in portal.methods)


@pytest.mark.parametrize("execution", [_range(), _partitioned()], ids=["range", "partitioned"])
@pytest.mark.asyncio
async def test_explicit_fast_with_a_page_stop_or_direct_dispatch_is_rejected_before_any_request(
    execution: Execution,
) -> None:
    from b24api import DirectDispatch  # noqa: PLC0415 - one focused import

    portal = BatchPortal(_answer({"p0": DISTRIBUTIONS["dense"]}))
    page_stop = type("_Stop", (), {"on_page": staticmethod(lambda _boundary: ContinuePage.CONTINUE)})()
    async with client_for(portal) as client:
        with pytest.raises(CapabilityError, match="page stop requires sequential"):
            client.iter_reference_outcomes(
                _base(),
                [_parent("p0")],
                traversal=_traversal(execution),
                page_stop=page_stop,
            )
        with pytest.raises(CapabilityError, match="requires BatchDispatch"):
            client.iter_reference_outcomes(
                _base(), [_parent("p0")], traversal=_traversal(execution), dispatch=DirectDispatch()
            )
        outcomes, _report = await _drain(
            client.iter_reference_source_outcomes(
                [
                    ReferenceSource(_base(), _traversal(execution), _parent("p0"), page_stop=page_stop),
                    ReferenceSource(_base(), _traversal(execution), _parent("p0", "ok")),
                ]
            )
        )
    assert isinstance(outcomes[0], ReferenceNotExecuted) or isinstance(outcomes[-1], ReferenceNotExecuted)
    assert any(isinstance(outcome, ReferenceComplete) and outcome.correlation == "ok" for outcome in outcomes)


# A27-VIEW: the projection differs only in rows=G; engine and executor get the shared context; one
# shared delta per adjustment; the runtime balance equals the lease holding after every wave.


@pytest.mark.parametrize("execution", [_range(), _auto()], ids=["range", "auto"])
@pytest.mark.asyncio
async def test_the_budget_view_projects_only_the_lease_and_keeps_execution_on_the_shared_context(
    execution: Execution,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    views: list[tuple[BindingBudgetView, int]] = []
    over = BindingBudgetView.over

    def tracked_over(shared: ExecutionContext, lease: RowLease) -> BindingBudgetView:
        view = over(shared, lease)
        views.append((view, lease.capacity))
        return view

    monkeypatch.setattr(BindingBudgetView, "over", staticmethod(tracked_over))
    shared_deltas: list[int] = []
    shared_adjust = ExecutionContext.adjust_buffered_rows

    async def tracked_shared(self: ExecutionContext, delta: int) -> None:
        shared_deltas.append(delta)
        await shared_adjust(self, delta)

    monkeypatch.setattr(ExecutionContext, "adjust_buffered_rows", tracked_shared)
    view_adjust = BindingBudgetView.adjust_buffered_rows
    per_adjust: list[int] = []

    async def tracked_view(self: BindingBudgetView, delta: int) -> None:
        before = len(shared_deltas)
        await view_adjust(self, delta)
        per_adjust.append(len(shared_deltas) - before)

    monkeypatch.setattr(BindingBudgetView, "adjust_buffered_rows", tracked_view)
    engine_contexts: list[object] = []
    engine_execute = wave_engine.SharedWaveEngine.execute_requests

    async def tracked_engine(self: object, requests: tuple[Request, ...], **options: object) -> object:
        engine_contexts.append(options["context"])
        return await engine_execute(self, requests, **options)  # type: ignore[arg-type]

    monkeypatch.setattr(wave_engine.SharedWaveEngine, "execute_requests", tracked_engine)
    balances: list[tuple[int, int]] = []
    execute_wave = KeysetRuntime.execute_wave

    async def tracked_wave(self: KeysetRuntime, plans: tuple[object, ...]) -> object:
        receipts = await execute_wave(self, plans)  # type: ignore[arg-type]
        context = cast("BindingBudgetView", self.context)
        balances.append((self.transactions.buffer_balance, context.lease.held))
        return receipts

    monkeypatch.setattr(KeysetRuntime, "execute_wave", tracked_wave)

    portal = BatchPortal(_answer({"p0": DISTRIBUTIONS["skewed"], "p1": DISTRIBUTIONS["dense"]}))
    async with client_for(portal) as client:
        outcomes, _report = await _drain(
            client.iter_reference_outcomes(_base(), [_parent("p0"), _parent("p1")], traversal=_traversal(execution))
        )

    assert set(_completes(outcomes)) == {"p0", "p1"}
    assert len(views) == 2
    _assert_only_bounded_plans_trim(views, _completes(outcomes))
    for view, _granted in views:
        shared_policy = view.shared.policy
        differing = {
            item.name
            for item in dataclasses.fields(shared_policy)
            if getattr(view.policy, item.name) != getattr(shared_policy, item.name)
        }
        assert differing <= {"max_buffered_rows"}
        assert view.execution_context is view.shared
        assert view.lease.held == 0
    assert per_adjust
    assert set(per_adjust) == {1}
    assert sum(shared_deltas) == 0
    assert engine_contexts
    assert all(type(context) is ExecutionContext for context in engine_contexts)
    assert all(context is views[0][0].shared for context in engine_contexts)
    assert balances
    assert all(balance == held for balance, held in balances)


def _assert_only_bounded_plans_trim(
    views: list[tuple[BindingBudgetView, int]],
    completes: Mapping[object, ReferenceComplete[object]],
) -> None:
    """A plan that bounds the rows it can still hold trims its lease (#33); a lane plan keeps the grant."""
    bounded = sum(
        complete.keyset_execution is not None
        and complete.keyset_execution.selected_kind
        in {KeysetExecutionKind.SEQUENTIAL, KeysetExecutionKind.BOUNDARY_ONLY}
        for complete in completes.values()
    )
    assert sum(view.lease.capacity < granted for view, granted in views) == bounded
    for view, granted in views:
        assert view.policy.max_buffered_rows == view.lease.capacity <= granted
        assert view.lease.capacity in {granted, PAGE}


def test_keyset_context_names_exactly_the_members_fast_keyset_uses() -> None:
    from b24api.traversal import keyset_context  # noqa: PLC0415 - focused protocol import

    members = {name for name in vars(keyset_context.KeysetContext) if not name.startswith("_")}
    assert members == {
        "policy",
        "execution_context",
        "start",
        "snapshot",
        "reserve_page",
        "reserve_pages",
        "commit_page",
        "release_page",
        "adjust_buffered_rows",
    }


# A27-RETAIN / A27-WAVE1: a lease never grows; the wave fit and the charge use the same capacity.


def _shared(rows: int = 2500) -> ExecutionContext:
    from b24api.execution.rate import RateCoordinator  # noqa: PLC0415 - one focused import

    return ExecutionContext(ExecutionPolicy(max_buffered_rows=rows), RateCoordinator(max_concurrency=1))


@pytest.mark.parametrize(("held", "fitted"), [(40, 1), (30, 2)])
@pytest.mark.asyncio
async def test_retained_rows_shrink_the_fitted_wave_and_the_charge_equals_the_fit(held: int, fitted: int) -> None:
    lease = RowLease(50)
    view: KeysetContext = BindingBudgetView.over(_shared(), lease)
    await view.adjust_buffered_rows(held)
    candidates = ("first", "second")
    wave = fit_wave(
        candidates,
        reserves=(10, 10),
        commands=2,
        rows=view.policy.max_buffered_rows - lease.held,
    )
    assert len(wave) == fitted
    await view.adjust_buffered_rows(10 * len(wave))
    assert lease.held == held + 10 * len(wave) <= lease.capacity == 50
    await view.adjust_buffered_rows(-lease.held)
    assert lease.held == 0
    assert lease.capacity == 50


@pytest.mark.asyncio
async def test_one_free_page_runs_two_candidates_as_two_atomic_one_command_waves() -> None:
    lease = RowLease(30)
    view = BindingBudgetView.over(_shared(), lease)
    await view.adjust_buffered_rows(20)  # the planning barrier retains two pages; one page (C) is free
    remaining: tuple[str, ...] = ("a", "b")
    waves: list[tuple[str, ...]] = []
    while remaining:
        wave = fit_wave(remaining, reserves=(10,) * len(remaining), commands=2, rows=30 - lease.held)
        assert wave
        await view.adjust_buffered_rows(10)
        waves.append(wave)
        await view.adjust_buffered_rows(-10)
        remaining = remaining[len(wave) :]
    assert waves == [("a",), ("b",)]
    with pytest.raises(Exception, match="row lease"):
        await view.adjust_buffered_rows(11)
    assert lease.held == 20


@pytest.mark.asyncio
async def test_a_lease_returns_to_the_pool_and_its_neighbour_exhausts(leases: _Leases) -> None:
    portal = BatchPortal(_answer({"fast": DISTRIBUTIONS["dense"], "plain": tuple(range(1, 31))}))
    policy = ExecutionPolicy(max_buffered_rows=40)
    async with client_for(portal, policy=policy) as client:
        outcomes, _report = await _drain(
            client.iter_reference_source_outcomes(
                [
                    ReferenceSource(_base(), _traversal(_range()), _parent("fast")),
                    ReferenceSource(_base(), _traversal(SequentialKeysetExecution()), _parent("plain")),
                ]
            )
        )
    rows = _rows_by_parent(outcomes)
    assert rows["fast"] == sorted(DISTRIBUTIONS["dense"])
    assert rows["plain"] == list(range(1, 31))
    assert set(_completes(outcomes)) == {"fast", "plain"}
    assert [amount for _index, amount in leases.granted] == [30]
    assert leases.outstanding == 0


# A27-GRANT-LOW / A27-GRANT-OK: AUTO below the floor runs exclusively; a sufficient pool runs both.


@pytest.mark.parametrize("order", [DeliveryOrder.READY, DeliveryOrder.INPUT])
@pytest.mark.asyncio
async def test_auto_below_the_floor_runs_exclusively_and_matches_the_flat_oracle(
    order: DeliveryOrder,
    leases: _Leases,
) -> None:
    ids = DISTRIBUTIONS["dense"]
    portal = BatchPortal(_answer({"p0": ids, "p1": ids}))
    policy = ExecutionPolicy(max_buffered_rows=30)  # C=10, A=4: P=34 > B=30
    async with client_for(portal, policy=policy) as client:
        outcomes, _report = await asyncio.wait_for(
            _drain(
                client.iter_reference_outcomes(
                    _base(),
                    [_parent("p0"), _parent("p1")],
                    traversal=_traversal(_auto()),
                    dispatch=BatchDispatch(output_order=order),
                )
            ),
            10,
        )
    assert leases.granted == [(0, 30), (1, 30)]
    assert leases.high_water == 30
    assert leases.outstanding == 0
    completes = _completes(outcomes)
    assert set(completes) == {"p0", "p1"}
    oracle = await _flat_oracle(ids, _auto(), 30)
    for complete in completes.values():
        assert complete.keyset_execution is not None
        assert _cost(complete.keyset_execution) == _cost(oracle)


@pytest.mark.asyncio
async def test_a_sufficient_pool_runs_two_auto_bindings_to_the_flat_oracle(leases: _Leases) -> None:
    ids = DISTRIBUTIONS["dense"]
    portal = BatchPortal(_answer({"p0": ids, "p1": ids}))
    policy = ExecutionPolicy(max_buffered_rows=200, max_buffered_commands=50)
    async with client_for(portal, policy=policy) as client:
        outcomes, report = await _drain(
            client.iter_reference_outcomes(_base(), [_parent("p0"), _parent("p1")], traversal=_traversal(_auto()))
        )
    rows = _rows_by_parent(outcomes)
    assert sum(len(values) for values in rows.values()) == 400
    completes = _completes(outcomes)
    assert len(completes) == 2
    assert all(complete.exhausted for complete in completes.values())
    grants = dict(leases.granted)
    for index, name in enumerate(("p0", "p1")):
        evidence = completes[name].keyset_execution
        assert evidence is not None
        assert _cost(evidence) == _cost(await _flat_oracle(ids, _auto(), grants[index]))
    assert leases.outstanding == 0
    assert report.buffered_rows_high_water <= 200


# A27-INPUT-TIGHT / A27-INPUT-OK: partitioned lanes beside an ordinary source in INPUT order.


@pytest.mark.asyncio
async def test_partitioned_below_its_floor_fails_finitely_before_any_request() -> None:
    portal = BatchPortal(_answer({"p0": DISTRIBUTIONS["dense"]}))
    policy = ExecutionPolicy(max_buffered_rows=30)  # C=10, lanes 8: P=38 > B=30
    dispatch = BatchDispatch(output_order=DeliveryOrder.INPUT)
    async with client_for(portal, policy=policy) as client:
        with pytest.raises(CapabilityError):
            client.iter_reference_outcomes(
                _base(), [_parent("p0"), _parent("p1")], traversal=_traversal(_partitioned(8)), dispatch=dispatch
            )
        outcomes, _report = await asyncio.wait_for(
            _drain(
                client.iter_reference_source_outcomes(
                    [ReferenceSource(_base(), _traversal(_partitioned(8)), _parent("p0"))], dispatch=dispatch
                )
            ),
            10,
        )
    assert [type(outcome) for outcome in outcomes] == [ReferenceNotExecuted]
    assert portal.waves == []


@pytest.mark.asyncio
async def test_two_partitioned_sources_and_an_ordinary_one_exhaust_in_input_order(leases: _Leases) -> None:
    data = {"a": tuple(range(1, 151)), "b": tuple(range(1001, 1151)), "plain": tuple(range(1, 31))}
    portal = BatchPortal(_answer(data))
    policy = ExecutionPolicy(max_buffered_rows=120)
    async with client_for(portal, policy=policy) as client:
        outcomes, report = await asyncio.wait_for(
            _drain(
                client.iter_reference_source_outcomes(
                    [
                        ReferenceSource(_base(), _traversal(_partitioned(8)), _parent("a")),
                        ReferenceSource(_base(), _traversal(_partitioned(8)), _parent("b")),
                        ReferenceSource(_base(), _traversal(SequentialKeysetExecution()), _parent("plain")),
                    ],
                    dispatch=BatchDispatch(output_order=DeliveryOrder.INPUT),
                )
            ),
            10,
        )
    order = [outcome.correlation for outcome in outcomes]
    rank = {"a": 0, "b": 1, "plain": 2}
    assert order == sorted(order, key=lambda name: rank[cast("str", name)])
    rows = _rows_by_parent(outcomes)
    assert {name: rows[name] for name in data} == {name: sorted(ids) for name, ids in data.items()}
    assert sum(len(values) for values in rows.values()) == 330
    assert set(_completes(outcomes)) == set(data)
    assert report.state is TerminalState.COMPLETED
    assert leases.outstanding == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("execution", "distribution", "max_requests"),
    [
        (_range, "dense", 2),
        (_partitioned, "dense", 7),
        (_auto, "dense", 1),
        (_range, "sparse", 28),
    ],
    ids=["range", "partitioned", "auto", "range-sparse"],
)
async def test_a_keyset_binding_finish_the_request_budget_refuses_ends_with_the_budget(
    execution: Callable[[], Execution],
    distribution: str,
    max_requests: int,
) -> None:
    # The budget refuses a binding's final confirmation: the operation ends with it, not with a failed command.
    data = {"a": DISTRIBUTIONS[distribution], "b": DISTRIBUTIONS[distribution]}
    portal = BatchPortal(_answer(data))
    async with client_for(portal, policy=ExecutionPolicy(max_requests=max_requests)) as client:
        stream = client.iter_reference_outcomes(
            _base(), [_parent("a"), _parent("b")], traversal=_traversal(execution()), dispatch=BatchDispatch()
        )
        with pytest.raises(BudgetExceededError, match="physical request budget exhausted"):
            async with stream:
                _ = [outcome async for outcome in stream]

    report = stream.report
    assert report is not None
    assert (report.state, report.terminal_reason) == (TerminalState.INCOMPLETE, "BudgetExceededError")
    assert {item.code for item in report.violations} == {"budget_exhausted", "reference_failure"}
    # A page the budget kept from its send left no request to record: none is traced as a failed command.
    assert [record for record in report.page_trace if record.outcome is not PageOutcome.COMMITTED] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("spent_at", [4, 6, 7, 10, 11, 13, 16, 19, 25, 32])
async def test_keyset_bindings_the_deadline_ends_record_no_failed_command(
    monkeypatch: pytest.MonkeyPatch, spent_at: int
) -> None:
    # The deadline passes while the spent_at-th request is answered. Another binding's admission or page reservation
    # is refused first, and every keyset command refused after it names other work (its permit, its dispatch): each
    # is the same operation's end, so none is a failed command.
    skew = [0.0]

    def context(executor: Executor, policy: ExecutionPolicy | None = None) -> ExecutionContext:
        return ExecutionContext(
            policy or ExecutionPolicy(), executor.coordinator, clock=lambda: time.monotonic() + skew[0]
        )

    monkeypatch.setattr(Executor, "context", context)
    data = {f"p{index}": DISTRIBUTIONS["sparse"] for index in range(3)}
    answer = _answer(data)

    def spending(method: str, query: Query) -> Envelope:
        if len(portal.waves) == spent_at and not skew[0]:
            skew[0] = 10_000.0
        return answer(method, query)

    portal = BatchPortal(spending)
    async with client_for(portal, policy=ExecutionPolicy(max_elapsed=600.0)) as client:
        stream = client.iter_reference_outcomes(
            _base(), [_parent(name) for name in data], traversal=_traversal(_range()), dispatch=BatchDispatch()
        )
        # Which work the deadline refuses first (an admission, a page's rows, a send) depends on the interleaving.
        with pytest.raises(BudgetExceededError):
            async with stream:
                _ = [outcome async for outcome in stream]

    report = stream.report
    assert report is not None
    assert (report.state, report.terminal_reason) == (TerminalState.INCOMPLETE, "BudgetExceededError")
    assert {item.code for item in report.violations} == {"budget_exhausted", "reference_failure"}


@pytest.mark.asyncio
@pytest.mark.parametrize("refused_at", [1, 2, 3])
async def test_a_keyset_wave_replay_the_deadline_refuses_at_its_send_records_no_failed_command(
    monkeypatch: pytest.MonkeyPatch, refused_at: int
) -> None:
    # A binding's wave is answered 429 as a whole and its replay is planned; the replay's send is granted its permit
    # as the deadline passes, so the operation's refusal stops each command it held: the operation's end, not a
    # failed command.
    skew = [0.0]
    armed = [False]

    def context(executor: Executor, policy: ExecutionPolicy | None = None) -> ExecutionContext:
        return ExecutionContext(
            policy or ExecutionPolicy(), executor.coordinator, clock=lambda: time.monotonic() + skew[0]
        )

    monkeypatch.setattr(Executor, "context", context)
    acquire = RateCoordinator.acquire

    async def late_grant(coordinator: RateCoordinator, *args: object, **kwargs: object) -> object:
        permit = await acquire(coordinator, *args, **kwargs)  # type: ignore[arg-type]
        if armed[0]:
            skew[0] = 10_000.0
        return permit

    monkeypatch.setattr(RateCoordinator, "acquire", late_grant)
    portal = BatchPortal(_answer({"a": DISTRIBUTIONS["dense"]}))

    async def respond(request: Request) -> WireResponse:
        if request.method == "batch" and sum(sent.method == "batch" for sent in transport.requests) == refused_at:
            armed[0] = True
            return WireResponse(429, (("content-type", "text/plain"),), b"slow down")
        return await portal.send(request, attempt_timeout=1.0, max_response_bytes=1 << 20)

    transport = ResponderTransport(respond)
    policy = ExecutionPolicy(
        max_elapsed=600.0,
        max_retry_elapsed_per_request=600.0,
        retry=RetryPolicy(initial_delay=0.001, maximum_delay=0.001, jitter=0),
    )
    async with client_for(transport, policy=policy) as client:
        stream = client.iter_reference_outcomes(
            _base(), [_parent("a")], traversal=_traversal(_range()), dispatch=BatchDispatch()
        )
        with pytest.raises(BudgetExceededError):
            async with stream:
                _ = [outcome async for outcome in stream]

    report = stream.report
    assert report is not None
    assert (report.state, report.terminal_reason) == (TerminalState.INCOMPLETE, "BudgetExceededError")
    assert {item.code for item in report.violations} == {"budget_exhausted", "reference_failure"}


@pytest.mark.asyncio
async def test_a_keyset_command_its_own_budget_stops_fails_also_once_the_operation_is_refused() -> None:
    # The first wave is answered 429 only after the next binding's batch went and the third one's send was refused
    # by the request budget, which ends the operation. The wave's commands, with no attempt left for a retry, are
    # stopped by their own attempt budget: that refuses them alone, so they fail as commands, not as the end.
    data = {f"p{index}": DISTRIBUTIONS["dense"] for index in range(3)}
    portal = BatchPortal(_answer(data))

    async def respond(request: Request) -> WireResponse:
        if not transport.requests[1:]:
            for _turn in range(20):
                await asyncio.sleep(0)
            return WireResponse(429, (("content-type", "text/plain"),), b"slow down")
        return await portal.send(request, attempt_timeout=1.0, max_response_bytes=1 << 20)

    transport = ResponderTransport(respond)
    policy = ExecutionPolicy(max_requests=2, max_attempts_per_request=1)
    async with client_for(transport, policy=policy) as client:
        stream = client.iter_reference_outcomes(
            _base(),
            [_parent(name) for name in data],
            traversal=_traversal(_range()),
            dispatch=BatchDispatch(batch_size=2, concurrency=2),
        )
        with pytest.raises(BudgetExceededError, match="physical request budget exhausted"):
            async with stream:
                _ = [outcome async for outcome in stream]

    report = stream.report
    assert report is not None
    failures = [item for item in report.violations if item.code == "command_failure"]
    assert failures
    assert report.terminal_reason == "BudgetExceededError"


# --- A keyset page's batch index ---------------------------------------------------------------------


async def _indexed(
    portal: BatchPortal, execution: Execution, parents: int, policy: ExecutionPolicy | None = None
) -> list[tuple[int | None, str | None, int | None]]:
    """Run ``parents`` bindings through one dispatcher; return each page's binding, phase and batch index."""
    request = Request(
        "item.list", {"filter": {"STATUS": "open"}}, route=RouteKind.BARE, replay_safety=ReplaySafety.SAFE
    )
    async with client_for(portal, policy=policy or ExecutionPolicy(page_trace_limit=1024)) as client:
        stream = client.iter_reference_outcomes(
            request,
            [_parent(f"p{index}") for index in range(parents)],
            traversal=_traversal(execution),
            dispatch=BatchDispatch(),
        )
        outcomes, report = await _drain(stream)
    assert not any(isinstance(outcome, ReferenceFailure | ReferenceOutcomeUnknown) for outcome in outcomes)
    assert all(record.outcome is PageOutcome.COMMITTED for record in report.page_trace)
    return [
        (record.reference_index, None if record.phase is None else record.phase.value, record.batch_index)
        for record in report.page_trace
    ]


def _wire(portal: BatchPortal) -> list[list[tuple[str, str | None]]]:
    """Return each physical batch's commands as (parent, lower bound), in their order in the batch."""
    sends: list[list[tuple[str, str | None]]] = []
    start = 0
    for count in portal.waves:
        sends.append([(query["filter[=parent]"], query.get("filter[>ID]")) for query in portal.queries[start:][:count]])
        start += count
    return sends


@pytest.mark.asyncio
async def test_a_keyset_reference_page_records_its_index_in_the_shared_batch_it_was_sent_with() -> None:
    # Four one-row parents: the first batch carries three parents' boundary probes (ascending, descending), the
    # second the fourth parent's probes after the first three parents' finish pages, the third its finish page.
    # Every page records its command's index in that shared batch, a finish page too, not its index in its own
    # binding's wave.
    portal = BatchPortal(_answer({f"p{index}": (1,) for index in range(4)}))

    pages = await _indexed(portal, AutoKeysetExecution(_CONTRACT), 4)

    assert _wire(portal) == [
        [("p0", None), ("p0", None), ("p1", None), ("p1", None), ("p2", None), ("p2", None)],
        [("p3", None), ("p3", None), ("p0", "1"), ("p1", "1"), ("p2", "1")],
        [("p3", "1")],
    ]
    assert pages == [
        (0, "boundary", 0),
        (0, "boundary", 1),
        (0, "finish", 2),
        (1, "boundary", 2),
        (1, "boundary", 3),
        (1, "finish", 3),
        (2, "boundary", 4),
        (2, "boundary", 5),
        (2, "finish", 4),
        (3, "boundary", 0),
        (3, "boundary", 1),
        (3, "finish", 0),
    ]


@pytest.mark.asyncio
async def test_a_range_keyset_reference_body_page_records_its_index_in_the_shared_batch() -> None:
    # Three 40-row parents share a body batch of three range pages each: each body page records its index in it.
    portal = BatchPortal(_answer({f"p{index}": tuple(range(1, 41)) for index in range(3)}))

    pages = await _indexed(portal, _range(), 3)

    assert [len(send) for send in _wire(portal)] == [6, 9, 3]
    body = [(binding, index) for binding, phase, index in pages if phase == "body"]
    assert body == [(binding, binding * 3 + offset) for binding in range(3) for offset in range(3)]
    finish = [(binding, index) for binding, phase, index in pages if phase == "finish"]
    assert finish == [(0, 0), (1, 1), (2, 2)]


@pytest.mark.asyncio
async def test_a_replayed_keyset_reference_page_keeps_the_index_of_the_batch_it_was_first_sent_with() -> None:
    # The first batch refuses the second parent's descending boundary probe (QUERY_LIMIT_EXCEEDED) at index 3; its
    # replay goes alone under the same envelope key. The page records index 3, the command's key in every send.
    refused: list[str] = []
    answer = _answer({f"p{index}": (1,) for index in range(3)})

    def refuse_once(method: str, query: Query) -> Envelope:
        if query.get("filter[=parent]") == "p1" and query.get("order[id]") == "DESC" and not refused:
            refused.append(method)
            return {"error": "QUERY_LIMIT_EXCEEDED", "error_description": "slow"}
        return answer(method, query)

    portal = BatchPortal(refuse_once)
    retry = RetryPolicy(initial_delay=0.001, maximum_delay=0.002, jitter=0.0)

    pages = await _indexed(
        portal, AutoKeysetExecution(_CONTRACT), 3, ExecutionPolicy(page_trace_limit=1024, retry=retry)
    )

    assert [len(send) for send in _wire(portal)][:2] == [6, 1]
    boundaries = [(binding, index) for binding, phase, index in pages if phase == "boundary"]
    assert boundaries == [(0, 0), (0, 1), (1, 2), (1, 3), (2, 4), (2, 5)]


def _deny(answer: Callable[..., Envelope], parent: str, *, finish: bool) -> Callable[..., Envelope]:
    """Answer ``parent``'s descending boundary probe, or its finish page, with ACCESS_DENIED."""

    def denied(method: str, query: Query) -> Envelope:
        page = "filter[>ID]" in query if finish else query.get("order[id]") == "DESC"
        if query.get("filter[=parent]") == parent and page:
            return {"error": "ACCESS_DENIED", "error_description": "denied"}
        return answer(method, query)

    return denied


_ONE_ROW = {f"p{index}": (1,) for index in range(4)}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("answer", "failed"),
    [
        # The second parent's boundary probes sit at 2 and 3 of the first batch: the denied one is rejected by its
        # command, and its valid partner is rejected as aborted with the wave.
        (
            _deny(_answer(_ONE_ROW), "p1", finish=False),
            [
                (1, "boundary", 3, PageOutcome.REJECTED, PageRejectionCode.COMMAND_FAILURE),
                (1, "boundary", 2, PageOutcome.REJECTED, PageRejectionCode.TRANSACTION_ABORTED),
            ],
        ),
        # Its finish page sits at 3 of the second batch: denied, or answered with a row its range excludes.
        (
            _deny(_answer(_ONE_ROW), "p1", finish=True),
            [(1, "finish", 3, PageOutcome.REJECTED, PageRejectionCode.COMMAND_FAILURE)],
        ),
        (
            _answer(_ONE_ROW, ignore_bounds=frozenset({"p1"})),
            [(1, "finish", 3, PageOutcome.REJECTED, PageRejectionCode.RANGE_CONTRADICTION)],
        ),
    ],
    ids=["wave-denied-and-aborted", "finish-denied", "finish-range-contradiction"],
)
async def test_a_rejected_keyset_reference_page_records_its_index_in_the_shared_batch(
    answer: Callable[..., Envelope],
    failed: list[tuple[int, str, int, PageOutcome, PageRejectionCode]],
) -> None:
    # Four one-row parents share the batches as in the committed case; the second parent's failed pages record
    # their commands' indexes in those shared batches, as its committed pages would.
    portal = BatchPortal(answer)
    async with client_for(portal, policy=ExecutionPolicy(page_trace_limit=1024)) as client:
        stream = client.iter_reference_outcomes(
            _base(),
            [_parent(f"p{index}") for index in range(4)],
            traversal=_traversal(AutoKeysetExecution(_CONTRACT)),
            dispatch=BatchDispatch(),
        )
        outcomes, report = await _drain(stream)

    assert [outcome.correlation for outcome in outcomes if isinstance(outcome, ReferenceFailure)] == ["p1"]
    assert [
        (
            record.reference_index,
            None if record.phase is None else record.phase.value,
            record.batch_index,
            record.outcome,
            record.rejection_code,
        )
        for record in report.page_trace
        if record.outcome is not PageOutcome.COMMITTED
    ] == failed


@pytest.mark.asyncio
async def test_a_flat_keyset_finish_page_sent_directly_records_no_batch_index() -> None:
    # The flat traversal's boundary probes share its own batch; its finish page is a direct request, with no batch
    # command to point to.
    portal = BatchPortal(_flat_answer((1,)))
    async with client_for(portal, policy=ExecutionPolicy(page_trace_limit=1024)) as client:
        stream = client.iter_list_keyset(
            _base(),
            selector=ResultSelector.root(),
            identity=_identity(),
            page_size=PAGE,
            keyset=_keyset(),
            execution=AutoKeysetExecution(_CONTRACT),
        )
        _items, report = await _drain(stream)

    assert portal.waves == [2, 1]
    assert [
        (None if record.phase is None else record.phase.value, record.dispatch, record.batch_index)
        for record in report.page_trace
    ] == [
        ("boundary", PageDispatch.BATCH, 0),
        ("boundary", PageDispatch.BATCH, 1),
        ("finish", PageDispatch.DIRECT, None),
    ]


# --- A descending finish page the admission frontier refuses ------------------------------------------


def _hides_low_rows_ascending(parents: frozenset[str]) -> Callable[..., Envelope]:
    """Serve rows 1..12 per parent; a listed parent's ascending view hides the rows below 5."""

    def answer(method: str, query: Query) -> Envelope:
        del method
        parent = query.get("filter[=parent]", "")
        ids = tuple(range(1, 13))
        if parent in parents and query.get("order[id]") != "DESC":
            ids = tuple(value for value in ids if value >= 5)
        return {"result": _rows(ids, query)}

    return answer


def _descending(execution: Execution) -> KeysetTraversal:
    return KeysetTraversal(
        selector=ResultSelector.root(),
        identity=_identity(),
        page_size=PAGE,
        keyset=KeysetSpec(limit_path=ParameterPath(("limit",)), direction="descending"),
        execution=execution,
    )


def _finish_rejection(report: OperationReport) -> list[tuple[object, ...]]:
    return [
        (
            record.reference_index,
            record.dispatch,
            record.batch_index,
            record.rows_selected,
            record.outcome,
            record.rejection_code,
        )
        for record in report.page_trace
        if record.phase is not None and record.phase.value == "finish" and record.outcome is not PageOutcome.COMMITTED
    ]


@pytest.mark.asyncio
async def test_a_flat_descending_finish_page_the_admission_frontier_refuses_is_traced_as_a_range_contradiction() -> (
    None
):
    # The boundary probes overlap ([5..12] ascending, [12..3] descending), so the traversal finishes below the
    # ascending probe's lowest row, 5, while the descending head already admitted down to 3. The finish page
    # answers [4, 3, 2, 1]: inside its lane, but behind the admitted frontier. The traversal fails with that
    # contradiction, and the direct finish page is traced as one.
    portal = BatchPortal(_hides_low_rows_ascending(frozenset({""})))
    async with client_for(portal, policy=ExecutionPolicy(page_trace_limit=1024)) as client:
        stream = client.iter_list_keyset(
            _base(),
            selector=ResultSelector.root(),
            identity=_identity(),
            page_size=PAGE,
            keyset=KeysetSpec(limit_path=ParameterPath(("limit",)), direction="descending"),
            execution=AutoKeysetExecution(_CONTRACT),
        )
        with pytest.raises(IncompleteTraversalError) as raised:
            await _drain(stream)

    assert isinstance(raised.value.__cause__, PaginationError)
    assert "did not advance" in str(raised.value.__cause__)
    assert portal.waves == [2, 1]
    report = stream.report
    assert report is not None
    # The refused page selected its four rows and admitted none: they are discarded, not lost from the provenance.
    assert _finish_rejection(report) == [
        (None, PageDispatch.DIRECT, None, 4, PageOutcome.REJECTED, PageRejectionCode.RANGE_CONTRADICTION)
    ]
    assert report.keyset_execution is not None
    assert report.keyset_execution.probe_rows_discarded == 4


@pytest.mark.asyncio
async def test_a_reference_descending_finish_page_the_admission_frontier_refuses_fails_its_binding_alone() -> None:
    # The same contradiction for the second of three parents: its binding fails with it, the others complete, and
    # its finish page, first in the shared second batch, is traced as a range contradiction at index 0.
    portal = BatchPortal(_hides_low_rows_ascending(frozenset({"p1"})))
    async with client_for(portal, policy=ExecutionPolicy(page_trace_limit=1024)) as client:
        stream = client.iter_reference_outcomes(
            _base(),
            [_parent(f"p{index}") for index in range(3)],
            traversal=_descending(AutoKeysetExecution(_CONTRACT)),
            dispatch=BatchDispatch(),
        )
        outcomes, report = await _drain(stream)

    failures = [outcome for outcome in outcomes if isinstance(outcome, ReferenceFailure)]
    assert [failure.correlation for failure in failures] == ["p1"]
    assert isinstance(failures[0].error, IncompleteTraversalError)
    assert isinstance(failures[0].error.__cause__, PaginationError)
    assert "did not advance" in str(failures[0].error.__cause__)
    assert set(_completes(outcomes)) == {"p0", "p2"}
    assert portal.waves == [6, 3]
    assert _finish_rejection(report) == [
        (1, PageDispatch.BATCH, 0, 4, PageOutcome.REJECTED, PageRejectionCode.RANGE_CONTRADICTION)
    ]


# --- A keyset engine call that fails as a whole -------------------------------------------------------

_UNDECODABLE = WireResponse(200, (("content-type", "application/json"),), b"[" * _NESTED + b"]" * _NESTED)
_DENIED = WireResponse(
    200, (("content-type", "application/json"),), b'{"error":"ACCESS_DENIED","error_description":"denied"}'
)
_UNKNOWN_PAGE = (PageOutcome.UNKNOWN, PageRejectionCode.AMBIGUOUS_EXECUTION)


def _failing(portal: BatchPortal, failing_send: int, answer: WireResponse) -> ResponderTransport:
    """Answer the ``failing_send``-th physical request with ``answer`` and every other one from ``portal``."""
    sends = [0]

    async def respond(request: Request) -> WireResponse:
        sends[0] += 1
        if sends[0] == failing_send:
            return answer
        return await portal.send(request, attempt_timeout=1.0, max_response_bytes=1 << 22)

    return ResponderTransport(respond, host=portal.host)


def _failed_pages(report: OperationReport) -> list[tuple[object, ...]]:
    return [
        (
            record.reference_index,
            None if record.phase is None else record.phase.value,
            record.dispatch,
            record.batch_index,
            record.outcome,
            record.rejection_code,
        )
        for record in report.page_trace
        if record.outcome is not PageOutcome.COMMITTED
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failing_send", "phase"),
    [(1, "boundary"), (2, "finish")],
    ids=["boundary-wave", "finish-pages"],
)
async def test_a_shared_keyset_batch_that_fails_as_a_whole_traces_each_page_the_way_it_settles(
    failing_send: int, phase: str
) -> None:
    # Three one-row parents share each batch: the boundary probes (two per parent) or the finish pages (one each).
    # The batch's answer cannot be decoded, so it fails as a whole: each binding fails, and each of its pages in
    # that batch, which may have run, is traced unknown with its command's index in the shared batch.
    portal = BatchPortal(_answer({f"p{index}": (1,) for index in range(3)}))
    async with client_for(
        _failing(portal, failing_send, _UNDECODABLE), policy=ExecutionPolicy(page_trace_limit=1024)
    ) as client:
        stream = client.iter_reference_outcomes(
            _base(),
            [_parent(f"p{index}") for index in range(3)],
            traversal=_traversal(AutoKeysetExecution(_CONTRACT)),
            dispatch=BatchDispatch(),
        )
        outcomes, report = await _drain(stream)

    assert sorted(str(outcome.correlation) for outcome in outcomes if isinstance(outcome, ReferenceFailure)) == [
        "p0",
        "p1",
        "p2",
    ]
    per_binding = 2 if phase == "boundary" else 1
    assert sorted(_failed_pages(report), key=lambda page: str(page[3])) == [
        (index // per_binding, phase, PageDispatch.BATCH, index, *_UNKNOWN_PAGE) for index in range(3 * per_binding)
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failing_send", "answer", "raised", "failed"),
    [
        # The boundary wave's private batch cannot be decoded: both probes may have run.
        (
            1,
            _UNDECODABLE,
            RecursionError,
            [
                (None, "boundary", PageDispatch.BATCH, 0, *_UNKNOWN_PAGE),
                (None, "boundary", PageDispatch.BATCH, 1, *_UNKNOWN_PAGE),
            ],
        ),
        # The direct finish request is refused: rejected by its own failure, with no batch to point to.
        (
            2,
            _DENIED,
            ApiResponseError,
            [(None, "finish", PageDispatch.DIRECT, None, PageOutcome.REJECTED, PageRejectionCode.COMMAND_FAILURE)],
        ),
    ],
    ids=["boundary-wave", "direct-finish"],
)
async def test_a_flat_keyset_request_that_fails_as_a_whole_traces_its_pages_the_way_they_settle(
    failing_send: int, answer: WireResponse, raised: type[Exception], failed: list[tuple[object, ...]]
) -> None:
    portal = BatchPortal(_flat_answer((1,)))
    async with client_for(
        _failing(portal, failing_send, answer), policy=ExecutionPolicy(page_trace_limit=1024)
    ) as client:
        stream = client.iter_list_keyset(
            _base(),
            selector=ResultSelector.root(),
            identity=_identity(),
            page_size=PAGE,
            keyset=_keyset(),
            execution=AutoKeysetExecution(_CONTRACT),
        )
        with pytest.raises(raised):
            await _drain(stream)

    report = stream.report
    assert report is not None
    assert _failed_pages(report) == failed


@pytest.mark.asyncio
async def test_a_flat_keyset_wave_its_cancellation_interrupts_traces_no_failed_page() -> None:
    # A cancellation is no failure of the wave's pages: they settle from what their commands did, and the
    # operation ends with no failed page to trace.
    started = asyncio.Event()
    serve = _flat_answer((1,))

    async def answer(method: str, query: Query) -> Envelope:
        started.set()
        await asyncio.Future[None]()
        return serve(method, query)

    portal = BatchPortal(answer)
    async with client_for(portal, policy=ExecutionPolicy(page_trace_limit=1024)) as client:
        stream = client.iter_list_keyset(
            _base(),
            selector=ResultSelector.root(),
            identity=_identity(),
            page_size=PAGE,
            keyset=_keyset(),
            execution=AutoKeysetExecution(_CONTRACT),
        )
        pull = asyncio.create_task(_drain(stream))
        await asyncio.wait_for(started.wait(), HANG_BOUND)
        pull.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pull

    report = stream.report
    assert report is not None
    assert _failed_pages(report) == []
