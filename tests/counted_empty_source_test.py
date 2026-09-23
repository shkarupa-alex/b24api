"""A counted head that returns no rows and no total closes as an observed empty source."""

from __future__ import annotations
import json
from typing import TYPE_CHECKING

import pytest

from b24api import (
    Bitrix24,
    ConsistencyPolicy,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    OffsetContinuation,
    OffsetSpec,
    PageDispatch,
    PageOutcome,
    PageRejectionCode,
    ReplaySafety,
    Request,
    TerminalState,
    TotalTermination,
    TraversalAssurance,
    WireResponse,
)
from b24api.completion.gate import CompletionReportFacts
from b24api.completion.recorder import CountedCompletionRecorder
from b24api.contracts.completion import (
    EMPTY_SOURCE_WITNESS,
    BindingClosure,
    CleanupState,
    CommandSettlement,
    StreamClosure,
)
from b24api.contracts.policy import (
    ConfirmationPolicy,
    DuplicatePolicy,
    IdentityRequirement,
    KernelState,
    OrderSemantics,
    SnapshotRequirement,
)
from b24api.contracts.request import RouteKind
from b24api.contracts.response import Response
from b24api.errors import CapabilityError, IncompleteTraversalError
from b24api.execution import Executor
from b24api.execution.snapshot import KernelReport
from b24api.traversal.driver import PaginationDriver
from b24api.traversal.plans import CountedOffsetPlan

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.contracts.json import JsonValue
    from b24api.contracts.report import OperationReport
    from b24api.contracts.stream import OperationStream

PAGE_SIZE = 50
_ABSENT = object()
_IDENTITY = IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_INTEGER)
_COUNTED = OffsetSpec(total_termination=TotalTermination.EXACT_QUALIFIED)


class _Transport:
    host = "test.invalid"

    def __init__(self, *payloads: dict[str, object] | Callable[[Request], dict[str, object]]) -> None:
        self.payloads = list(payloads)
        self.requests: list[Request] = []

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        self.requests.append(request)
        payload = self.payloads.pop(0)
        body = payload(request) if callable(payload) else payload
        return WireResponse(200, (("Content-Type", "application/json"),), json.dumps(body).encode())


def _batch_tail(rows: list[dict[str, int]], *, total: object = _ABSENT) -> Callable[[Request], dict[str, object]]:
    def respond(request: Request) -> dict[str, object]:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        (key,) = commands
        envelope: dict[str, object] = {"result": {key: rows}, "result_error": {}}
        if total is not _ABSENT:
            envelope["result_total"] = {key: total}
        return {"result": envelope}

    return respond


def _payload(rows: list[dict[str, int]], *, total: object = _ABSENT, next_value: object = _ABSENT) -> dict[str, object]:
    payload: dict[str, object] = {"result": rows}
    if total is not _ABSENT:
        payload["total"] = total
    if next_value is not _ABSENT:
        payload["next"] = next_value
    return payload


@pytest.fixture
def closures(monkeypatch: pytest.MonkeyPatch) -> list[tuple[BindingClosure, StreamClosure, int | None]]:
    observed: list[tuple[BindingClosure, StreamClosure, int | None]] = []
    original = CountedCompletionRecorder.terminal

    def spy(
        self: CountedCompletionRecorder,
        closure: BindingClosure,
        stream: StreamClosure,
        *,
        qualified_total: int | None = None,
    ) -> None:
        observed.append((closure, stream, qualified_total))
        original(self, closure, stream, qualified_total=qualified_total)

    monkeypatch.setattr(CountedCompletionRecorder, "terminal", spy)
    return observed


def _stream(
    transport: _Transport,
    *,
    identity: IdentitySpec | None = None,
    offset: OffsetSpec = _COUNTED,
    policy: ExecutionPolicy | None = None,
) -> OperationStream[JsonValue]:
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001
    return client.iter_list_counted(
        Request("user.get", {"filter": {"ID": 0}}, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        identity=identity,
        page_size=PAGE_SIZE,
        offset=offset,
        policy=policy,
    )


def _source_report(stream: OperationStream[JsonValue]) -> KernelReport:
    source = stream._source.report  # type: ignore[attr-defined] # noqa: SLF001
    assert isinstance(source, KernelReport)
    return source


@pytest.mark.asyncio
@pytest.mark.parametrize("total", [_ABSENT, None, -1], ids=["missing", "null", "unknown-sentinel"])
@pytest.mark.parametrize(
    ("identity", "assurance"),
    [(None, TraversalAssurance.MECHANICS_ONLY), (_IDENTITY, TraversalAssurance.IDENTITY_EXACT)],
    ids=["no-identity", "identity"],
)
async def test_empty_head_without_usable_total_closes_as_source_empty(
    closures: list[tuple[BindingClosure, StreamClosure, int | None]],
    total: object,
    identity: IdentitySpec | None,
    assurance: TraversalAssurance,
) -> None:
    transport = _Transport(_payload([], total=total))
    stream = _stream(transport, identity=identity)

    assert [row async for row in stream] == []

    assert len(transport.requests) == 1
    assert transport.requests[0].copy_parameters()["start"] == 0
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.COMPLETED
    assert report.exhausted
    assert report.assurance is assurance
    assert report.terminal_reason == EMPTY_SOURCE_WITNESS.terminal_reason
    assert report.physical_requests == 1
    assert report.batch_requests == 0
    assert report.batch_commands == 0
    assert report.emitted == 0
    assert report.violations == ()
    assert [
        (record.offset, record.dispatch, record.outcome, record.reported_total) for record in report.page_trace
    ] == [
        (0, PageDispatch.DIRECT, PageOutcome.COMMITTED, None),
    ]
    assert closures == [(BindingClosure.SOURCE_EMPTY, StreamClosure.NATURAL, None)]
    assert _source_report(stream).empty_source_witness is EMPTY_SOURCE_WITNESS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("identity", "assurance"),
    [(None, TraversalAssurance.COUNT_MATCHED), (_IDENTITY, TraversalAssurance.IDENTITY_AND_COUNT_MATCHED)],
    ids=["no-identity", "identity"],
)
async def test_empty_head_with_zero_total_keeps_the_qualified_total_closure(
    closures: list[tuple[BindingClosure, StreamClosure, int | None]],
    identity: IdentitySpec | None,
    assurance: TraversalAssurance,
) -> None:
    transport = _Transport(_payload([], total=0))
    stream = _stream(transport, identity=identity)

    assert [row async for row in stream] == []

    report = stream.report
    assert report is not None
    assert report.state is TerminalState.COMPLETED
    assert report.exhausted
    assert report.assurance is assurance
    assert report.terminal_reason != EMPTY_SOURCE_WITNESS.terminal_reason
    assert [record.reported_total for record in report.page_trace] == [0]
    assert closures == [(BindingClosure.QUALIFIED_TOTAL, StreamClosure.NATURAL, 0)]
    assert _source_report(stream).empty_source_witness is None


@pytest.mark.asyncio
async def test_positive_total_head_is_unchanged_and_batches_its_tail(
    closures: list[tuple[BindingClosure, StreamClosure, int | None]],
) -> None:
    rows = [{"ID": identity} for identity in range(1, PAGE_SIZE + 1)]
    tail = _batch_tail([{"ID": PAGE_SIZE + 1}], total=PAGE_SIZE + 1)
    transport = _Transport(_payload(rows, total=PAGE_SIZE + 1, next_value=PAGE_SIZE), tail)
    stream = _stream(transport, identity=_IDENTITY)

    assert len([row async for row in stream]) == PAGE_SIZE + 1

    report = stream.report
    assert report is not None
    assert report.state is TerminalState.COMPLETED
    assert report.assurance is TraversalAssurance.IDENTITY_AND_COUNT_MATCHED
    assert report.batch_requests == 1
    assert closures == [(BindingClosure.QUALIFIED_TOTAL, StreamClosure.NATURAL, PAGE_SIZE + 1)]


def _incomplete_cases() -> list[object]:
    fixed_step = OffsetSpec(
        continuation=OffsetContinuation.FIXED_STEP,
        step=PAGE_SIZE,
        total_termination=TotalTermination.EXACT_QUALIFIED,
    )
    qualified = ExecutionPolicy(consistency=ConsistencyPolicy(confirmation_policy=ConfirmationPolicy.QUALIFIED_TOTAL))
    return [
        pytest.param(_payload([{"ID": 1}]), _COUNTED, None, id="nonempty-missing-total"),
        pytest.param(_payload([{"ID": 1}], total=-1), _COUNTED, None, id="nonempty-unknown-total"),
        pytest.param(_payload([], next_value=PAGE_SIZE), _COUNTED, None, id="empty-with-continuation"),
        pytest.param(_payload([], total=-1, next_value=PAGE_SIZE), _COUNTED, None, id="empty-unknown-with-next"),
        pytest.param(_payload([], total=3), _COUNTED, None, id="positive-total-no-rows"),
        pytest.param(_payload([]), fixed_step, None, id="fixed-step"),
        pytest.param(_payload([]), _COUNTED, qualified, id="explicit-qualified-total"),
        pytest.param({"result": {"unexpected": "shape"}}, _COUNTED, None, id="invalid-page-shape"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(("payload", "offset", "policy"), _incomplete_cases())
async def test_heads_outside_the_empty_source_contract_remain_incomplete(
    closures: list[tuple[BindingClosure, StreamClosure, int | None]],
    payload: dict[str, object],
    offset: OffsetSpec,
    policy: ExecutionPolicy | None,
) -> None:
    transport = _Transport(payload)
    stream = _stream(transport, offset=offset, policy=policy)

    with pytest.raises(IncompleteTraversalError):
        _ = [row async for row in stream]

    assert len(transport.requests) == 1
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.INCOMPLETE
    assert not report.exhausted
    assert report.assurance is TraversalAssurance.MECHANICS_ONLY
    assert report.terminal_reason != EMPTY_SOURCE_WITNESS.terminal_reason
    assert [record.outcome for record in report.page_trace] == [PageOutcome.REJECTED]
    assert [closure for closure, _stream_closure, _total in closures] == [BindingClosure.FAILURE]
    assert _source_report(stream).empty_source_witness is None


@pytest.mark.asyncio
async def test_later_page_without_total_remains_incomplete() -> None:
    rows = [{"ID": identity} for identity in range(1, PAGE_SIZE + 1)]
    tail = _batch_tail([{"ID": PAGE_SIZE + 1}])
    transport = _Transport(_payload(rows, total=PAGE_SIZE + 1, next_value=PAGE_SIZE), tail)
    stream = _stream(transport, identity=_IDENTITY)

    with pytest.raises(IncompleteTraversalError) as caught:
        _ = [row async for row in stream]

    assert isinstance(caught.value.__cause__, CapabilityError)
    assert "non-negative total" in str(caught.value.__cause__)
    report = stream.report
    assert report is not None
    assert report.state is TerminalState.INCOMPLETE
    assert report.terminal_reason != EMPTY_SOURCE_WITNESS.terminal_reason
    assert [record.outcome for record in report.page_trace] == [PageOutcome.COMMITTED, PageOutcome.REJECTED]
    assert _source_report(stream).empty_source_witness is None


@pytest.mark.asyncio
async def test_empty_source_still_requires_a_verified_snapshot(
    closures: list[tuple[BindingClosure, StreamClosure, int | None]],
) -> None:
    policy = ExecutionPolicy(consistency=ConsistencyPolicy(snapshot_requirement=SnapshotRequirement.FROZEN_MANIFEST))
    transport = _Transport(_payload([]))
    stream = _stream(transport, policy=policy)

    assert [row async for row in stream] == []

    report = stream.report
    assert report is not None
    assert report.state is TerminalState.INCOMPLETE
    assert not report.exhausted
    assert report.assurance is TraversalAssurance.MECHANICS_ONLY
    assert report.terminal_reason == "required snapshot was not verified"
    assert [closure for closure, _stream_closure, _total in closures] == [BindingClosure.FAILURE]
    assert _source_report(stream).empty_source_witness is None


@pytest.mark.asyncio
async def test_rejected_eligible_head_leaves_no_empty_source_evidence(
    monkeypatch: pytest.MonkeyPatch,
    closures: list[tuple[BindingClosure, StreamClosure, int | None]],
) -> None:
    def refuse(self: CountedCompletionRecorder, identities: object, row_count: int) -> None:
        del self, identities, row_count
        raise CapabilityError("scripted commit refusal")

    monkeypatch.setattr(CountedCompletionRecorder, "validated", refuse)
    transport = _Transport(_payload([]))
    stream = _stream(transport)

    with pytest.raises(IncompleteTraversalError):
        _ = [row async for row in stream]

    report = stream.report
    assert report is not None
    assert report.state is TerminalState.INCOMPLETE
    assert report.terminal_reason != EMPTY_SOURCE_WITNESS.terminal_reason
    last = report.page_trace[-1]
    assert (last.outcome, last.rejection_code) == (PageOutcome.REJECTED, PageRejectionCode.RANGE_CONTRADICTION)
    assert [closure for closure, _stream_closure, _total in closures] == [BindingClosure.FAILURE]
    assert _source_report(stream).empty_source_witness is None
    driver = stream._source._driver  # type: ignore[attr-defined] # noqa: SLF001
    assert driver.empty_source_witness is None
    assert driver.terminal_reason is None
    assert driver._empty_source_allowance is False  # noqa: SLF001


def test_empty_source_witness_is_only_valid_on_a_completed_report_without_rows() -> None:
    KernelReport(state=KernelState.COMPLETED, empty_source_witness=EMPTY_SOURCE_WITNESS)
    with pytest.raises(ValueError, match="empty-source witness"):
        KernelReport(state=KernelState.INCOMPLETE, empty_source_witness=EMPTY_SOURCE_WITNESS)
    with pytest.raises(ValueError, match="empty-source witness"):
        KernelReport(
            state=KernelState.COMPLETED,
            emitted_rows=1,
            empty_source_witness=EMPTY_SOURCE_WITNESS,
        )


def _driver() -> PaginationDriver:
    executor = Executor(_Transport())
    plan = CountedOffsetPlan(
        identity_requirement=IdentityRequirement.OPTIONAL,
        order_semantics=OrderSemantics.UNORDERED,
        duplicate_policy=DuplicatePolicy.ERROR,
    )
    driver = PaginationDriver(
        executor,
        Request("user.get", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        plan,
        selector=None,
        identity=None,
        context=executor.context(ExecutionPolicy()),
    )
    driver.begin_external_validation()
    driver.schedule_page(offset=0, dispatch=PageDispatch.DIRECT)
    return driver


def test_empty_source_eligibility_is_pure_and_the_transaction_owns_the_witness() -> None:
    driver = _driver()
    response = Response([])
    items = driver.select_page(response)

    for _ in range(2):
        assert driver.empty_source_head_eligible(response, driver.source_page.current(items), items)
    assert driver.page_trace_count == 0
    assert driver.empty_source_witness is None
    assert driver.terminal_reason is None

    driver.validate_external_page(items, response, terminal=True, empty_source=True)

    assert driver.empty_source_witness is EMPTY_SOURCE_WITNESS
    assert driver.terminal_reason == EMPTY_SOURCE_WITNESS.terminal_reason
    assert driver._expected_total is None  # noqa: SLF001 - no synthesized zero total
    assert [record.outcome for record in driver.page_trace] == [PageOutcome.COMMITTED]
    assert not driver.empty_source_head_eligible(response, (), ())


def test_requested_allowance_on_an_ineligible_page_keeps_the_strict_total_checks() -> None:
    driver = _driver()
    response = Response([{"ID": 1}])
    items = driver.select_page(response)

    assert not driver.empty_source_head_eligible(response, driver.source_page.current(items), items)
    with pytest.raises(CapabilityError, match="non-negative total"):
        driver.validate_external_page(items, response, terminal=True, empty_source=True)

    assert driver.empty_source_witness is None
    assert driver.terminal_reason is None
    assert driver.validated_rows == 0
    assert [record.outcome for record in driver.page_trace] == [PageOutcome.REJECTED]


def _finished_gate(closure: BindingClosure, *, witness: bool) -> OperationReport:
    recorder = CountedCompletionRecorder()
    recorder.activate(recorder.reserve())
    recorder.settled(CommandSettlement.SUCCESS)
    recorder.validated((), 0)
    recorder.terminal(
        closure, StreamClosure.NATURAL, qualified_total=0 if closure is BindingClosure.QUALIFIED_TOTAL else None
    )
    recorder.cleanup(CleanupState.SUCCESS)
    source = KernelReport(
        state=KernelState.COMPLETED,
        empty_source_witness=EMPTY_SOURCE_WITNESS if witness else None,
    )
    recorder.gate.attach_report(
        CompletionReportFacts(
            source=source,
            operation="iter_list_counted",
            assurance=TraversalAssurance.IDENTITY_AND_COUNT_MATCHED,
            admitted=0,
            emitted=0,
            successes=0,
            failures=0,
            not_executed=0,
            unknown=0,
            buffered_commands_high_water=0,
            active_references_high_water=0,
        )
    )
    return recorder.gate.finish()


def test_gate_caps_a_witnessed_report_and_refuses_an_unbacked_witness() -> None:
    witnessed = _finished_gate(BindingClosure.SOURCE_EMPTY, witness=True)
    assert witnessed.state is TerminalState.COMPLETED
    assert witnessed.exhausted
    assert witnessed.assurance is TraversalAssurance.IDENTITY_EXACT

    counted = _finished_gate(BindingClosure.QUALIFIED_TOTAL, witness=False)
    assert counted.state is TerminalState.COMPLETED
    assert counted.assurance is TraversalAssurance.IDENTITY_AND_COUNT_MATCHED

    unbacked = _finished_gate(BindingClosure.QUALIFIED_TOTAL, witness=True)
    assert unbacked.state is TerminalState.INCOMPLETE
    assert not unbacked.exhausted
    assert [violation.code for violation in unbacked.violations] == ["completion_empty_witness_mismatch"]
