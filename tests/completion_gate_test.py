"""Positive controls for correlated terminal evidence and bounded state."""

from __future__ import annotations

import pytest

from b24api.completion import CompletionGate
from b24api.completion.gate import CompletionReportFacts
from b24api.contracts.completion import (
    BindingAdmitted,
    BindingClosure,
    BindingTerminal,
    CleanupOutcome,
    CleanupState,
    CommandSettlement,
    PageAcknowledged,
    PageCommandOutcome,
    PageDelivered,
    PageRejected,
    PageScheduled,
    PageValidated,
    StreamClosure,
    StreamTerminal,
)
from b24api.contracts.policy import KernelState
from b24api.contracts.report import TerminalState, TraversalAssurance
from b24api.execution.snapshot import KernelReport


def _page(gate: CompletionGate, *, acknowledged: bool = True, row_count: int = 2) -> int:
    gate.emit(BindingAdmitted(operation_id="run", sequence=0, binding_id=0))
    gate.emit(PageScheduled(operation_id="run", sequence=1, binding_id=0, page_id=0))
    gate.emit(
        PageCommandOutcome(
            operation_id="run",
            sequence=2,
            binding_id=0,
            page_id=0,
            outcome=CommandSettlement.SUCCESS,
        )
    )
    gate.emit(
        PageValidated(
            operation_id="run",
            sequence=3,
            binding_id=0,
            page_id=0,
            identity_digest="abc",
            row_count=row_count,
        )
    )
    gate.emit(PageDelivered(operation_id="run", sequence=4, binding_id=0, page_id=0))
    if acknowledged:
        gate.emit(PageAcknowledged(operation_id="run", sequence=5, binding_id=0, page_id=0))
        return 6
    return 5


def _close(gate: CompletionGate, sequence: int, closure: BindingClosure, *, qualified_total: int | None = None) -> None:
    gate.emit(
        BindingTerminal(
            operation_id="run",
            sequence=sequence,
            binding_id=0,
            closure=closure,
            qualified_total=qualified_total,
        )
    )
    gate.emit(StreamTerminal(operation_id="run", sequence=sequence + 1, closure=StreamClosure.NATURAL))
    gate.emit(CleanupOutcome(operation_id="run", sequence=sequence + 2, state=CleanupState.SUCCESS))


def test_gate_requires_acknowledged_page_and_qualified_binding_closure() -> None:
    gate = CompletionGate("run")
    sequence = _page(gate, row_count=0)
    _close(gate, sequence, BindingClosure.SOURCE_EMPTY)
    decision = gate.decision()
    assert decision.state is TerminalState.COMPLETED
    assert decision.exhausted
    assert (decision.pages_scheduled, decision.pages_acknowledged) == (1, 1)


def test_gate_rejects_nonempty_page_as_source_empty() -> None:
    gate = CompletionGate("run")
    _close(gate, _page(gate), BindingClosure.SOURCE_EMPTY)
    assert gate.decision().state is TerminalState.INCOMPLETE
    assert "completion_missing_empty_witness" in {item.code for item in gate.decision().violations}


def test_gate_rejects_natural_terminal_without_binding_or_empty_source_witness() -> None:
    gate = CompletionGate("run")
    gate.emit(StreamTerminal(operation_id="run", sequence=0, closure=StreamClosure.NATURAL))
    gate.emit(CleanupOutcome(operation_id="run", sequence=1, state=CleanupState.SUCCESS))
    assert gate.decision().state is TerminalState.INCOMPLETE
    assert not gate.decision().exhausted


def test_gate_accepts_explicit_empty_source_and_exact_total_only() -> None:
    empty = CompletionGate("run")
    empty.emit(StreamTerminal(operation_id="run", sequence=0, closure=StreamClosure.NATURAL, empty_source=True))
    empty.emit(CleanupOutcome(operation_id="run", sequence=1, state=CleanupState.SUCCESS))
    assert empty.decision().state is TerminalState.COMPLETED
    good = CompletionGate("run")
    _close(good, _page(good), BindingClosure.QUALIFIED_TOTAL, qualified_total=2)
    assert good.decision().state is TerminalState.COMPLETED
    wrong = CompletionGate("run")
    _close(wrong, _page(wrong), BindingClosure.QUALIFIED_TOTAL, qualified_total=3)
    assert wrong.decision().state is TerminalState.INCOMPLETE


@pytest.mark.parametrize("closure", [BindingClosure.CALLER_STOP, BindingClosure.BOUNDARY_SEEN])
def test_gate_reports_successful_bounded_prefix_without_exhaustion(closure: BindingClosure) -> None:
    gate = CompletionGate("run")
    sequence = _page(gate)
    _close(gate, sequence, closure)
    decision = gate.decision()
    assert decision.state is TerminalState.COMPLETED
    assert not decision.exhausted


def test_gate_rejects_missing_acknowledgement_and_duplicate_page() -> None:
    gate = CompletionGate("run")
    sequence = _page(gate, acknowledged=False)
    gate.emit(PageScheduled(operation_id="run", sequence=sequence, binding_id=0, page_id=0))
    _close(gate, sequence + 1, BindingClosure.SOURCE_EMPTY)
    decision = gate.decision()
    assert decision.state is TerminalState.INCOMPLETE
    assert {violation.code for violation in decision.violations} == {
        "completion_duplicate_or_unbound_page",
        "completion_invalid_binding_terminal",
    }


def test_gate_rejects_delivery_before_validation() -> None:
    gate = CompletionGate("run")
    gate.emit(BindingAdmitted(operation_id="run", sequence=0, binding_id=0))
    gate.emit(PageScheduled(operation_id="run", sequence=1, binding_id=0, page_id=0))
    gate.emit(PageDelivered(operation_id="run", sequence=2, binding_id=0, page_id=0))
    gate.emit(StreamTerminal(operation_id="run", sequence=3, closure=StreamClosure.NATURAL))
    gate.emit(CleanupOutcome(operation_id="run", sequence=4, state=CleanupState.SUCCESS))
    assert gate.decision().state is TerminalState.INCOMPLETE
    assert gate.decision().violations[0].code == "completion_delivery_before_validation"


def test_gate_cannot_turn_a_failed_page_into_a_successful_binding() -> None:
    gate = CompletionGate("run")
    gate.emit(BindingAdmitted(operation_id="run", sequence=0, binding_id=0))
    gate.emit(PageScheduled(operation_id="run", sequence=1, binding_id=0, page_id=0))
    gate.emit(
        PageCommandOutcome(
            operation_id="run",
            sequence=2,
            binding_id=0,
            page_id=0,
            outcome=CommandSettlement.FAILURE,
        )
    )
    gate.emit(BindingTerminal(operation_id="run", sequence=3, binding_id=0, closure=BindingClosure.SOURCE_EMPTY))
    gate.emit(StreamTerminal(operation_id="run", sequence=4, closure=StreamClosure.NATURAL))
    gate.emit(CleanupOutcome(operation_id="run", sequence=5, state=CleanupState.SUCCESS))
    decision = gate.decision()
    assert decision.state is TerminalState.INCOMPLETE
    assert decision.violations[0].code == "completion_negative_binding_claimed_success"


def test_gate_accounts_for_all_failed_bindings_without_claiming_exhaustion() -> None:
    gate = CompletionGate("run")
    gate.emit(BindingAdmitted(operation_id="run", sequence=0, binding_id=0))
    gate.emit(PageScheduled(operation_id="run", sequence=1, binding_id=0, page_id=0))
    gate.emit(
        PageCommandOutcome(
            operation_id="run",
            sequence=2,
            binding_id=0,
            page_id=0,
            outcome=CommandSettlement.FAILURE,
        )
    )
    _close(gate, 3, BindingClosure.FAILURE)
    decision = gate.decision()
    assert decision.state is TerminalState.COMPLETED_WITH_FAILURES
    assert not decision.exhausted


def test_gate_accounts_for_typed_unknown_without_claiming_exhaustion() -> None:
    gate = CompletionGate("run")
    gate.emit(BindingAdmitted(operation_id="run", sequence=0, binding_id=0))
    gate.emit(PageScheduled(operation_id="run", sequence=1, binding_id=0, page_id=0))
    gate.emit(
        PageCommandOutcome(
            operation_id="run",
            sequence=2,
            binding_id=0,
            page_id=0,
            outcome=CommandSettlement.UNKNOWN,
        )
    )
    _close(gate, 3, BindingClosure.UNKNOWN)
    decision = gate.decision()
    assert decision.state is TerminalState.COMPLETED_WITH_FAILURES
    assert not decision.exhausted


@pytest.mark.parametrize("settlement", [CommandSettlement.FAILURE, CommandSettlement.NOT_EXECUTED])
def test_gate_accounts_for_each_known_negative_command_settlement(settlement: CommandSettlement) -> None:
    gate = CompletionGate("run")
    gate.emit(BindingAdmitted(operation_id="run", sequence=0, binding_id=0))
    gate.emit(PageScheduled(operation_id="run", sequence=1, binding_id=0, page_id=0))
    gate.emit(
        PageCommandOutcome(
            operation_id="run",
            sequence=2,
            binding_id=0,
            page_id=0,
            outcome=settlement,
        )
    )
    _close(gate, 3, BindingClosure.FAILURE)
    decision = gate.decision()
    assert decision.state is TerminalState.COMPLETED_WITH_FAILURES
    assert not decision.exhausted


def test_gate_accounts_for_page_rejection_and_cleanup_failure() -> None:
    rejected = CompletionGate("run")
    rejected.emit(BindingAdmitted(operation_id="run", sequence=0, binding_id=0))
    rejected.emit(PageScheduled(operation_id="run", sequence=1, binding_id=0, page_id=0))
    rejected.emit(
        PageCommandOutcome(
            operation_id="run",
            sequence=2,
            binding_id=0,
            page_id=0,
            outcome=CommandSettlement.SUCCESS,
        )
    )
    rejected.emit(PageRejected(operation_id="run", sequence=3, binding_id=0, page_id=0, reason="invalid"))
    _close(rejected, 4, BindingClosure.FAILURE)
    assert rejected.decision().state is TerminalState.COMPLETED_WITH_FAILURES

    cleanup_failed = CompletionGate("run")
    sequence = _page(cleanup_failed, row_count=0)
    cleanup_failed.emit(
        BindingTerminal(
            operation_id="run",
            sequence=sequence,
            binding_id=0,
            closure=BindingClosure.SOURCE_EMPTY,
        )
    )
    cleanup_failed.emit(
        StreamTerminal(operation_id="run", sequence=sequence + 1, closure=StreamClosure.NATURAL),
    )
    cleanup_failed.emit(
        CleanupOutcome(operation_id="run", sequence=sequence + 2, state=CleanupState.FAILURE),
    )
    assert cleanup_failed.decision().state is TerminalState.INCOMPLETE


def test_gate_retains_protocol_violations_for_rejection_order_and_terminal_order() -> None:
    gate = CompletionGate("run")
    sequence = _page(gate, acknowledged=False)
    gate.emit(PageRejected(operation_id="run", sequence=sequence, binding_id=0, page_id=0, reason="late"))
    gate.emit(StreamTerminal(operation_id="run", sequence=sequence + 1, closure=StreamClosure.NATURAL))
    gate.emit(BindingTerminal(operation_id="run", sequence=sequence + 2, binding_id=0, closure=BindingClosure.FAILURE))
    gate.emit(CleanupOutcome(operation_id="run", sequence=sequence + 3, state=CleanupState.SUCCESS))
    violations = {item.code for item in gate.decision().violations}
    assert "completion_rejection_after_delivery" in violations
    assert "completion_after_terminal" in violations


def test_gate_retains_non_monotonic_sequence_and_invalid_cleanup_order() -> None:
    gate = CompletionGate("run")
    gate.emit(CleanupOutcome(operation_id="run", sequence=1, state=CleanupState.SUCCESS))
    gate.emit(StreamTerminal(operation_id="run", sequence=0, closure=StreamClosure.NATURAL, empty_source=True))
    gate.emit(StreamTerminal(operation_id="run", sequence=2, closure=StreamClosure.NATURAL, empty_source=True))
    gate.emit(CleanupOutcome(operation_id="run", sequence=3, state=CleanupState.SUCCESS))
    violations = {item.code for item in gate.decision().violations}
    assert "completion_invalid_cleanup" in violations
    assert "completion_event_order" in violations


def test_gate_rejects_public_success_counts_that_omit_a_failed_binding() -> None:
    gate = CompletionGate("run")
    gate.emit(BindingAdmitted(operation_id="run", sequence=0, binding_id=0))
    gate.emit(PageScheduled(operation_id="run", sequence=1, binding_id=0, page_id=0))
    gate.emit(
        PageCommandOutcome(
            operation_id="run",
            sequence=2,
            binding_id=0,
            page_id=0,
            outcome=CommandSettlement.FAILURE,
        )
    )
    _close(gate, 3, BindingClosure.FAILURE)
    gate.attach_report(
        CompletionReportFacts(
            source=KernelReport(state=KernelState.COMPLETED),
            operation="batch_outcomes",
            assurance=None,
            admitted=1,
            emitted=0,
            successes=0,
            failures=0,
            not_executed=0,
            unknown=0,
            buffered_commands_high_water=0,
            active_references_high_water=0,
        )
    )
    report = gate.finish()
    assert report.state is TerminalState.INCOMPLETE
    assert "completion_outcome_count_mismatch" in {item.code for item in report.violations}


@pytest.mark.parametrize(
    ("declared", "reported"),
    [
        (TraversalAssurance.RAW_RANGE_COVERED, TraversalAssurance.MECHANICS_ONLY),
        (TraversalAssurance.IDENTITY_EXACT, TraversalAssurance.MECHANICS_ONLY),
        (TraversalAssurance.COUNT_MATCHED, TraversalAssurance.MECHANICS_ONLY),
        (None, None),
    ],
)
def test_incomplete_report_never_claims_its_declared_assurance(
    declared: TraversalAssurance | None,
    reported: TraversalAssurance | None,
) -> None:
    gate = CompletionGate("run")
    gate.emit(BindingAdmitted(operation_id="run", sequence=0, binding_id=0))
    gate.emit(PageScheduled(operation_id="run", sequence=1, binding_id=0, page_id=0))
    gate.emit(
        PageCommandOutcome(operation_id="run", sequence=2, binding_id=0, page_id=0, outcome=CommandSettlement.UNKNOWN)
    )
    _close(gate, 3, BindingClosure.UNKNOWN)
    gate.attach_report(
        CompletionReportFacts(
            source=KernelReport(state=KernelState.INCOMPLETE),
            operation="iter_list",
            assurance=declared,
            admitted=1,
            emitted=0,
            successes=0,
            failures=0,
            not_executed=0,
            unknown=1,
            buffered_commands_high_water=0,
            active_references_high_water=0,
        )
    )
    report = gate.finish()
    assert report.state not in {TerminalState.COMPLETED, TerminalState.COMPLETED_WITH_FAILURES}
    assert not report.exhausted
    assert report.assurance is reported


@pytest.mark.parametrize("settled", [False, True], ids=["scheduled-only", "settled"])
def test_rejection_requires_successful_command_settlement(settled: bool) -> None:  # noqa: FBT001
    gate = CompletionGate("run")
    gate.emit(BindingAdmitted(operation_id="run", sequence=0, binding_id=0))
    gate.emit(PageScheduled(operation_id="run", sequence=1, binding_id=0, page_id=0))
    sequence = 2
    if settled:
        gate.emit(
            PageCommandOutcome(
                operation_id="run",
                sequence=sequence,
                binding_id=0,
                page_id=0,
                outcome=CommandSettlement.SUCCESS,
            )
        )
        sequence += 1
    gate.emit(PageRejected(operation_id="run", sequence=sequence, binding_id=0, page_id=0, reason="shape"))
    gate.emit(
        BindingTerminal(operation_id="run", sequence=sequence + 1, binding_id=0, closure=BindingClosure.FAILURE),
    )
    gate.emit(StreamTerminal(operation_id="run", sequence=sequence + 2, closure=StreamClosure.NATURAL))
    gate.emit(CleanupOutcome(operation_id="run", sequence=sequence + 3, state=CleanupState.SUCCESS))
    decision = gate.decision()
    violations = {item.code for item in decision.violations}
    if settled:
        assert decision.state is TerminalState.COMPLETED_WITH_FAILURES
        assert not violations
    else:
        assert decision.state is TerminalState.INCOMPLETE
        assert "completion_rejection_before_settlement" in violations


def _caller_stop_report(
    source_state: KernelState,
    *,
    cleanup: CleanupState = CleanupState.SUCCESS,
    early_closed: bool = False,
) -> tuple[TerminalState, TraversalAssurance | None, bool]:
    gate = CompletionGate("run")
    sequence = _page(gate)
    gate.emit(BindingTerminal(operation_id="run", sequence=sequence, binding_id=0, closure=BindingClosure.CALLER_STOP))
    gate.emit(StreamTerminal(operation_id="run", sequence=sequence + 1, closure=StreamClosure.NATURAL))
    gate.emit(CleanupOutcome(operation_id="run", sequence=sequence + 2, state=cleanup))
    gate.attach_report(
        CompletionReportFacts(
            source=KernelReport(state=source_state),
            operation="iter_list",
            assurance=TraversalAssurance.IDENTITY_EXACT,
            admitted=1,
            emitted=2,
            successes=1,
            failures=0,
            not_executed=0,
            unknown=0,
            buffered_commands_high_water=0,
            active_references_high_water=0,
            early_closed=early_closed,
        )
    )
    report = gate.finish()
    return report.state, report.assurance, report.exhausted


@pytest.mark.parametrize(
    ("source_state", "cleanup", "early_closed"),
    [
        (KernelState.INCOMPLETE, CleanupState.SUCCESS, False),
        (KernelState.FAILED, CleanupState.SUCCESS, False),
        (KernelState.CANCELLED, CleanupState.SUCCESS, False),
        (KernelState.COMPLETED, CleanupState.FAILURE, False),
        (KernelState.COMPLETED, CleanupState.SUCCESS, True),
    ],
    ids=["incomplete", "failed", "cancelled", "cleanup-failure", "early-closed"],
)
def test_caller_stop_never_lifts_assurance_of_an_unfinished_report(
    source_state: KernelState,
    cleanup: CleanupState,
    early_closed: bool,  # noqa: FBT001
) -> None:
    state, assurance, exhausted = _caller_stop_report(source_state, cleanup=cleanup, early_closed=early_closed)
    assert state not in {TerminalState.COMPLETED, TerminalState.COMPLETED_WITH_FAILURES}
    assert assurance is TraversalAssurance.MECHANICS_ONLY
    assert not exhausted


def test_completed_caller_stop_reports_bounded_prefix() -> None:
    assert _caller_stop_report(KernelState.COMPLETED) == (
        TerminalState.COMPLETED,
        TraversalAssurance.BOUNDED_PREFIX,
        False,
    )
