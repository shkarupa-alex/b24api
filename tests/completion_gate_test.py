"""Positive controls for correlated terminal evidence and bounded state."""

from __future__ import annotations

import pytest

from b24api.completion import CompletionGate
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
    PageScheduled,
    PageValidated,
    StreamClosure,
    StreamTerminal,
)
from b24api.contracts.report import TerminalState


def _page(gate: CompletionGate, *, acknowledged: bool = True) -> int:
    gate.emit(BindingAdmitted(operation_id="run", sequence=0, binding_id=0))
    gate.emit(PageScheduled(operation_id="run", sequence=1, binding_id=0, page_id=0))
    gate.emit(PageCommandOutcome(
        operation_id="run", sequence=2, binding_id=0, page_id=0, outcome=CommandSettlement.SUCCESS,
    ))
    gate.emit(PageValidated(
        operation_id="run", sequence=3, binding_id=0, page_id=0, identity_digest="abc", row_count=2,
    ))
    gate.emit(PageDelivered(operation_id="run", sequence=4, binding_id=0, page_id=0))
    if acknowledged:
        gate.emit(PageAcknowledged(operation_id="run", sequence=5, binding_id=0, page_id=0))
        return 6
    return 5


def _close(gate: CompletionGate, sequence: int, closure: BindingClosure) -> None:
    gate.emit(BindingTerminal(operation_id="run", sequence=sequence, binding_id=0, closure=closure))
    gate.emit(StreamTerminal(operation_id="run", sequence=sequence + 1, closure=StreamClosure.NATURAL))
    gate.emit(CleanupOutcome(operation_id="run", sequence=sequence + 2, state=CleanupState.SUCCESS))


def test_gate_requires_acknowledged_page_and_qualified_binding_closure() -> None:
    gate = CompletionGate("run")
    sequence = _page(gate)
    _close(gate, sequence, BindingClosure.SOURCE_EMPTY)
    decision = gate.finish()
    assert decision.state is TerminalState.COMPLETED
    assert decision.exhausted
    assert (decision.pages_scheduled, decision.pages_acknowledged) == (1, 1)


@pytest.mark.parametrize("closure", [BindingClosure.CALLER_STOP, BindingClosure.BOUNDARY_SEEN])
def test_gate_reports_successful_bounded_prefix_without_exhaustion(closure: BindingClosure) -> None:
    gate = CompletionGate("run")
    sequence = _page(gate)
    _close(gate, sequence, closure)
    decision = gate.finish()
    assert decision.state is TerminalState.COMPLETED
    assert not decision.exhausted


def test_gate_rejects_missing_acknowledgement_and_duplicate_page() -> None:
    gate = CompletionGate("run")
    sequence = _page(gate, acknowledged=False)
    gate.emit(PageScheduled(operation_id="run", sequence=sequence, binding_id=0, page_id=0))
    _close(gate, sequence + 1, BindingClosure.SOURCE_EMPTY)
    decision = gate.finish()
    assert decision.state is TerminalState.INCOMPLETE
    assert {violation.code for violation in decision.violations} == {
        "completion_duplicate_or_unbound_page", "completion_invalid_binding_terminal",
    }


def test_gate_rejects_delivery_before_validation() -> None:
    gate = CompletionGate("run")
    gate.emit(BindingAdmitted(operation_id="run", sequence=0, binding_id=0))
    gate.emit(PageScheduled(operation_id="run", sequence=1, binding_id=0, page_id=0))
    gate.emit(PageDelivered(operation_id="run", sequence=2, binding_id=0, page_id=0))
    gate.emit(StreamTerminal(operation_id="run", sequence=3, closure=StreamClosure.NATURAL))
    gate.emit(CleanupOutcome(operation_id="run", sequence=4, state=CleanupState.SUCCESS))
    assert gate.finish().state is TerminalState.INCOMPLETE
    assert gate.finish().violations[0].code == "completion_delivery_before_validation"


def test_gate_cannot_turn_a_failed_page_into_a_successful_binding() -> None:
    gate = CompletionGate("run")
    gate.emit(BindingAdmitted(operation_id="run", sequence=0, binding_id=0))
    gate.emit(PageScheduled(operation_id="run", sequence=1, binding_id=0, page_id=0))
    gate.emit(PageCommandOutcome(
        operation_id="run", sequence=2, binding_id=0, page_id=0, outcome=CommandSettlement.FAILURE,
    ))
    gate.emit(BindingTerminal(operation_id="run", sequence=3, binding_id=0, closure=BindingClosure.SOURCE_EMPTY))
    gate.emit(StreamTerminal(operation_id="run", sequence=4, closure=StreamClosure.NATURAL))
    gate.emit(CleanupOutcome(operation_id="run", sequence=5, state=CleanupState.SUCCESS))
    decision = gate.finish()
    assert decision.state is TerminalState.INCOMPLETE
    assert decision.violations[0].code == "completion_negative_binding_claimed_success"


def test_gate_accounts_for_all_failed_bindings_without_claiming_exhaustion() -> None:
    gate = CompletionGate("run")
    gate.emit(BindingAdmitted(operation_id="run", sequence=0, binding_id=0))
    gate.emit(PageScheduled(operation_id="run", sequence=1, binding_id=0, page_id=0))
    gate.emit(PageCommandOutcome(
        operation_id="run", sequence=2, binding_id=0, page_id=0, outcome=CommandSettlement.FAILURE,
    ))
    _close(gate, 3, BindingClosure.FAILURE)
    decision = gate.finish()
    assert decision.state is TerminalState.COMPLETED_WITH_FAILURES
    assert not decision.exhausted
