"""Fast completion requires scheduler-qualified keyset closure evidence."""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.completion.fast_recorder import FastCompletionRecorder
from b24api.contracts.completion import (
    BindingClosure,
    BindingTerminal,
    CleanupState,
    CommandSettlement,
    PageCommandOutcome,
    PageScheduled,
)
from b24api.contracts.policy import KernelState
from b24api.contracts.report import PageOutcome, TerminalState

if TYPE_CHECKING:
    import pytest

    from b24api.contracts.completion import CompletionEvidence


def test_discarded_nonempty_probe_cannot_claim_source_exhaustion() -> None:
    recorder = FastCompletionRecorder()
    recorder.schedule("probe")
    recorder.settle("probe", CommandSettlement.SUCCESS)
    recorder.validated("probe")
    recorder.recorded("probe", PageOutcome.COMMITTED)

    recorder.terminal(
        KernelState.COMPLETED,
        rows_emitted=0,
        rows_admitted=0,
        cleanup=CleanupState.SUCCESS,
    )

    decision = recorder.gate.decision()
    assert decision.state is TerminalState.COMPLETED_WITH_FAILURES
    assert decision.exhausted is False


def test_a_page_never_handed_to_an_engine_settles_as_not_executed_and_fails_its_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An engine settles every page it was handed, also when its call raises; a page scheduled but never handed to
    # one is settled by the ledger as not executed, which retires it, and its binding, with nothing that may have
    # run, closes as failed.
    recorder = FastCompletionRecorder()
    events: list[CompletionEvidence] = []
    emit = recorder.gate.emit

    def observe(event: CompletionEvidence) -> None:
        events.append(event)
        emit(event)

    monkeypatch.setattr(recorder.gate, "emit", observe)
    recorder.schedule("never_handed")

    recorder.terminal(KernelState.FAILED, rows_emitted=0, rows_admitted=0, cleanup=CleanupState.SUCCESS)

    decision = recorder.gate.decision()
    assert not decision.violations
    assert (decision.pages_scheduled, decision.bindings_terminal) == (1, 1)
    assert [type(event) for event in events][:3] == [PageScheduled, PageCommandOutcome, BindingTerminal]
    settled, terminal = events[1], events[2]
    assert isinstance(settled, PageCommandOutcome)
    assert settled.outcome is CommandSettlement.NOT_EXECUTED
    assert isinstance(terminal, BindingTerminal)
    assert terminal.closure is BindingClosure.FAILURE
