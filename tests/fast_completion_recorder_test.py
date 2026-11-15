"""Fast completion requires scheduler-qualified keyset closure evidence."""

from b24api.completion.fast_recorder import FastCompletionRecorder
from b24api.contracts.completion import CleanupState, CommandSettlement
from b24api.contracts.policy import KernelState
from b24api.contracts.report import PageOutcome, TerminalState


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


def test_a_page_never_handed_to_an_engine_settles_as_not_executed_before_it_is_rejected() -> None:
    # An engine settles every page it was handed, also when its call raises; a page scheduled but never handed to
    # one is settled by the ledger as not executed, so its rejection follows a settlement.
    recorder = FastCompletionRecorder()
    recorder.schedule("handed")
    recorder.settle("handed", CommandSettlement.UNKNOWN)
    recorder.schedule("never_handed")

    recorder.terminal(KernelState.FAILED, rows_emitted=0, rows_admitted=0, cleanup=CleanupState.SUCCESS)

    decision = recorder.gate.decision()
    assert not decision.violations
    assert (decision.pages_scheduled, decision.bindings_terminal) == (2, 1)
