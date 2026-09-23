"""Fast completion requires scheduler-qualified keyset closure evidence."""

from b24api.completion.fast_recorder import FastCompletionRecorder
from b24api.contracts.completion import CleanupState, CommandSettlement
from b24api.contracts.policy import KernelState
from b24api.contracts.report import PageOutcome, TerminalState


def test_discarded_nonempty_probe_cannot_claim_source_exhaustion() -> None:
    recorder = FastCompletionRecorder()
    recorder.schedule("probe")
    recorder.settle("probe", CommandSettlement.SUCCESS)
    recorder.validated("probe", (1, 2))
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
