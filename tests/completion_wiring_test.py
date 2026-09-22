"""Public traversal proves live gate events are emitted around physical dispatch."""

from __future__ import annotations
import json

import pytest

from b24api import Bitrix24, Request, RouteKind, TerminalState
from b24api.completion import CompletionGate
from b24api.contracts.completion import (
    BindingTerminal,
    CleanupOutcome,
    PageAcknowledged,
    PageCommandOutcome,
    PageDelivered,
    PageScheduled,
    PageValidated,
    StreamTerminal,
)
from b24api.execution import Executor, WireResponse


class GateProbeTransport:
    """Assert scheduling evidence exists before each physical send."""

    def __init__(self) -> None:
        """Capture the observer installed after stream construction."""
        self.events: list[type[object]] = []
        self.calls = 0

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Return one full page and one empty confirmation."""
        assert request.method == "item.list"
        assert attempt_timeout > 0
        assert max_response_bytes > 0
        assert self.events[-1] is PageScheduled
        self.calls += 1
        rows = [{"id": 1}, {"id": 2}] if self.calls == 1 else []
        payload = {"result": rows, "next": 2 if rows else None}
        return WireResponse(200, (), json.dumps(payload).encode())


@pytest.mark.asyncio
async def test_public_offset_traversal_emits_ordered_page_and_cleanup_evidence() -> None:
    transport = GateProbeTransport()
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    stream = client.iter_list(Request("item.list", route=RouteKind.BARE), page_size=2)
    gate = stream._source.completion_gate  # noqa: SLF001 - observe the actual kernel gate
    assert isinstance(gate, CompletionGate)
    original_emit = gate.emit

    def observe(event: object) -> None:
        transport.events.append(type(event))
        original_emit(event)

    gate.emit = observe
    assert [row["id"] async for row in stream] == [1, 2]
    assert transport.events == [
        PageScheduled,
        PageCommandOutcome,
        PageValidated,
        PageDelivered,
        PageAcknowledged,
        PageScheduled,
        PageCommandOutcome,
        PageValidated,
        PageDelivered,
        PageAcknowledged,
        BindingTerminal,
        StreamTerminal,
        CleanupOutcome,
    ]
    decision = gate.decision()
    assert decision.state is TerminalState.COMPLETED
    assert decision.exhausted
    assert (
        decision.pages_scheduled
        == decision.pages_acknowledged
        == transport.calls
        == len(
            [event for event in transport.events if event is PageScheduled],
        )
    )
    assert stream.report is not None
    assert stream.report.exhausted
    assert gate.finish() == stream.report
