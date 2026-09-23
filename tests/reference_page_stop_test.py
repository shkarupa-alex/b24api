"""A stopped reference binding leaves sibling pagination and correlation intact."""

from __future__ import annotations
import json
from urllib.parse import parse_qs

import pytest

from b24api import (
    BatchDispatch,
    Binding,
    Bitrix24,
    CallerStop,
    ContinuePage,
    CursorDomain,
    CursorSpec,
    DirectDispatch,
    IdentityCoercion,
    IncompleteTraversalError,
    PageBoundary,
    PaginationError,
    ParameterPath,
    ParameterUpdate,
    ReferenceComplete,
    ReferenceFailed,
    ReferenceFailure,
    ReferenceItem,
    Request,
    ResultSelector,
    RouteKind,
    TerminalState,
    TraversalAssurance,
)
from b24api.execution import Executor, WireResponse


class ChatTransport:
    """Two independent descending chats with a finite source oracle."""

    host = "fixture.invalid"

    def __init__(self) -> None:
        """Track each exact binding and cursor sent over the wire."""
        self.requests: list[tuple[str, int]] = []
        self.ids = {"a": (5, 4, 3, 2, 1), "b": (15, 14, 13)}
        self.physical_batches = 0

    def _rows(self, parent: str, control: int) -> list[dict[str, int]]:
        self.requests.append((parent, control))
        return [{"id": value} for value in self.ids[parent] if value < control][:2]

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Return at most two IDs strictly below LAST_ID."""
        assert attempt_timeout > 0
        assert max_response_bytes > 0
        if request.method == "batch":
            self.physical_batches += 1
            commands = request.copy_parameters()["cmd"]
            assert isinstance(commands, dict)
            result: dict[str, object] = {}
            for key, command in commands.items():
                assert isinstance(command, str)
                parameters = parse_qs(command.split("?", 1)[1])
                result[key] = self._rows(parameters["parent"][0], int(parameters["LAST_ID"][0]))
            return WireResponse(200, (), json.dumps({"result": {"result": result, "result_error": []}}).encode())
        parameters = request.copy_parameters()
        parent = parameters["parent"]
        control = parameters["LAST_ID"]
        assert isinstance(parent, str)
        assert isinstance(control, int)
        rows = self._rows(parent, control)
        return WireResponse(200, (), json.dumps({"result": rows}).encode())


class StopFirstChat:
    """Durably observe whole pages and stop only the first binding."""

    def __init__(self) -> None:
        """Store per-binding page evidence."""
        self.pages: list[PageBoundary] = []

    async def on_page(self, boundary: PageBoundary) -> ContinuePage | CallerStop:
        """Return one caller stop after the first chat's initial whole page."""
        self.pages.append(boundary)
        if boundary.binding_id == 0:
            return CallerStop("first chat cutoff")
        return ContinuePage.CONTINUE


@pytest.mark.asyncio
@pytest.mark.parametrize("dispatch_kind", ["direct", "batch"])
async def test_reference_page_stop_is_per_binding_and_not_source_exhaustion(dispatch_kind: str) -> None:
    transport = ChatTransport()
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    path = ParameterPath(("parent",))
    bindings = (
        Binding("chat a", (ParameterUpdate(path, "a"),), "a"),
        Binding("chat b", (ParameterUpdate(path, "b"),), "b"),
    )
    cursor = CursorSpec(
        ParameterPath(("LAST_ID",)),
        ("id",),
        IdentityCoercion.EXACT_INTEGER,
        "descending",
        "last",
        domain=CursorDomain.EXCLUSIVE_POSITIVE_INTEGER,
    )
    stop = StopFirstChat()
    dispatch = (
        BatchDispatch(batch_size=2, coalesce_wait=0) if dispatch_kind == "batch" else DirectDispatch(concurrency=2)
    )
    stream = client.iter_cursors(
        Request("messages.get", {"parent": "", "LAST_ID": 100}, route=RouteKind.BARE),
        bindings,
        selector=ResultSelector.root(),
        cursor=cursor,
        page_size=2,
        dispatch=dispatch,
        page_stop=stop,
    )
    events = [event async for event in stream]
    items = [event for event in events if isinstance(event, ReferenceItem)]
    completions = {event.binding_index: event for event in events if isinstance(event, ReferenceComplete)}
    assert [(item.binding_index, item.item["id"]) for item in items if item.binding_index == 0] == [(0, 5), (0, 4)]
    assert [(item.binding_index, item.item["id"]) for item in items if item.binding_index == 1] == [
        (1, 15),
        (1, 14),
        (1, 13),
    ]
    assert [control for parent, control in transport.requests if parent == "a"] == [100]
    assert [control for parent, control in transport.requests if parent == "b"] == [100, 14, 13]
    assert (transport.physical_batches > 0) is (dispatch_kind == "batch")
    assert not completions[0].exhausted
    assert completions[0].stop_reason == "first chat cutoff"
    assert completions[1].exhausted
    assert completions[1].stop_reason is None
    assert {boundary.binding_id for boundary in stop.pages} == {0, 1}
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED, stream.report.violations
    gate = stream._source.completion_gate  # noqa: SLF001 - observe the active scheduler gate
    decision = gate.decision()
    assert decision.state is TerminalState.COMPLETED
    assert not decision.exhausted
    assert decision.bindings_admitted == decision.bindings_terminal == len(bindings)
    assert decision.pages_scheduled == decision.pages_acknowledged
    assert gate.finish() == stream.report
    assert stream.report.assurance is TraversalAssurance.BOUNDED_PREFIX
    assert not stream.report.exhausted
    assert stream.report.partial


class RepeatingSecondChat(ChatTransport):
    """Chat b answers its second page with IDs it has already delivered."""

    def _rows(self, parent: str, control: int) -> list[dict[str, int]]:
        if parent == "b" and control != 100:  # noqa: PLR2004 - the initial LAST_ID of the fixture
            self.requests.append((parent, control))
            return [{"id": 15}, {"id": 14}]
        return super()._rows(parent, control)


def _two_chat_stream(transport: ChatTransport):  # noqa: ANN202 - public stream type is internal here
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    path = ParameterPath(("parent",))
    return client.iter_cursors(
        Request("messages.get", {"parent": "", "LAST_ID": 100}, route=RouteKind.BARE),
        (Binding("chat a", (ParameterUpdate(path, "a"),), "a"), Binding("chat b", (ParameterUpdate(path, "b"),), "b")),
        selector=ResultSelector.root(),
        cursor=CursorSpec(
            ParameterPath(("LAST_ID",)),
            ("id",),
            IdentityCoercion.EXACT_INTEGER,
            "descending",
            "last",
            domain=CursorDomain.EXCLUSIVE_POSITIVE_INTEGER,
        ),
        page_size=2,
        dispatch=DirectDispatch(concurrency=1),
        page_stop=StopFirstChat(),
    )


@pytest.mark.asyncio
async def test_incomplete_multi_chat_mirror_with_a_stopped_chat_reports_mechanics_only() -> None:
    stream = _two_chat_stream(RepeatingSecondChat())
    with pytest.raises(ReferenceFailed) as raised:
        async for _event in stream:
            pass
    failure = next(outcome for outcome in raised.value.outcomes if isinstance(outcome, ReferenceFailure))
    assert failure.correlation == "b"
    assert failure.partial_rows == 2  # noqa: PLR2004 - the first page of chat b
    assert isinstance(failure.error, IncompleteTraversalError)
    # Only the stream has an operation report; the binding carries its typed cause instead.
    assert failure.error.report is None
    assert isinstance(failure.error.error, PaginationError)
    assert str(failure.error) == "Traversal did not complete"
    assert stream.report is not None
    assert stream.report.state is TerminalState.INCOMPLETE
    assert stream.report.assurance is TraversalAssurance.MECHANICS_ONLY
    assert not stream.report.exhausted


@pytest.mark.asyncio
async def test_early_closed_multi_chat_mirror_with_a_stopped_chat_reports_mechanics_only() -> None:
    stream = _two_chat_stream(ChatTransport())
    async for event in stream:
        if isinstance(event, ReferenceComplete) and event.binding_index == 0:
            break
    await stream.aclose()
    assert stream.report is not None
    assert stream.report.state is TerminalState.EARLY_CLOSED
    assert stream.report.assurance is TraversalAssurance.MECHANICS_ONLY
    assert not stream.report.exhausted
