"""Bounded completion evidence for concurrently scheduled reference bindings."""

from __future__ import annotations
from typing import TypedDict
from uuid import uuid4

from b24api.completion.fast_recorder import FastPageLedger
from b24api.completion.gate import CompletionGate
from b24api.contracts.completion import (
    BindingAdmitted,
    BindingClosure,
    BindingTerminal,
    CleanupOutcome,
    CleanupState,
    CommandSettlement,
    CompletionEvidence,
    PageAcknowledged,
    PageCommandOutcome,
    PageDelivered,
    PageRejected,
    PageScheduled,
    PageValidated,
    StreamClosure,
    StreamTerminal,
)
from b24api.contracts.report import PageRejectionCode


class _PageFields(TypedDict):
    operation_id: str
    sequence: int
    binding_id: int
    page_id: int


class ReferenceCompletionRecorder:
    """Own one operation gate and a bounded set of active binding recorders."""

    def __init__(self) -> None:
        """Initialize without admitting or dispatching any reference."""
        self.gate = CompletionGate(uuid4().hex)
        self._sequence = 0
        self._bindings: dict[int, ReferenceBindingRecorder] = {}
        self._admitted_count = 0

    def _take(self) -> int:
        sequence = self._sequence
        self._sequence += 1
        return sequence

    def admit(self, binding_id: int) -> ReferenceBindingRecorder:
        """Register a binding before its worker can schedule a page."""
        recorder = ReferenceBindingRecorder(self, binding_id)
        self.gate.emit(
            BindingAdmitted(
                operation_id=self.gate.operation_id,
                sequence=self._take(),
                binding_id=binding_id,
            )
        )
        self._bindings[binding_id] = recorder
        self._admitted_count += 1
        return recorder

    def binding(self, binding_id: int) -> ReferenceBindingRecorder:
        """Return the active adapter for one admitted binding."""
        return self._bindings[binding_id]

    def terminal(
        self,
        binding_id: int,
        closure: BindingClosure,
        *,
        qualified_total: int | None = None,
        declared_short_page_width: int | None = None,
        qualified_witnesses: int | None = None,
    ) -> None:
        """Retire one accounted binding after its final outcome is delivered."""
        self.gate.emit(
            BindingTerminal(
                operation_id=self.gate.operation_id,
                sequence=self._take(),
                binding_id=binding_id,
                closure=closure,
                qualified_total=qualified_total,
                qualified_witnesses=qualified_witnesses,
                declared_short_page_width=declared_short_page_width,
            )
        )
        del self._bindings[binding_id]

    def stream_terminal(self, closure: StreamClosure) -> None:
        """Record producer termination before cleanup evidence."""
        self.gate.emit(
            StreamTerminal(
                operation_id=self.gate.operation_id,
                sequence=self._take(),
                closure=closure,
                empty_source=closure is StreamClosure.NATURAL and self._admitted_count == 0,
            )
        )

    def cleanup(self, state: CleanupState) -> None:
        """Record the final cleanup outcome once."""
        self.gate.emit(
            CleanupOutcome(
                operation_id=self.gate.operation_id,
                sequence=self._take(),
                state=state,
            )
        )


class ReferenceBindingRecorder:
    """Correlate the pages of one active reference binding: one current page, any number in delivery."""

    def __init__(self, operation: ReferenceCompletionRecorder, binding_id: int) -> None:
        """Initialize the binding's local page sequence."""
        self._operation = operation
        self.binding_id = binding_id
        self._next_page = 0
        self._current: int | None = None
        self._validated_rows: int | None = None
        self._reserved: set[int] = set()
        # Pages handed to delivery, each mapped to whether the consumer has delivered it yet: the
        # binding schedules its next page as soon as one is handed over, not once it is acknowledged.
        self._handed: dict[int, bool] = {}
        self.unknown = False

    def _emit(self, event: CompletionEvidence) -> None:
        self._operation.gate.emit(event)

    def _fields(self, page_id: int | None = None) -> _PageFields:
        current = self._current if page_id is None else page_id
        if current is None:
            raise RuntimeError("reference completion event lacks a scheduled page")
        return {
            "operation_id": self._operation.gate.operation_id,
            "sequence": self._operation._take(),  # noqa: SLF001 - one correlated operation sequence
            "binding_id": self.binding_id,
            "page_id": current,
        }

    def allocate_page(self) -> int:
        """Return the binding's next page id for a page scheduled outside the sequential protocol."""
        page_id = self._next_page
        self._next_page += 1
        return page_id

    def emit_page(self, event_type: type, page_id: int, **fields: object) -> None:
        """Emit one page event of this binding under the operation's correlated sequence."""
        self._emit(event_type(**self._fields(page_id), **fields))

    def mark_unknown(self) -> None:
        """Record that one of the binding's commands has an unknown outcome."""
        self.unknown = True

    def reserve(self) -> int:
        """Schedule a planned page whose command may be sent before the active page is acknowledged."""
        page_id = self._next_page
        self._next_page += 1
        self._emit(PageScheduled(**self._fields(page_id)))
        self._reserved.add(page_id)
        return page_id

    def activate(self, page_id: int) -> None:
        """Make one reserved page the binding's current page, in planned order."""
        if self._current is not None or page_id not in self._reserved:
            raise RuntimeError("reference page activation lacks a reserved page")
        self._reserved.discard(page_id)
        self._current = page_id
        self._validated_rows = None

    def abandon_reserved(self, page_id: int, outcome: CommandSettlement) -> None:
        """Settle a reserved page its producer will never commit: negative, or successful but unconsumed."""
        if page_id not in self._reserved:
            raise RuntimeError("reference page abandonment lacks a reserved page")
        self._reserved.discard(page_id)
        self._emit(PageCommandOutcome(**self._fields(page_id), outcome=outcome))
        if outcome is CommandSettlement.SUCCESS:
            self._emit(PageRejected(**self._fields(page_id), reason=PageRejectionCode.TRANSACTION_ABORTED.value))
        elif outcome is CommandSettlement.UNKNOWN:
            self.unknown = True

    def scheduled(self) -> None:
        """Record a page before its buffer reservation and physical dispatch."""
        if self._current is not None:
            raise RuntimeError("previous reference page remains active")
        self._current = self._next_page
        self._next_page += 1
        self._validated_rows = None
        self._emit(PageScheduled(**self._fields()))

    def settled(self, outcome: CommandSettlement) -> None:
        """Record the physical command outcome for the scheduled page."""
        self._emit(PageCommandOutcome(**self._fields(), outcome=outcome))
        if outcome is not CommandSettlement.SUCCESS:
            self.unknown = outcome is CommandSettlement.UNKNOWN
            self._current = None

    def validated(self, row_count: int) -> None:
        """Record committed validation without retaining identity values."""
        self._emit(PageValidated(**self._fields(), row_count=row_count))
        self._validated_rows = row_count

    def complete_omitted_empty(self) -> None:
        """Acknowledge an empty terminal page suppressed by the traversal driver."""
        if self._current is not None and self._validated_rows == 0:
            self.deliver_current()

    def deliver_current(self) -> None:
        """Hand over, deliver and acknowledge the current page in one step: no consumer stands between."""
        page_id = self.hand_over()
        self.delivered(page_id)
        self.acknowledged(page_id)

    def rejected(self, reason: str) -> None:
        """Retire a successful command whose page validation failed."""
        self._emit(PageRejected(**self._fields(), reason=reason))
        self._current = None

    def hand_over(self) -> int:
        """Hand the current page to delivery and return its id; the binding may schedule its next page."""
        page_id = self._current
        if page_id is None:
            raise RuntimeError("reference page handover lacks a scheduled page")
        self._current = None
        self._validated_rows = None
        self._handed[page_id] = False
        return page_id

    def delivered(self, page_id: int) -> None:
        """Mark a handed-over page delivered after its final consumer yield."""
        if page_id not in self._handed:
            raise RuntimeError("reference page delivery lacks a handed-over page")
        self._emit(PageDelivered(**self._fields(page_id)))
        self._handed[page_id] = True

    def acknowledged(self, page_id: int) -> None:
        """Acknowledge only after delivery and optional page-stop commit."""
        if not self._handed.get(page_id, False):
            return
        del self._handed[page_id]
        self._emit(PageAcknowledged(**self._fields(page_id)))


class ReferenceFastLedger(FastPageLedger):
    """Fast keyset page evidence of one reference binding, emitted into the shared operation gate."""

    def __init__(self, binding: ReferenceBindingRecorder) -> None:
        """Bind the binding recorder whose page ids and sequence the fast pages use."""
        super().__init__()
        self._binding = binding

    def _new_page(self) -> int:
        return self._binding.allocate_page()

    def _emit_page(self, event_type: type, page_id: int, **fields: object) -> None:
        self._binding.emit_page(event_type, page_id, **fields)

    def settle(self, command_id: str, outcome: CommandSettlement) -> None:
        """Record one settlement; an unknown outcome makes the binding's closure unknown."""
        super().settle(command_id, outcome)
        if outcome is CommandSettlement.UNKNOWN:
            self._binding.mark_unknown()

    def settle_wave(self, outcomes: tuple[CommandSettlement, ...]) -> None:
        """Settle the trailing scheduled pages of a wave whose engine call raised, in schedule order."""
        unsettled = [command_id for command_id, page in self._active.items() if not page.settled]
        if len(outcomes) > len(unsettled):
            raise RuntimeError("fast wave settlement exceeds its scheduled pages")
        for command_id, outcome in zip(unsettled[len(unsettled) - len(outcomes) :], outcomes, strict=True):
            self.settle(command_id, outcome)

    def close(self, *, successful: bool, reason: str, unsettled: CommandSettlement) -> None:
        """Retire every open page before the binding terminal: discarded probes, or a failed traversal."""
        if successful:
            self.discard_unadmitted()
            if not self._active:
                return
        for command_id, page in tuple(self._active.items()):
            if not page.settled:
                self.settle(command_id, unsettled)
        self._reject_active(reason)


__all__ = ["ReferenceBindingRecorder", "ReferenceCompletionRecorder", "ReferenceFastLedger"]
