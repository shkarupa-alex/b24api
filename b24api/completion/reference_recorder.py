"""Bounded completion evidence for concurrently scheduled reference bindings."""

from __future__ import annotations
import hashlib
import json
from typing import TYPE_CHECKING, TypedDict
from uuid import uuid4

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

if TYPE_CHECKING:
    from collections.abc import Sequence

    from b24api.traversal.values import IdentityValue


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

    def _take(self) -> int:
        sequence = self._sequence
        self._sequence += 1
        return sequence

    def admit(self, binding_id: int) -> ReferenceBindingRecorder:
        """Register a binding before its worker can schedule a page."""
        recorder = ReferenceBindingRecorder(self, binding_id)
        self.gate.emit(BindingAdmitted(
            operation_id=self.gate.operation_id, sequence=self._take(), binding_id=binding_id,
        ))
        self._bindings[binding_id] = recorder
        return recorder

    def binding(self, binding_id: int) -> ReferenceBindingRecorder:
        """Return the active adapter for one admitted binding."""
        return self._bindings[binding_id]

    def terminal(self, binding_id: int, closure: BindingClosure) -> None:
        """Retire one accounted binding after its final outcome is delivered."""
        self.gate.emit(BindingTerminal(
            operation_id=self.gate.operation_id, sequence=self._take(),
            binding_id=binding_id, closure=closure,
        ))
        del self._bindings[binding_id]

    def stream_terminal(self, closure: StreamClosure) -> None:
        """Record producer termination before cleanup evidence."""
        self.gate.emit(StreamTerminal(
            operation_id=self.gate.operation_id, sequence=self._take(), closure=closure,
        ))

    def cleanup(self, state: CleanupState) -> None:
        """Record the final cleanup outcome once."""
        self.gate.emit(CleanupOutcome(
            operation_id=self.gate.operation_id, sequence=self._take(), state=state,
        ))


class ReferenceBindingRecorder:
    """Correlate sequential pages of one active reference binding."""

    def __init__(self, operation: ReferenceCompletionRecorder, binding_id: int) -> None:
        """Initialize the binding's local page sequence."""
        self._operation = operation
        self.binding_id = binding_id
        self._next_page = 0
        self._current: int | None = None
        self._delivered = False
        self._validated_rows: int | None = None
        self.unknown = False

    def _emit(self, event: CompletionEvidence) -> None:
        self._operation.gate.emit(event)

    def _fields(self) -> _PageFields:
        if self._current is None:
            raise RuntimeError("reference completion event lacks a scheduled page")
        return {
            "operation_id": self._operation.gate.operation_id,
            "sequence": self._operation._take(),  # noqa: SLF001 - one correlated operation sequence
            "binding_id": self.binding_id,
            "page_id": self._current,
        }

    def scheduled(self) -> None:
        """Record a page before its buffer reservation and physical dispatch."""
        if self._current is not None:
            raise RuntimeError("previous reference page remains active")
        self._current = self._next_page
        self._next_page += 1
        self._delivered = False
        self._validated_rows = None
        self._emit(PageScheduled(**self._fields()))

    def settled(self, outcome: CommandSettlement) -> None:
        """Record the physical command outcome for the scheduled page."""
        self._emit(PageCommandOutcome(**self._fields(), outcome=outcome))
        if outcome is not CommandSettlement.SUCCESS:
            self.unknown = outcome is CommandSettlement.UNKNOWN
            self._current = None

    def validated(self, identities: Sequence[IdentityValue], row_count: int) -> None:
        """Record committed validation without retaining identity values."""
        digest = hashlib.sha256(json.dumps(identities, separators=(",", ":")).encode()).hexdigest()
        self._emit(PageValidated(**self._fields(), identity_digest=digest, row_count=row_count))
        self._validated_rows = row_count

    def complete_omitted_empty(self) -> None:
        """Acknowledge an empty terminal page suppressed by the traversal driver."""
        if self._current is not None and self._validated_rows == 0:
            self.delivered()
            self.acknowledged()

    def rejected(self, reason: str) -> None:
        """Retire a successful command whose page validation failed."""
        self._emit(PageRejected(**self._fields(), reason=reason))
        self._current = None

    def delivered(self) -> None:
        """Mark the whole page delivered after its final consumer yield."""
        self._emit(PageDelivered(**self._fields()))
        self._delivered = True

    def acknowledged(self) -> None:
        """Acknowledge only after delivery and optional page-stop commit."""
        if not self._delivered:
            return
        self._emit(PageAcknowledged(**self._fields()))
        self._current = None
        self._delivered = False
