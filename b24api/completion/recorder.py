"""One-binding event adapter for direct sequential traversal mechanics."""

from __future__ import annotations
from typing import Protocol
from uuid import uuid4

from b24api.completion.closure import qualified_closure
from b24api.completion.gate import CompletionGate
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


class CompletionSink(Protocol):
    """Page lifecycle methods required by the traversal driver."""

    def scheduled(self) -> None:
        """Register a page before dispatch."""
        ...

    def settled(self, outcome: CommandSettlement) -> None:
        """Register a physical outcome."""
        ...

    def validated(self, row_count: int) -> None:
        """Register an admitted page."""
        ...

    def rejected(self, reason: str) -> None:
        """Retire a rejected page."""
        ...


class CompletionRecorder:
    """Emit bounded correlated events in the physical traversal lifecycle."""

    def __init__(self) -> None:
        """Admit the sole binding before its first possible page dispatch."""
        self.gate = CompletionGate(uuid4().hex)
        self._sequence = 0
        self._next_page = 0
        self._current: int | None = None
        self._unknown = False
        self.gate.emit(BindingAdmitted(operation_id=self.gate.operation_id, sequence=self._take(), binding_id=0))

    def _take(self) -> int:
        sequence = self._sequence
        self._sequence += 1
        return sequence

    @property
    def has_unknown(self) -> bool:
        """Report whether any physical dispatch remained ambiguous."""
        return self._unknown

    def scheduled(self) -> None:
        """Record a unique page before reserving or dispatching it."""
        if self._current is not None:
            raise RuntimeError("previous logical page is still active")
        page_id = self._next_page
        self._next_page += 1
        self._current = page_id
        self.gate.emit(
            PageScheduled(
                operation_id=self.gate.operation_id,
                sequence=self._take(),
                binding_id=0,
                page_id=page_id,
            )
        )

    def settled(self, outcome: CommandSettlement) -> None:
        """Record dispatch success or a terminal negative command outcome."""
        page_id = self._require_page()
        self.gate.emit(
            PageCommandOutcome(
                operation_id=self.gate.operation_id,
                sequence=self._take(),
                binding_id=0,
                page_id=page_id,
                outcome=outcome,
            )
        )
        if outcome is not CommandSettlement.SUCCESS:
            self._unknown = self._unknown or outcome is CommandSettlement.UNKNOWN
            self._current = None

    def validated(self, row_count: int) -> None:
        """Record the validated row count of the current page without retaining values."""
        page_id = self._require_page()
        self.gate.emit(
            PageValidated(
                operation_id=self.gate.operation_id,
                sequence=self._take(),
                binding_id=0,
                page_id=page_id,
                row_count=row_count,
            )
        )
        if row_count == 0:
            self.delivered()
            self.acknowledged()

    def rejected(self, reason: str) -> None:
        """Retire one settled but unadmitted page."""
        page_id = self._require_page()
        self.gate.emit(
            PageRejected(
                operation_id=self.gate.operation_id,
                sequence=self._take(),
                binding_id=0,
                page_id=page_id,
                reason=reason,
            )
        )
        self._current = None

    def delivered(self) -> None:
        """Mark the complete validated page delivered to the consumer."""
        self.gate.emit(
            PageDelivered(
                operation_id=self.gate.operation_id,
                sequence=self._take(),
                binding_id=0,
                page_id=self._require_page(),
            )
        )

    def acknowledged(self) -> None:
        """Acknowledge after the caller accepts and optionally commits the page."""
        page_id = self._require_page()
        self.gate.emit(
            PageAcknowledged(
                operation_id=self.gate.operation_id,
                sequence=self._take(),
                binding_id=0,
                page_id=page_id,
            )
        )
        self._current = None

    def terminal(self, closure: BindingClosure, stream: StreamClosure, *, qualified_total: int | None = None) -> None:
        """Settle binding and producer before owned-resource cleanup."""
        self.gate.emit(
            BindingTerminal(
                operation_id=self.gate.operation_id,
                sequence=self._take(),
                binding_id=0,
                closure=closure,
                qualified_total=qualified_total,
            )
        )
        self.gate.emit(
            StreamTerminal(
                operation_id=self.gate.operation_id,
                sequence=self._take(),
                closure=stream,
            )
        )

    def terminal_from_kernel(
        self,
        state: KernelState,
        *,
        boundary: bool,
        caller_stopped: bool,
        terminal_reason: str | None = None,
        qualified_total: int | None = None,
    ) -> None:
        """Preserve the driver's qualified closure distinction in gate evidence."""
        # A kernel that did not complete never claims a success closure, whatever its plan shape.
        closure = (
            BindingClosure.UNKNOWN
            if self._unknown
            else BindingClosure.CALLER_STOP
            if caller_stopped
            else BindingClosure.FAILURE
            if state is not KernelState.COMPLETED
            else BindingClosure.BOUNDARY_SEEN
            if boundary
            else qualified_closure(terminal_reason) or BindingClosure.SOURCE_EMPTY
        )
        stream = (
            StreamClosure.NATURAL
            if state is KernelState.COMPLETED
            else StreamClosure.CANCELLED
            if state is KernelState.CANCELLED
            else StreamClosure.EARLY_CLOSE
        )
        self.terminal(closure, stream, qualified_total=qualified_total)

    def cleanup(self, state: CleanupState) -> None:
        """Finish only after cleanup was observed."""
        self.gate.emit(
            CleanupOutcome(
                operation_id=self.gate.operation_id,
                sequence=self._take(),
                state=state,
            )
        )

    def _require_page(self) -> int:
        page_id = self._current
        if page_id is None:
            raise RuntimeError("completion event lacks an active logical page")
        return page_id


class CountedCompletionRecorder(CompletionRecorder):
    """Correlate bounded in-flight counted batch pages by command index."""

    def __init__(self) -> None:
        """Initialize the base gate and bounded open-reservation ledger."""
        super().__init__()
        self._reserved: set[int] = set()

    def reserve(self) -> int:
        """Schedule a page before its physical command can be dispatched."""
        page_id = self._next_page
        self._next_page += 1
        self.gate.emit(
            PageScheduled(
                operation_id=self.gate.operation_id,
                sequence=self._take(),
                binding_id=0,
                page_id=page_id,
            )
        )
        self._reserved.add(page_id)
        return page_id

    def settle_unobserved(self) -> None:
        """Retire scheduled tail pages whose dispatch outcome was not observed."""
        for page_id in tuple(sorted(self._reserved - {self._current})):
            self.activate(page_id)
            self.settled(CommandSettlement.UNKNOWN)

    def activate(self, page_id: int) -> None:
        """Select one returned logical page for settlement and validation."""
        if self._current is not None or page_id not in self._reserved:
            raise RuntimeError("counted outcome lacks a scheduled page")
        self._current = page_id

    def settled(self, outcome: CommandSettlement) -> None:
        """Settle an active reservation and forget negative terminal pages."""
        page_id = self._require_page()
        super().settled(outcome)
        if outcome is not CommandSettlement.SUCCESS:
            self._reserved.discard(page_id)

    def rejected(self, reason: str) -> None:
        """Reject and forget an active reservation."""
        page_id = self._require_page()
        super().rejected(reason)
        self._reserved.discard(page_id)

    def acknowledged(self) -> None:
        """Acknowledge and forget an active reservation."""
        page_id = self._require_page()
        super().acknowledged()
        self._reserved.discard(page_id)
