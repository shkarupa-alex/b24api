"""Correlate fast keyset wave commands with the sole completion gate."""

from __future__ import annotations
from dataclasses import dataclass
from uuid import uuid4

from b24api.completion.gate import CompletionGate
from b24api.completion.page_failure import unconsumed_rejection
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


@dataclass(slots=True)
class _CommandPage:
    page_id: int
    settled: bool = False
    recorded: bool = False
    validated: bool = False
    remaining: int | None = None


class FastPageLedger:
    """Correlate fast keyset commands with completion evidence, independent of where it is emitted.

    A flat traversal emits into its own operation gate as binding 0; a reference binding emits into
    the shared operation gate under its own binding id. The page protocol is identical.
    """

    def __init__(self) -> None:
        """Start with no scheduled command."""
        self._active: dict[str, _CommandPage] = {}
        self._negative = False
        self._unknown = False

    @property
    def negative(self) -> bool:
        """Return whether any page of this binding settled or was retired negatively."""
        return self._negative

    @property
    def unknown(self) -> bool:
        """Return whether any page of this binding settled with an unknown outcome."""
        return self._unknown

    def _new_page(self) -> int:
        """Return the next page id of this binding."""
        raise NotImplementedError

    def _emit_page(self, event_type: type, page_id: int, **fields: object) -> None:
        """Emit one page event of this binding."""
        raise NotImplementedError

    def schedule(self, command_id: str) -> None:
        """Register a logical page before its containing physical batch dispatch."""
        if command_id in self._active:
            raise RuntimeError("fast command was scheduled twice")
        page_id = self._new_page()
        self._active[command_id] = _CommandPage(page_id)
        self._emit_page(PageScheduled, page_id)

    def settle(self, command_id: str, outcome: CommandSettlement) -> None:
        """Record one correlated physical command settlement."""
        page = self._active[command_id]
        if page.settled:
            raise RuntimeError("fast command settled twice")
        page.settled = True
        self._emit_page(PageCommandOutcome, page.page_id, outcome=outcome)
        if outcome is not CommandSettlement.SUCCESS:
            self._negative = True
            self._unknown = self._unknown or outcome is CommandSettlement.UNKNOWN
            del self._active[command_id]

    def validated(self, command_id: str) -> None:
        """Mark a settled page validated until ordered admission is decided."""
        page = self._active[command_id]
        if not page.settled or page.validated:
            raise RuntimeError("fast page validation lacks a unique successful settlement")
        page.validated = True

    def recorded(self, command_id: str, rejected: str | None) -> None:
        """Retire a semantically accepted page, or one ``rejected`` with that reason (its trace's code)."""
        page = self._active.get(command_id)
        if page is None:
            return  # a negative physical settlement already retired it
        if not page.settled:
            raise RuntimeError("fast observation preceded physical settlement")
        if rejected is None:
            if not page.validated:
                raise RuntimeError("fast accepted page was not validated")
            page.recorded = True
        else:
            self._emit_page(PageRejected, page.page_id, reason=rejected)
            self._negative = True
            del self._active[command_id]

    def admit(self, command_id: str, count: int) -> None:
        """Validate exactly the rows accepted into ordered output."""
        page = self._active.get(command_id)
        if page is None and count == 0:
            return  # an empty selected page was already retired
        if page is None or not page.recorded or page.remaining is not None or count < 0:
            raise RuntimeError("fast page admission lacks a committed physical page")
        page.remaining = count
        self._emit_page(PageValidated, page.page_id, row_count=count)
        if count == 0:
            self._acknowledge(command_id)

    def emitted(self, command_id: str) -> None:
        """Acknowledge a page only after its last admitted row reaches the caller."""
        page = self._active[command_id]
        if page.remaining is None or page.remaining < 1:
            raise RuntimeError("fast emitted row has no outstanding admitted page")
        page.remaining -= 1
        if page.remaining == 0:
            self._acknowledge(command_id)

    def _acknowledge(self, command_id: str) -> None:
        page = self._active[command_id]
        self._emit_page(PageDelivered, page.page_id)
        self._emit_page(PageAcknowledged, page.page_id)
        del self._active[command_id]

    def discard_unadmitted(self) -> None:
        """Retire accepted probes that were never selected for output."""
        for command_id, page in tuple(self._active.items()):
            if page.recorded and page.remaining is None:
                self.admit(command_id, 0)

    def close(self, *, successful: bool) -> None:
        """Retire every open page before the binding terminal: discarded probes, or a failed traversal.

        Only a settled page can be rejected. A wave engine settles each page it was handed from what its command
        did, also when its call raises, so a page still unsettled here was never handed to one: it settles as not
        executed. Every page still open then succeeded and was read but never consumed: it is rejected as aborted
        with the traversal (``unconsumed_rejection``), as the counted and reference ledgers reject one. This is the
        one rule for every path, flat or reference, whatever ended the traversal.
        """
        if successful:
            self.discard_unadmitted()
            if not self._active:
                return
        for command_id, page in tuple(self._active.items()):
            if not page.settled:
                self.settle(command_id, CommandSettlement.NOT_EXECUTED)
        for command_id, page in tuple(self._active.items()):
            self._emit_page(PageRejected, page.page_id, reason=unconsumed_rejection(CommandSettlement.SUCCESS))
            self._negative = True
            del self._active[command_id]


class FastCompletionRecorder(FastPageLedger):
    """Track bounded in-flight physical commands of one flat traversal through scheduler acceptance."""

    def __init__(self) -> None:
        """Admit the sole keyset binding before any physical scheduling."""
        super().__init__()
        self.gate = CompletionGate(uuid4().hex)
        self._sequence = 0
        self._page_id = 0
        self._terminal = False
        self._emit(BindingAdmitted, binding_id=0)

    def _emit(self, event_type: type, **fields: object) -> None:
        self.gate.emit(
            event_type(
                operation_id=self.gate.operation_id,
                sequence=self._sequence,
                **fields,
            )
        )
        self._sequence += 1

    def _new_page(self) -> int:
        page_id = self._page_id
        self._page_id += 1
        return page_id

    def _emit_page(self, event_type: type, page_id: int, **fields: object) -> None:
        self._emit(event_type, binding_id=0, page_id=page_id, **fields)

    def terminal(  # noqa: PLR0913
        self,
        state: KernelState,
        *,
        rows_emitted: int,
        rows_admitted: int,
        cleanup: CleanupState,
        closure: BindingClosure | None = None,
        qualified_witnesses: int = 0,
    ) -> None:
        """Settle the operation only after owned scheduler cleanup has finished."""
        if self._terminal:
            return
        self._terminal = True
        successful = state is KernelState.COMPLETED and rows_emitted == rows_admitted
        self.close(successful=successful)
        if successful and closure not in {BindingClosure.SOURCE_EMPTY, BindingClosure.KEYSET_PLAN_COVERED}:
            closure = BindingClosure.FAILURE
            self._negative = True
        elif not successful:
            closure = (
                BindingClosure.UNKNOWN
                if self._unknown
                else BindingClosure.CALLER_STOP
                if state is KernelState.CANCELLED and not self._negative
                else BindingClosure.FAILURE
            )
        stream = (
            StreamClosure.NATURAL
            if state is KernelState.COMPLETED
            else StreamClosure.CANCELLED
            if state is KernelState.CANCELLED
            else StreamClosure.EARLY_CLOSE
        )
        self._emit(
            BindingTerminal,
            binding_id=0,
            closure=closure,
            qualified_witnesses=qualified_witnesses if closure is BindingClosure.KEYSET_PLAN_COVERED else None,
        )
        self._emit(StreamTerminal, closure=stream)
        self._emit(CleanupOutcome, state=cleanup)


__all__ = ["FastCompletionRecorder", "FastPageLedger"]
