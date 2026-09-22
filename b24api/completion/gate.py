"""Bounded state machine for correlated page and binding completion evidence."""

from __future__ import annotations
from dataclasses import dataclass
from enum import IntEnum

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
from b24api.contracts.report import TerminalState, Violation, ViolationSeverity

_MAX_ID_LENGTH = 128
_MAX_VIOLATIONS = 128


class _Stage(IntEnum):
    SCHEDULED = 1
    SETTLED = 2
    VALIDATED = 3
    DELIVERED = 4


@dataclass(slots=True)
class _Page:
    stage: _Stage = _Stage.SCHEDULED


@dataclass(slots=True)
class _Binding:
    last_page_id: int = -1
    open_pages: int = 0
    negative_pages: int = 0
    unknown_pages: int = 0


@dataclass(frozen=True, slots=True)
class CompletionDecision:
    """Terminal verdict derived solely from accepted completion evidence."""

    state: TerminalState
    exhausted: bool
    violations: tuple[Violation, ...]
    bindings_admitted: int
    bindings_terminal: int
    pages_scheduled: int
    pages_acknowledged: int


class CompletionGate:
    """Accept ordered evidence with O(active bindings + in-flight pages) memory."""

    def __init__(self, operation_id: str) -> None:
        """Initialize an empty operation before dispatch."""
        if not isinstance(operation_id, str) or not 0 < len(operation_id) <= _MAX_ID_LENGTH:
            raise ValueError("operation_id must be a bounded non-empty string")
        self.operation_id = operation_id
        self._sequence = -1
        self._last_binding_id = -1
        self._bindings: dict[int, _Binding] = {}
        self._pages: dict[tuple[int, int], _Page] = {}
        self._admitted = 0
        self._terminal = 0
        self._scheduled = 0
        self._acknowledged = 0
        self._negative_pages = 0
        self._negative_bindings = 0
        self._unknown = 0
        self._caller_stops = 0
        self._bounded = 0
        self._stream: StreamClosure | None = None
        self._cleanup: CleanupState | None = None
        self._violations: list[Violation] = []

    def _violate(self, code: str) -> None:
        if len(self._violations) < _MAX_VIOLATIONS:
            self._violations.append(Violation(ViolationSeverity.BLOCKING, code, "completion event protocol violated"))

    @staticmethod
    def _id(value: object) -> bool:
        return type(value) is int and value >= 0

    def _page(
        self, event: PageCommandOutcome | PageValidated | PageDelivered | PageAcknowledged | PageRejected,
    ) -> _Page | None:
        if not self._id(event.binding_id) or not self._id(event.page_id):
            self._violate("completion_invalid_page_id")
            return None
        page = self._pages.get((event.binding_id, event.page_id))
        if page is None:
            self._violate("completion_unknown_page")
        return page

    def emit(self, event: CompletionEvidence) -> None:  # noqa: C901, PLR0911, PLR0912, PLR0915
        """Apply one immutable event or retain a bounded blocking violation."""
        if event.operation_id != self.operation_id or event.sequence <= self._sequence:
            self._violate("completion_event_order")
            return
        self._sequence = event.sequence
        if self._cleanup is not None or (self._stream is not None and not isinstance(event, CleanupOutcome)):
            self._violate("completion_after_terminal")
            return
        if isinstance(event, BindingAdmitted):
            if not self._id(event.binding_id) or event.binding_id <= self._last_binding_id:
                self._violate("completion_duplicate_binding")
                return
            self._last_binding_id = event.binding_id
            self._bindings[event.binding_id] = _Binding()
            self._admitted += 1
        elif isinstance(event, PageScheduled):
            binding = self._bindings.get(event.binding_id)
            if binding is None or not self._id(event.page_id) or event.page_id <= binding.last_page_id:
                self._violate("completion_duplicate_or_unbound_page")
                return
            binding.last_page_id = event.page_id
            binding.open_pages += 1
            self._pages[event.binding_id, event.page_id] = _Page()
            self._scheduled += 1
        elif isinstance(event, PageCommandOutcome):
            page = self._page(event)
            if page is None:
                return
            if page.stage is not _Stage.SCHEDULED or not isinstance(event.outcome, CommandSettlement):
                self._violate("completion_invalid_command_settlement")
                return
            if event.outcome is CommandSettlement.SUCCESS:
                page.stage = _Stage.SETTLED
            else:
                self._negative_pages += 1
                self._bindings[event.binding_id].negative_pages += 1
                if event.outcome is CommandSettlement.UNKNOWN:
                    self._unknown += 1
                    self._bindings[event.binding_id].unknown_pages += 1
                self._retire_page(event.binding_id, event.page_id)
        elif isinstance(event, PageValidated):
            page = self._page(event)
            if page is None:
                return
            if (
                page.stage is not _Stage.SETTLED
                or type(event.row_count) is not int
                or event.row_count < 0
                or not event.identity_digest
            ):
                self._violate("completion_invalid_validation")
                return
            page.stage = _Stage.VALIDATED
        elif isinstance(event, PageDelivered):
            page = self._page(event)
            if page is None:
                return
            if page.stage is not _Stage.VALIDATED:
                self._violate("completion_delivery_before_validation")
                return
            page.stage = _Stage.DELIVERED
        elif isinstance(event, PageAcknowledged):
            page = self._page(event)
            if page is None:
                return
            if page.stage is not _Stage.DELIVERED:
                self._violate("completion_ack_before_delivery")
                return
            self._acknowledged += 1
            self._retire_page(event.binding_id, event.page_id)
        elif isinstance(event, PageRejected):
            page = self._page(event)
            if page is None:
                return
            if page.stage is _Stage.DELIVERED:
                self._violate("completion_rejection_after_delivery")
                return
            self._negative_pages += 1
            self._bindings[event.binding_id].negative_pages += 1
            self._retire_page(event.binding_id, event.page_id)
        elif isinstance(event, BindingTerminal):
            binding = self._bindings.get(event.binding_id)
            if binding is None or binding.open_pages or not isinstance(event.closure, BindingClosure):
                self._violate("completion_invalid_binding_terminal")
                return
            if binding.negative_pages and event.closure not in {BindingClosure.FAILURE, BindingClosure.UNKNOWN}:
                self._violate("completion_negative_binding_claimed_success")
                return
            if binding.unknown_pages and event.closure is not BindingClosure.UNKNOWN:
                self._violate("completion_unknown_binding_claimed_known")
                return
            if not binding.negative_pages and event.closure in {BindingClosure.FAILURE, BindingClosure.UNKNOWN}:
                self._violate("completion_negative_terminal_lacks_evidence")
                return
            if binding.last_page_id < 0 and event.closure != BindingClosure.CALLER_STOP:
                self._violate("completion_terminal_lacks_page_witness")
                return
            if event.closure in {BindingClosure.FAILURE, BindingClosure.UNKNOWN}:
                self._negative_bindings += 1
                if event.closure is BindingClosure.UNKNOWN:
                    self._unknown += 1
            if event.closure is BindingClosure.CALLER_STOP:
                self._caller_stops += 1
            if event.closure is BindingClosure.BOUNDARY_SEEN:
                self._bounded += 1
            self._terminal += 1
            del self._bindings[event.binding_id]
        elif isinstance(event, StreamTerminal):
            if not isinstance(event.closure, StreamClosure):
                self._violate("completion_invalid_stream_terminal")
                return
            self._stream = event.closure
        elif isinstance(event, CleanupOutcome):
            if self._stream is None or not isinstance(event.state, CleanupState):
                self._violate("completion_invalid_cleanup")
                return
            self._cleanup = event.state
        else:
            self._violate("completion_unknown_event")

    def _retire_page(self, binding_id: int, page_id: int) -> None:
        del self._pages[binding_id, page_id]
        self._bindings[binding_id].open_pages -= 1

    def finish(self) -> CompletionDecision:
        """Decide only after terminal and cleanup evidence is available."""
        if self._stream is None or self._cleanup is None:
            raise RuntimeError("completion gate requires stream terminal and cleanup outcome")
        incomplete = bool(
            self._violations
            or self._bindings
            or self._pages
            or self._scheduled != self._acknowledged + self._negative_pages
            or self._cleanup is CleanupState.FAILURE
            or self._unknown,
        )
        if self._stream is StreamClosure.CANCELLED:
            state = TerminalState.CANCELLED
        elif self._stream is StreamClosure.EARLY_CLOSE:
            state = TerminalState.EARLY_CLOSED
        elif incomplete:
            state = TerminalState.INCOMPLETE
        elif self._negative_pages or self._negative_bindings:
            state = TerminalState.COMPLETED_WITH_FAILURES
        else:
            state = TerminalState.COMPLETED
        return CompletionDecision(
            state,
            state is TerminalState.COMPLETED and not self._caller_stops and not self._bounded,
            tuple(self._violations),
            self._admitted,
            self._terminal,
            self._scheduled,
            self._acknowledged,
        )
