"""Bounded state machine for correlated page and binding completion evidence."""

from __future__ import annotations
from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING

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
from b24api.contracts.policy import KernelState
from b24api.contracts.report import (
    OperationReport,
    TerminalState,
    TraversalAssurance,
    Violation,
    ViolationSeverity,
)
from b24api.contracts.violation import retain_violations

if TYPE_CHECKING:
    from b24api.execution.snapshot import KernelReport

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
    row_count: int | None = None


@dataclass(slots=True)
class _Binding:
    last_page_id: int = -1
    open_pages: int = 0
    negative_pages: int = 0
    unknown_pages: int = 0
    acknowledged_pages: int = 0
    acknowledged_rows: int = 0
    last_acknowledged_rows: int | None = None


@dataclass(frozen=True, slots=True)
class CompletionDecision:
    """Terminal verdict derived solely from accepted completion evidence."""

    state: TerminalState
    exhausted: bool
    caller_stopped: bool
    violations: tuple[Violation, ...]
    bindings_admitted: int
    bindings_terminal: int
    pages_scheduled: int
    pages_acknowledged: int


@dataclass(frozen=True, slots=True)
class CompletionReportFacts:
    """Post-cleanup source snapshot and bounded public delivery counters."""

    source: KernelReport
    operation: str
    assurance: TraversalAssurance | None
    admitted: int
    emitted: int
    successes: int
    failures: int
    not_executed: int
    unknown: int
    buffered_commands_high_water: int
    active_references_high_water: int
    early_closed: bool = False
    forced_state: TerminalState | None = None
    extra_violations: tuple[Violation, ...] = ()


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
        self._source_empty = 0
        self._stream: StreamClosure | None = None
        self._empty_source = False
        self._cleanup: CleanupState | None = None
        self._violations: list[Violation] = []
        self._report_facts: CompletionReportFacts | None = None

    def _violate(self, code: str) -> None:
        if len(self._violations) < _MAX_VIOLATIONS:
            self._violations.append(Violation(ViolationSeverity.BLOCKING, code, "completion event protocol violated"))

    @staticmethod
    def _id(value: object) -> bool:
        return type(value) is int and value >= 0

    def _page(
        self,
        event: PageCommandOutcome | PageValidated | PageDelivered | PageAcknowledged | PageRejected,
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
            page.row_count = event.row_count
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
            binding = self._bindings[event.binding_id]
            if page.row_count is None:
                raise RuntimeError("acknowledged page lost validated row count")
            binding.acknowledged_pages += 1
            binding.acknowledged_rows += page.row_count
            binding.last_acknowledged_rows = page.row_count
            self._retire_page(event.binding_id, event.page_id)
        elif isinstance(event, PageRejected):
            page = self._page(event)
            if page is None:
                return
            if page.stage is _Stage.DELIVERED:
                self._violate("completion_rejection_after_delivery")
                return
            if page.stage is _Stage.SCHEDULED:
                # A rejection is the negative result of a settled page; without the settlement the
                # physical outcome is unknown and the page stays open for incomplete accounting.
                self._violate("completion_rejection_before_settlement")
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
            if binding.last_page_id < 0 and event.closure not in {
                BindingClosure.CALLER_STOP,
                BindingClosure.FAILURE,
                BindingClosure.UNKNOWN,
            }:
                self._violate("completion_terminal_lacks_page_witness")
                return
            if event.closure is BindingClosure.SOURCE_EMPTY and binding.last_acknowledged_rows != 0:
                self._violate("completion_missing_empty_witness")
                return
            if event.closure is BindingClosure.QUALIFIED_TOTAL and (
                type(event.qualified_total) is not int
                or event.qualified_total < 0
                or binding.acknowledged_pages == 0
                or binding.acknowledged_rows != event.qualified_total
            ):
                self._violate("completion_invalid_total_witness")
                return
            if event.closure is BindingClosure.SINGLE_RESPONSE and binding.acknowledged_pages != 1:
                self._violate("completion_invalid_single_response_witness")
                return
            if event.closure in {BindingClosure.RAW_RANGE_COVERED, BindingClosure.BOUNDARY_SEEN} and (
                binding.acknowledged_pages == 0
            ):
                self._violate("completion_missing_range_witness")
                return
            if event.closure is BindingClosure.KEYSET_PLAN_COVERED and (
                type(event.qualified_witnesses) is not int
                or event.qualified_witnesses < 1
                or binding.acknowledged_pages == 0
            ):
                self._violate("completion_missing_keyset_plan_witness")
                return
            if event.closure in {BindingClosure.FAILURE, BindingClosure.UNKNOWN}:
                self._negative_bindings += 1
                if event.closure is BindingClosure.UNKNOWN:
                    self._unknown += 1
            if event.closure is BindingClosure.CALLER_STOP:
                self._caller_stops += 1
            if event.closure is BindingClosure.BOUNDARY_SEEN:
                self._bounded += 1
            if event.closure is BindingClosure.SOURCE_EMPTY:
                self._source_empty += 1
            self._terminal += 1
            del self._bindings[event.binding_id]
        elif isinstance(event, StreamTerminal):
            if not isinstance(event.closure, StreamClosure):
                self._violate("completion_invalid_stream_terminal")
                return
            if type(event.empty_source) is not bool or (
                event.empty_source and (event.closure is not StreamClosure.NATURAL or self._admitted != 0)
            ):
                self._violate("completion_invalid_empty_source_witness")
                return
            self._stream = event.closure
            self._empty_source = event.empty_source
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

    def decision(self) -> CompletionDecision:
        """Decide only after terminal and cleanup evidence is available."""
        if self._stream is None or self._cleanup is None:
            raise RuntimeError("completion gate requires stream terminal and cleanup outcome")
        incomplete = bool(
            self._violations
            or self._bindings
            or self._pages
            or self._scheduled != self._acknowledged + self._negative_pages
            or (self._stream is StreamClosure.NATURAL and self._admitted == 0 and not self._empty_source)
            or self._cleanup is CleanupState.FAILURE,
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
            bool(self._caller_stops),
            tuple(self._violations),
            self._admitted,
            self._terminal,
            self._scheduled,
            self._acknowledged,
        )

    def attach_report(self, facts: CompletionReportFacts) -> None:
        """Bind exactly one post-cleanup source snapshot for final report creation."""
        if not isinstance(facts, CompletionReportFacts) or self._report_facts is not None:
            raise RuntimeError("completion report facts must be attached once")
        self._report_facts = facts

    def abort_unstarted(self) -> None:
        """Close a lazy operation that was never pulled, without claiming exhaustion."""
        if self._stream is not None or self._pages:
            raise RuntimeError("only an unstarted gate can be aborted")
        sequence = self._sequence + 1
        for binding_id in tuple(self._bindings):
            self.emit(
                BindingTerminal(
                    operation_id=self.operation_id,
                    sequence=sequence,
                    binding_id=binding_id,
                    closure=BindingClosure.CALLER_STOP,
                )
            )
            sequence += 1
        self.emit(
            StreamTerminal(
                operation_id=self.operation_id,
                sequence=sequence,
                closure=StreamClosure.CANCELLED,
            )
        )
        self.emit(
            CleanupOutcome(
                operation_id=self.operation_id,
                sequence=sequence + 1,
                state=CleanupState.SUCCESS,
            )
        )

    def _witness_mismatch(self, source: KernelReport) -> tuple[Violation, ...]:
        """Refuse a report empty-source witness that no source-empty binding closure backs."""
        if source.empty_source_witness is None or self._source_empty:
            return ()
        return (
            Violation(
                ViolationSeverity.BLOCKING,
                "completion_empty_witness_mismatch",
                "report empty-source witness lacks a source-empty binding closure",
            ),
        )

    def finish(self) -> OperationReport:
        """Build the sole strong frozen report after correlated cleanup evidence."""
        decision = self.decision()
        facts = self._report_facts
        if facts is None:
            raise RuntimeError("completion report facts were not attached")
        source = facts.source
        negative_outcomes = facts.failures + facts.not_executed + facts.unknown
        if facts.early_closed:
            state = TerminalState.EARLY_CLOSED
        elif source.state is KernelState.COMPLETED:
            state = decision.state
        elif source.state is KernelState.INCOMPLETE:
            state = TerminalState.INCOMPLETE
        elif source.state is KernelState.CANCELLED:
            state = TerminalState.CANCELLED
        else:
            state = TerminalState.FAILED
        violations = retain_violations(
            (*source.violations, *decision.violations, *facts.extra_violations, *self._witness_mismatch(source))
        )
        gate_negative = decision.state is TerminalState.COMPLETED_WITH_FAILURES
        if source.state is KernelState.COMPLETED and gate_negative != bool(negative_outcomes):
            violations = retain_violations(
                (
                    *violations,
                    Violation(
                        ViolationSeverity.BLOCKING,
                        "completion_outcome_count_mismatch",
                        "public negative outcome counts disagree with completion evidence",
                    ),
                )
            )
            state = TerminalState.INCOMPLETE
        if state is TerminalState.COMPLETED and (
            decision.state is not TerminalState.COMPLETED
            or any(item.severity is ViolationSeverity.BLOCKING for item in violations)
        ):
            state = TerminalState.INCOMPLETE
        if facts.forced_state in {TerminalState.COMPLETED, TerminalState.COMPLETED_WITH_FAILURES}:
            raise ValueError("forced terminal state cannot claim successful completion")
        if facts.forced_state is not None:
            state = facts.forced_state
        return OperationReport(
            state=state,
            operation=facts.operation,
            terminal_reason=source.terminal_reason or state.value,
            exhausted=state is TerminalState.COMPLETED and decision.exhausted,
            assurance=_observed_assurance(facts.assurance, source, caller_stopped=decision.caller_stopped, state=state),
            admitted=facts.admitted,
            emitted=facts.emitted,
            successes=facts.successes,
            failures=facts.failures,
            not_executed=facts.not_executed,
            unknown=facts.unknown,
            unique_rows=source.unique_rows,
            physical_requests=source.physical_requests,
            logical_pages=source.logical_pages,
            batch_requests=source.batch_requests,
            batch_commands=source.batch_commands,
            retries=source.retries,
            cooldown_seconds=source.cooldown_seconds,
            buffered_commands_high_water=facts.buffered_commands_high_water,
            buffered_rows_high_water=source.buffered_rows_high_water,
            active_references_high_water=facts.active_references_high_water,
            violations=violations,
            page_trace=source.page_trace,
            page_trace_truncated=source.page_trace_truncated,
            keyset_execution=source.keyset_execution,
        )


_IDENTITY_STRENGTH = frozenset({TraversalAssurance.IDENTITY_EXACT, TraversalAssurance.IDENTITY_AND_COUNT_MATCHED})


def _observed_assurance(
    declared: TraversalAssurance | None,
    source: KernelReport,
    *,
    caller_stopped: bool,
    state: TerminalState,
) -> TraversalAssurance | None:
    """Finalize assurance with what the traversal observed, not only what its plan declared.

    A report that did not complete proves at most mechanics, even when a caller stopped one of its
    bindings. A completed caller stop proves only a bounded prefix. An identity repeated across offset
    pages is the signature of a shifting source that may also have skipped rows, so identity-strength
    claims fall to mechanics even when the stream reached a natural end.
    """
    if state not in {TerminalState.COMPLETED, TerminalState.COMPLETED_WITH_FAILURES}:
        return TraversalAssurance.MECHANICS_ONLY if declared is not None or caller_stopped else None
    if caller_stopped:
        return TraversalAssurance.BOUNDED_PREFIX
    if source.empty_source_witness is not None and declared is not None:
        # No nonnegative total was observed, so a count-matched claim keeps only its identity strength.
        declared = _without_count(declared)
    if source.duplicate_identities and declared in _IDENTITY_STRENGTH:
        return TraversalAssurance.MECHANICS_ONLY
    return declared


def _without_count(declared: TraversalAssurance) -> TraversalAssurance:
    """Drop the count half of a count-matched claim and keep its identity strength."""
    if declared is TraversalAssurance.IDENTITY_AND_COUNT_MATCHED:
        return TraversalAssurance.IDENTITY_EXACT
    if declared is TraversalAssurance.COUNT_MATCHED:
        return TraversalAssurance.MECHANICS_ONLY
    return declared
