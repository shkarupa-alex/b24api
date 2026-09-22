"""Characterization of the v2 public mechanics contracts."""

from __future__ import annotations
import inspect
from dataclasses import FrozenInstanceError, fields
from typing import is_typeddict

import pytest

import b24api
import b24api.errors
from b24api.contracts import (
    BatchDispatch,
    Binding,
    Command,
    CommandFailure,
    CommandNotExecuted,
    CommandOutcomeUnknown,
    CommandSuccess,
    CountedTraversal,
    CursorSpec,
    DeliveryOrder,
    DirectDispatch,
    IdentitySpec,
    NotExecutedReason,
    OperationReport,
    ParameterPath,
    ParameterUpdate,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ReferenceNotExecuted,
    ReferenceOutcomeUnknown,
    ReplaySafety,
    Request,
    RequestSpec,
    Response,
    RouteKind,
    TerminalState,
    TraversalAssurance,
    partition_command_outcomes,
    partition_reference_outcomes,
)
from b24api.contracts.policy import IdentityCoercion
from b24api.errors import ProtocolError

CORRELATION_VALUE = 7
DIRECT_CONCURRENCY = 3
BATCH_SIZE = 7
SUMMARY_LIMIT = 256


def test_public_error_module_export_snapshot_is_static_contract_evidence() -> None:
    assert b24api.errors.__all__ == (
        "AmbiguousExecutionError",
        "ApiResponseError",
        "B24ApiError",
        "BatchCommandError",
        "BatchFailed",
        "BudgetExceededError",
        "CapabilityError",
        "EnvelopeContractError",
        "ErrorOrigin",
        "FailurePhase",
        "HTTPGatewayError",
        "IdentityContractError",
        "IncompleteTraversalError",
        "InputSourceError",
        "PaginationError",
        "ProtocolError",
        "ReferenceFailed",
        "ResponseTooLargeError",
        "KeysetCapabilityError",
        "PageAdaptationError",
        "PageAdaptationViolation",
        "ResultShapeError",
        "TransportError",
        "ValidationIssue",
    )


def test_keyset_execution_report_declaration_snapshot() -> None:
    expected = (
        ("requested_kind", "KeysetExecutionKind"),
        ("selected_kind", "KeysetExecutionKind"),
        ("preselection_reason", "KeysetSelectionReason"),
        ("final_selection_reason", "KeysetSelectionReason | None"),
        ("assurance_source", "KeysetAssuranceSource"),
        ("planning_requests", "int"),
        ("boundary_requests", "int"),
        ("canary_requests", "int"),
        ("anchor_probe_requests", "int"),
        ("canary_commands", "int"),
        ("canary_rows", "int"),
        ("anchor_probe_commands", "int"),
        ("anchor_count", "int"),
        ("empty_anchor_probes", "int"),
        ("probe_rows_discarded", "int"),
        ("boundary_overlap_rows", "int"),
        ("head_page_admitted", "bool"),
        ("sequential_requests_estimate", "int | None"),
        ("selected_requests_estimate", "int | None"),
        ("head_rows", "int"),
        ("tail_rows", "int"),
        ("interior_span", "int | None"),
        ("interior_rows_estimate", "int | None"),
        ("total_rows_estimate", "int | None"),
        ("density_numerator", "int | None"),
        ("density_denominator", "int | None"),
        ("effective_window_width", "int | None"),
        ("range_window_count", "int | None"),
        ("target_lanes", "int | None"),
        ("actual_lanes", "int | None"),
        ("continuation_count", "int"),
        ("closure_witness_counts", "tuple[tuple[ClosureWitness, int], ...]"),
        ("effective_batch_capacity", "int"),
        ("total_hint_requested", "bool"),
        ("total_hint_observed", "int | None"),
        ("total_hint_plausible", "bool"),
        ("total_hint_used", "bool"),
        ("trace_retained_by_class", "tuple[tuple[TraceClass, int], ...]"),
        ("trace_dropped_by_class", "tuple[tuple[TraceClass, int], ...]"),
    )

    assert (
        tuple(
            (field.name, b24api.KeysetExecutionReport.__annotations__[field.name])
            for field in fields(b24api.KeysetExecutionReport)
        )
        == expected
    )


def test_v2_root_export_snapshot_contains_no_engine_or_legacy_symbols() -> None:
    assert b24api.__all__ == [
        "AdaptedPage",
        "AmbiguityPolicy",
        "AmbiguityReason",
        "AmbiguousExecutionError",
        "ApiResponseError",
        "AutoKeysetExecution",
        "B24ApiError",
        "BatchCommandError",
        "BatchDispatch",
        "BatchFailed",
        "BinaryEvidence",
        "BinaryResponse",
        "Binding",
        "BindingAdmitted",
        "BindingClosure",
        "BindingTerminal",
        "Bitrix24",
        "BodyEncoding",
        "BoundedIdentityRange",
        "BudgetExceededError",
        "CapabilityError",
        "CleanupOutcome",
        "CleanupState",
        "ClosureWitness",
        "Command",
        "CommandFailure",
        "CommandNotExecuted",
        "CommandOutcome",
        "CommandOutcomeUnknown",
        "CommandSettlement",
        "CommandSuccess",
        "CompletionEvent",
        "CompositeIdentitySpec",
        "ConsistencyPolicy",
        "CountedTraversal",
        "CursorSpec",
        "CursorTraversal",
        "DeliveryOrder",
        "DirectDispatch",
        "EmptyArray",
        "EmptyObject",
        "EnvelopeContractError",
        "ExecutionPolicy",
        "FrozenJson",
        "FrozenMapping",
        "HTTPGatewayError",
        "IdentityCoercion",
        "IdentityComponent",
        "IdentityContractError",
        "IdentityPageAdapter",
        "IdentitySpec",
        "IncompleteTraversalError",
        "InputSourceError",
        "KeysetAssuranceSource",
        "KeysetCapabilityCheckName",
        "KeysetCapabilityCheckOutcome",
        "KeysetCapabilityCheckResult",
        "KeysetCapabilityError",
        "KeysetCapabilityReport",
        "KeysetCapabilityVerdict",
        "KeysetExecution",
        "KeysetExecutionKind",
        "KeysetExecutionReport",
        "KeysetInconclusiveReason",
        "KeysetPageCompletion",
        "KeysetPhase",
        "KeysetSelectionReason",
        "KeysetSpec",
        "KeysetTraversal",
        "MembershipRecheck",
        "NotExecutedReason",
        "Null",
        "OffsetContinuation",
        "OffsetSpec",
        "Omitted",
        "OperationReport",
        "OperationStream",
        "PageAcknowledged",
        "PageAdaptationError",
        "PageAdaptationViolation",
        "PageAdapter",
        "PageCommandOutcome",
        "PageDelivered",
        "PageDispatch",
        "PageIndex",
        "PageOutcome",
        "PageRecord",
        "PageRejected",
        "PageRejectionCode",
        "PageScheduled",
        "PageStride",
        "PageValidated",
        "PageView",
        "PaginationError",
        "ParameterPath",
        "ParameterUpdate",
        "PartialResult",
        "PartitionedKeysetExecution",
        "PositionalArguments",
        "PositionalLayout",
        "Present",
        "ProtocolError",
        "RangeKeysetExecution",
        "ReferenceComplete",
        "ReferenceEvent",
        "ReferenceFailed",
        "ReferenceFailure",
        "ReferenceItem",
        "ReferenceNotExecuted",
        "ReferenceOutcome",
        "ReferenceOutcomeUnknown",
        "ReplaySafety",
        "Request",
        "RequestHeaders",
        "RequestSummary",
        "Response",
        "ResponseTooLargeError",
        "ResultCollectionShape",
        "ResultErrorShape",
        "ResultErrorSpec",
        "ResultSelector",
        "ResultShapeError",
        "RetryPolicy",
        "RouteKind",
        "SequentialKeysetExecution",
        "SequentialTraversal",
        "Settings",
        "SlotContract",
        "SlotShape",
        "SparseRawBound",
        "SplitOrderSpec",
        "StableIntegerKeysetContract",
        "StreamClosure",
        "StreamTerminal",
        "TerminalState",
        "TotalHintMode",
        "TotalTermination",
        "TraceClass",
        "Transport",
        "TransportCapabilities",
        "TransportError",
        "TraversalAssurance",
        "TraversalIdentity",
        "UnknownRequestAudit",
        "UnknownRequestCollector",
        "ValidationIssue",
        "Violation",
        "ViolationSeverity",
        "WireRequest",
        "WireResponse",
        "WireTransport",
        "partition_command_outcomes",
        "partition_reference_outcomes",
        "traversal_control_paths",
    ]


def test_keyset_cursor_streams_add_only_the_declared_fields_and_methods() -> None:
    assert tuple(field.name for field in fields(Binding)) == ("summary", "updates", "correlation", "start_cursor")
    assert tuple(field.name for field in fields(BatchDispatch)) == (
        "batch_size",
        "concurrency",
        "output_order",
        "coalesce_wait",
    )
    for traversal in (b24api.SequentialTraversal, b24api.CountedTraversal):
        assert tuple(field.name for field in fields(traversal))[-1] == "page_adapter"
    for traversal in (b24api.KeysetTraversal, b24api.CursorTraversal):
        assert tuple(field.name for field in fields(traversal))[-1] == "page_adapter"
    assert tuple(field.name for field in fields(b24api.MembershipRecheck)) == (
        "identities",
        "still_observed",
        "no_longer_observed",
        "contradictory",
        "truncated",
    )
    assert tuple(field.name for field in fields(b24api.KeysetCapabilityCheckResult)) == (
        "name",
        "outcome",
        "rows_selected",
        "out_of_interval_identities",
        "missing_in_interval_identities",
        "extra_in_interval_identities",
        "contradictory_identities",
        "recheck",
    )
    assert b24api.MembershipRecheck.__annotations__ == {
        "identities": "tuple[FrozenJson, ...]",
        "still_observed": "tuple[FrozenJson, ...]",
        "no_longer_observed": "tuple[FrozenJson, ...]",
        "contradictory": "tuple[FrozenJson, ...]",
        "truncated": "bool",
    }
    for name in (
        "out_of_interval_identities",
        "missing_in_interval_identities",
        "extra_in_interval_identities",
        "contradictory_identities",
    ):
        assert b24api.KeysetCapabilityCheckResult.__annotations__[name] == "tuple[FrozenJson, ...]"
    assert b24api.PageRejectionCode.PAGE_ADAPTATION.value == "page_adaptation"
    assert hasattr(b24api.Bitrix24, "iter_cursors")
    assert hasattr(b24api.Bitrix24, "verify_keyset_capability")
    assert not hasattr(b24api.Bitrix24, "iter_cursor_outcomes")


def test_request_mapping_contract_is_a_closed_typed_dict() -> None:
    assert is_typeddict(RequestSpec)
    assert RequestSpec.__required_keys__ == frozenset({"method", "route"})
    assert RequestSpec.__optional_keys__ == frozenset(
        {"parameters", "replay_safety", "encoding", "headers", "result_error"},
    )


def test_not_executed_reason_is_the_exact_frozen_enum() -> None:
    assert tuple(reason.value for reason in NotExecutedReason) == (
        "halted",
        "source_failed",
        "local_validation_failed",
        "scheduler_stopped",
    )


def _error() -> ProtocolError:
    return ProtocolError("synthetic failure")


def test_command_correlation_is_opaque_and_excluded_from_repr() -> None:
    correlation = {"private": object()}
    command = Command(Request("test.method", {"wire": 1}, route=RouteKind.BARE), correlation)

    assert command.correlation is correlation
    assert "private" not in repr(command)
    assert command.request.copy_parameters() == {"wire": 1}
    with pytest.raises(FrozenInstanceError):
        command.request = Request("other.method", route=RouteKind.BARE)  # type: ignore[misc]


def test_command_outcome_partition_retains_every_closed_variant() -> None:
    request = Request("test.method", replay_safety=ReplaySafety.UNKNOWN, route=RouteKind.BARE)
    correlation = object()
    outcomes = (
        CommandSuccess(0, correlation, request.summary, Response({"ok": True})),
        CommandFailure(1, correlation, request.summary, _error()),
        CommandNotExecuted(2, correlation, request.summary, NotExecutedReason.HALTED),
        CommandOutcomeUnknown(3, correlation, request.summary, _error()),
    )

    buckets = partition_command_outcomes(outcomes)

    bucket_sizes = tuple(
        len(items) for items in (buckets.successes, buckets.failures, buckets.not_executed, buckets.unknown)
    )
    assert bucket_sizes == (
        1,
        1,
        1,
        1,
    )
    assert buckets.successes[0].result == {"ok": True}
    assert all(outcome.correlation is correlation for outcome in outcomes)


def test_binding_rejects_overlapping_paths_and_never_exposes_correlation_in_repr() -> None:
    correlation = ["caller-owned"]
    binding = Binding(
        "safe summary",
        (ParameterUpdate(ParameterPath(("filter", "ID")), CORRELATION_VALUE),),
        correlation,
    )

    assert binding.correlation is correlation
    assert binding.updates[0].value == CORRELATION_VALUE
    assert "caller-owned" not in repr(binding)

    with pytest.raises(ValueError, match="overlapping"):
        Binding(
            "bad",
            (
                ParameterUpdate(ParameterPath(("filter",)), {}),
                ParameterUpdate(ParameterPath(("FILTER", "ID")), CORRELATION_VALUE),
            ),
            correlation,
        )


def test_binding_summary_is_redacted_utf8_text_bounded_to_256_code_points() -> None:
    summary = "я" * SUMMARY_LIMIT
    binding = Binding(summary, (), object())

    assert binding.summary == summary
    assert len(binding.summary) == SUMMARY_LIMIT
    with pytest.raises(ValueError, match=r"1\.\.256"):
        Binding("я" * (SUMMARY_LIMIT + 1), (), object())

    raw_summary = "Authorization: Bearer n1x2y3z4q5w6e7r8"
    redacted = Binding(raw_summary, (), object())
    assert raw_summary not in redacted.summary
    assert "n1x2y3z4q5w6e7r8" not in redacted.summary


def test_reference_partition_retains_items_empty_completion_and_negative_variants() -> None:
    correlation = object()
    outcomes = (
        ReferenceItem(0, correlation, 0, {"id": 1}),
        ReferenceComplete(0, correlation, 1),
        ReferenceFailure(1, correlation, _error(), 2),
        ReferenceNotExecuted(2, correlation, NotExecutedReason.SCHEDULER_STOPPED),
        ReferenceOutcomeUnknown(3, correlation, _error(), 0),
    )

    buckets = partition_reference_outcomes(outcomes)

    assert tuple(
        len(items)
        for items in (buckets.items, buckets.completions, buckets.failures, buckets.not_executed, buckets.unknown)
    ) == (1, 1, 1, 1, 1)
    assert all(outcome.correlation is correlation for outcome in outcomes)


def test_dispatch_is_a_discriminated_union_without_irrelevant_controls() -> None:
    direct = DirectDispatch(concurrency=DIRECT_CONCURRENCY, output_order=DeliveryOrder.INPUT)
    assert direct.concurrency == DIRECT_CONCURRENCY
    assert BatchDispatch(batch_size=BATCH_SIZE, concurrency=2).batch_size == BATCH_SIZE
    with pytest.raises(ValueError, match="between 1 and 50"):
        BatchDispatch(batch_size=51)
    with pytest.raises(TypeError):
        DirectDispatch(batch_size=BATCH_SIZE)  # type: ignore[call-arg]


def test_counted_reference_traversal_has_no_competing_batch_size() -> None:
    identity = IdentitySpec(("ID",), "ID", "ID")

    assert "batch_size" not in inspect.signature(CountedTraversal).parameters
    with pytest.raises(TypeError):
        CountedTraversal(identity, batch_size=5)  # type: ignore[call-arg]


def test_cursor_contract_uses_exact_public_enum_values() -> None:
    cursor = CursorSpec(
        parameter_path=ParameterPath(("LAST_ID",)),
        item_path=("id",),
        coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
        direction="ascending",
        take="last",
    )

    assert cursor.direction == "ascending"
    assert cursor.take == "last"


def test_operation_report_terminal_properties_are_explicit() -> None:
    completed = OperationReport(
        TerminalState.COMPLETED,
        "iter_list",
        "empty confirmation",
        assurance=TraversalAssurance.IDENTITY_EXACT,
        emitted=2,
        unique_rows=2,
    )
    partial = OperationReport(TerminalState.EARLY_CLOSED, "iter_list", "helper limit", emitted=1)

    assert completed.successful
    assert completed.exhausted
    assert not completed.partial
    assert not partial.successful
    assert not partial.exhausted
    assert partial.partial
    with_failures = OperationReport(TerminalState.COMPLETED_WITH_FAILURES, "batch", "some commands failed")
    assert not with_failures.exhausted
    assert with_failures.partial
