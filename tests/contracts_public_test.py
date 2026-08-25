"""Characterization of the v2 public mechanics contracts."""

from __future__ import annotations
from dataclasses import FrozenInstanceError

import pytest

from b24api.contracts import (
    BatchDispatch,
    Binding,
    Command,
    CommandFailure,
    CommandNotExecuted,
    CommandOutcomeUnknown,
    CommandSuccess,
    CursorSpec,
    DeliveryOrder,
    DirectDispatch,
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
    Response,
    TerminalState,
    TraversalAssurance,
    partition_command_outcomes,
    partition_reference_outcomes,
)
from b24api.error import ProtocolError
from b24api.models import IdentityCoercion

CORRELATION_VALUE = 7
DIRECT_CONCURRENCY = 3
BATCH_SIZE = 7


def _error() -> ProtocolError:
    return ProtocolError("synthetic failure")


def test_command_correlation_is_opaque_and_excluded_from_repr() -> None:
    correlation = {"private": object()}
    command = Command(Request("test.method", {"wire": 1}), correlation)

    assert command.correlation is correlation
    assert "private" not in repr(command)
    assert command.request.copy_parameters() == {"wire": 1}
    with pytest.raises(FrozenInstanceError):
        command.request = Request("other.method")  # type: ignore[misc]


def test_command_outcome_partition_retains_every_closed_variant() -> None:
    request = Request("test.method", replay_safety=ReplaySafety.UNKNOWN)
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
