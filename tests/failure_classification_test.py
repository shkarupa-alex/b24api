"""Normative terminal-failure classification and rendering regressions."""

from __future__ import annotations

import b24api.errors as error_types
from b24api import IdentityCoercion, OperationReport, ResultSelector, TerminalState
from b24api.contracts import AmbiguityReason, ReplayDisposition, ResultCollectionShape
from b24api.execution.failure import classify_failure, finalize_failure


def test_failure_classification_matches_the_public_contract_table() -> None:
    report = OperationReport(operation="test", state=TerminalState.INCOMPLETE, terminal_reason="test failed")
    cases = (
        (error_types.IncompleteTraversalError(report=report), "pagination_invariant"),
        (
            error_types.IdentityContractError(
                path=("ID",),
                coercion=IdentityCoercion.EXACT_INTEGER,
                observed_type="string",
                row_offset=0,
            ),
            "identity_contract",
        ),
        (
            error_types.ResultShapeError(
                selector=ResultSelector.root(),
                expected_shape=ResultCollectionShape.MAPPING_VALUES,
                observed_type="string",
            ),
            "result_shape",
        ),
        (error_types.PaginationError("page"), "pagination_invariant"),
        (error_types.CapabilityError("capability"), "capability_rejected"),
        (
            error_types.AmbiguousExecutionError(
                "ambiguous",
                reason=AmbiguityReason.HTTP_STATUS_AFTER_DISPATCH,
                declared_unsafe=False,
            ),
            "ambiguous_execution",
        ),
        (error_types.BudgetExceededError("budget"), "budget_exhausted"),
        (error_types.ResponseTooLargeError("large"), "response_too_large"),
        (error_types.TransportError("transport"), "transport_failure"),
        (error_types.EnvelopeContractError("envelope"), "envelope_contract"),
        (error_types.HTTPGatewayError("http"), "http_failure"),
        (error_types.ProtocolError("protocol"), "protocol_failure"),
        (error_types.BatchCommandError(code="batch", description=None), "batch_command_failure"),
        (error_types.ApiResponseError(code="api", description=None), "api_error"),
        (error_types.InputSourceError("source"), "source_failure"),
        (RuntimeError("internal"), "internal_failure"),
    )

    assert [(classify_failure(error).code) for error, _expected in cases] == [expected for _error, expected in cases]


def test_finalize_failure_adds_one_stable_blocking_violation_before_attachment() -> None:
    report = OperationReport(operation="call", state=TerminalState.COMPLETED, terminal_reason="completed")
    error = error_types.EnvelopeContractError("envelope")

    finalized, propagated = finalize_failure(
        error,
        report,
        operation="call",
        terminal_reason="Malformed successful HTTP response",
    )

    assert propagated is error
    assert finalized.state is TerminalState.FAILED
    assert [(violation.code, violation.message) for violation in finalized.violations] == [
        ("envelope_contract", "Malformed successful HTTP response (EnvelopeContractError)"),
    ]
    assert error.report is finalized
    violation = finalized.violations[0]
    assert violation.error is error
    assert violation.replay_disposition is ReplayDisposition.NOT_ELIGIBLE
    assert violation.to_safe_dict()["error"] == error.to_safe_dict()


def test_incomplete_traversal_exposes_its_original_cause_and_replay_decision() -> None:
    report = OperationReport(operation="list", state=TerminalState.INCOMPLETE, terminal_reason="page failed")
    cause = error_types.ApiResponseError(code="QUERY_LIMIT_EXCEEDED", description="retry later")
    error = error_types.IncompleteTraversalError(
        report=report,
        error=cause,
        replay_disposition=ReplayDisposition.ELIGIBLE,
    )

    assert error.error is cause
    assert error.replay_disposition is ReplayDisposition.ELIGIBLE
    assert error.to_safe_dict()["cause"] == cause.to_safe_dict()
    assert error.to_safe_dict()["replay_disposition"] == "eligible"
