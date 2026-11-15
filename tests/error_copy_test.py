"""Every public error class survives ``pickle``, ``copy`` and ``deepcopy`` with its attributes.

Error trackers, multiprocessing pools and task queues pickle exceptions, and frameworks copy them. The
samples cover every exception class ``b24api.errors`` exports, so a new class fails here until it has one.
"""

from __future__ import annotations
import copy
import pickle
from typing import TYPE_CHECKING

import pytest

import b24api.errors
from b24api import ReplaySafety, Request
from b24api.contracts import (
    CommandFailure,
    KeysetCapabilityCheckName,
    KeysetCapabilityCheckOutcome,
    KeysetCapabilityCheckResult,
    KeysetCapabilityReport,
    KeysetCapabilityVerdict,
    OperationReport,
    ReferenceFailure,
    ResultSelector,
    TerminalState,
)
from b24api.contracts.keyset_capability import KeysetInconclusiveReason
from b24api.contracts.policy import AmbiguityReason, IdentityCoercion, ReplayDisposition
from b24api.contracts.response import ResultCollectionShape
from b24api.errors import (
    AmbiguousExecutionError,
    ApiResponseError,
    B24ApiError,
    BatchCommandError,
    BatchFailed,
    BudgetExceededError,
    CapabilityError,
    CleanupOverrunError,
    ClientClosedError,
    ContinuationError,
    ContinuationReason,
    EnvelopeContractError,
    ErrorOrigin,
    FailurePhase,
    HTTPGatewayError,
    IdentityContractError,
    IncompleteTraversalError,
    InputSourceError,
    KeysetCapabilityError,
    PageAdaptationError,
    PageAdaptationViolation,
    PaginationError,
    ProtocolError,
    ReferenceFailed,
    ResponseTooLargeError,
    ResultShapeError,
    ResultTooLargeError,
    TransportError,
    ValidationIssue,
)
from b24api.redaction import SafeText

if TYPE_CHECKING:
    from collections.abc import Callable

_SUMMARY = Request.bare("crm.deal.add", {"fields": {"TITLE": "x"}}, ReplaySafety.UNSAFE).summary
_REPORT = OperationReport(TerminalState.FAILED, "batch", "failed")


def _api_error() -> ApiResponseError:
    return ApiResponseError(
        code="ACCESS_DENIED",
        description="Access denied",
        request_summary=_SUMMARY,
        http_status=403,
        headers={"x-request-id": "r-1"},
        body_preview='{"error":"ACCESS_DENIED"}',
        validation=(ValidationIssue("fields.TITLE", "required"),),
    )


def _keyset_report() -> KeysetCapabilityReport:
    return KeysetCapabilityReport(
        verdict=KeysetCapabilityVerdict.INCONCLUSIVE,
        checks=tuple(
            KeysetCapabilityCheckResult(name, KeysetCapabilityCheckOutcome.PASSED) for name in KeysetCapabilityCheckName
        ),
        physical_requests=2,
        batch_waves=2,
        logical_commands=7,
        cross_digit_pair_exercised=False,
        inconclusive_reason=KeysetInconclusiveReason.INSUFFICIENT_ROWS,
        inconclusive_detail=None,
        violations=(),
        page_trace=(),
        page_trace_truncated=False,
    )


_SAMPLES: dict[str, Callable[[], B24ApiError]] = {
    "AmbiguousExecutionError": lambda: AmbiguousExecutionError(
        "outcome unknown",
        reason=next(iter(AmbiguityReason)),
        declared_unsafe=True,
        request_summary=_SUMMARY,
    ),
    "ApiResponseError": _api_error,
    "B24ApiError": lambda: B24ApiError("base", origin=ErrorOrigin.PROTOCOL, description="detail"),
    "BatchCommandError": lambda: BatchCommandError(code="NOT_FOUND", description="Not found", request_summary=_SUMMARY),
    "BatchFailed": lambda: BatchFailed(
        (CommandFailure(0, "deal-1", _SUMMARY, _api_error(), ReplayDisposition.NOT_ELIGIBLE),),
        report=_REPORT,
    ),
    "BudgetExceededError": lambda: BudgetExceededError("budget", request_summary=_SUMMARY),
    "CapabilityError": lambda: CapabilityError("capability"),
    "CleanupOverrunError": lambda: CleanupOverrunError("cleanup exceeded the operation time budget"),
    "ClientClosedError": lambda: ClientClosedError(request_summary=_SUMMARY),
    "ContinuationError": lambda: ContinuationError(
        next(iter(ContinuationReason)), "no proof", request_summary=_SUMMARY
    ),
    "EnvelopeContractError": lambda: EnvelopeContractError("not an envelope"),
    "HTTPGatewayError": lambda: HTTPGatewayError("gateway"),
    "IdentityContractError": lambda: IdentityContractError(
        path=("ID",),
        coercion=IdentityCoercion.EXACT_INTEGER,
        observed_type="str",
        row_offset=3,
        component_index=0,
        component_label="id",
    ),
    "IncompleteTraversalError": lambda: IncompleteTraversalError(
        report=_REPORT,
        error=TransportError("reset", phase=FailurePhase.DISPATCH_STARTED),
        replay_disposition=ReplayDisposition.NOT_ELIGIBLE,
    ),
    "InputSourceError": lambda: InputSourceError("source failed"),
    "KeysetCapabilityError": lambda: KeysetCapabilityError(report=_keyset_report()),
    "PageAdaptationError": lambda: PageAdaptationError(
        violation=PageAdaptationViolation.CARDINALITY_CHANGED, adapter="adapter", row_offset=1, page_offset=50
    ),
    "PaginationError": lambda: PaginationError("pagination"),
    "ProtocolError": lambda: ProtocolError(SafeText("protocol")),
    "ReferenceFailed": lambda: ReferenceFailed(
        (ReferenceFailure(0, "parent-1", _api_error(), 0),),
        report=_REPORT,
    ),
    "ResponseTooLargeError": lambda: ResponseTooLargeError("too large"),
    "ResultShapeError": lambda: ResultShapeError(
        selector=ResultSelector(("items",)),
        expected_shape=ResultCollectionShape.SEQUENCE,
        observed_type="dict",
        page_offset=50,
    ),
    "ResultTooLargeError": lambda: ResultTooLargeError(rows=3, limit=2, request_summary=_SUMMARY),
    "TransportError": lambda: TransportError(
        "reset", phase=FailurePhase.HEADERS_RECEIVED, request_summary=_SUMMARY, retryable=False
    ),
}
_EXPORTED_ERRORS = sorted(
    name
    for name in b24api.errors.__all__
    if isinstance(exported := getattr(b24api.errors, name), type) and issubclass(exported, BaseException)
)
_COPIERS: dict[str, Callable[[B24ApiError], B24ApiError]] = {
    "pickle": lambda error: pickle.loads(pickle.dumps(error)),  # noqa: S301 - round trip of a value built here
    "copy": copy.copy,
    "deepcopy": copy.deepcopy,
}


def test_every_exported_error_class_has_a_sample() -> None:
    assert sorted(_SAMPLES) == _EXPORTED_ERRORS


@pytest.mark.parametrize("copier", sorted(_COPIERS))
@pytest.mark.parametrize("name", _EXPORTED_ERRORS)
def test_exported_error_survives_pickle_and_copy_with_its_attributes(name: str, copier: str) -> None:
    original = _SAMPLES[name]()
    original.add_note("note kept with the error")

    restored = _COPIERS[copier](original)

    assert type(restored) is type(original) is getattr(b24api.errors, name)
    assert restored is not original
    assert restored.args == original.args
    assert str(restored) == str(original)
    assert repr(restored) == repr(original)
    assert restored.to_safe_dict() == original.to_safe_dict()
    # Every attribute is kept, nested errors and reports included; an error compares by identity, so the
    # attributes are compared by their safe representations.
    assert {key: repr(value) for key, value in vars(restored).items()} == {
        key: repr(value) for key, value in vars(original).items()
    }
