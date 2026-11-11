"""The one mapping from a failed page's execution facts to its trace outcome and rejection code."""

from __future__ import annotations

from b24api.contracts.report import PageOutcome, PageRejectionCode
from b24api.errors import ProtocolError
from b24api.execution.facts import Execution, execution


def failed_page(error: object) -> tuple[PageOutcome, PageRejectionCode]:
    """Return how a page whose command failed is traced.

    The outcome follows the command's execution facts, as its settlement does: unknown only when it may have run,
    otherwise rejected. The code names why, from the failure that explains it: the physical batch envelope's own
    protocol failure, or the command's.
    """
    if execution(error) is Execution.POSSIBLY_RUN:
        if _batch_envelope_failure(getattr(error, "__cause__", None)):
            return PageOutcome.UNKNOWN, PageRejectionCode.BATCH_ENVELOPE
        return PageOutcome.UNKNOWN, PageRejectionCode.AMBIGUOUS_EXECUTION
    if _batch_envelope_failure(error):
        return PageOutcome.REJECTED, PageRejectionCode.BATCH_ENVELOPE
    return PageOutcome.REJECTED, PageRejectionCode.COMMAND_FAILURE


def _batch_envelope_failure(error: object) -> bool:
    """Return whether a failure is the physical batch envelope's own protocol failure, not one command's."""
    if not isinstance(error, ProtocolError):
        return False
    summary = error.request_summary
    return summary is not None and summary.method == "batch"


__all__ = ["failed_page"]
