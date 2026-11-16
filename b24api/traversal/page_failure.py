"""The one rule of a failed page's trace: whether it is traced (it was sent) and how (as it settles)."""

from __future__ import annotations

from b24api.contracts.completion import CommandSettlement
from b24api.contracts.report import PageOutcome, PageRejectionCode
from b24api.errors import ProtocolError


def failed_page(
    error: object, settled: CommandSettlement, *, sent: bool
) -> tuple[PageOutcome, PageRejectionCode] | None:
    """Return how a page whose command failed is traced, or ``None`` for a page that was never sent.

    The trace records a request that was sent: a page whose command was sent (``sent``, its execution's
    ``dispatched``) is traced, whatever its settlement; one that never left the client is not, wherever it was
    stopped (by the operation's budget, a closed client, a cancellation) and on every path, direct or batched.
    The page is traced the way it settles (``settled``): unknown only when it may have run, otherwise rejected. The
    code names why, from the failure that explains it: the physical batch envelope's own protocol failure, or the
    command's; a page whose own command succeeded was dropped with the batch that failed around it, as a page of an
    aborted keyset transaction is (``TRANSACTION_ABORTED``), not by a failure of its command.
    """
    if not sent:
        return None
    if settled is CommandSettlement.UNKNOWN:
        if _batch_envelope_failure(getattr(error, "__cause__", None)):
            return PageOutcome.UNKNOWN, PageRejectionCode.BATCH_ENVELOPE
        return PageOutcome.UNKNOWN, PageRejectionCode.AMBIGUOUS_EXECUTION
    if settled is CommandSettlement.SUCCESS:
        # Its own command succeeded: the page is rejected with the physical batch that failed around it.
        return PageOutcome.REJECTED, PageRejectionCode.TRANSACTION_ABORTED
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
