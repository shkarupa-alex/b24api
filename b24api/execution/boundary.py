"""Executor boundary around one call of a possibly foreign transport."""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api._error_types import FailurePhase
from b24api.errors import B24ApiError, CapabilityError, ResponseTooLargeError, TransportError
from b24api.transport.base import TransportCapabilities

if TYPE_CHECKING:
    from b24api.contracts.request import Request
    from b24api.transport.base import Transport, WireRequest, WireResponse, WireTransport

_UNCLASSIFIED_TRANSPORT_FAILURE = "Transport raised an unclassified failure after the send began"
_RESPONSE_CEILING_EXCEEDED = "Decompressed response body exceeded the configured byte ceiling"


async def send_transport(  # noqa: PLR0913 - keeps legacy and wire boundaries explicit
    transport: Transport,
    wire_transport: WireTransport | None,
    request: Request,
    *,
    wire_request: WireRequest | None,
    attempt_timeout: float,
    max_response_bytes: int,
) -> WireResponse:
    """Call the transport once and classify what escapes it at the executor boundary.

    Validation before the transport is entered keeps its own proven classification. Once the
    transport's ``send``/``send_wire`` has been called, an arbitrary ``Exception`` cannot prove
    that the request never reached the server, so it becomes a non-retryable
    ``TransportError(phase=DISPATCH_STARTED)`` whose ``__cause__`` is the original exception.
    ``B24ApiError`` values keep the transport's own classification, ``TimeoutError`` keeps the
    executor's deadline classification, and ``BaseException`` values such as cancellation are
    never converted. A returned body larger than ``max_response_bytes`` is rejected before any
    protocol parsing with the same ``ResponseTooLargeError`` that ``HttpxTransport`` raises.
    """
    if wire_transport is not None:
        capabilities = wire_transport.capabilities
        if not isinstance(capabilities, TransportCapabilities):
            raise CapabilityError("transport exposes malformed capabilities")
        if wire_request is None or wire_request.route is not request.route or wire_request.method != request.method:
            raise CapabilityError("wire request differs from canonical request", request_summary=request.summary)
    try:
        if wire_transport is not None and wire_request is not None:
            wire = await wire_transport.send_wire(
                wire_request,
                attempt_timeout=attempt_timeout,
                max_response_bytes=max_response_bytes,
            )
        else:
            wire = await transport.send(
                request,
                attempt_timeout=attempt_timeout,
                max_response_bytes=max_response_bytes,
            )
    except (B24ApiError, TimeoutError):
        raise
    except Exception as error:
        raise TransportError(
            _UNCLASSIFIED_TRANSPORT_FAILURE,
            phase=FailurePhase.DISPATCH_STARTED,
            request_summary=request.summary,
            retryable=False,
        ) from error
    enforce_response_ceiling(wire, request, max_response_bytes=max_response_bytes)
    return wire


def enforce_response_ceiling(wire: WireResponse, request: Request, *, max_response_bytes: int) -> None:
    """Reject a returned body over the policy ceiling before it reaches a decoder.

    ``HttpxTransport`` and ``ScriptedTransport`` already enforce the ceiling while reading, so for
    them this O(1) check never fires; it binds injected transports to the same contract.
    """
    if len(wire.body) > max_response_bytes:
        raise ResponseTooLargeError(_RESPONSE_CEILING_EXCEEDED, request_summary=request.summary)


__all__ = ["enforce_response_ceiling", "send_transport"]
