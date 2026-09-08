"""Closed error classification values shared by the public hierarchy."""

from enum import StrEnum


class ErrorOrigin(StrEnum):
    """Layer that produced or detected an error."""

    REST_MODULE = "rest_module"
    BATCH_COMMAND = "batch_command"
    HTTP_GATEWAY = "http_gateway"
    TRANSPORT = "transport"
    PROTOCOL = "protocol"
    CAPABILITY = "capability"
    PAGINATION = "pagination"
    BUDGET = "budget"
    AMBIGUOUS_EXECUTION = "ambiguous_execution"


class FailurePhase(StrEnum):
    """Last transport lifecycle phase conclusively reached before failure."""

    NOT_DISPATCHED = "not_dispatched"
    CONNECTION_ESTABLISHED = "connection_established"
    DISPATCH_STARTED = "dispatch_started"
    HEADERS_RECEIVED = "headers_received"
    BODY_PARTIALLY_RECEIVED = "body_partially_received"
    RESPONSE_COMPLETE = "response_complete"


__all__ = ["ErrorOrigin", "FailurePhase"]
