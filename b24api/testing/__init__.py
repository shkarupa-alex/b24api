"""Reusable dependency-free conformance helpers for transport authors."""

from b24api.testing.scripted import ScriptedExchange, ScriptedTransport
from b24api.testing.transport import (
    ConformanceCase,
    ConformanceOutcome,
    ConformanceReport,
    TransportFactory,
    run_transport_conformance,
)

__all__ = [
    "ConformanceCase",
    "ConformanceOutcome",
    "ConformanceReport",
    "ScriptedExchange",
    "ScriptedTransport",
    "TransportFactory",
    "run_transport_conformance",
]
