"""Native v3 batch envelope value (#21, A21-WIRE / A21-PREFLIGHT): evidence, guards and capability preflight."""

# ruff: noqa: SLF001 - white-box checks of the batch-owned envelope constructor

from __future__ import annotations

import pytest

from b24api.contracts.json import FrozenMapping
from b24api.contracts.request import ReplaySafety, RouteKind
from b24api.contracts.wire_body import V3BatchBody, V3BatchCommand, envelope_safety
from b24api.errors import CapabilityError
from b24api.testing import ScriptedTransport
from b24api.transport import TransportCapabilities, WireRequest
from b24api.transport.base import preflight_wire, transport_supports_json_array_body

SAFE = ReplaySafety.SAFE
UNSAFE = ReplaySafety.UNSAFE
UNKNOWN = ReplaySafety.UNKNOWN


def _envelope(*, safety: ReplaySafety = SAFE) -> WireRequest:
    body = V3BatchBody((V3BatchCommand("x.secret.get", FrozenMapping({"token": "s3cr3t"})),))
    return WireRequest._v3_batch(body, replay_safety=safety)


def test_a21_wire_envelope_is_a_value_free_api_v3_batch_request() -> None:
    """A21-WIRE: route, method, array body and evidence of the native envelope."""
    envelope = _envelope()
    assert envelope.method == "batch"
    assert envelope.route is RouteKind.API_V3
    assert envelope.replay_safety is SAFE
    assert envelope.array_body is not None
    assert envelope.array_body.to_wire() == [{"method": "x.secret.get", "query": {"token": "s3cr3t"}}]
    summary = envelope.summary
    assert summary.method == "batch"
    assert summary.parameter_keys == ("@v3_batch",)
    assert summary.route is RouteKind.API_V3
    evidence = repr(envelope) + repr(summary) + repr(envelope.array_body)
    assert "s3cr3t" not in evidence
    assert "x.secret.get" not in evidence
    with pytest.raises(ValueError, match="array"):
        envelope.copy_parameters()


def test_a21_wire_body_cardinality_and_envelope_safety() -> None:
    """A21-WIRE: 1..50 commands; all SAFE is SAFE, any UNSAFE is UNSAFE, otherwise UNKNOWN."""
    with pytest.raises(ValueError, match="1"):
        V3BatchBody(())
    command = V3BatchCommand("x.item.get", FrozenMapping({}))
    with pytest.raises(ValueError, match="50"):
        V3BatchBody((command,) * 51)
    assert envelope_safety((SAFE, SAFE)) is SAFE
    assert envelope_safety((SAFE, UNKNOWN)) is UNKNOWN
    assert envelope_safety((UNKNOWN, UNSAFE, SAFE)) is UNSAFE


class _NoArray(ScriptedTransport):
    capabilities = TransportCapabilities(positional_json=True, routes=frozenset(RouteKind))


class _NoV3Route(ScriptedTransport):
    capabilities = TransportCapabilities(json_array_body=True, routes=frozenset({RouteKind.BARE}))


def test_a21_preflight_requires_array_body_and_the_api_v3_route() -> None:
    """A21-PREFLIGHT: the envelope is refused locally unless both capabilities are declared."""
    envelope = _envelope(safety=UNSAFE)
    assert transport_supports_json_array_body(ScriptedTransport(()))
    preflight_wire(ScriptedTransport(()), envelope)
    for transport in (None, _NoArray(()), _NoV3Route(())):
        assert not transport_supports_json_array_body(transport)
        with pytest.raises(CapabilityError):
            preflight_wire(transport, envelope)
