"""Exact offline transport fixture with no implicit network or empty fallbacks."""

from __future__ import annotations
import json
from dataclasses import dataclass

from b24api.contracts.request import Request
from b24api.transport.base import TransportCapabilities, WireRequest, WireResponse


@dataclass(frozen=True, slots=True)
class ScriptedExchange:
    """One exact expected request and its frozen wire response."""

    request: Request
    response: WireResponse

    @classmethod
    def json(cls, request: Request, payload: object) -> ScriptedExchange:
        """Build a normal JSON REST response from deterministic fixture data."""
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return cls(request, WireResponse(200, (("content-type", "application/json"),), body))


class ScriptedTransport:
    """Match a finite frozen exchange table by complete canonical Request value."""

    capabilities = TransportCapabilities(positional_json=True)

    def __init__(self, exchanges: tuple[ScriptedExchange, ...], *, host: str = "fixture.invalid") -> None:
        """Copy fixture entries without reading credentials or opening a socket."""
        if any(not isinstance(item, ScriptedExchange) for item in exchanges):
            raise TypeError("exchanges must contain ScriptedExchange values")
        if not isinstance(host, str) or not host or "/" in host or "@" in host:
            raise ValueError("host must be a credential-free hostname")
        self._host = host
        self._remaining = list(exchanges)
        self._calls: list[Request] = []

    @property
    def host(self) -> str:
        """Return the fixture host without credentials."""
        return self._host

    @property
    def calls(self) -> tuple[Request, ...]:
        """Return the exact requests observed so far."""
        return tuple(self._calls)

    @property
    def remaining(self) -> int:
        """Return the count of unused frozen exchanges."""
        return len(self._remaining)

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Consume one exact exchange or fail closed on an unknown request."""
        if attempt_timeout <= 0 or max_response_bytes < 1:
            raise ValueError("transport budgets must be positive")
        self._calls.append(request)
        for index, exchange in enumerate(self._remaining):
            if exchange.request == request:
                response = self._remaining.pop(index).response
                if response.byte_length > max_response_bytes:
                    raise AssertionError("scripted response exceeds the caller's byte ceiling")
                return response
        raise AssertionError(f"unexpected scripted request: {request.method}")

    async def send_wire(
        self, request: WireRequest, *, attempt_timeout: float, max_response_bytes: int,
    ) -> WireResponse:
        """Match advanced JSON requests, including exact positional slot values."""
        canonical = Request(
            request.method,
            parameters=request.positional if request.positional is not None else request.copy_parameters(),
            replay_safety=request.replay_safety,
            encoding=request.encoding, headers=request.headers,
            result_error=request.result_error, route=request.route,
        )
        return await self.send(canonical, attempt_timeout=attempt_timeout, max_response_bytes=max_response_bytes)

    def assert_exhausted(self) -> None:
        """Fail when a recipe did not issue every expected fixture request."""
        if self._remaining:
            raise AssertionError(f"{len(self._remaining)} scripted exchange(s) were not requested")
