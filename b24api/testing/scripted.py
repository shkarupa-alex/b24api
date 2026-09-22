"""Exact offline transport fixture with no implicit network or empty fallbacks."""

from __future__ import annotations
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from b24api.contracts.request import ReplaySafety, Request, RouteKind
from b24api.encoding import encode_php_query
from b24api.transport.base import TransportCapabilities, WireRequest, WireResponse

if TYPE_CHECKING:
    from collections.abc import Mapping

_PORTAL_BATCH_CAP = 50


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

    @classmethod
    def batch(
        cls,
        commands: tuple[Request, ...],
        results: tuple[object, ...],
        *,
        total: int,
        continuations: tuple[int | None, ...] | None = None,
    ) -> ScriptedExchange:
        """Freeze an exact physical batch request and correlated result envelope."""
        if not commands or len(commands) > _PORTAL_BATCH_CAP or len(results) != len(commands):
            raise ValueError("batch fixture requires 1..50 commands and matching results")
        if continuations is None:
            continuations = (None,) * len(commands)
        if len(continuations) != len(commands):
            raise ValueError("batch fixture continuations must match commands")
        if any(command.route is not RouteKind.BARE or command.replay_safety is not ReplaySafety.SAFE
               or command.positional is not None or command.headers.items for command in commands):
            raise ValueError("batch fixture requires safe BARE commands without positional slots or headers")
        keys = tuple(f"c{index:012d}" for index in range(len(commands)))
        queries = tuple(
            encode_php_query(cast("Mapping[str | int, object]", command.to_wire_parameters()))
            for command in commands
        )
        encoded = {
            key: command.method if not query else f"{command.method}?{query}"
            for key, command, query in zip(keys, commands, queries, strict=True)
        }
        request = Request(
            "batch", parameters={"halt": 0, "cmd": encoded},
            replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE,
        )
        payload = {"result": {
            "result": dict(zip(keys, results, strict=True)),
            "result_error": {},
            "result_total": dict.fromkeys(keys, total),
            "result_next": {key: value for key, value in zip(keys, continuations, strict=True)
                            if value is not None},
        }}
        return cls.json(request, payload)


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
