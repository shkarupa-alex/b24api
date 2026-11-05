"""Exact offline transport fixture with no implicit network or empty fallbacks."""

from __future__ import annotations
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from b24api.contracts.dispatch import PORTAL_BATCH_CAP
from b24api.contracts.json import _freeze_json
from b24api.contracts.request import ReplaySafety, Request
from b24api.contracts.request_summary import RouteKind
from b24api.contracts.wire import BodyEncoding
from b24api.contracts.wire_body import V3_BATCH_METHOD, envelope_safety
from b24api.encoding import encode_php_query
from b24api.transport.base import TransportCapabilities, WireRequest, WireResponse

if TYPE_CHECKING:
    from b24api.contracts.json import JsonValue

_JSON_HEADERS = (("content-type", "application/json"),)


@dataclass(frozen=True, slots=True)
class ScriptedV3Command:
    """The wire-visible part of one native REST v3 batch command: its method and query object.

    ``query`` is kept as a frozen canonical copy, so two commands compare equal exactly when the same JSON
    object is sent for them. Per-command replay safety and embedded-error contracts are not on the wire and
    are not part of this value.
    """

    method: str
    query: Mapping[str, JsonValue] = field(repr=False)

    def __post_init__(self) -> None:
        """Freeze the query into its canonical immutable form."""
        if not isinstance(self.method, str) or not self.method:
            raise ValueError("scripted v3 command method must be a non-empty string")
        if not isinstance(self.query, Mapping):
            raise TypeError("scripted v3 command query must be a mapping")
        object.__setattr__(self, "query", _freeze_json(self.query))


def _v3_descriptor(replay_safety: ReplaySafety) -> Request:
    return Request(V3_BATCH_METHOD, parameters={}, replay_safety=replay_safety, route=RouteKind.API_V3)


def _is_v3_descriptor(request: Request) -> bool:
    return (
        isinstance(request, Request)
        and request.method == V3_BATCH_METHOD
        and request.route is RouteKind.API_V3
        and request.encoding is BodyEncoding.JSON
        and request.positional is None
        and not request.headers.items
        and request.result_error is None
        and not request.copy_parameters()
    )


@dataclass(frozen=True, slots=True)
class ScriptedExchange:
    """One exact expected request and its frozen wire response.

    A native REST v3 batch exchange keeps ``request`` as the envelope descriptor (``batch`` on API_V3 with
    the envelope's replay safety) and lists the wire-visible commands, in order, in ``v3_wire``.
    """

    request: Request
    response: WireResponse
    v3_wire: tuple[ScriptedV3Command, ...] | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        """Validate the v3 batch descriptor invariant."""
        if self.v3_wire is None:
            return
        wire = tuple(self.v3_wire)
        object.__setattr__(self, "v3_wire", wire)
        if not 1 <= len(wire) <= PORTAL_BATCH_CAP or any(not isinstance(item, ScriptedV3Command) for item in wire):
            raise ValueError("v3 batch fixture requires 1..50 ScriptedV3Command values")
        if not _is_v3_descriptor(self.request):
            raise ValueError("v3 batch fixture request must be the batch envelope descriptor on API_V3")

    @classmethod
    def json(cls, request: Request, payload: object) -> ScriptedExchange:
        """Build a normal JSON REST response from deterministic fixture data."""
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return cls(request, WireResponse(200, _JSON_HEADERS, body))

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
        if not commands or len(commands) > PORTAL_BATCH_CAP or len(results) != len(commands):
            raise ValueError("batch fixture requires 1..50 commands and matching results")
        if continuations is None:
            continuations = (None,) * len(commands)
        if len(continuations) != len(commands):
            raise ValueError("batch fixture continuations must match commands")
        if any(
            command.route is not RouteKind.BARE
            or command.replay_safety is not ReplaySafety.SAFE
            or command.positional is not None
            or command.headers.items
            for command in commands
        ):
            raise ValueError("batch fixture requires safe BARE commands without positional slots or headers")
        keys = tuple(f"c{index:012d}" for index in range(len(commands)))
        queries = tuple(
            encode_php_query(cast("Mapping[str | int, object]", command.to_wire_parameters())) for command in commands
        )
        encoded = {
            key: command.method if not query else f"{command.method}?{query}"
            for key, command, query in zip(keys, commands, queries, strict=True)
        }
        request = Request(
            "batch",
            parameters={"halt": 0, "cmd": encoded},
            replay_safety=ReplaySafety.SAFE,
            route=RouteKind.BARE,
        )
        payload = {
            "result": {
                "result": dict(zip(keys, results, strict=True)),
                "result_error": {},
                "result_total": dict.fromkeys(keys, total),
                "result_next": {
                    key: value for key, value in zip(keys, continuations, strict=True) if value is not None
                },
            }
        }
        return cls.json(request, payload)

    @classmethod
    def v3_batch(
        cls,
        commands: tuple[Request, ...],
        results: tuple[object, ...] | None = None,
        *,
        payload: object | None = None,
        status: int = 200,
    ) -> ScriptedExchange:
        """Freeze one native REST v3 batch envelope and its response.

        ``results`` builds the all-success ``{"result": [...]}`` answer in input order; ``payload`` (with
        ``status``) freezes any other body, such as a top-level abort or a short or malformed array.
        """
        commands = tuple(commands)
        if not 1 <= len(commands) <= PORTAL_BATCH_CAP:
            raise ValueError("v3 batch fixture requires 1..50 commands")
        if any(
            not isinstance(command, Request)
            or command.route is not RouteKind.API_V3
            or command.encoding is not BodyEncoding.JSON
            or command.positional is not None
            or command.headers.items
            for command in commands
        ):
            raise ValueError("v3 batch fixture requires API_V3 JSON commands without positional slots or headers")
        if (results is None) == (payload is None):
            raise ValueError("v3 batch fixture requires exactly one of results or payload")
        if results is not None:
            results = tuple(results)
            if len(results) != len(commands):
                raise ValueError("v3 batch fixture results must match commands")
            payload = {"result": list(results)}
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return cls(
            _v3_descriptor(envelope_safety(command.replay_safety for command in commands)),
            WireResponse(status, _JSON_HEADERS, body),
            v3_wire=tuple(ScriptedV3Command(command.method, command.copy_parameters()) for command in commands),
        )


@dataclass(frozen=True, slots=True)
class ScriptedCall:
    """One observed transport call, as it was on the wire.

    A classic or positional call keeps its canonical ``request``; a native v3 batch envelope has no
    ``Request`` form and records its wire-visible commands in ``v3_wire`` instead.
    """

    request: Request | None
    v3_wire: tuple[ScriptedV3Command, ...] | None
    replay_safety: ReplaySafety


class ScriptedTransport:
    """Match a finite frozen exchange table by complete canonical Request value."""

    capabilities = TransportCapabilities(positional_json=True, json_array_body=True, routes=frozenset(RouteKind))

    def __init__(self, exchanges: tuple[ScriptedExchange, ...], *, host: str = "fixture.invalid") -> None:
        """Copy fixture entries without reading credentials or opening a socket."""
        if any(not isinstance(item, ScriptedExchange) for item in exchanges):
            raise TypeError("exchanges must contain ScriptedExchange values")
        if not isinstance(host, str) or not host or "/" in host or "@" in host:
            raise ValueError("host must be a credential-free hostname")
        self._host = host
        self._remaining = list(exchanges)
        self._calls: list[Request] = []
        self._wire_calls: list[ScriptedCall] = []

    @property
    def host(self) -> str:
        """Return the fixture host without credentials."""
        return self._host

    @property
    def calls(self) -> tuple[Request, ...]:
        """Return the exact requests observed so far; a v3 batch envelope has no Request form (``wire_calls``)."""
        return tuple(self._calls)

    @property
    def wire_calls(self) -> tuple[ScriptedCall, ...]:
        """Return every observed call in send order, including native v3 batch envelopes."""
        return tuple(self._wire_calls)

    @property
    def remaining(self) -> int:
        """Return the count of unused frozen exchanges."""
        return len(self._remaining)

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Consume one exact exchange or fail closed on an unknown request."""
        if attempt_timeout <= 0 or max_response_bytes < 1:
            raise ValueError("transport budgets must be positive")
        self._calls.append(request)
        self._wire_calls.append(ScriptedCall(request, None, request.replay_safety))
        for index, exchange in enumerate(self._remaining):
            # A v3 batch exchange never matches a classic call, even one equal to its envelope descriptor.
            if exchange.v3_wire is None and exchange.request == request:
                return self._take(index, max_response_bytes)
        raise AssertionError(f"unexpected scripted request: {request.method}")

    async def send_wire(
        self,
        request: WireRequest,
        *,
        attempt_timeout: float,
        max_response_bytes: int,
    ) -> WireResponse:
        """Match advanced JSON requests, including exact positional slot values and v3 batch envelopes."""
        if request.array_body is not None:
            if attempt_timeout <= 0 or max_response_bytes < 1:
                raise ValueError("transport budgets must be positive")
            return self._send_v3_batch(request, max_response_bytes=max_response_bytes)
        canonical = Request(
            request.method,
            parameters=request.positional if request.positional is not None else request.copy_parameters(),
            replay_safety=request.replay_safety,
            encoding=request.encoding,
            headers=request.headers,
            result_error=request.result_error,
            route=request.route,
        )
        return await self.send(canonical, attempt_timeout=attempt_timeout, max_response_bytes=max_response_bytes)

    def _send_v3_batch(self, request: WireRequest, *, max_response_bytes: int) -> WireResponse:
        body = request.array_body
        if body is None:  # pragma: no cover - guarded by send_wire
            raise AssertionError("scripted v3 batch call has no array body")
        wire = tuple(
            ScriptedV3Command(command.method, cast("Mapping[str, JsonValue]", command.query))
            for command in body.commands
        )
        self._wire_calls.append(ScriptedCall(None, wire, request.replay_safety))
        for index, exchange in enumerate(self._remaining):
            # Only wire-visible data matches: the ordered commands and the envelope's replay safety.
            if exchange.v3_wire == wire and exchange.request.replay_safety is request.replay_safety:
                return self._take(index, max_response_bytes)
        raise AssertionError(f"unexpected scripted request: {request.method}")

    def _take(self, index: int, max_response_bytes: int) -> WireResponse:
        response = self._remaining.pop(index).response
        if response.byte_length > max_response_bytes:
            raise AssertionError("scripted response exceeds the caller's byte ceiling")
        return response

    def assert_exhausted(self) -> None:
        """Fail when a recipe did not issue every expected fixture request."""
        if self._remaining:
            raise AssertionError(f"{len(self._remaining)} scripted exchange(s) were not requested")
