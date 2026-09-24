"""Core scripted scenarios for the golden gate, one per public operation family.

Each scenario runs the public ``Bitrix24`` API against ``GoldenPortal``, a deterministic in-memory
list endpoint with offset, keyset, cursor, batch and failing-item behavior. The scenarios are chosen
to cover every public stream family and the terminal causes a lifecycle refactor must preserve:
exhaustion, early close, command failure and transport failure.
"""

from __future__ import annotations
import contextlib
import json
from typing import TYPE_CHECKING
from urllib.parse import parse_qsl

from b24api import (
    BatchDispatch,
    Binding,
    Bitrix24,
    Command,
    CountedTraversal,
    CursorSpec,
    DeliveryOrder,
    DirectDispatch,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    OffsetSpec,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    SequentialKeysetExecution,
    SequentialTraversal,
    Settings,
    TotalTermination,
)
from b24api.contracts import CallerStop, PageBoundary
from b24api.errors import TransportError
from b24api.transport import TransportCapabilities, WireResponse

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from b24api.contracts.json import JsonValue

HOST = "fixture.invalid"
WEBHOOK = f"https://{HOST}/rest/1/gold/"
ROWS = tuple({"ID": identity, "NAME": f"row-{identity}"} for identity in range(1, 131))
PAGE = 50
FAILING_ITEM = 7


def _nested(pairs: list[tuple[str, str]]) -> dict[str, object]:
    """Decode a PHP bracket query into nested mappings with string leaves."""
    decoded: dict[str, object] = {}
    for key, value in pairs:
        parts = key.replace("]", "").split("[")
        target = decoded
        for part in parts[:-1]:
            target = target.setdefault(part, {})  # type: ignore[assignment]
        target[parts[-1]] = value
    return decoded


def _number(value: object, default: int) -> int:
    return default if value is None else int(str(value))


class GoldenPortal:
    """Deterministic list, cursor, item and batch endpoint over ``ROWS``."""

    capabilities = TransportCapabilities(routes=frozenset(RouteKind))

    def __init__(self, *, fail_list_after: int | None = None) -> None:
        """Start with no observed requests."""
        self.host = HOST
        self.list_requests = 0
        self.fail_list_after = fail_list_after
        self.calls: list[Request] = []

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Answer one request with a Bitrix-shaped JSON envelope."""
        del attempt_timeout
        self.calls.append(request)
        if request.method == "batch":
            payload = self._batch(request.copy_parameters())
        else:
            payload = self._dispatch(request.method, request.copy_parameters())
        body = json.dumps(payload, separators=(",", ":")).encode()
        assert len(body) <= max_response_bytes
        return WireResponse(200, (("content-type", "application/json"),), body)

    async def aclose(self) -> None:
        """Own no resources."""

    def _batch(self, parameters: dict[str, JsonValue]) -> object:
        commands = parameters["cmd"]
        assert isinstance(commands, dict)
        results: dict[str, object] = {}
        errors: dict[str, object] = {}
        totals: dict[str, int] = {}
        nexts: dict[str, int] = {}
        for key, encoded in commands.items():
            assert isinstance(encoded, str)
            method, _, query = encoded.partition("?")
            envelope = self._dispatch(method, _nested(parse_qsl(query)))
            assert isinstance(envelope, dict)
            if "error" in envelope:
                errors[key] = envelope
                continue
            results[key] = envelope["result"]
            if "total" in envelope:
                totals[key] = envelope["total"]
            if "next" in envelope:
                nexts[key] = envelope["next"]
        return {
            "result": {
                "result": results,
                "result_error": errors,
                "result_total": totals,
                "result_next": nexts,
            }
        }

    def _dispatch(self, method: str, parameters: Mapping[str, object]) -> object:
        if method == "golden.item.get":
            identity = _number(parameters.get("id"), 0)
            if identity == FAILING_ITEM:
                return {"error": "ACCESS_DENIED", "error_description": "denied"}
            return {"result": {"ID": identity}}
        if method == "golden.cursor.list":
            boundary = _number(parameters.get("LAST_ID"), 0)
            return {"result": [row for row in ROWS if _number(row["ID"], 0) > boundary][:PAGE]}
        assert method == "golden.item.list", method
        self.list_requests += 1
        if self.fail_list_after is not None and self.list_requests > self.fail_list_after:
            raise TransportError("synthetic dispatch failure")  # default phase: dispatch started
        return self._list(parameters)

    def _list(self, parameters: Mapping[str, object]) -> object:
        filters = parameters.get("filter") or {}
        order = parameters.get("order") or {}
        assert isinstance(filters, dict)
        assert isinstance(order, dict)
        owner = filters.get("OWNER")
        rows = [row for row in ROWS if owner is None or _number(row["ID"], 0) % 3 == _number(owner, 0) % 3]
        if ">ID" in filters:
            rows = [row for row in rows if _number(row["ID"], 0) > _number(filters[">ID"], 0)]
        if "<ID" in filters:
            rows = [row for row in rows if _number(row["ID"], 0) < _number(filters["<ID"], 0)]
        if "ID" in filters:
            rows = [row for row in rows if _number(row["ID"], 0) == _number(filters["ID"], 0)]
        if str(order.get("ID", "ASC")).upper() == "DESC":
            rows.reverse()
        start = _number(parameters.get("start"), 0)
        if start < 0:
            # Bitrix disables counting with start=-1: one page, no total and no continuation.
            return {"result": rows[:PAGE]}
        page = rows[start : start + PAGE]
        envelope: dict[str, object] = {"result": page, "total": len(rows)}
        if start + PAGE < len(rows):
            envelope["next"] = start + PAGE
        return envelope


def _identity() -> IdentitySpec:
    return IdentitySpec(
        item_path=("ID",),
        filter_key="ID",
        order_key="ID",
        coercion=IdentityCoercion.EXACT_INTEGER,
    )


def _list_request() -> Request:
    return Request("golden.item.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)


def _client(portal: GoldenPortal) -> Bitrix24:
    return Bitrix24(Settings(webhook_url=WEBHOOK), transport=portal)


async def _drain(stream: object) -> None:
    async with stream:  # type: ignore[attr-defined]
        async for _item in stream:  # type: ignore[attr-defined]
            pass


async def _drain_tolerating(stream: object) -> None:
    # The golden projection records the terminal error class; the scenario itself tolerates it.
    with contextlib.suppress(Exception):
        await _drain(stream)


def _commands(identities: range) -> list[Command[int]]:
    return [
        Command(Request("golden.item.get", {"id": identity}, ReplaySafety.SAFE, route=RouteKind.BARE), identity)
        for identity in identities
    ]


class _StopAfterFirstPage:
    def on_page(self, boundary: PageBoundary) -> CallerStop:
        del boundary
        return CallerStop("first page committed")


async def iter_list_offset(portal: GoldenPortal) -> None:
    """Sequential offset traversal with identity confirms the end with an empty page."""
    async with _client(portal) as client:
        await _drain(client.iter_list(_list_request(), identity=_identity()))


async def iter_list_exact_total(portal: GoldenPortal) -> None:
    """Sequential offset traversal closed by a caller-qualified exact total."""
    async with _client(portal) as client:
        offset = OffsetSpec(total_termination=TotalTermination.EXACT_QUALIFIED)
        await _drain(client.iter_list(_list_request(), identity=_identity(), offset=offset))


async def iter_list_counted(portal: GoldenPortal) -> None:
    """Direct counted head plus a physically batched tail."""
    async with _client(portal) as client:
        await _drain(client.iter_list_counted(_list_request(), identity=_identity(), page_size=PAGE))


async def iter_list_keyset_auto(portal: GoldenPortal) -> None:
    """Keyset traversal with the default automatic execution."""
    async with _client(portal) as client:
        await _drain(
            client.iter_list_keyset(
                _list_request(), selector=ResultSelector.root(), identity=_identity(), keyset=KeysetSpec()
            )
        )


async def iter_list_keyset_sequential(portal: GoldenPortal) -> None:
    """Keyset traversal with explicit sequential execution."""
    async with _client(portal) as client:
        stream = client.iter_list_keyset(
            _list_request(),
            selector=ResultSelector.root(),
            identity=_identity(),
            keyset=KeysetSpec(),
            execution=SequentialKeysetExecution(),
        )
        await _drain(stream)


async def iter_list_cursor(portal: GoldenPortal) -> None:
    """Dependent cursor traversal ends on an empty page."""
    async with _client(portal) as client:
        cursor = CursorSpec(ParameterPath(("LAST_ID",)), ("ID",), IdentityCoercion.EXACT_INTEGER, "ascending", "last")
        request = Request("golden.cursor.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)
        await _drain(client.iter_list_cursor(request, selector=ResultSelector.root(), cursor=cursor))


async def iter_list_page_stop(portal: GoldenPortal) -> None:
    """A caller page-stop policy ends the traversal after the first committed page."""
    async with _client(portal) as client:
        await _drain(client.iter_list(_list_request(), identity=_identity(), page_stop=_StopAfterFirstPage()))


async def iter_list_transport_failure(portal: GoldenPortal) -> None:
    """A dispatch failure after the first page yields an incomplete traversal."""
    portal.fail_list_after = 1
    async with _client(portal) as client:
        await _drain_tolerating(client.iter_list(_list_request(), identity=_identity()))


async def iter_list_early_close(portal: GoldenPortal) -> None:
    """Closing a traversal after one item records an early close."""
    async with _client(portal) as client:
        stream = client.iter_list(_list_request(), identity=_identity())
        async with stream:
            await anext(stream)


async def batch_success(portal: GoldenPortal) -> None:
    """Fail-fast logical batch over successful commands."""
    async with _client(portal) as client:
        await _drain(client.batch(_commands(range(1, 7)), batch_size=4))


async def batch_fail_fast(portal: GoldenPortal) -> None:
    """Fail-fast logical batch stops at the failing command."""
    async with _client(portal) as client:
        await _drain_tolerating(client.batch(_commands(range(1, 12)), batch_size=4))


async def batch_outcomes(portal: GoldenPortal) -> None:
    """Tolerant logical batch yields every outcome including the failure."""
    async with _client(portal) as client:
        await _drain(client.batch_outcomes(_commands(range(1, 12)), batch_size=4))


async def batch_early_close(portal: GoldenPortal) -> None:
    """Closing a logical batch after one outcome accounts the admitted window."""
    async with _client(portal) as client:
        stream = client.batch(_commands(range(1, 12)), batch_size=4)
        async with stream:
            await anext(stream)


async def fan_out_direct(portal: GoldenPortal) -> None:
    """Direct fan-out in input order."""
    async with _client(portal) as client:
        dispatch = DirectDispatch(concurrency=2, output_order=DeliveryOrder.INPUT)
        await _drain(client.fan_out(_commands(range(1, 6)), dispatch=dispatch))


async def fan_out_outcomes_batch(portal: GoldenPortal) -> None:
    """Tolerant batched fan-out in input order, including the failing command."""
    async with _client(portal) as client:
        dispatch = BatchDispatch(batch_size=3, concurrency=1, output_order=DeliveryOrder.INPUT)
        await _drain(client.fan_out_outcomes(_commands(range(1, 12)), dispatch=dispatch))


def _bindings() -> list[Binding[int]]:
    return [
        Binding(f"owner-{owner}", (ParameterUpdate(ParameterPath(("filter", "OWNER")), owner),), owner)
        for owner in (1, 2)
    ]


async def references_sequential(portal: GoldenPortal) -> None:
    """Reference traversal with sequential offset bindings."""
    async with _client(portal) as client:
        stream = client.iter_references(
            _list_request(),
            _bindings(),
            traversal=SequentialTraversal(identity=_identity()),
            dispatch=DirectDispatch(concurrency=2, output_order=DeliveryOrder.INPUT),
        )
        await _drain(stream)


async def references_counted(portal: GoldenPortal) -> None:
    """Reference traversal with counted bindings over physical batches."""
    async with _client(portal) as client:
        stream = client.iter_reference_outcomes(
            _list_request(),
            _bindings(),
            traversal=CountedTraversal(identity=_identity()),
            dispatch=BatchDispatch(batch_size=10, concurrency=1, output_order=DeliveryOrder.INPUT),
        )
        await _drain(stream)


SCENARIOS: dict[str, Callable[[GoldenPortal], Awaitable[None]]] = {
    function.__name__: function
    for function in (
        iter_list_offset,
        iter_list_exact_total,
        iter_list_counted,
        iter_list_keyset_auto,
        iter_list_keyset_sequential,
        iter_list_cursor,
        iter_list_page_stop,
        iter_list_transport_failure,
        iter_list_early_close,
        batch_success,
        batch_fail_fast,
        batch_outcomes,
        batch_early_close,
        fan_out_direct,
        fan_out_outcomes_batch,
        references_sequential,
        references_counted,
    )
}
