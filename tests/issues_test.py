"""Bounded sampled-issue traceability through the public v2 surface."""

from __future__ import annotations
import json
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlsplit

import pytest

from b24api import (
    Binding,
    Bitrix24,
    Command,
    CommandFailure,
    CommandNotExecuted,
    CommandOutcomeUnknown,
    CommandSuccess,
    CursorSpec,
    DeliveryOrder,
    DirectDispatch,
    IdentitySpec,
    OffsetSpec,
    ParameterPath,
    ParameterUpdate,
    ReferenceComplete,
    ReferenceItem,
    ReplaySafety,
    Request,
    ResultSelector,
    SequentialTraversal,
    Settings,
    TerminalState,
    TraversalAssurance,
)
from b24api.contracts import IdentityCoercion
from b24api.errors import (
    AmbiguousExecutionError,
    B24ApiError,
    FailurePhase,
    IncompleteTraversalError,
    InputSourceError,
    TransportError,
)
from b24api.execution import WireResponse

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from b24api.contracts import JsonValue

_HTTP_OK = 200
_PAGE = 50
_EXPECTED_OWNER = 10
_EXPECTED_ROWS = 2


class FixtureTransport:
    """Credential-free method-agnostic response fixture."""

    host = "bitrix24.com"

    def __init__(self, handler: Callable[[Request], object]) -> None:
        """Store the deterministic response handler."""
        self.handler = handler
        self.requests: list[Request] = []

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Return or raise the handler's deterministic outcome."""
        assert attempt_timeout > 0
        self.requests.append(request)
        outcome = self.handler(request)
        if isinstance(outcome, BaseException):
            raise outcome
        body = json.dumps(outcome, separators=(",", ":")).encode()
        assert len(body) <= max_response_bytes
        return WireResponse(_HTTP_OK, (("content-type", "application/json"),), body)

    async def aclose(self) -> None:
        """Caller-owned fixture has no resources."""


def _client(transport: FixtureTransport) -> Bitrix24:
    return Bitrix24(Settings(webhook_url="https://bitrix24.com/rest/0/test/"), transport=transport)


def _identity(
    *,
    item_path: tuple[str | int, ...] = ("ID",),
    filter_key: str = "ID",
    order_key: str = "ID",
) -> IdentitySpec:
    return IdentitySpec(
        item_path=item_path,
        filter_key=filter_key,
        order_key=order_key,
        coercion=IdentityCoercion.EXACT_INTEGER,
    )


@pytest.mark.asyncio
async def test_issue_b1_tolerant_batch_preserves_all_correlated_states() -> None:
    correlations = {name: ["profile-correlation", name] for name in ("success", "failure", "not", "unknown")}

    def mixed_handler(request: Request) -> object:
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        keys = list(commands)
        assert "profile-correlation" not in json.dumps(request.copy_parameters())
        return {
            "result": {
                "result": {keys[0]: {"ok": True}},
                "result_error": {keys[1]: {"error": "denied", "error_description": "no access"}},
            },
        }

    mixed = _client(FixtureTransport(mixed_handler)).batch_outcomes(
        [
            Command(Request("sample.get", replay_safety=ReplaySafety.SAFE), correlations["success"]),
            Command(Request("sample.get", replay_safety=ReplaySafety.SAFE), correlations["failure"]),
        ],
    )
    mixed_outcomes = [outcome async for outcome in mixed]

    async def broken_source() -> AsyncGenerator[Command[list[str]]]:
        yield Command(Request("sample.get", replay_safety=ReplaySafety.SAFE), correlations["not"])
        raise RuntimeError("source failed")

    not_executed = _client(
        FixtureTransport(lambda _request: pytest.fail("partial source must not dispatch")),
    ).batch_outcomes(
        broken_source(),
        batch_size=2,
    )
    not_outcomes = [await anext(not_executed)]
    with pytest.raises(InputSourceError):
        await anext(not_executed)

    ambiguous = TransportError("response lost", phase=FailurePhase.DISPATCH_STARTED)
    unknown_transport = FixtureTransport(lambda _request: ambiguous)
    unknown_stream = _client(unknown_transport).batch_outcomes(
        [Command(Request("sample.add", replay_safety=ReplaySafety.UNKNOWN), correlations["unknown"])],
    )
    unknown_outcomes = [outcome async for outcome in unknown_stream]

    outcomes = (*mixed_outcomes, *not_outcomes, *unknown_outcomes)
    assert [type(outcome) for outcome in outcomes] == [
        CommandSuccess,
        CommandFailure,
        CommandNotExecuted,
        CommandOutcomeUnknown,
    ]
    assert all(outcome.correlation is correlations[name] for outcome, name in zip(outcomes, correlations, strict=True))


@pytest.mark.asyncio
async def test_issue_b2_unknown_write_is_not_replayed_after_dispatch() -> None:
    transport = FixtureTransport(
        lambda _request: TransportError("response lost", phase=FailurePhase.DISPATCH_STARTED),
    )

    with pytest.raises(AmbiguousExecutionError):
        await _client(transport).call(Request("sample.add", replay_safety=ReplaySafety.UNKNOWN))

    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_issue_b3_limit_conflict_rejects_before_network() -> None:
    transport = FixtureTransport(lambda _request: {"result": []})
    stream = _client(transport).iter_list(
        Request("sample.list", {"LIMIT": 99}, ReplaySafety.SAFE),
        page_size=_PAGE,
        offset=OffsetSpec(limit_path=ParameterPath(("LIMIT",))),
    )

    with pytest.raises(B24ApiError, match="conflict"):
        await anext(stream)

    assert transport.requests == []


@pytest.mark.asyncio
async def test_issue_b4_async_binding_correlation_and_selector() -> None:
    correlations = [["profile-correlation", owner] for owner in (1, 2)]
    source_closed = False

    async def bindings() -> AsyncGenerator[Binding[list[object]]]:
        nonlocal source_closed
        try:
            for owner in (1, 2):
                yield Binding(
                    f"owner-{owner}",
                    (ParameterUpdate(ParameterPath(("filter", "OWNER")), owner),),
                    correlations[owner - 1],
                )
        finally:
            source_closed = True

    def handler(request: Request) -> object:
        parameters = request.copy_parameters()
        assert "profile-correlation" not in json.dumps(parameters)
        filters = parameters["filter"]
        assert isinstance(filters, dict)
        owner = filters["OWNER"]
        assert isinstance(owner, int)
        assert not isinstance(owner, bool)
        return {"result": {"items": [] if parameters["start"] else [{"ID": owner}]}}

    transport = FixtureTransport(handler)
    stream = _client(transport).iter_references(
        Request("sample.list", {"filter": {"OWNER": 0}}, ReplaySafety.SAFE),
        bindings(),
        traversal=SequentialTraversal(selector=ResultSelector(("items",)), identity=_identity()),
        dispatch=DirectDispatch(concurrency=2, output_order=DeliveryOrder.INPUT),
    )
    events = [event async for event in stream]

    assert source_closed
    assert [type(event) for event in events] == [ReferenceItem, ReferenceComplete] * 2
    assert all(event.correlation is correlations[event.binding_index] for event in events)


@pytest.mark.asyncio
async def test_issue_b5_counted_stride_uses_observed_head_width() -> None:
    identities = tuple(range(1, 6))
    observed_starts: list[int] = []

    def page(start: int) -> list[dict[str, int]]:
        return [{"ID": value} for value in identities[start : start + 2]]

    def handler(request: Request) -> object:
        if request.method == "sample.list":
            return {"result": page(0), "total": len(identities), "next": 2}
        commands = request.copy_parameters()["cmd"]
        assert isinstance(commands, dict)
        results: dict[str, object] = {}
        totals: dict[str, int] = {}
        continuations: dict[str, int] = {}
        for key, encoded in commands.items():
            assert isinstance(encoded, str)
            start = int(parse_qs(urlsplit(encoded).query)["start"][0])
            observed_starts.append(start)
            results[key] = page(start)
            totals[key] = len(identities)
            if start + 2 < len(identities):
                continuations[key] = start + 2
        return {
            "result": {
                "result": results,
                "result_error": {},
                "result_total": totals,
                "result_next": continuations,
            },
        }

    stream = _client(FixtureTransport(handler)).iter_list_counted(
        Request("sample.list", replay_safety=ReplaySafety.SAFE),
        identity=_identity(),
        page_size=_PAGE,
    )

    assert [row async for row in stream] == [{"ID": value} for value in identities]
    assert observed_starts == [2, 4]


@pytest.mark.asyncio
async def test_issue_c1b_distinct_item_filter_order_paths_are_exact() -> None:
    sent: list[dict[str, JsonValue]] = []

    def handler(request: Request) -> object:
        parameters = request.copy_parameters()
        sent.append(parameters)
        filters = parameters.get("filter", {})
        assert isinstance(filters, dict)
        raw_boundary = filters.get(">ID", 0)
        assert isinstance(raw_boundary, int)
        assert not isinstance(raw_boundary, bool)
        boundary = raw_boundary
        rows = [{"id": value} for value in (1, 2) if value > boundary]
        return {"result": rows[:1]}

    stream = _client(FixtureTransport(handler)).iter_list_keyset(
        Request("sample.list", replay_safety=ReplaySafety.SAFE),
        selector=ResultSelector.root(),
        identity=_identity(item_path=("id",), filter_key="ID", order_key="id"),
        page_size=1,
    )

    assert [row async for row in stream] == [{"id": 1}, {"id": 2}]
    assert sent[0]["order"] == {"id": "ASC"}
    assert sent[1]["filter"] == {">ID": 1}


@pytest.mark.asyncio
async def test_issue_c5_repeated_cursor_boundary_is_not_reported_complete() -> None:
    transport = FixtureTransport(lambda _request: {"result": [{"ID": 1}, {"ID": 1}]})
    stream = _client(transport).iter_list_cursor(
        Request("sample.list", replay_safety=ReplaySafety.SAFE),
        selector=ResultSelector.root(),
        cursor=CursorSpec(
            ParameterPath(("LAST_ID",)),
            ("ID",),
            IdentityCoercion.EXACT_INTEGER,
            "ascending",
            "last",
        ),
    )

    with pytest.raises(IncompleteTraversalError):
        await anext(stream)

    assert stream.report is not None
    assert stream.report.state is TerminalState.INCOMPLETE


@pytest.mark.asyncio
async def test_issue_c34_ignored_filter_requires_application_reconciliation() -> None:
    pages = [[{"ID": 1, "owner": 10}, {"ID": 2, "owner": 20}], []]
    stream = _client(FixtureTransport(lambda _request: {"result": pages.pop(0)})).iter_list(
        Request("sample.list", {"filter": {"owner": 10}}, ReplaySafety.SAFE),
        identity=_identity(),
    )
    rows = [row async for row in stream]
    unexpected = [row for row in rows if isinstance(row, dict) and row.get("owner") != _EXPECTED_OWNER]

    assert unexpected == [{"ID": 2, "owner": 20}]
    assert stream.report is not None
    assert stream.report.assurance is TraversalAssurance.IDENTITY_EXACT
    assert all(violation.code != "business_filter_verified" for violation in stream.report.violations)


@pytest.mark.asyncio
async def test_issue_c35_overmatched_multifield_is_not_claimed_verified() -> None:
    pages = [[{"ID": 1, "email": "a@example.invalid"}, {"ID": 2, "phone": "100"}], []]
    stream = _client(FixtureTransport(lambda _request: {"result": pages.pop(0)})).iter_list(
        Request("sample.list", {"filter": {"has_email": True, "has_phone": True}}, ReplaySafety.SAFE),
        identity=_identity(),
    )
    rows = [row async for row in stream]
    application_matches = [row for row in rows if isinstance(row, dict) and "email" in row and "phone" in row]

    assert application_matches == []
    assert len(rows) == _EXPECTED_ROWS
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    assert all(violation.code != "business_filter_verified" for violation in stream.report.violations)
