"""#24: a cursor page order independent of the boundary direction, and a response-flag terminal (A24)."""

from __future__ import annotations
from typing import TYPE_CHECKING

import pytest

from b24api import (
    Binding,
    CapabilityError,
    CursorSpec,
    CursorTraversal,
    DirectDispatch,
    ExecutionPolicy,
    IdentityCoercion,
    IncompleteTraversalError,
    PaginationError,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    ResultSelector,
    TraversalAssurance,
)
from b24api.contracts import (
    BindingClosure,
    ConsistencyPolicy,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ResponseFlagTerminal,
)
from b24api.contracts.policy import OrderSemantics
from b24api.errors import ContinuationError, ContinuationReason
from b24api.testing.scripted import ScriptedExchange, ScriptedTransport

if TYPE_CHECKING:
    from b24api import Bitrix24
    from b24api.contracts import JsonValue, OperationStream, ReferenceOutcome
    from tests.scripting import ClientFactory

_METHOD = "imopenlines.v2.Session.getHistory"
_HAS_PREV = ResponseFlagTerminal(ResultSelector(("hasPrevPage",)))
_FLAG_TERMINAL = "response flag declared terminal page"


def _request(last_id: int, **extra: object) -> Request:
    return Request.bare(_METHOD, {**extra, "lastId": last_id, "limit": 2}, ReplaySafety.SAFE)


def _page(ids: list[int], *, more: bool | None = None) -> dict[str, object]:
    result: dict[str, object] = {"messages": [{"id": value} for value in ids]}
    if more is not None:
        result["hasPrevPage"] = more
    return {"result": result}


def _cursor(*, flag: ResponseFlagTerminal | None = _HAS_PREV) -> CursorSpec:
    return CursorSpec(
        ParameterPath(("lastId",)),
        ("id",),
        IdentityCoercion.EXACT_INTEGER,
        "descending",
        "first",
        ParameterPath(("limit",)),
        allow_create_controls=False,
        page_order="ascending",
        continuation=flag,
    )


def _flat(
    client_factory: ClientFactory, exchanges: tuple[ScriptedExchange, ...], **kwargs: object
) -> tuple[ScriptedTransport, Bitrix24]:
    transport = ScriptedTransport(exchanges)
    client = client_factory(transport, **kwargs)
    return transport, client


@pytest.mark.asyncio
async def test_a24_ascending_pages_descending_boundary_flag_terminal_flat(scripted_client: ClientFactory) -> None:
    """A24: `[3,4]` then `[1,2]` with hasPrevPage=false is delivered as `[3,4,1,2]` in two HTTP requests."""
    transport, client = _flat(
        scripted_client,
        (
            ScriptedExchange.json(_request(0), _page([3, 4], more=True)),
            ScriptedExchange.json(_request(3), _page([1, 2], more=False)),
        ),
    )
    stream = client.iter_list_cursor(_request(0), selector=ResultSelector(("messages",)), cursor=_cursor(), page_size=2)
    async with stream:
        rows = [_row_id(row) async for row in stream]
    assert rows == [3, 4, 1, 2]
    transport.assert_exhausted()
    assert [call.copy_parameters()["lastId"] for call in transport.calls] == [0, 3]
    report = stream.report
    assert report is not None
    assert report.exhausted
    assert report.terminal_reason == _FLAG_TERMINAL
    assert report.assurance is TraversalAssurance.MECHANICS_ONLY


@pytest.mark.asyncio
async def test_a24_page_order_without_flag_keeps_the_empty_confirmation(scripted_client: ClientFactory) -> None:
    """A24: without a declared flag the reversed page order still ends on an empty confirmation page."""
    transport, client = _flat(
        scripted_client,
        (
            ScriptedExchange.json(_request(0), _page([3, 4])),
            ScriptedExchange.json(_request(3), _page([1, 2])),
            ScriptedExchange.json(_request(1), _page([])),
        ),
    )
    stream = client.iter_list_cursor(
        _request(0), selector=ResultSelector(("messages",)), cursor=_cursor(flag=None), page_size=2
    )
    async with stream:
        rows = [_row_id(row) async for row in stream]
    assert rows == [3, 4, 1, 2]
    transport.assert_exhausted()
    assert stream.report is not None
    assert stream.report.exhausted
    assert stream.report.assurance is TraversalAssurance.IDENTITY_EXACT


@pytest.mark.asyncio
async def test_a24_two_references_with_overlapping_ids_each_close_on_their_flag(
    scripted_client: ClientFactory,
) -> None:
    """A24: two reference parents with the same message IDs keep separate boundaries and terminals."""
    exchanges = tuple(
        exchange
        for session in (10, 20)
        for exchange in (
            ScriptedExchange.json(_request(0, sessionId=session), _page([3, 4], more=True)),
            ScriptedExchange.json(_request(3, sessionId=session), _page([1, 2], more=False)),
        )
    )
    transport, client = _flat(scripted_client, exchanges)
    bindings = [
        Binding(f"session {session}", (ParameterUpdate(ParameterPath(("sessionId",)), session),), session)
        for session in (10, 20)
    ]
    stream = client.iter_reference_outcomes(
        _request(0),
        bindings,
        traversal=CursorTraversal(ResultSelector(("messages",)), _cursor(), page_size=2),
        dispatch=DirectDispatch(),
    )
    async with stream:
        outcomes: list[ReferenceOutcome[int]] = [outcome async for outcome in stream]
    transport.assert_exhausted()
    for session in (10, 20):
        rows = [
            _row_id(outcome.item)
            for outcome in outcomes
            if isinstance(outcome, ReferenceItem) and outcome.correlation == session
        ]
        assert rows == [3, 4, 1, 2]
    completes = [outcome for outcome in outcomes if isinstance(outcome, ReferenceComplete)]
    assert len(completes) == len(bindings)
    assert all(outcome.exhausted for outcome in completes)
    assert {outcome.closure for outcome in completes} == {BindingClosure.DECLARED_RESPONSE_TERMINAL}
    assert stream.report is not None
    assert stream.report.exhausted
    assert stream.report.assurance is TraversalAssurance.MECHANICS_ONLY


@pytest.mark.asyncio
@pytest.mark.parametrize("order", [OrderSemantics.ASCENDING, OrderSemantics.DESCENDING])
async def test_a24_explicit_global_order_policy_rejects_before_http(
    order: OrderSemantics, scripted_client: ClientFactory
) -> None:
    """A24: an explicit global ASC/DESC policy cannot verify a reversed page order: CapabilityError before HTTP."""
    policy = ExecutionPolicy(consistency=ConsistencyPolicy(order_semantics=order))
    transport, client = _flat(scripted_client, ())
    stream = client.iter_list_cursor(
        _request(0), selector=ResultSelector(("messages",)), cursor=_cursor(), page_size=2, policy=policy
    )
    with pytest.raises(CapabilityError, match="global row order"):
        async with stream:
            _ = [row async for row in stream]
    with pytest.raises(CapabilityError, match="global row order"):
        client.iter_reference_outcomes(
            _request(0),
            [Binding("one", (), 1)],
            traversal=CursorTraversal(ResultSelector(("messages",)), _cursor(), page_size=2),
            dispatch=DirectDispatch(),
            policy=policy,
        )
    assert transport.calls == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "second_page",
    [
        pytest.param([1, 5], id="value-above-bound-3"),
        pytest.param([2, 2], id="duplicate"),
        pytest.param([2, 1], id="bad-page-order"),
        pytest.param([3], id="repeated-boundary"),
    ],
)
async def test_a24_invalid_second_page_is_rejected_before_emission(
    second_page: list[int], scripted_client: ClientFactory
) -> None:
    """A24: every second-page value is checked against bound 3; a bad page fails closed before any row of it."""
    transport, client = _flat(
        scripted_client,
        (
            ScriptedExchange.json(_request(0), _page([3, 4], more=True)),
            ScriptedExchange.json(_request(3), _page(second_page, more=False)),
        ),
    )
    stream = client.iter_list_cursor(_request(0), selector=ResultSelector(("messages",)), cursor=_cursor(), page_size=2)
    rows: list[int] = []
    with pytest.raises(IncompleteTraversalError) as caught:
        await _collect_ids(stream, rows)
    assert rows == [3, 4]
    assert isinstance(caught.value.error, PaginationError)
    transport.assert_exhausted()
    assert stream.report is not None
    assert not stream.report.exhausted


def _row_id(row: JsonValue) -> int:
    assert isinstance(row, dict)
    value = row["id"]
    assert isinstance(value, int)
    return value


async def _collect_ids(stream: OperationStream[JsonValue], rows: list[int]) -> None:
    """Append each delivered row ID, keeping the prefix delivered before a failure."""
    async with stream:
        async for row in stream:
            rows.append(_row_id(row))  # noqa: PERF401 - keep the delivered prefix


@pytest.mark.asyncio
async def test_a24_first_page_out_of_declared_order_emits_nothing(scripted_client: ClientFactory) -> None:
    """A24: a first page against its declared ascending order is rejected before emission."""
    transport, client = _flat(scripted_client, (ScriptedExchange.json(_request(0), _page([4, 3], more=True)),))
    stream = client.iter_list_cursor(_request(0), selector=ResultSelector(("messages",)), cursor=_cursor(), page_size=2)
    rows: list[int] = []
    with pytest.raises(IncompleteTraversalError):
        await _collect_ids(stream, rows)
    assert rows == []
    transport.assert_exhausted()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        pytest.param(_page([3, 4]), ContinuationReason.MISSING_CONTINUATION, id="missing-flag"),
        pytest.param(
            {"result": {"messages": [{"id": 3}], "hasPrevPage": "Y"}},
            ContinuationReason.MISSING_CONTINUATION,
            id="non-boolean-flag",
        ),
        pytest.param(_page([], more=True), ContinuationReason.MISSING_CONTINUATION, id="more-without-rows"),
    ],
)
async def test_a24_declared_flag_is_required_and_boolean(
    payload: dict[str, object], reason: ContinuationReason, scripted_client: ClientFactory
) -> None:
    """A24: a declared flag must be present and boolean; contradictory terminal data fails closed."""
    transport, client = _flat(scripted_client, (ScriptedExchange.json(_request(0), payload),))
    stream = client.iter_list_cursor(_request(0), selector=ResultSelector(("messages",)), cursor=_cursor(), page_size=2)
    with pytest.raises(IncompleteTraversalError) as caught:
        async with stream:
            _ = [row async for row in stream]
    assert isinstance(caught.value.error, ContinuationError)
    assert caught.value.error.reason is reason
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a24_tolerant_reference_failure_does_not_end_the_neighbour(scripted_client: ClientFactory) -> None:
    """A24: in tolerant mode one parent's rejected page fails only that parent."""
    exchanges = (
        ScriptedExchange.json(_request(0, sessionId=10), _page([3, 4], more=True)),
        ScriptedExchange.json(_request(3, sessionId=10), _page([1, 2], more=False)),
        ScriptedExchange.json(_request(0, sessionId=20), _page([3, 4], more=True)),
        ScriptedExchange.json(_request(3, sessionId=20), _page([5], more=False)),
    )
    transport, client = _flat(scripted_client, exchanges)
    bindings = [
        Binding(f"session {session}", (ParameterUpdate(ParameterPath(("sessionId",)), session),), session)
        for session in (10, 20)
    ]
    stream = client.iter_reference_outcomes(
        _request(0),
        bindings,
        traversal=CursorTraversal(ResultSelector(("messages",)), _cursor(), page_size=2),
        dispatch=DirectDispatch(),
    )
    async with stream:
        outcomes = [outcome async for outcome in stream]
    transport.assert_exhausted()
    complete = [outcome for outcome in outcomes if isinstance(outcome, ReferenceComplete)]
    failed = [outcome for outcome in outcomes if isinstance(outcome, ReferenceFailure)]
    assert [outcome.correlation for outcome in complete] == [10]
    assert [outcome.correlation for outcome in failed] == [20]
    assert failed[0].partial_rows == len(_page([3, 4])["result"]["messages"])  # type: ignore[index]
    assert stream.report is not None
    assert not stream.report.exhausted


def test_a24_reversed_page_order_requires_first_take_and_keeps_old_defaults() -> None:
    """A24: a page order against the direction needs take='first'; omitted fields keep the old contract."""
    with pytest.raises(ValueError, match="take='first'"):
        CursorSpec(
            ParameterPath(("lastId",)),
            ("id",),
            IdentityCoercion.EXACT_INTEGER,
            "descending",
            "last",
            page_order="ascending",
        )
    legacy = CursorSpec(ParameterPath(("lastId",)), ("id",), IdentityCoercion.EXACT_INTEGER, "descending", "last")
    assert legacy.page_order is None
    assert legacy.continuation is None
    with pytest.raises(TypeError):
        CursorSpec(
            ParameterPath(("lastId",)),
            ("id",),
            IdentityCoercion.EXACT_INTEGER,
            "descending",
            "first",
            None,
            True,  # noqa: FBT003 - positional field order is part of the pinned contract
            continuation=ResultSelector(("hasPrevPage",)),  # type: ignore[arg-type]
        )
