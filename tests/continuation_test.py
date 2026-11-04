"""#22: pure caller continuation rules over SDK-owned controls (A22-A, A22-B, A22-END, A22-N)."""

from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from b24api import (
    Binding,
    CapabilityError,
    DirectDispatch,
    IdentityCoercion,
    IdentitySpec,
    IncompleteTraversalError,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    TraversalAssurance,
)
from b24api.completion import CompletionGate
from b24api.contracts import (
    BindingClosure,
    CompositeIdentitySpec,
    ContinuationTraversal,
    ContinueWith,
    EndOfSource,
    IdentityComponent,
    NotExecutedReason,
    PageView,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    ReferenceNotExecuted,
    ResponseCursorRule,
    ResponseFlagTerminal,
)
from b24api.contracts.completion import (
    BindingAdmitted,
    BindingTerminal,
    CleanupOutcome,
    CleanupState,
    CommandSettlement,
    PageAcknowledged,
    PageCommandOutcome,
    PageDelivered,
    PageScheduled,
    PageValidated,
    StreamClosure,
    StreamTerminal,
)
from b24api.contracts.report import TerminalState
from b24api.errors import ContinuationError, ContinuationReason
from b24api.testing.scripted import ScriptedExchange, ScriptedTransport
from b24api.traversal.continuation import CONTROL_VECTOR_MAXIMUM_BYTES

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.scripting import ClientFactory

_RULE_END = "continuation rule declared end of source"
_CURSOR = {"startDate": "2026-02-01T09:00:00.123456+00:00", "id": 1}
_AFTER = ParameterPath(("pagination", "afterCursor"))
_LAST_ID = ParameterPath(("LAST_ID",))
_LAST_TYPE = ParameterPath(("LAST_TYPE",))
_ID = IdentitySpec(("id",), "id", "id", IdentityCoercion.EXACT_INTEGER)


def _followup(after: object = None, *, route: RouteKind = RouteKind.API_V3, **extra: object) -> Request:
    pagination: dict[str, object] = {"limit": 1}
    if after is not None:
        pagination["afterCursor"] = after
    return Request(
        "call.followup.list",
        {**extra, "order": {"startDate": "asc"}, "pagination": pagination},
        ReplaySafety.SAFE,
        route=route,
    )


def _followup_traversal() -> ContinuationTraversal:
    return ContinuationTraversal(
        ResultSelector(("items",)),
        IdentitySpec(("callId",), "callId", "callId", IdentityCoercion.EXACT_INTEGER),
        (_AFTER,),
        ResponseCursorRule(
            ResultSelector(("afterCursor",)), _AFTER, ResponseFlagTerminal(ResultSelector(("hasMore",)))
        ),
        limit_path=ParameterPath(("pagination", "limit")),
        page_size=1,
    )


def _items(call_id: int, *, more: bool, cursor: object) -> dict[str, object]:
    return {"result": {"items": [{"callId": call_id}], "hasMore": more, "afterCursor": cursor}}


def _field(row: object, key: str) -> object:
    assert isinstance(row, dict)
    return row[key]


async def _drain(stream: object) -> tuple[list[object], BaseException | None]:
    rows: list[object] = []
    failure: BaseException | None = None
    try:
        async with stream:  # type: ignore[attr-defined]
            async for row in stream:  # type: ignore[attr-defined]
                rows.append(row)  # noqa: PERF401 - keep the prefix delivered before a failure
    except Exception as error:  # noqa: BLE001 - the test inspects the typed failure
        failure = error
    return rows, failure


@pytest.mark.asyncio
async def test_a22_a_response_object_cursor_is_carried_whole_and_rule_ends_nonempty_page(
    scripted_client: ClientFactory,
) -> None:
    """A22-A: the whole `.123456` object cursor goes into the second request; 2 rows, 2 HTTP, RULE_DECLARED_END."""
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(_followup(), _items(1, more=True, cursor=_CURSOR)),
            ScriptedExchange.json(_followup(_CURSOR), _items(2, more=False, cursor=None)),
        )
    )
    stream = scripted_client(transport).iter_list_continuation(_followup(), traversal=_followup_traversal())
    rows, failure = await _drain(stream)
    assert failure is None
    assert [row["callId"] for row in rows] == [1, 2]  # type: ignore[index]
    transport.assert_exhausted()
    assert transport.calls[1].copy_parameters()["pagination"] == {"limit": 1, "afterCursor": _CURSOR}
    report = stream.report
    assert report is not None
    assert report.operation == "iter_list_continuation"
    assert report.exhausted
    assert report.terminal_reason == _RULE_END
    assert report.physical_requests == len(transport.calls)
    assert report.assurance is TraversalAssurance.MECHANICS_ONLY


@pytest.mark.asyncio
async def test_a22_a_two_references_carry_their_own_object_cursors(scripted_client: ClientFactory) -> None:
    """A22-A: two reference parents keep independent cursors and each closes with RULE_DECLARED_END."""
    other = {"startDate": "2026-03-01T00:00:00.000001+00:00", "id": 9}
    exchanges = []
    for owner, cursor in ((7, _CURSOR), (8, other)):
        exchanges.append(
            ScriptedExchange.json(
                _followup(route=RouteKind.BARE, ownerId=owner), _items(owner * 10, more=True, cursor=cursor)
            )
        )
        exchanges.append(
            ScriptedExchange.json(
                _followup(cursor, route=RouteKind.BARE, ownerId=owner), _items(owner * 10 + 1, more=False, cursor=None)
            )
        )
    transport = ScriptedTransport(tuple(exchanges))
    bindings = [
        Binding(f"owner {owner}", (ParameterUpdate(ParameterPath(("ownerId",)), owner),), owner) for owner in (7, 8)
    ]
    stream = scripted_client(transport).iter_reference_outcomes(
        _followup(route=RouteKind.BARE), bindings, traversal=_followup_traversal(), dispatch=DirectDispatch()
    )
    outcomes, failure = await _drain(stream)
    assert failure is None
    transport.assert_exhausted()
    for owner in (7, 8):
        assert [
            _field(outcome.item, "callId")
            for outcome in outcomes
            if isinstance(outcome, ReferenceItem) and outcome.correlation == owner
        ] == [owner * 10, owner * 10 + 1]
    completes = [outcome for outcome in outcomes if isinstance(outcome, ReferenceComplete)]
    assert {outcome.closure for outcome in completes} == {BindingClosure.RULE_DECLARED_END}
    assert all(outcome.exhausted for outcome in completes)
    assert stream.report is not None
    assert stream.report.exhausted


@dataclass(frozen=True)
class _NotificationRule:
    """Pure caller rule: LAST_TYPE is 1 after a confirmation (type 1) and 3 after any other type."""

    def evaluate(self, page: PageView, /) -> ContinueWith | EndOfSource:
        if not page.items:
            return EndOfSource()
        last = page.items[-1]
        notify_type = last["notify_type"]  # type: ignore[index, call-overload]
        return ContinueWith(
            (
                ParameterUpdate(_LAST_ID, last["id"]),  # type: ignore[index, call-overload]
                ParameterUpdate(_LAST_TYPE, 1 if notify_type == 1 else 3),
            )
        )


def _notify(**controls: object) -> Request:
    return Request.bare("im.notify.get", {"LIMIT": 1, **controls}, ReplaySafety.SAFE)


def _notifications(*rows: tuple[int, int]) -> dict[str, object]:
    return {"result": {"notifications": [{"id": row_id, "notify_type": kind} for row_id, kind in rows]}}


@pytest.mark.asyncio
async def test_a22_b_paired_notification_controls_follow_the_phase_change(scripted_client: ClientFactory) -> None:
    """A22-B: 5/type1 -> 20/type2 -> empty sends LAST_ID+LAST_TYPE pairs; a rising ID across phases is legal."""
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(_notify(), _notifications((5, 1))),
            ScriptedExchange.json(_notify(LAST_ID=5, LAST_TYPE=1), _notifications((20, 2))),
            ScriptedExchange.json(_notify(LAST_ID=20, LAST_TYPE=3), _notifications()),
        )
    )
    traversal = ContinuationTraversal(
        ResultSelector(("notifications",)),
        _ID,
        (_LAST_ID, _LAST_TYPE),
        _NotificationRule(),
        limit_path=ParameterPath(("LIMIT",)),
        page_size=1,
    )
    stream = scripted_client(transport).iter_list_continuation(_notify(), traversal=traversal)
    rows, failure = await _drain(stream)
    assert failure is None
    assert [row["id"] for row in rows] == [5, 20]  # type: ignore[index]
    transport.assert_exhausted()
    assert stream.report is not None
    assert stream.report.exhausted
    assert stream.report.terminal_reason == _RULE_END


def _sticker(pack_id: int, pack_type: str) -> Request:
    return Request.bare(
        "im.v2.Sticker.Pack.tail", {"id": pack_id, "packType": pack_type, "limit": 1}, ReplaySafety.SAFE
    )


def _packs(pack_id: int, pack_type: str, *, more: bool) -> dict[str, object]:
    return {"result": {"packs": [{"id": pack_id, "type": pack_type}], "hasNextPage": more}}


@dataclass(frozen=True)
class _StickerRule:
    """Pure caller rule: continue from the last pack's (id, type) while hasNextPage is true."""

    def evaluate(self, page: PageView, /) -> ContinueWith | EndOfSource:
        if not page.result["hasNextPage"]:  # type: ignore[index, call-overload]
            return EndOfSource()
        last = page.items[-1]
        return ContinueWith(
            (
                ParameterUpdate(ParameterPath(("id",)), last["id"]),  # type: ignore[index, call-overload]
                ParameterUpdate(ParameterPath(("packType",)), last["type"]),  # type: ignore[index, call-overload]
            )
        )


def _sticker_traversal() -> ContinuationTraversal:
    return ContinuationTraversal(
        ResultSelector(("packs",)),
        CompositeIdentitySpec(
            (
                IdentityComponent(("type",), IdentityCoercion.EXACT_STRING, "type"),
                IdentityComponent(("id",), IdentityCoercion.EXACT_INTEGER, "id"),
            )
        ),
        (ParameterPath(("id",)), ParameterPath(("packType",))),
        _StickerRule(),
        limit_path=ParameterPath(("limit",)),
        allow_create_controls=False,
        page_size=1,
    )


def _sticker_exchanges() -> tuple[ScriptedExchange, ...]:
    return (
        ScriptedExchange.json(_sticker(0, "custom"), _packs(40, "custom", more=True)),
        ScriptedExchange.json(_sticker(40, "custom"), _packs(7, "custom", more=True)),
        ScriptedExchange.json(_sticker(7, "custom"), _packs(40, "vendor", more=True)),
        ScriptedExchange.json(_sticker(40, "vendor"), _packs(11, "vendor", more=False)),
    )


@pytest.mark.asyncio
async def test_a22_b_sticker_namespaces_keep_composite_identity_and_no_id_monotonicity(
    scripted_client: ClientFactory,
) -> None:
    """A22-B: (custom,40),(custom,7),(vendor,40),(vendor,11); ID 40 is legal in both namespaces."""
    transport = ScriptedTransport(_sticker_exchanges())
    stream = scripted_client(transport).iter_list_continuation(_sticker(0, "custom"), traversal=_sticker_traversal())
    rows, failure = await _drain(stream)
    assert failure is None
    assert [(row["type"], row["id"]) for row in rows] == [  # type: ignore[index]
        ("custom", 40),
        ("custom", 7),
        ("vendor", 40),
        ("vendor", 11),
    ]
    transport.assert_exhausted()
    assert stream.report is not None
    assert stream.report.exhausted
    assert stream.report.unique_rows == len(rows)


@pytest.mark.asyncio
async def test_a22_end_end_of_source_on_nonempty_and_empty_pages_flat(scripted_client: ClientFactory) -> None:
    """A22-END: EndOfSource on a non-empty page delivers it and sends nothing more; on an empty page too."""
    nonempty = ScriptedTransport((ScriptedExchange.json(_sticker(0, "custom"), _packs(40, "custom", more=False)),))
    stream = scripted_client(nonempty).iter_list_continuation(_sticker(0, "custom"), traversal=_sticker_traversal())
    rows, failure = await _drain(stream)
    assert failure is None
    assert len(rows) == 1
    nonempty.assert_exhausted()
    assert stream.report is not None
    assert stream.report.exhausted
    assert stream.report.terminal_reason == _RULE_END
    empty = ScriptedTransport((ScriptedExchange.json(_notify(), _notifications()),))
    traversal = ContinuationTraversal(
        ResultSelector(("notifications",)), _ID, (_LAST_ID, _LAST_TYPE), _NotificationRule()
    )
    stream = scripted_client(empty).iter_list_continuation(_notify(), traversal=traversal)
    rows, failure = await _drain(stream)
    assert failure is None
    assert rows == []
    empty.assert_exhausted()
    assert stream.report is not None
    assert stream.report.exhausted
    assert stream.report.terminal_reason == _RULE_END


@pytest.mark.asyncio
async def test_a22_end_two_references_close_with_rule_declared_end(scripted_client: ClientFactory) -> None:
    """A22-END: one parent ends on a non-empty page, the other on an empty page; both RULE_DECLARED_END."""
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(_notify(userId=1), _notifications((5, 1))),
            ScriptedExchange.json(_notify(userId=1, LAST_ID=5, LAST_TYPE=1), _notifications()),
            ScriptedExchange.json(_notify(userId=2), _notifications()),
        )
    )
    traversal = ContinuationTraversal(
        ResultSelector(("notifications",)), _ID, (_LAST_ID, _LAST_TYPE), _NotificationRule()
    )
    bindings = [Binding(f"user {user}", (ParameterUpdate(ParameterPath(("userId",)), user),), user) for user in (1, 2)]
    stream = scripted_client(transport).iter_reference_outcomes(
        _notify(), bindings, traversal=traversal, dispatch=DirectDispatch()
    )
    outcomes, failure = await _drain(stream)
    assert failure is None
    transport.assert_exhausted()
    completes = {outcome.correlation: outcome for outcome in outcomes if isinstance(outcome, ReferenceComplete)}
    assert set(completes) == {1, 2}
    assert completes[1].row_count == 1
    assert completes[2].row_count == 0
    assert {outcome.closure for outcome in completes.values()} == {BindingClosure.RULE_DECLARED_END}
    assert stream.report is not None
    assert stream.report.exhausted


@dataclass(frozen=True)
class _ScriptedRule:
    """Return a fixed decision, or raise a callback error whose text must stay out of diagnostics."""

    decide: Callable[[PageView], object]

    def evaluate(self, page: PageView, /) -> ContinueWith | EndOfSource:
        return self.decide(page)  # type: ignore[return-value]


def _raise_secret(_page: PageView) -> object:
    message = "secret-callback-text"
    raise RuntimeError(message)


_N_CASES = [
    pytest.param(
        lambda _page: ContinueWith((ParameterUpdate(_LAST_ID, 5),)),
        ContinuationReason.INVALID_UPDATES,
        id="missing-control",
    ),
    pytest.param(
        lambda _page: ContinueWith(
            (
                ParameterUpdate(_LAST_ID, 5),
                ParameterUpdate(_LAST_TYPE, 1),
                ParameterUpdate(ParameterPath(("LIMIT",)), 9),
            )
        ),
        ContinuationReason.INVALID_UPDATES,
        id="illegal-limit-path",
    ),
    pytest.param(
        lambda _page: ContinueWith((ParameterUpdate(_LAST_ID, 5), ParameterUpdate(ParameterPath(("OTHER",)), 1))),
        ContinuationReason.INVALID_UPDATES,
        id="undeclared-path",
    ),
    pytest.param(
        lambda _page: ContinueWith((ParameterUpdate(_LAST_ID, 5), ParameterUpdate(_LAST_ID, 6))),
        ContinuationReason.INVALID_UPDATES,
        id="repeated-control",
    ),
    pytest.param(
        lambda _page: ContinueWith(
            (ParameterUpdate(_LAST_ID, "x" * CONTROL_VECTOR_MAXIMUM_BYTES), ParameterUpdate(_LAST_TYPE, 1))
        ),
        ContinuationReason.INVALID_UPDATES,
        id="oversized-vector",
    ),
    pytest.param(
        lambda _page: ContinueWith((ParameterUpdate(_LAST_TYPE, 7), ParameterUpdate(_LAST_ID, 3))),
        ContinuationReason.CONTINUATION_CYCLE,
        id="cycle-to-initial-vector-in-any-order",
    ),
    pytest.param(lambda _page: "continue", ContinuationReason.RULE_FAILED, id="invalid-decision"),
    pytest.param(_raise_secret, ContinuationReason.RULE_FAILED, id="rule-raised"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("decide", "reason"), _N_CASES)
async def test_a22_n_invalid_decisions_fail_closed_without_commit(
    decide: Callable[[PageView], object], reason: ContinuationReason, scripted_client: ClientFactory
) -> None:
    """A22-N: illegal paths, oversized vectors, cycles and rule failures reject the page before commit."""
    head = _notify(LAST_ID=3, LAST_TYPE=7)
    transport = ScriptedTransport((ScriptedExchange.json(head, _notifications((5, 1))),))
    traversal = ContinuationTraversal(
        ResultSelector(("notifications",)),
        _ID,
        (_LAST_ID, _LAST_TYPE),
        _ScriptedRule(decide),
        limit_path=ParameterPath(("LIMIT",)),
        page_size=1,
    )
    stream = scripted_client(transport).iter_list_continuation(head, traversal=traversal)
    rows, failure = await _drain(stream)
    assert rows == []
    assert isinstance(failure, IncompleteTraversalError)
    assert isinstance(failure.error, ContinuationError)
    assert failure.error.reason is reason
    assert "secret-callback-text" not in str(failure.error)
    assert "x" * 64 not in str(failure.error)
    transport.assert_exhausted()
    assert stream.report is not None
    assert not stream.report.exhausted
    assert stream.report.emitted == 0


@pytest.mark.asyncio
async def test_a22_n_cycle_to_an_earlier_page_vector_keeps_the_delivered_prefix(scripted_client: ClientFactory) -> None:
    """A22-N: a vector repeating an earlier page fails as a cycle; earlier pages stay delivered, the repeat is not."""
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(_notify(), _notifications((5, 1))),
            ScriptedExchange.json(_notify(LAST_ID=5, LAST_TYPE=1), _notifications((6, 1))),
        )
    )
    rule = _ScriptedRule(lambda _page: ContinueWith((ParameterUpdate(_LAST_ID, 5), ParameterUpdate(_LAST_TYPE, 1))))
    traversal = ContinuationTraversal(ResultSelector(("notifications",)), _ID, (_LAST_ID, _LAST_TYPE), rule)
    stream = scripted_client(transport).iter_list_continuation(_notify(), traversal=traversal)
    rows, failure = await _drain(stream)
    assert [row["id"] for row in rows] == [5]  # type: ignore[index]
    assert isinstance(failure, IncompleteTraversalError)
    assert isinstance(failure.error, ContinuationError)
    assert failure.error.reason is ContinuationReason.CONTINUATION_CYCLE
    transport.assert_exhausted()


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor", [None, "absent"], ids=["null-cursor", "missing-cursor"])
async def test_a22_n_missing_response_token_is_incomplete(cursor: object, scripted_client: ClientFactory) -> None:
    """A22-N: hasMore=true without a usable cursor fails as MISSING_CONTINUATION and commits nothing."""
    payload = _items(1, more=True, cursor=cursor)
    if cursor == "absent":
        del payload["result"]["afterCursor"]  # type: ignore[attr-defined]
    transport = ScriptedTransport((ScriptedExchange.json(_followup(), payload),))
    stream = scripted_client(transport).iter_list_continuation(_followup(), traversal=_followup_traversal())
    rows, failure = await _drain(stream)
    assert rows == []
    assert isinstance(failure, IncompleteTraversalError)
    assert isinstance(failure.error, ContinuationError)
    assert failure.error.reason is ContinuationReason.MISSING_CONTINUATION
    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_a22_n_reference_owned_update_and_start_cursor_are_local_rejections(
    scripted_client: ClientFactory,
) -> None:
    """A22-N: a binding writing an SDK-owned control or carrying start_cursor is rejected before HTTP."""
    transport = ScriptedTransport((ScriptedExchange.json(_notify(userId=3), _notifications()),))
    traversal = ContinuationTraversal(
        ResultSelector(("notifications",)),
        _ID,
        (_LAST_ID, _LAST_TYPE),
        _NotificationRule(),
        limit_path=ParameterPath(("LIMIT",)),
        page_size=1,
    )
    bindings = [
        Binding("owned control", (ParameterUpdate(_LAST_ID, 9),), 1),
        Binding("owned limit", (ParameterUpdate(ParameterPath(("LIMIT",)), 9),), 2),
        Binding("start cursor", (), 4, start_cursor=5),
        Binding("ok", (ParameterUpdate(ParameterPath(("userId",)), 3),), 3),
    ]
    stream = scripted_client(transport).iter_reference_outcomes(
        _notify(), bindings, traversal=traversal, dispatch=DirectDispatch()
    )
    outcomes, failure = await _drain(stream)
    assert failure is None
    transport.assert_exhausted()
    rejected = {outcome.correlation for outcome in outcomes if isinstance(outcome, ReferenceNotExecuted)}
    assert rejected == {1, 2, 4}
    assert all(
        outcome.reason is NotExecutedReason.LOCAL_VALIDATION_FAILED
        for outcome in outcomes
        if isinstance(outcome, ReferenceNotExecuted)
    )
    assert [outcome.correlation for outcome in outcomes if isinstance(outcome, ReferenceComplete)] == [3]


@pytest.mark.asyncio
async def test_a22_n_tolerant_reference_failure_keeps_the_neighbour(scripted_client: ClientFactory) -> None:
    """A22-N: in tolerant mode a parent missing its token fails alone; the neighbour still completes."""
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(_followup(route=RouteKind.BARE, ownerId=1), _items(10, more=True, cursor=None)),
            ScriptedExchange.json(_followup(route=RouteKind.BARE, ownerId=2), _items(20, more=False, cursor=None)),
        )
    )
    bindings = [
        Binding(f"owner {owner}", (ParameterUpdate(ParameterPath(("ownerId",)), owner),), owner) for owner in (1, 2)
    ]
    stream = scripted_client(transport).iter_reference_outcomes(
        _followup(route=RouteKind.BARE), bindings, traversal=_followup_traversal(), dispatch=DirectDispatch()
    )
    outcomes, failure = await _drain(stream)
    assert failure is None
    transport.assert_exhausted()
    failed = [outcome for outcome in outcomes if isinstance(outcome, ReferenceFailure)]
    assert [outcome.correlation for outcome in failed] == [1]
    assert isinstance(failed[0].error, IncompleteTraversalError)
    assert [outcome.correlation for outcome in outcomes if isinstance(outcome, ReferenceComplete)] == [2]


@pytest.mark.asyncio
async def test_a22_n_missing_owned_control_without_create_rejects_before_http(scripted_client: ClientFactory) -> None:
    """A22-N: with allow_create_controls=False every declared control must already exist in the request."""
    transport = ScriptedTransport(())
    stream = scripted_client(transport).iter_list_continuation(
        Request.bare("im.v2.Sticker.Pack.tail", {"id": 0, "limit": 1}, ReplaySafety.SAFE),
        traversal=_sticker_traversal(),
    )
    rows, failure = await _drain(stream)
    assert rows == []
    assert isinstance(failure, CapabilityError)
    assert transport.calls == ()


def test_a22_contract_validation_is_local() -> None:
    """A22: controls are declared, disjoint and bounded; a response cursor rule owns exactly its control."""
    rule = ResponseCursorRule(
        ResultSelector(("afterCursor",)), _AFTER, ResponseFlagTerminal(ResultSelector(("hasMore",)))
    )
    with pytest.raises(ValueError, match="owns exactly"):
        ContinuationTraversal(ResultSelector(("items",)), _ID, (_LAST_ID,), rule)
    with pytest.raises(ValueError, match="non-overlapping"):
        ContinuationTraversal(
            ResultSelector(("items",)), _ID, (_AFTER,), rule, limit_path=ParameterPath(("pagination",))
        )
    with pytest.raises(ValueError, match="declared controls"):
        ContinuationTraversal(ResultSelector(("items",)), _ID, (), rule)
    with pytest.raises(TypeError, match="evaluate"):
        ContinuationTraversal(ResultSelector(("items",)), _ID, (_LAST_ID,), object())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="non-root"):
        ResponseFlagTerminal(ResultSelector.root())


def _declared_end_gate(closure: BindingClosure, *, pages: int) -> CompletionGate:
    """Drive one binding through ``pages`` acknowledged empty pages, then close it with ``closure``."""
    gate = CompletionGate("run")
    sequence = 0

    def emit(event_type: type, **fields: object) -> None:
        nonlocal sequence
        gate.emit(event_type(operation_id="run", sequence=sequence, **fields))
        sequence += 1

    emit(BindingAdmitted, binding_id=0)
    for page_id in range(pages):
        emit(PageScheduled, binding_id=0, page_id=page_id)
        emit(PageCommandOutcome, binding_id=0, page_id=page_id, outcome=CommandSettlement.SUCCESS)
        emit(PageValidated, binding_id=0, page_id=page_id, row_count=0)
        emit(PageDelivered, binding_id=0, page_id=page_id)
        emit(PageAcknowledged, binding_id=0, page_id=page_id)
    emit(BindingTerminal, binding_id=0, closure=closure)
    emit(StreamTerminal, closure=StreamClosure.NATURAL)
    emit(CleanupOutcome, state=CleanupState.SUCCESS)
    return gate


@pytest.mark.parametrize("closure", [BindingClosure.RULE_DECLARED_END, BindingClosure.DECLARED_RESPONSE_TERMINAL])
def test_a22_end_declared_end_closures_need_an_acknowledged_page(closure: BindingClosure) -> None:
    """A22-END/A29-HEAD: a declared end closes after one acknowledged (even empty) page, never without one."""
    witnessed = _declared_end_gate(closure, pages=1).decision()
    assert witnessed.state is TerminalState.COMPLETED
    assert witnessed.exhausted
    missing = _declared_end_gate(closure, pages=0).decision()
    assert missing.state is TerminalState.INCOMPLETE
    assert {item.code for item in missing.violations} & {
        "completion_missing_declared_end_witness",
        "completion_terminal_lacks_page_witness",
    }


@dataclass(frozen=True)
class _PhaseRule:
    """Pure caller rule: an empty phase-1 page moves to phase 2; an empty phase-2 page ends the source."""

    def evaluate(self, page: PageView, /) -> ContinueWith | EndOfSource:
        phase = page.result["phase"]  # type: ignore[index, call-overload]
        if page.items:
            return ContinueWith(
                (
                    ParameterUpdate(_LAST_ID, page.items[-1]["id"]),  # type: ignore[index, call-overload]
                    ParameterUpdate(_LAST_TYPE, 1 if phase == 1 else 3),
                )
            )
        if phase == 1:
            return ContinueWith((ParameterUpdate(_LAST_ID, 0), ParameterUpdate(_LAST_TYPE, 3)))
        return EndOfSource()


@pytest.mark.asyncio
async def test_two_empty_pages_with_a_changed_control_vector_end_by_the_rule(scripted_client: ClientFactory) -> None:
    """Progress is the control vector: two empty pages are not a repeated page when the vector moved."""
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(_notify(), {"result": {"notifications": [], "phase": 1}}),
            ScriptedExchange.json(_notify(LAST_ID=0, LAST_TYPE=3), {"result": {"notifications": [], "phase": 2}}),
        )
    )
    traversal = ContinuationTraversal(
        ResultSelector(("notifications",)),
        _ID,
        (_LAST_ID, _LAST_TYPE),
        _PhaseRule(),
        limit_path=ParameterPath(("LIMIT",)),
        page_size=1,
    )
    stream = scripted_client(transport).iter_list_continuation(_notify(), traversal=traversal)
    rows, failure = await _drain(stream)
    assert failure is None
    assert rows == []
    transport.assert_exhausted()
    assert stream.report is not None
    assert stream.report.exhausted
    assert stream.report.terminal_reason == _RULE_END
