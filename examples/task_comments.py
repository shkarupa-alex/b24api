"""Scenario 6: resolve modern task chats and traverse legacy comments.

One physical batch resolves four task IDs: a modern chat with messages, a
legacy task without a chat, an empty modern chat, and a chat whose messages
are inaccessible. The legacy fallback uses the exact TASKID/ORDER/FILTER
positional ABI and public keyset traversal; the stub tasks.task.comment.list
is never used. Run the frozen fixture, including the verifier canary:
`uv run python -m examples.task_comments`.
"""

from __future__ import annotations
import asyncio
import os

from b24api import (
    ApiResponseError,
    Binding,
    Bitrix24,
    CursorSpec,
    CursorTraversal,
    DirectDispatch,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    OperationReport,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    SequentialKeysetExecution,
    Settings,
    TerminalState,
)
from b24api.contracts import (
    Command,
    CommandSuccess,
    CursorDomain,
    PositionalArguments,
    PositionalLayout,
    Present,
    ReferenceComplete,
    ReferenceFailure,
    ReferenceItem,
    SlotContract,
    SlotShape,
)
from b24api.testing import ScriptedExchange, ScriptedTransport
from examples._support.evidence import RecipeEvidence

CHAT_METHOD = "im.chat.get"
MESSAGE_METHOD = "im.dialog.messages.get"
LEGACY_METHOD = "task.commentitem.getlist"
TASKS = (42, 43, 44, 45)
CHAT_IDS = {42: 900, 44: 901, 45: 902}
EXPECTED_MODERN = (10009, 10008)
EXPECTED_LEGACY = (1, 2)
LAYOUT = PositionalLayout(
    "task.commentitem.getlist.three.v1",
    (
        SlotContract("TASKID", SlotShape.SCALAR, fixed=True),
        SlotContract("ORDER", SlotShape.OBJECT),
        SlotContract("FILTER", SlotShape.OBJECT),
    ),
    control_paths=frozenset({(1, "ID"), (2, "ID"), (2, ">ID"), (2, "<ID")}),
)


def _chat_request(task_id: int) -> Request:
    return Request(
        CHAT_METHOD,
        {"ENTITY_TYPE": "TASKS_TASK", "ENTITY_ID": str(task_id)},
        replay_safety=ReplaySafety.SAFE,
        route=RouteKind.BARE,
    )


def _message_request(chat_id: int, cursor: int | None) -> Request:
    params = {"DIALOG_ID": f"chat{chat_id}", "LIMIT": 2}
    if cursor is not None:
        params["LAST_ID"] = cursor
    return Request(
        MESSAGE_METHOD,
        params,
        replay_safety=ReplaySafety.SAFE,
        route=RouteKind.BARE,
    )


def _legacy_request(*, cursor: int | None = None, ordered: bool = False) -> Request:
    order = {"ID": "ASC"} if ordered else {}
    keyset_filter = {} if cursor is None else {">ID": cursor}
    arguments = PositionalArguments(
        (Present(43), Present(order), Present(keyset_filter)),
        LAYOUT.layout_id,
        layout=LAYOUT,
    )
    return Request(LEGACY_METHOD, arguments, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)


def _legacy_control_request(direction: str, lower: int | None, upper: int | None) -> Request:
    keyset_filter = {
        **({">ID": lower} if lower is not None else {}),
        **({"<ID": upper} if upper is not None else {}),
    }
    arguments = PositionalArguments(
        (Present(43), Present({"ID": direction}), Present(keyset_filter)),
        LAYOUT.layout_id,
        layout=LAYOUT,
    )
    return Request(LEGACY_METHOD, arguments, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)


def _legacy_verifier_exchanges() -> tuple[ScriptedExchange, ...]:
    controls = (
        ("ASC", None, None, EXPECTED_LEGACY),
        ("DESC", None, None, tuple(reversed(EXPECTED_LEGACY))),
        ("ASC", 0, 1, ()),
        ("ASC", 1, 2, ()),
        ("ASC", 0, 2, (1,)),
        ("ASC", 0, 3, EXPECTED_LEGACY),
        ("DESC", 0, 3, tuple(reversed(EXPECTED_LEGACY))),
    )
    return tuple(
        ScriptedExchange.json(
            _legacy_control_request(direction, lower, upper),
            {"result": [{"ID": str(value)} for value in values]},
        )
        for direction, lower, upper, values in controls
    )


def _fixture() -> ScriptedTransport:
    verifier = _legacy_verifier_exchanges() if os.environ.get("ENV") != "PROD" else ()
    return ScriptedTransport(
        (
            ScriptedExchange.batch(
                tuple(_chat_request(task_id) for task_id in TASKS),
                ({"ID": 900}, None, {"ID": 901}, {"ID": 902}),
                total=0,
            ),
            ScriptedExchange.json(
                _message_request(900, None),
                {"result": {"messages": [{"id": value} for value in EXPECTED_MODERN]}},
            ),
            ScriptedExchange.json(_message_request(900, 10008), {"result": {"messages": []}}),
            ScriptedExchange.json(_message_request(901, None), {"result": {"messages": []}}),
            ScriptedExchange.json(
                _message_request(902, None),
                {"error": "ACCESS_ERROR", "error_description": "denied"},
            ),
            *verifier,
            ScriptedExchange.json(
                _legacy_request(ordered=True),
                {"result": [{"ID": str(value)} for value in EXPECTED_LEGACY]},
            ),
            ScriptedExchange.json(_legacy_request(cursor=2, ordered=True), {"result": []}),
        )
    )


def _id(row: object, key: str) -> int:
    if not isinstance(row, dict):
        raise TypeError("comment row must be an object")
    value = row.get(key)
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise TypeError("comment ID must be a string or integer")
    return int(value)


async def _resolve_chats(client: Bitrix24) -> tuple[dict[int, int | None], OperationReport]:
    stream = client.batch_outcomes(tuple(Command(_chat_request(task_id), task_id) for task_id in TASKS))
    resolved: dict[int, int | None] = {}
    async for outcome in stream:
        if not isinstance(outcome, CommandSuccess):
            raise TypeError("scenario 6 chat-resolution batch lost a task outcome")
        result = outcome.result
        resolved[outcome.correlation] = _id(result, "ID") if result is not None else None
    if resolved != {42: 900, 43: None, 44: 901, 45: 902}:
        raise AssertionError("scenario 6 batch chat correlation differed from oracle")
    if stream.report is None or stream.report.state is not TerminalState.COMPLETED:
        raise AssertionError("scenario 6 chat batch lacked completion")
    return resolved, stream.report


async def _read_modern(client: Bitrix24, resolved: dict[int, int | None]) -> tuple[int, OperationReport]:
    bindings = tuple(
        Binding(
            f"task:{task_id}",
            (ParameterUpdate(ParameterPath(("DIALOG_ID",)), f"chat{chat_id}"),),
            task_id,
        )
        for task_id, chat_id in resolved.items()
        if chat_id is not None
    )
    stream = client.iter_reference_outcomes(
        _message_request(900, None),
        bindings,
        traversal=CursorTraversal(
            selector=ResultSelector(("messages",)),
            cursor=CursorSpec(
                ParameterPath(("LAST_ID",)),
                ("id",),
                IdentityCoercion.EXACT_INTEGER,
                "descending",
                "last",
                domain=CursorDomain.EXCLUSIVE_POSITIVE_INTEGER,
                limit_path=ParameterPath(("LIMIT",)),
            ),
            page_size=2,
        ),
        dispatch=DirectDispatch(concurrency=1),
    )
    messages: dict[int, list[int]] = {task_id: [] for task_id in CHAT_IDS}
    completed: set[int] = set()
    denied: set[int] = set()
    async for outcome in stream:
        if isinstance(outcome, ReferenceItem):
            messages[outcome.correlation].append(_id(outcome.item, "id"))
        elif isinstance(outcome, ReferenceComplete):
            completed.add(outcome.correlation)
        elif isinstance(outcome, ReferenceFailure):
            if not isinstance(outcome.error, ApiResponseError) or outcome.error.original_code != "ACCESS_ERROR":
                raise AssertionError("scenario 6 inaccessible chat lost typed ACCESS_ERROR")
            denied.add(outcome.correlation)
    if messages != {42: [10009, 10008], 44: [], 45: []} or completed != {42, 44} or denied != {45}:
        raise AssertionError("scenario 6 modern empty/inaccessible outcomes collapsed")
    if stream.report is None or stream.report.state is not TerminalState.COMPLETED_WITH_FAILURES:
        raise AssertionError("scenario 6 modern failure falsely claimed global completion")
    return sum(len(values) for values in messages.values()), stream.report


async def _read_legacy(client: Bitrix24) -> OperationReport:
    request = _legacy_request()
    selector = ResultSelector.root()
    identity = IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.DECIMAL_STRING_INTEGER)
    keyset = KeysetSpec(
        filter_path=ParameterPath((2,)),
        order_path=ParameterPath((1,)),
        start_suppression_path=None,
    )
    if os.environ.get("ENV") != "PROD":
        # An endpoint accepting ID filters may still ignore strict bounds or ordering.
        # Keep this fail-closed guard until this exact portal/request is qualified;
        # after qualification, set ENV=PROD or deliberately remove the guard.
        await client.verify_keyset_capability(
            request,
            selector=selector,
            identity=identity,
            page_size=2,
            keyset=keyset,
        )
    stream = client.iter_list_keyset(
        request,
        selector=selector,
        identity=identity,
        page_size=2,
        keyset=keyset,
        execution=SequentialKeysetExecution(),
    )
    observed = tuple([_id(row, "ID") async for row in stream])
    if observed != EXPECTED_LEGACY or stream.report is None or not stream.report.exhausted:
        raise AssertionError("scenario 6 legacy positional keyset differed from oracle")
    return stream.report


async def run() -> RecipeEvidence:
    """Retain modern and legacy outcomes through public traversal APIs."""
    transport = _fixture()
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    async with Bitrix24(settings, transport=transport) as client:
        resolved, resolution_report = await _resolve_chats(client)
        observed_count, modern_report = await _read_modern(client, resolved)
        legacy_report = await _read_legacy(client)
    transport.assert_exhausted()
    legacy_slots = tuple(
        request.positional.to_wire_slots()
        for request in transport.calls
        if request.method == LEGACY_METHOD and request.positional is not None
    )
    if legacy_slots[-2:] != ([43, {"ID": "ASC"}, {}], [43, {"ID": "ASC"}, {">ID": 2}]):
        raise AssertionError("scenario 6 legacy TASKID/ORDER/FILTER wire order changed")
    return RecipeEvidence(
        observed_count,
        modern_report,
        (resolution_report, modern_report, legacy_report),
    )


if __name__ == "__main__":
    asyncio.run(run())
