"""One ``Request`` means the same direct and in classic batch, or it is refused before any send."""

from __future__ import annotations
import enum
from typing import TYPE_CHECKING, cast

import pytest

from b24api import (
    AutoKeysetExecution,
    BatchDispatch,
    IdentityCoercion,
    IdentitySpec,
    KeysetTraversal,
    ReplaySafety,
    Request,
    ResultSelector,
    SequentialTraversal,
)
from b24api.contracts import (
    Binding,
    Command,
    CommandFailure,
    CommandSuccess,
    ParameterPath,
    ParameterUpdate,
    ReferenceComplete,
    ReferenceFailure,
)
from b24api.contracts.keyset_execution import StableIntegerKeysetContract
from b24api.contracts.wire import BodyEncoding
from b24api.errors import BatchFailed, CapabilityError
from b24api.testing import ScriptedExchange, ScriptedTransport
from tests.scripting import BatchPortal, ResponderTransport, client_for

if TYPE_CHECKING:
    from tests.scripting import Envelope


class _Stage(str, enum.Enum):  # noqa: UP042 - a StrEnum already renders its value; the mixin does not
    NEW = "NEW"


class _Priority(int, enum.Enum):
    HIGH = 2


class _Ratio(float, enum.Enum):
    HALF = 0.5


def _answer(request: Request) -> Envelope:
    if request.method != "batch":
        return {"result": True}
    commands = cast("dict[str, str]", request.copy_parameters()["cmd"])
    return {"result": {"result": dict.fromkeys(commands, True), "result_error": []}}


def _commands(transport: ResponderTransport) -> list[str]:
    return [
        query
        for request in transport.requests
        if request.method == "batch"
        for query in cast("dict[str, str]", request.copy_parameters()["cmd"]).values()
    ]


def test_a_str_int_or_float_subclass_is_stored_as_its_plain_value() -> None:
    request = Request.bare(
        "crm.deal.update",
        {"fields": {_Stage.NEW: _Stage.NEW, "PRIO": _Priority.HIGH, "RATIO": _Ratio.HALF, "IDS": [_Priority.HIGH]}},
    )
    plain = Request.bare("crm.deal.update", {"fields": {"NEW": "NEW", "PRIO": 2, "RATIO": 0.5, "IDS": [2]}})

    fields = cast("dict[str, object]", request.copy_parameters()["fields"])
    assert [(key, type(key)) for key in fields] == [("NEW", str), ("PRIO", str), ("RATIO", str), ("IDS", str)]
    assert [type(value) for value in fields.values()] == [str, int, float, list]
    assert cast("list[object]", fields["IDS"])[0].__class__ is int
    assert request == plain
    assert hash(request) == hash(plain)


class _Flag(enum.IntFlag):
    A = 1
    B = 4


class _Code(enum.StrEnum):
    NEW = "N"


class _Loud(str):
    __slots__ = ()

    def __str__(self) -> str:
        return "overridden"


def test_flags_str_enums_and_overriding_subclasses_freeze_to_their_built_in_value() -> None:
    request = Request.bare("x.list", {"flags": _Flag.A | _Flag.B, "stage": _Code.NEW})
    loud = Request.bare("x.list", {"name": _Loud("plain")})

    assert request.copy_parameters() == {"flags": 5, "stage": "N"}
    assert [type(value) for value in request.copy_parameters().values()] == [int, str]
    assert loud.copy_parameters() == {"name": "plain"}
    assert type(loud.copy_parameters()["name"]) is str


@pytest.mark.asyncio
async def test_an_enum_member_reaches_classic_batch_as_the_value_a_direct_call_sends() -> None:
    transport = ResponderTransport(_answer)
    request = Request.bare("crm.deal.update", {"id": 5, "fields": {"STAGE_ID": _Stage.NEW, "PRIO": _Priority.HIGH}})
    async with client_for(transport) as client:
        await client.call(request)
        outcomes = [outcome async for outcome in client.batch_outcomes([Command(request, "deal")])]

    assert isinstance(outcomes[0], CommandSuccess)
    assert transport.requests[0].copy_parameters() == {"id": 5, "fields": {"STAGE_ID": "NEW", "PRIO": 2}}
    assert _commands(transport) == ["crm.deal.update?id=5&fields%5BSTAGE_ID%5D=NEW&fields%5BPRIO%5D=2"]


@pytest.mark.parametrize("empty", [[], {}], ids=["array", "object"])
@pytest.mark.asyncio
async def test_classic_batch_refuses_an_empty_container_it_would_drop(empty: object) -> None:
    transport = ResponderTransport(_answer)
    clearing = Request.bare("crm.deal.update", {"id": 5, "fields": {"UF_MULTI": empty}}, ReplaySafety.UNSAFE)
    other = Request.bare("crm.deal.update", {"id": 6, "fields": {"TITLE": "kept"}}, ReplaySafety.UNSAFE)
    async with client_for(transport) as client:
        outcomes = [outcome async for outcome in client.batch_outcomes([Command(clearing, "a"), Command(other, "b")])]

    refused = outcomes[0]
    assert isinstance(refused, CommandFailure)
    assert isinstance(refused.error, CapabilityError)
    assert "empty array or object" in str(refused.error)
    assert isinstance(outcomes[1], CommandSuccess)
    # Only the representable command travels; the clearing update is never sent as ``crm.deal.update?id=5``.
    assert _commands(transport) == ["crm.deal.update?id=6&fields%5BTITLE%5D=kept"]


@pytest.mark.asyncio
async def test_a_direct_json_call_keeps_its_empty_containers() -> None:
    transport = ResponderTransport(_answer)
    async with client_for(transport) as client:
        await client.call(Request.bare("crm.deal.update", {"id": 5, "fields": {"UF_MULTI": [], "UF_MAP": {}}}))

    assert transport.requests[0].copy_parameters() == {"id": 5, "fields": {"UF_MULTI": [], "UF_MAP": {}}}


def test_a_form_encoded_request_refuses_an_empty_container() -> None:
    with pytest.raises(ValueError, match="empty array or object"):
        Request.bare("disk.folder.add", {"data": {"TAGS": []}}, encoding=BodyEncoding.FORM_URLENCODED)
    assert Request.bare("disk.folder.add", {"data": {"NAME": "x"}}, encoding=BodyEncoding.FORM_URLENCODED)


_CLEARING = Request.bare("crm.item.list", {"filter": {"@STAGE_ID": []}}, ReplaySafety.SAFE)


@pytest.mark.asyncio
async def test_counted_traversal_refuses_an_empty_container_before_its_direct_head() -> None:
    transport = ResponderTransport(_answer)
    async with client_for(transport) as client:
        with pytest.raises(CapabilityError, match="use iter_list"):
            client.iter_list_counted(_CLEARING, page_size=50)

    assert transport.requests == []


@pytest.mark.asyncio
async def test_fast_keyset_refuses_an_empty_container_before_any_request() -> None:
    transport = ResponderTransport(_answer)
    async with client_for(transport) as client:
        with pytest.raises(CapabilityError, match="SequentialKeysetExecution"):
            client.iter_list_keyset(
                _CLEARING,
                selector=ResultSelector.root(),
                identity=IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER),
            )

    assert transport.requests == []


@pytest.mark.asyncio
async def test_a_binding_that_fills_an_empty_template_placeholder_travels_in_classic_batch() -> None:
    portal = BatchPortal(lambda _method, _query: {"result": [], "total": 0})
    template = Request.bare("crm.item.list", {"filter": {"@PARENT": []}}, ReplaySafety.SAFE)
    bindings = [Binding("parent", (ParameterUpdate(ParameterPath(("filter", "@PARENT")), [2]),), 2)]
    async with client_for(portal) as client:
        stream = client.iter_reference_outcomes(
            template, bindings, traversal=SequentialTraversal(page_size=50), dispatch=BatchDispatch()
        )
        outcomes = [outcome async for outcome in stream]

    assert [type(outcome) for outcome in outcomes] == [ReferenceComplete]
    assert portal.queries == [{"filter[@PARENT][0]": "2", "start": "0"}]


@pytest.mark.asyncio
async def test_a_binding_that_injects_an_empty_array_fails_alone_before_it_is_sent() -> None:
    portal = BatchPortal(lambda _method, _query: {"result": [], "total": 0})
    template = Request.bare("crm.item.list", {"filter": {"@PARENT": [0]}}, ReplaySafety.SAFE)
    bindings = [
        Binding("parent", (ParameterUpdate(ParameterPath(("filter", "@PARENT")), values),), name)
        for name, values in (("empty", []), ("full", [2]))
    ]
    async with client_for(portal) as client:
        stream = client.iter_reference_outcomes(
            template, bindings, traversal=SequentialTraversal(page_size=50), dispatch=BatchDispatch()
        )
        outcomes = {outcome.correlation: outcome async for outcome in stream}

    refused = outcomes["empty"]
    assert isinstance(refused, ReferenceFailure)
    assert isinstance(refused.error, CapabilityError)
    assert isinstance(outcomes["full"], ReferenceComplete)
    assert portal.queries == [{"filter[@PARENT][0]": "2", "start": "0"}]


@pytest.mark.asyncio
async def test_fail_fast_batch_sends_nothing_when_one_command_cannot_be_encoded() -> None:
    transport = ResponderTransport(_answer)
    clearing = Request.bare("crm.deal.update", {"id": 5, "fields": {"UF_MULTI": []}}, ReplaySafety.UNSAFE)
    other = Request.bare("crm.deal.update", {"id": 6, "fields": {"TITLE": "kept"}}, ReplaySafety.UNSAFE)
    async with client_for(transport) as client:
        stream = client.batch([Command(clearing, "a"), Command(other, "b")])
        with pytest.raises(BatchFailed) as raised:
            _ = [outcome async for outcome in stream]

    refused = raised.value.outcomes[0]
    assert isinstance(refused, CommandFailure)
    assert isinstance(refused.error, CapabilityError)
    assert transport.requests == []


@pytest.mark.asyncio
async def test_native_v3_batch_keeps_an_empty_array_in_its_json_body() -> None:
    commands = (Request.v3("crm.item.update", {"id": 5, "fields": {"ufMulti": []}}, ReplaySafety.UNSAFE),)
    transport = ScriptedTransport((ScriptedExchange.v3_batch(commands, ({"item": {"id": 5}},)),))
    async with client_for(transport) as client:
        outcomes = [outcome async for outcome in client.batch_outcomes([Command(commands[0], "v3")])]

    assert isinstance(outcomes[0], CommandSuccess)
    transport.assert_exhausted()


_ROWS = 120


@pytest.mark.asyncio
async def test_a_counted_head_and_its_batched_tail_send_the_same_enum_filter() -> None:
    def answer(_method: str, query: dict[str, str]) -> Envelope:
        offset = int(query.get("start", "0"))
        rows = [{"ID": row} for row in range(offset, min(offset + 50, _ROWS))]
        return {"result": rows, "total": _ROWS, **({"next": offset + 50} if offset + 50 < _ROWS else {})}

    portal = BatchPortal(answer)
    async with client_for(portal) as client:
        stream = client.iter_list_counted(
            Request.bare("crm.item.list", {"filter": {"STAGE_ID": _Stage.NEW}}, ReplaySafety.SAFE), page_size=50
        )
        async with stream:
            assert len([row async for row in stream]) == _ROWS

    assert len(portal.queries) == 3  # noqa: PLR2004 - a direct head and two batched tail pages
    assert {query["filter[STAGE_ID]"] for query in portal.queries} == {"NEW"}


@pytest.mark.asyncio
async def test_keyset_verification_refuses_an_empty_container_before_any_probe() -> None:
    transport = ResponderTransport(_answer)
    async with client_for(transport) as client:
        with pytest.raises(CapabilityError, match="keyset verifier"):
            await client.verify_keyset_capability(
                _CLEARING,
                selector=ResultSelector.root(),
                identity=IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER),
            )

    assert transport.requests == []


@pytest.mark.asyncio
async def test_batched_fan_out_fails_only_the_command_it_cannot_encode() -> None:
    transport = ResponderTransport(_answer)
    clearing = Request.bare("crm.deal.update", {"id": 5, "fields": {"UF_MULTI": []}}, ReplaySafety.UNSAFE)
    other = Request.bare("crm.deal.update", {"id": 6, "fields": {"TITLE": "kept"}}, ReplaySafety.UNSAFE)
    async with client_for(transport) as client:
        stream = client.fan_out_outcomes([Command(clearing, "a"), Command(other, "b")], dispatch=BatchDispatch())
        outcomes = {outcome.correlation: outcome async for outcome in stream}

    refused = outcomes["a"]
    assert isinstance(refused, CommandFailure)
    assert isinstance(refused.error, CapabilityError)
    assert isinstance(outcomes["b"], CommandSuccess)
    assert _commands(transport) == ["crm.deal.update?id=6&fields%5BTITLE%5D=kept"]


@pytest.mark.asyncio
async def test_an_auto_keyset_binding_that_fills_an_empty_placeholder_is_sent_and_one_that_injects_it_fails() -> None:
    portal = BatchPortal(lambda _method, _query: {"result": []})
    template = Request.bare("crm.item.list", {"filter": {"@PARENT": []}}, ReplaySafety.SAFE)
    bindings = [
        Binding("parent", (ParameterUpdate(ParameterPath(("filter", "@PARENT")), values),), name)
        for name, values in (("empty", []), ("full", [2]))
    ]
    traversal = KeysetTraversal(
        selector=ResultSelector.root(),
        identity=IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER),
        execution=AutoKeysetExecution(StableIntegerKeysetContract()),
    )
    async with client_for(portal) as client:
        stream = client.iter_reference_outcomes(template, bindings, traversal=traversal, dispatch=BatchDispatch())
        outcomes = {outcome.correlation: outcome async for outcome in stream}

    refused = outcomes["empty"]
    assert isinstance(refused, ReferenceFailure)
    assert isinstance(refused.error, CapabilityError)
    assert isinstance(outcomes["full"], ReferenceComplete)
    assert portal.queries
    assert {query["filter[@PARENT][0]"] for query in portal.queries} == {"2"}
