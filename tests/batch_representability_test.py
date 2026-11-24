"""One rule decides what a classic batch command carries as a direct JSON call does; the rest is refused.

A classic batch command is a PHP bracket query read back by ``parse_str`` and then by the batch handler,
which substitutes ``$result[...]`` references inside every value. A ``None``, an empty container, a key
bracket syntax would rename, or a value holding a reference pattern would reach the method as something
else. Each such command is refused before any request; automatic keyset traversal settles on direct
pages instead, and every value a request cannot encode at all is refused when the ``Request`` is built.
"""

from __future__ import annotations
import copy
import os
import pickle
from typing import TYPE_CHECKING, cast

import pytest

from b24api import (
    BatchDispatch,
    IdentityCoercion,
    IdentitySpec,
    RangeKeysetExecution,
    ReplaySafety,
    Request,
    ResultSelector,
    StableIntegerKeysetContract,
)
from b24api.contracts import (
    Binding,
    Command,
    CommandFailure,
    CommandSuccess,
    KeysetExecutionKind,
    KeysetSelectionReason,
    ParameterPath,
    ParameterUpdate,
    Present,
)
from b24api.contracts.wire import BodyEncoding
from b24api.encoding import bracket_obstacle
from b24api.errors import CapabilityError
from tests.scripting import ResponderTransport, client_for

if TYPE_CHECKING:
    from collections.abc import Callable

_IDENTITY = IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_INTEGER)


def _answer(request: Request) -> object:
    if request.method != "batch":
        return {"result": True}
    commands = cast("dict[str, str]", request.copy_parameters()["cmd"])
    return {"result": {"result": dict.fromkeys(commands, True), "result_error": []}}


def _batched(transport: ResponderTransport) -> list[str]:
    return [
        query
        for request in transport.requests
        if request.method == "batch"
        for query in cast("dict[str, str]", request.copy_parameters()["cmd"]).values()
    ]


async def _batch_outcomes(transport: ResponderTransport, *requests: Request) -> list[object]:
    async with client_for(transport) as client:
        stream = client.batch_outcomes([Command(request, index) for index, request in enumerate(requests)])
        return [outcome async for outcome in stream]


_KEPT = Request.bare("crm.deal.update", {"id": 6, "fields": {"TITLE": "kept"}}, ReplaySafety.UNSAFE)
_KEPT_QUERY = "crm.deal.update?id=6&fields%5BTITLE%5D=kept"


@pytest.mark.parametrize(
    ("fields", "named"),
    [
        ({"ASSIGNED_BY_ID": None}, "None value"),
        ({"UF_MULTI": [None, 5]}, "None value"),
        ({"COMMENTS": "plain $result[c000000000000][ID]"}, "$result reference"),
        ({"TITLE": "x-$RESULT_TOTAL[a]-y"}, "$result reference"),
        ({"a]b": 1}, "key"),
        ({"": 1}, "key"),
        ({"a[b": 1}, "key"),
    ],
    ids=["none", "none-in-list", "result", "result-total-upper", "close-bracket-key", "empty-key", "open-bracket-key"],
)
@pytest.mark.asyncio
async def test_a_command_classic_batch_would_carry_differently_fails_alone_before_it_is_sent(
    fields: dict[str, object], named: str
) -> None:
    transport = ResponderTransport(_answer)
    changed = Request.bare("crm.deal.update", {"id": 5, "fields": fields}, ReplaySafety.UNSAFE)

    outcomes = await _batch_outcomes(transport, changed, _KEPT)

    refused = outcomes[0]
    assert isinstance(refused, CommandFailure)
    assert isinstance(refused.error, CapabilityError)
    assert named in str(refused.error)
    assert "use direct dispatch" in str(refused.error)
    assert isinstance(outcomes[1], CommandSuccess)
    assert _batched(transport) == [_KEPT_QUERY]


@pytest.mark.parametrize("name", ["result", "result_error", "result_total", "result_next", "result_time"])
@pytest.mark.asyncio
async def test_every_name_the_batch_handler_substitutes_is_a_reference_pattern(name: str) -> None:
    transport = ResponderTransport(_answer)
    request = Request.bare("crm.lead.add", {"fields": {"TITLE": f"probe-${name}[a][0][ID]-end"}}, ReplaySafety.UNSAFE)

    outcomes = await _batch_outcomes(transport, request)

    assert isinstance(outcomes[0], CommandFailure)
    assert "$result reference" in str(outcomes[0].error)
    assert transport.requests == []


@pytest.mark.asyncio
async def test_text_that_only_resembles_a_reference_and_keys_nested_with_dots_travel_in_classic_batch() -> None:
    transport = ResponderTransport(_answer)
    request = Request.bare(
        "crm.deal.update",
        {"id": 5, "fields": {"PRICE": "$result", "NOTE": "$results[a]", "UF.CRM x": True, "OFF": False}},
        ReplaySafety.UNSAFE,
    )

    outcomes = await _batch_outcomes(transport, request)

    assert isinstance(outcomes[0], CommandSuccess)
    # A bool travels as PHP's http_build_query writes it: 1 or 0.
    assert _batched(transport) == [
        "crm.deal.update?id=5&fields%5BPRICE%5D=%24result&fields%5BNOTE%5D=%24results%5Ba%5D"
        "&fields%5BUF.CRM+x%5D=1&fields%5BOFF%5D=0"
    ]


@pytest.mark.parametrize("key", ["a.b", "a b"])
@pytest.mark.asyncio
async def test_a_top_level_key_php_would_rename_fails_in_classic_batch(key: str) -> None:
    transport = ResponderTransport(_answer)

    outcomes = await _batch_outcomes(transport, Request.bare("x.y", {key: 1}, ReplaySafety.UNSAFE))

    assert isinstance(outcomes[0], CommandFailure)
    assert "key" in str(outcomes[0].error)
    assert transport.requests == []


@pytest.mark.parametrize("key", ["A[B", "A]B", "A\x00B", ""], ids=["open-bracket", "close-bracket", "nul", "empty"])
@pytest.mark.asyncio
async def test_a_nested_key_php_would_cut_or_rename_fails_in_classic_batch(key: str) -> None:
    transport = ResponderTransport(_answer)
    request = Request.bare("crm.deal.update", {"id": 1, "fields": {key: 1}}, ReplaySafety.UNSAFE)

    outcomes = await _batch_outcomes(transport, request)

    assert isinstance(outcomes[0], CommandFailure)
    assert "key" in str(outcomes[0].error)
    assert transport.requests == []


@pytest.mark.parametrize(
    "clone",
    [copy.copy, copy.deepcopy, lambda request: pickle.loads(pickle.dumps(request))],  # noqa: S301 - a value built here
    ids=["copy", "deepcopy", "pickle"],
)
def test_a_copied_or_unpickled_request_keeps_its_batch_verdict(clone: Callable[[Request], Request]) -> None:
    plain = Request.bare("crm.deal.update", {"id": 1, "fields": {"TITLE": "x"}}, ReplaySafety.UNSAFE)
    clearing = Request.bare("crm.deal.update", {"id": 1, "fields": {"TITLE": None}}, ReplaySafety.UNSAFE)

    assert clone(plain).classic_batch_obstacle is None
    assert clone(clearing).classic_batch_obstacle == clearing.classic_batch_obstacle is not None


@pytest.mark.asyncio
async def test_a_direct_call_sends_none_and_reference_text_as_written() -> None:
    transport = ResponderTransport(_answer)
    parameters = {"id": 5, "fields": {"ASSIGNED_BY_ID": None, "COMMENTS": "plain $result[a][0][ID]"}}
    async with client_for(transport) as client:
        await client.call(Request.bare("crm.deal.update", parameters, ReplaySafety.UNSAFE))

    assert transport.requests[0].copy_parameters() == parameters


@pytest.mark.asyncio
async def test_batched_fan_out_fails_only_the_command_carrying_none() -> None:
    transport = ResponderTransport(_answer)
    clearing = Request.bare("crm.deal.update", {"id": 5, "fields": {"ASSIGNED_BY_ID": None}}, ReplaySafety.UNSAFE)
    async with client_for(transport) as client:
        stream = client.fan_out_outcomes([Command(clearing, "a"), Command(_KEPT, "b")], dispatch=BatchDispatch())
        outcomes = {outcome.correlation: outcome async for outcome in stream}

    assert isinstance(outcomes["a"], CommandFailure)
    assert isinstance(outcomes["a"].error, CapabilityError)
    assert isinstance(outcomes["b"], CommandSuccess)
    assert _batched(transport) == [_KEPT_QUERY]


# Ch-1 on a live portal: ``crm.deal.list`` with ``{"COMPANY_ID": null}`` selects fewer rows than with no filter,
# and a batch command without the key selects all of them. This portal keeps those semantics: a direct call
# whose filter holds ``COMPANY_ID: null`` selects the odd identities, and a filter without the key all of them.
_ROWS = 400
_PAGE = 50
_NULL_FILTERED = Request.bare("crm.deal.list", {"filter": {"COMPANY_ID": None}}, ReplaySafety.SAFE)


def _null_aware_portal(request: Request) -> object:
    if request.method == "batch":
        raise AssertionError("a request carrying None must never travel as a classic batch command")
    parameters = request.copy_parameters()
    selection = cast("dict[str, object]", parameters.get("filter", {}))
    after = cast("int", selection.get(">ID", 0))
    rows = [
        {"ID": identity}
        for identity in range(1, _ROWS + 1)
        if identity > after and ("COMPANY_ID" not in selection or identity % 2)
    ]
    return {"result": rows[:_PAGE]}


@pytest.mark.asyncio
async def test_auto_keyset_settles_on_direct_pages_when_classic_batch_would_drop_a_none_filter() -> None:
    transport = ResponderTransport(_null_aware_portal)
    async with client_for(transport) as client:
        stream = client.iter_list_keyset(
            _NULL_FILTERED, selector=ResultSelector.root(), identity=_IDENTITY, page_size=_PAGE
        )
        async with stream:
            identities = [cast("dict[str, int]", row)["ID"] async for row in stream]

    assert identities == list(range(1, _ROWS + 1, 2))
    assert all(request.method == "crm.deal.list" for request in transport.requests)
    report = stream.report
    assert report is not None
    assert report.keyset_selection is not None
    assert report.keyset_selection.requested_kind is KeysetExecutionKind.AUTO
    assert report.keyset_selection.selected_kind is KeysetExecutionKind.SEQUENTIAL
    assert report.keyset_selection.reason is KeysetSelectionReason.BATCH_UNREPRESENTABLE


@pytest.mark.asyncio
async def test_explicit_fast_keyset_counted_and_verification_refuse_a_none_filter_before_any_request() -> None:
    transport = ResponderTransport(_null_aware_portal)
    async with client_for(transport) as client:
        with pytest.raises(CapabilityError, match=r"None value.*SequentialKeysetExecution"):
            client.iter_list_keyset(
                _NULL_FILTERED,
                selector=ResultSelector.root(),
                identity=_IDENTITY,
                execution=RangeKeysetExecution(StableIntegerKeysetContract()),
            )
        with pytest.raises(CapabilityError, match=r"None value.*use iter_list"):
            client.iter_list_counted(_NULL_FILTERED, page_size=_PAGE)
        with pytest.raises(CapabilityError, match=r"keyset verifier.*None value"):
            await client.verify_keyset_capability(_NULL_FILTERED, selector=ResultSelector.root(), identity=_IDENTITY)

    assert transport.requests == []


@pytest.mark.parametrize(
    "parameters",
    [{"a": None}, {"a": {"b": None}}, {"a": [None]}, {"a]b": 1}, {"a.b": 1}],
    ids=["none", "nested-none", "none-in-list", "bracket-key", "dotted-top-key"],
)
def test_a_form_encoded_request_refuses_what_bracket_encoding_would_change(parameters: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="form-encoded parameters cannot carry") as refused:
        Request.bare("disk.folder.add", parameters, encoding=BodyEncoding.FORM_URLENCODED)
    obstacle = bracket_obstacle(parameters, batch=False)
    assert str(refused.value) == f"form-encoded parameters cannot carry {obstacle}; use JSON encoding"
    # A reference pattern means nothing outside a batch, so a form body sends it as written.
    assert Request.bare("x.y", {"a": "$result[a]"}, encoding=BodyEncoding.FORM_URLENCODED)


# K-2 and T-14: a value no encoding can write is refused where every other value check is, before any engine.
_LONE_SURROGATE = os.fsdecode(b"report-\xff.pdf")
_TOO_LONG = 10**5000


@pytest.mark.parametrize(
    "parameters",
    [{"data": {"NAME": _LONE_SURROGATE}}, {_LONE_SURROGATE: 1}, {"id": _TOO_LONG}, {"ids": [1, _TOO_LONG]}],
    ids=["surrogate-value", "surrogate-key", "long-int", "long-int-in-list"],
)
def test_a_request_refuses_a_value_no_encoding_can_write_when_it_is_built(parameters: dict[str, object]) -> None:
    with pytest.raises(ValueError, match=r"JSON (strings|integers)"):
        Request.bare("disk.folder.addsubfolder", parameters, ReplaySafety.UNSAFE)


def test_binding_values_positional_slots_and_cursors_refuse_an_unencodable_value() -> None:
    with pytest.raises(ValueError, match="lone surrogates"):
        ParameterUpdate(ParameterPath(("filter", "NAME")), _LONE_SURROGATE)
    with pytest.raises(ValueError, match="digit limit"):
        Present(_TOO_LONG)
    with pytest.raises(ValueError, match="lone surrogates"):
        Binding("seeded", (), 1, start_cursor=_LONE_SURROGATE)
