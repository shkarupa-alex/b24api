"""``ScriptedTransport`` matches what a call sends, as an HTTP transport would send it, and names a mismatch."""

from __future__ import annotations

import pytest

from b24api import ReplaySafety, Request
from b24api.contracts import RequestHeaders
from b24api.contracts.wire import BodyEncoding
from b24api.errors import B24ApiError
from b24api.testing import ScriptedExchange, ScriptedTransport
from tests.scripting import client_for

_ORDERED = Request.bare("crm.deal.list", {"order": {"DATE_CREATE": "ASC", "ID": "DESC"}}, ReplaySafety.SAFE)
_REVERSED = Request.bare("crm.deal.list", {"order": {"ID": "DESC", "DATE_CREATE": "ASC"}}, ReplaySafety.SAFE)


@pytest.mark.asyncio
async def test_form_bodies_and_scoped_headers_are_served_as_an_http_transport_serves_them() -> None:
    form = Request.bare(
        "disk.folder.add", {"data": {"NAME": "x"}}, ReplaySafety.SAFE, encoding=BodyEncoding.FORM_URLENCODED
    )
    scoped = Request.bare("user.current", replay_safety=ReplaySafety.SAFE, headers=RequestHeaders({"X-Trace": "abc"}))
    transport = ScriptedTransport(
        (ScriptedExchange.json(form, {"result": 1}), ScriptedExchange.json(scoped, {"result": 2}))
    )
    async with client_for(transport) as client:
        assert await client.call(form) == 1
        assert await client.call(scoped) == 2  # noqa: PLR2004 - the second fixture's result

    transport.assert_exhausted()


@pytest.mark.asyncio
async def test_an_unmatched_call_is_named_first_and_calls_keep_the_key_order_they_were_sent_in() -> None:
    # Equal as Request values, so either matches the fixture; PHP reads ``order`` in the order it is sent.
    assert _ORDERED == _REVERSED
    other = Request.bare("crm.deal.get", {"id": 1}, ReplaySafety.SAFE)
    transport = ScriptedTransport((ScriptedExchange.json(_ORDERED, {"result": ["ordered"]}),))
    async with client_for(transport) as client:
        with pytest.raises(B24ApiError):
            await client.call(other)
        assert await client.call(_REVERSED) == ["ordered"]

    assert [call.request for call in transport.unexpected] == [other]
    assert list(transport.calls[-1].copy_parameters()["order"]) == ["ID", "DATE_CREATE"]
    with pytest.raises(AssertionError, match=r"1 unexpected scripted request\(s\): crm.deal.get"):
        transport.assert_exhausted()


def test_a_batch_fixture_refuses_a_command_classic_batch_would_refuse() -> None:
    clearing = Request.bare("crm.deal.update", {"id": 1, "fields": {"TITLE": None}}, ReplaySafety.SAFE)

    with pytest.raises(ValueError, match="None value"):
        ScriptedExchange.batch((clearing,), (True,), total=0)
