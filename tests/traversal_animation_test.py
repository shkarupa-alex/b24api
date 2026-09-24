"""The list traversal animation replays executed traces, not a drawing (spec §3.13).

One scripted portal serves exactly the inputs the SVG illustrates: 1,000 dense integer IDs, 50 rows
per page, the BARE route and the portal batch capacity. Each lane's logical commands and physical
sends are recorded here, and the SVG's semantic labels (HTTP counts, batch sizes, the final keyset
call) must match them. The animation's timing is not compared.
"""

from __future__ import annotations
import html
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs

import pytest

from b24api import (
    BoundedIdentityRange,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    ParameterPath,
    Request,
    ResultSelector,
    SequentialKeysetExecution,
    TraversalAssurance,
)
from tests.scripting import ResponderTransport, client_for

ROOT = Path(__file__).resolve().parents[1]
SVG = ROOT / "list-traversal-comparison.svg"
IDS = tuple(range(1, 1001))
PAGE = 50
REQUEST = Request.bare("item.list")
IDENTITY = IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.EXACT_INTEGER)
KEYSET = KeysetSpec(limit_path=ParameterPath(("limit",)))
TIMES = "\N{MULTIPLICATION SIGN}"
_TEXT = re.compile(r"<text[^>]*>([^<]*)</text>")


@dataclass(frozen=True)
class _Query:
    """One list command: an offset (``start >= 0``) or a keyset filter with an order and a limit."""

    start: int
    filters: Mapping[str, int]
    descending: bool
    limit: int

    @property
    def offset(self) -> bool:
        return self.start >= 0 and not self.filters


def _integer(value: object) -> int:
    assert isinstance(value, int)
    return value


def _ids(rows: Iterable[object]) -> list[int]:
    identities = []
    for row in rows:
        assert isinstance(row, Mapping)
        identities.append(_integer(row["ID"]))
    return identities


def _query(parameters: Mapping[str, object]) -> _Query:
    filters = parameters.get("filter", {})
    order = parameters.get("order", {"ID": "ASC"})
    assert isinstance(filters, Mapping)
    assert order in ({"ID": "ASC"}, {"ID": "DESC"})
    return _Query(
        start=_integer(parameters.get("start", 0)),
        filters={str(key): _integer(value) for key, value in filters.items()},
        descending=order == {"ID": "DESC"},
        limit=_integer(parameters.get("limit", PAGE)),
    )


def _command(encoded: object) -> dict[str, object]:
    """Decode one batch command's PHP query into the nested parameters a direct call carries."""
    assert isinstance(encoded, str)
    parameters: dict[str, object] = {}
    for key, values in parse_qs(encoded.partition("?")[2]).items():
        name, _, rest = key.partition("[")
        value: object = int(values[0]) if values[0].lstrip("-").isdigit() else values[0]
        if rest:
            nested = parameters.setdefault(name, {})
            assert isinstance(nested, dict)
            nested[rest.rstrip("]")] = value
        else:
            parameters[name] = value
    return parameters


def _parameters(request: Request) -> Mapping[str, object]:
    return request.copy_parameters()


def _commands(request: Request) -> dict[str, dict[str, object]]:
    commands = _parameters(request)["cmd"]
    assert isinstance(commands, Mapping)
    return {str(key): _command(value) for key, value in commands.items()}


def _page(query: _Query) -> list[dict[str, int]]:
    if query.offset:
        return [{"ID": value} for value in IDS[query.start : query.start + PAGE]]
    lower = query.filters.get(">ID", 0)
    below = min(query.filters.get("<ID", len(IDS) + 1), query.filters.get("<=ID", len(IDS)) + 1)
    selected = [value for value in IDS if lower < value < below]
    return [{"ID": value} for value in sorted(selected, reverse=query.descending)[: query.limit]]


def _portal(request: Request) -> object:
    if request.method == "batch":
        queries = {key: _query(command) for key, command in _commands(request).items()}
        return {
            "result": {
                "result": {key: _page(query) for key, query in queries.items()},
                "result_error": [],
                "result_total": {key: len(IDS) for key, query in queries.items() if query.offset},
            }
        }
    query = _query(_parameters(request))
    envelope: dict[str, object] = {"result": _page(query), "total": len(IDS)}
    if query.offset and query.start + PAGE < len(IDS):
        envelope["next"] = query.start + PAGE
    return envelope


def _sends(transport: ResponderTransport) -> list[tuple[str, int]]:
    """Physical sends as ``("call", 1)`` or ``("batch", commands)``."""
    return [
        ("batch", len(_commands(request))) if request.method == "batch" else ("call", 1)
        for request in transport.requests
    ]


def _svg_labels() -> list[str]:
    return [html.unescape(text) for text in _TEXT.findall(SVG.read_text(encoding="utf-8"))]


async def _lanes() -> dict[str, ResponderTransport]:
    lanes = {name: ResponderTransport(_portal) for name in ("iter_list", "iter_list_counted", "iter_list_keyset")}
    async with client_for(lanes["iter_list"]) as client:
        rows = [row async for row in client.iter_list(REQUEST, identity=IDENTITY, page_size=PAGE)]
        assert _ids(rows) == list(IDS)
    async with client_for(lanes["iter_list_counted"]) as client:
        rows = [row async for row in client.iter_list_counted(REQUEST, identity=IDENTITY, page_size=PAGE)]
        assert sorted(_ids(rows)) == list(IDS)
    async with client_for(lanes["iter_list_keyset"]) as client:
        stream = client.iter_list_keyset(
            REQUEST, selector=ResultSelector.root(), identity=IDENTITY, page_size=PAGE, keyset=KEYSET
        )
        rows = [row async for row in stream]
        assert sorted(_ids(rows)) == list(IDS)
        assert stream.report is not None
        assert stream.report.exhausted
        assert stream.report.assurance is TraversalAssurance.IDENTITY_EXACT
    return lanes


@pytest.mark.asyncio
async def test_each_lane_sends_what_the_animation_draws() -> None:
    lanes = await _lanes()

    assert _sends(lanes["iter_list"]) == [("call", 1)] * 21
    assert _sends(lanes["iter_list_counted"]) == [("call", 1), ("batch", 19)]
    assert _sends(lanes["iter_list_keyset"]) == [("batch", 2), ("batch", 19), ("call", 1)]
    bounds, _ranges, confirmation = lanes["iter_list_keyset"].requests
    assert [_query(command) for command in _commands(bounds).values()] == [
        _Query(start=-1, filters={}, descending=descending, limit=PAGE) for descending in (False, True)
    ]
    # The terminal witness: one direct call above the last ID, answered with an empty page.
    assert _query(_parameters(confirmation)).filters == {">ID": IDS[-1]}
    assert _page(_query(_parameters(confirmation))) == []


@pytest.mark.asyncio
async def test_animation_labels_match_the_executed_traces() -> None:
    lanes = await _lanes()
    labels = _svg_labels()
    keyset = _sends(lanes["iter_list_keyset"])
    last_id = _query(_parameters(lanes["iter_list_keyset"].requests[-1])).filters[">ID"]

    assert [label for label in labels if label.endswith(" HTTP")] == [
        f"{len(lanes[name].requests)} HTTP" for name in lanes
    ]
    assert [label for label in labels if label.startswith(f"batch {TIMES}")] == [
        f"batch {TIMES}{size}"
        for name in ("iter_list_counted", "iter_list_keyset")
        for kind, size in _sends(lanes[name])
        if kind == "batch"
    ]
    assert f"bounds {TIMES}{keyset[0][1]} → ranges" in labels
    assert f"call · ID>{last_id}" in labels
    assert "keyset: ∅ confirms the end · a bounded range stops at its upper ID" in labels


@pytest.mark.asyncio
async def test_the_readme_caption_quotes_the_same_traces() -> None:
    lanes = await _lanes()
    readme = " ".join((ROOT / "README.md").read_text(encoding="utf-8").split())
    keyset = _sends(lanes["iter_list_keyset"])
    last_id = _query(_parameters(lanes["iter_list_keyset"].requests[-1])).filters[">ID"]

    assert f"sends {len(lanes['iter_list'].requests) - 1} pages and one empty confirmation" in readme
    assert f"one batch of {_sends(lanes['iter_list_counted'])[1][1]} pages" in readme
    assert f"the {keyset[1][1]} ranges between them in a second" in readme
    assert f"one call for `ID > {last_id}`" in readme
    for name in lanes:
        assert f"({len(lanes[name].requests)} HTTP)" in readme


@pytest.mark.asyncio
async def test_a_bounded_sequential_range_stops_at_its_upper_id_without_confirmation() -> None:
    transport = ResponderTransport(_portal)
    boundary = BoundedIdentityRange.capture(
        REQUEST,
        filter_path=ParameterPath(("filter",)),
        upper_id=IDS[-1],
        lower_exclusive=0,
        fence_path=ParameterPath(("filter", "<=ID")),
        source_version="animation-fixture-v1",
    )
    async with client_for(transport) as client:
        stream = client.iter_list_keyset(
            REQUEST,
            selector=ResultSelector.root(),
            identity=IDENTITY,
            page_size=PAGE,
            keyset=KeysetSpec(limit_path=ParameterPath(("limit",)), boundary=boundary),
            execution=SequentialKeysetExecution(),
        )
        rows = [row async for row in stream]

    assert _ids(rows) == list(IDS)
    assert _sends(transport) == [("call", 1)] * (len(IDS) // PAGE)
    assert all(_page(_query(_parameters(request))) for request in transport.requests)
    assert stream.report is not None
    assert stream.report.assurance is TraversalAssurance.BOUNDED_RANGE_OBSERVED
