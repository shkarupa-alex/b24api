"""Issue #26: mapping-backed result collections on shared (multi-parent) traversals (A26, A26-LOWER)."""

from __future__ import annotations
from typing import TYPE_CHECKING, Any

import pytest

import b24api.references.facade as reference_facade
import b24api.traversal.facade as flat_facade
from b24api import (
    Binding,
    Bitrix24,
    CountedTraversal,
    CursorSpec,
    CursorTraversal,
    KeysetTraversal,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    ResultSelector,
    SequentialTraversal,
    Settings,
)
from b24api.contracts import (
    IdentityCoercion,
    IdentitySpec,
    ReferenceComplete,
    ReferenceItem,
    ResultCollectionShape,
)
from b24api.contracts.request import _MappingValuesResultSelector, _TolerantMappingValuesResultSelector
from b24api.traversal.facade_support import _collection_selector
from tests.scripting import BatchPortal, Envelope, Query

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.contracts import OperationReport, OperationStream, ReferenceOutcome, TraversalSpec

ITEMS = ResultSelector(("items",))
ID = IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.DECIMAL_STRING_INTEGER)
MAPPING_MODES = (ResultCollectionShape.MAPPING_VALUES, ResultCollectionShape.MAPPING_VALUES_OR_EMPTY)
# parent101 serves 10 rows, then 1 row, then an empty page; parent202 serves 1 row, then an empty page.
PAGES = {"101": (10, 1, 0), "202": (1, 0)}
TOTAL_ROWS = 12
WAVES = [2, 2, 1]


def _empty(shape: ResultCollectionShape) -> object:
    # A strict mapping collection ends with an empty mapping; the tolerant mode also accepts ``[]``.
    return [] if shape is ResultCollectionShape.MAPPING_VALUES_OR_EMPTY else {}


def _offset_answer(shape: ResultCollectionShape) -> Callable[[str, Query], Envelope]:
    def answer(method: str, query: Query) -> Envelope:
        assert method == "sonet.list"
        parent = query["PARENT"]
        offset = int(query["start"])
        widths = PAGES[parent]
        page = 0 if offset == 0 else 1 if offset == widths[0] else 2
        rows = widths[page]
        base = int(parent) * 1000 + sum(widths[:page])
        items: object = {str(base + row): {"ID": str(base + row)} for row in range(rows)} or _empty(shape)
        envelope: Envelope = {"result": {"items": items}}
        if rows == 10:  # noqa: PLR2004 - a full page reports its server continuation
            envelope["next"] = offset + rows
        return envelope

    return answer


def _bindings() -> list[Binding[str]]:
    return [
        Binding("parent", updates=(ParameterUpdate(ParameterPath(("PARENT",)), parent),), correlation=parent)
        for parent in PAGES
    ]


def _client(portal: BatchPortal) -> Bitrix24:
    return Bitrix24(Settings(webhook_url="https://fixture.invalid/rest/1/token/"), transport=portal)


def _request() -> Request:
    return Request.bare("sonet.list", {}, ReplaySafety.SAFE)


async def _collect_references(api: Bitrix24, traversal: TraversalSpec) -> list[ReferenceOutcome[str]]:
    stream = api.iter_reference_outcomes(_request(), _bindings(), traversal=traversal)
    async with stream:
        events = [event async for event in stream]
    assert _report(stream).successful
    assert _report(stream).exhausted
    return events


def _report(stream: OperationStream[Any]) -> OperationReport:
    report = stream.report
    assert report is not None
    return report


@pytest.mark.parametrize("shape", MAPPING_MODES)
@pytest.mark.asyncio
async def test_a26_sequential_references_read_mapping_pages_in_one_shared_queue(shape: ResultCollectionShape) -> None:
    portal = BatchPortal(_offset_answer(shape))
    async with _client(portal) as api:
        events = await _collect_references(
            api,
            SequentialTraversal(selector=ITEMS, identity=ID, page_size=10, collection_shape=shape),
        )
    items = [event for event in events if isinstance(event, ReferenceItem)]
    completes = [event for event in events if isinstance(event, ReferenceComplete)]
    assert len(items) == TOTAL_ROWS
    assert sorted(complete.correlation for complete in completes) == ["101", "202"]
    assert all(complete.exhausted for complete in completes)
    assert portal.waves == WAVES


@pytest.mark.parametrize("shape", MAPPING_MODES)
@pytest.mark.asyncio
async def test_a26_flat_list_reads_the_same_mapping_pages(shape: ResultCollectionShape) -> None:
    portal = BatchPortal(_offset_answer(shape))
    async with _client(portal) as api:
        stream = api.iter_list(
            Request.bare("sonet.list", {"PARENT": "101"}, ReplaySafety.SAFE),
            selector=ITEMS,
            identity=ID,
            page_size=10,
            collection_shape=shape,
        )
        async with stream:
            rows = [row async for row in stream]
    assert len(rows) == PAGES["101"][0] + PAGES["101"][1]
    assert _report(stream).exhausted


def _cursor_answer(shape: ResultCollectionShape) -> Callable[[str, Query], Envelope]:
    def answer(method: str, query: Query) -> Envelope:
        assert method == "sonet.list"
        parent = query["PARENT"]
        after = int(query.get("AFTER", "0"))
        widths = PAGES[parent]
        base = int(parent) * 1000
        boundaries = [base + sum(widths[:page]) for page in range(len(widths))]
        page = 0 if after == 0 else boundaries.index(after + 1)
        first = boundaries[page]
        items: object = {str(first + row): {"ID": str(first + row)} for row in range(widths[page])} or _empty(shape)
        return {"result": {"items": items}}

    return answer


@pytest.mark.parametrize("shape", MAPPING_MODES)
@pytest.mark.asyncio
async def test_a26_iter_cursors_passes_the_shape_to_the_shared_cursor_traversal(shape: ResultCollectionShape) -> None:
    portal = BatchPortal(_cursor_answer(shape))
    cursor = CursorSpec(
        parameter_path=ParameterPath(("AFTER",)),
        item_path=("ID",),
        coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
        direction="ascending",
        take="last",
    )
    async with _client(portal) as api:
        stream = api.iter_cursors(
            _request(),
            _bindings(),
            selector=ITEMS,
            cursor=cursor,
            collection_shape=shape,
            page_size=10,
        )
        async with stream:
            events = [event async for event in stream]
    assert sum(isinstance(event, ReferenceItem) for event in events) == TOTAL_ROWS
    assert sum(isinstance(event, ReferenceComplete) for event in events) == len(PAGES)
    assert _report(stream).exhausted
    assert portal.waves == WAVES


@pytest.mark.asyncio
async def test_a26_lower_applies_the_collection_selector_exactly_once_per_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[ResultCollectionShape] = []

    def counting(selector: ResultSelector, shape: ResultCollectionShape) -> ResultSelector:
        calls.append(shape)
        return _collection_selector(selector, shape)

    monkeypatch.setattr(reference_facade, "_collection_selector", counting)
    monkeypatch.setattr(flat_facade, "_collection_selector", counting)
    shape = ResultCollectionShape.MAPPING_VALUES_OR_EMPTY
    portal = BatchPortal(_offset_answer(shape))
    async with _client(portal) as api:
        await _collect_references(
            api,
            SequentialTraversal(selector=ITEMS, identity=ID, page_size=10, collection_shape=shape),
        )
        assert calls == [shape]
        calls.clear()
        stream = api.iter_list(
            Request.bare("sonet.list", {"PARENT": "202"}, ReplaySafety.SAFE),
            selector=ITEMS,
            identity=ID,
            page_size=10,
            collection_shape=shape,
        )
        async with stream:
            assert len([row async for row in stream]) == 1
        assert calls == [shape]


@pytest.mark.parametrize(
    "build",
    [
        lambda selector: SequentialTraversal(selector=selector, collection_shape=ResultCollectionShape.MAPPING_VALUES),
        lambda selector: CountedTraversal(selector=selector, collection_shape=ResultCollectionShape.MAPPING_VALUES),
        lambda selector: KeysetTraversal(selector, ID, collection_shape=ResultCollectionShape.MAPPING_VALUES),
        lambda selector: CursorTraversal(
            selector,
            CursorSpec(ParameterPath(("AFTER",)), ("ID",), IdentityCoercion.EXACT_INTEGER, "ascending", "last"),
            collection_shape=ResultCollectionShape.MAPPING_VALUES,
        ),
    ],
)
@pytest.mark.parametrize("lowered", [_MappingValuesResultSelector(("items",)), _TolerantMappingValuesResultSelector()])
def test_a26_lower_rejects_a_privately_lowered_selector_with_a_mapping_shape(
    build: Callable[[ResultSelector], object],
    lowered: ResultSelector,
) -> None:
    with pytest.raises(ValueError, match="already lowered"):
        build(lowered)


def test_a26_lower_rejects_double_lowering_and_keeps_the_private_sequence_path() -> None:
    lowered = _collection_selector(ITEMS, ResultCollectionShape.MAPPING_VALUES)
    with pytest.raises(ValueError, match="already lowered"):
        _collection_selector(lowered, ResultCollectionShape.MAPPING_VALUES_OR_EMPTY)
    assert _collection_selector(lowered, ResultCollectionShape.SEQUENCE) is lowered
    # The documented private workaround (#26) keeps working: a lowered selector with the default shape.
    traversal = SequentialTraversal(selector=lowered)
    assert traversal.collection_shape is ResultCollectionShape.SEQUENCE
    with pytest.raises(TypeError, match="collection_shape"):
        SequentialTraversal(collection_shape="mapping_values")  # type: ignore[arg-type]
