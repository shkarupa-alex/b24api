"""Reference bindings compose constant sibling filter fields with each binding's own keyset cursor."""

from __future__ import annotations
import ast
import re
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlsplit

import pytest

from b24api import (
    AutoKeysetExecution,
    BatchDispatch,
    Binding,
    Bitrix24,
    DirectDispatch,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    KeysetTraversal,
    ParameterPath,
    ParameterUpdate,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    SequentialKeysetExecution,
    StableIntegerKeysetContract,
    TerminalState,
)
from b24api.contracts import (
    BoundedIdentityRange,
    NotExecutedReason,
    OperationReport,
    PositionalArguments,
    PositionalLayout,
    Present,
    ReferenceComplete,
    ReferenceItem,
    ReferenceNotExecuted,
    SlotContract,
    SlotShape,
    SplitOrderSpec,
    traversal_control_paths,
)
from b24api.encoding import encode_php_query
from b24api.errors import CapabilityError
from b24api.references.binding import _bind_request, _BindingLocalValidationError, binding_update_conflict
from tests.scripting import ResponderTransport, client_for

if TYPE_CHECKING:
    from collections.abc import Iterator

    from b24api.contracts import JsonValue, OperationStream

PAGE = 2
OWNERS = {"D": (7573, (11, 14, 20)), "L": (42, (12, 13, 19, 25))}
FILTER = ParameterPath(("filter",))


def _identity(filter_key: str = "id") -> IdentitySpec:
    return IdentitySpec(("id",), filter_key, filter_key, IdentityCoercion.DECIMAL_STRING_INTEGER)


def _traversal(
    *,
    filter_key: str = "id",
    keyset: KeysetSpec | None = None,
    execution: object = None,
) -> KeysetTraversal:
    return KeysetTraversal(
        selector=ResultSelector(("productRows",)),
        identity=_identity(filter_key),
        page_size=PAGE,
        keyset=keyset or KeysetSpec(filter_path=FILTER, order_path=ParameterPath(("order",))),
        execution=execution or SequentialKeysetExecution(),  # type: ignore[arg-type]
    )


def _owner(owner_type: str, correlation: object = None) -> Binding[object]:
    owner_id = OWNERS[owner_type][0]
    return Binding(
        f"{owner_type} {owner_id}",
        (
            ParameterUpdate(ParameterPath(("filter", "=ownerType")), owner_type),
            ParameterUpdate(ParameterPath(("filter", "=ownerId")), owner_id),
        ),
        owner_type if correlation is None else correlation,
    )


class _ProductRows:
    """Per-owner productrow oracle: rows after the cursor, ascending, at most one page."""

    def __init__(self) -> None:
        self.queries: list[dict[str, str]] = []
        self.physical = 0

    def _answer(self, query: str) -> dict[str, object]:
        decoded = {key: values[0] for key, values in parse_qs(query, keep_blank_values=True).items()}
        self.queries.append(decoded)
        owner_id, ids = OWNERS[decoded["filter[=ownerType]"]]
        assert decoded["filter[=ownerId]"] == str(owner_id)
        assert decoded["order[id]"] == "ASC"
        after = int(decoded.get("filter[>id]", "0"))
        upper = int(decoded.get("filter[<=id]", "1000"))
        rows = [{"id": str(value)} for value in ids if after < value <= upper][:PAGE]
        return {"result": {"productRows": rows}}

    def __call__(self, request: Request) -> dict[str, object]:
        self.physical += 1
        parameters = request.copy_parameters()
        if request.method != "batch":
            query: dict[str | int, object] = dict(parameters.items())
            return self._answer(encode_php_query(query))
        commands = parameters["cmd"]
        assert isinstance(commands, dict)
        results = {key: self._answer(urlsplit(str(command)).query)["result"] for key, command in commands.items()}
        return {"result": {"result": results, "result_error": {}}}


def _client(portal: _ProductRows) -> Bitrix24:
    return client_for(ResponderTransport(portal))


def _request(parameters: dict[str, JsonValue] | None = None) -> Request:
    return Request("crm.item.productrow.list", parameters or {}, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)


async def _drain[T](stream: OperationStream[T]) -> list[T]:
    async with stream:
        return [outcome async for outcome in stream]


# A17.1: every owner keeps its own constants and its own cursor over both dispatch routes.


@pytest.mark.parametrize("dispatch", [DirectDispatch(concurrency=2), BatchDispatch(batch_size=2, concurrency=1)])
@pytest.mark.asyncio
async def test_each_owner_binding_keeps_its_constants_and_its_own_cursor(
    dispatch: DirectDispatch | BatchDispatch,
) -> None:
    portal = _ProductRows()
    async with _client(portal) as client:
        stream = client.iter_references(
            _request(), [_owner("D"), _owner("L")], traversal=_traversal(), dispatch=dispatch
        )
        outcomes = await _drain(stream)

    rows = {
        owner: [item.item for item in outcomes if isinstance(item, ReferenceItem) and item.correlation == owner]
        for owner in OWNERS
    }
    assert rows == {owner: [{"id": str(value)} for value in ids] for owner, (_, ids) in OWNERS.items()}
    completions = {item.correlation: item.row_count for item in outcomes if isinstance(item, ReferenceComplete)}
    assert completions == {owner: len(ids) for owner, (_, ids) in OWNERS.items()}
    cursors = {
        owner: [query.get("filter[>id]") for query in portal.queries if query["filter[=ownerType]"] == owner]
        for owner in OWNERS
    }
    # Each owner advances along its own rows only; no cursor crosses into the other owner.
    assert cursors == {"D": [None, "14", "20"], "L": [None, "13", "25"]}
    assert stream.report is not None
    assert stream.report.state is TerminalState.COMPLETED
    if isinstance(dispatch, BatchDispatch):
        assert portal.physical < len(portal.queries)
    else:
        assert portal.physical == len(portal.queries)


# A17.2: an update that could reach the cursor or reshape the filter fails locally, before any driver exists.


REJECTED_UPDATES = (
    ParameterUpdate(FILTER, {"=ownerId": 1}),
    ParameterUpdate(ParameterPath(("filter", "=ownerId", "x")), 1),
    ParameterUpdate(ParameterPath(("filter", 0)), 1),
    ParameterUpdate(ParameterPath(("filter", "123")), 1),
    ParameterUpdate(ParameterPath(("filter", "LOGIC")), "OR"),
    ParameterUpdate(ParameterPath(("filter", "and")), 1),
    ParameterUpdate(ParameterPath(("filter", "=OR")), 1),
    ParameterUpdate(ParameterPath(("filter", "id")), 1),
    ParameterUpdate(ParameterPath(("filter", ">id")), 1),
    ParameterUpdate(ParameterPath(("filter", ">ID")), 0),
    ParameterUpdate(ParameterPath(("filter", "<=id")), 1),
    ParameterUpdate(ParameterPath(("filter", "=id")), 1),
    ParameterUpdate(ParameterPath(("filter", "!=Id")), 1),
    ParameterUpdate(ParameterPath(("filter", "!%id")), 1),
    ParameterUpdate(ParameterPath(("filter", "=ownerId")), {"nested": 1}),
    ParameterUpdate(ParameterPath(("filter", "=ownerId")), [[1]]),
    ParameterUpdate(ParameterPath(("filter", "@ownerId")), [{"nested": 1}]),
)


@pytest.mark.parametrize("update", REJECTED_UPDATES, ids=lambda update: repr(update.path.path))
def test_unsafe_keyset_filter_updates_have_a_value_free_local_reason(update: ParameterUpdate) -> None:
    reason = binding_update_conflict(_traversal(), update)

    assert reason is not None
    assert "nested" not in reason
    assert "OR" not in reason


@pytest.mark.parametrize(
    ("update", "filter_key"),
    [
        (ParameterUpdate(ParameterPath(("filter", "=ownerType")), "D"), "id"),
        (ParameterUpdate(ParameterPath(("filter", "=ownerId")), 7573), "id"),
        (ParameterUpdate(ParameterPath(("FILTER", "@ownerId")), [1, "2", None, True, 1.5]), "id"),
        (ParameterUpdate(ParameterPath(("filter", "!%title")), None), "id"),
        (ParameterUpdate(ParameterPath(("filter", "=idx")), 1), "id"),
        (ParameterUpdate(ParameterPath(("filter", "владелец")), 1), "id"),
        (ParameterUpdate(ParameterPath(("filter", "=владелец")), 1), "идентификатор"),
        (ParameterUpdate(ParameterPath(("select",)), ["id"]), "id"),
    ],
)
def test_simple_sibling_fields_compose_with_the_cursor(update: ParameterUpdate, filter_key: str) -> None:
    assert binding_update_conflict(_traversal(filter_key=filter_key), update) is None


@pytest.mark.parametrize(
    ("update", "filter_key"),
    [
        # An operator-bearing cursor key is not comparable by field name, so nothing may sit beside it.
        (ParameterUpdate(ParameterPath(("filter", "=ownerId")), 1), "=id"),
        (ParameterUpdate(ParameterPath(("filter", ">идентификатор")), 1), "идентификатор"),
        (ParameterUpdate(ParameterPath(("filter", "ИДЕНТИФИКАТОР")), 1), "идентификатор"),
    ],
)
def test_cursor_field_variants_are_refused_for_every_filter_key(update: ParameterUpdate, filter_key: str) -> None:
    assert binding_update_conflict(_traversal(filter_key=filter_key), update) is not None


@pytest.mark.parametrize("update", REJECTED_UPDATES, ids=lambda update: repr(update.path.path))
@pytest.mark.parametrize("dispatch", [DirectDispatch(concurrency=2), BatchDispatch(batch_size=2)])
@pytest.mark.asyncio
async def test_only_the_offending_binding_fails_locally_without_io(
    update: ParameterUpdate,
    dispatch: DirectDispatch | BatchDispatch,
) -> None:
    portal = _ProductRows()
    offending = object()
    async with _client(portal) as client:
        stream = client.iter_reference_outcomes(
            _request(),
            [_owner("D"), Binding("offending", (update,), offending)],
            traversal=_traversal(),
            dispatch=dispatch,
        )
        outcomes = await _drain(stream)

    [local] = [outcome for outcome in outcomes if not isinstance(outcome, ReferenceItem | ReferenceComplete)]
    # A local refusal, not a per-reference CapabilityError from a driver's repeated preflight.
    assert isinstance(local, ReferenceNotExecuted)
    assert local.correlation is offending
    assert local.reason is NotExecutedReason.LOCAL_VALIDATION_FAILED
    assert {query["filter[=ownerType]"] for query in portal.queries} == {"D"}
    assert [item.correlation for item in outcomes if isinstance(item, ReferenceComplete)] == ["D"]


# A17.3: every other traversal control stays exclusive.


@pytest.mark.parametrize(
    ("path", "keyset"),
    [
        (("order",), None),
        (("order", "id"), None),
        (("ORDER", "title"), None),
        (("start",), None),
        (("limit",), KeysetSpec(limit_path=ParameterPath(("limit",)))),
        (
            ("sort",),
            KeysetSpec(order_path=None, split_order=SplitOrderSpec(ParameterPath(("sort",)), ParameterPath(("dir",)))),
        ),
        (
            ("dir",),
            KeysetSpec(order_path=None, split_order=SplitOrderSpec(ParameterPath(("sort",)), ParameterPath(("dir",)))),
        ),
    ],
)
def test_order_start_limit_and_split_order_remain_exclusive(
    path: tuple[str, ...],
    keyset: KeysetSpec | None,
) -> None:
    update = ParameterUpdate(ParameterPath(path), "value")

    assert binding_update_conflict(_traversal(keyset=keyset), update) == (
        "binding update collides with a traversal control path"
    )


# A17.4: the base request keeps its existing global preflight; a positional binding stays a local refusal.


@pytest.mark.parametrize("base_filter", [{"<=id": 19}, {"=id": [14, 19]}])
@pytest.mark.asyncio
async def test_base_identity_constraints_other_than_the_cursor_remain_admitted(
    base_filter: dict[str, JsonValue],
) -> None:
    portal = _ProductRows()
    async with _client(portal) as client:
        outcomes = await _drain(
            client.iter_reference_outcomes(
                _request({"filter": base_filter}),
                [_owner("D")],
                traversal=_traversal(),
                dispatch=DirectDispatch(concurrency=1),
            )
        )

    assert [type(outcome) for outcome in outcomes][-1] is ReferenceComplete
    [key] = base_filter
    assert all(any(name.startswith(f"filter[{key}]") for name in query) for query in portal.queries)
    assert any("filter[>id]" in query for query in portal.queries)


def _refused_bindings() -> Iterator[Binding[object]]:
    raise AssertionError("a globally refused base request must not read bindings")
    yield  # pragma: no cover


@pytest.mark.parametrize("parameters", [{"filter": [1]}, {"filter": ">ID"}, {"filter": {">ID": 5}}])
@pytest.mark.asyncio
async def test_base_cursor_collision_and_non_mapping_filter_fail_before_bindings(
    parameters: dict[str, JsonValue],
) -> None:
    portal = _ProductRows()
    async with _client(portal) as client:
        with pytest.raises(CapabilityError):
            client.iter_reference_outcomes(
                _request(parameters),
                _refused_bindings(),
                traversal=_traversal(),
                dispatch=DirectDispatch(concurrency=1),
            )

    assert portal.physical == 0


def test_positional_request_binding_remains_a_local_refusal() -> None:
    layout = PositionalLayout(
        "productrow.positional.keyset.v1",
        (SlotContract("order", SlotShape.OBJECT), SlotContract("filter", SlotShape.OBJECT)),
        control_paths=frozenset({(0, "ID"), (1, ">ID")}),
    )
    request = Request(
        "crm.item.productrow.list",
        PositionalArguments((Present({}), Present({})), layout.layout_id, layout=layout),
        replay_safety=ReplaySafety.SAFE,
        route=RouteKind.BARE,
    )
    traversal = KeysetTraversal(
        selector=ResultSelector.root(),
        identity=IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.DECIMAL_STRING_INTEGER),
        page_size=PAGE,
        keyset=KeysetSpec(filter_path=ParameterPath((1,)), order_path=ParameterPath((0,)), start_suppression_path=None),
    )
    binding = Binding("owner", (ParameterUpdate(ParameterPath((1, "=ownerId")), 7),), 1)

    # The sibling key is admitted; the positional request still cannot take a named binding update.
    assert binding_update_conflict(traversal, binding.updates[0]) is None
    with pytest.raises(_BindingLocalValidationError):
        _bind_request(request, binding, 0, traversal)


# A17.5: the public container helper and the reference execution limits are unchanged.


def test_control_paths_still_name_the_whole_filter_container() -> None:
    assert traversal_control_paths(_traversal()) == (
        FILTER,
        ParameterPath(("order",)),
        ParameterPath(("start",)),
    )


@pytest.mark.parametrize(
    "traversal",
    [
        _traversal(execution=RangeKeysetExecution(StableIntegerKeysetContract())),
        _traversal(execution=PartitionedKeysetExecution(StableIntegerKeysetContract())),
        _traversal(execution=AutoKeysetExecution(StableIntegerKeysetContract())),
        _traversal(
            keyset=KeysetSpec(
                boundary=BoundedIdentityRange.capture(
                    _request(),
                    filter_path=FILTER,
                    upper_id=100,
                    lower_exclusive=0,
                    fence_path=ParameterPath(("filter", "<=id")),
                    source_version="qualified-test-v1",
                ),
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_references_still_refuse_fast_and_bounded_keyset(traversal: KeysetTraversal) -> None:
    portal = _ProductRows()
    async with _client(portal) as client:
        with pytest.raises(CapabilityError):
            client.iter_reference_outcomes(_request(), [_owner("D")], traversal=traversal)

    assert portal.physical == 0


def test_rejected_update_reason_is_one_of_the_stable_local_messages() -> None:
    reasons: set[str | None] = {binding_update_conflict(_traversal(), update) for update in REJECTED_UPDATES}
    assert None not in reasons
    assert all(isinstance(reason, str) and reason.startswith(("binding", "keyset")) for reason in reasons)


# The per-owner productrow recipe runs verbatim from the user documentation against the owner oracle.

RECIPES = Path(__file__).resolve().parents[1] / "docs" / "recipes.md"
PYTHON_BLOCK = re.compile(r"```python\n(.*?)\n```", re.DOTALL)


@pytest.mark.asyncio
async def test_productrow_recipe_runs_verbatim_with_its_own_cursor() -> None:
    blocks = PYTHON_BLOCK.findall(RECIPES.read_text("utf-8"))
    [source] = [block for block in blocks if "crm.item.productrow.list" in block]
    portal = _ProductRows()
    namespace: dict[str, object] = {}
    async with _client(portal) as client:
        namespace["client"] = client
        result = eval(  # noqa: S307 - exact trusted repository documentation source
            compile(source, str(RECIPES), "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT),
            namespace,
        )
        if result is not None:
            await result

    assert [query.get("filter[>id]") for query in portal.queries] == [None, "14", "20"]
    assert {(query["filter[=ownerType]"], query["filter[=ownerId]"]) for query in portal.queries} == {("D", "7573")}
    report = getattr(namespace["stream"], "report", None)
    assert isinstance(report, OperationReport)
    assert report.state is TerminalState.COMPLETED
