"""Contract values refuse a bare string where they expect parts, and stay frozen however they are built."""

from __future__ import annotations
from typing import TYPE_CHECKING, cast

import pytest

from b24api import IdentitySpec, ParameterPath, ResultSelector, RetryPolicy
from b24api.contracts import (
    AmbiguityPolicy,
    Binding,
    CompositeIdentitySpec,
    CursorSpec,
    FrozenMapping,
    IdentityCoercion,
    IdentityComponent,
    InclusiveWindowTraversal,
    ParameterUpdate,
    ResultErrorSpec,
)
from b24api.contracts.positional import PositionalArguments, PositionalLayout, Present, SlotContract, SlotShape

if TYPE_CHECKING:
    from b24api.contracts.json import JsonValue

_CURSOR = ParameterPath(("filter", ">ID"))
_LAYOUT = PositionalLayout("layout", (SlotContract("a", SlotShape.ANY),), frozenset({(0, "a")}))
_ARGUMENTS = PositionalArguments((Present({"a": 1}),), "layout", layout=_LAYOUT)


@pytest.mark.parametrize(
    "build",
    [
        lambda: ParameterPath("OWNER"),  # type: ignore[arg-type]
        lambda: ParameterPath(b"OWNER"),  # type: ignore[arg-type]
        lambda: ResultSelector("items"),  # type: ignore[arg-type]
        lambda: IdentitySpec("ID", "ID", "ID"),  # type: ignore[arg-type]
        lambda: IdentityComponent("ID"),  # type: ignore[arg-type]
        lambda: CompositeIdentitySpec("A"),  # type: ignore[arg-type]
        lambda: CompositeIdentitySpec("ABCDEFGHI"),  # type: ignore[arg-type]
        lambda: ResultErrorSpec(ResultSelector(), "error"),  # type: ignore[arg-type]
        lambda: ResultErrorSpec(ResultSelector(), ("error",), "description"),  # type: ignore[arg-type]
        lambda: CursorSpec(_CURSOR, "ID", IdentityCoercion.EXACT_INTEGER, "ascending", "last"),  # type: ignore[arg-type]
        lambda: RetryPolicy(transient_api_codes="QUERY_LIMIT_EXCEEDED"),  # type: ignore[arg-type]
        lambda: AmbiguityPolicy(refusal_api_codes="QUERY_LIMIT_EXCEEDED"),  # type: ignore[arg-type]
        lambda: PositionalLayout("layout", (SlotContract("a", SlotShape.ANY),), frozenset({"0a"})),  # type: ignore[arg-type]
        lambda: _ARGUMENTS.write_control("0a", 1),  # type: ignore[arg-type]
        lambda: _ARGUMENTS.write_control(b"0a", 1),  # type: ignore[arg-type]
        lambda: InclusiveWindowTraversal(None, None, "DATE", None, None, None, 1),  # type: ignore[arg-type]
    ],
    ids=[
        "parameter-path",
        "parameter-path-bytes",
        "result-selector",
        "identity-item-path",
        "identity-component",
        "composite-components-short",
        "composite-components-long",
        "result-error-code-path",
        "result-error-description-path",
        "cursor-item-path",
        "retry-api-codes",
        "ambiguity-api-codes",
        "positional-control-path",
        "positional-write-control",
        "positional-write-control-bytes",
        "inclusive-window-timestamp-path",
    ],
)
def test_a_bare_string_where_a_sequence_of_parts_is_expected_is_refused(build: object) -> None:
    with pytest.raises(TypeError, match="not a single string"):
        cast("type", build)()


def test_a_sequence_of_parts_is_still_accepted() -> None:
    # Any non-text sequence is read as its parts, a list as a tuple is.
    assert ParameterPath(["filter", "OWNER"]).path == ("filter", "OWNER")  # type: ignore[arg-type]
    codes = ["QUERY_LIMIT_EXCEEDED"]
    assert RetryPolicy(transient_api_codes=codes).transient_api_codes == {"query_limit_exceeded"}  # type: ignore[arg-type]


def test_a_binding_start_cursor_is_frozen_hashable_and_detached_from_the_callers_value() -> None:
    cursor: dict[str, JsonValue] = {"a": [1, 2]}
    binding = Binding("seeded", (), 1, start_cursor=cursor)
    cast("list[int]", cursor["a"]).append(3)

    assert binding.start_cursor == FrozenMapping({"a": [1, 2]})
    assert hash(binding) == hash(Binding("seeded", (), 1, start_cursor={"a": [1, 2]}))
    with pytest.raises(AttributeError):
        cast("tuple[int, ...]", cast("FrozenMapping", binding.start_cursor)["a"]).append(4)  # type: ignore[attr-defined]


def test_a_frozen_mapping_built_directly_freezes_what_it_holds() -> None:
    nested: dict[str, object] = {"a": [1, 2], "b": {"c": 1}}
    frozen = FrozenMapping(nested)
    cast("list[int]", nested["a"]).append(3)

    assert frozen["a"] == (1, 2)
    assert isinstance(frozen["b"], FrozenMapping)
    assert frozen == FrozenMapping({"a": (1, 2), "b": {"c": 1}})
    assert hash(frozen) == hash(FrozenMapping({"a": [1, 2], "b": {"c": 1}}))
    with pytest.raises(TypeError, match="keys must be strings"):
        FrozenMapping({1: 2})  # type: ignore[dict-item]


def test_a_parameter_update_still_freezes_its_value() -> None:
    value: dict[str, object] = {"ids": [1]}
    update = ParameterUpdate(ParameterPath(("filter",)), value)
    cast("list[int]", value["ids"]).append(2)

    assert update.value == {"ids": [1]}
