"""§3.12 (C1, C13): ``Request.bare`` and ``Request.v3`` are the constructor with a fixed route."""

from __future__ import annotations
import inspect

import pytest

from b24api import ReplaySafety, Request, ResultSelector, RouteKind
from b24api.contracts import (
    BodyEncoding,
    PositionalArguments,
    PositionalLayout,
    Present,
    RequestHeaders,
    ResultErrorSpec,
    SlotContract,
    SlotShape,
)


def _parameters(function: object, *, drop: set[str]) -> list[tuple[str, object, object]]:
    signature = inspect.signature(function)  # type: ignore[arg-type]
    return [
        (name, parameter.kind, parameter.default)
        for name, parameter in signature.parameters.items()
        if name not in drop
    ]


def test_bare_mirrors_the_constructor_without_its_route() -> None:
    assert _parameters(Request.bare, drop=set()) == _parameters(Request.__init__, drop={"self", "route"})


def test_v3_mirrors_the_constructor_without_route_or_encoding() -> None:
    assert _parameters(Request.v3, drop=set()) == _parameters(Request.__init__, drop={"self", "route", "encoding"})


def test_bare_builds_the_same_request_as_the_constructor() -> None:
    headers = RequestHeaders((("x-trace", "1"),))
    result_error = ResultErrorSpec(ResultSelector.root(), ("code",))

    built = Request.bare(
        "crm.deal.list",
        {"select": ["ID"]},
        ReplaySafety.SAFE,
        encoding=BodyEncoding.FORM_URLENCODED,
        headers=headers,
        result_error=result_error,
    )

    assert built == Request(
        "crm.deal.list",
        {"select": ["ID"]},
        ReplaySafety.SAFE,
        encoding=BodyEncoding.FORM_URLENCODED,
        headers=headers,
        result_error=result_error,
        route=RouteKind.BARE,
    )
    assert Request.bare("user.get") == Request("user.get", route=RouteKind.BARE)


def _positional() -> PositionalArguments:
    layout = PositionalLayout("sample_positional", (SlotContract("id", SlotShape.SCALAR),))
    return PositionalArguments((Present(1),), layout.layout_id, layout=layout)


def test_bare_accepts_positional_arguments() -> None:
    arguments = _positional()

    built = Request.bare("task.elapseditem.getlist", arguments)

    assert built.route is RouteKind.BARE
    assert built.positional == arguments


def test_v3_builds_a_json_mapping_request_on_the_v3_route() -> None:
    built = Request.v3("tasks.task.get", {"id": 1}, ReplaySafety.SAFE)

    assert built == Request("tasks.task.get", {"id": 1}, ReplaySafety.SAFE, route=RouteKind.API_V3)
    assert built.encoding is BodyEncoding.JSON


def test_v3_rejects_positional_arguments_and_an_encoding() -> None:
    with pytest.raises(ValueError, match="API_V3"):
        Request.v3("tasks.task.get", _positional())  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Request.v3("tasks.task.get", encoding=BodyEncoding.FORM_URLENCODED)  # type: ignore[call-arg]


def test_factories_reject_unknown_arguments_and_an_explicit_route() -> None:
    with pytest.raises(TypeError):
        Request.bare("user.get", route=RouteKind.API_V3)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        Request.v3("user.get", timeout=1)  # type: ignore[call-arg]
