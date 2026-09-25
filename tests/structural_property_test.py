"""Structural properties of frozen JSON, PHP query encoding and keyset window planning (B16).

Each property compares the implementation with an independent oracle written here:

- a request's parameters survive freezing and thawing exactly, stay detached from the caller's
  objects, and frozen equality keeps the JSON scalar type (``1``, ``1.0`` and ``true`` differ);
- ``encode_php_query`` produces exactly the ``(bracket path, text)`` pairs of a direct flattening of
  the value, in order, and nothing a PHP parser would read differently;
- numeric windows and anchor lanes cover every integer strictly inside the fence exactly once, in
  output order, and each window owns at most ``width - 1`` integers.

The redactor's own properties live in ``redaction_property_test.py`` (§3.9).
"""

from __future__ import annotations
import copy
from collections.abc import Mapping, Sequence
from urllib.parse import parse_qsl

from hypothesis import given, settings
from hypothesis import strategies as st

from b24api import Request, RouteKind
from b24api.encoding import encode_php_query
from b24api.traversal.keyset_fast_plan import plan_windows, window_count
from b24api.traversal.keyset_geometry import partition_lane_specs

_keys = st.text(min_size=1, max_size=8)
_scalars = st.none() | st.booleans() | st.integers() | st.floats(allow_nan=False, allow_infinity=False) | st.text()
_json = st.recursive(
    _scalars,
    lambda children: st.lists(children, max_size=4) | st.dictionaries(_keys, children, max_size=4),
    max_leaves=25,
)
_parameters = st.dictionaries(_keys, _json, max_size=6)


def _typed(value: object) -> object:
    """Oracle for type-sensitive JSON equality: tag every scalar with its JSON type."""
    if isinstance(value, Mapping):
        return ("object", sorted((key, _typed(item)) for key, item in value.items()))
    if isinstance(value, Sequence) and not isinstance(value, str):
        return ("array", [_typed(item) for item in value])
    if isinstance(value, bool) or value is None:
        return (type(value).__name__, value)
    if isinstance(value, float):
        return ("number", value.hex())
    return (type(value).__name__, value)


@settings(max_examples=150)
@given(_parameters)
def test_request_parameters_round_trip_exactly_and_stay_detached(parameters: dict[str, object]) -> None:
    original = copy.deepcopy(parameters)
    request = Request("example.get", parameters, route=RouteKind.BARE)
    parameters["injected"] = "after construction"

    thawed = request.copy_parameters()

    assert _typed(thawed) == _typed(original)
    thawed["injected"] = "after copy"
    assert _typed(request.copy_parameters()) == _typed(original)


@settings(max_examples=150)
@given(_parameters, _parameters)
def test_frozen_equality_and_hash_follow_typed_json_equality(
    first: dict[str, object], second: dict[str, object]
) -> None:
    left = Request("example.get", first, route=RouteKind.BARE)
    right = Request("example.get", second, route=RouteKind.BARE)

    twin = Request("example.get", copy.deepcopy(first), route=RouteKind.BARE)

    assert (left == right) is (_typed(first) == _typed(second))
    assert left == twin
    assert hash(left) == hash(twin)


def test_frozen_equality_keeps_the_json_scalar_type() -> None:
    requests = [Request("example.get", {"value": value}, route=RouteKind.BARE) for value in (1, 1.0, True)]

    assert len(set(requests)) == len(requests)


def _flatten(value: object, path: str) -> list[tuple[str, str]]:
    """Oracle: PHP bracket pairs of one value, skipping ``None`` and empty containers."""
    if value is None:
        return []
    if isinstance(value, Sequence) and not isinstance(value, str):
        value = dict(enumerate(value))
    if isinstance(value, Mapping):
        return [pair for key, item in value.items() for pair in _flatten(item, f"{path}[{key}]")]
    text = "1" if value is True else "0" if value is False else str(value)
    return [(path, text)]


_query_keys = st.text(st.characters(codec="utf-8", exclude_characters="[]"), min_size=1, max_size=8)
_query_json = st.recursive(
    _scalars,
    lambda children: st.lists(children, max_size=4) | st.dictionaries(_query_keys, children, max_size=4),
    max_leaves=25,
)


@settings(max_examples=200)
@given(st.dictionaries(_query_keys, _query_json, max_size=6))
def test_php_query_is_the_ordered_flattening_of_the_value(parameters: dict[str, object]) -> None:
    expected = [pair for key, value in parameters.items() for pair in _flatten(value, key)]

    query: dict[str | int, object] = {}
    query.update(parameters)
    encoded = encode_php_query(query)

    assert parse_qsl(encoded, keep_blank_values=True) == expected
    assert (encoded == "") is (not expected)


def _exact(bound: int | None) -> int:
    assert bound is not None
    return bound


def _owned(lower: int | None, upper: int | None) -> range:
    return range(_exact(lower) + 1, _exact(upper))


@settings(max_examples=300)
@given(st.integers(-(10**15), 10**15), st.integers(0, 2_000), st.integers(2, 200))
def test_windows_partition_the_open_fence_exactly_in_order(lo: int, span: int, width: int) -> None:
    upper = lo + span + 1
    windows = plan_windows(lo=lo, upper_exclusive=upper, width=width)
    owned = [list(_owned(window.bounds.lower_exclusive, window.bounds.upper_exclusive)) for window in windows]

    assert [value for values in owned for value in values] == list(range(lo + 1, upper))
    assert all(0 < len(values) <= width - 1 for values in owned)
    assert len(windows) == window_count(lo=lo, upper_exclusive=upper, width=width)
    assert [window.ordinal for window in windows] == list(range(len(windows)))


@settings(max_examples=300)
@given(
    lo=st.integers(-(10**9), 10**9),
    span=st.integers(0, 500),
    offsets=st.lists(st.integers(-600, 600), max_size=30),
    descending=st.booleans(),
)
def test_anchor_lanes_own_every_fenced_identity_once_in_output_order(
    *, lo: int, span: int, offsets: list[int], descending: bool
) -> None:
    upper = lo + span + 1
    anchors = tuple(lo + value for value in offsets)
    lanes = partition_lane_specs(lo=lo, upper_exclusive=upper, anchors=anchors, descending=descending)
    retained = [lane.retained_upper_anchor for lane in lanes if lane.retained_upper_anchor is not None]
    windows = [value for lane in lanes for value in _owned(lane.bounds.lower_exclusive, lane.bounds.upper_exclusive)]

    assert sorted([*windows, *retained]) == list(range(lo + 1, upper))
    assert len(set(retained)) == len(retained)
    assert set(retained) == {anchor for anchor in anchors if lo < anchor < upper}
    assert [lane.ordinal for lane in lanes] == list(range(len(lanes)))
    assert all(lane.descending is descending for lane in lanes)
    lowers = [_exact(lane.bounds.lower_exclusive) for lane in lanes]
    assert lowers == sorted(lowers, reverse=descending)
