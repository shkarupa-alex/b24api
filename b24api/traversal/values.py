"""Result selection, identity coercion and ordering primitives."""

from __future__ import annotations
import hashlib
import itertools
import json
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, cast

from b24api.contracts.json import FrozenJson, FrozenMapping, _json_type_name
from b24api.contracts.policy import IdentityCoercion
from b24api.contracts.request import ResultSelector
from b24api.contracts.response import ResultCollectionShape
from b24api.errors import CapabilityError, PaginationError, ResultShapeError

if TYPE_CHECKING:
    from collections.abc import Iterable

    from b24api.contracts.response import Response
    from b24api.traversal.plans import ItemCursorPlan

type IdentityValue = str | int | tuple[str | int, ...]
_MISSING = object()


class _MappingValuesResultSelector(ResultSelector):
    pass


class _TolerantMappingValuesResultSelector(_MappingValuesResultSelector):
    pass


def _mapping_values(response: Response, selector: ResultSelector) -> tuple[FrozenJson, ...]:
    selected = _selected_value(response, selector)

    if (
        isinstance(selector, _TolerantMappingValuesResultSelector)
        and isinstance(selected, list | tuple)
        and not selected
    ):
        return ()
    if not isinstance(selected, Mapping):
        raise ResultShapeError(
            selector=ResultSelector(selector.path),
            expected_shape=(
                ResultCollectionShape.MAPPING_VALUES_OR_EMPTY
                if isinstance(selector, _TolerantMappingValuesResultSelector)
                else ResultCollectionShape.MAPPING_VALUES
            ),
            observed_type=_json_type_name(selected),
        )
    return tuple(selected.values())


def _selected_value(response: Response, selector: ResultSelector) -> FrozenJson:
    """Resolve a declared selector or fail without exposing response values."""
    selected = response._frozen_result()  # noqa: SLF001 - validate the immutable transport snapshot
    for part in selector.path:
        if isinstance(part, str):
            if not isinstance(selected, Mapping) or part not in selected:
                raise CapabilityError("response result does not satisfy the declared selector")
            selected = selected[part]
        else:
            if not isinstance(selected, tuple) or part >= len(selected):
                raise CapabilityError("response result does not satisfy the declared selector")
            selected = selected[part]
    return selected


def _response_items(response: Response, selector: ResultSelector, *, single: bool = False) -> tuple[FrozenJson, ...]:
    if isinstance(selector, _MappingValuesResultSelector):
        return _mapping_values(response, selector)
    frozen_result = response._frozen_result()  # noqa: SLF001 - avoid whole-result thaw
    if single and selector.path == () and not isinstance(frozen_result, tuple):
        return (frozen_result,)
    selected = _selected_value(response, selector)
    if not isinstance(selected, tuple):
        raise ResultShapeError(
            selector=selector,
            expected_shape=ResultCollectionShape.SEQUENCE,
            observed_type=_json_type_name(selected),
        )
    return selected


def _mapping_shape_degraded(response: Response, selector: ResultSelector) -> bool:
    if not isinstance(selector, _TolerantMappingValuesResultSelector):
        return False
    selected = response._frozen_result()  # noqa: SLF001 - avoid a second whole-result thaw
    for part in selector.path:
        if isinstance(part, str):
            if not isinstance(selected, Mapping) or part not in selected:
                return False
            selected = selected[part]
        else:
            if not isinstance(selected, tuple) or part >= len(selected):
                return False
            selected = selected[part]
    return isinstance(selected, list | tuple) and not selected


def _page_fingerprint(items: Iterable[FrozenJson]) -> str:
    digest = hashlib.sha256()

    def update(value: FrozenJson) -> None:
        if isinstance(value, FrozenMapping):
            digest.update(b"{")
            for index, key in enumerate(sorted(value)):
                if index:
                    digest.update(b",")
                digest.update(json.dumps(key, ensure_ascii=False).encode())
                digest.update(b":")
                update(value[key])
            digest.update(b"}")
        elif isinstance(value, tuple):
            digest.update(b"[")
            for index, item in enumerate(value):
                if index:
                    digest.update(b",")
                update(item)
            digest.update(b"]")
        else:
            digest.update(json.dumps(value, ensure_ascii=False, allow_nan=False).encode())

    update(tuple(items))
    return digest.hexdigest()


def _page_fingerprint_policy(items: tuple[FrozenJson, ...], plan: object) -> tuple[str, bool]:
    """Allow sparse raw windows to have several empty selected pages."""
    return _page_fingerprint(items), bool(items) or getattr(plan, "sparse_raw_bound", None) is None


def _extract_path(value: FrozenJson, path: tuple[str | int, ...]) -> FrozenJson:
    current = value
    for part in path:
        if isinstance(part, int):
            if not isinstance(current, tuple) or part >= len(current):
                raise PaginationError(f"identity path is missing: {path!r}")
            current = current[part]
        else:
            if not isinstance(current, Mapping) or part not in current:
                raise PaginationError(f"identity path is missing: {path!r}")
            current = current[part]
    return current


def _coerce_identity(value: FrozenJson, coercion: IdentityCoercion) -> IdentityValue:
    if coercion is IdentityCoercion.EXACT_STRING:
        if not isinstance(value, str):
            raise PaginationError("identity must be an exact string")
        return value
    if coercion is IdentityCoercion.EXACT_INTEGER:
        if not isinstance(value, int) or isinstance(value, bool):
            raise PaginationError("identity must be an exact integer")
        return value
    if isinstance(value, bool) or not isinstance(value, str | int):
        raise PaginationError("identity must be a decimal string or integer")
    try:
        return int(value)
    except ValueError as error:
        raise PaginationError("identity is not a decimal integer") from error


def _validate_order(values: Sequence[IdentityValue], direction: str | None) -> None:
    for previous, current in itertools.pairwise(values):
        comparison = _compare_identities(current, previous)
        if direction == "asc" and comparison <= 0:
            raise PaginationError("page identities are not strictly ascending")
        if direction == "desc" and comparison >= 0:
            raise PaginationError("page identities are not strictly descending")


def _compare_identities(left: IdentityValue, right: IdentityValue) -> int:
    if type(left) is not type(right):
        raise PaginationError("identity values are not mutually orderable")
    if left == right:
        return 0
    if isinstance(left, int) and isinstance(right, int):
        return 1 if left > right else -1
    if isinstance(left, str) and isinstance(right, str):
        return 1 if left > right else -1
    raise PaginationError("identity values are not mutually orderable")


def _cursor_values(items: Sequence[FrozenJson], plan: ItemCursorPlan) -> list[IdentityValue]:
    raw_values = [_extract_optional_path(item, plan.cursor_item_path) for item in items]
    exhausted = [value is _MISSING or value is None for value in raw_values]
    if any(exhausted):
        raise PaginationError(f"cursor path is missing: {plan.cursor_item_path!r}")
    return [_coerce_cursor(cast("FrozenJson", value), plan.cursor_coercion) for value in raw_values]


def _extract_optional_path(value: FrozenJson, path: tuple[str | int, ...]) -> FrozenJson | object:
    current = value
    for part in path:
        if isinstance(part, int):
            if not isinstance(current, tuple) or part >= len(current):
                return _MISSING
            current = current[part]
        else:
            if not isinstance(current, Mapping) or part not in current:
                return _MISSING
            current = current[part]
    return current


def _coerce_cursor(value: FrozenJson, coercion: IdentityCoercion) -> IdentityValue:
    try:
        return _coerce_identity(value, coercion)
    except PaginationError as error:
        raise PaginationError("cursor value does not satisfy cursor_coercion") from error


def _take_cursor(values: list[IdentityValue], mode: str) -> IdentityValue:
    if mode == "first":
        return values[0]
    if mode != "last":
        raise RuntimeError("cursor take mode escaped its closed contract")
    return values[-1]


__all__: list[str] = []
