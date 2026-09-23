"""Qualified raw-range evidence for sparse selected offset pages."""

from __future__ import annotations
from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

from b24api.contracts.traversal import RawTotalSource
from b24api.errors import PaginationError
from b24api.traversal.closure import SPARSE_RAW_RANGE_COVERED

if TYPE_CHECKING:
    from b24api.contracts.request import ResultSelector
    from b24api.contracts.response import Response
    from b24api.contracts.traversal import SparseRawBound


def raw_total_from_response(response: Response, path: ResultSelector | RawTotalSource) -> int:
    """Read an exact non-negative raw total without using selected row count."""
    if path is RawTotalSource.ENVELOPE:
        if response.total is None:
            raise PaginationError("qualified sparse raw total is missing")
        if response.total < 0:
            raise PaginationError("qualified sparse raw total is not a non-negative integer")
        return response.total
    current: object = response.result
    for part in path.path:
        if type(part) is str and isinstance(current, Mapping) and part in current:
            current = cast("Mapping[str, object]", current)[part]
        elif type(part) is int and isinstance(current, list) and 0 <= part < len(current):
            current = cast("list[object]", current)[part]
        else:
            raise PaginationError("qualified sparse raw total is missing")
    if type(current) is not int or current < 0:
        raise PaginationError("qualified sparse raw total is not a non-negative integer")
    return current


def sparse_page_terminal(
    bound: SparseRawBound,
    response: Response,
    *,
    offset: int,
    previous_total: int | None,
) -> tuple[str | None, int]:
    """Validate stable raw extent and return its structural closure witness."""
    total = raw_total_from_response(response, bound.total_path)
    if previous_total is not None and total != previous_total:
        raise PaginationError("sparse raw total changed during traversal")
    terminal = SPARSE_RAW_RANGE_COVERED if offset + bound.stride.wire_increment >= total else None
    return terminal, total
