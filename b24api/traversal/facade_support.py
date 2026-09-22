"""Small method-agnostic selectors shared by traversal entry points."""

from __future__ import annotations
from typing import TYPE_CHECKING, Literal

from b24api.contracts.response import ResultCollectionShape
from b24api.traversal.values import _MappingValuesResultSelector, _TolerantMappingValuesResultSelector

if TYPE_CHECKING:
    from b24api.contracts.request import ResultSelector


def _direction(value: str) -> Literal["asc", "desc"]:
    return "asc" if value == "ascending" else "desc"


def _collection_selector(selector: ResultSelector, shape: ResultCollectionShape) -> ResultSelector:
    if not isinstance(shape, ResultCollectionShape):
        raise TypeError("collection_shape must be a ResultCollectionShape")
    if shape is ResultCollectionShape.SEQUENCE:
        return selector
    if shape is ResultCollectionShape.MAPPING_VALUES_OR_EMPTY:
        return _TolerantMappingValuesResultSelector(selector.path)
    return _MappingValuesResultSelector(selector.path)
