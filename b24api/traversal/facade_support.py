"""Small method-agnostic selectors shared by traversal entry points."""

from __future__ import annotations
from typing import TYPE_CHECKING, Literal

from b24api.contracts.identity_store import IdentityStore
from b24api.contracts.response import ResultCollectionShape
from b24api.traversal.values import _MappingValuesResultSelector, _TolerantMappingValuesResultSelector

if TYPE_CHECKING:
    from b24api.contracts.request import ResultSelector, TraversalIdentity


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


def _checked_identity_store(store: object, identity: TraversalIdentity | None) -> IdentityStore | None:
    """Admit a caller-owned ledger only where it can witness declared identities."""
    if store is None:
        return None
    if identity is None:
        raise ValueError("identity_store requires an identity declaration")
    if not isinstance(store, IdentityStore):
        raise TypeError("identity_store must implement add_if_absent(key) -> bool")
    return store
