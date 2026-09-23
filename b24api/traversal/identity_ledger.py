"""Adapter from the public caller-owned identity store to the traversal's page transaction.

The in-memory store answers membership before a page commits; an external ledger can only answer by
recording. This adapter therefore reports no prior membership during validation and turns the store's
``add_if_absent`` result into the page's cross-page duplicates at commit, keeping in process only a
counter, so identity memory stays bounded by one page regardless of source size. The driver's
repeated-page fingerprints are separate evidence: one short digest per page, bounded by
``ExecutionPolicy.max_pages``.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.contracts.identity_store import identity_store_key
from b24api.errors import PaginationError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from b24api.contracts.identity_store import IdentityStore
    from b24api.traversal.values import IdentityValue


class _ExternalIdentityStore:
    def __init__(self, store: IdentityStore) -> None:
        self._store = store
        self._count = 0

    @property
    def count(self) -> int:
        return self._count

    def contains(self, value: IdentityValue) -> bool:
        del value
        return False

    def add(self, value: IdentityValue) -> None:
        self.commit((value,))

    def commit(self, values: Sequence[IdentityValue]) -> frozenset[IdentityValue]:
        present: set[IdentityValue] = set()
        for value in values:
            try:
                added = self._store.add_if_absent(identity_store_key(value))
            except Exception as error:
                raise PaginationError("identity store could not record a traversal identity") from error
            if type(added) is not bool:
                raise PaginationError("identity store must answer add_if_absent with a bool")
            if added:
                self._count += 1
            else:
                present.add(value)
        return frozenset(present)

    def ensure_capacity(self, additional: int) -> None:
        del additional

    def close(self) -> None:
        """Leave the caller-owned store untouched; the application owns its lifetime."""
