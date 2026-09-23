"""Caller-owned external identity ledger for exact duplicate evidence on unordered traversals.

Exact duplicate detection over an arbitrarily ordered full result set cannot fit the client's bounded
memory: the in-process key budget (``ExecutionPolicy.max_identity_keys``) fails such a traversal closed
instead of growing without limit. The architecture therefore admits strong identity assurance on
unordered data only through a qualified strictly monotonic order or through this public store, whose
backing (a database table, a key-value service) belongs to the application. Without it, large
unordered extractions would have no bounded way to prove uniqueness.
"""

from __future__ import annotations
import json
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Sequence


@runtime_checkable
class IdentityStore(Protocol):
    """Durable set of canonical identity keys owned by the caller for one traversal.

    The client writes one page's new keys as a bounded chunk after every other page check passed and
    before the page is delivered. A key reported as already present is a duplicate: under
    ``DuplicatePolicy.ERROR`` it rejects the page, whose other keys may already be recorded, and the
    operation ends incomplete. Any exception raised here likewise rejects the page; the client never
    reports completion on keys it could not record. The client does not close or clear the store.
    """

    def add_if_absent(self, key: str) -> bool:
        """Record ``key`` and return ``True`` only when it was not already present."""
        ...


def identity_store_key(value: str | int | Sequence[str | int]) -> str:
    """Return the canonical text key for one traversal identity value.

    Compact JSON keeps ``1`` and ``"1"`` distinct and keeps composite identities ordered, so an
    application can store keys in an ordinary text primary key without its own encoding rules.
    """
    normalized = list(value) if isinstance(value, tuple | list) else value
    return json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))


__all__ = ["IdentityStore", "identity_store_key"]
