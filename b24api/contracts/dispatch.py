"""Bounded fan-out and reference dispatch contracts."""

from __future__ import annotations
import math
from dataclasses import dataclass
from enum import StrEnum

# The Bitrix portal accepts at most 50 commands in one physical batch; every layer reads this constant.
PORTAL_BATCH_CAP = 50
# The Bitrix portal answers one list page with at most 50 rows.
PORTAL_PAGE_ROWS = 50


class DeliveryOrder(StrEnum):
    """Cross-command or cross-binding output order."""

    READY = "ready"
    INPUT = "input"


def _positive_plain_integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


@dataclass(frozen=True, slots=True)
class DirectDispatch:
    """Bounded independent direct requests.

    ``concurrency`` is a ceiling of this operation alone: the requests in flight are also bounded by the
    policy (``max_direct_concurrency`` and ``max_active_references``, for references and fan-out alike) and by
    ``Settings.max_concurrent_requests``, which the client's operations share; the smallest applies.
    """

    concurrency: int = 10
    output_order: DeliveryOrder = DeliveryOrder.READY

    def __post_init__(self) -> None:
        """Validate the discriminated direct controls."""
        _positive_plain_integer(self.concurrency, "concurrency")
        if not isinstance(self.output_order, DeliveryOrder):
            raise TypeError("output_order must be a DeliveryOrder")


@dataclass(frozen=True, slots=True)
class BatchDispatch:
    """Bounded independent physical Bitrix batch requests.

    ``concurrency`` is a ceiling of this operation alone: the batches in flight are also bounded by the policy's
    ``max_active_references`` and by ``Settings.max_concurrent_requests``, which the client's operations share;
    the smallest applies. ``ExecutionPolicy.max_direct_concurrency`` does not limit them. The commands a REST 3.0
    isolation re-sends one by one are not batches: only ``Settings.max_concurrent_requests`` bounds them. A fan-out
    divides ``max_buffered_rows`` among the commands of every batch it may keep in flight (``batch_size`` times that
    smallest limit, at most ``max_active_references``), so a wider ``concurrency`` gives each command a smaller
    result limit: 312 rows under ``batch_size=1, concurrency=8``. While the budget holds a full 50-row portal page
    per command of one batch, the limit never drops below a page: the row budget then holds back the batches beyond
    it, so the defaults, ``concurrency=2`` and ``batch_size=20, concurrency=4`` all keep 50 rows.
    """

    batch_size: int = PORTAL_BATCH_CAP
    concurrency: int = 1
    output_order: DeliveryOrder = DeliveryOrder.READY
    coalesce_wait: float = 0.020

    def __post_init__(self) -> None:
        """Validate the discriminated batch controls."""
        if not 1 <= _positive_plain_integer(self.batch_size, "batch_size") <= PORTAL_BATCH_CAP:
            raise ValueError(f"batch_size must be between 1 and {PORTAL_BATCH_CAP}")
        _positive_plain_integer(self.concurrency, "concurrency")
        if not isinstance(self.output_order, DeliveryOrder):
            raise TypeError("output_order must be a DeliveryOrder")
        if not isinstance(self.coalesce_wait, int | float) or isinstance(self.coalesce_wait, bool):
            raise TypeError("coalesce_wait must be a finite number")
        if not math.isfinite(self.coalesce_wait) or not 0 <= self.coalesce_wait <= 1:
            raise ValueError("coalesce_wait must be between 0 and 1 second")
        object.__setattr__(self, "coalesce_wait", float(self.coalesce_wait))


type DispatchSpec = DirectDispatch | BatchDispatch

__all__ = ["BatchDispatch", "DeliveryOrder", "DirectDispatch", "DispatchSpec"]
