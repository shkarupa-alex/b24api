"""Bounded fan-out and reference dispatch contracts."""

from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum

_PORTAL_BATCH_CAP = 50


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
    """Bounded independent direct requests."""

    concurrency: int = 10
    output_order: DeliveryOrder = DeliveryOrder.READY

    def __post_init__(self) -> None:
        """Validate the discriminated direct controls."""
        _positive_plain_integer(self.concurrency, "concurrency")
        if not isinstance(self.output_order, DeliveryOrder):
            raise TypeError("output_order must be a DeliveryOrder")


@dataclass(frozen=True, slots=True)
class BatchDispatch:
    """Bounded independent physical Bitrix batch requests."""

    batch_size: int = 50
    concurrency: int = 1
    output_order: DeliveryOrder = DeliveryOrder.READY

    def __post_init__(self) -> None:
        """Validate the discriminated batch controls."""
        if not 1 <= _positive_plain_integer(self.batch_size, "batch_size") <= _PORTAL_BATCH_CAP:
            raise ValueError("batch_size must be between 1 and 50")
        _positive_plain_integer(self.concurrency, "concurrency")
        if not isinstance(self.output_order, DeliveryOrder):
            raise TypeError("output_order must be a DeliveryOrder")


type DispatchSpec = DirectDispatch | BatchDispatch

__all__ = ["BatchDispatch", "DeliveryOrder", "DirectDispatch", "DispatchSpec"]
