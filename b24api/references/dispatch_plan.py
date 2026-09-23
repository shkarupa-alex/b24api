"""Translate public dispatch limits into bounded reference scheduler plans."""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.contracts.dispatch import DeliveryOrder, DirectDispatch, DispatchSpec
from b24api.traversal.plans import BatchDispatch, DispatchPlan, ReferenceOutputOrder
from b24api.traversal.plans import DirectDispatch as KernelDirectDispatch

if TYPE_CHECKING:
    from b24api.contracts.policy import ExecutionPolicy


def kernel_dispatch(dispatch: DispatchSpec, policy: ExecutionPolicy) -> DispatchPlan:
    """Bound concurrency and batch size by the operation policy."""
    order = ReferenceOutputOrder.READY if dispatch.output_order is DeliveryOrder.READY else ReferenceOutputOrder.INPUT
    if isinstance(dispatch, DirectDispatch):
        return KernelDirectDispatch(
            concurrency=min(dispatch.concurrency, policy.max_direct_concurrency, policy.max_active_references),
            output_order=order,
        )
    return BatchDispatch(
        batch_size=min(dispatch.batch_size, policy.max_buffered_commands),
        concurrency=min(dispatch.concurrency, policy.max_active_references),
        output_order=order,
        coalesce_wait=dispatch.coalesce_wait,
    )
