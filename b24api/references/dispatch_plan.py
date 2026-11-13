"""Translate public dispatch limits into bounded reference scheduler plans."""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.contracts.dispatch import DeliveryOrder, DirectDispatch, DispatchSpec
from b24api.traversal.plans import DispatchPlan, KernelBatchDispatch, KernelDirectDispatch, ReferenceOutputOrder

if TYPE_CHECKING:
    from b24api.contracts.policy import ExecutionPolicy


def kernel_dispatch(dispatch: DispatchSpec, policy: ExecutionPolicy, *, client_concurrency: int) -> DispatchPlan:
    """Bound concurrency and batch size by the operation policy and the client's request limit.

    The plan's concurrency is the smallest limit that applies (``client_concurrency`` is
    ``Settings.max_concurrent_requests``), so it is what the operation can really keep in flight.
    """
    order = ReferenceOutputOrder.READY if dispatch.output_order is DeliveryOrder.READY else ReferenceOutputOrder.INPUT
    if isinstance(dispatch, DirectDispatch):
        return KernelDirectDispatch(
            concurrency=min(
                dispatch.concurrency,
                policy.max_direct_concurrency,
                policy.max_active_references,
                client_concurrency,
            ),
            output_order=order,
        )
    return KernelBatchDispatch(
        batch_size=min(dispatch.batch_size, policy.max_buffered_commands),
        concurrency=min(dispatch.concurrency, policy.max_active_references, client_concurrency),
        output_order=order,
        coalesce_wait=dispatch.coalesce_wait,
    )
