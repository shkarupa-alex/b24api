"""Translate public dispatch limits into bounded reference scheduler plans."""

from __future__ import annotations
from dataclasses import replace
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
    concurrency = min(dispatch.concurrency, client_concurrency)
    plan: DispatchPlan = (
        KernelDirectDispatch(concurrency=concurrency, output_order=order)
        if isinstance(dispatch, DirectDispatch)
        else KernelBatchDispatch(
            batch_size=dispatch.batch_size,
            concurrency=concurrency,
            output_order=order,
            coalesce_wait=dispatch.coalesce_wait,
        )
    )
    return bound_dispatch(plan, policy)


def bound_dispatch(plan: DispatchPlan, policy: ExecutionPolicy) -> DispatchPlan:
    """Bound a plan by the operation policy: the concurrency and batch size the operation can really use.

    Every reference operation's plan passes here once, so the dispatchers take its limits as they are.
    """
    if isinstance(plan, KernelDirectDispatch):
        return replace(
            plan,
            concurrency=min(plan.concurrency, policy.max_direct_concurrency, policy.max_active_references),
        )
    return replace(
        plan,
        batch_size=min(plan.batch_size, policy.max_buffered_commands),
        concurrency=min(plan.concurrency, policy.max_active_references),
    )
