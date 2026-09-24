"""Synchronous no-I/O admission checks for fast integer-keyset execution."""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.contracts.dispatch import PORTAL_BATCH_CAP
from b24api.contracts.keyset_execution import (
    AutoKeysetExecution,
    KeysetPageCompletion,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
)
from b24api.contracts.policy import (
    ConfirmationPolicy,
    IdentityCoercion,
    IdentityRequirement,
    OrderSemantics,
    SnapshotRequirement,
    TotalSemantics,
)
from b24api.contracts.request import RouteKind
from b24api.contracts.wire import BodyEncoding
from b24api.errors import CapabilityError
from b24api.traversal.identity import _child_path, _request_with_controls

if TYPE_CHECKING:
    from b24api.contracts.policy import ExecutionPolicy
    from b24api.contracts.request import IdentitySpec, ParameterPath, Request
    from b24api.contracts.traversal import KeysetSpec
    from b24api.execution.executor import Executor

# A bounded keyset reads both boundaries (ascending and descending) before it plans.
_BOUNDARY_READS = 2


def _path_lookup(
    parameters: object,
    path: ParameterPath,
    *,
    case_sensitive: bool = False,
) -> tuple[bool, object]:
    current = parameters
    for part in path.path:
        if isinstance(part, str):
            if not isinstance(current, dict):
                return False, None
            matches = (
                [part]
                if case_sensitive and part in current
                else ([] if case_sensitive else [key for key in current if key.casefold() == part.casefold()])
            )
            if len(matches) > 1:
                raise CapabilityError("request contains a case-insensitively ambiguous traversal control")
            if not matches:
                return False, None
            current = current[matches[0]]
        else:
            if not isinstance(current, list) or part >= len(current):
                return False, None
            current = current[part]
    return True, current


def _reject_owned_controls(request: Request, identity: IdentitySpec, keyset: KeysetSpec) -> None:
    positional_arguments = request.positional
    positional = positional_arguments is not None
    parameters = positional_arguments.to_wire_slots() if positional_arguments is not None else request.copy_parameters()
    filter_exists, filter_value = _path_lookup(parameters, keyset.filter_path, case_sensitive=positional)
    if filter_exists:
        if not isinstance(filter_value, dict):
            raise CapabilityError("keyset filter path must contain an object")
        forbidden = {
            identity.filter_key.casefold(),
            *(f"{operator}{identity.filter_key}".casefold() for operator in (">", "<", ">=", "<=")),
        }
        if any(str(key).casefold() in forbidden for key in filter_value):
            raise CapabilityError("caller identity bounds conflict with fast keyset traversal")
    order_paths: tuple[ParameterPath | None, ...]
    if keyset.split_order is None:
        if positional and keyset.order_path is not None:
            order_exists, order_value = _path_lookup(
                parameters,
                keyset.order_path,
                case_sensitive=True,
            )
            if order_exists and not isinstance(order_value, dict):
                raise CapabilityError("keyset order path must contain an object")
            if order_exists and order_value:
                raise CapabilityError("caller traversal control conflicts with fast keyset traversal")
            order_paths = ()
        else:
            order_paths = (keyset.order_path,)
    else:
        order_paths = (keyset.split_order.field_path, keyset.split_order.direction_path)
    for path in (*order_paths, keyset.start_suppression_path, keyset.limit_path):
        if path is not None and _path_lookup(parameters, path, case_sensitive=positional)[0]:
            raise CapabilityError("caller traversal control conflicts with fast keyset traversal")


def _effective_cap(
    execution: RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution,
    keyset: KeysetSpec,
    page_size: int,
) -> int:
    contract = execution.contract
    if contract.page_completion is KeysetPageCompletion.EMPTY_CONFIRMATION:
        return page_size
    if keyset.limit_path is None and contract.endpoint_page_cap is None:
        raise CapabilityError("short-page completion requires a writable limit or endpoint_page_cap")
    if (
        keyset.limit_path is not None
        and contract.endpoint_page_cap is not None
        and contract.endpoint_page_cap != page_size
    ):
        raise CapabilityError("endpoint_page_cap must equal page_size when both caps are supplied")
    effective = page_size if keyset.limit_path is not None else contract.endpoint_page_cap
    if effective is None:
        raise CapabilityError("fast keyset page cap is unavailable")
    return effective


def validate_fast_keyset(  # noqa: C901, PLR0912, PLR0913
    executor: Executor,
    request: Request,
    *,
    identity: IdentitySpec,
    keyset: KeysetSpec,
    page_size: int,
    execution: RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution,
    policy: ExecutionPolicy,
) -> int:
    """Prove every static prerequisite before registration and I/O."""
    if keyset.boundary is not None:
        raise CapabilityError("exact-boundary completion is not qualified for fast or auto keyset execution")
    if not isinstance(page_size, int) or isinstance(page_size, bool) or page_size < 1:
        raise ValueError("page_size must be a positive integer")
    if (
        request.route is not RouteKind.BARE
        or request.encoding is not BodyEncoding.JSON
        or request.headers.items
        or request.positional is not None
    ):
        raise CapabilityError(
            "fast keyset traversal supports named BARE JSON requests without scoped headers; "
            "use SequentialKeysetExecution for positional requests"
        )
    if identity.coercion not in {IdentityCoercion.EXACT_INTEGER, IdentityCoercion.DECIMAL_STRING_INTEGER}:
        raise CapabilityError("fast keyset traversal requires integer identity coercion")
    consistency = policy.consistency
    if consistency.snapshot_requirement is not SnapshotRequirement.TRAVERSAL_ONLY:
        raise CapabilityError("fast keyset traversal cannot prove the requested snapshot assurance")
    if consistency.confirmation_policy not in {ConfirmationPolicy.NONE, ConfirmationPolicy.EMPTY_AFTER_BOUNDARY}:
        raise CapabilityError("fast keyset traversal cannot supply the requested confirmation policy")
    if consistency.total_semantics is not TotalSemantics.IGNORE:
        raise CapabilityError("fast keyset traversal cannot supply policy-level total semantics")
    if consistency.identity_requirement is IdentityRequirement.COMPOSITE:
        raise CapabilityError("fast keyset traversal requires a scalar identity contract")
    requested_order = consistency.order_semantics
    actual_order = OrderSemantics.ASCENDING if keyset.direction == "ascending" else OrderSemantics.DESCENDING
    if requested_order not in {OrderSemantics.UNORDERED, actual_order}:
        raise CapabilityError("fast keyset traversal cannot supply the requested order semantics")
    _reject_owned_controls(request, identity, keyset)
    effective_cap = _effective_cap(execution, keyset, page_size)
    retained_anchors = execution.target_lanes if isinstance(execution, PartitionedKeysetExecution) else 0
    if (
        isinstance(execution, RangeKeysetExecution | PartitionedKeysetExecution)
        and policy.max_buffered_rows < 3 * effective_cap + retained_anchors
    ):
        raise CapabilityError("execution policy cannot retain boundaries and plan a bounded keyset")
    requested_batch = execution.batch_size or PORTAL_BATCH_CAP
    capacity = min(
        requested_batch, PORTAL_BATCH_CAP, policy.max_buffered_commands, policy.max_buffered_rows // effective_cap
    )
    if (
        capacity < 1
        or policy.max_buffered_commands < _BOUNDARY_READS
        or policy.max_pages < _BOUNDARY_READS
        or policy.max_requests < 1
        or policy.max_buffered_rows < 2 * effective_cap
    ):
        raise CapabilityError("execution policy cannot fit the fast keyset boundary wave")
    direction = "ASC" if keyset.direction == "ascending" else "DESC"
    updates: dict[ParameterPath, object] = {}
    if keyset.split_order is None:
        if keyset.order_path is None:
            raise CapabilityError("keyset ordering controls are unavailable")
        updates[_child_path(keyset.order_path, identity.order_key)] = direction
    else:
        updates[keyset.split_order.field_path] = keyset.split_order.field_value or identity.order_key
        updates[keyset.split_order.direction_path] = keyset.split_order.ascending
    if keyset.start_suppression_path is not None:
        updates[keyset.start_suppression_path] = -1
    if keyset.limit_path is not None:
        updates[keyset.limit_path] = effective_cap
    _request_with_controls(request, updates, allow_create=keyset.allow_create_controls)
    executor._preflight_request(request)  # noqa: SLF001 - synchronous operation-wide preflight
    return effective_cap


__all__ = ["validate_fast_keyset"]
