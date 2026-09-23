"""Method-agnostic admission checks for exclusive range cursors."""

from __future__ import annotations
from typing import TYPE_CHECKING

from b24api.contracts.json import _freeze_json
from b24api.contracts.traversal import CursorDomain
from b24api.errors import CapabilityError, PaginationError
from b24api.traversal.keyset_eligibility import _path_lookup
from b24api.traversal.values import _coerce_identity

if TYPE_CHECKING:
    from b24api.contracts.request import ParameterPath, Request
    from b24api.traversal.plans import ItemCursorPlan
    from b24api.traversal.values import IdentityValue


def validate_cursor_value(value: IdentityValue, domain: CursorDomain) -> None:
    """Reject values known to restart a positive exclusive range at the head."""
    if domain is CursorDomain.EXCLUSIVE_POSITIVE_INTEGER and (type(value) is not int or value < 1):
        raise CapabilityError("exclusive range cursor must be a positive integer")


def validate_cursor_progression(value: IdentityValue, domain: CursorDomain) -> None:
    """Treat a server-emitted ignored cursor as an incomplete traversal."""
    if domain is CursorDomain.EXCLUSIVE_POSITIVE_INTEGER and (type(value) is not int or value < 1):
        raise PaginationError("exclusive range cursor page emitted an ignored value")


def validate_initial_cursor(request: Request, plan: ItemCursorPlan) -> IdentityValue | None:
    """Reject ignored or invalid caller controls before the first dispatch."""
    if plan.domain is CursorDomain.OPAQUE:
        return None
    exists, raw = _path_lookup(request.copy_parameters(), plan.cursor_request_path)
    if not exists:
        return None
    try:
        value = _coerce_identity(_freeze_json(raw), plan.cursor_coercion)
    except (PaginationError, TypeError, ValueError) as error:
        raise CapabilityError("exclusive range cursor control has an invalid value") from error
    validate_cursor_value(value, plan.domain)
    return value


def cursor_probe_updates(
    request: Request,
    plan: ItemCursorPlan,
) -> tuple[dict[ParameterPath, object], dict[ParameterPath, object]]:
    """Return two structurally valid future control updates for preflight."""
    validate_initial_cursor(request, plan)
    first, second = (1, 2) if plan.domain is CursorDomain.EXCLUSIVE_POSITIVE_INTEGER else (0, 1)
    return ({plan.cursor_request_path: first}, {plan.cursor_request_path: second})


def cursor_controls_replace(plan: ItemCursorPlan, initial_cursor: IdentityValue | None) -> bool:
    """Allow traversal-owned replacement of a validated range cursor."""
    return (
        initial_cursor is not None
        or not plan.allow_create_controls
        or plan.domain is CursorDomain.EXCLUSIVE_POSITIVE_INTEGER
    )
