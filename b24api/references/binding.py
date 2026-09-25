"""Lazy exact binding of caller context to method-agnostic requests."""

from __future__ import annotations
from collections.abc import AsyncIterable, Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from b24api._sources import OwnedSource
from b24api.contracts.command import NotExecutedReason
from b24api.contracts.json import _freeze_json
from b24api.contracts.reference import Binding
from b24api.contracts.traversal import CursorTraversal, TraversalSpec, traversal_control_paths
from b24api.errors import CapabilityError, InputSourceError, PaginationError
from b24api.references.outcome import ReferenceRequest
from b24api.traversal.cursor_domain import validate_cursor_value
from b24api.traversal.identity import _request_with_controls
from b24api.traversal.values import _coerce_identity

if TYPE_CHECKING:
    from b24api.contracts.json import JsonValue
    from b24api.contracts.report import Violation
    from b24api.contracts.request import ParameterPath, Request

type BindingSource[C] = Iterable[Binding[C]] | AsyncIterable[Binding[C]]


@dataclass(frozen=True, slots=True)
class _BindingContext:
    index: int
    correlation: object


class _BindingSourceError(Exception):
    """Carry a failed binding source; reports name the public failure it maps to (``report_cause``)."""

    def __init__(self) -> None:
        super().__init__("reference input source failed")
        self.report_cause = InputSourceError("Reference input source failed")


class _BindingLocalValidationError(ValueError):
    """The client's exact binding transform rejected a known Binding."""


def _normalized(path: ParameterPath) -> tuple[str | int, ...]:
    return tuple(part.casefold() if isinstance(part, str) else part for part in path.path)


def _overlaps(left: tuple[str | int, ...], right: tuple[str | int, ...]) -> bool:
    shared = min(len(left), len(right))
    return left[:shared] == right[:shared]


def _validate_binding_controls(binding: Binding[object], traversal: TraversalSpec) -> None:
    controls = tuple(_normalized(path) for path in traversal_control_paths(traversal))
    for update in binding.updates:
        update_path = _normalized(update.path)
        if any(_overlaps(update_path, control) for control in controls):
            raise ValueError("binding update collides with a traversal control path")


def _matching_key(mapping: dict[str, JsonValue], requested: str) -> str | None:
    matches = [key for key in mapping if key.casefold() == requested.casefold()]
    if len(matches) > 1:
        raise ValueError(f"ambiguous case-insensitive binding key: {requested}")
    return matches[0] if matches else None


def _replace_path(root: dict[str, JsonValue], path: tuple[str | int, ...], value: JsonValue) -> None:
    current: JsonValue = root
    for part in path[:-1]:
        if isinstance(part, str):
            if not isinstance(current, dict):
                raise TypeError("binding path traverses a non-mapping value")
            actual = _matching_key(current, part)
            if actual is None:
                current[part] = {}
                actual = part
            current = current[actual]
            continue
        if not isinstance(current, list) or part >= len(current):
            raise KeyError(f"missing binding list index: {part}")
        current = current[part]
    final = path[-1]
    if isinstance(final, str):
        if not isinstance(current, dict):
            raise TypeError("binding path terminates in a non-mapping value")
        actual = _matching_key(current, final)
        current[final if actual is None else actual] = value
        return
    if not isinstance(current, list) or final >= len(current):
        raise KeyError(f"missing binding list index: {final}")
    current[final] = value


def _bind_request(base: Request, binding: Binding[object], index: int, traversal: TraversalSpec) -> ReferenceRequest:
    try:
        _validate_binding_controls(binding, traversal)
        parameters = base.copy_parameters()
        for update in binding.updates:
            _replace_path(parameters, update.path.path, update.value)
        request = base.with_parameters(parameters)
        initial_cursor = None
        if binding.start_cursor is not None:
            if not isinstance(traversal, CursorTraversal):
                raise ValueError("start_cursor is valid only for CursorTraversal")  # noqa: TRY301 - joins the local-validation mapping below
            initial_cursor = _coerce_identity(_freeze_json(binding.start_cursor), traversal.cursor.coercion)
            validate_cursor_value(initial_cursor, traversal.cursor.domain)
            request = _request_with_controls(
                request,
                {traversal.cursor.parameter_path: initial_cursor},
                allow_create=traversal.cursor.allow_create_controls,
                replace=frozenset({traversal.cursor.parameter_path}),
            )
    except (CapabilityError, KeyError, PaginationError, TypeError, ValueError) as error:
        raise _BindingLocalValidationError from error
    return ReferenceRequest(
        request,
        f"r{index:012d}",
        _BindingContext(index, binding.correlation),
        initial_cursor=initial_cursor,
    )


def _local_validation_failure(base: Request, binding: Binding[object], index: int) -> ReferenceRequest:
    """Retain an admitted binding as a proved local non-execution state."""
    return ReferenceRequest(
        base,
        f"r{index:012d}",
        _BindingContext(index, binding.correlation),
        NotExecutedReason.LOCAL_VALIDATION_FAILED,
    )


def binding_source[C](
    base: Request,
    source: BindingSource[C],
    traversal: TraversalSpec,
    audit: Callable[[Request], Violation | None] | None = None,
) -> OwnedSource[ReferenceRequest]:
    """Adapt a lazy binding source while preserving exact iterator ownership."""

    def accept(binding: object, index: int) -> ReferenceRequest:
        if not isinstance(binding, Binding):
            raise TypeError("reference source must yield Binding values")
        canonical = cast("Binding[object]", binding)
        try:
            return _bind_request(base, canonical, index, traversal)
        except _BindingLocalValidationError:
            return _local_validation_failure(base, canonical, index)

    return OwnedSource.adapt(
        source,
        accept=accept,
        observe=None if audit is None else (lambda reference: audit(reference.request)),
        failure=lambda _error: _BindingSourceError(),
    )


__all__ = ["BindingSource", "binding_source"]
