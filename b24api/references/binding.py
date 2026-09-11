"""Lazy exact binding of caller context to method-agnostic requests."""

# ruff: noqa: TRY301 - iterator adapters normalize source failures

from __future__ import annotations
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, Self, cast, runtime_checkable

from b24api.contracts.command import NotExecutedReason
from b24api.contracts.json import _freeze_json
from b24api.contracts.reference import Binding
from b24api.contracts.traversal import CursorTraversal, TraversalSpec, traversal_control_paths
from b24api.errors import CapabilityError, PaginationError
from b24api.references.outcome import ReferenceRequest
from b24api.traversal.identity import _request_with_controls
from b24api.traversal.values import _coerce_identity

if TYPE_CHECKING:
    from b24api.contracts.json import JsonValue
    from b24api.contracts.report import Violation
    from b24api.contracts.request import ParameterPath, Request

type BindingSource[C] = Iterable[Binding[C]] | AsyncIterable[Binding[C]]


@runtime_checkable
class _SyncClosable(Protocol):
    def close(self) -> None:
        """Close a synchronous iterator."""
        ...


@runtime_checkable
class _AsyncClosable(Protocol):
    async def aclose(self) -> None:
        """Close an asynchronous iterator."""
        ...


@dataclass(frozen=True, slots=True)
class _BindingContext:
    index: int
    correlation: object


class _BindingSourceError(Exception):
    pass


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
                raise ValueError("start_cursor is valid only for CursorTraversal")
            initial_cursor = _coerce_identity(_freeze_json(binding.start_cursor), traversal.cursor.coercion)
            request = _request_with_controls(
                request,
                {traversal.cursor.parameter_path: initial_cursor},
                allow_create=traversal.cursor.allow_create_controls,
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


class _SyncBindingAdapter[C](Iterator[ReferenceRequest]):
    def __init__(
        self,
        base: Request,
        source: Iterable[Binding[C]],
        traversal: TraversalSpec,
        audit: Callable[[Request], Violation | None] | None,
    ) -> None:
        self._base = base
        self._iterator = iter(source)
        self._traversal = traversal
        self._audit = audit
        self._index = 0
        self.violations: list[Violation] = []

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> ReferenceRequest:
        try:
            binding = next(self._iterator)
            if not isinstance(binding, Binding):
                raise TypeError("reference source must yield Binding values")
            canonical = cast("Binding[object]", binding)
            try:
                request = _bind_request(self._base, canonical, self._index, self._traversal)
            except _BindingLocalValidationError:
                request = _local_validation_failure(self._base, canonical, self._index)
        except StopIteration:
            raise
        except Exception as error:
            raise _BindingSourceError from error
        if self._audit is not None:
            violation = self._audit(request.request)
            if violation is not None:
                self.violations.append(violation)
        self._index += 1
        return request

    def close(self) -> None:
        if isinstance(self._iterator, _SyncClosable):
            self._iterator.close()


class _AsyncBindingAdapter[C](AsyncIterator[ReferenceRequest]):
    def __init__(
        self,
        base: Request,
        source: AsyncIterable[Binding[C]],
        traversal: TraversalSpec,
        audit: Callable[[Request], Violation | None] | None,
    ) -> None:
        self._base = base
        self._iterator = aiter(source)
        self._traversal = traversal
        self._audit = audit
        self._index = 0
        self.violations: list[Violation] = []

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> ReferenceRequest:
        try:
            binding = await anext(self._iterator)
            if not isinstance(binding, Binding):
                raise TypeError("reference source must yield Binding values")
            canonical = cast("Binding[object]", binding)
            try:
                request = _bind_request(self._base, canonical, self._index, self._traversal)
            except _BindingLocalValidationError:
                request = _local_validation_failure(self._base, canonical, self._index)
        except StopAsyncIteration:
            raise
        except Exception as error:
            raise _BindingSourceError from error
        if self._audit is not None:
            violation = self._audit(request.request)
            if violation is not None:
                self.violations.append(violation)
        self._index += 1
        return request

    async def aclose(self) -> None:
        if isinstance(self._iterator, _AsyncClosable):
            await self._iterator.aclose()


def binding_source[C](
    base: Request,
    source: BindingSource[C],
    traversal: TraversalSpec,
    audit: Callable[[Request], Violation | None] | None = None,
) -> Iterable[ReferenceRequest] | AsyncIterable[ReferenceRequest]:
    """Adapt a lazy binding source while preserving exact iterator ownership."""
    if isinstance(source, AsyncIterable):
        return _AsyncBindingAdapter(base, source, traversal, audit)
    return _SyncBindingAdapter(base, source, traversal, audit)


__all__ = ["BindingSource", "binding_source"]
