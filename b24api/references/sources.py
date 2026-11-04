"""Heterogeneous reference sources (#28): each source lowered to its own program in one operation.

Every admitted ``ReferenceSource`` is checked by the same pure compatibility rule the homogeneous
facades apply before reading bindings, lowered once per distinct traversal contract, preflighted
against its own request and bound to its ordinal. A source the operation cannot run becomes a
correlated local non-execution before any HTTP request; a value that is not a ``ReferenceSource``
fails the input source. All admitted sources share one dispatcher, row pool, policy and gate.
"""

from __future__ import annotations
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

from b24api._sources import OwnedSource
from b24api.completion.operation_stream import MappedOperationStream
from b24api.contracts.reference_source import ReferenceSource
from b24api.errors import B24ApiError
from b24api.references.binding import (
    _bind_request,
    _BindingLocalValidationError,
    _BindingSourceError,
    _local_validation_failure,
)
from b24api.references.compatibility import source_incompatibility
from b24api.references.dispatch_plan import kernel_dispatch
from b24api.references.facade import (
    ReferenceKernelStream,
    _kernel_plan,
    _reference_assurance,
    _reference_error,
    _reference_error_items,
    _reference_page_cap,
    _reference_terminal,
    _reference_variant,
    _ReferenceEventMapper,
)
from b24api.references.program import SourceProgram
from b24api.references.scheduler import ReferenceScheduler
from b24api.references.stream import ReferenceStream
from b24api.references.support import _page_cap
from b24api.traversal.driver import PaginationDriver
from b24api.traversal.plans import CountedOffsetPlan, KernelBatchDispatch

if TYPE_CHECKING:
    from collections.abc import AsyncIterable, Callable, Iterable

    from b24api.contracts.dispatch import DispatchSpec
    from b24api.contracts.page_stop import PageStopPolicy
    from b24api.contracts.policy import ExecutionPolicy
    from b24api.contracts.reference import ReferenceOutcome
    from b24api.contracts.report import TraversalAssurance, Violation
    from b24api.contracts.request import Request
    from b24api.contracts.stream import OperationStream
    from b24api.contracts.traversal import TraversalSpec
    from b24api.references.outcome import ReferenceRequest

type ReferenceSources[C] = Iterable[ReferenceSource[C]] | AsyncIterable[ReferenceSource[C]]
_LOWERED_CONTRACTS = 64


@dataclass(frozen=True, slots=True)
class _Lowered:
    traversal: TraversalSpec
    page_stop: PageStopPolicy | None
    program: SourceProgram


class SourceAdmission:
    """Admit heterogeneous sources one at a time; lowering is cached per distinct contract."""

    def __init__(self, executor: object, dispatch: DispatchSpec, policy: ExecutionPolicy) -> None:
        """Bind the operation's executor, dispatch and policy; nothing is read yet."""
        from b24api.execution import Executor  # noqa: PLC0415 - narrow internal composition import

        if not isinstance(executor, Executor):
            raise TypeError("executor must be an Executor")
        self._executor = executor
        self._dispatch = dispatch
        self._dispatch_plan = kernel_dispatch(dispatch, policy)
        self._policy = policy
        # A preflight-only context: request controls are checked against it, it never starts or sends.
        self._preflight = executor.context(policy)
        self._lowered: dict[tuple[int, int], _Lowered] = {}
        self.assurance: TraversalAssurance | None = None

    def admit(self, item: object, index: int) -> ReferenceRequest:
        """Bind one source to its ordinal, or retain it as a correlated local non-execution."""
        if not isinstance(item, ReferenceSource):
            raise TypeError("reference sources must yield ReferenceSource values")
        source = cast("ReferenceSource[object]", item)
        try:
            program = self._program(source)
            reference = _bind_request(source.request, source.binding, index, source.traversal)
        except (B24ApiError, ValueError, TypeError, _BindingLocalValidationError):
            return _local_validation_failure(source.request, source.binding, index)
        if program.assurance is not None:
            # A declared stop rule weakens the whole operation; a neighbour never strengthens it.
            self.assurance = program.assurance
        return replace(reference, program=program)

    def _program(self, source: ReferenceSource[object]) -> SourceProgram:
        incompatible = source_incompatibility(source.request, source.traversal, self._dispatch, source.page_stop)
        if incompatible is not None:
            raise incompatible
        program = self._lower(source.traversal, source.page_stop)
        PaginationDriver(
            self._executor,
            source.request,
            program.plan,
            selector=program.selector,
            identity=program.identity,
            context=self._preflight,
            page_adapter=program.page_adapter,
        ).preflight_capabilities()
        self._executor.preflight_request(source.request)
        return program

    def _lower(self, traversal: TraversalSpec, page_stop: PageStopPolicy | None) -> SourceProgram:
        key = (id(traversal), id(page_stop))
        cached = self._lowered.get(key)
        if cached is not None and cached.traversal is traversal and cached.page_stop is page_stop:
            return cached.program
        plan, selector, identity = _kernel_plan(traversal)
        PaginationDriver.validate_contract(plan, identity, self._policy)
        hint = _reference_page_cap(traversal)
        counted = isinstance(plan, CountedOffsetPlan) and isinstance(self._dispatch_plan, KernelBatchDispatch)
        program = SourceProgram(
            plan,
            selector,
            identity,
            traversal.page_adapter,
            _page_cap(plan, self._dispatch_plan, self._policy, whole_result=False, page_cap_hint=hint),
            page_cap_hint=hint,
            page_stop=page_stop,
            counted_page_size=hint if counted else None,
            assurance=_reference_assurance(traversal),
        )
        if len(self._lowered) >= _LOWERED_CONTRACTS:
            self._lowered.clear()
        self._lowered[key] = _Lowered(traversal, page_stop, program)
        return program

    def stream[C](
        self,
        sources: ReferenceSources[C],
        *,
        tolerant: bool,
        audit: Callable[[Request], Violation | None] | None,
        deregister: Callable[[object], None],
    ) -> OperationStream[ReferenceOutcome[C]]:
        """Open the sources and compose one public reference operation over them."""
        owned = OwnedSource.adapt(
            sources,
            accept=self.admit,
            observe=None if audit is None else (lambda reference: audit(reference.request)),
            failure=lambda _error: _BindingSourceError(),
        )
        scheduler = ReferenceScheduler(
            self._executor,
            plan=None,
            dispatch=self._dispatch_plan,
            selector=None,
            identity=None,
            output_order=self._dispatch_plan.output_order,
            tolerant=tolerant,
            policy=self._policy,
            emit_complete=True,
            capture_fail_fast=not tolerant,
        )
        kernel = cast("ReferenceKernelStream", ReferenceStream(scheduler, owned))
        mapper = _ReferenceEventMapper()
        stream = MappedOperationStream(
            kernel,
            mapper,
            operation="iter_reference_source_outcomes" if tolerant else "iter_reference_sources",
            assurance_source=lambda: self.assurance,
            classify=_reference_variant,
            error_mapper=lambda error, report: _reference_error(error, report, mapper),
            error_items=lambda error: _reference_error_items(error, mapper),
            count_admitted=_reference_terminal,
            source_active_references=lambda: kernel.active_references_high_water,
            deregister=deregister,
        )
        return cast("OperationStream[ReferenceOutcome[C]]", stream)


__all__ = ["ReferenceSources", "SourceAdmission"]
