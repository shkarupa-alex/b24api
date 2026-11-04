"""One fast or automatic keyset reference binding on the shared queue (#27).

Each binding runs its own ``KeysetRuntime`` (fingerprints, bounds, identity, planning, ordered
admission) under a fixed row lease ``G`` taken from the shared row pool at activation and returned
once at terminal. The runtime plans and fits against a budget view whose policy differs from the
operation policy only in ``max_buffered_rows=G``; its atomic waves travel through the shared
dispatcher, and its page evidence is recorded into the shared operation gate under its binding id.
"""

from __future__ import annotations
import asyncio
from dataclasses import replace
from typing import TYPE_CHECKING, Protocol

from b24api.completion.reference_recorder import ReferenceFastLedger
from b24api.contracts.completion import CommandSettlement
from b24api.contracts.policy import KernelState
from b24api.contracts.report import KeysetSelectionSummary, Violation, ViolationSeverity
from b24api.errors import AmbiguousExecutionError, PaginationError
from b24api.references.budget_view import BindingBudgetView, RowLease
from b24api.references.dispatch import _DoneEvent, _failed_settlement, _FailureEvent, _PageEvent
from b24api.references.wave_engine import SharedWaveEngine, WaveHost
from b24api.traversal.keyset_eligibility import validate_fast_keyset
from b24api.traversal.keyset_observation import FastTraceRecorder
from b24api.traversal.keyset_runtime import KeysetRuntime

if TYPE_CHECKING:
    from b24api.contracts.completion import BindingClosure
    from b24api.contracts.json import FrozenJson
    from b24api.contracts.report import PageRecord
    from b24api.execution import Executor
    from b24api.references.dispatch import _Reservation, _RowBuffer
    from b24api.references.program import FastKeysetProgram
    from b24api.references.run import _ReferenceRun


class FastKeysetHost(WaveHost, Protocol):
    """The scheduler members a fast keyset binding runs on."""

    @property
    def executor(self) -> Executor:
        """Return the operation executor."""
        ...

    @property
    def buffer(self) -> _RowBuffer:
        """Return the shared row pool."""
        ...

    def take_page_sequence(self) -> int:
        """Return the next operation-wide page sequence."""
        ...

    async def lease_rows(self, run: _ReferenceRun, fast: FastKeysetProgram) -> _Reservation:
        """Wait for and hold the binding's fixed row lease in the shared pool."""
        ...


async def run_fast_keyset(host: FastKeysetHost, run: _ReferenceRun, fast: FastKeysetProgram) -> None:
    """Run one binding to its terminal event; the lease is returned exactly once."""
    ledger = ReferenceFastLedger(run.completion)
    trace = FastTraceRecorder(host.context.policy.page_trace_limit)
    runtime: KeysetRuntime | None = None
    lease: _Reservation | None = None
    try:
        request = run.work.reference.request
        # A binding may add a control the fast runtime owns; it fails before any request is sent.
        validate_fast_keyset(
            host.executor,
            request,
            identity=fast.identity,
            keyset=fast.keyset,
            page_size=fast.page_size,
            execution=fast.execution,
            policy=host.context.policy,
        )
        lease = await host.lease_rows(run, fast)
        runtime = KeysetRuntime(
            executor=host.executor,
            request=request,
            identity=fast.identity,
            keyset=fast.keyset,
            selector=fast.selector,
            collection_shape=fast.collection_shape,
            page_size=fast.page_size,
            effective_page_cap=fast.effective_page_cap,
            execution=fast.execution,
            context=BindingBudgetView.over(host.context, RowLease.of(lease)),
            engine=SharedWaveEngine(host, run, ledger),
            trace=trace,
            page_adapter=run.program.page_adapter,
            completion_recorder=ledger,
        )
        while rows := await runtime.next_rows():
            await _deliver(host, run, runtime, rows)
        closure, witnesses = runtime.completion_closure()
        await runtime.aclose()
        ledger.close(successful=True, reason=KernelState.COMPLETED.value, unsettled=CommandSettlement.NOT_EXECUTED)
        closure = _witnessed(closure, ledger)
        report = runtime.report_fragment()
        await run.output.put(
            _DoneEvent(
                run.work,
                run.partial_rows,
                tuple(runtime.violations),
                _page_records(host, run, trace),
                closure=closure,
                qualified_witnesses=witnesses or None,
                keyset_selection=KeysetSelectionSummary.of(report),
                keyset_execution=report,
            ),
        )
    except asyncio.CancelledError:
        raise
    except BaseException as error:
        # The scheduler's per-reference boundary turns the error into this binding's terminal event.
        if runtime is not None and not runtime.closed:
            await runtime.aclose()
        ledger.close(successful=False, reason=KernelState.FAILED.value, unsettled=_unsettled(error))
        run.failure = _FailureEvent(
            run.work,
            error,
            None,
            run.page_state,
            run.partial_rows,
            () if runtime is None else _binding_violations(runtime.violations),
            _page_records(host, run, trace),
        )
        raise
    finally:
        if runtime is not None and not runtime.closed:
            await runtime.aclose()
        if lease is not None:
            await host.buffer.abort(lease)


async def _deliver(
    host: FastKeysetHost,
    run: _ReferenceRun,
    runtime: KeysetRuntime,
    rows: tuple[FrozenJson, ...],
) -> None:
    """Hand one admitted row group to the consumer; its rows stay charged to the lease until acknowledged."""
    state, key = host.producer_state, run.producer_key
    state.runnable.discard(key)
    state.touch()
    acknowledged = asyncio.get_running_loop().create_future()
    await run.output.put(
        _PageEvent(
            run.work,
            rows,
            None,
            (1,) * len(rows),
            (True,) * len(rows),
            (),
            (),
            None,
            acknowledged,
            emitted=runtime.mark_emitted,
        ),
    )
    await acknowledged
    run.partial_rows += len(rows)
    state.runnable.add(key)
    state.touch()


def _page_records(host: FastKeysetHost, run: _ReferenceRun, trace: FastTraceRecorder) -> tuple[PageRecord, ...]:
    """Tag the binding's retained fast page records with operation-wide sequences and its index."""
    records, _dropped = trace.snapshot()
    return tuple(
        replace(record, sequence=host.take_page_sequence(), reference_index=run.work.index) for record in records
    )


def _witnessed(closure: BindingClosure | None, ledger: ReferenceFastLedger) -> BindingClosure:
    """Return the runtime's closure witness of a natural end, or fail a traversal that lacks one."""
    if closure is None or ledger.negative:
        raise PaginationError("fast keyset traversal completed without a closure witness")
    return closure


def _binding_violations(violations: list[Violation]) -> tuple[Violation, ...]:
    """Report a failed binding's page violations as warnings: its failure outcome carries the error.

    Like a sequential reference binding, one failed fast binding does not make the whole operation's
    evidence blocking; a tolerant operation completes with failures, a fail-fast one fails on it.
    """
    return tuple(replace(violation, severity=ViolationSeverity.WARNING) for violation in violations)


def _unsettled(error: BaseException) -> CommandSettlement:
    """Classify a page still unsettled when the traversal failed (a direct finish page)."""
    if isinstance(error, AmbiguousExecutionError):
        return CommandSettlement.UNKNOWN
    return _failed_settlement(error)


__all__ = ["FastKeysetHost", "run_fast_keyset"]
