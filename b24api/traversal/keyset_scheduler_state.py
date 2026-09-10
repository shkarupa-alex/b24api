"""Structural state contract for stateless keyset scheduler helpers."""

from __future__ import annotations
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections import Counter

    from b24api.batch.engine import BatchExecutor
    from b24api.contracts.json import JsonValue
    from b24api.contracts.keyset_execution import (
        AutoKeysetExecution,
        KeysetPageCompletion,
        KeysetPhase,
        PartitionedKeysetExecution,
        RangeKeysetExecution,
    )
    from b24api.contracts.report import Violation
    from b24api.contracts.request import IdentitySpec, Request, ResultSelector
    from b24api.contracts.response import Response, ResultCollectionShape
    from b24api.contracts.traversal import KeysetSpec
    from b24api.execution import ExecutionContext
    from b24api.traversal.keyset_fast_plan import LaneBounds, LaneState
    from b24api.traversal.keyset_fast_stream import FastTraceRecorder
    from b24api.traversal.ordered_admission import OrderedAdmissionState
    from b24api.traversal.page_validation import LaneCommandPlan, LaneReceipt


class SchedulerState(Protocol):
    """State and scheduler-owned callbacks used by pure helper functions."""

    request: Request
    identity: IdentitySpec
    keyset: KeysetSpec
    selector: ResultSelector
    collection_shape: ResultCollectionShape
    effective_page_cap: int
    execution: RangeKeysetExecution | PartitionedKeysetExecution | AutoKeysetExecution
    context: ExecutionContext
    engine: BatchExecutor
    trace: FastTraceRecorder
    admission: OrderedAdmissionState
    violations: list[Violation]
    batch_capacity: int
    batch_requests: int
    batch_commands: int
    _planning_requests: Counter[KeysetPhase]
    _planning_physical_requests: int
    _observation_ordinal: int
    _planning_bounds: dict[str, LaneBounds]
    _planning_descending: dict[str, bool]
    _last_wave_responses: dict[str, Response]
    _buffer_balance: int
    _finish_lane: LaneState
    _lanes: list[LaneState]
    _lane_rows: dict[int, list[JsonValue]]
    _lane_identities: dict[int, list[int]]
    _lane_commands: dict[int, list[tuple[str, int]]]
    _lane_index: int
    _anchor_rows: dict[int, JsonValue]
    _anchor_commands: dict[int, str]
    _anchor_count: int
    _anchor_probe_commands: int
    _empty_anchor_probes: int

    @property
    def _completion(self) -> KeysetPageCompletion: ...

    async def _adjust_buffer(self, delta: int) -> None: ...

    def _lane_plan(
        self,
        lane: LaneState,
        *,
        phase: KeysetPhase,
        request: Request,
        reserve: int | None = None,
        single: bool = False,
    ) -> LaneCommandPlan: ...

    def _admit_receipt(self, receipt: LaneReceipt) -> None: ...


__all__: list[str] = []
