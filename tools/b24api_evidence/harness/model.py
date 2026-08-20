"""Deterministic model portal and exact offline evidence matrix."""

from __future__ import annotations
import asyncio
import json
from dataclasses import dataclass
from typing import Any

from b24api.execution import Executor, Transport, WireResponse
from b24api.models import (
    DuplicatePolicy,
    ExecutionPolicy,
    IdentityCoercion,
    IdentityRequirement,
    IdentitySpec,
    OrderSemantics,
    ReplaySafety,
    Request,
    ResultSelector,
    TotalSemantics,
)
from b24api.pagination import iter_list
from b24api.plans import (
    KeysetPlan,
    KeysetTerminalRule,
    OffsetContinuation,
    OffsetSequentialPlan,
    OffsetTerminalRule,
)

from .contracts import MAX_MUTATION_RETRIES, content_sha256

MODEL_METHOD = "model.entity.list"
PAGE_SIZE = 50
MODEL_REQUEST_SECONDS = 0.001


@dataclass(frozen=True, slots=True)
class ModelCase:
    """One deterministic selection and its independent expected identities."""

    case_id: str
    distribution: str
    base_count: int
    identities: tuple[int, ...]
    mutation: bool = False

    @property
    def expected_hash(self) -> str:
        return content_sha256(list(self.identities))


@dataclass(frozen=True, slots=True)
class ModelRun:
    """One canonical traversal result and normalized counters."""

    case_id: str
    plan: str
    identities: tuple[int, ...]
    expected_hash: str
    actual_hash: str
    pre_hash: str
    post_hash: str
    requests: int
    logical_pages: int
    operating_seconds: float
    time_to_first_row_seconds: float | None
    wall_seconds: float
    buffered_rows_high_water: int
    outcome: str
    snapshot_state: str
    mutation_retries: int


def exact_model_cases() -> tuple[ModelCase, ...]:
    """Return every normative deterministic scale/distribution cross-cut."""
    dense = tuple(range(1, 10_001))
    uniform_sparse = tuple(range(10, 100_001, 10))
    clustered = tuple(range(40_001, 50_001))
    skewed = (*range(1, 9_001), *range(100_001, 101_001))
    deleted = tuple(value for value in range(1, 11_668) if value % 7 != 0)[:10_000]
    return (
        ModelCase("empty", "empty", 0, ()),
        ModelCase("one", "single", 1, (1,)),
        ModelCase("nineteen", "boundary", 19, tuple(range(1, 20))),
        ModelCase("five-hundred", "boundary", 500, tuple(range(1, 501))),
        ModelCase("dense-10k", "dense", 10_000, dense),
        ModelCase("uniform-sparse-10k", "uniform_sparse", 100_001, uniform_sparse),
        ModelCase("clustered-sparse-10k", "clustered_sparse", 120_000, clustered),
        ModelCase("skewed-10k", "skewed", 120_000, skewed),
        ModelCase("deleted-id-10k", "deleted_id", 11_667, deleted),
        ModelCase("mutation", "mutation", 500, tuple(range(1, 501)), mutation=True),
    )


class DeterministicPortal(Transport):
    """A finite in-memory Bitrix-shaped endpoint with exact accounting."""

    def __init__(self, case: ModelCase) -> None:
        self.case = case
        self.requests = 0
        self.operating_seconds = 0.0
        self._identities = list(case.identities)
        self._oracle_reads = 0

    def oracle_snapshot(self) -> str:
        """Read an independent model snapshot, with real persistent churn when requested."""
        if self.case.mutation and self._oracle_reads:
            self._identities.append(max(self._identities, default=0) + 1)
        self._oracle_reads += 1
        return content_sha256(self._identities)

    async def send(self, request: Request, *, attempt_timeout: float) -> WireResponse:
        if request.method != MODEL_METHOD:
            raise AssertionError(f"unexpected model method: {request.method}")
        if attempt_timeout <= 0:
            raise AssertionError("model received an invalid attempt timeout")
        self.requests += 1
        parameters = request.copy_parameters()
        limit = _positive_integer(parameters.get("LIMIT", PAGE_SIZE), "LIMIT")
        identities = tuple(self._identities)
        filter_value = parameters.get("filter", {})
        if not isinstance(filter_value, dict):
            raise TypeError("model filter must be an object")
        after = filter_value.get(">ID")
        if after is not None:
            if isinstance(after, bool) or not isinstance(after, str | int):
                raise TypeError("model keyset cursor must be a string or integer")
            cursor = int(after)
            selected = tuple(value for value in identities if value > cursor)
            offset = 0
        else:
            selected = identities
            start = parameters.get("start", 0)
            offset = 0 if start == -1 else _nonnegative_integer(start, "start")
        page_ids = selected[offset : offset + limit]
        next_offset = offset + len(page_ids)
        result: dict[str, Any] = {
            "result": [{"ID": value} for value in page_ids],
            "total": len(identities),
            "time": {"duration": MODEL_REQUEST_SECONDS * 2, "operating": MODEL_REQUEST_SECONDS},
        }
        if after is None and next_offset < len(selected):
            result["next"] = next_offset
        self.operating_seconds += MODEL_REQUEST_SECONDS
        return WireResponse(
            status_code=200,
            headers=(("content-type", "application/json"), ("x-request-id", f"model-{self.requests}")),
            body=json.dumps(result, separators=(",", ":")).encode(),
        )


async def run_model_case(case: ModelCase, *, plan_name: str) -> ModelRun:
    """Exercise the production executor/traversal stack against one model case."""
    portal = DeterministicPortal(case)
    executor = Executor(portal)
    identity = IdentitySpec(
        item_path=("ID",),
        filter_key="ID",
        order_key="ID",
        coercion=IdentityCoercion.EXACT_INTEGER,
    )
    plan: OffsetSequentialPlan | KeysetPlan
    if plan_name == "offset":
        plan = OffsetSequentialPlan(
            limit_path=None,
            requested_page_size=None,
            continuation=OffsetContinuation.SERVER_NEXT_OR_OBSERVED_COUNT,
            terminal=frozenset({OffsetTerminalRule.EMPTY_PAGE, OffsetTerminalRule.QUALIFIED_TOTAL}),
            identity_requirement=IdentityRequirement.REQUIRED,
            order_semantics=OrderSemantics.ASCENDING,
            duplicate_policy=DuplicatePolicy.ERROR,
            total_semantics=TotalSemantics.ADVISORY,
        )
    elif plan_name == "keyset":
        plan = KeysetPlan(
            direction="asc",
            limit_path=None,
            requested_page_size=None,
            terminal=KeysetTerminalRule.EMPTY_CONFIRMATION,
            identity_requirement=IdentityRequirement.REQUIRED,
            order_semantics=OrderSemantics.ASCENDING,
            duplicate_policy=DuplicatePolicy.ERROR,
            total_semantics=TotalSemantics.IGNORE,
        )
    else:
        raise ValueError(f"unknown model plan: {plan_name}")
    request = Request(MODEL_METHOD, replay_safety=ReplaySafety.SAFE)
    policy = ExecutionPolicy(
        max_requests=1_000,
        max_pages=1_000,
        max_elapsed=120,
        max_tracked_identities=20_000,
    )
    stream = iter_list(
        executor,
        request,
        plan=plan,
        selector=ResultSelector.root(),
        identity=identity,
        policy=policy,
        _page_cap_hint=PAGE_SIZE,
    )
    pre_hash = portal.oracle_snapshot()
    rows: list[dict[str, Any]] = []
    async with stream:
        async for item in stream:
            if not isinstance(item, dict) or not isinstance(item.get("ID"), int):
                raise TypeError("model traversal emitted malformed row")
            rows.append(item)
    report = stream.report
    identities = tuple(int(row["ID"]) for row in rows)
    actual_hash = content_sha256(list(identities))
    post_hash = portal.oracle_snapshot()
    mutation_retries = 0
    while pre_hash != post_hash and mutation_retries < MAX_MUTATION_RETRIES:
        mutation_retries += 1
        pre_hash = post_hash
        post_hash = portal.oracle_snapshot()
    snapshot_state = "changed" if pre_hash != post_hash else "verified"
    outcome = "INCONCLUSIVE" if case.mutation else "PASS"
    return ModelRun(
        case_id=case.case_id,
        plan=plan_name,
        identities=identities,
        expected_hash=case.expected_hash,
        actual_hash=actual_hash,
        pre_hash=pre_hash,
        post_hash=post_hash,
        requests=report.physical_requests,
        logical_pages=report.logical_pages,
        operating_seconds=portal.operating_seconds,
        time_to_first_row_seconds=MODEL_REQUEST_SECONDS if identities else None,
        wall_seconds=portal.operating_seconds,
        buffered_rows_high_water=report.buffered_rows_high_water,
        outcome=outcome,
        snapshot_state=snapshot_state,
        mutation_retries=mutation_retries,
    )


async def run_exact_matrix() -> tuple[ModelRun, ...]:
    """Run both admitted baseline plans over the full deterministic matrix."""
    runs: list[ModelRun] = []
    for case in exact_model_cases():
        for plan_name in ("offset", "keyset"):
            run = await run_model_case(case, plan_name=plan_name)
            if not case.mutation and (run.identities != case.identities or run.actual_hash != run.expected_hash):
                raise AssertionError(f"model correctness failure in {case.case_id}/{plan_name}")
            runs.append(run)
    return tuple(runs)


def run_exact_matrix_sync() -> tuple[ModelRun, ...]:
    """Synchronous CLI bridge for the deterministic matrix."""
    return asyncio.run(run_exact_matrix())


def _positive_integer(value: object, field: str) -> int:
    result = _nonnegative_integer(value, field)
    if result == 0:
        raise AssertionError(f"{field} must be positive")
    return result


def _nonnegative_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AssertionError(f"{field} must be a non-negative integer")
    return value
