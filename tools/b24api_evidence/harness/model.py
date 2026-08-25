"""Deterministic model portal and exact offline evidence matrix."""

from __future__ import annotations
import asyncio
import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl

from b24api.contracts.policy import (
    DuplicatePolicy,
    ExecutionPolicy,
    IdentityCoercion,
    IdentityRequirement,
    OrderSemantics,
    TotalSemantics,
)
from b24api.contracts.request import IdentitySpec, ReplaySafety, Request, ResultSelector
from b24api.execution import Executor, Transport, WireResponse
from b24api.traversal import PaginationDriver, iter_list
from b24api.traversal.plans import (
    CountedOffsetPlan,
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
        """Return the expected hash."""
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
    batch_requests: int
    batch_commands: int
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
        """Initialize instance state."""
        self.case = case
        self.requests = 0
        self.logical_pages = 0
        self.operating_seconds = 0.0
        self._identities = list(case.identities)
        self._oracle_reads = 0

    def oracle_snapshot(self) -> str:
        """Read an independent model snapshot, with real persistent churn when requested."""
        if self.case.mutation and self._oracle_reads:
            self._identities.append(max(self._identities, default=0) + 1)
        self._oracle_reads += 1
        return content_sha256(self._identities)

    @property
    def host(self) -> str:
        """Return the synthetic credential-free portal host."""
        return "model.invalid"

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Send one transport request attempt."""
        if attempt_timeout <= 0:
            raise AssertionError("model received an invalid attempt timeout")
        if max_response_bytes <= 0:
            raise AssertionError("model received an invalid response byte ceiling")
        self.requests += 1
        if request.method == MODEL_METHOD:
            envelope = self._page_envelope(request.copy_parameters())
            self.logical_pages += 1
            self.operating_seconds += MODEL_REQUEST_SECONDS
        elif request.method == "batch":
            envelope = self._batch_envelope(request.copy_parameters())
        else:
            raise AssertionError(f"unexpected model method: {request.method}")
        return WireResponse(
            status_code=200,
            headers=(("content-type", "application/json"), ("x-request-id", f"model-{self.requests}")),
            body=json.dumps(envelope, separators=(",", ":")).encode(),
        )

    def _page_envelope(self, parameters: dict[str, Any]) -> dict[str, Any]:
        """Return one deterministic list response without charging a physical request."""
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
        envelope: dict[str, Any] = {
            "result": [{"ID": value} for value in page_ids],
            "total": len(identities),
            "time": {"duration": MODEL_REQUEST_SECONDS * 2, "operating": MODEL_REQUEST_SECONDS},
        }
        if after is None and next_offset < len(selected):
            envelope["next"] = next_offset
        return envelope

    def _batch_envelope(self, parameters: dict[str, Any]) -> dict[str, Any]:
        """Execute the exact Bitrix batch shape used by counted traversal."""
        commands = parameters.get("cmd")
        if not isinstance(commands, dict) or not commands:
            raise AssertionError("model batch requires a non-empty command map")
        results: dict[str, Any] = {}
        totals: dict[str, int] = {}
        continuations: dict[str, int] = {}
        result_times: dict[str, dict[str, float]] = {}
        for key, encoded in commands.items():
            if not isinstance(key, str) or not isinstance(encoded, str):
                raise TypeError("model batch command is malformed")
            method, separator, query = encoded.partition("?")
            if method != MODEL_METHOD:
                raise AssertionError(f"unexpected model batch method: {method}")
            command_parameters = _flat_command_parameters(query if separator else "")
            command = self._page_envelope(command_parameters)
            results[key] = command["result"]
            totals[key] = int(command["total"])
            if "next" in command:
                continuations[key] = int(command["next"])
            result_times[key] = {"duration": MODEL_REQUEST_SECONDS * 2, "operating": MODEL_REQUEST_SECONDS}
        command_count = len(commands)
        self.logical_pages += command_count
        self.operating_seconds += MODEL_REQUEST_SECONDS * command_count
        return {
            "result": {
                "result": results,
                "result_error": {},
                "result_total": totals,
                "result_next": continuations,
                "result_time": result_times,
            },
            "time": {
                "duration": MODEL_REQUEST_SECONDS * command_count * 2,
                "operating": MODEL_REQUEST_SECONDS * command_count,
            },
        }


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
    if plan_name == "fixed_1x_batch":
        return await _run_fixed_1x_batch_case(case, portal=portal)
    if plan_name == "counted_batch":
        return await _run_counted_batch_case(case, portal=portal, executor=executor, identity=identity)
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
        batch_requests=report.batch_requests,
        batch_commands=report.batch_commands,
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
        for plan_name in ("offset", "keyset", "fixed_1x_batch", "counted_batch"):
            run = await run_model_case(case, plan_name=plan_name)
            if not case.mutation and (run.identities != case.identities or run.actual_hash != run.expected_hash):
                raise AssertionError(f"model correctness failure in {case.case_id}/{plan_name}")
            runs.append(run)
    return tuple(runs)


def run_exact_matrix_sync() -> tuple[ModelRun, ...]:
    """Synchronous CLI bridge for the deterministic matrix."""
    return asyncio.run(run_exact_matrix())


async def _run_fixed_1x_batch_case(case: ModelCase, *, portal: DeterministicPortal) -> ModelRun:
    """Replay the frozen 1.0.1 direct-head plus 50-command batched-tail algorithm."""
    pre_hash = portal.oracle_snapshot()
    head_wire = await portal.send(
        Request(MODEL_METHOD, {"start": 0}, ReplaySafety.SAFE),
        attempt_timeout=120,
        max_response_bytes=16 * 1024 * 1024,
    )
    head = json.loads(head_wire.body)
    rows = list(head["result"])
    buffered_rows_high_water = len(rows)
    total = int(head["total"])
    tail_starts = tuple(range(PAGE_SIZE, total, PAGE_SIZE))
    batch_requests = 0
    for chunk_start in range(0, len(tail_starts), 50):
        starts = tail_starts[chunk_start : chunk_start + 50]
        commands = {f"_{index:02d}": f"{MODEL_METHOD}?start={start}" for index, start in enumerate(starts)}
        batch_wire = await portal.send(
            Request("batch", {"halt": True, "cmd": commands}, ReplaySafety.SAFE),
            attempt_timeout=120,
            max_response_bytes=16 * 1024 * 1024,
        )
        batch_requests += 1
        batch = json.loads(batch_wire.body)["result"]["result"]
        buffered_rows_high_water = max(buffered_rows_high_water, sum(len(batch[key]) for key in commands))
        for key in commands:
            rows.extend(batch[key])
    identities = tuple(int(row["ID"]) for row in rows)
    actual_hash = content_sha256(list(identities))
    post_hash = portal.oracle_snapshot()
    mutation_retries = 0
    while pre_hash != post_hash and mutation_retries < MAX_MUTATION_RETRIES:
        mutation_retries += 1
        pre_hash = post_hash
        post_hash = portal.oracle_snapshot()
    snapshot_state = "changed" if pre_hash != post_hash else "verified"
    return ModelRun(
        case_id=case.case_id,
        plan="fixed_1x_batch",
        identities=identities,
        expected_hash=case.expected_hash,
        actual_hash=actual_hash,
        pre_hash=pre_hash,
        post_hash=post_hash,
        requests=portal.requests,
        logical_pages=portal.logical_pages,
        batch_requests=batch_requests,
        batch_commands=len(tail_starts),
        operating_seconds=portal.operating_seconds,
        time_to_first_row_seconds=MODEL_REQUEST_SECONDS if identities else None,
        wall_seconds=portal.requests * MODEL_REQUEST_SECONDS,
        buffered_rows_high_water=buffered_rows_high_water,
        outcome="INCONCLUSIVE" if case.mutation else "PASS",
        snapshot_state=snapshot_state,
        mutation_retries=mutation_retries,
    )


async def _run_counted_batch_case(
    case: ModelCase,
    *,
    portal: DeterministicPortal,
    executor: Executor,
    identity: IdentitySpec,
) -> ModelRun:
    """Exercise the production validated head-plus-batched-tail driver."""
    plan = CountedOffsetPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
        duplicate_policy=DuplicatePolicy.ERROR,
        total_semantics=TotalSemantics.FILTERED_EXACT,
    )
    policy = ExecutionPolicy(
        max_requests=1_000,
        max_pages=1_000,
        max_elapsed=120,
        max_buffered_rows=2_500,
    )
    context = executor.context(policy)
    driver = PaginationDriver(
        executor,
        Request(MODEL_METHOD, replay_safety=ReplaySafety.SAFE),
        plan,
        selector=ResultSelector.root(),
        identity=identity,
        context=context,
        page_cap_hint=PAGE_SIZE,
    )
    pre_hash = portal.oracle_snapshot()
    rows: list[dict[str, Any]] = []
    async for page in driver.counted_batch_pages(batch_size=50, page_size=PAGE_SIZE):
        for item in page.items:
            if not isinstance(item, dict) or not isinstance(item.get("ID"), int):
                raise TypeError("model counted traversal emitted malformed row")
            rows.append(item)
    snapshot = await context.snapshot()
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
        plan="counted_batch",
        identities=identities,
        expected_hash=case.expected_hash,
        actual_hash=actual_hash,
        pre_hash=pre_hash,
        post_hash=post_hash,
        mutation_retries=mutation_retries,
        requests=portal.requests,
        logical_pages=portal.logical_pages,
        batch_requests=driver.batch_report.batch_requests if driver.batch_report is not None else 0,
        batch_commands=driver.batch_report.batch_commands if driver.batch_report is not None else 0,
        operating_seconds=portal.operating_seconds,
        time_to_first_row_seconds=MODEL_REQUEST_SECONDS if identities else None,
        wall_seconds=portal.requests * MODEL_REQUEST_SECONDS,
        buffered_rows_high_water=snapshot.counters.buffered_rows_high_water,
        outcome=outcome,
        snapshot_state=snapshot_state,
    )


def _flat_command_parameters(query: str) -> dict[str, Any]:
    """Decode the scalar controls emitted by the counted model batch."""
    parameters: dict[str, Any] = {}
    for key, value in parse_qsl(query, keep_blank_values=True):
        if key not in {"start", "LIMIT"}:
            raise AssertionError(f"unexpected counted model control: {key}")
        parameters[key] = int(value)
    return parameters


def _positive_integer(value: object, field: str) -> int:
    result = _nonnegative_integer(value, field)
    if result == 0:
        raise AssertionError(f"{field} must be positive")
    return result


def _nonnegative_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AssertionError(f"{field} must be a non-negative integer")
    return value
