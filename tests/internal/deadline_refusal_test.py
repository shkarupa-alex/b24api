"""Time refuses work in one place: ``ExecutionContext.time_left`` checks the deadline, ``deadline`` waits under it.

Both record a refusal by the operation's deadline (``ExecutionContext.refusal``), which ends the operation, before
they raise it; a request's own retry window closing first refuses that request alone and records nothing.

What holds every site to this rule is its own behavioural test: each place that refuses work at the deadline has
one showing the refusal recorded and the operation ended with it (below for the permit, a retry, a replay round and
an envelope's first send; ``tests/budget_end_cleanup_test.py`` for the waits, the source pull and the streams;
``tests/v3_batch_test.py`` for the v3 replay). The static checks here are a cheap tripwire for the usual idioms, not
a proof: the members of ``ExecutionContext`` that read its clock, by name; the readers of the time left that refuse
nothing; the timeouts armed through ``asyncio`` (under any name it is imported as, ``asyncio.wait`` with a timeout
included); and every ``BudgetExceededError`` (or subclass) constructed. A new site written in one of these idioms
fails them until it is listed with the reason it is not a refusal by the deadline; one written otherwise passes, and
only its behavioural test catches it.
"""

from __future__ import annotations
import ast
import asyncio
import json
from collections import Counter
from pathlib import Path

import pytest

from b24api import ExecutionPolicy, ReplaySafety, Request, RetryPolicy, RouteKind
from b24api.batch.correlation import _Command
from b24api.batch.engine import BatchExecutor
from b24api.batch.outcome import BatchFailure
from b24api.errors import B24ApiError, BudgetExceededError
from b24api.execution import ExecutionContext, Executor, RateCoordinator, WireResponse
from b24api.execution.context import RETRY_WINDOW_EXHAUSTED
from tests.scripting import DEADLINE_BUDGET, HANG_BOUND, ResponderTransport, always

PACKAGE = Path(__file__).resolve().parents[2] / "b24api"
CONTEXT = "execution/context.py"
LEDGER = "ExecutionContext"

# What the operation's clock is read through: only the ledger that owns it, and the policy that validates it, read it.
CLOCK_ATTRIBUTES = {"max_elapsed", "max_retry_elapsed_per_request", "elapsed", "_time_left"}
CLOCK_POLICY = "contracts/policy.py"
# The ledger's own clock state, read by name inside it.
LEDGER_CLOCK = CLOCK_ATTRIBUTES | {"_clock", "_start"}
# The ledger's members that read its clock: the readers above and below, and the bookkeeping around them.
CLOCK_MEMBERS = {
    f"{LEDGER}.{member}"
    for member in (
        "start",
        "elapsed",
        "snapshot",
        "_time_left",
        "_refuse",
        "time_left",
        "coalescing_time_left",
        "cleanup_window",
    )
}

# Readers of the time left that refuse nothing: (module, owner, method) -> calls.
UNREFUSED_READERS = Counter(
    {
        # The coalescing wait stops at the deadline and sends the chunk it holds, which its permit admits or refuses.
        ("references/dispatch.py", "_BatchPageDispatcher._run", "coalescing_time_left"): 1,
        # Cleanup steps: they wait at least the grace and refuse no work.
        ("completion/operation_stream.py", "MappedOperationStream._cleanup_window", "cleanup_window"): 1,
        (CONTEXT, "AsyncIteratorController.aclose", "cleanup_window"): 1,
        ("references/dispatch.py", "_BatchPageDispatcher.aclose", "cleanup_window"): 1,
        ("references/scheduler.py", "ReferenceScheduler._cleanup", "cleanup_window"): 1,
    }
)
UNREFUSED_METHODS = {method for _, _, method in UNREFUSED_READERS}

# Where a timeout may be armed: the context's own ``deadline``; a send, which the deadline must not cut into a
# refusal (a cut send may have run, and is reported by the executor as such); and the rate coordinator's wait,
# which is handed the deadline of the ``deadline`` block around it.
TIMEOUTS = Counter(
    {
        (CONTEXT, f"{LEDGER}.deadline", "timeout"): 1,
        ("execution/executor.py", "Executor._send_attempt", "timeout"): 1,
        ("execution/executor.py", "Executor._send_envelope", "timeout"): 1,
        ("execution/rate.py", "RateCoordinator.acquire", "timeout_at"): 1,
        # The batch dispatcher's coalescing wait, handed ``coalescing_time_left``: it ends in a send.
        ("references/dispatch.py", "_wait_first", "wait"): 1,
        # Cleanup waits, bounded by ``cleanup_window`` or a deadline handed in: they refuse no work.
        (CONTEXT, "AsyncIteratorController.aclose", "wait"): 1,
        (CONTEXT, "await_cleanup_resistant", "wait"): 1,
        ("references/dispatch.py", "_BatchPageDispatcher.aclose", "wait"): 1,
        ("references/support.py", "_wait_for_cleanup_tasks", "wait"): 1,
        # The scripted transport's bound on extension code, a test double outside any operation's budget.
        ("testing/transport.py", "_await_bounded", "wait"): 1,
    }
)
# The ``asyncio`` calls that arm a timeout; ``asyncio.wait`` arms one only with a ``timeout`` argument.
ARMING = {"timeout", "timeout_at", "wait_for", "Timeout"}

# Every construction of a ``BudgetExceededError`` or a subclass: (module, owner, call) -> calls.
CONSTRUCTIONS = Counter(
    {
        # The deadline's own refusal: by the operation's deadline, or by a request's retry window first; and the
        # recorded refusal raised again to end the operation with it.
        (CONTEXT, f"{LEDGER}._refuse", "BudgetExceededError(refusal)"): 1,
        (CONTEXT, f"{LEDGER}._refuse", "BudgetExceededError(RETRY_WINDOW_EXHAUSTED)"): 1,
        (CONTEXT, f"{LEDGER}.raise_if_refused", "BudgetExceededError(self._refusal)"): 1,
        # Count budgets: the request budget and the attempt, page, row and identity ceilings, which time does not move.
        (CONTEXT, f"{LEDGER}.admit_request", "BudgetExceededError(REQUEST_BUDGET_EXHAUSTED)"): 1,
        (CONTEXT, "AsyncIteratorController.get", "BudgetExceededError(REQUEST_BUDGET_EXHAUSTED)"): 1,
        (CONTEXT, f"{LEDGER}.reserve_attempt", "BudgetExceededError('per-request attempt budget exhausted')"): 1,
        (
            CONTEXT,
            f"{LEDGER}.reserve_pages",
            "BudgetExceededError('logical page budget cannot fit the requested wave')",
        ): 1,
        (CONTEXT, f"{LEDGER}._check_page_budget", "BudgetExceededError('logical page budget exhausted')"): 1,
        (CONTEXT, f"{LEDGER}._check_page_budget", "BudgetExceededError('per-reference page budget exhausted')"): 1,
        (CONTEXT, f"{LEDGER}._hold_buffered_rows", "BudgetExceededError('buffered row budget exhausted')"): 1,
        (
            CONTEXT,
            f"{LEDGER}.ensure_identity_capacity",
            "BudgetExceededError('identity evidence budget exhausted')",
        ): 1,
        (
            "execution/facts.py",
            "dropped_result",
            "ResultTooLargeError(rows=rows, limit=limit, request_summary=request_summary)",
        ): 1,
        (
            "references/budget_view.py",
            "RowLease.charge",
            'BudgetExceededError("fast keyset rows exceed the binding\'s row lease")',
        ): 1,
        (
            "references/dispatch.py",
            "_RowBuffer.reserve",
            "BudgetExceededError('one decoded page exceeds the global row buffer')",
        ): 1,
        (
            "references/dispatch.py",
            "_RowBuffer.accept",
            "BudgetExceededError('decoded page exceeded its reserved row capacity')",
        ): 1,
        (
            "references/scheduler.py",
            "ReferenceScheduler._lease_size",
            "BudgetExceededError('fast keyset lease cannot fit its plan at the INPUT frontier')",
        ): 1,
        (
            "references/scheduler.py",
            "ReferenceScheduler._lease_size",
            "BudgetExceededError('fast keyset lease cannot fit the boundary wave at the INPUT frontier')",
        ): 1,
        (
            "traversal/keyset_plan.py",
            "KeysetPlanner._anchor_waves",
            "BudgetExceededError('fast keyset planning wave has no available row capacity')",
        ): 1,
        (
            "traversal/keyset_transactions.py",
            "execute_wave",
            "BudgetExceededError('fast keyset wave exceeds currently available row capacity')",
        ): 1,
        (
            "traversal/keyset_transactions.py",
            "execute_body_wave",
            "BudgetExceededError('fast keyset body wave has no available row capacity')",
        ): 1,
        (
            "traversal/sequential.py",
            "OffsetStrategy._request",
            "BudgetExceededError('sparse raw page budget exhausted')",
        ): 1,
        # One request refused while time is left: a hold or a retry delay its window cannot carry, a send that may
        # not be repeated left less window than it waited, or no attempt left; the operation goes on.
        ("execution/executor.py", "_permit_window", "BudgetExceededError(_HOLD_REFUSAL)"): 1,
        (
            "execution/executor.py",
            "_RequestAttempts.admit",
            "BudgetExceededError('retry window left after the wait is shorter than the wait')",
        ): 1,
        ("execution/executor.py", "Executor._prepare_retry", "BudgetExceededError(_retry_refusal(replay))"): 1,
        ("batch/engine.py", "BatchExecutor._split_held", "BudgetExceededError(_HOLD_REFUSAL)"): 1,
        # A command the plan of its replay round stopped, for a refusal made above: by time (``time_left``), or
        # while time is left.
        ("batch/engine.py", "_Lineup._stop", "BudgetExceededError(refusal)"): 1,
        ("batch/v3.py", "_V3Resolution._not_run", "BudgetExceededError(stop)"): 1,
        # Cleanup overruns: a cleanup step outlasted its window; it refused no work and records no refusal.
        (CONTEXT, "AsyncIteratorController.aclose", "CleanupOverrunError(self._cleanup_error)"): 1,
        (CONTEXT, "await_cleanup_resistant", "CleanupOverrunError('cleanup exceeded the operation time budget')"): 1,
        (
            "references/dispatch.py",
            "_BatchPageDispatcher.aclose",
            "CleanupOverrunError('batch dispatcher cleanup exceeded operation time budget')",
        ): 1,
        (
            "references/scheduler.py",
            "ReferenceScheduler._cleanup",
            "CleanupOverrunError('reference task cleanup exceeded operation time budget')",
        ): 1,
    }
)


def _modules() -> list[tuple[str, ast.Module]]:
    return [
        (path.relative_to(PACKAGE).as_posix(), ast.parse(path.read_text(encoding="utf-8")))
        for path in sorted(PACKAGE.rglob("*.py"))
    ]


def _owners(tree: ast.Module) -> dict[int, str]:
    """Map every node to its innermost enclosing function or class, qualified by the classes around it."""
    owners: dict[int, str] = {}

    def visit(node: ast.AST, owner: str | None) -> None:
        for child in ast.iter_child_nodes(node):
            own = owner
            if isinstance(child, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
                own = child.name if owner is None else f"{owner}.{child.name}"
            owners[id(child)] = own or "<module>"
            visit(child, own)

    visit(tree, None)
    return owners


def _name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _budget_errors(modules: list[tuple[str, ast.Module]]) -> set[str]:
    """Return ``BudgetExceededError`` and every class the package derives from it, however indirectly."""
    names = {BudgetExceededError.__name__}
    classes = [node for _, tree in modules for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]
    while derived := {node.name for node in classes if any(_name(base) in names for base in node.bases)} - names:
        names |= derived
    return names


def _in_ledger(module: str, owner: str) -> bool:
    return module == CONTEXT and (owner == LEDGER or owner.startswith(f"{LEDGER}."))


def _asyncio_names(tree: ast.Module) -> tuple[set[str], dict[str, str]]:
    """Return the names ``asyncio`` is imported as, and its functions imported by name (local name -> function)."""
    modules: set[str] = set()
    functions: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules |= {alias.asname or alias.name for alias in node.names if alias.name == "asyncio"}
        elif isinstance(node, ast.ImportFrom) and node.module == "asyncio":
            functions |= {alias.asname or alias.name: alias.name for alias in node.names}
    return modules, functions


def _armed(node: ast.Call, modules: set[str], functions: dict[str, str]) -> str | None:
    """Return the ``asyncio`` function ``node`` arms a timeout with, if it does."""
    func = node.func
    called: str | None
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) and func.value.id in modules:
        called = func.attr
    elif isinstance(func, ast.Name):
        called = functions.get(func.id)
    else:
        return None
    if called in ARMING or (called == "wait" and any(keyword.arg == "timeout" for keyword in node.keywords)):
        return called
    return None


def test_only_the_ledger_reads_the_operation_clock() -> None:
    reads = {
        (module, owner)
        for module, tree in _modules()
        if module != CLOCK_POLICY
        for owners in [_owners(tree)]
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Load)
        for owner in [owners[id(node)]]
        if node.attr in (LEDGER_CLOCK if _in_ledger(module, owner) else CLOCK_ATTRIBUTES)
    }
    assert reads == {(CONTEXT, member) for member in CLOCK_MEMBERS}, (
        "ask for the time left through ExecutionContext.time_left or deadline"
    )


def test_the_time_left_is_read_without_a_refusal_only_where_listed() -> None:
    reads = Counter(
        (module, owners[id(node)], node.func.attr)
        for module, tree in _modules()
        for owners in [_owners(tree)]
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in UNREFUSED_METHODS
    )
    assert reads == UNREFUSED_READERS, "ask for the time left through ExecutionContext.time_left or deadline"


def test_no_timeout_is_armed_outside_the_listed_waits() -> None:
    armed = Counter(
        (module, owners[id(node)], called)
        for module, tree in _modules()
        for owners, (modules, functions) in [(_owners(tree), _asyncio_names(tree))]
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and (called := _armed(node, modules, functions)) is not None
    )
    assert armed == TIMEOUTS, "wait under the operation's deadline with ExecutionContext.deadline"


def test_every_budget_error_constructed_is_listed() -> None:
    modules = _modules()
    errors = _budget_errors(modules)
    constructed: Counter[tuple[str, str, str]] = Counter()
    for module, tree in modules:
        owners = _owners(tree)
        for node in ast.walk(tree):
            # A call constructs one; so does raising the class itself.
            if isinstance(node, ast.Call) and _name(node.func) in errors:
                constructed[module, owners[id(node)], ast.unparse(node)] += 1
            elif isinstance(node, ast.Raise) and node.exc is not None and _name(node.exc) in errors:
                constructed[module, owners[id(node)], ast.unparse(node.exc)] += 1
    assert constructed == CONSTRUCTIONS, "refuse work by time through ExecutionContext.time_left or deadline"


def test_every_budget_error_class_is_seen() -> None:
    # The classes the AST walk finds are all those loaded (one built by a call, not a ``class``, would hide).
    def subclasses(cls: type[BaseException]) -> set[str]:
        return {cls.__name__}.union(*(subclasses(sub) for sub in cls.__subclasses__()))

    assert subclasses(BudgetExceededError) <= _budget_errors(_modules())


POLICY = ExecutionPolicy(max_elapsed=10.0, max_retry_elapsed_per_request=3.0)


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


async def _context(clock: _Clock) -> ExecutionContext:
    context = ExecutionContext(POLICY, RateCoordinator(), clock=clock)
    await context.start()
    return context


@pytest.mark.asyncio
async def test_a_retry_window_closing_first_refuses_its_request_alone() -> None:
    clock = _Clock()
    context = await _context(clock)
    # The window opened at 1 closes at 4, before the operation's deadline at 10.
    clock.now = 4.0
    with pytest.raises(BudgetExceededError) as refused:
        context.time_left("the send", retry_started=1.0)
    # The message says which window closed: the request's, not the operation's.
    assert str(refused.value) == RETRY_WINDOW_EXHAUSTED
    assert context.refusal is None
    assert context.time_left("the next binding") == pytest.approx(6.0)


@pytest.mark.asyncio
async def test_windows_closing_together_end_the_operation() -> None:
    clock = _Clock()
    context = await _context(clock)
    # The window opened at 7 closes at 10, with the operation's deadline: the operation has no time left either.
    clock.now = 10.0
    with pytest.raises(BudgetExceededError, match="the send"):
        context.time_left("the send", retry_started=7.0)
    assert context.refusal == "the send"


@pytest.mark.asyncio
async def test_the_first_refusal_is_the_one_kept() -> None:
    clock = _Clock()
    context = await _context(clock)
    clock.now = 10.0
    for work in ("the first send", "the second send"):
        with pytest.raises(BudgetExceededError, match=work):
            context.time_left(work)
    assert context.refusal == "the first send"
    with pytest.raises(BudgetExceededError, match="the first send"):
        context.raise_if_refused()


@pytest.mark.asyncio
async def test_the_coalescing_read_refuses_nothing() -> None:
    clock = _Clock()
    context = await _context(clock)
    clock.now = 11.0
    assert context.coalescing_time_left() == pytest.approx(-1.0)
    assert context.refusal is None


@pytest.mark.asyncio
async def test_the_operation_deadline_closing_first_ends_the_operation() -> None:
    clock = _Clock()
    context = await _context(clock)
    # The window opened at 8 would close at 11, after the operation's deadline at 10.
    clock.now = 10.0
    with pytest.raises(BudgetExceededError, match="the send"):
        context.time_left("the send", retry_started=8.0)
    assert context.refusal == "the send"
    with pytest.raises(BudgetExceededError, match="the send"):
        context.raise_if_refused()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("now", "retry_started", "recorded"),
    [
        # The window opened at 0 has the deadline's time left to run when it closes, at 3.
        (3.0 - DEADLINE_BUDGET, 0.0, None),
        # The window opened at 8 would close at 11; the operation's deadline at 10 closes first.
        (10.0 - DEADLINE_BUDGET, 8.0, "the wait"),
    ],
    ids=["retry_window_first", "operation_deadline_first"],
)
async def test_a_wait_under_the_deadline_records_only_the_operation_deadline(
    now: float, retry_started: float, recorded: str | None
) -> None:
    clock = _Clock()
    context = await _context(clock)
    clock.now = now
    never = asyncio.Event()
    with pytest.raises(BudgetExceededError, match=recorded or RETRY_WINDOW_EXHAUSTED):
        async with asyncio.timeout(HANG_BOUND), context.deadline("the wait", retry_started=retry_started):
            await never.wait()
    assert context.refusal == recorded


@pytest.mark.asyncio
async def test_a_wait_under_a_spent_deadline_is_refused_before_it_starts() -> None:
    clock = _Clock()
    context = await _context(clock)
    clock.now = 10.0
    started = False
    with pytest.raises(BudgetExceededError, match="the wait"):
        async with context.deadline("the wait"):
            started = True
    assert not started
    assert context.refusal == "the wait"


@pytest.mark.asyncio
async def test_a_wait_that_ends_in_time_is_not_refused() -> None:
    clock = _Clock()
    context = await _context(clock)
    ready = asyncio.Event()
    ready.set()
    async with context.deadline("the wait") as timeout:
        await ready.wait()
    assert not timeout.expired()
    assert context.refusal is None


# The executor and the batch engine ask the context for the time left at the permit, and when they plan a retry,
# a replay round or an envelope's first send: each refuses with no time left as ``time_left`` does.

RETRY = RetryPolicy(initial_delay=0.5, maximum_delay=0.5, backoff=1.0, jitter=0.0)
_JSON = (("content-type", "application/json"),)
_QUOTA = json.dumps({"error": "QUERY_LIMIT_EXCEEDED", "error_description": "Too many requests"}).encode()


def _get(index: int = 0) -> Request:
    return Request("crm.item.get", {"id": index}, ReplaySafety.SAFE, route=RouteKind.BARE)


def _refusal_at(clock: _Clock, now: float) -> ResponderTransport:
    """Refuse every send for quota (retryable, proven not run), the virtual clock reading ``now`` by the answer."""

    def respond(request: Request) -> WireResponse:
        del request
        clock.now = now
        return WireResponse(503, _JSON, _QUOTA)

    return ResponderTransport(respond)


def _executor(transport: ResponderTransport, clock: _Clock) -> Executor:
    coordinator = RateCoordinator(clock=clock)

    async def sleep(seconds: float) -> None:
        clock.now += seconds
        await asyncio.sleep(0)

    return Executor(transport, coordinator=coordinator, clock=clock, sleep=sleep, random_source=lambda: 0.5)


async def _started(executor: Executor, policy: ExecutionPolicy) -> ExecutionContext:
    context = executor.context(policy)
    await context.start()
    return context


@pytest.mark.asyncio
async def test_a_send_with_no_time_left_before_its_permit_ends_the_operation() -> None:
    clock = _Clock()
    transport = ResponderTransport(always(WireResponse(200, _JSON, b'{"result":[]}')))
    executor = _executor(transport, clock)
    context = await _started(executor, POLICY)
    clock.now = 10.0

    with pytest.raises(BudgetExceededError, match="execution time budget exhausted"):
        await executor.execute(_get(), context=context)

    assert transport.requests == []
    assert context.refusal == "execution time budget exhausted"
    await executor.coordinator.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("policy", "answered_at", "message", "recorded"),
    [
        # The deadline at 10 has passed when the refusal is answered: the retry is refused by time, recorded.
        (
            ExecutionPolicy(max_elapsed=10.0, max_retry_elapsed_per_request=1000.0, retry=RETRY),
            10.0,
            "execution time budget exhausted before the retry",
            True,
        ),
        # The request's retry window (opened at 0) closed at 3, before the operation's deadline: that request alone.
        (
            ExecutionPolicy(max_elapsed=10.0, max_retry_elapsed_per_request=3.0, retry=RETRY),
            3.0,
            RETRY_WINDOW_EXHAUSTED,
            False,
        ),
        # Time is left, but less than the retry's delay: the request alone, refused while time is left.
        (
            ExecutionPolicy(max_elapsed=10.0, max_retry_elapsed_per_request=1000.0, retry=RETRY),
            9.75,
            "retry delay would exceed elapsed budget",
            False,
        ),
    ],
    ids=["operation_deadline", "retry_window_first", "delay_longer_than_the_time_left"],
)
async def test_a_retry_refused_by_time_ends_the_operation_only_at_its_deadline(
    policy: ExecutionPolicy,
    answered_at: float,
    message: str,
    recorded: bool,  # noqa: FBT001
) -> None:
    clock = _Clock()
    transport = _refusal_at(clock, answered_at)
    executor = _executor(transport, clock)
    context = await _started(executor, policy)

    with pytest.raises(BudgetExceededError) as refused:
        await executor.execute(_get(), context=context)

    assert str(refused.value) == message
    assert isinstance(refused.value.__cause__, B24ApiError)
    assert len(transport.requests) == 1
    assert context.refusal == (message if recorded else None)
    await executor.coordinator.close()


@pytest.mark.asyncio
async def test_a_batch_replay_round_with_no_time_left_ends_the_operation() -> None:
    clock = _Clock()
    transport = _refusal_at(clock, 10.0)
    executor = _executor(transport, clock)
    context = await _started(executor, ExecutionPolicy(max_elapsed=10.0, retry=RETRY))

    outcomes = await BatchExecutor(executor).execute_requests((_get(0), _get(1)), context=context)

    assert len(transport.requests) == 1
    for outcome in outcomes:
        assert isinstance(outcome, BatchFailure)
        assert isinstance(outcome.error, BudgetExceededError)
        assert str(outcome.error) == "execution time budget exhausted before the retry"
    assert context.refusal == "execution time budget exhausted before the retry"
    await executor.coordinator.close()


@pytest.mark.asyncio
async def test_an_envelope_planned_with_no_time_left_is_refused_whole_and_ends_the_operation() -> None:
    clock = _Clock()
    transport = ResponderTransport(always(WireResponse(200, _JSON, b'{"result":{}}')))
    executor = _executor(transport, clock)
    context = await _started(executor, POLICY)
    clock.now = 10.0
    commands = tuple(_Command(index, f"c{index:012d}", _get(index), None) for index in range(2))

    outcomes = await BatchExecutor(executor).execute_chunk(commands, context=context, halt=False, split_held=True)

    assert transport.requests == []
    assert [str(outcome.error) for outcome in outcomes if isinstance(outcome, BatchFailure)] == [
        "execution time budget exhausted"
    ] * 2
    assert context.refusal == "execution time budget exhausted"
    await executor.coordinator.close()


@pytest.mark.asyncio
async def test_a_deadline_passing_while_the_first_send_is_planned_is_recorded_if_it_refuses() -> None:
    # The plan reads the deadline more than once; it passes after the first read. Whichever read refuses, the
    # refusal is the operation's deadline: recorded, as at the permit.
    reads = iter((0.0, 9.5))  # the start, then the plan's first read; every later read is past the deadline

    def crossing() -> float:
        return next(reads, 10.0)

    clock = _Clock()
    clock.now = 9.5  # the executor opens the send's window at the plan's first read
    executor = _executor(ResponderTransport(always(WireResponse(200, _JSON, b'{"result":{}}'))), clock)
    context = ExecutionContext(POLICY, executor.coordinator, clock=crossing)
    await context.start()

    refused: BudgetExceededError | None = None
    try:
        executor.plan_first_send(context, methods=(frozenset({"crm.item.get"}), frozenset({"crm.item.list"})))
    except BudgetExceededError as error:
        refused = error
    assert context.refusal == (None if refused is None else str(refused))
    await executor.coordinator.close()
