"""Rate coordinator ownership by the client and synchronous permit accounting."""

from __future__ import annotations
import asyncio
import json

import pytest

from b24api import Bitrix24, ReplaySafety, Request, Settings
from b24api.contracts.policy import ExecutionPolicy, RetryPolicy
from b24api.contracts.request import RouteKind
from b24api.errors import BudgetExceededError
from b24api.execution import CoordinatorState, Executor, RateCoordinator, WireResponse, WorkClass
from b24api.execution.cleanup import close_owned_resources

HOST = "test.invalid"
WAKE_TASK_NAME = "b24api-rate-wake"


class _Transport:
    """Answer every request with one fixed wire response."""

    host = HOST

    def __init__(self, response: WireResponse) -> None:
        self.response = response
        self.calls = 0
        self.closed = False

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del request, attempt_timeout, max_response_bytes
        self.calls += 1
        return self.response

    async def aclose(self) -> None:
        self.closed = True


def _json(status: int, payload: object, headers: tuple[tuple[str, str], ...] = ()) -> WireResponse:
    return WireResponse(status, (("content-type", "application/json"), *headers), json.dumps(payload).encode())


def _wake_tasks() -> list[asyncio.Task[object]]:
    return [task for task in asyncio.all_tasks() if task.get_name() == WAKE_TASK_NAME]


def _short_policy() -> ExecutionPolicy:
    return ExecutionPolicy(
        max_elapsed=1.0,
        max_retry_elapsed_per_request=1.0,
        retry=RetryPolicy(initial_delay=0, maximum_delay=0, jitter=0),
    )


# --- A5 --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_client_closes_its_own_coordinator_and_awaits_the_wake_task() -> None:
    transport = _Transport(_json(200, {"error": "OPERATION_TIME_LIMIT", "error_description": "wait"}))
    settings = Settings(webhook_url=f"https://{HOST}/rest/1/token/")

    async with Bitrix24(settings, transport=transport) as client:
        coordinator = client._executor.coordinator  # noqa: SLF001 - ownership observation
        with pytest.raises(BudgetExceededError):
            await client.call(
                Request("sample.get", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
                policy=_short_policy(),
            )
        assert _wake_tasks(), "OPERATION_TIME_LIMIT must schedule the named cooldown wake task"

    assert (await coordinator.snapshot()).state is CoordinatorState.CLOSED
    assert coordinator._wake_task is None  # noqa: SLF001 - owned background task was released
    assert _wake_tasks() == []
    assert not transport.closed, "an injected transport stays caller-owned"


@pytest.mark.asyncio
async def test_client_over_injected_executor_leaves_the_coordinator_to_its_owner() -> None:
    coordinator = RateCoordinator()
    executor = Executor(_Transport(_json(200, {"result": True})), coordinator=coordinator)
    client = Bitrix24._from_executor(executor)  # noqa: SLF001 - caller-owned executor seam

    await client.aclose()

    assert (await coordinator.snapshot()).state is CoordinatorState.OPEN
    await coordinator.aclose()
    assert (await coordinator.snapshot()).state is CoordinatorState.CLOSED


class _Resource:
    def __init__(self, name: str, order: list[str], *, fail: bool = False) -> None:
        self.name = name
        self.order = order
        self.fail = fail

    async def aclose(self) -> None:
        self.order.append(self.name)
        if self.fail:
            raise RuntimeError(f"{self.name} cleanup failed")


@pytest.mark.asyncio
async def test_owned_cleanup_closes_streams_then_coordinator_then_transport_keeping_the_first_failure() -> None:
    order: list[str] = []
    streams = (_Resource("stream", order), _Resource("failing-stream", order, fail=True))

    with pytest.raises(RuntimeError, match="failing-stream cleanup failed") as captured:
        await close_owned_resources(
            streams,
            _Resource("transport", order),
            _Resource("coordinator", order, fail=True),
        )

    assert order == ["stream", "failing-stream", "coordinator", "transport"]
    assert captured.value.__notes__ == ["coordinator cleanup failure: RuntimeError"]


@pytest.mark.asyncio
async def test_coordinator_close_is_idempotent_and_names_its_wake_task() -> None:
    coordinator = RateCoordinator()
    await coordinator.observe_throttle(60, reason="test")
    assert len(_wake_tasks()) == 1

    await coordinator.close()
    await coordinator.aclose()

    assert _wake_tasks() == []
    assert (await coordinator.snapshot()).state is CoordinatorState.CLOSED


# --- B0 --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_synchronous_release_hands_capacity_over_before_marking_the_permit() -> None:
    coordinator = RateCoordinator(max_concurrency=1)
    held = await coordinator.acquire(WorkClass.BATCH, methods=frozenset({"profile"}))
    waiter = asyncio.create_task(coordinator.acquire(WorkClass.RETRY, methods=frozenset({"profile"})))
    await asyncio.sleep(0)

    coordinator._active = 0  # noqa: SLF001 - force an accounting failure
    with pytest.raises(RuntimeError, match="underflow"):
        held.release_now()
    assert held._released is False  # noqa: SLF001 - the flag follows successful accounting only
    coordinator._active = 1  # noqa: SLF001

    held.release_now()  # no await: accounting and the grant happen synchronously

    assert held._released is True  # noqa: SLF001
    assert coordinator._active == 1  # noqa: SLF001 - capacity went straight to the waiter
    replacement = await asyncio.wait_for(waiter, timeout=1)
    await held.release()
    assert (await coordinator.snapshot()).active_permits == 1
    await replacement.release()
    assert (await coordinator.snapshot()).active_permits == 0
    await coordinator.close()
