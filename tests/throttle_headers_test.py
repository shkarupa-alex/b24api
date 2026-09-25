"""Host throttle header parsing: Retry-After priority and epoch-aware X-Bitrix-RateLimit-Reset."""

from __future__ import annotations
import email.utils
import json
import time

import pytest

from b24api import ReplaySafety, Request
from b24api.contracts.policy import ExecutionPolicy, RetryPolicy
from b24api.contracts.request import RouteKind
from b24api.errors import BudgetExceededError
from b24api.execution import CoordinatorState, Executor, RateCoordinator, WireResponse
from b24api.execution.throttle import parse_rate_limit_reset, parse_retry_after
from tests.scripting import ResponderTransport, always

HOST = "test.invalid"
CAP = 3_600.0
WALL_NOW = 1_800_000_000.0


def _json(status: int, payload: object, headers: tuple[tuple[str, str], ...] = ()) -> WireResponse:
    return WireResponse(status, (("content-type", "application/json"), *headers), json.dumps(payload).encode())


def _short_policy() -> ExecutionPolicy:
    return ExecutionPolicy(
        max_elapsed=1.0,
        max_retry_elapsed_per_request=1.0,
        retry=RetryPolicy(initial_delay=0, maximum_delay=0, jitter=0),
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("5", 5.0),
        ("0", 0.0),
        ("12.5", 12.5),
        (str(int(WALL_NOW) + 30), 30.0),
        (str(int(WALL_NOW) - 30), 0.0),
        (str(int(WALL_NOW) + 7_200), CAP),
        ("1000000000", 0.0),
        ("999999999", CAP),
        (email.utils.formatdate(WALL_NOW + 40, usegmt=True), 40.0),
        ("abc", None),
        ("-1", None),
        ("nan", None),
        ("inf", None),
        ("", None),
    ],
)
def test_rate_limit_reset_accepts_delta_epoch_and_http_date(raw: str, expected: float | None) -> None:
    assert parse_rate_limit_reset(raw, wall_now=WALL_NOW, cap=CAP) == expected


def test_retry_after_keeps_rfc_forms_without_epoch_reinterpretation() -> None:
    assert parse_retry_after("7", now=WALL_NOW, cap=CAP) == 7.0  # noqa: PLR2004
    assert parse_retry_after(str(int(WALL_NOW) + 30), now=WALL_NOW, cap=CAP) == CAP
    assert parse_retry_after(email.utils.formatdate(WALL_NOW + 40, usegmt=True), now=WALL_NOW, cap=CAP) == 40.0  # noqa: PLR2004
    assert parse_retry_after("soon", now=WALL_NOW, cap=CAP) is None


async def _cooldown_after(headers: tuple[tuple[str, str], ...]) -> float:
    coordinator = RateCoordinator()
    transport = ResponderTransport(always(_json(429, {"error": "QUERY_LIMIT_EXCEEDED"}, headers)))
    executor = Executor(transport, coordinator=coordinator)
    try:
        with pytest.raises(BudgetExceededError):
            await executor.execute(
                Request("sample.get", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
                policy=_short_policy(),
            )
        snapshot = await coordinator.snapshot()
        assert snapshot.cooldown_until is not None
        return snapshot.cooldown_until - time.monotonic()
    finally:
        await coordinator.close()


@pytest.mark.asyncio
async def test_epoch_rate_limit_reset_does_not_freeze_the_host_for_the_cap() -> None:
    remaining = await _cooldown_after((("x-bitrix-ratelimit-reset", str(int(time.time()) + 3)),))

    assert 0 < remaining <= 4  # noqa: PLR2004 - before A19 the epoch value froze the host for 3600 s


@pytest.mark.asyncio
async def test_retry_after_keeps_priority_over_rate_limit_reset() -> None:
    remaining = await _cooldown_after(
        (("retry-after", "2"), ("x-bitrix-ratelimit-reset", str(int(time.time()) + 600))),
    )

    assert 0 < remaining <= 2  # noqa: PLR2004


@pytest.mark.asyncio
async def test_invalid_rate_limit_reset_never_freezes_the_host() -> None:
    coordinator = RateCoordinator()
    transport = ResponderTransport(
        always(_json(429, {"error": "QUERY_LIMIT_EXCEEDED"}, (("x-bitrix-ratelimit-reset", "garbage"),)))
    )
    executor = Executor(transport, coordinator=coordinator)
    try:
        with pytest.raises(BudgetExceededError):
            await executor.execute(
                Request("sample.get", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
                policy=_short_policy(),
            )
        snapshot = await coordinator.snapshot()
        assert snapshot.state is CoordinatorState.OPEN
        assert snapshot.cooldown_until is None
    finally:
        await coordinator.close()
