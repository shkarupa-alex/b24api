"""Retry delay and host throttle header parsing."""

from __future__ import annotations
import contextlib
import email.utils
import math
import time
from typing import TYPE_CHECKING

from b24api.errors import ApiResponseError, B24ApiError

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.contracts.policy import ExecutionPolicy
    from b24api.transport.base import WireResponse

_RETRY_AFTER_CAP_SECONDS = 3_600.0
_EPOCH_SECONDS_MINIMUM = 1e9


def _retry_delay(
    policy: ExecutionPolicy,
    *,
    retry_number: int,
    random_source: Callable[[], float],
) -> float:
    retry = policy.retry
    base = min(retry.maximum_delay, retry.initial_delay * retry.backoff ** max(0, retry_number - 1))
    if retry.jitter == 0 or base == 0:
        return base
    factor = 1 - retry.jitter + (2 * retry.jitter * random_source())
    return max(0.0, base * factor)


def _retry_after_seconds(wire: WireResponse) -> float | None:
    """Return the bounded host cooldown hinted by a response; ``Retry-After`` has priority."""
    headers = wire.header_map
    wall_now = time.time()
    retry_after = headers.get("retry-after")
    if retry_after:
        return parse_retry_after(retry_after, now=wall_now, cap=_RETRY_AFTER_CAP_SECONDS)
    reset = headers.get("x-bitrix-ratelimit-reset")
    if reset is None:
        return None
    return parse_rate_limit_reset(reset, wall_now=wall_now, cap=_RETRY_AFTER_CAP_SECONDS)


def parse_retry_after(raw: str, *, now: float, cap: float) -> float | None:
    """Parse RFC 9110 ``Retry-After`` as delta-seconds or an HTTP-date.

    Args:
        raw: Header value.
        now: Wall-clock ``time.time()`` reading an HTTP-date is measured against.
        cap: Upper bound of the returned delay in seconds.

    Returns:
        Delay in seconds within ``[0, cap]``, or ``None`` for an unusable value.
    """
    delay = _finite_non_negative(raw)
    if delay is not None:
        return min(delay, cap)
    return _http_date_delay(raw, now=now, cap=cap)


def parse_rate_limit_reset(raw: str, *, wall_now: float, cap: float) -> float | None:
    """Parse ``X-Bitrix-RateLimit-Reset`` as delta-seconds, epoch seconds or an HTTP-date.

    A number at or above ``1e9`` is an absolute Unix timestamp and is measured against the wall
    clock, never a monotonic one; smaller numbers are delta-seconds. The caller converts the
    returned delay into a monotonic deadline.

    Args:
        raw: Header value.
        wall_now: Wall-clock ``time.time()`` reading.
        cap: Upper bound of the returned delay in seconds.

    Returns:
        Delay in seconds within ``[0, cap]``, or ``None`` for an unusable value.
    """
    value = _finite_non_negative(raw)
    if value is None:
        return _http_date_delay(raw, now=wall_now, cap=cap)
    if value >= _EPOCH_SECONDS_MINIMUM:
        value = max(0.0, value - wall_now)
    return min(value, cap)


def _finite_non_negative(raw: str) -> float | None:
    with contextlib.suppress(ValueError):
        value = float(raw)
        if math.isfinite(value) and value >= 0:
            return value
    return None


def _http_date_delay(raw: str, *, now: float, cap: float) -> float | None:
    with contextlib.suppress(TypeError, ValueError, OverflowError):
        parsed = email.utils.parsedate_to_datetime(raw)
        return min(max(0.0, parsed.timestamp() - now), cap)
    return None


def _throttle_reason(error: B24ApiError) -> str:
    if isinstance(error, ApiResponseError):
        return error.normalized_code
    return f"http_{error.http_status}"
