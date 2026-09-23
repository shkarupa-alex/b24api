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
    headers = wire.header_map
    raw = headers.get("retry-after") or headers.get("x-bitrix-ratelimit-reset")
    if raw is None:
        return None
    with contextlib.suppress(ValueError):
        value = float(raw)
        if math.isfinite(value) and value >= 0:
            return min(value, _RETRY_AFTER_CAP_SECONDS)
    with contextlib.suppress(TypeError, ValueError, OverflowError):
        parsed = email.utils.parsedate_to_datetime(raw)
        return min(max(0.0, parsed.timestamp() - time.time()), _RETRY_AFTER_CAP_SECONDS)
    return None


def _throttle_reason(error: B24ApiError) -> str:
    if isinstance(error, ApiResponseError):
        return error.normalized_code
    return f"http_{error.http_status}"
