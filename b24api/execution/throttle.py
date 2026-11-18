"""Retry delay and host throttle header parsing."""

from __future__ import annotations
import contextlib
import datetime
import email.utils
import math
import sys
import time
from enum import Enum
from typing import TYPE_CHECKING

from b24api.contracts.v3_codes import RATE_LIMIT_ERROR_CODE
from b24api.errors import ApiResponseError, B24ApiError

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.contracts.policy import ExecutionPolicy
    from b24api.transport.base import WireResponse

_RETRY_AFTER_CAP_SECONDS = 3_600.0
_EPOCH_SECONDS_MINIMUM = 1e9
# How far a response's ``Date`` may lag the local clock and still measure a dated hint (``_hint_now``).
_DATE_LAG_TOLERANCE_SECONDS = 60.0


# A power whose natural logarithm stays below this is a float, with a margin for rounding in the logarithm.
_LOG_FLOAT_MAX = math.log(sys.float_info.max) - 1


def _retry_delay(
    policy: ExecutionPolicy,
    *,
    retry_number: int,
    random_source: Callable[[], float],
) -> float:
    retry = policy.retry
    base = backoff_delay(policy, retry_number)
    if retry.jitter == 0 or base == 0:
        return base
    factor = 1 - retry.jitter + (2 * retry.jitter * random_source())
    return max(0.0, base * factor)


def backoff_delay(policy: ExecutionPolicy, number: int) -> float:
    """Return the ``number``-th exponential delay of ``policy.retry``, without jitter, saturated at its maximum.

    The delay is taken only below the exponent at which it reaches the maximum, so it is a float itself. The power
    alone may not be one, when the initial delay is tiny and the maximum huge: the product is then taken in
    logarithms. So no retry count, however large, and no valid policy overflows it.
    """
    retry = policy.retry
    exponent = max(0, number - 1)
    if retry.initial_delay == 0 or retry.backoff == 1:
        return retry.initial_delay
    if exponent >= (math.log(retry.maximum_delay) - math.log(retry.initial_delay)) / math.log(retry.backoff):
        return retry.maximum_delay
    if exponent * math.log(retry.backoff) < _LOG_FLOAT_MAX:
        return min(retry.maximum_delay, retry.initial_delay * retry.backoff**exponent)
    return min(retry.maximum_delay, math.exp(math.log(retry.initial_delay) + exponent * math.log(retry.backoff)))


def _retry_after_seconds(wire: WireResponse) -> float | None:
    """Return the bounded host cooldown hinted by a response; ``Retry-After`` has priority.

    A hint given as a date or an epoch is measured from ``_hint_now``: the response's own ``Date`` when plausible,
    else the local wall clock.
    """
    headers = wire.header_map
    wall_now = _hint_now(headers.get("date"))
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


def operating_reset_delay(reset_at: float, date: str | None) -> float:
    """Return the seconds until an epoch ``operating_reset_at``, measured as a dated hint is, within the cap."""
    return min(max(0.0, reset_at - _hint_now(date)), _RETRY_AFTER_CAP_SECONDS)


def _hint_now(date: str | None) -> float:
    """Return the epoch time a dated hint is measured from: the response's ``Date`` if plausible, else the local clock.

    The server's clock set the hint, so its ``Date`` keeps a local clock running ahead from shortening the wait. A
    ``Date`` more than a minute behind the local clock is not plausible: a stale answer (from a cache or a relay) or a
    server clock set back would lengthen the wait by as much, up to the cap, while the hint holds the whole host. The
    local clock then measures the hint, as it does without a usable ``Date``. A ``Date`` ahead of the local clock only
    shortens the wait. So a dated hint never waits more than a minute longer than the local clock measures, and
    never longer than the cap.
    """
    local = time.time()
    server = _http_date(date) if date else None
    if server is None or server < local - _DATE_LAG_TOLERANCE_SECONDS:
        return local
    return server


def _http_date(raw: str) -> float | None:
    """Return the epoch time of an HTTP date, or ``None`` for an unusable value.

    RFC 9110 dates are GMT, so a date without a zone (the asctime form or ``-0000``) is read as UTC, never as
    the local time zone that a naive ``datetime`` would assume.
    """
    with contextlib.suppress(TypeError, ValueError, OverflowError):
        parsed = email.utils.parsedate_to_datetime(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.UTC)
        return parsed.timestamp()
    return None


def _finite_non_negative(raw: str) -> float | None:
    with contextlib.suppress(ValueError):
        value = float(raw)
        if math.isfinite(value) and value >= 0:
            return value
    return None


def _http_date_delay(raw: str, *, now: float, cap: float) -> float | None:
    moment = _http_date(raw)
    return None if moment is None else min(max(0.0, moment - now), cap)


class Limit(Enum):
    """A server's load limit a refusal names: the whole portal's request rate, or one method's operating time."""

    HOST = "host"
    METHOD = "method"


# The request-rate refusals: the classic quota and the REST 3.0 load limiter.
_HOST_LIMIT_CODES = frozenset({"query_limit_exceeded", RATE_LIMIT_ERROR_CODE.casefold()})
_METHOD_LIMIT_CODE = "operation_time_limit"
_HTTP_TOO_MANY_REQUESTS = 429
# The statuses whose throttle hint a server sends about its load, whatever their body says.
_LOAD_STATUSES = frozenset({_HTTP_TOO_MANY_REQUESTS, 503})


def limit_of(error: B24ApiError) -> Limit | None:
    """Return the load limit a refusal names, if it names one: by its code, or HTTP 429."""
    code = error.normalized_code if isinstance(error, ApiResponseError) else None
    if code == _METHOD_LIMIT_CODE:
        return Limit.METHOD
    if code in _HOST_LIMIT_CODES or error.http_status == _HTTP_TOO_MANY_REQUESTS:
        return Limit.HOST
    return None


def hints_load(error: B24ApiError) -> bool:
    """Return whether a failed answer's throttle hint is about the server's load: 429, 503 or a limit's code.

    Any other failure's hint (a ``Retry-After`` on a 401 or a 404) says nothing about the load, and never holds
    the host.
    """
    return error.http_status in _LOAD_STATUSES or limit_of(error) is not None


def _throttle_reason(error: B24ApiError) -> str:
    if isinstance(error, ApiResponseError):
        return error.normalized_code
    return f"http_{error.http_status}"
