# B8 and B12: 16 MiB benchmark

Design record for findings B8 (one strict JSON parse of a success body) and B12 (no per-page identity
digest) of the astra-fable review. Both outcomes were decided on correctness grounds: B8 removes a
second parse whose result was never needed, B12 removes a digest the gate only checked for being
non-empty. The benchmark confirms that neither change costs time. It does not gate either decision.

## Method

The script below drives the public client against an in-process transport. `call` decodes one
15.8 MiB success body. `list` walks the same volume as 323 offset pages of 50 rows each, so every
page goes through validation and the completion recorder. Each run takes the median of 7 timings
after one warm-up. The baseline tree (`1640fb8`) and the changed tree ran back to back in pairs
on the same machine, with the baseline imported from a detached worktree through `PYTHONPATH`.

## Results (macOS, CPython 3.12, milliseconds, median)

| Pair | `call` before | `call` after | `list` before | `list` after |
|---|---|---|---|---|
| 1 | 65.5 | 54.2 | 770.6 | 758.2 |
| 2 | 63.9 | 57.2 | 898.6 | 897.8 |

- B8: about 15% less time for a 16 MiB call. The error codec no longer parses a success body that
  has no top-level `error`.
- B12: neutral within noise. The shift between pairs comes from the machine, not the change: the
  per-page SHA-256 covered only 50 small identities.

## Behavior kept

A JSON success body goes through one strict `json.loads` of its UTF-8 text, which rejects NaN and
Infinity and, for physical batches, duplicate correlation keys. A body without a top-level `error`
skips the error codec, which would have returned no error for it anyway. A body with a top-level
`error`, and a body that fails the strict parse, still go to the codec with the raw bytes. So
structured errors keep their preview, syntax errors stay a `ProtocolError`, and defects only the
strict parse catches (invalid UTF-8, non-finite numbers, duplicate correlation keys) stay an
`EnvelopeContractError`. `tests/internal/execution_boundary_test.py` pins one parse per success
body for a direct call and for a batch.

## Script

```python
"""16 MiB benchmark for B8 (one strict parse) and B12 (no identity digest)."""

import asyncio
import json
import statistics
import sys
import time

from b24api import Bitrix24, ExecutionPolicy, ReplaySafety, Request, RouteKind, Settings
from b24api.execution import WireResponse

MIB = 1024 * 1024
PAGE = 50
ROW_PAD = "x" * 1000
ROWS = 16 * MIB // (len(ROW_PAD) + 40)


def _row(index: int) -> dict[str, object]:
    return {"ID": str(index + 1), "TITLE": ROW_PAD}


BIG_CALL = json.dumps({"result": [_row(index) for index in range(ROWS)]}).encode()
PAGES = {
    start: json.dumps(
        {
            "result": [_row(index) for index in range(start, min(start + PAGE, ROWS))],
            "total": ROWS,
            **({"next": start + PAGE} if start + PAGE < ROWS else {}),
        },
    ).encode()
    for start in range(0, ROWS, PAGE)
}

EMPTY = json.dumps({"result": [], "total": ROWS}).encode()


class Transport:
    host = "bench.invalid"

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        if request.method == "big.get":
            body = BIG_CALL
        else:
            body = PAGES.get(int(request.copy_parameters().get("start", 0)), EMPTY)
        return WireResponse(200, (("content-type", "application/json"),), body)


def _client() -> Bitrix24:
    policy = ExecutionPolicy(max_response_bytes=64 * MIB, max_pages=100_000, max_elapsed=600.0)
    return Bitrix24(Settings(webhook_url="https://bench.invalid/rest/1/token/"), transport=Transport(), policy=policy)


async def call_once() -> float:
    client = _client()
    started = time.perf_counter()
    await client.call(Request("big.get", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE))
    elapsed = time.perf_counter() - started
    await client.aclose()
    return elapsed


async def list_once() -> float:
    client = _client()
    started = time.perf_counter()
    rows = 0
    async with client.iter_list(Request("bench.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)) as stream:
        async for _row_value in stream:
            rows += 1
    elapsed = time.perf_counter() - started
    assert rows == ROWS, rows
    await client.aclose()
    return elapsed


async def main() -> None:
    repeats = int(sys.argv[1]) if len(sys.argv) > 1 else 7
    await call_once()
    await list_once()
    calls = [await call_once() for _ in range(repeats)]
    lists = [await list_once() for _ in range(repeats)]
    print(f"body={len(BIG_CALL) / MIB:.1f}MiB pages={len(PAGES)} rows={ROWS}")
    print(f"call   median={statistics.median(calls) * 1000:.1f}ms min={min(calls) * 1000:.1f}ms")
    print(f"list   median={statistics.median(lists) * 1000:.1f}ms min={min(lists) * 1000:.1f}ms")


asyncio.run(main())
```
