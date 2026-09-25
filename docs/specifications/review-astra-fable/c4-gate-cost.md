# C4: completion gate cost before and after the lifecycle move

Design record for C4 of the astra-fable review. The decision is to keep one gate guarantee for
batch and fan-out and to drop only the empty B12 digest. Moving the lifecycle onto
`OperationRunner` (§3.1) and splitting the gate's `emit` under 100 lines (B25, `d7c4a48`) must not
change what the gate costs or what it decides. The second half is proven by the golden traces:
public states, reasons and violations match `c4cafdd` except for the declared deltas in
`tests/golden/DELTAS.md`. This record measures the first half.

## Method

The script below drives the public client against an in-process portal that answers every batch
and every call. Each workload runs 10,000 commands: `batch()` over 200 physical batches, and
`fan_out()` with `BatchDispatch()`, whose default 20 ms coalescing window dominates its wall time.

- **Wall time.** Median of 5 runs after one warm-up.
- **Gate cost.** One cProfile run gives the self time of `b24api/completion/gate.py` and the number
  of `CompletionGate.emit` calls.
- **Runs.** The base tree (`c4cafdd`) was imported from a detached worktree through `PYTHONPATH`.
  Base and head (`13ecfc6`) ran back to back on the same machine (macOS, CPython 3.12).

## Results

| Workload | Tree | Wall, ms (median) | `emit` calls | Gate self time, profiled ms | Gate function calls |
|---|---|---|---|---|---|
| `batch` | `c4cafdd` | 1774.8 | 70,002 | 69.9 | 220,009 |
| `batch` | `13ecfc6` | 1767.5 | 70,002 | 124.8 | 370,013 |
| `fan_out` | `c4cafdd` | 23,034.6 | 70,002 | 128.3 | — |
| `fan_out` | `13ecfc6` | 22,934.9 | 70,002 | 230.8 | — |

- **What the gate receives.** Exactly the same events on both trees: 7 per command plus 2 per
  operation.
- **Wall time.** Unchanged within noise. An unprofiled rerun of `batch` gave 1767 ms on the base
  and 1780 ms on the head.
- **Profiled self time.** Higher on the head because B25 split `emit` into helpers. The gate module
  now runs 21 functions instead of 11, which is 370,013 calls instead of 220,009, and cProfile
  charges each call. Without the profiler the difference does not show in wall time. In both
  trees the gate stays below 4% of profiled time for `batch` and 0.4% for `fan_out`.

## Outcome

The lifecycle move kept the gate's cost and its decisions. There is no reason to simplify
completion for batch or fan-out beyond B12.

## Script

```python
"""C4: completion-gate cost for batch() and fan_out(BatchDispatch) on 10,000 scripted commands."""
import asyncio, cProfile, json, pstats, statistics, sys, time, warnings
warnings.simplefilter("ignore", DeprecationWarning)
import b24api
from b24api import BatchDispatch, Bitrix24, ReplaySafety, Request, RouteKind, Settings
try:
    from b24api.contracts import Command
except ImportError:
    from b24api import Command
try:
    from b24api.transport import WireResponse
except ImportError:
    from b24api import WireResponse

N = 10_000
HEADERS = (("content-type", "application/json"),)

class Portal:
    host = "fixture.invalid"
    async def send(self, request, *, attempt_timeout, max_response_bytes):
        params = request.copy_parameters() if hasattr(request, "copy_parameters") else dict(request.parameters)
        if request.method == "batch":
            keys = list(params["cmd"])
            body = {"result": {"result": {k: {"ok": True} for k in keys}, "result_error": [], "result_total": [], "result_next": [], "result_time": []}}
        else:
            body = {"result": {"ok": True}}
        return WireResponse(200, HEADERS, json.dumps(body).encode())

def commands():
    return (Command(Request("item.get", {"id": i}, ReplaySafety.SAFE, route=RouteKind.BARE), correlation=i) for i in range(N))

async def run(kind):
    async with Bitrix24(Settings(webhook_url="https://fixture.invalid/rest/1/token/"), transport=Portal()) as client:
        stream = client.batch(commands()) if kind == "batch" else client.fan_out(commands(), dispatch=BatchDispatch())
        async with stream:
            count = sum([1 async for _ in stream])
        assert count == N, count
        return stream.report

def main():
    out = {"b24api": b24api.__file__.rsplit("/b24api/", 1)[0].rsplit("/", 1)[-1]}
    for kind in ("batch", "fan_out"):
        asyncio.run(run(kind))  # warm-up
        walls = []
        for _ in range(5):
            start = time.perf_counter(); asyncio.run(run(kind)); walls.append(time.perf_counter() - start)
        profile = cProfile.Profile(); profile.enable(); report = asyncio.run(run(kind)); profile.disable()
        stats = pstats.Stats(profile).stats
        gate = [(f, v) for f, v in stats.items() if f[0].endswith("completion/gate.py")]
        gate_own = sum(v[2] for _, v in gate)
        emits = sum(v[1] for f, v in gate if f[2] == "emit")
        total = sum(v[2] for v in stats.values())
        out[kind] = {
            "wall_ms_median": round(statistics.median(walls) * 1000, 1),
            "gate_self_ms_profiled": round(gate_own * 1000, 1),
            "gate_share_pct": round(100 * gate_own / total, 2),
            "gate_emit_calls": emits,
            "state": report.state.value, "physical_requests": report.physical_requests,
        }
    print(json.dumps(out))
main()
```
